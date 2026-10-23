//! Moving a key's rows from one region to another, and the directory table
//! that records where every key lives.
//!
//! `RELOCATE <key> TO <region>` moves the key in every geo-partitioned table
//! at once. Rows that share a key are joined on it; moving one table's rows
//! alone would turn those joins into joins across regions.
//!
//! The move is one distributed transaction: the rows are inserted in the
//! destination, deleted from the source, and the directory row changed in
//! every region, all prepared and committed together. Either all of that
//! happens or none of it does, so there is no half-moved key to repair and
//! no moment, once it commits, when a key's rows are in two regions or none.
//!
//! Around the transaction:
//!
//! 1. **Cutover.** Writes to the key wait. The move then waits for writes
//!    that were planned before the cutover and are still under way, so none
//!    of them can land in the source after its rows have been taken.
//! 2. **Copy.** In the source, the tables are locked against writes — reads
//!    carry on — which also waits for any open transaction that has written
//!    to them, then the rows are read, inserted in the destination and
//!    deleted from the source, each count checked against the last.
//! 3. **Commit.** Reads of the key wait too while the regions commit, since
//!    they commit one after another. Then the key is resident in the
//!    destination and everything that waited carries on there.
//!
//! Anything that fails before the commit leaves everything as it was.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use luxbyte_core::Refusal;
use luxbyte_core::directory::{Directory, KeyState, Location};
use luxbyte_core::topology::Placement;
use luxbyte_pool::{Pool, PoolError, QueryResult};
use luxbyte_sql::Access;
use luxbyte_txn::{CommitOutcome, Transaction, TxnError};
use tokio::sync::Notify;

use crate::config::DirectoryStore;
use crate::session::Shared;

/// Counts the writes under way by when they were planned, so that a move
/// waits for exactly the writes that were planned before it began, however
/// many start after.
#[derive(Debug, Default)]
struct Gate {
    state: parking_lot::Mutex<GateState>,
    drained: Notify,
}

#[derive(Debug, Default)]
struct GateState {
    generation: u64,
    /// Writes under way, by the generation they were planned in.
    open: BTreeMap<u64, usize>,
}

/// A write under way. Dropping it says the write is over.
#[derive(Debug)]
pub struct Ticket {
    gate: Arc<Gate>,
    generation: u64,
}

impl Drop for Ticket {
    fn drop(&mut self) {
        let mut state = self.gate.state.lock();
        if let Some(count) = state.open.get_mut(&self.generation) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.open.remove(&self.generation);
            }
        }
        drop(state);
        self.gate.drained.notify_waiters();
    }
}

impl Gate {
    fn enter(self: &Arc<Self>) -> Ticket {
        let mut state = self.state.lock();
        let generation = state.generation;
        let count = state.open.entry(generation).or_insert(0);
        *count = count.saturating_add(1);
        Ticket {
            gate: Arc::clone(self),
            generation,
        }
    }

    /// Starts a new generation and returns it. Every write planned from
    /// here on sees the directory as it is now.
    fn advance(&self) -> u64 {
        let mut state = self.state.lock();
        state.generation = state.generation.saturating_add(1);
        state.generation
    }

    fn busy_before(&self, generation: u64) -> bool {
        self.state.lock().open.range(..generation).next().is_some()
    }

    /// Waits for every write planned before `generation` to finish.
    async fn drain(&self, generation: u64, limit: Duration) -> bool {
        let started = Instant::now();
        loop {
            let drained = self.drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();
            if !self.busy_before(generation) {
                return true;
            }
            let remaining = limit.saturating_sub(started.elapsed());
            if remaining.is_zero() || tokio::time::timeout(remaining, drained).await.is_err() {
                return !self.busy_before(generation);
            }
        }
    }
}

/// What the sessions share about moves.
#[derive(Debug)]
pub struct Moves {
    gate: Arc<Gate>,
    changed: Notify,
    running: tokio::sync::Mutex<()>,
    store: Option<DirectoryStore>,
}

impl Moves {
    /// Moves backed by `store`, or none at all without one.
    #[must_use]
    pub fn new(store: Option<DirectoryStore>) -> Self {
        Self {
            gate: Arc::default(),
            changed: Notify::new(),
            running: tokio::sync::Mutex::new(()),
            store,
        }
    }

    /// Marks a write as under way, before it is planned. Held until the
    /// statement is answered.
    #[must_use]
    pub fn write(&self) -> Ticket {
        self.gate.enter()
    }

    /// Resolves the next time a move changes phase or ends.
    pub fn changed(&self) -> tokio::sync::futures::Notified<'_> {
        self.changed.notified()
    }

    /// How long a statement waits for a moving key.
    #[must_use]
    pub fn hold(&self) -> Duration {
        Duration::from_millis(self.store.as_ref().map_or(0, |store| store.hold_ms))
    }

    /// The durable directory, when one is configured.
    #[must_use]
    pub const fn store(&self) -> Option<&DirectoryStore> {
        self.store.as_ref()
    }

    /// Wakes every statement waiting on a move, to plan again.
    pub(crate) fn announce(&self) {
        self.changed.notify_waiters();
    }

    /// Starts a new generation of writes: every write planned from here on
    /// sees the directory as it is now.
    pub(crate) fn advance(&self) -> u64 {
        self.gate.advance()
    }

    /// Waits up to `limit` for the writes planned before `generation`.
    pub(crate) async fn drained(&self, generation: u64, limit: Duration) -> bool {
        self.gate.drain(generation, limit).await
    }
}

// ------------------------------------------------------------- the command

/// Reads `RELOCATE <key> TO <region>`. `None` if the statement is something
/// else; `Some(Err(_))` if it is a malformed `RELOCATE`.
#[must_use]
pub fn parse(sql: &str) -> Option<Result<(String, String), String>> {
    let text = sql.trim().trim_end_matches(';').trim();
    let first = text.split_whitespace().next()?;
    if !first.eq_ignore_ascii_case("RELOCATE") {
        return None;
    }
    let usage = "say it as: RELOCATE <key> TO <region>, quoting a key that has spaces in it \
                 as 'like this'"
        .to_owned();
    let Some(words) = words(text.get(first.len()..).unwrap_or_default()) else {
        return Some(Err(usage));
    };
    Some(match words.as_slice() {
        [key, to, region] if to.eq_ignore_ascii_case("TO") => Ok((key.clone(), region.clone())),
        _ => Err(usage),
    })
}

/// A `REBALANCE`: move up to `limit` keys from one region to another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rebalance {
    /// Where the keys are.
    pub from: String,
    /// Where they go.
    pub to: String,
    /// The most keys to move.
    pub limit: u64,
}

/// How many keys a `REBALANCE` without `LIMIT` moves: enough to matter,
/// few enough that one command cannot hold an operator's terminal for long.
const REBALANCE_DEFAULT: u64 = 100;

/// Reads `REBALANCE <from> TO <to> [LIMIT <n>]`. `None` if the statement is
/// something else; `Some(Err(_))` if it is a malformed `REBALANCE`.
#[must_use]
pub fn parse_rebalance(sql: &str) -> Option<Result<Rebalance, String>> {
    let text = sql.trim().trim_end_matches(';').trim();
    let first = text.split_whitespace().next()?;
    if !first.eq_ignore_ascii_case("REBALANCE") {
        return None;
    }
    let usage = "say it as: REBALANCE <from-region> TO <to-region> [LIMIT <keys>]".to_owned();
    let Some(words) = words(text.get(first.len()..).unwrap_or_default()) else {
        return Some(Err(usage));
    };
    let rebalance = |from: &str, to: &str, limit: u64| Rebalance {
        from: from.to_owned(),
        to: to.to_owned(),
        limit,
    };
    Some(match words.as_slice() {
        [from, word, to] if word.eq_ignore_ascii_case("TO") => {
            Ok(rebalance(from, to, REBALANCE_DEFAULT))
        }
        [from, word, to, limit_word, limit]
            if word.eq_ignore_ascii_case("TO") && limit_word.eq_ignore_ascii_case("LIMIT") =>
        {
            match limit.parse::<u64>() {
                Ok(limit) if limit > 0 => Ok(rebalance(from, to, limit)),
                _ => Err(usage),
            }
        }
        _ => Err(usage),
    })
}

/// Moves up to `limit` keys from one region to another, one `RELOCATE` at
/// a time, and says what happened to each.
///
/// Each key moves atomically; the rebalance as a whole does not. It stops at
/// the first key that cannot move, since what stopped it — a lock, a proxy
/// that did not confirm — would most likely stop the next one too, and
/// reports the keys moved before it.
///
/// The keys are read from the source's rows, in order, so a key whose rows
/// predate the directory moves too.
///
/// # Errors
///
/// When the regions are wrong or the source cannot be read.
pub async fn rebalance(shared: Arc<Shared>, command: Rebalance) -> Result<QueryResult, Refusal> {
    let Rebalance { from, to, limit } = command;
    let topology = shared.router.topology();
    for region in [&from, &to] {
        if topology.region(region).is_none() {
            return Err(fail("22023", format!("there is no region \"{region}\"")));
        }
    }
    if from == to {
        return Err(fail("22023", "REBALANCE needs two different regions"));
    }
    let keys_query: Vec<String> = topology
        .tables
        .iter()
        .filter_map(|policy| match &policy.placement {
            Placement::GeoPartitioned { key } => Some(format!(
                "SELECT DISTINCT {}::text AS k FROM {}.{}",
                ident(key),
                ident(&policy.schema),
                ident(&policy.table)
            )),
            _ => None,
        })
        .collect();
    if keys_query.is_empty() {
        return Err(fail(
            "55000",
            "no table is geo_partitioned, so there is nothing to move",
        ));
    }
    let pool = shared
        .pool_for(&from, Access::ReadWrite, 0)
        .ok_or_else(|| fail("08000", format!("region {from} is not configured")))?;
    let found = pool
        .simple_query(&format!(
            "SELECT k FROM ({}) AS keys WHERE k IS NOT NULL ORDER BY k LIMIT {limit}",
            keys_query.join(" UNION ")
        ))
        .await
        .map_err(|error| region_error(&from, &error))?;
    let keys: Vec<String> = found
        .rows
        .iter()
        .filter_map(|row| row.first().cloned().flatten())
        .collect();

    let mut rows: Vec<Vec<Option<String>>> = Vec::with_capacity(keys.len());
    for key in keys {
        match relocate(Arc::clone(&shared), key.clone(), to.clone()).await {
            Ok(report) => {
                let moved: u64 = report.tables.iter().map(|table| table.rows).sum();
                rows.push(vec![
                    Some(key),
                    Some(moved.to_string()),
                    Some(report.state()),
                ]);
            }
            Err(refusal) => {
                rows.push(vec![
                    Some(key),
                    Some("0".to_owned()),
                    Some(format!("refused: {}; stopped here", refusal.message)),
                ]);
                break;
            }
        }
    }
    Ok(QueryResult {
        columns: ["key", "rows", "state"].map(ToOwned::to_owned).to_vec(),
        // text, int8, text
        type_oids: vec![25, 20, 25],
        tags: vec![format!("SELECT {}", rows.len())],
        rows,
        ..QueryResult::default()
    })
}

/// Splits on whitespace, reading `'quoted words'` with `''` for a quote.
fn words(text: &str) -> Option<Vec<String>> {
    let mut words = Vec::new();
    let mut chars = text.chars().peekable();
    loop {
        while chars.next_if(|c| c.is_whitespace()).is_some() {}
        let Some(first) = chars.next() else {
            return Some(words);
        };
        let mut word = String::new();
        if first == '\'' {
            loop {
                match chars.next()? {
                    '\'' if chars.next_if_eq(&'\'').is_some() => word.push('\''),
                    '\'' => break,
                    other => word.push(other),
                }
            }
        } else {
            word.push(first);
            while let Some(c) = chars.next_if(|c| !c.is_whitespace()) {
                word.push(c);
            }
        }
        words.push(word);
    }
}

/// What a move did to one table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Moved {
    /// The table.
    pub table: String,
    /// Where its rows were, if anywhere.
    pub from: Option<String>,
    /// How many rows moved.
    pub rows: u64,
}

/// What a move did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    /// Where the key went.
    pub to: String,
    /// Each table.
    pub tables: Vec<Moved>,
    /// Regions that have not yet acknowledged the commit. The move has
    /// happened; while the destination or a source is among them, the key
    /// waits for them.
    pub pending: Vec<String>,
}

impl Report {
    /// How the move ended, in a word or a few.
    #[must_use]
    pub fn state(&self) -> String {
        if self.pending.is_empty() {
            "moved".to_owned()
        } else {
            format!("moved; waiting for {} to confirm", self.pending.join(", "))
        }
    }

    /// The report as a result set.
    #[must_use]
    pub fn result(&self) -> QueryResult {
        let state = self.state();
        let rows: Vec<Vec<Option<String>>> = self
            .tables
            .iter()
            .map(|moved| {
                vec![
                    Some(moved.table.clone()),
                    moved.from.clone(),
                    Some(self.to.clone()),
                    Some(moved.rows.to_string()),
                    Some(state.clone()),
                ]
            })
            .collect();
        QueryResult {
            columns: ["table", "from", "to", "rows", "state"]
                .map(ToOwned::to_owned)
                .to_vec(),
            // text, text, text, int8, text
            type_oids: vec![25, 25, 25, 20, 25],
            tags: vec![format!("SELECT {}", rows.len())],
            rows,
            status: luxbyte_proto::backend::TransactionStatus::Idle,
        }
    }
}

fn fail(sqlstate: &str, message: impl Into<String>) -> Refusal {
    Refusal {
        sqlstate: sqlstate.to_owned(),
        message: message.into(),
        hint: None,
        retryable: false,
    }
}

fn hinted(sqlstate: &str, message: impl Into<String>, hint: impl Into<String>) -> Refusal {
    Refusal {
        hint: Some(hint.into()),
        ..fail(sqlstate, message)
    }
}

fn region_error(region: &str, error: &PoolError) -> Refusal {
    fail(
        error.sqlstate().unwrap_or("08000"),
        format!("{region}: {error}; nothing was moved"),
    )
}

fn txn_error(region: &str, error: &TxnError) -> Refusal {
    match error {
        TxnError::Pool(error) => region_error(region, error),
        other => fail("XX000", format!("{region}: {other}; nothing was moved")),
    }
}

/// One table's part in a move.
#[derive(Debug)]
struct Part {
    /// The name the directory knows it by.
    name: String,
    /// Qualified and quoted, for SQL.
    qualified: String,
    /// The key column, quoted.
    column: String,
    /// Where its rows are; `None` if it has none in any region.
    source: Option<String>,
    /// Whether the directory already had the key in the destination.
    settled: bool,
    /// The rows taken from the source, as a JSON array, and how many.
    rows: Option<(String, u64)>,
}

/// Undoes a cutover unless the move committed, however the move ends.
struct Cutover {
    shared: Arc<Shared>,
    key: String,
    tables: Vec<String>,
    committed: bool,
}

impl Drop for Cutover {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        for table in &self.tables {
            let _ = self.shared.directory.abort_relocation(table, &self.key);
        }
        self.shared.moves.announce();
    }
}

/// Moves `key` to `to`. Refused, with nothing changed, if anything stands
/// in the way.
///
/// Runs as a task of its own, so that a client hanging up cannot stop a
/// move halfway through its commit and leave prepared transactions holding
/// the source's tables locked.
pub async fn relocate(shared: Arc<Shared>, key: String, to: String) -> Result<Report, Refusal> {
    tokio::spawn(async move {
        let outcome = run_move(&shared, &key, &to).await;
        shared
            .metrics
            .relocation(if outcome.is_ok() { "moved" } else { "refused" });
        outcome
    })
    .await
    .unwrap_or_else(|_| Err(fail("XX000", "the move stopped unexpectedly")))
}

#[allow(clippy::too_many_lines)]
async fn run_move(shared: &Arc<Shared>, key: &str, to: &str) -> Result<Report, Refusal> {
    let Some(store) = shared.moves.store() else {
        return Err(hinted(
            "55000",
            "RELOCATE needs a durable directory",
            "add a [directory_store] section, so the directory changes in the same transaction \
             as the rows",
        ));
    };
    let topology = shared.router.topology();
    if topology.region(to).is_none() {
        return Err(fail("22023", format!("there is no region \"{to}\"")));
    }
    let lock_timeout = Duration::from_millis(store.lock_timeout_ms);

    // One at a time: two moves would each wait for the other's locks.
    let _running = shared.moves.running.lock().await;
    let mut parts: Vec<Part> = topology
        .tables
        .iter()
        .filter_map(|policy| match &policy.placement {
            Placement::GeoPartitioned { key: column } => Some(Part {
                name: policy.table.clone(),
                qualified: format!("{}.{}", ident(&policy.schema), ident(&policy.table)),
                column: ident(column),
                source: None,
                settled: false,
                rows: None,
            }),
            _ => None,
        })
        .collect();
    if parts.is_empty() {
        return Err(fail(
            "55000",
            "no table is geo_partitioned, so there is nothing to move",
        ));
    }

    // With other proxies sharing the directory, they hold the key too.
    let announcement = match &shared.peers {
        Some(peers) => Some(peers.announce(shared, key, to).await?),
        None => None,
    };

    // 1. Cutover: writes to the key wait from here on.
    let mut cutover = Cutover {
        shared: Arc::clone(shared),
        key: key.to_owned(),
        tables: Vec::new(),
        committed: false,
    };
    for part in &mut parts {
        part.settled = matches!(
            shared.directory.locate(&part.name, key),
            Location::Known(KeyState::Resident { ref region }) if region == to
        );
        shared
            .directory
            .begin_relocation(&part.name, key, to)
            .map_err(|error| fail("55006", error.to_string()))?;
        cutover.tables.push(part.name.clone());
    }
    shared.moves.announce();
    let generation = shared.moves.gate.advance();
    if !shared.moves.gate.drain(generation, lock_timeout).await {
        return Err(hinted(
            "55P03",
            format!(
                "writes already under way did not finish within {} ms; nothing was moved",
                store.lock_timeout_ms
            ),
            "try again when the database is less busy, or raise directory_store.lock_timeout_ms",
        ));
    }
    if let Some(announcement) = &announcement {
        announcement
            .confirmed(crate::peers::Phase::Cutover, lock_timeout)
            .await?;
    }

    // Where each table's rows are: as the shared directory says, when there
    // is one, since this proxy's copy may know less. A key the directory
    // did not know may have rows anywhere, so every region is asked.
    let shared_regions = match &shared.peers {
        Some(peers) => Some(
            peers
                .regions_of(shared, key)
                .await
                .map_err(|error| fail("57P03", format!("{error}; nothing was moved")))?,
        ),
        None => None,
    };
    let literal_key = literal(key);
    for part in &mut parts {
        part.source = match &shared_regions {
            Some(regions) => regions
                .iter()
                .find(|(table, _)| *table == part.name)
                .map(|(_, region)| region.clone()),
            None => match shared.directory.locate(&part.name, key) {
                Location::Known(state) => state.source().map(ToOwned::to_owned),
                Location::Unseen => None,
            },
        };
        if shared_regions.is_some() {
            part.settled = part.source.as_deref() == Some(to);
        }
        if part.source.is_none() {
            part.source = find_rows(shared, part, &literal_key).await?;
            let _ = shared
                .directory
                .found_source(&part.name, key, part.source.as_deref());
        }
    }
    if parts.iter().all(|part| part.settled) {
        // Already there: nothing to do, and the cutover is undone on return.
        return Ok(Report {
            to: to.to_owned(),
            tables: parts
                .iter()
                .map(|part| Moved {
                    table: part.name.clone(),
                    from: part.source.clone(),
                    rows: 0,
                })
                .collect(),
            pending: Vec::new(),
        });
    }

    // 2. The transaction.
    let sources: BTreeSet<String> = parts
        .iter()
        .filter_map(|part| part.source.clone())
        .filter(|source| source != to)
        .collect();
    let mut transaction = shared.coordinator.begin();
    let gid = transaction.gid().to_owned();
    if let Err(refusal) = copy(
        shared,
        &mut transaction,
        &mut parts,
        &sources,
        key,
        to,
        store,
    )
    .await
    {
        let _ = transaction.rollback().await;
        return Err(refusal);
    }

    // 3. Commit. Reads wait too while the regions commit one by one.
    for part in &parts {
        let _ = shared.directory.begin_committing(&part.name, key);
    }
    shared.moves.announce();
    if let Some(announcement) = &announcement {
        let agreed = match announcement.advance(crate::peers::Phase::Committing).await {
            Ok(()) => {
                announcement
                    .confirmed(crate::peers::Phase::Committing, lock_timeout)
                    .await
            }
            Err(refusal) => Err(refusal),
        };
        if let Err(refusal) = agreed {
            let _ = transaction.rollback().await;
            return Err(refusal);
        }
    }
    let outcome = transaction
        .commit()
        .await
        .map_err(|error| txn_error(to, &error))?;
    cutover.committed = true;
    let pending = match outcome {
        CommitOutcome::CommittedPendingAcknowledgement { unresolved, .. } => unresolved,
        _ => Vec::new(),
    };
    // The rows are where the key now is unless the destination or a source
    // has yet to confirm, and until they do the key keeps waiting.
    let holding = pending
        .iter()
        .any(|region| region == to || sources.contains(region));
    if !holding {
        settle(shared, &parts, key);
    }
    // The other proxies read where the key went from the authority region,
    // so they are let go once it has committed.
    let authority_pending = shared
        .peers
        .as_ref()
        .is_some_and(|peers| pending.iter().any(|region| region == peers.authority()));
    let announcement = match announcement {
        Some(announcement) if !holding && !authority_pending => {
            let tables: Vec<String> = parts.iter().map(|part| part.name.clone()).collect();
            announcement.moved(&tables, to);
            None
        }
        other => other,
    };
    if !pending.is_empty() {
        finish_later(
            shared,
            gid,
            &pending,
            holding,
            &parts,
            key,
            to,
            announcement,
        );
    }
    tracing::info!(key, to, "relocated a key");

    Ok(Report {
        to: to.to_owned(),
        tables: parts
            .iter()
            .map(|part| Moved {
                table: part.name.clone(),
                from: part.source.clone(),
                rows: part.rows.as_ref().map_or(0, |(_, count)| *count),
            })
            .collect(),
        pending,
    })
}

fn settle(shared: &Shared, parts: &[Part], key: &str) {
    for part in parts {
        let _ = shared.directory.finish_relocation(&part.name, key);
    }
    shared.moves.announce();
}

/// Keeps asking the regions that did not acknowledge a committed move until
/// they do, then lets the key go if it was waiting for them.
#[allow(clippy::too_many_arguments)]
fn finish_later(
    shared: &Arc<Shared>,
    gid: String,
    pending: &[String],
    holding: bool,
    parts: &[Part],
    key: &str,
    to: &str,
    announcement: Option<crate::peers::Announcement>,
) {
    let pools: Vec<Pool> = pending
        .iter()
        .filter_map(|region| shared.pool_for(region, Access::ReadWrite, 0))
        .collect();
    let tables: Vec<String> = parts.iter().map(|part| part.name.clone()).collect();
    let shared = Arc::clone(shared);
    let key = key.to_owned();
    let to = to.to_owned();
    tokio::spawn(async move {
        let mut wait = Duration::from_millis(250);
        loop {
            tokio::time::sleep(wait).await;
            if matches!(shared.coordinator.finish(&gid, &pools).await, Ok(true)) {
                break;
            }
            wait = wait.saturating_mul(2).min(Duration::from_secs(30));
        }
        if holding {
            for table in &tables {
                let _ = shared.directory.finish_relocation(table, &key);
            }
            shared.moves.announce();
        }
        if let Some(announcement) = announcement {
            announcement.moved(&tables, &to);
        }
        tracing::info!(%gid, "every region acknowledged a relocation");
    });
}

/// The regions holding rows for the key, asked of each primary so that a
/// lagging replica cannot hide one.
async fn find_rows(shared: &Shared, part: &Part, key: &str) -> Result<Option<String>, Refusal> {
    let probe = format!(
        "SELECT 1 FROM {} WHERE {} = {key} LIMIT 1",
        part.qualified, part.column
    );
    let mut found = Vec::new();
    for region in shared.router.topology().region_ids() {
        let Some(pool) = shared.pool_for(region, Access::ReadWrite, 0) else {
            continue;
        };
        let result = pool
            .simple_query(&probe)
            .await
            .map_err(|error| region_error(region, &error))?;
        if !result.rows.is_empty() {
            found.push(region.to_owned());
        }
    }
    match found.as_slice() {
        [] => Ok(None),
        [region] => Ok(Some(region.clone())),
        _ => Err(hinted(
            "55000",
            format!(
                "\"{}\" has rows for the key in {}, and the directory does not say which is \
                 right",
                part.name,
                found.join(" and ")
            ),
            "merge them into one region by hand, then RELOCATE",
        )),
    }
}

/// Copies the rows, deletes them from the source and records the new
/// region, all inside `transaction`. Nothing is committed here.
async fn copy(
    shared: &Shared,
    transaction: &mut Transaction,
    parts: &mut [Part],
    sources: &BTreeSet<String>,
    key: &str,
    to: &str,
    store: &DirectoryStore,
) -> Result<(), Refusal> {
    // The destination is enlisted first so that it commits first: a scan
    // of the whole table that lands between two commits sees the key's rows
    // twice rather than not at all.
    let mut order = vec![to.to_owned()];
    order.extend(sources.iter().cloned());
    order.extend(
        shared
            .router
            .topology()
            .region_ids()
            .filter(|region| *region != to && !sources.contains(*region))
            .map(ToOwned::to_owned),
    );
    // The move itself writes rows where the directory does not yet place
    // them, which the guards let through only for it.
    let set_timeout = format!(
        "SET LOCAL lock_timeout = '{}ms'; SET LOCAL {MOVING} = 'on'",
        store.lock_timeout_ms
    );
    for region in &order {
        let pool = shared
            .pool_for(region, Access::ReadWrite, 0)
            .ok_or_else(|| fail("08000", format!("region {region} is not configured")))?;
        transaction
            .enlist(region, &pool)
            .await
            .map_err(|error| txn_error(region, &error))?;
        run(transaction, region, &set_timeout).await?;
    }

    let literal_key = literal(key);
    take_rows(transaction, parts, sources, &literal_key).await?;
    let tables = dependency_order(transaction, parts, to).await?;
    place_rows(transaction, parts, &tables, &literal_key, to).await?;
    remove_rows(transaction, parts, &tables, &literal_key, to).await?;

    // The directory, in every region, in the same transaction.
    let values: Vec<String> = parts
        .iter()
        .map(|part| {
            format!(
                "({}, {}, {})",
                literal(&part.name),
                literal(key),
                literal(to)
            )
        })
        .collect();
    let record = format!(
        "INSERT INTO {} (table_name, shard_key, region) VALUES {} \
         ON CONFLICT (table_name, shard_key) DO UPDATE SET region = EXCLUDED.region, \
         moved_at = now()",
        store_table(store),
        values.join(", ")
    );
    for region in &order {
        run(transaction, region, &record).await?;
    }
    Ok(())
}

/// Reads the key's rows in each source, under a lock that keeps every other
/// writer out until the move commits and lets readers carry on.
async fn take_rows(
    transaction: &mut Transaction,
    parts: &mut [Part],
    sources: &BTreeSet<String>,
    literal_key: &str,
) -> Result<(), Refusal> {
    for source in sources {
        let tables: Vec<&str> = parts
            .iter()
            .filter(|part| part.source.as_deref() == Some(source.as_str()))
            .map(|part| part.qualified.as_str())
            .collect();
        run(
            transaction,
            source,
            &format!("LOCK TABLE {} IN EXCLUSIVE MODE", tables.join(", ")),
        )
        .await?;
        for part in parts
            .iter_mut()
            .filter(|part| part.source.as_deref() == Some(source.as_str()))
        {
            let read = format!(
                "SELECT coalesce(json_agg(t), '[]'::json)::text FROM {} AS t WHERE t.{} = \
                 {literal_key}",
                part.qualified, part.column
            );
            let json = first_value(&run(transaction, source, &read).await?)
                .unwrap_or_else(|| "[]".to_owned());
            let Ok(serde_json::Value::Array(rows)) = serde_json::from_str(&json) else {
                return Err(fail(
                    "XX000",
                    format!(
                        "{source} returned rows of \"{}\" that are not a JSON array",
                        part.name
                    ),
                ));
            };
            part.rows = Some((json, u64::try_from(rows.len()).unwrap_or(u64::MAX)));
        }
    }
    Ok(())
}

/// Inserts the rows in the destination, parents before children so that
/// foreign keys hold at every step, after checking it has none of its own.
async fn place_rows(
    transaction: &mut Transaction,
    parts: &[Part],
    tables: &[usize],
    literal_key: &str,
    to: &str,
) -> Result<(), Refusal> {
    for part in tables.iter().filter_map(|index| parts.get(*index)) {
        let Some(source) = part.source.as_deref().filter(|source| *source != to) else {
            continue;
        };
        let there = format!(
            "SELECT count(*) FROM {} WHERE {} = {literal_key}",
            part.qualified, part.column
        );
        let existing = first_value(&run(transaction, to, &there).await?)
            .and_then(|count| count.parse::<u64>().ok())
            .unwrap_or(0);
        if existing > 0 {
            return Err(hinted(
                "55000",
                format!(
                    "\"{}\" already has {existing} rows for the key in {to}, and moving the \
                     rows from {source} would put both sets there",
                    part.name
                ),
                "merge them into one region by hand, then RELOCATE",
            ));
        }
        let Some((json, count)) = &part.rows else {
            continue;
        };
        if *count == 0 {
            continue;
        }
        let insert = insert_statement(transaction, part, json, to).await?;
        let wrote = affected(&run(transaction, to, &insert).await?);
        if wrote != *count {
            return Err(fail(
                "XX000",
                format!(
                    "{to} took {wrote} of the {count} rows of \"{}\"; nothing was moved",
                    part.name
                ),
            ));
        }
    }
    Ok(())
}

/// Deletes the rows from the source, children before parents so that no
/// cascade removes a row before it is counted.
async fn remove_rows(
    transaction: &mut Transaction,
    parts: &[Part],
    tables: &[usize],
    literal_key: &str,
    to: &str,
) -> Result<(), Refusal> {
    for part in tables.iter().rev().filter_map(|index| parts.get(*index)) {
        let Some(source) = part.source.as_deref().filter(|source| *source != to) else {
            continue;
        };
        let count = part.rows.as_ref().map_or(0, |(_, count)| *count);
        let delete = format!(
            "DELETE FROM {} WHERE {} = {literal_key}",
            part.qualified, part.column
        );
        let deleted = affected(&run(transaction, source, &delete).await?);
        if deleted != count {
            return Err(fail(
                "XX000",
                format!(
                    "{source} deleted {deleted} rows of \"{}\" where {count} were copied; \
                     nothing was moved",
                    part.name
                ),
            ));
        }
    }
    Ok(())
}

/// The order to insert the tables in: each after every table it has a
/// foreign key onto. Refuses if a table outside the move has a foreign key
/// onto one inside it, since moving the rows would break it.
async fn dependency_order(
    transaction: &mut Transaction,
    parts: &[Part],
    to: &str,
) -> Result<Vec<usize>, Refusal> {
    let configured: Vec<usize> = (0..parts.len()).collect();
    let oids_query = format!(
        "SELECT {}",
        parts
            .iter()
            .map(|part| format!("{}::regclass::oid::text", literal(&part.qualified)))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let oids = run(transaction, to, &oids_query).await?;
    let Some(row) = oids.rows.first() else {
        return Ok(configured);
    };
    let oids: Vec<String> = row
        .iter()
        .map(|oid| oid.clone().unwrap_or_default())
        .collect();
    let index_of = |oid: &str| oids.iter().position(|candidate| candidate == oid);
    let array = literal(&format!("{{{}}}", oids.join(",")));
    let constraints = run(
        transaction,
        to,
        &format!(
            "SELECT c.conrelid::text, c.confrelid::text, c.conrelid::regclass::text, \
             c.confrelid::regclass::text FROM pg_catalog.pg_constraint c \
             WHERE c.contype = 'f' AND (c.conrelid = ANY({array}::oid[]) \
             OR c.confrelid = ANY({array}::oid[]))"
        ),
    )
    .await?;

    let mut parents: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); parts.len()];
    for row in &constraints.rows {
        let field = |n: usize| row.get(n).cloned().flatten().unwrap_or_default();
        match (index_of(&field(0)), index_of(&field(1))) {
            (None, Some(_)) => {
                return Err(hinted(
                    "0A000",
                    format!(
                        "{} has a foreign key onto {}, which moving the key would break",
                        field(2),
                        field(3)
                    ),
                    "make the referencing table geo_partitioned on the same key, or drop the \
                     foreign key",
                ));
            }
            (Some(child), Some(parent)) if child != parent => {
                if let Some(set) = parents.get_mut(child) {
                    set.insert(parent);
                }
            }
            _ => {}
        }
    }

    // Kahn's algorithm, in configured order; a cycle keeps configured order
    // for what remains and leaves PostgreSQL to say whether it works.
    let mut placed = Vec::with_capacity(parts.len());
    let mut done = vec![false; parts.len()];
    loop {
        let next = (0..parts.len()).find(|&index| {
            !done.get(index).copied().unwrap_or(true)
                && parents.get(index).is_none_or(|set| {
                    set.iter()
                        .all(|parent| done.get(*parent).copied().unwrap_or(true))
                })
        });
        let Some(index) = next else { break };
        if let Some(flag) = done.get_mut(index) {
            *flag = true;
        }
        placed.push(index);
    }
    placed.extend((0..parts.len()).filter(|index| !done.get(*index).copied().unwrap_or(true)));
    Ok(placed)
}

/// The `INSERT` that recreates the rows in `to`, naming every column the
/// table stores, so that generated columns are computed afresh and identity
/// columns keep their values.
async fn insert_statement(
    transaction: &mut Transaction,
    part: &Part,
    json: &str,
    to: &str,
) -> Result<String, Refusal> {
    let catalog = format!(
        "SELECT a.attname::text, a.attidentity::text FROM pg_catalog.pg_attribute a \
         WHERE a.attrelid = {}::regclass AND a.attnum > 0 AND NOT a.attisdropped \
         AND a.attgenerated = '' ORDER BY a.attnum",
        literal(&part.qualified)
    );
    let columns = run(transaction, to, &catalog).await?;
    let names: Vec<String> = columns
        .rows
        .iter()
        .filter_map(|row| row.first().cloned().flatten())
        .map(|name| ident(&name))
        .collect();
    if names.is_empty() {
        return Err(fail(
            "XX000",
            format!("could not read the columns of \"{}\" in {to}", part.name),
        ));
    }
    let always_identity = columns
        .rows
        .iter()
        .any(|row| row.get(1).cloned().flatten().as_deref() == Some("a"));
    let list = names.join(", ");
    Ok(format!(
        "INSERT INTO {} ({list}) {}SELECT {list} FROM json_populate_recordset(NULL::{}, {})",
        part.qualified,
        if always_identity {
            "OVERRIDING SYSTEM VALUE "
        } else {
            ""
        },
        part.qualified,
        literal(json)
    ))
}

async fn run(
    transaction: &mut Transaction,
    region: &str,
    sql: &str,
) -> Result<QueryResult, Refusal> {
    transaction
        .execute(region, sql)
        .await
        .map_err(|error| txn_error(region, &error))
}

fn first_value(result: &QueryResult) -> Option<String> {
    result.rows.first()?.first()?.clone()
}

/// The row count in a command tag such as `INSERT 0 3` or `DELETE 3`.
fn affected(result: &QueryResult) -> u64 {
    result
        .tags
        .last()
        .and_then(|tag| tag.rsplit(' ').next())
        .and_then(|count| count.parse().ok())
        .unwrap_or(0)
}

/// A string literal that means the same whatever
/// `standard_conforming_strings` is set to.
pub(crate) fn literal(text: &str) -> String {
    let mut out = String::with_capacity(text.len().saturating_add(3));
    out.push_str("E'");
    for character in text.chars() {
        match character {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("''"),
            other => out.push(other),
        }
    }
    out.push('\'');
    out
}

pub(crate) fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub(crate) fn store_table(store: &DirectoryStore) -> String {
    match store.table.split_once('.') {
        Some((schema, table)) => format!("{}.{}", ident(schema), ident(table)),
        None => ident(&store.table),
    }
}

// ----------------------------------------------------- the directory table

/// Records keys written for the first time, each in the home shard it
/// hashes to, in the home region's copy of the directory: the copy a proxy
/// alone reads when it starts.
///
/// One statement per batch, `ON CONFLICT DO UPDATE` with no change, so
/// PostgreSQL returns the row whether this inserted it or a move or an
/// earlier run did, and a key already placed keeps its place.
///
/// # Errors
///
/// When the home region cannot be written.
pub async fn record(shared: &Shared, keys: &[(String, String)]) -> Result<(), Refusal> {
    let Some(store) = shared.moves.store() else {
        // Without a table to record them in, they are known for this run.
        for (table, key) in keys {
            let region = shared.router.topology().new_key_region(key).to_owned();
            shared.directory.place(table, key, &region);
        }
        return Ok(());
    };
    let topology = shared.router.topology();
    let home = topology.home_region.clone();
    let wanted: Vec<(String, String, String)> = keys
        .iter()
        .map(|(table, key)| {
            (
                table.clone(),
                key.clone(),
                topology.new_key_region(key).to_owned(),
            )
        })
        .collect();
    let values: Vec<String> = wanted
        .iter()
        .map(|(table, key, region)| {
            format!(
                "({}, {}, {})",
                literal(table),
                literal(key),
                literal(region)
            )
        })
        .collect();
    let table = store_table(store);
    let pool = shared
        .pool_for(&home, Access::ReadWrite, 0)
        .ok_or_else(|| fail("08000", format!("the home region {home} is not configured")))?;
    let result = pool
        .simple_query(&format!(
            "INSERT INTO {table} AS d (table_name, shard_key, region) VALUES {} \
             ON CONFLICT (table_name, shard_key) DO UPDATE SET region = d.region \
             RETURNING table_name, shard_key, region",
            values.join(", ")
        ))
        .await
        .map_err(|error| region_error(&home, &error))?;
    for (table, key, chosen) in &wanted {
        let placed = result
            .rows
            .iter()
            .find(|row| {
                row.first().cloned().flatten().as_deref() == Some(table.as_str())
                    && row.get(1).cloned().flatten().as_deref() == Some(key.as_str())
            })
            .and_then(|row| row.get(2).cloned().flatten())
            // PostgreSQL returns every row; a backend that returned none
            // still has the key where this proxy chose to put it.
            .unwrap_or_else(|| chosen.clone());
        if topology.region(&placed).is_none() {
            return Err(fail(
                "XX000",
                format!("the directory puts {table} {key} in unknown region {placed}"),
            ));
        }
        // A key a move has started on since stays held for the move.
        if !matches!(
            shared.directory.locate(table, key),
            Location::Known(KeyState::Relocating { .. })
        ) {
            shared.directory.place(table, key, &placed);
        }
    }
    Ok(())
}

/// The setting a move's transaction turns on, which the guards let through.
const MOVING: &str = "luxbyte.moving";

/// The SQLSTATE a guard refuses with: `serialization_failure`, which client
/// libraries retry, by when the proxy routes the key where it now is.
const GUARD_SQLSTATE: &str = "40001";

/// Installs, in every region, a trigger on each geo-partitioned table that
/// refuses a write for a key this region's directory places elsewhere.
///
/// A move changes the directory in every region in its own transaction, so
/// once it commits the region a key left says where it went, and a write
/// that still arrives there — delayed in the network past the move, or sent
/// by something that never heard of it — is refused by PostgreSQL itself
/// rather than stranded where no read will look. Keys the directory does not
/// list are not checked: nothing is known about them.
///
/// Best effort: a region that cannot be reached, or whose tables the backend
/// role does not own, is reported and left unguarded.
async fn guard(shared: &Shared, store: &DirectoryStore) {
    let directory = store_table(store);
    let topology = shared.router.topology();
    for region in topology.region_ids() {
        let Some(pool) = shared.pool_for(region, Access::ReadWrite, 0) else {
            continue;
        };
        for policy in &topology.tables {
            let Placement::GeoPartitioned { key } = &policy.placement else {
                continue;
            };
            let sql = guard_sql(&directory, &policy.schema, &policy.table, key, region);
            match pool.simple_query(&sql).await {
                Ok(_) => tracing::debug!(%region, table = %policy.table, "guarded"),
                Err(error) => tracing::warn!(
                    %region,
                    table = %policy.table,
                    %error,
                    "could not install the move guard; writes there are not checked"
                ),
            }
        }
    }
}

/// The function and trigger that guard one table in one region, replaced
/// in one transaction so that the table is never left half-guarded.
fn guard_sql(directory: &str, schema: &str, table: &str, key: &str, region: &str) -> String {
    let function = format!(
        "{}.{}",
        ident(schema),
        ident(&format!("luxbyte_guard_{table}"))
    );
    let qualified = format!("{}.{}", ident(schema), ident(table));
    let column = ident(key);
    let check = |row: &str| {
        format!(
            "SELECT region INTO placed FROM {directory} WHERE table_name = {name} \
             AND shard_key = {row}.{column}::text; \
             IF placed IS NOT NULL AND placed <> {here} THEN \
             RAISE EXCEPTION USING ERRCODE = '{GUARD_SQLSTATE}', \
             MESSAGE = format('key %s of %s lives in %s, not in %s', \
             {row}.{column}, {name}, placed, {here}), \
             HINT = 'the key has moved; retry, and the proxy sends it where it is'; \
             END IF;",
            name = literal(table),
            here = literal(region),
        )
    };
    format!(
        "BEGIN; SET LOCAL lock_timeout = '5s'; \
         CREATE OR REPLACE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $luxbyte$ \
         DECLARE placed text; BEGIN \
         IF coalesce(current_setting('{MOVING}', true), '') = 'on' THEN \
         RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END; END IF; \
         IF TG_OP = 'DELETE' OR (TG_OP = 'UPDATE' AND OLD.{column} IS DISTINCT FROM NEW.{column}) \
         THEN {old} END IF; \
         IF TG_OP IN ('INSERT', 'UPDATE') THEN {new} END IF; \
         RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END; \
         END $luxbyte$; \
         CREATE OR REPLACE TRIGGER luxbyte_guard BEFORE INSERT OR UPDATE OR DELETE ON {qualified} \
         FOR EACH ROW EXECUTE FUNCTION {function}(); \
         COMMIT",
        old = check("OLD"),
        new = check("NEW"),
    )
}

/// Creates the directory table where it is missing and loads it from the
/// home region. Returns how many keys it holds.
///
/// Run before serving, after any transaction a previous run left in doubt
/// has been resolved, so that what is loaded is what was decided.
pub async fn load(shared: &Shared) -> Result<usize, String> {
    let Some(store) = shared.moves.store() else {
        return Ok(0);
    };
    let table = store_table(store);
    let create = format!(
        "CREATE TABLE IF NOT EXISTS {table} (\
         table_name text NOT NULL, \
         shard_key text NOT NULL, \
         region text NOT NULL, \
         moved_at timestamptz NOT NULL DEFAULT now(), \
         PRIMARY KEY (table_name, shard_key))"
    );
    let topology = shared.router.topology();
    let home = topology.home_region.clone();
    for region in topology.region_ids() {
        let Some(pool) = shared.pool_for(region, Access::ReadWrite, 0) else {
            continue;
        };
        if let Err(error) = pool.simple_query(&create).await {
            // The home region's copy is the one read here; another region's
            // is only written by a move, which then fails and says why.
            if region == home {
                return Err(format!("could not create {table} in {region}: {error}"));
            }
            tracing::warn!(%region, %error, "could not create the directory table");
        }
    }
    if store.guard {
        guard(shared, store).await;
    }
    // Shared, the authority region's copy is the one read, when the proxy
    // joins the others.
    if shared.peers.is_some() {
        return Ok(0);
    }

    let pool = shared
        .pool_for(&home, Access::ReadWrite, 0)
        .ok_or_else(|| format!("the home region {home} is not configured"))?;
    let rows = pool
        .simple_query(&format!(
            "SELECT table_name, shard_key, region FROM {table}"
        ))
        .await
        .map_err(|error| format!("could not read {table} in {home}: {error}"))?;
    let mut loaded = 0_usize;
    for row in &rows.rows {
        let (Some(Some(name)), Some(Some(key)), Some(Some(region))) =
            (row.first(), row.get(1), row.get(2))
        else {
            continue;
        };
        if topology.region(region).is_none() {
            return Err(format!(
                "{table} puts {name} {key} in region \"{region}\", which the topology does not \
                 have"
            ));
        }
        shared.directory.place(name, key, region);
        loaded = loaded.saturating_add(1);
    }
    Ok(loaded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_command_is_read_with_quoted_keys() {
        assert_eq!(
            parse("RELOCATE alice TO us-east;"),
            Some(Ok(("alice".to_owned(), "us-east".to_owned())))
        );
        assert_eq!(
            parse("relocate 'o''brien & co' to 'us-east'"),
            Some(Ok(("o'brien & co".to_owned(), "us-east".to_owned())))
        );
        assert_eq!(parse("SELECT 1"), None);
        assert!(matches!(parse("RELOCATE alice"), Some(Err(_))));
        assert!(matches!(
            parse("RELOCATE 'unterminated TO us"),
            Some(Err(_))
        ));
        assert!(matches!(parse("RELOCATE a b TO c"), Some(Err(_))));
    }

    #[test]
    fn rebalance_is_read_with_an_optional_limit() {
        assert_eq!(
            parse_rebalance("REBALANCE eu-west-1 TO eu-west-2;"),
            Some(Ok(Rebalance {
                from: "eu-west-1".to_owned(),
                to: "eu-west-2".to_owned(),
                limit: REBALANCE_DEFAULT,
            }))
        );
        assert_eq!(
            parse_rebalance("rebalance 'eu 1' to eu-2 limit 7"),
            Some(Ok(Rebalance {
                from: "eu 1".to_owned(),
                to: "eu-2".to_owned(),
                limit: 7,
            }))
        );
        assert_eq!(parse_rebalance("RELOCATE alice TO us"), None);
        for wrong in [
            "REBALANCE a",
            "REBALANCE a TO b LIMIT 0",
            "REBALANCE a TO b LIMIT many",
            "REBALANCE a b c",
        ] {
            assert!(matches!(parse_rebalance(wrong), Some(Err(_))), "{wrong}");
        }
    }

    #[test]
    fn literals_mean_the_same_whatever_the_server_settings() {
        assert_eq!(literal("it's"), "E'it''s'");
        assert_eq!(literal(r"back\slash"), r"E'back\\slash'");
    }

    #[test]
    fn identifiers_are_quoted_and_a_schema_is_kept_apart() {
        assert_eq!(ident("we\"ird"), "\"we\"\"ird\"");
        let mut store = DirectoryStore::default();
        assert_eq!(store_table(&store), "\"luxbyte_directory\"");
        store.table = "ops.keys".to_owned();
        assert_eq!(store_table(&store), "\"ops\".\"keys\"");
    }

    #[tokio::test]
    async fn a_move_waits_for_writes_planned_before_it_and_no_others() {
        let gate = Arc::new(Gate::default());
        let before = gate.enter();
        let generation = gate.advance();
        let _after = gate.enter();
        assert!(!gate.drain(generation, Duration::from_millis(20)).await);
        drop(before);
        assert!(gate.drain(generation, Duration::from_millis(20)).await);
    }
}
