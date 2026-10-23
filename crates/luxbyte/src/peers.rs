//! Several proxies sharing one directory.
//!
//! One proxy keeps its directory in memory, loaded from a table and changed
//! only by its own moves. Several proxies can read the same table, but each
//! would then route on its own copy, and a move made by one would leave the
//! others sending the key's writes to the region its rows just left.
//!
//! So proxies sharing a directory meet in one region, the *authority*, and
//! agree there:
//!
//! - **Leases.** Each proxy records a heartbeat there every `poll_ms`, and
//!   routes on its directory only while its last visit is under `lease_ms`
//!   old. Past that it refuses whatever depends on the directory, because it
//!   can no longer hear about moves; when it gets through again it reads the
//!   whole directory before routing on it.
//! - **Moves.** A move is announced there before it starts. Every live
//!   proxy holds the key's writes, waits for its own writes already under
//!   way, and says so; only then are rows copied. Before the commit every
//!   proxy holds the key's reads too, and says so again. A proxy that does
//!   not answer stops the move, unless its last heartbeat is more than half
//!   again `lease_ms` old, by when it has stopped routing by itself. Once the
//!   announcement is withdrawn each proxy reads where the key now is.
//! - **New keys.** A write to a key nobody has placed claims it there first,
//!   so two proxies in different regions cannot each create it at home.
//!
//! What it costs: one small query to the authority every `poll_ms` from
//! each proxy, a round trip there on the first write of each new key, and
//! while a key moves, holds on it up to two polls longer than one proxy
//! alone would hold it.
//!
//! What it does not cover: a write that a proxy sent before its lease ran
//! out, and that reaches its region more than `lease_ms / 2` later, can land
//! after a move the proxy never heard of. Leases trade that window for not
//! needing a consensus protocol between proxies.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use luxbyte_core::Refusal;
use luxbyte_core::directory::{Directory, KeyState, Location};
use luxbyte_core::topology::Placement;
use luxbyte_pool::{Pool, QueryResult};
use luxbyte_sql::Access;

use crate::config::DirectoryStore;
use crate::relocate::{ident, literal, store_table};
use crate::session::Shared;

/// How far a move has got, as every proxy is told.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Phase {
    /// Writes to the key wait.
    Cutover,
    /// Reads of the key wait too: the regions are about to commit.
    Committing,
}

impl Phase {
    /// The name stored for it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cutover => "cutover",
            Self::Committing => "committing",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        match text {
            "cutover" => Some(Self::Cutover),
            "committing" => Some(Self::Committing),
            _ => None,
        }
    }
}

/// A move announced in the authority region.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Announced {
    /// Its number, unique among moves.
    pub id: i64,
    /// The key being moved.
    pub key: String,
    /// Where it is going.
    pub to: String,
    /// The proxy moving it.
    pub mover: String,
    /// How far it has got.
    pub phase: Phase,
}

/// Another proxy, as of the last visit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    /// Its `server.node`.
    pub node: String,
    /// Milliseconds since its last heartbeat.
    pub heartbeat_age_ms: u64,
}

/// What one visit to the authority region learned.
#[derive(Debug, Default)]
struct Visit {
    /// The heartbeat was this process's: no other process has taken the
    /// proxy's name.
    current: bool,
    moves: Vec<Announced>,
    peers: Vec<Peer>,
}

/// A move another proxy is making, as this one follows it.
#[derive(Debug)]
struct Following {
    key: String,
    to: String,
    /// Writes planned before this generation were under way when the key
    /// was held; they finish before the hold is confirmed.
    generation: u64,
    /// The furthest phase confirmed.
    confirmed: Option<Phase>,
    since: Instant,
}

#[derive(Debug, Default)]
struct State {
    /// Until when this proxy may route on its directory.
    lease_until: Option<Instant>,
    /// Another process started under this proxy's name and took over its
    /// heartbeat. This one never routes on the directory again.
    replaced: bool,
    following: BTreeMap<i64, Following>,
    /// Moves this process announced, which it does not follow.
    mine: BTreeSet<i64>,
    peers: Vec<Peer>,
    last_error: Option<String>,
}

/// Where the shared state lives.
#[derive(Debug)]
enum Store {
    /// Tables in the authority region.
    Postgres(Tables),
    /// Memory shared by proxies in one process.
    Memory(Arc<MemoryPeers>),
}

/// The tables in the authority region, qualified and quoted.
#[derive(Debug)]
struct Tables {
    directory: String,
    proxies: String,
    moves: String,
    acks: String,
}

/// This proxy's part in a shared directory.
#[derive(Debug)]
pub struct Peers {
    node: String,
    /// Tells this process apart from an earlier or later one with the same
    /// name.
    instance: String,
    authority: String,
    poll: Duration,
    lease: Duration,
    store: Store,
    state: parking_lot::Mutex<State>,
}

impl Peers {
    /// Settings for a shared directory, checked against the regions.
    ///
    /// # Errors
    ///
    /// When the authority is not a region, or the timings cannot work.
    pub fn new(
        config: &DirectoryStore,
        node: &str,
        home: &str,
        regions: &[String],
        memory: Option<Arc<MemoryPeers>>,
    ) -> Result<Self, String> {
        let authority = config.authority.clone().unwrap_or_else(|| home.to_owned());
        if !regions.contains(&authority) {
            return Err(format!(
                "directory_store.authority is \"{authority}\", which is not a region"
            ));
        }
        if config.poll_ms == 0 || config.lease_ms < config.poll_ms.saturating_mul(2) {
            return Err(format!(
                "directory_store.lease_ms ({}) must be at least twice poll_ms ({}), and poll_ms \
                 more than zero, or one slow visit would stop the proxy routing",
                config.lease_ms, config.poll_ms
            ));
        }
        let mut instance = [0_u8; 8];
        {
            use rand::Rng as _;
            rand::rng().fill_bytes(&mut instance);
        }
        let store = match memory {
            Some(memory) => Store::Memory(memory),
            None => Store::Postgres(Tables {
                directory: store_table(config),
                proxies: derived(config, "proxies"),
                moves: derived(config, "moves"),
                acks: derived(config, "acks"),
            }),
        };
        Ok(Self {
            node: node.to_owned(),
            instance: instance.iter().fold(String::new(), |mut out, byte| {
                use std::fmt::Write as _;
                let _ = write!(out, "{byte:02x}");
                out
            }),
            authority,
            poll: Duration::from_millis(config.poll_ms),
            lease: Duration::from_millis(config.lease_ms),
            store,
            state: parking_lot::Mutex::new(State::default()),
        })
    }

    /// The region where the proxies meet.
    #[must_use]
    pub fn authority(&self) -> &str {
        &self.authority
    }

    /// This proxy's name.
    #[must_use]
    pub fn node(&self) -> &str {
        &self.node
    }

    /// How long another proxy counts as alive after its last heartbeat.
    /// Half as long again as the lease, so that by then it has stopped
    /// routing by itself even if its clock runs a little slow.
    fn alive_for(&self) -> Duration {
        self.lease
            .saturating_add(self.lease.checked_div(2).unwrap_or_default())
    }

    /// Whether this proxy must not route on its directory now.
    #[must_use]
    pub fn fenced(&self) -> bool {
        let state = self.state.lock();
        state.replaced || state.lease_until.is_none_or(|until| Instant::now() > until)
    }

    /// What a statement that depends on the directory is told while the
    /// proxy is fenced.
    #[must_use]
    pub fn fence_refusal(&self) -> Refusal {
        let state = self.state.lock();
        if state.replaced {
            return Refusal::new(
                "57P03",
                format!(
                    "another proxy started as \"{}\" and took over this one's place in the \
                     shared directory",
                    self.node
                ),
            )
            .with_hint("give every proxy its own server.node, and stop this one");
        }
        let why = state
            .last_error
            .clone()
            .unwrap_or_else(|| "it has not heard from it recently".to_owned());
        Refusal::new(
            "57P03",
            format!(
                "this proxy cannot reach the shared directory in {}, so it cannot know where keys \
                 are: {why}",
                self.authority
            ),
        )
        .with_hint(
            "statements on geo-partitioned tables resume once it can; other tables are not \
             affected",
        )
    }

    /// The other proxies, as of the last visit.
    #[must_use]
    pub fn peers(&self) -> Vec<Peer> {
        self.state.lock().peers.clone()
    }

    /// Moves made by other proxies that this one is holding keys for, with
    /// how long it has held each.
    #[must_use]
    pub fn following(&self) -> Vec<(String, String, Option<Phase>, Duration)> {
        self.state
            .lock()
            .following
            .values()
            .map(|move_| {
                (
                    move_.key.clone(),
                    move_.to.clone(),
                    move_.confirmed,
                    move_.since.elapsed(),
                )
            })
            .collect()
    }

    /// Why the last visit failed, if it did.
    #[must_use]
    pub fn last_error(&self) -> Option<String> {
        self.state.lock().last_error.clone()
    }

    // ---------------------------------------------------------- starting

    /// Joins the shared directory: creates its tables, records this proxy,
    /// withdraws moves an earlier run of it left announced, reads the
    /// directory and follows the moves under way.
    ///
    /// `recovered` says the transactions an earlier run left in doubt are
    /// all resolved. Only then is the directory known to say where its
    /// moves left their keys, and their announcements safe to withdraw.
    ///
    /// # Errors
    ///
    /// When the authority cannot be reached, or another live process uses
    /// this proxy's name.
    pub async fn start(&self, shared: &Shared, recovered: bool) -> Result<(), String> {
        if let Store::Postgres(tables) = &self.store {
            let pool = self.pool(shared)?;
            create(&pool, tables).await?;
        }
        self.register(shared).await?;
        if recovered {
            self.withdraw_stale(shared).await?;
        } else {
            tracing::warn!(
                "transactions are still in doubt, so moves an earlier run of this proxy \
                 announced stay announced and their keys stay held"
            );
        }
        let started = Instant::now();
        let visit = self.visit(shared).await?;
        if !visit.current {
            return Err(format!(
                "another proxy took the name \"{}\" while this one started",
                self.node
            ));
        }
        self.reload(shared).await?;
        self.follow(shared, &visit.moves).await;
        self.renewed(started, visit.peers);
        Ok(())
    }

    /// Keeps visiting the authority region until the task is stopped.
    pub async fn run(shared: Arc<Shared>) {
        let Some(peers) = shared.peers.as_ref() else {
            return;
        };
        loop {
            let started = Instant::now();
            peers.round(&shared).await;
            tokio::time::sleep(peers.poll.saturating_sub(started.elapsed())).await;
        }
    }

    /// One visit: renews the lease, follows the moves under way, and lets
    /// go of keys whose moves are over.
    pub async fn round(&self, shared: &Shared) {
        let started = Instant::now();
        let visit = match self.visit(shared).await {
            Ok(visit) => visit,
            Err(error) => {
                tracing::warn!(%error, "could not reach the shared directory");
                self.state.lock().last_error = Some(error);
                return;
            }
        };
        if !visit.current {
            let mut state = self.state.lock();
            if !state.replaced {
                tracing::error!(
                    node = %self.node,
                    "another proxy took this one's name; it stops routing on the directory"
                );
            }
            state.replaced = true;
            return;
        }
        // Moves may have started and finished unseen while the lease was
        // out, so the directory is read whole before it is trusted again.
        let lapsed = self
            .state
            .lock()
            .lease_until
            .is_none_or(|until| started > until);
        if lapsed {
            if let Err(error) = self.reload(shared).await {
                tracing::warn!(%error, "could not read the shared directory");
                self.state.lock().last_error = Some(error);
                return;
            }
            tracing::info!("reached the shared directory again and read it whole");
        }
        self.follow(shared, &visit.moves).await;
        self.renewed(started, visit.peers);
        shared.moves.announce();
    }

    fn renewed(&self, started: Instant, peers: Vec<Peer>) {
        let mut state = self.state.lock();
        state.lease_until = started.checked_add(self.lease);
        state.peers = peers;
        state.last_error = None;
    }

    // -------------------------------------------------------- following

    /// Holds the keys of moves other proxies announced, confirms each hold
    /// once this proxy's own writes to them are done, and lets go of keys
    /// whose moves are over.
    async fn follow(&self, shared: &Shared, moves: &[Announced]) {
        let tables = geo_tables(shared);
        let mine = self.state.lock().mine.clone();
        for announced in moves.iter().filter(|move_| !mine.contains(&move_.id)) {
            let known = self
                .state
                .lock()
                .following
                .get(&announced.id)
                .map(|following| (following.generation, following.confirmed));
            let (generation, confirmed) = if let Some(known) = known {
                known
            } else {
                for table in &tables {
                    // A key already held, by a move this proxy announced
                    // in an earlier run, stays held.
                    let _ = shared
                        .directory
                        .begin_relocation(table, &announced.key, &announced.to);
                }
                shared.moves.announce();
                let generation = shared.moves.advance();
                self.state.lock().following.insert(
                    announced.id,
                    Following {
                        key: announced.key.clone(),
                        to: announced.to.clone(),
                        generation,
                        confirmed: None,
                        since: Instant::now(),
                    },
                );
                (generation, None)
            };

            if confirmed.is_none() {
                // Writes already under way may be bound for the key's old
                // region; the mover copies its rows only after they finish.
                if !shared.moves.drained(generation, self.poll).await {
                    continue;
                }
                if self.confirm(shared, announced.id, Phase::Cutover).await {
                    self.confirmed(announced.id, Phase::Cutover);
                } else {
                    continue;
                }
            }
            if announced.phase == Phase::Committing && confirmed != Some(Phase::Committing) {
                for table in &tables {
                    let _ = shared.directory.begin_committing(table, &announced.key);
                }
                shared.moves.announce();
                if self.confirm(shared, announced.id, Phase::Committing).await {
                    self.confirmed(announced.id, Phase::Committing);
                }
            }
        }

        let over: Vec<(i64, String)> = self
            .state
            .lock()
            .following
            .iter()
            .filter(|(id, _)| !moves.iter().any(|move_| move_.id == **id))
            .map(|(id, following)| (*id, following.key.clone()))
            .collect();
        for (id, key) in over {
            match self.regions_of(shared, &key).await {
                Ok(regions) => {
                    for table in &tables {
                        match regions.iter().find(|(name, _)| name == table) {
                            Some((_, region)) => shared.directory.place(table, &key, region),
                            None => shared.directory.forget(table, &key),
                        }
                    }
                    self.state.lock().following.remove(&id);
                    shared.moves.announce();
                    tracing::info!(%key, "a move by another proxy is over");
                }
                // Held until this proxy can find out where the key went.
                Err(error) => self.state.lock().last_error = Some(error),
            }
        }
    }

    fn confirmed(&self, id: i64, phase: Phase) {
        if let Some(following) = self.state.lock().following.get_mut(&id) {
            following.confirmed = Some(phase);
        }
    }

    /// Tells the mover this proxy holds the key as far as `phase`.
    async fn confirm(&self, shared: &Shared, id: i64, phase: Phase) -> bool {
        let outcome = match &self.store {
            Store::Postgres(tables) => match self.pool(shared) {
                Ok(pool) => pool
                    .simple_query(&format!(
                        "INSERT INTO {} (move_id, node, phase) VALUES ({id}, {}, {}) \
                         ON CONFLICT DO NOTHING",
                        tables.acks,
                        literal(&self.node),
                        literal(phase.as_str())
                    ))
                    .await
                    .map(drop)
                    .map_err(|error| error.to_string()),
                Err(error) => Err(error),
            },
            Store::Memory(memory) => memory.confirm(&self.node, id, phase),
        };
        match outcome {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(%error, id, "could not confirm a hold");
                self.state.lock().last_error = Some(error);
                false
            }
        }
    }

    // ---------------------------------------------------------- claiming

    /// Claims keys no proxy has placed, in this proxy's home region unless
    /// another got there first, and records where each went.
    ///
    /// # Errors
    ///
    /// When the proxy is fenced or the authority cannot be reached.
    pub async fn claim(&self, shared: &Shared, keys: &[(String, String)]) -> Result<(), Refusal> {
        if self.fenced() {
            return Err(self.fence_refusal());
        }
        let topology = shared.router.topology();
        // Each in this proxy's home region, or the home shard it hashes to.
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
        let placed = match &self.store {
            Store::Postgres(tables) => {
                let pool = self.pool(shared).map_err(|error| unreachable(&error))?;
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
                let pairs: Vec<String> = keys
                    .iter()
                    .map(|(table, key)| format!("({}, {})", literal(table), literal(key)))
                    .collect();
                // Two statements, so that the second sees a key another
                // proxy claimed while the first waited for it.
                let result = pool
                    .simple_query(&format!(
                        "INSERT INTO {directory} (table_name, shard_key, region) VALUES {values} \
                         ON CONFLICT (table_name, shard_key) DO NOTHING; \
                         SELECT table_name, shard_key, region FROM {directory} \
                         WHERE (table_name, shard_key) IN ({pairs})",
                        directory = tables.directory,
                        values = values.join(", "),
                        pairs = pairs.join(", ")
                    ))
                    .await
                    .map_err(|error| unreachable(&error.to_string()))?;
                triples(&result)
            }
            Store::Memory(memory) => memory
                .claim(&self.node, &wanted)
                .map_err(|error| unreachable(&error))?,
        };
        for (table, key) in keys {
            let Some((_, _, region)) = placed
                .iter()
                .find(|(name, claimed, _)| name == table && claimed == key)
            else {
                return Err(Refusal::new(
                    "XX000",
                    format!("the shared directory did not place {table} {key}"),
                ));
            };
            if topology.region(region).is_none() {
                return Err(Refusal::new(
                    "XX000",
                    format!("the shared directory puts {table} {key} in unknown region {region}"),
                ));
            }
            // A key a move has started on since stays held for the move.
            if matches!(
                shared.directory.locate(table, key),
                Location::Known(KeyState::Relocating { .. })
            ) {
                continue;
            }
            shared.directory.place(table, key, region);
        }
        Ok(())
    }

    // ------------------------------------------------------------ moving

    /// Announces a move of `key` to `to`, for every other proxy to hold it.
    ///
    /// # Errors
    ///
    /// When this proxy is fenced, the key is already moving, or the
    /// authority cannot be reached.
    pub async fn announce(
        &self,
        shared: &Arc<Shared>,
        key: &str,
        to: &str,
    ) -> Result<Announcement, Refusal> {
        if self.fenced() {
            return Err(self.fence_refusal());
        }
        let outcome = match &self.store {
            Store::Postgres(tables) => {
                let pool = self.pool(shared).map_err(|error| unreachable(&error))?;
                let result = pool
                    .simple_query(&format!(
                        "WITH added AS (INSERT INTO {moves} (shard_key, to_region, mover, phase) \
                         VALUES ({key}, {to}, {mover}, 'cutover') ON CONFLICT (shard_key) DO \
                         NOTHING RETURNING id) \
                         SELECT 'added', id::text FROM added \
                         UNION ALL SELECT 'existing', mover FROM {moves} WHERE shard_key = {key}",
                        moves = tables.moves,
                        key = literal(key),
                        to = literal(to),
                        mover = literal(&self.node)
                    ))
                    .await
                    .map_err(|error| unreachable(&error.to_string()))?;
                let row = |kind: &str| {
                    result
                        .rows
                        .iter()
                        .find(|row| row.first().cloned().flatten().as_deref() == Some(kind))
                        .and_then(|row| row.get(1).cloned().flatten())
                };
                match (row("added"), row("existing")) {
                    (Some(id), _) => id
                        .parse::<i64>()
                        .map(Ok)
                        .map_err(|_| Refusal::new("XX000", "a move was announced without an id")),
                    (None, Some(mover)) => Ok(Err(mover)),
                    (None, None) => Ok(Err("another proxy, at the same moment".to_owned())),
                }?
            }
            Store::Memory(memory) => memory
                .announce(&self.node, key, to)
                .map_err(|error| unreachable(&error))?,
        };
        match outcome {
            Ok(id) => {
                self.state.lock().mine.insert(id);
                Ok(Announcement {
                    shared: Arc::clone(shared),
                    id,
                    key: key.to_owned(),
                    withdrawn: false,
                })
            }
            Err(mover) => Err(Refusal::new(
                "55006",
                format!("{key} is already being moved, by {mover}"),
            )
            .with_hint("wait for that move to finish, then RELOCATE again")),
        }
    }

    /// The proxies alive that have not confirmed holding the key as far as
    /// `phase`.
    async fn unconfirmed(
        &self,
        shared: &Shared,
        id: i64,
        phase: Phase,
    ) -> Result<Vec<String>, String> {
        let window = self.alive_for().as_millis();
        match &self.store {
            Store::Postgres(tables) => {
                let result = self
                    .pool(shared)?
                    .simple_query(&format!(
                        "SELECT p.node FROM {proxies} p WHERE p.node <> {me} \
                         AND p.heartbeat_at > clock_timestamp() - interval '{window} milliseconds' \
                         AND NOT EXISTS (SELECT 1 FROM {acks} a WHERE a.move_id = {id} \
                         AND a.node = p.node AND a.phase = {phase}) ORDER BY p.node",
                        proxies = tables.proxies,
                        acks = tables.acks,
                        me = literal(&self.node),
                        phase = literal(phase.as_str())
                    ))
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(result
                    .rows
                    .iter()
                    .filter_map(|row| row.first().cloned().flatten())
                    .collect())
            }
            Store::Memory(memory) => memory.unconfirmed(&self.node, id, phase, self.alive_for()),
        }
    }

    async fn set_phase(&self, shared: &Shared, id: i64, phase: Phase) -> Result<(), String> {
        match &self.store {
            Store::Postgres(tables) => self
                .pool(shared)?
                .simple_query(&format!(
                    "UPDATE {} SET phase = {} WHERE id = {id}",
                    tables.moves,
                    literal(phase.as_str())
                ))
                .await
                .map(drop)
                .map_err(|error| error.to_string()),
            Store::Memory(memory) => memory.set_phase(&self.node, id, phase),
        }
    }

    async fn withdraw(&self, shared: &Shared, id: i64) -> Result<(), String> {
        let outcome = match &self.store {
            Store::Postgres(tables) => self
                .pool(shared)?
                .simple_query(&format!(
                    "WITH confirmed AS (DELETE FROM {acks} WHERE move_id = {id}) \
                     DELETE FROM {moves} WHERE id = {id}",
                    acks = tables.acks,
                    moves = tables.moves
                ))
                .await
                .map(drop)
                .map_err(|error| error.to_string()),
            Store::Memory(memory) => memory.withdraw(&self.node, id),
        };
        if outcome.is_ok() {
            self.state.lock().mine.remove(&id);
        }
        outcome
    }

    /// Where each table's rows for `key` are, as the shared directory says.
    ///
    /// # Errors
    ///
    /// When the authority cannot be reached.
    pub async fn regions_of(
        &self,
        shared: &Shared,
        key: &str,
    ) -> Result<Vec<(String, String)>, String> {
        match &self.store {
            Store::Postgres(tables) => {
                let result = self
                    .pool(shared)?
                    .simple_query(&format!(
                        "SELECT table_name, shard_key, region FROM {} WHERE shard_key = {}",
                        tables.directory,
                        literal(key)
                    ))
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(triples(&result)
                    .into_iter()
                    .map(|(table, _, region)| (table, region))
                    .collect())
            }
            Store::Memory(memory) => memory.regions_of(&self.node, key),
        }
    }

    // ----------------------------------------------------------- plumbing

    fn pool(&self, shared: &Shared) -> Result<Pool, String> {
        shared
            .pool_for(&self.authority, Access::ReadWrite, 0)
            .ok_or_else(|| format!("the authority region {} is not configured", self.authority))
    }

    async fn register(&self, shared: &Shared) -> Result<(), String> {
        let window = self.alive_for();
        let taken = match &self.store {
            Store::Postgres(tables) => {
                let result = self
                    .pool(shared)?
                    .simple_query(&format!(
                        "INSERT INTO {proxies} AS p (node, instance, heartbeat_at) \
                         VALUES ({node}, {instance}, clock_timestamp()) \
                         ON CONFLICT (node) DO UPDATE SET instance = EXCLUDED.instance, \
                         heartbeat_at = EXCLUDED.heartbeat_at \
                         WHERE p.heartbeat_at < clock_timestamp() - interval '{ms} milliseconds' \
                         RETURNING 1",
                        proxies = tables.proxies,
                        node = literal(&self.node),
                        instance = literal(&self.instance),
                        ms = window.as_millis()
                    ))
                    .await
                    .map_err(|error| error.to_string())?;
                result.rows.is_empty()
            }
            Store::Memory(memory) => !memory.register(&self.node, &self.instance, window)?,
        };
        if taken {
            return Err(format!(
                "another proxy named \"{}\" has been seen in the last {} ms; every proxy sharing \
                 a directory needs its own server.node",
                self.node,
                window.as_millis()
            ));
        }
        Ok(())
    }

    async fn withdraw_stale(&self, shared: &Shared) -> Result<(), String> {
        match &self.store {
            Store::Postgres(tables) => self
                .pool(shared)?
                .simple_query(&format!(
                    "WITH mine AS (DELETE FROM {moves} WHERE mover = {node} RETURNING id) \
                     DELETE FROM {acks} WHERE move_id IN (SELECT id FROM mine)",
                    moves = tables.moves,
                    acks = tables.acks,
                    node = literal(&self.node)
                ))
                .await
                .map(drop)
                .map_err(|error| error.to_string()),
            Store::Memory(memory) => memory.withdraw_all(&self.node),
        }
    }

    async fn visit(&self, shared: &Shared) -> Result<Visit, String> {
        match &self.store {
            Store::Postgres(tables) => {
                let result = self
                    .pool(shared)?
                    .simple_query(&format!(
                        "WITH beat AS (UPDATE {proxies} SET heartbeat_at = clock_timestamp() \
                         WHERE node = {node} AND instance = {instance} RETURNING 1) \
                         SELECT 'beat'::text, (SELECT count(*) FROM beat)::text, NULL::text, \
                         NULL::text, NULL::text, NULL::text \
                         UNION ALL SELECT 'move', id::text, shard_key, to_region, mover, phase \
                         FROM {moves} \
                         UNION ALL SELECT 'peer', node, \
                         (extract(epoch FROM clock_timestamp() - heartbeat_at) * 1000)::bigint::text, \
                         NULL, NULL, NULL FROM {proxies}",
                        proxies = tables.proxies,
                        moves = tables.moves,
                        node = literal(&self.node),
                        instance = literal(&self.instance)
                    ))
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(read_visit(&result))
            }
            Store::Memory(memory) => memory.visit(&self.node, &self.instance),
        }
    }

    /// Reads the whole directory from the authority and makes the working
    /// copy match it, leaving keys that are moving alone.
    async fn reload(&self, shared: &Shared) -> Result<(), String> {
        let rows = match &self.store {
            Store::Postgres(tables) => {
                let result = self
                    .pool(shared)?
                    .simple_query(&format!(
                        "SELECT table_name, shard_key, region FROM {}",
                        tables.directory
                    ))
                    .await
                    .map_err(|error| error.to_string())?;
                triples(&result)
            }
            Store::Memory(memory) => memory.everything(&self.node)?,
        };
        let topology = shared.router.topology();
        let mut wanted: BTreeMap<(String, String), String> = BTreeMap::new();
        for (table, key, region) in rows {
            if topology.region(&region).is_none() {
                return Err(format!(
                    "the shared directory puts {table} {key} in region \"{region}\", which the \
                     topology does not have"
                ));
            }
            wanted.insert((table, key), region);
        }
        for (table, key, state) in shared.directory.snapshot() {
            if let KeyState::Resident { region } = state {
                match wanted.get(&(table.clone(), key.clone())) {
                    Some(now) if *now == region => {}
                    Some(now) => shared.directory.place(&table, &key, now),
                    None => shared.directory.forget(&table, &key),
                }
            }
        }
        for ((table, key), region) in &wanted {
            if matches!(shared.directory.locate(table, key), Location::Unseen) {
                shared.directory.place(table, key, region);
            }
        }
        Ok(())
    }
}

/// A move this proxy announced. Withdrawn when dropped, so that a move that
/// fails anywhere lets the other proxies go.
#[derive(Debug)]
pub struct Announcement {
    shared: Arc<Shared>,
    id: i64,
    key: String,
    withdrawn: bool,
}

impl Announcement {
    fn peers(&self) -> Option<&Peers> {
        self.shared.peers.as_ref()
    }

    /// Waits until every live proxy holds the key as far as `phase`.
    ///
    /// # Errors
    ///
    /// When one has not within `limit`, naming those that have not.
    pub async fn confirmed(&self, phase: Phase, limit: Duration) -> Result<(), Refusal> {
        let Some(peers) = self.peers() else {
            return Ok(());
        };
        let started = Instant::now();
        let missing = loop {
            let missing = match peers.unconfirmed(&self.shared, self.id, phase).await {
                Ok(waiting) if waiting.is_empty() => return Ok(()),
                Ok(waiting) => waiting,
                Err(error) => vec![format!("(the authority region: {error})")],
            };
            if started.elapsed() >= limit {
                break missing;
            }
            let pause = peers.poll.checked_div(2).unwrap_or_default();
            tokio::time::sleep(pause.max(Duration::from_millis(5))).await;
        };
        Err(Refusal::new(
            "55P03",
            format!(
                "{} did not confirm {} {} within {} ms; nothing was moved",
                missing.join(", "),
                if missing.len() == 1 {
                    "it holds"
                } else {
                    "they hold"
                },
                self.key,
                limit.as_millis()
            ),
        )
        .with_hint(
            "see SHOW PEERS; a proxy that cannot reach the authority region stops routing once \
             its lease runs out, and a move stops waiting for it soon after",
        ))
    }

    /// Tells every proxy the move has reached `phase`.
    ///
    /// # Errors
    ///
    /// When the authority cannot be reached.
    pub async fn advance(&self, phase: Phase) -> Result<(), Refusal> {
        let Some(peers) = self.peers() else {
            return Ok(());
        };
        peers
            .set_phase(&self.shared, self.id, phase)
            .await
            .map_err(|error| unreachable(&error))
    }

    /// The move committed: records where the key went, where the store does
    /// not already have it from the move's own transaction, and withdraws
    /// the announcement.
    pub fn moved(mut self, tables: &[String], to: &str) {
        if let Some(Store::Memory(memory)) = self.peers().map(|peers| &peers.store) {
            for table in tables {
                memory.place(table, &self.key, to);
            }
        }
        self.withdrawn = true;
        withdraw_later(Arc::clone(&self.shared), self.id);
    }
}

impl Drop for Announcement {
    fn drop(&mut self) {
        if !self.withdrawn {
            withdraw_later(Arc::clone(&self.shared), self.id);
        }
    }
}

/// Withdraws an announcement, trying until the authority answers: until
/// then every other proxy holds the key.
fn withdraw_later(shared: Arc<Shared>, id: i64) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    runtime.spawn(async move {
        let Some(peers) = shared.peers.as_ref() else {
            return;
        };
        let mut wait = Duration::from_millis(50);
        loop {
            match peers.withdraw(&shared, id).await {
                Ok(()) => return,
                Err(error) => tracing::warn!(%error, id, "could not withdraw a move; retrying"),
            }
            tokio::time::sleep(wait).await;
            wait = wait.saturating_mul(2).min(Duration::from_secs(30));
        }
    });
}

fn unreachable(error: &str) -> Refusal {
    Refusal::new(
        "57P03",
        format!("the shared directory could not be reached: {error}"),
    )
    .with_hint("try again; see SHOW PEERS")
}

/// The geo-partitioned tables, whose keys the directory places.
fn geo_tables(shared: &Shared) -> Vec<String> {
    shared
        .router
        .topology()
        .tables
        .iter()
        .filter(|policy| matches!(policy.placement, Placement::GeoPartitioned { .. }))
        .map(|policy| policy.table.clone())
        .collect()
}

/// `table_name, shard_key, region` rows.
fn triples(result: &QueryResult) -> Vec<(String, String, String)> {
    result
        .rows
        .iter()
        .filter_map(|row| match (row.first(), row.get(1), row.get(2)) {
            (Some(Some(table)), Some(Some(key)), Some(Some(region))) => {
                Some((table.clone(), key.clone(), region.clone()))
            }
            _ => None,
        })
        .collect()
}

fn read_visit(result: &QueryResult) -> Visit {
    let mut visit = Visit::default();
    for row in &result.rows {
        let field = |n: usize| row.get(n).cloned().flatten();
        match field(0).as_deref() {
            Some("beat") => visit.current = field(1).as_deref() == Some("1"),
            Some("move") => {
                if let (Some(id), Some(key), Some(to), Some(mover), Some(phase)) = (
                    field(1).and_then(|id| id.parse().ok()),
                    field(2),
                    field(3),
                    field(4),
                    field(5).as_deref().and_then(Phase::parse),
                ) {
                    visit.moves.push(Announced {
                        id,
                        key,
                        to,
                        mover,
                        phase,
                    });
                }
            }
            Some("peer") => {
                if let Some(node) = field(1) {
                    visit.peers.push(Peer {
                        node,
                        heartbeat_age_ms: field(2)
                            .and_then(|age| age.parse::<i64>().ok())
                            .map_or(0, |age| u64::try_from(age).unwrap_or(0)),
                    });
                }
            }
            _ => {}
        }
    }
    visit
}

/// A table beside the directory's, in the same schema.
fn derived(store: &DirectoryStore, suffix: &str) -> String {
    match store.table.split_once('.') {
        Some((schema, table)) => {
            format!("{}.{}", ident(schema), ident(&format!("{table}_{suffix}")))
        }
        None => ident(&format!("{}_{suffix}", store.table)),
    }
}

async fn create(pool: &Pool, tables: &Tables) -> Result<(), String> {
    let statements = [
        format!(
            "CREATE TABLE IF NOT EXISTS {} (node text PRIMARY KEY, instance text NOT NULL, \
             heartbeat_at timestamptz NOT NULL)",
            tables.proxies
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY \
             KEY, shard_key text NOT NULL UNIQUE, to_region text NOT NULL, mover text NOT NULL, \
             phase text NOT NULL, started_at timestamptz NOT NULL DEFAULT now())",
            tables.moves
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (move_id bigint NOT NULL, node text NOT NULL, phase \
             text NOT NULL, PRIMARY KEY (move_id, node, phase))",
            tables.acks
        ),
    ];
    for statement in &statements {
        // Two proxies starting together can race to create the same table;
        // the loser's error is gone on the next try.
        let mut tries = 0_u32;
        loop {
            match pool.simple_query(statement).await {
                Ok(_) => break,
                Err(error) if tries < 3 => {
                    tracing::debug!(%error, "retrying a table creation");
                    tries = tries.saturating_add(1);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(error) => return Err(format!("could not create the shared tables: {error}")),
            }
        }
    }
    Ok(())
}

// ------------------------------------------------------------ in memory

/// The shared state, kept in memory for proxies running in one process.
///
/// It stands in for the tables in the authority region, including the
/// directory table: where a real move writes that table in its own
/// transaction, a move here records the key's new region when it commits.
/// [`MemoryPeers::cut_off`] makes it unreachable from one proxy, to see
/// what that proxy does without it.
#[derive(Debug, Default)]
pub struct MemoryPeers {
    state: parking_lot::Mutex<MemoryState>,
}

#[derive(Debug, Default)]
struct MemoryState {
    proxies: BTreeMap<String, (String, Instant)>,
    moves: BTreeMap<i64, Announced>,
    confirmations: BTreeSet<(i64, String, Phase)>,
    directory: BTreeMap<(String, String), String>,
    next_id: i64,
    cut_off: BTreeSet<String>,
}

impl MemoryPeers {
    /// Empty shared state.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::default()
    }

    /// Places a key, as a directory entry written before any proxy started.
    pub fn place(&self, table: &str, key: &str, region: &str) {
        self.state
            .lock()
            .directory
            .insert((table.to_owned(), key.to_owned()), region.to_owned());
    }

    /// Where the shared directory puts a key.
    #[must_use]
    pub fn region_of(&self, table: &str, key: &str) -> Option<String> {
        self.state
            .lock()
            .directory
            .get(&(table.to_owned(), key.to_owned()))
            .cloned()
    }

    /// How many moves are announced.
    #[must_use]
    pub fn announced(&self) -> usize {
        self.state.lock().moves.len()
    }

    /// Makes the shared state unreachable from `node`, or reachable again.
    pub fn cut_off(&self, node: &str, cut: bool) {
        let mut state = self.state.lock();
        if cut {
            state.cut_off.insert(node.to_owned());
        } else {
            state.cut_off.remove(node);
        }
    }

    fn reach(&self, node: &str) -> Result<parking_lot::MutexGuard<'_, MemoryState>, String> {
        let state = self.state.lock();
        if state.cut_off.contains(node) {
            return Err("the authority region cannot be reached".to_owned());
        }
        Ok(state)
    }

    fn register(&self, node: &str, instance: &str, window: Duration) -> Result<bool, String> {
        let mut state = self.reach(node)?;
        if let Some((other, beat)) = state.proxies.get(node) {
            if other != instance && beat.elapsed() < window {
                return Ok(false);
            }
        }
        state
            .proxies
            .insert(node.to_owned(), (instance.to_owned(), Instant::now()));
        Ok(true)
    }

    fn visit(&self, node: &str, instance: &str) -> Result<Visit, String> {
        let mut state = self.reach(node)?;
        let current = match state.proxies.get_mut(node) {
            Some((held, beat)) if held == instance => {
                *beat = Instant::now();
                true
            }
            _ => false,
        };
        Ok(Visit {
            current,
            moves: state.moves.values().cloned().collect(),
            peers: state
                .proxies
                .iter()
                .map(|(node, (_, beat))| Peer {
                    node: node.clone(),
                    heartbeat_age_ms: u64::try_from(beat.elapsed().as_millis()).unwrap_or(u64::MAX),
                })
                .collect(),
        })
    }

    fn confirm(&self, node: &str, id: i64, phase: Phase) -> Result<(), String> {
        self.reach(node)?
            .confirmations
            .insert((id, node.to_owned(), phase));
        Ok(())
    }

    fn claim(
        &self,
        node: &str,
        keys: &[(String, String, String)],
    ) -> Result<Vec<(String, String, String)>, String> {
        let mut state = self.reach(node)?;
        Ok(keys
            .iter()
            .map(|(table, key, region)| {
                let placed = state
                    .directory
                    .entry((table.clone(), key.clone()))
                    .or_insert_with(|| region.clone())
                    .clone();
                (table.clone(), key.clone(), placed)
            })
            .collect())
    }

    fn announce(&self, node: &str, key: &str, to: &str) -> Result<Result<i64, String>, String> {
        let mut state = self.reach(node)?;
        if let Some(existing) = state.moves.values().find(|move_| move_.key == key) {
            return Ok(Err(existing.mover.clone()));
        }
        state.next_id = state.next_id.saturating_add(1);
        let id = state.next_id;
        state.moves.insert(
            id,
            Announced {
                id,
                key: key.to_owned(),
                to: to.to_owned(),
                mover: node.to_owned(),
                phase: Phase::Cutover,
            },
        );
        Ok(Ok(id))
    }

    fn unconfirmed(
        &self,
        node: &str,
        id: i64,
        phase: Phase,
        window: Duration,
    ) -> Result<Vec<String>, String> {
        let state = self.reach(node)?;
        Ok(state
            .proxies
            .iter()
            .filter(|(other, (_, beat))| *other != node && beat.elapsed() < window)
            .filter(|(other, _)| !state.confirmations.contains(&(id, (*other).clone(), phase)))
            .map(|(other, _)| other.clone())
            .collect())
    }

    fn set_phase(&self, node: &str, id: i64, phase: Phase) -> Result<(), String> {
        if let Some(move_) = self.reach(node)?.moves.get_mut(&id) {
            move_.phase = phase;
        }
        Ok(())
    }

    fn withdraw(&self, node: &str, id: i64) -> Result<(), String> {
        let mut state = self.reach(node)?;
        state.moves.remove(&id);
        state.confirmations.retain(|(move_id, _, _)| *move_id != id);
        Ok(())
    }

    fn withdraw_all(&self, node: &str) -> Result<(), String> {
        let mut state = self.reach(node)?;
        let mine: Vec<i64> = state
            .moves
            .values()
            .filter(|move_| move_.mover == node)
            .map(|move_| move_.id)
            .collect();
        for id in mine {
            state.moves.remove(&id);
            state.confirmations.retain(|(move_id, _, _)| *move_id != id);
        }
        Ok(())
    }

    fn regions_of(&self, node: &str, key: &str) -> Result<Vec<(String, String)>, String> {
        Ok(self
            .reach(node)?
            .directory
            .iter()
            .filter(|((_, held), _)| held == key)
            .map(|((table, _), region)| (table.clone(), region.clone()))
            .collect())
    }

    fn everything(&self, node: &str) -> Result<Vec<(String, String, String)>, String> {
        Ok(self
            .reach(node)?
            .directory
            .iter()
            .map(|((table, key), region)| (table.clone(), key.clone(), region.clone()))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tables_sit_beside_the_directory_in_its_schema() {
        let mut store = DirectoryStore::default();
        assert_eq!(derived(&store, "moves"), "\"luxbyte_directory_moves\"");
        store.table = "ops.keys".to_owned();
        assert_eq!(derived(&store, "acks"), "\"ops\".\"keys_acks\"");
    }

    #[test]
    fn a_visit_is_read_from_one_result() {
        let result = QueryResult {
            rows: vec![
                vec![Some("beat".to_owned()), Some("1".to_owned())],
                vec![
                    Some("move".to_owned()),
                    Some("7".to_owned()),
                    Some("alice".to_owned()),
                    Some("us-east".to_owned()),
                    Some("proxy-a".to_owned()),
                    Some("committing".to_owned()),
                ],
                vec![
                    Some("peer".to_owned()),
                    Some("proxy-b".to_owned()),
                    Some("120".to_owned()),
                ],
            ],
            ..QueryResult::default()
        };
        let visit = read_visit(&result);
        assert!(visit.current);
        assert_eq!(
            visit.moves,
            [Announced {
                id: 7,
                key: "alice".to_owned(),
                to: "us-east".to_owned(),
                mover: "proxy-a".to_owned(),
                phase: Phase::Committing,
            }]
        );
        assert_eq!(
            visit.peers,
            [Peer {
                node: "proxy-b".to_owned(),
                heartbeat_age_ms: 120
            }]
        );
    }

    #[test]
    fn the_timings_must_leave_room_for_a_slow_visit() {
        let regions = ["eu".to_owned()];
        let mut store = DirectoryStore {
            shared: true,
            lease_ms: 300,
            poll_ms: 200,
            ..DirectoryStore::default()
        };
        assert!(Peers::new(&store, "a", "eu", &regions, None).is_err());
        store.lease_ms = 400;
        assert!(Peers::new(&store, "a", "eu", &regions, None).is_ok());
        store.authority = Some("mars".to_owned());
        assert!(Peers::new(&store, "a", "eu", &regions, None).is_err());
    }

    #[test]
    fn a_name_in_use_by_a_live_process_cannot_be_taken() {
        let memory = MemoryPeers::new();
        let window = Duration::from_secs(5);
        assert_eq!(memory.register("a", "one", window), Ok(true));
        assert_eq!(memory.register("a", "two", window), Ok(false));
        assert_eq!(memory.register("a", "one", window), Ok(true));
        assert_eq!(memory.register("a", "two", Duration::ZERO), Ok(true));
        let visit = memory.visit("a", "one").unwrap();
        assert!(!visit.current, "the second process took the name");
    }

    #[test]
    fn the_first_claim_of_a_key_wins() {
        let memory = MemoryPeers::new();
        let key = |region: &str| [("users".to_owned(), "carol".to_owned(), region.to_owned())];
        let first = memory.claim("a", &key("eu")).unwrap();
        let second = memory.claim("b", &key("us")).unwrap();
        assert_eq!(first, second);
        assert_eq!(memory.region_of("users", "carol").as_deref(), Some("eu"));
    }
}
