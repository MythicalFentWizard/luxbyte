//! Several proxies sharing one directory.
//!
//! One proxy keeps its directory in memory, loaded from a table and changed
//! only by its own moves. Several proxies can read the same table, but each
//! would then route on its own copy, and a move made by one would leave the
//! others sending the key's writes to the region its rows just left.
//!
//! So proxies sharing a directory agree, either in one region, the
//! *authority*, or across a majority of all the regions:
//!
//! - **Leases.** Each proxy visits every `poll_ms`, and routes on its
//!   directory only while its lease, renewed by visits that got through, is
//!   under `lease_ms` old. Past that it refuses whatever depends on the
//!   directory, because it can no longer hear about moves; when it gets
//!   through again it reads the whole directory before routing on it.
//! - **Moves.** A move is announced before it starts. Every proxy that may
//!   hold a lease holds the key's writes, waits for its own writes already
//!   under way, and says so; only then are rows copied. Before the commit
//!   every proxy holds the key's reads too, and says so again. A proxy that
//!   does not answer stops the move, unless its lease was last renewed more
//!   than half again `lease_ms` ago, by when it has stopped routing by
//!   itself. Once the announcement is withdrawn each proxy reads where the
//!   key now is.
//! - **New keys.** A write to a key nobody has placed claims it first, so
//!   two proxies in different regions cannot each create it at home.
//!
//! # A majority instead of an authority
//!
//! With `authority = "majority"` every region keeps the shared tables, and
//! each step counts once a majority of regions has it. Any two majorities
//! share a region, which is what the protocol rests on: a proxy reading a
//! majority sees every move announced to one, every confirmation and every
//! phase change; a withdrawn move leaves a marker, so that a region which
//! missed the withdrawal cannot bring it back. Claims, which two proxies
//! may make at once, are decided by single-decree Paxos with each region's
//! database as an acceptor. Directory entries carry a version that every
//! move raises in every region, so the newest answer wins.
//!
//! Leases need one more step. With one authority, a heartbeat that reaches
//! it is the lease. Spread over regions, a proxy cut off with one region
//! could keep refreshing its heartbeat there, and a mover reading that
//! region would wait on it for ever. So a visit *grants* only when the
//! visit before it reached a majority, and a proxy routes on a grant only
//! once a later visit has reached a majority too: a mover sees every grant
//! a proxy may be routing on, and a proxy cut off from the majority stops
//! granting at once.
//!
//! So losing a minority of regions stops nothing: every proxy that reaches
//! a majority keeps its lease, moves and claims carry on, and only the keys
//! whose rows were in the lost regions are out of reach, which no
//! arrangement of proxies can change.
//!
//! What it costs: one small query to the authority, or to every region at
//! once, every `poll_ms` from each proxy; on the first write of each new
//! key, a round trip to the authority or two to a majority; and while a key
//! moves, holds on it up to two polls longer than one proxy alone would
//! hold it.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
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

/// The `authority` that means a majority of regions rather than one.
pub const MAJORITY: &str = "majority";

/// How long a withdrawn move's marker is kept where some region never
/// answered the withdrawal: long past any request still in flight.
const MARKER_KEPT: &str = "1 hour";

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

/// A move announced to the other proxies.
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
    /// What it said of itself with that heartbeat.
    pub summary: String,
}

/// What one region said on a visit.
#[derive(Debug, Default, Clone)]
struct RegionView {
    /// The heartbeat row is this process's.
    held: bool,
    /// Moves, with whether each has been withdrawn.
    moves: Vec<(Announced, bool)>,
    peers: Vec<Peer>,
}

/// What a visit learned, from every region that answered.
#[derive(Debug, Default)]
struct Visit {
    /// No region said another process holds this proxy's name.
    current: bool,
    moves: Vec<Announced>,
    peers: Vec<Peer>,
    /// When the lease this visit renews began, if it renews one.
    lease_from: Option<Instant>,
}

/// A heartbeat, as sent to each region.
#[derive(Debug)]
struct Beat<'a> {
    node: &'a str,
    instance: &'a str,
    summary: &'a str,
    /// Another process may take over the name once its holder has not
    /// been seen for this long.
    window: Duration,
    /// Whether this heartbeat grants a lease a mover must respect.
    grant: bool,
    /// Placements decided by claims, recorded in the region's directory.
    learn: &'a [(String, String, String)],
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
    /// Transactions an earlier run left in doubt are resolved, so a move
    /// announced under this proxy's name and not by this process can be
    /// withdrawn.
    recovered: bool,
    /// Across a majority: when the last visit began, if it reached one.
    reached: Option<Instant>,
    /// Across a majority: when the last visit began, if it reached one and
    /// granted. The next visit to reach a majority makes it the lease.
    granted: Option<Instant>,
    /// Placements decided by claims, for the next visit to record.
    to_learn: Vec<(String, String, String)>,
    /// Across a majority: the regions the last visit did not reach.
    unreached: BTreeSet<String>,
    /// A region the last visit did not reach answered again, and may have
    /// missed directory changes meanwhile.
    repair_due: bool,
}

/// The tables beside the directory, qualified and quoted.
#[derive(Debug, Clone)]
struct Tables {
    directory: String,
    proxies: String,
    moves: String,
    acks: String,
    claims: String,
}

/// One region's copy of the shared state.
#[derive(Debug)]
enum Replica {
    /// Tables in a region's database.
    Postgres { region: String, tables: Tables },
    /// Memory shared by proxies in one process.
    Memory {
        index: usize,
        memory: Arc<MemoryPeers>,
    },
}

/// What announcing a move found in one region.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Announce {
    Added,
    /// Another move of the key is under way, by this proxy.
    Taken(String),
}

/// What one acceptor said to a Paxos prepare.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Promise {
    /// The ballot it last accepted, 0 for none, and the value; `None` when
    /// it has promised a higher ballot.
    granted: Option<(i64, Option<String>)>,
    /// Where the region's directory already puts the key, and the entry's
    /// version.
    placed: Option<(String, i64)>,
}

/// How long to wait for the regions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Wait {
    /// For a majority, and then for the rest as long again as the majority
    /// took: a region that has stopped answering costs every step only a
    /// little, and one that is merely far away is still heard.
    Quorum,
    /// For every region.
    Every,
    /// For every region, as long as creating tables may take.
    Slow,
}

/// A directory entry: table, key, region and version.
type Row = (String, String, String, i64);

/// How long ago each proxy last granted, and who confirmed which phase.
type Progress = (Vec<(String, u64)>, Vec<(String, Phase)>);

/// This proxy's part in a shared directory.
#[derive(Debug)]
pub struct Peers {
    node: String,
    /// Tells this process apart from an earlier or later one with the same
    /// name.
    instance: String,
    /// The region the proxies meet in, or `None` for a majority of them.
    authority: Option<String>,
    poll: Duration,
    lease: Duration,
    replicas: Vec<Replica>,
    state: parking_lot::Mutex<State>,
}

impl Peers {
    /// Settings for a shared directory, checked against the regions.
    ///
    /// `memory` keeps the shared state in this process instead of in the
    /// regions' databases, for proxies running side by side in tests.
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
        let named = config.authority.clone().unwrap_or_else(|| home.to_owned());
        let authority = (named != MAJORITY).then_some(named);
        if let Some(authority) = &authority {
            if !regions.contains(authority) {
                return Err(format!(
                    "directory_store.authority is \"{authority}\", which is neither a region nor \
                     \"{MAJORITY}\""
                ));
            }
        }
        // Across a majority a lease is applied a visit late, so it has to
        // outlast two visits rather than one.
        let visits: u64 = if authority.is_some() { 2 } else { 3 };
        if config.poll_ms == 0 || config.lease_ms < config.poll_ms.saturating_mul(visits) {
            return Err(format!(
                "directory_store.lease_ms ({}) must be at least {visits} times poll_ms ({}), and \
                 poll_ms more than zero, or one slow visit would stop the proxy routing",
                config.lease_ms, config.poll_ms
            ));
        }
        let mut instance = [0_u8; 8];
        {
            use rand::Rng as _;
            rand::rng().fill_bytes(&mut instance);
        }
        let tables = Tables {
            directory: store_table(config),
            proxies: derived(config, "proxies"),
            moves: derived(config, "moves"),
            acks: derived(config, "acks"),
            claims: derived(config, "claims"),
        };
        let replicas = match memory {
            Some(memory) => {
                let count = if authority.is_some() {
                    1
                } else {
                    memory.regions()
                };
                (0..count)
                    .map(|index| Replica::Memory {
                        index,
                        memory: Arc::clone(&memory),
                    })
                    .collect()
            }
            None => match &authority {
                Some(region) => vec![Replica::Postgres {
                    region: region.clone(),
                    tables,
                }],
                None => regions
                    .iter()
                    .map(|region| Replica::Postgres {
                        region: region.clone(),
                        tables: tables.clone(),
                    })
                    .collect(),
            },
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
            replicas,
            state: parking_lot::Mutex::new(State::default()),
        })
    }

    /// Where the proxies meet, for people: a region, or a majority of them.
    #[must_use]
    pub fn meeting(&self) -> String {
        self.authority.clone().unwrap_or_else(|| {
            format!(
                "a majority of the {} regions ({} needed)",
                self.replicas.len(),
                self.quorum()
            )
        })
    }

    /// Whether a committed move must keep the other proxies holding its
    /// keys while `pending` regions have yet to commit it: they read where
    /// the keys went from the authority, or from any majority, which has to
    /// include a region that has.
    #[must_use]
    pub fn must_wait_for(&self, pending: &[String]) -> bool {
        match &self.authority {
            Some(authority) => pending.iter().any(|region| region == authority),
            None => self.replicas.len().saturating_sub(pending.len()) < self.quorum(),
        }
    }

    /// This proxy's name.
    #[must_use]
    pub fn node(&self) -> &str {
        &self.node
    }

    /// How many regions must answer for a step to count.
    fn quorum(&self) -> usize {
        self.replicas
            .len()
            .checked_div(2)
            .unwrap_or(0)
            .saturating_add(1)
    }

    /// How long another proxy counts as alive after its last grant. Half as
    /// long again as the lease, so that by then it has stopped routing by
    /// itself even if its clock runs a little slow.
    fn alive_for(&self) -> Duration {
        self.lease
            .saturating_add(self.lease.checked_div(2).unwrap_or_default())
    }

    /// How long a step waits for the regions before counting those that
    /// have not answered as down.
    fn patience(&self) -> Duration {
        self.lease.checked_div(2).unwrap_or_default()
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
                self.meeting()
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
    /// When too few regions can be reached, or another live process uses
    /// this proxy's name.
    pub async fn start(&self, shared: &Shared, recovered: bool) -> Result<(), String> {
        let created = self
            .on_every(Wait::Slow, |replica| replica.create(shared))
            .await;
        self.enough(&created, "create the shared tables")?;

        let first = self.visit(shared, Instant::now()).await?;
        if !first.current {
            return Err(format!(
                "another proxy named \"{}\" has been seen in the last {} ms; every proxy sharing \
                 a directory needs its own server.node",
                self.node,
                self.alive_for().as_millis()
            ));
        }
        if recovered {
            self.withdraw_stale(shared).await?;
        } else {
            tracing::warn!(
                "transactions are still in doubt, so moves an earlier run of this proxy \
                 announced stay announced and their keys stay held"
            );
        }
        self.state.lock().recovered = recovered;
        if self.majority() {
            self.repair(shared).await?;
        }

        // One visit gives a lease with an authority; across a majority the
        // first visit to grant is made the lease by the one after.
        let mut visit;
        let mut visits = 0_u32;
        loop {
            visit = self.visit(shared, Instant::now()).await?;
            if !visit.current {
                return Err(format!(
                    "another proxy took the name \"{}\" while this one started",
                    self.node
                ));
            }
            visits = visits.saturating_add(1);
            if visit.lease_from.is_some() || visits >= 3 {
                break;
            }
        }
        let Some(from) = visit.lease_from else {
            return Err("the shared directory did not grant this proxy a lease".to_owned());
        };
        self.reload(shared).await?;
        self.follow(shared, &visit.moves).await;
        self.renewed(Some(from), visit.peers);
        Ok(())
    }

    /// Keeps visiting until the task is stopped.
    pub async fn run(shared: Arc<Shared>) {
        let Some(peers) = shared.peers.as_ref() else {
            return;
        };
        let mut repairing: Option<tokio::task::JoinHandle<()>> = None;
        loop {
            let started = Instant::now();
            peers.round(&shared).await;
            // A region that is back is brought up to date on the side, so
            // that copying a large directory never holds up a lease.
            let due = std::mem::take(&mut peers.state.lock().repair_due);
            if due
                && repairing
                    .as_ref()
                    .is_none_or(tokio::task::JoinHandle::is_finished)
            {
                let shared = Arc::clone(&shared);
                repairing = Some(tokio::spawn(async move {
                    if let Some(peers) = shared.peers.as_ref() {
                        if let Err(error) = peers.repair(&shared).await {
                            tracing::warn!(%error, "could not bring a region's directory up to date");
                            peers.state.lock().repair_due = true;
                        }
                    }
                }));
            }
            tokio::time::sleep(peers.poll.saturating_sub(started.elapsed())).await;
        }
    }

    /// One visit: renews the lease, follows the moves under way, and lets
    /// go of keys whose moves are over.
    pub async fn round(&self, shared: &Shared) {
        let visit = match self.visit(shared, Instant::now()).await {
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
        if self.fenced() {
            if let Err(error) = self.reload(shared).await {
                tracing::warn!(%error, "could not read the shared directory");
                self.state.lock().last_error = Some(error);
                return;
            }
            tracing::info!("reached the shared directory again and read it whole");
        }
        self.follow(shared, &visit.moves).await;
        self.withdraw_strays(shared, &visit.moves).await;
        self.renewed(visit.lease_from, visit.peers);
        shared.moves.announce();
    }

    /// What this proxy says of itself with each heartbeat, for the others'
    /// `SHOW PEERS` and dashboards. Only counters already kept: it is sent
    /// every `poll_ms`.
    fn summary(&self, shared: &Shared) -> String {
        let counters = shared.metrics.summary();
        let statements: u64 = counters
            .iter()
            .filter(|(name, _)| name.starts_with("statements_"))
            .map(|(_, count)| *count)
            .fold(0, u64::saturating_add);
        let held = counters.get("held_statements_total").copied().unwrap_or(0);
        let following = self.state.lock().following.len();
        format!(
            "{} sessions, {statements} statements, {held} held, holding {following} keys for \
             moves{}, home {}",
            shared.metrics.active_sessions(),
            if self.fenced() { ", fenced" } else { "" },
            shared.router.topology().home_region
        )
    }

    fn renewed(&self, from: Option<Instant>, peers: Vec<Peer>) {
        let mut state = self.state.lock();
        if let Some(until) = from.and_then(|from| from.checked_add(self.lease)) {
            state.lease_until = Some(state.lease_until.map_or(until, |now| now.max(until)));
        }
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

    /// Withdraws moves announced under this proxy's name that this process
    /// is not making: left by an earlier run, or an announcement that
    /// reached a region only after its move had been withdrawn everywhere
    /// else. Nobody else would ever let their keys go.
    async fn withdraw_strays(&self, shared: &Shared, moves: &[Announced]) {
        let strays: Vec<i64> = {
            let state = self.state.lock();
            if !state.recovered {
                return;
            }
            moves
                .iter()
                .filter(|move_| move_.mover == self.node && !state.mine.contains(&move_.id))
                .map(|move_| move_.id)
                .collect()
        };
        for id in strays {
            tracing::info!(id, "withdrawing a move this process is not making");
            if let Err(error) = self.withdraw(shared, id).await {
                tracing::warn!(%error, id, "could not withdraw a stray move");
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
        let node = self.node.as_str();
        let answers = self
            .on_every(Wait::Quorum, |replica| {
                replica.confirm(shared, id, node, phase)
            })
            .await;
        match self.enough(&answers, "confirm a hold") {
            Ok(_) => true,
            Err(error) => {
                tracing::warn!(%error, id, "could not confirm a hold");
                self.state.lock().last_error = Some(error);
                false
            }
        }
    }

    // ---------------------------------------------------------- claiming

    /// Claims keys no proxy has placed, in this proxy's home shard unless
    /// another got there first, and records where each went.
    ///
    /// # Errors
    ///
    /// When the proxy is fenced or too few regions can be reached.
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
        let placed: Vec<(String, String, String)> =
            if let (Some(_), Some(replica)) = (&self.authority, self.replicas.first()) {
                replica
                    .claim_directly(shared, &self.node, &wanted)
                    .await
                    .map_err(|error| unreachable(&error))?
            } else {
                let decided = futures_util::future::join_all(
                    wanted
                        .iter()
                        .map(|(table, key, region)| self.decide(shared, table, key, region)),
                )
                .await;
                let mut placed = Vec::with_capacity(wanted.len());
                for ((table, key, _), chosen) in wanted.iter().zip(decided) {
                    placed.push((
                        table.clone(),
                        key.clone(),
                        chosen.map_err(|error| unreachable(&error))?,
                    ));
                }
                placed
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

    /// Where a new key goes, agreed by a majority of regions.
    ///
    /// A key the directory already places is wherever its newest entry
    /// says; the prepare reads it at no extra cost. Otherwise the first
    /// placement a majority accepts is the key's for good: single-decree
    /// Paxos, each region's claims table an acceptor, a ballot from the
    /// clock with a random tail, and a higher ballot after a refusal. The
    /// decision is recorded in each region's directory by the next visit.
    async fn decide(
        &self,
        shared: &Shared,
        table: &str,
        key: &str,
        proposal: &str,
    ) -> Result<String, String> {
        let node = self.node.as_str();
        let mut last = String::new();
        for attempt in 0_u32..8 {
            let number = ballot();
            let answers = self
                .on_every(Wait::Quorum, |replica| {
                    replica.prepare(shared, node, table, key, number)
                })
                .await;
            let promises = self.enough(&answers, "ask where a new key goes")?;
            if let Some((region, _)) = promises
                .iter()
                .filter_map(|promise| promise.placed.clone())
                .max_by_key(|(_, version)| *version)
            {
                return Ok(region);
            }
            let granted: Vec<(i64, Option<String>)> = promises
                .iter()
                .filter_map(|promise| promise.granted.clone())
                .collect();
            if granted.len() >= self.quorum() {
                // Whatever a majority may already have accepted has to be
                // proposed again; only a free choice is this proxy's own.
                let value = granted
                    .iter()
                    .filter(|(accepted, value)| *accepted > 0 && value.is_some())
                    .max_by_key(|(accepted, _)| *accepted)
                    .and_then(|(_, value)| value.clone())
                    .unwrap_or_else(|| proposal.to_owned());
                let accepted = self
                    .on_every(Wait::Quorum, |replica| {
                        replica.accept(shared, node, (table, key), number, &value)
                    })
                    .await;
                let yes = accepted
                    .iter()
                    .filter(|answer| matches!(answer, Ok(true)))
                    .count();
                if yes >= self.quorum() {
                    self.state.lock().to_learn.push((
                        table.to_owned(),
                        key.to_owned(),
                        value.clone(),
                    ));
                    return Ok(value);
                }
                last = format!("{yes} of {} regions accepted", self.replicas.len());
            } else {
                last = format!(
                    "{} of {} regions promised",
                    granted.len(),
                    self.replicas.len()
                );
            }
            // Another proposer is ahead; back off a little, differently.
            let pause = 5_u64.saturating_mul(u64::from(attempt).saturating_add(1));
            tokio::time::sleep(Duration::from_millis(pause.saturating_add(jitter(5)))).await;
        }
        Err(format!("could not agree where {table} {key} goes: {last}"))
    }

    // ------------------------------------------------------------ moving

    /// Announces a move of `key` to `to`, for every other proxy to hold it.
    ///
    /// # Errors
    ///
    /// When this proxy is fenced, the key is already moving, or too few
    /// regions can be reached.
    pub async fn announce(
        &self,
        shared: &Arc<Shared>,
        key: &str,
        to: &str,
    ) -> Result<Announcement, Refusal> {
        if self.fenced() {
            return Err(self.fence_refusal());
        }
        let id = move_id();
        // Recorded before it is sent, so that this proxy's own visits never
        // take it for a stray, and so that whatever happens next withdraws
        // it.
        self.state.lock().mine.insert(id);
        let announcement = Announcement {
            shared: Arc::clone(shared),
            id,
            key: key.to_owned(),
            withdrawn: false,
        };
        let node = self.node.as_str();
        let answers = self
            .on_every(Wait::Quorum, |replica| {
                replica.announce(shared, id, key, to, node)
            })
            .await;
        if let Some(mover) = answers.iter().find_map(|answer| match answer {
            Ok(Announce::Taken(mover)) => Some(mover.clone()),
            _ => None,
        }) {
            return Err(
                Refusal::new("55006", format!("{key} is already being moved, by {mover}"))
                    .with_hint("wait for that move to finish, then RELOCATE again"),
            );
        }
        self.enough(&answers, "announce the move")
            .map_err(|error| unreachable(&error))?;
        Ok(announcement)
    }

    /// The proxies that may hold a lease and have not confirmed holding the
    /// key as far as `phase`.
    async fn unconfirmed(
        &self,
        shared: &Shared,
        id: i64,
        phase: Phase,
    ) -> Result<Vec<String>, String> {
        let node = self.node.as_str();
        let answers = self
            .on_every(Wait::Quorum, |replica| replica.progress(shared, node, id))
            .await;
        let views = self.enough(&answers, "read the confirmations")?;
        let window = u64::try_from(self.alive_for().as_millis()).unwrap_or(u64::MAX);
        let mut youngest: BTreeMap<String, u64> = BTreeMap::new();
        let mut confirmed: BTreeSet<String> = BTreeSet::new();
        for (grants, acks) in views {
            for (node, age) in grants {
                let entry = youngest.entry(node).or_insert(u64::MAX);
                *entry = (*entry).min(age);
            }
            confirmed.extend(
                acks.into_iter()
                    .filter(|(_, acked)| *acked == phase)
                    .map(|(node, _)| node),
            );
        }
        Ok(youngest
            .into_iter()
            .filter(|(node, age)| *node != self.node && *age < window && !confirmed.contains(node))
            .map(|(node, _)| node)
            .collect())
    }

    async fn set_phase(&self, shared: &Shared, id: i64, phase: Phase) -> Result<(), String> {
        let node = self.node.as_str();
        let answers = self
            .on_every(Wait::Quorum, |replica| {
                replica.set_phase(shared, node, id, phase)
            })
            .await;
        self.enough(&answers, "tell the proxies the move is committing")
            .map(drop)
    }

    /// Withdraws a move: a marker in a majority, so that no majority read
    /// sees the move as under way again; once every region has the marker
    /// the move is forgotten. `Ok(false)` while some region has not.
    async fn withdraw(&self, shared: &Shared, id: i64) -> Result<bool, String> {
        let node = self.node.as_str();
        let answers = self
            .on_every(Wait::Every, |replica| replica.withdraw(shared, node, id))
            .await;
        self.enough(&answers, "withdraw a move")?;
        self.state.lock().mine.remove(&id);
        if answers.iter().all(Result::is_ok) {
            let _ = self
                .on_every(Wait::Every, |replica| replica.forget(shared, node, id))
                .await;
            return Ok(true);
        }
        Ok(false)
    }

    /// Where each table's rows for `key` are, as the shared directory says.
    ///
    /// # Errors
    ///
    /// When too few regions can be reached.
    pub async fn regions_of(
        &self,
        shared: &Shared,
        key: &str,
    ) -> Result<Vec<(String, String)>, String> {
        Ok(self
            .directory(shared, Some(&[key.to_owned()]))
            .await?
            .into_iter()
            .map(|(table, _, region, _)| (table, region))
            .collect())
    }

    /// Whether moves may leave out an unreachable region that holds none
    /// of their keys: across a majority, the regions that missed a move
    /// are outvoted by the versions of those that did not.
    #[must_use]
    pub fn majority(&self) -> bool {
        self.authority.is_none()
    }

    /// Whether `count` regions are a majority of them.
    #[must_use]
    pub fn is_majority(&self, count: usize) -> bool {
        count >= self.quorum()
    }

    /// The version a move of `keys` gives their directory entries: above
    /// any a majority has, so that regions the move leaves out, which keep
    /// an older one, lose to it.
    ///
    /// # Errors
    ///
    /// When too few regions can be reached.
    pub async fn next_version(&self, shared: &Shared, keys: &[String]) -> Result<i64, String> {
        Ok(self
            .directory(shared, Some(keys))
            .await?
            .iter()
            .map(|(_, _, _, version)| *version)
            .max()
            .unwrap_or(0)
            .saturating_add(1))
    }

    /// The directory, or some keys' part of it: the newest entry for each
    /// table and key among the regions that answer.
    async fn directory(
        &self,
        shared: &Shared,
        keys: Option<&[String]>,
    ) -> Result<Vec<Row>, String> {
        let node = self.node.as_str();
        let answers = self
            .on_every(Wait::Quorum, |replica| {
                replica.directory(shared, node, keys)
            })
            .await;
        let mut newest: BTreeMap<(String, String), (String, i64)> = BTreeMap::new();
        for rows in self.enough(&answers, "read the shared directory")? {
            for (table, key, region, version) in rows {
                match newest.get(&(table.clone(), key.clone())) {
                    Some((_, seen)) if *seen >= version => {}
                    _ => {
                        newest.insert((table, key), (region, version));
                    }
                }
            }
        }
        Ok(newest
            .into_iter()
            .map(|((table, key), (region, version))| (table, key, region, version))
            .collect())
    }

    // ----------------------------------------------------------- plumbing

    /// Runs `step` against every region at once and collects what each
    /// said, in the order of the regions. A region that has not answered
    /// when the wait is over counts as having failed.
    async fn on_every<'a, T, F, Fut>(&'a self, wait: Wait, mut step: F) -> Vec<Result<T, String>>
    where
        F: FnMut(&'a Replica) -> Fut,
        Fut: Future<Output = Result<T, String>>,
    {
        use futures_util::stream::{FuturesUnordered, StreamExt as _};

        let started = Instant::now();
        let limit = match wait {
            Wait::Slow => Duration::from_secs(30),
            Wait::Quorum | Wait::Every => self.patience(),
        };
        let mut until = started.checked_add(limit).unwrap_or(started);
        let mut pending: FuturesUnordered<_> = self
            .replicas
            .iter()
            .enumerate()
            .map(|(index, replica)| {
                let answer = step(replica);
                async move { (index, answer.await) }
            })
            .collect();
        let mut answers: Vec<Option<Result<T, String>>> = std::iter::repeat_with(|| None)
            .take(self.replicas.len())
            .collect();
        let mut good = 0_usize;
        while let Ok(Some((index, answer))) =
            tokio::time::timeout_at(tokio::time::Instant::from_std(until), pending.next()).await
        {
            if answer.is_ok() {
                good = good.saturating_add(1);
                if good == self.quorum() && wait == Wait::Quorum {
                    let grace = started.elapsed().max(Duration::from_millis(10));
                    until = until.min(Instant::now().checked_add(grace).unwrap_or(until));
                }
            }
            if let Some(slot) = answers.get_mut(index) {
                *slot = Some(answer);
            }
        }
        answers
            .into_iter()
            .zip(&self.replicas)
            .map(|(answer, replica)| {
                answer.unwrap_or_else(|| {
                    Err(format!(
                        "{}: no answer within {} ms",
                        replica.name(),
                        started.elapsed().as_millis()
                    ))
                })
            })
            .collect()
    }

    /// The answers, when a majority of regions gave one.
    fn enough<T: Clone>(
        &self,
        answers: &[Result<T, String>],
        what: &str,
    ) -> Result<Vec<T>, String> {
        let good: Vec<T> = answers
            .iter()
            .filter_map(|answer| answer.as_ref().ok().cloned())
            .collect();
        if good.len() >= self.quorum() {
            return Ok(good);
        }
        let errors: Vec<&str> = answers
            .iter()
            .filter_map(|answer| answer.as_ref().err().map(String::as_str))
            .collect();
        Err(if self.replicas.len() == 1 {
            format!("could not {what}: {}", errors.join("; "))
        } else {
            format!(
                "could not {what}: {} of {} regions answered, {} needed; {}",
                good.len(),
                self.replicas.len(),
                self.quorum(),
                errors.join("; ")
            )
        })
    }

    /// Copies each directory entry to the regions that lack it or hold an
    /// older version: entries a claim recorded under an authority, before
    /// the proxies met across a majority, and regions a move left out
    /// while they were unreachable. Until then those regions outvote
    /// nothing, but a majority without the region that does hold an entry
    /// would not know the key at all.
    async fn repair(&self, shared: &Shared) -> Result<(), String> {
        let node = self.node.as_str();
        let answers = self
            .on_every(Wait::Slow, |replica| replica.directory(shared, node, None))
            .await;
        let mut newest: BTreeMap<(String, String), (String, i64)> = BTreeMap::new();
        for rows in self.enough(&answers, "read the shared directory")? {
            for (table, key, region, version) in rows {
                match newest.get(&(table.clone(), key.clone())) {
                    Some((_, seen)) if *seen >= version => {}
                    _ => {
                        newest.insert((table, key), (region, version));
                    }
                }
            }
        }
        let mut fixes: BTreeMap<String, Vec<Row>> = BTreeMap::new();
        for (answer, replica) in answers.iter().zip(&self.replicas) {
            let Ok(rows) = answer else {
                continue;
            };
            let held: BTreeMap<(&str, &str), (&str, i64)> = rows
                .iter()
                .map(|(table, key, region, version)| {
                    ((table.as_str(), key.as_str()), (region.as_str(), *version))
                })
                .collect();
            let missing: Vec<Row> = newest
                .iter()
                .filter(|((table, key), (_, version))| {
                    held.get(&(table.as_str(), key.as_str()))
                        .is_none_or(|(_, seen)| seen < version)
                })
                .map(|((table, key), (region, version))| {
                    (table.clone(), key.clone(), region.clone(), *version)
                })
                .collect();
            if !missing.is_empty() {
                fixes.insert(replica.name(), missing);
            }
        }
        if fixes.is_empty() {
            return Ok(());
        }
        let copied: usize = fixes.values().map(Vec::len).sum();
        let repaired = self
            .on_every(Wait::Slow, |replica| {
                let rows = fixes.get(&replica.name());
                async move {
                    match rows {
                        Some(rows) => replica.repair(shared, node, rows).await,
                        None => Ok(()),
                    }
                }
            })
            .await;
        self.enough(&repaired, "repair the shared directory")?;
        tracing::info!(
            entries = copied,
            regions = fixes.len(),
            "copied directory entries to regions that lacked them"
        );
        Ok(())
    }

    async fn withdraw_stale(&self, shared: &Shared) -> Result<(), String> {
        let node = self.node.as_str();
        let answers = self
            .on_every(Wait::Quorum, |replica| replica.withdraw_all(shared, node))
            .await;
        self.enough(&answers, "withdraw an earlier run's moves")
            .map(drop)
    }

    /// Records a heartbeat in every region and reads what they hold.
    ///
    /// With an authority, a visit that reaches it renews the lease from
    /// when it began. Across a majority it grants only if the visit before
    /// reached a majority too, and the lease it renews is the previous
    /// visit's grant: see the module's notes.
    async fn visit(&self, shared: &Shared, started: Instant) -> Result<Visit, String> {
        let summary = self.summary(shared);
        let (grant, learn) = {
            let state = self.state.lock();
            (
                self.authority.is_some() || state.reached.is_some(),
                state.to_learn.clone(),
            )
        };
        let beat = Beat {
            node: &self.node,
            instance: &self.instance,
            summary: &summary,
            window: self.alive_for(),
            grant,
            learn: &learn,
        };
        let answers = self
            .on_every(Wait::Quorum, |replica| replica.beat(shared, &beat))
            .await;
        let outcome = self.enough(&answers, "renew this proxy's lease");
        let mut state = self.state.lock();
        if self.majority() {
            let unreached: BTreeSet<String> = answers
                .iter()
                .zip(&self.replicas)
                .filter(|(answer, _)| answer.is_err())
                .map(|(_, replica)| replica.name())
                .collect();
            if state.unreached.iter().any(|name| !unreached.contains(name)) {
                state.repair_due = true;
            }
            state.unreached = unreached;
        }
        match outcome {
            Ok(views) => {
                let mut visit = merge(views);
                visit.lease_from = if self.authority.is_some() {
                    Some(started)
                } else {
                    state.granted
                };
                state.granted = grant.then_some(started);
                state.reached = Some(started);
                let learned = learn.len().min(state.to_learn.len());
                state.to_learn.drain(..learned);
                Ok(visit)
            }
            Err(error) => {
                state.reached = None;
                state.granted = None;
                Err(error)
            }
        }
    }

    /// Reads the whole directory and makes the working copy match it,
    /// leaving keys that are moving alone.
    async fn reload(&self, shared: &Shared) -> Result<(), String> {
        let rows = self.directory(shared, None).await?;
        let topology = shared.router.topology();
        let mut wanted: BTreeMap<(String, String), String> = BTreeMap::new();
        for (table, key, region, _) in rows {
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

/// One visit's answers, as one: moves under way unless any region has
/// seen them withdrawn, at the furthest phase any region has, and each
/// proxy at its freshest heartbeat.
fn merge(views: Vec<RegionView>) -> Visit {
    let mut visit = Visit {
        current: views.iter().all(|view| view.held),
        ..Visit::default()
    };
    let mut moves: BTreeMap<i64, (Announced, bool)> = BTreeMap::new();
    let mut peers: BTreeMap<String, Peer> = BTreeMap::new();
    for view in views {
        for (announced, withdrawn) in view.moves {
            let entry = moves
                .entry(announced.id)
                .or_insert_with(|| (announced.clone(), withdrawn));
            entry.1 |= withdrawn;
            entry.0.phase = entry.0.phase.max(announced.phase);
        }
        for peer in view.peers {
            match peers.get(&peer.node) {
                Some(seen) if seen.heartbeat_age_ms <= peer.heartbeat_age_ms => {}
                _ => {
                    peers.insert(peer.node.clone(), peer);
                }
            }
        }
    }
    visit.moves = moves
        .into_values()
        .filter(|(_, withdrawn)| !withdrawn)
        .map(|(announced, _)| announced)
        .collect();
    visit.peers = peers.into_values().collect();
    visit
}

/// A move's number: random, so that proxies need not agree on a counter.
fn move_id() -> i64 {
    use rand::Rng as _;
    let mut bytes = [0_u8; 8];
    rand::rng().fill_bytes(&mut bytes);
    i64::from_be_bytes(bytes) & i64::MAX
}

/// A Paxos ballot: milliseconds since the epoch, with a random tail so that
/// two proposers in the same millisecond still differ.
fn ballot() -> i64 {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
        });
    let tail = i64::try_from(jitter(1 << 20)).unwrap_or(0);
    millis.saturating_mul(1 << 20).saturating_add(tail)
}

/// A random number below `bound`.
fn jitter(bound: u64) -> u64 {
    use rand::Rng as _;
    let mut bytes = [0_u8; 8];
    rand::rng().fill_bytes(&mut bytes);
    u64::from_be_bytes(bytes)
        .checked_rem(bound.max(1))
        .unwrap_or(0)
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

    /// Waits until every proxy that may hold a lease holds the key as far
    /// as `phase`.
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
                Err(error) => vec![format!("({error})")],
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
            "see SHOW PEERS; a proxy that cannot reach the shared directory stops routing once \
             its lease runs out, and a move stops waiting for it soon after",
        ))
    }

    /// Tells every proxy the move has reached `phase`.
    ///
    /// # Errors
    ///
    /// When too few regions can be reached.
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
        if let Some(peers) = self.peers() {
            if let Some(Replica::Memory { memory, .. }) = peers.replicas.first() {
                for table in tables {
                    memory.place(table, &self.key, to);
                }
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

/// Withdraws an announcement, trying until a majority has the marker,
/// since until then the other proxies hold the key, and then until every
/// region has it, so that it can be forgotten.
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
                Ok(true) => return,
                Ok(false) => tracing::debug!(id, "withdrawn; waiting for every region"),
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

/// A table beside the directory's, in the same schema.
fn derived(store: &DirectoryStore, suffix: &str) -> String {
    match store.table.split_once('.') {
        Some((schema, table)) => {
            format!("{}.{}", ident(schema), ident(&format!("{table}_{suffix}")))
        }
        None => ident(&format!("{}_{suffix}", store.table)),
    }
}

/// A quoted name's own text, for deriving another name from it.
fn unquoted(name: &str) -> String {
    name.rsplit('.')
        .next()
        .unwrap_or(name)
        .trim_matches('"')
        .replace("\"\"", "\"")
}

/// `(table, key, region)` rows as SQL `VALUES`.
fn values_of(rows: &[(String, String, String)]) -> String {
    rows.iter()
        .map(|(table, key, region)| {
            format!(
                "({}, {}, {})",
                literal(table),
                literal(key),
                literal(region)
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

// ------------------------------------------------------- one region's copy

impl Replica {
    fn name(&self) -> String {
        match self {
            Self::Postgres { region, .. } => region.clone(),
            Self::Memory { index, .. } => format!("region {index}"),
        }
    }

    async fn query(shared: &Shared, region: &str, sql: &str) -> Result<QueryResult, String> {
        let pool: Pool = shared
            .pool_for(region, Access::ReadWrite, 0)
            .ok_or_else(|| format!("region {region} is not configured"))?;
        pool.simple_query(sql)
            .await
            .map_err(|error| format!("{region}: {error}"))
    }

    async fn create(&self, shared: &Shared) -> Result<(), String> {
        let Self::Postgres { region, tables } = self else {
            return Ok(());
        };
        let statements = [
            format!(
                "CREATE TABLE IF NOT EXISTS {} (node text PRIMARY KEY, instance text NOT NULL, \
                 heartbeat_at timestamptz NOT NULL)",
                tables.proxies
            ),
            // Added after the table first shipped.
            format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS summary text, \
                 ADD COLUMN IF NOT EXISTS granted_at timestamptz",
                tables.proxies
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {} (id bigint GENERATED BY DEFAULT AS IDENTITY \
                 PRIMARY KEY, shard_key text NOT NULL, to_region text NOT NULL, mover text NOT \
                 NULL, phase text NOT NULL, started_at timestamptz NOT NULL DEFAULT now())",
                tables.moves
            ),
            format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS withdrawn boolean NOT NULL DEFAULT false",
                tables.moves
            ),
            // One move of a key under way at a time; a withdrawn one does
            // not count. Replaces the plain unique constraint the table
            // first shipped with.
            format!(
                "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} (shard_key) WHERE NOT withdrawn",
                ident(&format!("{}_active", unquoted(&tables.moves))),
                tables.moves
            ),
            format!(
                "ALTER TABLE {} DROP CONSTRAINT IF EXISTS {}",
                tables.moves,
                ident(&format!("{}_shard_key_key", unquoted(&tables.moves)))
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {} (move_id bigint NOT NULL, node text NOT NULL, phase \
                 text NOT NULL, PRIMARY KEY (move_id, node, phase))",
                tables.acks
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {} (table_name text NOT NULL, shard_key text NOT \
                 NULL, promised bigint NOT NULL, accepted bigint NOT NULL DEFAULT 0, value text, \
                 PRIMARY KEY (table_name, shard_key))",
                tables.claims
            ),
        ];
        for statement in &statements {
            // Two proxies starting together can race to create the same
            // table; the loser's error is gone on the next try.
            let mut tries = 0_u32;
            loop {
                match Self::query(shared, region, statement).await {
                    Ok(_) => break,
                    Err(error) if tries < 3 => {
                        tracing::debug!(%error, "retrying a table creation");
                        tries = tries.saturating_add(1);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Err(error) => {
                        return Err(format!("could not create the shared tables: {error}"));
                    }
                }
            }
        }
        Ok(())
    }

    /// Records a heartbeat, taking over this proxy's row from an earlier
    /// run that has stopped, records decided placements, and reads the
    /// moves and the other proxies.
    async fn beat(&self, shared: &Shared, beat: &Beat<'_>) -> Result<RegionView, String> {
        let (region, tables) = match self {
            Self::Postgres { region, tables } => (region, tables),
            Self::Memory { index, memory } => return memory.beat(*index, beat),
        };
        let grant = if beat.grant {
            "clock_timestamp()"
        } else {
            "NULL::timestamptz"
        };
        let learned = if beat.learn.is_empty() {
            String::new()
        } else {
            format!(
                "learned AS (INSERT INTO {} (table_name, shard_key, region) VALUES {} \
                 ON CONFLICT (table_name, shard_key) DO NOTHING), ",
                tables.directory,
                values_of(beat.learn)
            )
        };
        let result = Self::query(
            shared,
            region,
            &format!(
                "WITH beat AS (INSERT INTO {proxies} AS p (node, instance, heartbeat_at, \
                 granted_at, summary) VALUES ({node}, {instance}, clock_timestamp(), {grant}, \
                 {summary}) \
                 ON CONFLICT (node) DO UPDATE SET instance = EXCLUDED.instance, \
                 heartbeat_at = EXCLUDED.heartbeat_at, \
                 granted_at = coalesce(EXCLUDED.granted_at, p.granted_at), \
                 summary = EXCLUDED.summary \
                 WHERE p.instance = EXCLUDED.instance \
                 OR p.heartbeat_at < clock_timestamp() - interval '{ms} milliseconds' \
                 RETURNING 1), {learned}\
                 expired AS (DELETE FROM {moves} WHERE withdrawn \
                 AND started_at < clock_timestamp() - interval '{MARKER_KEPT}') \
                 SELECT 'beat'::text, (SELECT count(*) FROM beat)::text, NULL::text, \
                 NULL::text, NULL::text, NULL::text, NULL::text \
                 UNION ALL SELECT 'move', id::text, shard_key, to_region, mover, phase, \
                 withdrawn::text FROM {moves} \
                 UNION ALL SELECT 'peer', node, \
                 (extract(epoch FROM clock_timestamp() - heartbeat_at) * 1000)::bigint::text, \
                 coalesce(summary, ''), NULL, NULL, NULL FROM {proxies}",
                proxies = tables.proxies,
                moves = tables.moves,
                node = literal(beat.node),
                instance = literal(beat.instance),
                summary = literal(beat.summary),
                ms = beat.window.as_millis()
            ),
        )
        .await?;
        Ok(read_view(&result))
    }

    async fn announce(
        &self,
        shared: &Shared,
        id: i64,
        key: &str,
        to: &str,
        mover: &str,
    ) -> Result<Announce, String> {
        let (region, tables) = match self {
            Self::Postgres { region, tables } => (region, tables),
            Self::Memory { index, memory } => return memory.announce(*index, mover, id, key, to),
        };
        let result = Self::query(
            shared,
            region,
            &format!(
                "WITH added AS (INSERT INTO {moves} (id, shard_key, to_region, mover, phase) \
                 VALUES ({id}, {key}, {to}, {mover}, 'cutover') ON CONFLICT DO NOTHING \
                 RETURNING id) \
                 SELECT 'added', id::text FROM added \
                 UNION ALL SELECT 'existing', mover FROM {moves} WHERE shard_key = {key} \
                 AND NOT withdrawn AND id <> {id}",
                moves = tables.moves,
                key = literal(key),
                to = literal(to),
                mover = literal(mover)
            ),
        )
        .await?;
        let first = |kind: &str| {
            result
                .rows
                .iter()
                .find(|row| row.first().cloned().flatten().as_deref() == Some(kind))
                .and_then(|row| row.get(1).cloned().flatten())
        };
        Ok(match (first("existing"), first("added")) {
            (Some(mover), _) => Announce::Taken(mover),
            (None, Some(_)) => Announce::Added,
            // Another move of the key was added at the same moment and the
            // unique index turned this one away.
            (None, None) => Announce::Taken("another proxy, at the same moment".to_owned()),
        })
    }

    async fn confirm(
        &self,
        shared: &Shared,
        id: i64,
        node: &str,
        phase: Phase,
    ) -> Result<(), String> {
        match self {
            Self::Postgres { region, tables } => Self::query(
                shared,
                region,
                &format!(
                    "INSERT INTO {} (move_id, node, phase) VALUES ({id}, {}, {}) ON CONFLICT DO \
                     NOTHING",
                    tables.acks,
                    literal(node),
                    literal(phase.as_str())
                ),
            )
            .await
            .map(drop),
            Self::Memory { index, memory } => memory.confirm(*index, node, id, phase),
        }
    }

    /// How long ago each proxy last granted, and the confirmations of one
    /// move.
    async fn progress(&self, shared: &Shared, node: &str, id: i64) -> Result<Progress, String> {
        let (region, tables) = match self {
            Self::Postgres { region, tables } => (region, tables),
            Self::Memory { index, memory } => return memory.progress(*index, node, id),
        };
        let result = Self::query(
            shared,
            region,
            &format!(
                "SELECT 'grant'::text, node, \
                 (extract(epoch FROM clock_timestamp() - granted_at) * 1000)::bigint::text \
                 FROM {proxies} WHERE granted_at IS NOT NULL \
                 UNION ALL SELECT 'ack', node, phase FROM {acks} WHERE move_id = {id}",
                proxies = tables.proxies,
                acks = tables.acks
            ),
        )
        .await?;
        let mut grants = Vec::new();
        let mut acks = Vec::new();
        for row in &result.rows {
            let field = |n: usize| row.get(n).cloned().flatten();
            match (field(0).as_deref(), field(1), field(2)) {
                (Some("grant"), Some(node), Some(age)) => grants.push((
                    node,
                    age.parse::<i64>()
                        .ok()
                        .and_then(|age| u64::try_from(age).ok())
                        .unwrap_or(0),
                )),
                (Some("ack"), Some(node), Some(phase)) => {
                    if let Some(phase) = Phase::parse(&phase) {
                        acks.push((node, phase));
                    }
                }
                _ => {}
            }
        }
        Ok((grants, acks))
    }

    async fn set_phase(
        &self,
        shared: &Shared,
        node: &str,
        id: i64,
        phase: Phase,
    ) -> Result<(), String> {
        match self {
            Self::Postgres { region, tables } => Self::query(
                shared,
                region,
                &format!(
                    "UPDATE {} SET phase = {} WHERE id = {id} AND NOT withdrawn",
                    tables.moves,
                    literal(phase.as_str())
                ),
            )
            .await
            .map(drop),
            Self::Memory { index, memory } => memory.set_phase(*index, node, id, phase),
        }
    }

    async fn withdraw(&self, shared: &Shared, node: &str, id: i64) -> Result<(), String> {
        match self {
            Self::Postgres { region, tables } => Self::query(
                shared,
                region,
                &format!(
                    "WITH gone AS (DELETE FROM {acks} WHERE move_id = {id}) \
                     UPDATE {moves} SET withdrawn = true WHERE id = {id}",
                    acks = tables.acks,
                    moves = tables.moves
                ),
            )
            .await
            .map(drop),
            Self::Memory { index, memory } => memory.withdraw(*index, node, id),
        }
    }

    /// Forgets a withdrawn move, once every region has its marker.
    async fn forget(&self, shared: &Shared, node: &str, id: i64) -> Result<(), String> {
        match self {
            Self::Postgres { region, tables } => Self::query(
                shared,
                region,
                &format!("DELETE FROM {} WHERE id = {id} AND withdrawn", tables.moves),
            )
            .await
            .map(drop),
            Self::Memory { index, memory } => memory.forget(*index, node, id),
        }
    }

    async fn withdraw_all(&self, shared: &Shared, node: &str) -> Result<(), String> {
        match self {
            Self::Postgres { region, tables } => Self::query(
                shared,
                region,
                &format!(
                    "WITH mine AS (UPDATE {moves} SET withdrawn = true WHERE mover = {node} \
                     AND NOT withdrawn RETURNING id) \
                     DELETE FROM {acks} WHERE move_id IN (SELECT id FROM mine)",
                    moves = tables.moves,
                    acks = tables.acks,
                    node = literal(node)
                ),
            )
            .await
            .map(drop),
            Self::Memory { index, memory } => memory.withdraw_all(*index, node),
        }
    }

    /// Directory entries, all or some keys', with their versions.
    async fn directory(
        &self,
        shared: &Shared,
        node: &str,
        keys: Option<&[String]>,
    ) -> Result<Vec<Row>, String> {
        let (region, tables) = match self {
            Self::Postgres { region, tables } => (region, tables),
            Self::Memory { index, memory } => return memory.directory(*index, node, keys),
        };
        let filter = keys.map_or_else(String::new, |keys| {
            if keys.is_empty() {
                return " WHERE false".to_owned();
            }
            let list: Vec<String> = keys.iter().map(|key| literal(key)).collect();
            format!(" WHERE shard_key IN ({})", list.join(", "))
        });
        let result = Self::query(
            shared,
            region,
            &format!(
                "SELECT table_name, shard_key, region, version::text FROM {}{filter}",
                tables.directory
            ),
        )
        .await?;
        Ok(result
            .rows
            .iter()
            .filter_map(|row| {
                let field = |n: usize| row.get(n).cloned().flatten();
                Some((
                    field(0)?,
                    field(1)?,
                    field(2)?,
                    field(3)
                        .and_then(|version| version.parse().ok())
                        .unwrap_or(0),
                ))
            })
            .collect())
    }

    /// Writes entries this region lacks or holds an older version of.
    async fn repair(&self, shared: &Shared, node: &str, rows: &[Row]) -> Result<(), String> {
        let (region, tables) = match self {
            Self::Postgres { region, tables } => (region, tables),
            Self::Memory { index, memory } => return memory.repair(*index, node, rows),
        };
        // In batches, so that a large directory is not one huge statement;
        // a row a move holds waits no longer than a move would.
        for chunk in rows.chunks(500) {
            let values: Vec<String> = chunk
                .iter()
                .map(|(table, key, region, version)| {
                    format!(
                        "({}, {}, {}, {version})",
                        literal(table),
                        literal(key),
                        literal(region)
                    )
                })
                .collect();
            Self::query(
                shared,
                region,
                &format!(
                    "SET lock_timeout = '2s'; \
                     INSERT INTO {} AS d (table_name, shard_key, region, version) VALUES {} \
                     ON CONFLICT (table_name, shard_key) DO UPDATE SET region = EXCLUDED.region, \
                     version = EXCLUDED.version WHERE d.version < EXCLUDED.version; \
                     RESET lock_timeout",
                    tables.directory,
                    values.join(", ")
                ),
            )
            .await?;
        }
        Ok(())
    }

    /// The claim of a lone authority: the first placement written wins.
    async fn claim_directly(
        &self,
        shared: &Shared,
        node: &str,
        wanted: &[(String, String, String)],
    ) -> Result<Vec<(String, String, String)>, String> {
        let (region, tables) = match self {
            Self::Postgres { region, tables } => (region, tables),
            Self::Memory { index, memory } => {
                return memory.claim_directly(*index, node, wanted);
            }
        };
        let pairs: Vec<String> = wanted
            .iter()
            .map(|(table, key, _)| format!("({}, {})", literal(table), literal(key)))
            .collect();
        // Two statements, so that the second sees a key another proxy
        // claimed while the first waited for it.
        let result = Self::query(
            shared,
            region,
            &format!(
                "INSERT INTO {directory} (table_name, shard_key, region) VALUES {values} \
                 ON CONFLICT (table_name, shard_key) DO NOTHING; \
                 SELECT table_name, shard_key, region FROM {directory} \
                 WHERE (table_name, shard_key) IN ({pairs})",
                directory = tables.directory,
                values = values_of(wanted),
                pairs = pairs.join(", ")
            ),
        )
        .await?;
        Ok(result
            .rows
            .iter()
            .filter_map(|row| match (row.first(), row.get(1), row.get(2)) {
                (Some(Some(table)), Some(Some(key)), Some(Some(region))) => {
                    Some((table.clone(), key.clone(), region.clone()))
                }
                _ => None,
            })
            .collect())
    }

    /// Paxos, phase one: promises to accept nothing below `ballot` and says
    /// what it has accepted; with it, where the region's directory already
    /// puts the key.
    async fn prepare(
        &self,
        shared: &Shared,
        node: &str,
        table: &str,
        key: &str,
        ballot: i64,
    ) -> Result<Promise, String> {
        let (region, tables) = match self {
            Self::Postgres { region, tables } => (region, tables),
            Self::Memory { index, memory } => {
                return memory.prepare(*index, node, (table, key), ballot);
            }
        };
        let result = Self::query(
            shared,
            region,
            &format!(
                "WITH promise AS (INSERT INTO {claims} AS c (table_name, shard_key, promised) \
                 VALUES ({table}, {key}, {ballot}) \
                 ON CONFLICT (table_name, shard_key) DO UPDATE SET promised = EXCLUDED.promised \
                 WHERE c.promised < EXCLUDED.promised RETURNING accepted, value) \
                 SELECT 'promise'::text, accepted::text, value FROM promise \
                 UNION ALL SELECT 'placed', region, version::text FROM {directory} \
                 WHERE table_name = {table} AND shard_key = {key}",
                claims = tables.claims,
                directory = tables.directory,
                table = literal(table),
                key = literal(key)
            ),
        )
        .await?;
        let mut promise = Promise::default();
        for row in &result.rows {
            let field = |n: usize| row.get(n).cloned().flatten();
            match field(0).as_deref() {
                Some("promise") => {
                    promise.granted = Some((
                        field(1)
                            .and_then(|accepted| accepted.parse().ok())
                            .unwrap_or(0),
                        field(2),
                    ));
                }
                Some("placed") => {
                    if let Some(region) = field(1) {
                        promise.placed = Some((
                            region,
                            field(2)
                                .and_then(|version| version.parse().ok())
                                .unwrap_or(0),
                        ));
                    }
                }
                _ => {}
            }
        }
        Ok(promise)
    }

    /// Paxos, phase two: accepts `value` unless it has promised a higher
    /// ballot since.
    async fn accept(
        &self,
        shared: &Shared,
        node: &str,
        (table, key): (&str, &str),
        ballot: i64,
        value: &str,
    ) -> Result<bool, String> {
        let (region, tables) = match self {
            Self::Postgres { region, tables } => (region, tables),
            Self::Memory { index, memory } => {
                return memory.accept(*index, node, (table, key), ballot, value);
            }
        };
        let result = Self::query(
            shared,
            region,
            &format!(
                "INSERT INTO {claims} AS c (table_name, shard_key, promised, accepted, value) \
                 VALUES ({table}, {key}, {ballot}, {ballot}, {value}) \
                 ON CONFLICT (table_name, shard_key) DO UPDATE SET promised = EXCLUDED.promised, \
                 accepted = EXCLUDED.accepted, value = EXCLUDED.value \
                 WHERE c.promised <= EXCLUDED.promised RETURNING 1",
                claims = tables.claims,
                table = literal(table),
                key = literal(key),
                value = literal(value)
            ),
        )
        .await?;
        Ok(!result.rows.is_empty())
    }
}

fn read_view(result: &QueryResult) -> RegionView {
    let mut view = RegionView::default();
    for row in &result.rows {
        let field = |n: usize| row.get(n).cloned().flatten();
        match field(0).as_deref() {
            Some("beat") => view.held = field(1).as_deref() == Some("1"),
            Some("move") => {
                if let (Some(id), Some(key), Some(to), Some(mover), Some(phase)) = (
                    field(1).and_then(|id| id.parse().ok()),
                    field(2),
                    field(3),
                    field(4),
                    field(5).as_deref().and_then(Phase::parse),
                ) {
                    view.moves.push((
                        Announced {
                            id,
                            key,
                            to,
                            mover,
                            phase,
                        },
                        field(6).as_deref() == Some("true"),
                    ));
                }
            }
            Some("peer") => {
                if let Some(node) = field(1) {
                    view.peers.push(Peer {
                        node,
                        heartbeat_age_ms: field(2)
                            .and_then(|age| age.parse::<i64>().ok())
                            .map_or(0, |age| u64::try_from(age).unwrap_or(0)),
                        summary: field(3).unwrap_or_default(),
                    });
                }
            }
            _ => {}
        }
    }
    view
}

// ------------------------------------------------------------ in memory

/// The shared state, kept in memory for proxies running in one process:
/// one copy, standing in for an authority region, or one for each region
/// of a majority.
///
/// Each copy stands in for the shared tables, including the directory
/// table: where a real move writes that table in its own transaction, a
/// move here records the key's new region in every copy when it commits.
/// [`MemoryPeers::cut_off`] makes every copy unreachable from one proxy,
/// and [`MemoryPeers::cut_off_region`] one copy, to see what a proxy does
/// without them.
#[derive(Debug)]
pub struct MemoryPeers {
    regions: Vec<parking_lot::Mutex<MemoryRegion>>,
    /// Raised by every directory change, so that newer entries win.
    version: std::sync::atomic::AtomicI64,
}

#[derive(Debug)]
struct MemoryProxy {
    instance: String,
    seen: Instant,
    granted: Option<Instant>,
    summary: String,
}

/// A Paxos acceptor: promised, accepted, and the value accepted.
type Acceptor = (i64, i64, Option<String>);

#[derive(Debug, Default)]
struct MemoryRegion {
    proxies: BTreeMap<String, MemoryProxy>,
    moves: BTreeMap<i64, (Announced, bool)>,
    confirmations: BTreeSet<(i64, String, Phase)>,
    directory: BTreeMap<(String, String), (String, i64)>,
    claims: BTreeMap<(String, String), Acceptor>,
    cut_off: BTreeSet<String>,
}

fn millis_since(then: Instant) -> u64 {
    u64::try_from(then.elapsed().as_millis()).unwrap_or(u64::MAX)
}

impl MemoryPeers {
    /// Shared state with one copy.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Self::with_regions(1)
    }

    /// Shared state with a copy for each of `count` regions, for proxies
    /// that meet in a majority.
    #[must_use]
    pub fn with_regions(count: usize) -> Arc<Self> {
        Arc::new(Self {
            regions: (0..count.max(1))
                .map(|_| parking_lot::Mutex::default())
                .collect(),
            version: std::sync::atomic::AtomicI64::new(1),
        })
    }

    fn regions(&self) -> usize {
        self.regions.len()
    }

    fn next_version(&self) -> i64 {
        self.version
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Places a key in every copy, as a directory entry written before any
    /// proxy started or by a move's transaction.
    pub fn place(&self, table: &str, key: &str, region: &str) {
        let version = self.next_version();
        for copy in &self.regions {
            copy.lock().directory.insert(
                (table.to_owned(), key.to_owned()),
                (region.to_owned(), version),
            );
        }
    }

    /// Places a key in one copy only, as an entry that only one region's
    /// table holds: recorded by a claim under an authority, say.
    pub fn place_in(&self, index: usize, table: &str, key: &str, region: &str) {
        let version = self.next_version();
        if let Some(copy) = self.regions.get(index) {
            copy.lock().directory.insert(
                (table.to_owned(), key.to_owned()),
                (region.to_owned(), version),
            );
        }
    }

    /// Where one copy puts a key.
    #[must_use]
    pub fn region_in(&self, index: usize, table: &str, key: &str) -> Option<String> {
        self.regions.get(index).and_then(|copy| {
            copy.lock()
                .directory
                .get(&(table.to_owned(), key.to_owned()))
                .map(|(region, _)| region.clone())
        })
    }

    /// Where the newest copy that has a key puts it.
    #[must_use]
    pub fn region_of(&self, table: &str, key: &str) -> Option<String> {
        self.regions
            .iter()
            .filter_map(|copy| {
                copy.lock()
                    .directory
                    .get(&(table.to_owned(), key.to_owned()))
                    .cloned()
            })
            .max_by_key(|(_, version)| *version)
            .map(|(region, _)| region)
    }

    /// How many moves some copy still shows under way.
    #[must_use]
    pub fn announced(&self) -> usize {
        let mut ids = BTreeSet::new();
        for copy in &self.regions {
            for (id, (_, withdrawn)) in &copy.lock().moves {
                if !withdrawn {
                    ids.insert(*id);
                }
            }
        }
        ids.len()
    }

    /// Makes every copy unreachable from `node`, or reachable again.
    pub fn cut_off(&self, node: &str, cut: bool) {
        for index in 0..self.regions.len() {
            self.cut_off_region(node, index, cut);
        }
    }

    /// Makes one copy unreachable from `node`, or reachable again.
    pub fn cut_off_region(&self, node: &str, index: usize, cut: bool) {
        if let Some(copy) = self.regions.get(index) {
            let mut copy = copy.lock();
            if cut {
                copy.cut_off.insert(node.to_owned());
            } else {
                copy.cut_off.remove(node);
            }
        }
    }

    fn reach(
        &self,
        index: usize,
        node: &str,
    ) -> Result<parking_lot::MutexGuard<'_, MemoryRegion>, String> {
        let copy = self
            .regions
            .get(index)
            .ok_or_else(|| format!("there is no region {index}"))?
            .lock();
        if copy.cut_off.contains(node) {
            return Err(format!("region {index} cannot be reached"));
        }
        Ok(copy)
    }

    fn beat(&self, index: usize, beat: &Beat<'_>) -> Result<RegionView, String> {
        let version = self.next_version();
        let mut copy = self.reach(index, beat.node)?;
        let now = Instant::now();
        let held = match copy.proxies.get_mut(beat.node) {
            Some(proxy)
                if proxy.instance == beat.instance || proxy.seen.elapsed() >= beat.window =>
            {
                beat.instance.clone_into(&mut proxy.instance);
                proxy.seen = now;
                if beat.grant {
                    proxy.granted = Some(now);
                }
                beat.summary.clone_into(&mut proxy.summary);
                true
            }
            Some(_) => false,
            None => {
                copy.proxies.insert(
                    beat.node.to_owned(),
                    MemoryProxy {
                        instance: beat.instance.to_owned(),
                        seen: now,
                        granted: beat.grant.then_some(now),
                        summary: beat.summary.to_owned(),
                    },
                );
                true
            }
        };
        for (table, key, region) in beat.learn {
            copy.directory
                .entry((table.clone(), key.clone()))
                .or_insert_with(|| (region.clone(), version));
        }
        Ok(RegionView {
            held,
            moves: copy.moves.values().cloned().collect(),
            peers: copy
                .proxies
                .iter()
                .map(|(node, proxy)| Peer {
                    node: node.clone(),
                    heartbeat_age_ms: millis_since(proxy.seen),
                    summary: proxy.summary.clone(),
                })
                .collect(),
        })
    }

    fn confirm(&self, index: usize, node: &str, id: i64, phase: Phase) -> Result<(), String> {
        self.reach(index, node)?
            .confirmations
            .insert((id, node.to_owned(), phase));
        Ok(())
    }

    fn announce(
        &self,
        index: usize,
        node: &str,
        id: i64,
        key: &str,
        to: &str,
    ) -> Result<Announce, String> {
        let mut copy = self.reach(index, node)?;
        if let Some((existing, _)) = copy
            .moves
            .values()
            .find(|(move_, withdrawn)| move_.key == key && !withdrawn && move_.id != id)
        {
            return Ok(Announce::Taken(existing.mover.clone()));
        }
        copy.moves.entry(id).or_insert_with(|| {
            (
                Announced {
                    id,
                    key: key.to_owned(),
                    to: to.to_owned(),
                    mover: node.to_owned(),
                    phase: Phase::Cutover,
                },
                false,
            )
        });
        Ok(Announce::Added)
    }

    fn progress(&self, index: usize, node: &str, id: i64) -> Result<Progress, String> {
        let copy = self.reach(index, node)?;
        Ok((
            copy.proxies
                .iter()
                .filter_map(|(node, proxy)| {
                    proxy
                        .granted
                        .map(|granted| (node.clone(), millis_since(granted)))
                })
                .collect(),
            copy.confirmations
                .iter()
                .filter(|(move_id, _, _)| *move_id == id)
                .map(|(_, node, phase)| (node.clone(), *phase))
                .collect(),
        ))
    }

    fn set_phase(&self, index: usize, node: &str, id: i64, phase: Phase) -> Result<(), String> {
        if let Some((move_, withdrawn)) = self.reach(index, node)?.moves.get_mut(&id) {
            if !*withdrawn {
                move_.phase = phase;
            }
        }
        Ok(())
    }

    fn withdraw(&self, index: usize, node: &str, id: i64) -> Result<(), String> {
        let mut copy = self.reach(index, node)?;
        if let Some((_, withdrawn)) = copy.moves.get_mut(&id) {
            *withdrawn = true;
        }
        copy.confirmations.retain(|(move_id, _, _)| *move_id != id);
        Ok(())
    }

    fn forget(&self, index: usize, node: &str, id: i64) -> Result<(), String> {
        let mut copy = self.reach(index, node)?;
        if copy.moves.get(&id).is_some_and(|(_, withdrawn)| *withdrawn) {
            copy.moves.remove(&id);
        }
        Ok(())
    }

    fn withdraw_all(&self, index: usize, node: &str) -> Result<(), String> {
        let mut copy = self.reach(index, node)?;
        let mine: Vec<i64> = copy
            .moves
            .values()
            .filter(|(move_, withdrawn)| move_.mover == node && !withdrawn)
            .map(|(move_, _)| move_.id)
            .collect();
        for id in mine {
            if let Some((_, withdrawn)) = copy.moves.get_mut(&id) {
                *withdrawn = true;
            }
            copy.confirmations.retain(|(move_id, _, _)| *move_id != id);
        }
        Ok(())
    }

    fn directory(
        &self,
        index: usize,
        node: &str,
        keys: Option<&[String]>,
    ) -> Result<Vec<Row>, String> {
        Ok(self
            .reach(index, node)?
            .directory
            .iter()
            .filter(|((_, held), _)| keys.is_none_or(|keys| keys.contains(held)))
            .map(|((table, key), (region, version))| {
                (table.clone(), key.clone(), region.clone(), *version)
            })
            .collect())
    }

    fn claim_directly(
        &self,
        index: usize,
        node: &str,
        wanted: &[(String, String, String)],
    ) -> Result<Vec<(String, String, String)>, String> {
        let version = self.next_version();
        let mut copy = self.reach(index, node)?;
        Ok(wanted
            .iter()
            .map(|(table, key, region)| {
                let placed = copy
                    .directory
                    .entry((table.clone(), key.clone()))
                    .or_insert_with(|| (region.clone(), version))
                    .0
                    .clone();
                (table.clone(), key.clone(), placed)
            })
            .collect())
    }

    fn repair(&self, index: usize, node: &str, rows: &[Row]) -> Result<(), String> {
        let mut copy = self.reach(index, node)?;
        for (table, key, region, version) in rows {
            let entry = copy
                .directory
                .entry((table.clone(), key.clone()))
                .or_insert_with(|| (region.clone(), *version));
            if entry.1 < *version {
                *entry = (region.clone(), *version);
            }
        }
        Ok(())
    }

    fn prepare(
        &self,
        index: usize,
        node: &str,
        (table, key): (&str, &str),
        ballot: i64,
    ) -> Result<Promise, String> {
        let mut copy = self.reach(index, node)?;
        let name = (table.to_owned(), key.to_owned());
        let placed = copy.directory.get(&name).cloned();
        let acceptor = copy.claims.entry(name).or_insert((0, 0, None));
        let granted = (acceptor.0 < ballot).then(|| {
            acceptor.0 = ballot;
            (acceptor.1, acceptor.2.clone())
        });
        Ok(Promise { granted, placed })
    }

    fn accept(
        &self,
        index: usize,
        node: &str,
        (table, key): (&str, &str),
        ballot: i64,
        value: &str,
    ) -> Result<bool, String> {
        let mut copy = self.reach(index, node)?;
        let acceptor = copy
            .claims
            .entry((table.to_owned(), key.to_owned()))
            .or_insert((0, 0, None));
        if acceptor.0 > ballot {
            return Ok(false);
        }
        *acceptor = (ballot, ballot, Some(value.to_owned()));
        Ok(true)
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
        assert_eq!(unquoted("\"ops\".\"keys_moves\""), "keys_moves");
    }

    #[test]
    fn a_region_view_is_read_from_one_result() {
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
                    Some("true".to_owned()),
                ],
                vec![
                    Some("peer".to_owned()),
                    Some("proxy-b".to_owned()),
                    Some("120".to_owned()),
                ],
            ],
            ..QueryResult::default()
        };
        let view = read_view(&result);
        assert!(view.held);
        assert_eq!(
            view.moves,
            [(
                Announced {
                    id: 7,
                    key: "alice".to_owned(),
                    to: "us-east".to_owned(),
                    mover: "proxy-a".to_owned(),
                    phase: Phase::Committing,
                },
                true
            )]
        );
        assert_eq!(
            view.peers,
            [Peer {
                node: "proxy-b".to_owned(),
                heartbeat_age_ms: 120,
                summary: String::new(),
            }]
        );
    }

    fn move_(id: i64, phase: Phase) -> Announced {
        Announced {
            id,
            key: "alice".to_owned(),
            to: "us-east".to_owned(),
            mover: "a".to_owned(),
            phase,
        }
    }

    fn peer(age: u64, summary: &str) -> Peer {
        Peer {
            node: "b".to_owned(),
            heartbeat_age_ms: age,
            summary: summary.to_owned(),
        }
    }

    #[test]
    fn regions_are_merged_so_a_withdrawal_or_a_phase_anywhere_counts() {
        let views = vec![
            RegionView {
                held: true,
                moves: vec![
                    (move_(1, Phase::Cutover), false),
                    (move_(2, Phase::Cutover), false),
                ],
                peers: vec![peer(900, "old")],
            },
            RegionView {
                held: true,
                // This region saw move 1 withdrawn and move 2 committing.
                moves: vec![
                    (move_(1, Phase::Cutover), true),
                    (move_(2, Phase::Committing), false),
                ],
                peers: vec![peer(100, "new")],
            },
        ];
        let visit = merge(views);
        assert!(visit.current);
        assert_eq!(visit.moves, [move_(2, Phase::Committing)]);
        assert_eq!(visit.peers, [peer(100, "new")]);

        let taken = merge(vec![
            RegionView {
                held: true,
                ..RegionView::default()
            },
            RegionView::default(),
        ]);
        assert!(
            !taken.current,
            "one region gave the name to another process"
        );
    }

    #[test]
    fn the_timings_and_the_authority_are_checked() {
        let regions = ["eu".to_owned(), "us".to_owned(), "ap".to_owned()];
        let mut store = DirectoryStore {
            shared: true,
            lease_ms: 300,
            poll_ms: 200,
            ..DirectoryStore::default()
        };
        assert!(Peers::new(&store, "a", "eu", &regions, None).is_err());
        store.lease_ms = 400;
        let peers = Peers::new(&store, "a", "eu", &regions, None).unwrap();
        assert_eq!(peers.quorum(), 1, "one authority");
        assert!(peers.must_wait_for(&["eu".to_owned()]));
        assert!(!peers.must_wait_for(&["us".to_owned()]));
        store.authority = Some("mars".to_owned());
        assert!(Peers::new(&store, "a", "eu", &regions, None).is_err());

        store.authority = Some(MAJORITY.to_owned());
        assert!(
            Peers::new(&store, "a", "eu", &regions, None).is_err(),
            "a majority applies a lease a visit late, so it needs three polls"
        );
        store.lease_ms = 600;
        let peers = Peers::new(&store, "a", "eu", &regions, None).unwrap();
        assert_eq!(peers.quorum(), 2, "two of three");
        assert!(!peers.must_wait_for(&["ap".to_owned()]), "two have it");
        assert!(peers.must_wait_for(&["ap".to_owned(), "us".to_owned()]));
        assert!(peers.meeting().contains("majority of the 3 regions"));
    }

    fn beat(instance: &str, window: Duration, grant: bool) -> Beat<'_> {
        Beat {
            node: "a",
            instance,
            summary: "",
            window,
            grant,
            learn: &[],
        }
    }

    #[test]
    fn a_name_in_use_by_a_live_process_cannot_be_taken() {
        let memory = MemoryPeers::new();
        let window = Duration::from_secs(5);
        assert!(memory.beat(0, &beat("one", window, true)).unwrap().held);
        assert!(!memory.beat(0, &beat("two", window, true)).unwrap().held);
        assert!(memory.beat(0, &beat("one", window, true)).unwrap().held);
        assert!(
            memory
                .beat(0, &beat("two", Duration::ZERO, true))
                .unwrap()
                .held
        );
        assert!(
            !memory.beat(0, &beat("one", window, true)).unwrap().held,
            "taken over"
        );
    }

    #[test]
    fn only_a_granting_heartbeat_makes_a_mover_wait() {
        let memory = MemoryPeers::with_regions(3);
        let window = Duration::from_secs(5);
        // Seen, but not granting: its visits are not reaching a majority.
        memory.beat(2, &beat("one", window, false)).unwrap();
        let (grants, _) = memory.progress(2, "mover", 1).unwrap();
        assert!(grants.is_empty());
        memory.beat(2, &beat("one", window, true)).unwrap();
        let (grants, _) = memory.progress(2, "mover", 1).unwrap();
        assert_eq!(grants.len(), 1);
    }

    #[test]
    fn paxos_keeps_the_first_value_a_majority_accepted() {
        let memory = MemoryPeers::with_regions(3);
        let carol = ("users", "carol");
        // One proposer gets acceptors 0 and 1 to accept us-east.
        for index in [0, 1] {
            assert_eq!(
                memory.prepare(index, "a", carol, 10).unwrap().granted,
                Some((0, None))
            );
            assert!(memory.accept(index, "a", carol, 10, "us-east").unwrap());
        }
        // A later proposer learns it from any majority, which includes one
        // of them, and must propose it again.
        let promises: Vec<_> = [1, 2]
            .iter()
            .map(|index| memory.prepare(*index, "b", carol, 20).unwrap().granted)
            .collect();
        assert!(promises.contains(&Some((10, Some("us-east".to_owned())))));
        // The earlier ballot is refused from now on.
        assert_eq!(memory.prepare(2, "a", carol, 15).unwrap().granted, None);
        assert!(!memory.accept(1, "a", carol, 15, "eu-west").unwrap());
    }

    #[test]
    fn a_prepare_reports_where_the_directory_already_puts_the_key() {
        let memory = MemoryPeers::with_regions(3);
        memory.place("users", "carol", "ap-south");
        let promise = memory.prepare(1, "a", ("users", "carol"), 10).unwrap();
        assert_eq!(
            promise.placed.map(|(region, _)| region).as_deref(),
            Some("ap-south")
        );
    }
}
