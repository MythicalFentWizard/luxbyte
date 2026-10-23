//! Which backends are up, how far each replica lags, and which replica may
//! serve a read.
//!
//! Every endpoint is probed on its own connection, named `luxbyte health`
//! so it is easy to tell apart in `pg_stat_activity`. A probe asks whether
//! the server is in recovery and for its WAL position: a primary's write
//! position, a replica's replay position. From those:
//!
//! - **Lag.** A replica further behind its primary than
//!   `max_replica_lag_bytes` serves no reads; they go to the primary.
//! - **Read your writes.** After a session writes in a region, its reads
//!   there go to the primary until a replica is known to have replayed past
//!   the write: a probe of the primary taken after the write gives a
//!   position at or past it, and a replica whose replay position has
//!   reached that has the write. No extra query is sent for this.
//! - **Up and down.** Three failed probes in a row mark an endpoint down,
//!   and a connection failure on the data path marks it down at once; two
//!   good probes bring it back. A replica that is down serves no reads.
//!
//! The proxy observes and never promotes: a primary that reports being in
//! recovery, or a replica that reports not being, is logged and shown, and
//! the topology is left for an operator to correct.
//!
//! Choosing the primary when no replica qualifies keeps reads correct at
//! the cost of the primary's load. A stale replica is never chosen because
//! the primary is down: a read that fails says so, where a stale one would
//! look like an answer.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use luxbyte_core::topology::{Endpoint, Region};
use luxbyte_pool::{BackendTls, Connection, ConnectionTarget, Credentials, PoolError};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Failed probes in a row that mark an endpoint down.
const FAILURES_TO_DOWN: u32 = 3;

/// Good probes in a row that bring a down endpoint back.
const SUCCESSES_TO_UP: u32 = 2;

/// What a probe asks. One round trip, no table access.
const PROBE: &str = "SELECT pg_is_in_recovery()::text, \
                     ((CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() \
                     ELSE pg_current_wal_lsn() END) - '0/0'::pg_lsn)::text";

/// The `application_name` probes connect with.
pub const PROBE_APPLICATION: &str = "luxbyte health";

/// The `[health]` section.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct HealthConfig {
    /// Milliseconds between probes. Zero turns probing off, and with it
    /// lag checks and read-your-writes.
    pub interval_ms: u64,
    /// Milliseconds a probe may take, connecting included.
    pub timeout_ms: u64,
    /// The furthest behind its primary a replica may be and still serve
    /// reads, in bytes of WAL.
    pub max_replica_lag_bytes: u64,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            interval_ms: 2_000,
            timeout_ms: 2_000,
            max_replica_lag_bytes: 16 * 1024 * 1024,
        }
    }
}

/// How an endpoint is doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Answering.
    Up,
    /// A probe failed; still used.
    Suspect,
    /// Not used.
    Down,
    /// Answering again after being down; not used until it has a while.
    Recovering,
}

impl State {
    /// Its name, for the console and metrics.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Up => "up",
            Self::Suspect => "suspect",
            Self::Down => "down",
            Self::Recovering => "recovering",
        }
    }

    const fn usable(self) -> bool {
        matches!(self, Self::Up | Self::Suspect)
    }
}

/// What is known about one endpoint.
#[derive(Debug, Clone)]
pub struct Status {
    /// The region it serves.
    pub region: String,
    /// Where it is.
    pub endpoint: String,
    /// Whether the topology calls it a replica.
    pub replica: bool,
    /// How it is doing.
    pub state: State,
    failures: u32,
    successes: u32,
    /// Why the last probe failed, if it did.
    pub last_error: Option<String>,
    /// Its WAL position, and when that was seen.
    pub position: Option<(u64, Instant)>,
    /// Whether it says it is in recovery.
    pub in_recovery: Option<bool>,
    /// How long the last good probe took.
    pub probe_time: Option<Duration>,
    /// Taken out of service by an operator: serves no new reads.
    pub drained: bool,
}

impl Status {
    /// Whether it plays the role the topology gives it, as far as is known.
    #[must_use]
    pub fn role_matches(&self) -> bool {
        self.in_recovery
            .is_none_or(|recovering| recovering == self.replica)
    }
}

/// A session's last write in a region, and the WAL position a replica has
/// to have replayed to show it.
#[derive(Debug, Clone, Copy)]
pub struct Wrote {
    at: Instant,
    /// The primary's position at its first probe after the write, once
    /// there has been one: at or past the write.
    target: Option<u64>,
}

impl Wrote {
    /// A write that has just committed.
    #[must_use]
    pub fn now() -> Self {
        Self {
            at: Instant::now(),
            target: None,
        }
    }
}

/// What a replica must have replayed to serve a session's read.
#[derive(Debug, Clone, Copy)]
enum Needs {
    /// The session has not written there.
    Nothing,
    /// It has, and no probe since has said how far that reached.
    Unknown,
    /// This position, or later.
    Position(u64),
}

/// The health of every endpoint.
#[derive(Debug)]
pub struct Health {
    config: HealthConfig,
    endpoints: RwLock<BTreeMap<String, Status>>,
    kept_off_replicas: AtomicU64,
    failovers: AtomicU64,
}

/// An endpoint's key: `host:port`.
#[must_use]
pub fn key(endpoint: &Endpoint) -> String {
    format!("{}:{}", endpoint.host, endpoint.port)
}

impl Health {
    /// Health for every endpoint of `regions`, all presumed up until a
    /// probe says otherwise.
    #[must_use]
    pub fn new(config: HealthConfig, regions: &[Region]) -> Self {
        let mut endpoints = BTreeMap::new();
        for region in regions {
            let all = std::iter::once((&region.primary, false))
                .chain(region.replicas.iter().map(|replica| (replica, true)));
            for (endpoint, replica) in all {
                endpoints.insert(
                    key(endpoint),
                    Status {
                        region: region.id.clone(),
                        endpoint: key(endpoint),
                        replica,
                        state: State::Up,
                        failures: 0,
                        successes: 0,
                        last_error: None,
                        position: None,
                        in_recovery: None,
                        probe_time: None,
                        drained: false,
                    },
                );
            }
        }
        Self {
            config,
            endpoints: RwLock::new(endpoints),
            kept_off_replicas: AtomicU64::new(0),
            failovers: AtomicU64::new(0),
        }
    }

    /// Whether probing is on.
    #[must_use]
    pub const fn probing(&self) -> bool {
        self.config.interval_ms > 0
    }

    /// The furthest a replica may lag and still serve reads.
    #[must_use]
    pub const fn max_replica_lag_bytes(&self) -> u64 {
        self.config.max_replica_lag_bytes
    }

    /// The endpoint a read in `region` should use, given when this session
    /// last wrote there: the `nth` replica of those that qualify, or the
    /// primary when none does.
    pub fn read_endpoint<'a>(
        &self,
        region: &'a Region,
        nth: usize,
        wrote: Option<&mut Wrote>,
    ) -> &'a Endpoint {
        if region.replicas.is_empty() {
            return &region.primary;
        }
        let endpoints = self.endpoints.read();
        if !self.probing() {
            // Without probes nothing is known about lag, so every replica an
            // operator has not drained takes its turn.
            let serving: Vec<&Endpoint> = region
                .replicas
                .iter()
                .filter(|replica| {
                    endpoints
                        .get(&key(replica))
                        .is_none_or(|status| !status.drained)
                })
                .collect();
            let index = nth.checked_rem(serving.len().max(1)).unwrap_or(0);
            return serving.get(index).copied().unwrap_or(&region.primary);
        }
        let primary = endpoints.get(&key(&region.primary));
        let needs = match wrote {
            None => Needs::Nothing,
            Some(wrote) => {
                if wrote.target.is_none() {
                    wrote.target = primary
                        .and_then(|primary| primary.position)
                        .filter(|(_, seen)| *seen > wrote.at)
                        .map(|(position, _)| position);
                }
                wrote.target.map_or(Needs::Unknown, Needs::Position)
            }
        };
        let qualifying: Vec<&Endpoint> = region
            .replicas
            .iter()
            .filter(|replica| {
                endpoints
                    .get(&key(replica))
                    .is_some_and(|status| self.serves_reads(primary, status, needs))
            })
            .collect();
        drop(endpoints);
        if qualifying.is_empty() {
            self.kept_off_replicas.fetch_add(1, Ordering::Relaxed);
            return &region.primary;
        }
        let index = nth.checked_rem(qualifying.len()).unwrap_or(0);
        qualifying.get(index).copied().unwrap_or(&region.primary)
    }

    fn serves_reads(&self, primary: Option<&Status>, replica: &Status, needs: Needs) -> bool {
        if !replica.state.usable() || replica.drained {
            return false;
        }
        let Some((here, _)) = replica.position else {
            return false;
        };
        let Some((ahead, _)) = primary.and_then(|primary| primary.position) else {
            return false;
        };
        if ahead.saturating_sub(here) > self.config.max_replica_lag_bytes {
            return false;
        }
        match needs {
            Needs::Nothing => true,
            Needs::Unknown => false,
            // Positions only grow, so a replica that has replayed past a
            // position read after the write has the write.
            Needs::Position(target) => here >= target,
        }
    }

    /// Records that the data path could not reach an endpoint, which marks
    /// it down at once rather than after three probes.
    pub fn failed(&self, endpoint: &str, error: &PoolError) {
        let mut endpoints = self.endpoints.write();
        let Some(status) = endpoints.get_mut(endpoint) else {
            return;
        };
        if status.state != State::Down {
            tracing::warn!(endpoint, %error, "marking an endpoint down after a failed connection");
        }
        status.state = State::Down;
        status.failures = FAILURES_TO_DOWN;
        status.successes = 0;
        status.last_error = Some(error.to_string());
    }

    /// Takes a replica out of service, or puts it back. A primary cannot be
    /// drained: there is nowhere else for its writes to go.
    pub fn set_drained(&self, endpoint: &str, drained: bool) -> Result<(), String> {
        let mut endpoints = self.endpoints.write();
        let status = endpoints
            .get_mut(endpoint)
            .ok_or_else(|| format!("no backend is configured at {endpoint}"))?;
        if !status.replica {
            return Err(format!(
                "{endpoint} is a primary, and its writes have nowhere else to go"
            ));
        }
        status.drained = drained;
        Ok(())
    }

    /// Counts a read that went to the primary after its replica failed.
    pub fn failed_over(&self) {
        self.failovers.fetch_add(1, Ordering::Relaxed);
    }

    fn record(&self, endpoint: &str, outcome: Result<(bool, Option<u64>, Duration), String>) {
        let mut endpoints = self.endpoints.write();
        let Some(status) = endpoints.get_mut(endpoint) else {
            return;
        };
        let before = status.state;
        let matched = status.role_matches();
        match outcome {
            Ok((in_recovery, position, took)) => {
                status.in_recovery = Some(in_recovery);
                status.position = position.map(|position| (position, Instant::now()));
                status.probe_time = Some(took);
                status.last_error = None;
                status.failures = 0;
                status.successes = status.successes.saturating_add(1);
                status.state = match status.state {
                    State::Up | State::Suspect => State::Up,
                    State::Down | State::Recovering if status.successes >= SUCCESSES_TO_UP => {
                        State::Up
                    }
                    State::Down | State::Recovering => State::Recovering,
                };
            }
            Err(error) => {
                status.last_error = Some(error);
                status.successes = 0;
                status.failures = status.failures.saturating_add(1);
                status.state = if status.failures >= FAILURES_TO_DOWN || status.state == State::Down
                {
                    State::Down
                } else {
                    State::Suspect
                };
            }
        }
        if status.state != before {
            tracing::info!(
                endpoint,
                from = before.name(),
                to = status.state.name(),
                error = status.last_error.as_deref().unwrap_or(""),
                "endpoint state changed"
            );
        }
        if matched && !status.role_matches() {
            tracing::warn!(
                endpoint,
                configured = if status.replica { "replica" } else { "primary" },
                "this endpoint no longer plays the role the topology gives it; a promotion or \
                 demotion happened outside the proxy, which observes and never promotes"
            );
        }
    }

    /// Every endpoint, in order.
    #[must_use]
    pub fn snapshot(&self) -> Vec<Status> {
        self.endpoints.read().values().cloned().collect()
    }

    /// How far a replica is behind its primary, in bytes of WAL.
    #[must_use]
    pub fn lag(&self, region: &Region, replica: &Endpoint) -> Option<u64> {
        let endpoints = self.endpoints.read();
        let (ahead, _) = endpoints.get(&key(&region.primary))?.position?;
        let (here, _) = endpoints.get(&key(replica))?.position?;
        Some(ahead.saturating_sub(here))
    }

    /// Reads sent to a primary because no replica qualified.
    #[must_use]
    pub fn kept_off_replicas(&self) -> u64 {
        self.kept_off_replicas.load(Ordering::Relaxed)
    }

    /// Reads that went to a primary after their replica failed.
    #[must_use]
    pub fn failovers(&self) -> u64 {
        self.failovers.load(Ordering::Relaxed)
    }
}

/// Reads `DRAIN <host:port>` and `UNDRAIN <host:port>`: whether to drain,
/// and the endpoint. `None` if the statement is something else.
#[must_use]
pub fn parse_drain(sql: &str) -> Option<Result<(bool, String), String>> {
    let text = sql.trim().trim_end_matches(';').trim();
    let mut words = text.split_whitespace();
    let drain = match words.next()?.to_ascii_uppercase().as_str() {
        "DRAIN" => true,
        "UNDRAIN" => false,
        _ => return None,
    };
    Some(match (words.next(), words.next()) {
        (Some(endpoint), None) => Ok((drain, endpoint.trim_matches('\'').to_owned())),
        _ => Err("say it as: DRAIN <host:port> or UNDRAIN <host:port>".to_owned()),
    })
}

/// One endpoint's probe and the connection it keeps.
struct Probe {
    endpoint: String,
    target: ConnectionTarget,
    connection: Option<Connection>,
}

/// Probes every endpoint, on connections of its own.
pub struct Prober {
    probes: Vec<Probe>,
    credentials: Credentials,
    tls: BackendTls,
    timeout: Duration,
    interval: Duration,
}

impl std::fmt::Debug for Prober {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Prober")
            .field("endpoints", &self.probes.len())
            .field("interval", &self.interval)
            .finish_non_exhaustive()
    }
}

impl Prober {
    /// A prober for every endpoint of `regions`.
    #[must_use]
    pub fn new(
        config: &HealthConfig,
        regions: &[Region],
        database: &str,
        user: &str,
        credentials: Credentials,
        tls: BackendTls,
    ) -> Self {
        let probes = regions
            .iter()
            .flat_map(|region| std::iter::once(&region.primary).chain(&region.replicas))
            .map(|endpoint| Probe {
                endpoint: key(endpoint),
                target: ConnectionTarget::new(endpoint, database, user),
                connection: None,
            })
            .collect();
        Self {
            probes,
            credentials,
            tls,
            timeout: Duration::from_millis(config.timeout_ms),
            interval: Duration::from_millis(config.interval_ms),
        }
    }

    /// Probes every endpoint once, all at the same time.
    pub async fn round(&mut self, health: &Health) {
        let mut tasks = tokio::task::JoinSet::new();
        for mut probe in std::mem::take(&mut self.probes) {
            let credentials = self.credentials.clone();
            let tls = self.tls.clone();
            let timeout = self.timeout;
            tasks.spawn(async move {
                let outcome = tokio::time::timeout(
                    timeout,
                    probe_once(&mut probe, &credentials, &tls, timeout),
                )
                .await
                .unwrap_or_else(|_| Err(format!("no answer within {timeout:?}")));
                if outcome.is_err() {
                    probe.connection = None;
                }
                (probe, outcome)
            });
        }
        while let Some(joined) = tasks.join_next().await {
            if let Ok((probe, outcome)) = joined {
                health.record(&probe.endpoint, outcome);
                self.probes.push(probe);
            }
        }
    }

    /// Probes forever.
    pub async fn run(mut self, shared: std::sync::Arc<crate::session::Shared>) {
        loop {
            tokio::time::sleep(self.interval).await;
            self.round(&shared.health).await;
        }
    }
}

async fn probe_once(
    probe: &mut Probe,
    credentials: &Credentials,
    tls: &BackendTls,
    timeout: Duration,
) -> Result<(bool, Option<u64>, Duration), String> {
    let started = Instant::now();
    if probe.connection.is_none() {
        probe.connection = Some(
            Connection::connect_named(&probe.target, credentials, timeout, tls, PROBE_APPLICATION)
                .await
                .map_err(|error| error.to_string())?,
        );
    }
    let connection = probe
        .connection
        .as_mut()
        .ok_or_else(|| "no connection".to_owned())?;
    let result = connection
        .simple_query(PROBE)
        .await
        .map_err(|error| error.to_string())?;
    let row = result
        .rows
        .first()
        .ok_or_else(|| "the probe returned no row".to_owned())?;
    // `boolean::text` is `true` or `false`, not the `t` or `f` of the
    // boolean's own output.
    let in_recovery = matches!(
        row.first().cloned().flatten().as_deref(),
        Some("true" | "t")
    );
    // A replica that has not replayed anything yet has no position.
    let position = row
        .get(1)
        .cloned()
        .flatten()
        .and_then(|text| text.split('.').next().and_then(|whole| whole.parse().ok()));
    Ok((in_recovery, position, started.elapsed()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn region() -> Region {
        luxbyte_core::Topology::from_toml(
            r#"
home_region = "eu"

[[regions]]
id = "eu"
primary = { host = "p", port = 1 }
replicas = [{ host = "r1", port = 1 }, { host = "r2", port = 1 }]
"#,
        )
        .unwrap()
        .regions
        .remove(0)
    }

    fn health() -> Health {
        Health::new(HealthConfig::default(), &[region()])
    }

    fn at(health: &Health, endpoint: &str, position: u64) {
        health.record(
            endpoint,
            Ok((endpoint != "p:1", Some(position), Duration::ZERO)),
        );
    }

    #[test]
    fn a_replica_with_no_probe_yet_serves_no_reads() {
        let region = region();
        assert_eq!(health().read_endpoint(&region, 0, None).host, "p");
    }

    #[test]
    fn a_replica_too_far_behind_serves_no_reads() {
        let region = region();
        let health = health();
        at(&health, "p:1", 100 * 1024 * 1024);
        at(&health, "r1:1", 100 * 1024 * 1024 - 10);
        at(&health, "r2:1", 1024);
        for nth in 0..4 {
            assert_eq!(health.read_endpoint(&region, nth, None).host, "r1");
        }
        assert_eq!(
            health.lag(&region, &region.replicas[1]),
            Some(100 * 1024 * 1024 - 1024)
        );
    }

    #[test]
    fn after_a_write_reads_wait_for_a_replica_that_has_replayed_it() {
        let region = region();
        let health = health();
        at(&health, "p:1", 500);
        at(&health, "r1:1", 500);
        at(&health, "r2:1", 500);
        let mut wrote = Wrote::now();
        std::thread::sleep(Duration::from_millis(2));
        // Nothing has been probed since the write.
        assert_eq!(health.read_endpoint(&region, 0, Some(&mut wrote)).host, "p");
        at(&health, "p:1", 900);
        at(&health, "r1:1", 700);
        at(&health, "r2:1", 900);
        assert_eq!(
            health.read_endpoint(&region, 0, Some(&mut wrote)).host,
            "r2"
        );
        assert_eq!(
            health.read_endpoint(&region, 1, Some(&mut wrote)).host,
            "r2"
        );
        // The primary moving on does not raise the bar for this write.
        at(&health, "p:1", 1_200);
        at(&health, "r1:1", 950);
        assert_eq!(
            health.read_endpoint(&region, 0, Some(&mut wrote)).host,
            "r1"
        );
    }

    #[test]
    fn three_failed_probes_mark_an_endpoint_down_and_two_good_ones_bring_it_back() {
        let region = region();
        let health = health();
        at(&health, "p:1", 10);
        at(&health, "r1:1", 10);
        for _ in 0..3 {
            health.record("r1:1", Err("refused".to_owned()));
        }
        assert_eq!(health.snapshot()[1].state, State::Down);
        assert_eq!(health.read_endpoint(&region, 0, None).host, "p");
        at(&health, "r1:1", 10);
        assert_eq!(health.snapshot()[1].state, State::Recovering);
        at(&health, "r1:1", 10);
        assert_eq!(health.snapshot()[1].state, State::Up);
        assert_eq!(health.read_endpoint(&region, 0, None).host, "r1");
    }

    #[test]
    fn a_role_that_does_not_match_the_topology_is_noticed() {
        let health = health();
        health.record("p:1", Ok((true, Some(1), Duration::ZERO)));
        assert!(!health.snapshot()[0].role_matches());
    }
}
