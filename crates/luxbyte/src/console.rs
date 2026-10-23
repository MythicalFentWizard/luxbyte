//! Commands the proxy answers itself.
//!
//! An operator needs to see what the router is thinking, and the only tool
//! guaranteed to be on the machine is `psql`. So the answers come back as
//! ordinary result sets over the same connection: no second port, no
//! separate client, no dashboard to deploy.
//!
//! Every command here is read-only. Moving a key is a change to data and
//! belongs in a path with the same care as a write, not in a convenience
//! console.

use luxbyte_core::directory::{Directory, KeyState, Location, RelocationPhase};
use luxbyte_core::topology::Placement;
use luxbyte_pool::QueryResult;

use crate::session::Shared;

/// Builds a result set from string columns and rows.
fn table(columns: &[&str], rows: Vec<Vec<String>>) -> QueryResult {
    let count = rows.len();
    QueryResult {
        columns: columns.iter().map(|name| (*name).to_owned()).collect(),
        // 25 is `text`; everything the console reports is text.
        type_oids: vec![25; columns.len()],
        rows: rows
            .into_iter()
            .map(|row| row.into_iter().map(Some).collect())
            .collect(),
        tags: vec![format!("SELECT {count}")],
        status: luxbyte_proto::backend::TransactionStatus::Idle,
    }
}

/// Normalises a statement for matching: collapsed whitespace, upper case,
/// no trailing semicolon.
fn normalise(sql: &str) -> String {
    sql.trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_uppercase()
}

/// Answers a console command, or returns `None` if this is ordinary SQL.
#[must_use]
pub fn handle(shared: &Shared, sql: &str) -> Option<QueryResult> {
    let command = normalise(sql);

    match command.as_str() {
        "SHOW REGIONS" => Some(show_regions(shared)),
        "SHOW PLACEMENT" => Some(show_placement(shared)),
        "SHOW POOLS" => Some(show_pools(shared)),
        "SHOW DIRECTORY" => Some(show_directory(shared)),
        "SHOW IN DOUBT" => Some(show_in_doubt(shared)),
        "SHOW HEALTH" => Some(show_health(shared)),
        "SHOW PEERS" => Some(show_peers(shared)),
        "SHOW ALERTS" => Some(table(
            &["alert", "subject", "detail"],
            crate::alerts::current(shared)
                .into_iter()
                .map(|alert| vec![alert.name.to_owned(), alert.subject, alert.detail])
                .collect(),
        )),
        _ if command.starts_with("ROUTE ") => {
            let original = sql.trim();
            let statement = original.get("ROUTE".len()..).unwrap_or_default();
            Some(route(shared, statement))
        }
        _ => {
            // `WHERE IS <table> <key>` takes arguments, so it cannot be
            // matched as a whole string.
            command.strip_prefix("WHERE IS ")?;
            let original = sql.trim().trim_end_matches(';');
            let arguments: Vec<&str> = original.split_whitespace().skip(2).collect();
            match arguments.as_slice() {
                [table, key] => Some(where_is(shared, table, key)),
                _ => Some(table_error(
                    "WHERE IS needs a table and a key, as in: WHERE IS users alice",
                )),
            }
        }
    }
}

/// Where a statement would run, with the live directory, without running
/// it.
fn route(shared: &Shared, sql: &str) -> QueryResult {
    let analyses = match luxbyte_sql::analyze(sql) {
        Ok(analyses) => analyses,
        Err(error) => return table_error(&error.to_string()),
    };
    let texts = luxbyte_sql::statements::split(sql);
    let mut rows = Vec::new();
    for (position, analysis) in analyses.iter().enumerate() {
        let text = texts.get(position).copied().unwrap_or(sql);
        let hint = match luxbyte_sql::hint::find(text) {
            Ok(hint) => hint,
            Err(message) => return table_error(&message),
        };
        let plan = shared.router.plan_hinted(analysis, None, hint.as_ref());
        for (step, detail) in crate::explain::lines(text, analysis, &plan) {
            rows.push(vec![
                position.saturating_add(1).to_string(),
                step.to_owned(),
                detail,
            ]);
        }
    }
    table(&["statement", "step", "detail"], rows)
}

/// The proxies sharing the directory, as of this proxy's last visit to the
/// authority region, and the moves it is holding keys for.
fn show_peers(shared: &Shared) -> QueryResult {
    let Some(peers) = &shared.peers else {
        return table_error("this proxy does not share its directory");
    };
    let mut rows: Vec<Vec<String>> = peers
        .peers()
        .into_iter()
        .map(|peer| {
            let role = if peer.node == peers.node() {
                if peers.fenced() {
                    "this proxy, fenced"
                } else {
                    "this proxy"
                }
            } else {
                "peer"
            };
            vec![
                peer.node,
                role.to_owned(),
                peer.heartbeat_age_ms.to_string(),
                String::new(),
            ]
        })
        .collect();
    for (key, to, confirmed, held) in peers.following() {
        rows.push(vec![
            String::new(),
            "holding for a move".to_owned(),
            held.as_millis().to_string(),
            format!(
                "{key} to {to}, confirmed {}",
                confirmed.map_or("nothing yet", crate::peers::Phase::as_str)
            ),
        ]);
    }
    if let Some(error) = peers.last_error() {
        rows.push(vec![
            String::new(),
            "last visit failed".to_owned(),
            String::new(),
            error,
        ]);
    }
    table(&["node", "role", "age_ms", "detail"], rows)
}

fn table_error(message: &str) -> QueryResult {
    table(&["error"], vec![vec![message.to_owned()]])
}

fn show_regions(shared: &Shared) -> QueryResult {
    let topology = shared.router.topology();
    let home_shards = topology.home_shards();
    let rows = topology
        .regions
        .iter()
        .map(|region| {
            vec![
                region.id.clone(),
                region.location().to_owned(),
                if region.id == topology.home_region {
                    "yes".to_owned()
                } else if home_shards.contains(&region.id.as_str()) {
                    "shard".to_owned()
                } else {
                    "no".to_owned()
                },
                region.primary.to_string(),
                region
                    .replicas
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
            ]
        })
        .collect();
    table(&["region", "location", "home", "primary", "replicas"], rows)
}

fn show_placement(shared: &Shared) -> QueryResult {
    let rows = shared
        .router
        .topology()
        .tables
        .iter()
        .map(|policy| {
            let (kind, detail) = match &policy.placement {
                Placement::GeoPartitioned { key } => ("geo_partitioned", key.clone()),
                Placement::GlobalReplicated => ("global_replicated", String::new()),
                Placement::Pinned { region } => ("pinned", region.clone()),
            };
            vec![
                format!("{}.{}", policy.schema, policy.table),
                kind.to_owned(),
                detail,
            ]
        })
        .collect();
    table(&["table", "placement", "detail"], rows)
}

fn show_pools(shared: &Shared) -> QueryResult {
    let rows = shared
        .pools
        .stats()
        .into_iter()
        .map(|(target, stats)| {
            vec![
                target.to_string(),
                stats.idle.to_string(),
                stats.opened.to_string(),
                stats.reused.to_string(),
                stats.discarded.to_string(),
                stats.timeouts.to_string(),
            ]
        })
        .collect();
    table(
        &[
            "backend",
            "idle",
            "opened",
            "reused",
            "discarded",
            "timeouts",
        ],
        rows,
    )
}

fn show_directory(shared: &Shared) -> QueryResult {
    let rows = shared
        .directory
        .snapshot()
        .into_iter()
        .map(|(table_name, key, state)| {
            let (region, status) = describe(&state);
            vec![table_name, key, region, status]
        })
        .collect();
    table(&["table", "key", "region", "state"], rows)
}

pub(crate) fn describe(state: &KeyState) -> (String, String) {
    match state {
        KeyState::Resident { region } => (region.clone(), "resident".to_owned()),
        KeyState::Relocating { from, to, phase } => (
            from.clone()
                .unwrap_or_else(|| "being looked for".to_owned()),
            match phase {
                RelocationPhase::Cutover => format!("moving to {to}; writes wait"),
                RelocationPhase::Committing => {
                    format!("moving to {to}; committing, reads and writes wait")
                }
            },
        ),
    }
}

fn show_health(shared: &Shared) -> QueryResult {
    let topology = shared.router.topology();
    let rows = shared
        .health
        .snapshot()
        .into_iter()
        .map(|status| {
            let lag = topology
                .region(&status.region)
                .zip(status.replica.then_some(()))
                .and_then(|(region, ())| {
                    let endpoint = region
                        .replicas
                        .iter()
                        .find(|replica| crate::health::key(replica) == status.endpoint)?;
                    shared.health.lag(region, endpoint)
                })
                .map(|lag| lag.to_string())
                .unwrap_or_default();
            let role = match (status.replica, status.role_matches()) {
                (true, true) => "replica",
                (false, true) => "primary",
                (true, false) => "replica, but not in recovery",
                (false, false) => "primary, but in recovery",
            };
            vec![
                status.region.clone(),
                status.endpoint.clone(),
                role.to_owned(),
                if status.drained {
                    format!("{}, drained", status.state.name())
                } else {
                    status.state.name().to_owned()
                },
                lag,
                status
                    .probe_time
                    .map(|took| format!("{:.1}", took.as_secs_f64() * 1000.0))
                    .unwrap_or_default(),
                status.last_error.unwrap_or_default(),
            ]
        })
        .collect();
    table(
        &[
            "region",
            "endpoint",
            "role",
            "state",
            "lag_bytes",
            "probe_ms",
            "last_error",
        ],
        rows,
    )
}

fn show_in_doubt(shared: &Shared) -> QueryResult {
    let records = shared.coordinator.log().pending().unwrap_or_default();
    let rows = records
        .into_iter()
        .map(|record| {
            vec![
                record.gid.clone(),
                record.participants.join(", "),
                format!("{:?}", record.in_doubt_for()),
            ]
        })
        .collect();
    table(&["transaction", "participants", "in_doubt_for"], rows)
}

pub(crate) fn where_is(shared: &Shared, table_name: &str, key: &str) -> QueryResult {
    let rows = match shared.directory.locate(table_name, key) {
        Location::Known(state) => {
            let (region, status) = describe(&state);
            vec![vec![table_name.to_owned(), key.to_owned(), region, status]]
        }
        // Not an error: an unseen key is created in the home region on
        // first write, and read from everywhere until then.
        Location::Unseen => vec![vec![
            table_name.to_owned(),
            key.to_owned(),
            shared.router.topology().home_region.clone(),
            "unseen; would be created here".to_owned(),
        ]],
    };
    table(&["table", "key", "region", "state"], rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_are_recognised_whatever_their_spacing_or_case() {
        assert_eq!(normalise(" show   regions ;"), "SHOW REGIONS");
        assert_eq!(normalise("Show Regions"), "SHOW REGIONS");
        assert_eq!(normalise("WHERE IS users alice;"), "WHERE IS USERS ALICE");
    }

    #[test]
    fn ordinary_sql_is_left_alone() {
        // Nothing here may swallow a statement meant for a database.
        for sql in [
            "SELECT * FROM regions",
            "SHOW search_path",
            "UPDATE users SET a = 1 WHERE id = 1",
        ] {
            let command = normalise(sql);
            assert!(
                !matches!(
                    command.as_str(),
                    "SHOW REGIONS" | "SHOW PLACEMENT" | "SHOW POOLS" | "SHOW DIRECTORY"
                ) && !command.starts_with("WHERE IS "),
                "{sql} should not be a console command"
            );
        }
    }

    #[test]
    fn a_table_result_reports_its_row_count() {
        let result = table(&["a"], vec![vec!["1".to_owned()], vec!["2".to_owned()]]);
        assert_eq!(result.tags, ["SELECT 2"]);
        assert_eq!(result.type_oids, [25]);
    }
}
