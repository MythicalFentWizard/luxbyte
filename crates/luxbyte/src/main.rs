//! The Luxbyte command line.
//!
//! `luxbyte explain` answers the question a geo-router exists to answer —
//! *where would this statement run?* — without a database, a network, or a
//! deployment. Point it at a topology and a statement and it prints the plan,
//! including the refusals and the reasons for them.
//!
//! `luxbyte serve` runs the proxy.

use std::fmt::Write as _;
use std::process::ExitCode;
use std::sync::Arc;

use luxbyte_core::directory::Directory;
use luxbyte_core::{MemoryDirectory, ParameterValues, Plan, Router, Topology};

const USAGE: &str = "\
luxbyte — a geo-distributed PostgreSQL proxy

USAGE:
    luxbyte serve   --config <FILE>
    luxbyte explain --topology <FILE> [OPTIONS] <SQL>

SERVE OPTIONS:
    --config <FILE>      Proxy configuration in TOML. Required.

EXPLAIN OPTIONS:
    --topology <FILE>    Topology in TOML. Required.
    --directory <FILE>   Key directory, one `table key region` per line.
    --param <VALUE>      Bind a placeholder; repeat for $1, $2, ... in order.

    -h, --help           Print this message.

EXAMPLES:
    luxbyte serve --config examples/luxbyte.toml

    luxbyte explain --topology topology.toml \\
        --directory keys.tsv \\
        \"SELECT * FROM users WHERE tenant_id = 'alice'\"
";

fn main() -> ExitCode {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    match run(&arguments) {
        Ok(output) => {
            println!("{output}");
            ExitCode::SUCCESS
        }
        Err(message) => {
            eprintln!("luxbyte: {message}");
            ExitCode::FAILURE
        }
    }
}

fn run(arguments: &[String]) -> Result<String, String> {
    let first = arguments.first().map(String::as_str);
    match first {
        None | Some("-h" | "--help" | "help") => Ok(USAGE.to_owned()),
        Some("serve") => serve(arguments.get(1..).unwrap_or_default()),
        Some("explain") => explain(arguments.get(1..).unwrap_or_default()),
        Some(other) => Err(format!("unknown command {other:?}\n\n{USAGE}")),
    }
}

/// Starts the proxy and serves until interrupted.
fn serve(arguments: &[String]) -> Result<String, String> {
    let mut path = None;
    let mut index = 0_usize;
    while let Some(argument) = arguments.get(index) {
        match argument.as_str() {
            "--config" => {
                path = arguments.get(index.saturating_add(1)).cloned();
                index = index.saturating_add(2);
            }
            other => {
                return Err(format!(
                    "unknown option {other:?}

{USAGE}"
                ));
            }
        }
    }
    let path = path.ok_or_else(|| {
        format!(
            "--config is required

{USAGE}"
        )
    })?;
    let config = luxbyte::Config::load(&path).map_err(|error| error.to_string())?;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("luxbyte=info")),
        )
        .init();

    let runtime = tokio::runtime::Runtime::new().map_err(|error| error.to_string())?;
    runtime.block_on(async move {
        let server = luxbyte::Server::bind(&config)
            .await
            .map_err(|error| error.to_string())?;
        let address = server.local_addr().map_err(|error| error.to_string())?;

        // Resolve anything a previous run left in doubt before taking
        // traffic: an unresolved prepared transaction holds locks and stops
        // VACUUM reclaiming, and serving more work makes that worse.
        match server.recover().await {
            Ok(report) if report.is_clean() => {
                tracing::info!("no transactions were left in doubt");
            }
            Ok(report) => tracing::warn!(
                committed = report.committed.len(),
                rolled_back = report.rolled_back.len(),
                still_in_doubt = report.still_in_doubt.len(),
                "resolved transactions from a previous run"
            ),
            Err(error) => tracing::error!(%error, "recovery failed; continuing"),
        }

        if config.server.commit_log.is_none() {
            tracing::warn!(
                "no commit_log configured: a crash between prepare and commit will leave \
                 prepared transactions for an operator to resolve by hand"
            );
        }

        if let Some(admin) = server.admin_addr() {
            tracing::info!(%admin, "admin port listening: /metrics, /snapshot, /key, /");
        }
        tracing::info!(%address, node = %config.server.node, "listening");
        server.serve().await;
        Ok::<String, String>(format!("stopped listening on {address}"))
    })
}

/// Arguments to `luxbyte explain`.
#[derive(Debug, Default)]
struct ExplainArguments {
    topology: Option<String>,
    directory: Option<String>,
    parameters: Vec<Option<String>>,
    sql: Option<String>,
}

fn parse_explain(arguments: &[String]) -> Result<ExplainArguments, String> {
    let mut parsed = ExplainArguments::default();
    let mut index = 0_usize;
    while let Some(argument) = arguments.get(index) {
        let mut take_value = |name: &str| -> Result<String, String> {
            let value = arguments
                .get(index.saturating_add(1))
                .ok_or_else(|| format!("{name} needs a value"))?;
            index = index.saturating_add(2);
            Ok(value.clone())
        };
        match argument.as_str() {
            "--topology" => parsed.topology = Some(take_value("--topology")?),
            "--directory" => parsed.directory = Some(take_value("--directory")?),
            "--param" => {
                let value = take_value("--param")?;
                // `NULL` is how a bound-but-null parameter is written, since
                // an empty string is a perfectly good shard key.
                parsed.parameters.push((value != "NULL").then_some(value));
            }
            other if other.starts_with("--") => {
                return Err(format!("unknown option {other:?}"));
            }
            _ => {
                if parsed.sql.is_some() {
                    return Err("only one statement can be explained at a time".to_owned());
                }
                parsed.sql = Some(argument.clone());
                index = index.saturating_add(1);
            }
        }
    }
    Ok(parsed)
}

fn explain(arguments: &[String]) -> Result<String, String> {
    let parsed = parse_explain(arguments)?;
    let topology_path = parsed
        .topology
        .ok_or_else(|| format!("--topology is required\n\n{USAGE}"))?;
    let sql = parsed
        .sql
        .ok_or_else(|| format!("a statement is required\n\n{USAGE}"))?;

    let text = std::fs::read_to_string(&topology_path)
        .map_err(|error| format!("could not read {topology_path}: {error}"))?;
    let topology = Topology::from_toml(&text).map_err(|error| error.to_string())?;

    let directory = MemoryDirectory::new();
    if let Some(path) = &parsed.directory {
        load_directory(&directory, path)?;
    }

    let router = Router::new(topology, Arc::new(directory));
    let analyses = luxbyte_sql::analyze(&sql).map_err(|error| error.to_string())?;
    let parameters =
        (!parsed.parameters.is_empty()).then(|| ParameterValues::new(parsed.parameters.clone()));

    let texts = luxbyte_sql::statements::split(&sql);
    let mut report = String::new();
    for (position, analysis) in analyses.iter().enumerate() {
        if analyses.len() > 1 {
            let _ = writeln!(report, "statement {}:", position.saturating_add(1));
        }
        let plan = router.plan(analysis, parameters.as_ref());
        let text = texts.get(position).copied().unwrap_or(&sql);
        let _ = write!(report, "{}", describe(text, analysis, &plan));
    }
    Ok(report.trim_end().to_owned())
}

fn load_directory(directory: &MemoryDirectory, path: &str) -> Result<(), String> {
    let text =
        std::fs::read_to_string(path).map_err(|error| format!("could not read {path}: {error}"))?;
    for (number, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields: Vec<&str> = line.split_whitespace().collect();
        let [table, key, region] = fields.as_slice() else {
            return Err(format!(
                "{path}:{}: expected `table key region`",
                number.saturating_add(1)
            ));
        };
        directory.place(table, key, region);
    }
    Ok(())
}

fn describe(sql: &str, analysis: &luxbyte_sql::StatementAnalysis, plan: &Plan) -> String {
    let mut out = String::new();
    for (label, detail) in luxbyte::explain::lines(sql, analysis, plan) {
        let _ = writeln!(out, "  {label:<7} {detail}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(items: &[&str]) -> Vec<String> {
        items.iter().map(|item| (*item).to_owned()).collect()
    }

    #[test]
    fn no_arguments_prints_the_usage_rather_than_failing() {
        assert!(run(&[]).is_ok_and(|output| output.contains("USAGE")));
    }

    #[test]
    fn an_unknown_command_is_an_error() {
        assert!(run(&arguments(&["frobnicate"])).is_err());
    }

    #[test]
    fn explain_requires_a_topology() {
        let error = explain(&arguments(&["SELECT 1"]));
        assert!(error.is_err_and(|message| message.contains("--topology is required")));
    }

    #[test]
    fn a_missing_option_value_is_reported_rather_than_ignored() {
        let error = parse_explain(&arguments(&["--topology"]));
        assert!(error.is_err_and(|message| message.contains("--topology needs a value")));
    }

    #[test]
    fn parameters_are_collected_in_order_and_null_is_recognised() {
        let parsed = parse_explain(&arguments(&[
            "--param", "alice", "--param", "NULL", "SELECT 1",
        ]))
        .expect("should parse");
        assert_eq!(parsed.parameters, vec![Some("alice".to_owned()), None]);
        assert_eq!(parsed.sql.as_deref(), Some("SELECT 1"));
    }

    #[test]
    fn two_statements_cannot_be_passed_as_two_arguments() {
        let error = parse_explain(&arguments(&["SELECT 1", "SELECT 2"]));
        assert!(error.is_err());
    }
}
