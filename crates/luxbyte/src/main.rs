//! The Luxbyte command line.
//!
//! `luxbyte explain` answers the question a geo-router exists to answer —
//! *where would this statement run?* — without a database, a network, or a
//! deployment. Point it at a topology and a statement and it prints the plan,
//! including the refusals and the reasons for them.
//!
//! The proxy server itself is the next thing to land here; the planner it
//! will use is already complete and tested.

use std::fmt::Write as _;
use std::process::ExitCode;
use std::sync::Arc;

use luxbyte_core::directory::Directory;
use luxbyte_core::{MemoryDirectory, ParameterValues, Plan, Router, Topology};
use luxbyte_sql::MergeStrategy;

const USAGE: &str = "\
luxbyte — a geo-distributed PostgreSQL proxy

USAGE:
    luxbyte explain --topology <FILE> [OPTIONS] <SQL>

OPTIONS:
    --topology <FILE>    Topology in TOML. Required.
    --directory <FILE>   Key directory, one `table key region` per line.
    --param <VALUE>      Bind a placeholder; repeat for $1, $2, ... in order.
    -h, --help           Print this message.

EXAMPLE:
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
        Some("explain") => explain(arguments.get(1..).unwrap_or_default()),
        Some(other) => Err(format!("unknown command {other:?}\n\n{USAGE}")),
    }
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

    let mut report = String::new();
    for (position, analysis) in analyses.iter().enumerate() {
        if analyses.len() > 1 {
            let _ = writeln!(report, "statement {}:", position.saturating_add(1));
        }
        let plan = router.plan(analysis, parameters.as_ref());
        let _ = write!(report, "{}", describe(analysis, &plan));
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

fn describe(analysis: &luxbyte_sql::StatementAnalysis, plan: &Plan) -> String {
    let mut out = String::new();
    let tables: Vec<String> = analysis.tables.iter().map(ToString::to_string).collect();
    let _ = writeln!(
        out,
        "  kind    {:?}, {:?}{}",
        analysis.kind,
        analysis.access,
        if tables.is_empty() {
            String::new()
        } else {
            format!(", tables: {}", tables.join(", "))
        }
    );

    match plan {
        Plan::Local(intent) => {
            let _ = writeln!(out, "  plan    handled in the session ({intent:?})");
        }
        Plan::Single { region, access } => {
            let _ = writeln!(out, "  plan    one region: {region} ({access:?})");
        }
        Plan::ScatterGather { regions, merge } => {
            let _ = writeln!(out, "  plan    scatter-gather over {}", regions.join(", "));
            let _ = writeln!(
                out,
                "  merge   {}",
                match merge {
                    MergeStrategy::Concatenate => "concatenate".to_owned(),
                    MergeStrategy::SortedMerge(keys) => format!(
                        "sorted merge on {}",
                        keys.iter()
                            .map(|key| format!(
                                "{}{}",
                                key.expression,
                                if key.descending { " desc" } else { "" }
                            ))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                }
            );
        }
        Plan::Distributed { regions } => {
            let _ = writeln!(
                out,
                "  plan    distributed write over {} (two-phase commit)",
                regions.join(", ")
            );
        }
        Plan::NeedsParameters { parameters } => {
            let _ = writeln!(
                out,
                "  plan    deferred until {} is bound",
                parameters
                    .iter()
                    .map(|index| format!("${index}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let _ = writeln!(out, "  hint    pass --param to see the resolved plan");
        }
        Plan::Refuse(refusal) => {
            let _ = writeln!(out, "  plan    refused [{}]", refusal.sqlstate);
            let _ = writeln!(out, "  reason  {}", refusal.message);
            if let Some(hint) = &refusal.hint {
                let _ = writeln!(out, "  hint    {hint}");
            }
            if refusal.retryable {
                let _ = writeln!(out, "  retry   yes, the client should retry unchanged");
            }
        }
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
