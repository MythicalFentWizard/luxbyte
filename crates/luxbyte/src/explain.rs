//! Saying where a statement would run, and why, without running it.
//!
//! The same description serves the `explain` command, which needs no
//! database at all, and the console's `ROUTE`, which uses the proxy's live
//! directory.

use luxbyte_core::Plan;
use luxbyte_sql::combine::{Aggregates, Finish};
use luxbyte_sql::{MergeStrategy, StatementAnalysis};

/// The plan as labelled lines: what the statement is, where it goes, and
/// for a read across regions how the answers are combined.
#[must_use]
pub fn lines(sql: &str, analysis: &StatementAnalysis, plan: &Plan) -> Vec<(&'static str, String)> {
    let mut out = Vec::new();
    let tables: Vec<String> = analysis.tables.iter().map(ToString::to_string).collect();
    out.push((
        "kind",
        format!(
            "{:?}, {:?}{}",
            analysis.kind,
            analysis.access,
            if tables.is_empty() {
                String::new()
            } else {
                format!(", tables: {}", tables.join(", "))
            }
        ),
    ));

    match plan {
        Plan::Local(intent) => out.push(("plan", format!("handled in the session ({intent:?})"))),
        Plan::Single { region, access } => {
            out.push(("plan", format!("one region: {region} ({access:?})")));
        }
        Plan::ScatterGather { regions, merge } => {
            out.push((
                "plan",
                format!("scatter-gather over {}", regions.join(", ")),
            ));
            match (
                merge,
                luxbyte_sql::combine::plan(sql, &Aggregates::default()),
            ) {
                (MergeStrategy::Concatenate, Ok(_)) => {
                    out.push(("merge", "concatenate".to_owned()));
                }
                (_, Ok(plan)) => {
                    out.push(("regions", plan.region_sql));
                    out.push(("finish", finish(&plan.finish)));
                }
                (_, Err(reason)) => {
                    out.push(("combine", "refused [0A000]".to_owned()));
                    out.push(("reason", reason.to_string()));
                }
            }
        }
        Plan::Distributed { regions } => out.push((
            "plan",
            format!(
                "distributed write over {} (two-phase commit)",
                regions.join(", ")
            ),
        )),
        Plan::NeedsParameters { parameters } => {
            out.push((
                "plan",
                format!(
                    "deferred until {} is bound",
                    parameters
                        .iter()
                        .map(|index| format!("${index}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ));
            out.push(("hint", "bind values to see the resolved plan".to_owned()));
        }
        Plan::Refuse(refusal) => {
            out.push(("plan", format!("refused [{}]", refusal.sqlstate)));
            out.push(("reason", refusal.message.clone()));
            if let Some(hint) = &refusal.hint {
                out.push(("hint", hint.clone()));
            }
            if refusal.retryable {
                out.push(("retry", "yes, the client should retry unchanged".to_owned()));
            }
        }
    }
    out
}

fn finish(finish: &Finish) -> String {
    match finish {
        Finish::Rows(rows) => format!(
            "rows: {}{}{}{}",
            if rows.order.is_empty() {
                "concatenated"
            } else {
                "merged in order"
            },
            if rows.distinct { ", deduplicated" } else { "" },
            rows.limit
                .map(|limit| format!(", limit {limit}"))
                .unwrap_or_default(),
            if rows.offset > 0 {
                format!(", offset {}", rows.offset)
            } else {
                String::new()
            }
        ),
        Finish::Aggregate(aggregates) => format!(
            "{} partial aggregate(s) regrouped in the home region",
            aggregates.aggregates.len()
        ),
    }
}
