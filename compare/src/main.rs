//! Prints what each analyser decides for the same statements.
//!
//! Speed is the easy comparison and the less interesting one. This is the
//! other half: given identical input and an equivalent placement policy,
//! which statements does each project agree to run?

use luxbyte_compare::{
    Decision, STATEMENTS, luxbyte_decision, luxbyte_router, shahrah_decision, shahrah_policy,
};
use luxbyte_sql::AnalysisCache;
use shahrah_sql::cache::Cache;

fn main() {
    let router = luxbyte_router();
    let luxbyte_cache = AnalysisCache::default();
    let policy = shahrah_policy();
    let shahrah_cache = Cache::new(4_096);

    let rows: Vec<(&str, Decision, Decision)> = STATEMENTS
        .iter()
        .map(|(name, sql)| {
            (
                *name,
                luxbyte_decision(&router, &luxbyte_cache, sql),
                shahrah_decision(&shahrah_cache, &policy, sql),
            )
        })
        .collect();

    println!("| Statement | Luxbyte | shahrah |");
    println!("|---|---|---|");
    for (name, ours, theirs) in &rows {
        let mark = if ours == theirs { "" } else { "  **<-**" };
        println!(
            "| {name} | {} | {}{mark} |",
            ours.symbol(),
            theirs.symbol()
        );
    }

    let runs_here = rows.iter().filter(|(_, ours, _)| ours.runs()).count();
    let runs_there = rows.iter().filter(|(_, _, theirs)| theirs.runs()).count();
    println!();
    println!(
        "Luxbyte runs {runs_here} of {} statements; shahrah runs {runs_there}.",
        rows.len()
    );

    println!();
    println!("## Where they differ");
    println!();
    for (name, ours, theirs) in &rows {
        if ours == theirs {
            continue;
        }
        println!("**{name}**");
        println!("  - Luxbyte: {}{}", ours.symbol(), suffix(ours));
        println!("  - shahrah:  {}{}", theirs.symbol(), suffix(theirs));
    }
}

fn suffix(decision: &Decision) -> String {
    decision
        .reason()
        .map(|reason| format!(" — {reason}"))
        .unwrap_or_default()
}
