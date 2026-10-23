//! What the proxy has been doing, in Prometheus's text format.
//!
//! Counted where the decisions are made rather than inferred afterwards: a
//! statement is counted by the plan it got, a refusal by its SQLSTATE, a
//! read across regions by how it was finished, a statement sent to a region
//! by that region. The numbers an operator of a geo-router needs are the
//! ones a generic pooler cannot give: how much traffic leaves the home
//! region, how much fans out, and how much is refused and why.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

/// Upper bounds of the latency buckets, in seconds.
const BUCKETS: [f64; 14] = [
    0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

#[derive(Debug, Clone, Default)]
struct Histogram {
    /// Observations at or below each bound, cumulatively as Prometheus wants.
    buckets: [u64; BUCKETS.len()],
    count: u64,
    sum: f64,
}

impl Histogram {
    fn observe(&mut self, seconds: f64) {
        for (bound, bucket) in BUCKETS.iter().zip(self.buckets.iter_mut()) {
            if seconds <= *bound {
                *bucket = bucket.saturating_add(1);
            }
        }
        self.count = self.count.saturating_add(1);
        self.sum += seconds;
    }
}

/// Every counter the proxy keeps.
#[derive(Debug)]
pub struct Metrics {
    started: Instant,
    sessions_total: AtomicU64,
    sessions_ended: AtomicU64,
    auth_failures: AtomicU64,
    cancels_received: AtomicU64,
    cancels_delivered: AtomicU64,
    remote_statements: AtomicU64,
    statements: Mutex<BTreeMap<&'static str, Histogram>>,
    errors: Mutex<BTreeMap<String, u64>>,
    region_statements: Mutex<BTreeMap<String, u64>>,
    combines: Mutex<BTreeMap<&'static str, u64>>,
    transactions: Mutex<BTreeMap<&'static str, u64>>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            sessions_total: AtomicU64::new(0),
            sessions_ended: AtomicU64::new(0),
            auth_failures: AtomicU64::new(0),
            cancels_received: AtomicU64::new(0),
            cancels_delivered: AtomicU64::new(0),
            remote_statements: AtomicU64::new(0),
            statements: Mutex::new(BTreeMap::new()),
            errors: Mutex::new(BTreeMap::new()),
            region_statements: Mutex::new(BTreeMap::new()),
            combines: Mutex::new(BTreeMap::new()),
            transactions: Mutex::new(BTreeMap::new()),
        }
    }
}

fn bump<K: Ord>(map: &Mutex<BTreeMap<K, u64>>, key: K) {
    let mut map = map.lock();
    let slot = map.entry(key).or_insert(0);
    *slot = slot.saturating_add(1);
}

/// Ends a session's count when dropped, whichever way the session ends.
#[derive(Debug)]
pub struct SessionGuard<'a> {
    metrics: &'a Metrics,
}

impl Drop for SessionGuard<'_> {
    fn drop(&mut self) {
        self.metrics.sessions_ended.fetch_add(1, Ordering::Relaxed);
    }
}

impl Metrics {
    /// A session started.
    pub fn session(&self) -> SessionGuard<'_> {
        self.sessions_total.fetch_add(1, Ordering::Relaxed);
        SessionGuard { metrics: self }
    }

    /// A client failed to authenticate.
    pub fn auth_failed(&self) {
        self.auth_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// A cancel arrived for a live session, and was delivered to `delivered`
    /// backend connections.
    pub fn cancelled(&self, delivered: usize) {
        self.cancels_received.fetch_add(1, Ordering::Relaxed);
        self.cancels_delivered.fetch_add(
            u64::try_from(delivered).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    /// A statement was planned as `plan` and took `elapsed` to answer.
    pub fn statement(&self, plan: &'static str, elapsed: Duration) {
        self.statements
            .lock()
            .entry(plan)
            .or_default()
            .observe(elapsed.as_secs_f64());
    }

    /// The client was sent an error with this SQLSTATE.
    pub fn error(&self, sqlstate: &str) {
        bump(&self.errors, sqlstate.to_owned());
    }

    /// A statement was sent to `region`, which is not the home region when
    /// `remote` is set.
    pub fn sent(&self, region: &str, remote: bool) {
        bump(&self.region_statements, region.to_owned());
        if remote {
            self.remote_statements.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A read across regions was finished this way.
    pub fn combined(&self, how: &'static str) {
        bump(&self.combines, how);
    }

    /// A transaction ended this way.
    pub fn transaction(&self, outcome: &'static str) {
        bump(&self.transactions, outcome);
    }

    /// Seconds since the proxy started.
    #[must_use]
    pub fn uptime(&self) -> Duration {
        self.started.elapsed()
    }

    /// Sessions open right now.
    #[must_use]
    pub fn active_sessions(&self) -> u64 {
        self.sessions_total
            .load(Ordering::Relaxed)
            .saturating_sub(self.sessions_ended.load(Ordering::Relaxed))
    }

    /// The counters as name/value pairs, for a snapshot.
    #[must_use]
    pub fn summary(&self) -> BTreeMap<String, u64> {
        let mut out = BTreeMap::new();
        out.insert(
            "sessions_total".to_owned(),
            self.sessions_total.load(Ordering::Relaxed),
        );
        out.insert("sessions_active".to_owned(), self.active_sessions());
        out.insert(
            "auth_failures_total".to_owned(),
            self.auth_failures.load(Ordering::Relaxed),
        );
        out.insert(
            "remote_statements_total".to_owned(),
            self.remote_statements.load(Ordering::Relaxed),
        );
        out.insert(
            "cancels_total".to_owned(),
            self.cancels_received.load(Ordering::Relaxed),
        );
        for (plan, histogram) in self.statements.lock().iter() {
            out.insert(format!("statements_{plan}"), histogram.count);
        }
        for (sqlstate, count) in self.errors.lock().iter() {
            out.insert(format!("errors_{sqlstate}"), *count);
        }
        for (how, count) in self.combines.lock().iter() {
            out.insert(format!("combined_{how}"), *count);
        }
        for (outcome, count) in self.transactions.lock().iter() {
            out.insert(format!("transactions_{outcome}"), *count);
        }
        out
    }

    /// Everything, in Prometheus's text exposition format.
    #[must_use]
    pub fn render(&self, extra: &[Gauge]) -> String {
        let mut out = String::new();
        for (name, kind, help, value) in [
            (
                "luxbyte_uptime_seconds",
                "gauge",
                "Seconds since the proxy started.",
                self.uptime().as_secs(),
            ),
            (
                "luxbyte_sessions_total",
                "counter",
                "Client sessions accepted.",
                self.sessions_total.load(Ordering::Relaxed),
            ),
            (
                "luxbyte_sessions_active",
                "gauge",
                "Client sessions open.",
                self.active_sessions(),
            ),
            (
                "luxbyte_auth_failures_total",
                "counter",
                "Client logins refused.",
                self.auth_failures.load(Ordering::Relaxed),
            ),
            (
                "luxbyte_cancels_total",
                "counter",
                "Cancel requests received for a live session.",
                self.cancels_received.load(Ordering::Relaxed),
            ),
            (
                "luxbyte_cancels_delivered_total",
                "counter",
                "Backend connections a cancel was relayed to.",
                self.cancels_delivered.load(Ordering::Relaxed),
            ),
            (
                "luxbyte_remote_statements_total",
                "counter",
                "Statements sent to a region other than the home region.",
                self.remote_statements.load(Ordering::Relaxed),
            ),
        ] {
            header(&mut out, name, kind, help);
            let _ = writeln!(out, "{name} {value}");
        }

        header(
            &mut out,
            "luxbyte_statement_duration_seconds",
            "histogram",
            "Time to answer a statement, by the plan it got.",
        );
        for (plan, histogram) in self.statements.lock().iter() {
            for (bound, count) in BUCKETS.iter().zip(histogram.buckets.iter()) {
                let _ = writeln!(
                    out,
                    "luxbyte_statement_duration_seconds_bucket{{plan=\"{plan}\",le=\"{bound}\"}} {count}"
                );
            }
            let _ = writeln!(
                out,
                "luxbyte_statement_duration_seconds_bucket{{plan=\"{plan}\",le=\"+Inf\"}} {}\n\
                 luxbyte_statement_duration_seconds_sum{{plan=\"{plan}\"}} {}\n\
                 luxbyte_statement_duration_seconds_count{{plan=\"{plan}\"}} {}",
                histogram.count, histogram.sum, histogram.count
            );
        }

        self.render_counters(&mut out);

        for gauge in extra {
            header(&mut out, gauge.name, "gauge", gauge.help);
            for (labels, value) in &gauge.values {
                let rendered: Vec<String> = labels
                    .iter()
                    .map(|(key, value)| format!("{key}=\"{}\"", escape(value)))
                    .collect();
                let _ = writeln!(out, "{}{{{}}} {value}", gauge.name, rendered.join(","));
            }
        }
        out
    }
}

impl Metrics {
    /// The labelled counter families.
    fn render_counters(&self, out: &mut String) {
        labelled(
            out,
            "luxbyte_errors_total",
            "Errors sent to clients, by SQLSTATE: refusals and backend errors alike.",
            "sqlstate",
            self.errors.lock().iter().map(|(k, v)| (k.as_str(), *v)),
        );
        labelled(
            out,
            "luxbyte_region_statements_total",
            "Statements sent to each region.",
            "region",
            self.region_statements
                .lock()
                .iter()
                .map(|(k, v)| (k.as_str(), *v)),
        );
        labelled(
            out,
            "luxbyte_combined_reads_total",
            "Reads across regions, by how the result was finished.",
            "finish",
            self.combines.lock().iter().map(|(k, v)| (*k, *v)),
        );
        labelled(
            out,
            "luxbyte_transactions_total",
            "Transactions, by how they ended.",
            "outcome",
            self.transactions.lock().iter().map(|(k, v)| (*k, *v)),
        );
    }
}

/// A gauge read from elsewhere at render time, such as pool occupancy.
#[derive(Debug, Clone)]
pub struct Gauge {
    /// Its metric name.
    pub name: &'static str,
    /// Its description.
    pub help: &'static str,
    /// Each labelled value.
    pub values: Vec<(Vec<(&'static str, String)>, u64)>,
}

fn header(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}");
}

fn labelled<'a>(
    out: &mut String,
    name: &str,
    help: &str,
    label: &str,
    values: impl Iterator<Item = (&'a str, u64)>,
) {
    header(out, name, "counter", help);
    for (key, value) in values {
        let _ = writeln!(out, "{name}{{{label}=\"{}\"}} {value}", escape(key));
    }
}

/// Escapes a label value as the exposition format requires.
fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_histogram_counts_cumulatively() {
        let metrics = Metrics::default();
        metrics.statement("single", Duration::from_micros(800));
        metrics.statement("single", Duration::from_millis(30));
        let text = metrics.render(&[]);
        assert!(
            text.contains(
                "luxbyte_statement_duration_seconds_bucket{plan=\"single\",le=\"0.001\"} 1"
            )
        );
        assert!(
            text.contains(
                "luxbyte_statement_duration_seconds_bucket{plan=\"single\",le=\"0.05\"} 2"
            )
        );
        assert!(text.contains("luxbyte_statement_duration_seconds_count{plan=\"single\"} 2"));
    }

    #[test]
    fn labels_are_escaped() {
        let metrics = Metrics::default();
        metrics.sent("eu\"west\\1", true);
        let text = metrics.render(&[]);
        assert!(text.contains("region=\"eu\\\"west\\\\1\""));
        assert!(text.contains("luxbyte_remote_statements_total 1"));
    }

    #[test]
    fn sessions_end_however_they_end() {
        let metrics = Metrics::default();
        {
            let _first = metrics.session();
            let _second = metrics.session();
            assert_eq!(metrics.active_sessions(), 2);
        }
        assert_eq!(metrics.active_sessions(), 0);
    }

    #[test]
    fn every_family_is_declared_before_its_samples() {
        let metrics = Metrics::default();
        metrics.error("0A000");
        let text = metrics.render(&[]);
        let declared = text.find("# TYPE luxbyte_errors_total counter");
        let sample = text.find("luxbyte_errors_total{sqlstate=\"0A000\"} 1");
        assert!(declared.is_some() && declared < sample, "{text}");
    }
}
