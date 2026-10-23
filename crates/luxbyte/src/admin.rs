//! The admin port: Prometheus metrics, a JSON snapshot, where a key lives,
//! and a dashboard.
//!
//! It shows the topology and which region any named user's rows are in, so
//! it is guarded as carefully as the data port:
//!
//! - It listens nowhere unless configured, and refuses to listen beyond
//!   loopback without a password unless told that something else in front
//!   of it authenticates.
//! - The password is compared as a constant-time equality of its SHA-256,
//!   so a wrong guess takes the same time whatever it got right.
//! - Five wrong answers from one address lock that address out for thirty
//!   seconds, doubling up to fifteen minutes; every wrong answer costs a
//!   quarter of a second; and the table of addresses is bounded, because a
//!   table that grows with the attacker is the denial of service it was
//!   meant to prevent.
//!
//! The HTTP is deliberately small: `GET` only, one request per connection,
//! a bounded request head read under a timeout. It serves a handful of
//! read-only pages, and that is all it needs to understand.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use luxbyte_pool::QueryResult;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::metrics::Gauge;
use crate::session::Shared;

/// Largest request head read before giving up on a client.
const MAX_HEAD: usize = 16 * 1024;
/// How long a client has to send its request head.
const READ_TIMEOUT: Duration = Duration::from_secs(5);
/// What every wrong password costs, however it arrived.
const WRONG_ANSWER_DELAY: Duration = Duration::from_millis(250);
/// Wrong answers before an address is locked out.
const ALLOWED_FAILURES: u32 = 5;
const FIRST_LOCK: Duration = Duration::from_secs(30);
const LONGEST_LOCK: Duration = Duration::from_secs(15 * 60);
/// Addresses remembered at once.
const MAX_TRACKED: usize = 10_000;

/// The `[admin]` section.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct AdminConfig {
    /// Where to listen. Unset, the admin port is off.
    pub listen: Option<String>,
    /// The operator password. Prefer `password_env`, so the password is not
    /// in a file that might be committed.
    pub password: Option<String>,
    /// The name of an environment variable holding the password.
    pub password_env: Option<String>,
    /// Serve a non-loopback address with no password anyway, for a
    /// deployment that authenticates in front of the proxy.
    pub open: bool,
    /// Serve HTTPS with the `[tls]` certificate. A plain request to an
    /// HTTPS port is dropped, never answered in the clear.
    pub tls: bool,
}

impl AdminConfig {
    /// The password, from wherever it was configured.
    #[must_use]
    pub fn password(&self) -> Option<String> {
        self.password_env
            .as_ref()
            .and_then(|name| std::env::var(name).ok())
            .or_else(|| self.password.clone())
            .filter(|password| !password.is_empty())
    }
}

#[derive(Debug, Default)]
struct Attempts {
    failures: u32,
    locks: u32,
    locked_until: Option<Instant>,
    last_seen: Option<Instant>,
}

/// Wrong-password bookkeeping per address.
#[derive(Debug, Default)]
struct Lockout {
    addresses: Mutex<HashMap<IpAddr, Attempts>>,
}

impl Lockout {
    /// How much longer `address` is locked out, if it is.
    fn remaining(&self, address: IpAddr) -> Option<Duration> {
        let addresses = self.addresses.lock();
        let until = addresses.get(&address)?.locked_until?;
        until.checked_duration_since(Instant::now())
    }

    fn failed(&self, address: IpAddr) {
        let mut addresses = self.addresses.lock();
        if addresses.len() >= MAX_TRACKED && !addresses.contains_key(&address) {
            evict(&mut addresses);
        }
        let attempts = addresses.entry(address).or_default();
        attempts.last_seen = Some(Instant::now());
        attempts.failures = attempts.failures.saturating_add(1);
        if attempts.failures >= ALLOWED_FAILURES {
            attempts.failures = 0;
            attempts.locks = attempts.locks.saturating_add(1);
            let doubling = 1_u32
                .checked_shl(attempts.locks.saturating_sub(1))
                .unwrap_or(u32::MAX);
            let lock = FIRST_LOCK
                .checked_mul(doubling)
                .unwrap_or(LONGEST_LOCK)
                .min(LONGEST_LOCK);
            attempts.locked_until = Instant::now().checked_add(lock);
        }
    }

    fn succeeded(&self, address: IpAddr) {
        self.addresses.lock().remove(&address);
    }
}

/// Makes room for one more address: forgets an unlocked one seen longest
/// ago, or else the lock that ends soonest.
fn evict(addresses: &mut HashMap<IpAddr, Attempts>) {
    let now = Instant::now();
    let victim = addresses
        .iter()
        .filter(|(_, attempts)| attempts.locked_until.is_none_or(|until| until <= now))
        .min_by_key(|(_, attempts)| attempts.last_seen)
        .or_else(|| {
            addresses
                .iter()
                .min_by_key(|(_, attempts)| attempts.locked_until)
        })
        .map(|(address, _)| *address);
    if let Some(victim) = victim {
        addresses.remove(&victim);
    }
}

/// The admin port's state.
pub struct Admin {
    shared: Arc<Shared>,
    tls: Option<crate::tls::Acceptor>,
    /// SHA-256 of the password; never the password itself.
    password: Option<[u8; 32]>,
    lockout: Lockout,
}

impl std::fmt::Debug for Admin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Admin")
            .field("password", &self.password.is_some())
            .finish_non_exhaustive()
    }
}

fn digest(text: &str) -> [u8; 32] {
    let mut out = [0_u8; 32];
    out.copy_from_slice(&Sha256::digest(text.as_bytes()));
    out
}

impl Admin {
    /// Prepares the admin port.
    #[must_use]
    pub fn new(
        shared: Arc<Shared>,
        password: Option<&str>,
        tls: Option<crate::tls::Acceptor>,
    ) -> Self {
        Self {
            shared,
            tls,
            password: password.map(digest),
            lockout: Lockout::default(),
        }
    }

    /// Whether a request's `Authorization` header carries the password.
    fn authorised(&self, header: Option<&str>) -> bool {
        let Some(expected) = self.password else {
            return true;
        };
        let Some(offered) = header.and_then(credential) else {
            return false;
        };
        digest(&offered).ct_eq(&expected).into()
    }
}

/// The password in a `Bearer` or `Basic` authorization header.
fn credential(header: &str) -> Option<String> {
    let (scheme, value) = header.trim().split_once(' ')?;
    if scheme.eq_ignore_ascii_case("bearer") {
        return Some(value.trim().to_owned());
    }
    if scheme.eq_ignore_ascii_case("basic") {
        use base64::Engine as _;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(value.trim())
            .ok()?;
        let text = String::from_utf8(decoded).ok()?;
        // The user name is not checked: there is one operator password.
        return text
            .split_once(':')
            .map(|(_, password)| password.to_owned());
    }
    None
}

/// Serves the admin port until the listener fails.
pub async fn serve(listener: TcpListener, admin: Arc<Admin>) {
    loop {
        let Ok((stream, peer)) = listener.accept().await else {
            return;
        };
        let admin = Arc::clone(&admin);
        tokio::spawn(async move {
            let outcome = match &admin.tls {
                Some(acceptor) => match acceptor.accept(stream).await {
                    Ok(encrypted) => handle(encrypted, peer, &admin).await,
                    Err(error) => Err(error),
                },
                None => handle(stream, peer, &admin).await,
            };
            if let Err(error) = outcome {
                tracing::debug!(%peer, %error, "admin request ended");
            }
        });
    }
}

struct Request {
    method: String,
    path: String,
    query: String,
    authorization: Option<String>,
}

async fn read_request<S: AsyncRead + Unpin>(stream: &mut S) -> std::io::Result<Option<Request>> {
    let mut head = Vec::with_capacity(1024);
    let mut chunk = [0_u8; 1024];
    let complete = tokio::time::timeout(READ_TIMEOUT, async {
        loop {
            let read = stream.read(&mut chunk).await?;
            if read == 0 {
                return Ok::<bool, std::io::Error>(false);
            }
            head.extend_from_slice(chunk.get(..read).unwrap_or(&[]));
            if head.windows(4).any(|window| window == b"\r\n\r\n") {
                return Ok(true);
            }
            if head.len() > MAX_HEAD {
                return Ok(false);
            }
        }
    })
    .await
    .unwrap_or(Ok(false))?;
    if !complete {
        return Ok(None);
    }
    let text = String::from_utf8_lossy(&head);
    let mut lines = text.split("\r\n");
    let mut first = lines.next().unwrap_or("").split(' ');
    let method = first.next().unwrap_or("").to_owned();
    let target = first.next().unwrap_or("");
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let authorization = lines.take_while(|line| !line.is_empty()).find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("authorization")
            .then(|| value.trim().to_owned())
    });
    Ok(Some(Request {
        method,
        path: path.to_owned(),
        query: query.to_owned(),
        authorization,
    }))
}

struct Response {
    status: &'static str,
    content_type: &'static str,
    extra: Vec<(&'static str, String)>,
    body: String,
}

impl Response {
    fn new(status: &'static str, content_type: &'static str, body: String) -> Self {
        Self {
            status,
            content_type,
            extra: Vec::new(),
            body,
        }
    }

    fn text(status: &'static str, body: &str) -> Self {
        Self::new(status, "text/plain; charset=utf-8", format!("{body}\n"))
    }

    fn json(value: &Value) -> Self {
        Self::new(
            "200 OK",
            "application/json",
            serde_json::to_string_pretty(value).unwrap_or_default(),
        )
    }
}

async fn handle<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    peer: SocketAddr,
    admin: &Admin,
) -> std::io::Result<()> {
    let Some(request) = read_request(&mut stream).await? else {
        return write(
            &mut stream,
            &Response::text("400 Bad Request", "bad request"),
        )
        .await;
    };
    // A load balancer's health check needs no password and learns nothing.
    if request.path == "/healthz" {
        return write(&mut stream, &Response::text("200 OK", "ok")).await;
    }
    if request.method != "GET" {
        return write(
            &mut stream,
            &Response::text("405 Method Not Allowed", "only GET is served here"),
        )
        .await;
    }

    let address = peer.ip();
    if let Some(remaining) = admin.lockout.remaining(address) {
        let mut response = Response::text("429 Too Many Requests", "too many wrong passwords");
        response
            .extra
            .push(("Retry-After", remaining.as_secs().max(1).to_string()));
        return write(&mut stream, &response).await;
    }
    if !admin.authorised(request.authorization.as_deref()) {
        admin.lockout.failed(address);
        tokio::time::sleep(WRONG_ANSWER_DELAY).await;
        let mut response = Response::text("401 Unauthorized", "a password is needed");
        response
            .extra
            .push(("WWW-Authenticate", "Basic realm=\"luxbyte\"".to_owned()));
        return write(&mut stream, &response).await;
    }
    if admin.password.is_some() {
        admin.lockout.succeeded(address);
    }

    let response = route(admin, &request);
    write(&mut stream, &response).await
}

fn route(admin: &Admin, request: &Request) -> Response {
    let shared = &admin.shared;
    match request.path.as_str() {
        "/metrics" => Response::new(
            "200 OK",
            "text/plain; version=0.0.4; charset=utf-8",
            shared.metrics.render(&gauges(shared)),
        ),
        "/snapshot" => Response::json(&snapshot(shared)),
        "/" => Response::new("200 OK", "text/html; charset=utf-8", dashboard(shared)),
        "/key" => {
            let table = query_value(&request.query, "table").unwrap_or_default();
            let key = query_value(&request.query, "key").unwrap_or_default();
            key_page(shared, &table, &key)
        }
        path => match path
            .strip_prefix("/key/")
            .and_then(|rest| rest.split_once('/'))
        {
            Some((table, key)) => key_page(shared, &decode(table), &decode(key)),
            None => Response::text("404 Not Found", "no such page"),
        },
    }
}

async fn write<S: AsyncWrite + Unpin>(stream: &mut S, response: &Response) -> std::io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\
         Cache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\n\
         X-Frame-Options: DENY\r\nReferrer-Policy: no-referrer\r\n\
         Content-Security-Policy: default-src 'none'; style-src 'unsafe-inline'; \
         form-action 'self'\r\n",
        response.status,
        response.content_type,
        response.body.len()
    );
    for (name, value) in &response.extra {
        head.push_str(name);
        head.push_str(": ");
        head.push_str(value);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(response.body.as_bytes()).await?;
    stream.shutdown().await
}

/// Percent-decodes a path segment or query value.
fn decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0_usize;
    while let Some(byte) = bytes.get(index).copied() {
        let hex = bytes
            .get(index.saturating_add(1)..index.saturating_add(3))
            .and_then(|pair| std::str::from_utf8(pair).ok())
            .and_then(|pair| u8::from_str_radix(pair, 16).ok());
        match (byte, hex) {
            (b'%', Some(decoded)) => {
                out.push(decoded);
                index = index.saturating_add(3);
            }
            (b'+', _) => {
                out.push(b' ');
                index = index.saturating_add(1);
            }
            _ => {
                out.push(byte);
                index = index.saturating_add(1);
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn query_value(query: &str, name: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == name).then(|| decode(value))
    })
}

fn rows_as_json(result: &QueryResult) -> Value {
    Value::Array(
        result
            .rows
            .iter()
            .map(|row| {
                let mut object = Map::new();
                for (name, value) in result.columns.iter().zip(row) {
                    object.insert(
                        name.clone(),
                        value.clone().map_or(Value::Null, Value::String),
                    );
                }
                Value::Object(object)
            })
            .collect(),
    )
}

fn console(shared: &Shared, command: &str) -> QueryResult {
    crate::console::handle(shared, command).unwrap_or_default()
}

/// Everything the dashboard shows, as JSON.
fn snapshot(shared: &Shared) -> Value {
    let cache = shared.cache.stats();
    json!({
        "home_region": shared.router.topology().home_region,
        "uptime_seconds": shared.metrics.uptime().as_secs(),
        "counters": shared.metrics.summary(),
        "regions": rows_as_json(&console(shared, "SHOW REGIONS")),
        "pools": rows_as_json(&console(shared, "SHOW POOLS")),
        "in_doubt": rows_as_json(&console(shared, "SHOW IN DOUBT")),
        "health": rows_as_json(&console(shared, "SHOW HEALTH")),
        "directory_keys": shared.directory.snapshot().len(),
        "statement_cache": {
            "hits": cache.hits,
            "misses": cache.misses,
            "unverifiable": cache.unverifiable,
            "evictions": cache.evictions,
            "entries": shared.cache.len(),
        },
    })
}

/// Gauges read from elsewhere at the moment of scraping.
fn gauges(shared: &Shared) -> Vec<Gauge> {
    let pools = shared.pools.stats();
    let per_pool = |name: &'static str,
                    help: &'static str,
                    value: fn(&luxbyte_pool::PoolStats) -> u64| Gauge {
        name,
        help,
        values: pools
            .iter()
            .map(|(target, stats)| (vec![("backend", target.to_string())], value(stats)))
            .collect(),
    };
    let cache = shared.cache.stats();
    let single = |name: &'static str, help: &'static str, value: u64| Gauge {
        name,
        help,
        values: vec![(Vec::new(), value)],
    };
    vec![
        per_pool(
            "luxbyte_pool_idle",
            "Idle connections in each pool.",
            |stats| u64::try_from(stats.idle).unwrap_or(u64::MAX),
        ),
        per_pool(
            "luxbyte_pool_opened",
            "Connections each pool has opened.",
            |stats| stats.opened,
        ),
        per_pool(
            "luxbyte_pool_reused",
            "Borrows served from each pool's idle list.",
            |stats| stats.reused,
        ),
        per_pool(
            "luxbyte_pool_discarded",
            "Connections each pool retired as unusable.",
            |stats| stats.discarded,
        ),
        per_pool(
            "luxbyte_pool_timeouts",
            "Borrows that gave up waiting.",
            |stats| stats.timeouts,
        ),
        single(
            "luxbyte_cache_hits",
            "Statement shapes served from the analysis cache.",
            cache.hits,
        ),
        single(
            "luxbyte_cache_misses",
            "Statement shapes analysed in full.",
            cache.misses,
        ),
        single(
            "luxbyte_directory_keys",
            "Keys in the directory.",
            u64::try_from(shared.directory.snapshot().len()).unwrap_or(u64::MAX),
        ),
        single(
            "luxbyte_in_doubt_transactions",
            "Two-phase commits decided but not yet acknowledged everywhere.",
            u64::try_from(console(shared, "SHOW IN DOUBT").rows.len()).unwrap_or(u64::MAX),
        ),
        single(
            "luxbyte_reads_kept_off_replicas",
            "Reads sent to a primary because no replica was up, close enough, and caught up \
             with the session's writes.",
            shared.health.kept_off_replicas(),
        ),
        single(
            "luxbyte_read_failovers",
            "Reads that went to a primary after their replica could not be reached.",
            shared.health.failovers(),
        ),
        health_gauge(
            shared,
            "luxbyte_endpoint_up",
            "Whether each backend is up (1) or not (0).",
            |status, _| Some(u64::from(status.state == crate::health::State::Up)),
        ),
        health_gauge(
            shared,
            "luxbyte_replica_lag_bytes",
            "How far each replica is behind its primary, in bytes of WAL.",
            |status, lag| lag.filter(|_| status.replica),
        ),
    ]
}

/// A gauge with one value per backend.
fn health_gauge(
    shared: &Shared,
    name: &'static str,
    help: &'static str,
    value: fn(&crate::health::Status, Option<u64>) -> Option<u64>,
) -> Gauge {
    let topology = shared.router.topology();
    let values = shared
        .health
        .snapshot()
        .iter()
        .filter_map(|status| {
            let lag = topology.region(&status.region).and_then(|region| {
                let replica = region
                    .replicas
                    .iter()
                    .find(|replica| crate::health::key(replica) == status.endpoint)?;
                shared.health.lag(region, replica)
            });
            let labels = vec![
                ("endpoint", status.endpoint.clone()),
                ("region", status.region.clone()),
                (
                    "role",
                    if status.replica { "replica" } else { "primary" }.to_owned(),
                ),
            ];
            value(status, lag).map(|value| (labels, value))
        })
        .collect();
    Gauge { name, help, values }
}

fn key_page(shared: &Shared, table: &str, key: &str) -> Response {
    if table.is_empty() || key.is_empty() {
        return Response::text("400 Bad Request", "a table and a key are needed");
    }
    let found = crate::console::where_is(shared, table, key);
    let placement = shared
        .router
        .topology()
        .placement(None, table)
        .map(|placement| format!("{placement:?}"));
    let home = &shared.router.topology().home_region;
    let row = rows_as_json(&found);
    let region = found
        .rows
        .first()
        .and_then(|row| row.get(2).cloned().flatten())
        .unwrap_or_default();
    Response::json(&json!({
        "table": table,
        "key": key,
        "placement": placement,
        "location": row.get(0).cloned().unwrap_or(Value::Null),
        "local_to_this_proxy": region == *home,
        "home_region": home,
    }))
}

fn escape_html(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn html_table(result: &QueryResult) -> String {
    use std::fmt::Write as _;
    let mut out = String::from("<table><tr>");
    for column in &result.columns {
        let _ = write!(out, "<th>{}</th>", escape_html(column));
    }
    out.push_str("</tr>");
    if result.rows.is_empty() {
        let _ = write!(
            out,
            "<tr><td colspan=\"{}\" class=\"none\">none</td></tr>",
            result.columns.len().max(1)
        );
    }
    for row in &result.rows {
        out.push_str("<tr>");
        for value in row {
            let _ = write!(
                out,
                "<td>{}</td>",
                escape_html(value.as_deref().unwrap_or(""))
            );
        }
        out.push_str("</tr>");
    }
    out.push_str("</table>");
    out
}

/// One page, rendered on the server, refreshing itself: no script, no
/// fonts, nothing fetched from anywhere.
fn dashboard(shared: &Shared) -> String {
    let counters: Vec<String> = shared
        .metrics
        .summary()
        .into_iter()
        .map(|(name, value)| {
            format!(
                "<div class=\"tile\"><b>{value}</b><span>{}</span></div>",
                escape_html(&name.replace('_', " "))
            )
        })
        .collect();
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
         <meta http-equiv=\"refresh\" content=\"5\"><title>Luxbyte</title><style>\
         :root{{color-scheme:light dark;--fg:#1d1d1f;--bg:#fafafa;--line:#ddd;--dim:#666}}\
         @media (prefers-color-scheme:dark){{:root{{--fg:#eee;--bg:#161618;--line:#333;--dim:#999}}}}\
         body{{font:14px/1.45 system-ui,sans-serif;margin:0 auto;padding:16px;max-width:1100px;\
         color:var(--fg);background:var(--bg)}}h1{{font-size:20px}}h2{{font-size:15px;margin-top:24px}}\
         .tiles{{display:flex;flex-wrap:wrap;gap:8px}}.tile{{border:1px solid var(--line);\
         border-radius:6px;padding:8px 12px;min-width:120px}}.tile b{{display:block;font-size:18px}}\
         .tile span{{color:var(--dim);font-size:12px}}table{{border-collapse:collapse;width:100%;\
         overflow-x:auto;display:block}}th,td{{text-align:left;padding:4px 8px;\
         border-bottom:1px solid var(--line);white-space:nowrap}}.none{{color:var(--dim)}}\
         form{{display:flex;gap:8px;flex-wrap:wrap}}input{{padding:4px}}\
         </style></head><body><h1>Luxbyte · home region {home}</h1>\
         <p class=\"none\">up {uptime}s · refreshes every 5s</p>\
         <div class=\"tiles\">{counters}</div>\
         <h2>Where is a key?</h2><form action=\"/key\" method=\"get\">\
         <input name=\"table\" placeholder=\"table\" required>\
         <input name=\"key\" placeholder=\"key\" required><button>Look up</button></form>\
         <h2>Regions</h2>{regions}<h2>Pools</h2>{pools}\
         <h2>In-doubt transactions</h2>{in_doubt}</body></html>",
        home = escape_html(&shared.router.topology().home_region),
        uptime = shared.metrics.uptime().as_secs(),
        counters = counters.join(""),
        regions = html_table(&console(shared, "SHOW REGIONS")),
        pools = html_table(&console(shared, "SHOW POOLS")),
        in_doubt = html_table(&console(shared, "SHOW IN DOUBT")),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_header_schemes_carry_the_password() {
        assert_eq!(credential("Bearer s3cret").as_deref(), Some("s3cret"));
        // "op:s3cret"
        assert_eq!(credential("Basic b3A6czNjcmV0").as_deref(), Some("s3cret"));
        assert_eq!(credential("Digest whatever"), None);
        assert_eq!(credential("Basic !!!"), None);
    }

    #[test]
    fn five_wrong_answers_lock_an_address_out_and_the_lock_grows() {
        let lockout = Lockout::default();
        let address: IpAddr = "192.0.2.7".parse().unwrap_or(IpAddr::from([0, 0, 0, 0]));
        for _ in 0..4 {
            lockout.failed(address);
        }
        assert!(lockout.remaining(address).is_none());
        lockout.failed(address);
        let first = lockout.remaining(address).unwrap_or_default();
        assert!(first > Duration::from_secs(25) && first <= FIRST_LOCK);
        for _ in 0..5 {
            lockout.failed(address);
        }
        assert!(lockout.remaining(address).unwrap_or_default() > FIRST_LOCK);
        lockout.succeeded(address);
        assert!(lockout.remaining(address).is_none());
    }

    #[test]
    fn the_table_of_addresses_stays_bounded() {
        let lockout = Lockout::default();
        for n in 0..(MAX_TRACKED + 50) {
            let n = u32::try_from(n).unwrap_or(u32::MAX);
            lockout.failed(IpAddr::from(n.to_be_bytes()));
        }
        assert!(lockout.addresses.lock().len() <= MAX_TRACKED);
    }

    #[test]
    fn paths_and_queries_are_percent_decoded() {
        assert_eq!(decode("caf%C3%A9+au%20lait"), "café au lait");
        assert_eq!(decode("100%"), "100%");
        assert_eq!(
            query_value("table=users&key=a%2Fb", "key").as_deref(),
            Some("a/b")
        );
    }

    #[test]
    fn html_is_escaped() {
        assert_eq!(
            escape_html("<script>alert('x')</script>"),
            "&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt;"
        );
    }
}
