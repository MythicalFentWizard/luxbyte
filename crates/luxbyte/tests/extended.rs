//! The extended query protocol, end to end.
//!
//! This is what psycopg3, asyncpg and JDBC speak by default, so it is the
//! path most real traffic takes. The client here is `luxbyte-pool`'s own
//! pipelining connection, the same code the proxy uses towards backends.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::time::Duration;

use luxbyte::{Config, ServerHandle};
use luxbyte_pool::{Connection, ConnectionTarget, Credentials, Step, StepKind, StepReply};
use luxbyte_proto::backend::{Backend as Message, TransactionStatus};
use luxbyte_proto::frontend::{self, Format, Target};
use luxbyte_testkit::{Backend, BackendConfig, PreparedOutcome, Reply};

struct Fixture {
    eu: Backend,
    us: Backend,
    /// A replica of the home region, when the test asked for one.
    replica: Option<Backend>,
    proxy: ServerHandle,
}

async fn fixture_with(eu: BackendConfig, us: BackendConfig, replica: bool) -> Fixture {
    let eu = Backend::start(eu).await.expect("eu");
    let us = Backend::start(us).await.expect("us");
    let replica = if replica {
        Some(
            Backend::start(BackendConfig::new("eu-replica"))
                .await
                .expect("replica"),
        )
    } else {
        None
    };
    let replicas = replica.as_ref().map_or(String::new(), |backend| {
        format!(
            "replicas = [{{ host = \"127.0.0.1\", port = {} }}]",
            backend.port()
        )
    });

    let toml = format!(
        r#"
[server]
listen = "127.0.0.1:0"
node = "test-proxy"

[backend]
user = "app"
database = "shop"

[topology]
home_region = "eu-west"

[[topology.regions]]
id = "eu-west"
primary = {{ host = "127.0.0.1", port = {eu_port} }}
{replicas}

[[topology.regions]]
id = "us-east"
primary = {{ host = "127.0.0.1", port = {us_port} }}

[[topology.tables]]
table = "users"
placement = "geo_partitioned"
key = "tenant_id"

[[directory]]
table = "users"
key = "alice"
region = "eu-west"

[[directory]]
table = "users"
key = "bob"
region = "us-east"

[[directory]]
table = "users"
key = "42"
region = "us-east"
"#,
        eu_port = eu.port(),
        us_port = us.port(),
    );

    let config = Config::from_toml(&toml).expect("config should parse");
    let server = luxbyte::Server::bind(&config).await.expect("should bind");
    let proxy = server.spawn().expect("should serve");
    Fixture {
        eu,
        us,
        replica,
        proxy,
    }
}

async fn fixture() -> Fixture {
    fixture_with(
        BackendConfig::new("eu-west"),
        BackendConfig::new("us-east"),
        false,
    )
    .await
}

async fn client(proxy: &ServerHandle) -> Connection {
    Connection::connect(
        &ConnectionTarget {
            host: "127.0.0.1".to_owned(),
            port: proxy.port(),
            database: "shop".to_owned(),
            user: "app".to_owned(),
        },
        &Credentials::Trust,
        Duration::from_secs(5),
    )
    .await
    .expect("should connect to the proxy")
}

// --------------------------------------------------------------- messages

fn parse(name: &str, sql: &str) -> Step {
    parse_typed(name, sql, &[])
}

fn parse_typed(name: &str, sql: &str, types: &[i32]) -> Step {
    Step::new(StepKind::Parse, frontend::parse(name, sql, types).unwrap())
}

fn bind(portal: &str, statement: &str, values: &[&str]) -> Step {
    let values: Vec<Option<&[u8]>> = values.iter().map(|v| Some(v.as_bytes())).collect();
    Step::new(
        StepKind::Bind,
        frontend::bind(portal, statement, &[], &values, &[]).unwrap(),
    )
}

fn bind_binary(portal: &str, statement: &str, value: &[u8]) -> Step {
    Step::new(
        StepKind::Bind,
        frontend::bind(portal, statement, &[Format::Binary], &[Some(value)], &[]).unwrap(),
    )
}

fn describe_portal(name: &str) -> Step {
    Step::new(
        StepKind::DescribePortal,
        frontend::describe(Target::Portal, name).unwrap(),
    )
}

fn describe_statement(name: &str) -> Step {
    Step::new(
        StepKind::DescribeStatement,
        frontend::describe(Target::Statement, name).unwrap(),
    )
}

fn execute(portal: &str, max_rows: i32) -> Step {
    Step::new(
        StepKind::Execute,
        frontend::execute(portal, max_rows).unwrap(),
    )
}

fn sync() -> Step {
    Step::new(StepKind::Sync, frontend::sync().unwrap())
}

/// Each step's answer as its tag bytes, so a test can see exactly what the
/// client was told and in which order.
fn tags(replies: &[StepReply]) -> Vec<String> {
    replies
        .iter()
        .map(|reply| {
            reply
                .messages
                .iter()
                .map(|message| char::from(message.tag.unwrap_or(b'?')))
                .collect()
        })
        .collect()
}

/// The SQLSTATE of the first error in the answers.
fn error_code(replies: &[StepReply]) -> Option<String> {
    replies
        .iter()
        .flat_map(|reply| &reply.messages)
        .find_map(|message| match Message::decode(message) {
            Ok(Message::Error(notice)) => Some(notice.code().to_owned()),
            _ => None,
        })
}

fn error_message(replies: &[StepReply]) -> String {
    replies
        .iter()
        .flat_map(|reply| &reply.messages)
        .find_map(|message| match Message::decode(message) {
            Ok(Message::Error(notice)) => Some(notice.message().to_owned()),
            _ => None,
        })
        .unwrap_or_default()
}

/// What a backend rolled back because its connection went away.
///
/// The proxy answers the client as soon as it has dropped the connection;
/// the backend notices the hang-up a moment later.
async fn abandoned(backend: &Backend) -> Vec<String> {
    for _ in 0..200 {
        let abandoned = backend.abandoned();
        if !abandoned.is_empty() {
            return abandoned;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Vec::new()
}

const SELECT_USER: &str = "SELECT id FROM users WHERE tenant_id = $1";
const INSERT_USER: &str = "INSERT INTO users (tenant_id, name) VALUES ($1, 'x')";

// ------------------------------------------------------------------ routing

#[tokio::test]
async fn a_prepared_statement_routes_on_the_key_bound_to_it() {
    let f = fixture().await;
    let mut client = client(&f.proxy).await;

    let replies = client
        .pipeline(&[
            parse("", SELECT_USER),
            bind("", "", &["bob"]),
            describe_portal(""),
            execute("", 0),
            sync(),
        ])
        .await
        .expect("pipeline");

    // Exactly what PostgreSQL would have said, message for message.
    assert_eq!(tags(&replies), ["1", "2", "n", "C", "Z"]);
    assert_eq!(client.status(), TransactionStatus::Idle);
    // The key arrived in the Bind, and the Bind is what routed it.
    assert_eq!(
        f.us.bound(),
        [(SELECT_USER.to_owned(), vec![Some("bob".to_owned())])]
    );
    assert!(!f.eu.saw(SELECT_USER));
}

#[tokio::test]
async fn rows_come_back_through_the_extended_protocol() {
    let f = fixture_with(
        BackendConfig::new("eu-west").reply_to(
            "FROM users",
            Reply::rows(&["id", "name"], &[&["1", "ada"], &["2", "grace"]]),
        ),
        BackendConfig::new("us-east"),
        false,
    )
    .await;
    let mut client = client(&f.proxy).await;

    let replies = client
        .pipeline(&[
            parse("", SELECT_USER),
            bind("", "", &["alice"]),
            describe_portal(""),
            execute("", 0),
            sync(),
        ])
        .await
        .expect("pipeline");
    assert_eq!(tags(&replies), ["1", "2", "T", "DDC", "Z"]);
    let row = replies
        .get(3)
        .and_then(|reply| reply.messages.first())
        .expect("a row");
    let Ok(Message::DataRow { columns }) = Message::decode(row) else {
        panic!("expected a row");
    };
    assert_eq!(columns, [Some(&b"1"[..]), Some(&b"ada"[..])]);
}

#[tokio::test]
async fn a_named_statement_prepared_once_runs_wherever_each_bind_sends_it() {
    // The client prepares once; the proxy must make the statement exist on
    // whichever backend connection each later Bind is routed to.
    let f = fixture().await;
    let mut client = client(&f.proxy).await;

    let replies = client
        .pipeline(&[parse("by_tenant", SELECT_USER), sync()])
        .await
        .expect("prepare");
    assert_eq!(tags(&replies), ["1", "Z"]);

    for tenant in ["alice", "bob", "alice"] {
        let replies = client
            .pipeline(&[bind("", "by_tenant", &[tenant]), execute("", 0), sync()])
            .await
            .expect("execute");
        assert_eq!(tags(&replies), ["2", "C", "Z"], "{tenant}");
    }
    assert_eq!(f.eu.count_of(SELECT_USER), 2);
    assert_eq!(f.us.count_of(SELECT_USER), 1);
}

#[tokio::test]
async fn a_statement_bound_many_times_in_one_batch_is_parsed_once() {
    let f = fixture().await;
    let mut client = client(&f.proxy).await;

    let replies = client
        .pipeline(&[
            parse("ins", INSERT_USER),
            bind("", "ins", &["alice"]),
            execute("", 0),
            bind("", "ins", &["alice"]),
            execute("", 0),
            bind("", "ins", &["alice"]),
            execute("", 0),
            sync(),
        ])
        .await
        .expect("pipeline");
    assert_eq!(tags(&replies), ["1", "2", "C", "2", "C", "2", "C", "Z"]);
    assert_eq!(f.eu.count_of(INSERT_USER), 3);
    assert_eq!(f.eu.parses(), 1);
    assert!(f.eu.abandoned().is_empty());
}

#[tokio::test]
async fn a_named_statement_is_parsed_once_per_backend_connection() {
    let f = fixture().await;
    let mut client = client(&f.proxy).await;
    client
        .pipeline(&[parse("by_tenant", SELECT_USER), sync()])
        .await
        .expect("prepare");

    for _ in 0..5 {
        let replies = client
            .pipeline(&[bind("", "by_tenant", &["alice"]), execute("", 0), sync()])
            .await
            .expect("execute");
        assert_eq!(tags(&replies), ["2", "C", "Z"]);
    }
    assert_eq!(f.eu.count_of(SELECT_USER), 5);
    // Every batch borrowed the same pooled connection, which kept it.
    assert_eq!(f.eu.connections_accepted(), 1);
    assert_eq!(f.eu.parses(), 1);
}

#[tokio::test]
async fn a_backend_copy_older_than_the_clients_statement_is_prepared_afresh() {
    // A schema change between the two Parses must not reach the second
    // client as a broken cached plan, so its statement gets a fresh copy;
    // from then on both clients share it.
    let f = fixture().await;
    let mut first = client(&f.proxy).await;
    let mut second = client(&f.proxy).await;
    let run = [bind("", "q", &["alice"]), execute("", 0), sync()];

    first
        .pipeline(&[parse("q", SELECT_USER), sync()])
        .await
        .unwrap();
    assert_eq!(tags(&first.pipeline(&run).await.unwrap()), ["2", "C", "Z"]);
    assert_eq!(f.eu.parses(), 1);

    second
        .pipeline(&[parse("q", SELECT_USER), sync()])
        .await
        .unwrap();
    assert_eq!(tags(&second.pipeline(&run).await.unwrap()), ["2", "C", "Z"]);
    assert_eq!(f.eu.parses(), 2, "parsed after the second client's Parse");

    for client in [&mut first, &mut second] {
        assert_eq!(tags(&client.pipeline(&run).await.unwrap()), ["2", "C", "Z"]);
    }
    assert_eq!(f.eu.parses(), 2);
    assert_eq!(f.eu.connections_accepted(), 1);
}

#[tokio::test]
async fn an_unnamed_statement_stays_unnamed_and_is_parsed_each_time() {
    // A client that did not name a statement asked for a one-off plan.
    let f = fixture().await;
    let mut client = client(&f.proxy).await;
    for _ in 0..3 {
        let replies = client
            .pipeline(&[
                parse("", SELECT_USER),
                bind("", "", &["alice"]),
                execute("", 0),
                sync(),
            ])
            .await
            .expect("execute");
        assert_eq!(tags(&replies), ["1", "2", "C", "Z"]);
    }
    assert_eq!(f.eu.parses(), 3);
}

#[tokio::test]
async fn a_broken_plan_fails_as_on_one_server_until_the_client_prepares_again() {
    // PostgreSQL refuses a cached plan whose result type a schema change
    // altered, every time, until the client prepares the statement again;
    // drivers rely on that to know when to. The proxy neither hides the
    // error nor clears it behind the client's back.
    let f = fixture_with(
        BackendConfig::new("eu-west").reply_to("boom", Reply::fail("0A000", "cached plan")),
        BackendConfig::new("us-east"),
        false,
    )
    .await;
    let mut client = client(&f.proxy).await;
    let sql = "SELECT id FROM users WHERE tenant_id = $1 AND name <> 'boom'";
    let run = [bind("", "stale", &["alice"]), execute("", 0), sync()];
    client
        .pipeline(&[parse("stale", sql), sync()])
        .await
        .expect("prepare");

    for _ in 0..2 {
        let replies = client.pipeline(&run).await.expect("execute");
        assert_eq!(error_code(&replies).as_deref(), Some("0A000"));
        assert_eq!(f.eu.parses(), 1);
    }

    client
        .pipeline(&[
            Step::new(
                StepKind::Close,
                frontend::close(Target::Statement, "stale").unwrap(),
            ),
            parse("stale", sql),
            sync(),
        ])
        .await
        .expect("prepare again");
    client.pipeline(&run).await.expect("execute");
    assert_eq!(f.eu.parses(), 2, "prepared afresh for the new Parse");
    assert_eq!(f.eu.connections_accepted(), 1);
}

#[tokio::test]
async fn a_batch_never_sent_leaves_no_statement_behind() {
    // The client hangs up before its Sync: nothing reached the backend, so
    // the next session to bind the same statement must still prepare it.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let f = fixture().await;
    let mut quitter = tokio::net::TcpStream::connect(("127.0.0.1", f.proxy.port()))
        .await
        .unwrap();
    let mut startup = luxbyte_proto::startup::StartupParameters::new();
    startup.set("user", "app").set("database", "shop");
    quitter.write_all(&startup.encode().unwrap()).await.unwrap();
    // Read up to the first ReadyForQuery.
    loop {
        let tag = quitter.read_u8().await.unwrap();
        let length = quitter.read_i32().await.unwrap();
        let mut body = vec![0_u8; usize::try_from(length - 4).unwrap()];
        quitter.read_exact(&mut body).await.unwrap();
        if tag == b'Z' {
            break;
        }
    }
    let mut batch = Vec::new();
    for step in [
        parse("q", SELECT_USER),
        bind("", "q", &["alice"]),
        execute("", 0),
    ] {
        batch.extend_from_slice(&step.message);
    }
    quitter.write_all(&batch).await.unwrap();
    // The Bind borrowed a connection; wait for it to go back to the pool
    // when the session ends, so the next session gets the same one.
    while f.eu.connections_accepted() == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    drop(quitter);
    let started = std::time::Instant::now();
    while f
        .proxy
        .shared()
        .pools
        .stats()
        .iter()
        .all(|(_, stats)| stats.idle == 0)
    {
        assert!(started.elapsed() < Duration::from_secs(5), "never returned");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let mut client = client(&f.proxy).await;
    let replies = client
        .pipeline(&[
            parse("q", SELECT_USER),
            bind("", "q", &["alice"]),
            execute("", 0),
            sync(),
        ])
        .await
        .expect("execute");
    assert_eq!(tags(&replies), ["1", "2", "C", "Z"]);
    assert_eq!(f.eu.connections_accepted(), 1);
    assert_eq!(f.eu.parses(), 1);
}

#[tokio::test]
async fn a_binary_key_routes_when_its_type_is_declared() {
    let f = fixture().await;
    let mut client = client(&f.proxy).await;

    // `int4` 42, which the directory places in us-east.
    let replies = client
        .pipeline(&[
            parse_typed("", SELECT_USER, &[luxbyte_proto::types::INT4]),
            bind_binary("", "", &42_i32.to_be_bytes()),
            execute("", 0),
            sync(),
        ])
        .await
        .expect("pipeline");
    assert_eq!(tags(&replies), ["1", "2", "C", "Z"]);
    assert!(f.us.saw(SELECT_USER));
    assert!(!f.eu.saw(SELECT_USER));
}

#[tokio::test]
async fn a_binary_key_routes_once_a_describe_has_reported_its_type() {
    // asyncpg's pattern: prepare without types, ask the server what they
    // are, then send every parameter in binary.
    let f = fixture().await;
    let mut client = client(&f.proxy).await;

    let replies = client
        .pipeline(&[parse("s", SELECT_USER), describe_statement("s"), sync()])
        .await
        .expect("describe");
    assert_eq!(tags(&replies), ["1", "tn", "Z"]);

    let replies = client
        .pipeline(&[bind_binary("", "s", b"bob"), execute("", 0), sync()])
        .await
        .expect("execute");
    assert_eq!(tags(&replies), ["2", "C", "Z"]);
    assert!(f.us.saw(SELECT_USER));
}

#[tokio::test]
async fn a_binary_key_of_unknown_type_is_written_everywhere_rather_than_guessed() {
    let f = fixture().await;
    let mut client = client(&f.proxy).await;

    // No declared type and no Describe: the proxy cannot read the key, so
    // the write goes to every region, where it changes only the rows that
    // match — and commits in two phases.
    let replies = client
        .pipeline(&[
            parse("", "UPDATE users SET name = 'x' WHERE tenant_id = $1"),
            bind_binary("", "", b"bob"),
            execute("", 0),
            sync(),
        ])
        .await
        .expect("pipeline");
    assert_eq!(tags(&replies), ["1", "2", "C", "Z"]);
    for backend in [&f.eu, &f.us] {
        assert!(backend.saw("UPDATE users SET name = 'x' WHERE tenant_id = (E'bob'::text)"));
        assert_eq!(backend.prepared_transactions().len(), 1);
    }
}

// ------------------------------------------------------------------ errors

#[tokio::test]
async fn an_error_discards_the_rest_of_the_batch_and_its_statements() {
    let f = fixture_with(
        BackendConfig::new("eu-west").reply_to("boom", Reply::fail("23505", "duplicate key")),
        BackendConfig::new("us-east"),
        false,
    )
    .await;
    let mut client = client(&f.proxy).await;

    let replies = client
        .pipeline(&[
            parse(
                "",
                "INSERT INTO users (tenant_id, name) VALUES ($1, 'boom')",
            ),
            bind("", "", &["alice"]),
            execute("", 0),
            parse("later", "SELECT 1"),
            bind("", "later", &[]),
            execute("", 0),
            sync(),
        ])
        .await
        .expect("pipeline");
    assert_eq!(tags(&replies), ["1", "2", "E", "", "", "", "Z"]);
    assert_eq!(error_code(&replies).as_deref(), Some("23505"));
    assert!(!f.eu.saw("SELECT 1"));

    // PostgreSQL never created "later", so preparing it again succeeds.
    let replies = client
        .pipeline(&[parse("later", "SELECT 1"), sync()])
        .await
        .expect("prepare");
    assert_eq!(tags(&replies), ["1", "Z"]);

    // The backend saw the error itself and rolled back at the Sync, so its
    // connection is clean and goes back to the pool rather than being
    // dropped.
    client
        .pipeline(&[
            parse("", SELECT_USER),
            bind("", "", &["alice"]),
            execute("", 0),
            sync(),
        ])
        .await
        .expect("next batch");
    assert_eq!(f.eu.connections_accepted(), 1);
    assert!(f.eu.abandoned().is_empty());
}

#[tokio::test]
async fn a_batch_writing_two_regions_outside_a_transaction_is_refused_and_rolled_back() {
    let f = fixture().await;
    let mut client = client(&f.proxy).await;

    let replies = client
        .pipeline(&[
            parse("ins", INSERT_USER),
            bind("", "ins", &["alice"]),
            execute("", 0),
            bind("", "ins", &["bob"]),
            execute("", 0),
            sync(),
        ])
        .await
        .expect("pipeline");
    // The first insert ran, then the second was refused...
    assert_eq!(tags(&replies), ["1", "2", "C", "E", "", "Z"]);
    assert_eq!(error_code(&replies).as_deref(), Some("0A000"));
    assert!(error_message(&replies).contains("more than one region"));
    assert_eq!(client.status(), TransactionStatus::Idle);
    // ...and so the first was rolled back: its connection was dropped before
    // the Sync that would have committed it.
    assert_eq!(abandoned(&f.eu).await, [INSERT_USER.to_owned()]);
    assert!(!f.us.saw(INSERT_USER));
}

#[tokio::test]
async fn a_batch_may_read_several_regions_outside_a_transaction() {
    let f = fixture().await;
    let mut client = client(&f.proxy).await;

    let replies = client
        .pipeline(&[
            parse("s", SELECT_USER),
            bind("", "s", &["alice"]),
            execute("", 0),
            bind("", "s", &["bob"]),
            execute("", 0),
            sync(),
        ])
        .await
        .expect("pipeline");
    assert_eq!(tags(&replies), ["1", "2", "C", "2", "C", "Z"]);
    assert!(f.eu.saw(SELECT_USER));
    assert!(f.us.saw(SELECT_USER));
}

#[tokio::test]
async fn an_error_in_one_region_stops_the_batch_before_the_next_region() {
    // The failure only comes to light when the batch moves on to us-east
    // and the eu-west work is sent. Nothing after it may reach us-east.
    let f = fixture_with(
        BackendConfig::new("eu-west").reply_to("FROM users", Reply::fail("57014", "canceled")),
        BackendConfig::new("us-east"),
        false,
    )
    .await;
    let mut client = client(&f.proxy).await;

    let replies = client
        .pipeline(&[
            parse("s", SELECT_USER),
            bind("", "s", &["alice"]),
            execute("", 0),
            bind("", "s", &["bob"]),
            execute("", 0),
            sync(),
        ])
        .await
        .expect("pipeline");
    assert_eq!(tags(&replies), ["1", "2", "E", "", "", "Z"]);
    assert_eq!(error_code(&replies).as_deref(), Some("57014"));
    assert_eq!(f.us.parses(), 0);
    assert!(!f.us.saw(SELECT_USER));
}

// ------------------------------------------------------- across regions

fn bind_formats(portal: &str, statement: &str, values: &[&str], results: &[Format]) -> Step {
    let values: Vec<Option<&[u8]>> = values.iter().map(|v| Some(v.as_bytes())).collect();
    Step::new(
        StepKind::Bind,
        frontend::bind(portal, statement, &[], &values, results).unwrap(),
    )
}

/// The values of every `DataRow` in the answers, as text.
fn data(replies: &[StepReply]) -> Vec<Vec<String>> {
    replies
        .iter()
        .flat_map(|reply| &reply.messages)
        .filter_map(|message| match Message::decode(message) {
            Ok(Message::DataRow { columns }) => Some(
                columns
                    .iter()
                    .map(|value| String::from_utf8_lossy(value.unwrap_or_default()).into_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

fn ids(region: &str, ids: &[&str]) -> BackendConfig {
    let rows: Vec<Vec<Option<&str>>> = ids.iter().map(|id| vec![Some(*id)]).collect();
    let rows: Vec<&[Option<&str>]> = rows.iter().map(Vec::as_slice).collect();
    BackendConfig::new(region).reply_to("SELECT id FROM users", Reply::typed(&[("id", 23)], &rows))
}

#[tokio::test]
async fn a_read_across_regions_is_combined_over_the_extended_protocol() {
    let f = fixture_with(
        ids("eu-west", &["1", "4"]),
        ids("us-east", &["2", "3"]),
        false,
    )
    .await;
    let mut client = client(&f.proxy).await;

    let replies = client
        .pipeline(&[
            // A limit is a bigint, as PostgreSQL would infer.
            parse_typed("", "SELECT id FROM users ORDER BY id LIMIT $1", &[20]),
            bind("", "", &["3"]),
            describe_portal(""),
            execute("", 0),
            sync(),
        ])
        .await
        .expect("pipeline");
    assert_eq!(tags(&replies), ["1", "2", "T", "DDDC", "Z"]);
    // Merged in order across the regions, then limited — the bound limit
    // written in as a literal of its own type.
    assert_eq!(data(&replies), [["1"], ["2"], ["3"]]);
    assert!(f.us.saw("ORDER BY id LIMIT 3"));
}

#[tokio::test]
async fn a_combined_read_can_be_fetched_a_few_rows_at_a_time() {
    let f = fixture_with(ids("eu-west", &["1"]), ids("us-east", &["2"]), false).await;
    let mut client = client(&f.proxy).await;

    client.simple_query("BEGIN").await.unwrap();
    let replies = client
        .pipeline(&[
            parse("", "SELECT id FROM users"),
            bind("p", "", &[]),
            execute("p", 1),
            sync(),
        ])
        .await
        .expect("first part");
    assert_eq!(tags(&replies), ["1", "2", "Ds", "Z"]);
    let replies = client
        .pipeline(&[execute("p", 1), sync()])
        .await
        .expect("second part");
    assert_eq!(tags(&replies), ["Ds", "Z"]);
    let replies = client
        .pipeline(&[execute("p", 1), sync()])
        .await
        .expect("the end");
    assert_eq!(tags(&replies), ["C", "Z"]);
    client.simple_query("COMMIT").await.unwrap();
}

#[tokio::test]
async fn binary_results_across_regions_are_encoded_by_the_home_region() {
    let f = fixture_with(
        ids("eu-west", &["1"]).reply_to(
            "WITH ORDINALITY",
            Reply::typed(&[("c1", 23)], &[&[Some("binary-1")], &[Some("binary-2")]]),
        ),
        ids("us-east", &["2"]),
        false,
    )
    .await;
    let mut client = client(&f.proxy).await;
    let replies = client
        .pipeline(&[
            parse("", "SELECT id FROM users"),
            bind_formats("", "", &[], &[Format::Binary]),
            execute("", 0),
            sync(),
        ])
        .await
        .expect("pipeline");
    assert_eq!(tags(&replies), ["1", "2", "DDC", "Z"]);
    // The rows carried home as typed arrays, in order, and encoded there.
    assert!(f.eu.saw(
        "SELECT p.c1 FROM unnest(E'{\"1\",\"2\"}'::int4[]) WITH ORDINALITY AS p(c1, n) ORDER BY p.n"
    ));
    assert_eq!(data(&replies), [["binary-1"], ["binary-2"]]);
}

#[tokio::test]
async fn a_write_across_regions_outside_a_transaction_commits_with_its_batch_in_two_phases() {
    let f = fixture_with(
        BackendConfig::new("eu-west").reply_to(
            "UPDATE users",
            Reply::Done {
                tag: "UPDATE 2".to_owned(),
            },
        ),
        BackendConfig::new("us-east").reply_to(
            "UPDATE users",
            Reply::Done {
                tag: "UPDATE 3".to_owned(),
            },
        ),
        false,
    )
    .await;
    let mut client = client(&f.proxy).await;
    let replies = client
        .pipeline(&[
            parse("", "UPDATE users SET name = $1"),
            bind("", "", &["it's \\ fine"]),
            execute("", 0),
            parse("", SELECT_USER),
            bind("", "", &["bob"]),
            execute("", 0),
            sync(),
        ])
        .await
        .expect("pipeline");
    assert_eq!(tags(&replies), ["1", "2", "C", "1", "2", "C", "Z"]);
    let complete = replies
        .get(2)
        .and_then(|reply| reply.messages.first())
        .map(|message| match Message::decode(message) {
            Ok(Message::CommandComplete { tag }) => tag.to_owned(),
            other => panic!("{other:?}"),
        });
    assert_eq!(complete.as_deref(), Some("UPDATE 5"));
    assert_eq!(client.status(), TransactionStatus::Idle);
    for backend in [&f.eu, &f.us] {
        // Quotes and backslashes survive the trip as a literal.
        assert!(backend.saw("UPDATE users SET name = (E'it''s \\\\ fine'::text)"));
        let prepared = backend.prepared_transactions();
        assert_eq!(prepared.len(), 1, "{prepared:?}");
        assert_eq!(prepared[0].1, PreparedOutcome::Committed);
    }
    // The read that followed ran inside the same transaction.
    assert!(f.us.saw(SELECT_USER));
}

#[tokio::test]
async fn a_failure_later_in_the_batch_rolls_back_a_write_across_regions() {
    let f = fixture_with(
        BackendConfig::new("eu-west").reply_to("boom", Reply::fail("23505", "duplicate key")),
        BackendConfig::new("us-east"),
        false,
    )
    .await;
    let mut client = client(&f.proxy).await;
    let replies = client
        .pipeline(&[
            parse("", "UPDATE users SET name = 'x'"),
            bind("", "", &[]),
            execute("", 0),
            parse("", "UPDATE users SET name = 'boom' WHERE tenant_id = $1"),
            bind("", "", &["alice"]),
            execute("", 0),
            sync(),
        ])
        .await
        .expect("pipeline");
    assert_eq!(error_code(&replies).as_deref(), Some("23505"));
    for backend in [&f.eu, &f.us] {
        assert!(backend.saw("UPDATE users SET name = 'x'"));
        assert!(backend.prepared_transactions().is_empty());
        assert!(backend.saw("ROLLBACK"));
    }
    assert_eq!(client.status(), TransactionStatus::Idle);
    assert!(client.simple_query("SELECT 1").await.is_ok());
}

#[tokio::test]
async fn a_multi_row_insert_with_parameters_is_split_by_key() {
    let f = fixture().await;
    let mut client = client(&f.proxy).await;
    let replies = client
        .pipeline(&[
            parse(
                "",
                "INSERT INTO users (tenant_id, name) VALUES ($1, 'a'), ($2, 'b')",
            ),
            bind("", "", &["alice", "bob"]),
            execute("", 0),
            sync(),
        ])
        .await
        .expect("pipeline");
    assert_eq!(tags(&replies), ["1", "2", "C", "Z"]);
    assert!(f.eu.saw("VALUES ((E'alice'::TEXT), 'a')"));
    assert!(!f.eu.saw("bob"));
    assert!(f.us.saw("VALUES ((E'bob'::TEXT), 'b')"));
    assert!(!f.us.saw("'alice'"));
}

#[tokio::test]
async fn a_batch_that_wrote_outside_a_transaction_cannot_then_write_elsewhere() {
    let f = fixture().await;
    let mut client = client(&f.proxy).await;
    let replies = client
        .pipeline(&[
            parse("", INSERT_USER),
            bind("", "", &["alice"]),
            execute("", 0),
            parse("", "UPDATE users SET name = 'y'"),
            bind("", "", &[]),
            execute("", 0),
            sync(),
        ])
        .await
        .expect("pipeline");
    assert_eq!(error_code(&replies).as_deref(), Some("0A000"));
    assert!(error_message(&replies).contains("has written outside a transaction"));
    assert!(!f.us.saw("UPDATE"));
}

#[tokio::test]
async fn inside_a_transaction_a_write_across_regions_waits_for_commit() {
    let f = fixture().await;
    let mut client = client(&f.proxy).await;
    client.simple_query("BEGIN").await.unwrap();
    let replies = client
        .pipeline(&[
            parse("", "DELETE FROM users WHERE name = $1"),
            bind("", "", &["gone"]),
            execute("", 0),
            sync(),
        ])
        .await
        .expect("pipeline");
    assert_eq!(tags(&replies), ["1", "2", "C", "Z"]);
    assert_eq!(client.status(), TransactionStatus::InTransaction);
    assert!(f.eu.prepared_transactions().is_empty());
    client.simple_query("COMMIT").await.unwrap();
    for backend in [&f.eu, &f.us] {
        assert!(backend.saw("DELETE FROM users WHERE name = (E'gone'::text)"));
        assert_eq!(backend.prepared_transactions().len(), 1);
    }
}

#[tokio::test]
async fn several_commands_in_one_prepared_statement_are_refused() {
    let f = fixture().await;
    let mut client = client(&f.proxy).await;
    let replies = client
        .pipeline(&[parse("", "SELECT 1; SELECT 2"), sync()])
        .await
        .expect("pipeline");
    assert_eq!(tags(&replies), ["E", "Z"]);
    assert_eq!(error_code(&replies).as_deref(), Some("42601"));
}

#[tokio::test]
async fn binding_a_statement_that_does_not_exist_is_an_error_not_a_hang() {
    let f = fixture().await;
    let mut client = client(&f.proxy).await;
    let replies = tokio::time::timeout(
        Duration::from_secs(5),
        client.pipeline(&[bind("", "nope", &[]), execute("", 0), sync()]),
    )
    .await
    .expect("must not hang")
    .expect("pipeline");
    assert_eq!(tags(&replies), ["E", "", "Z"]);
    assert_eq!(error_code(&replies).as_deref(), Some("26000"));
}

// ------------------------------------------------------------ transactions

#[tokio::test]
async fn begin_and_commit_sent_as_prepared_statements_work() {
    // JDBC sends BEGIN through the extended protocol, in the same batch as
    // the first statement.
    let f = fixture().await;
    let mut client = client(&f.proxy).await;

    let replies = client
        .pipeline(&[
            parse("", "BEGIN"),
            bind("", "", &[]),
            execute("", 0),
            parse("", INSERT_USER),
            bind("", "", &["alice"]),
            execute("", 0),
            sync(),
        ])
        .await
        .expect("pipeline");
    assert_eq!(tags(&replies), ["1", "2", "C", "1", "2", "C", "Z"]);
    assert_eq!(client.status(), TransactionStatus::InTransaction);

    let replies = client
        .pipeline(&[
            parse("", "COMMIT"),
            bind("", "", &[]),
            execute("", 0),
            sync(),
        ])
        .await
        .expect("commit");
    assert_eq!(tags(&replies), ["1", "2", "C", "Z"]);
    assert_eq!(client.status(), TransactionStatus::Idle);
    assert!(f.eu.saw("BEGIN"));
    assert!(f.eu.saw(INSERT_USER));
    assert!(f.eu.saw("COMMIT"));
    assert!(f.eu.abandoned().is_empty());
}

#[tokio::test]
async fn a_batch_writing_two_regions_inside_a_transaction_commits_in_two_phases() {
    let f = fixture().await;
    let mut client = client(&f.proxy).await;

    client.simple_query("BEGIN").await.expect("begin");
    let replies = client
        .pipeline(&[
            parse("ins", INSERT_USER),
            bind("", "ins", &["alice"]),
            execute("", 0),
            bind("", "ins", &["bob"]),
            execute("", 0),
            sync(),
        ])
        .await
        .expect("pipeline");
    assert_eq!(tags(&replies), ["1", "2", "C", "2", "C", "Z"]);
    client.simple_query("COMMIT").await.expect("commit");

    for backend in [&f.eu, &f.us] {
        let prepared = backend.prepared_transactions();
        assert_eq!(prepared.len(), 1, "{}", backend.name());
        assert!(
            prepared
                .iter()
                .all(|(_, outcome)| *outcome == PreparedOutcome::Committed)
        );
    }
}

#[tokio::test]
async fn a_failed_extended_statement_aborts_the_transaction_block() {
    let f = fixture_with(
        BackendConfig::new("eu-west").reply_to("boom", Reply::fail("23505", "duplicate key")),
        BackendConfig::new("us-east"),
        false,
    )
    .await;
    let mut client = client(&f.proxy).await;

    client.simple_query("BEGIN").await.expect("begin");
    let replies = client
        .pipeline(&[
            parse("", "UPDATE users SET name = 'boom' WHERE tenant_id = $1"),
            bind("", "", &["alice"]),
            execute("", 0),
            sync(),
        ])
        .await
        .expect("pipeline");
    assert_eq!(tags(&replies), ["1", "2", "E", "Z"]);
    assert_eq!(client.status(), TransactionStatus::Failed);

    let replies = client
        .pipeline(&[parse("", SELECT_USER), bind("", "", &["alice"]), sync()])
        .await
        .expect("blocked");
    // Refused at the Parse already, as PostgreSQL does.
    assert_eq!(tags(&replies), ["E", "", "Z"]);
    assert_eq!(error_code(&replies).as_deref(), Some("25P02"));

    let result = client.simple_query("COMMIT").await.expect("commit");
    // A failed block cannot commit; PostgreSQL rolls it back and says so.
    assert_eq!(result.tags.last().map(String::as_str), Some("ROLLBACK"));
    assert_eq!(client.status(), TransactionStatus::Idle);
}

#[tokio::test]
async fn committing_after_a_refusal_rolls_back_instead() {
    // The refusal never reached a backend, so every backend is healthy and
    // would happily commit. The client was told the block had failed, so it
    // must not.
    let f = fixture().await;
    let mut client = client(&f.proxy).await;

    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("UPDATE users SET name = 'x' WHERE tenant_id = 'alice'")
        .await
        .expect("update");
    let _ = client
        .simple_query("SELECT count(*) FROM users")
        .await
        .expect_err("an aggregate across regions is refused");
    let result = client.simple_query("COMMIT").await.expect("commit");

    assert_eq!(result.tags.last().map(String::as_str), Some("ROLLBACK"));
    assert!(f.eu.saw("ROLLBACK"));
    assert!(!f.eu.saw("COMMIT"));
}

#[tokio::test]
async fn a_transaction_that_reads_first_still_writes_to_the_primary() {
    let f = fixture_with(
        BackendConfig::new("eu-west"),
        BackendConfig::new("us-east"),
        true,
    )
    .await;
    let replica = f.replica.as_ref().expect("replica");
    let mut client = client(&f.proxy).await;

    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("SELECT id FROM users WHERE tenant_id = 'alice'")
        .await
        .expect("read");
    client
        .simple_query("UPDATE users SET name = 'x' WHERE tenant_id = 'alice'")
        .await
        .expect("write");
    client.simple_query("COMMIT").await.expect("commit");

    // A replica would have refused the UPDATE: the participant has to be
    // the primary from the start.
    assert!(f.eu.saw("UPDATE users"));
    assert!(!replica.saw("UPDATE users"));
}

// --------------------------------------------------------- session details

#[tokio::test]
async fn settings_are_replayed_before_a_prepared_statement() {
    let f = fixture().await;
    let mut client = client(&f.proxy).await;

    client
        .simple_query("SET statement_timeout = 5000")
        .await
        .expect("set");
    let replies = client
        .pipeline(&[
            parse("", SELECT_USER),
            bind("", "", &["bob"]),
            execute("", 0),
            sync(),
        ])
        .await
        .expect("pipeline");
    // The replay is the proxy's business; the client sees only its own.
    assert_eq!(tags(&replies), ["1", "2", "C", "Z"]);

    let statements = f.us.statements();
    let set = statements
        .iter()
        .position(|sql| sql.contains("statement_timeout"));
    let select = statements.iter().position(|sql| sql == SELECT_USER);
    assert!(set.is_some() && set < select, "{statements:?}");
}

#[tokio::test]
async fn a_portal_can_be_fetched_in_pieces() {
    let f = fixture_with(
        BackendConfig::new("eu-west").reply_to(
            "FROM users",
            Reply::rows(&["id"], &[&["1"], &["2"], &["3"]]),
        ),
        BackendConfig::new("us-east"),
        false,
    )
    .await;
    let mut client = client(&f.proxy).await;

    // No Sync: the portal has to survive until the client asks for the
    // rest, which is how a cursor-style fetch works.
    let first = client
        .pipeline(&[
            parse("", SELECT_USER),
            bind("", "", &["alice"]),
            execute("", 2),
        ])
        .await
        .expect("first page");
    assert_eq!(tags(&first), ["1", "2", "DDs"]);

    let rest = client
        .pipeline(&[execute("", 2), sync()])
        .await
        .expect("rest");
    assert_eq!(tags(&rest), ["DC", "Z"]);
}

#[tokio::test]
async fn inside_a_transaction_a_portal_outlives_the_sync() {
    let f = fixture_with(
        BackendConfig::new("eu-west").reply_to(
            "FROM users",
            Reply::rows(&["id"], &[&["1"], &["2"], &["3"]]),
        ),
        BackendConfig::new("us-east"),
        false,
    )
    .await;
    let mut client = client(&f.proxy).await;

    client.simple_query("BEGIN").await.expect("begin");
    let first = client
        .pipeline(&[
            parse("", SELECT_USER),
            bind("cursor", "", &["alice"]),
            execute("cursor", 2),
            sync(),
        ])
        .await
        .expect("first page");
    assert_eq!(tags(&first), ["1", "2", "DDs", "Z"]);

    let rest = client
        .pipeline(&[execute("cursor", 2), sync()])
        .await
        .expect("rest");
    assert_eq!(tags(&rest), ["DC", "Z"]);
    client.simple_query("COMMIT").await.expect("commit");

    // The transaction ended, and its portals with it.
    let gone = client
        .pipeline(&[execute("cursor", 0), sync()])
        .await
        .expect("after commit");
    assert_eq!(error_code(&gone).as_deref(), Some("34000"));
}

#[tokio::test]
async fn describing_a_statement_reports_its_parameters_and_columns() {
    let f = fixture_with(
        BackendConfig::new("eu-west").reply_to("FROM users", Reply::rows(&["id"], &[])),
        BackendConfig::new("us-east"),
        false,
    )
    .await;
    let mut client = client(&f.proxy).await;

    let replies = client
        .pipeline(&[parse("s", SELECT_USER), describe_statement("s"), sync()])
        .await
        .expect("describe");
    assert_eq!(tags(&replies), ["1", "tT", "Z"]);
    let parameters = replies
        .get(1)
        .and_then(|reply| reply.messages.first())
        .expect("parameters");
    assert_eq!(
        Message::decode(parameters).unwrap(),
        Message::ParameterDescription { types: vec![25] }
    );
}

#[tokio::test]
async fn the_console_answers_over_the_extended_protocol() {
    let f = fixture().await;
    let mut client = client(&f.proxy).await;

    let replies = client
        .pipeline(&[
            parse("", "SHOW REGIONS"),
            bind("", "", &[]),
            describe_portal(""),
            execute("", 0),
            sync(),
        ])
        .await
        .expect("pipeline");
    assert_eq!(tags(&replies), ["1", "2", "T", "DDC", "Z"]);
    assert!(f.eu.statements().is_empty());
}

#[tokio::test]
async fn a_simple_query_after_an_unfinished_batch_does_not_hang() {
    let f = fixture().await;
    let mut client = client(&f.proxy).await;

    // A batch with no Sync, then a simple query.
    let replies = client
        .pipeline(&[parse("", SELECT_USER), bind("", "", &["alice"])])
        .await
        .expect("partial batch");
    assert_eq!(tags(&replies), ["1", "2"]);
    let result = tokio::time::timeout(Duration::from_secs(5), client.simple_query("SELECT 1"))
        .await
        .expect("must not hang");
    assert!(result.is_ok());
}

#[tokio::test]
async fn backend_connections_are_reused_after_extended_batches() {
    let f = fixture().await;
    let mut client = client(&f.proxy).await;
    for _ in 0..10 {
        client
            .pipeline(&[
                parse("", SELECT_USER),
                bind("", "", &["alice"]),
                execute("", 0),
                sync(),
            ])
            .await
            .expect("pipeline");
    }
    assert_eq!(f.eu.connections_accepted(), 1);
}
