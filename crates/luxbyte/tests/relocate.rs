//! `RELOCATE`, end to end: a client asks the proxy to move a key, and the
//! proxy moves its rows and its directory entry in one two-phase commit.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::time::{Duration, Instant};

use luxbyte::{Config, Server, ServerHandle};
use luxbyte_core::directory::{Directory, KeyState, Location};
use luxbyte_pool::{Connection, ConnectionTarget, Credentials, QueryResult};
use luxbyte_proto::auth::ScramVerifier;
use luxbyte_testkit::{Backend, BackendConfig, PreparedOutcome, Reply};

const STORE: &str = "[directory_store]\n";

/// Two users: `ops`, an operator, and `app`, who is not.
fn auth() -> String {
    let ops = ScramVerifier::derive("ops-pw", b"salt-ops", 4096).unwrap();
    let app = ScramVerifier::derive("app-pw", b"salt-app", 4096).unwrap();
    format!(
        "[auth]\noperators = [\"ops\"]\n\n[[auth.users]]\nname = \"ops\"\nverifier = \"{}\"\n\n\
         [[auth.users]]\nname = \"app\"\nverifier = \"{}\"\n",
        ops.to_postgres_string(),
        app.to_postgres_string()
    )
}

fn config(eu: &Backend, us: &Backend, sections: &str) -> Config {
    Config::from_toml(&format!(
        r#"
[server]
listen = "127.0.0.1:0"
node = "test-proxy"

[backend]
user = "luxbyte"
database = "shop"

{sections}

[topology]
home_region = "eu-west"

[[topology.regions]]
id = "eu-west"
primary = {{ host = "127.0.0.1", port = {eu} }}

[[topology.regions]]
id = "us-east"
primary = {{ host = "127.0.0.1", port = {us} }}

[[topology.tables]]
table = "orders"
placement = "geo_partitioned"
key = "tenant_id"

[[topology.tables]]
table = "users"
placement = "geo_partitioned"
key = "tenant_id"

[[topology.tables]]
table = "currencies"
placement = "global_replicated"
"#,
        eu = eu.port(),
        us = us.port(),
    ))
    .expect("config should parse")
}

async fn proxy(eu: &Backend, us: &Backend, sections: &str) -> ServerHandle {
    Server::bind(&config(eu, us, sections))
        .await
        .expect("should bind")
        .spawn()
        .expect("should serve")
}

async fn connect(proxy: &ServerHandle, user: &str) -> Connection {
    Connection::connect(
        &ConnectionTarget {
            host: "127.0.0.1".to_owned(),
            port: proxy.port(),
            database: "shop".to_owned(),
            user: user.to_owned(),
        },
        &Credentials::Password(format!("{user}-pw")),
        Duration::from_secs(5),
    )
    .await
    .expect("should log in")
}

const LOAD: &str = "SELECT table_name, shard_key, region FROM";

/// Europe, the home region, holding alice: two users rows and one order.
fn europe() -> BackendConfig {
    BackendConfig::new("eu-west")
        .reply_to(
            LOAD,
            Reply::rows(
                &["table_name", "shard_key", "region"],
                &[
                    &["users", "alice", "eu-west"],
                    &["orders", "alice", "eu-west"],
                ],
            ),
        )
        .reply_to(
            "FROM \"public\".\"users\" AS t",
            Reply::rows(
                &["json"],
                &[&[r#"[{"tenant_id":"alice","name":"Alice"},{"tenant_id":"alice","name":"Al"}]"#]],
            ),
        )
        .reply_to(
            "FROM \"public\".\"orders\" AS t",
            Reply::rows(&["json"], &[&[r#"[{"tenant_id":"alice","id":7}]"#]]),
        )
        .reply_to(
            "DELETE FROM \"public\".\"users\"",
            Reply::Done {
                tag: "DELETE 2".to_owned(),
            },
        )
        .reply_to(
            "DELETE FROM \"public\".\"orders\"",
            Reply::Done {
                tag: "DELETE 1".to_owned(),
            },
        )
}

/// The United States, where alice is going.
fn america() -> BackendConfig {
    BackendConfig::new("us-east")
        .reply_to(
            "\"public\".\"users\"'::regclass AND",
            Reply::rows(
                &["attname", "attidentity"],
                &[&["tenant_id", ""], &["name", ""]],
            ),
        )
        .reply_to(
            "\"public\".\"orders\"'::regclass AND",
            Reply::rows(
                &["attname", "attidentity"],
                &[&["tenant_id", ""], &["id", "a"]],
            ),
        )
        .reply_to(
            "INSERT INTO \"public\".\"users\"",
            Reply::Done {
                tag: "INSERT 0 2".to_owned(),
            },
        )
        .reply_to(
            "INSERT INTO \"public\".\"orders\"",
            Reply::Done {
                tag: "INSERT 0 1".to_owned(),
            },
        )
}

async fn start(
    eu: BackendConfig,
    us: BackendConfig,
    sections: &str,
) -> (Backend, Backend, ServerHandle) {
    let eu = Backend::start(eu).await.unwrap();
    let us = Backend::start(us).await.unwrap();
    let proxy = proxy(&eu, &us, sections).await;
    (eu, us, proxy)
}

fn sections() -> String {
    format!("{}\n{STORE}", auth())
}

fn region_of(proxy: &ServerHandle, table: &str, key: &str) -> Location {
    proxy.shared().directory.locate(table, key)
}

fn resident(region: &str) -> Location {
    Location::Known(KeyState::Resident {
        region: region.to_owned(),
    })
}

fn rows(result: &QueryResult) -> Vec<Vec<String>> {
    result
        .rows
        .iter()
        .map(|row| row.iter().map(|v| v.clone().unwrap_or_default()).collect())
        .collect()
}

fn position(backend: &Backend, needle: &str) -> usize {
    backend
        .statements()
        .iter()
        .position(|sql| sql.contains(needle))
        .unwrap_or_else(|| panic!("{} never saw {needle}", backend.name()))
}

#[tokio::test]
async fn a_key_moves_with_its_rows_and_directory_entry_in_one_commit() {
    let (eu, us, proxy) = start(europe(), america(), &sections()).await;
    assert_eq!(region_of(&proxy, "users", "alice"), resident("eu-west"));

    let mut ops = connect(&proxy, "ops").await;
    let result = ops
        .simple_query("RELOCATE alice TO 'us-east'")
        .await
        .expect("the move should succeed");
    assert_eq!(
        rows(&result),
        [
            ["orders", "eu-west", "us-east", "1", "moved"],
            ["users", "eu-west", "us-east", "2", "moved"],
        ]
    );

    // The source was locked against writers, read, and emptied.
    assert!(eu.saw("LOCK TABLE \"public\".\"orders\", \"public\".\"users\" IN EXCLUSIVE MODE"));
    assert!(eu.saw("DELETE FROM \"public\".\"users\" WHERE \"tenant_id\" = E'alice'"));
    // The destination got exactly the stored columns, identity values kept.
    assert!(us.saw(
        "INSERT INTO \"public\".\"users\" (\"tenant_id\", \"name\") SELECT \"tenant_id\", \"name\" \
         FROM json_populate_recordset(NULL::\"public\".\"users\", E'[{\"tenant_id\":\"alice\""
    ));
    assert!(
        us.saw("INSERT INTO \"public\".\"orders\" (\"tenant_id\", \"id\") OVERRIDING SYSTEM VALUE")
    );
    // And every region's directory changed in the same transaction.
    for backend in [&eu, &us] {
        assert!(backend.saw("INSERT INTO \"luxbyte_directory\""));
        assert!(backend.saw("E'us-east'"));
        let prepared = backend.prepared_transactions();
        assert_eq!(prepared.len(), 1, "{prepared:?}");
        assert_eq!(prepared[0].1, PreparedOutcome::Committed);
    }
    assert_eq!(
        eu.prepared_transactions()[0].0,
        us.prepared_transactions()[0].0
    );

    assert_eq!(region_of(&proxy, "users", "alice"), resident("us-east"));
    assert_eq!(region_of(&proxy, "orders", "alice"), resident("us-east"));
    let mut app = connect(&proxy, "app").await;
    app.simple_query("SELECT * FROM users WHERE tenant_id = 'alice'")
        .await
        .unwrap();
    assert!(us.saw("SELECT * FROM users WHERE tenant_id = 'alice'"));
}

#[tokio::test]
async fn parents_are_inserted_before_children_and_deleted_after_them() {
    // orders comes first in the topology, but has a foreign key onto users.
    let (eu, us, proxy) = start(
        europe(),
        america()
            .reply_to(
                "::regclass::oid::text",
                Reply::rows(&["orders", "users"], &[&["16390", "16384"]]),
            )
            .reply_to(
                "pg_constraint",
                Reply::rows(
                    &["child", "parent", "child_name", "parent_name"],
                    &[&["16390", "16384", "orders", "users"]],
                ),
            ),
        &sections(),
    )
    .await;
    let mut ops = connect(&proxy, "ops").await;
    ops.simple_query("RELOCATE alice TO us-east").await.unwrap();
    assert!(
        position(&us, "INSERT INTO \"public\".\"users\"")
            < position(&us, "INSERT INTO \"public\".\"orders\"")
    );
    assert!(
        position(&eu, "DELETE FROM \"public\".\"orders\"")
            < position(&eu, "DELETE FROM \"public\".\"users\"")
    );
}

#[tokio::test]
async fn a_foreign_key_from_outside_the_move_is_refused() {
    let (eu, us, proxy) = start(
        europe(),
        america()
            .reply_to(
                "::regclass::oid::text",
                Reply::rows(&["orders", "users"], &[&["16390", "16384"]]),
            )
            .reply_to(
                "pg_constraint",
                Reply::rows(
                    &["child", "parent", "child_name", "parent_name"],
                    &[&["17000", "16384", "audit", "users"]],
                ),
            ),
        &sections(),
    )
    .await;
    let mut ops = connect(&proxy, "ops").await;
    let error = ops
        .simple_query("RELOCATE alice TO us-east")
        .await
        .expect_err("refused");
    assert_eq!(error.sqlstate(), Some("0A000"), "{error}");
    assert!(error.to_string().contains("audit"), "{error}");
    nothing_moved(&eu, &us, &proxy);
}

/// Checks that a refused move left no trace.
fn nothing_moved(eu: &Backend, us: &Backend, proxy: &ServerHandle) {
    for backend in [eu, us] {
        assert!(
            backend.prepared_transactions().is_empty(),
            "{} prepared something",
            backend.name()
        );
    }
    assert_eq!(region_of(proxy, "users", "alice"), resident("eu-west"));
    assert_eq!(region_of(proxy, "orders", "alice"), resident("eu-west"));
    assert_eq!(proxy.shared().directory.moving_in("users"), None);
}

#[tokio::test]
async fn a_move_that_fails_anywhere_changes_nothing() {
    let (eu, us, proxy) = start(
        europe(),
        BackendConfig::new("us-east")
            .reply_to(
                "INSERT INTO \"public\"",
                Reply::fail("23505", "duplicate key value"),
            )
            .reply_to(
                "'::regclass AND",
                Reply::rows(&["attname", "attidentity"], &[&["tenant_id", ""]]),
            ),
        &sections(),
    )
    .await;
    let mut ops = connect(&proxy, "ops").await;
    let error = ops
        .simple_query("RELOCATE alice TO us-east")
        .await
        .expect_err("the insert fails");
    assert_eq!(error.sqlstate(), Some("23505"), "{error}");
    assert!(error.to_string().contains("nothing was moved"), "{error}");
    assert!(eu.saw("ROLLBACK"));
    nothing_moved(&eu, &us, &proxy);

    // Writes carry on in the source.
    let mut app = connect(&proxy, "app").await;
    app.simple_query("UPDATE users SET name = 'x' WHERE tenant_id = 'alice'")
        .await
        .unwrap();
    assert!(eu.saw("UPDATE users SET name = 'x'"));
}

#[tokio::test]
async fn rows_that_do_not_add_up_abort_the_move() {
    // Scripted replies match in order, so the miscount goes first.
    let mut eu = europe();
    eu.scripted.insert(
        0,
        (
            "DELETE FROM \"public\".\"orders\"".to_owned(),
            Reply::Done {
                tag: "DELETE 3".to_owned(),
            },
        ),
    );
    let (eu, us, proxy) = start(eu, america(), &sections()).await;
    let mut ops = connect(&proxy, "ops").await;
    let error = ops
        .simple_query("RELOCATE alice TO us-east")
        .await
        .expect_err("refused");
    assert!(error.to_string().contains("where 1 were copied"), "{error}");
    nothing_moved(&eu, &us, &proxy);
}

#[tokio::test]
async fn a_destination_that_already_has_rows_for_the_key_is_refused() {
    let (eu, us, proxy) = start(
        europe(),
        america().reply_to(
            "SELECT count(*) FROM \"public\".\"users\"",
            Reply::rows(&["count"], &[&["4"]]),
        ),
        &sections(),
    )
    .await;
    let mut ops = connect(&proxy, "ops").await;
    let error = ops
        .simple_query("RELOCATE alice TO us-east")
        .await
        .expect_err("refused");
    assert_eq!(error.sqlstate(), Some("55000"), "{error}");
    nothing_moved(&eu, &us, &proxy);
}

#[tokio::test]
async fn a_key_with_no_entry_moves_from_wherever_its_rows_are() {
    let (eu, us, proxy) = start(
        europe()
            .reply_to(
                "\"public\".\"users\"'::regclass AND",
                Reply::rows(&["attname", "attidentity"], &[&["tenant_id", ""]]),
            )
            .reply_to(
                "INSERT INTO \"public\".\"users\"",
                Reply::Done {
                    tag: "INSERT 0 1".to_owned(),
                },
            ),
        BackendConfig::new("us-east")
            .reply_to(
                "SELECT DISTINCT \"tenant_id\"::text FROM \"public\".\"users\"",
                Reply::rows(&["tenant_id"], &[&["carol"]]),
            )
            .reply_to(
                "FROM \"public\".\"users\" AS t",
                Reply::rows(&["json"], &[&[r#"[{"tenant_id":"carol"}]"#]]),
            )
            .reply_to(
                "DELETE FROM \"public\".\"users\"",
                Reply::Done {
                    tag: "DELETE 1".to_owned(),
                },
            ),
        &sections(),
    )
    .await;
    assert_eq!(region_of(&proxy, "users", "carol"), Location::Unseen);
    let mut ops = connect(&proxy, "ops").await;
    let result = ops.simple_query("RELOCATE carol TO eu-west").await.unwrap();
    assert_eq!(
        rows(&result),
        [
            ["orders", "", "eu-west", "0", "moved"],
            ["users", "us-east", "eu-west", "1", "moved"],
        ]
    );
    assert!(us.saw("DELETE FROM \"public\".\"users\" WHERE \"tenant_id\" = E'carol'"));
    assert!(eu.saw("INSERT INTO \"public\".\"users\""));
    // Both tables now say where carol lives, so her next order goes there.
    assert_eq!(region_of(&proxy, "users", "carol"), resident("eu-west"));
    assert_eq!(region_of(&proxy, "orders", "carol"), resident("eu-west"));
}

#[tokio::test]
async fn a_key_with_rows_in_two_regions_is_refused_rather_than_guessed() {
    let probe = || Reply::rows(&["tenant_id"], &[&["carol"]]);
    let users = "SELECT DISTINCT \"tenant_id\"::text FROM \"public\".\"users\"";
    let (eu, us, proxy) = start(
        europe().reply_to(users, probe()),
        america().reply_to(users, probe()),
        &sections(),
    )
    .await;
    let mut ops = connect(&proxy, "ops").await;
    let error = ops
        .simple_query("RELOCATE carol TO eu-west")
        .await
        .expect_err("refused");
    assert_eq!(error.sqlstate(), Some("55000"), "{error}");
    assert!(error.to_string().contains("eu-west and us-east"), "{error}");
    assert!(error.to_string().contains("carol"), "{error}");
    assert_eq!(region_of(&proxy, "users", "carol"), Location::Unseen);
    nothing_moved(&eu, &us, &proxy);
}

#[tokio::test]
async fn moving_a_key_to_where_it_already_is_does_nothing() {
    let (eu, us, proxy) = start(europe(), america(), &sections()).await;
    let mut ops = connect(&proxy, "ops").await;
    let result = ops.simple_query("RELOCATE alice TO eu-west").await.unwrap();
    assert_eq!(rows(&result).len(), 2);
    assert!(rows(&result).iter().all(|row| row[3] == "0"));
    nothing_moved(&eu, &us, &proxy);
    assert!(!eu.saw("LOCK TABLE"));
}

#[tokio::test]
async fn writes_wait_for_the_move_then_go_to_the_new_region_while_reads_carry_on() {
    let (eu, us, proxy) = start(
        europe(),
        america().slow_on(
            "INSERT INTO \"public\".\"orders\"",
            Duration::from_millis(400),
        ),
        &sections(),
    )
    .await;
    let mut ops = connect(&proxy, "ops").await;
    let mut app = connect(&proxy, "app").await;
    let mover = tokio::spawn(async move { ops.simple_query("RELOCATE alice TO us-east").await });

    // Wait for the cutover.
    let started = Instant::now();
    while proxy.shared().directory.moving_in("users").is_none() {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the move never started"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // A read during the copy is answered by the source at once.
    let read = Instant::now();
    app.simple_query("SELECT * FROM users WHERE tenant_id = 'alice'")
        .await
        .unwrap();
    assert!(
        read.elapsed() < Duration::from_millis(300),
        "the read waited"
    );
    assert!(eu.saw("SELECT * FROM users WHERE tenant_id = 'alice'"));

    // A write waits for the move, then lands where the rows now are.
    app.simple_query("UPDATE users SET name = 'moved' WHERE tenant_id = 'alice'")
        .await
        .expect("the write should wait, not fail");
    assert!(mover.is_finished() || mover.await.unwrap().is_ok());
    assert!(us.saw("UPDATE users SET name = 'moved'"));
    assert!(!eu.saw("UPDATE users SET name = 'moved'"));
    assert!(proxy.shared().metrics.summary()["held_statements_total"] >= 1);
}

#[tokio::test]
async fn a_statement_that_cannot_wait_is_refused_with_a_retryable_error() {
    let (_eu, _us, proxy) = start(
        europe(),
        america().slow_on(
            "INSERT INTO \"public\".\"orders\"",
            Duration::from_millis(600),
        ),
        &format!("{}\n[directory_store]\nhold_ms = 50\n", auth()),
    )
    .await;
    let mut ops = connect(&proxy, "ops").await;
    let mut app = connect(&proxy, "app").await;
    let mover = tokio::spawn(async move { ops.simple_query("RELOCATE alice TO us-east").await });
    while proxy.shared().directory.moving_in("users").is_none() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let error = app
        .simple_query("UPDATE users SET name = 'x' WHERE tenant_id = 'alice'")
        .await
        .expect_err("waited too long");
    assert_eq!(error.sqlstate(), Some("40001"), "{error}");

    // Inside a transaction that has done work, it is refused at once: the
    // move may be waiting for that transaction's locks.
    app.simple_query("BEGIN").await.unwrap();
    app.simple_query("SELECT * FROM currencies").await.unwrap();
    let asked = Instant::now();
    let error = app
        .simple_query("SELECT * FROM orders WHERE tenant_id = 'alice' FOR UPDATE")
        .await
        .expect_err("refused");
    assert_eq!(error.sqlstate(), Some("40001"), "{error}");
    assert!(asked.elapsed() < Duration::from_millis(40));
    app.simple_query("ROLLBACK").await.unwrap();
    mover.await.unwrap().expect("the move still succeeds");
}

#[tokio::test]
async fn only_an_authenticated_operator_may_move_a_key_and_not_inside_a_transaction() {
    let (eu, us, proxy) = start(europe(), america(), &sections()).await;
    let mut app = connect(&proxy, "app").await;
    let error = app
        .simple_query("RELOCATE alice TO us-east")
        .await
        .expect_err("not an operator");
    assert_eq!(error.sqlstate(), Some("42501"), "{error}");

    let mut ops = connect(&proxy, "ops").await;
    ops.simple_query("BEGIN").await.unwrap();
    let error = ops
        .simple_query("RELOCATE alice TO us-east")
        .await
        .expect_err("in a transaction");
    assert_eq!(error.sqlstate(), Some("25001"), "{error}");
    ops.simple_query("ROLLBACK").await.unwrap();

    let error = ops
        .simple_query("RELOCATE alice TO mars")
        .await
        .expect_err("no such region");
    assert_eq!(error.sqlstate(), Some("22023"), "{error}");
    let error = ops
        .simple_query("RELOCATE alice")
        .await
        .expect_err("malformed");
    assert_eq!(error.sqlstate(), Some("42601"), "{error}");
    nothing_moved(&eu, &us, &proxy);
}

#[tokio::test]
async fn with_trust_authentication_nobody_may_move_a_key() {
    let (_eu, _us, proxy) = start(europe(), america(), STORE).await;
    let mut client = connect(&proxy, "ops").await;
    let error = client
        .simple_query("RELOCATE alice TO us-east")
        .await
        .expect_err("refused");
    assert_eq!(error.sqlstate(), Some("42501"), "{error}");
}

#[tokio::test]
async fn without_a_durable_directory_relocate_is_refused() {
    let (_eu, _us, proxy) = start(europe(), america(), &auth()).await;
    let mut ops = connect(&proxy, "ops").await;
    let error = ops
        .simple_query("RELOCATE alice TO us-east")
        .await
        .expect_err("refused");
    assert_eq!(error.sqlstate(), Some("55000"), "{error}");
}

#[tokio::test]
async fn the_directory_is_created_where_missing_and_loaded_from_the_home_region() {
    let (eu, us, proxy) = start(europe(), america(), &sections()).await;
    for backend in [&eu, &us] {
        assert!(backend.saw("CREATE TABLE IF NOT EXISTS \"luxbyte_directory\""));
    }
    assert!(eu.saw(LOAD));
    assert!(!us.saw(LOAD));
    assert_eq!(proxy.shared().directory.len(), 2);
}

#[tokio::test]
async fn a_directory_naming_an_unknown_region_stops_the_proxy_from_starting() {
    let eu = Backend::start(BackendConfig::new("eu-west").reply_to(
        LOAD,
        Reply::rows(
            &["table_name", "shard_key", "region"],
            &[&["users", "alice", "mars"]],
        ),
    ))
    .await
    .unwrap();
    let us = Backend::start(america()).await.unwrap();
    let error = Server::bind(&config(&eu, &us, &sections()))
        .await
        .expect_err("refused");
    assert!(error.to_string().contains("mars"), "{error}");
}

#[tokio::test]
async fn seeded_entries_and_a_store_cannot_both_be_configured() {
    let eu = Backend::start(europe()).await.unwrap();
    let us = Backend::start(america()).await.unwrap();
    let sections = format!(
        "{}\n[[directory]]\ntable = \"users\"\nkey = \"bob\"\nregion = \"us-east\"\n",
        sections()
    );
    let error = Server::bind(&config(&eu, &us, &sections))
        .await
        .expect_err("refused");
    assert!(error.to_string().contains("cannot both be set"), "{error}");
}

#[tokio::test]
async fn relocate_over_the_extended_protocol_is_refused_clearly() {
    use luxbyte_pool::{Step, StepKind};
    use luxbyte_proto::backend::Backend as Message;
    use luxbyte_proto::frontend;

    let (_eu, _us, proxy) = start(europe(), america(), &sections()).await;
    let mut ops = connect(&proxy, "ops").await;
    let replies = ops
        .pipeline(&[
            Step::new(
                StepKind::Parse,
                frontend::parse("", "RELOCATE alice TO us-east", &[]).unwrap(),
            ),
            Step::new(StepKind::Sync, frontend::sync().unwrap()),
        ])
        .await
        .expect("pipeline");
    let code = replies
        .iter()
        .flat_map(|reply| &reply.messages)
        .find_map(|message| match Message::decode(message) {
            Ok(Message::Error(notice)) => Some(notice.code().to_owned()),
            _ => None,
        });
    assert_eq!(code.as_deref(), Some("0A000"));
}

#[tokio::test]
async fn a_move_waits_for_writes_already_under_way() {
    // A write planned before the cutover is still running in the source; the
    // move may not take the rows until it is done, and here gives up.
    let (eu, us, proxy) = start(
        europe().slow_on("UPDATE users SET name = 'slow'", Duration::from_millis(500)),
        america(),
        &format!("{}\n[directory_store]\nlock_timeout_ms = 100\n", auth()),
    )
    .await;
    let mut app = connect(&proxy, "app").await;
    let mut ops = connect(&proxy, "ops").await;
    let writer = tokio::spawn(async move {
        app.simple_query("UPDATE users SET name = 'slow' WHERE tenant_id = 'alice'")
            .await
    });
    while !eu.saw("UPDATE users SET name = 'slow'") {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let error = ops
        .simple_query("RELOCATE alice TO us-east")
        .await
        .expect_err("the write is still under way");
    assert_eq!(error.sqlstate(), Some("55P03"), "{error}");
    writer.await.unwrap().expect("the write itself succeeds");
    nothing_moved(&eu, &us, &proxy);
}

#[tokio::test]
async fn a_commit_the_destination_has_not_acknowledged_keeps_the_key_waiting() {
    let (_eu, _us, proxy) = start(
        europe(),
        america().disconnecting_on("COMMIT PREPARED"),
        &sections(),
    )
    .await;
    let mut ops = connect(&proxy, "ops").await;
    let result = ops.simple_query("RELOCATE alice TO us-east").await.unwrap();
    assert!(
        rows(&result)
            .iter()
            .all(|row| row[4] == "moved; waiting for us-east to confirm"),
        "{:?}",
        rows(&result)
    );
    // Its rows may not be visible in us-east yet, so nothing reads them.
    assert_eq!(
        proxy.shared().directory.moving_in("users"),
        Some(luxbyte_core::RelocationPhase::Committing)
    );
}

#[tokio::test]
async fn a_prepared_write_waits_for_the_move_too() {
    use luxbyte_pool::{Step, StepKind};
    use luxbyte_proto::frontend;

    let (eu, us, proxy) = start(
        europe(),
        america().slow_on(
            "INSERT INTO \"public\".\"orders\"",
            Duration::from_millis(300),
        ),
        &sections(),
    )
    .await;
    let mut ops = connect(&proxy, "ops").await;
    let mut app = connect(&proxy, "app").await;
    let mover = tokio::spawn(async move { ops.simple_query("RELOCATE alice TO us-east").await });
    while proxy.shared().directory.moving_in("users").is_none() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let update = "UPDATE users SET name = 'prepared' WHERE tenant_id = $1";
    app.pipeline(&[
        Step::new(StepKind::Parse, frontend::parse("", update, &[]).unwrap()),
        Step::new(
            StepKind::Bind,
            frontend::bind("", "", &[], &[Some(b"alice".as_slice())], &[]).unwrap(),
        ),
        Step::new(StepKind::Execute, frontend::execute("", 0).unwrap()),
        Step::new(StepKind::Sync, frontend::sync().unwrap()),
    ])
    .await
    .expect("pipeline");
    mover.await.unwrap().expect("moved");
    assert_eq!(
        us.bound(),
        [(update.to_owned(), vec![Some("alice".to_owned())])]
    );
    assert!(eu.bound().is_empty());
}

// -------------------------------------------------------------- REBALANCE

const KEYS: &str = "SELECT k FROM (";

#[tokio::test]
async fn rebalance_moves_each_key_the_source_holds_with_its_own_relocate() {
    let (eu, us, proxy) = start(
        europe().reply_to(KEYS, Reply::rows(&["k"], &[&["alice"]])),
        america(),
        &sections(),
    )
    .await;
    let mut ops = connect(&proxy, "ops").await;
    let result = ops
        .simple_query("REBALANCE eu-west TO us-east LIMIT 5")
        .await
        .expect("rebalance");
    assert_eq!(result.columns, ["key", "rows", "state"]);
    assert_eq!(rows(&result), [["alice", "3", "moved"]]);
    // The keys come from the source's rows, across every geo-partitioned
    // table, so keys the directory never recorded move too.
    assert!(eu.saw(
        "SELECT k FROM (SELECT DISTINCT \"tenant_id\"::text AS k FROM \"public\".\"orders\" UNION \
         SELECT DISTINCT \"tenant_id\"::text AS k FROM \"public\".\"users\") AS keys WHERE k IS \
         NOT NULL ORDER BY k LIMIT 5"
    ));
    assert_eq!(region_of(&proxy, "users", "alice"), resident("us-east"));
    assert!(us.saw("INSERT INTO \"public\".\"users\""));
}

#[tokio::test]
async fn rebalance_stops_at_the_first_key_that_cannot_move() {
    // zoe is unknown to the directory and has rows in both regions, which
    // RELOCATE refuses to guess about; key by key, nothing after her is
    // tried.
    let both = "\"tenant_id\" = E'zoe'";
    let (_eu, _us, proxy) = start(
        europe()
            .reply_to(KEYS, Reply::rows(&["k"], &[&["alice"], &["zoe"], &["zz"]]))
            .reply_to(both, Reply::rows(&["tenant_id"], &[&["zoe"]])),
        america().reply_to(both, Reply::rows(&["tenant_id"], &[&["zoe"]])),
        &sections(),
    )
    .await;
    let mut ops = connect(&proxy, "ops").await;
    let result = ops
        .simple_query("REBALANCE eu-west TO us-east BATCH 1")
        .await
        .expect("a report, not an error");
    let rows = rows(&result);
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(rows[0], ["alice", "3", "moved"]);
    assert_eq!(rows[1][0], "zoe");
    assert!(rows[1][2].starts_with("refused: "), "{rows:?}");
    assert!(rows[1][2].contains("stopped here"), "{rows:?}");
}

#[tokio::test]
async fn rebalance_is_for_operators_and_needs_two_regions() {
    let (_eu, _us, proxy) = start(europe(), america(), &sections()).await;
    let mut app = connect(&proxy, "app").await;
    let error = app
        .simple_query("REBALANCE eu-west TO us-east")
        .await
        .expect_err("not an operator");
    assert_eq!(error.sqlstate(), Some("42501"), "{error}");
    let mut ops = connect(&proxy, "ops").await;
    for (sql, code) in [
        ("REBALANCE eu-west TO eu-west", "22023"),
        ("REBALANCE eu-west TO mars", "22023"),
        ("REBALANCE eu-west", "42601"),
    ] {
        let error = ops.simple_query(sql).await.expect_err(sql);
        assert_eq!(error.sqlstate(), Some(code), "{sql}: {error}");
    }
}

// ------------------------------------------------------- recording new keys

const NEW_ROW: &str = "INSERT INTO users (tenant_id, name) VALUES ('carol', 'C')";
const READ_NEW: &str = "SELECT * FROM users WHERE tenant_id = 'carol'";

#[tokio::test]
async fn a_key_written_for_the_first_time_is_recorded_and_read_from_one_region() {
    let (eu, us, proxy) = start(europe(), america(), &sections()).await;
    let mut app = connect(&proxy, "app").await;
    app.simple_query(NEW_ROW).await.unwrap();
    // Recorded in the home region's directory before the row was written.
    let recorded = position(&eu, "INSERT INTO \"luxbyte_directory\" AS d");
    assert!(recorded < position(&eu, NEW_ROW));
    assert!(eu.saw("E'carol'"));
    assert_eq!(region_of(&proxy, "users", "carol"), resident("eu-west"));

    // So reading it asks only Europe, rather than every region.
    app.simple_query(READ_NEW).await.unwrap();
    assert_eq!(eu.count_of(READ_NEW), 1);
    assert_eq!(us.count_of(READ_NEW), 0);
}

#[tokio::test]
async fn recording_can_be_turned_off_for_rows_the_directory_does_not_list_yet() {
    let (eu, us, proxy) = start(
        europe(),
        america(),
        &format!("{}\n{STORE}record_new_keys = false\n", auth()),
    )
    .await;
    let mut app = connect(&proxy, "app").await;
    app.simple_query(NEW_ROW).await.unwrap();
    assert!(!eu.saw("INSERT INTO \"luxbyte_directory\" AS d"));
    app.simple_query(READ_NEW).await.unwrap();
    assert_eq!(eu.count_of(READ_NEW), 1);
    assert_eq!(us.count_of(READ_NEW), 1, "unrecorded, so read everywhere");
}

// ------------------------------------------------------------ move guards

#[tokio::test]
async fn with_guards_every_region_refuses_writes_for_keys_it_does_not_hold() {
    let (eu, us, proxy) = start(
        europe(),
        america(),
        &format!("{}\n{STORE}guard = true\n", auth()),
    )
    .await;
    // Each region's trigger knows which region it is in.
    for (backend, here) in [(&eu, "E'eu-west'"), (&us, "E'us-east'")] {
        for table in ["users", "orders"] {
            assert!(backend.saw(&format!(
                "CREATE OR REPLACE TRIGGER luxbyte_guard BEFORE INSERT OR UPDATE OR DELETE ON \
                 \"public\".\"{table}\""
            )));
        }
        assert!(
            backend.saw(&format!("placed <> {here}")),
            "{}",
            backend.name()
        );
    }
    // A move tells the guards it is the one writing.
    let mut ops = connect(&proxy, "ops").await;
    ops.simple_query("RELOCATE alice TO us-east").await.unwrap();
    for backend in [&eu, &us] {
        assert!(
            backend.saw("SET LOCAL luxbyte.moving = 'on'"),
            "{}",
            backend.name()
        );
    }
}

#[tokio::test]
async fn without_guards_no_trigger_is_installed() {
    let (eu, us, _proxy) = start(europe(), america(), &sections()).await;
    assert!(!eu.saw("luxbyte_guard"));
    assert!(!us.saw("luxbyte_guard"));
}

#[tokio::test]
async fn several_keys_move_together_in_one_transaction() {
    // dave has no directory entry; his rows are found in Europe, with
    // alice's, so both come from one region in one move.
    let (eu, us, proxy) = start(
        europe().reply_to(
            "SELECT DISTINCT \"tenant_id\"::text FROM \"public\".\"users\"",
            Reply::rows(&["tenant_id"], &[&["dave"]]),
        ),
        america(),
        &sections(),
    )
    .await;
    let mut ops = connect(&proxy, "ops").await;
    let result = ops
        .simple_query("RELOCATE alice, dave TO us-east")
        .await
        .expect("both move");
    assert_eq!(rows(&result).len(), 2, "one row per table");

    // One lock, one read, one delete per table for both keys...
    assert!(eu.saw("DELETE FROM \"public\".\"users\" WHERE \"tenant_id\" IN (E'alice', E'dave')"));
    // ...and one two-phase commit for the whole move.
    for backend in [&eu, &us] {
        assert_eq!(
            backend.prepared_transactions().len(),
            1,
            "{}",
            backend.name()
        );
        assert!(backend.saw("(E'users', E'dave', E'us-east')"));
    }
    for key in ["alice", "dave"] {
        assert_eq!(region_of(&proxy, "users", key), resident("us-east"));
        assert_eq!(region_of(&proxy, "orders", key), resident("us-east"));
    }
}

#[tokio::test]
async fn a_key_already_at_the_destination_stays_while_the_others_move() {
    // alice is in Europe; bob has no entry, and his rows are in America.
    let (eu, us, proxy) = start(
        europe(),
        america().reply_to(
            "SELECT DISTINCT \"tenant_id\"::text FROM \"public\".\"users\"",
            Reply::rows(&["tenant_id"], &[&["bob"]]),
        ),
        &sections(),
    )
    .await;
    let mut ops = connect(&proxy, "ops").await;
    ops.simple_query("RELOCATE alice, bob TO eu-west")
        .await
        .expect("bob moves; alice is there already");
    assert!(us.saw("DELETE FROM \"public\".\"users\" WHERE \"tenant_id\" = E'bob'"));
    assert!(!eu.saw("DELETE FROM \"public\".\"users\""));
    assert_eq!(region_of(&proxy, "users", "bob"), resident("eu-west"));
    assert_eq!(region_of(&proxy, "users", "alice"), resident("eu-west"));
}
