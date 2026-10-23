//! Client authentication, end to end.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::time::Duration;

use luxbyte::{Config, Server, ServerHandle};
use luxbyte_pool::{Connection, ConnectionTarget, Credentials, PoolError};
use luxbyte_proto::auth::ScramVerifier;
use luxbyte_testkit::{Backend, BackendConfig, Reply};

fn verifier(password: &str, salt: &[u8]) -> ScramVerifier {
    ScramVerifier::derive(password, salt, 4096).unwrap()
}

fn config(eu: &Backend, us: &Backend, listen: &str, auth: &str) -> Config {
    Config::from_toml(&format!(
        r#"
[server]
listen = "{listen}"
node = "test-proxy"

[backend]
user = "luxbyte"
database = "shop"

{auth}

[topology]
home_region = "eu-west"

[[topology.regions]]
id = "eu-west"
primary = {{ host = "127.0.0.1", port = {eu} }}

[[topology.regions]]
id = "us-east"
primary = {{ host = "127.0.0.1", port = {us} }}

[[topology.tables]]
table = "users"
placement = "geo_partitioned"
key = "tenant_id"

[[directory]]
table = "users"
key = "bob"
region = "us-east"
"#,
        eu = eu.port(),
        us = us.port(),
    ))
    .expect("config should parse")
}

async fn proxy(eu: &Backend, us: &Backend, auth: &str) -> ServerHandle {
    Server::bind(&config(eu, us, "127.0.0.1:0", auth))
        .await
        .expect("should bind")
        .spawn()
        .expect("should serve")
}

async fn connect(
    proxy: &ServerHandle,
    user: &str,
    password: &str,
) -> Result<Connection, PoolError> {
    Connection::connect(
        &ConnectionTarget {
            host: "127.0.0.1".to_owned(),
            port: proxy.port(),
            database: "shop".to_owned(),
            user: user.to_owned(),
        },
        &Credentials::Password(password.to_owned()),
        Duration::from_secs(5),
    )
    .await
}

fn users_section(user: &str, verifier: &ScramVerifier, passthrough: bool) -> String {
    format!(
        "[auth]\npassthrough = {passthrough}\n\n[[auth.users]]\nname = \"{user}\"\nverifier = \"{}\"\n",
        verifier.to_postgres_string()
    )
}

#[tokio::test]
async fn the_right_password_gets_in() {
    let eu = Backend::start(BackendConfig::new("eu-west")).await.unwrap();
    let us = Backend::start(BackendConfig::new("us-east")).await.unwrap();
    let proxy = proxy(
        &eu,
        &us,
        &users_section("app", &verifier("s3cret", b"salt-one"), false),
    )
    .await;

    let mut client = connect(&proxy, "app", "s3cret")
        .await
        .expect("should log in");
    assert!(client.simple_query("SELECT 1").await.is_ok());
}

#[tokio::test]
async fn a_wrong_password_and_an_unknown_role_fail_the_same_way() {
    let eu = Backend::start(BackendConfig::new("eu-west")).await.unwrap();
    let us = Backend::start(BackendConfig::new("us-east")).await.unwrap();
    let proxy = proxy(
        &eu,
        &us,
        &users_section("app", &verifier("s3cret", b"salt-one"), false),
    )
    .await;

    let wrong = connect(&proxy, "app", "guess")
        .await
        .expect_err("wrong password");
    let unknown = connect(&proxy, "nobody", "guess")
        .await
        .expect_err("unknown role");
    // Telling the two apart would tell an attacker which roles exist.
    assert_eq!(wrong.sqlstate(), Some("28P01"));
    assert_eq!(unknown.sqlstate(), Some("28P01"));
    assert!(wrong.to_string().contains("password authentication failed"));
    assert!(
        unknown
            .to_string()
            .contains("password authentication failed")
    );
}

#[tokio::test]
async fn with_passthrough_backends_see_the_clients_own_role() {
    // The same verifier on the backends as in the proxy, as `CREATE ROLE ...
    // PASSWORD 'SCRAM-SHA-256$...'` would give them.
    let shared = verifier("s3cret", b"salt-one");
    let eu = Backend::start(BackendConfig::new("eu-west").requiring_scram("app", shared.clone()))
        .await
        .unwrap();
    let us = Backend::start(BackendConfig::new("us-east").requiring_scram("app", shared.clone()))
        .await
        .unwrap();
    let proxy = proxy(&eu, &us, &users_section("app", &shared, true)).await;

    let mut client = connect(&proxy, "app", "s3cret")
        .await
        .expect("should log in");
    client
        .simple_query("SELECT id FROM users WHERE tenant_id = 'bob'")
        .await
        .expect("query as app");

    // The backend authenticated the proxy as "app" with SCRAM, though the
    // proxy never knew the password.
    assert_eq!(us.users(), ["app"]);
    assert!(us.saw("tenant_id = 'bob'"));
}

#[tokio::test]
async fn passthrough_to_a_backend_with_another_verifier_says_why() {
    let eu = Backend::start(
        BackendConfig::new("eu-west").requiring_scram("app", verifier("s3cret", b"salt-two")),
    )
    .await
    .unwrap();
    let us = Backend::start(BackendConfig::new("us-east")).await.unwrap();
    let proxy = proxy(
        &eu,
        &us,
        &users_section("app", &verifier("s3cret", b"salt-one"), true),
    )
    .await;

    let mut client = connect(&proxy, "app", "s3cret")
        .await
        .expect("should log in");
    let error = client
        .simple_query("SELECT 1")
        .await
        .expect_err("the backend's verifier differs");
    assert!(error.to_string().contains("same verifier"), "{error}");
}

#[tokio::test]
async fn verifiers_can_be_read_from_the_home_region() {
    let stored = verifier("s3cret", b"salt-one").to_postgres_string();
    let eu = Backend::start(
        BackendConfig::new("eu-west")
            .reply_to("pg_authid", Reply::rows(&["rolpassword"], &[&[&stored]])),
    )
    .await
    .unwrap();
    let us = Backend::start(BackendConfig::new("us-east")).await.unwrap();
    let proxy = proxy(
        &eu,
        &us,
        "[auth]\npassthrough = false\nquery = \"SELECT rolpassword FROM pg_authid WHERE rolname = $1\"\n",
    )
    .await;

    let mut client = connect(&proxy, "app", "s3cret")
        .await
        .expect("should log in");
    assert!(client.simple_query("SELECT 1").await.is_ok());

    // The role name went in as a bound parameter, never as SQL text.
    let lookups = eu.bound();
    assert_eq!(lookups.len(), 1);
    assert_eq!(
        lookups.first().map(|(_, values)| values.clone()),
        Some(vec![Some("app".to_owned())])
    );

    // A second login within the cache's lifetime does not ask again.
    let _again = connect(&proxy, "app", "s3cret").await.expect("again");
    assert_eq!(eu.bound().len(), 1);

    assert!(connect(&proxy, "app", "wrong").await.is_err());
}

#[tokio::test]
async fn trust_on_a_reachable_address_has_to_be_asked_for() {
    let eu = Backend::start(BackendConfig::new("eu-west")).await.unwrap();
    let us = Backend::start(BackendConfig::new("us-east")).await.unwrap();

    let error = Server::bind(&config(&eu, &us, "0.0.0.0:0", ""))
        .await
        .expect_err("an open proxy on the network is refused by default");
    assert!(error.to_string().contains("no client authentication"));

    // Saying so explicitly is allowed.
    assert!(
        Server::bind(&config(
            &eu,
            &us,
            "0.0.0.0:0",
            "[auth]\nmethod = \"trust\"\n"
        ))
        .await
        .is_ok()
    );
}
