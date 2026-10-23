//! The admin port, over real HTTP.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::net::SocketAddr;
use std::time::Duration;

use luxbyte::{Config, Server, ServerHandle};
use luxbyte_pool::{Connection, ConnectionTarget, Credentials};
use luxbyte_testkit::{Backend, BackendConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn config(eu: &Backend, us: &Backend, admin: &str) -> Config {
    Config::from_toml(&format!(
        r#"
[server]
listen = "127.0.0.1:0"
node = "test-proxy"

[backend]
user = "app"
database = "shop"

[admin]
{admin}

[topology]
home_region = "eu-west"

[[topology.regions]]
id = "eu-west"
primary = {{ host = "127.0.0.1", port = {} }}

[[topology.regions]]
id = "us-east"
primary = {{ host = "127.0.0.1", port = {} }}

[[topology.tables]]
table = "users"
placement = "geo_partitioned"
key = "tenant_id"

[[directory]]
table = "users"
key = "bob"
region = "us-east"
"#,
        eu.port(),
        us.port()
    ))
    .unwrap()
}

async fn backends() -> (Backend, Backend) {
    (
        Backend::start(BackendConfig::new("eu-west")).await.unwrap(),
        Backend::start(BackendConfig::new("us-east")).await.unwrap(),
    )
}

async fn proxy(eu: &Backend, us: &Backend, admin: &str) -> ServerHandle {
    Server::bind(&config(eu, us, admin))
        .await
        .unwrap()
        .spawn()
        .unwrap()
}

/// Sends one GET and returns the status code and body.
async fn get(addr: SocketAddr, path: &str, authorization: Option<&str>) -> (u16, String, String) {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let auth = authorization
        .map(|value| format!("Authorization: {value}\r\n"))
        .unwrap_or_default();
    stream
        .write_all(format!("GET {path} HTTP/1.1\r\nHost: test\r\n{auth}\r\n").as_bytes())
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let (head, body) = response.split_once("\r\n\r\n").unwrap_or((&response, ""));
    let status = head
        .split(' ')
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or(0);
    (status, head.to_owned(), body.to_owned())
}

#[tokio::test]
async fn metrics_reflect_what_the_proxy_did() {
    let (eu, us) = backends().await;
    let proxy = proxy(&eu, &us, "listen = \"127.0.0.1:0\"").await;
    let admin = proxy.admin_addr().expect("admin port");

    let mut client = Connection::connect(
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
    .unwrap();
    client
        .simple_query("SELECT id FROM users WHERE tenant_id = 'bob'")
        .await
        .unwrap();
    let _ = client
        .simple_query("SELECT stddev(age) FROM users")
        .await
        .expect_err("refused");

    let (status, head, body) = get(admin, "/metrics", None).await;
    assert_eq!(status, 200, "{head}");
    assert!(head.contains("text/plain; version=0.0.4"));
    // The read went to us-east, which is not the home region.
    assert!(
        body.contains("luxbyte_region_statements_total{region=\"us-east\"} 1"),
        "{body}"
    );
    assert!(body.contains("luxbyte_remote_statements_total 1"), "{body}");
    assert!(
        body.contains("luxbyte_errors_total{sqlstate=\"0A000\"} 1"),
        "{body}"
    );
    assert!(body.contains("luxbyte_statement_duration_seconds_count{plan=\"single\"} 1"));
    assert!(body.contains("luxbyte_sessions_active 1"));
    assert!(body.contains("luxbyte_pool_opened{backend="));
}

#[tokio::test]
async fn health_needs_no_password_and_everything_else_does() {
    let (eu, us) = backends().await;
    let proxy = proxy(&eu, &us, "listen = \"127.0.0.1:0\"\npassword = \"s3cret\"").await;
    let admin = proxy.admin_addr().expect("admin port");

    assert_eq!(get(admin, "/healthz", None).await.0, 200);

    let (status, head, _) = get(admin, "/metrics", None).await;
    assert_eq!(status, 401);
    assert!(head.contains("WWW-Authenticate: Basic"));
    assert_eq!(get(admin, "/metrics", Some("Bearer wrong")).await.0, 401);
    assert_eq!(get(admin, "/metrics", Some("Bearer s3cret")).await.0, 200);
    // "op:s3cret", as a browser sends it.
    assert_eq!(get(admin, "/", Some("Basic b3A6czNjcmV0")).await.0, 200);
}

#[tokio::test]
async fn repeated_wrong_passwords_lock_the_address_out() {
    let (eu, us) = backends().await;
    let proxy = proxy(&eu, &us, "listen = \"127.0.0.1:0\"\npassword = \"s3cret\"").await;
    let admin = proxy.admin_addr().expect("admin port");
    for _ in 0..5 {
        assert_eq!(get(admin, "/metrics", Some("Bearer guess")).await.0, 401);
    }
    // Even the right password waits out the lock.
    let (status, head, _) = get(admin, "/metrics", Some("Bearer s3cret")).await;
    assert_eq!(status, 429);
    assert!(head.contains("Retry-After:"));
}

#[tokio::test]
async fn a_key_lookup_says_where_a_users_data_lives() {
    let (eu, us) = backends().await;
    let proxy = proxy(&eu, &us, "listen = \"127.0.0.1:0\"").await;
    let admin = proxy.admin_addr().expect("admin port");

    let (status, _, body) = get(admin, "/key/users/bob", None).await;
    assert_eq!(status, 200);
    let json: serde_json::Value = serde_json::from_str(&body).expect("json");
    assert_eq!(json["location"]["region"], "us-east");
    assert_eq!(json["local_to_this_proxy"], false);

    // The dashboard's form sends a query string instead.
    let (_, _, body) = get(admin, "/key?table=users&key=nobody", None).await;
    let json: serde_json::Value = serde_json::from_str(&body).expect("json");
    assert_eq!(json["location"]["region"], "eu-west");
}

#[tokio::test]
async fn the_snapshot_and_dashboard_describe_the_proxy() {
    let (eu, us) = backends().await;
    let proxy = proxy(&eu, &us, "listen = \"127.0.0.1:0\"").await;
    let admin = proxy.admin_addr().expect("admin port");

    let (_, _, body) = get(admin, "/snapshot", None).await;
    let json: serde_json::Value = serde_json::from_str(&body).expect("json");
    assert_eq!(json["home_region"], "eu-west");
    assert_eq!(json["regions"].as_array().map(Vec::len), Some(2));
    assert_eq!(json["directory_keys"], 1);

    let (status, head, body) = get(admin, "/", None).await;
    assert_eq!(status, 200);
    assert!(head.contains("Content-Security-Policy: default-src 'none'"));
    assert!(body.contains("home region eu-west"));
    assert!(!body.contains("<script"));
}

#[tokio::test]
async fn an_unprotected_admin_port_beyond_loopback_is_refused() {
    let (eu, us) = backends().await;
    let error = Server::bind(&config(&eu, &us, "listen = \"0.0.0.0:0\""))
        .await
        .expect_err("refused");
    assert!(error.to_string().contains("has no password"), "{error}");
    assert!(
        Server::bind(&config(&eu, &us, "listen = \"0.0.0.0:0\"\nopen = true"))
            .await
            .is_ok()
    );
}
