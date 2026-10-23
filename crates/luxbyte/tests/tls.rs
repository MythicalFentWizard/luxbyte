//! TLS between client, proxy and backend, end to end.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::path::PathBuf;
use std::time::Duration;

use luxbyte::{Config, Server, ServerHandle};
use luxbyte_pool::{BackendTls, Connection, ConnectionTarget, Credentials, TlsMode};
use luxbyte_testkit::{Backend, BackendConfig, TestCertificates};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Writes the proxy's certificate and key where the configuration can name
/// them.
fn files(certificates: &TestCertificates) -> (PathBuf, PathBuf) {
    let directory = std::env::temp_dir().join(format!(
        "luxbyte-tls-{}-{}",
        std::process::id(),
        rand_suffix()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let cert = directory.join("cert.pem");
    let key = directory.join("key.pem");
    std::fs::write(&cert, &certificates.cert_pem).unwrap();
    std::fs::write(&key, &certificates.key_pem).unwrap();
    (cert, key)
}

fn rand_suffix() -> u64 {
    use std::hash::{BuildHasher, RandomState};
    RandomState::new().hash_one(std::time::Instant::now())
}

async fn proxy(backend: &Backend, extra: &str) -> ServerHandle {
    let config = Config::from_toml(&format!(
        r#"
[server]
listen = "127.0.0.1:0"
node = "test-proxy"

{extra}

[topology]
home_region = "eu-west"

[[topology.regions]]
id = "eu-west"
primary = {{ host = "localhost", port = {} }}
"#,
        backend.port()
    ))
    .expect("config");
    Server::bind(&config)
        .await
        .expect("bind")
        .spawn()
        .expect("serve")
}

fn backend_section(mode: &str, ca: Option<&PathBuf>) -> String {
    format!(
        "[backend]\nuser = \"app\"\ndatabase = \"shop\"\ntls = \"{mode}\"\n{}",
        ca.map(|path| format!("tls_ca_file = {:?}\n", path.display().to_string()))
            .unwrap_or_default()
    )
}

async fn client(
    proxy: &ServerHandle,
    tls: &BackendTls,
) -> Result<Connection, luxbyte_pool::PoolError> {
    Connection::connect_with(
        &ConnectionTarget {
            host: "localhost".to_owned(),
            port: proxy.port(),
            database: "shop".to_owned(),
            user: "app".to_owned(),
        },
        &Credentials::Trust,
        Duration::from_secs(5),
        tls,
    )
    .await
}

#[tokio::test]
async fn a_client_can_verify_the_proxy_and_the_proxy_its_backend() {
    let backend_certificates = TestCertificates::generate(&["localhost"]).unwrap();
    let backend = Backend::start(BackendConfig::new("eu").with_tls(backend_certificates.clone()))
        .await
        .unwrap();
    let ca_file = {
        let (cert, _) = files(&backend_certificates);
        let ca = cert.with_file_name("ca.pem");
        std::fs::write(&ca, &backend_certificates.ca_pem).unwrap();
        ca
    };

    let proxy_certificates = TestCertificates::generate(&["localhost"]).unwrap();
    let (cert, key) = files(&proxy_certificates);
    let proxy = proxy(
        &backend,
        &format!(
            "{}\n[tls]\ncert_file = {:?}\nkey_file = {:?}\n",
            backend_section("verify-full", Some(&ca_file)),
            cert.display().to_string(),
            key.display().to_string()
        ),
    )
    .await;

    let tls = BackendTls::new(
        TlsMode::VerifyFull,
        Some(proxy_certificates.ca_pem.as_bytes()),
    )
    .unwrap();
    let mut client = client(&proxy, &tls)
        .await
        .expect("verified TLS to the proxy");
    assert!(client.is_encrypted());
    client.simple_query("SELECT 1").await.expect("query");
    // And the proxy reached the backend over verified TLS too.
    assert_eq!(backend.tls_connections(), 1);
    assert!(backend.saw("SELECT 1"));
}

#[tokio::test]
async fn a_proxy_that_requires_tls_refuses_plain_clients() {
    let backend = Backend::start(BackendConfig::new("eu")).await.unwrap();
    let certificates = TestCertificates::generate(&["localhost"]).unwrap();
    let (cert, key) = files(&certificates);
    let proxy = proxy(
        &backend,
        &format!(
            "{}\n[tls]\ncert_file = {:?}\nkey_file = {:?}\nrequired = true\n",
            backend_section("disable", None),
            cert.display().to_string(),
            key.display().to_string()
        ),
    )
    .await;

    let error = client(&proxy, &BackendTls::disabled())
        .await
        .expect_err("plain text refused");
    assert_eq!(error.sqlstate(), Some("28000"), "{error}");
    let tls = BackendTls::new(TlsMode::Require, None).unwrap();
    assert!(client(&proxy, &tls).await.is_ok());
}

#[tokio::test]
async fn bytes_sent_before_the_handshake_are_not_taken_as_encrypted() {
    // CVE-2021-23214: plaintext a man in the middle slipped in after the
    // SSLRequest must not become part of the encrypted session.
    let backend = Backend::start(BackendConfig::new("eu")).await.unwrap();
    let certificates = TestCertificates::generate(&["localhost"]).unwrap();
    let (cert, key) = files(&certificates);
    let proxy = proxy(
        &backend,
        &format!(
            "{}\n[tls]\ncert_file = {:?}\nkey_file = {:?}\n",
            backend_section("disable", None),
            cert.display().to_string(),
            key.display().to_string()
        ),
    )
    .await;

    let mut stream = tokio::net::TcpStream::connect(proxy.addr()).await.unwrap();
    let mut injected = luxbyte_proto::startup::encode_ssl_request()
        .unwrap()
        .to_vec();
    let mut startup = luxbyte_proto::startup::StartupParameters::new();
    startup.set("user", "app");
    injected.extend_from_slice(&startup.encode().unwrap());
    stream.write_all(&injected).await.unwrap();
    let mut answer = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut answer)).await;
    assert!(
        answer.is_empty(),
        "the proxy answered {answer:?} instead of hanging up"
    );
}

#[tokio::test]
async fn a_gssapi_request_is_declined_so_the_client_can_carry_on() {
    // libpq built with GSSAPI asks for GSS encryption first, and expects
    // `N` rather than an error.
    let backend = Backend::start(BackendConfig::new("eu")).await.unwrap();
    let proxy = proxy(&backend, &backend_section("disable", None)).await;

    let mut stream = tokio::net::TcpStream::connect(proxy.addr()).await.unwrap();
    let mut request = 8_i32.to_be_bytes().to_vec();
    request.extend_from_slice(&luxbyte_proto::startup::GSSENC_REQUEST_CODE.to_be_bytes());
    stream.write_all(&request).await.unwrap();
    let mut answer = [0_u8; 1];
    stream.read_exact(&mut answer).await.unwrap();
    assert_eq!(&answer, b"N");

    let mut startup = luxbyte_proto::startup::StartupParameters::new();
    startup.set("user", "app").set("database", "shop");
    stream.write_all(&startup.encode().unwrap()).await.unwrap();
    let mut first = [0_u8; 1];
    stream.read_exact(&mut first).await.unwrap();
    assert_eq!(&first, b"R", "an authentication message follows");
}

#[tokio::test]
async fn the_admin_port_can_serve_https_and_drops_plain_http() {
    let backend = Backend::start(BackendConfig::new("eu")).await.unwrap();
    let certificates = TestCertificates::generate(&["localhost"]).unwrap();
    let (cert, key) = files(&certificates);
    let proxy = proxy(
        &backend,
        &format!(
            "{}\n[tls]\ncert_file = {:?}\nkey_file = {:?}\n\n[admin]\nlisten = \"127.0.0.1:0\"\ntls = true\n",
            backend_section("disable", None),
            cert.display().to_string(),
            key.display().to_string()
        ),
    )
    .await;
    let admin = proxy.admin_addr().expect("admin port");

    let mut roots = rustls::RootCertStore::empty();
    {
        use rustls_pki_types::pem::PemObject as _;
        for certificate in
            rustls_pki_types::CertificateDer::pem_slice_iter(certificates.ca_pem.as_bytes())
        {
            roots.add(certificate.unwrap()).unwrap();
        }
    }
    let client = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(client));
    let stream = tokio::net::TcpStream::connect(admin).await.unwrap();
    let mut stream = connector
        .connect(
            rustls_pki_types::ServerName::try_from("localhost").unwrap(),
            stream,
        )
        .await
        .expect("verified TLS to the admin port");
    stream
        .write_all(b"GET /healthz HTTP/1.1\r\nHost: test\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    let _ = stream.read_to_end(&mut response).await;
    let response = String::from_utf8_lossy(&response);
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");

    // A plain request gets no answer at all, so nothing leaks in the clear.
    let mut plain = tokio::net::TcpStream::connect(admin).await.unwrap();
    plain
        .write_all(b"GET /healthz HTTP/1.1\r\nHost: test\r\n\r\n")
        .await
        .unwrap();
    let mut answer = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), plain.read_to_end(&mut answer)).await;
    assert!(
        !String::from_utf8_lossy(&answer).contains("HTTP/1.1"),
        "plain HTTP was answered: {answer:?}"
    );
}

#[tokio::test]
async fn admin_tls_without_a_certificate_is_refused_at_start() {
    let backend = Backend::start(BackendConfig::new("eu")).await.unwrap();
    let config = Config::from_toml(&format!(
        r#"
[server]
listen = "127.0.0.1:0"
node = "test-proxy"

{}

[admin]
listen = "127.0.0.1:0"
tls = true

[topology]
home_region = "eu-west"

[[topology.regions]]
id = "eu-west"
primary = {{ host = "localhost", port = {} }}
"#,
        backend_section("disable", None),
        backend.port()
    ))
    .expect("config");
    let error = Server::bind(&config).await.expect_err("refused");
    assert!(error.to_string().contains("admin.tls"), "{error}");
}
