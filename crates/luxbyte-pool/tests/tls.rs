//! TLS to backends, in every libpq mode, against a real handshake.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::time::Duration;

use luxbyte_pool::{BackendTls, Connection, ConnectionTarget, Credentials, PoolError, TlsMode};
use luxbyte_testkit::{Backend, BackendConfig, TestCertificates};

fn target(backend: &Backend, host: &str) -> ConnectionTarget {
    ConnectionTarget {
        host: host.to_owned(),
        port: backend.port(),
        database: "shop".to_owned(),
        user: "app".to_owned(),
    }
}

async fn connect(
    backend: &Backend,
    host: &str,
    mode: TlsMode,
    ca: Option<&str>,
) -> Result<Connection, PoolError> {
    let tls = BackendTls::new(mode, ca.map(str::as_bytes)).expect("tls config");
    Connection::connect_with(
        &target(backend, host),
        &Credentials::Trust,
        Duration::from_secs(5),
        &tls,
    )
    .await
}

async fn tls_backend(names: &[&str]) -> (Backend, TestCertificates) {
    let certificates = TestCertificates::generate(names).expect("certificates");
    let backend = Backend::start(BackendConfig::new("eu").with_tls(certificates.clone()))
        .await
        .expect("backend");
    (backend, certificates)
}

#[tokio::test]
async fn verify_full_checks_the_chain_and_the_host_name() {
    let (backend, certificates) = tls_backend(&["localhost"]).await;
    let mut connection = connect(
        &backend,
        "localhost",
        TlsMode::VerifyFull,
        Some(&certificates.ca_pem),
    )
    .await
    .expect("the certificate names localhost and chains to the CA");
    assert!(connection.is_encrypted());
    assert!(connection.simple_query("SELECT 1").await.is_ok());
    assert_eq!(backend.tls_connections(), 1);

    // The same certificate does not name 127.0.0.1.
    let error = connect(
        &backend,
        "127.0.0.1",
        TlsMode::VerifyFull,
        Some(&certificates.ca_pem),
    )
    .await
    .expect_err("wrong host name");
    assert!(error.to_string().contains("tls"), "{error}");
}

#[tokio::test]
async fn verify_full_refuses_a_certificate_from_another_authority() {
    let (backend, _) = tls_backend(&["localhost"]).await;
    let stranger = TestCertificates::generate(&["localhost"]).expect("certificates");
    let error = connect(
        &backend,
        "localhost",
        TlsMode::VerifyFull,
        Some(&stranger.ca_pem),
    )
    .await
    .expect_err("a man in the middle with his own CA");
    assert!(error.to_string().contains("tls"), "{error}");
}

#[tokio::test]
async fn verify_ca_checks_the_chain_but_not_the_name() {
    let (backend, certificates) = tls_backend(&["db.internal"]).await;
    assert!(
        connect(
            &backend,
            "127.0.0.1",
            TlsMode::VerifyCa,
            Some(&certificates.ca_pem)
        )
        .await
        .expect("the chain is good")
        .is_encrypted()
    );
    let stranger = TestCertificates::generate(&["db.internal"]).expect("certificates");
    assert!(
        connect(
            &backend,
            "127.0.0.1",
            TlsMode::VerifyCa,
            Some(&stranger.ca_pem)
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn require_encrypts_without_checking_and_fails_without_tls() {
    let (backend, _) = tls_backend(&["anything"]).await;
    assert!(
        connect(&backend, "127.0.0.1", TlsMode::Require, None)
            .await
            .expect("encrypts")
            .is_encrypted()
    );

    let plain = Backend::start(BackendConfig::new("plain"))
        .await
        .expect("backend");
    let error = connect(&plain, "127.0.0.1", TlsMode::Require, None)
        .await
        .expect_err("the server declined TLS");
    assert!(error.to_string().contains("does not offer TLS"), "{error}");
}

#[tokio::test]
async fn prefer_encrypts_when_it_can_and_carries_on_when_it_cannot() {
    let (backend, _) = tls_backend(&["anything"]).await;
    assert!(
        connect(&backend, "127.0.0.1", TlsMode::Prefer, None)
            .await
            .expect("encrypts")
            .is_encrypted()
    );
    let plain = Backend::start(BackendConfig::new("plain"))
        .await
        .expect("backend");
    let mut connection = connect(&plain, "127.0.0.1", TlsMode::Prefer, None)
        .await
        .expect("falls back to plain text");
    assert!(!connection.is_encrypted());
    assert!(connection.simple_query("SELECT 1").await.is_ok());
}
