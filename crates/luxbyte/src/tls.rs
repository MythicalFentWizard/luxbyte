//! TLS to clients.
//!
//! A client asks for TLS with an `SSLRequest` before its startup packet; the
//! proxy answers one byte, `S` or `N`, and the handshake follows on the same
//! socket. Anything the client sent *after* its `SSLRequest` and before the
//! handshake arrived in the clear, and accepting it would let a man in the
//! middle put plaintext into what the client believes is an encrypted
//! session (CVE-2021-23214 was exactly this), so such a connection is
//! closed.

use std::sync::Arc;

use rustls::ServerConfig;
use rustls_pki_types::pem::PemObject as _;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;

use crate::config::ClientTls;

/// What accepts TLS from clients.
#[derive(Clone)]
pub struct Acceptor(TlsAcceptor);

impl std::fmt::Debug for Acceptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Acceptor")
    }
}

impl Acceptor {
    /// Performs the server side of a handshake.
    pub async fn accept(
        &self,
        stream: tokio::net::TcpStream,
    ) -> std::io::Result<tokio_rustls::server::TlsStream<tokio::net::TcpStream>> {
        self.0.accept(stream).await
    }
}

/// The acceptor for client connections, if a certificate is configured.
pub fn acceptor(config: &ClientTls) -> Result<Option<Acceptor>, String> {
    let (cert_file, key_file) = match (&config.cert_file, &config.key_file) {
        (None, None) if config.required => {
            return Err("tls.required needs tls.cert_file and tls.key_file".to_owned());
        }
        (None, None) => return Ok(None),
        (Some(cert), Some(key)) => (cert, key),
        _ => return Err("tls needs both cert_file and key_file".to_owned()),
    };
    let certificates = CertificateDer::pem_file_iter(cert_file)
        .and_then(Iterator::collect::<Result<Vec<_>, _>>)
        .map_err(|error| format!("could not read {}: {error}", cert_file.display()))?;
    if certificates.is_empty() {
        return Err(format!("{} holds no certificates", cert_file.display()));
    }
    let key = PrivateKeyDer::from_pem_file(key_file)
        .map_err(|error| format!("could not read {}: {error}", key_file.display()))?;
    let config =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|error| error.to_string())?
            .with_no_client_auth()
            .with_single_cert(certificates, key)
            .map_err(|error| format!("the certificate and key do not form a pair: {error}"))?;
    Ok(Some(Acceptor(TlsAcceptor::from(Arc::new(config)))))
}
