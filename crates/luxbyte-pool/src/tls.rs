//! Encrypting connections to backends.
//!
//! The modes are libpq's `sslmode`, with libpq's meanings, so an operator
//! who knows one knows the other:
//!
//! | mode | encrypted | certificate checked |
//! |---|---|---|
//! | `disable` | never | — |
//! | `prefer` | if the server offers it | no |
//! | `require` | always | no |
//! | `verify-ca` | always | chain, not host name |
//! | `verify-full` | always | chain and host name |
//!
//! Only `verify-full` stops a man in the middle; `prefer` and `require`
//! stop a passive eavesdropper. Without a CA file, chains are checked
//! against Mozilla's roots, which suits a managed database with a public
//! certificate and not one signed by a private CA.

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use tokio_rustls::TlsConnector;

use crate::error::PoolError;

/// How a backend connection is encrypted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TlsMode {
    /// Never.
    #[default]
    Disable,
    /// When the server offers it, without checking its certificate.
    Prefer,
    /// Always, without checking the certificate.
    Require,
    /// Always, checking the certificate chain but not the host name.
    VerifyCa,
    /// Always, checking the chain and that it names the host.
    VerifyFull,
}

impl std::fmt::Display for TlsMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Disable => "disable",
            Self::Prefer => "prefer",
            Self::Require => "require",
            Self::VerifyCa => "verify-ca",
            Self::VerifyFull => "verify-full",
        })
    }
}

/// A mode and the TLS client configuration that implements it.
#[derive(Clone)]
pub struct BackendTls {
    mode: TlsMode,
    connector: Option<TlsConnector>,
}

impl std::fmt::Debug for BackendTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendTls")
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

// Two configurations for the same mode behave alike; that is all a pool's
// configuration is compared for.
impl PartialEq for BackendTls {
    fn eq(&self, other: &Self) -> bool {
        self.mode == other.mode
    }
}

impl Eq for BackendTls {}

impl Default for BackendTls {
    fn default() -> Self {
        Self::disabled()
    }
}

impl BackendTls {
    /// No encryption.
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            mode: TlsMode::Disable,
            connector: None,
        }
    }

    /// The given mode, checking chains against `ca_pem` when given and
    /// against Mozilla's roots otherwise.
    pub fn new(mode: TlsMode, ca_pem: Option<&[u8]>) -> Result<Self, PoolError> {
        if mode == TlsMode::Disable {
            return Ok(Self::disabled());
        }
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = ClientConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .map_err(|error| PoolError::Tls(error.to_string()))?;

        let config = match mode {
            TlsMode::Disable | TlsMode::Prefer | TlsMode::Require => builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(Unverified { provider }))
                .with_no_client_auth(),
            TlsMode::VerifyCa | TlsMode::VerifyFull => {
                let roots = roots(ca_pem)?;
                let verifier = rustls::client::WebPkiServerVerifier::builder_with_provider(
                    Arc::new(roots),
                    provider,
                )
                .build()
                .map_err(|error| PoolError::Tls(error.to_string()))?;
                if mode == TlsMode::VerifyCa {
                    builder
                        .dangerous()
                        .with_custom_certificate_verifier(Arc::new(ChainOnly { inner: verifier }))
                        .with_no_client_auth()
                } else {
                    builder
                        .dangerous()
                        .with_custom_certificate_verifier(verifier)
                        .with_no_client_auth()
                }
            }
        };
        Ok(Self {
            mode,
            connector: Some(TlsConnector::from(Arc::new(config))),
        })
    }

    /// The mode.
    #[must_use]
    pub const fn mode(&self) -> TlsMode {
        self.mode
    }

    pub(crate) const fn connector(&self) -> Option<&TlsConnector> {
        self.connector.as_ref()
    }
}

fn roots(ca_pem: Option<&[u8]>) -> Result<RootCertStore, PoolError> {
    use rustls_pki_types::pem::PemObject as _;
    let mut store = RootCertStore::empty();
    match ca_pem {
        Some(pem) => {
            for certificate in CertificateDer::pem_slice_iter(pem) {
                let certificate =
                    certificate.map_err(|error| PoolError::Tls(format!("bad CA file: {error}")))?;
                store
                    .add(certificate)
                    .map_err(|error| PoolError::Tls(format!("bad CA certificate: {error}")))?;
            }
            if store.is_empty() {
                return Err(PoolError::Tls(
                    "the CA file holds no certificates".to_owned(),
                ));
            }
        }
        None => store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
    }
    Ok(store)
}

/// Accepts any certificate, as `sslmode=require` does, while still
/// checking the handshake's signatures: the server has to hold the key for
/// the certificate it presents, even if nobody vouches for it.
#[derive(Debug)]
struct Unverified {
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for Unverified {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Checks the chain but not the host name, as `sslmode=verify-ca` does.
#[derive(Debug)]
struct ChainOnly {
    inner: Arc<rustls::client::WebPkiServerVerifier>,
}

impl ServerCertVerifier for ChainOnly {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}
