//! The untagged handshake messages.
//!
//! Before authentication, messages carry no tag byte; the first four bytes are
//! the length and the next four are a version code. Four codes matter: the
//! real protocol version, and three request codes that are versions only in
//! the sense that they occupy the same field.

use bytes::Bytes;

use crate::buf::{Reader, Writer};
use crate::error::ProtocolError;
use crate::raw::RawMessage;

/// Request code for `SSLRequest`.
pub const SSL_REQUEST_CODE: i32 = 80_877_103;
/// Request code for `CancelRequest`.
pub const CANCEL_REQUEST_CODE: i32 = 80_877_102;
/// Request code for `GSSENCRequest`.
pub const GSSENC_REQUEST_CODE: i32 = 80_877_104;

/// What a client sent as its first message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupRequest {
    /// A normal startup packet.
    Startup(StartupParameters),
    /// A request to negotiate TLS before the real startup packet.
    Ssl,
    /// A request to negotiate GSSAPI encryption.
    GssEnc,
    /// An out-of-band request to cancel a running query.
    Cancel(CancelRequest),
}

/// Identifies the backend whose query should be cancelled.
///
/// Sent on a brand-new connection that carries no credentials, so the secret
/// is the only thing standing between a stranger and cancelling your query.
/// A proxy must therefore hand out *its own* key pair to clients and keep the
/// real backend pair private — otherwise the cancel is unroutable, because
/// nothing in the request says which backend it was meant for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancelRequest {
    /// The process ID the server reported in `BackendKeyData`.
    pub process_id: i32,
    /// The secret the server reported in `BackendKeyData`.
    pub secret_key: i32,
}

/// The parameters a client asks for at startup.
///
/// Order is preserved. The proxy replays these to the backend, and while
/// PostgreSQL does not care about order, keeping it makes a captured
/// handshake and a replayed one compare byte for byte, which is worth a great
/// deal when debugging.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StartupParameters {
    version: i32,
    params: Vec<(String, String)>,
}

impl StartupParameters {
    /// Builds an empty parameter set at protocol 3.0.
    #[must_use]
    pub fn new() -> Self {
        Self {
            version: crate::PROTOCOL_VERSION_3,
            params: Vec::new(),
        }
    }

    /// The protocol version the client asked for, in packed form.
    #[must_use]
    pub const fn version(&self) -> i32 {
        self.version
    }

    /// The major protocol version.
    #[must_use]
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    pub const fn major(&self) -> u16 {
        ((self.version >> 16) & 0xFFFF) as u16
    }

    /// The minor protocol version.
    #[must_use]
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    pub const fn minor(&self) -> u16 {
        (self.version & 0xFFFF) as u16
    }

    /// Looks a parameter up by name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// The `user` parameter, which PostgreSQL requires.
    #[must_use]
    pub fn user(&self) -> Option<&str> {
        self.get("user")
    }

    /// The `database` parameter, which defaults to the user name.
    #[must_use]
    pub fn database(&self) -> Option<&str> {
        self.get("database").or_else(|| self.user())
    }

    /// Every parameter, in the order the client sent them.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.params.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// Sets a parameter, replacing any existing value for that name.
    pub fn set(&mut self, name: &str, value: &str) -> &mut Self {
        if let Some(slot) = self.params.iter_mut().find(|(k, _)| k == name) {
            value.clone_into(&mut slot.1);
        } else {
            self.params.push((name.to_owned(), value.to_owned()));
        }
        self
    }

    /// Removes a parameter.
    pub fn remove(&mut self, name: &str) -> &mut Self {
        self.params.retain(|(k, _)| k != name);
        self
    }

    /// Encodes the packet for sending to a backend.
    pub fn encode(&self) -> Result<Bytes, ProtocolError> {
        let mut w = Writer::untagged();
        w.i32(self.version);
        for (key, value) in &self.params {
            w.cstr(key);
            w.cstr(value);
        }
        // A bare NUL closes the list.
        w.u8(0);
        w.finish()
    }
}

impl StartupRequest {
    /// Decodes an untagged startup-phase message.
    pub fn decode(message: &RawMessage) -> Result<Self, ProtocolError> {
        let mut r = Reader::new(&message.body);
        let code = r.i32()?;
        match code {
            SSL_REQUEST_CODE => Ok(Self::Ssl),
            GSSENC_REQUEST_CODE => Ok(Self::GssEnc),
            CANCEL_REQUEST_CODE => Ok(Self::Cancel(CancelRequest {
                process_id: r.i32()?,
                secret_key: r.i32()?,
            })),
            version => {
                let major = u16::try_from((version >> 16) & 0xFFFF).unwrap_or(0);
                let minor = u16::try_from(version & 0xFFFF).unwrap_or(0);
                if major != 3 {
                    return Err(ProtocolError::UnsupportedVersion { major, minor });
                }
                let mut params = Vec::new();
                // The list ends at a zero-length name, which `cstr` returns as
                // an empty string; a body that simply runs out is also an end,
                // and tolerating it costs nothing.
                while !r.is_empty() {
                    let key = r.cstr()?;
                    if key.is_empty() {
                        break;
                    }
                    let value = r.cstr()?;
                    params.push((key.to_owned(), value.to_owned()));
                }
                Ok(Self::Startup(StartupParameters { version, params }))
            }
        }
    }
}

/// Builds a `CancelRequest` packet.
pub fn encode_cancel(request: CancelRequest) -> Result<Bytes, ProtocolError> {
    let mut w = Writer::untagged();
    w.i32(CANCEL_REQUEST_CODE);
    w.i32(request.process_id);
    w.i32(request.secret_key);
    w.finish()
}

/// Builds an `SSLRequest` packet.
pub fn encode_ssl_request() -> Result<Bytes, ProtocolError> {
    let mut w = Writer::untagged();
    w.i32(SSL_REQUEST_CODE);
    w.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(body: &[u8]) -> Result<StartupRequest, ProtocolError> {
        StartupRequest::decode(&RawMessage::untagged(Bytes::copy_from_slice(body)))
    }

    #[test]
    fn decodes_a_normal_startup_packet() {
        let mut body = crate::PROTOCOL_VERSION_3.to_be_bytes().to_vec();
        body.extend_from_slice(b"user\0alice\0database\0shop\0\0");
        let Ok(StartupRequest::Startup(params)) = decode(&body) else {
            panic!("expected a startup packet");
        };
        assert_eq!(params.user(), Some("alice"));
        assert_eq!(params.database(), Some("shop"));
        assert_eq!(params.major(), 3);
    }

    #[test]
    fn database_falls_back_to_the_user_name() {
        let mut body = crate::PROTOCOL_VERSION_3.to_be_bytes().to_vec();
        body.extend_from_slice(b"user\0alice\0\0");
        let Ok(StartupRequest::Startup(params)) = decode(&body) else {
            panic!("expected a startup packet");
        };
        assert_eq!(params.database(), Some("alice"));
    }

    #[test]
    fn recognises_the_three_request_codes() {
        assert!(matches!(
            decode(&SSL_REQUEST_CODE.to_be_bytes()),
            Ok(StartupRequest::Ssl)
        ));
        assert!(matches!(
            decode(&GSSENC_REQUEST_CODE.to_be_bytes()),
            Ok(StartupRequest::GssEnc)
        ));
        let mut cancel = CANCEL_REQUEST_CODE.to_be_bytes().to_vec();
        cancel.extend_from_slice(&7_i32.to_be_bytes());
        cancel.extend_from_slice(&99_i32.to_be_bytes());
        assert!(matches!(
            decode(&cancel),
            Ok(StartupRequest::Cancel(CancelRequest {
                process_id: 7,
                secret_key: 99
            }))
        ));
    }

    #[test]
    fn rejects_protocol_2() {
        let v2: i32 = 2 << 16;
        assert!(matches!(
            decode(&v2.to_be_bytes()),
            Err(ProtocolError::UnsupportedVersion { major: 2, minor: 0 })
        ));
    }

    #[test]
    fn parameters_survive_a_round_trip_in_order() {
        let mut params = StartupParameters::new();
        params
            .set("user", "alice")
            .set("database", "shop")
            .set("application_name", "psql");
        let encoded = params.encode().ok();
        let body = encoded
            .as_ref()
            .and_then(|b| b.get(4..))
            .map(Bytes::copy_from_slice);
        let Some(Ok(StartupRequest::Startup(back))) =
            body.map(|b| StartupRequest::decode(&RawMessage::untagged(b)))
        else {
            panic!("expected a startup packet");
        };
        assert_eq!(back, params);
        assert_eq!(
            back.iter().map(|(k, _)| k).collect::<Vec<_>>(),
            ["user", "database", "application_name"]
        );
    }

    #[test]
    fn setting_a_parameter_twice_replaces_it_in_place() {
        let mut params = StartupParameters::new();
        params.set("user", "alice").set("x", "1").set("user", "bob");
        assert_eq!(params.user(), Some("bob"));
        assert_eq!(params.iter().count(), 2);
    }
}
