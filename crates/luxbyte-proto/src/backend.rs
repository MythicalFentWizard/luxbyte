//! Typed views of, and builders for, server-to-client messages.
//!
//! The proxy both reads these (to follow transaction state and authentication)
//! and writes them (to answer console queries and to report its own errors in
//! a form every PostgreSQL client already understands).

use bytes::Bytes;

use crate::buf::{Reader, Writer};
use crate::error::{Peer, ProtocolError};
use crate::raw::{RawMessage, tag};

/// The transaction state reported by every `ReadyForQuery`.
///
/// This is the proxy's ground truth for whether a backend connection can be
/// returned to the pool. Handing a connection back mid-transaction leaks the
/// transaction onto whoever borrows it next, and a failed transaction that is
/// never rolled back holds locks indefinitely.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TransactionStatus {
    /// Not in a transaction block.
    #[default]
    Idle,
    /// Inside a transaction block.
    InTransaction,
    /// Inside a failed transaction block; only `ROLLBACK` will be accepted.
    Failed,
}

impl TransactionStatus {
    /// Decodes the byte the wire uses.
    pub fn from_byte(byte: u8) -> Result<Self, ProtocolError> {
        match byte {
            b'I' => Ok(Self::Idle),
            b'T' => Ok(Self::InTransaction),
            b'E' => Ok(Self::Failed),
            other => Err(ProtocolError::UnexpectedTag {
                tag: char::from(other),
                peer: Peer::Backend,
            }),
        }
    }

    /// The byte the wire uses.
    #[must_use]
    pub const fn as_byte(self) -> u8 {
        match self {
            Self::Idle => b'I',
            Self::InTransaction => b'T',
            Self::Failed => b'E',
        }
    }

    /// Whether a connection in this state may be returned to the pool as is.
    #[must_use]
    pub const fn is_poolable(self) -> bool {
        matches!(self, Self::Idle)
    }
}

/// Which authentication exchange the server is asking for.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Authentication<'a> {
    /// Authentication succeeded.
    Ok,
    /// Send the password as plaintext.
    CleartextPassword,
    /// Send `md5(md5(password + user) + salt)`.
    Md5Password {
        /// The four-byte salt.
        salt: [u8; 4],
    },
    /// Begin SASL with one of the offered mechanisms.
    Sasl {
        /// Mechanism names, in the server's order of preference.
        mechanisms: Vec<&'a str>,
    },
    /// A SASL challenge.
    SaslContinue {
        /// Mechanism-specific data.
        data: &'a [u8],
    },
    /// The final SASL message.
    SaslFinal {
        /// Mechanism-specific data.
        data: &'a [u8],
    },
    /// Something this crate does not implement, such as GSSAPI or SSPI.
    Unsupported {
        /// The sub-kind code from the message body.
        kind: i32,
    },
}

/// One field of an `ErrorResponse` or `NoticeResponse`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoticeField<'a> {
    /// The single-byte field type, such as `b'C'` for SQLSTATE.
    pub kind: u8,
    /// The field's value.
    pub value: &'a str,
}

/// A decoded `ErrorResponse` or `NoticeResponse`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Notice<'a> {
    /// Every field, in the order the server sent them.
    pub fields: Vec<NoticeField<'a>>,
}

impl<'a> Notice<'a> {
    /// Looks a field up by its type byte.
    #[must_use]
    pub fn field(&self, kind: u8) -> Option<&'a str> {
        self.fields.iter().find(|f| f.kind == kind).map(|f| f.value)
    }

    /// The localised severity (`S`), falling back to the non-localised `V`.
    #[must_use]
    pub fn severity(&self) -> &'a str {
        self.field(b'S')
            .or_else(|| self.field(b'V'))
            .unwrap_or("ERROR")
    }

    /// The five-character SQLSTATE (`C`).
    #[must_use]
    pub fn code(&self) -> &'a str {
        self.field(b'C').unwrap_or("XX000")
    }

    /// The primary message (`M`).
    #[must_use]
    pub fn message(&self) -> &'a str {
        self.field(b'M').unwrap_or("")
    }

    /// Turns the notice into a standalone error.
    #[must_use]
    pub fn to_error(&self) -> ProtocolError {
        ProtocolError::Server {
            severity: self.severity().to_owned(),
            code: self.code().to_owned(),
            message: self.message().to_owned(),
        }
    }
}

/// One column of a `RowDescription`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDescription<'a> {
    /// Column name.
    pub name: &'a str,
    /// OID of the table it came from, or `0` if it is computed.
    pub table_oid: i32,
    /// Attribute number within that table, or `0`.
    pub column_id: i16,
    /// OID of the column's datatype.
    pub type_oid: i32,
    /// Datatype size; negative for variable-length types.
    pub type_size: i16,
    /// Type-specific modifier.
    pub type_modifier: i32,
    /// Whether the values arrive as text or binary.
    pub format: i16,
}

/// A server-to-client message, decoded if the proxy depends on it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Backend<'a> {
    /// An `Authentication*` message.
    Authentication(Authentication<'a>),
    /// `BackendKeyData`, needed to route a later `CancelRequest`.
    BackendKeyData {
        /// The backend's process ID.
        process_id: i32,
        /// The secret that authorises cancelling its queries.
        secret_key: i32,
    },
    /// A `ParameterStatus` report.
    ParameterStatus {
        /// Parameter name, such as `server_encoding`.
        name: &'a str,
        /// Its new value.
        value: &'a str,
    },
    /// `ReadyForQuery`, carrying the transaction state.
    ReadyForQuery {
        /// The state the server is now in.
        status: TransactionStatus,
    },
    /// An `ErrorResponse`.
    Error(Notice<'a>),
    /// A `NoticeResponse`.
    Notice(Notice<'a>),
    /// A `CommandComplete`.
    CommandComplete {
        /// The command tag, such as `INSERT 0 1` or `SELECT 3`.
        tag: &'a str,
    },
    /// A `RowDescription`.
    RowDescription {
        /// One entry per result column.
        fields: Vec<FieldDescription<'a>>,
    },
    /// A `DataRow`.
    DataRow {
        /// One entry per column, with `None` for SQL `NULL`.
        columns: Vec<Option<&'a [u8]>>,
    },
    /// An `EmptyQueryResponse`.
    EmptyQueryResponse,
    /// A `NoData`.
    NoData,
    /// Anything else, left opaque.
    Other {
        /// The tag byte.
        tag: u8,
    },
}

impl<'a> Backend<'a> {
    /// Decodes a message that arrived from a database server.
    pub fn decode(message: &'a RawMessage) -> Result<Self, ProtocolError> {
        let Some(tag_byte) = message.tag else {
            return Err(ProtocolError::UnexpectedTag {
                tag: '\0',
                peer: Peer::Backend,
            });
        };
        let mut r = Reader::new(&message.body);
        match tag_byte {
            tag::AUTHENTICATION => Ok(Self::Authentication(decode_authentication(&mut r)?)),
            tag::BACKEND_KEY_DATA => Ok(Self::BackendKeyData {
                process_id: r.i32()?,
                secret_key: r.i32()?,
            }),
            tag::SYNC_OR_PARAMETER_STATUS => Ok(Self::ParameterStatus {
                name: r.cstr()?,
                value: r.cstr()?,
            }),
            tag::READY_FOR_QUERY => Ok(Self::ReadyForQuery {
                status: TransactionStatus::from_byte(r.u8()?)?,
            }),
            tag::EXECUTE_OR_ERROR => Ok(Self::Error(decode_notice(&mut r)?)),
            tag::NOTICE_RESPONSE => Ok(Self::Notice(decode_notice(&mut r)?)),
            tag::CLOSE_OR_COMMAND_COMPLETE => Ok(Self::CommandComplete { tag: r.cstr()? }),
            tag::TERMINATE_OR_ROW_DESCRIPTION => {
                let count = count(r.i16()?)?;
                let mut fields = Vec::with_capacity(count.min(256));
                for _ in 0..count {
                    fields.push(FieldDescription {
                        name: r.cstr()?,
                        table_oid: r.i32()?,
                        column_id: r.i16()?,
                        type_oid: r.i32()?,
                        type_size: r.i16()?,
                        type_modifier: r.i32()?,
                        format: r.i16()?,
                    });
                }
                Ok(Self::RowDescription { fields })
            }
            tag::DESCRIBE_OR_DATA_ROW => {
                let count = count(r.i16()?)?;
                let mut columns = Vec::with_capacity(count.min(256));
                for _ in 0..count {
                    columns.push(r.nullable_bytes()?);
                }
                Ok(Self::DataRow { columns })
            }
            tag::EMPTY_QUERY_RESPONSE => Ok(Self::EmptyQueryResponse),
            tag::NO_DATA => Ok(Self::NoData),
            other => Ok(Self::Other { tag: other }),
        }
    }
}

fn decode_authentication<'a>(r: &mut Reader<'a>) -> Result<Authentication<'a>, ProtocolError> {
    match r.i32()? {
        0 => Ok(Authentication::Ok),
        3 => Ok(Authentication::CleartextPassword),
        5 => {
            let salt: [u8; 4] = r
                .take(4)?
                .try_into()
                .map_err(|_| ProtocolError::truncated(4, 0))?;
            Ok(Authentication::Md5Password { salt })
        }
        10 => {
            let mut mechanisms = Vec::new();
            // The list ends at an empty name.
            while !r.is_empty() {
                let name = r.cstr()?;
                if name.is_empty() {
                    break;
                }
                mechanisms.push(name);
            }
            Ok(Authentication::Sasl { mechanisms })
        }
        11 => Ok(Authentication::SaslContinue {
            data: r.take_rest(),
        }),
        12 => Ok(Authentication::SaslFinal {
            data: r.take_rest(),
        }),
        kind => Ok(Authentication::Unsupported { kind }),
    }
}

fn decode_notice<'a>(r: &mut Reader<'a>) -> Result<Notice<'a>, ProtocolError> {
    let mut fields = Vec::new();
    loop {
        let kind = r.u8()?;
        if kind == 0 {
            break;
        }
        fields.push(NoticeField {
            kind,
            value: r.cstr()?,
        });
    }
    Ok(Notice { fields })
}

fn count(value: i16) -> Result<usize, ProtocolError> {
    usize::try_from(value).map_err(|_| ProtocolError::ValueOutOfRange {
        value: i64::from(value),
        width: "count",
    })
}

/// Builders for messages the proxy itself emits.
///
/// These exist so Luxbyte can answer a client directly — for console queries,
/// and for refusals it decides on without ever reaching a database. A client
/// cannot tell the difference, which is the point.
pub mod build {
    use super::{Bytes, ProtocolError, TransactionStatus, Writer, tag};

    /// A `ReadyForQuery`.
    pub fn ready_for_query(status: TransactionStatus) -> Result<Bytes, ProtocolError> {
        let mut w = Writer::tagged(tag::READY_FOR_QUERY);
        w.u8(status.as_byte());
        w.finish()
    }

    /// An `AuthenticationOk`.
    pub fn authentication_ok() -> Result<Bytes, ProtocolError> {
        let mut w = Writer::tagged(tag::AUTHENTICATION);
        w.i32(0);
        w.finish()
    }

    /// An `AuthenticationSASL` offering one mechanism.
    pub fn authentication_sasl(mechanism: &str) -> Result<Bytes, ProtocolError> {
        let mut w = Writer::tagged(tag::AUTHENTICATION);
        w.i32(10);
        w.cstr(mechanism);
        w.u8(0);
        w.finish()
    }

    /// An `AuthenticationSASLContinue`.
    pub fn authentication_sasl_continue(data: &[u8]) -> Result<Bytes, ProtocolError> {
        let mut w = Writer::tagged(tag::AUTHENTICATION);
        w.i32(11);
        w.bytes(data);
        w.finish()
    }

    /// An `AuthenticationSASLFinal`.
    pub fn authentication_sasl_final(data: &[u8]) -> Result<Bytes, ProtocolError> {
        let mut w = Writer::tagged(tag::AUTHENTICATION);
        w.i32(12);
        w.bytes(data);
        w.finish()
    }

    /// A `ParameterStatus`.
    pub fn parameter_status(name: &str, value: &str) -> Result<Bytes, ProtocolError> {
        let mut w = Writer::tagged(tag::SYNC_OR_PARAMETER_STATUS);
        w.cstr(name);
        w.cstr(value);
        w.finish()
    }

    /// A `BackendKeyData`.
    pub fn backend_key_data(process_id: i32, secret_key: i32) -> Result<Bytes, ProtocolError> {
        let mut w = Writer::tagged(tag::BACKEND_KEY_DATA);
        w.i32(process_id);
        w.i32(secret_key);
        w.finish()
    }

    /// A `CommandComplete`.
    pub fn command_complete(command_tag: &str) -> Result<Bytes, ProtocolError> {
        let mut w = Writer::tagged(tag::CLOSE_OR_COMMAND_COMPLETE);
        w.cstr(command_tag);
        w.finish()
    }

    /// An `EmptyQueryResponse`.
    pub fn empty_query_response() -> Result<Bytes, ProtocolError> {
        Writer::tagged(tag::EMPTY_QUERY_RESPONSE).finish()
    }

    /// A `RowDescription` of all-text columns of the given type.
    ///
    /// Console results are text, so the full descriptor is unnecessary
    /// ceremony; this covers every row the proxy generates itself.
    pub fn row_description(columns: &[&str], type_oid: i32) -> Result<Bytes, ProtocolError> {
        let mut w = Writer::tagged(tag::TERMINATE_OR_ROW_DESCRIPTION);
        w.count16(columns.len())?;
        for name in columns {
            w.cstr(name);
            w.i32(0); // table OID: computed, not from a table
            w.i16(0); // column attribute number
            w.i32(type_oid);
            w.i16(-1); // variable length
            w.i32(-1); // no type modifier
            w.i16(0); // text format
        }
        w.finish()
    }

    /// A `DataRow` of text values, with `None` for SQL `NULL`.
    pub fn data_row(values: &[Option<&str>]) -> Result<Bytes, ProtocolError> {
        let mut w = Writer::tagged(tag::DESCRIBE_OR_DATA_ROW);
        w.count16(values.len())?;
        for value in values {
            w.nullable_bytes(value.map(str::as_bytes))?;
        }
        w.finish()
    }

    /// Builds an `ErrorResponse` or `NoticeResponse`.
    ///
    /// Every refusal Luxbyte makes travels this way, with a real SQLSTATE, so
    /// that a client's existing error handling applies unchanged.
    #[derive(Debug, Clone)]
    pub struct ErrorBuilder {
        severity: String,
        code: String,
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        is_notice: bool,
    }

    impl ErrorBuilder {
        /// Starts an `ERROR` with the given SQLSTATE and message.
        #[must_use]
        pub fn error(code: &str, message: impl Into<String>) -> Self {
            Self {
                severity: "ERROR".to_owned(),
                code: code.to_owned(),
                message: message.into(),
                detail: None,
                hint: None,
                is_notice: false,
            }
        }

        /// Starts a `FATAL`, which also ends the connection.
        #[must_use]
        pub fn fatal(code: &str, message: impl Into<String>) -> Self {
            let mut this = Self::error(code, message);
            "FATAL".clone_into(&mut this.severity);
            this
        }

        /// Starts a `NOTICE`, which is informational and does not fail the
        /// statement.
        #[must_use]
        pub fn notice(message: impl Into<String>) -> Self {
            let mut this = Self::error("00000", message);
            "NOTICE".clone_into(&mut this.severity);
            this.is_notice = true;
            this
        }

        /// Adds a `DETAIL` line.
        #[must_use]
        pub fn detail(mut self, detail: impl Into<String>) -> Self {
            self.detail = Some(detail.into());
            self
        }

        /// Adds a `HINT` line.
        #[must_use]
        pub fn hint(mut self, hint: impl Into<String>) -> Self {
            self.hint = Some(hint.into());
            self
        }

        /// Encodes the message.
        pub fn build(&self) -> Result<Bytes, ProtocolError> {
            let message_tag = if self.is_notice {
                tag::NOTICE_RESPONSE
            } else {
                tag::EXECUTE_OR_ERROR
            };
            let mut w = Writer::tagged(message_tag);
            w.u8(b'S');
            w.cstr(&self.severity);
            // `V` is the non-localised severity; clients added after
            // PostgreSQL 9.6 prefer it, older ones ignore it.
            w.u8(b'V');
            w.cstr(&self.severity);
            w.u8(b'C');
            w.cstr(&self.code);
            w.u8(b'M');
            w.cstr(&self.message);
            if let Some(detail) = &self.detail {
                w.u8(b'D');
                w.cstr(detail);
            }
            if let Some(hint) = &self.hint {
                w.u8(b'H');
                w.cstr(hint);
            }
            w.u8(0);
            w.finish()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn as_raw(bytes: &Bytes) -> RawMessage {
        RawMessage {
            tag: bytes.first().copied(),
            body: bytes.slice(5..),
        }
    }

    #[test]
    fn ready_for_query_round_trips_every_state() {
        for status in [
            TransactionStatus::Idle,
            TransactionStatus::InTransaction,
            TransactionStatus::Failed,
        ] {
            let wire = build::ready_for_query(status).unwrap();
            assert_eq!(
                Backend::decode(&as_raw(&wire)).unwrap(),
                Backend::ReadyForQuery { status }
            );
        }
    }

    #[test]
    fn only_idle_connections_are_poolable() {
        assert!(TransactionStatus::Idle.is_poolable());
        assert!(!TransactionStatus::InTransaction.is_poolable());
        assert!(!TransactionStatus::Failed.is_poolable());
    }

    #[test]
    fn an_error_keeps_its_sqlstate_through_a_round_trip() {
        let wire = build::ErrorBuilder::error("0A000", "statement spans regions")
            .detail("two regions were involved")
            .hint("add a shard key")
            .build()
            .unwrap();
        let raw = as_raw(&wire);
        let Ok(Backend::Error(notice)) = Backend::decode(&raw) else {
            panic!("expected an ErrorResponse");
        };
        assert_eq!(notice.code(), "0A000");
        assert_eq!(notice.severity(), "ERROR");
        assert_eq!(notice.message(), "statement spans regions");
        assert_eq!(notice.field(b'D'), Some("two regions were involved"));
        assert_eq!(notice.field(b'H'), Some("add a shard key"));
    }

    #[test]
    fn a_notice_decodes_as_a_notice_not_an_error() {
        let wire = build::ErrorBuilder::notice("relocation finished")
            .build()
            .unwrap();
        assert!(matches!(
            Backend::decode(&as_raw(&wire)),
            Ok(Backend::Notice(_))
        ));
    }

    #[test]
    fn decodes_the_sasl_mechanism_list() {
        let wire = build::authentication_sasl("SCRAM-SHA-256").unwrap();
        let raw = as_raw(&wire);
        assert_eq!(
            Backend::decode(&raw).unwrap(),
            Backend::Authentication(Authentication::Sasl {
                mechanisms: vec!["SCRAM-SHA-256"]
            })
        );
    }

    #[test]
    fn unimplemented_auth_methods_are_reported_rather_than_mistaken_for_ok() {
        let mut w = Writer::tagged(tag::AUTHENTICATION);
        w.i32(7); // GSSAPI
        let wire = w.finish().unwrap();
        assert_eq!(
            Backend::decode(&as_raw(&wire)).unwrap(),
            Backend::Authentication(Authentication::Unsupported { kind: 7 })
        );
    }

    #[test]
    fn data_rows_keep_null_distinct_from_empty() {
        let wire = build::data_row(&[Some("eu-west"), None, Some("")]).unwrap();
        let raw = as_raw(&wire);
        let Ok(Backend::DataRow { columns }) = Backend::decode(&raw) else {
            panic!("expected a DataRow");
        };
        assert_eq!(columns, vec![Some(&b"eu-west"[..]), None, Some(&b""[..])]);
    }

    #[test]
    fn row_descriptions_round_trip_their_column_names() {
        let wire = build::row_description(&["key", "region"], 25).unwrap();
        let raw = as_raw(&wire);
        let Ok(Backend::RowDescription { fields }) = Backend::decode(&raw) else {
            panic!("expected a RowDescription");
        };
        assert_eq!(
            fields.iter().map(|f| f.name).collect::<Vec<_>>(),
            ["key", "region"]
        );
        assert!(fields.iter().all(|f| f.type_oid == 25));
    }
}
