//! Typed views of client-to-server messages.
//!
//! Only the messages that affect routing are decoded. Everything else stays a
//! [`RawMessage`] and is relayed byte for byte.

use crate::buf::{Reader, Writer};
use crate::error::{Peer, ProtocolError};
use crate::raw::{RawMessage, tag};
use bytes::Bytes;

/// What a `Describe` or `Close` is aimed at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    /// A prepared statement.
    Statement,
    /// A bound portal.
    Portal,
}

impl Target {
    /// Decodes the single byte the wire uses.
    pub fn from_byte(byte: u8) -> Result<Self, ProtocolError> {
        match byte {
            b'S' => Ok(Self::Statement),
            b'P' => Ok(Self::Portal),
            other => Err(ProtocolError::UnexpectedTag {
                tag: char::from(other),
                peer: Peer::Frontend,
            }),
        }
    }

    /// The byte the wire uses.
    #[must_use]
    pub const fn as_byte(self) -> u8 {
        match self {
            Self::Statement => b'S',
            Self::Portal => b'P',
        }
    }
}

/// How one bind parameter or result column is encoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// The value is the datatype's text representation.
    Text,
    /// The value is the datatype's binary representation.
    Binary,
}

impl Format {
    /// Decodes the `i16` the wire uses, rejecting anything undefined.
    pub fn from_code(code: i16) -> Result<Self, ProtocolError> {
        match code {
            0 => Ok(Self::Text),
            1 => Ok(Self::Binary),
            other => Err(ProtocolError::ValueOutOfRange {
                value: i64::from(other),
                width: "format code",
            }),
        }
    }
}

/// A `Bind`, decoded far enough to recover a shard key from a parameter.
///
/// This is the message that lets Luxbyte route a parameterised statement.
/// A proxy that only reads `Parse` sees `WHERE tenant_id = $1` and knows the
/// column but not the value; the value arrives here, and without decoding it
/// the only honest options are to broadcast or to refuse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bind<'a> {
    /// Destination portal; empty names the unnamed portal.
    pub portal: &'a str,
    /// Source prepared statement; empty names the unnamed statement.
    pub statement: &'a str,
    /// Format of each parameter.
    ///
    /// Empty means every parameter is text; one entry means that format
    /// applies to all of them; otherwise there is one entry per parameter.
    pub parameter_formats: Vec<Format>,
    /// Parameter values, with `None` for SQL `NULL`.
    pub parameters: Vec<Option<&'a [u8]>>,
    /// Requested format of each result column, under the same fan-out rule.
    pub result_formats: Vec<Format>,
}

impl Bind<'_> {
    /// The format of the parameter at `index`, applying the fan-out rule.
    #[must_use]
    pub fn parameter_format(&self, index: usize) -> Format {
        match self.parameter_formats.len() {
            0 => Format::Text,
            1 => self
                .parameter_formats
                .first()
                .copied()
                .unwrap_or(Format::Text),
            _ => self
                .parameter_formats
                .get(index)
                .copied()
                .unwrap_or(Format::Text),
        }
    }

    /// The parameter at `index` interpreted as UTF-8 text.
    ///
    /// Returns `None` for a `NULL`, for a binary-format parameter, or for
    /// bytes that are not valid UTF-8 — all three mean "not a shard key I can
    /// read", and the caller should fall back rather than guess.
    #[must_use]
    pub fn parameter_as_text(&self, index: usize) -> Option<&str> {
        if self.parameter_format(index) != Format::Text {
            return None;
        }
        let value = self.parameters.get(index).copied().flatten()?;
        std::str::from_utf8(value).ok()
    }
}

/// A `Parse`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parse<'a> {
    /// Name to give the prepared statement; empty names the unnamed one.
    pub name: &'a str,
    /// The SQL text.
    pub sql: &'a str,
    /// Parameter type OIDs the client chose to specify; `0` means "infer".
    pub parameter_types: Vec<i32>,
}

/// A client-to-server message, decoded if routing depends on it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Frontend<'a> {
    /// A simple query, which may contain several statements.
    Query {
        /// The SQL text.
        sql: &'a str,
    },
    /// A `Parse`.
    Parse(Parse<'a>),
    /// A `Bind`.
    Bind(Bind<'a>),
    /// An `Execute`.
    Execute {
        /// Portal to run; empty names the unnamed portal.
        portal: &'a str,
        /// Row limit, where `0` means no limit.
        max_rows: i32,
    },
    /// A `Describe`.
    Describe {
        /// Whether a statement or a portal is being described.
        target: Target,
        /// Its name.
        name: &'a str,
    },
    /// A `Close`.
    Close {
        /// Whether a statement or a portal is being closed.
        target: Target,
        /// Its name.
        name: &'a str,
    },
    /// A `Sync`, which ends the implicit transaction of an extended-query batch.
    Sync,
    /// A `Flush`.
    Flush,
    /// A `Terminate`.
    Terminate,
    /// A `CopyData` payload.
    CopyData(&'a [u8]),
    /// A `CopyDone`.
    CopyDone,
    /// A `CopyFail`.
    CopyFail {
        /// The reason the client gave.
        message: &'a str,
    },
    /// A password, SASL initial response, or SASL response.
    Password(&'a [u8]),
    /// Anything else, left opaque.
    Other {
        /// The tag byte.
        tag: u8,
    },
}

impl<'a> Frontend<'a> {
    /// Decodes a message that arrived from a client.
    pub fn decode(message: &'a RawMessage) -> Result<Self, ProtocolError> {
        let Some(tag_byte) = message.tag else {
            return Err(ProtocolError::UnexpectedTag {
                tag: '\0',
                peer: Peer::Frontend,
            });
        };
        let mut r = Reader::new(&message.body);
        match tag_byte {
            tag::QUERY => Ok(Self::Query { sql: r.cstr()? }),
            tag::PARSE_OR_PARSE_COMPLETE => {
                let name = r.cstr()?;
                let sql = r.cstr()?;
                let count = count(r.i16()?)?;
                let mut parameter_types = Vec::with_capacity(count.min(64));
                for _ in 0..count {
                    parameter_types.push(r.i32()?);
                }
                Ok(Self::Parse(Parse {
                    name,
                    sql,
                    parameter_types,
                }))
            }
            tag::BIND => {
                let portal = r.cstr()?;
                let statement = r.cstr()?;
                let format_count = count(r.i16()?)?;
                let mut parameter_formats = Vec::with_capacity(format_count.min(64));
                for _ in 0..format_count {
                    parameter_formats.push(Format::from_code(r.i16()?)?);
                }
                let parameter_count = count(r.i16()?)?;
                let mut parameters = Vec::with_capacity(parameter_count.min(64));
                for _ in 0..parameter_count {
                    parameters.push(r.nullable_bytes()?);
                }
                let result_count = count(r.i16()?)?;
                let mut result_formats = Vec::with_capacity(result_count.min(64));
                for _ in 0..result_count {
                    result_formats.push(Format::from_code(r.i16()?)?);
                }
                Ok(Self::Bind(Bind {
                    portal,
                    statement,
                    parameter_formats,
                    parameters,
                    result_formats,
                }))
            }
            tag::EXECUTE_OR_ERROR => Ok(Self::Execute {
                portal: r.cstr()?,
                max_rows: r.i32()?,
            }),
            tag::DESCRIBE_OR_DATA_ROW => Ok(Self::Describe {
                target: Target::from_byte(r.u8()?)?,
                name: r.cstr()?,
            }),
            tag::CLOSE_OR_COMMAND_COMPLETE => Ok(Self::Close {
                target: Target::from_byte(r.u8()?)?,
                name: r.cstr()?,
            }),
            tag::SYNC_OR_PARAMETER_STATUS => Ok(Self::Sync),
            tag::FLUSH => Ok(Self::Flush),
            tag::TERMINATE_OR_ROW_DESCRIPTION => Ok(Self::Terminate),
            tag::COPY_DATA => Ok(Self::CopyData(r.take_rest())),
            tag::COPY_DONE => Ok(Self::CopyDone),
            tag::COPY_FAIL => Ok(Self::CopyFail { message: r.cstr()? }),
            tag::PASSWORD => Ok(Self::Password(r.take_rest())),
            other => Ok(Self::Other { tag: other }),
        }
    }

    /// The SQL text this message carries, if any.
    ///
    /// `Query` and `Parse` are the only two messages that introduce new SQL;
    /// `Bind` and `Execute` refer back to a statement parsed earlier.
    #[must_use]
    pub const fn sql(&self) -> Option<&'a str> {
        match self {
            Self::Query { sql } => Some(sql),
            Self::Parse(parse) => Some(parse.sql),
            _ => None,
        }
    }

    /// Whether this message ends a batch and expects a `ReadyForQuery`.
    #[must_use]
    pub const fn ends_batch(&self) -> bool {
        matches!(self, Self::Query { .. } | Self::Sync)
    }
}

/// Converts a wire count to a length, rejecting the negative values that a
/// malformed or hostile peer can put in an `i16`.
fn count(value: i16) -> Result<usize, ProtocolError> {
    usize::try_from(value).map_err(|_| ProtocolError::ValueOutOfRange {
        value: i64::from(value),
        width: "count",
    })
}

/// Builds a `Query`.
pub fn query(sql: &str) -> Result<Bytes, ProtocolError> {
    let mut w = Writer::tagged(tag::QUERY);
    w.cstr(sql);
    w.finish()
}

/// Builds a `Parse`.
pub fn parse(name: &str, sql: &str, parameter_types: &[i32]) -> Result<Bytes, ProtocolError> {
    let mut w = Writer::tagged(tag::PARSE_OR_PARSE_COMPLETE);
    w.cstr(name);
    w.cstr(sql);
    w.count16(parameter_types.len())?;
    for oid in parameter_types {
        w.i32(*oid);
    }
    w.finish()
}

/// Builds a `Sync`.
pub fn sync() -> Result<Bytes, ProtocolError> {
    Writer::tagged(tag::SYNC_OR_PARAMETER_STATUS).finish()
}

/// Builds a `Terminate`.
pub fn terminate() -> Result<Bytes, ProtocolError> {
    Writer::tagged(tag::TERMINATE_OR_ROW_DESCRIPTION).finish()
}

/// Builds a password-family message, used for all of cleartext, MD5 and SASL.
pub fn password(payload: &[u8]) -> Result<Bytes, ProtocolError> {
    let mut w = Writer::tagged(tag::PASSWORD);
    w.bytes(payload);
    w.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decoded(bytes: &Bytes) -> RawMessage {
        let tag = bytes.first().copied();
        let body = bytes.slice(5..);
        RawMessage { tag, body }
    }

    #[test]
    fn decodes_a_simple_query() {
        let wire = query("SELECT 1").unwrap();
        let raw = decoded(&wire);
        assert_eq!(
            Frontend::decode(&raw).unwrap(),
            Frontend::Query { sql: "SELECT 1" }
        );
    }

    #[test]
    fn decodes_a_parse_with_declared_parameter_types() {
        let wire = parse("s1", "SELECT * FROM t WHERE id = $1", &[23]).unwrap();
        let raw = decoded(&wire);
        let Ok(Frontend::Parse(p)) = Frontend::decode(&raw) else {
            panic!("expected Parse");
        };
        assert_eq!(p.name, "s1");
        assert_eq!(p.sql, "SELECT * FROM t WHERE id = $1");
        assert_eq!(p.parameter_types, vec![23]);
    }

    /// Hand-rolled because this crate never needs to *send* a `Bind`; the
    /// proxy only ever reads one and relays the original bytes.
    fn bind_wire(params: &[Option<&[u8]>], formats: &[i16]) -> Bytes {
        let mut w = Writer::tagged(tag::BIND);
        w.cstr("");
        w.cstr("s1");
        w.count16(formats.len()).unwrap();
        for f in formats {
            w.i16(*f);
        }
        w.count16(params.len()).unwrap();
        for p in params {
            w.nullable_bytes(*p).unwrap();
        }
        w.count16(0).unwrap();
        w.finish().unwrap()
    }

    #[test]
    fn decodes_bind_parameters_and_reads_one_as_text() {
        let wire = bind_wire(&[Some(b"tenant-42"), None], &[]);
        let raw = decoded(&wire);
        let Ok(Frontend::Bind(b)) = Frontend::decode(&raw) else {
            panic!("expected Bind");
        };
        assert_eq!(b.statement, "s1");
        assert_eq!(b.parameters.len(), 2);
        assert_eq!(b.parameter_as_text(0), Some("tenant-42"));
        // A NULL parameter is not a shard key.
        assert_eq!(b.parameter_as_text(1), None);
        // Nor is a parameter that was never sent.
        assert_eq!(b.parameter_as_text(9), None);
    }

    #[test]
    fn a_single_format_code_applies_to_every_parameter() {
        let wire = bind_wire(&[Some(b"\x00\x00\x00\x2a"), Some(b"x")], &[1]);
        let raw = decoded(&wire);
        let Ok(Frontend::Bind(b)) = Frontend::decode(&raw) else {
            panic!("expected Bind");
        };
        assert_eq!(b.parameter_format(0), Format::Binary);
        assert_eq!(b.parameter_format(1), Format::Binary);
        // Binary parameters are not read as text, even when they would decode.
        assert_eq!(b.parameter_as_text(1), None);
    }

    #[test]
    fn rejects_an_undefined_format_code() {
        let wire = bind_wire(&[Some(b"x")], &[7]);
        let raw = decoded(&wire);
        assert!(matches!(
            Frontend::decode(&raw),
            Err(ProtocolError::ValueOutOfRange { .. })
        ));
    }

    #[test]
    fn rejects_a_negative_parameter_count() {
        let mut w = Writer::tagged(tag::BIND);
        w.cstr("");
        w.cstr("s1");
        w.i16(-1);
        let wire = w.finish().unwrap();
        let raw = decoded(&wire);
        assert!(matches!(
            Frontend::decode(&raw),
            Err(ProtocolError::ValueOutOfRange { width: "count", .. })
        ));
    }

    #[test]
    fn unknown_tags_stay_opaque_instead_of_failing() {
        let raw = RawMessage::tagged(b'@', Bytes::new());
        assert_eq!(
            Frontend::decode(&raw).unwrap(),
            Frontend::Other { tag: b'@' }
        );
    }

    #[test]
    fn only_query_and_parse_carry_sql() {
        let sync_raw = decoded(&sync().unwrap());
        assert_eq!(Frontend::decode(&sync_raw).unwrap().sql(), None);
        let query_raw = decoded(&query("SELECT 1").unwrap());
        assert_eq!(
            Frontend::decode(&query_raw).unwrap().sql(),
            Some("SELECT 1")
        );
    }
}
