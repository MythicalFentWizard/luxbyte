//! The transport unit: a tag byte and an opaque body.

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::ProtocolError;

/// Message tag bytes.
///
/// Several tags are reused by both peers with different meanings — `'D'` is
/// `Describe` from the client and `DataRow` from the server, `'C'` is `Close`
/// one way and `CommandComplete` the other. Nothing in a tag tells you which;
/// only the direction of travel does. That is why decoding lives in the
/// direction-specific [`crate::frontend`] and [`crate::backend`] modules
/// rather than on [`RawMessage`] itself.
pub mod tag {
    /// Client: `Bind`.
    pub const BIND: u8 = b'B';
    /// Client: `Close`. Server: `CommandComplete`.
    pub const CLOSE_OR_COMMAND_COMPLETE: u8 = b'C';
    /// Client: `Describe`. Server: `DataRow`.
    pub const DESCRIBE_OR_DATA_ROW: u8 = b'D';
    /// Client: `Execute`. Server: `ErrorResponse`.
    pub const EXECUTE_OR_ERROR: u8 = b'E';
    /// Client: `FunctionCall`.
    pub const FUNCTION_CALL: u8 = b'F';
    /// Client: `Flush`.
    pub const FLUSH: u8 = b'H';
    /// Client: `Parse`. Server: `ParseComplete`.
    pub const PARSE_OR_PARSE_COMPLETE: u8 = b'P';
    /// Client: `Query`.
    pub const QUERY: u8 = b'Q';
    /// Client: `Sync`. Server: `ParameterStatus`.
    pub const SYNC_OR_PARAMETER_STATUS: u8 = b'S';
    /// Client: `Terminate`. Server: `RowDescription`.
    pub const TERMINATE_OR_ROW_DESCRIPTION: u8 = b'T';
    /// Client: password, SASL initial response, or SASL response.
    pub const PASSWORD: u8 = b'p';

    /// Server: one of the `Authentication*` family; the sub-kind is the first
    /// `i32` of the body.
    pub const AUTHENTICATION: u8 = b'R';
    /// Server: `BackendKeyData`.
    pub const BACKEND_KEY_DATA: u8 = b'K';
    /// Server: `BindComplete`.
    pub const BIND_COMPLETE: u8 = b'2';
    /// Server: `CloseComplete`.
    pub const CLOSE_COMPLETE: u8 = b'3';
    /// Server: `EmptyQueryResponse`.
    pub const EMPTY_QUERY_RESPONSE: u8 = b'I';
    /// Server: `NoData`.
    pub const NO_DATA: u8 = b'n';
    /// Server: `NoticeResponse`.
    pub const NOTICE_RESPONSE: u8 = b'N';
    /// Server: `NotificationResponse`, from `LISTEN`/`NOTIFY`.
    pub const NOTIFICATION_RESPONSE: u8 = b'A';
    /// Server: `ParameterDescription`.
    pub const PARAMETER_DESCRIPTION: u8 = b't';
    /// Server: `ParseComplete`.
    pub const PARSE_COMPLETE: u8 = b'1';
    /// Server: `PortalSuspended`.
    pub const PORTAL_SUSPENDED: u8 = b's';
    /// Server: `ReadyForQuery`.
    pub const READY_FOR_QUERY: u8 = b'Z';
    /// Server: `NegotiateProtocolVersion`.
    pub const NEGOTIATE_PROTOCOL_VERSION: u8 = b'v';

    /// Either peer: `CopyData`.
    pub const COPY_DATA: u8 = b'd';
    /// Either peer: `CopyDone`.
    pub const COPY_DONE: u8 = b'c';
    /// Client: `CopyFail`.
    pub const COPY_FAIL: u8 = b'f';
    /// Server: `CopyInResponse`.
    pub const COPY_IN_RESPONSE: u8 = b'G';
    /// Server: `CopyOutResponse`.
    pub const COPY_OUT_RESPONSE: u8 = b'H';
    /// Server: `CopyBothResponse`.
    pub const COPY_BOTH_RESPONSE: u8 = b'W';
}

/// One framed protocol message.
///
/// The body excludes both the tag and the length prefix, so `body.len()` is
/// the payload length and nothing else. Cloning is cheap: [`Bytes`] is
/// reference-counted, which is what makes fan-out to several backends during
/// a scatter-gather free of copying.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawMessage {
    /// The tag byte, or `None` for the untagged startup-phase messages.
    pub tag: Option<u8>,
    /// The payload, excluding tag and length.
    pub body: Bytes,
}

impl RawMessage {
    /// Builds a tagged message.
    #[must_use]
    pub const fn tagged(tag: u8, body: Bytes) -> Self {
        Self {
            tag: Some(tag),
            body,
        }
    }

    /// Builds an untagged (startup-phase) message.
    #[must_use]
    pub const fn untagged(body: Bytes) -> Self {
        Self { tag: None, body }
    }

    /// The tag rendered for logs and error messages.
    #[must_use]
    pub fn tag_char(&self) -> char {
        self.tag.map_or('\0', char::from)
    }

    /// Whether this message is the given tag.
    #[must_use]
    pub fn is(&self, tag: u8) -> bool {
        self.tag == Some(tag)
    }

    /// A reader positioned at the start of the body.
    #[must_use]
    pub fn reader(&self) -> crate::buf::Reader<'_> {
        crate::buf::Reader::new(&self.body)
    }

    /// The number of bytes this message occupies on the wire.
    #[must_use]
    pub fn wire_len(&self) -> usize {
        // tag (if any) + four length bytes + body
        let tag_len = usize::from(self.tag.is_some());
        tag_len.saturating_add(4).saturating_add(self.body.len())
    }

    /// Re-encodes the message for transmission.
    pub fn encode(&self, out: &mut BytesMut) -> Result<(), ProtocolError> {
        let declared = self.body.len().saturating_add(4);
        let declared = i32::try_from(declared).map_err(|_| ProtocolError::ValueOutOfRange {
            value: i64::try_from(declared).unwrap_or(i64::MAX),
            width: "i32",
        })?;
        out.reserve(self.wire_len());
        if let Some(tag) = self.tag {
            out.put_u8(tag);
        }
        out.put_i32(declared);
        out.put_slice(&self.body);
        Ok(())
    }

    /// Re-encodes the message into a fresh buffer.
    pub fn to_bytes(&self) -> Result<Bytes, ProtocolError> {
        let mut out = BytesMut::with_capacity(self.wire_len());
        self.encode(&mut out)?;
        Ok(out.freeze())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoding_restores_the_original_framing() {
        let msg = RawMessage::tagged(tag::QUERY, Bytes::from_static(b"SELECT 1\0"));
        let wire = msg.to_bytes().ok();
        assert_eq!(wire.as_ref().map(Bytes::len), Some(msg.wire_len()));
        assert_eq!(wire.as_ref().and_then(|w| w.first().copied()), Some(b'Q'));
        // 4 length bytes + 9 body bytes
        assert_eq!(
            wire.as_ref()
                .and_then(|w| w.get(1..5))
                .and_then(|s| <[u8; 4]>::try_from(s).ok()),
            Some(13_i32.to_be_bytes())
        );
    }

    #[test]
    fn untagged_messages_carry_no_tag_byte() {
        let msg = RawMessage::untagged(Bytes::from_static(&[0, 3, 0, 0]));
        assert_eq!(msg.wire_len(), 8);
        assert_eq!(msg.to_bytes().ok().map(|b| b.len()), Some(8));
    }
}
