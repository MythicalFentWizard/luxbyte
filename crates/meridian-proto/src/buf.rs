//! Bounds-checked big-endian reading and writing.
//!
//! Every accessor returns a `Result`. The workspace denies indexing and
//! slicing, so a malformed message arriving from the network cannot panic the
//! proxy; it can only produce an error that the session layer turns into a
//! clean disconnect.

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::ProtocolError;

/// A cursor over a message body.
///
/// Borrows its input, so decoded strings and byte ranges are views into the
/// original buffer rather than copies.
#[derive(Debug, Clone)]
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    /// Wraps a byte slice.
    #[must_use]
    pub const fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// How many bytes have not been consumed.
    #[must_use]
    pub const fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    /// Whether the body has been fully consumed.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    /// Consumes `n` bytes and returns them.
    pub fn take(&mut self, n: usize) -> Result<&'a [u8], ProtocolError> {
        let available = self.remaining();
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| ProtocolError::truncated(n, available))?;
        let slice = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| ProtocolError::truncated(n, available))?;
        self.pos = end;
        Ok(slice)
    }

    /// Consumes everything that is left.
    pub fn take_rest(&mut self) -> &'a [u8] {
        let rest = self.buf.get(self.pos..).unwrap_or(&[]);
        self.pos = self.buf.len();
        rest
    }

    /// Reads one byte.
    pub fn u8(&mut self) -> Result<u8, ProtocolError> {
        self.take(1)?
            .first()
            .copied()
            .ok_or_else(|| ProtocolError::truncated(1, 0))
    }

    /// Reads a big-endian `i16`.
    pub fn i16(&mut self) -> Result<i16, ProtocolError> {
        let bytes: [u8; 2] = self
            .take(2)?
            .try_into()
            .map_err(|_| ProtocolError::truncated(2, 0))?;
        Ok(i16::from_be_bytes(bytes))
    }

    /// Reads a big-endian `i32`.
    pub fn i32(&mut self) -> Result<i32, ProtocolError> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| ProtocolError::truncated(4, 0))?;
        Ok(i32::from_be_bytes(bytes))
    }

    /// Reads a NUL-terminated string and consumes the NUL.
    pub fn cstr(&mut self) -> Result<&'a str, ProtocolError> {
        let rest = self
            .buf
            .get(self.pos..)
            .ok_or_else(|| ProtocolError::truncated(1, 0))?;
        let nul = rest
            .iter()
            .position(|b| *b == 0)
            .ok_or(ProtocolError::UnterminatedString)?;
        let text = rest
            .get(..nul)
            .ok_or_else(|| ProtocolError::truncated(nul, rest.len()))?;
        self.pos = self.pos.saturating_add(nul).saturating_add(1);
        Ok(std::str::from_utf8(text)?)
    }

    /// Reads a length-prefixed value, as used for bind parameters and result
    /// columns, where a length of `-1` means SQL `NULL`.
    ///
    /// The distinction matters: `NULL` and the empty string are different
    /// values, and collapsing them is a classic proxy bug.
    pub fn nullable_bytes(&mut self) -> Result<Option<&'a [u8]>, ProtocolError> {
        let len = self.i32()?;
        if len < 0 {
            return Ok(None);
        }
        let len = usize::try_from(len).map_err(|_| ProtocolError::ValueOutOfRange {
            value: i64::from(len),
            width: "usize",
        })?;
        Ok(Some(self.take(len)?))
    }
}

/// Builds one protocol message, patching its length prefix on completion.
#[derive(Debug)]
pub struct Writer {
    buf: BytesMut,
    /// Offset of the four length bytes, so [`Writer::finish`] can patch them.
    length_at: usize,
}

impl Writer {
    /// Starts a tagged message, such as `'Q'` for a simple query.
    #[must_use]
    pub fn tagged(tag: u8) -> Self {
        let mut buf = BytesMut::with_capacity(32);
        buf.put_u8(tag);
        let length_at = buf.len();
        buf.put_i32(0);
        Self { buf, length_at }
    }

    /// Starts an untagged message, as used during startup, where the length is
    /// the first field on the wire.
    #[must_use]
    pub fn untagged() -> Self {
        let mut buf = BytesMut::with_capacity(32);
        let length_at = buf.len();
        buf.put_i32(0);
        Self { buf, length_at }
    }

    /// Appends a byte.
    pub fn u8(&mut self, value: u8) -> &mut Self {
        self.buf.put_u8(value);
        self
    }

    /// Appends a big-endian `i16`.
    pub fn i16(&mut self, value: i16) -> &mut Self {
        self.buf.put_i16(value);
        self
    }

    /// Appends a big-endian `i32`.
    pub fn i32(&mut self, value: i32) -> &mut Self {
        self.buf.put_i32(value);
        self
    }

    /// Appends a count that the wire stores as an `i16`.
    pub fn count16(&mut self, value: usize) -> Result<&mut Self, ProtocolError> {
        let narrowed = i16::try_from(value).map_err(|_| ProtocolError::ValueOutOfRange {
            value: i64::try_from(value).unwrap_or(i64::MAX),
            width: "i16",
        })?;
        self.buf.put_i16(narrowed);
        Ok(self)
    }

    /// Appends raw bytes.
    pub fn bytes(&mut self, value: &[u8]) -> &mut Self {
        self.buf.put_slice(value);
        self
    }

    /// Appends a string and its NUL terminator.
    pub fn cstr(&mut self, value: &str) -> &mut Self {
        self.buf.put_slice(value.as_bytes());
        self.buf.put_u8(0);
        self
    }

    /// Appends a length-prefixed value, encoding `None` as SQL `NULL`.
    pub fn nullable_bytes(&mut self, value: Option<&[u8]>) -> Result<&mut Self, ProtocolError> {
        match value {
            None => {
                self.buf.put_i32(-1);
            }
            Some(bytes) => {
                let len =
                    i32::try_from(bytes.len()).map_err(|_| ProtocolError::ValueOutOfRange {
                        value: i64::try_from(bytes.len()).unwrap_or(i64::MAX),
                        width: "i32",
                    })?;
                self.buf.put_i32(len);
                self.buf.put_slice(bytes);
            }
        }
        Ok(self)
    }

    /// Patches the length prefix and yields the finished message.
    pub fn finish(self) -> Result<Bytes, ProtocolError> {
        let mut buf = self.buf;
        // The declared length covers the four length bytes and the body, but
        // never the tag, so it is measured from `length_at` onwards.
        let declared = buf.len().saturating_sub(self.length_at);
        let declared = i32::try_from(declared).map_err(|_| ProtocolError::ValueOutOfRange {
            value: i64::try_from(declared).unwrap_or(i64::MAX),
            width: "i32",
        })?;
        let end = self.length_at.saturating_add(4);
        let slot = buf
            .get_mut(self.length_at..end)
            .ok_or_else(|| ProtocolError::truncated(4, 0))?;
        slot.copy_from_slice(&declared.to_be_bytes());
        Ok(buf.freeze())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reader_rejects_a_body_that_ends_mid_field() {
        let mut r = Reader::new(&[0x00, 0x01]);
        assert!(matches!(r.i32(), Err(ProtocolError::Truncated { .. })));
    }

    #[test]
    fn reader_rejects_a_string_with_no_terminator() {
        let mut r = Reader::new(b"users");
        assert!(matches!(r.cstr(), Err(ProtocolError::UnterminatedString)));
    }

    #[test]
    fn reader_leaves_the_cursor_just_past_the_nul() {
        let mut r = Reader::new(b"users\0tenant\0");
        assert_eq!(r.cstr().ok(), Some("users"));
        assert_eq!(r.cstr().ok(), Some("tenant"));
        assert!(r.is_empty());
    }

    #[test]
    fn nullable_bytes_distinguishes_null_from_empty() {
        let mut r = Reader::new(&[0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00]);
        assert_eq!(r.nullable_bytes().ok(), Some(None));
        assert_eq!(r.nullable_bytes().ok(), Some(Some(&[][..])));
    }

    #[test]
    fn writer_patches_a_length_that_excludes_the_tag() {
        let mut w = Writer::tagged(b'Q');
        w.cstr("SELECT 1");
        let msg = w.finish().ok();
        // tag + 4 length bytes + 8 characters + NUL
        assert_eq!(msg.as_ref().map(Bytes::len), Some(14));
        let declared = msg
            .as_ref()
            .and_then(|m| m.get(1..5))
            .and_then(|s| <[u8; 4]>::try_from(s).ok());
        assert_eq!(declared, Some(13_i32.to_be_bytes()));
    }

    #[test]
    fn untagged_writer_counts_only_itself_and_the_body() {
        let mut w = Writer::untagged();
        w.i32(crate::PROTOCOL_VERSION_3);
        assert_eq!(w.finish().ok().map(|m| m.len()), Some(8));
    }

    #[test]
    fn count16_refuses_a_value_the_wire_cannot_carry() {
        let mut w = Writer::tagged(b'D');
        assert!(matches!(
            w.count16(70_000),
            Err(ProtocolError::ValueOutOfRange { width: "i16", .. })
        ));
    }
}
