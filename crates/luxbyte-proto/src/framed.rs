//! Message framing over an async byte stream.

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{Peer, ProtocolError};
use crate::raw::RawMessage;
use crate::{DEFAULT_MAX_MESSAGE_SIZE, buf::Writer};

/// How much room to make available to a single `read` syscall.
///
/// Large enough that a busy stream is drained in few calls, small enough that
/// ten thousand idle pooled connections do not cost a gigabyte between them.
const READ_CHUNK: usize = 16 * 1024;

/// A bidirectional, buffered protocol stream.
///
/// Writes are staged in an internal buffer and go out on [`Framed::flush`].
/// That is deliberate: the extended query protocol sends `Parse`/`Bind`/
/// `Describe`/`Execute`/`Sync` as five messages that belong in one packet, and
/// flushing each one separately turns a single round trip into five.
#[derive(Debug)]
pub struct Framed<S> {
    stream: S,
    peer: Peer,
    read_buf: BytesMut,
    write_buf: BytesMut,
    max_message_size: usize,
}

impl<S> Framed<S> {
    /// Wraps a stream, recording which peer is at the far end.
    #[must_use]
    pub fn new(stream: S, peer: Peer) -> Self {
        Self {
            stream,
            peer,
            read_buf: BytesMut::with_capacity(READ_CHUNK),
            write_buf: BytesMut::with_capacity(READ_CHUNK),
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
        }
    }

    /// Overrides the largest message body that will be accepted.
    #[must_use]
    pub const fn with_max_message_size(mut self, max: usize) -> Self {
        self.max_message_size = max;
        self
    }

    /// Which peer is at the far end.
    #[must_use]
    pub const fn peer(&self) -> Peer {
        self.peer
    }

    /// Whether anything is staged but not yet written.
    #[must_use]
    pub fn has_pending_writes(&self) -> bool {
        !self.write_buf.is_empty()
    }

    /// Bytes buffered from the stream but not yet parsed.
    ///
    /// Non-zero after a message is read means the peer pipelined, which the
    /// session layer uses to decide whether a flush is worth the syscall.
    #[must_use]
    pub fn buffered_input(&self) -> usize {
        self.read_buf.len()
    }

    /// The stream underneath.
    #[must_use]
    pub const fn get_ref(&self) -> &S {
        &self.stream
    }

    /// Unwraps the stream, discarding any buffered input.
    ///
    /// Only sound once the protocol is at a message boundary with nothing
    /// pipelined behind it — otherwise the discarded bytes are a message the
    /// next reader will never see.
    pub fn into_inner(self) -> S {
        self.stream
    }

    /// Stages an already-encoded message.
    pub fn queue_raw(&mut self, encoded: &[u8]) {
        self.write_buf.put_slice(encoded);
    }

    /// Stages a message, encoding it into the write buffer in place.
    pub fn queue(&mut self, message: &RawMessage) -> Result<(), ProtocolError> {
        message.encode(&mut self.write_buf)
    }

    /// Stages a message built by a [`Writer`].
    pub fn queue_built(&mut self, writer: Writer) -> Result<(), ProtocolError> {
        let bytes = writer.finish()?;
        self.write_buf.put_slice(&bytes);
        Ok(())
    }
}

impl<S: AsyncRead + Unpin> Framed<S> {
    /// Reads until the buffer holds at least `n` bytes.
    async fn fill_at_least(&mut self, n: usize) -> Result<(), ProtocolError> {
        while self.read_buf.len() < n {
            // Keep a whole chunk available so the kernel can hand us a full
            // read rather than dribbling bytes into a nearly full buffer.
            let shortfall = n.saturating_sub(self.read_buf.len());
            self.read_buf.reserve(shortfall.max(READ_CHUNK));
            if self.stream.read_buf(&mut self.read_buf).await? == 0 {
                return Err(ProtocolError::Closed(self.peer));
            }
        }
        Ok(())
    }

    /// Validates a declared length and returns the body size it implies.
    fn body_len(&self, declared: i32) -> Result<usize, ProtocolError> {
        let bad = || ProtocolError::BadLength {
            declared: i64::from(declared),
            max: self.max_message_size,
        };
        // The declared length counts its own four bytes, so anything under
        // four is malformed rather than merely empty.
        let body = declared.checked_sub(4).ok_or_else(bad)?;
        let body = usize::try_from(body).map_err(|_| bad())?;
        if body > self.max_message_size {
            return Err(bad());
        }
        Ok(body)
    }

    /// Reads one tagged message.
    pub async fn read_message(&mut self) -> Result<RawMessage, ProtocolError> {
        // Tag plus length prefix.
        self.fill_at_least(5).await?;
        let header: [u8; 4] = self
            .read_buf
            .get(1..5)
            .and_then(|s| <[u8; 4]>::try_from(s).ok())
            .ok_or_else(|| ProtocolError::truncated(4, self.read_buf.len()))?;
        let tag = self
            .read_buf
            .first()
            .copied()
            .ok_or_else(|| ProtocolError::truncated(1, 0))?;
        let body_len = self.body_len(i32::from_be_bytes(header))?;

        let total = body_len.saturating_add(5);
        self.fill_at_least(total).await?;
        let mut frame = self.read_buf.split_to(total);
        frame.advance(5);
        Ok(RawMessage::tagged(tag, frame.freeze()))
    }

    /// Reads one untagged message, as sent during the startup phase.
    pub async fn read_untagged(&mut self) -> Result<RawMessage, ProtocolError> {
        self.fill_at_least(4).await?;
        let header: [u8; 4] = self
            .read_buf
            .get(..4)
            .and_then(|s| <[u8; 4]>::try_from(s).ok())
            .ok_or_else(|| ProtocolError::truncated(4, self.read_buf.len()))?;
        let body_len = self.body_len(i32::from_be_bytes(header))?;

        let total = body_len.saturating_add(4);
        self.fill_at_least(total).await?;
        let mut frame = self.read_buf.split_to(total);
        frame.advance(4);
        Ok(RawMessage::untagged(frame.freeze()))
    }
}

impl<S: AsyncWrite + Unpin> Framed<S> {
    /// Writes everything staged and flushes the stream.
    pub async fn flush(&mut self) -> Result<(), ProtocolError> {
        if self.write_buf.is_empty() {
            return Ok(());
        }
        self.stream.write_all(&self.write_buf).await?;
        self.write_buf.clear();
        self.stream.flush().await?;
        Ok(())
    }

    /// Stages one message and flushes immediately.
    pub async fn send(&mut self, message: &RawMessage) -> Result<(), ProtocolError> {
        self.queue(message)?;
        self.flush().await
    }

    /// Closes the write half.
    pub async fn shutdown(&mut self) -> Result<(), ProtocolError> {
        self.flush().await?;
        self.stream.shutdown().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::raw::tag;

    fn framed_over(input: &[u8]) -> Framed<std::io::Cursor<Vec<u8>>> {
        Framed::new(std::io::Cursor::new(input.to_vec()), Peer::Frontend)
    }

    #[tokio::test]
    async fn reads_a_message_split_across_two_reads() {
        // A duplex pair lets us deliver the header and body separately, which
        // is the case a naive "read once, then parse" loop gets wrong.
        let (mut client, server) = tokio::io::duplex(64);
        let mut framed = Framed::new(server, Peer::Frontend);

        let writer = tokio::spawn(async move {
            let _ = client.write_all(&[b'Q', 0, 0, 0, 11]).await;
            let _ = client.flush().await;
            tokio::task::yield_now().await;
            let _ = client.write_all(b"SELECT\0").await;
            let _ = client.flush().await;
        });

        let msg = framed.read_message().await.ok();
        assert_eq!(msg.as_ref().map(|m| m.tag), Some(Some(tag::QUERY)));
        assert_eq!(
            msg.as_ref().map(|m| m.body.clone()),
            Some(Bytes::from_static(b"SELECT\0"))
        );
        let _ = writer.await;
    }

    #[tokio::test]
    async fn reads_two_pipelined_messages_from_one_buffer() {
        let mut framed = framed_over(&[b'S', 0, 0, 0, 4, b'S', 0, 0, 0, 4]);
        assert!(framed.read_message().await.is_ok());
        assert!(framed.read_message().await.is_ok());
        // The third read hits end of stream, which is a clean close, not a hang.
        assert!(matches!(
            framed.read_message().await,
            Err(ProtocolError::Closed(Peer::Frontend))
        ));
    }

    #[tokio::test]
    async fn rejects_a_length_below_its_own_width() {
        let mut framed = framed_over(&[b'Q', 0, 0, 0, 2]);
        assert!(matches!(
            framed.read_message().await,
            Err(ProtocolError::BadLength { declared: 2, .. })
        ));
    }

    #[tokio::test]
    async fn rejects_a_negative_length_without_allocating() {
        let mut framed = framed_over(&[b'Q', 0xFF, 0xFF, 0xFF, 0xFF]);
        assert!(matches!(
            framed.read_message().await,
            Err(ProtocolError::BadLength { declared: -1, .. })
        ));
    }

    #[tokio::test]
    async fn refuses_a_body_larger_than_the_cap_before_reading_it() {
        let mut framed = framed_over(&[b'Q', 0x00, 0x10, 0x00, 0x00]).with_max_message_size(1024);
        assert!(matches!(
            framed.read_message().await,
            Err(ProtocolError::BadLength { max: 1024, .. })
        ));
    }

    #[tokio::test]
    async fn a_zero_length_body_is_valid() {
        let mut framed = framed_over(&[tag::SYNC_OR_PARAMETER_STATUS, 0, 0, 0, 4]);
        let msg = framed.read_message().await.ok();
        assert_eq!(msg.map(|m| m.body.len()), Some(0));
    }

    #[tokio::test]
    async fn untagged_reads_consume_only_the_length_prefix() {
        let mut framed = framed_over(&[0, 0, 0, 8, 0, 3, 0, 0]);
        let msg = framed.read_untagged().await.ok();
        assert_eq!(msg.as_ref().map(|m| m.tag), Some(None));
        assert_eq!(msg.map(|m| m.body.len()), Some(4));
    }
}
