//! A connection that may or may not be encrypted.
//!
//! PostgreSQL negotiates TLS inside its own protocol: an `SSLRequest`, one
//! byte of answer, then the handshake on the same socket. So a connection
//! starts as plain TCP and may become TLS part-way, in either direction —
//! the proxy is a TLS server to clients and a TLS client to backends — and
//! everything above it reads and writes the same way regardless.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// A TCP connection, encrypted or not.
#[derive(Debug)]
pub enum Stream {
    /// Plain TCP.
    Plain(TcpStream),
    /// TLS, with this side as the client.
    Client(Box<tokio_rustls::client::TlsStream<TcpStream>>),
    /// TLS, with this side as the server.
    Server(Box<tokio_rustls::server::TlsStream<TcpStream>>),
}

impl Stream {
    /// Whether the connection is encrypted.
    #[must_use]
    pub const fn is_encrypted(&self) -> bool {
        !matches!(self, Self::Plain(_))
    }

    /// The plain TCP connection, if it has not been upgraded.
    pub fn into_plain(self) -> Option<TcpStream> {
        match self {
            Self::Plain(stream) => Some(stream),
            _ => None,
        }
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Client(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
            Self::Server(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Client(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
            Self::Server(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Self::Client(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
            Self::Server(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Client(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
            Self::Server(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}
