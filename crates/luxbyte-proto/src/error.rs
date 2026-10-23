//! Protocol-level failures.

/// Which side of the connection a message came from.
///
/// Carried in errors so that a tag we could not interpret is reported against
/// the peer that sent it, rather than leaving the reader to guess.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Peer {
    /// The client, speaking the frontend half of the protocol.
    Frontend,
    /// The database server, speaking the backend half.
    Backend,
}

impl std::fmt::Display for Peer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Frontend => "frontend",
            Self::Backend => "backend",
        })
    }
}

/// Anything that can go wrong while reading, writing or interpreting a message.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProtocolError {
    /// The underlying stream failed.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// A message body ended before the field being read did.
    #[error("truncated message: wanted {wanted} more bytes, {available} available")]
    Truncated {
        /// How many bytes the field needed.
        wanted: usize,
        /// How many were left in the body.
        available: usize,
    },

    /// The declared length was negative, too small to include itself, or
    /// larger than the configured cap.
    #[error("declared message length {declared} is not in 4..={max}")]
    BadLength {
        /// The length the peer declared, including its own four bytes.
        declared: i64,
        /// The largest length that would have been accepted.
        max: usize,
    },

    /// A message carried a tag byte with no meaning for that peer.
    #[error("unexpected message tag {tag:?} from the {peer}")]
    UnexpectedTag {
        /// The tag byte as received.
        tag: char,
        /// The peer that sent it.
        peer: Peer,
    },

    /// The client asked for a protocol version this proxy does not speak.
    #[error("unsupported protocol version {major}.{minor}")]
    UnsupportedVersion {
        /// Major version from the startup packet.
        major: u16,
        /// Minor version from the startup packet.
        minor: u16,
    },

    /// A string field was not valid UTF-8.
    #[error("invalid utf-8 in a string field: {0}")]
    InvalidUtf8(#[from] std::str::Utf8Error),

    /// A C string ran to the end of the body without a NUL.
    #[error("unterminated string: no NUL byte before the end of the message")]
    UnterminatedString,

    /// A length or count did not fit the width the wire gives it.
    #[error("value {value} does not fit in a wire {width}")]
    ValueOutOfRange {
        /// The offending value.
        value: i64,
        /// The wire field it was destined for, such as `"i16"`.
        width: &'static str,
    },

    /// Authentication did not complete.
    #[error("authentication failed: {0}")]
    Auth(String),

    /// The peer demanded an authentication method this crate cannot perform.
    #[error("unsupported authentication method: {0}")]
    UnsupportedAuth(String),

    /// The peer hung up where a message was expected.
    #[error("the {0} closed the connection")]
    Closed(Peer),

    /// The server reported an error, already decoded into its fields.
    #[error("{severity} {code}: {message}")]
    Server {
        /// `ERROR`, `FATAL` or `PANIC`.
        severity: String,
        /// The five-character SQLSTATE.
        code: String,
        /// The primary human-readable message.
        message: String,
    },
}

impl ProtocolError {
    /// Builds a [`ProtocolError::Truncated`] from a wanted size and what is left.
    #[must_use]
    pub const fn truncated(wanted: usize, available: usize) -> Self {
        Self::Truncated { wanted, available }
    }

    /// Whether the error means the connection is unusable from here on.
    ///
    /// Framing and I/O failures leave the stream desynchronised, so the only
    /// safe response is to close it. A decoding failure inside a body we were
    /// merely inspecting does not, by itself, cost us the stream.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::Io(_) | Self::BadLength { .. } | Self::Closed(_) | Self::UnexpectedTag { .. }
        )
    }
}
