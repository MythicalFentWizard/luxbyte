//! Extended-query messages sent as one batch, with the replies kept apart.
//!
//! A proxy relaying the extended protocol has to know, for every message it
//! sends, which reply ends the answer to it. Nothing in a reply says which
//! request it answers; only the order does. Getting the boundary wrong by
//! one message leaves the connection mid-response, and the next borrower
//! reads the previous borrower's rows.

use bytes::Bytes;
use luxbyte_proto::RawMessage;
use luxbyte_proto::tag;

/// Which extended-query message a [`Step`] carries.
///
/// This decides which reply ends the answer to it, and which replies may
/// come before that.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepKind {
    /// A `Parse`, answered by `ParseComplete`.
    Parse,
    /// A `Bind`, answered by `BindComplete`.
    Bind,
    /// A `Describe` of a statement: `ParameterDescription`, then
    /// `RowDescription` or `NoData`.
    DescribeStatement,
    /// A `Describe` of a portal: `RowDescription` or `NoData`.
    DescribePortal,
    /// An `Execute`: rows, then `CommandComplete`, `EmptyQueryResponse` or
    /// `PortalSuspended`.
    Execute,
    /// A `Close`, answered by `CloseComplete`.
    Close,
    /// A `Sync`, answered by `ReadyForQuery`.
    Sync,
}

impl StepKind {
    /// Whether `tag` is the reply that finishes this step.
    pub(crate) const fn ends_with(self, tag: u8) -> bool {
        match self {
            Self::Parse => tag == tag::PARSE_COMPLETE,
            Self::Bind => tag == tag::BIND_COMPLETE,
            Self::DescribeStatement | Self::DescribePortal => {
                tag == tag::TERMINATE_OR_ROW_DESCRIPTION || tag == tag::NO_DATA
            }
            Self::Execute => {
                tag == tag::CLOSE_OR_COMMAND_COMPLETE
                    || tag == tag::EMPTY_QUERY_RESPONSE
                    || tag == tag::PORTAL_SUSPENDED
            }
            Self::Close => tag == tag::CLOSE_COMPLETE,
            Self::Sync => tag == tag::READY_FOR_QUERY,
        }
    }

    /// Whether `tag` may arrive before the reply that finishes this step.
    pub(crate) const fn continues_with(self, tag: u8) -> bool {
        match self {
            Self::DescribeStatement => tag == tag::PARAMETER_DESCRIPTION,
            Self::Execute => tag == tag::DESCRIBE_OR_DATA_ROW,
            _ => false,
        }
    }
}

/// One encoded extended-query message and what it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    /// What the message is.
    pub kind: StepKind,
    /// The message as it goes on the wire.
    pub message: Bytes,
}

impl Step {
    /// Pairs an encoded message with its kind.
    #[must_use]
    pub const fn new(kind: StepKind, message: Bytes) -> Self {
        Self { kind, message }
    }
}

/// The server's answer to one [`Step`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StepReply {
    /// Every message that belonged to the answer, in order, including any
    /// notices and the message that finished it.
    ///
    /// Empty for a step the server skipped because an earlier one failed:
    /// after an error it discards everything up to the next `Sync`.
    pub messages: Vec<RawMessage>,
    /// Whether the answer was an `ErrorResponse`.
    pub failed: bool,
}
