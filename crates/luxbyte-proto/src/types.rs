//! Binary parameter values, rendered the way PostgreSQL prints them.
//!
//! A shard key bound in binary format is still a shard key. Drivers such as
//! asyncpg send every parameter in binary, so a proxy that only reads text
//! parameters routes none of their statements. Rendering a value as its text
//! form puts it on the same path as a literal, and the directory is keyed by
//! text.
//!
//! Only types whose text form is unambiguous are rendered. Anything else
//! comes back as `None`, which the planner treats as an unknown key — a
//! fan-out or a refusal, never a guess.

use std::fmt::Write as _;

/// `int2`.
pub const INT2: i32 = 21;
/// `int4`.
pub const INT4: i32 = 23;
/// `int8`.
pub const INT8: i32 = 20;
/// `text`.
pub const TEXT: i32 = 25;
/// `varchar`.
pub const VARCHAR: i32 = 1_043;
/// `name`.
pub const NAME: i32 = 19;
/// `uuid`.
pub const UUID: i32 = 2_950;

/// Renders a binary-format value of the given type as PostgreSQL's text
/// output would print it.
///
/// Returns `None` for a type this function does not know, and for bytes of
/// the wrong length for their type — a malformed value is not a key.
#[must_use]
pub fn text_from_binary(type_oid: i32, bytes: &[u8]) -> Option<String> {
    match type_oid {
        INT2 => <[u8; 2]>::try_from(bytes)
            .ok()
            .map(|b| i16::from_be_bytes(b).to_string()),
        INT4 => <[u8; 4]>::try_from(bytes)
            .ok()
            .map(|b| i32::from_be_bytes(b).to_string()),
        INT8 => <[u8; 8]>::try_from(bytes)
            .ok()
            .map(|b| i64::from_be_bytes(b).to_string()),
        // The binary form of these is the text itself.
        TEXT | VARCHAR | NAME => std::str::from_utf8(bytes).ok().map(ToOwned::to_owned),
        UUID => {
            let bytes = <[u8; 16]>::try_from(bytes).ok()?;
            let mut out = String::with_capacity(36);
            for (index, byte) in bytes.iter().enumerate() {
                if matches!(index, 4 | 6 | 8 | 10) {
                    out.push('-');
                }
                write!(out, "{byte:02x}").ok()?;
            }
            Some(out)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_print_in_decimal_whatever_their_width() {
        assert_eq!(
            text_from_binary(INT2, &(-7_i16).to_be_bytes()).as_deref(),
            Some("-7")
        );
        assert_eq!(
            text_from_binary(INT4, &42_i32.to_be_bytes()).as_deref(),
            Some("42")
        );
        assert_eq!(
            text_from_binary(INT8, &9_007_199_254_740_993_i64.to_be_bytes()).as_deref(),
            Some("9007199254740993")
        );
    }

    #[test]
    fn a_value_of_the_wrong_width_is_not_a_key() {
        // Four bytes claiming to be an int8 would otherwise be read as
        // whatever happened to follow them.
        assert_eq!(text_from_binary(INT8, &42_i32.to_be_bytes()), None);
    }

    #[test]
    fn uuids_print_lowercase_and_hyphenated() {
        let bytes = [
            0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17,
            0x40, 0x00,
        ];
        assert_eq!(
            text_from_binary(UUID, &bytes).as_deref(),
            Some("123e4567-e89b-12d3-a456-426614174000")
        );
    }

    #[test]
    fn text_types_are_their_own_binary_form() {
        assert_eq!(text_from_binary(TEXT, b"alice").as_deref(), Some("alice"));
        assert_eq!(text_from_binary(VARCHAR, b"bob").as_deref(), Some("bob"));
        assert_eq!(text_from_binary(TEXT, &[0xff, 0xfe]), None);
    }

    #[test]
    fn unknown_types_are_not_guessed_at() {
        // `numeric` has a binary form that is not its text, and `0` is the
        // "let the server infer it" OID — neither may be read as text.
        assert_eq!(text_from_binary(1_700, b"12"), None);
        assert_eq!(text_from_binary(0, b"12"), None);
    }
}
