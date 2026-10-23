//! Reducing a statement to its shape, so that analysis can be reused.
//!
//! Parsing is roughly 95% of what it costs to route a statement, and an
//! application issues the same handful of statements over and over. Caching
//! the analysis is therefore the whole performance story — but only if the
//! cache key groups statements that genuinely analyse the same way.
//!
//! Keying on the SQL text alone misses on every query with an inlined
//! literal, which is most of them outside a well-written ORM. So literals are
//! replaced by placeholders and the *shape* becomes the key:
//!
//! ```text
//! SELECT * FROM users WHERE tenant_id = 'alice'   ->   ... WHERE tenant_id = $1
//! SELECT * FROM users WHERE tenant_id = 'bob'     ->   ... WHERE tenant_id = $1
//! ```
//!
//! Both produce one shape, analysed once. The literals come back out as
//! values, and from there they travel the same path a bound parameter does —
//! so there is one routing path, not two.
//!
//! The normalised text is *never sent to a backend*. It exists only to be
//! analysed; the original statement is what goes on the wire.
//!
//! # Why this is safe
//!
//! Replacing literals with placeholders can change what a statement means.
//! `ORDER BY 1` sorts by the first column; `ORDER BY $1` does not. A scanner
//! that gets this wrong returns the wrong rows in the wrong order, and the
//! failure is silent.
//!
//! Rather than try to enumerate every such case, the cache *checks*: on a
//! miss it analyses both the original and the normalised text and compares
//! them, and only caches the shape when they agree. A shape that does not
//! survive the check is remembered as uncacheable and always analysed in
//! full. See [`crate::cache`].
//!
//! That means a bug in this scanner costs performance, never correctness.

use crate::analyze::scan_highest_placeholder;

/// A byte range in the original statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    /// First byte.
    pub start: usize,
    /// One past the last byte.
    pub end: usize,
}

/// What a literal was, once it has been lifted out of the statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Literal {
    /// A value that can be read as text, which is what a shard key lookup
    /// needs.
    Text(String),
    /// A literal that was recognised but cannot be compared as text, such as
    /// a string containing an escaped quote. It still has to occupy its
    /// placeholder position, but it will never match a directory entry.
    Opaque,
}

impl Literal {
    /// The text form, if there is one.
    #[must_use]
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(text) => Some(text),
            Self::Opaque => None,
        }
    }
}

/// A statement with its literals lifted out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shape {
    /// The statement with literals replaced by placeholders. This is the
    /// cache key, and the text that gets analysed.
    pub normalized: String,
    /// The literals, in the order their placeholders were numbered.
    pub literals: Vec<Literal>,
    /// The number given to the first lifted literal.
    ///
    /// Placeholders the statement already contained keep their own numbers,
    /// so lifted literals are numbered after the highest of them. A
    /// statement mixing `$1` with an inlined literal therefore still has
    /// distinct, correctly ordered parameters.
    pub first_placeholder: u16,
}

impl Shape {
    /// Whether any literal was lifted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.literals.is_empty()
    }

    /// The lifted literals as positional values starting at
    /// [`Shape::first_placeholder`].
    #[must_use]
    pub fn literal_values(&self) -> Vec<Option<String>> {
        self.literals
            .iter()
            .map(|literal| literal.as_text().map(ToOwned::to_owned))
            .collect()
    }
}

/// Characters after which a `-` or `+` is a sign rather than an operator.
///
/// `id = -1` is a negative one; `total - 1` is a subtraction. Getting this
/// backwards is how `where id = -1` ends up routed as if it were `1`, which
/// is a real bug that a real proxy shipped.
const SIGN_FOLLOWS: &[u8] = b"=<>!(,+-*/%";

/// Reduces a statement to its shape.
///
/// Skips string bodies, quoted identifiers, dollar-quoted blocks and
/// comments, so a `9` inside any of them is left exactly where it is.
#[must_use]
pub fn normalize(sql: &str) -> Shape {
    let bytes = sql.as_bytes();
    let first_placeholder = scan_highest_placeholder(sql).saturating_add(1);

    let mut normalized = String::with_capacity(sql.len());
    let mut literals = Vec::new();
    let mut next_placeholder = first_placeholder;
    let mut index = 0_usize;

    let at = |position: usize| bytes.get(position).copied();

    while index < bytes.len() {
        let Some(byte) = at(index) else { break };
        match byte {
            b'\'' => {
                let (literal, end) = read_string(sql, index);
                push_placeholder(
                    &mut normalized,
                    &mut literals,
                    &mut next_placeholder,
                    literal,
                );
                index = end;
            }
            b'"' => {
                // A quoted identifier is a name, not a value.
                let end = skip_quoted(bytes, index, b'"');
                push_range(&mut normalized, sql, index, end);
                index = end;
            }
            b'-' if at(index.saturating_add(1)) == Some(b'-') => {
                let end = skip_line_comment(bytes, index);
                push_range(&mut normalized, sql, index, end);
                index = end;
            }
            b'/' if at(index.saturating_add(1)) == Some(b'*') => {
                let end = skip_block_comment(bytes, index);
                push_range(&mut normalized, sql, index, end);
                index = end;
            }
            b'$' => {
                let end = skip_dollar(sql, bytes, index);
                push_range(&mut normalized, sql, index, end);
                index = end;
            }
            b'0'..=b'9' => {
                // Only a number that starts a token is a literal; the `1` in
                // `col1` is part of a name. This has to look at the byte
                // immediately before, not the last significant one, or the
                // space in `ORDER BY 1` is skipped and it reads as `BY1`.
                let preceding = index.checked_sub(1).and_then(at);
                if preceding.is_some_and(is_name_byte) {
                    push_range(&mut normalized, sql, index, index.saturating_add(1));
                    index = index.saturating_add(1);
                    continue;
                }
                let end = read_number_end(bytes, index);
                let start = sign_start(bytes, &normalized, index);
                if start < index {
                    // Take the sign back off the output: it belongs to the
                    // literal, not to the shape.
                    normalized
                        .truncate(normalized.len().saturating_sub(index.saturating_sub(start)));
                }
                let text = sql.get(start..end).unwrap_or_default().to_owned();
                push_placeholder(
                    &mut normalized,
                    &mut literals,
                    &mut next_placeholder,
                    Literal::Text(text),
                );
                index = end;
            }
            _ => {
                push_range(&mut normalized, sql, index, index.saturating_add(1));
                index = index.saturating_add(1);
            }
        }
    }

    Shape {
        normalized,
        literals,
        first_placeholder,
    }
}

const fn is_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn push_range(out: &mut String, sql: &str, start: usize, end: usize) {
    out.push_str(sql.get(start..end).unwrap_or_default());
}

fn push_placeholder(
    out: &mut String,
    literals: &mut Vec<Literal>,
    next: &mut u16,
    literal: Literal,
) {
    out.push('$');
    out.push_str(&next.to_string());
    literals.push(literal);
    *next = next.saturating_add(1);
}

/// Reads a single-quoted string, returning its value and the byte after it.
fn read_string(sql: &str, start: usize) -> (Literal, usize) {
    let bytes = sql.as_bytes();
    let mut index = start.saturating_add(1);
    let mut escaped = false;
    while index < bytes.len() {
        if bytes.get(index) == Some(&b'\'') {
            if bytes.get(index.saturating_add(1)) == Some(&b'\'') {
                // A doubled quote is one literal quote. Reconstructing the
                // value would mean unescaping, so the literal is kept as a
                // position without a usable value.
                escaped = true;
                index = index.saturating_add(2);
                continue;
            }
            let value = sql
                .get(start.saturating_add(1)..index)
                .unwrap_or_default()
                .to_owned();
            let literal = if escaped {
                Literal::Opaque
            } else {
                Literal::Text(value)
            };
            return (literal, index.saturating_add(1));
        }
        index = index.saturating_add(1);
    }
    // Unterminated: the parser will reject it, so the value does not matter.
    (Literal::Opaque, bytes.len())
}

fn skip_quoted(bytes: &[u8], start: usize, quote: u8) -> usize {
    let mut index = start.saturating_add(1);
    while index < bytes.len() && bytes.get(index) != Some(&quote) {
        index = index.saturating_add(1);
    }
    index.saturating_add(1).min(bytes.len())
}

fn skip_line_comment(bytes: &[u8], start: usize) -> usize {
    let mut index = start;
    while index < bytes.len() && bytes.get(index) != Some(&b'\n') {
        index = index.saturating_add(1);
    }
    index
}

fn skip_block_comment(bytes: &[u8], start: usize) -> usize {
    let mut index = start.saturating_add(2);
    while index < bytes.len() {
        if bytes.get(index) == Some(&b'*') && bytes.get(index.saturating_add(1)) == Some(&b'/') {
            return index.saturating_add(2).min(bytes.len());
        }
        index = index.saturating_add(1);
    }
    bytes.len()
}

/// Skips a `$1` placeholder or a `$tag$ ... $tag$` block, returning the byte
/// after it.
fn skip_dollar(sql: &str, bytes: &[u8], start: usize) -> usize {
    let mut cursor = start.saturating_add(1);
    let digits_from = cursor;
    while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
        cursor = cursor.saturating_add(1);
    }
    if cursor > digits_from {
        return cursor;
    }
    let mut tag_end = cursor;
    while bytes.get(tag_end).is_some_and(|byte| *byte != b'$') {
        tag_end = tag_end.saturating_add(1);
    }
    let Some(tag) = sql.get(start..tag_end.saturating_add(1)) else {
        return bytes.len();
    };
    if bytes.get(tag_end) != Some(&b'$') {
        return bytes.len();
    }
    let body_start = tag_end.saturating_add(1);
    sql.get(body_start..)
        .and_then(|rest| rest.find(tag))
        .map_or(bytes.len(), |offset| {
            body_start
                .saturating_add(offset)
                .saturating_add(tag.len())
                .min(bytes.len())
        })
}

fn read_number_end(bytes: &[u8], start: usize) -> usize {
    let mut index = start;
    let mut seen_dot = false;
    while let Some(byte) = bytes.get(index).copied() {
        if byte.is_ascii_digit() {
            index = index.saturating_add(1);
        } else if byte == b'.' && !seen_dot {
            seen_dot = true;
            index = index.saturating_add(1);
        } else {
            break;
        }
    }
    index
}

/// Keywords after which a `-` introduces a negative number rather than a
/// subtraction.
const SIGN_KEYWORDS: &[&str] = &[
    "AND",
    "BETWEEN",
    "BY",
    "CASE",
    "DEFAULT",
    "ELSE",
    "IS",
    "LIKE",
    "LIMIT",
    "NOT",
    "OFFSET",
    "OR",
    "RETURNING",
    "SELECT",
    "SET",
    "THEN",
    "VALUES",
    "WHEN",
];

/// Where a numeric literal really starts, taking in a leading sign when the
/// sign belongs to the number rather than standing between two values.
///
/// `id = -1` is a negative one. `total - 1` is a subtraction. Reading the
/// second as the first is how `where id = -1` ends up routed as if it were
/// `1`, which is a bug a real proxy shipped.
fn sign_start(bytes: &[u8], emitted: &str, number_start: usize) -> usize {
    let emitted_bytes = emitted.as_bytes();

    // The sign, if there is one, is the last non-whitespace byte emitted.
    let mut before_sign = emitted_bytes
        .iter()
        .rev()
        .skip_while(|byte| byte.is_ascii_whitespace());
    let Some(&sign) = before_sign.next() else {
        // Nothing emitted yet, so there is no sign either.
        return number_start;
    };
    if sign != b'-' && sign != b'+' {
        return number_start;
    }

    // What stands before the sign decides what the sign is.
    let rest: Vec<u8> = before_sign.copied().collect();
    let preceding = rest
        .iter()
        .find(|byte| !byte.is_ascii_whitespace())
        .copied();

    let is_sign = match preceding {
        // The statement opens with it.
        None => true,
        // An operator or an opening bracket cannot be a left operand.
        Some(byte) if SIGN_FOLLOWS.contains(&byte) => true,
        // A word: a keyword leaves the sign unary, a column name does not.
        Some(byte) if is_name_byte(byte) => {
            let mut word: Vec<u8> = rest
                .iter()
                .skip_while(|byte| byte.is_ascii_whitespace())
                .take_while(|byte| is_name_byte(**byte))
                .copied()
                .collect();
            // `rest` runs backwards, so the word does too.
            word.reverse();
            String::from_utf8(word)
                .is_ok_and(|word| SIGN_KEYWORDS.contains(&word.to_uppercase().as_str()))
        }
        // A closing bracket or a quote is a value, so this is a subtraction.
        _ => false,
    };
    if !is_sign {
        return number_start;
    }

    // Walk back over the whitespace to the sign character itself.
    let mut start = number_start;
    while start > 0 {
        let candidate = start.saturating_sub(1);
        match bytes.get(candidate).copied() {
            Some(byte) if byte.is_ascii_whitespace() => start = candidate,
            Some(byte) if byte == b'-' || byte == b'+' => return candidate,
            _ => break,
        }
    }
    number_start
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape(sql: &str) -> Shape {
        normalize(sql)
    }

    fn texts(shape: &Shape) -> Vec<Option<&str>> {
        shape
            .literals
            .iter()
            .map(|literal| literal.as_text())
            .collect()
    }

    #[test]
    fn two_statements_differing_only_in_a_literal_share_a_shape() {
        let a = shape("SELECT * FROM users WHERE tenant_id = 'alice'");
        let b = shape("SELECT * FROM users WHERE tenant_id = 'bob'");
        assert_eq!(a.normalized, b.normalized);
        assert_eq!(a.normalized, "SELECT * FROM users WHERE tenant_id = $1");
        assert_eq!(texts(&a), [Some("alice")]);
        assert_eq!(texts(&b), [Some("bob")]);
    }

    #[test]
    fn numbers_are_lifted_too() {
        let s = shape("SELECT * FROM users WHERE tenant_id = 42");
        assert_eq!(s.normalized, "SELECT * FROM users WHERE tenant_id = $1");
        assert_eq!(texts(&s), [Some("42")]);
    }

    #[test]
    fn a_negative_number_keeps_its_sign() {
        // The bug this guards against: lifting `1` out of `= -1` and leaving
        // the minus behind, so the value routed on is the absolute value.
        let s = shape("SELECT * FROM t WHERE id = -1");
        assert_eq!(s.normalized, "SELECT * FROM t WHERE id = $1");
        assert_eq!(texts(&s), [Some("-1")]);
    }

    #[test]
    fn a_subtraction_is_not_a_sign() {
        let s = shape("SELECT total - 1 FROM t");
        assert_eq!(s.normalized, "SELECT total - $1 FROM t");
        assert_eq!(texts(&s), [Some("1")]);
    }

    #[test]
    fn a_negative_and_a_positive_literal_give_different_values() {
        let negative = shape("SELECT * FROM t WHERE id = -1");
        let positive = shape("SELECT * FROM t WHERE id = 1");
        assert_eq!(negative.normalized, positive.normalized);
        assert_ne!(texts(&negative), texts(&positive));
    }

    #[test]
    fn decimals_are_lifted_whole() {
        let s = shape("SELECT * FROM t WHERE rate = 1.25");
        assert_eq!(texts(&s), [Some("1.25")]);
    }

    #[test]
    fn digits_inside_a_name_are_left_alone() {
        let s = shape("SELECT col1, x2y FROM t3");
        assert_eq!(s.normalized, "SELECT col1, x2y FROM t3");
        assert!(s.is_empty());
    }

    #[test]
    fn literals_inside_strings_comments_and_identifiers_are_left_alone() {
        assert_eq!(shape("SELECT '9'").literals.len(), 1);
        assert_eq!(
            shape("SELECT a -- 9\nFROM t").normalized,
            "SELECT a -- 9\nFROM t"
        );
        assert_eq!(
            shape("SELECT a /* 9 */ FROM t").normalized,
            "SELECT a /* 9 */ FROM t"
        );
        assert_eq!(
            shape("SELECT \"col9\" FROM t").normalized,
            "SELECT \"col9\" FROM t"
        );
        assert_eq!(
            shape("SELECT $tag$ 9 $tag$").normalized,
            "SELECT $tag$ 9 $tag$"
        );
    }

    #[test]
    fn existing_placeholders_are_kept_and_lifted_ones_numbered_after_them() {
        let s = shape("SELECT * FROM t WHERE a = $1 AND b = 'x'");
        assert_eq!(s.first_placeholder, 2);
        assert_eq!(s.normalized, "SELECT * FROM t WHERE a = $1 AND b = $2");
        assert_eq!(texts(&s), [Some("x")]);
    }

    #[test]
    fn a_literal_before_an_existing_placeholder_still_numbers_after_it() {
        let s = shape("SELECT * FROM t WHERE a = 'x' AND b = $1");
        assert_eq!(s.first_placeholder, 2);
        assert_eq!(s.normalized, "SELECT * FROM t WHERE a = $2 AND b = $1");
        assert_eq!(texts(&s), [Some("x")]);
    }

    #[test]
    fn an_escaped_quote_makes_the_literal_opaque_rather_than_wrong() {
        let s = shape("SELECT * FROM t WHERE name = 'O''Brien'");
        assert_eq!(s.literals, vec![Literal::Opaque]);
    }

    #[test]
    fn a_statement_with_no_literals_normalizes_to_itself() {
        let s = shape("SELECT * FROM users");
        assert_eq!(s.normalized, "SELECT * FROM users");
        assert!(s.is_empty());
    }

    #[test]
    fn an_unterminated_string_does_not_loop() {
        let s = shape("SELECT 'unterminated");
        assert_eq!(s.literals, vec![Literal::Opaque]);
    }

    #[test]
    fn an_unterminated_dollar_quote_does_not_loop() {
        assert_eq!(shape("SELECT $tag$ x").normalized, "SELECT $tag$ x");
    }

    #[test]
    fn literal_values_are_positional() {
        let s = shape("INSERT INTO t (a, b, c) VALUES ('x', 2, 'z')");
        assert_eq!(
            s.literal_values(),
            vec![
                Some("x".to_owned()),
                Some("2".to_owned()),
                Some("z".to_owned())
            ]
        );
        assert_eq!(s.first_placeholder, 1);
    }
}
