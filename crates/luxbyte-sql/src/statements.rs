//! Cutting a simple query into its statements.
//!
//! A simple query may carry several statements, and each may belong in a
//! different region. Routing them one by one is pointless unless each is
//! also *sent* on its own: sending the whole text to every region a
//! statement was routed to runs the other statements there too.
//!
//! The parser does not report where each statement began, so this finds the
//! semicolons that separate statements, skipping every place PostgreSQL's
//! lexer would not treat one as a separator: quoted strings (including the
//! backslash escapes of `E'...'`), quoted identifiers, dollar-quoted bodies,
//! and line and block comments (which nest).

/// The statements in `sql`, trimmed, without empty ones.
#[must_use]
pub fn split(sql: &str) -> Vec<&str> {
    let bytes = sql.as_bytes();
    let at = |position: usize| bytes.get(position).copied();
    let next = |position: usize| position.saturating_add(1);

    let mut statements = Vec::new();
    let mut start = 0_usize;
    let mut index = 0_usize;
    while index < bytes.len() {
        match at(index) {
            Some(b';') => {
                push(&mut statements, sql.get(start..index));
                start = next(index);
                index = start;
            }
            Some(b'\'') => {
                // `E'...'` takes backslash escapes; a plain string does not,
                // and in both a doubled quote is a quote.
                let escapes = index > 0
                    && matches!(at(index.saturating_sub(1)), Some(b'e' | b'E'))
                    && (index < 2
                        || !at(index.saturating_sub(2))
                            .is_some_and(|byte| byte.is_ascii_alphanumeric() || byte == b'_'));
                index = next(index);
                while index < bytes.len() {
                    match at(index) {
                        Some(b'\\') if escapes => index = next(next(index)),
                        Some(b'\'') if at(next(index)) == Some(b'\'') => {
                            index = next(next(index));
                        }
                        Some(b'\'') => break,
                        _ => index = next(index),
                    }
                }
                index = next(index);
            }
            Some(b'"') => {
                index = next(index);
                while index < bytes.len() {
                    match at(index) {
                        Some(b'"') if at(next(index)) == Some(b'"') => index = next(next(index)),
                        Some(b'"') => break,
                        _ => index = next(index),
                    }
                }
                index = next(index);
            }
            Some(b'-') if at(next(index)) == Some(b'-') => {
                while index < bytes.len() && at(index) != Some(b'\n') {
                    index = next(index);
                }
            }
            Some(b'/') if at(next(index)) == Some(b'*') => {
                let mut depth = 0_usize;
                while index < bytes.len() {
                    if at(index) == Some(b'/') && at(next(index)) == Some(b'*') {
                        depth = depth.saturating_add(1);
                        index = next(next(index));
                    } else if at(index) == Some(b'*') && at(next(index)) == Some(b'/') {
                        depth = depth.saturating_sub(1);
                        index = next(next(index));
                        if depth == 0 {
                            break;
                        }
                    } else {
                        index = next(index);
                    }
                }
            }
            Some(b'$') => index = skip_dollar(sql, index),
            _ => index = next(index),
        }
    }
    push(&mut statements, sql.get(start..));
    statements
}

fn push<'a>(statements: &mut Vec<&'a str>, piece: Option<&'a str>) {
    if let Some(piece) = piece.map(str::trim).filter(|piece| !piece.is_empty()) {
        statements.push(piece);
    }
}

/// Skips a dollar-quoted body starting at `index`, or just the `$` when it
/// is a placeholder or an identifier character instead.
fn skip_dollar(sql: &str, index: usize) -> usize {
    let bytes = sql.as_bytes();
    let after = index.saturating_add(1);
    // `$1` is a placeholder; a `$` after an identifier character belongs to
    // the identifier. Neither opens a quote.
    let follows_identifier = index > 0
        && bytes
            .get(index.saturating_sub(1))
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_');
    if follows_identifier || bytes.get(after).is_some_and(u8::is_ascii_digit) {
        return after;
    }
    let mut tag_end = after;
    while bytes
        .get(tag_end)
        .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
    {
        tag_end = tag_end.saturating_add(1);
    }
    if bytes.get(tag_end) != Some(&b'$') {
        return after;
    }
    let Some(tag) = sql.get(index..tag_end.saturating_add(1)) else {
        return after;
    };
    let body = tag_end.saturating_add(1);
    sql.get(body..)
        .and_then(|rest| rest.find(tag))
        .map_or(bytes.len(), |offset| {
            body.saturating_add(offset).saturating_add(tag.len())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statements_are_separated_and_trimmed() {
        assert_eq!(split("SELECT 1; SELECT 2;"), ["SELECT 1", "SELECT 2"]);
        assert_eq!(split("  ;; SELECT 1 ;  "), ["SELECT 1"]);
    }

    #[test]
    fn semicolons_inside_strings_identifiers_and_comments_do_not_separate() {
        assert_eq!(
            split("SELECT 'a;b', \"c;d\" -- e;f\n; SELECT /* g; /* nested; */ h; */ 2"),
            [
                "SELECT 'a;b', \"c;d\" -- e;f",
                "SELECT /* g; /* nested; */ h; */ 2"
            ]
        );
    }

    #[test]
    fn doubled_quotes_and_backslash_escapes_stay_inside_their_string() {
        assert_eq!(
            split("SELECT 'it''s;'; SELECT 2"),
            ["SELECT 'it''s;'", "SELECT 2"]
        );
        assert_eq!(
            split(r"SELECT E'it\'s;'; SELECT 2"),
            [r"SELECT E'it\'s;'", "SELECT 2"]
        );
        // Without the E, a backslash is an ordinary character.
        assert_eq!(
            split(r"SELECT 'a\'; SELECT 2"),
            [r"SELECT 'a\'", "SELECT 2"]
        );
    }

    #[test]
    fn dollar_quoted_bodies_are_skipped_whole() {
        let function = "CREATE FUNCTION f() RETURNS int AS $body$ SELECT 1; $body$ LANGUAGE sql";
        assert_eq!(
            split(&format!("{function}; SELECT 2")),
            [function, "SELECT 2"]
        );
        assert_eq!(
            split("SELECT $$a;b$$; SELECT 2"),
            ["SELECT $$a;b$$", "SELECT 2"]
        );
    }

    #[test]
    fn placeholders_and_identifiers_with_dollars_are_not_quotes() {
        assert_eq!(
            split("SELECT $1; SELECT a$b FROM t; SELECT 3"),
            ["SELECT $1", "SELECT a$b FROM t", "SELECT 3"]
        );
    }
}
