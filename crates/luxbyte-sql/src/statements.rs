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
    let mut statements = Vec::new();
    let mut start = 0_usize;
    for (at, end, mark) in scan(sql) {
        if mark == Mark::Separator {
            push(&mut statements, sql.get(start..at));
            start = end;
        }
    }
    push(&mut statements, sql.get(start..));
    statements
}

/// Replaces each placeholder `$n` with `values[n - 1]`, leaving alone
/// anything that only looks like one: inside a string, a quoted identifier,
/// a dollar-quoted body or a comment, or part of an identifier.
///
/// Returns the number of a placeholder with no value, if there is one.
pub fn substitute(sql: &str, values: &[String]) -> Result<String, usize> {
    let mut out = String::with_capacity(sql.len());
    let mut copied = 0_usize;
    for (at, end, mark) in scan(sql) {
        let Mark::Placeholder(number) = mark else {
            continue;
        };
        let value = number
            .checked_sub(1)
            .and_then(|index| values.get(index))
            .ok_or(number)?;
        out.push_str(sql.get(copied..at).unwrap_or_default());
        out.push_str(value);
        copied = end;
    }
    out.push_str(sql.get(copied..).unwrap_or_default());
    Ok(out)
}

/// Something the lexer found that callers act on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mark {
    /// A `;` between statements.
    Separator,
    /// A placeholder, by number.
    Placeholder(usize),
}

/// Where the separators and placeholders are, as byte ranges, skipping
/// every place PostgreSQL's lexer would not see one.
fn scan(sql: &str) -> Vec<(usize, usize, Mark)> {
    let bytes = sql.as_bytes();
    let at = |position: usize| bytes.get(position).copied();
    let next = |position: usize| position.saturating_add(1);

    let mut marks = Vec::new();
    let mut index = 0_usize;
    while index < bytes.len() {
        match at(index) {
            Some(b';') => {
                marks.push((index, next(index), Mark::Separator));
                index = next(index);
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
            Some(b'$') => {
                let end = placeholder_end(bytes, index);
                if end > next(index) {
                    let number = sql
                        .get(next(index)..end)
                        .and_then(|digits| digits.parse().ok())
                        .unwrap_or(0);
                    marks.push((index, end, Mark::Placeholder(number)));
                    index = end;
                } else {
                    index = skip_dollar(sql, index);
                }
            }
            _ => index = next(index),
        }
    }
    marks
}

/// Where a placeholder starting at `index` ends, or `index + 1` if the `$`
/// there does not start one.
fn placeholder_end(bytes: &[u8], index: usize) -> usize {
    let after = index.saturating_add(1);
    if follows_identifier(bytes, index) {
        return after;
    }
    let mut end = after;
    while bytes.get(end).is_some_and(u8::is_ascii_digit) {
        end = end.saturating_add(1);
    }
    end
}

/// Whether the byte before `index` is part of an identifier, which makes a
/// `$` there part of it too.
fn follows_identifier(bytes: &[u8], index: usize) -> bool {
    index > 0
        && bytes
            .get(index.saturating_sub(1))
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_' || *byte == b'$')
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
    if follows_identifier(bytes, index) || bytes.get(after).is_some_and(u8::is_ascii_digit) {
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

    fn values(list: &[&str]) -> Vec<String> {
        list.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn placeholders_are_replaced_and_lookalikes_left_alone() {
        assert_eq!(
            substitute(
                "SELECT $1, '$1', \"$1\", $$ $1 $$, a$1, -- $1\n $2 /* $1 */",
                &values(&["'x'", "2"])
            ),
            Ok("SELECT 'x', '$1', \"$1\", $$ $1 $$, a$1, -- $1\n 2 /* $1 */".to_owned())
        );
        assert_eq!(
            substitute(
                "SELECT $10 + $1",
                &values(&["1", "", "", "", "", "", "", "", "", "10"])
            ),
            Ok("SELECT 10 + 1".to_owned())
        );
        assert_eq!(substitute("SELECT $2", &values(&["1"])), Err(2));
        assert_eq!(
            substitute("SELECT $tag$ $1 $tag$", &[]),
            Ok("SELECT $tag$ $1 $tag$".to_owned())
        );
    }

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
