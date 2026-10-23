//! Routing hints an application writes into a statement's comments.
//!
//! Sometimes the key is there but the proxy cannot see it: behind a view, in
//! a function's argument, in a subquery. And sometimes the proxy cannot read
//! the statement at all, because its parser does not know a corner of
//! PostgreSQL's grammar. The application knows where the statement belongs,
//! and says so:
//!
//! ```sql
//! /* luxbyte: key=alice */ SELECT * FROM user_summary;
//! /* luxbyte: region=eu-west */ SELECT pg_size_pretty(pg_database_size('shop'));
//! ```
//!
//! `key=` routes as if every geo-partitioned table in the statement were
//! constrained to that key; `region=` sends the statement to one region.
//! A hint that cannot be read is an error, never ignored: a statement the
//! application meant for one place must not quietly go somewhere else.

use crate::statements::block_comments;

/// What a comment opening with this says is for the proxy.
pub const MARKER: &str = "luxbyte:";

/// A routing hint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Hint {
    /// Route as if every geo-partitioned table were constrained to this key.
    Key(String),
    /// Send the statement to this region.
    Region(String),
}

/// Finds the hint in a statement, if there is one.
///
/// Only a comment that begins with [`MARKER`] is read, so prose that happens
/// to mention the proxy is not taken for an instruction.
///
/// # Errors
///
/// When a hint is malformed, or there is more than one.
pub fn find(sql: &str) -> Result<Option<Hint>, String> {
    // Cheap enough to run on every statement: most have no comment at all.
    if !sql.contains(MARKER) {
        return Ok(None);
    }
    let mut found = None;
    for comment in block_comments(sql) {
        let Some(directive) = comment.trim_start().strip_prefix(MARKER) else {
            continue;
        };
        if found.is_some() {
            return Err("a statement carries more than one luxbyte hint".to_owned());
        }
        found = Some(directive_of(directive.trim())?);
    }
    Ok(found)
}

fn directive_of(text: &str) -> Result<Hint, String> {
    let usage = "a luxbyte hint says key=<value> or region=<region>";
    let (name, value) = text
        .split_once('=')
        .ok_or_else(|| format!("{usage}, not \"{text}\""))?;
    let value = value.trim();
    let value = match value
        .strip_prefix('\'')
        .and_then(|inner| inner.strip_suffix('\''))
    {
        Some(inner) => inner.replace("''", "'"),
        None if value.contains(char::is_whitespace) => {
            return Err(format!(
                "{usage}; quote a value with spaces in it, as key='like this'"
            ));
        }
        None => value.to_owned(),
    };
    if value.is_empty() {
        return Err(format!("{usage}, with a value"));
    }
    match name.trim().to_ascii_lowercase().as_str() {
        "key" => Ok(Hint::Key(value)),
        "region" => Ok(Hint::Region(value)),
        other => Err(format!("{usage}; \"{other}\" is not one of those")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_or_a_region_is_read_from_a_comment() {
        assert_eq!(
            find("/* luxbyte: key=alice */ SELECT * FROM v"),
            Ok(Some(Hint::Key("alice".to_owned())))
        );
        assert_eq!(
            find("SELECT 1 /*luxbyte:region = eu-west*/"),
            Ok(Some(Hint::Region("eu-west".to_owned())))
        );
        assert_eq!(
            find("/* luxbyte: key='o''brien & co' */ SELECT 1"),
            Ok(Some(Hint::Key("o'brien & co".to_owned())))
        );
        assert_eq!(find("SELECT 1"), Ok(None));
    }

    #[test]
    fn what_only_looks_like_a_hint_is_not_one() {
        // Inside a string, or a comment that merely mentions the proxy.
        assert_eq!(find("SELECT '/* luxbyte: region=x */'"), Ok(None));
        assert_eq!(find("/* ask luxbyte: region=x */ SELECT 1"), Ok(None));
        assert_eq!(find("-- luxbyte: region=x\nSELECT 1"), Ok(None));
    }

    #[test]
    fn a_hint_that_cannot_be_read_is_an_error() {
        for wrong in [
            "/* luxbyte: shard=3 */ SELECT 1",
            "/* luxbyte: key */ SELECT 1",
            "/* luxbyte: key= */ SELECT 1",
            "/* luxbyte: key=two words */ SELECT 1",
            "/* luxbyte: key=a */ SELECT 1 /* luxbyte: region=b */",
        ] {
            assert!(find(wrong).is_err(), "{wrong}");
        }
    }
}
