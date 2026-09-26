//! Lexical helpers for splicing caller-supplied text into SurrealQL.
//!
//! Every place the crate renders a name or a value into query text goes
//! through one of these functions, so the quoting rules live in one spot.
//! They follow the engine's own printer (`surrealdb-core` `fmt/escape.rs`)
//! and were checked against the SurrealDB 3.0 parser:
//!
//! * An identifier is bare when it matches `[A-Za-z_][A-Za-z0-9_]*`, is not a
//!   reserved word (`CREATE select` is a parse error, `CREATE none` evaluates
//!   `NONE`), and is not `NaN` or `Infinity`. Anything else is
//!   backtick-quoted.
//! * A string literal is single-quoted.
//! * A record-id key is bare when identifier-shaped and not made only of
//!   digits and underscores. Otherwise it is wrapped in `⟨ ⟩`, which keeps
//!   keys such as `⟨123⟩` and `⟨-5⟩` string keys rather than integer ones.
//!   `⟨ ⟩` has no escape for `⟩`, so a key containing one is backtick-quoted
//!   instead, which is the form the engine itself prints.
//!
//! Inside any quotes, `\` and the closing delimiter are backslash-escaped, and
//! the control characters the engine escapes (`\0`, `\r`, `\t`, `\n`, form
//! feed, backspace) are written the same way it writes them.

use super::reserved::is_reserved_word;

/// `true` when `s` has the bare-identifier shape `[A-Za-z_][A-Za-z0-9_]*`.
///
/// Reserved words have this shape too; use [`quote_ident`] to render a name
/// safely, and this function to decide whether a name is acceptable where
/// the API contract is "an identifier".
///
/// ## Examples
///
/// ```
/// use surql::types::escape::is_identifier;
///
/// assert!(is_identifier("user_2"));
/// assert!(!is_identifier("2user"));
/// assert!(!is_identifier("user; DELETE user"));
/// assert!(!is_identifier(""));
/// ```
pub fn is_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Render `s` as a single-quoted SurrealQL string literal.
///
/// ## Examples
///
/// ```
/// use surql::types::escape::quote_str;
///
/// assert_eq!(quote_str("it's"), r"'it\'s'");
/// assert_eq!(quote_str(r"a\b"), r"'a\\b'");
/// assert_eq!(quote_str("a\nb"), r"'a\nb'");
/// ```
pub fn quote_str(s: &str) -> String {
    quoted(s, '\'', '\'')
}

/// Render `s` as a SurrealQL identifier (a table, field, or edge name).
///
/// Bare when it is identifier-shaped and not a reserved word, backtick-quoted
/// otherwise. This is also the form a record id's table half needs:
/// `select:abc` is a parse error where `` `select`:abc `` is not.
///
/// ## Examples
///
/// ```
/// use surql::types::escape::quote_ident;
///
/// assert_eq!(quote_ident("user"), "user");
/// assert_eq!(quote_ident("select"), "`select`");
/// assert_eq!(quote_ident("my-table"), "`my-table`");
/// assert_eq!(quote_ident("a`b"), r"`a\`b`");
/// ```
pub fn quote_ident(s: &str) -> String {
    let bare =
        is_identifier(s) && !is_reserved_word(s) && !is_engine_reserved(s) && !is_float_keyword(s);
    if bare {
        s.to_owned()
    } else {
        quoted(s, '`', '`')
    }
}

/// Render `s` as the key half of a record id (`table:<key>`).
///
/// Bare when identifier-shaped, `⟨ ⟩`-wrapped otherwise, and backtick-quoted
/// when the key contains `⟩`.
///
/// ## Examples
///
/// ```
/// use surql::types::escape::quote_record_key;
///
/// assert_eq!(quote_record_key("alice"), "alice");
/// assert_eq!(quote_record_key("123"), "⟨123⟩");
/// assert_eq!(quote_record_key("a-b"), "⟨a-b⟩");
/// assert_eq!(quote_record_key(r"a\b"), r"⟨a\\b⟩");
/// assert_eq!(quote_record_key("x⟩y"), "`x⟩y`");
/// ```
pub fn quote_record_key(s: &str) -> String {
    let bare = is_identifier(s)
        && s.contains(|c: char| !c.is_ascii_digit() && c != '_')
        && !is_float_keyword(s);
    if bare {
        s.to_owned()
    } else if s.contains('⟩') {
        quoted(s, '`', '`')
    } else {
        quoted(s, '⟨', '⟩')
    }
}

/// Reverse the escaping inside a quoted string literal, identifier, or
/// record-id key body.
///
/// Understands every escape the engine prints (`\0`, `\r`, `\t`, `\n`, `\f`,
/// `\u{…}`); any other `\x` becomes `x`, which covers `\\` and escaped
/// delimiters.
///
/// ## Examples
///
/// ```
/// use surql::types::escape::unescape;
///
/// assert_eq!(unescape(r"x\`y\\z"), r"x`y\z");
/// assert_eq!(unescape(r"a\nb"), "a\nb");
/// ```
pub fn unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('0') => out.push('\0'),
            Some('r') => out.push('\r'),
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some('f') => out.push('\x0C'),
            Some('u') => {
                let rest = chars.as_str();
                let decoded = rest
                    .strip_prefix('{')
                    .and_then(|r| r.split_once('}'))
                    .and_then(|(hex, tail)| {
                        let c = u32::from_str_radix(hex, 16).ok().and_then(char::from_u32)?;
                        Some((c, tail))
                    });
                if let Some((decoded, tail)) = decoded {
                    out.push(decoded);
                    chars = tail.chars();
                } else {
                    out.push('u');
                }
            }
            Some(other) => out.push(other),
            None => out.push('\\'),
        }
    }
    out
}

/// Strip the quotes from a `'…'` or `"…"` string literal and reverse its
/// escapes. `None` when `s` is not a quoted literal.
///
/// The engine prints a string containing `'` in double quotes, so both
/// styles occur in `INFO FOR …` output.
///
/// ## Examples
///
/// ```
/// use surql::types::escape::unquote_str;
///
/// assert_eq!(unquote_str(r#""it's""#).as_deref(), Some("it's"));
/// assert_eq!(unquote_str(r"'a\'b'").as_deref(), Some("a'b"));
/// assert_eq!(unquote_str("bare"), None);
/// ```
pub fn unquote_str(s: &str) -> Option<String> {
    let inner = s
        .strip_prefix('\'')
        .and_then(|r| r.strip_suffix('\''))
        .or_else(|| s.strip_prefix('"').and_then(|r| r.strip_suffix('"')))?;
    Some(unescape(inner))
}

/// `NaN` and `Infinity` lex as floats, so the engine never prints them bare.
fn is_float_keyword(s: &str) -> bool {
    s == "NaN" || s == "Infinity"
}

/// Words the engine's parser may read as a keyword where an identifier is
/// expected (`RESERVED_KEYWORD` in surrealdb-core's lexer). The crate's own
/// [`is_reserved_word`] list exists to warn about field names and is not the
/// same set, so both are consulted.
const ENGINE_RESERVED: &[&str] = &[
    "ALTER", "BEGIN", "BREAK", "CANCEL", "COMMIT", "CONTINUE", "CREATE", "DEFINE", "DELETE", "FOR",
    "IF", "INFO", "INSERT", "KILL", "LIVE", "OPTION", "REBUILD", "RETURN", "RELATE", "REMOVE",
    "SELECT", "LET", "SHOW", "SLEEP", "THROW", "UPDATE", "UPSERT", "USE", "DIFF", "RAND", "NONE",
    "NULL", "AFTER", "BEFORE", "VALUE", "BY", "ALL", "TRUE", "FALSE", "WHERE", "TABLE", "TB",
    "SEQUENCE", "FUNCTION",
];

fn is_engine_reserved(s: &str) -> bool {
    ENGINE_RESERVED.iter().any(|k| k.eq_ignore_ascii_case(s))
}

fn quoted(s: &str, open: char, close: char) -> String {
    let mut out = String::with_capacity(s.len() + 2 * open.len_utf8());
    out.push(open);
    for c in s.chars() {
        match c {
            '\0' => out.push_str("\\0"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\x08' => out.push_str("\\u{8}"),
            '\x0C' => out.push_str("\\f"),
            '\\' => out.push_str("\\\\"),
            c if c == close => {
                out.push('\\');
                out.push(c);
            }
            c => out.push(c),
        }
    }
    out.push(close);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifier_shape() {
        assert!(is_identifier("_"));
        assert!(is_identifier("a1"));
        assert!(!is_identifier("1a"));
        assert!(!is_identifier("é"));
        assert!(!is_identifier("a.b"));
        assert!(!is_identifier("a b"));
    }

    #[test]
    fn quote_str_escapes_quote_backslash_and_controls() {
        assert_eq!(quote_str(""), "''");
        assert_eq!(quote_str(r"\'"), r"'\\\''");
        assert_eq!(quote_str("\0\r\t\x08\x0C"), r"'\0\r\t\u{8}\f'");
    }

    #[test]
    fn quote_ident_quotes_reserved_and_float_words() {
        assert_eq!(quote_ident("SELECT"), "`SELECT`");
        assert_eq!(quote_ident("none"), "`none`");
        assert_eq!(quote_ident("NaN"), "`NaN`");
        assert_eq!(quote_ident("Infinity"), "`Infinity`");
        assert_eq!(quote_ident(""), "``");
    }

    #[test]
    fn quote_ident_quotes_engine_only_keywords() {
        for word in [
            "upsert", "Option", "RAND", "tb", "sequence", "function", "rebuild",
        ] {
            assert_eq!(quote_ident(word), format!("`{word}`"));
        }
    }

    #[test]
    fn quote_ident_escapes_backslash_and_backtick() {
        assert_eq!(quote_ident(r"a\`b"), r"`a\\\`b`");
    }

    #[test]
    fn record_key_brackets_numeric_looking_strings() {
        assert_eq!(quote_record_key("007"), "⟨007⟩");
        assert_eq!(quote_record_key("-5"), "⟨-5⟩");
        assert_eq!(quote_record_key("_1"), "⟨_1⟩");
        assert_eq!(quote_record_key("1_000"), "⟨1_000⟩");
        assert_eq!(quote_record_key("Infinity"), "⟨Infinity⟩");
        assert_eq!(quote_record_key(""), "⟨⟩");
        assert_eq!(quote_record_key("_a1"), "_a1");
    }

    #[test]
    fn record_key_with_closing_bracket_uses_backticks() {
        assert_eq!(
            quote_record_key("x⟩; DELETE user; --"),
            "`x⟩; DELETE user; --`"
        );
        assert_eq!(quote_record_key("⟩`"), r"`⟩\``");
    }

    /// The text between the first and last character of `quoted`.
    fn body(quoted: &str) -> &str {
        let mut chars = quoted.chars();
        chars.next();
        chars.next_back();
        chars.as_str()
    }

    #[test]
    fn unescape_reverses_every_quoting_form() {
        let samples = [
            "a\\b",
            "x`y",
            "it's",
            "⟩",
            "\\",
            "⟩⟩`",
            "a\nb",
            "\0\r\t\x08\x0C",
            "\\u{41}",
        ];
        for raw in samples {
            let key = quote_record_key(raw);
            assert_eq!(unescape(body(&key)), raw, "{key}");
            let ident = quote_ident(raw);
            assert_eq!(unescape(body(&ident)), raw, "{ident}");
            assert_eq!(unquote_str(&quote_str(raw)).as_deref(), Some(raw));
        }
    }

    #[test]
    fn unescape_decodes_unicode_escapes() {
        assert_eq!(unescape(r"\u{41}\u{8}"), "A\x08");
        assert_eq!(unescape(r"\u{zz}"), "u{zz}");
        assert_eq!(unescape("trailing\\"), "trailing\\");
    }

    #[test]
    fn unquote_str_accepts_both_quote_styles() {
        assert_eq!(unquote_str(r#""it's""#).as_deref(), Some("it's"));
        assert_eq!(unquote_str(r"'a\'b'").as_deref(), Some("a'b"));
        assert_eq!(unquote_str("bare"), None);
    }
}
