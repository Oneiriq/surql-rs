//! Just enough of the SurrealQL lexer to tell code from comments and quoted
//! text.
//!
//! Migration files are split into statements, and statements are classified
//! by their leading keywords, before the engine ever sees them. Both jobs go
//! wrong when a `;` or a keyword inside a comment or a string literal is
//! taken for code, so both go through this scanner. It follows the engine's
//! lexer (`surrealdb-core` `syn/lexer`): `--`, `//` and `#` comment to the end
//! of the line, `/* … */` comments do not nest, `'…'` and `"…"` strings and
//! `` `…` `` and `⟨…⟩` identifiers run to their closing delimiter, and inside
//! quotes a `\` escapes the character after it.

/// What a [`Segment`] of text is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    /// Keywords, names, operators and whitespace.
    Code,
    /// A `--`, `//` or `#` comment, without the line break that ends it.
    LineComment,
    /// A `/* … */` comment.
    BlockComment,
    /// A string literal or quoted identifier, delimiters included; the
    /// payload is the opening delimiter.
    Quoted(char),
}

/// A maximal run of text of one [`Kind`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Segment<'a> {
    /// What the text is.
    pub kind: Kind,
    /// The text itself, a slice of the scanned input.
    pub text: &'a str,
}

/// Cut `text` into code, comment and quoted segments. Concatenating the
/// segments gives back `text` exactly.
pub(crate) fn segments(text: &str) -> Vec<Segment<'_>> {
    let mut out: Vec<Segment<'_>> = Vec::new();
    let mut rest = text;
    while !rest.is_empty() {
        let (kind, len) = next_segment(rest);
        let (head, tail) = rest.split_at(len);
        out.push(Segment { kind, text: head });
        rest = tail;
    }
    out
}

/// The kind and byte length of the segment `s` starts with. The length is
/// always at least one character, so the caller's loop makes progress.
fn next_segment(s: &str) -> (Kind, usize) {
    let mut chars = s.chars();
    let first = chars.next().unwrap_or(' ');
    let second = chars.next();
    match (first, second) {
        ('-', Some('-')) | ('/', Some('/')) | ('#', _) => (Kind::LineComment, line_end(s)),
        ('/', Some('*')) => {
            let len = s
                .get(2..)
                .and_then(|body| body.find("*/"))
                .map_or(s.len(), |end| end + 4);
            (Kind::BlockComment, len)
        }
        ('\'' | '"' | '`', _) => (Kind::Quoted(first), quoted_len(s, first)),
        ('⟨', _) => (Kind::Quoted(first), quoted_len(s, '⟩')),
        _ => (Kind::Code, code_len(s)),
    }
}

/// Byte offset of the line break ending the comment `s` starts with. The
/// engine ends a line comment at `\n`, `\r`, and the Unicode line and
/// paragraph separators and next-line character.
fn line_end(s: &str) -> usize {
    s.find(['\n', '\r', '\u{2028}', '\u{2029}', '\u{85}'])
        .unwrap_or(s.len())
}

/// Byte length of the quoted run `s` starts with, closing delimiter
/// included; the rest of `s` when it is never closed.
fn quoted_len(s: &str, close: char) -> usize {
    let mut escaped = false;
    for (idx, c) in s.char_indices().skip(1) {
        if escaped {
            escaped = false;
        } else if c == '\\' {
            escaped = true;
        } else if c == close {
            return idx + c.len_utf8();
        }
    }
    s.len()
}

/// Byte length of the code run `s` starts with: up to the next character
/// that opens a comment or a quote. The first character is code by
/// construction, so the scan starts after it.
fn code_len(s: &str) -> usize {
    let mut iter = s.char_indices().skip(1).peekable();
    while let Some((idx, c)) = iter.next() {
        let next = iter.peek().map(|(_, n)| *n);
        let opens = matches!(
            (c, next),
            ('-', Some('-')) | ('/', Some('/' | '*')) | ('#' | '\'' | '"' | '`' | '⟨', _)
        );
        if opens {
            return idx;
        }
    }
    s.len()
}

/// Split SurrealQL text into statements.
///
/// Only a `;` in code, outside every `{ }`, `( )` and `[ ]`, ends a
/// statement: a `DEFINE FUNCTION` body and a `FOR` loop both carry
/// semicolons inside braces, and comments and string literals may carry
/// anything at all. Each statement keeps its own text verbatim, trailing `;`
/// included, with surrounding whitespace trimmed; a comment before a
/// statement stays attached to it. A piece holding nothing but whitespace
/// and comments is not a statement and is dropped.
pub(crate) fn split_statements(text: &str) -> Vec<String> {
    let mut statements: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut has_code = false;
    let mut depth = 0usize;

    for segment in segments(text) {
        match segment.kind {
            Kind::Code => {
                for ch in segment.text.chars() {
                    current.push(ch);
                    match ch {
                        '{' | '(' | '[' => depth += 1,
                        '}' | ')' | ']' => depth = depth.saturating_sub(1),
                        ';' if depth == 0 => {
                            if has_code {
                                statements.push(current.trim().to_owned());
                            }
                            current.clear();
                            has_code = false;
                            continue;
                        }
                        _ => {}
                    }
                    if !ch.is_whitespace() {
                        has_code = true;
                    }
                }
            }
            Kind::Quoted(_) => {
                current.push_str(segment.text);
                has_code = true;
            }
            Kind::LineComment | Kind::BlockComment => current.push_str(segment.text),
        }
    }
    if has_code {
        statements.push(current.trim().to_owned());
    }
    statements
}

/// `stmt` with a statement terminator after it, unless it already ends in
/// one. A statement ending in a line comment gets its `;` on a line of its
/// own, where the comment cannot swallow it.
pub(crate) fn terminate_statement(stmt: &str) -> String {
    let trimmed = stmt.trim();
    match segments(trimmed).last() {
        Some(last) if last.kind == Kind::LineComment => format!("{trimmed}\n;"),
        Some(last) if last.kind == Kind::Code && last.text.ends_with(';') => trimmed.to_owned(),
        _ => format!("{trimmed};"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(text: &str) -> Vec<(Kind, &str)> {
        segments(text)
            .into_iter()
            .map(|s| (s.kind, s.text))
            .collect()
    }

    #[test]
    fn segments_round_trip_the_input() {
        let text = "A 'b;c' -- d\n/* e */ `f` ⟨g⟩ # h\r\nI // j";
        let joined: String = segments(text).iter().map(|s| s.text).collect();
        assert_eq!(joined, text);
    }

    #[test]
    fn segments_classify_every_comment_and_quote_form() {
        assert_eq!(
            kinds("a -- b\nc"),
            vec![
                (Kind::Code, "a "),
                (Kind::LineComment, "-- b"),
                (Kind::Code, "\nc")
            ]
        );
        assert_eq!(kinds("# x")[0].0, Kind::LineComment);
        assert_eq!(kinds("// x")[0].0, Kind::LineComment);
        assert_eq!(
            kinds("/* a; */b"),
            vec![(Kind::BlockComment, "/* a; */"), (Kind::Code, "b")]
        );
        assert_eq!(kinds(r"'it\'s'")[0], (Kind::Quoted('\''), r"'it\'s'"));
        assert_eq!(kinds("⟨a;b⟩c")[0], (Kind::Quoted('⟨'), "⟨a;b⟩"));
        // Unterminated runs swallow the rest, as they would in the engine.
        assert_eq!(kinds("'open")[0], (Kind::Quoted('\''), "'open"));
        assert_eq!(kinds("/* open")[0], (Kind::BlockComment, "/* open"));
    }

    #[test]
    fn a_single_minus_or_slash_is_code() {
        assert_eq!(kinds("a - b / c"), vec![(Kind::Code, "a - b / c")]);
    }

    #[test]
    fn split_ignores_semicolons_in_comments() {
        let stmts = split_statements(
            "DEFINE TABLE a SCHEMAFULL; -- step 1; see JIRA-12\nDEFINE TABLE b SCHEMAFULL;",
        );
        assert_eq!(
            stmts,
            vec![
                "DEFINE TABLE a SCHEMAFULL;".to_owned(),
                "-- step 1; see JIRA-12\nDEFINE TABLE b SCHEMAFULL;".to_owned(),
            ]
        );
    }

    /// An apostrophe in a comment used to open a "string" that ran on into
    /// the next statement, so a `;` inside a real literal split it.
    #[test]
    fn split_is_not_confused_by_an_apostrophe_in_a_comment() {
        let stmts = split_statements("-- user's table\nCREATE user SET note = 'a;b';");
        assert_eq!(
            stmts,
            vec!["-- user's table\nCREATE user SET note = 'a;b';".to_owned()]
        );
        let stmts = split_statements("/* it's */ CREATE t SET s = 'x;y'; # don't\nSELECT 1;");
        assert_eq!(stmts.len(), 2, "{stmts:#?}");
        assert!(stmts[0].ends_with("'x;y';"));
    }

    #[test]
    fn split_respects_quoted_identifiers() {
        let stmts = split_statements("CREATE `a;b` SET x = 1; CREATE t:⟨c;d⟩; SELECT 1");
        assert_eq!(stmts.len(), 3, "{stmts:#?}");
        assert_eq!(stmts[2], "SELECT 1");
    }

    #[test]
    fn split_drops_comment_only_pieces() {
        assert!(split_statements("-- nothing here\n/* or here */\n").is_empty());
        let stmts = split_statements("SELECT 1;\n-- trailing note");
        assert_eq!(stmts, vec!["SELECT 1;".to_owned()]);
        assert!(split_statements(" ; ;; ").is_empty());
    }

    #[test]
    fn terminate_statement_keeps_the_terminator_out_of_comments() {
        assert_eq!(terminate_statement("SELECT 1"), "SELECT 1;");
        assert_eq!(terminate_statement("SELECT 1;"), "SELECT 1;");
        assert_eq!(
            terminate_statement("SELECT 1 -- note"),
            "SELECT 1 -- note\n;"
        );
        assert_eq!(
            terminate_statement("SELECT 1 -- note;"),
            "SELECT 1 -- note;\n;"
        );
        assert_eq!(terminate_statement("SELECT 1 /* x */"), "SELECT 1 /* x */;");
    }
}
