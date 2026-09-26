//! Quote- and bracket-aware scanning shared by the `INFO` parsers.
//!
//! The engine's echo of a definition is SurrealQL, so a clause keyword can
//! also appear inside a string literal (`COMMENT 'no changefeed needed'`), a
//! backticked name, a record-id key, or a nested expression
//! (`ASSERT $value INSIDE (SELECT VALUE name FROM tag)`). Every search here
//! only matches at the top level: outside any quoted run (`'…'`, `"…"`,
//! `` `…` ``, `⟨…⟩`, each honouring backslash escapes) and outside any `()`,
//! `[]`, or `{}` pair.
//!
//! The echo is server text, so nothing here indexes or slices by an offset
//! it has not checked: every slice goes through `str::get`.

use crate::types::escape::unescape;

/// Walks SurrealQL one character at a time, tracking quotes and brackets.
#[derive(Default)]
struct Lexer {
    depth: usize,
    quote: Option<char>,
    escaped: bool,
}

impl Lexer {
    /// Consume `c`, returning whether it sat at the top level (outside
    /// quotes, bracket depth zero) before it was consumed.
    fn step(&mut self, c: char) -> bool {
        let top = self.quote.is_none() && self.depth == 0;
        match self.quote {
            Some(close) => {
                if self.escaped {
                    self.escaped = false;
                } else if c == '\\' {
                    self.escaped = true;
                } else if c == close {
                    self.quote = None;
                }
            }
            None => match c {
                '\'' | '"' | '`' => self.quote = Some(c),
                '⟨' => self.quote = Some('⟩'),
                '(' | '[' | '{' => self.depth += 1,
                ')' | ']' | '}' => self.depth = self.depth.saturating_sub(1),
                _ => {}
            },
        }
        top
    }
}

/// Per byte of `text`: `true` when it sits at the top level.
pub(super) fn top_level_mask(text: &str) -> Vec<bool> {
    let mut mask = vec![false; text.len()];
    let mut lexer = Lexer::default();
    for (at, c) in text.char_indices() {
        if lexer.step(c) {
            for slot in mask.iter_mut().skip(at).take(c.len_utf8()) {
                *slot = true;
            }
        }
    }
    mask
}

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// `true` when `keyword` starts at byte `at` as a whole top-level word.
///
/// The left neighbour must be whitespace, a separator, or a closing bracket,
/// so `$value`, `a.value`, and `fn::value` never read as the `VALUE`
/// keyword. The right neighbour must not continue a word, a path, or a
/// `::` function name.
fn keyword_at(text: &str, mask: &[bool], at: usize, keyword: &str) -> bool {
    let bytes = text.as_bytes();
    if !mask.get(at).copied().unwrap_or(false) {
        return false;
    }
    let end = at + keyword.len();
    let Some(candidate) = bytes.get(at..end) else {
        return false;
    };
    if !candidate.eq_ignore_ascii_case(keyword.as_bytes()) {
        return false;
    }
    let left_ok = at
        .checked_sub(1)
        .and_then(|before| bytes.get(before))
        .is_none_or(|b| b.is_ascii_whitespace() || matches!(b, b',' | b';' | b')' | b']' | b'}'));
    let right_ok = bytes
        .get(end)
        .is_none_or(|b| !is_word_byte(*b) && !matches!(b, b':' | b'.' | b'['));
    left_ok && right_ok
}

/// Byte offset of the first top-level occurrence of `keyword` at or after
/// `from`, matched case-insensitively as a whole word.
pub(super) fn find_keyword_from(text: &str, keyword: &str, from: usize) -> Option<usize> {
    let mask = top_level_mask(text);
    (from..text.len()).find(|&at| keyword_at(text, &mask, at, keyword))
}

/// [`find_keyword_from`] from the start of `text`.
pub(super) fn find_keyword(text: &str, keyword: &str) -> Option<usize> {
    find_keyword_from(text, keyword, 0)
}

/// One whitespace-delimited top-level token and its byte range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Token<'a> {
    /// The token text.
    pub text: &'a str,
    /// Byte offset of its first character.
    pub start: usize,
    /// Byte offset one past its last character.
    pub end: usize,
}

impl Token<'_> {
    /// Case-insensitive comparison against a keyword, ignoring a trailing
    /// `,` or `;` glued to the token.
    pub(super) fn is(&self, keyword: &str) -> bool {
        self.text
            .trim_end_matches([',', ';'])
            .eq_ignore_ascii_case(keyword)
    }
}

/// Split `text` at top-level whitespace. Quoted runs and bracketed groups
/// stay whole, so `(SELECT * FROM x)` is one token.
pub(super) fn tokens(text: &str) -> Vec<Token<'_>> {
    let mut spans = Vec::new();
    let mut lexer = Lexer::default();
    let mut start: Option<usize> = None;
    for (at, c) in text.char_indices() {
        let top = lexer.step(c);
        let separator = top && c.is_whitespace();
        match (separator, start) {
            (true, Some(from)) => {
                spans.push((from, at));
                start = None;
            }
            (false, None) => start = Some(at),
            _ => {}
        }
    }
    if let Some(from) = start {
        spans.push((from, text.len()));
    }
    spans
        .into_iter()
        .filter_map(|(start, end)| {
            text.get(start..end).map(|slice| Token {
                text: slice,
                start,
                end,
            })
        })
        .collect()
}

/// Split `text` at every top-level occurrence of `separator`.
pub(super) fn split_top_level(text: &str, separator: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut lexer = Lexer::default();
    let mut start = 0;
    for (at, c) in text.char_indices() {
        if lexer.step(c) && c == separator {
            parts.push(text.get(start..at).unwrap_or(""));
            start = at + c.len_utf8();
        }
    }
    parts.push(text.get(start..).unwrap_or(""));
    parts
}

/// Byte offset of the bracket that closes the one at `open`, skipping any
/// bracket inside a quoted run. `None` when `open` is not an opening
/// bracket or the group never closes.
pub(super) fn matching_close(text: &str, open: usize) -> Option<usize> {
    let tail = text.get(open..)?;
    if !tail.starts_with(['(', '[', '{']) {
        return None;
    }
    let mut lexer = Lexer::default();
    for (at, c) in tail.char_indices() {
        lexer.step(c);
        if lexer.quote.is_none() && lexer.depth == 0 {
            return Some(open + at);
        }
    }
    None
}

/// The body of `text` when all of it is one `open … close` group, trimmed.
/// `None` when the group closes before the end or `text` does not start
/// with `open`.
pub(super) fn strip_group(text: &str, open: char) -> Option<&str> {
    let trimmed = text.trim();
    if !trimmed.starts_with(open) {
        return None;
    }
    let close = matching_close(trimmed, 0)?;
    if close + 1 != trimmed.len() {
        return None;
    }
    trimmed.get(open.len_utf8()..close).map(str::trim)
}

/// The unescaped name inside a backticked or `⟨…⟩` identifier token, or the
/// token itself when it is bare.
pub(super) fn unquote_ident(token: &str) -> String {
    let token = token.trim();
    let inner = token
        .strip_prefix('`')
        .and_then(|rest| rest.strip_suffix('`'))
        .or_else(|| {
            token
                .strip_prefix('⟨')
                .and_then(|rest| rest.strip_suffix('⟩'))
        });
    match inner {
        Some(inner) => unescape(inner),
        None => token.to_string(),
    }
}

/// How the text after a clause keyword is shaped, which decides whether a
/// word that spells a keyword really starts a clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Shape {
    /// Followed by an expression (`VALUE <expr>`).
    Expr,
    /// A bare flag (`READONLY`), or one with an optional `ON …` tail
    /// (`REFERENCE ON DELETE CASCADE`).
    Flag,
    /// Followed by a string literal (`COMMENT '…'`).
    Str,
    /// Followed by `SELECT` (`AS SELECT …` on a view), so a projection alias
    /// (`count() AS total`) is not a clause.
    Select,
}

/// One clause found by [`clauses`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Clause<'a> {
    /// The keyword, as spelled in the table passed to [`clauses`].
    pub keyword: &'static str,
    /// The trimmed text between this clause's keyword and the next clause.
    pub body: &'a str,
}

/// Binary operators: a keyword-shaped word next to one of these is an
/// operand in an expression (`ASSERT type = 'x'`), not a clause.
const OPERATORS: &[&str] = &[
    "=",
    "==",
    "!=",
    "?=",
    "*=",
    "<",
    ">",
    "<=",
    ">=",
    "+",
    "*",
    "/",
    "%",
    "**",
    "&&",
    "||",
    "??",
    "?:",
    "~",
    "!~",
    "?~",
    "*~",
    "@@",
    "AND",
    "OR",
    "IS",
    "IN",
    "INSIDE",
    "NOTINSIDE",
    "CONTAINS",
    "CONTAINSNOT",
    "CONTAINSALL",
    "CONTAINSANY",
    "CONTAINSNONE",
    "ALLINSIDE",
    "ANYINSIDE",
    "NONEINSIDE",
    "OUTSIDE",
    "INTERSECTS",
    "MATCHES",
];

/// Words after which the next word is an operand: `WHERE permissions`,
/// `SELECT comment`, `FROM permissions`.
const OPERAND_PREFIXES: &[&str] = &[
    "WHERE", "SELECT", "FROM", "BY", "RETURN", "THEN", "ELSE", "SET", "NOT", "-", "!",
];

fn is_operator(token: &Token<'_>) -> bool {
    OPERATORS
        .iter()
        .any(|op| token.text.eq_ignore_ascii_case(op))
}

/// `true` when the word after `prev` must be an operand.
fn opens_operand(prev: &Token<'_>) -> bool {
    is_operator(prev)
        || prev.text.ends_with(',')
        || OPERAND_PREFIXES
            .iter()
            .any(|p| prev.text.eq_ignore_ascii_case(p))
}

/// Split `text` into the clauses introduced by `keywords`, in source order.
///
/// A word spelling a keyword starts a clause only where the grammar allows
/// one: not next to a binary operator, not as the first word of an
/// expression clause's body, a [`Shape::Str`] keyword only before a string
/// literal, and a [`Shape::Flag`] keyword only before the end, `;`, `ON`,
/// or another keyword. That is what lets `ASSERT readonly = true`,
/// `VALUE default + 1`, and `ASSERT comment != ''` read as expressions.
pub(super) fn clauses<'a>(text: &'a str, keywords: &[(&'static str, Shape)]) -> Vec<Clause<'a>> {
    let toks = tokens(text);
    let lookup = |token: &Token<'_>| keywords.iter().find(|(kw, _)| token.is(kw)).copied();
    let mut starts: Vec<(usize, &'static str, usize)> = Vec::new();
    for (i, token) in toks.iter().enumerate() {
        let Some((keyword, shape)) = lookup(token) else {
            continue;
        };
        let prev = i.checked_sub(1).and_then(|p| toks.get(p));
        let next = toks.get(i + 1);
        if prev.is_some_and(opens_operand) || next.is_some_and(is_operator) {
            continue;
        }
        let opens_body = starts.last().is_some_and(|&(idx, kw, _)| {
            idx + 1 == i
                && keywords
                    .iter()
                    .any(|(k, s)| *k == kw && matches!(s, Shape::Expr | Shape::Select))
        });
        if opens_body {
            continue;
        }
        let allowed = match shape {
            Shape::Expr => true,
            Shape::Str => next.is_some_and(|t| t.text.starts_with(['\'', '"'])),
            Shape::Select => next.is_some_and(|t| t.is("SELECT")),
            Shape::Flag => next.is_none_or(|t| t.is("ON") || t.text == ";" || lookup(t).is_some()),
        };
        if allowed {
            starts.push((i, keyword, token.end));
        }
    }
    starts
        .iter()
        .enumerate()
        .map(|(n, &(_, keyword, body_start))| {
            let body_end = starts
                .get(n + 1)
                .and_then(|&(idx, _, _)| toks.get(idx))
                .map_or(text.len(), |t| t.start);
            let body = text
                .get(body_start..body_end)
                .unwrap_or("")
                .trim()
                .trim_end_matches(';')
                .trim_end();
            Clause { keyword, body }
        })
        .collect()
}

/// The body of the first clause named `keyword`.
pub(super) fn clause<'a>(found: &[Clause<'a>], keyword: &str) -> Option<&'a str> {
    found.iter().find(|c| c.keyword == keyword).map(|c| c.body)
}

/// Where a `DEFINE <kind> <name> [ON [TABLE] <table>]` head ends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Head<'a> {
    /// The unquoted definition name.
    pub name: String,
    /// The unquoted table name, for kinds defined `ON` a table.
    pub table: Option<String>,
    /// Everything after the head.
    pub rest: &'a str,
}

/// Read the `DEFINE <kind> [OVERWRITE | IF NOT EXISTS] <name> [ON [TABLE]
/// <table>]` head of a statement, so clause scanning starts after the
/// name. A field called `default` or `reference` must not read as a clause.
///
/// `kind` is the word after `DEFINE` (`FIELD`, `INDEX`, ...) and `on_table`
/// says whether the kind names a table after `ON`. `None` when the text
/// does not start that way.
pub(super) fn define_head<'a>(text: &'a str, kind: &str, on_table: bool) -> Option<Head<'a>> {
    let toks = tokens(text);
    let mut at = 0;
    let expect = |word: &str, at: &mut usize| -> Option<()> {
        toks.get(*at).filter(|t| t.is(word))?;
        *at += 1;
        Some(())
    };
    expect("DEFINE", &mut at)?;
    expect(kind, &mut at)?;
    let word = |i: usize| toks.get(i);
    let guarded_name =
        |i: usize| word(i + 1).is_some() && (!on_table || word(i + 1).is_some_and(|t| !t.is("ON")));
    if word(at).is_some_and(|t| t.is("OVERWRITE")) && guarded_name(at) {
        at += 1;
    } else if word(at).is_some_and(|t| t.is("IF"))
        && word(at + 1).is_some_and(|t| t.is("NOT"))
        && word(at + 2).is_some_and(|t| t.is("EXISTS"))
        && guarded_name(at + 2)
    {
        at += 3;
    }
    let name = word(at)?;
    at += 1;
    let mut end = name.end;
    let mut table = None;
    if on_table {
        expect("ON", &mut at)?;
        if word(at).is_some_and(|t| t.is("TABLE")) && word(at + 1).is_some() {
            at += 1;
        }
        let target = word(at)?;
        table = Some(unquote_ident(target.text.trim_end_matches(';')));
        end = target.end;
    }
    Some(Head {
        name: unquote_ident(name.text.trim_end_matches(';')),
        table,
        rest: text.get(end..).unwrap_or(""),
    })
}

/// A leading string literal, unquoted and unescaped.
pub(super) fn string_literal(text: &str) -> Option<String> {
    let token = tokens(text).into_iter().next()?;
    crate::types::escape::unquote_str(token.text.trim_end_matches([';', ',']))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keywords_inside_quotes_brackets_and_names_are_skipped() {
        let text = "DEFAULT 'no comment' ASSERT $value INSIDE (SELECT VALUE x) COMMENT 'c'";
        assert_eq!(find_keyword(text, "COMMENT"), text.rfind("COMMENT"));
        assert_eq!(find_keyword(text, "VALUE"), None);
        assert_eq!(find_keyword("a `VALUE` b", "VALUE"), None);
        assert_eq!(find_keyword("a ⟨VALUE⟩ b", "VALUE"), None);
        assert_eq!(find_keyword("fn::value", "VALUE"), None);
        assert_eq!(find_keyword("x.value", "VALUE"), None);
        assert_eq!(find_keyword("blog_filters", "FILTERS"), None);
    }

    #[test]
    fn escaped_quotes_do_not_end_a_run() {
        let text = r#"COMMENT "a \" PERMISSIONS" PERMISSIONS FULL"#;
        assert_eq!(find_keyword(text, "PERMISSIONS"), text.rfind("PERMISSIONS"));
        let text = r"COMMENT 'it\'s PERMISSIONS' PERMISSIONS FULL";
        assert_eq!(find_keyword(text, "PERMISSIONS"), text.rfind("PERMISSIONS"));
    }

    #[test]
    fn non_ascii_text_never_panics() {
        assert_eq!(find_keyword("ŉŉŉ FILTERS x", "FILTERS"), Some(7));
        assert_eq!(tokens("ŉ ŉ").len(), 2);
        assert_eq!(split_top_level("ŉ,ŉ", ','), vec!["ŉ", "ŉ"]);
    }

    #[test]
    fn tokens_keep_groups_whole() {
        let toks: Vec<&str> = tokens("SIGNUP (CREATE user SET a = 1) WITH 'x y'")
            .iter()
            .map(|t| t.text)
            .collect();
        assert_eq!(toks, ["SIGNUP", "(CREATE user SET a = 1)", "WITH", "'x y'"]);
    }

    #[test]
    fn matching_close_skips_quoted_brackets() {
        let text = "{ RETURN string::replace($x,'}','') } COMMENT 'x'";
        let close = matching_close(text, 0).unwrap();
        assert_eq!(&text[..=close], "{ RETURN string::replace($x,'}','') }");
        assert_eq!(matching_close("{ '{' }", 0), Some(6));
        assert_eq!(matching_close("{ unclosed", 0), None);
        assert_eq!(matching_close("x", 0), None);
    }

    #[test]
    fn strip_group_needs_the_whole_text() {
        assert_eq!(strip_group(" { a; b } ", '{'), Some("a; b"));
        assert_eq!(strip_group("{ a } + { b }", '{'), None);
        assert_eq!(strip_group("a", '{'), None);
    }

    #[test]
    fn clauses_read_expressions_that_spell_keywords() {
        const KW: &[(&str, Shape)] = &[
            ("TYPE", Shape::Expr),
            ("DEFAULT", Shape::Expr),
            ("VALUE", Shape::Expr),
            ("ASSERT", Shape::Expr),
            ("READONLY", Shape::Flag),
            ("REFERENCE", Shape::Flag),
            ("COMMENT", Shape::Str),
            ("PERMISSIONS", Shape::Expr),
        ];
        let found = clauses(
            " TYPE bool ASSERT readonly = true VALUE default + 1 READONLY \
             REFERENCE COMMENT 'x' PERMISSIONS FULL",
            KW,
        );
        let got: Vec<(&str, &str)> = found.iter().map(|c| (c.keyword, c.body)).collect();
        assert_eq!(
            got,
            [
                ("TYPE", "bool"),
                ("ASSERT", "readonly = true"),
                ("VALUE", "default + 1"),
                ("READONLY", ""),
                ("REFERENCE", ""),
                ("COMMENT", "'x'"),
                ("PERMISSIONS", "FULL"),
            ]
        );
        let found = clauses(" ASSERT comment != '' COMMENT \"c\"", KW);
        assert_eq!(clause(&found, "ASSERT"), Some("comment != ''"));
        assert_eq!(clause(&found, "COMMENT"), Some("\"c\""));
    }

    #[test]
    fn define_head_skips_keyword_named_definitions() {
        let head = define_head("DEFINE FIELD default ON card TYPE bool", "FIELD", true).unwrap();
        assert_eq!(head.name, "default");
        assert_eq!(head.table.as_deref(), Some("card"));
        assert_eq!(head.rest, " TYPE bool");
        let head = define_head(
            "DEFINE FIELD OVERWRITE `value` ON TABLE `select` VALUE 1",
            "FIELD",
            true,
        )
        .unwrap();
        assert_eq!(head.name, "value");
        assert_eq!(head.table.as_deref(), Some("select"));
        assert_eq!(head.rest, " VALUE 1");
        let head = define_head("DEFINE FIELD overwrite ON t", "FIELD", true).unwrap();
        assert_eq!(head.name, "overwrite");
        let head = define_head(
            "DEFINE ANALYZER blog_filters TOKENIZERS blank",
            "ANALYZER",
            false,
        )
        .unwrap();
        assert_eq!(head.name, "blog_filters");
        assert_eq!(head.rest, " TOKENIZERS blank");
        assert!(define_head("DEFINE INDEX i", "FIELD", true).is_none());
    }

    #[test]
    fn string_literals_are_unescaped() {
        assert_eq!(
            string_literal(r"'a\nb' PERMISSIONS").as_deref(),
            Some("a\nb")
        );
        assert_eq!(string_literal(r#""it's""#).as_deref(), Some("it's"));
        assert_eq!(string_literal("bare"), None);
    }

    #[test]
    fn unquote_ident_handles_backticks() {
        assert_eq!(unquote_ident("`a\\`b`"), "a`b");
        assert_eq!(unquote_ident("plain"), "plain");
    }
}
