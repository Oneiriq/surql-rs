//! Expression normalisation: what the engine may reformat when it echoes an
//! expression back, folded away so code and echo compare equal.

use crate::types::escape::{quote_str, unquote_str};

/// Normalise an expression for semantic equality comparison.
///
/// A database server reformats expressions when it echoes them back, so the
/// comparison folds what the engine is free to change: runs of whitespace
/// become one space and the ends are trimmed, wrapping parentheses go (at
/// any depth), `IS NONE` / `IS NOT NONE` read as `= NONE` /
/// `!= NONE`, a cast loses the space after it (`<string> id`), and a string
/// literal takes one quote style (the engine prints `"it's"` for what code
/// wrote as `'it\'s'`).
///
/// None of that reaches inside a quoted token. String literals, backtick
/// identifiers, and `⟨…⟩` record keys keep every byte, so `'a  b'` and
/// `'a b'` stay different.
///
/// ## Examples
///
/// ```
/// use surql::migration::diff::normalize_expression;
///
/// assert_eq!(normalize_expression("($value  IS NONE)"), "$value = NONE");
/// assert_eq!(normalize_expression("\"a  b\""), "'a  b'");
/// ```
#[must_use]
pub fn normalize_expression(expr: &str) -> String {
    let mut pieces: Vec<Piece> = split_quoted(expr)
        .into_iter()
        .map(|piece| match piece {
            Piece::Code(code) => Piece::Code(collapse_whitespace(&code)),
            Piece::Quoted(quoted) => Piece::Quoted(canonical_literal(quoted)),
        })
        .collect();
    trim_code_ends(&mut pieces);
    // Every layer, not one: normalising must be idempotent, or code and
    // echo nested to different depths never compare equal.
    while strip_wrapping_parens(&mut pieces) {
        trim_code_ends(&mut pieces);
    }
    pieces
        .into_iter()
        .map(|piece| match piece {
            Piece::Code(code) => fold_cast_spacing(&fold_none_checks(&code)),
            Piece::Quoted(quoted) => quoted.text,
        })
        .collect()
}

/// One run of expression text.
enum Piece {
    /// SurrealQL outside any quotes, where formatting is free.
    Code(String),
    /// A quoted token, delimiters included, whose bytes are content.
    Quoted(Quoted),
}

/// A string literal, backtick identifier, or `⟨…⟩` record key.
struct Quoted {
    text: String,
    /// Whether the closing delimiter was found before the input ran out.
    terminated: bool,
}

/// Split `expr` into alternating code and quoted runs. Inside quotes a
/// backslash escapes the next character; an unterminated quote runs to the
/// end of the input.
fn split_quoted(expr: &str) -> Vec<Piece> {
    let mut pieces = Vec::new();
    let mut code = String::new();
    let mut chars = expr.chars();
    while let Some(ch) = chars.next() {
        let close = match ch {
            '\'' | '"' | '`' => ch,
            '⟨' => '⟩',
            _ => {
                code.push(ch);
                continue;
            }
        };
        if !code.is_empty() {
            pieces.push(Piece::Code(std::mem::take(&mut code)));
        }
        let mut text = String::from(ch);
        let mut terminated = false;
        let mut escaped = false;
        for inner in chars.by_ref() {
            text.push(inner);
            if escaped {
                escaped = false;
            } else if inner == '\\' {
                escaped = true;
            } else if inner == close {
                terminated = true;
                break;
            }
        }
        pieces.push(Piece::Quoted(Quoted { text, terminated }));
    }
    if !code.is_empty() {
        pieces.push(Piece::Code(code));
    }
    pieces
}

/// Collapse every run of whitespace to a single space.
fn collapse_whitespace(code: &str) -> String {
    let mut out = String::with_capacity(code.len());
    let mut in_space = false;
    for ch in code.chars() {
        if ch.is_whitespace() {
            if !in_space {
                out.push(' ');
            }
            in_space = true;
        } else {
            out.push(ch);
            in_space = false;
        }
    }
    out
}

/// Re-quote a complete string literal in the one style [`quote_str`]
/// renders; backtick identifiers, record keys, and unterminated text stay
/// as written.
fn canonical_literal(quoted: Quoted) -> Quoted {
    if !quoted.terminated {
        return quoted;
    }
    match unquote_str(&quoted.text) {
        Some(value) => Quoted {
            text: quote_str(&value),
            terminated: true,
        },
        None => quoted,
    }
}

/// Trim leading whitespace off the first run and trailing whitespace off
/// the last, when those runs are code.
fn trim_code_ends(pieces: &mut [Piece]) {
    if let Some(Piece::Code(first)) = pieces.first_mut() {
        *first = first.trim_start().to_owned();
    }
    if let Some(Piece::Code(last)) = pieces.last_mut() {
        *last = last.trim_end().to_owned();
    }
}

/// Drop one pair of parentheses that wraps the whole expression, returning
/// whether it did. Parentheses inside quotes do not count towards the
/// balance.
fn strip_wrapping_parens(pieces: &mut [Piece]) -> bool {
    let opens = matches!(pieces.first(), Some(Piece::Code(c)) if c.starts_with('('));
    let closes = matches!(pieces.last(), Some(Piece::Code(c)) if c.ends_with(')'));
    if !opens || !closes {
        return false;
    }
    let code_chars: usize = pieces
        .iter()
        .map(|piece| match piece {
            Piece::Code(code) => code.chars().count(),
            Piece::Quoted(_) => 0,
        })
        .sum();
    // The opening parenthesis wraps the whole expression when its depth
    // first returns to zero on the very last code character.
    let mut depth = 0usize;
    let mut seen = 0usize;
    for piece in pieces.iter() {
        let Piece::Code(code) = piece else { continue };
        for ch in code.chars() {
            seen += 1;
            match ch {
                '(' => depth += 1,
                ')' => depth = depth.saturating_sub(1),
                _ => {}
            }
            if depth == 0 && seen < code_chars {
                return false;
            }
        }
    }
    if let Some(Piece::Code(first)) = pieces.first_mut() {
        if let Some(rest) = first.strip_prefix('(') {
            *first = rest.to_owned();
        }
    }
    if let Some(Piece::Code(last)) = pieces.last_mut() {
        if let Some(rest) = last.strip_suffix(')') {
            *last = rest.to_owned();
        }
    }
    true
}

/// The engine echoes `IS NONE` as `= NONE` and `IS NOT NONE` as `!= NONE`.
fn fold_none_checks(code: &str) -> String {
    code.replace(" IS NOT NONE", " != NONE")
        .replace(" is not none", " != NONE")
        .replace(" IS NONE", " = NONE")
        .replace(" is none", " = NONE")
}

/// `<string> id` and `<string>id` are the same cast. Comparison spacing
/// (`a > b`) is kept by requiring the `<` side to look like a cast: a
/// non-empty run of ASCII letters and digits.
fn fold_cast_spacing(code: &str) -> String {
    let mut out = String::with_capacity(code.len());
    // Whether the text since the last `<` still looks like a cast name, and
    // how long it is; `None` outside any `<...>`.
    let mut cast: Option<(bool, usize)> = None;
    let mut chars = code.chars().peekable();
    while let Some(ch) = chars.next() {
        out.push(ch);
        match ch {
            '<' => cast = Some((true, 0)),
            '>' => {
                if matches!(cast, Some((true, len)) if len > 0) && chars.peek() == Some(&' ') {
                    chars.next();
                }
                cast = None;
            }
            other => {
                if let Some((looks_like_cast, len)) = cast.as_mut() {
                    *looks_like_cast = *looks_like_cast && other.is_ascii_alphanumeric();
                    *len += 1;
                }
            }
        }
    }
    out
}

/// Whether two optional expressions are the same once normalised with
/// [`normalize_expression`]. Two absent expressions are equal; an absent and
/// a present one are not.
///
/// ## Examples
///
/// ```
/// use surql::migration::diff::expr_eq;
///
/// assert!(expr_eq(Some("(a  >  1)"), Some("a > 1")));
/// assert!(expr_eq(None, None));
/// assert!(!expr_eq(Some("a > 1"), None));
/// ```
#[must_use]
pub fn expr_eq(a: Option<&str>, b: Option<&str>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => normalize_expression(x) == normalize_expression(y),
        _ => false,
    }
}
