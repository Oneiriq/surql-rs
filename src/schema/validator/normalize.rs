//! Expression normalisation shared by every comparison in the validator.

/// Normalize a SurrealQL expression for comparison purposes.
///
/// Delegates to [`crate::migration::diff::normalize_expression`], so schema
/// validation and migration diffing agree on what counts as the same
/// expression (whitespace runs, one level of wrapping parentheses, `IS NONE`
/// and cast spacing as the engine echoes them). On top of that a
/// double-quoted string literal is rewritten single-quoted, the form the
/// engine prints it in. Returns `None` for `None` inputs or expressions
/// that become empty after normalization.
pub fn normalize_expression(expr: Option<&str>) -> Option<String> {
    let normalized = canonical_expr(expr?);
    (!normalized.is_empty()).then_some(normalized)
}

pub(super) fn canonical_expr(expr: &str) -> String {
    fold_string_quotes(&crate::migration::diff::normalize_expression(expr))
}

pub(super) fn expr_eq(code: Option<&str>, db: Option<&str>) -> bool {
    normalize_expression(code) == normalize_expression(db)
}

/// Rewrite every `"..."` string literal as `'...'` when its text holds no
/// quote or escape, the spelling the engine echoes. Single-quoted literals
/// are copied through untouched.
fn fold_string_quotes(expr: &str) -> String {
    let mut out = String::with_capacity(expr.len());
    let mut chars = expr.chars();
    while let Some(c) = chars.next() {
        if c != '\'' && c != '"' {
            out.push(c);
            continue;
        }
        let mut body = String::new();
        let mut escaped = false;
        let mut closed = false;
        for inner in chars.by_ref() {
            if !escaped && inner == c {
                closed = true;
                break;
            }
            escaped = !escaped && inner == '\\';
            body.push(inner);
        }
        let quote = if c == '"' && closed && !body.contains(['\'', '\\']) {
            '\''
        } else {
            c
        };
        out.push(quote);
        out.push_str(&body);
        if closed {
            out.push(quote);
        }
    }
    out
}

/// `true` when `text` is `open ... close` with the opening delimiter
/// matched by the final character (quoted text is skipped).
fn wraps_whole(text: &str, open: char, close: char) -> bool {
    if !text.starts_with(open) || !text.ends_with(close) {
        return false;
    }
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    let mut escaped = false;
    let last = text.chars().count().saturating_sub(1);
    for (i, c) in text.chars().enumerate() {
        if let Some(q) = quote {
            if !escaped && c == q {
                quote = None;
            }
            escaped = !escaped && c == '\\';
            continue;
        }
        match c {
            '\'' | '"' => quote = Some(c),
            _ if c == open => depth += 1,
            _ if c == close => {
                depth = depth.saturating_sub(1);
                if depth == 0 && i != last {
                    return false;
                }
            }
            _ => {}
        }
    }
    depth == 0
}

/// Canonical form of an event `WHEN` / `THEN` body. On top of
/// [`normalize_expression`], the wrapping the engine adds or drops on echo is
/// peeled: a `{ ... }` block, one level of parentheses, and trailing `;`.
pub(super) fn canonical_event_body(body: &str) -> String {
    let mut current = canonical_expr(body);
    // Each round strictly shortens the text, so this terminates; the bound
    // only caps pathological nesting.
    for _ in 0..16 {
        let trimmed = current.trim().trim_end_matches(';').trim_end();
        let peeled = if wraps_whole(trimmed, '{', '}') || wraps_whole(trimmed, '(', ')') {
            trimmed
                .get(1..trimmed.len().saturating_sub(1))
                .unwrap_or(trimmed)
        } else {
            trimmed
        };
        let next = canonical_expr(peeled);
        if next == current {
            break;
        }
        current = next;
    }
    current
}
