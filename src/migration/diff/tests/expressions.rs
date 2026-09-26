use super::*;

// ----- normalize_expression -----

#[test]
fn normalize_expression_collapses_runs_of_whitespace() {
    assert_eq!(normalize_expression("a   b\tc\n d"), "a b c d");
}

#[test]
fn normalize_expression_trims_edges() {
    assert_eq!(normalize_expression("  hello world  "), "hello world");
}

#[test]
fn normalize_expression_empty_is_empty() {
    assert_eq!(normalize_expression("   "), "");
}

/// Whitespace inside a string literal is content, not formatting.
#[test]
fn normalize_expression_keeps_whitespace_inside_literals() {
    assert_eq!(normalize_expression("'a  b'"), "'a  b'");
    assert!(!expr_eq(Some("'a  b'"), Some("'a b'")));
    // A raw tab and its escape are the same character; both come out in
    // the escaped spelling the engine prints.
    assert_eq!(
        normalize_expression("  $value  =  'x\t\ty'  "),
        r"$value = 'x\t\ty'"
    );
    assert!(expr_eq(Some("'x\ty'"), Some(r"'x\ty'")));
    assert_eq!(normalize_expression("`my  field` = 1"), "`my  field` = 1");
    assert_eq!(normalize_expression("r:⟨a  b⟩"), "r:⟨a  b⟩");
}

/// The engine echoes every string literal single-quoted unless it
/// holds a `'`; the quote style is not a difference.
#[test]
fn normalize_expression_ignores_the_quote_style() {
    assert!(expr_eq(Some("\"hello  there\""), Some("'hello  there'")));
    assert!(expr_eq(
        Some(r"$value != 'it\'s'"),
        Some("$value != \"it's\"")
    ));
    assert!(!expr_eq(Some("\"a\""), Some("'b'")));
}

/// The echo folds only apply to code: a literal that happens to spell
/// `IS NONE`, a cast, or a parenthesis is left alone.
#[test]
fn normalize_expression_folds_nothing_inside_literals() {
    assert_eq!(
        normalize_expression("$value = ' IS NONE'"),
        "$value = ' IS NONE'"
    );
    assert_eq!(normalize_expression("'<string> x'"), "'<string> x'");
    assert_eq!(normalize_expression("(a = ')')"), "a = ')'");
    assert_eq!(normalize_expression("('(' + x"), "('(' + x");
}

#[test]
fn normalize_expression_folds_the_engine_echo() {
    assert_eq!(normalize_expression("($value IS NONE)"), "$value = NONE");
    assert_eq!(normalize_expression("$value IS NOT NONE"), "$value != NONE");
    assert_eq!(normalize_expression("<string> id"), "<string>id");
    assert_eq!(normalize_expression("a > b"), "a > b");
    assert_eq!(normalize_expression("(a) + (b)"), "(a) + (b)");
}

/// An unterminated literal and non-ASCII text survive intact.
#[test]
fn normalize_expression_handles_ragged_input() {
    assert_eq!(normalize_expression("'abc  "), "'abc  ");
    assert_eq!(normalize_expression(r"'a\'"), r"'a\'");
    assert_eq!(normalize_expression("é  <ü> ö"), "é <ü> ö");
    assert_eq!(normalize_expression("<int> 'ü  x'"), "<int>'ü  x'");
    assert_eq!(normalize_expression("(ä)"), "ä");
}

// ----- validate_event_expression -----

#[test]
fn validate_event_expression_allows_safe() {
    assert!(validate_event_expression("$event = \"CREATE\"", "condition").is_ok());
    assert!(validate_event_expression("$before.a != $after.a", "condition").is_ok());
    assert!(validate_event_expression("true", "condition").is_ok());
    assert!(validate_event_expression("CREATE log SET u = 1", "action").is_ok());
}

#[test]
fn validate_event_expression_rejects_statement_separator() {
    assert!(validate_event_expression("a; DROP b", "condition").is_err());
}

#[test]
fn validate_event_expression_rejects_trailing_semicolon() {
    assert!(validate_event_expression("a;", "condition").is_err());
}

#[test]
fn validate_event_expression_rejects_comment() {
    assert!(validate_event_expression("a -- b", "condition").is_err());
}

#[test]
fn validate_event_expression_rejects_semicolon_comment() {
    assert!(validate_event_expression("a;--b", "condition").is_err());
}

// ----- validate_default_value -----

#[test]
fn validate_default_value_accepts_literals() {
    assert!(validate_default_value("42").is_ok());
    assert!(validate_default_value("-1").is_ok());
    assert!(validate_default_value("3.14").is_ok());
    assert!(validate_default_value("true").is_ok());
    assert!(validate_default_value("false").is_ok());
    assert!(validate_default_value("NONE").is_ok());
    assert!(validate_default_value("NULL").is_ok());
    assert!(validate_default_value("'hello'").is_ok());
    assert!(validate_default_value("time::now()").is_ok());
    assert!(validate_default_value("$auth").is_ok());
}

#[test]
fn the_safe_default_pattern_compiles() {
    assert!(safe_default_regex().is_some());
}

#[test]
fn validate_default_value_rejects_unsafe() {
    assert!(validate_default_value("a; DROP TABLE u").is_err());
    assert!(validate_default_value("SELECT * FROM u").is_err());
}

#[test]
fn normalize_expression_strips_every_layer_of_wrapping_parentheses() {
    // Found by the `migration` fuzz target: one layer per call left
    // `(())` at `()`, so normalising twice changed the result.
    assert_eq!(normalize_expression("((a = 1))"), "a = 1");
    for expr in ["(())", "((($x IS NONE)))", "(a) AND (b)", "('(')"] {
        let once = normalize_expression(expr);
        assert_eq!(normalize_expression(&once), once, "{expr}");
    }
    assert_eq!(normalize_expression("(a) AND (b)"), "(a) AND (b)");
}
