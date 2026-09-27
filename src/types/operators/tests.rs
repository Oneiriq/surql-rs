use super::*;
use serde_json::json;

#[test]
fn eq_renders() {
    assert_eq!(eq("name", "Alice").to_surql(), "name = 'Alice'");
}

#[test]
fn ne_renders() {
    assert_eq!(ne("status", "deleted").to_surql(), "status != 'deleted'");
}

#[test]
fn gt_renders_integer() {
    assert_eq!(gt("age", 18).to_surql(), "age > 18");
}

#[test]
fn lt_renders_float() {
    assert_eq!(lt("price", 50.0).to_surql(), "price < 50.0");
}

#[test]
fn gte_and_lte() {
    assert_eq!(gte("score", 100).to_surql(), "score >= 100");
    assert_eq!(lte("quantity", 10).to_surql(), "quantity <= 10");
}

#[test]
fn contains_renders() {
    assert_eq!(
        contains("email", "@example.com").to_surql(),
        "email CONTAINS '@example.com'"
    );
}

#[test]
fn contains_not_renders() {
    assert_eq!(
        contains_not("tags", "spam").to_surql(),
        "tags CONTAINSNOT 'spam'"
    );
}

#[test]
fn contains_all_renders() {
    let op = contains_all("tags", [json!("python"), json!("database")]);
    assert_eq!(op.to_surql(), "tags CONTAINSALL ['python', 'database']");
}

#[test]
fn contains_any_renders() {
    let op = contains_any("tags", [json!("python"), json!("javascript")]);
    assert_eq!(op.to_surql(), "tags CONTAINSANY ['python', 'javascript']");
}

#[test]
fn inside_renders() {
    let op = inside("status", [json!("active"), json!("pending")]);
    assert_eq!(op.to_surql(), "status INSIDE ['active', 'pending']");
}

#[test]
fn not_inside_renders() {
    let op = not_inside("status", [json!("deleted"), json!("archived")]);
    assert_eq!(op.to_surql(), "status NOTINSIDE ['deleted', 'archived']");
}

#[test]
fn is_null_and_not_null() {
    assert_eq!(is_null("deleted_at").to_surql(), "deleted_at IS NULL");
    assert_eq!(
        is_not_null("created_at").to_surql(),
        "created_at IS NOT NULL"
    );
}

#[test]
fn is_none_and_not_none() {
    // An absent (unset) field is NONE, not NULL -- the correct guard for an
    // optional field that was never written.
    assert_eq!(is_none("deleted_at").to_surql(), "deleted_at IS NONE");
    assert_eq!(
        is_not_none("consolidated_expert").to_surql(),
        "consolidated_expert IS NOT NONE"
    );
}

#[test]
fn and_renders() {
    let op = and_(gt("age", 18), eq("status", "active"));
    assert_eq!(op.to_surql(), "(age > 18) AND (status = 'active')");
}

#[test]
fn or_renders() {
    let op = or_(eq("type", "admin"), eq("type", "moderator"));
    assert_eq!(op.to_surql(), "(type = 'admin') OR (type = 'moderator')");
}

#[test]
fn not_renders() {
    let op = not_(eq("status", "deleted"));
    assert_eq!(op.to_surql(), "NOT (status = 'deleted')");
}

#[test]
fn null_quoted_as_keyword() {
    assert_eq!(
        eq("deleted_at", Value::Null).to_surql(),
        "deleted_at = NULL"
    );
}

#[test]
fn bool_quoted_lowercase() {
    assert_eq!(eq("active", true).to_surql(), "active = true");
    assert_eq!(eq("active", false).to_surql(), "active = false");
}

#[test]
fn string_escapes_single_quote() {
    assert_eq!(eq("name", "O'Brien").to_surql(), "name = 'O\\'Brien'");
}

#[test]
fn string_escapes_backslash() {
    assert_eq!(eq("path", "a\\b").to_surql(), "path = 'a\\\\b'");
}

fn nested(depth: usize) -> Value {
    (0..depth).fold(json!("it's \\ x"), |inner, _| json!({ "a": [inner] }))
}

/// The parser refuses a literal nested 20 levels deep, so a value past
/// the inline limit travels as JSON text the engine decodes.
#[test]
fn deeply_nested_values_are_decoded_by_the_engine() {
    // Each `nested` level is an object and an array.
    let at_limit = nested(MAX_INLINE_DEPTH / 2);
    let inline = quote_value(&at_limit);
    assert!(inline.starts_with("{ a: [{ a: ["), "{inline}");
    assert!(!inline.contains("encoding::json::decode"));

    let deeper = nested(MAX_INLINE_DEPTH / 2 + 1);
    let decoded = quote_value(&deeper);
    let json = serde_json::to_string(&deeper).unwrap();
    assert_eq!(
        decoded,
        format!("encoding::json::decode({})", quote_str(&json))
    );
    // One string literal: the only nesting the parser sees.
    assert_eq!(
        crate::types::escape::unquote_str(
            decoded
                .strip_prefix("encoding::json::decode(")
                .and_then(|rest| rest.strip_suffix(')'))
                .unwrap()
        )
        .as_deref(),
        Some(json.as_str())
    );

    // A shallow neighbour of a deep value is rendered with it.
    let pair = json!({ "shallow": 1, "deep": deeper });
    assert!(quote_value(&pair).starts_with("encoding::json::decode("));
    assert_eq!(quote_value(&json!({ "shallow": 1 })), "{ shallow: 1 }");
}

#[test]
fn integers_beyond_i64_render_as_exact_decimals() {
    assert_eq!(quote_value(&json!(u64::MAX)), "18446744073709551615dec");
    assert_eq!(quote_value(&json!(i64::MAX)), "9223372036854775807");
    assert_eq!(quote_value(&json!(-5)), "-5");
}

#[test]
fn expression_shaped_data_renders_as_an_object_literal() {
    let hostile = json!({"body": {"expression": "1}; DELETE user; --"}});
    assert_eq!(
        quote_value(&hostile),
        "{ body: { expression: '1}; DELETE user; --' } }"
    );
    let harmless = json!({"expression": "1+1", "result": 2});
    assert_eq!(quote_value(&harmless), "{ expression: '1+1', result: 2 }");
}

#[test]
fn record_ref_shaped_data_renders_as_an_object_literal() {
    let shaped = json!({"table": "user", "record_id": "alice"});
    assert_eq!(
        quote_value(&shaped),
        "{ record_id: 'alice', table: 'user' }"
    );
}

#[test]
fn object_keys_are_quoted_when_not_identifiers() {
    assert_eq!(quote_value(&json!({"": 1})), "{ '': 1 }");
    assert_eq!(
        quote_value(&json!({"x: 1}; DELETE user; --": 1})),
        "{ 'x: 1}; DELETE user; --': 1 }"
    );
    assert_eq!(quote_value(&json!({"1a": 1})), "{ '1a': 1 }");
}

#[test]
fn record_ref_escapes_the_table() {
    assert_eq!(
        record_ref("user', 'x'); DELETE user; --", "a").to_surql(),
        r"type::record('user\', \'x\'); DELETE user; --', 'a')"
    );
    assert_eq!(
        type_record("a'b", "c").to_surql(),
        r"type::record('a\'b', 'c')"
    );
}

#[test]
fn surrealfn_renders_raw_only_through_the_typed_channel() {
    let now = super::super::surreal_fn::surql_fn("time::now", &[]);
    // Serialised into JSON it is data like any other object ...
    let as_json = serde_json::to_value(&now).unwrap();
    assert_eq!(
        eq("created_at", as_json).to_surql(),
        "created_at = { expression: 'time::now()' }"
    );
    // ... and only the typed comparison renders the call.
    assert_eq!(
        lt_expr("created_at", now).to_surql(),
        "created_at < time::now()"
    );
}

#[test]
fn record_ref_renders_raw_only_through_the_typed_channel() {
    let rr = record_ref("user", "alice");
    let as_json = serde_json::to_value(&rr).unwrap();
    assert_eq!(
        eq("author", as_json).to_surql(),
        "author = { record_id: 'alice', table: 'user' }"
    );
    assert_eq!(
        eq_expr("author", rr).to_surql(),
        "author = type::record('user', 'alice')"
    );
}

#[test]
fn expr_comparisons_compose_with_logical_operators() {
    use crate::query::expressions::{field, time_now};

    let op = and_(ne_expr("owner", field("author")), gte_expr("n", 1));
    assert_eq!(op.to_surql(), "(owner != author) AND (n >= 1)");
    assert_eq!(gt_expr("t", time_now()).to_surql(), "t > time::now()");
    assert_eq!(lte_expr("t", time_now()).to_surql(), "t <= time::now()");
    assert_eq!(Operator::from(Expression::raw("a = b")).to_surql(), "a = b");
}

#[test]
fn type_record_string_id_renders() {
    assert_eq!(
        type_record("task", "abc-123").to_surql(),
        "type::record('task', 'abc-123')"
    );
}

#[test]
fn type_record_int_id_renders() {
    assert_eq!(
        type_record("post", 42_i64).to_surql(),
        "type::record('post', 42)"
    );
}

#[test]
fn type_record_escapes_single_quote() {
    assert_eq!(
        type_record("user", "o'brien").to_surql(),
        "type::record('user', 'o\\'brien')"
    );
}

#[test]
fn type_record_is_function_expression() {
    let expr = type_record("task", "abc");
    assert_eq!(
        expr.kind,
        crate::query::expressions::ExpressionKind::Function
    );
}

#[test]
fn type_thing_renders_the_v3_function() {
    // SurrealDB 3 rejects `type::thing(...)` at parse time.
    assert_eq!(
        type_thing("user", "alice").to_surql(),
        "type::record('user', 'alice')"
    );
    assert_eq!(
        type_thing("post", 123_i64).to_surql(),
        "type::record('post', 123)"
    );
}

#[test]
fn type_thing_escapes_backslash() {
    assert_eq!(
        type_thing("path", "a\\b").to_surql(),
        "type::record('path', 'a\\\\b')"
    );
}

#[test]
fn type_thing_is_function_expression() {
    let expr = type_thing("user", "alice");
    assert_eq!(
        expr.kind,
        crate::query::expressions::ExpressionKind::Function
    );
}
