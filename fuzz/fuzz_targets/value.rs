//! A value rendered as a SurrealQL literal must be inert and faithful.
//!
//! Whatever JSON a caller passes as data, the rendered text must parse as a
//! single statement when returned, and back to the same JSON. An object
//! shaped like a raw-expression wrapper, or a string or key carrying quotes,
//! must not turn into executable SurrealQL.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Arbitrary};
use libfuzzer_sys::fuzz_target;
use serde_json::{Map, Number, Value};
use surql::types::operators::quote_value_public;
use surrealdb_core::syn;

#[derive(Arbitrary, Debug)]
enum Node {
    Null,
    Bool(bool),
    Int(i64),
    Big(u64),
    Float(f64),
    Str(String),
    Arr(Vec<Node>),
    Obj(Vec<(String, Node)>),
}

fn to_json(node: &Node, depth: usize) -> Value {
    if depth > 24 {
        return Value::Null;
    }
    match node {
        Node::Null => Value::Null,
        Node::Bool(b) => Value::Bool(*b),
        Node::Int(n) => Value::from(*n),
        Node::Big(n) => Value::from(*n),
        Node::Float(f) => Number::from_f64(*f).map_or(Value::Null, Value::Number),
        Node::Str(s) => Value::String(s.clone()),
        Node::Arr(items) => Value::Array(items.iter().map(|n| to_json(n, depth + 1)).collect()),
        Node::Obj(entries) => Value::Object(
            entries
                .iter()
                .map(|(k, n)| (k.clone(), to_json(n, depth + 1)))
                .collect::<Map<String, Value>>(),
        ),
    }
}

/// Structural equality with numbers compared by value, since the engine may
/// read a float literal back through a different numeric kind, and an
/// integer above `i64::MAX` comes back as a decimal (JSON text).
fn same(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::String(y)) if x.is_u64() && !x.is_i64() => x.to_string() == *y,
        (Value::Number(x), Value::Number(y)) => match (x.as_i64(), y.as_i64()) {
            (Some(x), Some(y)) => x == y,
            _ => match (x.as_f64(), y.as_f64()) {
                (Some(x), Some(y)) => {
                    x == y || (x - y).abs() <= f64::EPSILON * x.abs().max(y.abs())
                }
                _ => false,
            },
        },
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(x, y)| same(x, y))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| same(v, w)))
        }
        _ => a == b,
    }
}

fuzz_target!(|node: Node| {
    let value = to_json(&node, 0);
    let text = quote_value_public(&value);

    let statement = format!("RETURN {text};");
    let ast =
        syn::parse(&statement).unwrap_or_else(|e| panic!("engine rejected {statement:?}: {e}"));
    assert_eq!(ast.num_statements(), 1, "{statement:?}");

    let parsed = syn::value(&text).unwrap_or_else(|e| panic!("engine rejected {text:?}: {e}"));
    let back = parsed.into_json_value();
    assert!(
        same(&value, &back),
        "{value} rendered as {text} read back as {back}"
    );
});
