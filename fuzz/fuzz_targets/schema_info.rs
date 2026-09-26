//! The INFO parsers read text the server sent back: treat it as untrusted.
//!
//! Every definition parser, and the database- and table-level walkers that
//! route to them, must return rather than panic, hang, or recurse without
//! bound on arbitrary definition text.

#![no_main]

use libfuzzer_sys::fuzz_target;
use serde_json::{json, Value};
use surql::schema::parser;

const DB_KEYS: &[&str] = &[
    "tb", "tables", "ac", "accesses", "bu", "buckets", "az", "analyzers", "fc", "functions",
    "pa", "params", "sq", "sequences",
];
const TABLE_KEYS: &[&str] = &["fd", "fields", "ix", "indexes", "ev", "events"];

fuzz_target!(|input: (String, String)| {
    let (name, definition) = input;

    let _ = parser::parse_access(&name, &definition);
    let _ = parser::parse_analyzer(&name, &definition);
    let _ = parser::parse_bucket(&name, &definition);
    let _ = parser::parse_event(&name, &definition);
    let _ = parser::parse_field(&name, &definition);
    let _ = parser::parse_function(&name, &definition);
    let _ = parser::parse_index(&name, &definition);
    let _ = parser::parse_param(&name, &definition);
    let _ = parser::parse_sequence(&name, &definition);
    let _ = parser::parse_table_permissions(&definition);
    let _ = parser::parse_table_mode(&definition);
    let _ = parser::parse_changefeed(&definition);
    let _ = parser::parse_view(&definition);

    let entry = || json!({ name.clone(): definition.clone() });
    let db: serde_json::Map<String, Value> = DB_KEYS.iter().map(|k| ((*k).to_owned(), entry())).collect();
    let _ = parser::parse_db_info(&Value::Object(db));

    let table: serde_json::Map<String, Value> =
        TABLE_KEYS.iter().map(|k| ((*k).to_owned(), entry())).collect();
    let table = Value::Object(table);
    let _ = parser::parse_table_info(&name, &table, Some(&definition));
    let _ = parser::parse_edge_info(&name, &table, Some(&definition));
    let _ = parser::parse_table_full(&name, &definition, &table);
});
