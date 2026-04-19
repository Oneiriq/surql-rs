//! `DEFINE TABLE` / `INFO FOR TABLE` parser.
//!
//! Reconstructs [`TableDefinition`] values from SurrealDB `INFO FOR
//! TABLE` responses. Split out of the monolithic `parser.rs` so each
//! submodule stays under the 1000-LOC budget; see parent [`super`] for
//! the public entry points.

use serde_json::Value;

use super::event::parse_events;
use super::field::parse_fields;
use super::index::parse_indexes;
use super::{expect_object, pick_map, value_to_string_map};
use crate::error::Result;
use crate::schema::table::{TableDefinition, TableMode};

// --- Public parsers ----------------------------------------------------------

/// Parse the `DEFINE TABLE` statement into a [`TableMode`].
///
/// An empty input defaults to [`TableMode::Schemaless`], mirroring the Python
/// module's fallback.
pub fn parse_table_mode(definition: &str) -> TableMode {
    if definition.is_empty() {
        return TableMode::Schemaless;
    }
    let upper = definition.to_uppercase();
    if upper.contains("SCHEMAFULL") {
        TableMode::Schemafull
    } else if upper.contains("SCHEMALESS") {
        TableMode::Schemaless
    } else if upper.contains("DROP") {
        TableMode::Drop
    } else {
        TableMode::Schemaless
    }
}

/// Parse a SurrealDB `INFO FOR TABLE` response into a [`TableDefinition`].
///
/// Accepts either the short-key shape (`fd`, `ix`, `ev`) or the long-key shape
/// (`fields`, `indexes`, `events`). Unknown enum values surface as the default
/// variant (for example `FieldType::Any` for unknown types), matching the
/// Python behaviour.
///
/// Returns [`crate::error::SurqlError::SchemaParse`] when the top-level value
/// is not a JSON object.
pub fn parse_table_info(table_name: &str, info: &Value) -> Result<TableDefinition> {
    let obj = expect_object(info, &format!("INFO FOR TABLE {table_name}"))?;

    let tb_definition = obj.get("tb").and_then(Value::as_str).unwrap_or("");
    let mode = parse_table_mode(tb_definition);

    let fields_value = pick_map(obj, &["fields", "fd"]);
    let fields = fields_value
        .map(|v| parse_fields(&value_to_string_map(v)))
        .unwrap_or_default();

    let indexes_value = pick_map(obj, &["indexes", "ix"]);
    let indexes = indexes_value
        .map(|v| parse_indexes(&value_to_string_map(v)))
        .unwrap_or_default();

    let events_value = pick_map(obj, &["events", "ev"]);
    let events = events_value
        .map(|v| parse_events(&value_to_string_map(v)))
        .unwrap_or_default();

    Ok(TableDefinition {
        name: table_name.to_string(),
        mode,
        fields,
        indexes,
        events,
        permissions: None,
        drop: false,
    })
}
