//! `DEFINE TABLE` / `INFO FOR TABLE` parser.
//!
//! Reconstructs [`TableDefinition`] values from SurrealDB `INFO FOR
//! TABLE` responses. Split out of the monolithic `parser.rs` so each
//! submodule stays under the 1000-LOC budget; see parent [`super`] for
//! the public entry points.

use std::sync::OnceLock;

use regex::Regex;
use serde_json::Value;

use super::event::parse_events;
use super::field::parse_fields;
use super::index::parse_indexes;
use super::permissions::parse_table_permissions;
use super::view::parse_view;
use super::{expect_object, pick_map, regex_case_insensitive, value_to_string_map};
use crate::error::Result;
use crate::schema::changefeed::ChangeFeed;
use crate::schema::table::{TableDefinition, TableMode};

/// Matches `CHANGEFEED <duration> [INCLUDE ORIGINAL]`, the form the engine
/// both accepts and echoes.
fn changefeed_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        regex_case_insensitive(r"\bCHANGEFEED\s+(\S+?)\s*(\bINCLUDE\s+ORIGINAL\b)?(?:\s|;|$)")
    })
}

// --- Public parsers ----------------------------------------------------------

/// Parse the `CHANGEFEED` clause out of a `DEFINE TABLE` statement.
///
/// Returns `None` for a table with no change feed.
pub fn parse_changefeed(definition: &str) -> Option<ChangeFeed> {
    let caps = changefeed_regex().captures(definition)?;
    let duration = caps.get(1)?.as_str().trim_end_matches(';');
    if duration.is_empty() {
        return None;
    }
    Some(ChangeFeed::new(duration).include_original(caps.get(2).is_some()))
}

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
/// SurrealDB v3's `INFO FOR TABLE` does **not** include the table-level
/// `DEFINE TABLE` statement, so table mode and `PERMISSIONS` cannot be
/// recovered from it alone. Pass `define_table` — the
/// `DEFINE TABLE <name> ...` string from `INFO FOR DB`'s `tables.<name>`
/// entry — to recover them; without it, the parser falls back to the
/// legacy `tb` key inside the response (v1/v2 shape), and the table
/// mode defaults to [`TableMode::Schemaless`] / permissions to `None`
/// on v3.
///
/// Returns [`crate::error::SurqlError::SchemaParse`] when the top-level value
/// The complete definition for one table from the two `INFO` levels:
/// the database's `DEFINE TABLE` echo carries mode and permissions,
/// and the table's own `INFO FOR TABLE` carries fields, indexes, and
/// events. `INFO FOR DB` alone yields fieldless tables, which is a
/// trap for anyone diffing against it.
pub fn parse_table_full(
    table_name: &str,
    db_define: &str,
    table_info: &Value,
) -> Result<TableDefinition> {
    parse_table_info(table_name, table_info, Some(db_define))
}

/// is not a JSON object.
pub fn parse_table_info(
    table_name: &str,
    info: &Value,
    define_table: Option<&str>,
) -> Result<TableDefinition> {
    let obj = expect_object(info, &format!("INFO FOR TABLE {table_name}"))?;

    let tb_definition =
        define_table.unwrap_or_else(|| obj.get("tb").and_then(Value::as_str).unwrap_or(""));
    let mode = parse_table_mode(tb_definition);
    let permissions = parse_table_permissions(tb_definition);

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
        permissions,
        drop: false,
        changefeed: parse_changefeed(tb_definition),
        view: parse_view(tb_definition),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn changefeed_without_original() {
        let cf =
            parse_changefeed("DEFINE TABLE evt TYPE ANY SCHEMALESS CHANGEFEED 1d PERMISSIONS NONE")
                .expect("changefeed");
        assert_eq!(cf.duration, "1d");
        assert!(!cf.include_original);
    }

    #[test]
    fn changefeed_with_original() {
        let cf = parse_changefeed(
            "DEFINE TABLE evt TYPE ANY SCHEMALESS CHANGEFEED 3d INCLUDE ORIGINAL PERMISSIONS NONE",
        )
        .expect("changefeed");
        assert_eq!(cf.duration, "3d");
        assert!(cf.include_original);
    }

    #[test]
    fn changefeed_at_the_end_of_a_statement() {
        let cf =
            parse_changefeed("DEFINE TABLE evt SCHEMALESS CHANGEFEED 6h;").expect("changefeed");
        assert_eq!(cf.duration, "6h");
    }

    #[test]
    fn no_changefeed_is_none() {
        assert!(parse_changefeed("DEFINE TABLE evt SCHEMAFULL PERMISSIONS NONE").is_none());
        assert!(parse_changefeed("").is_none());
    }

    #[test]
    fn table_info_carries_the_changefeed_from_the_db_define() {
        let table = parse_table_info(
            "evt",
            &serde_json::json!({ "fields": {} }),
            Some("DEFINE TABLE evt TYPE ANY SCHEMALESS CHANGEFEED 1h INCLUDE ORIGINAL PERMISSIONS NONE"),
        )
        .unwrap();
        let cf = table.changefeed.expect("changefeed");
        assert_eq!(cf.duration, "1h");
        assert!(cf.include_original);
    }
}
