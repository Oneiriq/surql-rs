//! `INFO FOR DB` parser.
//!
//! Walks the top-level SurrealDB database description, partitioning
//! table entries into plain [`TableDefinition`] values vs relation-mode
//! [`EdgeDefinition`] values, and folding database-level access
//! definitions into [`DatabaseInfo`]. Split out of the monolithic
//! `parser.rs` so each submodule stays under the 1000-LOC budget; see
//! parent [`super`] for the public entry points.

use std::sync::OnceLock;

use regex::Regex;
use serde_json::Value;

use super::access::parse_access;
use super::table::parse_table_mode;
use super::{expect_object, pick_map, regex_case_insensitive, DatabaseInfo};
use crate::error::Result;
use crate::schema::edge::{EdgeDefinition, EdgeMode};
use crate::schema::table::TableDefinition;

// --- Regex accessors ---------------------------------------------------------

fn relation_from_to_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"TYPE\s+RELATION\s+FROM\s+(\w+)\s+TO\s+(\w+)"))
}

// --- Public parser -----------------------------------------------------------

/// Parse a SurrealDB `INFO FOR DB` response.
///
/// The response is inspected for tables (under `tb` / `tables`) and access
/// definitions (under `ac` / `accesses`). Tables declared with
/// `TYPE RELATION FROM ... TO ...` are routed into
/// [`DatabaseInfo::edges`] as [`EdgeDefinition`] values; every other table
/// becomes a [`TableDefinition`] in [`DatabaseInfo::tables`].
///
/// Returns [`crate::error::SurqlError::SchemaParse`] when the top-level value
/// is not a JSON object.
pub fn parse_db_info(info: &Value) -> Result<DatabaseInfo> {
    let obj = expect_object(info, "INFO FOR DB")?;

    let mut out = DatabaseInfo::default();

    if let Some(tb_value) = pick_map(obj, &["tb", "tables"]) {
        for (name, def_value) in tb_value.as_object().expect("checked by pick_map") {
            let Some(def) = def_value.as_str() else {
                continue;
            };
            if let Some((from, to)) = extract_relation_endpoints(def) {
                out.edges.insert(
                    name.clone(),
                    EdgeDefinition {
                        name: name.clone(),
                        mode: EdgeMode::Relation,
                        from_table: Some(from),
                        to_table: Some(to),
                        fields: Vec::new(),
                        indexes: Vec::new(),
                        events: Vec::new(),
                        permissions: None,
                    },
                );
            } else {
                let mode = parse_table_mode(def);
                out.tables.insert(
                    name.clone(),
                    TableDefinition {
                        name: name.clone(),
                        mode,
                        fields: Vec::new(),
                        indexes: Vec::new(),
                        events: Vec::new(),
                        permissions: None,
                        drop: false,
                    },
                );
            }
        }
    }

    if let Some(ac_value) = pick_map(obj, &["ac", "accesses"]) {
        for (name, def_value) in ac_value.as_object().expect("checked by pick_map") {
            let Some(def) = def_value.as_str() else {
                continue;
            };
            if let Some(access) = parse_access(name, def) {
                out.accesses.insert(name.clone(), access);
            }
        }
    }

    Ok(out)
}

// --- Edge extractor ----------------------------------------------------------

/// Pull the `FROM <tb> TO <tb>` pair out of a relation-mode
/// `DEFINE TABLE` statement. Exposed to the parser module for unit-test
/// coverage in [`super`]'s test module.
pub(super) fn extract_relation_endpoints(definition: &str) -> Option<(String, String)> {
    let caps = relation_from_to_regex().captures(definition)?;
    let from = caps.get(1)?.as_str().to_string();
    let to = caps.get(2)?.as_str().to_string();
    Some((from, to))
}
