//! `INFO FOR DB` parser.
//!
//! Walks the top-level SurrealDB database description, partitioning
//! table entries into plain [`TableDefinition`] values vs relation-mode
//! [`EdgeDefinition`] values, and folding database-level access
//! definitions into [`DatabaseInfo`]. Split out of the monolithic
//! `parser.rs` so each submodule stays under the 1000-LOC budget; see
//! parent [`super`] for the public entry points.

use serde_json::Value;

use super::access::parse_access;
use super::bucket::parse_bucket;
use super::edge::{parse_edge_endpoints, parse_edge_mode};
use super::function::parse_function;
use super::param::parse_param;
use super::permissions::parse_table_permissions;
use super::sequence::parse_sequence;
use super::table::{parse_changefeed, parse_table_mode};
use super::view::parse_view;
use super::{expect_object, pick_map, DatabaseInfo};
use crate::error::Result;
use crate::schema::edge::EdgeDefinition;
use crate::schema::table::TableDefinition;

// --- Edge classification -----------------------------------------------------

/// `true` when the `DEFINE TABLE` string declares a relation-mode edge.
/// Word-boundary anchored — `TYPE RELATIONAL_SOMETHING` will not match.
fn is_edge_definition(definition: &str) -> bool {
    // `parse_edge_mode` is the single source of truth for what counts as
    // a `TYPE RELATION` edge; using it here keeps the two call sites in
    // lockstep so `parse_db_info` cannot disagree with `parse_edge_info`
    // about whether a given DEFINE TABLE is an edge.
    matches!(
        parse_edge_mode(definition),
        crate::schema::edge::EdgeMode::Relation,
    )
}

/// Read one database-level map (`{ "<name>": "DEFINE ..." }`) into a keyed
/// collection, skipping non-string entries and definitions `parse` rejects.
///
/// Every kind of database-level object arrives in this shape, so the walk
/// exists once rather than per kind.
fn collect<T>(
    obj: &serde_json::Map<String, Value>,
    keys: &[&str],
    parse: impl Fn(&str, &str) -> Option<T>,
) -> std::collections::BTreeMap<String, T> {
    let Some(value) = pick_map(obj, keys) else {
        return std::collections::BTreeMap::new();
    };
    value
        .as_object()
        .expect("checked by pick_map")
        .iter()
        .filter_map(|(name, def)| {
            let parsed = parse(name, def.as_str()?)?;
            Some((name.clone(), parsed))
        })
        .collect()
}

// --- Public parser -----------------------------------------------------------

/// Parse a SurrealDB `INFO FOR DB` response.
///
/// The response is inspected for tables (under `tb` / `tables`), access
/// definitions (under `ac` / `accesses`), and object-storage buckets (under
/// `bu` / `buckets`). Tables declared with `TYPE RELATION FROM ... TO ...`
/// are routed into [`DatabaseInfo::edges`] as [`EdgeDefinition`] values;
/// every other table becomes a [`TableDefinition`] in
/// [`DatabaseInfo::tables`]. Buckets are parsed into
/// [`DatabaseInfo::buckets`].
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
            if is_edge_definition(def) {
                let (from_table, to_table) = parse_edge_endpoints(def);
                out.edges.insert(
                    name.clone(),
                    EdgeDefinition {
                        name: name.clone(),
                        mode: parse_edge_mode(def),
                        from_table,
                        to_table,
                        fields: Vec::new(),
                        indexes: Vec::new(),
                        events: Vec::new(),
                        permissions: parse_table_permissions(def),
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
                        permissions: parse_table_permissions(def),
                        drop: false,
                        changefeed: parse_changefeed(def),
                        view: parse_view(def),
                    },
                );
            }
        }
    }

    out.accesses = collect(obj, &["ac", "accesses"], parse_access);
    // Lenient like the rest: a definition this crate cannot model yet is
    // skipped rather than failing the whole introspection.
    out.analyzers = collect(obj, &["az", "analyzers"], |name, def| {
        super::analyzer::parse_analyzer(name, def).ok()
    });
    out.buckets = collect(obj, &["bu", "buckets"], parse_bucket);
    out.sequences = collect(obj, &["sq", "sequences"], parse_sequence);
    out.functions = collect(obj, &["fc", "functions"], parse_function);
    out.params = collect(obj, &["pa", "params"], parse_param);

    Ok(out)
}

// (Edge endpoint extraction lives in [`super::edge::parse_edge_endpoints`];
//  this file used to host its own RELATION-only helper before the 0.2.5
//  parser upgrade unified the two paths.)
