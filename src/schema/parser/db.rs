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
use super::permissions::parse_table_permissions;
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

    if let Some(az_value) = pick_map(obj, &["az", "analyzers"]) {
        for (name, def_value) in az_value.as_object().expect("checked by pick_map") {
            let Some(def) = def_value.as_str() else {
                continue;
            };
            // Lenient like access parsing: an analyzer this crate
            // cannot model yet is skipped rather than failing the
            // whole introspection.
            if let Ok(analyzer) = super::analyzer::parse_analyzer(name, def) {
                out.analyzers.insert(name.clone(), analyzer);
            }
        }
    }

    if let Some(bu_value) = pick_map(obj, &["bu", "buckets"]) {
        for (name, def_value) in bu_value.as_object().expect("checked by pick_map") {
            let Some(def) = def_value.as_str() else {
                continue;
            };
            if let Some(bucket) = parse_bucket(name, def) {
                out.buckets.insert(name.clone(), bucket);
            }
        }
    }

    Ok(out)
}

// (Edge endpoint extraction lives in [`super::edge::parse_edge_endpoints`];
//  this file used to host its own RELATION-only helper before the 0.2.5
//  parser upgrade unified the two paths.)
