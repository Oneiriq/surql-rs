//! Schema INFO parser.
//!
//! Port of `surql/schema/parser.py`. Parses SurrealDB `INFO FOR DB` /
//! `INFO FOR TABLE` response JSON back into [`TableDefinition`],
//! [`EdgeDefinition`], `FieldDefinition`, `IndexDefinition`,
//! `EventDefinition`, and [`AccessDefinition`] values.
//!
//! This is the inverse of the schema-definition → SurrealQL path: given the
//! JSON blob Surreal returns from `INFO FOR ...`, reconstruct the schema
//! definition objects. The parser accepts both shapes that `surql-py` handles:
//!
//! - object-keyed maps (`{"fields": { "name": "DEFINE FIELD ..." }}`);
//! - short-key maps (`{"fd": { "name": "..." }}`) as observed from SurrealDB.
//!
//! Input is always [`serde_json::Value`]; there is no tight coupling to the
//! `surrealdb` crate. The definition strings inside it are server text, so
//! the parsers never panic on them and never slice by an unchecked offset.
//!
//! Every statement is read after its `DEFINE <kind> <name> [ON <table>]`
//! head, and a word only opens a clause outside quotes, backticked names,
//! and brackets: a field named `default`, a comment mentioning
//! `PERMISSIONS`, or an assertion over a subquery all keep their meaning.
//!
//! The implementation is split into cohesive submodules so no file exceeds
//! the repository's 1000-LOC budget:
//!
//! - `scan` — the quote-, bracket-, and name-aware scanner the others use.
//! - `permissions` — table, edge, and field `PERMISSIONS` clauses.
//! - `field` — `DEFINE FIELD` parsing.
//! - `index` — `DEFINE INDEX` parsing (UNIQUE / FULLTEXT / MTREE / HNSW /
//!   DISKANN).
//! - `event` — `DEFINE EVENT` parsing.
//! - `access` — `DEFINE ACCESS` parsing (JWT + RECORD).
//! - `function` — `DEFINE FUNCTION` parsing.
//! - `param` — `DEFINE PARAM` parsing.
//! - `sequence` — `DEFINE SEQUENCE` parsing.
//! - `table` — `DEFINE TABLE` + `INFO FOR TABLE` parsing.
//! - `view` — `DEFINE TABLE ... AS SELECT` (view) parsing.
//! - `db` — `INFO FOR DB` parsing + edge partitioning.
//!
//! ## Example
//!
//! ```
//! use serde_json::json;
//!
//! use surql::schema::parser::{parse_table_info, parse_db_info};
//!
//! let info = json!({
//!     "tb": "DEFINE TABLE user SCHEMAFULL",
//!     "fields": { "name": "DEFINE FIELD name ON TABLE user TYPE string" }
//! });
//! let table = parse_table_info("user", &info, None).expect("valid table info");
//! assert_eq!(table.name, "user");
//! assert_eq!(table.fields.len(), 1);
//!
//! let db = json!({
//!     "tb": {
//!         "user": "DEFINE TABLE user SCHEMAFULL",
//!         "likes": "DEFINE TABLE likes TYPE RELATION FROM user TO post"
//!     }
//! });
//! let info = parse_db_info(&db).unwrap();
//! assert!(info.tables.contains_key("user"));
//! assert!(info.edges.contains_key("likes"));
//! ```

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Result, SurqlError};
use crate::schema::access::AccessDefinition;
use crate::schema::bucket::BucketDefinition;
use crate::schema::edge::EdgeDefinition;
use crate::schema::function::FunctionDefinition;
use crate::schema::param::ParamDefinition;
use crate::schema::sequence::SequenceDefinition;
use crate::schema::table::TableDefinition;

mod access;
mod analyzer;
mod bucket;
mod db;
mod edge;
mod event;
mod field;
mod function;
mod index;
mod param;
mod permissions;
mod scan;
mod sequence;
mod table;
mod view;

pub use access::parse_access;
pub use analyzer::parse_analyzer;
pub use bucket::parse_bucket;
pub use db::parse_db_info;
pub use edge::parse_edge_info;
pub use event::{parse_event, parse_events};
pub use field::{parse_field, parse_fields};
pub use function::parse_function;
pub use index::{parse_index, parse_indexes};
pub use param::parse_param;
pub use permissions::parse_table_permissions;
pub use sequence::parse_sequence;
pub use table::{parse_changefeed, parse_table_full, parse_table_info, parse_table_mode};
pub use view::parse_view;

// --- Shared JSON helpers -----------------------------------------------------

pub(super) fn expect_object<'a>(
    value: &'a Value,
    context: &str,
) -> Result<&'a serde_json::Map<String, Value>> {
    value.as_object().ok_or_else(|| SurqlError::SchemaParse {
        reason: format!(
            "{context}: expected JSON object, got {}",
            type_name_of(value)
        ),
    })
}

fn type_name_of(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Coerce a map of JSON values into a `BTreeMap<String, String>`.
///
/// Non-string values are skipped so callers can tolerate server responses that
/// stash additional metadata under the same key.
pub(super) fn value_to_string_map(
    map: &serde_json::Map<String, Value>,
) -> BTreeMap<String, String> {
    map.iter()
        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
        .collect()
}

/// Pick the first populated child object from `info` under any of `keys`.
pub(super) fn pick_map<'a>(
    info: &'a serde_json::Map<String, Value>,
    keys: &[&str],
) -> Option<&'a serde_json::Map<String, Value>> {
    keys.iter()
        .filter_map(|k| info.get(*k).and_then(Value::as_object))
        .find(|m| !m.is_empty())
}

// --- Parser state output -----------------------------------------------------

/// Collected `INFO FOR DB` response parsed into typed schema objects.
///
/// Tables and edges are keyed by name. `accesses` holds database-level
/// `DEFINE ACCESS` definitions.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseInfo {
    /// Regular (non-relation) tables.
    pub tables: BTreeMap<String, TableDefinition>,
    /// Text analyzers.
    #[serde(default)]
    pub analyzers: BTreeMap<String, crate::schema::analyzer::AnalyzerDefinition>,
    /// Relation-mode edge tables.
    pub edges: BTreeMap<String, EdgeDefinition>,
    /// Database-level access definitions.
    pub accesses: BTreeMap<String, AccessDefinition>,
    /// Object-storage bucket definitions.
    #[serde(default)]
    pub buckets: BTreeMap<String, BucketDefinition>,
    /// Monotonic ID sequences.
    #[serde(default)]
    pub sequences: BTreeMap<String, SequenceDefinition>,
    /// Custom `fn::` functions, keyed without the `fn::` prefix.
    #[serde(default)]
    pub functions: BTreeMap<String, FunctionDefinition>,
    /// Database-level params, keyed without the leading `$`.
    #[serde(default)]
    pub params: BTreeMap<String, ParamDefinition>,
}

#[cfg(test)]
#[allow(deprecated)] // MTREE stays covered: old snapshots and echoes still load
mod tests;
