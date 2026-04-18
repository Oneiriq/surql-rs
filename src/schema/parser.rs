//! Schema INFO parser.
//!
//! Port of `surql/schema/parser.py`. Parses SurrealDB `INFO FOR DB` /
//! `INFO FOR TABLE` response JSON back into [`TableDefinition`],
//! [`EdgeDefinition`], [`FieldDefinition`], [`IndexDefinition`],
//! [`EventDefinition`], and [`AccessDefinition`] values.
//!
//! This is the inverse of the schema-definition → SurrealQL path: given the
//! JSON blob Surreal returns from `INFO FOR ...`, reconstruct the schema
//! definition objects. The parser accepts both shapes that `surql-py` handles:
//!
//! - object-keyed maps (`{"fields": { "name": "DEFINE FIELD ..." }}`);
//! - short-key maps (`{"fd": { "name": "..." }}`) as observed from SurrealDB.
//!
//! Input is always [`serde_json::Value`]; there is no tight coupling to the
//! `surrealdb` crate.
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
//! let table = parse_table_info("user", &info).expect("valid table info");
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
use std::sync::OnceLock;

use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Result, SurqlError};

use super::access::{AccessDefinition, AccessType, JwtConfig, RecordAccessConfig};
use super::edge::{EdgeDefinition, EdgeMode};
use super::fields::{FieldDefinition, FieldType};
use super::table::{
    EventDefinition, HnswDistanceType, IndexDefinition, IndexType, MTreeDistanceType,
    MTreeVectorType, TableDefinition, TableMode,
};

// --- Regex accessors ---------------------------------------------------------

fn regex_case_insensitive(pattern: &str) -> Regex {
    Regex::new(&format!("(?i){pattern}")).expect("valid regex pattern")
}

fn type_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"TYPE\s+(\w+)"))
}

fn readonly_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"\bREADONLY\b"))
}

fn flexible_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"\bFLEXIBLE\b"))
}

fn columns_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        regex_case_insensitive(r"COLUMNS\s+([^;]+?)(?:UNIQUE|SEARCH|HNSW|MTREE|\s*;|\s*$)")
    })
}

fn fields_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        regex_case_insensitive(r"FIELDS\s+([^;]+?)(?:UNIQUE|SEARCH|HNSW|MTREE|\s*;|\s*$)")
    })
}

fn dimension_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"DIMENSION\s+(\d+)"))
}

fn distance_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"(?:DIST|DISTANCE)\s+(\w+)"))
}

fn efc_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"EFC\s+(\d+)"))
}

fn m_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"\bM\s+(\d+)"))
}

fn when_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"(?s)WHEN\s+(.+?)\s+THEN"))
}

fn then_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"(?s)THEN\s+(?:\{(.+?)\}|(.+?))(?:\s*;|\s*$)"))
}

fn relation_from_to_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"TYPE\s+RELATION\s+FROM\s+(\w+)\s+TO\s+(\w+)"))
}

fn algorithm_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"ALGORITHM\s+(\w+)"))
}

fn key_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"(?s)KEY\s+'([^']*)'"))
}

fn url_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"(?s)URL\s+'([^']*)'"))
}

fn issuer_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"(?s)WITH\s+ISSUER\s+'([^']*)'"))
}

fn signup_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        regex_case_insensitive(r"(?s)SIGNUP\s+\((.+?)\)(?:\s+SIGNIN|\s+DURATION|\s*;|\s*$)")
    })
}

fn signin_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        regex_case_insensitive(r"(?s)SIGNIN\s+\((.+?)\)(?:\s+SIGNUP|\s+DURATION|\s*;|\s*$)")
    })
}

fn session_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"FOR\s+SESSION\s+(\w+)"))
}

fn token_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"FOR\s+TOKEN\s+(\w+)"))
}

fn access_type_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"TYPE\s+(JWT|RECORD)"))
}

// --- Helpers -----------------------------------------------------------------

fn expect_object<'a>(
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

/// Coerce a map-of-string JSON value into a `BTreeMap<String, String>`.
///
/// Non-string values are skipped so callers can tolerate server responses that
/// stash additional metadata under the same key.
fn value_to_string_map(value: &Value) -> BTreeMap<String, String> {
    value
        .as_object()
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

/// Pick the first populated child object from `info` under any of `keys`.
fn pick_map<'a>(info: &'a serde_json::Map<String, Value>, keys: &[&str]) -> Option<&'a Value> {
    for k in keys {
        if let Some(v) = info.get(*k) {
            if v.as_object().is_some_and(|m| !m.is_empty()) {
                return Some(v);
            }
        }
    }
    None
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
    /// Relation-mode edge tables.
    pub edges: BTreeMap<String, EdgeDefinition>,
    /// Database-level access definitions.
    pub accesses: BTreeMap<String, AccessDefinition>,
}

// --- Primary entry points ----------------------------------------------------

/// Parse a SurrealDB `INFO FOR TABLE` response into a [`TableDefinition`].
///
/// Accepts either the short-key shape (`fd`, `ix`, `ev`) or the long-key shape
/// (`fields`, `indexes`, `events`). Unknown enum values surface as the default
/// variant (for example `FieldType::Any` for unknown types), matching the
/// Python behaviour.
///
/// Returns [`SurqlError::SchemaParse`] when the top-level value is not a JSON
/// object.
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

/// Parse a SurrealDB `INFO FOR DB` response.
///
/// The response is inspected for tables (under `tb` / `tables`) and access
/// definitions (under `ac` / `accesses`). Tables declared with
/// `TYPE RELATION FROM ... TO ...` are routed into
/// [`DatabaseInfo::edges`] as [`EdgeDefinition`] values; every other table
/// becomes a [`TableDefinition`] in [`DatabaseInfo::tables`].
///
/// Returns [`SurqlError::SchemaParse`] when the top-level value is not a JSON
/// object.
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

// --- Building-block parsers --------------------------------------------------

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

/// Parse every entry of a `fd` / `fields` map.
///
/// Entries that fail to parse are skipped; success entries land in the
/// returned vector in the iteration order of the underlying map.
pub fn parse_fields(fd: &BTreeMap<String, String>) -> Vec<FieldDefinition> {
    fd.iter()
        .filter_map(|(name, def)| parse_field(name, def))
        .collect()
}

/// Parse one `DEFINE FIELD` statement.
///
/// Returns `None` when the definition string is empty.
pub fn parse_field(name: &str, definition: &str) -> Option<FieldDefinition> {
    if definition.is_empty() {
        return None;
    }
    Some(FieldDefinition {
        name: name.to_string(),
        field_type: extract_field_type(definition),
        assertion: extract_assertion(definition),
        default: extract_default(definition),
        value: extract_value(definition),
        permissions: None,
        readonly: extract_readonly(definition),
        flexible: extract_flexible(definition),
    })
}

/// Parse every entry of an `ix` / `indexes` map.
pub fn parse_indexes(ix: &BTreeMap<String, String>) -> Vec<IndexDefinition> {
    ix.iter()
        .filter_map(|(name, def)| parse_index(name, def))
        .collect()
}

/// Parse one `DEFINE INDEX` statement.
///
/// Returns `None` when the definition string is empty.
pub fn parse_index(name: &str, definition: &str) -> Option<IndexDefinition> {
    if definition.is_empty() {
        return None;
    }

    let mut columns = extract_index_columns(definition);
    if columns.is_empty() {
        columns = extract_index_fields(definition);
    }

    let index_type = extract_index_type(definition);

    let mut dimension = None;
    let mut distance = None;
    let mut vector_type = None;
    let mut hnsw_distance = None;
    let mut efc = None;
    let mut m = None;

    match index_type {
        IndexType::Mtree => {
            dimension = extract_dimension(definition);
            distance = extract_mtree_distance(definition);
            vector_type = extract_vector_type(definition);
        }
        IndexType::Hnsw => {
            dimension = extract_dimension(definition);
            vector_type = extract_vector_type(definition);
            hnsw_distance = extract_hnsw_distance(definition);
            efc = extract_hnsw_efc(definition);
            m = extract_hnsw_m(definition);
        }
        _ => {}
    }

    Some(IndexDefinition {
        name: name.to_string(),
        columns,
        index_type,
        dimension,
        distance,
        vector_type,
        hnsw_distance,
        efc,
        m,
    })
}

/// Parse every entry of an `ev` / `events` map.
pub fn parse_events(ev: &BTreeMap<String, String>) -> Vec<EventDefinition> {
    ev.iter()
        .filter_map(|(name, def)| parse_event(name, def))
        .collect()
}

/// Parse one `DEFINE EVENT` statement.
///
/// Returns `None` when the condition or action cannot be located.
pub fn parse_event(name: &str, definition: &str) -> Option<EventDefinition> {
    if definition.is_empty() {
        return None;
    }
    let condition = extract_event_condition(definition)?;
    let action = extract_event_action(definition)?;
    Some(EventDefinition {
        name: name.to_string(),
        condition,
        action,
    })
}

/// Parse one `DEFINE ACCESS` statement.
///
/// Returns `None` when the access type cannot be determined.
pub fn parse_access(name: &str, definition: &str) -> Option<AccessDefinition> {
    if definition.is_empty() {
        return None;
    }
    let access_type = extract_access_type(definition)?;

    let mut acc = AccessDefinition {
        name: name.to_string(),
        access_type,
        jwt: None,
        record: None,
        duration_session: None,
        duration_token: None,
    };

    match access_type {
        AccessType::Jwt => {
            let algorithm = extract_algorithm(definition).unwrap_or_else(|| "HS256".into());
            acc.jwt = Some(JwtConfig {
                algorithm,
                key: extract_single_quoted(key_regex(), definition),
                url: extract_single_quoted(url_regex(), definition),
                issuer: extract_single_quoted(issuer_regex(), definition),
            });
        }
        AccessType::Record => {
            let signup = signup_regex()
                .captures(definition)
                .and_then(|c| c.get(1))
                .map(|m| m.as_str().trim().to_string());
            let signin = signin_regex()
                .captures(definition)
                .and_then(|c| c.get(1))
                .map(|m| m.as_str().trim().to_string());
            acc.record = Some(RecordAccessConfig { signup, signin });
        }
    }

    acc.duration_session = session_regex()
        .captures(definition)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string());
    acc.duration_token = token_regex()
        .captures(definition)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string());

    Some(acc)
}

// --- Field extractors --------------------------------------------------------

fn extract_field_type(definition: &str) -> FieldType {
    let Some(caps) = type_regex().captures(definition) else {
        return FieldType::Any;
    };
    let Some(m) = caps.get(1) else {
        return FieldType::Any;
    };
    match m.as_str().to_ascii_lowercase().as_str() {
        "string" => FieldType::String,
        "int" => FieldType::Int,
        "float" => FieldType::Float,
        "bool" => FieldType::Bool,
        "datetime" => FieldType::Datetime,
        "duration" => FieldType::Duration,
        "decimal" => FieldType::Decimal,
        "number" => FieldType::Number,
        "object" => FieldType::Object,
        "array" => FieldType::Array,
        "record" => FieldType::Record,
        "geometry" => FieldType::Geometry,
        _ => FieldType::Any,
    }
}

/// Locate the case-insensitive keyword `kw` in `text` only at word boundaries
/// (ASCII boundaries). Returns the byte offset at which the keyword starts.
///
/// When `require_whitespace_left` is true, the keyword must be preceded by
/// whitespace or sit at byte 0 (a `$`-prefixed identifier like `$value` does
/// not satisfy this, and therefore will not be mis-identified as a clause
/// terminator).
fn find_keyword(text: &str, kw: &str, require_whitespace_left: bool) -> Option<usize> {
    let text_upper = text.to_ascii_uppercase();
    let kw_upper = kw.to_ascii_uppercase();
    let bytes = text_upper.as_bytes();
    let needle = kw_upper.as_bytes();
    if needle.is_empty() {
        return None;
    }
    let mut i = 0;
    while i + needle.len() <= bytes.len() {
        if bytes[i..i + needle.len()] == *needle {
            let left_ok = if require_whitespace_left {
                i == 0 || bytes[i - 1].is_ascii_whitespace()
            } else {
                i == 0 || !is_ident_byte(bytes[i - 1])
            };
            let right_ok =
                i + needle.len() == bytes.len() || !is_ident_byte(bytes[i + needle.len()]);
            if left_ok && right_ok {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Extract the body of a `KEYWORD <body> [TERMINATOR | ;]` clause.
///
/// `terminators` lists other keywords that would end the clause; any such
/// occurrence after the `keyword` anchor truncates the body. A trailing
/// semicolon is always stripped.
fn extract_clause(definition: &str, keyword: &str, terminators: &[&str]) -> Option<String> {
    let start = find_keyword(definition, keyword, false)?;
    let after_kw = start + keyword.len();
    // Require at least one whitespace after the keyword (matches `\s+`).
    let rest_start = definition[after_kw..]
        .find(|c: char| !c.is_whitespace())
        .map(|off| after_kw + off)?;
    // Ensure we actually consumed whitespace between the keyword and the body.
    if rest_start == after_kw {
        return None;
    }
    let tail = &definition[rest_start..];

    let mut end = tail.len();
    for term in terminators {
        if let Some(pos) = find_keyword(tail, term, true) {
            if pos < end {
                end = pos;
            }
        }
    }
    if let Some(pos) = tail.find(';') {
        if pos < end {
            end = pos;
        }
    }

    let body = tail[..end].trim();
    if body.is_empty() {
        return None;
    }
    Some(body.to_string())
}

fn extract_assertion(definition: &str) -> Option<String> {
    extract_clause(
        definition,
        "ASSERT",
        &["DEFAULT", "VALUE", "READONLY", "FLEXIBLE", "PERMISSIONS"],
    )
}

fn extract_default(definition: &str) -> Option<String> {
    extract_clause(
        definition,
        "DEFAULT",
        &["VALUE", "READONLY", "FLEXIBLE", "PERMISSIONS", "ASSERT"],
    )
}

fn extract_value(definition: &str) -> Option<String> {
    extract_clause(
        definition,
        "VALUE",
        &["DEFAULT", "READONLY", "FLEXIBLE", "PERMISSIONS", "ASSERT"],
    )
}

fn extract_readonly(definition: &str) -> bool {
    readonly_regex().is_match(definition)
}

fn extract_flexible(definition: &str) -> bool {
    flexible_regex().is_match(definition)
}

// --- Index extractors --------------------------------------------------------

fn split_cols(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn extract_index_columns(definition: &str) -> Vec<String> {
    columns_regex()
        .captures(definition)
        .and_then(|c| c.get(1))
        .map(|m| split_cols(m.as_str()))
        .unwrap_or_default()
}

fn extract_index_fields(definition: &str) -> Vec<String> {
    fields_regex()
        .captures(definition)
        .and_then(|c| c.get(1))
        .map(|m| split_cols(m.as_str()))
        .unwrap_or_default()
}

fn extract_index_type(definition: &str) -> IndexType {
    let upper = definition.to_uppercase();
    if upper.contains("UNIQUE") {
        IndexType::Unique
    } else if upper.contains("SEARCH") {
        IndexType::Search
    } else if upper.contains("HNSW") {
        IndexType::Hnsw
    } else if upper.contains("MTREE") {
        IndexType::Mtree
    } else {
        IndexType::Standard
    }
}

fn extract_dimension(definition: &str) -> Option<u32> {
    dimension_regex()
        .captures(definition)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse().ok())
}

fn extract_mtree_distance(definition: &str) -> Option<MTreeDistanceType> {
    let caps = distance_regex().captures(definition)?;
    let m = caps.get(1)?;
    match m.as_str().to_uppercase().as_str() {
        "COSINE" => Some(MTreeDistanceType::Cosine),
        "EUCLIDEAN" => Some(MTreeDistanceType::Euclidean),
        "MANHATTAN" => Some(MTreeDistanceType::Manhattan),
        "MINKOWSKI" => Some(MTreeDistanceType::Minkowski),
        _ => None,
    }
}

fn extract_hnsw_distance(definition: &str) -> Option<HnswDistanceType> {
    let caps = distance_regex().captures(definition)?;
    let m = caps.get(1)?;
    match m.as_str().to_uppercase().as_str() {
        "CHEBYSHEV" => Some(HnswDistanceType::Chebyshev),
        "COSINE" => Some(HnswDistanceType::Cosine),
        "EUCLIDEAN" => Some(HnswDistanceType::Euclidean),
        "HAMMING" => Some(HnswDistanceType::Hamming),
        "JACCARD" => Some(HnswDistanceType::Jaccard),
        "MANHATTAN" => Some(HnswDistanceType::Manhattan),
        "MINKOWSKI" => Some(HnswDistanceType::Minkowski),
        "PEARSON" => Some(HnswDistanceType::Pearson),
        _ => None,
    }
}

fn extract_vector_type(definition: &str) -> Option<MTreeVectorType> {
    // MTREE/HNSW `TYPE` clauses usually appear after `MTREE` / `HNSW`. Scan
    // every TYPE occurrence in case the first one is swallowed by the field
    // type clause (SurrealDB uses `TYPE` twice for these indexes).
    for caps in type_regex().captures_iter(definition) {
        let Some(m) = caps.get(1) else { continue };
        match m.as_str().to_uppercase().as_str() {
            "F64" => return Some(MTreeVectorType::F64),
            "F32" => return Some(MTreeVectorType::F32),
            "I64" => return Some(MTreeVectorType::I64),
            "I32" => return Some(MTreeVectorType::I32),
            "I16" => return Some(MTreeVectorType::I16),
            _ => {}
        }
    }
    None
}

fn extract_hnsw_efc(definition: &str) -> Option<u32> {
    efc_regex()
        .captures(definition)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse().ok())
}

fn extract_hnsw_m(definition: &str) -> Option<u32> {
    m_regex()
        .captures(definition)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse().ok())
}

// --- Event extractors --------------------------------------------------------

fn extract_event_condition(definition: &str) -> Option<String> {
    when_regex()
        .captures(definition)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().trim().to_string())
}

fn extract_event_action(definition: &str) -> Option<String> {
    let caps = then_regex().captures(definition)?;
    if let Some(m) = caps.get(1) {
        return Some(m.as_str().trim().to_string());
    }
    caps.get(2).map(|m| m.as_str().trim().to_string())
}

// --- Access extractors -------------------------------------------------------

fn extract_access_type(definition: &str) -> Option<AccessType> {
    let caps = access_type_regex().captures(definition)?;
    match caps.get(1)?.as_str().to_uppercase().as_str() {
        "JWT" => Some(AccessType::Jwt),
        "RECORD" => Some(AccessType::Record),
        _ => None,
    }
}

fn extract_algorithm(definition: &str) -> Option<String> {
    algorithm_regex()
        .captures(definition)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string())
}

fn extract_single_quoted(re: &Regex, definition: &str) -> Option<String> {
    re.captures(definition)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string())
}

// --- Edge extractor ----------------------------------------------------------

fn extract_relation_endpoints(definition: &str) -> Option<(String, String)> {
    let caps = relation_from_to_regex().captures(definition)?;
    let from = caps.get(1)?.as_str().to_string();
    let to = caps.get(2)?.as_str().to_string();
    Some((from, to))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::access::{jwt_access, record_access};
    use crate::schema::edge::typed_edge;
    use crate::schema::fields::{datetime_field, int_field, string_field};
    use crate::schema::table::{mtree_index, search_index, table_schema, unique_index};
    use serde_json::json;

    // ---- parse_table_mode ---------------------------------------------------

    #[test]
    fn parse_table_mode_empty_is_schemaless() {
        assert_eq!(parse_table_mode(""), TableMode::Schemaless);
    }

    #[test]
    fn parse_table_mode_schemafull() {
        assert_eq!(
            parse_table_mode("DEFINE TABLE user SCHEMAFULL"),
            TableMode::Schemafull,
        );
    }

    #[test]
    fn parse_table_mode_schemaless() {
        assert_eq!(
            parse_table_mode("DEFINE TABLE user SCHEMALESS"),
            TableMode::Schemaless,
        );
    }

    #[test]
    fn parse_table_mode_drop() {
        assert_eq!(parse_table_mode("DEFINE TABLE user DROP"), TableMode::Drop,);
    }

    #[test]
    fn parse_table_mode_case_insensitive() {
        assert_eq!(
            parse_table_mode("define table user schemafull"),
            TableMode::Schemafull,
        );
    }

    // ---- parse_field --------------------------------------------------------

    #[test]
    fn parse_field_empty_definition_is_none() {
        assert!(parse_field("x", "").is_none());
    }

    #[test]
    fn parse_field_basic_string() {
        let f = parse_field("email", "DEFINE FIELD email ON TABLE user TYPE string").unwrap();
        assert_eq!(f.name, "email");
        assert_eq!(f.field_type, FieldType::String);
        assert!(f.assertion.is_none());
        assert!(!f.readonly);
    }

    #[test]
    fn parse_field_unknown_type_falls_back_to_any() {
        let f = parse_field("x", "DEFINE FIELD x ON TABLE t TYPE unknown_type_value").unwrap();
        assert_eq!(f.field_type, FieldType::Any);
    }

    #[test]
    fn parse_field_no_type_clause_falls_back_to_any() {
        let f = parse_field("x", "DEFINE FIELD x ON TABLE t").unwrap();
        assert_eq!(f.field_type, FieldType::Any);
    }

    #[test]
    fn parse_field_with_assertion_and_default() {
        let f = parse_field(
            "age",
            "DEFINE FIELD age ON TABLE user TYPE int ASSERT $value >= 0 DEFAULT 0",
        )
        .unwrap();
        assert_eq!(f.field_type, FieldType::Int);
        assert_eq!(f.assertion.as_deref(), Some("$value >= 0"));
        assert_eq!(f.default.as_deref(), Some("0"));
    }

    #[test]
    fn parse_field_with_value_readonly_flexible() {
        let f = parse_field(
            "full",
            "DEFINE FIELD full ON TABLE user TYPE string VALUE string::concat(a,b) READONLY FLEXIBLE",
        )
        .unwrap();
        assert_eq!(f.value.as_deref(), Some("string::concat(a,b)"));
        assert!(f.readonly);
        assert!(f.flexible);
    }

    // ---- parse_index --------------------------------------------------------

    #[test]
    fn parse_index_empty_is_none() {
        assert!(parse_index("idx", "").is_none());
    }

    #[test]
    fn parse_index_standard() {
        let idx = parse_index(
            "title_idx",
            "DEFINE INDEX title_idx ON TABLE post COLUMNS title",
        )
        .unwrap();
        assert_eq!(idx.index_type, IndexType::Standard);
        assert_eq!(idx.columns, vec!["title".to_string()]);
    }

    #[test]
    fn parse_index_unique_with_fields_keyword() {
        let idx = parse_index(
            "email_idx",
            "DEFINE INDEX email_idx ON TABLE user FIELDS email UNIQUE",
        )
        .unwrap();
        assert_eq!(idx.index_type, IndexType::Unique);
        assert_eq!(idx.columns, vec!["email".to_string()]);
    }

    #[test]
    fn parse_index_search() {
        let idx = parse_index(
            "content_search",
            "DEFINE INDEX content_search ON TABLE post COLUMNS title, content SEARCH ANALYZER ascii",
        )
        .unwrap();
        assert_eq!(idx.index_type, IndexType::Search);
        assert_eq!(
            idx.columns,
            vec!["title".to_string(), "content".to_string()]
        );
    }

    #[test]
    fn parse_index_mtree() {
        let idx = parse_index(
            "emb_idx",
            "DEFINE INDEX emb_idx ON TABLE doc COLUMNS embedding MTREE DIMENSION 1536 DIST COSINE TYPE F32",
        )
        .unwrap();
        assert_eq!(idx.index_type, IndexType::Mtree);
        assert_eq!(idx.dimension, Some(1536));
        assert_eq!(idx.distance, Some(MTreeDistanceType::Cosine));
        assert_eq!(idx.vector_type, Some(MTreeVectorType::F32));
    }

    #[test]
    fn parse_index_hnsw_with_efc_m() {
        let idx = parse_index(
            "feat_idx",
            "DEFINE INDEX feat_idx ON TABLE doc COLUMNS features HNSW DIMENSION 128 DIST COSINE TYPE F32 EFC 500 M 16",
        )
        .unwrap();
        assert_eq!(idx.index_type, IndexType::Hnsw);
        assert_eq!(idx.dimension, Some(128));
        assert_eq!(idx.hnsw_distance, Some(HnswDistanceType::Cosine));
        assert_eq!(idx.vector_type, Some(MTreeVectorType::F32));
        assert_eq!(idx.efc, Some(500));
        assert_eq!(idx.m, Some(16));
    }

    // ---- parse_event --------------------------------------------------------

    #[test]
    fn parse_event_empty_is_none() {
        assert!(parse_event("e", "").is_none());
    }

    #[test]
    fn parse_event_plain_action() {
        let ev = parse_event(
            "email_changed",
            "DEFINE EVENT email_changed ON TABLE user WHEN $before.email != $after.email THEN CREATE audit_log;",
        )
        .unwrap();
        assert_eq!(ev.condition, "$before.email != $after.email");
        assert_eq!(ev.action, "CREATE audit_log");
    }

    #[test]
    fn parse_event_brace_action() {
        let ev = parse_event(
            "n",
            "DEFINE EVENT n ON TABLE t WHEN true THEN { CREATE audit_log };",
        )
        .unwrap();
        assert_eq!(ev.action, "CREATE audit_log");
    }

    #[test]
    fn parse_event_missing_then_is_none() {
        assert!(parse_event("n", "DEFINE EVENT n ON TABLE t WHEN true;").is_none());
    }

    // ---- parse_access -------------------------------------------------------

    #[test]
    fn parse_access_jwt_hs256() {
        let acc = parse_access(
            "api",
            "DEFINE ACCESS api ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'secret';",
        )
        .unwrap();
        assert_eq!(acc.access_type, AccessType::Jwt);
        let jwt = acc.jwt.expect("jwt config");
        assert_eq!(jwt.algorithm, "HS256");
        assert_eq!(jwt.key.as_deref(), Some("secret"));
    }

    #[test]
    fn parse_access_jwt_with_url_and_issuer() {
        let acc = parse_access(
            "api",
            "DEFINE ACCESS api ON DATABASE TYPE JWT ALGORITHM RS256 URL 'https://auth.example.com/jwks' WITH ISSUER 'https://auth.example.com';",
        )
        .unwrap();
        let jwt = acc.jwt.unwrap();
        assert_eq!(jwt.algorithm, "RS256");
        assert_eq!(jwt.url.as_deref(), Some("https://auth.example.com/jwks"));
        assert_eq!(jwt.issuer.as_deref(), Some("https://auth.example.com"));
    }

    #[test]
    fn parse_access_record_signup_signin_duration() {
        let acc = parse_access(
            "user_auth",
            "DEFINE ACCESS user_auth ON DATABASE TYPE RECORD SIGNUP (CREATE user) SIGNIN (SELECT * FROM user) DURATION FOR SESSION 24h, FOR TOKEN 15m;",
        )
        .unwrap();
        assert_eq!(acc.access_type, AccessType::Record);
        let rec = acc.record.unwrap();
        assert_eq!(rec.signup.as_deref(), Some("CREATE user"));
        assert_eq!(rec.signin.as_deref(), Some("SELECT * FROM user"));
        assert_eq!(acc.duration_session.as_deref(), Some("24h"));
        assert_eq!(acc.duration_token.as_deref(), Some("15m"));
    }

    #[test]
    fn parse_access_unknown_type_is_none() {
        assert!(parse_access("x", "DEFINE ACCESS x ON DATABASE TYPE BOGUS;").is_none());
    }

    // ---- parse_table_info ---------------------------------------------------

    #[test]
    fn parse_table_info_requires_object() {
        let err = parse_table_info("user", &json!("not an object")).unwrap_err();
        assert!(matches!(err, SurqlError::SchemaParse { .. }));
    }

    #[test]
    fn parse_table_info_short_keys() {
        let info = json!({
            "tb": "DEFINE TABLE user SCHEMAFULL",
            "fd": { "email": "DEFINE FIELD email ON TABLE user TYPE string" },
            "ix": { "e_idx": "DEFINE INDEX e_idx ON TABLE user COLUMNS email UNIQUE" },
            "ev": {
                "on_change": "DEFINE EVENT on_change ON TABLE user WHEN true THEN CREATE log;"
            }
        });
        let t = parse_table_info("user", &info).unwrap();
        assert_eq!(t.mode, TableMode::Schemafull);
        assert_eq!(t.fields.len(), 1);
        assert_eq!(t.indexes.len(), 1);
        assert_eq!(t.events.len(), 1);
    }

    #[test]
    fn parse_table_info_long_keys() {
        let info = json!({
            "tb": "DEFINE TABLE post SCHEMALESS",
            "fields": { "title": "DEFINE FIELD title ON TABLE post TYPE string" },
            "indexes": {},
            "events": {}
        });
        let t = parse_table_info("post", &info).unwrap();
        assert_eq!(t.mode, TableMode::Schemaless);
        assert_eq!(t.fields.len(), 1);
    }

    #[test]
    fn parse_table_info_missing_tb_defaults_schemaless() {
        let info = json!({});
        let t = parse_table_info("post", &info).unwrap();
        assert_eq!(t.mode, TableMode::Schemaless);
        assert!(t.fields.is_empty());
    }

    // ---- parse_db_info ------------------------------------------------------

    #[test]
    fn parse_db_info_requires_object() {
        let err = parse_db_info(&json!([])).unwrap_err();
        assert!(matches!(err, SurqlError::SchemaParse { .. }));
    }

    #[test]
    fn parse_db_info_partitions_tables_and_edges() {
        let info = json!({
            "tb": {
                "user": "DEFINE TABLE user SCHEMAFULL",
                "post": "DEFINE TABLE post SCHEMALESS",
                "likes": "DEFINE TABLE likes TYPE RELATION FROM user TO post"
            }
        });
        let db = parse_db_info(&info).unwrap();
        assert_eq!(db.tables.len(), 2);
        assert_eq!(db.edges.len(), 1);
        let edge = db.edges.get("likes").unwrap();
        assert_eq!(edge.from_table.as_deref(), Some("user"));
        assert_eq!(edge.to_table.as_deref(), Some("post"));
    }

    #[test]
    fn parse_db_info_accepts_long_keys_and_accesses() {
        let info = json!({
            "tables": {
                "user": "DEFINE TABLE user SCHEMAFULL"
            },
            "accesses": {
                "api": "DEFINE ACCESS api ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'secret';"
            }
        });
        let db = parse_db_info(&info).unwrap();
        assert_eq!(db.tables.len(), 1);
        assert_eq!(db.accesses.len(), 1);
        assert_eq!(db.accesses.get("api").unwrap().access_type, AccessType::Jwt);
    }

    #[test]
    fn parse_db_info_empty_object_returns_empty() {
        let db = parse_db_info(&json!({})).unwrap();
        assert!(db.tables.is_empty());
        assert!(db.edges.is_empty());
        assert!(db.accesses.is_empty());
    }

    // ---- Round-trip: table --------------------------------------------------

    #[test]
    fn round_trip_table_schemafull_with_fields_and_index() {
        let code_table = table_schema("user")
            .with_fields([
                string_field("email").build_unchecked().unwrap(),
                int_field("age")
                    .assertion("$value >= 0")
                    .default("0")
                    .build_unchecked()
                    .unwrap(),
                datetime_field("created_at")
                    .default("time::now()")
                    .readonly(true)
                    .build_unchecked()
                    .unwrap(),
            ])
            .with_indexes([unique_index("email_idx", ["email"])]);

        let stmts = code_table.to_surql_all();
        let tb_stmt = stmts[0].trim_end_matches(';').to_string();
        let email_fd = stmts[1].trim_end_matches(';').to_string();
        let age_fd = stmts[2].trim_end_matches(';').to_string();
        let created_fd = stmts[3].trim_end_matches(';').to_string();
        let email_ix = stmts[4].trim_end_matches(';').to_string();

        let info = json!({
            "tb": tb_stmt,
            "fd": {
                "email": email_fd,
                "age": age_fd,
                "created_at": created_fd
            },
            "ix": { "email_idx": email_ix }
        });
        let parsed = parse_table_info("user", &info).unwrap();
        assert_eq!(parsed.mode, TableMode::Schemafull);
        assert_eq!(parsed.fields.len(), 3);
        assert_eq!(parsed.indexes.len(), 1);
        let ix = &parsed.indexes[0];
        assert_eq!(ix.index_type, IndexType::Unique);
        assert_eq!(ix.columns, vec!["email".to_string()]);
        let age = parsed
            .fields
            .iter()
            .find(|f| f.name == "age")
            .expect("age field present");
        assert_eq!(age.field_type, FieldType::Int);
        assert_eq!(age.assertion.as_deref(), Some("$value >= 0"));
    }

    #[test]
    fn round_trip_table_with_mtree_index() {
        let code_table = table_schema("doc").with_indexes([mtree_index(
            "emb_idx",
            "embedding",
            1536,
            MTreeDistanceType::Cosine,
            MTreeVectorType::F32,
        )]);

        let stmts = code_table.to_surql_all();
        let info = json!({
            "tb": stmts[0].trim_end_matches(';'),
            "ix": { "emb_idx": stmts[1].trim_end_matches(';') }
        });
        let parsed = parse_table_info("doc", &info).unwrap();
        let ix = &parsed.indexes[0];
        assert_eq!(ix.index_type, IndexType::Mtree);
        assert_eq!(ix.dimension, Some(1536));
        assert_eq!(ix.distance, Some(MTreeDistanceType::Cosine));
        assert_eq!(ix.vector_type, Some(MTreeVectorType::F32));
    }

    #[test]
    fn round_trip_table_with_search_index() {
        let code_table = table_schema("post")
            .with_indexes([search_index("content_search", ["title", "content"])]);

        let stmts = code_table.to_surql_all();
        let info = json!({
            "tb": stmts[0].trim_end_matches(';'),
            "ix": { "content_search": stmts[1].trim_end_matches(';') }
        });
        let parsed = parse_table_info("post", &info).unwrap();
        let ix = &parsed.indexes[0];
        assert_eq!(ix.index_type, IndexType::Search);
        assert_eq!(ix.columns.len(), 2);
    }

    // ---- Round-trip: DB + edge ----------------------------------------------

    #[test]
    fn round_trip_db_info_with_edge() {
        let user = table_schema("user");
        let post = table_schema("post");
        let likes = typed_edge("likes", "user", "post");

        let info = json!({
            "tb": {
                "user": user.to_surql().trim_end_matches(';'),
                "post": post.to_surql().trim_end_matches(';'),
                "likes": likes.to_surql().unwrap().trim_end_matches(';')
            }
        });
        let db = parse_db_info(&info).unwrap();
        assert_eq!(db.tables.len(), 2);
        let edge = db.edges.get("likes").expect("edge present");
        assert_eq!(edge.mode, EdgeMode::Relation);
        assert_eq!(edge.from_table.as_deref(), Some("user"));
        assert_eq!(edge.to_table.as_deref(), Some("post"));
    }

    // ---- Round-trip: access -------------------------------------------------

    #[test]
    fn round_trip_access_jwt() {
        let code = jwt_access("api", JwtConfig::hs256("secret"))
            .with_session("24h")
            .with_token("15m");
        let sql = code.to_surql().unwrap();
        let info = json!({ "ac": { "api": sql } });
        let db = parse_db_info(&info).unwrap();
        let parsed = db.accesses.get("api").unwrap();
        assert_eq!(parsed.access_type, AccessType::Jwt);
        assert_eq!(parsed.duration_session.as_deref(), Some("24h"));
        assert_eq!(parsed.duration_token.as_deref(), Some("15m"));
        let jwt = parsed.jwt.as_ref().unwrap();
        assert_eq!(jwt.algorithm, "HS256");
        assert_eq!(jwt.key.as_deref(), Some("secret"));
    }

    #[test]
    fn round_trip_access_record() {
        let code = record_access(
            "user_auth",
            RecordAccessConfig::new()
                .with_signup("CREATE user SET a = 1")
                .with_signin("SELECT * FROM user"),
        );
        let sql = code.to_surql().unwrap();
        let info = json!({ "ac": { "user_auth": sql } });
        let db = parse_db_info(&info).unwrap();
        let parsed = db.accesses.get("user_auth").unwrap();
        assert_eq!(parsed.access_type, AccessType::Record);
        let rec = parsed.record.as_ref().unwrap();
        assert_eq!(rec.signup.as_deref(), Some("CREATE user SET a = 1"));
        assert_eq!(rec.signin.as_deref(), Some("SELECT * FROM user"));
    }

    // ---- Negative cases -----------------------------------------------------

    #[test]
    fn parse_field_with_malformed_keywords_still_returns_any() {
        let f = parse_field("x", "DEFINE FIELD x ON TABLE t TYPE 123not_a_type").unwrap();
        // `123not_a_type` starts with a digit; `\w+` captures it but it is not
        // a known type keyword — we fall back to ANY.
        assert_eq!(f.field_type, FieldType::Any);
    }

    #[test]
    fn parse_db_info_rejects_array_input() {
        let err = parse_db_info(&json!([1, 2, 3])).unwrap_err();
        assert!(matches!(err, SurqlError::SchemaParse { .. }));
    }

    #[test]
    fn parse_table_info_rejects_null_input() {
        let err = parse_table_info("t", &Value::Null).unwrap_err();
        assert!(matches!(err, SurqlError::SchemaParse { .. }));
    }

    #[test]
    fn parse_index_without_columns_returns_empty_list() {
        let idx = parse_index("x", "DEFINE INDEX x ON TABLE t").unwrap();
        assert!(idx.columns.is_empty());
        assert_eq!(idx.index_type, IndexType::Standard);
    }

    #[test]
    fn parse_db_info_ignores_non_string_entries() {
        let info = json!({
            "tb": {
                "user": "DEFINE TABLE user SCHEMAFULL",
                "bogus": 42
            }
        });
        let db = parse_db_info(&info).unwrap();
        assert_eq!(db.tables.len(), 1);
        assert!(db.tables.contains_key("user"));
    }

    #[test]
    fn parse_table_info_prefers_long_keys_when_both_present() {
        let info = json!({
            "tb": "DEFINE TABLE user SCHEMAFULL",
            "fields": { "a": "DEFINE FIELD a ON TABLE user TYPE string" },
            "fd": { "b": "DEFINE FIELD b ON TABLE user TYPE int" }
        });
        let t = parse_table_info("user", &info).unwrap();
        assert_eq!(t.fields.len(), 1);
        assert_eq!(t.fields[0].name, "a");
    }

    #[test]
    fn parse_field_readonly_case_insensitive() {
        let f = parse_field("x", "DEFINE FIELD x ON TABLE t TYPE string readonly").unwrap();
        assert!(f.readonly);
    }

    #[test]
    fn parse_index_hnsw_without_efc_m() {
        let idx = parse_index(
            "feat_idx",
            "DEFINE INDEX feat_idx ON TABLE doc COLUMNS features HNSW DIMENSION 64 DIST EUCLIDEAN TYPE F64",
        )
        .unwrap();
        assert!(idx.efc.is_none());
        assert!(idx.m.is_none());
        assert_eq!(idx.hnsw_distance, Some(HnswDistanceType::Euclidean));
        assert_eq!(idx.vector_type, Some(MTreeVectorType::F64));
    }

    #[test]
    fn extract_relation_endpoints_parses_correctly() {
        let (from, to) =
            extract_relation_endpoints("DEFINE TABLE likes TYPE RELATION FROM user TO post")
                .unwrap();
        assert_eq!(from, "user");
        assert_eq!(to, "post");
    }

    #[test]
    fn extract_relation_endpoints_missing_returns_none() {
        assert!(extract_relation_endpoints("DEFINE TABLE likes SCHEMAFULL").is_none());
    }
}
