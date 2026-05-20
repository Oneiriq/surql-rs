//! Graph-edge `INFO FOR TABLE` parser.
//!
//! Counterpart to [`super::parse_table_info`] for tables defined via
//! `edge_schema` / [`EdgeDefinition`]. Port of `surql-py`'s 1.6.4
//! `parse_edge_info` and `surql`'s 1.5.0 `parseEdgeInfo`.
//!
//! Edges round-trip through SurrealDB as regular tables in
//! `INFO FOR DB.tables`; the only thing that makes them edges is the
//! `TYPE RELATION FROM <x> TO <y>` clause on the `DEFINE TABLE`
//! statement. Without an edge-aware parser, a drift detector using
//! [`super::parse_table_info`] against an edge table would see it as a
//! `Schemaless` table missing every field-level diff signal an edge
//! expects (mode, from/to constraints, auto `in`/`out` proxies).
//!
//! ## What this parser handles
//!
//! - **Edge mode detection** — `TYPE RELATION` resolves to
//!   [`EdgeMode::Relation`]; the `SCHEMAFULL` keyword resolves to
//!   [`EdgeMode::Schemafull`]; anything else (including the empty
//!   string when the caller passed no `define_table`) resolves to
//!   [`EdgeMode::Schemaless`]. `TYPE RELATION` wins over `SCHEMAFULL`
//!   when both are present — v3 accepts
//!   `DEFINE TABLE <e> TYPE RELATION SCHEMAFULL FROM x TO y` and the
//!   edge mode is what matters for downstream diffing.
//!
//! - **FROM / TO endpoints** — extracted independently so a malformed
//!   live definition that lost one clause surfaces as missing-endpoint
//!   drift instead of a parse failure. The emitter writes both when
//!   `TYPE RELATION` is set, but the parser stays permissive on read.
//!
//! - **Auto `in` / `out` field stripping** — on `Relation`-mode edges
//!   SurrealDB auto-emits `in` and `out` `FIELD` declarations. They are
//!   implicit when `TYPE RELATION` is set, so the code-side
//!   [`EdgeDefinition`] does not declare them. The parser strips them on
//!   read so round-trip diffs do not flag them as orphan additions.
//!
//! - **Per-action `PERMISSIONS`** — delegated to
//!   [`super::parse_table_permissions`], including the comma-joined
//!   `FOR select, create, update, delete WHERE …` shape v3 emits when
//!   several actions share a rule.
//!
//! ## v3 caller pattern
//!
//! SurrealDB v3's `INFO FOR TABLE` does **not** include the table-level
//! `DEFINE TABLE` statement — the `DEFINE TABLE` string only appears in
//! `INFO FOR DB`'s `tables.<name>` entry. Callers should fetch
//! `INFO FOR DB` once and pass the corresponding `tables[<name>]`
//! string as `define_table`; without it, mode + FROM/TO + PERMISSIONS
//! cannot be recovered and default to `Schemaless` / `None`.

use std::sync::OnceLock;

use regex::Regex;
use serde_json::Value;

use super::event::parse_events;
use super::field::parse_fields;
use super::index::parse_indexes;
use super::permissions::parse_table_permissions;
use super::{expect_object, pick_map, regex_case_insensitive, value_to_string_map};
use crate::error::Result;
use crate::schema::edge::{EdgeDefinition, EdgeMode};

// --- Regex accessors ---------------------------------------------------------

/// Match the `TYPE RELATION` keyword anywhere in a `DEFINE TABLE` string.
/// Word-boundary anchored so `TYPE RELATIONAL_SOMETHING` does not match.
fn type_relation_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"\bTYPE\s+RELATION\b"))
}

/// Match a `FROM <ident>` clause, independent of `TO`. Identifier
/// characters mirror SurrealDB's table-name grammar
/// (`[A-Za-z_][A-Za-z0-9_]*`).
fn edge_from_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"\bFROM\s+([A-Za-z_][A-Za-z0-9_]*)"))
}

/// Match a `TO <ident>` clause, independent of `FROM`.
fn edge_to_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"\bTO\s+([A-Za-z_][A-Za-z0-9_]*)"))
}

// --- Edge mode + endpoint helpers --------------------------------------------

/// Resolve the [`EdgeMode`] encoded in a `DEFINE TABLE` statement.
///
/// - `TYPE RELATION ...` → [`EdgeMode::Relation`]
/// - `SCHEMAFULL` (without `TYPE RELATION`) → [`EdgeMode::Schemafull`]
/// - anything else (empty / `SCHEMALESS` / missing) → [`EdgeMode::Schemaless`]
///
/// `TYPE RELATION` wins because v3 accepts
/// `DEFINE TABLE <e> TYPE RELATION SCHEMAFULL FROM x TO y` and the edge
/// mode is what matters for downstream diffing.
pub(super) fn parse_edge_mode(definition: &str) -> EdgeMode {
    if definition.is_empty() {
        return EdgeMode::Schemaless;
    }
    if type_relation_regex().is_match(definition) {
        return EdgeMode::Relation;
    }
    if definition.to_ascii_uppercase().contains("SCHEMAFULL") {
        return EdgeMode::Schemafull;
    }
    EdgeMode::Schemaless
}

/// Extract `FROM <table>` and `TO <table>` independently. Returns
/// `(None, None)` for an empty input and `(Some, None)` / `(None, Some)`
/// when only one clause is present — the parser is permissive on read
/// so a malformed live definition surfaces as missing-endpoint drift.
pub(super) fn parse_edge_endpoints(definition: &str) -> (Option<String>, Option<String>) {
    if definition.is_empty() {
        return (None, None);
    }
    let from = edge_from_regex()
        .captures(definition)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string());
    let to = edge_to_regex()
        .captures(definition)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string());
    (from, to)
}

// --- Public parser -----------------------------------------------------------

/// Parse a SurrealDB `INFO FOR TABLE` response that represents a graph
/// edge into an [`EdgeDefinition`].
///
/// `define_table` is the `DEFINE TABLE <name> ...` statement string,
/// fetched separately from `INFO FOR DB`'s `tables.<name>` entry. Pass
/// it on SurrealDB v3 — without it, the edge defaults to
/// [`EdgeMode::Schemaless`] with no endpoints and no permissions, and
/// drift detection on the edge will be incomplete.
///
/// For `Relation`-mode edges the auto-emitted `in` and `out` fields
/// SurrealDB stores are stripped on parse — they are implicit when
/// `TYPE RELATION` is set, so the code-side [`EdgeDefinition`] does not
/// declare them. Without this strip, `diff_edges(parsed, code)` would
/// flag every edge with a spurious "added field" diff.
///
/// Returns [`crate::error::SurqlError::SchemaParse`] when the top-level
/// JSON value is not an object.
///
/// ## Example
///
/// ```
/// use serde_json::json;
/// use surql::schema::edge::EdgeMode;
/// use surql::schema::parser::parse_edge_info;
///
/// let info = json!({
///     "fields": {
///         "weight": "DEFINE FIELD weight ON likes TYPE float",
///         "in": "DEFINE FIELD in ON likes TYPE record<user>",
///         "out": "DEFINE FIELD out ON likes TYPE record<product>"
///     }
/// });
/// let define = "DEFINE TABLE likes TYPE RELATION FROM user TO product";
/// let edge = parse_edge_info("likes", &info, Some(define)).unwrap();
/// assert_eq!(edge.mode, EdgeMode::Relation);
/// assert_eq!(edge.from_table.as_deref(), Some("user"));
/// assert_eq!(edge.to_table.as_deref(), Some("product"));
/// // Auto-emitted in/out fields are stripped on RELATION edges.
/// assert_eq!(edge.fields.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
///            vec!["weight"]);
/// ```
pub fn parse_edge_info(
    edge_name: &str,
    info: &Value,
    define_table: Option<&str>,
) -> Result<EdgeDefinition> {
    let obj = expect_object(info, &format!("INFO FOR TABLE {edge_name}"))?;

    // The caller-supplied DEFINE TABLE wins; fall back to the legacy
    // `tb` key inside the INFO FOR TABLE response (SurrealDB v1/v2
    // shape). v3 does not surface `tb` here — without `define_table`
    // mode + endpoints + permissions are lost.
    let tb_definition =
        define_table.unwrap_or_else(|| obj.get("tb").and_then(Value::as_str).unwrap_or(""));

    let mode = parse_edge_mode(tb_definition);
    let (from_table, to_table) = parse_edge_endpoints(tb_definition);
    let permissions = parse_table_permissions(tb_definition);

    let fields_value = pick_map(obj, &["fields", "fd"]);
    let mut fields = fields_value
        .map(|v| parse_fields(&value_to_string_map(v)))
        .unwrap_or_default();
    // Strip the auto-emitted `in` / `out` proxy fields on Relation-mode
    // edges. They are implicit when `TYPE RELATION` is set, so the
    // code-side EdgeDefinition does not declare them and round-trip
    // diffs would flag them as orphan additions.
    if mode == EdgeMode::Relation {
        fields.retain(|f| f.name != "in" && f.name != "out");
    }

    let indexes_value = pick_map(obj, &["indexes", "ix"]);
    let indexes = indexes_value
        .map(|v| parse_indexes(&value_to_string_map(v)))
        .unwrap_or_default();

    let events_value = pick_map(obj, &["events", "ev"]);
    let events = events_value
        .map(|v| parse_events(&value_to_string_map(v)))
        .unwrap_or_default();

    Ok(EdgeDefinition {
        name: edge_name.to_string(),
        mode,
        from_table,
        to_table,
        fields,
        indexes,
        events,
        permissions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rejects_non_object_info() {
        assert!(parse_edge_info("likes", &Value::Null, None).is_err());
        assert!(parse_edge_info("likes", &json!("not an object"), None).is_err());
        assert!(parse_edge_info("likes", &json!([1, 2, 3]), None).is_err());
    }

    #[test]
    fn extracts_from_to_and_fields_from_relation() {
        let info = json!({
            "tb": "DEFINE TABLE likes TYPE RELATION FROM user TO product",
            "fields": {
                "weight": "DEFINE FIELD weight ON TABLE likes TYPE float DEFAULT 1.0"
            }
        });
        let e = parse_edge_info("likes", &info, None).unwrap();
        assert_eq!(e.mode, EdgeMode::Relation);
        assert_eq!(e.from_table.as_deref(), Some("user"));
        assert_eq!(e.to_table.as_deref(), Some("product"));
        assert_eq!(e.fields.len(), 1);
        assert_eq!(e.fields[0].name, "weight");
    }

    #[test]
    fn uses_define_table_override_when_supplied() {
        // SurrealDB v3's `INFO FOR TABLE` does not include the table-level
        // DEFINE — drift detectors must pass it explicitly so the mode,
        // FROM/TO, and PERMISSIONS round-trip.
        let info = json!({ "fields": {} });
        let e = parse_edge_info(
            "likes",
            &info,
            Some("DEFINE TABLE likes TYPE RELATION FROM user TO product"),
        )
        .unwrap();
        assert_eq!(e.mode, EdgeMode::Relation);
        assert_eq!(e.from_table.as_deref(), Some("user"));
        assert_eq!(e.to_table.as_deref(), Some("product"));
    }

    #[test]
    fn detects_schemafull_edge_mode() {
        let e = parse_edge_info(
            "likes",
            &json!({ "fields": {} }),
            Some("DEFINE TABLE likes SCHEMAFULL"),
        )
        .unwrap();
        assert_eq!(e.mode, EdgeMode::Schemafull);
        assert_eq!(e.from_table, None);
        assert_eq!(e.to_table, None);
    }

    #[test]
    fn detects_schemaless_edge_mode_when_no_keyword() {
        let e = parse_edge_info(
            "likes",
            &json!({ "fields": {} }),
            Some("DEFINE TABLE likes"),
        )
        .unwrap();
        assert_eq!(e.mode, EdgeMode::Schemaless);
    }

    #[test]
    fn defaults_to_schemaless_when_no_define_table_provided() {
        let e = parse_edge_info("likes", &json!({ "fields": {} }), None).unwrap();
        assert_eq!(e.mode, EdgeMode::Schemaless);
    }

    #[test]
    fn type_relation_wins_over_schemafull() {
        let e = parse_edge_info(
            "likes",
            &json!({ "fields": {} }),
            Some("DEFINE TABLE likes TYPE RELATION SCHEMAFULL FROM user TO product"),
        )
        .unwrap();
        assert_eq!(e.mode, EdgeMode::Relation);
        assert_eq!(e.from_table.as_deref(), Some("user"));
        assert_eq!(e.to_table.as_deref(), Some("product"));
    }

    #[test]
    fn strips_auto_in_and_out_fields_on_relation_edges() {
        let info = json!({
            "tb": "DEFINE TABLE likes TYPE RELATION FROM user TO product",
            "fields": {
                "in": "DEFINE FIELD in ON likes TYPE record",
                "out": "DEFINE FIELD out ON likes TYPE record",
                "weight": "DEFINE FIELD weight ON likes TYPE float"
            }
        });
        let e = parse_edge_info("likes", &info, None).unwrap();
        assert_eq!(e.mode, EdgeMode::Relation);
        assert_eq!(
            e.fields.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
            vec!["weight"]
        );
    }

    #[test]
    fn keeps_in_and_out_fields_on_non_relation_edges() {
        // No auto-emission on non-RELATION edges; if a consumer
        // explicitly declares `in` / `out`, they should round-trip.
        let info = json!({
            "fields": {
                "in": "DEFINE FIELD in ON custom TYPE record",
                "out": "DEFINE FIELD out ON custom TYPE record"
            }
        });
        let e = parse_edge_info("custom", &info, Some("DEFINE TABLE custom SCHEMAFULL")).unwrap();
        assert_eq!(e.mode, EdgeMode::Schemafull);
        let mut names: Vec<&str> = e.fields.iter().map(|f| f.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, vec!["in", "out"]);
    }

    #[test]
    fn parses_per_action_permissions_from_define_table() {
        let define = "DEFINE TABLE likes TYPE RELATION FROM user TO product \
                      PERMISSIONS FOR select WHERE $auth.id != NONE \
                      FOR create WHERE $auth.id = in";
        let e = parse_edge_info("likes", &json!({ "fields": {} }), Some(define)).unwrap();
        let perms = e.permissions.unwrap();
        assert_eq!(
            perms.get("select").map(String::as_str),
            Some("$auth.id != NONE")
        );
        assert_eq!(
            perms.get("create").map(String::as_str),
            Some("$auth.id = in")
        );
    }

    #[test]
    fn parses_v3_comma_joined_permissions() {
        // v3 collapses `FOR select WHERE r FOR create WHERE r ...` to
        // `FOR select, create, update, delete WHERE r`. The parser must
        // explode that into the same per-action map shape, otherwise
        // every consumer with collapsed rules sees a false-positive
        // MODIFY_PERMISSIONS drift.
        let define = "DEFINE TABLE likes TYPE RELATION FROM user TO product \
                      PERMISSIONS FOR select, create, update, delete WHERE tenant = $auth.tenant";
        let e = parse_edge_info("likes", &json!({ "fields": {} }), Some(define)).unwrap();
        let perms = e.permissions.unwrap();
        assert_eq!(
            perms.get("select").map(String::as_str),
            Some("tenant = $auth.tenant")
        );
        assert_eq!(
            perms.get("create").map(String::as_str),
            Some("tenant = $auth.tenant")
        );
        assert_eq!(
            perms.get("update").map(String::as_str),
            Some("tenant = $auth.tenant")
        );
        assert_eq!(
            perms.get("delete").map(String::as_str),
            Some("tenant = $auth.tenant")
        );
    }

    #[test]
    fn parses_indexes_and_events_on_edges() {
        let info = json!({
            "tb": "DEFINE TABLE likes TYPE RELATION FROM user TO product",
            "fields": {},
            "indexes": {
                "unique_pair": "DEFINE INDEX unique_pair ON likes COLUMNS in, out UNIQUE"
            },
            "events": {
                "notify": "DEFINE EVENT notify ON likes WHEN $event = \"CREATE\" THEN { LET $_ = 1; }"
            }
        });
        let e = parse_edge_info("likes", &info, None).unwrap();
        assert_eq!(e.indexes.len(), 1);
        assert_eq!(e.indexes[0].name, "unique_pair");
        assert_eq!(e.events.len(), 1);
        assert_eq!(e.events[0].name, "notify");
    }

    #[test]
    fn handles_field_name_collision_with_clause_keyword() {
        // Regression target — a field named `default` was previously
        // ambiguous on read because the parser's clause-keyword scan
        // started at byte 0 of the DEFINE FIELD string. The field
        // parser handles the prefix-skip; this test confirms edges
        // inherit the fix.
        let info = json!({
            "fields": { "default": "DEFINE FIELD default ON custom TYPE bool DEFAULT false" }
        });
        let e = parse_edge_info("custom", &info, Some("DEFINE TABLE custom SCHEMAFULL")).unwrap();
        assert_eq!(e.fields.len(), 1);
        assert_eq!(e.fields[0].name, "default");
    }

    #[test]
    fn returns_endpoints_as_none_when_define_table_lacks_from_to() {
        let e = parse_edge_info(
            "likes",
            &json!({ "fields": {} }),
            Some("DEFINE TABLE likes TYPE RELATION"),
        )
        .unwrap();
        assert_eq!(e.mode, EdgeMode::Relation);
        assert_eq!(e.from_table, None);
        assert_eq!(e.to_table, None);
    }
}
