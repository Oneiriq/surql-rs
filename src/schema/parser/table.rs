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
use super::permissions::parse_table_permissions;
use super::scan::{clauses, define_head, tokens, unquote_ident, Shape, Token};
use super::view::parse_view;
use super::{expect_object, pick_map, value_to_string_map};
use crate::error::{Result, SurqlError};
use crate::schema::changefeed::ChangeFeed;
use crate::schema::table::{TableDefinition, TableMode};

// --- Statement reader --------------------------------------------------------

/// The endpoints of a `TYPE RELATION` table.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct Relation {
    /// Tables named after `IN` / `FROM`.
    pub from: Vec<String>,
    /// Tables named after `OUT` / `TO`.
    pub to: Vec<String>,
}

/// The parts of a `DEFINE TABLE` statement the parsers read.
// One bool per independent flag the statement can carry.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct TableStatement<'a> {
    /// `Some` for a `TYPE RELATION` table.
    pub relation: Option<Relation>,
    /// Whether the `DROP` flag is set.
    pub drop: bool,
    /// Whether the table is `SCHEMAFULL`.
    pub schemafull: bool,
    /// The `CHANGEFEED` clause body.
    pub changefeed: Option<&'a str>,
    /// The `AS` clause body of a view, starting at `SELECT`.
    pub view: Option<&'a str>,
    /// The `PERMISSIONS` clause body.
    pub permissions: Option<&'a str>,
    /// Whether a relation is `ENFORCED` (a lightweight one always is).
    pub enforced: bool,
    /// Whether a relation is `LIGHTWEIGHT`.
    pub lightweight: bool,
    /// The `INLINE EDGES` cap.
    pub inline_edges: Option<u32>,
    /// The `INLINE REFERENCES` cap.
    pub inline_references: Option<u32>,
}

/// Clauses that can follow the table mode. The engine echoes them in this
/// order, `PERMISSIONS` last.
const TAIL_CLAUSES: &[(&str, Shape)] = &[
    ("COMMENT", Shape::Str),
    ("GRAPHQL_ALIAS", Shape::Str),
    ("GRAPHQL_DEPRECATED", Shape::Str),
    ("AS", Shape::Select),
    ("CHANGEFEED", Shape::Expr),
    ("PERMISSIONS", Shape::Expr),
];

/// Read a `DEFINE TABLE` statement.
///
/// The engine echoes `DEFINE TABLE <name> TYPE <NORMAL | ANY | RELATION IN a
/// | b OUT c [ENFORCED] [LIGHTWEIGHT]> [DROP] <SCHEMAFULL | SCHEMALESS>
/// [INLINE EDGES n] [INLINE REFERENCES n] [COMMENT …] [AS SELECT …]
/// [CHANGEFEED …] PERMISSIONS …`; the flags are read word by word, so a
/// table named after a keyword (`IN schemafull`) stays a name, and the rest
/// is split into clauses outside quotes and brackets.
pub(super) fn read_table(definition: &str) -> TableStatement<'_> {
    let rest = define_head(definition, "TABLE", false).map_or(definition, |head| head.rest);
    let toks = tokens(rest);
    let mut out = TableStatement::default();
    let mut i = 0;
    while let Some(token) = toks.get(i) {
        if token.is("TYPE") || token.is("NORMAL") || token.is("ANY") {
            i += 1;
        } else if token.is("ENFORCED") {
            out.enforced = true;
            i += 1;
        } else if token.is("LIGHTWEIGHT") {
            out.lightweight = true;
            out.enforced = true;
            i += 1;
        } else if token.is("INLINE") {
            let cap = toks
                .get(i + 2)
                .and_then(|t| t.text.trim_end_matches(';').parse::<u32>().ok());
            match toks.get(i + 1) {
                Some(kind) if kind.is("EDGES") => out.inline_edges = cap,
                Some(kind) if kind.is("REFERENCES") => out.inline_references = cap,
                _ => break,
            }
            i += 3;
        } else if token.is("RELATION") {
            out.relation.get_or_insert_with(Relation::default);
            i += 1;
        } else if out.relation.is_some() && (token.is("IN") || token.is("FROM")) {
            let (names, next) = read_names(&toks, i + 1);
            if let Some(relation) = out.relation.as_mut() {
                relation.from = names;
            }
            i = next;
        } else if out.relation.is_some() && (token.is("OUT") || token.is("TO")) {
            let (names, next) = read_names(&toks, i + 1);
            if let Some(relation) = out.relation.as_mut() {
                relation.to = names;
            }
            i = next;
        } else if token.is("DROP") {
            out.drop = true;
            i += 1;
        } else if token.is("SCHEMAFULL") {
            out.schemafull = true;
            i += 1;
        } else if token.is("SCHEMALESS") {
            out.schemafull = false;
            i += 1;
        } else {
            break;
        }
    }
    let tail = toks.get(i).and_then(|t| rest.get(t.start..)).unwrap_or("");
    let found = clauses(tail, TAIL_CLAUSES);
    let last = |keyword: &str| {
        found
            .iter()
            .rev()
            .find(|c| c.keyword == keyword)
            .map(|c| c.body)
    };
    out.changefeed = last("CHANGEFEED");
    out.view = last("AS");
    out.permissions = last("PERMISSIONS");
    out
}

/// Read an `a | b | c` table list starting at token `at`, returning the
/// unquoted names and the index of the first token after the list.
fn read_names(toks: &[Token<'_>], at: usize) -> (Vec<String>, usize) {
    let mut raw = String::new();
    let mut i = at;
    while let Some(token) = toks.get(i) {
        let continues = raw.is_empty() || raw.ends_with('|') || token.text.starts_with('|');
        if !continues {
            break;
        }
        raw.push(' ');
        raw.push_str(token.text.trim_end_matches(';'));
        i += 1;
    }
    let names = raw
        .split('|')
        .map(|name| unquote_ident(name.trim()))
        .filter(|name| !name.is_empty())
        .collect();
    (names, i)
}

// --- Public parsers ----------------------------------------------------------

/// Parse the `CHANGEFEED` clause out of a `DEFINE TABLE` statement.
///
/// Returns `None` for a table with no change feed. The clause is found
/// outside quotes, so a `COMMENT 'no changefeed needed'` is not one.
pub fn parse_changefeed(definition: &str) -> Option<ChangeFeed> {
    let body = read_table(definition).changefeed?;
    let toks = tokens(body);
    let duration = toks.first()?.text.trim_end_matches(';');
    if duration.is_empty() {
        return None;
    }
    let include_original = toks
        .windows(2)
        .any(|pair| matches!(pair, [a, b] if a.is("INCLUDE") && b.is("ORIGINAL")));
    Some(ChangeFeed::new(duration).include_original(include_original))
}

/// Parse the `DEFINE TABLE` statement into a [`TableMode`].
///
/// The `DROP` flag wins, since the engine echoes it next to the schema
/// mode (`TYPE ANY DROP SCHEMALESS`). An empty input defaults to
/// [`TableMode::Schemaless`], mirroring the Python module's fallback.
pub fn parse_table_mode(definition: &str) -> TableMode {
    let table = read_table(definition);
    if table.drop {
        TableMode::Drop
    } else if table.schemafull {
        TableMode::Schemafull
    } else {
        TableMode::Schemaless
    }
}

/// The complete definition for one table from the two `INFO` levels:
/// the database's `DEFINE TABLE` echo carries mode and permissions,
/// and the table's own `INFO FOR TABLE` carries fields, indexes, and
/// events. `INFO FOR DB` alone yields fieldless tables, which is a
/// trap for anyone diffing against it.
///
/// Delegates to [`parse_table_info`], so `table_info` may be the bare
/// INFO object or the one-element array
/// [`query`](crate::DatabaseClient::query) returns for the statement.
///
/// Returns [`crate::error::SurqlError::SchemaParse`] when `table_info`
/// is not a JSON object (or a one-statement array holding one).
pub fn parse_table_full(
    table_name: &str,
    db_define: &str,
    table_info: &Value,
) -> Result<TableDefinition> {
    parse_table_info(table_name, table_info, Some(db_define))
}

/// Parse a SurrealDB `INFO FOR TABLE` response into a [`TableDefinition`].
///
/// Accepts either the short-key shape (`fd`, `ix`, `ev`) or the long-key shape
/// (`fields`, `indexes`, `events`). Unknown enum values surface as the default
/// variant (for example `FieldType::Any` for unknown types), matching the
/// Python behaviour.
///
/// Accepts either the INFO object itself or the value
/// [`query`](crate::DatabaseClient::query) returns for
/// `INFO FOR TABLE <name>;`, which wraps each statement's result in an
/// array. An INFO response is never itself an array, so the two shapes
/// cannot be confused; callers that index the wrapper themselves keep
/// working, since indexing yields the bare object.
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
/// is not a JSON object (or a one-statement array holding one).
pub fn parse_table_info(
    table_name: &str,
    info: &Value,
    define_table: Option<&str>,
) -> Result<TableDefinition> {
    let info = match info.as_array() {
        Some(items) => items.first().ok_or_else(|| SurqlError::SchemaParse {
            reason: format!("INFO FOR TABLE {table_name}: response held no statement result"),
        })?,
        None => info,
    };
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

    let statement = read_table(tb_definition);
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
        inline_edges: statement.inline_edges,
        inline_references: statement.inline_references,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The SurrealDB 3.3 echoes of the relation flags and inline caches.
    #[test]
    fn relation_flags_and_inline_caps_read_back() {
        let knows = read_table(
            "DEFINE TABLE knows TYPE RELATION IN person OUT person ENFORCED LIGHTWEIGHT \
             SCHEMALESS PERMISSIONS NONE",
        );
        assert!(knows.enforced && knows.lightweight);
        assert_eq!(knows.permissions, Some("NONE"));

        let likes = read_table(
            "DEFINE TABLE likes TYPE RELATION IN person OUT person ENFORCED SCHEMALESS INLINE \
             EDGES 8 INLINE REFERENCES 4 PERMISSIONS FULL",
        );
        assert!(likes.enforced && !likes.lightweight);
        assert_eq!(
            (likes.inline_edges, likes.inline_references),
            (Some(8), Some(4))
        );
        assert_eq!(likes.permissions, Some("FULL"));
        assert_eq!(
            likes.relation.map(|r| (r.from, r.to)),
            Some((vec!["person".to_string()], vec!["person".to_string()]))
        );

        let doc =
            read_table("DEFINE TABLE doc TYPE NORMAL SCHEMAFULL INLINE EDGES 16 PERMISSIONS NONE");
        assert!(doc.schemafull);
        assert_eq!((doc.inline_edges, doc.inline_references), (Some(16), None));

        // A table named `inline` is a name, not the clause.
        let named = read_table("DEFINE TABLE inline TYPE NORMAL SCHEMAFULL PERMISSIONS NONE");
        assert!(named.schemafull);
        assert_eq!(named.inline_edges, None);
    }

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
    fn a_comment_mentioning_changefeed_is_not_one() {
        assert!(parse_changefeed(
            "DEFINE TABLE t2 TYPE NORMAL SCHEMAFULL COMMENT 'no changefeed needed' PERMISSIONS NONE"
        )
        .is_none());
        let cf = parse_changefeed(
            "DEFINE TABLE t4 TYPE NORMAL SCHEMAFULL COMMENT \"it's\" CHANGEFEED 1d PERMISSIONS FULL",
        )
        .expect("changefeed");
        assert_eq!(cf.duration, "1d");
    }

    #[test]
    fn drop_is_read_next_to_the_schema_mode() {
        // Exact 3.0.5 echo of `DEFINE TABLE t3 DROP SCHEMALESS`.
        assert_eq!(
            parse_table_mode("DEFINE TABLE t3 TYPE ANY DROP SCHEMALESS PERMISSIONS FULL"),
            TableMode::Drop
        );
        assert_eq!(
            parse_table_mode("DEFINE TABLE schemafull_log TYPE NORMAL SCHEMALESS PERMISSIONS NONE"),
            TableMode::Schemaless
        );
        assert_eq!(
            parse_table_mode("DEFINE TABLE t TYPE NORMAL SCHEMAFULL COMMENT 'DROP me'"),
            TableMode::Schemafull
        );
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
