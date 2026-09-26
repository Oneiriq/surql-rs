//! Schema diffing engine.
//!
//! Port of `surql/migration/diff.py`. Compares two schema snapshots
//! (code-side vs database-side, or two code versions) and produces a list
//! of [`SchemaDiff`] entries describing every additive, destructive, or
//! modifying schema change.
//!
//! ## Public API
//!
//! The public entrypoints are free functions that operate on slices of
//! schema definitions and return a [`Vec<SchemaDiff>`]:
//!
//! - [`diff_tables`] — compare two sets of [`TableDefinition`]s.
//! - [`diff_fields`] — compare two sets of [`FieldDefinition`]s for a table.
//! - [`diff_indexes`] — compare two sets of [`IndexDefinition`]s for a table.
//! - [`diff_events`] — compare two sets of [`EventDefinition`]s for a table.
//! - [`diff_permissions`] — compare two permission maps for a table.
//! - [`diff_edges`] — compare two sets of [`EdgeDefinition`]s.
//! - [`diff_buckets`] / [`diff_analyzers`] — re-exported from
//!   [`crate::migration::diff_objects`], which holds every database-level
//!   object diff.
//! - [`diff_schemas`] — aggregate diff across full [`SchemaSnapshot`]s.
//!
//! ## Deviation from Python
//!
//! In the Python implementation the per-category diff helpers take a single
//! pair of objects (`old`, `new`). The Rust port exposes slice-based
//! signatures that internally compute the pair-wise comparison by name.
//! The old pair-wise helpers are preserved as `diff_*_pair` functions for
//! callers that want the fine-grained semantics (for example the migration
//! generator). Functions that render SurrealQL require an explicit `table`
//! parameter because the field / index / event / permission slices do not
//! carry table context on their own.
//!
//! ## Expression normalisation
//!
//! Field expressions (`assertion`, `default`, `value`) are compared using
//! whitespace-normalised equality so that cosmetic reformatting by the
//! database server does not produce spurious diffs.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::error::{Result, SurqlError};
pub use crate::migration::diff_objects::{diff_analyzers, diff_buckets};
use crate::migration::diff_objects::{diff_functions, diff_params, diff_sequences};
use crate::migration::models::{DiffOperation, SchemaDiff};
use crate::schema::bucket::BucketDefinition;
use crate::schema::edge::{EdgeDefinition, EdgeMode};
use crate::schema::fields::{FieldDefinition, FieldType};
use crate::schema::function::FunctionDefinition;
use crate::schema::index_vector::{
    DISKANN_DEFAULT_ALPHA, DISKANN_DEFAULT_DEGREE, DISKANN_DEFAULT_L_BUILD,
};
use crate::schema::param::ParamDefinition;
use crate::schema::sequence::SequenceDefinition;
use crate::schema::table::{
    DiskAnnDistanceType, EventDefinition, HnswDistanceType, IndexDefinition, IndexType,
    MTreeDistanceType, MTreeVectorType, TableDefinition,
};
use crate::schema::view::ViewDefinition;
use crate::types::escape::{quote_str, unquote_str};

/// Full schema snapshot passed to [`diff_schemas`].
///
/// Pairs table definitions with edge definitions to support a single-call
/// diff across all objects in a schema. Order is preserved as supplied but
/// comparisons are name-based so the input order does not affect output.
///
/// ## Examples
///
/// ```
/// use surql::migration::diff::SchemaSnapshot;
/// use surql::schema::table::table_schema;
///
/// let snapshot = SchemaSnapshot {
///     tables: vec![table_schema("user")],
///     edges: vec![],
///     ..Default::default()
/// };
/// assert_eq!(snapshot.tables.len(), 1);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaSnapshot {
    /// All tables known to this snapshot (in discovery order).
    #[serde(default)]
    pub tables: Vec<TableDefinition>,
    /// All edges known to this snapshot (in discovery order).
    #[serde(default)]
    pub edges: Vec<EdgeDefinition>,
    /// All object-storage buckets known to this snapshot (in discovery
    /// order). Defaults to empty so older snapshots without a `buckets` key
    /// still deserialise.
    #[serde(default)]
    pub buckets: Vec<BucketDefinition>,
    /// All text analyzers known to this snapshot (in discovery
    /// order). Defaults to empty so older snapshots still
    /// deserialise.
    #[serde(default)]
    pub analyzers: Vec<crate::schema::analyzer::AnalyzerDefinition>,
    /// All ID sequences known to this snapshot (in discovery order).
    /// Defaults to empty so older snapshots still deserialise.
    #[serde(default)]
    pub sequences: Vec<SequenceDefinition>,
    /// All custom functions known to this snapshot (in discovery order).
    /// Defaults to empty so older snapshots still deserialise.
    #[serde(default)]
    pub functions: Vec<FunctionDefinition>,
    /// All database-level params known to this snapshot (in discovery order).
    /// Defaults to empty so older snapshots still deserialise.
    #[serde(default)]
    pub params: Vec<ParamDefinition>,
}

impl SchemaSnapshot {
    /// Construct an empty snapshot.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Convenience constructor from tables + edges iterators (no buckets).
    pub fn from_parts<T, E>(tables: T, edges: E) -> Self
    where
        T: IntoIterator<Item = TableDefinition>,
        E: IntoIterator<Item = EdgeDefinition>,
    {
        Self {
            tables: tables.into_iter().collect(),
            edges: edges.into_iter().collect(),
            ..Default::default()
        }
    }

    /// Convenience constructor from tables + edges + buckets iterators.
    pub fn from_all_parts<T, E, B>(tables: T, edges: E, buckets: B) -> Self
    where
        T: IntoIterator<Item = TableDefinition>,
        E: IntoIterator<Item = EdgeDefinition>,
        B: IntoIterator<Item = BucketDefinition>,
    {
        Self {
            tables: tables.into_iter().collect(),
            edges: edges.into_iter().collect(),
            buckets: buckets.into_iter().collect(),
            ..Default::default()
        }
    }
}

/// Regex characters treated as safe in a default-value expression.
///
/// Preserved verbatim from the Python implementation to keep the validation
/// behaviour identical across runtimes.
const SAFE_DEFAULT_PATTERN: &str = concat!(
    r"^(",
    r"[a-zA-Z_][a-zA-Z0-9_]*(?:::[a-zA-Z_][a-zA-Z0-9_]*)*\([^;]*\)",
    r"|-?\d+(?:\.\d+)?",
    r"|true|false",
    r"|NONE|NULL",
    r"|'(?:[^'\\]|\\.)*'",
    r"|\$[a-zA-Z_][a-zA-Z0-9_]*",
    r")$",
);

fn safe_default_regex() -> &'static regex::Regex {
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(SAFE_DEFAULT_PATTERN).expect("valid regex"))
}

/// Validate that an event expression has no injection patterns.
///
/// Mirrors `_validate_event_expression` in Python: rejects statement
/// separators (`;`) and SQL comments (`--`).
///
/// # Errors
///
/// Returns [`SurqlError::Validation`] when the expression contains a
/// banned pattern.
pub fn validate_event_expression(expr: &str, label: &str) -> Result<()> {
    let stripped = expr.trim();
    if stripped.contains("; ") || stripped.contains(";--") || stripped.ends_with(';') {
        return Err(SurqlError::Validation {
            reason: format!(
                "Unsafe event {label}: {expr:?}. Event {label}s must not contain statement separators."
            ),
        });
    }
    if stripped.contains("--") {
        return Err(SurqlError::Validation {
            reason: format!(
                "Unsafe event {label}: {expr:?}. Event {label}s must not contain SQL comments."
            ),
        });
    }
    Ok(())
}

/// Validate that a default-value expression is one of the allowlisted forms.
///
/// # Errors
///
/// Returns [`SurqlError::Validation`] when the expression does not match
/// the safe-default pattern.
pub fn validate_default_value(default: &str) -> Result<()> {
    if !safe_default_regex().is_match(default.trim()) {
        return Err(SurqlError::Validation {
            reason: format!(
                "Unsafe default value expression: {default:?}. \
                 Defaults must be function calls, literals, or parameter references."
            ),
        });
    }
    Ok(())
}

/// Normalise an expression for semantic equality comparison.
///
/// A database server reformats expressions when it echoes them back, so the
/// comparison folds what the engine is free to change: runs of whitespace
/// become one space and the ends are trimmed, one level of wrapping
/// parentheses goes, `IS NONE` / `IS NOT NONE` read as `= NONE` /
/// `!= NONE`, a cast loses the space after it (`<string> id`), and a string
/// literal takes one quote style (the engine prints `"it's"` for what code
/// wrote as `'it\'s'`).
///
/// None of that reaches inside a quoted token. String literals, backtick
/// identifiers, and `⟨…⟩` record keys keep every byte, so `'a  b'` and
/// `'a b'` stay different.
///
/// ## Examples
///
/// ```
/// use surql::migration::diff::normalize_expression;
///
/// assert_eq!(normalize_expression("($value  IS NONE)"), "$value = NONE");
/// assert_eq!(normalize_expression("\"a  b\""), "'a  b'");
/// ```
#[must_use]
pub fn normalize_expression(expr: &str) -> String {
    let mut pieces: Vec<Piece> = split_quoted(expr)
        .into_iter()
        .map(|piece| match piece {
            Piece::Code(code) => Piece::Code(collapse_whitespace(&code)),
            Piece::Quoted(quoted) => Piece::Quoted(canonical_literal(quoted)),
        })
        .collect();
    trim_code_ends(&mut pieces);
    if strip_wrapping_parens(&mut pieces) {
        trim_code_ends(&mut pieces);
    }
    pieces
        .into_iter()
        .map(|piece| match piece {
            Piece::Code(code) => fold_cast_spacing(&fold_none_checks(&code)),
            Piece::Quoted(quoted) => quoted.text,
        })
        .collect()
}

/// One run of expression text.
enum Piece {
    /// SurrealQL outside any quotes, where formatting is free.
    Code(String),
    /// A quoted token, delimiters included, whose bytes are content.
    Quoted(Quoted),
}

/// A string literal, backtick identifier, or `⟨…⟩` record key.
struct Quoted {
    text: String,
    /// Whether the closing delimiter was found before the input ran out.
    terminated: bool,
}

/// Split `expr` into alternating code and quoted runs. Inside quotes a
/// backslash escapes the next character; an unterminated quote runs to the
/// end of the input.
fn split_quoted(expr: &str) -> Vec<Piece> {
    let mut pieces = Vec::new();
    let mut code = String::new();
    let mut chars = expr.chars();
    while let Some(ch) = chars.next() {
        let close = match ch {
            '\'' | '"' | '`' => ch,
            '⟨' => '⟩',
            _ => {
                code.push(ch);
                continue;
            }
        };
        if !code.is_empty() {
            pieces.push(Piece::Code(std::mem::take(&mut code)));
        }
        let mut text = String::from(ch);
        let mut terminated = false;
        let mut escaped = false;
        for inner in chars.by_ref() {
            text.push(inner);
            if escaped {
                escaped = false;
            } else if inner == '\\' {
                escaped = true;
            } else if inner == close {
                terminated = true;
                break;
            }
        }
        pieces.push(Piece::Quoted(Quoted { text, terminated }));
    }
    if !code.is_empty() {
        pieces.push(Piece::Code(code));
    }
    pieces
}

/// Collapse every run of whitespace to a single space.
fn collapse_whitespace(code: &str) -> String {
    let mut out = String::with_capacity(code.len());
    let mut in_space = false;
    for ch in code.chars() {
        if ch.is_whitespace() {
            if !in_space {
                out.push(' ');
            }
            in_space = true;
        } else {
            out.push(ch);
            in_space = false;
        }
    }
    out
}

/// Re-quote a complete string literal in the one style [`quote_str`]
/// renders; backtick identifiers, record keys, and unterminated text stay
/// as written.
fn canonical_literal(quoted: Quoted) -> Quoted {
    if !quoted.terminated {
        return quoted;
    }
    match unquote_str(&quoted.text) {
        Some(value) => Quoted {
            text: quote_str(&value),
            terminated: true,
        },
        None => quoted,
    }
}

/// Trim leading whitespace off the first run and trailing whitespace off
/// the last, when those runs are code.
fn trim_code_ends(pieces: &mut [Piece]) {
    if let Some(Piece::Code(first)) = pieces.first_mut() {
        *first = first.trim_start().to_owned();
    }
    if let Some(Piece::Code(last)) = pieces.last_mut() {
        *last = last.trim_end().to_owned();
    }
}

/// Drop one pair of parentheses that wraps the whole expression, returning
/// whether it did. Parentheses inside quotes do not count towards the
/// balance.
fn strip_wrapping_parens(pieces: &mut [Piece]) -> bool {
    let opens = matches!(pieces.first(), Some(Piece::Code(c)) if c.starts_with('('));
    let closes = matches!(pieces.last(), Some(Piece::Code(c)) if c.ends_with(')'));
    if !opens || !closes {
        return false;
    }
    let code_chars: usize = pieces
        .iter()
        .map(|piece| match piece {
            Piece::Code(code) => code.chars().count(),
            Piece::Quoted(_) => 0,
        })
        .sum();
    // The opening parenthesis wraps the whole expression when its depth
    // first returns to zero on the very last code character.
    let mut depth = 0usize;
    let mut seen = 0usize;
    for piece in pieces.iter() {
        let Piece::Code(code) = piece else { continue };
        for ch in code.chars() {
            seen += 1;
            match ch {
                '(' => depth += 1,
                ')' => depth = depth.saturating_sub(1),
                _ => {}
            }
            if depth == 0 && seen < code_chars {
                return false;
            }
        }
    }
    if let Some(Piece::Code(first)) = pieces.first_mut() {
        if let Some(rest) = first.strip_prefix('(') {
            *first = rest.to_owned();
        }
    }
    if let Some(Piece::Code(last)) = pieces.last_mut() {
        if let Some(rest) = last.strip_suffix(')') {
            *last = rest.to_owned();
        }
    }
    true
}

/// The engine echoes `IS NONE` as `= NONE` and `IS NOT NONE` as `!= NONE`.
fn fold_none_checks(code: &str) -> String {
    code.replace(" IS NOT NONE", " != NONE")
        .replace(" is not none", " != NONE")
        .replace(" IS NONE", " = NONE")
        .replace(" is none", " = NONE")
}

/// `<string> id` and `<string>id` are the same cast. Comparison spacing
/// (`a > b`) is kept by requiring the `<` side to look like a cast: a
/// non-empty run of ASCII letters and digits.
fn fold_cast_spacing(code: &str) -> String {
    let mut out = String::with_capacity(code.len());
    // Whether the text since the last `<` still looks like a cast name, and
    // how long it is; `None` outside any `<...>`.
    let mut cast: Option<(bool, usize)> = None;
    let mut chars = code.chars().peekable();
    while let Some(ch) = chars.next() {
        out.push(ch);
        match ch {
            '<' => cast = Some((true, 0)),
            '>' => {
                if matches!(cast, Some((true, len)) if len > 0) && chars.peek() == Some(&' ') {
                    chars.next();
                }
                cast = None;
            }
            other => {
                if let Some((looks_like_cast, len)) = cast.as_mut() {
                    *looks_like_cast = *looks_like_cast && other.is_ascii_alphanumeric();
                    *len += 1;
                }
            }
        }
    }
    out
}

/// Whether two optional expressions are the same once normalised with
/// [`normalize_expression`]. Two absent expressions are equal; an absent and
/// a present one are not.
///
/// ## Examples
///
/// ```
/// use surql::migration::diff::expr_eq;
///
/// assert!(expr_eq(Some("(a  >  1)"), Some("a > 1")));
/// assert!(expr_eq(None, None));
/// assert!(!expr_eq(Some("a > 1"), None));
/// ```
#[must_use]
pub fn expr_eq(a: Option<&str>, b: Option<&str>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => normalize_expression(x) == normalize_expression(y),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Public slice-based API
// ---------------------------------------------------------------------------

/// Compare two slices of tables and produce every required schema change.
///
/// Tables present in `code` but not in `db` are added (with all contained
/// fields/indexes/events/permissions). Tables present in `db` but not in
/// `code` are dropped. Tables present in both are recursively compared.
///
/// ## Examples
///
/// ```
/// use surql::migration::diff::diff_tables;
/// use surql::schema::table::table_schema;
///
/// let code = vec![table_schema("user")];
/// let db: Vec<_> = vec![];
/// let diffs = diff_tables(&code, &db);
/// assert_eq!(diffs.len(), 1);
/// ```
#[must_use]
pub fn diff_tables(code: &[TableDefinition], db: &[TableDefinition]) -> Vec<SchemaDiff> {
    let code_map = index_by_name(code, |t| t.name.as_str());
    let db_map = index_by_name(db, |t| t.name.as_str());
    let mut out: Vec<SchemaDiff> = Vec::new();

    // Added tables — present in code, absent in db.
    for name in sorted_keys(&code_map) {
        if !db_map.contains_key(name) {
            out.extend(generate_add_table_diffs(code_map[name]));
        }
    }
    // Dropped tables — present in db, absent in code.
    for name in sorted_keys(&db_map) {
        if !code_map.contains_key(name) {
            out.extend(generate_drop_table_diffs(db_map[name]));
        }
    }
    // Modified tables — present in both, diff recursively.
    for name in sorted_keys(&code_map) {
        if let Some(db_table) = db_map.get(name) {
            let code_table = code_map[name];
            out.extend(diff_table_pair_inner(code_table, db_table));
        }
    }
    out
}

/// Compare two field slices for the named table.
///
/// Added fields appear first (in `code` order), followed by dropped fields
/// (in `db` order), followed by modified fields (in `code` order).
///
/// ## Examples
///
/// ```
/// use surql::migration::diff::diff_fields;
/// use surql::schema::fields::{FieldDefinition, FieldType};
///
/// let code = vec![FieldDefinition::new("email", FieldType::String)];
/// let db: Vec<FieldDefinition> = vec![];
/// let diffs = diff_fields("user", &code, &db);
/// assert_eq!(diffs.len(), 1);
/// ```
#[must_use]
pub fn diff_fields(
    table: &str,
    code: &[FieldDefinition],
    db: &[FieldDefinition],
) -> Vec<SchemaDiff> {
    let code_map = index_by_name(code, |f| f.name.as_str());
    let db_map = index_by_name(db, |f| f.name.as_str());
    let mut out: Vec<SchemaDiff> = Vec::new();

    for f in code {
        if !db_map.contains_key(f.name.as_str()) {
            out.push(generate_add_field_diff(table, f));
        }
    }
    for f in db {
        if !code_map.contains_key(f.name.as_str()) {
            // The engine defines array-child fields (`name.*`) on its
            // own beside a declared array parent; those are engine
            // bookkeeping, never removals.
            if let Some(parent) = f.name.strip_suffix(".*") {
                if code_map.contains_key(parent) {
                    continue;
                }
            }
            out.push(generate_drop_field_diff(table, f));
        }
    }
    for f in code {
        if let Some(db_field) = db_map.get(f.name.as_str()) {
            if !fields_equal(f, db_field) {
                out.push(generate_modify_field_diff(table, db_field, f));
            }
        }
    }
    out
}

/// Compare two index slices for the named table.
///
/// Indexes only in `code` are added and indexes only in `db` dropped. An
/// index in both whose definition differs (see [`indexes_equal`]) is
/// re-defined with `DEFINE INDEX OVERWRITE` in both directions, reported as
/// [`DiffOperation::ModifyTable`] with [`SchemaDiff::index`] naming it.
#[must_use]
pub fn diff_indexes(
    table: &str,
    code: &[IndexDefinition],
    db: &[IndexDefinition],
) -> Vec<SchemaDiff> {
    let code_map = index_by_name(code, |i| i.name.as_str());
    let db_map = index_by_name(db, |i| i.name.as_str());
    let mut out: Vec<SchemaDiff> = Vec::new();

    for idx in code {
        if !db_map.contains_key(idx.name.as_str()) {
            out.push(generate_add_index_diff(table, idx));
        }
    }
    for idx in db {
        if !code_map.contains_key(idx.name.as_str()) {
            out.push(generate_drop_index_diff(table, idx));
        }
    }
    for idx in code {
        if let Some(db_idx) = db_map.get(idx.name.as_str()) {
            if !indexes_equal(idx, db_idx) {
                out.push(generate_modify_index_diff(table, db_idx, idx));
            }
        }
    }
    out
}

/// Compare two event slices for the named table.
///
/// Events only in `code` are added and events only in `db` dropped. An event
/// in both whose `WHEN` or `THEN` differs (see [`events_equal`]) is
/// re-defined with `DEFINE EVENT OVERWRITE` in both directions, reported as
/// [`DiffOperation::ModifyTable`] with [`SchemaDiff::event`] naming it.
#[must_use]
pub fn diff_events(
    table: &str,
    code: &[EventDefinition],
    db: &[EventDefinition],
) -> Vec<SchemaDiff> {
    let code_map = index_by_name(code, |e| e.name.as_str());
    let db_map = index_by_name(db, |e| e.name.as_str());
    let mut out: Vec<SchemaDiff> = Vec::new();

    for ev in code {
        if !db_map.contains_key(ev.name.as_str()) {
            out.push(generate_add_event_diff(table, ev));
        }
    }
    for ev in db {
        if !code_map.contains_key(ev.name.as_str()) {
            out.push(generate_drop_event_diff(table, ev));
        }
    }
    for ev in code {
        if let Some(db_ev) = db_map.get(ev.name.as_str()) {
            if !events_equal(ev, db_ev) {
                out.push(generate_modify_event_diff(table, db_ev, ev));
            }
        }
    }
    out
}

/// Compare two permission maps for the named table.
///
/// Emits at most one [`SchemaDiff`] describing the delta. If the maps are
/// equal, returns an empty vector. Both directions render
/// `ALTER TABLE <table> PERMISSIONS ...`, which replaces the permission set
/// and leaves every other clause of the table alone; an absent map renders
/// `PERMISSIONS NONE`, the engine's default for a table.
///
/// [`diff_tables`] and [`diff_edges`] carry permission changes as the full
/// `DEFINE TABLE OVERWRITE` statement instead, since they have the whole
/// definition to hand.
#[must_use]
pub fn diff_permissions(
    table: &str,
    code: Option<&BTreeMap<String, String>>,
    db: Option<&BTreeMap<String, String>>,
) -> Vec<SchemaDiff> {
    if permissions_equal(code, db) {
        return Vec::new();
    }
    vec![generate_modify_permissions_diff(table, code, db)]
}

/// Compare two edge slices.
///
/// Same high-level behaviour as [`diff_tables`]: added edges produce add
/// diffs for the edge and all of its contained objects; dropped edges
/// produce drop diffs; edges present in both are recursively compared on
/// fields/indexes/events/permissions. A change to an edge's own shape (its
/// mode, or a relation's `FROM` / `TO` table) re-defines it with
/// `DEFINE TABLE OVERWRITE`, reported as [`DiffOperation::ModifyTable`].
#[must_use]
pub fn diff_edges(code: &[EdgeDefinition], db: &[EdgeDefinition]) -> Vec<SchemaDiff> {
    let code_map = index_by_name(code, |e| e.name.as_str());
    let db_map = index_by_name(db, |e| e.name.as_str());
    let mut out: Vec<SchemaDiff> = Vec::new();

    for name in sorted_keys(&code_map) {
        if !db_map.contains_key(name) {
            out.extend(generate_add_edge_diffs(code_map[name]));
        }
    }
    for name in sorted_keys(&db_map) {
        if !code_map.contains_key(name) {
            out.extend(generate_drop_edge_diffs(db_map[name]));
        }
    }
    for name in sorted_keys(&code_map) {
        if let Some(db_edge) = db_map.get(name) {
            let code_edge = code_map[name];
            out.extend(diff_edge_pair_inner(code_edge, db_edge));
        }
    }
    out
}

/// Diff two complete snapshots and return every change required to make
/// `db` look like `code`.
///
/// The diffs come in the order a migration can apply them, and a rollback
/// (which runs the backward statements in reverse) can undo them:
///
/// 1. functions, params, sequences, analyzers, and buckets being added or
///    changed, so the tables that use them find them defined: a full-text
///    index cannot build over existing rows until its analyzer exists, and
///    a field backfill can call a function or read a param;
/// 2. every table and edge being dropped, before anything is defined, so a
///    name that turns from an edge into a table (or back) is free again;
/// 3. the remaining table changes, then the remaining edge changes;
/// 4. the database-level objects being dropped, last and in the reverse
///    kind order, once nothing that used them is left (the engine refuses
///    to remove an analyzer a full-text index still names).
#[must_use]
pub fn diff_schemas(code: &SchemaSnapshot, db: &SchemaSnapshot) -> Vec<SchemaDiff> {
    let objects = [
        diff_functions(&code.functions, &db.functions),
        diff_params(&code.params, &db.params),
        diff_sequences(&code.sequences, &db.sequences),
        diff_analyzers(&code.analyzers, &db.analyzers),
        diff_buckets(&code.buckets, &db.buckets),
    ];
    let mut out = Vec::new();
    let mut object_drops = Vec::new();
    for diffs in objects {
        let (drops, rest): (Vec<SchemaDiff>, Vec<SchemaDiff>) =
            diffs.into_iter().partition(|d| is_object_drop(d.operation));
        out.extend(rest);
        object_drops.push(drops);
    }
    let (table_drops, table_rest): (Vec<SchemaDiff>, Vec<SchemaDiff>) =
        diff_tables(&code.tables, &db.tables)
            .into_iter()
            .chain(diff_edges(&code.edges, &db.edges))
            .partition(|d| d.operation == DiffOperation::DropTable);
    out.extend(table_drops);
    out.extend(table_rest);
    out.extend(object_drops.into_iter().rev().flatten());
    out
}

/// Whether `operation` removes a database-level object.
fn is_object_drop(operation: DiffOperation) -> bool {
    matches!(
        operation,
        DiffOperation::DropFunction
            | DiffOperation::DropParam
            | DiffOperation::DropSequence
            | DiffOperation::DropAnalyzer
            | DiffOperation::DropBucket
    )
}

// ---------------------------------------------------------------------------
// Pair-wise helpers (preserved for migration-generator callers and tests)
// ---------------------------------------------------------------------------

/// Compare a single pair of tables, handling add/drop/modify.
///
/// Matches the semantics of the Python `diff_tables(old, new)` helper: pass
/// `None` for "table does not exist on that side".
#[must_use]
pub fn diff_table_pair(
    code: Option<&TableDefinition>,
    db: Option<&TableDefinition>,
) -> Vec<SchemaDiff> {
    match (code, db) {
        (Some(code), None) => generate_add_table_diffs(code),
        (None, Some(db)) => generate_drop_table_diffs(db),
        (Some(code), Some(db)) => diff_table_pair_inner(code, db),
        (None, None) => Vec::new(),
    }
}

/// Compare a single pair of edges, handling add/drop/modify.
#[must_use]
pub fn diff_edge_pair(
    code: Option<&EdgeDefinition>,
    db: Option<&EdgeDefinition>,
) -> Vec<SchemaDiff> {
    match (code, db) {
        (Some(code), None) => generate_add_edge_diffs(code),
        (None, Some(db)) => generate_drop_edge_diffs(db),
        (Some(code), Some(db)) => diff_edge_pair_inner(code, db),
        (None, None) => Vec::new(),
    }
}

fn diff_table_pair_inner(code: &TableDefinition, db: &TableDefinition) -> Vec<SchemaDiff> {
    let mut out = diff_fields(&code.name, &code.fields, &db.fields);
    out.extend(diff_indexes(&code.name, &code.indexes, &db.indexes));
    out.extend(diff_events(&code.name, &code.events, &db.events));
    out.extend(diff_table_body(code, db));
    if !permissions_equal(code.permissions.as_ref(), db.permissions.as_ref()) {
        // The full definition replaces: a permissions-only DEFINE
        // TABLE would silently reset the table's mode.
        out.push(modify_permissions_full(
            &code.name,
            code.to_surql_overwrite(),
            db.to_surql_overwrite(),
        ));
    }
    out
}

fn diff_edge_pair_inner(code: &EdgeDefinition, db: &EdgeDefinition) -> Vec<SchemaDiff> {
    let mut out = Vec::new();
    if edge_shape(code) != edge_shape(db) {
        out.push(SchemaDiff {
            operation: DiffOperation::ModifyTable,
            table: code.name.clone(),
            field: None,
            index: None,
            event: None,
            bucket: None,
            analyzer: None,
            object: None,
            description: format!("Modify edge {}", code.name),
            forward_sql: edge_define_sql(code, true),
            backward_sql: edge_define_sql(db, true),
            details: BTreeMap::new(),
        });
    }
    out.extend(diff_fields(&code.name, &code.fields, &db.fields));
    out.extend(diff_indexes(&code.name, &code.indexes, &db.indexes));
    out.extend(diff_events(&code.name, &code.events, &db.events));
    if !permissions_equal(code.permissions.as_ref(), db.permissions.as_ref()) {
        out.push(modify_permissions_full(
            &code.name,
            edge_define_sql(code, true),
            edge_define_sql(db, true),
        ));
    }
    out
}

/// What an edge's own `DEFINE TABLE` says about its shape: the mode and, for
/// a relation, the two endpoint tables. A non-relation edge renders no
/// endpoints, so any it carries are not part of its shape.
fn edge_shape(edge: &EdgeDefinition) -> (EdgeMode, Option<String>, Option<String>) {
    match edge.mode {
        EdgeMode::Relation => (
            edge.mode,
            edge.from_table.as_deref().map(normalize_expression),
            edge.to_table.as_deref().map(normalize_expression),
        ),
        mode => (mode, None, None),
    }
}

/// Compare the parts of a `DEFINE TABLE` statement that belong to the table
/// itself rather than to a field, index, event, or permission rule: the
/// change feed and the `AS SELECT` view body.
///
/// The table mode is deliberately left out. It is carried by the same
/// `OVERWRITE` statement, so a change to it rides along with any of the other
/// diffs; making it its own trigger would change long-standing behaviour for
/// every schema that leaves the mode implicit.
fn diff_table_body(code: &TableDefinition, db: &TableDefinition) -> Vec<SchemaDiff> {
    let changefeed_changed = code.changefeed != db.changefeed;
    // Views compare on the rendered clause with whitespace normalised: the
    // engine reformats a projection or predicate as it likes, and a diff on
    // the spacing alone would re-apply the view on every reconcile.
    let view_changed = !expr_eq(
        code.view.as_ref().map(ViewDefinition::to_clause).as_deref(),
        db.view.as_ref().map(ViewDefinition::to_clause).as_deref(),
    );
    if !changefeed_changed && !view_changed {
        return Vec::new();
    }
    let what = if view_changed { "view" } else { "change feed" };
    vec![SchemaDiff {
        operation: DiffOperation::ModifyTable,
        table: code.name.clone(),
        field: None,
        index: None,
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Modify {what} on {}", code.name),
        // The full definition replaces: a bare CHANGEFEED or AS SELECT
        // statement would silently reset the table's mode and permissions.
        forward_sql: code.to_surql_overwrite(),
        backward_sql: db.to_surql_overwrite(),
        details: BTreeMap::new(),
    }]
}

/// A permissions change carried as the owning definition's full
/// `OVERWRITE` form.
fn modify_permissions_full(table: &str, forward_sql: String, backward_sql: String) -> SchemaDiff {
    SchemaDiff {
        operation: DiffOperation::ModifyPermissions,
        table: table.to_string(),
        field: None,
        index: None,
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Modify permissions for {table}"),
        forward_sql,
        backward_sql,
        details: BTreeMap::new(),
    }
}

// ---------------------------------------------------------------------------
// Generator helpers (pure functions, rendered into SurrealQL text)
// ---------------------------------------------------------------------------

fn generate_add_table_diffs(table: &TableDefinition) -> Vec<SchemaDiff> {
    // The canonical renderer carries mode AND permissions in the one
    // statement; a separate permissions statement would re-define the
    // table it just created.
    let forward_sql = table.to_surql();
    let backward_sql = format!("REMOVE TABLE {};", table.name);
    let mut out = vec![SchemaDiff {
        operation: DiffOperation::AddTable,
        table: table.name.clone(),
        field: None,
        index: None,
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Add table {}", table.name),
        forward_sql,
        backward_sql,
        details: BTreeMap::new(),
    }];
    for field in &table.fields {
        out.push(generate_add_field_diff(&table.name, field));
    }
    for idx in &table.indexes {
        out.push(generate_add_index_diff(&table.name, idx));
    }
    for ev in &table.events {
        out.push(generate_add_event_diff(&table.name, ev));
    }
    out
}

fn generate_drop_table_diffs(table: &TableDefinition) -> Vec<SchemaDiff> {
    // REMOVE TABLE takes the fields, indexes, and events with it, so the
    // rollback re-creates all of them, not just the table's shell.
    let mut restore = vec![table.to_surql()];
    restore.extend(member_statements(
        &table.name,
        &table.fields,
        &table.indexes,
        &table.events,
    ));
    vec![SchemaDiff {
        operation: DiffOperation::DropTable,
        table: table.name.clone(),
        field: None,
        index: None,
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Drop table {}", table.name),
        forward_sql: format!("REMOVE TABLE {};", table.name),
        backward_sql: restore.join("\n"),
        details: BTreeMap::new(),
    }]
}

/// The statements that define a table's fields, indexes, and events, as the
/// add diffs render them.
fn member_statements(
    table: &str,
    fields: &[FieldDefinition],
    indexes: &[IndexDefinition],
    events: &[EventDefinition],
) -> Vec<String> {
    fields
        .iter()
        .map(|field| field_to_sql(table, field))
        .chain(indexes.iter().map(|idx| index_to_sql(table, idx, false)))
        .chain(events.iter().map(|ev| event_to_sql(table, ev, false)))
        .collect()
}

fn generate_add_field_diff(table: &str, field: &FieldDefinition) -> SchemaDiff {
    let mut forward_sql = field_to_sql(table, field);
    if let Some(default) = field.default.as_deref() {
        // Best-effort backfill: failures to validate default surface as a
        // skipped backfill rather than a panic (matches conservative Python
        // path — though Python raises, Rust returns a safe render because
        // this function is infallible by contract).
        if validate_default_value(default).is_ok() {
            let backfill = format!(
                "UPDATE {table} SET {name} = {default} WHERE {name} IS NONE;",
                name = field.name,
            );
            forward_sql.push('\n');
            forward_sql.push_str(&backfill);
        }
    }
    let backward_sql = format!("REMOVE FIELD {} ON TABLE {};", field.name, table);
    let mut details = BTreeMap::new();
    details.insert(
        "type".to_string(),
        serde_json::Value::String(field.field_type.as_str().into()),
    );
    SchemaDiff {
        operation: DiffOperation::AddField,
        table: table.to_string(),
        field: Some(field.name.clone()),
        index: None,
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Add field {} to {}", field.name, table),
        forward_sql,
        backward_sql,
        details,
    }
}

fn generate_drop_field_diff(table: &str, field: &FieldDefinition) -> SchemaDiff {
    let forward_sql = format!("REMOVE FIELD {} ON TABLE {};", field.name, table);
    let backward_sql = field_to_sql(table, field);
    SchemaDiff {
        operation: DiffOperation::DropField,
        table: table.to_string(),
        field: Some(field.name.clone()),
        index: None,
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Drop field {} from {}", field.name, table),
        forward_sql,
        backward_sql,
        details: BTreeMap::new(),
    }
}

fn generate_modify_field_diff(
    table: &str,
    old_field: &FieldDefinition,
    new_field: &FieldDefinition,
) -> SchemaDiff {
    // Plain DEFINE fails on an existing field; the replace form is
    // what a modification means.
    let forward_sql = new_field.to_surql_overwrite(table);
    let backward_sql = old_field.to_surql_overwrite(table);
    let mut details = BTreeMap::new();
    details.insert(
        "old_type".into(),
        serde_json::Value::String(old_field.field_type.as_str().into()),
    );
    details.insert(
        "new_type".into(),
        serde_json::Value::String(new_field.field_type.as_str().into()),
    );
    let mut description = format!("Modify field {} in {}", new_field.name, table);
    // Gaining REFERENCE is the one field change whose DDL alone leaves
    // the database lying: the engine backfills nothing, so every row
    // that already held a value stays invisible to `<~` until it is
    // rewritten (see [`crate::schema::reference_backfill_sql`]). The
    // rewrite rides `details` rather than `forward_sql` because it is
    // DML an application's own events may refuse, so a live reconciler
    // must choose where it runs; the migration generator, whose files
    // a person reviews, includes it right after the DDL.
    if old_field.reference.is_none() && new_field.reference.is_some() {
        // The Err arm is unreachable for schema-borne names, which
        // were validated at definition time; a name the validator
        // refuses could not have rendered the DDL above either.
        if let Ok(backfill) = crate::schema::reference_backfill_sql(table, &new_field.name) {
            details.insert(
                "reference_backfill_sql".into(),
                serde_json::Value::String(backfill),
            );
            description.push_str(" (gains REFERENCE: existing rows need the backfill rewrite)");
        }
    }
    SchemaDiff {
        operation: DiffOperation::ModifyField,
        table: table.to_string(),
        field: Some(new_field.name.clone()),
        index: None,
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description,
        forward_sql,
        backward_sql,
        details,
    }
}

fn generate_add_index_diff(table: &str, idx: &IndexDefinition) -> SchemaDiff {
    let forward_sql = index_to_sql(table, idx, false);
    let backward_sql = format!("REMOVE INDEX {} ON TABLE {};", idx.name, table);
    SchemaDiff {
        operation: DiffOperation::AddIndex,
        table: table.to_string(),
        field: None,
        index: Some(idx.name.clone()),
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Add index {} to {}", idx.name, table),
        forward_sql,
        backward_sql,
        details: BTreeMap::new(),
    }
}

fn generate_drop_index_diff(table: &str, idx: &IndexDefinition) -> SchemaDiff {
    let forward_sql = format!("REMOVE INDEX {} ON TABLE {};", idx.name, table);
    // The same renderer as the add, so a UNIQUE (or full-text, or vector)
    // index comes back as the kind it was.
    let backward_sql = index_to_sql(table, idx, false);
    SchemaDiff {
        operation: DiffOperation::DropIndex,
        table: table.to_string(),
        field: None,
        index: Some(idx.name.clone()),
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Drop index {} from {}", idx.name, table),
        forward_sql,
        backward_sql,
        details: BTreeMap::new(),
    }
}

/// An index whose definition changed, re-defined whole in both directions.
///
/// There is no index-specific modify operation, so the change reports as
/// [`DiffOperation::ModifyTable`] with [`SchemaDiff::index`] naming the index.
fn generate_modify_index_diff(
    table: &str,
    old_idx: &IndexDefinition,
    new_idx: &IndexDefinition,
) -> SchemaDiff {
    SchemaDiff {
        operation: DiffOperation::ModifyTable,
        table: table.to_string(),
        field: None,
        index: Some(new_idx.name.clone()),
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Modify index {} on {}", new_idx.name, table),
        forward_sql: index_to_sql(table, new_idx, true),
        backward_sql: index_to_sql(table, old_idx, true),
        details: BTreeMap::new(),
    }
}

fn generate_add_event_diff(table: &str, ev: &EventDefinition) -> SchemaDiff {
    let forward_sql = event_to_sql(table, ev, false);
    let backward_sql = format!("REMOVE EVENT {} ON TABLE {};", ev.name, table);
    SchemaDiff {
        operation: DiffOperation::AddEvent,
        table: table.to_string(),
        field: None,
        index: None,
        event: Some(ev.name.clone()),
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Add event {} to {}", ev.name, table),
        forward_sql,
        backward_sql,
        details: BTreeMap::new(),
    }
}

fn generate_drop_event_diff(table: &str, ev: &EventDefinition) -> SchemaDiff {
    let forward_sql = format!("REMOVE EVENT {} ON TABLE {};", ev.name, table);
    let backward_sql = event_to_sql(table, ev, false);
    SchemaDiff {
        operation: DiffOperation::DropEvent,
        table: table.to_string(),
        field: None,
        index: None,
        event: Some(ev.name.clone()),
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Drop event {} from {}", ev.name, table),
        forward_sql,
        backward_sql,
        details: BTreeMap::new(),
    }
}

/// An event whose `WHEN` or `THEN` changed, re-defined whole in both
/// directions; reported as [`DiffOperation::ModifyTable`] with
/// [`SchemaDiff::event`] naming the event, as for an index.
fn generate_modify_event_diff(
    table: &str,
    old_ev: &EventDefinition,
    new_ev: &EventDefinition,
) -> SchemaDiff {
    SchemaDiff {
        operation: DiffOperation::ModifyTable,
        table: table.to_string(),
        field: None,
        index: None,
        event: Some(new_ev.name.clone()),
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Modify event {} on {}", new_ev.name, table),
        forward_sql: event_to_sql(table, new_ev, true),
        backward_sql: event_to_sql(table, old_ev, true),
        details: BTreeMap::new(),
    }
}

fn generate_modify_permissions_diff(
    table: &str,
    new_permissions: Option<&BTreeMap<String, String>>,
    old_permissions: Option<&BTreeMap<String, String>>,
) -> SchemaDiff {
    let forward_sql = render_permission_statements(table, new_permissions);
    let backward_sql = render_permission_statements(table, old_permissions);
    SchemaDiff {
        operation: DiffOperation::ModifyPermissions,
        table: table.to_string(),
        field: None,
        index: None,
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Modify permissions for {table}"),
        forward_sql,
        backward_sql,
        details: BTreeMap::new(),
    }
}

fn generate_add_edge_diffs(edge: &EdgeDefinition) -> Vec<SchemaDiff> {
    // One statement carries the mode, the endpoints, and the permissions. A
    // separate permissions statement would re-define the table it just
    // created, and its OVERWRITE form would reset `TYPE RELATION`.
    let forward_sql = edge_define_sql(edge, false);
    let backward_sql = format!("REMOVE TABLE {};", edge.name);

    let mut out = vec![SchemaDiff {
        operation: DiffOperation::AddTable,
        table: edge.name.clone(),
        field: None,
        index: None,
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Add edge {}", edge.name),
        forward_sql,
        backward_sql,
        details: BTreeMap::new(),
    }];
    for field in &edge.fields {
        out.push(generate_add_field_diff(&edge.name, field));
    }
    for idx in &edge.indexes {
        out.push(generate_add_index_diff(&edge.name, idx));
    }
    for ev in &edge.events {
        out.push(generate_add_event_diff(&edge.name, ev));
    }
    out
}

/// Render an edge's `DEFINE TABLE` statement, in its `OVERWRITE` form when
/// `overwrite` is set.
///
/// The canonical renderer refuses a `RELATION` edge that names only one
/// endpoint (or none). The engine accepts that shape and constrains just the
/// side that is named, so it renders here instead of vanishing from the diff.
fn edge_define_sql(edge: &EdgeDefinition, overwrite: bool) -> String {
    let canonical = if overwrite {
        edge.to_surql_overwrite()
    } else {
        edge.to_surql()
    };
    canonical.unwrap_or_else(|_| {
        let guard = if overwrite { " OVERWRITE" } else { "" };
        let mut sql = format!("DEFINE TABLE{guard} {} TYPE RELATION", edge.name);
        if let Some(from) = edge.from_table.as_deref() {
            sql.push_str(" FROM ");
            sql.push_str(from);
        }
        if let Some(to) = edge.to_table.as_deref() {
            sql.push_str(" TO ");
            sql.push_str(to);
        }
        if let Some(rules) = permission_rules(edge.permissions.as_ref()) {
            sql.push_str(" PERMISSIONS ");
            sql.push_str(&rules);
        }
        sql.push(';');
        sql
    })
}

fn generate_drop_edge_diffs(edge: &EdgeDefinition) -> Vec<SchemaDiff> {
    // As for a table: the rollback re-creates the edge and everything the
    // REMOVE took with it.
    let mut restore = vec![edge_define_sql(edge, false)];
    restore.extend(member_statements(
        &edge.name,
        &edge.fields,
        &edge.indexes,
        &edge.events,
    ));
    vec![SchemaDiff {
        operation: DiffOperation::DropTable,
        table: edge.name.clone(),
        field: None,
        index: None,
        event: None,
        bucket: None,
        analyzer: None,
        object: None,
        description: format!("Drop edge {}", edge.name),
        forward_sql: format!("REMOVE TABLE {};", edge.name),
        backward_sql: restore.join("\n"),
        details: BTreeMap::new(),
    }]
}

/// Render a permission map as an `ALTER TABLE ... PERMISSIONS` statement.
///
/// `ALTER` replaces the table's whole permission set and nothing else, so the
/// table's mode, type, endpoints, and fields survive. `DEFINE TABLE` could not
/// carry a permissions-only change: the plain form fails on a table that
/// exists, and the `OVERWRITE` form resets every clause it does not repeat.
/// An absent or empty map is the engine's default for a table, `NONE`.
fn render_permission_statements(table: &str, perms: Option<&BTreeMap<String, String>>) -> String {
    let rules = permission_rules(perms).unwrap_or_else(|| "NONE".to_owned());
    format!("ALTER TABLE {table} PERMISSIONS {rules};")
}

/// The `FOR <action> WHERE <rule>` clauses of a permission map, or `None`
/// when there are none.
fn permission_rules(perms: Option<&BTreeMap<String, String>>) -> Option<String> {
    let perms = perms.filter(|p| !p.is_empty())?;
    let clauses: Vec<String> = perms
        .iter()
        .map(|(action, condition)| format!("FOR {action} WHERE {condition}"))
        .collect();
    Some(clauses.join(" "))
}

fn field_to_sql(table: &str, field: &FieldDefinition) -> String {
    // The canonical renderer, so clause ordering (FLEXIBLE after
    // TYPE, VALUE placement) has exactly one implementation.
    field.to_surql(table)
}

/// Render an index's `DEFINE INDEX` statement, in its `OVERWRITE` form when
/// `overwrite` is set. Every direction of the diff goes through here, so an
/// index is always re-created as the kind it was.
fn index_to_sql(table: &str, idx: &IndexDefinition, overwrite: bool) -> String {
    let guard = if overwrite { " OVERWRITE" } else { "" };
    match idx.index_type {
        IndexType::Mtree => mtree_index_to_sql(table, idx, guard),
        IndexType::Hnsw => hnsw_index_to_sql(table, idx, guard),
        // A DISKANN index renders through the canonical serializer, which
        // already spells the full DIST/TYPE/DEGREE/L_BUILD/ALPHA tail the
        // engine echoes; a full-text index does too, because it carries an
        // analyzer / BM25 / highlights clause the plain path below would drop.
        IndexType::Diskann | IndexType::Search => {
            if overwrite {
                idx.to_surql_overwrite(table)
            } else {
                idx.to_surql_with_options(table, false)
            }
        }
        IndexType::Unique | IndexType::Standard => {
            let columns = idx.columns.join(", ");
            let unique = if idx.index_type == IndexType::Unique {
                " UNIQUE"
            } else {
                ""
            };
            format!(
                "DEFINE INDEX{guard} {name} ON TABLE {table} COLUMNS {columns}{unique};",
                name = idx.name
            )
        }
    }
}

/// Render an event's `DEFINE EVENT` statement, in its `OVERWRITE` form when
/// `overwrite` is set. The action is wrapped in a block so a multi-statement
/// action stays one clause.
fn event_to_sql(table: &str, ev: &EventDefinition, overwrite: bool) -> String {
    let guard = if overwrite { " OVERWRITE" } else { "" };
    format!(
        "DEFINE EVENT{guard} {name} ON TABLE {table} WHEN {cond} THEN {{ {act} }};",
        name = ev.name,
        cond = ev.condition,
        act = ev.action,
    )
}

/// Whether two field definitions render the same stored field.
///
/// Every clause [`FieldDefinition::to_surql`] renders is compared: the type
/// with its `option<...>` wrapper and record target, `FLEXIBLE`, `READONLY`,
/// `REFERENCE`, the `ASSERT` / `DEFAULT` / `VALUE` / `COMPUTED` expressions
/// (through [`normalize_expression`]), and the permissions. What the engine
/// is free to spell differently compares equal: a record target on a type
/// that never renders one, and field permission rules that say `FULL`, the
/// field default the engine writes out for every action a rule set leaves
/// unnamed.
///
/// ## Examples
///
/// ```
/// use surql::migration::diff::fields_equal;
/// use surql::schema::{FieldDefinition, FieldType};
///
/// let text = FieldDefinition::new("title", FieldType::String);
/// let code = text.clone().with_assertion("$value  !=  NONE");
/// assert!(fields_equal(&code, &text.clone().with_assertion("$value != NONE")));
/// assert!(!fields_equal(&text, &text.clone().with_nullable(true)));
/// ```
#[must_use]
pub fn fields_equal(a: &FieldDefinition, b: &FieldDefinition) -> bool {
    a.name == b.name
        && a.field_type == b.field_type
        && a.nullable == b.nullable
        && rendered_target(a) == rendered_target(b)
        && a.readonly == b.readonly
        && a.flexible == b.flexible
        && a.reference == b.reference
        && expr_eq(a.assertion.as_deref(), b.assertion.as_deref())
        && expr_eq(a.default.as_deref(), b.default.as_deref())
        && expr_eq(a.value.as_deref(), b.value.as_deref())
        && expr_eq(a.computed.as_deref(), b.computed.as_deref())
        && permissions_equal(
            field_permissions(a.permissions.as_ref()).as_ref(),
            field_permissions(b.permissions.as_ref()).as_ref(),
        )
}

/// The record target a field actually renders: only `record<...>` and
/// `array<record<...>>` carry one.
fn rendered_target(field: &FieldDefinition) -> Option<&str> {
    match field.field_type {
        FieldType::Record | FieldType::Array => field.target_table.as_deref(),
        _ => None,
    }
}

/// A field's permission rules without the ones that restate the field
/// default, `FULL`.
fn field_permissions(perms: Option<&BTreeMap<String, String>>) -> Option<BTreeMap<String, String>> {
    let kept: BTreeMap<String, String> = expand_actions(perms?)
        .into_iter()
        .filter(|(_, rule)| !rule.trim().eq_ignore_ascii_case("FULL"))
        .collect();
    (!kept.is_empty()).then_some(kept)
}

/// Expand comma-grouped action keys (`"select, create"`) into one
/// entry per action, the shape the engine echoes. Code that groups
/// actions and a database that splits them must compare equal.
fn expand_actions(map: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for (key, value) in map {
        for action in key.split(',') {
            out.insert(action.trim().to_owned(), value.clone());
        }
    }
    out
}

/// Whether two per-action permission maps grant the same thing.
///
/// Comma-grouped keys (`"select, create"`) are split into one entry per
/// action, the shape the engine echoes, and each rule compares through
/// [`normalize_expression`]. An absent map equals an empty one.
///
/// ## Examples
///
/// ```
/// use std::collections::BTreeMap;
/// use surql::migration::diff::permissions_equal;
///
/// let grouped = BTreeMap::from([("select, create".to_owned(), "$auth.id = id".to_owned())]);
/// let split = BTreeMap::from([
///     ("select".to_owned(), "$auth.id  =  id".to_owned()),
///     ("create".to_owned(), "$auth.id = id".to_owned()),
/// ]);
/// assert!(permissions_equal(Some(&grouped), Some(&split)));
/// assert!(permissions_equal(None, Some(&BTreeMap::new())));
/// ```
#[must_use]
pub fn permissions_equal(
    a: Option<&BTreeMap<String, String>>,
    b: Option<&BTreeMap<String, String>>,
) -> bool {
    let expanded_a = a.map(expand_actions);
    let expanded_b = b.map(expand_actions);
    let (a, b) = (expanded_a.as_ref(), expanded_b.as_ref());
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => {
            if x.len() != y.len() {
                return false;
            }
            for (k, vx) in x {
                let Some(vy) = y.get(k) else { return false };
                if normalize_expression(vx) != normalize_expression(vy) {
                    return false;
                }
            }
            true
        }
        (Some(m), None) | (None, Some(m)) => m.is_empty(),
    }
}

/// HNSW construction defaults the engine fills in when a statement leaves
/// them out, and always echoes (`EFC 150 M 12`); from its `DEFINE INDEX`
/// parser.
const HNSW_DEFAULT_EFC: u32 = 150;
/// See [`HNSW_DEFAULT_EFC`].
const HNSW_DEFAULT_M: u32 = 12;

/// Whether two index definitions describe the same stored index.
///
/// The name, kind, and columns always count, and so does every member the
/// kind renders. A member the engine fills with a default when the statement
/// leaves it out compares as that default (an HNSW index's
/// `DIST EUCLIDEAN TYPE F32 EFC 150 M 12`, the DISKANN tail, a full-text
/// index's `ascii` analyzer), and a member the kind does not render is
/// ignored. So are `CONCURRENTLY`, a build directive the engine does not
/// store, and a full-text index's `BM25` flag: the engine scores every
/// full-text index with BM25 whether or not the statement asked for it.
///
/// ## Examples
///
/// ```
/// use surql::migration::diff::indexes_equal;
/// use surql::schema::{index, unique_index};
///
/// assert!(indexes_equal(&index("i", ["a"]), &index("i", ["a"]).with_concurrently(true)));
/// assert!(!indexes_equal(&index("i", ["a"]), &unique_index("i", ["a"])));
/// assert!(!indexes_equal(&index("i", ["a"]), &index("i", ["a", "b"])));
/// ```
#[must_use]
pub fn indexes_equal(a: &IndexDefinition, b: &IndexDefinition) -> bool {
    comparable_index(a) == comparable_index(b)
}

/// `idx` reduced to what the engine stores for its kind, with the engine's
/// defaults filled in.
fn comparable_index(idx: &IndexDefinition) -> IndexDefinition {
    let columns = idx
        .columns
        .iter()
        .map(|column| normalize_expression(column));
    let base = IndexDefinition::new(idx.name.clone(), columns).with_type(idx.index_type);
    match idx.index_type {
        IndexType::Unique | IndexType::Standard => base,
        IndexType::Search => IndexDefinition {
            analyzer: idx
                .analyzer
                .clone()
                .filter(|analyzer| !analyzer.eq_ignore_ascii_case("ascii")),
            highlights: idx.highlights,
            ..base
        },
        IndexType::Mtree => IndexDefinition {
            dimension: idx.dimension,
            distance: Some(idx.distance.unwrap_or(MTreeDistanceType::Euclidean)),
            vector_type: Some(idx.vector_type.unwrap_or(MTreeVectorType::F64)),
            ..base
        },
        IndexType::Hnsw => IndexDefinition {
            dimension: idx.dimension,
            hnsw_distance: Some(idx.hnsw_distance.unwrap_or(HnswDistanceType::Euclidean)),
            vector_type: Some(idx.vector_type.unwrap_or(MTreeVectorType::F32)),
            efc: Some(idx.efc.unwrap_or(HNSW_DEFAULT_EFC)),
            m: Some(idx.m.unwrap_or(HNSW_DEFAULT_M)),
            ..base
        },
        IndexType::Diskann => IndexDefinition {
            dimension: idx.dimension,
            diskann_distance: Some(
                idx.diskann_distance
                    .unwrap_or(DiskAnnDistanceType::Euclidean),
            ),
            vector_type: Some(idx.vector_type.unwrap_or(MTreeVectorType::F32)),
            degree: Some(idx.degree.unwrap_or(DISKANN_DEFAULT_DEGREE)),
            l_build: Some(idx.l_build.unwrap_or(DISKANN_DEFAULT_L_BUILD)),
            alpha: Some(
                idx.alpha
                    .clone()
                    .unwrap_or_else(|| DISKANN_DEFAULT_ALPHA.to_owned()),
            ),
            hashed_vector: idx.hashed_vector,
            ..base
        },
    }
}

/// Whether two event definitions fire on the same condition and run the
/// same action.
///
/// Both halves compare through [`normalize_expression`]. The action also
/// drops the block braces and trailing `;` the engine may add or remove: it
/// stores a block action as `{ ... }` and a bare one wrapped in parentheses.
///
/// ## Examples
///
/// ```
/// use surql::migration::diff::events_equal;
/// use surql::schema::event;
///
/// let code = event("audit", "$event = 'CREATE'", "CREATE log SET n = 1");
/// let echo = event("audit", "$event = \"CREATE\"", "(CREATE log SET n = 1)");
/// assert!(events_equal(&code, &echo));
/// assert!(!events_equal(&code, &event("audit", "true", "CREATE log SET n = 1")));
/// ```
#[must_use]
pub fn events_equal(a: &EventDefinition, b: &EventDefinition) -> bool {
    a.name == b.name
        && normalize_expression(&a.condition) == normalize_expression(&b.condition)
        && event_body(&a.action) == event_body(&b.action)
}

/// An event action without its block braces or trailing `;`, normalised.
fn event_body(action: &str) -> String {
    let trimmed = action.trim();
    let inner = trimmed
        .strip_prefix('{')
        .and_then(|rest| rest.strip_suffix('}'))
        .unwrap_or(trimmed);
    normalize_expression(inner.trim().trim_end_matches(';'))
}

fn mtree_index_to_sql(table: &str, idx: &IndexDefinition, guard: &str) -> String {
    let field = idx.columns.first().map_or("", String::as_str);
    let dim = idx.dimension.unwrap_or(0);
    let distance = idx.distance.unwrap_or(MTreeDistanceType::Euclidean);
    let vtype = idx.vector_type.unwrap_or(MTreeVectorType::F64);
    format!(
        "DEFINE INDEX{guard} {name} ON TABLE {table} COLUMNS {field} MTREE DIMENSION {dim} \
         DIST {distance} TYPE {vtype};",
        name = idx.name,
        distance = distance.as_str(),
        vtype = vtype.as_str(),
    )
}

fn hnsw_index_to_sql(table: &str, idx: &IndexDefinition, guard: &str) -> String {
    let field = idx.columns.first().map_or("", String::as_str);
    let dim = idx.dimension.unwrap_or(0);
    let distance = idx.hnsw_distance.unwrap_or(HnswDistanceType::Euclidean);
    // The engine's own default when TYPE is left out, and what the canonical
    // renderer (which leaves it out) therefore produces.
    let vtype = idx.vector_type.unwrap_or(MTreeVectorType::F32);
    let mut sql = format!(
        "DEFINE INDEX{guard} {name} ON TABLE {table} COLUMNS {field} HNSW DIMENSION {dim} \
         DIST {distance} TYPE {vtype}",
        name = idx.name,
        distance = distance.as_str(),
        vtype = vtype.as_str(),
    );
    if let Some(efc) = idx.efc {
        sql.push_str(&format!(" EFC {efc}"));
    }
    if let Some(m) = idx.m {
        sql.push_str(&format!(" M {m}"));
    }
    sql.push(';');
    sql
}

pub(super) fn index_by_name<'a, T, F>(items: &'a [T], key: F) -> BTreeMap<&'a str, &'a T>
where
    F: Fn(&'a T) -> &'a str,
{
    let mut map = BTreeMap::new();
    for item in items {
        map.insert(key(item), item);
    }
    map
}

pub(super) fn sorted_keys<'a, V>(map: &'a BTreeMap<&'a str, V>) -> Vec<&'a str> {
    // BTreeMap iterates in key order already, so we just need to collect
    // the keys into a concrete vector to avoid holding the borrow across
    // the map while iterating mutably elsewhere.
    let mut keys: Vec<&str> = map.keys().copied().collect();
    keys.sort_unstable();
    // dedupe is not needed — a BTreeMap cannot have duplicates — but keep
    // a stable Vec<&str> interface.
    let set: BTreeSet<&str> = keys.into_iter().collect();
    set.into_iter().collect()
}

// Silence clippy::missing_fields_in_debug warnings from older toolchains:
// all public types here derive Debug explicitly.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::edge::{EdgeDefinition, EdgeMode};
    use crate::schema::fields::{FieldDefinition, FieldType};
    use crate::schema::table::{
        diskann_index, event, hnsw_index, index, mtree_index, table_schema, unique_index,
        DiskAnnDistanceType, HnswDistanceType, IndexDefinition, IndexType, MTreeDistanceType,
        MTreeVectorType, TableMode,
    };

    fn tbl(name: &str) -> TableDefinition {
        table_schema(name)
    }

    fn f(name: &str, ty: FieldType) -> FieldDefinition {
        FieldDefinition::new(name, ty)
    }

    // ----- normalize_expression -----

    #[test]
    fn normalize_expression_collapses_runs_of_whitespace() {
        assert_eq!(normalize_expression("a   b\tc\n d"), "a b c d");
    }

    #[test]
    fn normalize_expression_trims_edges() {
        assert_eq!(normalize_expression("  hello world  "), "hello world");
    }

    #[test]
    fn normalize_expression_empty_is_empty() {
        assert_eq!(normalize_expression("   "), "");
    }

    /// Whitespace inside a string literal is content, not formatting.
    #[test]
    fn normalize_expression_keeps_whitespace_inside_literals() {
        assert_eq!(normalize_expression("'a  b'"), "'a  b'");
        assert!(!expr_eq(Some("'a  b'"), Some("'a b'")));
        // A raw tab and its escape are the same character; both come out in
        // the escaped spelling the engine prints.
        assert_eq!(
            normalize_expression("  $value  =  'x\t\ty'  "),
            r"$value = 'x\t\ty'"
        );
        assert!(expr_eq(Some("'x\ty'"), Some(r"'x\ty'")));
        assert_eq!(normalize_expression("`my  field` = 1"), "`my  field` = 1");
        assert_eq!(normalize_expression("r:⟨a  b⟩"), "r:⟨a  b⟩");
    }

    /// The engine echoes every string literal single-quoted unless it
    /// holds a `'`; the quote style is not a difference.
    #[test]
    fn normalize_expression_ignores_the_quote_style() {
        assert!(expr_eq(Some("\"hello  there\""), Some("'hello  there'")));
        assert!(expr_eq(
            Some(r"$value != 'it\'s'"),
            Some("$value != \"it's\"")
        ));
        assert!(!expr_eq(Some("\"a\""), Some("'b'")));
    }

    /// The echo folds only apply to code: a literal that happens to spell
    /// `IS NONE`, a cast, or a parenthesis is left alone.
    #[test]
    fn normalize_expression_folds_nothing_inside_literals() {
        assert_eq!(
            normalize_expression("$value = ' IS NONE'"),
            "$value = ' IS NONE'"
        );
        assert_eq!(normalize_expression("'<string> x'"), "'<string> x'");
        assert_eq!(normalize_expression("(a = ')')"), "a = ')'");
        assert_eq!(normalize_expression("('(' + x"), "('(' + x");
    }

    #[test]
    fn normalize_expression_folds_the_engine_echo() {
        assert_eq!(normalize_expression("($value IS NONE)"), "$value = NONE");
        assert_eq!(normalize_expression("$value IS NOT NONE"), "$value != NONE");
        assert_eq!(normalize_expression("<string> id"), "<string>id");
        assert_eq!(normalize_expression("a > b"), "a > b");
        assert_eq!(normalize_expression("(a) + (b)"), "(a) + (b)");
    }

    /// An unterminated literal and non-ASCII text survive intact.
    #[test]
    fn normalize_expression_handles_ragged_input() {
        assert_eq!(normalize_expression("'abc  "), "'abc  ");
        assert_eq!(normalize_expression(r"'a\'"), r"'a\'");
        assert_eq!(normalize_expression("é  <ü> ö"), "é <ü> ö");
        assert_eq!(normalize_expression("<int> 'ü  x'"), "<int>'ü  x'");
        assert_eq!(normalize_expression("(ä)"), "ä");
    }

    // ----- validate_event_expression -----

    #[test]
    fn validate_event_expression_allows_safe() {
        assert!(validate_event_expression("$event = \"CREATE\"", "condition").is_ok());
        assert!(validate_event_expression("$before.a != $after.a", "condition").is_ok());
        assert!(validate_event_expression("true", "condition").is_ok());
        assert!(validate_event_expression("CREATE log SET u = 1", "action").is_ok());
    }

    #[test]
    fn validate_event_expression_rejects_statement_separator() {
        assert!(validate_event_expression("a; DROP b", "condition").is_err());
    }

    #[test]
    fn validate_event_expression_rejects_trailing_semicolon() {
        assert!(validate_event_expression("a;", "condition").is_err());
    }

    #[test]
    fn validate_event_expression_rejects_comment() {
        assert!(validate_event_expression("a -- b", "condition").is_err());
    }

    #[test]
    fn validate_event_expression_rejects_semicolon_comment() {
        assert!(validate_event_expression("a;--b", "condition").is_err());
    }

    // ----- validate_default_value -----

    #[test]
    fn validate_default_value_accepts_literals() {
        assert!(validate_default_value("42").is_ok());
        assert!(validate_default_value("-1").is_ok());
        assert!(validate_default_value("3.14").is_ok());
        assert!(validate_default_value("true").is_ok());
        assert!(validate_default_value("false").is_ok());
        assert!(validate_default_value("NONE").is_ok());
        assert!(validate_default_value("NULL").is_ok());
        assert!(validate_default_value("'hello'").is_ok());
        assert!(validate_default_value("time::now()").is_ok());
        assert!(validate_default_value("$auth").is_ok());
    }

    #[test]
    fn validate_default_value_rejects_unsafe() {
        assert!(validate_default_value("a; DROP TABLE u").is_err());
        assert!(validate_default_value("SELECT * FROM u").is_err());
    }

    // ----- diff_tables: ADD -----

    #[test]
    fn diff_tables_adds_new_table() {
        let code = vec![tbl("user")];
        let db: Vec<TableDefinition> = vec![];
        let diffs = diff_tables(&code, &db);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::AddTable);
        assert_eq!(diffs[0].table, "user");
        assert!(diffs[0].forward_sql.starts_with("DEFINE TABLE user"));
        assert_eq!(diffs[0].backward_sql, "REMOVE TABLE user;");
    }

    #[test]
    fn diff_tables_adds_new_table_with_field_and_index() {
        let code_table = tbl("user")
            .with_fields([f("email", FieldType::String)])
            .with_indexes([unique_index("email_idx", ["email"])]);
        let diffs = diff_tables(&[code_table], &[]);
        // 1 table + 1 field + 1 index = 3 diffs.
        assert_eq!(diffs.len(), 3);
        assert_eq!(diffs[0].operation, DiffOperation::AddTable);
        assert!(diffs.iter().any(|d| d.operation == DiffOperation::AddField));
        assert!(diffs.iter().any(|d| d.operation == DiffOperation::AddIndex));
    }

    #[test]
    fn diff_tables_adds_table_with_event_and_perms() {
        let code_table = tbl("user")
            .with_events([event("on_upd", "true", "RETURN 1")])
            .with_permissions([("select", "true")]);
        let diffs = diff_tables(&[code_table], &[]);
        // Table (permissions ride the DEFINE TABLE itself) + event.
        assert_eq!(diffs.len(), 2);
        assert!(diffs.iter().any(|d| d.operation == DiffOperation::AddEvent));
        assert!(
            diffs[0].forward_sql.contains("PERMISSIONS"),
            "{}",
            diffs[0].forward_sql
        );
    }

    // ----- diff_tables: DROP -----

    #[test]
    fn diff_tables_drops_missing_table() {
        let db = vec![tbl("old").with_mode(TableMode::Schemaless)];
        let diffs = diff_tables(&[], &db);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::DropTable);
        assert_eq!(diffs[0].forward_sql, "REMOVE TABLE old;");
        assert_eq!(diffs[0].backward_sql, "DEFINE TABLE old SCHEMALESS;");
    }

    /// REMOVE TABLE takes everything on the table with it, so the rollback
    /// re-creates everything, as the add would have.
    #[test]
    fn a_dropped_table_rolls_back_to_its_whole_definition() {
        use crate::schema::ChangeFeed;
        let doc = tbl("doc")
            .with_fields([f("email", FieldType::String)])
            .with_indexes([unique_index("email_idx", ["email"])])
            .with_events([event("audit", "true", "CREATE log")])
            .with_permissions([("select", "true")])
            .with_changefeed(ChangeFeed::new("1d"));
        let diffs = diff_tables(&[], std::slice::from_ref(&doc));
        assert_eq!(diffs.len(), 1);
        let restore: Vec<String> = diff_tables(std::slice::from_ref(&doc), &[])
            .iter()
            .map(|d| d.forward_sql.clone())
            .collect();
        assert_eq!(diffs[0].backward_sql, restore.join("\n"));
        assert!(diffs[0].backward_sql.contains(
            "DEFINE TABLE doc SCHEMAFULL CHANGEFEED 1d PERMISSIONS FOR select WHERE true;"
        ));
        assert!(diffs[0]
            .backward_sql
            .contains("DEFINE INDEX email_idx ON TABLE doc COLUMNS email UNIQUE;"));
    }

    #[test]
    fn a_dropped_edge_rolls_back_to_its_whole_definition() {
        let likes = relation_edge("likes")
            .with_fields([f("weight", FieldType::Int)])
            .with_indexes([unique_index("pair", ["in", "out"])])
            .with_permissions([("select", "true")]);
        let diffs = diff_edges(&[], std::slice::from_ref(&likes));
        assert_eq!(diffs.len(), 1);
        assert_eq!(
            diffs[0].backward_sql,
            "DEFINE TABLE likes TYPE RELATION FROM user TO post PERMISSIONS FOR select WHERE true;\n\
             DEFINE FIELD weight ON TABLE likes TYPE int;\n\
             DEFINE INDEX pair ON TABLE likes COLUMNS in, out UNIQUE;"
        );
    }

    #[test]
    fn a_dropped_unique_index_comes_back_unique() {
        let diffs = diff_indexes("user", &[], &[unique_index("email_idx", ["email"])]);
        assert_eq!(
            diffs[0].backward_sql,
            "DEFINE INDEX email_idx ON TABLE user COLUMNS email UNIQUE;"
        );
    }

    // ----- diff_tables: MODIFY (no-op when identical) -----

    #[test]
    fn diff_tables_identical_produces_no_diff() {
        let a = tbl("user").with_fields([f("email", FieldType::String)]);
        let diffs = diff_tables(std::slice::from_ref(&a), std::slice::from_ref(&a));
        assert!(diffs.is_empty());
    }

    // ----- diff_fields: ADD / DROP / MODIFY -----

    #[test]
    fn diff_fields_detects_added() {
        let code = vec![f("email", FieldType::String)];
        let diffs = diff_fields("user", &code, &[]);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::AddField);
        assert_eq!(diffs[0].field.as_deref(), Some("email"));
        assert!(diffs[0].forward_sql.contains("DEFINE FIELD email"));
        assert!(diffs[0].backward_sql.contains("REMOVE FIELD email"));
    }

    #[test]
    fn diff_fields_detects_dropped() {
        let db = vec![f("old", FieldType::String)];
        let diffs = diff_fields("user", &[], &db);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::DropField);
        assert!(diffs[0].forward_sql.contains("REMOVE FIELD old"));
        assert!(diffs[0].backward_sql.contains("DEFINE FIELD old"));
    }

    #[test]
    fn diff_fields_detects_modified_type() {
        let code = vec![f("age", FieldType::Int)];
        let db = vec![f("age", FieldType::String)];
        let diffs = diff_fields("user", &code, &db);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::ModifyField);
        assert_eq!(
            diffs[0].details.get("old_type"),
            Some(&serde_json::json!("string"))
        );
        assert_eq!(
            diffs[0].details.get("new_type"),
            Some(&serde_json::json!("int"))
        );
    }

    fn linked(action: Option<crate::schema::ReferenceAction>) -> FieldDefinition {
        let mut field = f("blob", FieldType::Record);
        field.target_table = Some("blob".into());
        field.reference = action;
        field
    }

    /// Gaining `REFERENCE` is the one field change whose DDL alone
    /// leaves the tracking wrong for every pre-existing row, so that
    /// diff carries the rewrite and says so.
    #[test]
    fn a_gained_reference_carries_its_backfill() {
        use crate::schema::ReferenceAction;
        let diffs = diff_fields(
            "file",
            &[linked(Some(ReferenceAction::Ignore))],
            &[linked(None)],
        );
        assert_eq!(diffs.len(), 1);
        let backfill = diffs[0]
            .reference_backfill_sql()
            .expect("the rewrite rides the diff");
        assert!(backfill.contains("SELECT VALUE id FROM file"), "{backfill}");
        assert!(backfill.contains("?? []"), "{backfill}");
        assert!(backfill.contains("SET blob = NONE"), "{backfill}");
        assert!(
            diffs[0].description.contains("backfill"),
            "{}",
            diffs[0].description
        );
    }

    /// Everything else about a reference leaves the rewrite out: a
    /// changed action re-renders DDL over tracking that already exists,
    /// and a removed clause has nothing to register.
    #[test]
    fn other_reference_changes_carry_no_backfill() {
        use crate::schema::ReferenceAction;
        let changed = diff_fields(
            "file",
            &[linked(Some(ReferenceAction::Cascade))],
            &[linked(Some(ReferenceAction::Ignore))],
        );
        assert_eq!(changed.len(), 1);
        assert!(changed[0].reference_backfill_sql().is_none());

        let removed = diff_fields(
            "file",
            &[linked(None)],
            &[linked(Some(ReferenceAction::Ignore))],
        );
        assert_eq!(removed.len(), 1);
        assert!(removed[0].reference_backfill_sql().is_none());

        // A NEW field with REFERENCE has no pre-existing values to
        // register; the add diff stays plain DDL.
        let added = diff_fields("file", &[linked(Some(ReferenceAction::Ignore))], &[]);
        assert_eq!(added.len(), 1);
        assert_eq!(added[0].operation, DiffOperation::AddField);
        assert!(added[0].reference_backfill_sql().is_none());
    }

    /// Every clause the field renders is a clause a change can land in.
    #[test]
    fn every_rendered_field_clause_is_compared() {
        let base = f("owner", FieldType::Record).with_target_table("user");
        let changes = [
            ("nullable", base.clone().with_nullable(true)),
            ("target", base.clone().with_target_table("post")),
            (
                "reference",
                base.clone()
                    .with_reference(crate::schema::ReferenceAction::Reject),
            ),
            ("computed", base.clone().with_computed("<~post")),
            (
                "permissions",
                base.clone().with_permissions([("update", "$auth.admin")]),
            ),
        ];
        for (what, changed) in changes {
            let diffs = diff_fields(
                "t",
                std::slice::from_ref(&changed),
                std::slice::from_ref(&base),
            );
            assert_eq!(diffs.len(), 1, "a {what} change went unnoticed");
            assert_eq!(diffs[0].operation, DiffOperation::ModifyField);
            assert_eq!(
                diffs[0].forward_sql,
                changed.to_surql_overwrite("t"),
                "{what}"
            );
            assert_eq!(
                diffs[0].backward_sql,
                base.to_surql_overwrite("t"),
                "{what}"
            );
        }
    }

    /// A target table only renders on a record or array field, so on any
    /// other type it is not a difference.
    #[test]
    fn a_target_table_the_type_ignores_is_not_a_change() {
        let code = f("name", FieldType::String).with_target_table("user");
        let db = f("name", FieldType::String);
        assert!(diff_fields("t", &[code], &[db]).is_empty());
    }

    /// The engine spells out `FULL` for every action a field's rules leave
    /// out, which is the field default; it is not a change.
    #[test]
    fn default_field_permissions_are_not_a_change() {
        let code = f("x", FieldType::Int).with_permissions([("select", "$auth.id = id")]);
        let db = f("x", FieldType::Int).with_permissions([
            ("select", "$auth.id = id"),
            ("create", "FULL"),
            ("update", "FULL"),
        ]);
        assert!(diff_fields("t", &[code], &[db]).is_empty());
        let full = f("x", FieldType::Int).with_permissions([("select, create, update", "FULL")]);
        assert!(diff_fields("t", &[f("x", FieldType::Int)], &[full]).is_empty());
    }

    #[test]
    fn diff_fields_identical_yields_nothing() {
        let a = vec![f("x", FieldType::Int)];
        assert!(diff_fields("t", &a, &a).is_empty());
    }

    #[test]
    fn diff_fields_whitespace_different_assertion_is_not_a_diff() {
        let code = vec![f("x", FieldType::Int).with_assertion("$value  > 0")];
        let db = vec![f("x", FieldType::Int).with_assertion("$value > 0")];
        assert!(diff_fields("t", &code, &db).is_empty());
    }

    #[test]
    fn diff_fields_modify_detects_assertion_semantic_change() {
        let code = vec![f("x", FieldType::Int).with_assertion("$value > 0")];
        let db = vec![f("x", FieldType::Int).with_assertion("$value >= 0")];
        let diffs = diff_fields("t", &code, &db);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::ModifyField);
    }

    #[test]
    fn diff_fields_add_with_default_emits_backfill() {
        let code = vec![f("age", FieldType::Int).with_default("0")];
        let diffs = diff_fields("user", &code, &[]);
        assert!(diffs[0].forward_sql.contains("DEFAULT 0"));
        assert!(diffs[0]
            .forward_sql
            .contains("UPDATE user SET age = 0 WHERE age IS NONE;"));
    }

    #[test]
    fn diff_fields_add_with_unsafe_default_skips_backfill() {
        let code = vec![f("age", FieldType::Int).with_default("DROP TABLE x")];
        let diffs = diff_fields("user", &code, &[]);
        assert!(!diffs[0].forward_sql.contains("UPDATE"));
    }

    #[test]
    fn diff_fields_readonly_toggle_is_a_modify() {
        let code = vec![f("x", FieldType::Int).readonly(true)];
        let db = vec![f("x", FieldType::Int)];
        let diffs = diff_fields("t", &code, &db);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::ModifyField);
    }

    // ----- diff_indexes -----

    #[test]
    fn diff_indexes_detects_added_standard() {
        let code = vec![index("title_idx", ["title"])];
        let diffs = diff_indexes("post", &code, &[]);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::AddIndex);
        assert_eq!(
            diffs[0].forward_sql,
            "DEFINE INDEX title_idx ON TABLE post COLUMNS title;"
        );
    }

    #[test]
    fn diff_indexes_detects_added_unique() {
        let code = vec![unique_index("email_idx", ["email"])];
        let diffs = diff_indexes("user", &code, &[]);
        assert!(diffs[0].forward_sql.contains("UNIQUE"));
    }

    #[test]
    fn diff_indexes_detects_dropped() {
        let db = vec![index("old_idx", ["x"])];
        let diffs = diff_indexes("t", &[], &db);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::DropIndex);
        assert!(diffs[0].forward_sql.contains("REMOVE INDEX old_idx"));
        assert!(diffs[0].backward_sql.contains("DEFINE INDEX old_idx"));
    }

    #[test]
    fn diff_indexes_identical_yields_nothing() {
        let a = vec![index("x", ["a"])];
        assert!(diff_indexes("t", &a, &a).is_empty());
    }

    #[test]
    fn diff_indexes_added_mtree() {
        let idx = mtree_index(
            "e_idx",
            "embedding",
            1536,
            MTreeDistanceType::Cosine,
            MTreeVectorType::F32,
        );
        let diffs = diff_indexes("doc", &[idx], &[]);
        assert_eq!(diffs.len(), 1);
        assert!(diffs[0].forward_sql.contains("MTREE DIMENSION 1536"));
        assert!(diffs[0].forward_sql.contains("DIST COSINE"));
        assert!(diffs[0].forward_sql.contains("TYPE F32"));
    }

    #[test]
    fn diff_indexes_dropped_mtree_recreates_in_backward() {
        let idx = mtree_index(
            "e_idx",
            "embedding",
            8,
            MTreeDistanceType::Euclidean,
            MTreeVectorType::F64,
        );
        let diffs = diff_indexes("doc", &[], &[idx]);
        assert!(diffs[0].forward_sql.starts_with("REMOVE INDEX e_idx"));
        assert!(diffs[0].backward_sql.contains("MTREE DIMENSION 8"));
    }

    #[test]
    fn diff_indexes_added_hnsw() {
        let idx = hnsw_index(
            "h_idx",
            "v",
            64,
            HnswDistanceType::Cosine,
            MTreeVectorType::F32,
            Some(200),
            Some(16),
        );
        let diffs = diff_indexes("doc", &[idx], &[]);
        let sql = &diffs[0].forward_sql;
        assert!(sql.contains("HNSW DIMENSION 64"));
        assert!(sql.contains("DIST COSINE"));
        assert!(sql.contains("EFC 200"));
        assert!(sql.contains("M 16"));
    }

    #[test]
    fn diff_indexes_added_hnsw_without_tuning() {
        let idx = hnsw_index(
            "h_idx",
            "v",
            64,
            HnswDistanceType::Euclidean,
            MTreeVectorType::F64,
            None,
            None,
        );
        let diffs = diff_indexes("doc", &[idx], &[]);
        let sql = &diffs[0].forward_sql;
        assert!(!sql.contains("EFC"));
    }

    #[test]
    fn diff_indexes_added_diskann_spells_the_full_tail() {
        let idx = diskann_index(
            "d_idx",
            "v",
            3,
            DiskAnnDistanceType::Cosine,
            MTreeVectorType::F16,
        );
        let diffs = diff_indexes("doc", &[idx], &[]);
        assert_eq!(diffs.len(), 1);
        assert_eq!(
            diffs[0].forward_sql,
            "DEFINE INDEX d_idx ON TABLE doc COLUMNS v DISKANN DIMENSION 3 \
             DIST COSINE TYPE F16 DEGREE 64 L_BUILD 100 ALPHA 1.2;"
        );
        assert_eq!(diffs[0].backward_sql, "REMOVE INDEX d_idx ON TABLE doc;");
    }

    #[test]
    fn diff_indexes_dropped_diskann_recreates_in_backward() {
        let idx = diskann_index(
            "d_idx",
            "v",
            3,
            DiskAnnDistanceType::InnerProduct,
            MTreeVectorType::U8,
        )
        .with_hashed_vector(true);
        let diffs = diff_indexes("doc", &[], &[idx]);
        assert!(diffs[0].forward_sql.starts_with("REMOVE INDEX d_idx"));
        assert!(diffs[0].backward_sql.contains("DISKANN DIMENSION 3"));
        assert!(diffs[0].backward_sql.contains("DIST INNER_PRODUCT"));
        assert!(diffs[0].backward_sql.contains("HASHED_VECTOR"));
    }

    #[test]
    fn diff_indexes_search_index_emits_fulltext_keyword() {
        let idx = IndexDefinition::new("s_idx", ["body"]).with_type(IndexType::Search);
        let diffs = diff_indexes("post", &[idx], &[]);
        // SurrealDB 3.x renders the full-text index with the `FULLTEXT` keyword
        // (renamed from v1/v2 `SEARCH`).
        assert!(diffs[0].forward_sql.contains("FULLTEXT"));
    }

    /// An index that keeps its name but changes kind or columns is
    /// re-defined whole, both ways.
    #[test]
    fn a_changed_index_is_redefined_both_ways() {
        let diffs = diff_indexes(
            "user",
            &[unique_index("email_idx", ["email"])],
            &[index("email_idx", ["email"])],
        );
        assert_eq!(diffs.len(), 1, "{diffs:#?}");
        assert_eq!(diffs[0].operation, DiffOperation::ModifyTable);
        assert_eq!(diffs[0].index.as_deref(), Some("email_idx"));
        assert_eq!(
            diffs[0].forward_sql,
            "DEFINE INDEX OVERWRITE email_idx ON TABLE user COLUMNS email UNIQUE;"
        );
        assert_eq!(
            diffs[0].backward_sql,
            "DEFINE INDEX OVERWRITE email_idx ON TABLE user COLUMNS email;"
        );

        let diffs = diff_indexes("t", &[index("i", ["a", "b"])], &[index("i", ["a"])]);
        assert_eq!(diffs.len(), 1, "{diffs:#?}");
        assert!(diffs[0].forward_sql.contains("COLUMNS a, b"));
    }

    /// What the engine fills in or never stores is not a change.
    #[test]
    fn index_defaults_and_directives_are_not_changes() {
        let bare_hnsw = IndexDefinition {
            dimension: Some(4),
            ..IndexDefinition::new("h", ["v"]).with_type(IndexType::Hnsw)
        };
        let echoed_hnsw = hnsw_index(
            "h",
            "v",
            4,
            HnswDistanceType::Euclidean,
            MTreeVectorType::F32,
            Some(150),
            Some(12),
        );
        assert!(indexes_equal(&bare_hnsw, &echoed_hnsw));

        let fulltext = IndexDefinition::new("s", ["body"]).with_type(IndexType::Search);
        let echoed_fulltext = fulltext.clone().with_analyzer("ascii").with_bm25();
        assert!(indexes_equal(&fulltext, &echoed_fulltext));
        assert!(!indexes_equal(
            &fulltext,
            &fulltext.clone().with_analyzer("english")
        ));

        let concurrent = unique_index("u", ["a"]).with_concurrently(true);
        assert!(diff_indexes("t", &[concurrent], &[unique_index("u", ["a"])]).is_empty());
    }

    #[test]
    fn a_changed_hnsw_tuning_is_a_change() {
        let tuned = |efc| {
            hnsw_index(
                "h",
                "v",
                4,
                HnswDistanceType::Cosine,
                MTreeVectorType::F32,
                Some(efc),
                None,
            )
        };
        let diffs = diff_indexes("t", &[tuned(200)], &[tuned(150)]);
        assert_eq!(diffs.len(), 1, "{diffs:#?}");
        assert!(diffs[0].forward_sql.starts_with("DEFINE INDEX OVERWRITE h"));
        assert!(diffs[0].forward_sql.contains("EFC 200"));
        assert!(diffs[0].backward_sql.contains("EFC 150"));
    }

    // ----- diff_events -----

    #[test]
    fn a_changed_event_is_redefined_both_ways() {
        let old = event("audit", "true", "CREATE log SET n = 1");
        let new = event("audit", "$event = 'CREATE'", "CREATE log SET n = 2");
        let diffs = diff_events("t", std::slice::from_ref(&new), std::slice::from_ref(&old));
        assert_eq!(diffs.len(), 1, "{diffs:#?}");
        assert_eq!(diffs[0].operation, DiffOperation::ModifyTable);
        assert_eq!(diffs[0].event.as_deref(), Some("audit"));
        assert_eq!(
            diffs[0].forward_sql,
            "DEFINE EVENT OVERWRITE audit ON TABLE t WHEN $event = 'CREATE' \
             THEN { CREATE log SET n = 2 };"
        );
        assert_eq!(
            diffs[0].backward_sql,
            "DEFINE EVENT OVERWRITE audit ON TABLE t WHEN true THEN { CREATE log SET n = 1 };"
        );
    }

    /// The engine wraps a bare action in parentheses, keeps a block's
    /// trailing `;`, and drops parentheses around the condition.
    #[test]
    fn the_engine_echo_of_an_event_is_not_a_change() {
        let code = event(
            "audit",
            "($before.a != $after.a)",
            "LET $x = 1; CREATE log SET x = $x",
        );
        let echo = event(
            "audit",
            "$before.a != $after.a",
            "{ LET $x = 1; CREATE log SET x = $x; }",
        );
        assert!(diff_events("t", &[code], &[echo]).is_empty());
        let bare = event("e", "true", "CREATE log SET n = 1");
        let bare_echo = event("e", "true", "(CREATE log SET n = 1)");
        assert!(diff_events("t", &[bare], &[bare_echo]).is_empty());
    }

    #[test]
    fn diff_events_detects_added() {
        let ev = event("on_upd", "true", "RETURN 1");
        let diffs = diff_events("t", &[ev], &[]);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::AddEvent);
        assert_eq!(diffs[0].event.as_deref(), Some("on_upd"));
        assert!(diffs[0].forward_sql.contains("DEFINE EVENT on_upd"));
    }

    #[test]
    fn diff_events_detects_dropped() {
        let ev = event("on_upd", "true", "RETURN 1");
        let diffs = diff_events("t", &[], &[ev]);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::DropEvent);
        assert!(diffs[0].forward_sql.starts_with("REMOVE EVENT on_upd"));
    }

    #[test]
    fn diff_events_identical_yields_nothing() {
        let ev = event("on_upd", "true", "RETURN 1");
        let a = vec![ev];
        assert!(diff_events("t", &a, &a).is_empty());
    }

    // ----- diff_permissions -----

    #[test]
    fn diff_permissions_added() {
        let mut new_perms = BTreeMap::new();
        new_perms.insert("select".into(), "true".into());
        let diffs = diff_permissions("t", Some(&new_perms), None);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::ModifyPermissions);
        assert_eq!(
            diffs[0].forward_sql,
            "ALTER TABLE t PERMISSIONS FOR select WHERE true;"
        );
        assert!(!diffs[0].forward_sql.contains("DEFINE FIELD PERMISSIONS"));
        // The rollback restores the table default rather than doing nothing.
        assert_eq!(diffs[0].backward_sql, "ALTER TABLE t PERMISSIONS NONE;");
    }

    #[test]
    fn diff_permissions_removed_roundtrip() {
        let mut old_perms = BTreeMap::new();
        old_perms.insert("select".into(), "$auth.id = id".into());
        let diffs = diff_permissions("t", None, Some(&old_perms));
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].forward_sql, "ALTER TABLE t PERMISSIONS NONE;");
        assert_eq!(
            diffs[0].backward_sql,
            "ALTER TABLE t PERMISSIONS FOR select WHERE $auth.id = id;"
        );
    }

    #[test]
    fn diff_permissions_modified_carries_old_in_backward() {
        let mut old_perms = BTreeMap::new();
        old_perms.insert("select".into(), "$auth.id = id".into());
        let mut new_perms = BTreeMap::new();
        new_perms.insert("select".into(), "true".into());

        let diffs = diff_permissions("t", Some(&new_perms), Some(&old_perms));
        assert_eq!(diffs.len(), 1);
        assert!(diffs[0].forward_sql.contains("true"));
        assert!(diffs[0].backward_sql.contains("$auth.id = id"));
    }

    #[test]
    fn diff_permissions_identical_yields_nothing() {
        let mut p = BTreeMap::new();
        p.insert("select".into(), "true".into());
        assert!(diff_permissions("t", Some(&p), Some(&p)).is_empty());
    }

    #[test]
    fn diff_permissions_whitespace_variance_is_equal() {
        let mut code = BTreeMap::new();
        code.insert("select".into(), "$auth.id  =  id".into());
        let mut db = BTreeMap::new();
        db.insert("select".into(), "$auth.id = id".into());
        assert!(diff_permissions("t", Some(&code), Some(&db)).is_empty());
    }

    #[test]
    fn diff_permissions_none_and_empty_are_equal() {
        let empty: BTreeMap<String, String> = BTreeMap::new();
        assert!(diff_permissions("t", Some(&empty), None).is_empty());
        assert!(diff_permissions("t", None, Some(&empty)).is_empty());
    }

    // ----- diff_edges: ADD / DROP / MODIFY -----

    fn relation_edge(name: &str) -> EdgeDefinition {
        EdgeDefinition::new(name)
            .with_mode(EdgeMode::Relation)
            .with_from_table("user")
            .with_to_table("post")
    }

    #[test]
    fn diff_edges_detects_added_relation() {
        let code = vec![relation_edge("likes")];
        let diffs = diff_edges(&code, &[]);
        assert!(!diffs.is_empty());
        assert_eq!(diffs[0].operation, DiffOperation::AddTable);
        assert!(diffs[0].forward_sql.contains("TYPE RELATION"));
        assert!(diffs[0].forward_sql.contains("FROM user"));
        assert!(diffs[0].forward_sql.contains("TO post"));
    }

    /// The permissions ride the one `DEFINE TABLE` that creates the edge; a
    /// second statement would fail on the table the first one made.
    #[test]
    fn an_added_edge_carries_its_permissions_inline() {
        let code = vec![relation_edge("likes").with_permissions([("select", "true")])];
        let diffs = diff_edges(&code, &[]);
        assert_eq!(diffs.len(), 1, "{diffs:#?}");
        assert_eq!(
            diffs[0].forward_sql,
            "DEFINE TABLE likes TYPE RELATION FROM user TO post \
             PERMISSIONS FOR select WHERE true;"
        );
    }

    /// A relation edge naming one endpoint is valid to the engine; its
    /// permission change still renders one whole `OVERWRITE` statement.
    #[test]
    fn a_half_constrained_edge_changes_permissions_in_one_statement() {
        let db = EdgeDefinition::new("tagged").with_from_table("user");
        let code = db.clone().with_permissions([("select", "true")]);
        let diffs = diff_edges(&[code], &[db]);
        assert_eq!(diffs.len(), 1, "{diffs:#?}");
        assert_eq!(diffs[0].operation, DiffOperation::ModifyPermissions);
        assert_eq!(
            diffs[0].forward_sql,
            "DEFINE TABLE OVERWRITE tagged TYPE RELATION FROM user \
             PERMISSIONS FOR select WHERE true;"
        );
        assert_eq!(
            diffs[0].backward_sql,
            "DEFINE TABLE OVERWRITE tagged TYPE RELATION FROM user;"
        );
    }

    #[test]
    fn diff_edges_detects_added_schemafull() {
        let code = vec![EdgeDefinition::new("rel").with_mode(EdgeMode::Schemafull)];
        let diffs = diff_edges(&code, &[]);
        assert_eq!(diffs.len(), 1);
        assert!(diffs[0].forward_sql.contains("SCHEMAFULL"));
    }

    #[test]
    fn diff_edges_detects_dropped() {
        let db = vec![relation_edge("likes")];
        let diffs = diff_edges(&[], &db);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::DropTable);
        assert!(diffs[0].forward_sql.starts_with("REMOVE TABLE likes"));
    }

    #[test]
    fn diff_edges_field_added() {
        let old = relation_edge("likes");
        let new = relation_edge("likes").with_fields([f("weight", FieldType::Int)]);
        let diffs = diff_edges(&[new], &[old]);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::AddField);
        assert_eq!(diffs[0].field.as_deref(), Some("weight"));
    }

    #[test]
    fn diff_edges_field_removed() {
        let old = relation_edge("likes").with_fields([f("weight", FieldType::Int)]);
        let new = relation_edge("likes");
        let diffs = diff_edges(&[new], &[old]);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::DropField);
    }

    #[test]
    fn diff_edges_field_modified() {
        let old = relation_edge("likes").with_fields([f("weight", FieldType::Int)]);
        let new = relation_edge("likes").with_fields([f("weight", FieldType::Float)]);
        let diffs = diff_edges(&[new], &[old]);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::ModifyField);
    }

    #[test]
    fn diff_edges_index_and_event_and_perms() {
        let old = relation_edge("likes");
        let new = relation_edge("likes")
            .with_indexes([index("w_idx", ["weight"])])
            .with_events([event("on_like", "true", "RETURN 1")])
            .with_permissions([("select", "true")]);
        let diffs = diff_edges(&[new], &[old]);
        let ops: BTreeSet<DiffOperation> = diffs.iter().map(|d| d.operation).collect();
        assert!(ops.contains(&DiffOperation::AddIndex));
        assert!(ops.contains(&DiffOperation::AddEvent));
        assert!(ops.contains(&DiffOperation::ModifyPermissions));
    }

    /// A relation that changes endpoint or an edge that changes mode is
    /// re-defined whole, both ways.
    #[test]
    fn a_changed_edge_shape_is_redefined_both_ways() {
        let old = relation_edge("likes");
        let new = relation_edge("likes").with_to_table("comment");
        let diffs = diff_edges(std::slice::from_ref(&new), std::slice::from_ref(&old));
        assert_eq!(diffs.len(), 1, "{diffs:#?}");
        assert_eq!(diffs[0].operation, DiffOperation::ModifyTable);
        assert_eq!(
            diffs[0].forward_sql,
            "DEFINE TABLE OVERWRITE likes TYPE RELATION FROM user TO comment;"
        );
        assert_eq!(
            diffs[0].backward_sql,
            "DEFINE TABLE OVERWRITE likes TYPE RELATION FROM user TO post;"
        );

        let schemafull = relation_edge("likes").with_mode(EdgeMode::Schemafull);
        let diffs = diff_edges(&[schemafull], std::slice::from_ref(&old));
        assert_eq!(diffs.len(), 1, "{diffs:#?}");
        assert_eq!(
            diffs[0].forward_sql,
            "DEFINE TABLE OVERWRITE likes SCHEMAFULL;"
        );
    }

    /// Endpoints only render on a relation, so on any other edge they are
    /// not part of its shape.
    #[test]
    fn endpoints_off_a_relation_are_not_a_change() {
        let plain = EdgeDefinition::new("rel").with_mode(EdgeMode::Schemafull);
        let stray = plain.clone().with_from_table("user");
        assert!(diff_edges(&[stray], &[plain]).is_empty());
    }

    #[test]
    fn diff_edges_identical_yields_nothing() {
        let e = relation_edge("likes").with_fields([f("weight", FieldType::Int)]);
        assert!(diff_edges(std::slice::from_ref(&e), std::slice::from_ref(&e)).is_empty());
    }

    #[test]
    fn diff_schemas_includes_buckets() {
        use crate::schema::bucket::memory_bucket;
        let code = SchemaSnapshot::from_all_parts([tbl("user")], [], [memory_bucket("avatars")]);
        let db = SchemaSnapshot::default();
        let diffs = diff_schemas(&code, &db);
        assert!(diffs
            .iter()
            .any(|d| d.operation == DiffOperation::AddBucket));
        assert!(diffs.iter().any(|d| d.operation == DiffOperation::AddTable));
    }
    #[test]
    fn snapshot_without_buckets_key_deserialises() {
        // Older snapshots predate the `buckets` field; #[serde(default)]
        // must let them load with an empty bucket list.
        let json = r#"{ "tables": [], "edges": [] }"#;
        let snap: SchemaSnapshot = serde_json::from_str(json).unwrap();
        assert!(snap.buckets.is_empty());
    }

    // ----- change feeds -----

    #[test]
    fn diff_tables_detects_an_added_changefeed() {
        use crate::schema::ChangeFeed;
        let db = vec![table_schema("audit")];
        let code = vec![table_schema("audit").with_changefeed(ChangeFeed::new("1d"))];
        let diffs = diff_tables(&code, &db);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::ModifyTable);
        assert_eq!(diffs[0].table, "audit");
        assert_eq!(
            diffs[0].forward_sql,
            "DEFINE TABLE OVERWRITE audit SCHEMAFULL CHANGEFEED 1d;"
        );
        assert_eq!(
            diffs[0].backward_sql,
            "DEFINE TABLE OVERWRITE audit SCHEMAFULL;"
        );
    }

    #[test]
    fn diff_tables_detects_a_dropped_changefeed() {
        use crate::schema::ChangeFeed;
        let db = vec![table_schema("audit").with_changefeed(ChangeFeed::new("1d"))];
        let code = vec![table_schema("audit")];
        let diffs = diff_tables(&code, &db);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::ModifyTable);
        assert!(!diffs[0].forward_sql.contains("CHANGEFEED"));
        assert!(diffs[0].backward_sql.contains("CHANGEFEED 1d"));
    }

    #[test]
    fn diff_tables_detects_a_changed_retention_window() {
        use crate::schema::ChangeFeed;
        let db = vec![table_schema("audit").with_changefeed(ChangeFeed::new("1d"))];
        let code = vec![
            table_schema("audit").with_changefeed(ChangeFeed::new("3d").include_original(true))
        ];
        let diffs = diff_tables(&code, &db);
        assert_eq!(diffs.len(), 1);
        assert!(diffs[0]
            .forward_sql
            .contains("CHANGEFEED 3d INCLUDE ORIGINAL"));
    }

    #[test]
    fn diff_tables_ignores_an_unchanged_changefeed() {
        use crate::schema::ChangeFeed;
        let t = table_schema("audit").with_changefeed(ChangeFeed::new("1d"));
        assert!(diff_tables(std::slice::from_ref(&t), std::slice::from_ref(&t)).is_empty());
    }

    // ----- views -----

    #[test]
    fn diff_tables_detects_an_added_view() {
        use crate::schema::{ViewDefinition, ViewGroup};
        let db = vec![table_schema("stats")];
        let code = vec![table_schema("stats").with_view(
            ViewDefinition::new(["count() AS total"], ["comment"]).with_group(ViewGroup::All),
        )];
        let diffs = diff_tables(&code, &db);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].operation, DiffOperation::ModifyTable);
        assert!(diffs[0].description.contains("view"));
        assert!(diffs[0].forward_sql.contains("TYPE NORMAL"));
        assert!(diffs[0]
            .forward_sql
            .contains("AS SELECT count() AS total FROM comment GROUP ALL"));
        assert!(!diffs[0].backward_sql.contains("AS SELECT"));
    }

    #[test]
    fn diff_tables_detects_a_changed_view_body() {
        use crate::schema::ViewDefinition;
        let db = vec![table_schema("stats").with_view(ViewDefinition::new(["id"], ["comment"]))];
        let code = vec![table_schema("stats")
            .with_view(ViewDefinition::new(["id"], ["comment"]).with_condition("n > 2"))];
        let diffs = diff_tables(&code, &db);
        assert_eq!(diffs.len(), 1);
        assert!(diffs[0].forward_sql.contains("WHERE n > 2"));
    }

    #[test]
    fn diff_tables_ignores_view_whitespace_reformatting() {
        use crate::schema::ViewDefinition;
        let db = vec![table_schema("stats")
            .with_view(ViewDefinition::new(["id"], ["comment"]).with_condition("n   >    2"))];
        let code = vec![table_schema("stats")
            .with_view(ViewDefinition::new(["id"], ["comment"]).with_condition("n > 2"))];
        assert!(
            diff_tables(&code, &db).is_empty(),
            "the engine reformats freely; only the meaning may differ"
        );
    }

    // ----- diff_buckets -----

    // ----- diff_schemas aggregator -----

    #[test]
    fn diff_schemas_empty_snapshots_are_equal() {
        let a = SchemaSnapshot::default();
        let b = SchemaSnapshot::default();
        assert!(diff_schemas(&a, &b).is_empty());
    }

    #[test]
    fn diff_schemas_add_tables_and_edges() {
        let code = SchemaSnapshot::from_parts([tbl("user")], [relation_edge("likes")]);
        let db = SchemaSnapshot::default();
        let diffs = diff_schemas(&code, &db);
        let ops: Vec<DiffOperation> = diffs.iter().map(|d| d.operation).collect();
        // At least one AddTable for the user table and one AddTable for the edge.
        assert!(
            ops.iter()
                .filter(|o| **o == DiffOperation::AddTable)
                .count()
                >= 2
        );
    }

    #[test]
    fn diff_schemas_drops_removed_items() {
        let code = SchemaSnapshot::default();
        let db = SchemaSnapshot::from_parts([tbl("old")], [relation_edge("old_rel")]);
        let diffs = diff_schemas(&code, &db);
        let drops = diffs
            .iter()
            .filter(|d| d.operation == DiffOperation::DropTable)
            .count();
        assert_eq!(drops, 2);
    }

    #[test]
    fn diff_schemas_handles_mixed_add_drop_modify() {
        let shared = tbl("user").with_fields([f("email", FieldType::String)]);
        let shared_modified = tbl("user").with_fields([f("email", FieldType::Int)]);
        let code = SchemaSnapshot::from_parts([tbl("new"), shared_modified], []);
        let db = SchemaSnapshot::from_parts([shared, tbl("obsolete")], []);
        let diffs = diff_schemas(&code, &db);
        let ops: BTreeSet<DiffOperation> = diffs.iter().map(|d| d.operation).collect();
        assert!(ops.contains(&DiffOperation::AddTable));
        assert!(ops.contains(&DiffOperation::DropTable));
        assert!(ops.contains(&DiffOperation::ModifyField));
    }

    fn operations(diffs: &[SchemaDiff]) -> Vec<DiffOperation> {
        diffs.iter().map(|d| d.operation).collect()
    }

    /// An index that names an analyzer (or a backfill that calls a function)
    /// needs it defined first.
    #[test]
    fn diff_schemas_defines_objects_before_the_tables_that_use_them() {
        use crate::schema::{bm25_index, standard_analyzer, FunctionDefinition};
        let doc = tbl("doc").with_indexes([bm25_index("body_ft", ["body"], "words")]);
        let code = SchemaSnapshot {
            tables: vec![doc],
            analyzers: vec![standard_analyzer("words")],
            functions: vec![FunctionDefinition::new("greet", "RETURN 'hi'")],
            ..SchemaSnapshot::default()
        };
        let ops = operations(&diff_schemas(&code, &SchemaSnapshot::default()));
        assert_eq!(
            ops,
            vec![
                DiffOperation::AddFunction,
                DiffOperation::AddAnalyzer,
                DiffOperation::AddTable,
                DiffOperation::AddIndex,
            ]
        );
    }

    /// An edge turning into a table of the same name is removed before the
    /// table is defined, whichever pass each half comes from.
    #[test]
    fn diff_schemas_drops_before_it_defines() {
        let code = SchemaSnapshot::from_parts([tbl("x")], []);
        let db = SchemaSnapshot::from_parts([], [relation_edge("x")]);
        let diffs = diff_schemas(&code, &db);
        assert_eq!(
            operations(&diffs),
            vec![DiffOperation::DropTable, DiffOperation::AddTable]
        );
        assert_eq!(diffs[0].forward_sql, "REMOVE TABLE x;");

        let back = diff_schemas(&db, &code);
        assert_eq!(
            operations(&back),
            vec![DiffOperation::DropTable, DiffOperation::AddTable]
        );
        assert!(back[1].forward_sql.contains("TYPE RELATION"));
    }

    /// Objects go once nothing that used them is left: the engine refuses to
    /// remove an analyzer a full-text index still names.
    #[test]
    fn diff_schemas_removes_objects_after_the_tables_that_used_them() {
        use crate::schema::{bm25_index, standard_analyzer};
        let doc = tbl("doc").with_indexes([bm25_index("body_ft", ["body"], "words")]);
        let db = SchemaSnapshot {
            tables: vec![doc.clone()],
            analyzers: vec![standard_analyzer("words")],
            buckets: vec![crate::schema::memory_bucket("files")],
            ..SchemaSnapshot::default()
        };
        let code = SchemaSnapshot::from_parts([tbl("doc")], []);
        let ops = operations(&diff_schemas(&code, &db));
        assert_eq!(
            ops,
            vec![
                DiffOperation::DropIndex,
                DiffOperation::DropBucket,
                DiffOperation::DropAnalyzer,
            ]
        );
    }

    // ----- pair-wise helpers -----

    #[test]
    fn diff_table_pair_add_is_same_as_slice_form() {
        let t = tbl("user");
        let pair = diff_table_pair(Some(&t), None);
        let slice = diff_tables(std::slice::from_ref(&t), &[]);
        assert_eq!(pair, slice);
    }

    #[test]
    fn diff_table_pair_drop_is_same_as_slice_form() {
        let t = tbl("user");
        let pair = diff_table_pair(None, Some(&t));
        let slice = diff_tables(&[], std::slice::from_ref(&t));
        assert_eq!(pair, slice);
    }

    #[test]
    fn diff_table_pair_none_none_is_empty() {
        assert!(diff_table_pair(None, None).is_empty());
    }

    #[test]
    fn diff_edge_pair_none_none_is_empty() {
        assert!(diff_edge_pair(None, None).is_empty());
    }

    #[test]
    fn diff_edge_pair_add_matches_slice_form() {
        let e = relation_edge("likes");
        let pair = diff_edge_pair(Some(&e), None);
        let slice = diff_edges(std::slice::from_ref(&e), &[]);
        assert_eq!(pair, slice);
    }

    // ----- round-trip & details shape -----

    #[test]
    fn modify_field_details_contains_both_types() {
        let code = vec![f("n", FieldType::Int)];
        let db = vec![f("n", FieldType::Float)];
        let diffs = diff_fields("t", &code, &db);
        assert_eq!(diffs.len(), 1);
        let d = &diffs[0];
        assert_eq!(d.details.get("old_type"), Some(&serde_json::json!("float")));
        assert_eq!(d.details.get("new_type"), Some(&serde_json::json!("int")));
    }

    #[test]
    fn add_field_details_contains_type() {
        let code = vec![f("age", FieldType::Int)];
        let diffs = diff_fields("u", &code, &[]);
        assert_eq!(
            diffs[0].details.get("type"),
            Some(&serde_json::json!("int"))
        );
    }

    #[test]
    fn diff_permissions_multiple_entries_render_space_separated() {
        let mut code = BTreeMap::new();
        code.insert("select".into(), "true".into());
        code.insert("create".into(), "true".into());
        let diffs = diff_permissions("t", Some(&code), None);
        let fwd = &diffs[0].forward_sql;
        // One table-level statement carrying both actions inline (the valid
        // placement), not separate malformed DEFINE FIELD statements.
        assert_eq!(fwd.matches("ALTER TABLE").count(), 1);
        assert!(fwd.contains("FOR select WHERE true"));
        assert!(fwd.contains("FOR create WHERE true"));
        assert!(!fwd.contains("DEFINE FIELD PERMISSIONS"));
    }

    #[test]
    fn event_action_is_wrapped_in_braces() {
        let ev = event("e", "true", "RETURN 1");
        let diffs = diff_events("t", &[ev], &[]);
        assert!(diffs[0].forward_sql.contains("THEN { RETURN 1 }"));
    }

    #[test]
    fn modify_field_preserves_name_as_context() {
        let code = vec![f("email", FieldType::String)];
        let db = vec![f("email", FieldType::Int)];
        let diffs = diff_fields("user", &code, &db);
        assert_eq!(diffs[0].table, "user");
        assert_eq!(diffs[0].field.as_deref(), Some("email"));
    }

    // ----- snapshot round-trip -----

    #[test]
    fn snapshot_serde_roundtrip() {
        use crate::schema::bucket::memory_bucket;
        let snap = SchemaSnapshot::from_all_parts(
            [tbl("user")],
            [relation_edge("likes")],
            [memory_bucket("avatars")],
        );
        let j = serde_json::to_string(&snap).unwrap();
        let back: SchemaSnapshot = serde_json::from_str(&j).unwrap();
        assert_eq!(snap, back);
        assert_eq!(back.buckets.len(), 1);
    }

    #[test]
    fn snapshot_default_is_empty() {
        let s = SchemaSnapshot::default();
        assert!(s.tables.is_empty());
        assert!(s.edges.is_empty());
    }

    #[test]
    fn snapshot_new_matches_default() {
        assert_eq!(SchemaSnapshot::new(), SchemaSnapshot::default());
    }

    // ----- sorted_keys / index_by_name are tested indirectly via diff_* -----

    #[test]
    fn diff_tables_sort_stable_across_multiple_adds_drops() {
        let code = vec![tbl("a"), tbl("c")];
        let db = vec![tbl("b"), tbl("d")];
        let diffs = diff_tables(&code, &db);
        let adds: Vec<&str> = diffs
            .iter()
            .filter(|d| d.operation == DiffOperation::AddTable)
            .map(|d| d.table.as_str())
            .collect();
        let drops: Vec<&str> = diffs
            .iter()
            .filter(|d| d.operation == DiffOperation::DropTable)
            .map(|d| d.table.as_str())
            .collect();
        assert_eq!(adds, vec!["a", "c"]);
        assert_eq!(drops, vec!["b", "d"]);
    }

    #[test]
    fn field_expr_comparison_treats_value_whitespace() {
        let a = vec![f("x", FieldType::String).with_value("a  +  b")];
        let b = vec![f("x", FieldType::String).with_value("a + b")];
        assert!(diff_fields("t", &a, &b).is_empty());
    }

    #[test]
    fn field_expr_comparison_treats_default_whitespace() {
        let a = vec![f("x", FieldType::Int).with_default("42  ")];
        let b = vec![f("x", FieldType::Int).with_default("42")];
        assert!(diff_fields("t", &a, &b).is_empty());
    }
}
