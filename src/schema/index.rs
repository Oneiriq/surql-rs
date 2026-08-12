//! `DEFINE INDEX` schema definitions.
//!
//! Split out of [`super::table`] so both modules stay under the repository's
//! 1000-LOC budget. Everything here is re-exported from `schema::table`, so
//! existing paths keep resolving.
//!
//! Covers the plain / `UNIQUE` / `FULLTEXT` forms plus the `MTREE` and `HNSW`
//! vector indexes, and the `CONCURRENTLY` build directive that lets a large
//! index populate in the background.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::error::{Result, SurqlError};

/// Index type supported by `DEFINE INDEX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum IndexType {
    /// UNIQUE index.
    Unique,
    /// Full-text index. Renders the SurrealDB 3.x `FULLTEXT` keyword (the v1/v2
    /// `SEARCH` spelling was renamed in 3.0); pair with an analyzer + BM25 via
    /// [`bm25_index`] for scorable lexical recall.
    Search,
    /// Plain b-tree style index.
    Standard,
    /// MTREE vector similarity index.
    Mtree,
    /// HNSW vector similarity index.
    Hnsw,
}

impl IndexType {
    /// Render as SurrealQL keyword (matching the Python enum values).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unique => "UNIQUE",
            Self::Search => "FULLTEXT",
            Self::Standard => "INDEX",
            Self::Mtree => "MTREE",
            Self::Hnsw => "HNSW",
        }
    }
}

impl std::fmt::Display for IndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Distance metric for MTREE vector indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum MTreeDistanceType {
    /// Cosine distance.
    Cosine,
    /// Euclidean (L2) distance.
    Euclidean,
    /// Manhattan (L1) distance.
    Manhattan,
    /// Minkowski distance.
    Minkowski,
}

impl MTreeDistanceType {
    /// Render as SurrealQL keyword.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cosine => "COSINE",
            Self::Euclidean => "EUCLIDEAN",
            Self::Manhattan => "MANHATTAN",
            Self::Minkowski => "MINKOWSKI",
        }
    }
}

impl std::fmt::Display for MTreeDistanceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Distance metric for HNSW vector indexes (superset of [`MTreeDistanceType`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HnswDistanceType {
    /// Chebyshev distance.
    Chebyshev,
    /// Cosine distance.
    Cosine,
    /// Euclidean distance.
    Euclidean,
    /// Hamming distance.
    Hamming,
    /// Jaccard distance.
    Jaccard,
    /// Manhattan distance.
    Manhattan,
    /// Minkowski distance.
    Minkowski,
    /// Pearson correlation distance.
    Pearson,
}

impl HnswDistanceType {
    /// Render as SurrealQL keyword.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Chebyshev => "CHEBYSHEV",
            Self::Cosine => "COSINE",
            Self::Euclidean => "EUCLIDEAN",
            Self::Hamming => "HAMMING",
            Self::Jaccard => "JACCARD",
            Self::Manhattan => "MANHATTAN",
            Self::Minkowski => "MINKOWSKI",
            Self::Pearson => "PEARSON",
        }
    }
}

impl std::fmt::Display for HnswDistanceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Numeric type for vector components in MTREE/HNSW indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum MTreeVectorType {
    /// 64-bit float.
    F64,
    /// 32-bit float.
    F32,
    /// 64-bit integer.
    I64,
    /// 32-bit integer.
    I32,
    /// 16-bit integer.
    I16,
}

impl MTreeVectorType {
    /// Render as SurrealQL keyword.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::F64 => "F64",
            Self::F32 => "F32",
            Self::I64 => "I64",
            Self::I32 => "I32",
            Self::I16 => "I16",
        }
    }
}

impl std::fmt::Display for MTreeVectorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Immutable index definition describing one or more columns of a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDefinition {
    /// Index name.
    pub name: String,
    /// Columns participating in the index.
    pub columns: Vec<String>,
    /// Index kind.
    #[serde(rename = "type", default = "IndexDefinition::default_type")]
    pub index_type: IndexType,
    /// MTREE/HNSW dimension.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub dimension: Option<u32>,
    /// MTREE distance metric.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub distance: Option<MTreeDistanceType>,
    /// MTREE/HNSW vector component type.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub vector_type: Option<MTreeVectorType>,
    /// HNSW-specific distance metric.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub hnsw_distance: Option<HnswDistanceType>,
    /// HNSW exploration factor during construction.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub efc: Option<u32>,
    /// HNSW maximum bidirectional links per node.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub m: Option<u32>,
    /// Full-text `SEARCH` analyzer name. `None` renders the historical default
    /// (`ascii`); set via [`IndexDefinition::with_analyzer`].
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub analyzer: Option<String>,
    /// Whether a `SEARCH` index emits the `BM25` relevance-scoring clause —
    /// required for [`Query::search_score`](crate::query::builder::Query::search_score)
    /// to return a value. Uses the engine's default `(k1, b)` parameters.
    #[serde(default)]
    pub bm25: bool,
    /// Whether a `SEARCH` index stores positional `HIGHLIGHTS` data (enables
    /// `search::highlight` / `search::offsets`).
    #[serde(default)]
    pub highlights: bool,
    /// Whether the index builds in the background (`CONCURRENTLY`) instead of
    /// blocking the statement until every existing row is indexed.
    ///
    /// This is a build directive, not part of the stored definition: v3.0.5
    /// accepts it and then echoes the index back from `INFO FOR TABLE`
    /// **without** it. The parser therefore always reports `false`, and
    /// nothing compares this member — a `CONCURRENTLY` index that also
    /// diffed on it would re-apply on every reconcile. Watch the build with
    /// [`info_for_index_surql`] / [`IndexBuildStatus`].
    #[serde(default)]
    pub concurrently: bool,
}

impl IndexDefinition {
    fn default_type() -> IndexType {
        IndexType::Standard
    }

    /// Build a minimal [`IndexDefinition`] with only name and columns.
    pub fn new<I, S>(name: impl Into<String>, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            name: name.into(),
            columns: columns.into_iter().map(Into::into).collect(),
            index_type: IndexType::Standard,
            dimension: None,
            distance: None,
            vector_type: None,
            hnsw_distance: None,
            efc: None,
            m: None,
            analyzer: None,
            bm25: false,
            highlights: false,
            concurrently: false,
        }
    }

    /// Set the index kind.
    pub fn with_type(mut self, index_type: IndexType) -> Self {
        self.index_type = index_type;
        self
    }

    /// Set the full-text `SEARCH` analyzer (e.g. one defined via
    /// [`analyzer`](crate::schema::analyzer)). Only affects `SEARCH` indexes;
    /// when unset the index renders the historical `ascii` analyzer.
    pub fn with_analyzer(mut self, analyzer: impl Into<String>) -> Self {
        self.analyzer = Some(analyzer.into());
        self
    }

    /// Emit the `BM25` relevance-scoring clause on a `SEARCH` index (with the
    /// engine's default parameters). Required for
    /// [`search::score`](crate::query::builder::Query::search_score).
    pub fn with_bm25(mut self) -> Self {
        self.bm25 = true;
        self
    }

    /// Store positional `HIGHLIGHTS` data on a `SEARCH` index.
    pub fn with_highlights(mut self) -> Self {
        self.highlights = true;
        self
    }

    /// Build the index in the background (`CONCURRENTLY`).
    ///
    /// The `DEFINE INDEX` statement returns immediately and the engine
    /// populates the index behind it, so a large table stays writable.
    /// Poll [`info_for_index_surql`] until [`IndexBuildStatus::is_ready`].
    pub fn with_concurrently(mut self, concurrently: bool) -> Self {
        self.concurrently = concurrently;
        self
    }

    /// Validate the index definition.
    ///
    /// Returns [`SurqlError::Validation`] when the name or column list is
    /// empty, or when vector-index fields are missing required members.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(SurqlError::Validation {
                reason: "Index name cannot be empty".into(),
            });
        }
        if self.columns.is_empty() {
            return Err(SurqlError::Validation {
                reason: format!("Index {:?} must have at least one column", self.name),
            });
        }
        if matches!(self.index_type, IndexType::Mtree | IndexType::Hnsw) && self.dimension.is_none()
        {
            return Err(SurqlError::Validation {
                reason: format!("Vector index {:?} requires a dimension", self.name),
            });
        }
        Ok(())
    }

    /// Render the `DEFINE INDEX` statement for this index on the given table.
    pub fn to_surql(&self, table: &str) -> String {
        self.to_surql_with_options(table, false)
    }

    /// Render with optional `IF NOT EXISTS` clause.
    pub fn to_surql_with_options(&self, table: &str, if_not_exists: bool) -> String {
        self.render_guard(table, if if_not_exists { " IF NOT EXISTS" } else { "" })
    }

    /// Render with `OVERWRITE`, replacing an existing definition while
    /// leaving stored data untouched. What schema evolution applies
    /// when a stored definition no longer matches the code.
    pub fn to_surql_overwrite(&self, table: &str) -> String {
        self.render_guard(table, " OVERWRITE")
    }

    fn render_guard(&self, table: &str, ine: &str) -> String {
        match self.index_type {
            IndexType::Mtree => {
                let field = self.columns.first().map_or("", String::as_str);
                let dim = self.dimension.unwrap_or(0);
                let mut sql = format!(
                    "DEFINE INDEX{ine} {name} ON TABLE {table} COLUMNS {field} MTREE DIMENSION {dim}",
                    ine = ine,
                    name = self.name,
                    table = table,
                    field = field,
                    dim = dim,
                );
                if let Some(d) = self.distance {
                    write!(sql, " DIST {}", d.as_str()).expect("writing to String cannot fail");
                }
                if let Some(vt) = self.vector_type {
                    write!(sql, " TYPE {}", vt.as_str()).expect("writing to String cannot fail");
                }
                self.push_tail(&mut sql);
                sql
            }
            IndexType::Hnsw => {
                let field = self.columns.first().map_or("", String::as_str);
                let dim = self.dimension.unwrap_or(0);
                let mut sql = format!(
                    "DEFINE INDEX{ine} {name} ON TABLE {table} COLUMNS {field} HNSW DIMENSION {dim}",
                    ine = ine,
                    name = self.name,
                    table = table,
                    field = field,
                    dim = dim,
                );
                if let Some(d) = self.hnsw_distance {
                    write!(sql, " DIST {}", d.as_str()).expect("writing to String cannot fail");
                }
                if let Some(vt) = self.vector_type {
                    write!(sql, " TYPE {}", vt.as_str()).expect("writing to String cannot fail");
                }
                if let Some(efc) = self.efc {
                    write!(sql, " EFC {efc}").expect("writing to String cannot fail");
                }
                if let Some(m) = self.m {
                    write!(sql, " M {m}").expect("writing to String cannot fail");
                }
                self.push_tail(&mut sql);
                sql
            }
            _ => {
                let columns = self.columns.join(", ");
                let mut sql = format!(
                    "DEFINE INDEX{ine} {name} ON TABLE {table} COLUMNS {columns}",
                    ine = ine,
                    name = self.name,
                    table = table,
                    columns = columns,
                );
                match self.index_type {
                    IndexType::Unique => sql.push_str(" UNIQUE"),
                    IndexType::Search => {
                        let analyzer = self.analyzer.as_deref().unwrap_or("ascii");
                        write!(sql, " FULLTEXT ANALYZER {analyzer}")
                            .expect("writing to String cannot fail");
                        if self.bm25 {
                            sql.push_str(" BM25");
                        }
                        if self.highlights {
                            sql.push_str(" HIGHLIGHTS");
                        }
                    }
                    _ => {}
                }
                self.push_tail(&mut sql);
                sql
            }
        }
    }

    /// Close a rendered statement, appending the `CONCURRENTLY` build
    /// directive first. SurrealQL puts it last, after every type-specific
    /// clause.
    fn push_tail(&self, sql: &mut String) {
        if self.concurrently {
            sql.push_str(" CONCURRENTLY");
        }
        sql.push(';');
    }
}

/// Render `INFO FOR INDEX <name> ON <table>`, the statement that reports how
/// far a [`CONCURRENTLY`](IndexDefinition::concurrently) build has got.
///
/// ## Examples
///
/// ```
/// use surql::schema::info_for_index_surql;
///
/// assert_eq!(
///     info_for_index_surql("email_idx", "user"),
///     "INFO FOR INDEX email_idx ON user;"
/// );
/// ```
pub fn info_for_index_surql(name: &str, table: &str) -> String {
    format!("INFO FOR INDEX {name} ON {table};")
}

/// Progress of a background index build, as reported by
/// [`info_for_index_surql`].
///
/// The engine answers with `{ building: { status: 'indexing', initial, pending,
/// updated } }` while it works and `{ building: { status: 'ready' } }` when it
/// is done, so the counters are optional.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct IndexBuildStatus {
    /// Reported status word (`indexing`, `ready`, ...).
    pub status: String,
    /// Rows counted when the build started.
    #[serde(default)]
    pub initial: u64,
    /// Rows still queued.
    #[serde(default)]
    pub pending: u64,
    /// Rows written since the build started.
    #[serde(default)]
    pub updated: u64,
}

impl IndexBuildStatus {
    /// Read the status out of an `INFO FOR INDEX` response body.
    ///
    /// Accepts the `{ building: { ... } }` object the engine returns, the
    /// inner `{ ... }` on its own, or the single-element array
    /// `DatabaseClient::query` wraps results in. Returns `None` when no
    /// `status` is present.
    pub fn from_info(info: &serde_json::Value) -> Option<Self> {
        let value = match info {
            serde_json::Value::Array(items) => items.first()?,
            other => other,
        };
        let body = value.get("building").unwrap_or(value);
        let status = body.get("status")?.as_str()?.to_string();
        let count = |key: &str| {
            body.get(key)
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
        };
        Some(Self {
            status,
            initial: count("initial"),
            pending: count("pending"),
            updated: count("updated"),
        })
    }

    /// Whether the build has finished.
    pub fn is_ready(&self) -> bool {
        self.status.eq_ignore_ascii_case("ready")
    }
}

/// Build a standard index.
pub fn index<I, S>(name: impl Into<String>, columns: I) -> IndexDefinition
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    IndexDefinition::new(name, columns)
}

/// Build a `UNIQUE` index.
pub fn unique_index<I, S>(name: impl Into<String>, columns: I) -> IndexDefinition
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    IndexDefinition::new(name, columns).with_type(IndexType::Unique)
}

/// Build a full-text `SEARCH` index. With no analyzer set it renders the
/// historical `ascii` default; chain [`IndexDefinition::with_analyzer`] /
/// [`IndexDefinition::with_bm25`] / [`IndexDefinition::with_highlights`] for a
/// scorable index, or use [`bm25_index`].
pub fn search_index<I, S>(name: impl Into<String>, columns: I) -> IndexDefinition
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    IndexDefinition::new(name, columns).with_type(IndexType::Search)
}

/// Build a BM25-scored full-text `SEARCH` index over `columns`, analyzed by
/// `analyzer`. This is the index to pair with
/// [`Query::fulltext_search`](crate::query::builder::Query::fulltext_search) and
/// [`Query::search_score`](crate::query::builder::Query::search_score) for
/// lexical recall — BM25 is what makes `search::score` return a relevance value.
///
/// ## Examples
///
/// ```
/// use surql::schema::bm25_index;
///
/// let idx = bm25_index("content_bm25", ["content"], "text_en");
/// assert_eq!(
///     idx.to_surql("memory"),
///     "DEFINE INDEX content_bm25 ON TABLE memory COLUMNS content FULLTEXT ANALYZER text_en BM25;"
/// );
/// ```
pub fn bm25_index<I, S>(
    name: impl Into<String>,
    columns: I,
    analyzer: impl Into<String>,
) -> IndexDefinition
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    IndexDefinition::new(name, columns)
        .with_type(IndexType::Search)
        .with_analyzer(analyzer)
        .with_bm25()
}

/// Build an MTREE vector index.
pub fn mtree_index(
    name: impl Into<String>,
    column: impl Into<String>,
    dimension: u32,
    distance: MTreeDistanceType,
    vector_type: MTreeVectorType,
) -> IndexDefinition {
    IndexDefinition {
        name: name.into(),
        columns: vec![column.into()],
        index_type: IndexType::Mtree,
        dimension: Some(dimension),
        distance: Some(distance),
        vector_type: Some(vector_type),
        hnsw_distance: None,
        efc: None,
        m: None,
        analyzer: None,
        bm25: false,
        highlights: false,
        concurrently: false,
    }
}

/// Build an HNSW vector index.
///
/// `efc` and `m` are optional tuning parameters; when omitted, the server
/// defaults are used.
pub fn hnsw_index(
    name: impl Into<String>,
    column: impl Into<String>,
    dimension: u32,
    distance: HnswDistanceType,
    vector_type: MTreeVectorType,
    efc: Option<u32>,
    m: Option<u32>,
) -> IndexDefinition {
    IndexDefinition {
        name: name.into(),
        columns: vec![column.into()],
        index_type: IndexType::Hnsw,
        dimension: Some(dimension),
        distance: None,
        vector_type: Some(vector_type),
        hnsw_distance: Some(distance),
        efc,
        m,
        analyzer: None,
        bm25: false,
        highlights: false,
        concurrently: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrently_renders_last() {
        let idx = unique_index("email_idx", ["email"]).with_concurrently(true);
        assert_eq!(
            idx.to_surql("user"),
            "DEFINE INDEX email_idx ON TABLE user COLUMNS email UNIQUE CONCURRENTLY;"
        );
    }

    #[test]
    fn concurrently_is_off_by_default() {
        assert!(!index("i", ["a"]).concurrently);
        assert!(!index("i", ["a"]).to_surql("t").contains("CONCURRENTLY"));
    }

    #[test]
    fn concurrently_composes_with_the_guards() {
        let idx = index("i", ["a"]).with_concurrently(true);
        assert_eq!(
            idx.to_surql_with_options("t", true),
            "DEFINE INDEX IF NOT EXISTS i ON TABLE t COLUMNS a CONCURRENTLY;"
        );
        assert_eq!(
            idx.to_surql_overwrite("t"),
            "DEFINE INDEX OVERWRITE i ON TABLE t COLUMNS a CONCURRENTLY;"
        );
    }

    #[test]
    fn concurrently_renders_on_the_vector_forms() {
        let mtree = mtree_index("m", "v", 8, MTreeDistanceType::Cosine, MTreeVectorType::F32)
            .with_concurrently(true);
        assert!(mtree.to_surql("t").ends_with("TYPE F32 CONCURRENTLY;"));
        let hnsw = hnsw_index(
            "h",
            "v",
            8,
            HnswDistanceType::Cosine,
            MTreeVectorType::F32,
            Some(64),
            Some(8),
        )
        .with_concurrently(true);
        assert!(hnsw.to_surql("t").ends_with("M 8 CONCURRENTLY;"));
    }

    #[test]
    fn concurrently_survives_serde_and_defaults_on_old_snapshots() {
        let idx = index("i", ["a"]).with_concurrently(true);
        let json = serde_json::to_string(&idx).unwrap();
        let back: IndexDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(idx, back);
        let legacy: IndexDefinition =
            serde_json::from_str(r#"{"name":"i","columns":["a"]}"#).unwrap();
        assert!(!legacy.concurrently);
    }

    #[test]
    fn info_for_index_statement() {
        assert_eq!(
            info_for_index_surql("email_idx", "user"),
            "INFO FOR INDEX email_idx ON user;"
        );
    }

    #[test]
    fn build_status_reads_the_indexing_shape() {
        let info = serde_json::json!({
            "building": { "initial": 100, "pending": 20, "status": "indexing", "updated": 4 }
        });
        let status = IndexBuildStatus::from_info(&info).expect("status");
        assert_eq!(status.status, "indexing");
        assert_eq!(status.initial, 100);
        assert_eq!(status.pending, 20);
        assert_eq!(status.updated, 4);
        assert!(!status.is_ready());
    }

    #[test]
    fn build_status_reads_the_ready_shape_through_the_client_array() {
        let info = serde_json::json!([{ "building": { "status": "ready" } }]);
        let status = IndexBuildStatus::from_info(&info).expect("status");
        assert!(status.is_ready());
        assert_eq!(status.initial, 0);
    }

    #[test]
    fn build_status_is_none_without_a_status() {
        assert!(IndexBuildStatus::from_info(&serde_json::json!({})).is_none());
    }

    #[test]
    fn index_type_strings() {
        assert_eq!(IndexType::Unique.as_str(), "UNIQUE");
        assert_eq!(IndexType::Standard.as_str(), "INDEX");
        assert_eq!(IndexType::Mtree.as_str(), "MTREE");
        assert_eq!(IndexType::Hnsw.as_str(), "HNSW");
    }

    #[test]
    fn mtree_distance_display() {
        assert_eq!(format!("{}", MTreeDistanceType::Cosine), "COSINE");
    }

    #[test]
    fn hnsw_distance_display() {
        assert_eq!(format!("{}", HnswDistanceType::Chebyshev), "CHEBYSHEV");
    }

    #[test]
    fn mtree_vector_type_display() {
        assert_eq!(format!("{}", MTreeVectorType::F32), "F32");
    }

    #[test]
    fn index_new_defaults_to_standard() {
        let idx = index("title_idx", ["title"]);
        assert_eq!(idx.index_type, IndexType::Standard);
    }

    #[test]
    fn unique_index_to_surql() {
        let idx = unique_index("email_idx", ["email"]);
        assert_eq!(
            idx.to_surql("user"),
            "DEFINE INDEX email_idx ON TABLE user COLUMNS email UNIQUE;"
        );
    }

    #[test]
    fn standard_index_to_surql() {
        let idx = index("title_idx", ["title"]);
        assert_eq!(
            idx.to_surql("post"),
            "DEFINE INDEX title_idx ON TABLE post COLUMNS title;"
        );
    }

    #[test]
    fn search_index_to_surql() {
        let idx = search_index("content_search", ["title", "content"]);
        assert_eq!(
            idx.to_surql("post"),
            "DEFINE INDEX content_search ON TABLE post COLUMNS title, content FULLTEXT ANALYZER ascii;"
        );
    }

    #[test]
    fn bm25_index_renders_analyzer_and_bm25() {
        let idx = bm25_index("content_bm25", ["content"], "text_en");
        assert_eq!(
            idx.to_surql("memory"),
            "DEFINE INDEX content_bm25 ON TABLE memory COLUMNS content FULLTEXT ANALYZER text_en BM25;"
        );
    }

    #[test]
    fn search_index_with_analyzer_bm25_highlights() {
        let idx = search_index("s", ["content"])
            .with_analyzer("text_en")
            .with_bm25()
            .with_highlights();
        assert_eq!(
            idx.to_surql("doc"),
            "DEFINE INDEX s ON TABLE doc COLUMNS content FULLTEXT ANALYZER text_en BM25 HIGHLIGHTS;"
        );
    }

    #[test]
    fn bm25_index_if_not_exists() {
        let idx = bm25_index("content_bm25", ["content"], "text_en");
        assert_eq!(
            idx.to_surql_with_options("memory", true),
            "DEFINE INDEX IF NOT EXISTS content_bm25 ON TABLE memory COLUMNS content \
             FULLTEXT ANALYZER text_en BM25;"
        );
    }

    #[test]
    fn mtree_index_to_surql() {
        let idx = mtree_index(
            "embedding_idx",
            "embedding",
            1536,
            MTreeDistanceType::Cosine,
            MTreeVectorType::F32,
        );
        let sql = idx.to_surql("doc");
        assert!(sql.contains(
            "DEFINE INDEX embedding_idx ON TABLE doc COLUMNS embedding MTREE DIMENSION 1536"
        ));
        assert!(sql.contains("DIST COSINE"));
        assert!(sql.contains("TYPE F32"));
    }

    #[test]
    fn hnsw_index_to_surql_with_efc_m() {
        let idx = hnsw_index(
            "feat_idx",
            "features",
            128,
            HnswDistanceType::Cosine,
            MTreeVectorType::F32,
            Some(500),
            Some(16),
        );
        let sql = idx.to_surql("doc");
        assert!(sql.contains("HNSW DIMENSION 128"));
        assert!(sql.contains("DIST COSINE"));
        assert!(sql.contains("TYPE F32"));
        assert!(sql.contains("EFC 500"));
        assert!(sql.contains("M 16"));
    }

    #[test]
    fn hnsw_index_without_efc_m_omits_them() {
        let idx = hnsw_index(
            "feat_idx",
            "features",
            64,
            HnswDistanceType::Euclidean,
            MTreeVectorType::F64,
            None,
            None,
        );
        let sql = idx.to_surql("doc");
        assert!(!sql.contains("EFC"));
        assert!(!sql.contains("M 12"));
    }

    #[test]
    fn index_to_surql_if_not_exists() {
        let idx = unique_index("email_idx", ["email"]);
        assert_eq!(
            idx.to_surql_with_options("user", true),
            "DEFINE INDEX IF NOT EXISTS email_idx ON TABLE user COLUMNS email UNIQUE;"
        );
    }

    #[test]
    fn index_validate_rejects_empty_name() {
        let mut idx = unique_index("x", ["a"]);
        idx.name = String::new();
        assert!(idx.validate().is_err());
    }

    #[test]
    fn index_validate_rejects_empty_columns() {
        let idx = IndexDefinition::new("x", Vec::<String>::new()).with_type(IndexType::Unique);
        assert!(idx.validate().is_err());
    }

    #[test]
    fn index_validate_mtree_requires_dimension() {
        let mut idx = IndexDefinition::new("x", ["v"]).with_type(IndexType::Mtree);
        assert!(idx.validate().is_err());
        idx.dimension = Some(64);
        assert!(idx.validate().is_ok());
    }

    #[test]
    fn index_validate_hnsw_requires_dimension() {
        let idx = IndexDefinition::new("x", ["v"]).with_type(IndexType::Hnsw);
        assert!(idx.validate().is_err());
    }
}
