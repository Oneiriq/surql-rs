//! `DEFINE INDEX` schema definitions.
//!
//! Split out of [`super::table`] so both modules stay under the repository's
//! 1000-LOC budget. Everything here is re-exported from `schema::table`, so
//! existing paths keep resolving.
//!
//! Covers the plain / `UNIQUE` / `FULLTEXT` / `COUNT` forms plus the `HNSW`
//! and `DISKANN` vector indexes, and the `CONCURRENTLY` build directive that
//! lets a large index populate in the background. The vector vocabulary
//! (distance metrics, element types, vector builders) lives in
//! [`super::index_vector`] and is re-exported here. `MTREE` is kept only so
//! existing code and snapshots still load: SurrealDB 3 has no MTREE index,
//! and validation refuses one.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::error::{Result, SurqlError};
use crate::types::escape::quote_ident;

use super::fields::render_field_path;

#[allow(deprecated)] // re-exports `mtree_index`, kept for existing code
pub use super::index_vector::{
    diskann_index, hnsw_index, mtree_index, DiskAnnDistanceType, HnswDistanceType,
    MTreeDistanceType, MTreeVectorType, DISKANN_DEFAULT_ALPHA, DISKANN_DEFAULT_DEGREE,
    DISKANN_DEFAULT_L_BUILD,
};

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
    /// MTREE vector similarity index. SurrealDB 3 removed it: the engine
    /// refuses the statement, and [`IndexDefinition::validate`] refuses the
    /// definition. Kept so existing code and snapshots still load.
    #[deprecated(
        since = "0.34.0",
        note = "SurrealDB 3 has no MTREE index; use IndexType::Hnsw or IndexType::Diskann"
    )]
    Mtree,
    /// HNSW vector similarity index.
    Hnsw,
    /// DISKANN vector similarity index (SurrealDB 3.2+): an on-disk graph
    /// built for corpora too large for HNSW's memory residency. Reached by
    /// the same `<|k,ef|>` KNN operator.
    Diskann,
    /// COUNT index: keeps the number of the table's records, or of those
    /// matching its [`condition`](IndexDefinition::condition), so
    /// `SELECT count() FROM t GROUP ALL` reads it instead of scanning.
    /// Takes no columns. Build one with [`count_index`].
    Count,
}

impl IndexType {
    /// Render as SurrealQL keyword (matching the Python enum values).
    pub fn as_str(self) -> &'static str {
        #[allow(deprecated)]
        match self {
            Self::Unique => "UNIQUE",
            Self::Search => "FULLTEXT",
            Self::Standard => "INDEX",
            Self::Mtree => "MTREE",
            Self::Hnsw => "HNSW",
            Self::Diskann => "DISKANN",
            Self::Count => "COUNT",
        }
    }

    /// `true` for the kinds SurrealDB 3 no longer has (`MTREE`).
    #[allow(deprecated)]
    pub fn is_removed(self) -> bool {
        self == Self::Mtree
    }
}

impl std::fmt::Display for IndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Immutable index definition describing one or more columns of a table.
// The bools mirror independent DDL flags (BM25 / HIGHLIGHTS / HASHED_VECTOR /
// CONCURRENTLY); folding them into enums would only rename the same states.
#[allow(clippy::struct_excessive_bools)]
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
    /// DISKANN-specific distance metric.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub diskann_distance: Option<DiskAnnDistanceType>,
    /// DISKANN graph out-degree (`DEGREE`, engine default 64).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub degree: Option<u32>,
    /// DISKANN build-time candidate list size (`L_BUILD`, engine default 100).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub l_build: Option<u32>,
    /// DISKANN pruning slack (`ALPHA`, engine default 1.2), stored as the
    /// decimal literal the statement carries. The engine echoes a float
    /// literal with a trailing `f` suffix (`ALPHA 1.2f`), which the parser
    /// strips so code and echo compare equal.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub alpha: Option<String>,
    /// Whether a DISKANN index stores hashed vectors (`HASHED_VECTOR`).
    #[serde(default)]
    pub hashed_vector: bool,
    /// Full-text `SEARCH` analyzer name. `None` renders the historical default
    /// (`ascii`); set via [`IndexDefinition::with_analyzer`].
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub analyzer: Option<String>,
    /// Whether a `SEARCH` index emits the `BM25` relevance-scoring clause —
    /// required for [`Query::search_score`](crate::query::builder::Query::search_score)
    /// to return a value. Uses the engine's default `(k1, b)` parameters.
    ///
    /// SurrealDB 3.x scores every `FULLTEXT` index with BM25 and always
    /// echoes `BM25(k1,b)`, so a parsed full-text index always has this set.
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
    /// The `WHERE` condition of a `COUNT` index: only records matching it
    /// are counted. `None` counts every record.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub condition: Option<String>,
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
            diskann_distance: None,
            degree: None,
            l_build: None,
            alpha: None,
            hashed_vector: false,
            analyzer: None,
            bm25: false,
            highlights: false,
            concurrently: false,
            condition: None,
        }
    }

    /// Count only the records matching `condition` (a `COUNT` index's
    /// `WHERE` clause).
    pub fn with_condition(mut self, condition: impl Into<String>) -> Self {
        self.condition = Some(condition.into());
        self
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

    /// Set the DISKANN graph out-degree (`DEGREE`).
    pub fn with_degree(mut self, degree: u32) -> Self {
        self.degree = Some(degree);
        self
    }

    /// Set the DISKANN build-time candidate list size (`L_BUILD`).
    pub fn with_l_build(mut self, l_build: u32) -> Self {
        self.l_build = Some(l_build);
        self
    }

    /// Set the DISKANN pruning slack (`ALPHA`). Stored via the canonical
    /// `Display` rendering (`1.2` stays `1.2`, `2.0` becomes `2`), which is
    /// the shape the engine echoes back once its `f` suffix is stripped.
    pub fn with_alpha(mut self, alpha: f64) -> Self {
        self.alpha = Some(alpha.to_string());
        self
    }

    /// Store hashed vectors on a DISKANN index (`HASHED_VECTOR`).
    pub fn with_hashed_vector(mut self, hashed_vector: bool) -> Self {
        self.hashed_vector = hashed_vector;
        self
    }

    /// Validate the index definition.
    ///
    /// Returns [`SurqlError::Validation`] when the name is empty, when an
    /// index other than `COUNT` has no columns (a `COUNT` index takes
    /// none), when a condition is set on anything but a `COUNT` index, for
    /// an `MTREE` index (SurrealDB 3 has none), when vector-index fields are
    /// missing required members, or when a vector index carries a member
    /// combination the engine is known to refuse (see
    /// [`Self::validate_vector_members`]).
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(SurqlError::Validation {
                reason: "Index name cannot be empty".into(),
            });
        }
        if self.index_type.is_removed() {
            return Err(SurqlError::Validation {
                reason: format!(
                    "Index {:?} is an MTREE index, which SurrealDB 3 does not have; \
                     use hnsw_index or diskann_index",
                    self.name
                ),
            });
        }
        if self.index_type == IndexType::Count {
            if !self.columns.is_empty() {
                return Err(SurqlError::Validation {
                    reason: format!(
                        "COUNT index {:?} takes no columns; the engine refuses a count \
                         index with fields",
                        self.name
                    ),
                });
            }
            return Ok(());
        }
        if self.condition.is_some() {
            return Err(SurqlError::Validation {
                reason: format!(
                    "Index {:?} has a condition, which only a COUNT index takes",
                    self.name
                ),
            });
        }
        if self.columns.is_empty() {
            return Err(SurqlError::Validation {
                reason: format!("Index {:?} must have at least one column", self.name),
            });
        }
        if self.index_type == IndexType::Search && self.columns.len() != 1 {
            return Err(SurqlError::Validation {
                reason: format!(
                    "FULLTEXT index {:?} takes exactly one column; the engine refuses {}",
                    self.name,
                    self.columns.len()
                ),
            });
        }
        if matches!(self.index_type, IndexType::Hnsw | IndexType::Diskann)
            && self.dimension.is_none()
        {
            return Err(SurqlError::Validation {
                reason: format!("Vector index {:?} requires a dimension", self.name),
            });
        }
        self.validate_vector_members()
    }

    /// Engine-shaped refusals for the vector kinds, caught before a
    /// statement is sent (all probed against SurrealDB 3.2.4).
    ///
    /// - DISKANN accepts only `F32` / `F16` / `I8` / `U8` element types.
    /// - DISKANN accepts only the metrics [`DiskAnnDistanceType`] can spell
    ///   (`EUCLIDEAN`, `COSINE`, `INNER_PRODUCT`, `COSINE_NORMALIZED`); a
    ///   metric aimed at it through the MTREE/HNSW members would be silently
    ///   dropped by the renderer, so that mistake is refused here instead.
    ///
    /// HNSW accepts every [`MTreeVectorType`] variant, so it needs no check.
    fn validate_vector_members(&self) -> Result<()> {
        if self.index_type == IndexType::Diskann {
            if let Some(vt) = self.vector_type {
                if !matches!(
                    vt,
                    MTreeVectorType::F32
                        | MTreeVectorType::F16
                        | MTreeVectorType::I8
                        | MTreeVectorType::U8
                ) {
                    return Err(SurqlError::Validation {
                        reason: format!(
                            "DISKANN index {:?} cannot use TYPE {}: the engine only accepts \
                             F32, F16, I8, or U8 for DISKANN",
                            self.name,
                            vt.as_str()
                        ),
                    });
                }
            }
            if self.distance.is_some() || self.hnsw_distance.is_some() {
                return Err(SurqlError::Validation {
                    reason: format!(
                        "DISKANN index {:?} takes its metric through diskann_distance \
                         (EUCLIDEAN, COSINE, INNER_PRODUCT, or COSINE_NORMALIZED); the engine \
                         refuses every other MTREE/HNSW metric for DISKANN",
                        self.name
                    ),
                });
            }
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

    /// `DEFINE INDEX <name> ON TABLE <table> COLUMNS <columns>`, the part
    /// every index kind shares, with the names quoted where needed.
    fn render_head(&self, table: &str, ine: &str, columns: &[String]) -> String {
        let columns: Vec<String> = columns.iter().map(|c| render_field_path(c)).collect();
        format!(
            "DEFINE INDEX{ine} {name} ON TABLE {table} COLUMNS {columns}",
            name = quote_ident(&self.name),
            table = quote_ident(table),
            columns = columns.join(", "),
        )
    }

    fn render_guard(&self, table: &str, ine: &str) -> String {
        let first_column = self.columns.first().cloned().unwrap_or_default();
        let single = std::slice::from_ref(&first_column);
        #[allow(deprecated)]
        match self.index_type {
            IndexType::Count => {
                let mut sql = format!(
                    "DEFINE INDEX{ine} {name} ON TABLE {table} COUNT",
                    name = quote_ident(&self.name),
                    table = quote_ident(table),
                );
                if let Some(condition) = &self.condition {
                    let _ = write!(sql, " WHERE {condition}");
                }
                self.push_tail(&mut sql);
                sql
            }
            IndexType::Mtree => {
                let mut sql = self.render_head(table, ine, single);
                let _ = write!(sql, " MTREE DIMENSION {}", self.dimension.unwrap_or(0));
                if let Some(d) = self.distance {
                    let _ = write!(sql, " DIST {}", d.as_str());
                }
                if let Some(vt) = self.vector_type {
                    let _ = write!(sql, " TYPE {}", vt.as_str());
                }
                self.push_tail(&mut sql);
                sql
            }
            IndexType::Hnsw => {
                let mut sql = self.render_head(table, ine, single);
                let _ = write!(sql, " HNSW DIMENSION {}", self.dimension.unwrap_or(0));
                if let Some(d) = self.hnsw_distance {
                    let _ = write!(sql, " DIST {}", d.as_str());
                }
                if let Some(vt) = self.vector_type {
                    let _ = write!(sql, " TYPE {}", vt.as_str());
                }
                if let Some(efc) = self.efc {
                    let _ = write!(sql, " EFC {efc}");
                }
                if let Some(m) = self.m {
                    let _ = write!(sql, " M {m}");
                }
                self.push_tail(&mut sql);
                sql
            }
            IndexType::Diskann => self.render_diskann(table, ine, single),
            _ => {
                let mut sql = self.render_head(table, ine, &self.columns);
                match self.index_type {
                    IndexType::Unique => sql.push_str(" UNIQUE"),
                    IndexType::Search => {
                        let analyzer = self.analyzer.as_deref().unwrap_or("ascii");
                        let _ = write!(sql, " FULLTEXT ANALYZER {}", quote_ident(analyzer));
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

    /// Render the `DISKANN` form. The engine always echoes DIST / TYPE /
    /// DEGREE / `L_BUILD` / ALPHA back with its defaults filled in, even when
    /// the definition never stated them, so this spells them all — the same
    /// lesson as the sequence BATCH/START echo. A definition that omitted
    /// one would never compare equal to its own echo, and a reconcile would
    /// re-apply the index on every boot.
    fn render_diskann(&self, table: &str, ine: &str, column: &[String]) -> String {
        let dim = self.dimension.unwrap_or(0);
        let dist = self
            .diskann_distance
            .unwrap_or(DiskAnnDistanceType::Euclidean);
        let vt = self.vector_type.unwrap_or(MTreeVectorType::F32);
        let degree = self.degree.unwrap_or(DISKANN_DEFAULT_DEGREE);
        let l_build = self.l_build.unwrap_or(DISKANN_DEFAULT_L_BUILD);
        let alpha = self.alpha.as_deref().unwrap_or(DISKANN_DEFAULT_ALPHA);
        let mut sql = self.render_head(table, ine, column);
        let _ = write!(
            sql,
            " DISKANN DIMENSION {dim} DIST {dist} TYPE {vt} DEGREE {degree} \
             L_BUILD {l_build} ALPHA {alpha}",
            dist = dist.as_str(),
            vt = vt.as_str(),
        );
        if self.hashed_vector {
            sql.push_str(" HASHED_VECTOR");
        }
        self.push_tail(&mut sql);
        sql
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
    format!(
        "INFO FOR INDEX {} ON {};",
        quote_ident(name),
        quote_ident(table)
    )
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

/// Build a `COUNT` index, which keeps the table's record count so
/// `SELECT count() FROM t GROUP ALL` does not scan. Chain
/// [`IndexDefinition::with_condition`] to count only matching records.
///
/// ## Examples
///
/// ```
/// use surql::schema::count_index;
///
/// assert_eq!(
///     count_index("active_users").with_condition("active = true").to_surql("user"),
///     "DEFINE INDEX active_users ON TABLE user COUNT WHERE active = true;"
/// );
/// ```
pub fn count_index(name: impl Into<String>) -> IndexDefinition {
    IndexDefinition::new(name, Vec::<String>::new()).with_type(IndexType::Count)
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
/// let idx = bm25_index("body_bm25", ["body"], "text_en");
/// assert_eq!(
///     idx.to_surql("memory"),
///     "DEFINE INDEX body_bm25 ON TABLE memory COLUMNS body FULLTEXT ANALYZER text_en BM25;"
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

#[cfg(test)]
#[allow(deprecated)] // MTREE stays covered: old snapshots and echoes still load
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
        assert_eq!(IndexType::Diskann.as_str(), "DISKANN");
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

    /// SurrealDB 3 has no MTREE index; the statement is a parse error on
    /// every 3.x engine, so no MTREE definition validates.
    #[test]
    fn index_validate_refuses_mtree() {
        for vt in [MTreeVectorType::F32, MTreeVectorType::F64] {
            let idx = mtree_index("x", "v", 8, MTreeDistanceType::Cosine, vt);
            let err = idx.validate().expect_err("SurrealDB 3 has no MTREE");
            assert!(
                err.to_string().contains("hnsw_index or diskann_index"),
                "{err}"
            );
        }
        assert!(IndexType::Mtree.is_removed());
        assert!(!IndexType::Hnsw.is_removed());
    }

    #[test]
    fn count_index_renders_without_columns() {
        assert_eq!(
            count_index("n").to_surql("user"),
            "DEFINE INDEX n ON TABLE user COUNT;"
        );
        assert_eq!(
            count_index("n")
                .with_condition("active = true AND age > 18")
                .to_surql_overwrite("user"),
            "DEFINE INDEX OVERWRITE n ON TABLE user COUNT WHERE active = true AND age > 18;"
        );
        assert_eq!(
            count_index("n").with_concurrently(true).to_surql("user"),
            "DEFINE INDEX n ON TABLE user COUNT CONCURRENTLY;"
        );
        assert_eq!(IndexType::Count.as_str(), "COUNT");
    }

    #[test]
    fn count_index_validation() {
        assert!(count_index("n").validate().is_ok());
        assert!(count_index("n").with_condition("a > 1").validate().is_ok());
        let with_columns = IndexDefinition::new("n", ["a"]).with_type(IndexType::Count);
        let err = with_columns.validate().expect_err("COUNT takes no columns");
        assert!(err.to_string().contains("takes no columns"), "{err}");
        let err = index("i", ["a"])
            .with_condition("a > 1")
            .validate()
            .expect_err("only COUNT takes a condition");
        assert!(err.to_string().contains("only a COUNT index"), "{err}");
    }

    #[test]
    fn index_validate_hnsw_requires_dimension() {
        let idx = IndexDefinition::new("x", ["v"]).with_type(IndexType::Hnsw);
        assert!(idx.validate().is_err());
    }

    #[test]
    fn index_validate_diskann_requires_dimension() {
        let mut idx = IndexDefinition::new("x", ["v"]).with_type(IndexType::Diskann);
        assert!(idx.validate().is_err());
        idx.dimension = Some(64);
        assert!(idx.validate().is_ok());
    }

    #[test]
    fn index_validate_refuses_wide_types_on_diskann() {
        for vt in [
            MTreeVectorType::F64,
            MTreeVectorType::I64,
            MTreeVectorType::I32,
            MTreeVectorType::I16,
        ] {
            let idx = diskann_index("x", "v", 8, DiskAnnDistanceType::Cosine, vt);
            let err = idx.validate().expect_err("DISKANN refuses the wide types");
            assert!(err.to_string().contains("F32, F16, I8, or U8"), "{err}");
        }
        for vt in [
            MTreeVectorType::F32,
            MTreeVectorType::F16,
            MTreeVectorType::I8,
            MTreeVectorType::U8,
        ] {
            let idx = diskann_index("x", "v", 8, DiskAnnDistanceType::Cosine, vt);
            assert!(idx.validate().is_ok());
        }
    }

    #[test]
    fn index_validate_refuses_a_foreign_metric_on_diskann() {
        let mut idx = diskann_index(
            "x",
            "v",
            8,
            DiskAnnDistanceType::Cosine,
            MTreeVectorType::F32,
        );
        idx.hnsw_distance = Some(HnswDistanceType::Manhattan);
        let err = idx.validate().expect_err("DISKANN refuses HNSW metrics");
        assert!(err.to_string().contains("diskann_distance"), "{err}");
    }

    #[test]
    fn diskann_renders_the_defaults_even_when_unset() {
        // A hand-assembled definition with an empty tail still spells the
        // engine's defaults, per the echo discipline.
        let mut idx = IndexDefinition::new("pc", ["v"]).with_type(IndexType::Diskann);
        idx.dimension = Some(3);
        assert_eq!(
            idx.to_surql("t"),
            "DEFINE INDEX pc ON TABLE t COLUMNS v DISKANN DIMENSION 3 \
             DIST EUCLIDEAN TYPE F32 DEGREE 64 L_BUILD 100 ALPHA 1.2;"
        );
    }

    #[test]
    fn with_alpha_stores_the_canonical_decimal() {
        let idx = diskann_index(
            "pc",
            "v",
            3,
            DiskAnnDistanceType::Cosine,
            MTreeVectorType::F32,
        )
        .with_alpha(1.5);
        assert_eq!(idx.alpha.as_deref(), Some("1.5"));
        assert_eq!(
            diskann_index(
                "pc",
                "v",
                3,
                DiskAnnDistanceType::Cosine,
                MTreeVectorType::F32
            )
            .with_alpha(2.0)
            .alpha
            .as_deref(),
            Some("2")
        );
    }

    #[test]
    fn diskann_survives_serde_and_defaults_on_old_snapshots() {
        let idx = diskann_index(
            "pc",
            "v",
            3,
            DiskAnnDistanceType::CosineNormalized,
            MTreeVectorType::I8,
        )
        .with_degree(48)
        .with_hashed_vector(true);
        let json = serde_json::to_string(&idx).unwrap();
        let back: IndexDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(idx, back);
        // Stored snapshots and old contracts predate every DISKANN member;
        // they must keep deserialising with the members defaulted off.
        let legacy: IndexDefinition = serde_json::from_str(
            r#"{"name":"i","columns":["a"],"type":"HNSW","dimension":8,"efc":150}"#,
        )
        .unwrap();
        assert_eq!(legacy.index_type, IndexType::Hnsw);
        assert_eq!(legacy.diskann_distance, None);
        assert_eq!(legacy.degree, None);
        assert_eq!(legacy.l_build, None);
        assert_eq!(legacy.alpha, None);
        assert!(!legacy.hashed_vector);
    }
}
