//! Query optimization hints.
//!
//! Port of `surql/query/hints.py`. Each hint renders to a SurrealQL comment
//! that the query planner interprets (e.g. `/* USE INDEX user.email_idx */`).

use serde::{Deserialize, Serialize};

use crate::error::{Result, SurqlError};

/// Category of a query hint; used to deduplicate when [`merge_hints`]
/// collapses overlapping hints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HintType {
    /// Index selection hint.
    Index,
    /// Parallel execution hint.
    Parallel,
    /// Query timeout override.
    Timeout,
    /// Fetch strategy hint.
    Fetch,
    /// Include execution plan.
    Explain,
}

/// Render a [`QueryHint`] to its SurrealQL comment form.
pub trait HintExpr {
    /// SurrealQL comment representation.
    fn to_surql(&self) -> String;
    /// Hint category (used by [`merge_hints`]).
    fn hint_type(&self) -> HintType;
}

/// A query hint. Sum type over every supported hint kind.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum QueryHint {
    /// `/* USE INDEX table.index */` or `/* FORCE INDEX table.index */`
    Index(IndexHint),
    /// `/* PARALLEL ... */`
    Parallel(ParallelHint),
    /// `/* TIMEOUT Ns */`
    Timeout(TimeoutHint),
    /// `/* FETCH ... */`
    Fetch(FetchHint),
    /// `/* EXPLAIN */` / `/* EXPLAIN FULL */`
    Explain(ExplainHint),
}

impl HintExpr for QueryHint {
    fn to_surql(&self) -> String {
        match self {
            Self::Index(h) => h.to_surql(),
            Self::Parallel(h) => h.to_surql(),
            Self::Timeout(h) => h.to_surql(),
            Self::Fetch(h) => h.to_surql(),
            Self::Explain(h) => h.to_surql(),
        }
    }

    fn hint_type(&self) -> HintType {
        match self {
            Self::Index(_) => HintType::Index,
            Self::Parallel(_) => HintType::Parallel,
            Self::Timeout(_) => HintType::Timeout,
            Self::Fetch(_) => HintType::Fetch,
            Self::Explain(_) => HintType::Explain,
        }
    }
}

/// Hint to use (or force) a particular index for a table.
///
/// ## Examples
///
/// ```
/// use surql::query::IndexHint;
/// use surql::query::hints::HintExpr;
///
/// let h = IndexHint::new("user", "email_idx");
/// assert_eq!(h.to_surql(), "/* USE INDEX user.email_idx */");
///
/// let forced = IndexHint::new("user", "email_idx").force(true);
/// assert_eq!(forced.to_surql(), "/* FORCE INDEX user.email_idx */");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IndexHint {
    /// Table name the hint applies to.
    pub table: String,
    /// Index name the planner should use.
    pub index: String,
    /// Whether to force the index even when the planner disagrees.
    #[serde(default)]
    pub force: bool,
}

impl IndexHint {
    /// Construct a new [`IndexHint`].
    pub fn new(table: impl Into<String>, index: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            index: index.into(),
            force: false,
        }
    }

    /// Enable the FORCE flag.
    pub fn force(mut self, force: bool) -> Self {
        self.force = force;
        self
    }
}

impl HintExpr for IndexHint {
    fn to_surql(&self) -> String {
        let prefix = if self.force { "FORCE" } else { "USE" };
        format!("/* {prefix} INDEX {}.{} */", self.table, self.index)
    }
    fn hint_type(&self) -> HintType {
        HintType::Index
    }
}

/// Hint controlling parallel execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ParallelHint {
    /// Whether parallel execution is enabled.
    pub enabled: bool,
    /// Maximum parallel worker count (1..=32). `None` means server default.
    #[serde(default)]
    pub max_workers: Option<u8>,
}

impl ParallelHint {
    /// Enable parallel execution with the server-picked worker count.
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            max_workers: None,
        }
    }

    /// Disable parallel execution.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            max_workers: None,
        }
    }

    /// Enable parallel execution capped at `n` workers.
    ///
    /// `n` is clamped to 1..=32; values outside that range return
    /// [`SurqlError::Validation`].
    pub fn with_workers(n: u8) -> Result<Self> {
        if !(1..=32).contains(&n) {
            return Err(SurqlError::Validation {
                reason: format!("ParallelHint max_workers must be in 1..=32, got {n}"),
            });
        }
        Ok(Self {
            enabled: true,
            max_workers: Some(n),
        })
    }
}

impl HintExpr for ParallelHint {
    fn to_surql(&self) -> String {
        if !self.enabled {
            return "/* PARALLEL OFF */".into();
        }
        match self.max_workers {
            Some(n) => format!("/* PARALLEL {n} */"),
            None => "/* PARALLEL ON */".into(),
        }
    }
    fn hint_type(&self) -> HintType {
        HintType::Parallel
    }
}

/// Query timeout override hint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimeoutHint {
    /// Timeout in seconds. Must be positive.
    pub seconds: f64,
}

impl TimeoutHint {
    /// Build a timeout hint. Returns [`SurqlError::Validation`] when `seconds <= 0`.
    pub fn new(seconds: f64) -> Result<Self> {
        if seconds.is_nan() || seconds <= 0.0 {
            return Err(SurqlError::Validation {
                reason: format!("TimeoutHint seconds must be > 0, got {seconds}"),
            });
        }
        Ok(Self { seconds })
    }
}

impl HintExpr for TimeoutHint {
    fn to_surql(&self) -> String {
        format!("/* TIMEOUT {}s */", self.seconds)
    }
    fn hint_type(&self) -> HintType {
        HintType::Timeout
    }
}

/// Fetch strategy requested for record retrieval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FetchStrategy {
    /// Fetch all records up front.
    Eager,
    /// Fetch on demand.
    Lazy,
    /// Fetch in batches (requires `batch_size`).
    Batch,
}

/// Hint controlling how records are fetched.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FetchHint {
    /// Fetch strategy.
    pub strategy: FetchStrategy,
    /// Batch size for [`FetchStrategy::Batch`] (1..=10000).
    #[serde(default)]
    pub batch_size: Option<u32>,
}

impl FetchHint {
    /// Eager fetch strategy.
    pub fn eager() -> Self {
        Self {
            strategy: FetchStrategy::Eager,
            batch_size: None,
        }
    }

    /// Lazy fetch strategy.
    pub fn lazy() -> Self {
        Self {
            strategy: FetchStrategy::Lazy,
            batch_size: None,
        }
    }

    /// Batch fetch with the given size (1..=10000).
    pub fn batch(batch_size: u32) -> Result<Self> {
        if !(1..=10000).contains(&batch_size) {
            return Err(SurqlError::Validation {
                reason: format!("FetchHint batch_size must be in 1..=10000, got {batch_size}"),
            });
        }
        Ok(Self {
            strategy: FetchStrategy::Batch,
            batch_size: Some(batch_size),
        })
    }

    /// Validate internal consistency (Batch strategy requires a batch_size).
    pub fn validate(&self) -> Result<()> {
        if self.strategy == FetchStrategy::Batch && self.batch_size.is_none() {
            return Err(SurqlError::Validation {
                reason: "FetchHint batch_size required when strategy is batch".into(),
            });
        }
        Ok(())
    }
}

impl HintExpr for FetchHint {
    fn to_surql(&self) -> String {
        if self.strategy == FetchStrategy::Batch {
            if let Some(n) = self.batch_size {
                return format!("/* FETCH BATCH {n} */");
            }
        }
        let s = match self.strategy {
            FetchStrategy::Eager => "EAGER",
            FetchStrategy::Lazy => "LAZY",
            FetchStrategy::Batch => "BATCH",
        };
        format!("/* FETCH {s} */")
    }
    fn hint_type(&self) -> HintType {
        HintType::Fetch
    }
}

/// Include query execution plan in the response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExplainHint {
    /// Whether to request the full plan (`/* EXPLAIN FULL */`).
    #[serde(default)]
    pub full: bool,
}

impl ExplainHint {
    /// Short form.
    pub fn short() -> Self {
        Self { full: false }
    }
    /// Full form.
    pub fn full() -> Self {
        Self { full: true }
    }
}

impl HintExpr for ExplainHint {
    fn to_surql(&self) -> String {
        if self.full {
            "/* EXPLAIN FULL */".into()
        } else {
            "/* EXPLAIN */".into()
        }
    }
    fn hint_type(&self) -> HintType {
        HintType::Explain
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Validate a hint against the query's target table. Returns a list of
/// human-readable problems (empty on success).
pub fn validate_hint(hint: &QueryHint, table: Option<&str>) -> Vec<String> {
    let mut errors = Vec::new();
    if let (QueryHint::Index(h), Some(tbl)) = (hint, table) {
        if h.table != tbl {
            errors.push(format!(
                "Index hint table {:?} does not match query table {:?}",
                h.table, tbl
            ));
        }
    }
    errors
}

/// Collapse duplicate hints: later hints of the same [`HintType`] replace
/// earlier ones (preserving insertion order of the kept entries).
pub fn merge_hints(hints: impl IntoIterator<Item = QueryHint>) -> Vec<QueryHint> {
    use std::collections::HashMap;
    let mut map: HashMap<HintType, usize> = HashMap::new();
    let mut out: Vec<QueryHint> = Vec::new();
    for hint in hints {
        let ty = hint.hint_type();
        if let Some(idx) = map.get(&ty) {
            out[*idx] = hint;
        } else {
            map.insert(ty, out.len());
            out.push(hint);
        }
    }
    out
}

/// Render a slice of hints to a single SurrealQL comment string, suitable
/// for prepending to a statement.
pub fn render_hints(hints: &[QueryHint]) -> String {
    if hints.is_empty() {
        return String::new();
    }
    let merged = merge_hints(hints.iter().cloned());
    merged
        .into_iter()
        .map(|h| h.to_surql())
        .collect::<Vec<_>>()
        .join(" ")
}

/// Stateless renderer kept for API symmetry with the Python port.
#[derive(Debug, Default, Clone, Copy)]
pub struct HintRenderer;

impl HintRenderer {
    /// Same as [`render_hints`].
    pub fn render_hints(&self, hints: &[QueryHint]) -> String {
        render_hints(hints)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_hint_renders_use_and_force() {
        assert_eq!(
            IndexHint::new("user", "email_idx").to_surql(),
            "/* USE INDEX user.email_idx */"
        );
        assert_eq!(
            IndexHint::new("user", "email_idx").force(true).to_surql(),
            "/* FORCE INDEX user.email_idx */"
        );
    }

    #[test]
    fn parallel_hint_renders() {
        assert_eq!(ParallelHint::enabled().to_surql(), "/* PARALLEL ON */");
        assert_eq!(ParallelHint::disabled().to_surql(), "/* PARALLEL OFF */");
        assert_eq!(
            ParallelHint::with_workers(4).unwrap().to_surql(),
            "/* PARALLEL 4 */"
        );
    }

    #[test]
    fn parallel_hint_rejects_bad_worker_count() {
        assert!(ParallelHint::with_workers(0).is_err());
        assert!(ParallelHint::with_workers(33).is_err());
    }

    #[test]
    fn timeout_hint_renders() {
        assert_eq!(
            TimeoutHint::new(30.0).unwrap().to_surql(),
            "/* TIMEOUT 30s */"
        );
    }

    #[test]
    fn timeout_hint_rejects_nonpositive() {
        assert!(TimeoutHint::new(0.0).is_err());
        assert!(TimeoutHint::new(-1.0).is_err());
    }

    #[test]
    fn fetch_hint_renders() {
        assert_eq!(FetchHint::eager().to_surql(), "/* FETCH EAGER */");
        assert_eq!(FetchHint::lazy().to_surql(), "/* FETCH LAZY */");
        assert_eq!(
            FetchHint::batch(100).unwrap().to_surql(),
            "/* FETCH BATCH 100 */"
        );
    }

    #[test]
    fn fetch_hint_batch_validates_size() {
        assert!(FetchHint::batch(0).is_err());
        assert!(FetchHint::batch(10_001).is_err());
        // Direct construction with Batch + None fails validate()
        let bad = FetchHint {
            strategy: FetchStrategy::Batch,
            batch_size: None,
        };
        assert!(bad.validate().is_err());
    }

    #[test]
    fn explain_hint_renders() {
        assert_eq!(ExplainHint::short().to_surql(), "/* EXPLAIN */");
        assert_eq!(ExplainHint::full().to_surql(), "/* EXPLAIN FULL */");
    }

    #[test]
    fn validate_hint_checks_table() {
        let idx = QueryHint::Index(IndexHint::new("user", "email_idx"));
        assert!(validate_hint(&idx, Some("user")).is_empty());
        let errs = validate_hint(&idx, Some("post"));
        assert_eq!(errs.len(), 1);
        assert!(errs[0].contains("user"));
        assert!(errs[0].contains("post"));
    }

    #[test]
    fn merge_hints_replaces_duplicates() {
        let h1 = QueryHint::Timeout(TimeoutHint::new(10.0).unwrap());
        let h2 = QueryHint::Timeout(TimeoutHint::new(20.0).unwrap());
        let merged = merge_hints(vec![h1, h2]);
        assert_eq!(merged.len(), 1);
        match &merged[0] {
            QueryHint::Timeout(t) => assert!((t.seconds - 20.0).abs() < f64::EPSILON),
            _ => panic!("expected timeout"),
        }
    }

    #[test]
    fn merge_hints_keeps_distinct_types() {
        let hints = vec![
            QueryHint::Timeout(TimeoutHint::new(30.0).unwrap()),
            QueryHint::Parallel(ParallelHint::enabled()),
            QueryHint::Timeout(TimeoutHint::new(60.0).unwrap()),
        ];
        let merged = merge_hints(hints);
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn render_hints_empty_returns_empty() {
        assert_eq!(render_hints(&[]), "");
    }

    #[test]
    fn render_hints_joins_all() {
        let hints = vec![
            QueryHint::Timeout(TimeoutHint::new(30.0).unwrap()),
            QueryHint::Parallel(ParallelHint::enabled()),
        ];
        let out = render_hints(&hints);
        assert!(out.contains("/* TIMEOUT 30s */"));
        assert!(out.contains("/* PARALLEL ON */"));
    }

    #[test]
    fn renderer_matches_free_function() {
        let hints = vec![QueryHint::Explain(ExplainHint::full())];
        assert_eq!(HintRenderer.render_hints(&hints), render_hints(&hints));
    }
}
