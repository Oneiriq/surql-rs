//! Monotonic ID sequences (`DEFINE SEQUENCE`).
//!
//! A sequence hands out strictly increasing integers across the cluster via
//! `sequence::nextval("<name>")`. Each node reserves a batch of values up
//! front, so `batch` trades allocation round-trips against how large a gap a
//! node restart may leave.
//!
//! Mirrors [`crate::schema::bucket`]: a value object that renders its own
//! `DEFINE` / `REMOVE` DDL, with the inverse (`INFO FOR DB` -> definition) in
//! [`crate::schema::parser`].

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::error::{Result, SurqlError};

/// The engine's default batch size, and what it echoes for a sequence
/// defined without one.
pub const DEFAULT_BATCH: u64 = 1000;

/// The engine's default starting value.
pub const DEFAULT_START: i64 = 0;

/// Immutable `DEFINE SEQUENCE` schema definition.
///
/// `batch` and `start` are always rendered, including at their defaults,
/// because `INFO FOR DB` echoes them either way — omitting them would diff
/// against the database on every reconcile.
///
/// ## Examples
///
/// ```
/// use surql::schema::sequence_schema;
///
/// let s = sequence_schema("invoice_no").start(1000).build().unwrap();
/// assert_eq!(
///     s.to_surql().unwrap(),
///     "DEFINE SEQUENCE invoice_no BATCH 1000 START 1000;"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceDefinition {
    /// Sequence name.
    pub name: String,
    /// How many values each node reserves per allocation.
    #[serde(default = "SequenceDefinition::default_batch")]
    pub batch: u64,
    /// First value handed out.
    #[serde(default = "SequenceDefinition::default_start")]
    pub start: i64,
    /// Optional allocation timeout as a SurrealQL duration literal.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub timeout: Option<String>,
}

impl SequenceDefinition {
    fn default_batch() -> u64 {
        DEFAULT_BATCH
    }

    fn default_start() -> i64 {
        DEFAULT_START
    }

    /// Construct a sequence with the engine's default batch and start.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            batch: DEFAULT_BATCH,
            start: DEFAULT_START,
            timeout: None,
        }
    }

    /// Set the per-node allocation batch size.
    pub fn with_batch(mut self, batch: u64) -> Self {
        self.batch = batch;
        self
    }

    /// Set the first value handed out.
    pub fn with_start(mut self, start: i64) -> Self {
        self.start = start;
        self
    }

    /// Set the allocation timeout (a SurrealQL duration literal).
    pub fn with_timeout(mut self, timeout: impl Into<String>) -> Self {
        self.timeout = Some(timeout.into());
        self
    }

    /// Validate the definition.
    ///
    /// Returns [`SurqlError::Validation`] for an empty name or a zero batch,
    /// which would hand out no values.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(SurqlError::Validation {
                reason: "Sequence name cannot be empty".into(),
            });
        }
        if self.batch == 0 {
            return Err(SurqlError::Validation {
                reason: format!("Sequence {:?} must have a batch of at least 1", self.name),
            });
        }
        Ok(())
    }

    /// Render the `DEFINE SEQUENCE` statement.
    pub fn to_surql(&self) -> Result<String> {
        self.to_surql_with_options(false, false)
    }

    /// Render with optional `IF NOT EXISTS` or `OVERWRITE` guards (mutually
    /// exclusive in SurrealQL; `OVERWRITE` wins, matching the server).
    pub fn to_surql_with_options(&self, if_not_exists: bool, overwrite: bool) -> Result<String> {
        self.validate()?;
        let guard = guard_keyword(if_not_exists, overwrite);
        let mut sql = format!(
            "DEFINE SEQUENCE {guard}{name} BATCH {batch} START {start}",
            guard = guard,
            name = self.name,
            batch = self.batch,
            start = self.start,
        );
        if let Some(timeout) = &self.timeout {
            write!(sql, " TIMEOUT {timeout}").expect("writing to String cannot fail");
        }
        sql.push(';');
        Ok(sql)
    }

    /// Render the `OVERWRITE` form, which replaces a stored definition.
    pub fn to_surql_overwrite(&self) -> Result<String> {
        self.to_surql_with_options(false, true)
    }

    /// Render a `REMOVE SEQUENCE` statement for this sequence.
    pub fn to_remove_surql(&self) -> String {
        Self::remove_surql(&self.name)
    }

    /// Render a `REMOVE SEQUENCE` statement for a sequence by name.
    pub fn remove_surql(name: &str) -> String {
        format!("REMOVE SEQUENCE IF EXISTS {name};")
    }

    /// Render `sequence::nextval("<name>")`, the call that draws the next
    /// value.
    pub fn nextval_surql(name: &str) -> String {
        format!("sequence::nextval(\"{name}\")")
    }
}

/// Shared guard rendering: `OVERWRITE` outranks `IF NOT EXISTS`, matching the
/// server precedence and [`crate::schema::bucket`].
pub(super) fn guard_keyword(if_not_exists: bool, overwrite: bool) -> &'static str {
    if overwrite {
        "OVERWRITE "
    } else if if_not_exists {
        "IF NOT EXISTS "
    } else {
        ""
    }
}

/// Builder for a [`SequenceDefinition`].
#[derive(Debug, Clone)]
pub struct SequenceSchemaBuilder {
    inner: SequenceDefinition,
}

impl SequenceSchemaBuilder {
    /// Set the per-node allocation batch size.
    pub fn batch(mut self, batch: u64) -> Self {
        self.inner.batch = batch;
        self
    }

    /// Set the first value handed out.
    pub fn start(mut self, start: i64) -> Self {
        self.inner.start = start;
        self
    }

    /// Set the allocation timeout.
    pub fn timeout(mut self, timeout: impl Into<String>) -> Self {
        self.inner.timeout = Some(timeout.into());
        self
    }

    /// Finalise the builder, validating the definition.
    pub fn build(self) -> Result<SequenceDefinition> {
        self.inner.validate()?;
        Ok(self.inner)
    }
}

/// Functional constructor for a [`SequenceDefinition`].
///
/// ## Examples
///
/// ```
/// use surql::schema::sequence_schema;
///
/// let s = sequence_schema("order_no").batch(500).timeout("5s").build().unwrap();
/// assert_eq!(
///     s.to_surql().unwrap(),
///     "DEFINE SEQUENCE order_no BATCH 500 START 0 TIMEOUT 5s;"
/// );
/// ```
pub fn sequence_schema(name: impl Into<String>) -> SequenceSchemaBuilder {
    SequenceSchemaBuilder {
        inner: SequenceDefinition::new(name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_rendered_because_the_engine_echoes_them() {
        let s = SequenceDefinition::new("bare");
        assert_eq!(
            s.to_surql().unwrap(),
            "DEFINE SEQUENCE bare BATCH 1000 START 0;"
        );
    }

    #[test]
    fn batch_start_and_timeout_render() {
        let s = sequence_schema("s")
            .batch(500)
            .start(10)
            .timeout("5s")
            .build()
            .unwrap();
        assert_eq!(
            s.to_surql().unwrap(),
            "DEFINE SEQUENCE s BATCH 500 START 10 TIMEOUT 5s;"
        );
    }

    #[test]
    fn guards_render() {
        let s = SequenceDefinition::new("g");
        assert!(s
            .to_surql_with_options(true, false)
            .unwrap()
            .starts_with("DEFINE SEQUENCE IF NOT EXISTS g"));
        assert!(s
            .to_surql_overwrite()
            .unwrap()
            .starts_with("DEFINE SEQUENCE OVERWRITE g"));
    }

    #[test]
    fn overwrite_wins_over_if_not_exists() {
        let s = SequenceDefinition::new("g");
        assert!(s
            .to_surql_with_options(true, true)
            .unwrap()
            .starts_with("DEFINE SEQUENCE OVERWRITE g"));
    }

    #[test]
    fn remove_and_nextval_statements() {
        assert_eq!(
            SequenceDefinition::remove_surql("s"),
            "REMOVE SEQUENCE IF EXISTS s;"
        );
        assert_eq!(
            SequenceDefinition::new("s").to_remove_surql(),
            "REMOVE SEQUENCE IF EXISTS s;"
        );
        assert_eq!(
            SequenceDefinition::nextval_surql("s"),
            "sequence::nextval(\"s\")"
        );
    }

    #[test]
    fn validate_rejects_empty_name_and_zero_batch() {
        let mut s = SequenceDefinition::new("s");
        s.name = String::new();
        assert!(s.validate().is_err());
        assert!(SequenceDefinition::new("s")
            .with_batch(0)
            .validate()
            .is_err());
        assert!(sequence_schema("").build().is_err());
    }

    #[test]
    fn negative_start_is_allowed() {
        let s = SequenceDefinition::new("s").with_start(-5);
        assert!(s.to_surql().unwrap().contains("START -5"));
    }

    #[test]
    fn serde_roundtrip_and_legacy_defaults() {
        let s = sequence_schema("s")
            .batch(2)
            .start(3)
            .timeout("1s")
            .build()
            .unwrap();
        let json = serde_json::to_string(&s).unwrap();
        let back: SequenceDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);

        let legacy: SequenceDefinition = serde_json::from_str(r#"{"name":"s"}"#).unwrap();
        assert_eq!(legacy.batch, DEFAULT_BATCH);
        assert_eq!(legacy.start, DEFAULT_START);
        assert!(legacy.timeout.is_none());
    }
}
