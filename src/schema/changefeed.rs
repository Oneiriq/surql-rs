//! Table change feeds (`DEFINE TABLE ... CHANGEFEED <duration>`).
//!
//! A change feed makes the engine retain a log of every mutation on a table
//! for `duration`, readable with `SHOW CHANGES FOR TABLE` (see
//! [`crate::query::changes`]). `INCLUDE ORIGINAL` adds the pre-mutation row
//! to each entry, which is what a consumer needs to compute a delta rather
//! than just observe the new state.
//!
//! The retention window is a SurrealQL duration literal (`1h`, `3d`, `2w`),
//! passed through verbatim; the engine is the authority on what it accepts.

use serde::{Deserialize, Serialize};

/// Change-feed configuration for a [`TableDefinition`](super::table::TableDefinition).
///
/// ## Examples
///
/// ```
/// use surql::schema::ChangeFeed;
///
/// assert_eq!(ChangeFeed::new("1d").to_clause(), " CHANGEFEED 1d");
/// assert_eq!(
///     ChangeFeed::new("3d").include_original(true).to_clause(),
///     " CHANGEFEED 3d INCLUDE ORIGINAL",
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeFeed {
    /// Retention window as a SurrealQL duration literal (`1h`, `3d`, `2w`).
    pub duration: String,
    /// Whether each entry carries the pre-mutation row (`INCLUDE ORIGINAL`).
    #[serde(default)]
    pub include_original: bool,
}

impl ChangeFeed {
    /// Construct a change feed retained for `duration`.
    pub fn new(duration: impl Into<String>) -> Self {
        Self {
            duration: duration.into(),
            include_original: false,
        }
    }

    /// Include the pre-mutation row in every entry.
    pub fn include_original(mut self, include: bool) -> Self {
        self.include_original = include;
        self
    }

    /// Render the ` CHANGEFEED <duration> [INCLUDE ORIGINAL]` clause, ready to
    /// append to a `DEFINE TABLE` statement.
    pub fn to_clause(&self) -> String {
        let original = if self.include_original {
            " INCLUDE ORIGINAL"
        } else {
            ""
        };
        format!(" CHANGEFEED {}{original}", self.duration)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{table_schema, TableMode};

    #[test]
    fn clause_without_original() {
        assert_eq!(ChangeFeed::new("1h").to_clause(), " CHANGEFEED 1h");
    }

    #[test]
    fn clause_with_original() {
        assert_eq!(
            ChangeFeed::new("2w").include_original(true).to_clause(),
            " CHANGEFEED 2w INCLUDE ORIGINAL"
        );
    }

    #[test]
    fn table_renders_the_clause_between_mode_and_permissions() {
        let t = table_schema("audit")
            .with_mode(TableMode::Schemaless)
            .with_changefeed(ChangeFeed::new("1d"))
            .with_permissions([("select", "true")]);
        assert_eq!(
            t.to_surql(),
            "DEFINE TABLE audit SCHEMALESS CHANGEFEED 1d PERMISSIONS FOR select WHERE true;"
        );
    }

    #[test]
    fn table_without_a_changefeed_renders_unchanged() {
        assert_eq!(
            table_schema("plain").to_surql(),
            "DEFINE TABLE plain SCHEMAFULL;"
        );
    }

    #[test]
    fn guards_carry_the_clause() {
        let t = table_schema("audit").with_changefeed(ChangeFeed::new("6h").include_original(true));
        assert_eq!(
            t.to_surql_with_options(true),
            "DEFINE TABLE IF NOT EXISTS audit SCHEMAFULL CHANGEFEED 6h INCLUDE ORIGINAL;"
        );
        assert_eq!(
            t.to_surql_overwrite(),
            "DEFINE TABLE OVERWRITE audit SCHEMAFULL CHANGEFEED 6h INCLUDE ORIGINAL;"
        );
    }

    #[test]
    fn serde_roundtrip_and_legacy_default() {
        let t = table_schema("a").with_changefeed(ChangeFeed::new("1d"));
        let json = serde_json::to_string(&t).unwrap();
        let back: crate::schema::TableDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(t, back);

        let legacy: crate::schema::TableDefinition =
            serde_json::from_str(r#"{"name":"a","mode":"SCHEMAFULL"}"#).unwrap();
        assert!(legacy.changefeed.is_none());
    }
}
