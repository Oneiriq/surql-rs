//! Reading a table's change feed (`SHOW CHANGES FOR TABLE`).
//!
//! The read half of [`ChangeFeed`](crate::schema::ChangeFeed): once a table
//! retains a mutation log, `SHOW CHANGES FOR TABLE <t> SINCE <point> LIMIT <n>`
//! replays it. Each entry carries a `versionstamp` and a list of `changes`;
//! feeding the last `versionstamp` back as [`ChangeSince::Versionstamp`] is
//! how a consumer resumes where it stopped.
//!
//! The statement is rendered rather than built through
//! [`Query`](crate::query::builder::Query) because `SHOW` is not a `SELECT`
//! and shares none of its clauses.

use serde::{Deserialize, Serialize};

use crate::error::{Result, SurqlError};

/// Where a `SHOW CHANGES` read starts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeSince {
    /// A versionstamp, as returned by a previous read. `1` is "from the
    /// beginning of the retained window".
    Versionstamp(u64),
    /// An RFC 3339 timestamp, rendered as the `d'...'` datetime literal.
    Timestamp(String),
}

impl ChangeSince {
    /// Render as the `SINCE` operand.
    pub fn to_surql(&self) -> String {
        match self {
            Self::Versionstamp(v) => v.to_string(),
            Self::Timestamp(ts) => format!("d'{ts}'"),
        }
    }
}

impl From<u64> for ChangeSince {
    fn from(value: u64) -> Self {
        Self::Versionstamp(value)
    }
}

/// Render `SHOW CHANGES FOR TABLE <table> SINCE <since> [LIMIT <limit>]`.
///
/// Returns [`SurqlError::Validation`] for an empty table name or a `limit` of
/// zero, which the engine rejects.
///
/// ## Examples
///
/// ```
/// use surql::query::changes::{show_changes_surql, ChangeSince};
///
/// assert_eq!(
///     show_changes_surql("audit", &ChangeSince::Versionstamp(1), Some(10)).unwrap(),
///     "SHOW CHANGES FOR TABLE audit SINCE 1 LIMIT 10;",
/// );
/// assert_eq!(
///     show_changes_surql(
///         "audit",
///         &ChangeSince::Timestamp("2026-01-01T00:00:00Z".into()),
///         None,
///     )
///     .unwrap(),
///     "SHOW CHANGES FOR TABLE audit SINCE d'2026-01-01T00:00:00Z';",
/// );
/// ```
pub fn show_changes_surql(table: &str, since: &ChangeSince, limit: Option<u32>) -> Result<String> {
    if table.trim().is_empty() {
        return Err(SurqlError::Validation {
            reason: "SHOW CHANGES requires a table name".into(),
        });
    }
    if limit == Some(0) {
        return Err(SurqlError::Validation {
            reason: "SHOW CHANGES limit must be at least 1".into(),
        });
    }
    let limit = limit.map(|n| format!(" LIMIT {n}")).unwrap_or_default();
    Ok(format!(
        "SHOW CHANGES FOR TABLE {table} SINCE {since}{limit};",
        since = since.to_surql(),
    ))
}

/// One entry of a change-feed read: the mutations recorded at a single
/// versionstamp.
///
/// `changes` is left as raw JSON because its shape varies by mutation kind
/// (`define_table`, `update`, `delete`, ...) and by whether the feed was
/// declared `INCLUDE ORIGINAL`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangeSet {
    /// Monotonic position in the feed. Pass it back as
    /// [`ChangeSince::Versionstamp`] to resume after this entry.
    pub versionstamp: u64,
    /// The mutations recorded at this position.
    #[serde(default)]
    pub changes: Vec<serde_json::Value>,
}

impl ChangeSet {
    /// Read every entry out of a `SHOW CHANGES` response.
    ///
    /// Accepts the bare array of entries or the single-element wrapper
    /// `DatabaseClient::query` returns. Entries that do not carry a
    /// `versionstamp` are skipped.
    pub fn from_response(value: &serde_json::Value) -> Vec<Self> {
        let Some(items) = value.as_array() else {
            return Vec::new();
        };
        // `query` wraps each statement's result, so a lone nested array is the
        // wrapper rather than the entries.
        if items.len() == 1 && items[0].is_array() {
            return Self::from_response(&items[0]);
        }
        items
            .iter()
            .filter_map(|entry| {
                Some(Self {
                    versionstamp: entry.get("versionstamp")?.as_u64()?,
                    changes: entry
                        .get("changes")
                        .and_then(serde_json::Value::as_array)
                        .cloned()
                        .unwrap_or_default(),
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versionstamp_since_renders_bare() {
        assert_eq!(ChangeSince::Versionstamp(42).to_surql(), "42");
        assert_eq!(ChangeSince::from(7u64), ChangeSince::Versionstamp(7));
    }

    #[test]
    fn timestamp_since_renders_a_datetime_literal() {
        assert_eq!(
            ChangeSince::Timestamp("2026-01-01T00:00:00Z".into()).to_surql(),
            "d'2026-01-01T00:00:00Z'"
        );
    }

    #[test]
    fn statement_with_limit() {
        assert_eq!(
            show_changes_surql("audit", &ChangeSince::Versionstamp(1), Some(10)).unwrap(),
            "SHOW CHANGES FOR TABLE audit SINCE 1 LIMIT 10;"
        );
    }

    #[test]
    fn statement_without_limit() {
        assert_eq!(
            show_changes_surql("audit", &ChangeSince::Versionstamp(1), None).unwrap(),
            "SHOW CHANGES FOR TABLE audit SINCE 1;"
        );
    }

    #[test]
    fn empty_table_is_rejected() {
        assert!(show_changes_surql("  ", &ChangeSince::Versionstamp(1), None).is_err());
    }

    #[test]
    fn zero_limit_is_rejected() {
        assert!(show_changes_surql("t", &ChangeSince::Versionstamp(1), Some(0)).is_err());
    }

    #[test]
    fn response_parses_through_the_client_wrapper() {
        let response = serde_json::json!([[
            { "versionstamp": 12, "changes": [{ "update": { "id": "audit:a" } }] },
            { "versionstamp": 13, "changes": [] }
        ]]);
        let sets = ChangeSet::from_response(&response);
        assert_eq!(sets.len(), 2);
        assert_eq!(sets[0].versionstamp, 12);
        assert_eq!(sets[0].changes.len(), 1);
        assert!(sets[1].changes.is_empty());
    }

    #[test]
    fn response_parses_a_bare_entry_list() {
        let response = serde_json::json!([{ "versionstamp": 1, "changes": [] }]);
        assert_eq!(ChangeSet::from_response(&response).len(), 1);
    }

    #[test]
    fn entries_without_a_versionstamp_are_skipped() {
        let response = serde_json::json!([{ "changes": [] }]);
        assert!(ChangeSet::from_response(&response).is_empty());
        assert!(ChangeSet::from_response(&serde_json::json!({})).is_empty());
    }

    #[test]
    fn since_survives_serde() {
        for since in [
            ChangeSince::Versionstamp(9),
            ChangeSince::Timestamp("2026-01-01T00:00:00Z".into()),
        ] {
            let json = serde_json::to_string(&since).unwrap();
            let back: ChangeSince = serde_json::from_str(&json).unwrap();
            assert_eq!(since, back);
        }
    }
}
