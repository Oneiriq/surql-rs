//! Migration history tracking stored in SurrealDB.
//!
//! Port of `surql/migration/history.py`. Persists [`MigrationHistory`] rows
//! in a `_migration_history` table so the runtime can tell which migrations
//! have been applied.
//!
//! All functions require a live [`DatabaseClient`] and are therefore only
//! compiled with the `client` cargo feature.
//!
//! ## Deviation from Python
//!
//! * The Python module toggles auto-snapshot behaviour via a global
//!   `AUTO_SNAPSHOT_ENABLED` boolean. The Rust port uses an
//!   [`std::sync::atomic::AtomicBool`] guarded accessor, but the
//!   [`auto_snapshot_after_apply`] helper is explicit: callers pass the
//!   snapshots directory and the registry to snapshot.
//! * The Python version relied on `client.create`'s implicit ID generation.
//!   The Rust port pins the record id to the migration version, so one
//!   version can be recorded only once: the executor records inside the
//!   migration's own transaction, and a second runner applying the same
//!   migration concurrently has its whole transaction rejected.

use std::fmt::Write as _;
use std::path::Path;

use serde_json::Value;

use crate::connection::DatabaseClient;
use crate::error::{Result, SurqlError};
use crate::migration::hooks::is_auto_snapshot_enabled;
use crate::migration::models::MigrationHistory;
use crate::migration::versioning::{create_snapshot, store_snapshot};
use crate::schema::registry::SchemaRegistry;
use crate::types::escape::{quote_record_key, quote_str};

/// Name of the SurrealDB table used for migration history.
pub const MIGRATION_TABLE_NAME: &str = "_migration_history";

/// Create the migration history table.
///
/// Idempotent: uses `DEFINE TABLE … IF NOT EXISTS` variants under the hood
/// so repeated invocations are safe.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationHistory`] if any `DEFINE` statement fails.
pub async fn create_migration_table(client: &DatabaseClient) -> Result<()> {
    let statements: [&str; 7] = [
        "DEFINE TABLE IF NOT EXISTS _migration_history SCHEMAFULL;",
        "DEFINE FIELD IF NOT EXISTS version ON TABLE _migration_history TYPE string;",
        "DEFINE FIELD IF NOT EXISTS description ON TABLE _migration_history TYPE string;",
        "DEFINE FIELD IF NOT EXISTS applied_at ON TABLE _migration_history TYPE datetime;",
        "DEFINE FIELD IF NOT EXISTS checksum ON TABLE _migration_history TYPE string;",
        "DEFINE FIELD IF NOT EXISTS execution_time_ms ON TABLE _migration_history TYPE option<int>;",
        "DEFINE INDEX IF NOT EXISTS version_idx ON TABLE _migration_history COLUMNS version UNIQUE;",
    ];

    let mut surql = String::new();
    for stmt in statements {
        surql.push_str(stmt);
        surql.push('\n');
    }

    client
        .query(&surql)
        .await
        .map_err(|e| SurqlError::MigrationHistory {
            reason: format!("failed to create migration history table: {e}"),
        })?;
    Ok(())
}

/// Ensure the migration history table exists.
///
/// Currently a thin wrapper around [`create_migration_table`] since the
/// underlying `DEFINE … IF NOT EXISTS` is idempotent.
///
/// # Errors
///
/// See [`create_migration_table`].
pub async fn ensure_migration_table(client: &DatabaseClient) -> Result<()> {
    create_migration_table(client).await
}

/// Record a migration as applied in the history table.
///
/// The SurrealDB record id is pinned to the migration version, so a
/// version can be recorded only once. The migration executor records
/// inside the migration's own transaction instead of calling this.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationHistory`] if the `CREATE` fails (for
/// instance because the version is already recorded) or if the history
/// table cannot be ensured.
pub async fn record_migration(client: &DatabaseClient, entry: &MigrationHistory) -> Result<()> {
    ensure_migration_table(client).await?;
    let execution_time = entry.execution_time_ms.map(|ms| ms.to_string());
    let surql = record_statement(entry, execution_time.as_deref());
    client
        .query(&surql)
        .await
        .map_err(|e| SurqlError::MigrationHistory {
            reason: format!("failed to record migration {}: {e}", entry.version),
        })?;
    Ok(())
}

/// The `CREATE` that records `entry` as applied.
///
/// The record id is derived from the version, so a second attempt to
/// record the same version fails (and, inside a transaction, takes the
/// whole transaction down with it). `execution_time_ms` is a SurrealQL
/// expression for the field, or `None` to leave it unset. Every value is
/// rendered as a quoted literal.
pub(crate) fn record_statement(
    entry: &MigrationHistory,
    execution_time_ms: Option<&str>,
) -> String {
    // SurrealDB v3 rejects a bare ISO-8601 string for a datetime-typed
    // field, so the cast stays visible in the SurrealQL.
    let mut set = format!(
        "version = {version}, description = {description}, \
         applied_at = <datetime> {applied_at}, checksum = {checksum}",
        version = quote_str(&entry.version),
        description = quote_str(&entry.description),
        applied_at = quote_str(&entry.applied_at.to_rfc3339()),
        checksum = quote_str(&entry.checksum),
    );
    if let Some(ms) = execution_time_ms {
        let _ = write!(set, ", execution_time_ms = {ms}");
    }
    format!("CREATE {} SET {set};", history_record(&entry.version))
}

/// The statement that deletes `version`'s history row, and fails (taking a
/// surrounding transaction down with it) when there is none: rolling back
/// a migration that is not recorded as applied would run its down body
/// against a schema it was never applied to.
pub(crate) fn removal_statement(version: &str) -> String {
    let message = format!("migration {version} is not recorded as applied");
    format!(
        "IF array::len((DELETE {table} WHERE version = {version} RETURN BEFORE)) == 0 \
         {{ THROW {message} }};",
        table = MIGRATION_TABLE_NAME,
        version = quote_str(version),
        message = quote_str(&message),
    )
}

/// The record id of `version`'s history row. Distinct versions get
/// distinct ids (`v1.2` and `v1_2` do not collide).
fn history_record(version: &str) -> String {
    format!("{MIGRATION_TABLE_NAME}:{}", quote_record_key(version))
}

/// Remove a migration record from history (used during rollback).
///
/// Silently succeeds if the record does not exist.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationHistory`] if the `DELETE` fails.
pub async fn remove_migration_record(client: &DatabaseClient, version: &str) -> Result<()> {
    ensure_migration_table(client).await?;
    let surql = format!("DELETE FROM {MIGRATION_TABLE_NAME} WHERE version = $version;");
    let mut vars: std::collections::BTreeMap<String, Value> = std::collections::BTreeMap::new();
    vars.insert("version".into(), Value::String(version.to_string()));
    client
        .query_with_vars(&surql, vars)
        .await
        .map_err(|e| SurqlError::MigrationHistory {
            reason: format!("failed to remove migration record {version}: {e}"),
        })?;
    Ok(())
}

/// Fetch every applied migration, ordered by `applied_at` ascending.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationHistory`] if the query fails or rows
/// cannot be decoded.
pub async fn get_applied_migrations(client: &DatabaseClient) -> Result<Vec<MigrationHistory>> {
    ensure_migration_table(client).await?;
    let surql = format!("SELECT * FROM {MIGRATION_TABLE_NAME} ORDER BY applied_at ASC;");
    let raw = client
        .query(&surql)
        .await
        .map_err(|e| SurqlError::MigrationHistory {
            reason: format!("failed to fetch applied migrations: {e}"),
        })?;

    Ok(parse_history_rows(&raw))
}

/// `true` if the given version is recorded as applied.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationHistory`] on query failure.
pub async fn is_migration_applied(client: &DatabaseClient, version: &str) -> Result<bool> {
    ensure_migration_table(client).await?;
    // SELECT * here (rather than just `version`) so the row round-trips
    // through `parse_history_rows` -- that helper requires `applied_at`
    // to decode successfully, and would skip rows whose payload is
    // missing it.
    let surql = format!("SELECT * FROM {MIGRATION_TABLE_NAME} WHERE version = $version LIMIT 1;");
    let mut vars: std::collections::BTreeMap<String, Value> = std::collections::BTreeMap::new();
    vars.insert("version".into(), Value::String(version.to_string()));
    let raw =
        client
            .query_with_vars(&surql, vars)
            .await
            .map_err(|e| SurqlError::MigrationHistory {
                reason: format!("failed to query migration {version}: {e}"),
            })?;
    Ok(!parse_history_rows(&raw).is_empty())
}

/// Fetch every applied migration ordered by `applied_at`.
///
/// Alias for [`get_applied_migrations`] to mirror the Python public API.
///
/// # Errors
///
/// See [`get_applied_migrations`].
pub async fn get_migration_history(client: &DatabaseClient) -> Result<Vec<MigrationHistory>> {
    get_applied_migrations(client).await
}

/// Take a post-migration snapshot when [`is_auto_snapshot_enabled`] is on.
///
/// This is a best-effort helper: any failure is swallowed (logged via
/// `tracing::warn`) because a snapshot failure should never fail a
/// successful migration.
///
/// When auto-snapshots are disabled this function returns immediately.
pub fn auto_snapshot_after_apply(registry: &SchemaRegistry, snapshots_dir: &Path, version: &str) {
    if !is_auto_snapshot_enabled() {
        return;
    }
    match create_snapshot(registry, version, format!("auto: {version}")) {
        Ok(snapshot) => {
            if let Err(err) = store_snapshot(&snapshot, snapshots_dir) {
                tracing::warn!(target: "surql::migration::history", %err, %version, "auto_snapshot_store_failed");
            }
        }
        Err(err) => {
            tracing::warn!(target: "surql::migration::history", %err, %version, "auto_snapshot_create_failed");
        }
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn parse_history_rows(raw: &Value) -> Vec<MigrationHistory> {
    let mut out = Vec::new();
    collect_rows(raw, &mut out);
    out
}

fn collect_rows(value: &Value, out: &mut Vec<MigrationHistory>) {
    match value {
        Value::Array(items) => {
            for item in items {
                collect_rows(item, out);
            }
        }
        Value::Object(obj) => {
            if let Some(inner) = obj.get("result") {
                collect_rows(inner, out);
                return;
            }
            if let Some(entry) = history_from_object(obj) {
                out.push(entry);
            }
        }
        _ => {}
    }
}

fn history_from_object(obj: &serde_json::Map<String, Value>) -> Option<MigrationHistory> {
    let version = obj.get("version").and_then(Value::as_str)?.to_string();
    let description = obj
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let checksum = obj
        .get("checksum")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let applied_at = obj.get("applied_at").and_then(parse_datetime)?;
    let execution_time_ms = obj.get("execution_time_ms").and_then(|v| match v {
        Value::Number(n) => n.as_u64(),
        _ => None,
    });
    Some(MigrationHistory {
        version,
        description,
        applied_at,
        checksum,
        execution_time_ms,
    })
}

fn parse_datetime(value: &Value) -> Option<chrono::DateTime<chrono::Utc>> {
    let s = value.as_str()?;
    // Try RFC3339 first; fall back to treating it as an ISO-8601 lax string.
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&chrono::Utc));
    }
    if let Ok(dt) = chrono::DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.fZ") {
        return Some(dt.with_timezone(&chrono::Utc));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use serde_json::json;

    #[test]
    fn history_record_ids_keep_versions_distinct() {
        assert_eq!(
            history_record("20260102_120000"),
            "_migration_history:⟨20260102_120000⟩"
        );
        assert_eq!(history_record("v1.2"), "_migration_history:⟨v1.2⟩");
        assert_eq!(history_record("v1_2"), "_migration_history:v1_2");
        assert_ne!(history_record("v1.2"), history_record("v1_2"));
    }

    #[test]
    fn record_statement_quotes_every_value() {
        let entry = MigrationHistory {
            version: "v1".into(),
            description: "it's'; DELETE _migration_history; --".into(),
            applied_at: Utc.with_ymd_and_hms(2026, 1, 2, 12, 0, 0).unwrap(),
            checksum: "abc".into(),
            execution_time_ms: None,
        };
        let surql = record_statement(&entry, Some("42"));
        assert_eq!(
            surql,
            "CREATE _migration_history:v1 SET version = 'v1', \
             description = 'it\\'s\\'; DELETE _migration_history; --', \
             applied_at = <datetime> '2026-01-02T12:00:00+00:00', checksum = 'abc', \
             execution_time_ms = 42;"
        );
    }

    #[test]
    fn removal_statement_throws_when_nothing_was_recorded() {
        assert_eq!(
            removal_statement("v'1"),
            "IF array::len((DELETE _migration_history WHERE version = 'v\\'1' RETURN BEFORE)) \
             == 0 { THROW 'migration v\\'1 is not recorded as applied' };"
        );
    }

    #[test]
    fn parse_history_rows_extracts_nested_result() {
        let raw = json!([{
            "result": [{
                "version": "v1",
                "description": "initial",
                "applied_at": "2026-01-02T12:00:00Z",
                "checksum": "abc",
                "execution_time_ms": 42,
            }],
        }]);
        let rows = parse_history_rows(&raw);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].version, "v1");
        assert_eq!(rows[0].execution_time_ms, Some(42));
    }

    #[test]
    fn parse_history_rows_accepts_flat_array() {
        let raw = json!([{
            "version": "v1",
            "description": "d",
            "applied_at": "2026-01-02T12:00:00Z",
            "checksum": "abc",
        }]);
        let rows = parse_history_rows(&raw);
        assert_eq!(rows.len(), 1);
        assert!(rows[0].execution_time_ms.is_none());
    }

    #[test]
    fn parse_history_rows_skips_rows_without_timestamp() {
        let raw = json!([{ "result": [{ "version": "v1", "description": "d", "checksum": "c" }] }]);
        let rows = parse_history_rows(&raw);
        assert!(rows.is_empty());
    }

    // `auto_snapshot_flag_roundtrip` moved to `migration::hooks::tests`
    // alongside the AUTO_SNAPSHOT_TEST_LOCK mutex that serialises access
    // to the process-global toggle. Keeping the assertion here would
    // race with those tests when `cargo test --lib` runs threads in
    // parallel.

    #[test]
    fn parse_datetime_handles_rfc3339() {
        let v = json!("2026-01-02T12:00:00Z");
        let dt = parse_datetime(&v).unwrap();
        let expected = Utc.with_ymd_and_hms(2026, 1, 2, 12, 0, 0).unwrap();
        assert_eq!(dt, expected);
    }

    #[test]
    fn migration_table_name_constant_matches_python() {
        assert_eq!(MIGRATION_TABLE_NAME, "_migration_history");
    }
}
