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
//!   The Rust port pins the record id to the migration version to keep
//!   removal by version a single-statement `DELETE` (no extra `SELECT`).

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::{json, Value};

use crate::connection::DatabaseClient;
use crate::error::{Result, SurqlError};
use crate::migration::models::MigrationHistory;
use crate::migration::versioning::{create_snapshot, store_snapshot};
use crate::schema::registry::SchemaRegistry;

/// Name of the SurrealDB table used for migration history.
pub const MIGRATION_TABLE_NAME: &str = "_migration_history";

/// Auto-snapshot flag (mirrors Python `AUTO_SNAPSHOT_ENABLED`).
static AUTO_SNAPSHOT_ENABLED: AtomicBool = AtomicBool::new(false);

/// Enable automatic schema snapshots after successful migrations.
pub fn enable_auto_snapshots() {
    AUTO_SNAPSHOT_ENABLED.store(true, Ordering::Relaxed);
}

/// Disable automatic schema snapshots.
pub fn disable_auto_snapshots() {
    AUTO_SNAPSHOT_ENABLED.store(false, Ordering::Relaxed);
}

/// `true` when automatic snapshots are enabled.
pub fn is_auto_snapshot_enabled() -> bool {
    AUTO_SNAPSHOT_ENABLED.load(Ordering::Relaxed)
}

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
/// The SurrealDB record id is pinned to the migration version so
/// [`remove_migration_record`] can issue a single `DELETE` by id.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationHistory`] if the `CREATE` fails or if the
/// history table cannot be ensured.
pub async fn record_migration(client: &DatabaseClient, entry: &MigrationHistory) -> Result<()> {
    ensure_migration_table(client).await?;

    // SurrealDB v3 rejects bare ISO-8601 strings for datetime-typed
    // fields with "Expected `datetime` but found '...'", so we emit
    // CREATE ... SET applied_at = <datetime> $applied_at and keep the
    // cast visible in the SurrealQL rather than relying on CONTENT
    // auto-coercion.
    let mut vars: std::collections::BTreeMap<String, Value> = std::collections::BTreeMap::new();
    vars.insert("id".into(), Value::String(record_id_for(&entry.version)));
    vars.insert("version".into(), Value::String(entry.version.clone()));
    vars.insert(
        "description".into(),
        Value::String(entry.description.clone()),
    );
    vars.insert(
        "applied_at".into(),
        Value::String(entry.applied_at.to_rfc3339()),
    );
    vars.insert("checksum".into(), Value::String(entry.checksum.clone()));

    let mut set = String::from(
        "version = $version, description = $description, \
         applied_at = <datetime> $applied_at, checksum = $checksum",
    );
    if let Some(ms) = entry.execution_time_ms {
        vars.insert("execution_time_ms".into(), json!(ms));
        set.push_str(", execution_time_ms = $execution_time_ms");
    }

    let surql = format!(
        "CREATE type::record('{table}', $id) SET {set};",
        table = MIGRATION_TABLE_NAME,
    );

    client
        .query_with_vars(&surql, vars)
        .await
        .map_err(|e| SurqlError::MigrationHistory {
            reason: format!("failed to record migration {}: {e}", entry.version),
        })?;
    Ok(())
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
    let surql = format!(
        "DELETE FROM {table} WHERE version = $version;",
        table = MIGRATION_TABLE_NAME,
    );
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
    let surql = format!(
        "SELECT * FROM {table} ORDER BY applied_at ASC;",
        table = MIGRATION_TABLE_NAME,
    );
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
    let surql = format!(
        "SELECT * FROM {table} WHERE version = $version LIMIT 1;",
        table = MIGRATION_TABLE_NAME,
    );
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

fn record_id_for(version: &str) -> String {
    // SurrealDB record ids allow `⟨…⟩` delimiters; easier is to replace
    // anything non-alphanumeric with `_` so the id is valid without quoting.
    version
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

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
    fn record_id_sanitises_separators() {
        assert_eq!(record_id_for("20260102_120000"), "20260102_120000");
        assert_eq!(record_id_for("20260102-120000"), "20260102_120000");
        assert_eq!(record_id_for("v1.2.3"), "v1_2_3");
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

    #[test]
    fn auto_snapshot_flag_roundtrip() {
        disable_auto_snapshots();
        assert!(!is_auto_snapshot_enabled());
        enable_auto_snapshots();
        assert!(is_auto_snapshot_enabled());
        disable_auto_snapshots();
        assert!(!is_auto_snapshot_enabled());
    }

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
