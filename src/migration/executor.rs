//! Migration execution engine.
//!
//! Port of `surql/migration/executor.py`. Runs individual [`Migration`]
//! definitions against a live [`DatabaseClient`] inside a
//! [`Transaction`] (client-side buffered BEGIN/COMMIT) and records the
//! outcome in the [`MigrationHistory`] table.
//!
//! All items here require the `client` cargo feature.
//!
//! ## Deviations from Python
//!
//! * The Python implementation chooses between issuing raw `BEGIN` /
//!   `COMMIT` / `CANCEL` statements (remote) and running outside a
//!   transaction (embedded). The Rust port always uses
//!   [`Transaction`], which buffers statements client-side and flushes
//!   them as a single atomic `BEGIN…COMMIT` request.
//! * `get_migration_status` returns a structured
//!   [`MigrationStatusReport`] (total / applied / pending) instead of a
//!   flat list of [`MigrationStatus`].
//! * All arguments that the Python API accepts as a `list[Migration]`
//!   are replaced by a `migrations_dir: &Path`: the Rust runtime
//!   re-discovers migrations from disk at each call, matching the
//!   "migrations on disk" convention of the port.

use std::path::Path;
use std::time::Instant;

use chrono::Utc;

use crate::connection::{DatabaseClient, Transaction};
use crate::error::{Result, SurqlError};
use crate::migration::discovery::discover_migrations;
use crate::migration::history::{
    ensure_migration_table, get_applied_migrations as history_get_applied, is_migration_applied,
    record_migration, remove_migration_record,
};
use crate::migration::models::{
    Migration, MigrationDirection, MigrationHistory, MigrationPlan, MigrationState, MigrationStatus,
};

/// Aggregate status of a migrations directory relative to the database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationStatusReport {
    /// Total migrations discovered on disk.
    pub total: usize,
    /// Migrations that have been applied to the database.
    pub applied: Vec<MigrationStatus>,
    /// Migrations that have not yet been applied.
    pub pending: Vec<MigrationStatus>,
}

impl MigrationStatusReport {
    /// Total applied count (convenience).
    pub fn applied_count(&self) -> usize {
        self.applied.len()
    }

    /// Total pending count (convenience).
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

/// Options controlling a [`migrate_up`] run.
#[derive(Debug, Clone, Default)]
pub struct MigrateUpOptions {
    /// Apply at most this many pending migrations (`None` = apply all).
    pub steps: Option<usize>,
}

/// Execute a single migration in the requested direction.
///
/// Runs the migration's SurrealQL statements inside a
/// [`Transaction`]; on success records (or removes, when rolling back)
/// the migration from the history table.
///
/// Returns the resulting [`MigrationStatus`], including timing and, on
/// failure, the error message captured during execution.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationExecution`] when the transaction
/// itself cannot be begun or the history update fails. Per-statement
/// failures are reported via a [`MigrationStatus`] with
/// [`MigrationState::Failed`] and a populated `error`.
pub async fn execute_migration(
    client: &DatabaseClient,
    migration: &Migration,
    direction: MigrationDirection,
) -> Result<MigrationStatus> {
    let statements: &[String] = match direction {
        MigrationDirection::Up => &migration.up,
        MigrationDirection::Down => &migration.down,
    };

    let start = Instant::now();

    let mut tx = Transaction::begin(client)
        .await
        .map_err(|e| SurqlError::MigrationExecution {
            reason: format!("failed to begin transaction for {}: {e}", migration.version),
        })?;

    for (idx, statement) in statements.iter().enumerate() {
        if let Err(err) = tx.execute(statement).await {
            let _ = tx.rollback().await;
            return Ok(MigrationStatus {
                migration: migration.clone(),
                state: MigrationState::Failed,
                applied_at: None,
                error: Some(format!("statement {idx} failed: {err}")),
            });
        }
    }

    if let Err(err) = tx.commit().await {
        return Ok(MigrationStatus {
            migration: migration.clone(),
            state: MigrationState::Failed,
            applied_at: None,
            error: Some(format!("commit failed: {err}")),
        });
    }

    let applied_at = Utc::now();
    let execution_time_ms = u64::try_from(start.elapsed().as_millis()).ok();

    match direction {
        MigrationDirection::Up => {
            let entry = MigrationHistory {
                version: migration.version.clone(),
                description: migration.description.clone(),
                applied_at,
                checksum: migration.checksum.clone().unwrap_or_default(),
                execution_time_ms,
            };
            record_migration(client, &entry)
                .await
                .map_err(|e| SurqlError::MigrationExecution {
                    reason: format!("failed to record migration {}: {e}", migration.version),
                })?;
        }
        MigrationDirection::Down => {
            remove_migration_record(client, &migration.version)
                .await
                .map_err(|e| SurqlError::MigrationExecution {
                    reason: format!(
                        "failed to remove migration record {}: {e}",
                        migration.version
                    ),
                })?;
        }
    }

    let state = match direction {
        MigrationDirection::Up => MigrationState::Applied,
        MigrationDirection::Down => MigrationState::Pending,
    };

    Ok(MigrationStatus {
        migration: migration.clone(),
        state,
        applied_at: Some(applied_at),
        error: None,
    })
}

/// Apply all pending migrations found in `migrations_dir`.
///
/// Honours [`MigrateUpOptions::steps`] to cap the number of migrations
/// applied. Returns one [`MigrationStatus`] per migration that was
/// attempted.
///
/// Execution stops at the first failure; the failed migration's status
/// is included in the returned vector but subsequent migrations are
/// not attempted.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationExecution`] or
/// [`SurqlError::MigrationDiscovery`] if the directory cannot be
/// scanned or the history table cannot be ensured.
pub async fn migrate_up(
    client: &DatabaseClient,
    migrations_dir: &Path,
    opts: MigrateUpOptions,
) -> Result<Vec<MigrationStatus>> {
    ensure_migration_table(client).await?;
    let pending = get_pending_migrations(client, migrations_dir).await?;
    let to_apply: Vec<Migration> = match opts.steps {
        Some(n) => pending.into_iter().take(n).collect(),
        None => pending,
    };

    let mut out = Vec::with_capacity(to_apply.len());
    for migration in to_apply {
        let status = execute_migration(client, &migration, MigrationDirection::Up).await?;
        let failed = status.state == MigrationState::Failed;
        out.push(status);
        if failed {
            break;
        }
    }
    Ok(out)
}

/// Roll back the last `steps` applied migrations.
///
/// Walks the applied migrations in reverse chronological order and
/// runs each `down` body inside its own transaction. Stops at the
/// first failure (the failed status is included in the return value).
///
/// # Errors
///
/// Returns [`SurqlError::MigrationExecution`] on history or discovery
/// failure.
pub async fn migrate_down(
    client: &DatabaseClient,
    migrations_dir: &Path,
    steps: u32,
) -> Result<Vec<MigrationStatus>> {
    if steps == 0 {
        return Ok(Vec::new());
    }
    ensure_migration_table(client).await?;
    let mut applied = get_applied_migrations_ordered(client, migrations_dir).await?;
    applied.reverse();

    let take = usize::try_from(steps)
        .unwrap_or(usize::MAX)
        .min(applied.len());
    let to_roll = &applied[..take];

    // Join applied history metadata with on-disk migrations by version.
    let all_on_disk = discover_migrations(migrations_dir)?;
    let by_version: std::collections::BTreeMap<String, Migration> = all_on_disk
        .into_iter()
        .map(|m| (m.version.clone(), m))
        .collect();

    let mut out = Vec::with_capacity(to_roll.len());
    for history in to_roll {
        let Some(migration) = by_version.get(&history.version) else {
            out.push(MigrationStatus {
                migration: Migration {
                    version: history.version.clone(),
                    description: history.description.clone(),
                    path: std::path::PathBuf::new(),
                    up: Vec::new(),
                    down: Vec::new(),
                    checksum: Some(history.checksum.clone()),
                    depends_on: Vec::new(),
                },
                state: MigrationState::Failed,
                applied_at: None,
                error: Some(format!(
                    "cannot roll back {}: migration file missing on disk",
                    history.version
                )),
            });
            break;
        };
        let status = execute_migration(client, migration, MigrationDirection::Down).await?;
        let failed = status.state == MigrationState::Failed;
        out.push(status);
        if failed {
            break;
        }
    }
    Ok(out)
}

/// List migrations that have not yet been applied, sorted by version.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationExecution`] on discovery or history
/// failure.
pub async fn get_pending_migrations(
    client: &DatabaseClient,
    migrations_dir: &Path,
) -> Result<Vec<Migration>> {
    ensure_migration_table(client).await?;
    let on_disk = discover_migrations(migrations_dir)?;
    let applied = history_get_applied(client).await?;
    let applied_set: std::collections::BTreeSet<String> =
        applied.iter().map(|m| m.version.clone()).collect();

    let mut pending: Vec<Migration> = on_disk
        .into_iter()
        .filter(|m| !applied_set.contains(&m.version))
        .collect();
    pending.sort_by(|a, b| a.version.cmp(&b.version));
    Ok(pending)
}

/// Return every applied migration history row in `applied_at` order.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationExecution`] if the history query
/// fails.
pub async fn get_applied_migrations_ordered(
    client: &DatabaseClient,
    _migrations_dir: &Path,
) -> Result<Vec<MigrationHistory>> {
    history_get_applied(client)
        .await
        .map_err(|e| SurqlError::MigrationExecution {
            reason: format!("failed to read applied migrations: {e}"),
        })
}

/// Compute an applied / pending status report for a migrations directory.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationExecution`] if discovery or the
/// history query fail.
pub async fn get_migration_status(
    client: &DatabaseClient,
    migrations_dir: &Path,
) -> Result<MigrationStatusReport> {
    ensure_migration_table(client).await?;
    let on_disk = discover_migrations(migrations_dir)?;
    let applied_history = history_get_applied(client).await?;
    let applied_map: std::collections::BTreeMap<String, &MigrationHistory> = applied_history
        .iter()
        .map(|h| (h.version.clone(), h))
        .collect();

    let mut applied = Vec::new();
    let mut pending = Vec::new();
    for migration in on_disk.iter().cloned() {
        if let Some(history) = applied_map.get(&migration.version) {
            applied.push(MigrationStatus {
                migration,
                state: MigrationState::Applied,
                applied_at: Some(history.applied_at),
                error: None,
            });
        } else {
            pending.push(MigrationStatus {
                migration,
                state: MigrationState::Pending,
                applied_at: None,
                error: None,
            });
        }
    }
    applied.sort_by(|a, b| a.migration.version.cmp(&b.migration.version));
    pending.sort_by(|a, b| a.migration.version.cmp(&b.migration.version));

    Ok(MigrationStatusReport {
        total: on_disk.len(),
        applied,
        pending,
    })
}

/// Build the next migration plan (all pending migrations, forward).
///
/// # Errors
///
/// See [`get_pending_migrations`].
pub async fn create_migration_plan(
    client: &DatabaseClient,
    migrations_dir: &Path,
) -> Result<MigrationPlan> {
    let pending = get_pending_migrations(client, migrations_dir).await?;
    Ok(MigrationPlan {
        migrations: pending,
        direction: MigrationDirection::Up,
    })
}

/// Execute a [`MigrationPlan`] end-to-end.
///
/// For an `Up` plan, migrations are applied in ascending version order.
/// For a `Down` plan, they are applied in reverse order. Execution
/// stops at the first failure; the failed status is included in the
/// return value.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationExecution`] if the history table
/// cannot be ensured.
pub async fn execute_migration_plan(
    client: &DatabaseClient,
    plan: MigrationPlan,
) -> Result<Vec<MigrationStatus>> {
    ensure_migration_table(client).await?;
    let mut migrations = plan.migrations;
    migrations.sort_by(|a, b| a.version.cmp(&b.version));
    if plan.direction == MigrationDirection::Down {
        migrations.reverse();
    }
    let mut out = Vec::with_capacity(migrations.len());
    for migration in migrations {
        let status = execute_migration(client, &migration, plan.direction).await?;
        let failed = status.state == MigrationState::Failed;
        out.push(status);
        if failed {
            break;
        }
    }
    Ok(out)
}

/// Validate a migrations directory for duplicate versions and broken
/// dependencies.
///
/// Returns a list of human-readable error messages. An empty list
/// means the directory is self-consistent.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationDiscovery`] if the directory cannot
/// be read.
pub async fn validate_migrations(migrations_dir: &Path) -> Result<Vec<String>> {
    let migrations = discover_migrations(migrations_dir)?;
    let mut errors = Vec::new();

    let mut seen: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
    for m in &migrations {
        *seen.entry(m.version.clone()).or_insert(0) += 1;
    }
    for (version, count) in &seen {
        if *count > 1 {
            errors.push(format!("duplicate migration version: {version} (x{count})"));
        }
    }

    let versions: std::collections::BTreeSet<String> =
        migrations.iter().map(|m| m.version.clone()).collect();
    for m in &migrations {
        for dep in &m.depends_on {
            if !versions.contains(dep) {
                errors.push(format!(
                    "migration {} depends on missing migration {dep}",
                    m.version
                ));
            }
        }
    }

    Ok(errors)
}

/// Verify via the history table whether a migration is applied.
///
/// Convenience wrapper over [`is_migration_applied`] for use by the
/// rollback layer.
///
/// # Errors
///
/// See [`is_migration_applied`].
pub async fn version_is_applied(client: &DatabaseClient, version: &str) -> Result<bool> {
    is_migration_applied(client, version).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn write_migration(dir: &Path, filename: &str, body: &str) {
        std::fs::write(dir.join(filename), body).unwrap();
    }

    #[tokio::test]
    async fn validate_migrations_detects_duplicates() {
        let tmp = tempdir().unwrap();
        write_migration(
            tmp.path(),
            "20260101_000000_a.surql",
            "-- @metadata\n-- version: v1\n-- description: a\n-- @up\nDEFINE TABLE t1;\n-- @down\nREMOVE TABLE t1;\n",
        );
        write_migration(
            tmp.path(),
            "20260102_000000_b.surql",
            "-- @metadata\n-- version: v1\n-- description: b\n-- @up\nDEFINE TABLE t2;\n-- @down\nREMOVE TABLE t2;\n",
        );
        let errors = validate_migrations(tmp.path()).await.unwrap();
        assert!(errors.iter().any(|e| e.contains("duplicate")));
    }

    #[tokio::test]
    async fn validate_migrations_detects_missing_dep() {
        let tmp = tempdir().unwrap();
        write_migration(
            tmp.path(),
            "20260101_000000_a.surql",
            "-- @metadata\n-- version: v1\n-- description: a\n-- depends_on: vX\n-- @up\nDEFINE TABLE t;\n-- @down\nREMOVE TABLE t;\n",
        );
        let errors = validate_migrations(tmp.path()).await.unwrap();
        assert!(errors.iter().any(|e| e.contains("missing migration vX")));
    }

    #[tokio::test]
    async fn validate_migrations_empty_dir_returns_empty_errors() {
        let tmp = tempdir().unwrap();
        let errors = validate_migrations(tmp.path()).await.unwrap();
        assert!(errors.is_empty());
    }

    #[test]
    fn migration_status_report_counts() {
        let report = MigrationStatusReport {
            total: 3,
            applied: vec![MigrationStatus {
                migration: Migration {
                    version: "v1".into(),
                    description: String::new(),
                    path: PathBuf::new(),
                    up: vec![],
                    down: vec![],
                    checksum: None,
                    depends_on: vec![],
                },
                state: MigrationState::Applied,
                applied_at: None,
                error: None,
            }],
            pending: Vec::new(),
        };
        assert_eq!(report.applied_count(), 1);
        assert_eq!(report.pending_count(), 0);
    }
}
