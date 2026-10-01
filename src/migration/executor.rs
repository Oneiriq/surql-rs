//! Migration execution engine.
//!
//! Port of `surql/migration/executor.py`. Runs individual [`Migration`]
//! definitions against a live [`DatabaseClient`] inside a
//! [`Transaction`] (client-side buffered BEGIN/COMMIT) and records the
//! outcome in the [`MigrationHistory`] table within that same
//! transaction.
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

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use chrono::{DateTime, Utc};

use crate::connection::{DatabaseClient, Transaction};
use crate::error::{Result, SurqlError};
use crate::migration::discovery::{discover_migrations, modified_migrations, order_migrations};
use crate::migration::history::{
    ensure_migration_table, get_applied_migrations as history_get_applied, is_migration_applied,
    record_statement, removal_statement, update_migration_checksum,
};
use crate::migration::models::{
    Migration, MigrationDirection, MigrationHistory, MigrationPlan, MigrationState,
    MigrationStatus, ModifiedMigration,
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
    /// Applied migrations whose file changed after it was applied (see
    /// [`modified_migrations`]). Each is also listed in `applied`.
    pub modified: Vec<ModifiedMigration>,
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

/// Variable holding the transaction's start time, for the history row's
/// `execution_time_ms`.
const STARTED_VAR: &str = "$__surql_migration_started";

/// Execute a single migration in the requested direction.
///
/// Runs the migration's SurrealQL statements and the matching history
/// change (recording the version when applying, deleting its row when
/// rolling back) in one [`Transaction`], so the schema change and the
/// history row commit or fail together. The history row's id is derived
/// from the version: when two runners apply the same migration at once,
/// the second one's transaction is rejected as a whole, data statements
/// included. Rolling back a migration that is not recorded as applied
/// fails the same way.
///
/// A migration with no `down` statements (a squashed or a blank one) is
/// refused in the `Down` direction instead of silently deleting its
/// history row while the schema stays.
///
/// Returns the resulting [`MigrationStatus`], including timing and, on
/// failure, the error message captured during execution.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationExecution`] when the history table
/// cannot be ensured or the transaction cannot be begun. Failures of the
/// transaction itself (a bad statement, an already-recorded version) are
/// reported via a [`MigrationStatus`] with [`MigrationState::Failed`] and
/// a populated `error`; nothing was applied in that case.
pub async fn execute_migration(
    client: &DatabaseClient,
    migration: &Migration,
    direction: MigrationDirection,
) -> Result<MigrationStatus> {
    let failed = |error: String| MigrationStatus {
        migration: migration.clone(),
        state: MigrationState::Failed,
        applied_at: None,
        error: Some(error),
    };
    let statements: &[String] = match direction {
        MigrationDirection::Up => &migration.up,
        MigrationDirection::Down => &migration.down,
    };
    if direction == MigrationDirection::Down && statements.is_empty() {
        return Ok(failed(format!(
            "migration {} has no down statements; refusing to roll it back \
             (restore from a snapshot or backup instead)",
            migration.version
        )));
    }

    ensure_migration_table(client)
        .await
        .map_err(|e| SurqlError::MigrationExecution {
            reason: format!("failed to ensure the migration history table: {e}"),
        })?;

    let applied_at = Utc::now();
    let history = match direction {
        MigrationDirection::Up => {
            let entry = MigrationHistory {
                version: migration.version.clone(),
                description: migration.description.clone(),
                applied_at,
                checksum: migration.checksum.clone().unwrap_or_default(),
                execution_time_ms: None,
            };
            let elapsed = format!("duration::millis(time::now() - {STARTED_VAR})");
            record_statement(&entry, Some(&elapsed))
        }
        MigrationDirection::Down => removal_statement(&migration.version),
    };

    let mut tx = Transaction::begin(client)
        .await
        .map_err(|e| SurqlError::MigrationExecution {
            reason: format!("failed to begin transaction for {}: {e}", migration.version),
        })?;
    let started = format!("LET {STARTED_VAR} = time::now();");
    let queued = std::iter::once(started.as_str())
        .chain(statements.iter().map(String::as_str))
        .chain(std::iter::once(history.as_str()));
    for statement in queued {
        // Only fails when the transaction is no longer active, which a
        // freshly begun one always is.
        if let Err(err) = tx.execute(statement).await {
            return Ok(failed(format!("failed to queue statement: {err}")));
        }
    }

    if let Err(err) = tx.commit().await {
        return Ok(failed(format!("commit failed: {err}")));
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
/// Nothing is applied while an applied migration's file differs from the
/// checksum recorded for it (see [`get_modified_migrations`]): the schema
/// no longer matches the files, so migrations written against them may
/// not apply as intended. Revert the edit, or accept it with
/// [`rehash_migrations`].
///
/// # Errors
///
/// Returns [`SurqlError::MigrationExecution`] if an applied migration was
/// modified, or [`SurqlError::MigrationExecution`] or
/// [`SurqlError::MigrationDiscovery`] if the directory cannot be
/// scanned or the history table cannot be ensured.
pub async fn migrate_up(
    client: &DatabaseClient,
    migrations_dir: &Path,
    opts: MigrateUpOptions,
) -> Result<Vec<MigrationStatus>> {
    ensure_migration_table(client).await?;
    let on_disk = discover_migrations(migrations_dir)?;
    let history = history_get_applied(client).await?;
    refuse_modified(&modified_migrations(&on_disk, &history))?;
    let pending = pending_among(on_disk, &history);
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

    let take = usize::try_from(steps).unwrap_or(usize::MAX);
    let to_roll: Vec<&MigrationHistory> = applied.iter().take(take).collect();

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
                    squashed_from: Vec::new(),
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

/// List migrations that have not yet been applied, in the order they
/// would be applied (see [`discover_migrations`]).
///
/// A squashed migration is not pending where all of the migrations it
/// was squashed from are applied, or are pending ahead of it (applying
/// them covers it); and a migration a recorded squashed migration was
/// squashed from is not pending either.
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
    let history = history_get_applied(client).await?;
    Ok(pending_among(on_disk, &history))
}

/// The migrations of `on_disk` (in apply order) that `history` leaves
/// pending; see [`get_pending_migrations`].
fn pending_among(on_disk: Vec<Migration>, history: &[MigrationHistory]) -> Vec<Migration> {
    let mut applied: BTreeSet<String> = effective_applied(&on_disk, history).into_keys().collect();
    on_disk
        .into_iter()
        .filter(|m| {
            if applied.contains(&m.version) || covered(m, |v| applied.contains(v)) {
                return false;
            }
            // Once this is applied, a later squash of it is covered.
            applied.insert(m.version.clone());
            true
        })
        .collect()
}

/// When each on-disk migration counts as applied: the recorded versions,
/// the sources of every recorded squashed migration, and every squashed
/// migration whose sources are all applied (at the latest source's time).
fn effective_applied(
    on_disk: &[Migration],
    history: &[MigrationHistory],
) -> BTreeMap<String, DateTime<Utc>> {
    let mut applied: BTreeMap<String, DateTime<Utc>> = history
        .iter()
        .map(|h| (h.version.clone(), h.applied_at))
        .collect();
    // Repeat until nothing changes, for squashes of squashes.
    loop {
        let before = applied.len();
        for m in on_disk {
            if let Some(at) = applied.get(&m.version).copied() {
                for source in &m.squashed_from {
                    applied.entry(source.clone()).or_insert(at);
                }
            } else if covered(m, |v| applied.contains_key(v)) {
                let latest = m
                    .squashed_from
                    .iter()
                    .filter_map(|v| applied.get(v).copied())
                    .max();
                if let Some(at) = latest {
                    applied.insert(m.version.clone(), at);
                }
            }
        }
        if applied.len() == before {
            return applied;
        }
    }
}

/// `true` for a squashed migration whose sources are all applied.
fn covered(migration: &Migration, is_applied: impl Fn(&str) -> bool) -> bool {
    !migration.squashed_from.is_empty() && migration.squashed_from.iter().all(|v| is_applied(v))
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
/// Both lists follow the order [`discover_migrations`] returns. A squashed
/// migration whose sources are all applied, and a migration a recorded
/// squashed migration replaced, are reported as applied. Applied
/// migrations whose file changed since are also listed in
/// [`MigrationStatusReport::modified`].
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
    let history = history_get_applied(client).await?;
    let modified = modified_migrations(&on_disk, &history);
    let applied_map = effective_applied(&on_disk, &history);

    let mut applied = Vec::new();
    let mut pending = Vec::new();
    for migration in on_disk.iter().cloned() {
        if let Some(at) = applied_map.get(&migration.version) {
            applied.push(MigrationStatus {
                migration,
                state: MigrationState::Applied,
                applied_at: Some(*at),
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

    Ok(MigrationStatusReport {
        total: on_disk.len(),
        applied,
        pending,
        modified,
    })
}

/// List the applied migrations in `migrations_dir` whose file no longer
/// matches the checksum recorded when it was applied (see
/// [`modified_migrations`]).
///
/// # Errors
///
/// Returns [`SurqlError::MigrationExecution`] on discovery or history
/// failure.
pub async fn get_modified_migrations(
    client: &DatabaseClient,
    migrations_dir: &Path,
) -> Result<Vec<ModifiedMigration>> {
    ensure_migration_table(client).await?;
    let on_disk = discover_migrations(migrations_dir)?;
    let history = history_get_applied(client).await?;
    Ok(modified_migrations(&on_disk, &history))
}

/// Accept edits to applied migrations by recording their files' current
/// checksums, for edits that need no applying (comments, formatting).
///
/// Re-hashes every modified migration when `versions` is empty, and only
/// the listed ones otherwise. Returns the migrations that were re-hashed.
/// Nothing is executed against the schema.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationExecution`] when a listed version is not
/// a modified applied migration, or on discovery or history failure.
pub async fn rehash_migrations(
    client: &DatabaseClient,
    migrations_dir: &Path,
    versions: &[String],
) -> Result<Vec<ModifiedMigration>> {
    let modified = get_modified_migrations(client, migrations_dir).await?;
    if let Some(unknown) = versions
        .iter()
        .find(|v| !modified.iter().any(|m| &m.version == *v))
    {
        return Err(SurqlError::MigrationExecution {
            reason: format!("migration {unknown} is not an applied migration whose file changed"),
        });
    }
    let selected: Vec<ModifiedMigration> = modified
        .into_iter()
        .filter(|m| versions.is_empty() || versions.contains(&m.version))
        .collect();
    for migration in &selected {
        update_migration_checksum(client, &migration.version, &migration.current_checksum).await?;
    }
    Ok(selected)
}

/// An error naming every modified migration, or `Ok` when there are none.
pub(crate) fn refuse_modified(modified: &[ModifiedMigration]) -> Result<()> {
    if modified.is_empty() {
        return Ok(());
    }
    let versions: Vec<&str> = modified.iter().map(|m| m.version.as_str()).collect();
    Err(SurqlError::MigrationExecution {
        reason: format!(
            "applied migration(s) changed since they were applied: {}; revert the edit, \
             or accept it with `surql migrate rehash` if it needs no applying",
            versions.join(", ")
        ),
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
/// For an `Up` plan, migrations are applied in the order
/// [`discover_migrations`] uses (versions compared numerically, each after
/// its dependencies). For a `Down` plan, they are applied in reverse
/// order. Execution stops at the first failure; the failed status is
/// included in the return value.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationExecution`] if the history table
/// cannot be ensured, or [`SurqlError::MigrationDiscovery`] if the plan's
/// dependencies form a cycle.
pub async fn execute_migration_plan(
    client: &DatabaseClient,
    plan: MigrationPlan,
) -> Result<Vec<MigrationStatus>> {
    ensure_migration_table(client).await?;
    let mut migrations = order_migrations(plan.migrations)?;
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

    fn mig(version: &str, squashed_from: &[&str]) -> Migration {
        Migration {
            version: version.into(),
            description: String::new(),
            path: PathBuf::new(),
            up: vec![],
            down: vec![],
            checksum: None,
            depends_on: vec![],
            squashed_from: squashed_from.iter().map(|v| (*v).to_string()).collect(),
        }
    }

    fn row(version: &str) -> MigrationHistory {
        MigrationHistory {
            version: version.into(),
            description: String::new(),
            applied_at: Utc::now(),
            checksum: String::new(),
            execution_time_ms: None,
        }
    }

    fn pending_versions(on_disk: Vec<Migration>, history: &[MigrationHistory]) -> Vec<String> {
        pending_among(on_disk, history)
            .into_iter()
            .map(|m| m.version)
            .collect()
    }

    /// A squashed migration has a version of its own; on a database that
    /// applied its sources it used to be pending forever, and `migrate_up`
    /// re-applied every statement in it.
    #[test]
    fn a_squash_of_applied_migrations_is_not_pending() {
        let disk = || vec![mig("v1", &[]), mig("v2", &[]), mig("v3", &["v1", "v2"])];
        assert!(pending_versions(disk(), &[row("v1"), row("v2")]).is_empty());
        // Sources that will be applied first cover it as well.
        assert_eq!(pending_versions(disk(), &[]), vec!["v1", "v2"]);
        assert_eq!(pending_versions(disk(), &[row("v1")]), vec!["v2"]);
    }

    #[test]
    fn the_sources_of_an_applied_squash_are_not_pending() {
        let disk = vec![mig("v1", &[]), mig("v2", &[]), mig("v3", &["v1", "v2"])];
        assert!(pending_versions(disk, &[row("v3")]).is_empty());
    }

    #[test]
    fn a_squash_whose_sources_are_gone_is_applied_on_a_fresh_database() {
        let disk = vec![mig("v3", &["v1", "v2"]), mig("v4", &[])];
        assert_eq!(pending_versions(disk, &[]), vec!["v3", "v4"]);
    }

    #[test]
    fn squashes_of_squashes_resolve_transitively() {
        let disk = || {
            vec![
                mig("v1", &[]),
                mig("v2", &[]),
                mig("v3", &["v1", "v2"]),
                mig("v4", &[]),
                mig("v5", &["v3", "v4"]),
            ]
        };
        assert!(pending_versions(disk(), &[row("v1"), row("v2"), row("v4")]).is_empty());
        assert!(pending_versions(disk(), &[row("v5")]).is_empty());
        let applied = effective_applied(&disk(), &[row("v5")]);
        assert!(applied.contains_key("v1"), "{applied:?}");
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
                    squashed_from: vec![],
                },
                state: MigrationState::Applied,
                applied_at: None,
                error: None,
            }],
            pending: Vec::new(),
            modified: Vec::new(),
        };
        assert_eq!(report.applied_count(), 1);
        assert_eq!(report.pending_count(), 0);
    }
}
