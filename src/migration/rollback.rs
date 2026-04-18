//! Rollback safety analysis and execution.
//!
//! Port of `surql/migration/rollback.py`. Provides a three-tier
//! [`RollbackSafety`] classification, [`RollbackIssue`] descriptors, a
//! [`RollbackPlan`] builder, and an async
//! [`execute_rollback`] function that drives the plan through
//! [`crate::migration::executor::execute_migration`].
//!
//! The safety analysis is a pure text scan of the `down` body of each
//! migration; it does not connect to the database. Only
//! [`create_rollback_plan`], [`execute_rollback`], and the convenience
//! [`plan_rollback_to_version`] need a live client.
//!
//! ## Deviation from Python
//!
//! The Python enum variants are `safe`, `data_loss`, `unsafe`. The Rust
//! port renames them to [`RollbackSafety::Safe`],
//! [`RollbackSafety::Warning`] (data-loss) and
//! [`RollbackSafety::Danger`] (unsafe) as the task brief requires.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::connection::DatabaseClient;
use crate::error::{Result, SurqlError};
use crate::migration::discovery::discover_migrations;
use crate::migration::executor::execute_migration;
use crate::migration::history::get_applied_migrations;
use crate::migration::models::{Migration, MigrationDirection, MigrationStatus};

/// Safety tier of a rollback operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RollbackSafety {
    /// No data loss is expected.
    Safe,
    /// Some data may be lost (field drops, type changes).
    Warning,
    /// Significant data loss is likely (table drops, destructive resets).
    Danger,
}

impl RollbackSafety {
    /// Lowercase string form (matches Python `.value`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Safe => "safe",
            Self::Warning => "warning",
            Self::Danger => "danger",
        }
    }

    fn rank(self) -> u8 {
        match self {
            Self::Safe => 0,
            Self::Warning => 1,
            Self::Danger => 2,
        }
    }
}

impl std::fmt::Display for RollbackSafety {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One issue identified while analysing a rollback plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RollbackIssue {
    /// Severity of this particular issue.
    pub safety: RollbackSafety,
    /// Migration version the issue applies to.
    pub migration: String,
    /// Human-readable description of the issue.
    pub description: String,
    /// Short string describing which data is affected (table/field).
    #[serde(default)]
    pub affected_data: Option<String>,
    /// Optional mitigation recommendation.
    #[serde(default)]
    pub recommendation: Option<String>,
}

/// Ordered rollback plan with safety analysis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RollbackPlan {
    /// Version the database is being rolled back from.
    pub from_version: String,
    /// Target version after the rollback completes.
    pub to_version: String,
    /// Migrations to roll back, in execution order (newest first).
    pub migrations: Vec<Migration>,
    /// Worst-case safety level across all issues.
    pub overall_safety: RollbackSafety,
    /// Every issue surfaced by the analyser, in discovery order.
    #[serde(default)]
    pub issues: Vec<RollbackIssue>,
    /// `true` when the plan should require explicit user approval.
    #[serde(default)]
    pub requires_approval: bool,
}

impl RollbackPlan {
    /// Number of migrations in the plan.
    pub fn migration_count(&self) -> usize {
        self.migrations.len()
    }

    /// `true` when the plan is classified as fully safe.
    pub fn is_safe(&self) -> bool {
        self.overall_safety == RollbackSafety::Safe
    }

    /// `true` when any migration in the plan may lose data.
    pub fn has_data_loss(&self) -> bool {
        matches!(
            self.overall_safety,
            RollbackSafety::Warning | RollbackSafety::Danger
        )
    }
}

/// Outcome of executing a [`RollbackPlan`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RollbackResult {
    /// The plan that was executed.
    pub plan: RollbackPlan,
    /// `true` when every planned migration rolled back successfully.
    pub success: bool,
    /// Actual wall-clock duration in milliseconds.
    pub actual_duration_ms: u64,
    /// Number of migrations that were rolled back.
    pub rolled_back_count: usize,
    /// Per-migration error messages, if any.
    #[serde(default)]
    pub errors: Vec<String>,
    /// Statuses returned by each `execute_migration` call.
    #[serde(default)]
    pub statuses: Vec<MigrationStatus>,
}

impl RollbackResult {
    /// `true` when the number rolled back matches the plan's migration count.
    pub fn completed_all(&self) -> bool {
        self.rolled_back_count == self.plan.migration_count()
    }
}

/// Analyse a migrations directory for rollback safety to `target_version`.
///
/// This is a pure filesystem operation and does not touch the database.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationDiscovery`] if the directory cannot be
/// scanned, or [`SurqlError::Validation`] if `target_version` is missing
/// from the directory.
pub async fn analyze_rollback_safety(
    migrations_dir: &Path,
    target_version: &str,
) -> Result<Vec<RollbackIssue>> {
    let on_disk = discover_migrations(migrations_dir)?;
    if !on_disk.iter().any(|m| m.version == target_version) {
        return Err(SurqlError::Validation {
            reason: format!("target version {target_version} not found in migrations"),
        });
    }
    let mut issues = Vec::new();
    for migration in on_disk
        .iter()
        .filter(|m| m.version.as_str() > target_version)
    {
        issues.extend(analyse_migration(migration));
    }
    Ok(issues)
}

/// Build a rollback plan that moves the database to `target_version`.
///
/// # Errors
///
/// Returns [`SurqlError::Validation`] if the current database has no
/// applied migrations, if the target version is missing, or if the
/// target is not older than the current version.
pub async fn create_rollback_plan(
    client: &DatabaseClient,
    migrations_dir: &Path,
    target_version: &str,
) -> Result<RollbackPlan> {
    let on_disk = discover_migrations(migrations_dir)?;
    if !on_disk.iter().any(|m| m.version == target_version) {
        return Err(SurqlError::Validation {
            reason: format!("target version {target_version} not found in migrations"),
        });
    }

    let applied = get_applied_migrations(client).await?;
    let Some(latest) = applied.last() else {
        return Err(SurqlError::Validation {
            reason: "no migrations have been applied".to_string(),
        });
    };
    let current_version = latest.version.clone();

    if target_version >= current_version.as_str() {
        return Err(SurqlError::Validation {
            reason: format!(
                "target version {target_version} must be older than current version {current_version}"
            ),
        });
    }

    // Applied versions on the database (ordered ascending already).
    let applied_versions: std::collections::BTreeSet<String> =
        applied.iter().map(|m| m.version.clone()).collect();

    // The migrations we need to roll back are those applied on the server
    // whose version is strictly greater than the target. Newest first.
    let mut to_rollback: Vec<Migration> = on_disk
        .iter()
        .filter(|m| m.version.as_str() > target_version && applied_versions.contains(&m.version))
        .cloned()
        .collect();
    to_rollback.sort_by(|a, b| b.version.cmp(&a.version));

    let mut issues = Vec::new();
    let mut overall = RollbackSafety::Safe;
    for migration in &to_rollback {
        for issue in analyse_migration(migration) {
            if issue.safety.rank() > overall.rank() {
                overall = issue.safety;
            }
            issues.push(issue);
        }
    }

    Ok(RollbackPlan {
        from_version: current_version,
        to_version: target_version.to_string(),
        migrations: to_rollback,
        overall_safety: overall,
        requires_approval: overall != RollbackSafety::Safe,
        issues,
    })
}

/// Execute a rollback [`RollbackPlan`].
///
/// Runs each migration's `down` body in reverse chronological order
/// via [`execute_migration`]. Stops at the first failure.
///
/// # Errors
///
/// Returns [`SurqlError::MigrationExecution`] if a transaction cannot
/// be begun or the history update fails.
pub async fn execute_rollback(
    client: &DatabaseClient,
    plan: RollbackPlan,
) -> Result<RollbackResult> {
    let start = std::time::Instant::now();
    let mut rolled_back_count = 0usize;
    let mut errors: Vec<String> = Vec::new();
    let mut statuses: Vec<MigrationStatus> = Vec::with_capacity(plan.migrations.len());

    for migration in &plan.migrations {
        let status = execute_migration(client, migration, MigrationDirection::Down).await?;
        let failed = status.error.is_some();
        if failed {
            if let Some(err) = status.error.clone() {
                errors.push(format!("{}: {err}", migration.version));
            }
            statuses.push(status);
            break;
        }
        rolled_back_count += 1;
        statuses.push(status);
    }

    let duration_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
    let success = rolled_back_count == plan.migrations.len() && errors.is_empty();

    Ok(RollbackResult {
        plan,
        success,
        actual_duration_ms: duration_ms,
        rolled_back_count,
        errors,
        statuses,
    })
}

/// Convenience wrapper over [`create_rollback_plan`].
///
/// # Errors
///
/// See [`create_rollback_plan`].
pub async fn plan_rollback_to_version(
    client: &DatabaseClient,
    migrations_dir: &Path,
    target_version: &str,
) -> Result<RollbackPlan> {
    create_rollback_plan(client, migrations_dir, target_version).await
}

// ---------------------------------------------------------------------------
// Pure safety analysis (no DB)
// ---------------------------------------------------------------------------

fn analyse_migration(migration: &Migration) -> Vec<RollbackIssue> {
    let mut issues = Vec::new();
    if migration.down.is_empty() {
        issues.push(RollbackIssue {
            safety: RollbackSafety::Danger,
            migration: migration.version.clone(),
            description: "migration has no `down` statements; cannot roll back cleanly".into(),
            affected_data: None,
            recommendation: Some("add a `-- @down` block or restore from backup".into()),
        });
        return issues;
    }
    for statement in &migration.down {
        let upper = statement.to_ascii_uppercase();
        let trimmed = upper.trim();

        // Classify by looking at the first two significant tokens of the
        // statement ("REMOVE TABLE …", "REMOVE FIELD …", "REMOVE INDEX …",
        // "ALTER FIELD … TYPE …", etc.).
        let head = leading_tokens(trimmed, 2);
        let verb = head.first().map_or("", String::as_str);
        let object = head.get(1).map_or("", String::as_str);
        let is_remove_or_drop = matches!(verb, "REMOVE" | "DROP");

        if is_remove_or_drop && object == "TABLE" {
            let table = extract_after(statement, "TABLE").unwrap_or_else(|| "unknown".into());
            issues.push(RollbackIssue {
                safety: RollbackSafety::Danger,
                migration: migration.version.clone(),
                description: format!("dropping table: {table}"),
                affected_data: Some(format!("all records in table {table}")),
                recommendation: Some("export table data before rollback".into()),
            });
        } else if is_remove_or_drop && object == "FIELD" {
            let field = extract_after(statement, "FIELD").unwrap_or_else(|| "unknown".into());
            issues.push(RollbackIssue {
                safety: RollbackSafety::Warning,
                migration: migration.version.clone(),
                description: format!("dropping field: {field}"),
                affected_data: Some(format!("field data in {field}")),
                recommendation: Some("back up affected field data".into()),
            });
        } else if verb == "ALTER" && object == "FIELD" && trimmed.contains("TYPE") {
            issues.push(RollbackIssue {
                safety: RollbackSafety::Warning,
                migration: migration.version.clone(),
                description: "altering field type may cause data conversion issues".into(),
                affected_data: None,
                recommendation: Some("review data compatibility before rollback".into()),
            });
        }
        // Index / event drops and other operations are treated as safe.
    }
    issues
}

fn leading_tokens(upper: &str, n: usize) -> Vec<String> {
    upper
        .split(|c: char| c.is_whitespace() || c == ';' || c == ',')
        .filter(|s| !s.is_empty())
        .take(n)
        .map(str::to_string)
        .collect()
}

fn extract_after(statement: &str, anchor: &str) -> Option<String> {
    let upper = statement.to_ascii_uppercase();
    let anchor_upper = anchor.to_ascii_uppercase();
    let idx = upper.find(&anchor_upper)?;
    let after = &statement[idx + anchor.len()..];
    let token = after
        .split(|c: char| c.is_whitespace() || c == ';' || c == ',')
        .find(|s| !s.is_empty())?;
    Some(
        token
            .trim_matches(|c: char| c == ';' || c == ',')
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn m(version: &str, down: &[&str]) -> Migration {
        Migration {
            version: version.into(),
            description: "test".into(),
            path: PathBuf::from(format!("{version}.surql")),
            up: vec!["-- noop".into()],
            down: down.iter().map(|s| (*s).to_string()).collect(),
            checksum: None,
            depends_on: vec![],
        }
    }

    #[test]
    fn safe_rollback_for_index_drop() {
        let mig = m("v1", &["REMOVE INDEX idx_user_email ON TABLE user"]);
        let issues = analyse_migration(&mig);
        assert!(issues.is_empty());
    }

    #[test]
    fn table_drop_is_danger() {
        let mig = m("v2", &["REMOVE TABLE user"]);
        let issues = analyse_migration(&mig);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].safety, RollbackSafety::Danger);
        assert!(issues[0].description.contains("user"));
    }

    #[test]
    fn field_drop_is_warning() {
        let mig = m("v3", &["REMOVE FIELD email ON TABLE user"]);
        let issues = analyse_migration(&mig);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].safety, RollbackSafety::Warning);
    }

    #[test]
    fn alter_type_is_warning() {
        let mig = m("v4", &["ALTER FIELD age ON TABLE user TYPE string"]);
        let issues = analyse_migration(&mig);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].safety, RollbackSafety::Warning);
    }

    #[test]
    fn empty_down_is_danger() {
        let mig = m("v5", &[]);
        let issues = analyse_migration(&mig);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].safety, RollbackSafety::Danger);
    }

    #[test]
    fn safety_rank_orders_severity() {
        assert!(RollbackSafety::Safe.rank() < RollbackSafety::Warning.rank());
        assert!(RollbackSafety::Warning.rank() < RollbackSafety::Danger.rank());
    }

    #[test]
    fn rollback_plan_helpers() {
        let plan = RollbackPlan {
            from_version: "v3".into(),
            to_version: "v1".into(),
            migrations: vec![m("v3", &["REMOVE FIELD x ON TABLE t"])],
            overall_safety: RollbackSafety::Warning,
            issues: vec![],
            requires_approval: true,
        };
        assert_eq!(plan.migration_count(), 1);
        assert!(!plan.is_safe());
        assert!(plan.has_data_loss());
    }

    #[test]
    fn rollback_safety_serde_roundtrip() {
        for v in [
            RollbackSafety::Safe,
            RollbackSafety::Warning,
            RollbackSafety::Danger,
        ] {
            let j = serde_json::to_string(&v).unwrap();
            let back: RollbackSafety = serde_json::from_str(&j).unwrap();
            assert_eq!(v, back);
        }
    }

    #[test]
    fn extract_after_returns_table_name() {
        assert_eq!(
            extract_after("REMOVE TABLE user;", "TABLE"),
            Some("user".to_string())
        );
        assert_eq!(
            extract_after("remove table user;", "TABLE"),
            Some("user".to_string())
        );
    }

    #[tokio::test]
    async fn analyze_rollback_safety_rejects_missing_target() {
        use std::fs;
        let tmp = tempfile::tempdir().unwrap();
        fs::write(
            tmp.path().join("20260101_000000_a.surql"),
            "-- @metadata\n-- version: v1\n-- description: a\n-- @up\nDEFINE TABLE t;\n-- @down\nREMOVE TABLE t;\n",
        )
        .unwrap();
        let err = analyze_rollback_safety(tmp.path(), "vX").await.unwrap_err();
        assert!(matches!(err, SurqlError::Validation { .. }));
    }

    #[tokio::test]
    async fn analyze_rollback_safety_flags_table_drops() {
        use std::fs;
        let tmp = tempfile::tempdir().unwrap();
        fs::write(
            tmp.path().join("20260101_000000_a.surql"),
            "-- @metadata\n-- version: v1\n-- description: a\n-- @up\nDEFINE TABLE t;\n-- @down\nDEFINE TABLE t;\n",
        )
        .unwrap();
        fs::write(
            tmp.path().join("20260102_000000_b.surql"),
            "-- @metadata\n-- version: v2\n-- description: b\n-- @up\nDEFINE TABLE t2;\n-- @down\nREMOVE TABLE t2;\n",
        )
        .unwrap();
        let issues = analyze_rollback_safety(tmp.path(), "v1").await.unwrap();
        assert!(!issues.is_empty());
        assert!(issues.iter().any(|i| i.safety == RollbackSafety::Danger));
    }
}
