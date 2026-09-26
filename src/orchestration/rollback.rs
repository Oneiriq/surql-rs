//! Auto-rollback of a deployment that failed somewhere.
//!
//! When any environment of a [`DeploymentPlan`] fails and the plan asks
//! for auto-rollback, the coordinator hands every per-environment result
//! to [`rollback_deployment`]. Only the migrations the deployment itself
//! applied (its [`DeploymentResult::applied_versions`]) are reverted,
//! newest first; whatever an environment held before the deployment is
//! left alone.
//!
//! The status afterwards describes where the environment ended up:
//!
//! - [`DeploymentStatus::RolledBack`]: a successful environment whose
//!   applied migrations were all reverted.
//! - [`DeploymentStatus::Success`]: the rollback did not start (the
//!   reason is in `error`), so the deployment is still fully in place.
//! - [`DeploymentStatus::Failed`]: the environment's own deployment
//!   failed (anything it applied before failing is reverted), or the
//!   rollback stopped partway and left it between the two states.

use std::collections::HashMap;

use tracing::{error, info, warn};

use crate::connection::DatabaseClient;
use crate::migration::{execute_migration, Migration, MigrationDirection};
use crate::orchestration::coordinator::DeploymentPlan;
use crate::orchestration::environment::EnvironmentConfig;
use crate::orchestration::result::{DeploymentResult, DeploymentStatus};
use crate::orchestration::strategies::migration_failure;

/// Revert what the deployment applied to each environment in `results`.
pub(crate) async fn rollback_deployment(
    envs: &[EnvironmentConfig],
    plan: &DeploymentPlan,
    results: Vec<DeploymentResult>,
) -> Vec<DeploymentResult> {
    let mut out = Vec::with_capacity(results.len());
    for result in results {
        out.push(rollback_environment(envs, plan, result).await);
    }
    out
}

async fn rollback_environment(
    envs: &[EnvironmentConfig],
    plan: &DeploymentPlan,
    result: DeploymentResult,
) -> DeploymentResult {
    if result.applied_versions.is_empty() {
        return result;
    }
    let Some(env) = envs.iter().find(|e| e.name == result.environment) else {
        return not_started(result, "environment is not part of the plan");
    };
    let to_revert = match migrations_to_revert(plan, &result.applied_versions) {
        Ok(migrations) => migrations,
        Err(reason) => return not_started(result, &reason),
    };

    info!(environment = %env.name, count = to_revert.len(), "rolling_back_environment");
    let client = match DatabaseClient::new(env.connection.clone()) {
        Ok(client) => client,
        Err(err) => return not_started(result, &format!("cannot build client: {err}")),
    };
    if let Err(err) = client.connect().await {
        return not_started(result, &format!("cannot connect: {err}"));
    }

    let mut reverted: Vec<String> = Vec::with_capacity(to_revert.len());
    for migration in to_revert {
        let outcome = execute_migration(&client, migration, MigrationDirection::Down).await;
        if let Some(reason) = migration_failure(outcome, migration) {
            // Older `down` bodies assume this one ran; stop here.
            error!(
                environment = %env.name,
                migration = %migration.version,
                error = %reason,
                "rollback_migration_failed"
            );
            let _ = client.disconnect().await;
            return stopped(
                result,
                reverted,
                &format!("migration {}: {reason}", migration.version),
            );
        }
        reverted.push(migration.version.clone());
    }
    let _ = client.disconnect().await;
    info!(environment = %env.name, "environment_rolled_back");
    finished(result, reverted)
}

/// The plan migrations behind `applied`, newest first, or why they cannot
/// all be reverted.
fn migrations_to_revert<'a>(
    plan: &'a DeploymentPlan,
    applied: &[String],
) -> std::result::Result<Vec<&'a Migration>, String> {
    let by_version: HashMap<&str, &Migration> = plan
        .migrations
        .iter()
        .map(|m| (m.version.as_str(), m))
        .collect();
    applied
        .iter()
        .rev()
        .map(|version| {
            let migration = by_version
                .get(version.as_str())
                .copied()
                .ok_or_else(|| format!("migration {version} is not in the plan"))?;
            if migration.down.is_empty() {
                // Running an empty `down` would only drop the history row
                // and leave the schema change in place.
                return Err(format!(
                    "migration {version} has no down statements and cannot be reverted"
                ));
            }
            Ok(migration)
        })
        .collect()
}

fn append_error(existing: Option<String>, note: &str) -> String {
    match existing {
        Some(err) if !err.is_empty() => format!("{err}; {note}"),
        _ => note.to_string(),
    }
}

fn not_started(mut result: DeploymentResult, reason: &str) -> DeploymentResult {
    warn!(environment = %result.environment, reason, "rollback_not_started");
    let still = result.applied_versions.join(", ");
    result.error = Some(append_error(
        result.error.take(),
        &format!("auto-rollback did not run: {reason}; still applied: {still}"),
    ));
    result
}

fn stopped(mut result: DeploymentResult, reverted: Vec<String>, reason: &str) -> DeploymentResult {
    let still: Vec<&str> = result
        .applied_versions
        .iter()
        .filter(|v| !reverted.contains(v))
        .map(String::as_str)
        .collect();
    result.error = Some(append_error(
        result.error.take(),
        &format!(
            "auto-rollback stopped at {reason}; still applied: {}",
            still.join(", ")
        ),
    ));
    result.status = DeploymentStatus::Failed;
    result.rolled_back_versions = reverted;
    result
}

fn finished(mut result: DeploymentResult, reverted: Vec<String>) -> DeploymentResult {
    if result.status == DeploymentStatus::Success {
        result.status = DeploymentStatus::RolledBack;
    } else {
        result.error = Some(append_error(
            result.error.take(),
            &format!(
                "auto-rollback reverted {} migration(s) applied before the failure",
                reverted.len()
            ),
        ));
    }
    result.rolled_back_versions = reverted;
    result
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::orchestration::environment::EnvironmentRegistry;

    fn migration(version: &str, down: &[&str]) -> Migration {
        Migration {
            version: version.into(),
            description: String::new(),
            path: std::path::PathBuf::new(),
            up: vec![format!("DEFINE TABLE t{version};")],
            down: down.iter().map(|s| (*s).to_string()).collect(),
            checksum: None,
            depends_on: vec![],
        }
    }

    fn plan(migrations: Vec<Migration>) -> DeploymentPlan {
        DeploymentPlan::builder(EnvironmentRegistry::new())
            .migrations(migrations)
            .build()
    }

    #[test]
    fn reverts_only_applied_versions_newest_first() {
        let plan = plan(vec![
            migration("v1", &["REMOVE TABLE tv1;"]),
            migration("v2", &["REMOVE TABLE tv2;"]),
            migration("v3", &["REMOVE TABLE tv3;"]),
        ]);
        let order = migrations_to_revert(&plan, &["v2".into(), "v3".into()]).unwrap();
        let versions: Vec<&str> = order.iter().map(|m| m.version.as_str()).collect();
        assert_eq!(versions, vec!["v3", "v2"]);
    }

    #[test]
    fn refuses_migration_without_down() {
        let plan = plan(vec![migration("v1", &[])]);
        let err = migrations_to_revert(&plan, &["v1".into()]).unwrap_err();
        assert!(err.contains("no down statements"), "{err}");
    }

    #[test]
    fn finished_marks_success_rolled_back_and_keeps_failed() {
        let ok = DeploymentResult::builder("a", DeploymentStatus::Success, Utc::now())
            .applied_versions(vec!["v1".into()])
            .build();
        let ok = finished(ok, vec!["v1".into()]);
        assert_eq!(ok.status, DeploymentStatus::RolledBack);
        assert_eq!(ok.rolled_back_versions, vec!["v1"]);

        let bad = DeploymentResult::builder("b", DeploymentStatus::Failed, Utc::now())
            .error("boom")
            .applied_versions(vec!["v1".into()])
            .build();
        let bad = finished(bad, vec!["v1".into()]);
        assert_eq!(bad.status, DeploymentStatus::Failed);
        assert!(bad
            .error
            .unwrap()
            .starts_with("boom; auto-rollback reverted 1"));
    }

    #[test]
    fn stopped_marks_failed_and_lists_what_is_left() {
        let ok = DeploymentResult::builder("a", DeploymentStatus::Success, Utc::now())
            .applied_versions(vec!["v1".into(), "v2".into()])
            .build();
        let out = stopped(ok, vec!["v2".into()], "migration v1: nope");
        assert_eq!(out.status, DeploymentStatus::Failed);
        let err = out.error.unwrap();
        assert!(err.contains("still applied: v1"), "{err}");
    }
}
