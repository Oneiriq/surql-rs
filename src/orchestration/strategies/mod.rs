//! Deployment strategies for multi-database orchestration.
//!
//! Port of `surql/orchestration/strategy.py` (the strategy hierarchy
//! only — the `DeploymentResult`/`DeploymentStatus` value types live in
//! [`crate::orchestration::result`]).
//!
//! Each strategy implements the [`DeploymentStrategy`] trait, which
//! exposes a single async `deploy` method keyed off a
//! [`DeploymentPlan`]. The coordinator selects a concrete strategy at
//! runtime by wrapping it in `Arc<dyn DeploymentStrategy>`.

pub mod canary;
pub mod parallel;
pub mod rolling;
pub mod sequential;

use std::collections::HashSet;
use std::time::Instant;

use async_trait::async_trait;
use chrono::Utc;
use tracing::{error, info};

pub use canary::CanaryStrategy;
pub use parallel::ParallelStrategy;
pub use rolling::RollingStrategy;
pub use sequential::SequentialStrategy;

use crate::connection::DatabaseClient;
use crate::error::Result;
use crate::migration::{
    execute_migration, get_applied_migrations, Migration, MigrationDirection, MigrationState,
    MigrationStatus,
};
use crate::orchestration::coordinator::DeploymentPlan;
use crate::orchestration::environment::EnvironmentConfig;
use crate::orchestration::result::{DeploymentResult, DeploymentStatus};

/// Strategy for rolling migrations out to a plan's environments.
///
/// Port of `surql.orchestration.strategy.DeploymentStrategy`. The TS
/// port exposes plain functions (`sequentialDeploy`, ...); the Rust
/// port intentionally follows Python's class hierarchy so the
/// coordinator can hold an `Arc<dyn DeploymentStrategy>`.
#[async_trait]
pub trait DeploymentStrategy: std::fmt::Debug + Send + Sync {
    /// Deploy the supplied plan, returning one [`DeploymentResult`]
    /// per target environment (in the same order the plan listed them).
    async fn deploy(&self, plan: &DeploymentPlan) -> Result<Vec<DeploymentResult>>;
}

/// Deploy a plan's migrations to a single environment.
///
/// Only the plan's migrations that the environment's history does not
/// already record are applied, in ascending version order, so an
/// environment that is partway through the plan is brought up to date
/// rather than having its earlier migrations re-run. The versions applied
/// are listed in [`DeploymentResult::applied_versions`].
///
/// A dry run does not connect: it reports every plan migration as the
/// upper bound of what would be applied.
///
/// Shared helper used by every concrete strategy. Public so strategies
/// defined outside this module can also leverage the common
/// Python-compatible error handling.
pub async fn deploy_to_environment(
    env: &EnvironmentConfig,
    plan: &DeploymentPlan,
) -> DeploymentResult {
    let started_at = Utc::now();
    let failed = |reason: String, applied: Vec<String>| {
        DeploymentResult::builder(&env.name, DeploymentStatus::Failed, started_at)
            .completed_at(Utc::now())
            .error(reason)
            .applied_versions(applied)
            .build()
    };

    if plan.dry_run {
        info!(environment = %env.name, "dry_run_deployment");
        return DeploymentResult::builder(&env.name, DeploymentStatus::Success, started_at)
            .completed_at(Utc::now())
            .execution_time_ms(0)
            .migrations_applied(plan.migrations.len())
            .build();
    }

    let client = match DatabaseClient::new(env.connection.clone()) {
        Ok(client) => client,
        Err(err) => {
            error!(environment = %env.name, error = %err, "deployment_client_failed");
            return failed(err.to_string(), Vec::new());
        }
    };
    if let Err(err) = client.connect().await {
        error!(environment = %env.name, error = %err, "deployment_connect_failed");
        return failed(err.to_string(), Vec::new());
    }

    let pending = match pending_migrations(&client, &plan.migrations).await {
        Ok(pending) => pending,
        Err(err) => {
            error!(environment = %env.name, error = %err, "deployment_history_failed");
            let _ = client.disconnect().await;
            return failed(format!("cannot read migration history: {err}"), Vec::new());
        }
    };
    info!(
        environment = %env.name,
        pending = pending.len(),
        "deploying_to_environment"
    );

    let start = Instant::now();
    let mut applied = Vec::with_capacity(pending.len());
    for migration in pending {
        let outcome = execute_migration(&client, migration, MigrationDirection::Up).await;
        if let Some(reason) = migration_failure(outcome, migration) {
            error!(environment = %env.name, migration = %migration.version, error = %reason, "deployment_failed");
            let _ = client.disconnect().await;
            return failed(
                format!("migration {}: {reason}", migration.version),
                applied,
            );
        }
        applied.push(migration.version.clone());
    }
    let elapsed_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
    let _ = client.disconnect().await;

    info!(
        environment = %env.name,
        execution_time_ms = elapsed_ms,
        "deployment_successful"
    );

    DeploymentResult::builder(&env.name, DeploymentStatus::Success, started_at)
        .completed_at(Utc::now())
        .execution_time_ms(elapsed_ms)
        .applied_versions(applied)
        .build()
}

/// The migrations of `migrations` that the environment's history does
/// not record, one per version, in ascending version order.
async fn pending_migrations<'a>(
    client: &DatabaseClient,
    migrations: &'a [Migration],
) -> Result<Vec<&'a Migration>> {
    let applied: HashSet<String> = get_applied_migrations(client)
        .await?
        .into_iter()
        .map(|h| h.version)
        .collect();
    let mut seen: HashSet<&str> = HashSet::new();
    let mut pending: Vec<&Migration> = migrations
        .iter()
        .filter(|m| !applied.contains(&m.version) && seen.insert(m.version.as_str()))
        .collect();
    pending.sort_by(|a, b| a.version.cmp(&b.version));
    Ok(pending)
}

/// The failure reason of one `execute_migration` call, or `None` when the
/// migration ran.
///
/// `execute_migration` reports a failed statement or commit as `Ok` with a
/// [`MigrationState::Failed`] status and keeps `Err` for transport and
/// history errors. Both mean the migration did not take effect.
pub(crate) fn migration_failure(
    outcome: Result<MigrationStatus>,
    migration: &Migration,
) -> Option<String> {
    match outcome {
        Ok(status) if status.state == MigrationState::Failed => Some(
            status
                .error
                .unwrap_or_else(|| format!("migration {} failed", migration.version)),
        ),
        Ok(_) => None,
        Err(err) => Some(err.to_string()),
    }
}

/// Resolve the environment configurations referenced in a plan.
///
/// Helper shared by every strategy — returns the `EnvironmentConfig`s
/// in the order the plan declares them. A name listed more than once is
/// resolved once, at its first position, so no environment is deployed
/// twice.
///
/// # Errors
///
/// Returns [`SurqlError::Orchestration`](crate::error::SurqlError) when
/// any of the plan's environment names are not registered.
pub async fn resolve_plan_environments(plan: &DeploymentPlan) -> Result<Vec<EnvironmentConfig>> {
    let registry = plan.registry.clone();
    let mut seen: HashSet<&str> = HashSet::new();
    let mut out = Vec::with_capacity(plan.environments.len());
    for name in plan
        .environments
        .iter()
        .filter(|name| seen.insert(name.as_str()))
    {
        match registry.get(name).await {
            Some(cfg) => out.push(cfg),
            None => {
                return Err(crate::error::SurqlError::Orchestration {
                    reason: format!("Environment not found: {name}"),
                });
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::ConnectionConfig;
    use crate::orchestration::environment::EnvironmentRegistry;

    fn env(name: &str) -> EnvironmentConfig {
        let cfg = ConnectionConfig::builder()
            .url("ws://127.0.0.1:65535")
            .namespace("ns")
            .database(name)
            .build()
            .unwrap();
        EnvironmentConfig::builder(name, cfg).build().unwrap()
    }

    #[tokio::test]
    async fn resolve_plan_environments_drops_repeated_names() {
        let registry = EnvironmentRegistry::new();
        registry.register(env("prod")).await;
        registry.register(env("stage")).await;
        let plan = DeploymentPlan::builder(registry)
            .environments(["prod", "stage", "prod", "stage"])
            .build();
        let envs = resolve_plan_environments(&plan).await.unwrap();
        let names: Vec<&str> = envs.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["prod", "stage"]);
    }

    #[test]
    fn migration_failure_treats_failed_status_as_failure() {
        let migration = Migration {
            version: "v1".into(),
            description: String::new(),
            path: std::path::PathBuf::new(),
            up: vec![],
            down: vec![],
            checksum: None,
            depends_on: vec![],
        };
        let status = |state, error: Option<&str>| MigrationStatus {
            migration: migration.clone(),
            state,
            applied_at: None,
            error: error.map(str::to_string),
        };
        assert_eq!(
            migration_failure(Ok(status(MigrationState::Failed, Some("boom"))), &migration),
            Some("boom".to_string())
        );
        assert!(migration_failure(Ok(status(MigrationState::Applied, None)), &migration).is_none());
        assert!(migration_failure(
            Err(crate::error::SurqlError::MigrationExecution {
                reason: "gone".into()
            }),
            &migration
        )
        .is_some_and(|r| r.contains("gone")));
    }
}
