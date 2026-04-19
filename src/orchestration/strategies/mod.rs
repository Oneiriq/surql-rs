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
use crate::migration::{execute_migration, MigrationDirection};
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
/// Shared helper used by every concrete strategy. Public so strategies
/// defined outside this module can also leverage the common
/// Python-compatible error handling.
pub async fn deploy_to_environment(
    env: &EnvironmentConfig,
    plan: &DeploymentPlan,
) -> DeploymentResult {
    let started_at = Utc::now();

    if plan.dry_run {
        info!(environment = %env.name, "dry_run_deployment");
        return DeploymentResult::builder(&env.name, DeploymentStatus::Success, started_at)
            .completed_at(Utc::now())
            .execution_time_ms(0)
            .migrations_applied(plan.migrations.len())
            .build();
    }

    info!(
        environment = %env.name,
        migrations = plan.migrations.len(),
        "deploying_to_environment"
    );

    let client = match DatabaseClient::new(env.connection.clone()) {
        Ok(client) => client,
        Err(err) => {
            error!(environment = %env.name, error = %err, "deployment_client_failed");
            return DeploymentResult::builder(&env.name, DeploymentStatus::Failed, started_at)
                .completed_at(Utc::now())
                .error(err.to_string())
                .build();
        }
    };
    if let Err(err) = client.connect().await {
        error!(environment = %env.name, error = %err, "deployment_connect_failed");
        return DeploymentResult::builder(&env.name, DeploymentStatus::Failed, started_at)
            .completed_at(Utc::now())
            .error(err.to_string())
            .build();
    }

    let start = Instant::now();
    let mut applied = 0usize;
    for migration in &plan.migrations {
        if let Err(err) = execute_migration(&client, migration, MigrationDirection::Up).await {
            error!(environment = %env.name, migration = %migration.version, error = %err, "deployment_failed");
            let _ = client.disconnect().await;
            return DeploymentResult::builder(&env.name, DeploymentStatus::Failed, started_at)
                .completed_at(Utc::now())
                .error(err.to_string())
                .migrations_applied(applied)
                .build();
        }
        applied += 1;
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
        .migrations_applied(applied)
        .build()
}

/// Resolve the environment configurations referenced in a plan.
///
/// Helper shared by every strategy — returns the `EnvironmentConfig`s
/// in the order the plan declares them.
///
/// # Errors
///
/// Returns [`SurqlError::Orchestration`](crate::error::SurqlError) when
/// any of the plan's environment names are not registered.
pub async fn resolve_plan_environments(plan: &DeploymentPlan) -> Result<Vec<EnvironmentConfig>> {
    let registry = plan.registry.clone();
    let mut out = Vec::with_capacity(plan.environments.len());
    for name in &plan.environments {
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
