//! Migration deployment coordinator.
//!
//! Port of `surql/orchestration/coordinator.py`. Aggregates the
//! [`EnvironmentRegistry`], [`HealthCheck`], and one of the concrete
//! [`DeploymentStrategy`] implementations behind a single
//! [`MigrationCoordinator`] facade.

use std::collections::HashMap;
use std::sync::Arc;

use tracing::{error, info, warn};

use crate::connection::DatabaseClient;
use crate::error::{Result, SurqlError};
use crate::migration::{execute_migration, Migration, MigrationDirection};
use crate::orchestration::environment::{EnvironmentConfig, EnvironmentRegistry};
use crate::orchestration::health::HealthCheck;
use crate::orchestration::result::{DeploymentResult, DeploymentStatus};
use crate::orchestration::strategies::{
    CanaryStrategy, DeploymentStrategy, ParallelStrategy, RollingStrategy, SequentialStrategy,
};

/// Raised when orchestration fails in a fatal way (wraps
/// [`SurqlError::Orchestration`]).
///
/// This type is kept distinct to mirror the Python `OrchestrationError`
/// exception hierarchy; callers that only care about the underlying
/// [`SurqlError`] can simply use `?` or `Result` propagation since
/// [`OrchestrationError`] implements `Into<SurqlError>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestrationError {
    /// Human-readable reason.
    pub reason: String,
}

impl OrchestrationError {
    /// Construct a new orchestration error.
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl std::fmt::Display for OrchestrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "orchestration error: {}", self.reason)
    }
}

impl std::error::Error for OrchestrationError {}

impl From<OrchestrationError> for SurqlError {
    fn from(value: OrchestrationError) -> Self {
        Self::Orchestration {
            reason: value.reason,
        }
    }
}

/// Named deployment strategy recognised by the coordinator.
///
/// Convenience alias for string-based strategy selection that matches
/// Python's `strategy: str` parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrategyKind {
    /// Sequential, one-at-a-time deploy.
    Sequential,
    /// Parallel deploy (bounded concurrency).
    Parallel,
    /// Batched rolling deploy.
    Rolling,
    /// Canary deploy — subset first, then the rest.
    Canary,
}

impl StrategyKind {
    /// Parse a case-insensitive string label (as used in Python's
    /// `deploy_to_environments(strategy=...)`).
    ///
    /// # Errors
    ///
    /// Returns [`SurqlError::Validation`] for unknown labels.
    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "sequential" => Ok(Self::Sequential),
            "parallel" => Ok(Self::Parallel),
            "rolling" => Ok(Self::Rolling),
            "canary" => Ok(Self::Canary),
            other => Err(SurqlError::Validation {
                reason: format!(
                    "Unknown strategy: {other}. Must be one of: sequential, parallel, rolling, canary",
                ),
            }),
        }
    }
}

/// Plan describing what to deploy, where, and how.
///
/// Port of `surql.orchestration.coordinator.DeploymentPlan`. Cloneable
/// so strategies can pass copies into spawned tasks without borrowing
/// the coordinator.
#[derive(Debug, Clone)]
pub struct DeploymentPlan {
    /// Registry used for environment lookup.
    pub registry: EnvironmentRegistry,
    /// Target environment names (in deployment order).
    pub environments: Vec<String>,
    /// Migrations to deploy to each environment.
    pub migrations: Vec<Migration>,
    /// Strategy selection hint — informational only (the strategy
    /// instance wired into the coordinator is what actually runs).
    pub strategy: StrategyKind,
    /// Batch size for [`RollingStrategy`].
    pub batch_size: usize,
    /// Canary percentage for [`CanaryStrategy`] (1.0..=50.0).
    pub canary_percentage: f64,
    /// Max concurrent deployments for [`ParallelStrategy`].
    pub max_concurrent: usize,
    /// Verify environment health before deploying.
    pub verify_health: bool,
    /// Auto-rollback previously successful deployments on failure.
    pub auto_rollback: bool,
    /// Simulate deployment without executing migrations.
    pub dry_run: bool,
}

impl DeploymentPlan {
    /// Start a builder for a deployment plan.
    pub fn builder(registry: EnvironmentRegistry) -> DeploymentPlanBuilder {
        DeploymentPlanBuilder {
            registry,
            environments: Vec::new(),
            migrations: Vec::new(),
            strategy: StrategyKind::Sequential,
            batch_size: 1,
            canary_percentage: 10.0,
            max_concurrent: 5,
            verify_health: true,
            auto_rollback: true,
            dry_run: false,
        }
    }
}

/// Builder for [`DeploymentPlan`].
#[derive(Debug, Clone)]
pub struct DeploymentPlanBuilder {
    registry: EnvironmentRegistry,
    environments: Vec<String>,
    migrations: Vec<Migration>,
    strategy: StrategyKind,
    batch_size: usize,
    canary_percentage: f64,
    max_concurrent: usize,
    verify_health: bool,
    auto_rollback: bool,
    dry_run: bool,
}

impl DeploymentPlanBuilder {
    /// Replace the target environment names.
    pub fn environments<I, S>(mut self, envs: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.environments = envs.into_iter().map(Into::into).collect();
        self
    }

    /// Append a target environment name.
    pub fn environment(mut self, name: impl Into<String>) -> Self {
        self.environments.push(name.into());
        self
    }

    /// Replace the migration set.
    pub fn migrations(mut self, migrations: Vec<Migration>) -> Self {
        self.migrations = migrations;
        self
    }

    /// Override the strategy label (informational).
    pub fn strategy(mut self, kind: StrategyKind) -> Self {
        self.strategy = kind;
        self
    }

    /// Override the rolling batch size.
    pub fn batch_size(mut self, value: usize) -> Self {
        self.batch_size = value.max(1);
        self
    }

    /// Override the canary percentage.
    pub fn canary_percentage(mut self, value: f64) -> Self {
        self.canary_percentage = value;
        self
    }

    /// Override the parallel max concurrency.
    pub fn max_concurrent(mut self, value: usize) -> Self {
        self.max_concurrent = value.max(1);
        self
    }

    /// Toggle pre-flight health checks.
    pub fn verify_health(mut self, value: bool) -> Self {
        self.verify_health = value;
        self
    }

    /// Toggle auto-rollback on any failure.
    pub fn auto_rollback(mut self, value: bool) -> Self {
        self.auto_rollback = value;
        self
    }

    /// Toggle dry-run (no migrations executed).
    pub fn dry_run(mut self, value: bool) -> Self {
        self.dry_run = value;
        self
    }

    /// Finalise into a [`DeploymentPlan`].
    pub fn build(self) -> DeploymentPlan {
        DeploymentPlan {
            registry: self.registry,
            environments: self.environments,
            migrations: self.migrations,
            strategy: self.strategy,
            batch_size: self.batch_size,
            canary_percentage: self.canary_percentage,
            max_concurrent: self.max_concurrent,
            verify_health: self.verify_health,
            auto_rollback: self.auto_rollback,
            dry_run: self.dry_run,
        }
    }
}

/// Coordinates deployments across the supplied environment registry.
///
/// Port of `surql.orchestration.coordinator.MigrationCoordinator`.
#[derive(Debug, Clone)]
pub struct MigrationCoordinator {
    registry: EnvironmentRegistry,
    strategy: Arc<dyn DeploymentStrategy>,
    health_check: HealthCheck,
}

impl MigrationCoordinator {
    /// Build a new coordinator with an explicit strategy instance.
    pub fn new(registry: EnvironmentRegistry, strategy: Arc<dyn DeploymentStrategy>) -> Self {
        Self {
            registry,
            strategy,
            health_check: HealthCheck::new(),
        }
    }

    /// Build a coordinator by string-selecting the strategy (matches Python).
    ///
    /// # Errors
    ///
    /// Returns [`SurqlError::Validation`] when the label is unknown, or
    /// when the canary percentage is out of range.
    pub fn with_strategy_label(
        registry: EnvironmentRegistry,
        strategy: StrategyKind,
        batch_size: usize,
        canary_percentage: f64,
        max_concurrent: usize,
    ) -> Result<Self> {
        let strategy: Arc<dyn DeploymentStrategy> = match strategy {
            StrategyKind::Sequential => Arc::new(SequentialStrategy::new()),
            StrategyKind::Parallel => {
                Arc::new(ParallelStrategy::with_max_concurrent(max_concurrent))
            }
            StrategyKind::Rolling => Arc::new(RollingStrategy::with_batch_size(batch_size)),
            StrategyKind::Canary => Arc::new(CanaryStrategy::with_percentage(canary_percentage)?),
        };
        Ok(Self::new(registry, strategy))
    }

    /// Access the wired registry (mainly used in tests).
    pub fn registry(&self) -> &EnvironmentRegistry {
        &self.registry
    }

    /// Deploy the supplied plan.
    ///
    /// # Errors
    ///
    /// Returns [`SurqlError::Orchestration`] when environments cannot be
    /// resolved, pre-flight health checks fail, or the strategy raises
    /// a fatal error. Per-environment failures are reported through the
    /// returned map (status = [`DeploymentStatus::Failed`]).
    pub async fn deploy(&self, plan: &DeploymentPlan) -> Result<HashMap<String, DeploymentResult>> {
        info!(
            environments = plan.environments.len(),
            migrations = plan.migrations.len(),
            strategy = ?plan.strategy,
            dry_run = plan.dry_run,
            "orchestration_started"
        );

        // Resolve environments up front so missing names fail fast.
        let envs = resolve_environments(&self.registry, &plan.environments).await?;

        if plan.verify_health && !plan.dry_run {
            info!("verifying_environment_health");
            let statuses = self.health_check.verify_all_environments(&envs).await?;
            let unhealthy: Vec<String> = statuses
                .iter()
                .filter(|(_, status)| !status.is_healthy)
                .map(|(name, _)| name.clone())
                .collect();
            if !unhealthy.is_empty() {
                return Err(SurqlError::Orchestration {
                    reason: format!("Unhealthy environments: {}", unhealthy.join(", ")),
                });
            }
        }

        let results = self.strategy.deploy(plan).await.map_err(|err| {
            error!(error = %err, "deployment_execution_failed");
            SurqlError::Orchestration {
                reason: format!("Deployment failed: {err}"),
            }
        })?;

        let map: HashMap<String, DeploymentResult> = results
            .iter()
            .map(|r| (r.environment.clone(), r.clone()))
            .collect();

        let failed = results
            .iter()
            .filter(|r| r.status == DeploymentStatus::Failed)
            .count();

        if failed > 0 && plan.auto_rollback && !plan.dry_run {
            warn!(failed, "initiating_auto_rollback");
            rollback_successful(&envs, &plan.migrations, &results).await;
        }

        info!(
            total = results.len(),
            successful = results
                .iter()
                .filter(|r| r.status == DeploymentStatus::Success)
                .count(),
            failed,
            "orchestration_completed"
        );

        Ok(map)
    }

    /// Return a map `env_name -> is_healthy` for the supplied environments.
    pub async fn deployment_status(
        &self,
        environments: &[String],
    ) -> Result<HashMap<String, bool>> {
        let mut configs = Vec::with_capacity(environments.len());
        for name in environments {
            if let Some(cfg) = self.registry.get(name).await {
                configs.push(cfg);
            }
        }
        let statuses = self.health_check.verify_all_environments(&configs).await?;
        Ok(statuses
            .into_iter()
            .map(|(name, status)| (name, status.is_healthy))
            .collect())
    }
}

async fn resolve_environments(
    registry: &EnvironmentRegistry,
    names: &[String],
) -> Result<Vec<EnvironmentConfig>> {
    let mut out = Vec::with_capacity(names.len());
    for name in names {
        match registry.get(name).await {
            Some(cfg) => out.push(cfg),
            None => {
                return Err(SurqlError::Orchestration {
                    reason: format!("Environment not found: {name}"),
                });
            }
        }
    }
    Ok(out)
}

async fn rollback_successful(
    environments: &[EnvironmentConfig],
    migrations: &[Migration],
    results: &[DeploymentResult],
) {
    for result in results
        .iter()
        .filter(|r| r.status == DeploymentStatus::Success)
    {
        let Some(env) = environments.iter().find(|e| e.name == result.environment) else {
            continue;
        };
        info!(environment = %env.name, "rolling_back_environment");
        let client = match DatabaseClient::new(env.connection.clone()) {
            Ok(c) => c,
            Err(err) => {
                error!(environment = %env.name, error = %err, "rollback_client_failed");
                continue;
            }
        };
        if let Err(err) = client.connect().await {
            error!(environment = %env.name, error = %err, "rollback_connect_failed");
            continue;
        }
        for migration in migrations.iter().rev() {
            if let Err(err) = execute_migration(&client, migration, MigrationDirection::Down).await
            {
                error!(
                    environment = %env.name,
                    migration = %migration.version,
                    error = %err,
                    "rollback_migration_failed"
                );
            }
        }
        let _ = client.disconnect().await;
        info!(environment = %env.name, "environment_rolled_back");
    }
}

/// Convenience wrapper for the common `deploy(...)` invocation.
///
/// Matches the Python `deploy_to_environments` free function — builds a
/// [`DeploymentPlan`] from the supplied arguments, instantiates a
/// coordinator with the requested strategy, and executes the deploy.
///
/// # Errors
///
/// Propagates errors from [`MigrationCoordinator::with_strategy_label`]
/// and [`MigrationCoordinator::deploy`].
#[allow(clippy::too_many_arguments)]
pub async fn deploy_to_environments(
    registry: EnvironmentRegistry,
    environments: Vec<String>,
    migrations: Vec<Migration>,
    strategy: StrategyKind,
    batch_size: usize,
    canary_percentage: f64,
    max_concurrent: usize,
    verify_health: bool,
    auto_rollback: bool,
    dry_run: bool,
) -> Result<HashMap<String, DeploymentResult>> {
    let coordinator = MigrationCoordinator::with_strategy_label(
        registry.clone(),
        strategy,
        batch_size,
        canary_percentage,
        max_concurrent,
    )?;
    let plan = DeploymentPlan::builder(registry)
        .environments(environments)
        .migrations(migrations)
        .strategy(strategy)
        .batch_size(batch_size)
        .canary_percentage(canary_percentage)
        .max_concurrent(max_concurrent)
        .verify_health(verify_health)
        .auto_rollback(auto_rollback)
        .dry_run(dry_run)
        .build();
    coordinator.deploy(&plan).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strategy_kind_parse_accepts_each_variant() {
        for (raw, kind) in [
            ("sequential", StrategyKind::Sequential),
            ("Parallel", StrategyKind::Parallel),
            ("ROLLING", StrategyKind::Rolling),
            ("canary", StrategyKind::Canary),
        ] {
            assert_eq!(StrategyKind::parse(raw).unwrap(), kind);
        }
    }

    #[test]
    fn strategy_kind_parse_rejects_unknown() {
        let err = StrategyKind::parse("ultralight").unwrap_err();
        assert!(matches!(err, SurqlError::Validation { .. }));
    }

    #[test]
    fn orchestration_error_into_surql_error() {
        let err = OrchestrationError::new("nope");
        let wrapped: SurqlError = err.into();
        assert!(matches!(wrapped, SurqlError::Orchestration { .. }));
    }

    #[tokio::test]
    async fn deployment_plan_builder_captures_fields() {
        let registry = EnvironmentRegistry::new();
        let plan = DeploymentPlan::builder(registry)
            .environments(["staging", "prod"])
            .strategy(StrategyKind::Rolling)
            .batch_size(2)
            .canary_percentage(20.0)
            .max_concurrent(3)
            .verify_health(false)
            .auto_rollback(false)
            .dry_run(true)
            .build();
        assert_eq!(plan.environments, vec!["staging", "prod"]);
        assert_eq!(plan.strategy, StrategyKind::Rolling);
        assert_eq!(plan.batch_size, 2);
        assert!((plan.canary_percentage - 20.0).abs() < f64::EPSILON);
        assert_eq!(plan.max_concurrent, 3);
        assert!(!plan.verify_health);
        assert!(!plan.auto_rollback);
        assert!(plan.dry_run);
    }

    #[tokio::test]
    async fn coordinator_rejects_missing_environment() {
        let registry = EnvironmentRegistry::new();
        let coordinator = MigrationCoordinator::with_strategy_label(
            registry.clone(),
            StrategyKind::Sequential,
            1,
            10.0,
            5,
        )
        .unwrap();
        let plan = DeploymentPlan::builder(registry)
            .environments(["ghost"])
            .dry_run(true)
            .verify_health(false)
            .build();
        let err = coordinator.deploy(&plan).await.unwrap_err();
        assert!(matches!(err, SurqlError::Orchestration { .. }));
    }

    #[tokio::test]
    async fn coordinator_dry_run_produces_success_per_env() {
        let registry = EnvironmentRegistry::new();
        let connection = crate::connection::ConnectionConfig::builder()
            .url("ws://127.0.0.1:65535")
            .namespace("dry")
            .database("run")
            .timeout(1.0)
            .retry_max_attempts(1)
            .retry_min_wait(0.1)
            .retry_max_wait(1.0)
            .build()
            .unwrap();
        let env = EnvironmentConfig::builder("dry_run_env", connection)
            .build()
            .unwrap();
        registry.register(env).await;

        let coordinator = MigrationCoordinator::with_strategy_label(
            registry.clone(),
            StrategyKind::Sequential,
            1,
            10.0,
            1,
        )
        .unwrap();
        let plan = DeploymentPlan::builder(registry)
            .environments(["dry_run_env"])
            .dry_run(true)
            .verify_health(false)
            .build();
        let results = coordinator.deploy(&plan).await.unwrap();
        assert_eq!(results.len(), 1);
        let r = results.get("dry_run_env").unwrap();
        assert_eq!(r.status, DeploymentStatus::Success);
    }
}
