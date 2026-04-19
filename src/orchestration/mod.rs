//! Multi-database migration orchestration.
//!
//! Port of `surql/orchestration/` from `oneiriq-surql` (Python). Feature
//! gated behind the `orchestration` cargo feature. Provides the
//! machinery needed to deploy the same migration set to many database
//! instances with a choice of strategies (sequential, parallel,
//! rolling, canary).
//!
//! ## Components
//!
//! - [`EnvironmentConfig`] and [`EnvironmentRegistry`]: describe and
//!   register target databases.
//! - [`HealthCheck`] and [`HealthStatus`]: pre-flight reachability and
//!   migration-table probes.
//! - [`DeploymentPlan`] and [`MigrationCoordinator`]: bundle the target
//!   set, migration list, and runtime knobs; run the chosen strategy.
//! - [`strategies`]: [`DeploymentStrategy`] trait with the concrete
//!   [`SequentialStrategy`], [`ParallelStrategy`], [`RollingStrategy`],
//!   and [`CanaryStrategy`] implementations.
//! - [`DeploymentResult`] / [`DeploymentStatus`]: per-environment
//!   outcome values.
//!
//! ## Examples
//!
//! ```no_run
//! # #[cfg(feature = "orchestration")] {
//! use std::sync::Arc;
//! use surql::connection::ConnectionConfig;
//! use surql::orchestration::{
//!     DeploymentPlan, EnvironmentConfig, EnvironmentRegistry, MigrationCoordinator,
//!     SequentialStrategy, StrategyKind,
//! };
//!
//! # async fn demo() -> surql::error::Result<()> {
//! let registry = EnvironmentRegistry::new();
//! let connection = ConnectionConfig::builder()
//!     .url("ws://localhost:8000")
//!     .namespace("prod")
//!     .database("main")
//!     .build()?;
//! let env = EnvironmentConfig::builder("production", connection).build()?;
//! registry.register(env).await;
//!
//! let coordinator = MigrationCoordinator::new(registry.clone(), Arc::new(SequentialStrategy::new()));
//! let plan = DeploymentPlan::builder(registry)
//!     .environment("production")
//!     .strategy(StrategyKind::Sequential)
//!     .dry_run(true)
//!     .verify_health(false)
//!     .build();
//! let _results = coordinator.deploy(&plan).await?;
//! # Ok(()) }
//! # }
//! ```

pub mod coordinator;
pub mod environment;
pub mod health;
pub mod result;
pub mod strategies;

pub use coordinator::{
    deploy_to_environments, DeploymentPlan, DeploymentPlanBuilder, MigrationCoordinator,
    OrchestrationError, StrategyKind,
};
pub use environment::{
    configure_environments, get_registry, register_environment, set_registry, EnvironmentConfig,
    EnvironmentConfigBuilder, EnvironmentRegistry,
};
pub use health::{check_environment_health, verify_connectivity, HealthCheck, HealthStatus};
pub use result::{DeploymentResult, DeploymentResultBuilder, DeploymentStatus};
pub use strategies::{
    CanaryStrategy, DeploymentStrategy, ParallelStrategy, RollingStrategy, SequentialStrategy,
};
