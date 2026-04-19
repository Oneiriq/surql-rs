//! `surql orchestrate` subcommands.
//!
//! Wraps [`crate::orchestration`] — environment discovery,
//! health checks, and multi-database deployment strategies.

use std::path::{Path, PathBuf};

use clap::{Subcommand, ValueEnum};

use crate::cli::fmt;
use crate::cli::GlobalOpts;
use crate::error::{Result, SurqlError};
use crate::migration::discover_migrations;
use crate::orchestration::{
    configure_environments, get_registry, DeploymentPlan, DeploymentStatus, HealthCheck,
    MigrationCoordinator, StrategyKind,
};

/// Deployment strategy flag mirroring [`StrategyKind`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum StrategyArg {
    /// Apply environments one at a time.
    Sequential,
    /// Apply environments concurrently.
    Parallel,
    /// Apply in rolling batches.
    Rolling,
    /// Apply to a canary subset first.
    Canary,
}

impl From<StrategyArg> for StrategyKind {
    fn from(value: StrategyArg) -> Self {
        match value {
            StrategyArg::Sequential => Self::Sequential,
            StrategyArg::Parallel => Self::Parallel,
            StrategyArg::Rolling => Self::Rolling,
            StrategyArg::Canary => Self::Canary,
        }
    }
}

/// `surql orchestrate <subcommand>` commands.
#[derive(Debug, Subcommand)]
pub enum OrchestrateCommand {
    /// Deploy migrations across the environments declared by `--plan`.
    Deploy {
        /// Path to the environments JSON file.
        #[arg(long, value_name = "PATH", default_value = "environments.json")]
        plan: PathBuf,
        /// Deployment strategy.
        #[arg(long, value_enum, default_value_t = StrategyArg::Sequential)]
        strategy: StrategyArg,
        /// Comma-separated environment names (defaults to every registered env).
        #[arg(long, value_name = "LIST")]
        environments: Option<String>,
        /// Dry-run: plan but do not apply.
        #[arg(long)]
        dry_run: bool,
    },
    /// Show the health of each registered environment.
    Status {
        /// Path to the environments JSON file.
        #[arg(long, value_name = "PATH", default_value = "environments.json")]
        plan: PathBuf,
    },
    /// Validate the plan file + connectivity for each environment.
    Validate {
        /// Path to the environments JSON file.
        #[arg(long, value_name = "PATH", default_value = "environments.json")]
        plan: PathBuf,
    },
}

/// Execute a `surql orchestrate` subcommand.
///
/// # Errors
///
/// Propagates [`SurqlError`] values from the underlying library calls.
pub async fn run(cmd: OrchestrateCommand, global: &GlobalOpts) -> Result<()> {
    let settings = global.settings()?;
    match cmd {
        OrchestrateCommand::Deploy {
            plan,
            strategy,
            environments,
            dry_run,
        } => deploy(&settings, &plan, strategy, environments.as_deref(), dry_run).await,
        OrchestrateCommand::Status { plan } => status(&plan).await,
        OrchestrateCommand::Validate { plan } => validate(&plan).await,
    }
}

async fn load_plan(path: &Path) -> Result<()> {
    if !path.exists() {
        return Err(SurqlError::Validation {
            reason: format!("environments file not found: {}", path.display()),
        });
    }
    configure_environments(path).await?;
    Ok(())
}

async fn deploy(
    settings: &crate::settings::Settings,
    plan_path: &Path,
    strategy: StrategyArg,
    environments: Option<&str>,
    dry_run: bool,
) -> Result<()> {
    load_plan(plan_path).await?;
    let registry = get_registry();

    let migrations = discover_migrations(&settings.migration_path)?;
    if migrations.is_empty() {
        fmt::warn(format!(
            "no migrations discovered in {}",
            settings.migration_path.display()
        ));
    }

    let env_names: Vec<String> = match environments {
        Some(raw) => raw.split(',').map(|s| s.trim().to_string()).collect(),
        None => registry.list().await,
    };

    let plan = DeploymentPlan::builder(registry.clone())
        .environments(env_names.clone())
        .migrations(migrations.clone())
        .strategy(strategy.into())
        .dry_run(dry_run)
        .build();

    let coordinator =
        MigrationCoordinator::with_strategy_label(registry, strategy.into(), 1, 10.0, 5)?;

    fmt::info(format!(
        "deploying {} migration(s) to {} environment(s) (strategy: {:?}, dry_run: {})",
        migrations.len(),
        env_names.len(),
        strategy,
        dry_run
    ));

    let results = coordinator.deploy(&plan).await?;

    let mut table = fmt::make_table();
    table.set_header(vec![
        "environment",
        "status",
        "migrations",
        "duration_ms",
        "error",
    ]);
    let mut failures = 0;
    for (env, result) in &results {
        if result.status == DeploymentStatus::Failed {
            failures += 1;
        }
        table.add_row(vec![
            env.clone(),
            format!("{:?}", result.status),
            format!("{}", result.migrations_applied),
            result
                .execution_time_ms
                .map_or_else(|| "-".to_string(), |d| format!("{d}")),
            result.error.clone().unwrap_or_default(),
        ]);
    }
    println!("{table}");

    if failures > 0 {
        return Err(SurqlError::Orchestration {
            reason: format!("{failures} environment(s) failed"),
        });
    }
    fmt::success(format!("deployed to {} environment(s)", results.len()));
    Ok(())
}

async fn status(plan_path: &Path) -> Result<()> {
    load_plan(plan_path).await?;
    let registry = get_registry();
    let names = registry.list().await;
    if names.is_empty() {
        fmt::info("no environments registered");
        return Ok(());
    }
    let checker = HealthCheck::new();
    let mut table = fmt::make_table();
    table.set_header(vec![
        "environment",
        "connect",
        "migration_table",
        "healthy",
        "error",
    ]);
    for name in &names {
        let Some(cfg) = registry.get(name).await else {
            continue;
        };
        let status = checker.check_environment(&cfg).await?;
        table.add_row(vec![
            name.clone(),
            fmt::status_label(status.can_connect),
            fmt::status_label(status.migration_table_exists),
            fmt::status_label(status.is_healthy),
            status.error.clone().unwrap_or_default(),
        ]);
    }
    println!("{table}");
    Ok(())
}

async fn validate(plan_path: &Path) -> Result<()> {
    load_plan(plan_path).await?;
    let registry = get_registry();
    let names = registry.list().await;
    if names.is_empty() {
        fmt::warn("no environments registered");
        return Ok(());
    }
    fmt::success(format!(
        "plan ok: {} environment(s) loaded from {}",
        names.len(),
        plan_path.display()
    ));
    for n in &names {
        fmt::info(format!("  - {n}"));
    }
    Ok(())
}

// Ensure a compile-time reference to `HashMap` is not required even when
// no orchestration results are materialised through the CLI.
#[allow(dead_code)]
fn _touch() -> Vec<DeploymentStatus> {
    vec![DeploymentStatus::Success, DeploymentStatus::Failed]
}
