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
    configure_environments, deploy_to_environments, get_registry, DeploymentPlan, DeploymentResult,
    DeploymentStatus, HealthCheck, StrategyKind,
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

/// Flags of `surql orchestrate deploy`.
// Each bool is an independent command-line switch.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, clap::Args)]
pub struct DeployArgs {
    /// Path to the environments JSON file.
    #[arg(long, value_name = "PATH", default_value = "environments.json")]
    pub plan: PathBuf,
    /// Deployment strategy.
    #[arg(long, value_enum, default_value_t = StrategyArg::Sequential)]
    pub strategy: StrategyArg,
    /// Comma-separated environment names (defaults to every registered env).
    #[arg(long, value_name = "LIST")]
    pub environments: Option<String>,
    /// Dry-run: plan but do not apply.
    #[arg(long)]
    pub dry_run: bool,
    /// Approve changes to environments marked `require_approval`; without
    /// it the deploy is refused before anything runs.
    #[arg(long)]
    pub approve: bool,
    /// Skip the confirmation prompt (required when stdin is not a
    /// terminal).
    #[arg(long = "yes", short = 'y')]
    pub yes: bool,
    /// Leave successful environments deployed when another one fails
    /// (by default what this run applied is rolled back everywhere).
    #[arg(long)]
    pub no_auto_rollback: bool,
}

/// `surql orchestrate <subcommand>` commands.
#[derive(Debug, Subcommand)]
pub enum OrchestrateCommand {
    /// Deploy migrations across the environments declared by `--plan`.
    Deploy(DeployArgs),
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
        OrchestrateCommand::Deploy(args) => deploy(&settings, &args).await,
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

async fn deploy(settings: &crate::settings::Settings, args: &DeployArgs) -> Result<()> {
    let DeployArgs {
        plan: plan_path,
        strategy,
        environments,
        dry_run,
        approve,
        yes,
        no_auto_rollback,
    } = args.clone();
    load_plan(&plan_path).await?;
    let registry = get_registry();

    let migrations = discover_migrations(&settings.migration_path)?;
    if migrations.is_empty() {
        fmt::warn(format!(
            "no migrations discovered in {}",
            settings.migration_path.display()
        ));
    }

    let env_names: Vec<String> = match environments.as_deref() {
        Some(raw) => parse_environment_list(raw),
        None => registry.list().await,
    };

    if !dry_run {
        fmt::confirm(
            &format!(
                "deploy up to {} migration(s) to {} (auto-rollback {})",
                migrations.len(),
                env_names.join(", "),
                if no_auto_rollback { "off" } else { "on" }
            ),
            yes,
        )?;
    }

    let plan = DeploymentPlan::builder(registry)
        .environments(env_names.clone())
        .migrations(migrations.clone())
        .strategy(strategy.into())
        .dry_run(dry_run)
        .approved(approve)
        .auto_rollback(!no_auto_rollback)
        .build();

    fmt::info(format!(
        "deploying {} migration(s) to {} environment(s); each receives the ones it has not applied (strategy: {:?}, dry_run: {})",
        migrations.len(),
        env_names.len(),
        strategy,
        dry_run
    ));

    let results = deploy_to_environments(&plan).await?;

    let mut rows: Vec<&DeploymentResult> = results.values().collect();
    rows.sort_by(|a, b| a.environment.cmp(&b.environment));
    let mut table = fmt::make_table();
    table.set_header(vec![
        "environment",
        "status",
        "applied",
        "rolled_back",
        "duration_ms",
        "error",
    ]);
    for result in &rows {
        table.add_row(vec![
            result.environment.clone(),
            result.status.to_string(),
            if dry_run {
                format!("up to {}", result.migrations_applied)
            } else {
                result.applied_versions.join(", ")
            },
            result.rolled_back_versions.join(", "),
            result
                .execution_time_ms
                .map_or_else(|| "-".to_string(), |d| format!("{d}")),
            result.error.clone().unwrap_or_default(),
        ]);
    }
    println!("{table}");

    let unsuccessful = rows
        .iter()
        .filter(|r| r.status != DeploymentStatus::Success)
        .count();
    if unsuccessful > 0 {
        return Err(SurqlError::Orchestration {
            reason: format!(
                "{unsuccessful} of {} environment(s) did not end deployed",
                rows.len()
            ),
        });
    }
    fmt::success(format!("deployed to {} environment(s)", rows.len()));
    Ok(())
}

/// Split a `--environments a,b` list, dropping blanks and repeats so no
/// environment is deployed twice.
fn parse_environment_list(raw: &str) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    raw.split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty() && seen.insert(*name))
        .map(str::to_string)
        .collect()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn environment_list_drops_blanks_and_repeats() {
        assert_eq!(
            parse_environment_list("prod, prod,,stage ,prod"),
            vec!["prod".to_string(), "stage".to_string()]
        );
    }
}
