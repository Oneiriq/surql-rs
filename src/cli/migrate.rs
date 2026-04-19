//! `surql migrate` subcommands.
//!
//! Wraps the migration runtime ([`crate::migration`]). Mirrors
//! `surql-py`'s `surql.cli.migrate` typer group.

use std::path::{Path, PathBuf};

use clap::Subcommand;

use crate::cli::fmt;
use crate::cli::GlobalOpts;
use crate::connection::DatabaseClient;
use crate::error::{Result, SurqlError};
use crate::migration::{
    create_blank_migration, create_migration_plan, discover_migrations, execute_migration_plan,
    get_migration_history, get_migration_status, migrate_down as lib_migrate_down,
    squash_migrations, validate_migrations, MigrationDirection, MigrationPlan, SquashOptions,
};

/// `surql migrate <subcommand>` commands.
#[derive(Debug, Subcommand)]
pub enum MigrateCommand {
    /// Apply pending migrations (optionally up to a specific version).
    Up {
        /// Apply up to and including this migration version.
        #[arg(long, value_name = "VERSION")]
        target: Option<String>,
        /// Preview without executing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Roll back previously-applied migrations.
    Down {
        /// Roll back until (and including) this version is reached.
        #[arg(long, value_name = "VERSION")]
        target: Option<String>,
        /// Preview without executing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Show applied/pending counts for the current migrations directory.
    Status,
    /// Show the `_migration_history` table rows.
    History,
    /// Create a blank migration template on disk.
    Create {
        /// Human-readable description (used as both filename hint and the
        /// `description` field in the generated metadata block).
        description: String,
        /// Target directory. Defaults to `settings.migration_path`.
        #[arg(long, value_name = "PATH")]
        schema_dir: Option<PathBuf>,
    },
    /// Validate the migrations directory for structural issues.
    Validate {
        /// Optional version to focus the validation on.
        version: Option<String>,
    },
    /// Generate a diff-based migration (range-based).
    Generate {
        /// Start version (inclusive).
        #[arg(long, value_name = "VERSION")]
        from: Option<String>,
        /// End version (inclusive).
        #[arg(long, value_name = "VERSION")]
        to: Option<String>,
    },
    /// Squash a contiguous version range into one migration file.
    Squash {
        /// Start version (inclusive).
        from: String,
        /// End version (inclusive).
        to: String,
        /// Explicit output path. Defaults to an auto-named file in the
        /// migrations directory.
        #[arg(long, short = 'o', value_name = "PATH")]
        output: Option<PathBuf>,
        /// Preview the squash plan without writing a file.
        #[arg(long)]
        dry_run: bool,
    },
}

/// Execute a `surql migrate` subcommand.
///
/// # Errors
///
/// Propagates [`SurqlError`] values from the underlying library calls.
pub async fn run(cmd: MigrateCommand, global: &GlobalOpts) -> Result<()> {
    let settings = global.settings()?;
    let migrations_dir = settings.migration_path.clone();

    match cmd {
        MigrateCommand::Up { target, dry_run } => {
            up(&settings, &migrations_dir, target.as_deref(), dry_run).await
        }
        MigrateCommand::Down { target, dry_run } => {
            down(&settings, &migrations_dir, target.as_deref(), dry_run).await
        }
        MigrateCommand::Status => status(&settings, &migrations_dir).await,
        MigrateCommand::History => history(&settings).await,
        MigrateCommand::Create {
            description,
            schema_dir,
        } => {
            let dir = schema_dir.unwrap_or(migrations_dir);
            create(&description, &dir)
        }
        MigrateCommand::Validate { version } => validate(&migrations_dir, version.as_deref()).await,
        MigrateCommand::Generate { from, to } => {
            generate(&migrations_dir, from.as_deref(), to.as_deref())
        }
        MigrateCommand::Squash {
            from,
            to,
            output,
            dry_run,
        } => squash(&migrations_dir, &from, &to, output.as_deref(), dry_run),
    }
}

async fn connected_client(settings: &crate::settings::Settings) -> Result<DatabaseClient> {
    let client = DatabaseClient::new(settings.database().clone())?;
    client.connect().await?;
    Ok(client)
}

async fn up(
    settings: &crate::settings::Settings,
    migrations_dir: &Path,
    target: Option<&str>,
    dry_run: bool,
) -> Result<()> {
    let client = connected_client(settings).await?;
    let plan = create_migration_plan(&client, migrations_dir).await?;
    let selected = select_up_range(plan, target)?;

    if selected.migrations.is_empty() {
        fmt::info("no pending migrations");
        return Ok(());
    }

    if dry_run {
        fmt::info(format!(
            "dry-run: {} migration(s) would be applied",
            selected.migrations.len()
        ));
        for m in &selected.migrations {
            fmt::info(format!("  - {} {}", m.version, m.description));
        }
        return Ok(());
    }

    let statuses = execute_migration_plan(&client, selected).await?;
    render_statuses(&statuses);
    Ok(())
}

async fn down(
    settings: &crate::settings::Settings,
    migrations_dir: &Path,
    target: Option<&str>,
    dry_run: bool,
) -> Result<()> {
    let client = connected_client(settings).await?;
    let steps = resolve_down_steps(&client, migrations_dir, target).await?;

    if steps == 0 {
        fmt::info("nothing to roll back");
        return Ok(());
    }

    if dry_run {
        fmt::info(format!(
            "dry-run: {steps} migration(s) would be rolled back"
        ));
        return Ok(());
    }

    let statuses = lib_migrate_down(&client, migrations_dir, steps).await?;
    render_statuses(&statuses);
    Ok(())
}

async fn status(settings: &crate::settings::Settings, migrations_dir: &Path) -> Result<()> {
    let client = connected_client(settings).await?;
    let report = get_migration_status(&client, migrations_dir).await?;
    fmt::info(format!(
        "total: {}  applied: {}  pending: {}",
        report.total,
        report.applied_count(),
        report.pending_count()
    ));

    let mut table = fmt::make_table();
    table.set_header(vec!["version", "state", "description"]);
    for s in &report.applied {
        table.add_row(vec![
            s.migration.version.clone(),
            "applied".to_string(),
            s.migration.description.clone(),
        ]);
    }
    for s in &report.pending {
        table.add_row(vec![
            s.migration.version.clone(),
            "pending".to_string(),
            s.migration.description.clone(),
        ]);
    }
    println!("{table}");
    Ok(())
}

async fn history(settings: &crate::settings::Settings) -> Result<()> {
    let client = connected_client(settings).await?;
    let rows = get_migration_history(&client).await?;
    if rows.is_empty() {
        fmt::info("no migration history");
        return Ok(());
    }
    let mut table = fmt::make_table();
    table.set_header(vec!["version", "applied_at", "description", "checksum"]);
    for row in &rows {
        table.add_row(vec![
            row.version.clone(),
            row.applied_at.to_rfc3339(),
            row.description.clone(),
            row.checksum.clone(),
        ]);
    }
    println!("{table}");
    Ok(())
}

fn create(description: &str, directory: &Path) -> Result<()> {
    std::fs::create_dir_all(directory)?;
    let migration = create_blank_migration(description, description, directory)?;
    fmt::success(format!(
        "created {} at {}",
        migration.version,
        migration.path.display()
    ));
    Ok(())
}

async fn validate(migrations_dir: &Path, version: Option<&str>) -> Result<()> {
    let errors = validate_migrations(migrations_dir).await?;
    let filtered: Vec<&String> = match version {
        Some(v) => errors.iter().filter(|e| e.contains(v)).collect(),
        None => errors.iter().collect(),
    };
    if filtered.is_empty() {
        fmt::success("migrations are consistent");
        return Ok(());
    }
    for err in &filtered {
        fmt::error(err);
    }
    Err(SurqlError::MigrationDiscovery {
        reason: format!("{} validation error(s)", filtered.len()),
    })
}

fn generate(migrations_dir: &Path, from: Option<&str>, to: Option<&str>) -> Result<()> {
    // Range-based generation re-uses the squash machinery in dry-run mode to
    // collect statements spanning the range without writing anything.
    let all = discover_migrations(migrations_dir)?;
    let filtered: Vec<_> = crate::migration::filter_migrations_by_version(&all, from, to);
    if filtered.is_empty() {
        fmt::warn("no migrations in requested range");
        return Ok(());
    }
    let mut table = fmt::make_table();
    table.set_header(vec!["version", "description", "up_stmts", "down_stmts"]);
    for m in &filtered {
        table.add_row(vec![
            m.version.clone(),
            m.description.clone(),
            format!("{}", m.up.len()),
            format!("{}", m.down.len()),
        ]);
    }
    println!("{table}");
    fmt::info(format!(
        "{} migration(s) would be combined (use `surql migrate squash` to persist)",
        filtered.len()
    ));
    Ok(())
}

fn squash(
    migrations_dir: &Path,
    from: &str,
    to: &str,
    output: Option<&Path>,
    dry_run: bool,
) -> Result<()> {
    let mut opts = SquashOptions::new().from_version(from).to_version(to);
    if dry_run {
        opts = opts.dry_run(true);
    }
    if let Some(path) = output {
        opts = opts.output_path(path);
    }
    let result = squash_migrations(migrations_dir, &opts)?;
    fmt::success(format!(
        "squashed {} migration(s) into {} (statements: {}, optimisations: {})",
        result.original_count,
        result.squashed_path.display(),
        result.statement_count,
        result.optimizations_applied
    ));
    Ok(())
}

fn select_up_range(plan: MigrationPlan, target: Option<&str>) -> Result<MigrationPlan> {
    let Some(target) = target else {
        return Ok(plan);
    };
    let mut selected = Vec::new();
    let mut found = false;
    for m in plan.migrations {
        selected.push(m.clone());
        if m.version == target {
            found = true;
            break;
        }
    }
    if !found {
        return Err(SurqlError::Validation {
            reason: format!("target version {target:?} not found among pending migrations"),
        });
    }
    Ok(MigrationPlan {
        migrations: selected,
        direction: MigrationDirection::Up,
    })
}

async fn resolve_down_steps(
    client: &DatabaseClient,
    migrations_dir: &Path,
    target: Option<&str>,
) -> Result<u32> {
    let applied = get_migration_history(client).await?;
    if applied.is_empty() {
        return Ok(0);
    }
    // Merge with on-disk order to ensure deterministic step counting.
    let _on_disk = discover_migrations(migrations_dir)?;

    let Some(target) = target else {
        // Default: single-step rollback.
        return Ok(1);
    };

    // Count applied migrations strictly newer than the target (inclusive of
    // target means rollback stops *after* removing the target as well).
    let mut steps: u32 = 0;
    let mut seen_target = false;
    for row in applied.iter().rev() {
        steps = steps.saturating_add(1);
        if row.version == target {
            seen_target = true;
            break;
        }
    }
    if !seen_target {
        return Err(SurqlError::Validation {
            reason: format!("target version {target:?} not found in history"),
        });
    }
    Ok(steps)
}

fn render_statuses(statuses: &[crate::migration::MigrationStatus]) {
    let mut table = fmt::make_table();
    table.set_header(vec!["version", "state", "error"]);
    for s in statuses {
        table.add_row(vec![
            s.migration.version.clone(),
            format!("{:?}", s.state),
            s.error.clone().unwrap_or_default(),
        ]);
    }
    println!("{table}");
}
