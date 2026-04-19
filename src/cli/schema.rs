//! `surql schema` subcommands.
//!
//! Wraps the schema registry, parser, validator, visualiser, and hook
//! helpers. Mirrors `surql-py`'s `surql.cli.schema` typer group.

use std::path::{Path, PathBuf};

use clap::{Subcommand, ValueEnum};

use crate::cli::fmt;
use crate::cli::GlobalOpts;
use crate::connection::DatabaseClient;
use crate::error::{Result, SurqlError};
use crate::migration::{
    check_schema_drift_from_snapshots, discover_migrations, generate_precommit_config,
    list_snapshots, registry_to_snapshot,
};
use crate::schema::{
    generate_schema_sql, get_registered_edges, get_registered_tables, parse_db_info,
    visualize_from_registry, OutputFormat as VizFormat, ThemeOption,
};

/// Visualisation theme variants exposed on the CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ThemeArg {
    /// Modern preset (default).
    Modern,
    /// Dark preset.
    Dark,
    /// Forest preset.
    Forest,
    /// Minimal preset.
    Minimal,
}

impl ThemeArg {
    fn as_name(self) -> &'static str {
        match self {
            Self::Modern => "modern",
            Self::Dark => "dark",
            Self::Forest => "forest",
            Self::Minimal => "minimal",
        }
    }
}

/// Visualisation output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum VizFormatArg {
    /// Mermaid ER-diagram.
    Mermaid,
    /// GraphViz DOT.
    Graphviz,
    /// ASCII art.
    Ascii,
}

impl From<VizFormatArg> for VizFormat {
    fn from(value: VizFormatArg) -> Self {
        match value {
            VizFormatArg::Mermaid => Self::Mermaid,
            VizFormatArg::Graphviz => Self::GraphViz,
            VizFormatArg::Ascii => Self::Ascii,
        }
    }
}

/// Export format for `surql schema export`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ExportFormat {
    /// Emit a JSON representation of the parsed schema.
    Json,
    /// Emit the schema as raw SurrealQL (`DEFINE` statements).
    Yaml,
}

/// `surql schema <subcommand>` commands.
#[derive(Debug, Subcommand)]
pub enum SchemaCommand {
    /// Show the current database schema.
    Show {
        /// Limit the output to a single table.
        table: Option<String>,
    },
    /// Compare two schema snapshots.
    Diff {
        /// Source snapshot file (JSON / YAML). Defaults to the latest.
        #[arg(long, value_name = "PATH")]
        from: Option<PathBuf>,
        /// Destination snapshot file.
        #[arg(long, value_name = "PATH")]
        to: Option<PathBuf>,
    },
    /// Emit `DEFINE` SQL for every registered table / edge.
    Generate {
        /// Write to this file instead of stdout.
        #[arg(long, short = 'o', value_name = "PATH")]
        output: Option<PathBuf>,
    },
    /// Placeholder for code-to-database synchronisation.
    Sync {
        /// Preview what would change.
        #[arg(long)]
        dry_run: bool,
    },
    /// Export the live database schema.
    Export {
        /// Output format.
        #[arg(long, short = 'f', value_enum, default_value_t = ExportFormat::Json)]
        format: ExportFormat,
        /// Output file (defaults to stdout).
        #[arg(long, short = 'o', value_name = "PATH")]
        output: Option<PathBuf>,
    },
    /// List all tables in the live database.
    Tables,
    /// Inspect a single table's fields / indexes / events / permissions.
    Inspect {
        /// Table name.
        table: String,
    },
    /// Validate that the registered schema matches the live database.
    Validate,
    /// Detect schema drift against the latest snapshot.
    Check,
    /// Emit a `.pre-commit-config.yaml` fragment for schema checks.
    HookConfig,
    /// Stub: watch schema files for changes (feature-gated).
    Watch,
    /// Render the registered schema as mermaid / graphviz / ascii.
    Visualize {
        /// Visual theme preset.
        #[arg(long, value_enum, default_value_t = ThemeArg::Modern)]
        theme: ThemeArg,
        /// Output format.
        #[arg(long, short = 'f', value_enum, default_value_t = VizFormatArg::Mermaid)]
        format: VizFormatArg,
        /// Write to this file instead of stdout.
        #[arg(long, short = 'o', value_name = "PATH")]
        output: Option<PathBuf>,
    },
}

/// Execute a `surql schema` subcommand.
///
/// # Errors
///
/// Propagates [`SurqlError`] values from the underlying library calls.
pub async fn run(cmd: SchemaCommand, global: &GlobalOpts) -> Result<()> {
    let settings = global.settings()?;
    match cmd {
        SchemaCommand::Show { table } => show(&settings, table.as_deref()).await,
        SchemaCommand::Diff { from, to } => diff(&settings, from.as_deref(), to.as_deref()),
        SchemaCommand::Generate { output } => generate(output.as_deref()),
        SchemaCommand::Sync { dry_run } => {
            sync(dry_run);
            Ok(())
        }
        SchemaCommand::Export { format, output } => {
            export(&settings, format, output.as_deref()).await
        }
        SchemaCommand::Tables => tables(&settings).await,
        SchemaCommand::Inspect { table } => inspect(&settings, &table).await,
        SchemaCommand::Validate => validate(&settings).await,
        SchemaCommand::Check => {
            check(&settings);
            Ok(())
        }
        SchemaCommand::HookConfig => {
            let cfg = generate_precommit_config("schemas/", true);
            println!("{cfg}");
            Ok(())
        }
        SchemaCommand::Watch => watch(),
        SchemaCommand::Visualize {
            theme,
            format,
            output,
        } => visualize(theme, format, output.as_deref()),
    }
}

async fn connected_client(settings: &crate::settings::Settings) -> Result<DatabaseClient> {
    let client = DatabaseClient::new(settings.database().clone())?;
    client.connect().await?;
    Ok(client)
}

async fn show(settings: &crate::settings::Settings, table: Option<&str>) -> Result<()> {
    let client = connected_client(settings).await?;
    let stmt = table.map_or_else(
        || "INFO FOR DB;".to_string(),
        |t| format!("INFO FOR TABLE {t};"),
    );
    let result = client.query(&stmt).await?;
    fmt::print_json(&result)?;
    Ok(())
}

fn diff(
    settings: &crate::settings::Settings,
    from: Option<&Path>,
    to: Option<&Path>,
) -> Result<()> {
    // Pull snapshots from the migration_path/snapshots directory when no
    // explicit paths are supplied.
    let snapshots_dir = settings.migration_path.join("snapshots");
    let snapshots = list_snapshots(&snapshots_dir).unwrap_or_default();

    let from_snap = if let Some(p) = from {
        load_snapshot(p)?
    } else {
        if snapshots.len() < 2 {
            return Err(SurqlError::Validation {
                reason: "need at least two snapshots (or --from) to diff".into(),
            });
        }
        let v = &snapshots[snapshots.len() - 2];
        crate::migration::hooks::versioned_to_snapshot(v)
    };
    let to_snap = if let Some(p) = to {
        load_snapshot(p)?
    } else {
        if snapshots.is_empty() {
            return Err(SurqlError::Validation {
                reason: "no snapshots available; pass --to".into(),
            });
        }
        let v = &snapshots[snapshots.len() - 1];
        crate::migration::hooks::versioned_to_snapshot(v)
    };
    let report = check_schema_drift_from_snapshots(&from_snap, &to_snap);
    println!("{}", report.to_summary());
    Ok(())
}

fn load_snapshot(path: &Path) -> Result<crate::migration::SchemaSnapshot> {
    let body = std::fs::read_to_string(path)?;
    let parsed: crate::migration::VersionedSnapshot =
        serde_json::from_str(&body).map_err(|e| SurqlError::Serialization {
            reason: format!("{}: {e}", path.display()),
        })?;
    Ok(crate::migration::hooks::versioned_to_snapshot(&parsed))
}

fn generate(output: Option<&Path>) -> Result<()> {
    use std::collections::BTreeMap;
    let tables = get_registered_tables();
    let edges = get_registered_edges();
    let tables_btree: BTreeMap<_, _> = tables.into_iter().collect();
    let edges_btree: BTreeMap<_, _> = edges.into_iter().collect();
    let body = generate_schema_sql(Some(&tables_btree), Some(&edges_btree), false)?;
    match output {
        Some(path) => {
            std::fs::write(path, &body)?;
            fmt::success(format!("wrote {}", path.display()));
        }
        None => println!("{body}"),
    }
    Ok(())
}

fn sync(dry_run: bool) {
    fmt::warn("`schema sync` is not recommended: use `schema generate` + `migrate up`");
    if dry_run {
        fmt::info("dry-run requested: no changes would be made");
    }
}

async fn export(
    settings: &crate::settings::Settings,
    format: ExportFormat,
    output: Option<&Path>,
) -> Result<()> {
    let client = connected_client(settings).await?;
    let info = client.query("INFO FOR DB;").await?;
    let parsed = parse_db_info(&info).unwrap_or_default();
    let body = match format {
        ExportFormat::Json => serde_json::to_string_pretty(&serde_json::json!({
            "tables": parsed.tables.keys().collect::<Vec<_>>(),
            "edges": parsed.edges.keys().collect::<Vec<_>>(),
            "accesses": parsed.accesses.keys().collect::<Vec<_>>(),
        }))?,
        ExportFormat::Yaml => {
            // Minimal human-readable YAML-ish text.
            let mut out = String::new();
            out.push_str("tables:\n");
            for name in parsed.tables.keys() {
                use std::fmt::Write as _;
                writeln!(&mut out, "  - {name}").ok();
            }
            out.push_str("accesses:\n");
            for name in parsed.accesses.keys() {
                use std::fmt::Write as _;
                writeln!(&mut out, "  - {name}").ok();
            }
            out
        }
    };
    match output {
        Some(path) => {
            std::fs::write(path, &body)?;
            fmt::success(format!("wrote {}", path.display()));
        }
        None => println!("{body}"),
    }
    Ok(())
}

async fn tables(settings: &crate::settings::Settings) -> Result<()> {
    let client = connected_client(settings).await?;
    let info = client.query("INFO FOR DB;").await?;
    let parsed = parse_db_info(&info).unwrap_or_default();
    if parsed.tables.is_empty() {
        fmt::info("no tables defined");
        return Ok(());
    }
    let mut table = fmt::make_table();
    table.set_header(vec!["table"]);
    let mut names: Vec<_> = parsed.tables.keys().cloned().collect();
    names.sort();
    for n in names {
        table.add_row(vec![n]);
    }
    println!("{table}");
    Ok(())
}

async fn inspect(settings: &crate::settings::Settings, table: &str) -> Result<()> {
    let client = connected_client(settings).await?;
    let info = client.query(&format!("INFO FOR TABLE {table};")).await?;
    fmt::print_json(&info)?;
    Ok(())
}

async fn validate(settings: &crate::settings::Settings) -> Result<()> {
    let client = connected_client(settings).await?;
    let info = client.query("INFO FOR DB;").await?;
    let db = parse_db_info(&info).unwrap_or_default();

    let code_tables = get_registered_tables();
    let code_edges = get_registered_edges();
    // validate_schema wants `HashMap<String, TableDefinition>` for both
    // tables and db_edges (parser emits edges as tables in its own map).
    let db_tables: std::collections::HashMap<String, crate::schema::TableDefinition> =
        db.tables.clone().into_iter().collect();
    let results = crate::schema::validate_schema(&code_tables, &db_tables, Some(&code_edges), None);
    let report = crate::schema::format_validation_report(&results, false);
    println!("{report}");

    if crate::schema::has_errors(&results) {
        return Err(SurqlError::Validation {
            reason: "schema validation reported errors".into(),
        });
    }
    Ok(())
}

fn check(settings: &crate::settings::Settings) {
    let snapshot_dir = settings.migration_path.join("snapshots");
    let snapshots = list_snapshots(&snapshot_dir).unwrap_or_default();
    let registry = crate::schema::get_registry();
    let code_snapshot = registry_to_snapshot(registry);
    if snapshots.is_empty() {
        fmt::info("no snapshots on disk; skipping drift check");
        return;
    }
    let latest = &snapshots[snapshots.len() - 1];
    let db_snapshot = crate::migration::hooks::versioned_to_snapshot(latest);
    let report = check_schema_drift_from_snapshots(&db_snapshot, &code_snapshot);
    println!("{}", report.to_summary());

    // Also note whether any migrations are untracked vs the snapshot.
    let migrations = discover_migrations(&settings.migration_path).unwrap_or_default();
    fmt::info(format!("{} migration(s) present on disk", migrations.len()));
}

#[allow(clippy::unnecessary_wraps)]
fn watch() -> Result<()> {
    #[cfg(feature = "watcher")]
    {
        fmt::info("schema watch: start the watcher programmatically via `SchemaWatcher::start`");
        fmt::info("(CLI interactivity is intentionally minimal; hook into the lib API)");
        Ok(())
    }
    #[cfg(not(feature = "watcher"))]
    {
        Err(SurqlError::Validation {
            reason: "schema watch requires the `watcher` feature".into(),
        })
    }
}

fn visualize(theme: ThemeArg, format: VizFormatArg, output: Option<&Path>) -> Result<()> {
    let theme_name = theme.as_name();
    let theme_opt = ThemeOption::Named(theme_name);
    let body = visualize_from_registry_with_theme(format.into(), &theme_opt)?;
    match output {
        Some(path) => {
            std::fs::write(path, &body)?;
            fmt::success(format!("wrote {}", path.display()));
        }
        None => println!("{body}"),
    }
    Ok(())
}

fn visualize_from_registry_with_theme(fmt_: VizFormat, theme: &ThemeOption<'_>) -> Result<String> {
    // Equivalent to `visualize_schema` against the registry tables/edges.
    let reg = crate::schema::get_registry();
    let tables = reg.tables();
    let edges = reg.edges();
    crate::schema::visualize::visualize_schema(&tables, Some(&edges), fmt_, true, true, Some(theme))
}

// The `visualize_from_registry` helper is kept here as a hint that callers
// may prefer the non-themed helper for simpler setups.
#[allow(dead_code)]
fn _untouched_helper() -> Result<String> {
    visualize_from_registry(VizFormat::Mermaid, true, true)
}
