//! `surql schema` subcommands.
//!
//! Wraps the schema registry, parser, validator, visualiser, and hook
//! helpers. Mirrors `surql-py`'s `surql.cli.schema` typer group.

use std::collections::{BTreeMap, HashMap};
use std::hash::BuildHasher;
use std::path::{Path, PathBuf};

use clap::{Subcommand, ValueEnum};
use serde_json::Value;

use crate::cli::fmt;
use crate::cli::GlobalOpts;
use crate::connection::DatabaseClient;
use crate::error::{Result, SurqlError};
use crate::migration::{
    check_schema_drift_from_snapshots, discover_migrations, generate_precommit_config,
    list_snapshots, registry_to_snapshot,
};
use crate::schema::parser::parse_table_full;
use crate::schema::{
    generate_schema_sql, get_registered_buckets, get_registered_edges, get_registered_tables,
    parse_db_info, parse_edge_info, EdgeDefinition, OutputFormat as VizFormat, TableDefinition,
    ThemeOption,
};
use crate::types::escape::quote_ident;

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
        |t| format!("INFO FOR TABLE {};", quote_ident(t)),
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
        let Some(v) = snapshots
            .len()
            .checked_sub(2)
            .and_then(|i| snapshots.get(i))
        else {
            return Err(SurqlError::Validation {
                reason: "need at least two snapshots (or --from) to diff".into(),
            });
        };
        crate::migration::hooks::versioned_to_snapshot(v)
    };
    let to_snap = if let Some(p) = to {
        load_snapshot(p)?
    } else {
        let Some(v) = snapshots.last() else {
            return Err(SurqlError::Validation {
                reason: "no snapshots available; pass --to".into(),
            });
        };
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
    let tables = get_registered_tables();
    let edges = get_registered_edges();
    let buckets = get_registered_buckets();
    let tables_btree: BTreeMap<_, _> = tables.into_iter().collect();
    let edges_btree: BTreeMap<_, _> = edges.into_iter().collect();
    let mut body = generate_schema_sql(Some(&tables_btree), Some(&edges_btree), false)?;
    // Buckets are database-level objects; append their DEFINE statements after
    // the table/edge DDL (sorted by name for deterministic output).
    let buckets_btree: BTreeMap<_, _> = buckets.into_iter().collect();
    for bucket in buckets_btree.values() {
        if !body.is_empty() {
            body.push_str("\n\n");
        }
        body.push_str(&bucket.to_surql()?);
    }
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
    let parsed = parse_db_info(&info)?;
    let body = match format {
        ExportFormat::Json => serde_json::to_string_pretty(&serde_json::json!({
            "tables": parsed.tables.keys().collect::<Vec<_>>(),
            "edges": parsed.edges.keys().collect::<Vec<_>>(),
            "accesses": parsed.accesses.keys().collect::<Vec<_>>(),
        }))?,
        ExportFormat::Yaml => {
            // Minimal human-readable YAML-ish text.
            let list = |names: Vec<&String>| -> String {
                names.iter().fold(String::new(), |mut out, name| {
                    out.push_str("  - ");
                    out.push_str(name);
                    out.push('\n');
                    out
                })
            };
            format!(
                "tables:\n{}accesses:\n{}",
                list(parsed.tables.keys().collect()),
                list(parsed.accesses.keys().collect()),
            )
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
    let parsed = parse_db_info(&info)?;
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
    let info = client
        .query(&format!("INFO FOR TABLE {};", quote_ident(table)))
        .await?;
    fmt::print_json(&info)?;
    Ok(())
}

/// Tables and edges read back from a live database, complete with the
/// fields, indexes, and events each one's `INFO FOR TABLE` reports.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LiveSchema {
    /// Plain (non-edge) tables keyed by name.
    pub tables: HashMap<String, TableDefinition>,
    /// Edges keyed by name.
    pub edges: HashMap<String, EdgeDefinition>,
}

/// The `DEFINE TABLE` statement of every table in an `INFO FOR DB` response,
/// keyed by table name. Accepts the bare object or the one-statement array
/// [`DatabaseClient::query`] returns.
fn table_statements(info: &Value) -> BTreeMap<String, String> {
    let info = info
        .as_array()
        .and_then(|items| items.first())
        .unwrap_or(info);
    ["tables", "tb"]
        .iter()
        .find_map(|key| info.get(*key).and_then(Value::as_object))
        .map(|tables| {
            tables
                .iter()
                .filter_map(|(name, def)| Some((name.clone(), def.as_str()?.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

/// Read the live schema [`validate_schema`](crate::schema::validate_schema)
/// compares the registry against.
///
/// `INFO FOR DB` lists the tables but carries none of their fields, indexes,
/// or events, so every table is completed with its own `INFO FOR TABLE`. A
/// table the database declares `TYPE RELATION`, or that `code_edges`
/// registers as an edge, is parsed as an [`EdgeDefinition`] (tables and
/// edges share one namespace, and a non-`RELATION` edge is an ordinary table
/// to the engine); every other table as a [`TableDefinition`].
///
/// # Errors
///
/// Propagates query failures and [`SurqlError::SchemaParse`] for an `INFO`
/// response that is not an object.
pub async fn fetch_live_schema<S: BuildHasher>(
    client: &DatabaseClient,
    code_edges: &HashMap<String, EdgeDefinition, S>,
) -> Result<LiveSchema> {
    let info = client.query("INFO FOR DB;").await?;
    let relations = parse_db_info(&info)?.edges;
    let mut live = LiveSchema::default();
    for (name, define) in table_statements(&info) {
        let table_info = client
            .query(&format!("INFO FOR TABLE {};", quote_ident(&name)))
            .await?;
        if relations.contains_key(&name) || code_edges.contains_key(&name) {
            let edge = parse_edge_info(&name, &table_info, Some(&define))?;
            live.edges.insert(name, edge);
        } else {
            let table = parse_table_full(&name, &define, &table_info)?;
            live.tables.insert(name, table);
        }
    }
    Ok(live)
}

async fn validate(settings: &crate::settings::Settings) -> Result<()> {
    let client = connected_client(settings).await?;
    let code_tables = get_registered_tables();
    let code_edges = get_registered_edges();
    let live = fetch_live_schema(&client, &code_edges).await?;
    let results = crate::schema::validate_schema(
        &code_tables,
        &live.tables,
        Some(&code_edges),
        Some(&live.edges),
    );
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
    let Some(latest) = snapshots.last() else {
        fmt::info("no snapshots on disk; skipping drift check");
        return;
    };
    let db_snapshot = crate::migration::hooks::versioned_to_snapshot(latest);
    let report = check_schema_drift_from_snapshots(&db_snapshot, &code_snapshot);
    println!("{}", report.to_summary());

    // Also note whether any migrations are untracked vs the snapshot.
    let migrations = discover_migrations(&settings.migration_path).unwrap_or_default();
    fmt::info(format!("{} migration(s) present on disk", migrations.len()));
}

fn watch() -> Result<()> {
    if !cfg!(feature = "watcher") {
        return Err(SurqlError::Validation {
            reason: "schema watch requires the `watcher` feature".into(),
        });
    }
    fmt::info("schema watch: start the watcher programmatically via `SchemaWatcher::start`");
    fmt::info("(CLI interactivity is intentionally minimal; hook into the lib API)");
    Ok(())
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
