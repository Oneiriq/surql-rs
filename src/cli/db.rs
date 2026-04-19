//! `surql db` subcommands.
//!
//! Thin wrappers over [`crate::connection::DatabaseClient`] and the
//! migration history helpers. Mirrors `surql-py`'s `surql.cli.db`
//! typer group.

use std::path::PathBuf;

use clap::Subcommand;

use crate::cli::fmt;
use crate::cli::GlobalOpts;
use crate::connection::DatabaseClient;
use crate::error::{Result, SurqlError};
use crate::migration::history::{ensure_migration_table, MIGRATION_TABLE_NAME};

/// `surql db <subcommand>` commands.
#[derive(Debug, Subcommand)]
pub enum DbCommand {
    /// Initialise the database (ensures migration tracking table exists).
    Init,
    /// Ping the configured database.
    Ping,
    /// Show the resolved database connection info.
    Info {
        /// Emit as JSON instead of a formatted block.
        #[arg(long)]
        json: bool,
    },
    /// Remove every table from the configured database.
    Reset {
        /// Skip the interactive confirmation prompt.
        #[arg(long = "yes", short = 'y')]
        yes: bool,
    },
    /// Execute an inline SurrealQL query or one loaded from a file.
    Query {
        /// Raw SurrealQL to execute.
        #[arg(conflicts_with = "file")]
        surql: Option<String>,
        /// Load the query body from a file on disk.
        #[arg(long, value_name = "PATH")]
        file: Option<PathBuf>,
    },
    /// Show the SurrealDB server version (via `INFO FOR DB`).
    Version,
}

/// Execute a `surql db` subcommand.
///
/// # Errors
///
/// Propagates [`SurqlError`] values from the underlying library calls.
pub async fn run(cmd: DbCommand, global: &GlobalOpts) -> Result<()> {
    let settings = global.settings()?;
    match cmd {
        DbCommand::Init => init(&settings).await,
        DbCommand::Ping => ping(&settings).await,
        DbCommand::Info { json } => info(&settings, json),
        DbCommand::Reset { yes } => reset(&settings, yes).await,
        DbCommand::Query { surql, file } => {
            query(&settings, surql.as_deref(), file.as_deref()).await
        }
        DbCommand::Version => version(&settings).await,
    }
}

async fn connected_client(settings: &crate::settings::Settings) -> Result<DatabaseClient> {
    let client = DatabaseClient::new(settings.database().clone())?;
    client.connect().await?;
    Ok(client)
}

async fn init(settings: &crate::settings::Settings) -> Result<()> {
    fmt::info(format!(
        "connecting to {}/{}",
        settings.database().namespace(),
        settings.database().database()
    ));
    let client = connected_client(settings).await?;
    ensure_migration_table(&client).await?;
    fmt::success(format!(
        "database initialised (tracking table: {MIGRATION_TABLE_NAME})"
    ));
    Ok(())
}

async fn ping(settings: &crate::settings::Settings) -> Result<()> {
    fmt::info(format!("pinging {}", settings.database().url()));
    let client = connected_client(settings).await?;
    let healthy = client.health().await?;
    if healthy {
        fmt::success("pong");
        Ok(())
    } else {
        Err(SurqlError::Connection {
            reason: "database reported unhealthy".into(),
        })
    }
}

fn info(settings: &crate::settings::Settings, as_json: bool) -> Result<()> {
    let cfg = settings.database();
    let redacted_password = if cfg.password().is_some() { "***" } else { "" };

    if as_json {
        let payload = serde_json::json!({
            "url": cfg.url(),
            "namespace": cfg.namespace(),
            "database": cfg.database(),
            "username": cfg.username(),
            "password_set": cfg.password().is_some(),
            "timeout_s": cfg.timeout(),
            "max_connections": cfg.max_connections(),
            "retry_max_attempts": cfg.retry_max_attempts(),
        });
        fmt::print_json(&payload)?;
    } else {
        let mut table = fmt::make_table();
        table.set_header(vec!["field", "value"]);
        table.add_row(vec!["url".to_string(), cfg.url().to_string()]);
        table.add_row(vec!["namespace".to_string(), cfg.namespace().to_string()]);
        table.add_row(vec!["database".to_string(), cfg.database().to_string()]);
        table.add_row(vec![
            "username".to_string(),
            cfg.username().unwrap_or("").to_string(),
        ]);
        table.add_row(vec!["password".to_string(), redacted_password.to_string()]);
        table.add_row(vec!["timeout_s".to_string(), format!("{}", cfg.timeout())]);
        table.add_row(vec![
            "max_connections".to_string(),
            format!("{}", cfg.max_connections()),
        ]);
        table.add_row(vec![
            "retry_max_attempts".to_string(),
            format!("{}", cfg.retry_max_attempts()),
        ]);
        println!("{table}");
    }
    Ok(())
}

async fn reset(settings: &crate::settings::Settings, yes: bool) -> Result<()> {
    fmt::warn(format!(
        "this will DROP all tables in {}/{}",
        settings.database().namespace(),
        settings.database().database()
    ));
    if !yes {
        fmt::warn("re-run with --yes to confirm");
        return Ok(());
    }
    let client = connected_client(settings).await?;
    let info_value = client.query("INFO FOR DB;").await?;
    let mut tables: Vec<String> = Vec::new();
    if let serde_json::Value::Array(stmts) = &info_value {
        for stmt in stmts {
            if let Some(tb) = stmt.pointer("/tables").and_then(|v| v.as_object()) {
                tables.extend(tb.keys().cloned());
            } else if let Some(tb) = stmt.pointer("/tb").and_then(|v| v.as_object()) {
                tables.extend(tb.keys().cloned());
            }
        }
    }
    if tables.is_empty() {
        fmt::info("no tables present");
        return Ok(());
    }
    fmt::info(format!("removing {} table(s)", tables.len()));
    for t in &tables {
        client.query(&format!("REMOVE TABLE {t};")).await?;
    }
    fmt::success(format!("removed {} table(s)", tables.len()));
    Ok(())
}

async fn query(
    settings: &crate::settings::Settings,
    inline: Option<&str>,
    file: Option<&std::path::Path>,
) -> Result<()> {
    let body = match (inline, file) {
        (Some(s), None) => s.to_string(),
        (None, Some(path)) => std::fs::read_to_string(path)?,
        (None, None) => {
            return Err(SurqlError::Validation {
                reason: "either inline query or --file <PATH> is required".into(),
            });
        }
        (Some(_), Some(_)) => {
            return Err(SurqlError::Validation {
                reason: "inline query and --file are mutually exclusive".into(),
            });
        }
    };
    let client = connected_client(settings).await?;
    let result = client.query(&body).await?;
    fmt::print_json(&result)?;
    Ok(())
}

async fn version(settings: &crate::settings::Settings) -> Result<()> {
    let client = connected_client(settings).await?;
    let info = client.query("INFO FOR DB;").await?;
    fmt::info(format!("connected to {}", settings.database().url()));
    fmt::print_json(&info)?;
    Ok(())
}
