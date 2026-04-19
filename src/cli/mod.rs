//! `surql` CLI root.
//!
//! Implements the top-level command tree exposed by the `surql`
//! binary and dispatches to the per-group sub-modules ([`db`],
//! [`migrate`], [`schema`], [`orchestrate`]).
//!
//! The CLI is a thin wrapper around the library: it never contains
//! SurrealQL- or schema-specific logic of its own, and every side-effect
//! is delegated to an existing public function on [`crate`].
//!
//! Feature-gated behind `cli`.
//!
//! ## Exit codes
//!
//! - `0` success
//! - `1` operation failure
//! - `2` usage error (enforced by `clap`)
//!
//! ## Configuration
//!
//! Every subcommand accepts `--config <path>` to override the
//! automatic [`Settings`] discovery. Without
//! the flag the standard layered lookup runs (env, `.env`,
//! `Cargo.toml [package.metadata.surql]`).

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use crate::error::{Result, SurqlError};
use crate::settings::{Settings, SettingsBuilder};

pub mod db;
pub mod fmt;
pub mod migrate;
pub mod orchestrate;
pub mod schema;

/// Exit code returned on an unrecoverable operation failure.
pub const EXIT_FAILURE: u8 = 1;

/// Top-level CLI entry point.
///
/// The crate binary delegates to this function and uses the returned
/// [`ExitCode`] as its process exit status.
#[must_use]
pub fn run() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let cli = match Cli::try_parse_from(&args) {
        Ok(cli) => cli,
        Err(err) => {
            // clap already formats errors; exit codes match its defaults
            // (0 for `--help`, 2 for parse errors).
            err.exit();
        }
    };

    match execute(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            fmt::error(format!("error: {err}"));
            ExitCode::from(EXIT_FAILURE)
        }
    }
}

/// Dispatch a parsed [`Cli`] to the appropriate subcommand implementation.
///
/// Exposed (rather than inlined in [`run`]) so integration tests can drive
/// the CLI with a programmatically-constructed [`Cli`].
///
/// # Errors
///
/// Propagates [`SurqlError`] values emitted by any subcommand handler.
pub fn execute(cli: Cli) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| SurqlError::Io {
            reason: format!("failed to start async runtime: {e}"),
        })?;

    runtime.block_on(dispatch(cli))
}

async fn dispatch(cli: Cli) -> Result<()> {
    let global = &cli.global;
    match cli.command {
        Command::Version => {
            print_version();
            Ok(())
        }
        Command::Db(cmd) => db::run(cmd, global).await,
        Command::Migrate(cmd) => migrate::run(cmd, global).await,
        Command::Schema(cmd) => schema::run(cmd, global).await,
        Command::Orchestrate(cmd) => orchestrate::run(cmd, global).await,
    }
}

/// Print the crate version string in the canonical `surql <semver>` form.
pub fn print_version() {
    println!("surql {}", env!("CARGO_PKG_VERSION"));
}

/// Top-level CLI definition.
#[derive(Debug, Parser)]
#[command(
    name = "surql",
    about = "Code-first database toolkit for SurrealDB",
    version,
    propagate_version = true,
    disable_help_subcommand = true
)]
pub struct Cli {
    /// Global options shared by every subcommand.
    #[command(flatten)]
    pub global: GlobalOpts,

    /// Selected subcommand.
    #[command(subcommand)]
    pub command: Command,
}

/// Global flags shared by every subcommand group.
#[derive(Debug, Clone, clap::Args)]
pub struct GlobalOpts {
    /// Override the automatic `Settings` discovery with a `Cargo.toml`-style
    /// settings file.
    #[arg(long = "config", global = true, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Print extra diagnostic information for subcommands that support it.
    #[arg(long = "verbose", short = 'v', global = true)]
    pub verbose: bool,
}

impl GlobalOpts {
    /// Resolve the effective [`Settings`] for this invocation.
    ///
    /// When `--config <path>` is supplied the file's parent directory is
    /// used as the working directory for the settings loader, which
    /// causes it to pick up the TOML metadata declared in that file.
    /// Otherwise the loader walks upward from the current directory as
    /// documented on [`Settings::load`].
    ///
    /// # Errors
    ///
    /// Propagates validation errors from [`Settings::load`].
    pub fn settings(&self) -> Result<Settings> {
        let mut builder = SettingsBuilder::default();
        if let Some(path) = &self.config {
            let cwd = path
                .parent()
                .map_or_else(|| PathBuf::from("."), PathBuf::from);
            builder = builder.cwd(cwd);
        }
        builder.load()
    }
}

/// Top-level subcommand selector.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Print the crate version.
    Version,
    /// Database utility commands.
    #[command(subcommand)]
    Db(db::DbCommand),
    /// Migration commands.
    #[command(subcommand)]
    Migrate(migrate::MigrateCommand),
    /// Schema inspection / management commands.
    #[command(subcommand)]
    Schema(schema::SchemaCommand),
    /// Multi-database orchestration commands.
    #[command(subcommand)]
    Orchestrate(orchestrate::OrchestrateCommand),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_version_command() {
        let cli = Cli::try_parse_from(["surql", "version"]).unwrap();
        assert!(matches!(cli.command, Command::Version));
    }

    #[test]
    fn rejects_unknown_command() {
        let err = Cli::try_parse_from(["surql", "bogus"]).unwrap_err();
        assert_eq!(err.exit_code(), 2);
    }

    #[test]
    fn config_flag_is_accepted_before_subcommand() {
        let cli = Cli::try_parse_from(["surql", "--config", "/tmp/c.toml", "db", "info"]).unwrap();
        assert!(cli.global.config.is_some());
    }
}
