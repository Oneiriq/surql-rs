//! `surql` CLI root.
//!
//! Implements the top-level command tree exposed by the `surql`
//! binary and dispatches to the per-group sub-modules ([`db`],
//! [`migrate`], [`schema`], [`bucket`], [`orchestrate`]).
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
//! Every subcommand accepts `--config <path>` naming the TOML file
//! (or `Cargo.toml` directory) whose `[package.metadata.surql]` table the
//! [`Settings`] loader should read instead of discovering one; see
//! [`GlobalOpts::settings`]. Without the flag the standard layered lookup
//! runs (env, `.env`, `Cargo.toml [package.metadata.surql]`).

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use crate::error::{Result, SurqlError};
use crate::settings::{Settings, SettingsBuilder};

pub mod bucket;
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
    // `try_parse` reads `args_os`, so an argument that is not valid UTF-8
    // becomes a usage error instead of a panic in `std::env::args`.
    let cli = match Cli::try_parse() {
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
        Command::Bucket(cmd) => bucket::run(cmd, global).await,
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
    /// Read settings from the `[package.metadata.surql]` table of this TOML
    /// file (or the `Cargo.toml` in this directory) instead of discovering
    /// one from the current directory. The table must exist.
    #[arg(long = "config", global = true, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Print extra diagnostic information for subcommands that support it.
    #[arg(long = "verbose", short = 'v', global = true)]
    pub verbose: bool,
}

impl GlobalOpts {
    /// Resolve the effective [`Settings`] for this invocation.
    ///
    /// When `--config <path>` is supplied, the settings loader reads the
    /// `[package.metadata.surql]` table of that TOML file (any name; a
    /// directory means the `Cargo.toml` inside it) and the `.env` beside
    /// it, with the usual precedence: `SURQL_*` environment variables still
    /// win over the file. The file must exist, parse, and carry the table;
    /// anything else is an error rather than a silent fall back to the
    /// defaults. Otherwise the loader walks upward from the current
    /// directory as documented on [`Settings::load`].
    ///
    /// # Errors
    ///
    /// Returns [`SurqlError::Io`] for a file that cannot be read,
    /// [`SurqlError::Validation`] for one that does not parse or lacks the
    /// table, and propagates validation errors from [`Settings::load`].
    pub fn settings(&self) -> Result<Settings> {
        let mut builder = SettingsBuilder::default();
        if let Some(path) = &self.config {
            let file = if path.is_dir() {
                path.join("Cargo.toml")
            } else {
                path.clone()
            };
            let dir = match file.parent() {
                Some(dir) if !dir.as_os_str().is_empty() => dir.to_path_buf(),
                _ => PathBuf::from("."),
            };
            builder = builder.config_file(file).cwd(dir);
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
    /// Object-storage bucket + file commands (SurrealDB v3 files).
    #[command(subcommand)]
    Bucket(bucket::BucketCommand),
    /// Multi-database orchestration commands.
    #[command(subcommand)]
    Orchestrate(orchestrate::OrchestrateCommand),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

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

    fn opts(config: &Path) -> GlobalOpts {
        GlobalOpts {
            config: Some(config.to_path_buf()),
            verbose: false,
        }
    }

    const SURQL_CARGO: &str = "[package]\nname = \"demo\"\nversion = \"0.0.0\"\n\n\
        [package.metadata.surql]\napp_name = \"from-config-flag\"\n";

    #[test]
    fn config_flag_reads_the_named_cargo_toml() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("Cargo.toml");
        std::fs::write(&file, SURQL_CARGO).unwrap();
        assert_eq!(opts(&file).settings().unwrap().app_name, "from-config-flag");
        assert_eq!(
            opts(dir.path()).settings().unwrap().app_name,
            "from-config-flag"
        );
    }

    #[test]
    fn config_flag_rejects_a_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let err = opts(&dir.path().join("Cargo.toml")).settings().unwrap_err();
        assert!(err.to_string().contains("cannot read config file"), "{err}");
    }

    #[test]
    fn config_flag_reads_an_arbitrarily_named_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("prod.toml");
        std::fs::write(&file, SURQL_CARGO).unwrap();
        assert_eq!(opts(&file).settings().unwrap().app_name, "from-config-flag");
    }

    #[test]
    fn config_flag_rejects_a_file_without_surql_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("Cargo.toml");
        std::fs::write(&file, "[package]\nname = \"demo\"\n").unwrap();
        let err = opts(&file).settings().unwrap_err();
        assert!(
            err.to_string().contains("[package.metadata.surql]"),
            "{err}"
        );

        std::fs::write(&file, "not = [valid").unwrap();
        let err = opts(&file).settings().unwrap_err();
        assert!(err.to_string().contains("not valid TOML"), "{err}");
    }

    #[test]
    fn parse_bucket_define_command() {
        let cli = Cli::try_parse_from([
            "surql",
            "bucket",
            "define",
            "avatars",
            "--backend",
            "memory",
            "--readonly",
        ])
        .unwrap();
        match cli.command {
            Command::Bucket(bucket::BucketCommand::Define {
                name,
                backend,
                readonly,
                ..
            }) => {
                assert_eq!(name, "avatars");
                assert_eq!(backend, "memory");
                assert!(readonly);
            }
            other => panic!("expected bucket define, got {other:?}"),
        }
    }

    #[test]
    fn parse_bucket_put_command() {
        let cli = Cli::try_parse_from([
            "surql",
            "bucket",
            "put",
            "avatars",
            "alice.png",
            "--text",
            "hi",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Command::Bucket(bucket::BucketCommand::Put { .. })
        ));
    }

    #[test]
    fn bucket_put_text_and_file_conflict() {
        // clap should reject specifying both --text and --file.
        let err = Cli::try_parse_from([
            "surql", "bucket", "put", "b", "k", "--text", "hi", "--file", "x",
        ])
        .unwrap_err();
        assert_eq!(err.exit_code(), 2);
    }
}
