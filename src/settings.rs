//! Application settings loader.
//!
//! Port of `surql/settings.py`. Layered configuration: explicit
//! overrides win over environment variables, which win over a
//! `.env` file, which wins over `Cargo.toml [package.metadata.surql]`.
//!
//! Gated behind the `settings` feature so consumers who only need the
//! core types (and do not want to pull in `dotenvy` / `toml`) keep a
//! lean dependency set.
//!
//! ## Example
//!
//! ```no_run
//! # #[cfg(feature = "settings")]
//! # fn demo() -> surql::error::Result<()> {
//! use surql::settings::Settings;
//!
//! let settings = Settings::load()?;
//! println!("environment: {}", settings.environment);
//! println!("migrations at: {}", settings.migration_path.display());
//! # Ok(()) }
//! ```

use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use crate::connection::config::{ConnectionConfig, ENV_PREFIX};
use crate::error::{Result, SurqlError};

/// Application environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Environment {
    /// Local development.
    #[default]
    Development,
    /// Pre-production staging.
    Staging,
    /// Production deployment.
    Production,
}

impl std::str::FromStr for Environment {
    type Err = SurqlError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "development" | "dev" => Ok(Self::Development),
            "staging" | "stage" => Ok(Self::Staging),
            "production" | "prod" => Ok(Self::Production),
            other => Err(SurqlError::Validation {
                reason: format!("invalid environment {other:?}"),
            }),
        }
    }
}

impl std::fmt::Display for Environment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Development => "development",
            Self::Staging => "staging",
            Self::Production => "production",
        };
        f.write_str(s)
    }
}

/// Logging verbosity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum LogLevel {
    /// Fine-grained debug messages.
    Debug,
    /// Informational messages.
    #[default]
    Info,
    /// Warnings and above.
    Warning,
    /// Errors and above.
    Error,
    /// Critical (fatal) errors only.
    Critical,
}

impl std::str::FromStr for LogLevel {
    type Err = SurqlError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "DEBUG" => Ok(Self::Debug),
            "INFO" => Ok(Self::Info),
            "WARNING" | "WARN" => Ok(Self::Warning),
            "ERROR" => Ok(Self::Error),
            "CRITICAL" | "FATAL" => Ok(Self::Critical),
            other => Err(SurqlError::Validation {
                reason: format!("invalid log level {other:?}"),
            }),
        }
    }
}

/// Top-level application settings.
///
/// Nest a [`ConnectionConfig`] under `database`; use
/// [`Settings::database()`] to access it without cloning.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Settings {
    /// Active environment.
    pub environment: Environment,
    /// Debug mode toggle.
    pub debug: bool,
    /// Logging verbosity.
    pub log_level: LogLevel,
    /// Application name.
    pub app_name: String,
    /// Semantic version string.
    pub version: String,
    /// Location of the migrations directory.
    pub migration_path: PathBuf,
    /// Database connection configuration.
    pub database: ConnectionConfig,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            environment: Environment::default(),
            debug: true,
            log_level: LogLevel::default(),
            app_name: "surql".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            migration_path: PathBuf::from("migrations"),
            database: ConnectionConfig::default(),
        }
    }
}

impl Settings {
    /// Start a builder with default values.
    pub fn builder() -> SettingsBuilder {
        SettingsBuilder::default()
    }

    /// Borrow the nested database configuration.
    pub fn database(&self) -> &ConnectionConfig {
        &self.database
    }

    /// Load settings from layered sources.
    ///
    /// Order (lowest priority first):
    ///
    /// 1. [`Settings::default`]
    /// 2. `Cargo.toml [package.metadata.surql]` (nearest `Cargo.toml`
    ///    walking upward from the current directory).
    /// 3. `.env` file (via `dotenvy`, if present).
    /// 4. `SURQL_*` environment variables.
    ///
    /// Any explicit overrides applied through [`SettingsBuilder`]
    /// before calling [`SettingsBuilder::load`] win over every source.
    pub fn load() -> Result<Self> {
        SettingsBuilder::default().load()
    }
}

/// Builder for [`Settings`] honouring layered configuration sources.
#[derive(Debug, Clone, Default)]
pub struct SettingsBuilder {
    overrides: Overrides,
    cwd: Option<PathBuf>,
    skip_dotenv: bool,
}

#[derive(Debug, Clone, Default)]
struct Overrides {
    environment: Option<Environment>,
    debug: Option<bool>,
    log_level: Option<LogLevel>,
    app_name: Option<String>,
    version: Option<String>,
    migration_path: Option<PathBuf>,
    database: Option<ConnectionConfig>,
}

impl SettingsBuilder {
    /// Override the environment field.
    pub fn environment(mut self, env: Environment) -> Self {
        self.overrides.environment = Some(env);
        self
    }

    /// Override the debug flag.
    pub fn debug(mut self, on: bool) -> Self {
        self.overrides.debug = Some(on);
        self
    }

    /// Override the log level.
    pub fn log_level(mut self, level: LogLevel) -> Self {
        self.overrides.log_level = Some(level);
        self
    }

    /// Override the app name.
    pub fn app_name(mut self, name: impl Into<String>) -> Self {
        self.overrides.app_name = Some(name.into());
        self
    }

    /// Override the version string.
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.overrides.version = Some(version.into());
        self
    }

    /// Override the migration path.
    pub fn migration_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.overrides.migration_path = Some(path.into());
        self
    }

    /// Override the database connection configuration.
    pub fn database(mut self, cfg: ConnectionConfig) -> Self {
        self.overrides.database = Some(cfg);
        self
    }

    /// Set the working directory used for `Cargo.toml` lookup and
    /// `.env` discovery. Primarily used by tests.
    pub fn cwd(mut self, dir: impl Into<PathBuf>) -> Self {
        self.cwd = Some(dir.into());
        self
    }

    /// Disable `.env` loading (useful in tests and sandboxed builds).
    pub fn skip_dotenv(mut self, skip: bool) -> Self {
        self.skip_dotenv = skip;
        self
    }

    /// Run the layered load and apply overrides.
    pub fn load(self) -> Result<Settings> {
        let cwd = self
            .cwd
            .clone()
            .or_else(|| env::current_dir().ok())
            .unwrap_or_else(|| PathBuf::from("."));

        let toml_values = load_cargo_metadata(&cwd)?;
        let dotenv_values = if self.skip_dotenv {
            HashMap::new()
        } else {
            load_dotenv(&cwd)
        };
        let env_values = collect_env();
        let env_lookup = |key: &str| -> Option<String> {
            env_values
                .get(key)
                .cloned()
                .or_else(|| dotenv_values.get(key).cloned())
        };

        // Build the nested connection config honouring the same layered
        // sources. Cargo metadata may provide a [database] subtable.
        let database = build_connection_config(
            self.overrides.database.clone(),
            &env_lookup,
            toml_values.database.as_ref(),
        )?;

        let mut settings = Settings {
            database,
            ..Settings::default()
        };

        // Apply TOML values first (lowest non-default precedence).
        if let Some(env) = &toml_values.environment {
            settings.environment = env.parse()?;
        }
        if let Some(debug) = toml_values.debug {
            settings.debug = debug;
        }
        if let Some(level) = &toml_values.log_level {
            settings.log_level = level.parse()?;
        }
        if let Some(name) = toml_values.app_name.clone() {
            settings.app_name = name;
        }
        if let Some(version) = toml_values.version.clone() {
            settings.version = version;
        }
        if let Some(path) = toml_values.migration_path.clone() {
            settings.migration_path = PathBuf::from(path);
        }

        // .env + env vars (.env already folded into env_lookup above).
        if let Some(raw) = env_lookup("SURQL_ENVIRONMENT") {
            settings.environment = raw.parse()?;
        }
        if let Some(raw) = env_lookup("SURQL_DEBUG") {
            settings.debug = parse_bool(&raw)?;
        }
        if let Some(raw) = env_lookup("SURQL_LOG_LEVEL") {
            settings.log_level = raw.parse()?;
        }
        if let Some(raw) = env_lookup("SURQL_APP_NAME") {
            settings.app_name = raw;
        }
        if let Some(raw) = env_lookup("SURQL_VERSION") {
            settings.version = raw;
        }
        if let Some(raw) = env_lookup("SURQL_MIGRATION_PATH") {
            settings.migration_path = PathBuf::from(raw);
        }

        // Explicit overrides (highest precedence).
        if let Some(env) = self.overrides.environment {
            settings.environment = env;
        }
        if let Some(d) = self.overrides.debug {
            settings.debug = d;
        }
        if let Some(level) = self.overrides.log_level {
            settings.log_level = level;
        }
        if let Some(name) = self.overrides.app_name {
            settings.app_name = name;
        }
        if let Some(v) = self.overrides.version {
            settings.version = v;
        }
        if let Some(path) = self.overrides.migration_path {
            settings.migration_path = path;
        }

        Ok(settings)
    }
}

/// Global cached settings.
static SETTINGS: OnceLock<Settings> = OnceLock::new();

/// Return a lazily-loaded global [`Settings`] instance.
///
/// Uses default sources. Called with [`Settings::load`] on first
/// access; subsequent calls reuse the cached value. If loading fails
/// the default [`Settings`] is returned and the first error surfaces
/// via the crate's logging layer (eventually); callers that need the
/// hard failure behaviour should use [`Settings::load`] directly.
pub fn get_settings() -> &'static Settings {
    SETTINGS.get_or_init(|| Settings::load().unwrap_or_default())
}

/// Return the cached database configuration.
pub fn get_db_config() -> &'static ConnectionConfig {
    &get_settings().database
}

/// Return the migration path from the cached settings.
pub fn get_migration_path() -> &'static Path {
    &get_settings().migration_path
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Deserialize)]
struct CargoMetadataSection {
    #[serde(default)]
    environment: Option<String>,
    #[serde(default)]
    debug: Option<bool>,
    #[serde(default)]
    log_level: Option<String>,
    #[serde(default)]
    app_name: Option<String>,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    migration_path: Option<String>,
    #[serde(default)]
    database: Option<DatabaseTable>,
}

#[derive(Debug, Default, Deserialize)]
struct DatabaseTable {
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    namespace: Option<String>,
    #[serde(default)]
    database: Option<String>,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    timeout: Option<f64>,
    #[serde(default)]
    max_connections: Option<u32>,
    #[serde(default)]
    retry_max_attempts: Option<u32>,
    #[serde(default)]
    retry_min_wait: Option<f64>,
    #[serde(default)]
    retry_max_wait: Option<f64>,
    #[serde(default)]
    retry_multiplier: Option<f64>,
    #[serde(default)]
    enable_live_queries: Option<bool>,
}

fn load_cargo_metadata(start: &Path) -> Result<CargoMetadataSection> {
    let Some(cargo_path) = find_cargo_toml(start) else {
        return Ok(CargoMetadataSection::default());
    };
    let Ok(raw) = std::fs::read_to_string(&cargo_path) else {
        return Ok(CargoMetadataSection::default());
    };
    let parsed: toml::Value = match raw.parse() {
        Ok(v) => v,
        Err(_) => return Ok(CargoMetadataSection::default()),
    };
    let metadata = parsed
        .get("package")
        .and_then(|p| p.get("metadata"))
        .and_then(|m| m.get("surql"));
    let Some(value) = metadata else {
        return Ok(CargoMetadataSection::default());
    };
    let section: CargoMetadataSection =
        value
            .clone()
            .try_into()
            .map_err(|e| SurqlError::Validation {
                reason: format!("invalid [package.metadata.surql]: {e}"),
            })?;
    Ok(section)
}

fn find_cargo_toml(start: &Path) -> Option<PathBuf> {
    let mut current = start.to_path_buf();
    loop {
        let candidate = current.join("Cargo.toml");
        if candidate.is_file() {
            return Some(candidate);
        }
        if !current.pop() {
            return None;
        }
    }
}

fn load_dotenv(cwd: &Path) -> HashMap<String, String> {
    let path = cwd.join(".env");
    if !path.is_file() {
        return HashMap::new();
    }
    match dotenvy::from_path_iter(&path) {
        Ok(iter) => iter.flatten().collect(),
        Err(_) => HashMap::new(),
    }
}

fn collect_env() -> HashMap<String, String> {
    env::vars()
        .filter(|(k, _)| k.starts_with(ENV_PREFIX) || k.eq_ignore_ascii_case("SURQL_DEBUG"))
        .collect()
}

fn parse_bool(raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(SurqlError::Validation {
            reason: format!("invalid boolean {other:?}"),
        }),
    }
}

fn build_connection_config(
    explicit: Option<ConnectionConfig>,
    env_lookup: &impl Fn(&str) -> Option<String>,
    toml_db: Option<&DatabaseTable>,
) -> Result<ConnectionConfig> {
    if let Some(cfg) = explicit {
        cfg.validate()?;
        return Ok(cfg);
    }

    // Base: defaults overridden by Cargo.toml metadata.
    let mut cfg = ConnectionConfig::default();
    if let Some(db) = toml_db {
        if let Some(v) = &db.url {
            cfg.db_url.clone_from(v);
        }
        if let Some(v) = &db.namespace {
            cfg.db_ns.clone_from(v);
        }
        if let Some(v) = &db.database {
            cfg.db.clone_from(v);
        }
        if let Some(v) = &db.username {
            cfg.db_user = Some(v.clone());
        }
        if let Some(v) = &db.password {
            cfg.db_pass = Some(v.clone());
        }
        if let Some(v) = db.timeout {
            cfg.db_timeout = v;
        }
        if let Some(v) = db.max_connections {
            cfg.db_max_connections = v;
        }
        if let Some(v) = db.retry_max_attempts {
            cfg.db_retry_max_attempts = v;
        }
        if let Some(v) = db.retry_min_wait {
            cfg.db_retry_min_wait = v;
        }
        if let Some(v) = db.retry_max_wait {
            cfg.db_retry_max_wait = v;
        }
        if let Some(v) = db.retry_multiplier {
            cfg.db_retry_multiplier = v;
        }
        if let Some(v) = db.enable_live_queries {
            cfg.enable_live_queries = v;
        }
    }

    // Layer env/.env on top (via ConnectionConfig::from_source_with_prefix).
    let env_cfg = ConnectionConfig::from_source_with_prefix(ENV_PREFIX, env_lookup);
    match env_cfg {
        Ok(env_cfg) => {
            // `from_source_with_prefix` returns a fully-populated config
            // built from defaults+env. Only copy over fields that
            // actually had an env-supplied value; we re-query the lookup
            // to decide.
            apply_env_overrides(&mut cfg, env_lookup);
            // Validate once at the end, tolerating env-only configs.
            drop(env_cfg);
        }
        Err(_) => {
            // Ignore env errors: callers that need strict env validation
            // should call ConnectionConfig::from_env directly.
            apply_env_overrides(&mut cfg, env_lookup);
        }
    }

    cfg.validate()?;
    Ok(cfg)
}

fn apply_env_overrides(cfg: &mut ConnectionConfig, lookup: &impl Fn(&str) -> Option<String>) {
    if let Some(v) = first_env(lookup, &["SURQL_URL", "SURQL_DB_URL"]) {
        cfg.db_url = v;
    }
    if let Some(v) = first_env(lookup, &["SURQL_NAMESPACE", "SURQL_DB_NS"]) {
        cfg.db_ns = v;
    }
    if let Some(v) = first_env(lookup, &["SURQL_DATABASE", "SURQL_DB"]) {
        cfg.db = v;
    }
    if let Some(v) = first_env(lookup, &["SURQL_USERNAME", "SURQL_DB_USER"]) {
        cfg.db_user = Some(v);
    }
    if let Some(v) = first_env(lookup, &["SURQL_PASSWORD", "SURQL_DB_PASS"]) {
        cfg.db_pass = Some(v);
    }
    if let Some(v) = first_env(lookup, &["SURQL_TIMEOUT", "SURQL_DB_TIMEOUT"]) {
        if let Ok(parsed) = v.parse::<f64>() {
            cfg.db_timeout = parsed;
        }
    }
    if let Some(v) = first_env(
        lookup,
        &["SURQL_MAX_CONNECTIONS", "SURQL_DB_MAX_CONNECTIONS"],
    ) {
        if let Ok(parsed) = v.parse::<u32>() {
            cfg.db_max_connections = parsed;
        }
    }
    if let Some(v) = first_env(
        lookup,
        &["SURQL_RETRY_MAX_ATTEMPTS", "SURQL_DB_RETRY_MAX_ATTEMPTS"],
    ) {
        if let Ok(parsed) = v.parse::<u32>() {
            cfg.db_retry_max_attempts = parsed;
        }
    }
    if let Some(v) = first_env(lookup, &["SURQL_RETRY_MIN_WAIT", "SURQL_DB_RETRY_MIN_WAIT"]) {
        if let Ok(parsed) = v.parse::<f64>() {
            cfg.db_retry_min_wait = parsed;
        }
    }
    if let Some(v) = first_env(lookup, &["SURQL_RETRY_MAX_WAIT", "SURQL_DB_RETRY_MAX_WAIT"]) {
        if let Ok(parsed) = v.parse::<f64>() {
            cfg.db_retry_max_wait = parsed;
        }
    }
    if let Some(v) = first_env(
        lookup,
        &["SURQL_RETRY_MULTIPLIER", "SURQL_DB_RETRY_MULTIPLIER"],
    ) {
        if let Ok(parsed) = v.parse::<f64>() {
            cfg.db_retry_multiplier = parsed;
        }
    }
    if let Some(v) = first_env(lookup, &["SURQL_ENABLE_LIVE_QUERIES"]) {
        if let Ok(parsed) = parse_bool(&v) {
            cfg.enable_live_queries = parsed;
        }
    }
}

fn first_env(lookup: &impl Fn(&str) -> Option<String>, keys: &[&str]) -> Option<String> {
    for k in keys {
        if let Some(v) = lookup(k) {
            return Some(v);
        }
        let lower = k.to_ascii_lowercase();
        if let Some(v) = lookup(&lower) {
            return Some(v);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn write_cargo(dir: &Path, body: &str) {
        fs::write(dir.join("Cargo.toml"), body).unwrap();
    }

    #[test]
    fn defaults_are_sensible() {
        let s = Settings::default();
        assert_eq!(s.environment, Environment::Development);
        assert!(s.debug);
        assert_eq!(s.log_level, LogLevel::Info);
        assert_eq!(s.app_name, "surql");
        assert_eq!(s.migration_path, PathBuf::from("migrations"));
    }

    #[test]
    fn cargo_metadata_overrides_defaults() {
        let dir = TempDir::new().unwrap();
        write_cargo(
            dir.path(),
            r#"
[package]
name = "demo"
version = "0.0.0"

[package.metadata.surql]
environment = "production"
debug = false
log_level = "WARNING"
app_name = "demo-app"
migration_path = "db/migrations"

[package.metadata.surql.database]
url = "ws://db:8000/rpc"
namespace = "prod"
database = "core"
"#,
        );
        let settings = SettingsBuilder::default()
            .cwd(dir.path())
            .skip_dotenv(true)
            .load()
            .unwrap();
        assert_eq!(settings.environment, Environment::Production);
        assert!(!settings.debug);
        assert_eq!(settings.log_level, LogLevel::Warning);
        assert_eq!(settings.app_name, "demo-app");
        assert_eq!(settings.migration_path, PathBuf::from("db/migrations"));
        assert_eq!(settings.database.url(), "ws://db:8000/rpc");
        assert_eq!(settings.database.namespace(), "prod");
        assert_eq!(settings.database.database(), "core");
    }

    #[test]
    fn explicit_overrides_win() {
        let dir = TempDir::new().unwrap();
        write_cargo(
            dir.path(),
            r#"
[package]
name = "demo"
version = "0.0.0"

[package.metadata.surql]
environment = "production"
"#,
        );
        let settings = SettingsBuilder::default()
            .cwd(dir.path())
            .skip_dotenv(true)
            .environment(Environment::Development)
            .app_name("override")
            .load()
            .unwrap();
        assert_eq!(settings.environment, Environment::Development);
        assert_eq!(settings.app_name, "override");
    }

    #[test]
    fn dotenv_file_is_honoured() {
        let dir = TempDir::new().unwrap();
        write_cargo(
            dir.path(),
            r#"
[package]
name = "demo"
version = "0.0.0"
"#,
        );
        fs::write(
            dir.path().join(".env"),
            "SURQL_APP_NAME=dotenv-name\nSURQL_LOG_LEVEL=DEBUG\n",
        )
        .unwrap();
        let settings = SettingsBuilder::default().cwd(dir.path()).load().unwrap();
        assert_eq!(settings.app_name, "dotenv-name");
        assert_eq!(settings.log_level, LogLevel::Debug);
    }

    #[test]
    fn database_nested_override() {
        let cfg = ConnectionConfig::builder()
            .url("ws://explicit/rpc")
            .namespace("ns")
            .database("db")
            .build()
            .unwrap();
        let settings = SettingsBuilder::default()
            .skip_dotenv(true)
            .database(cfg.clone())
            .load()
            .unwrap();
        assert_eq!(settings.database, cfg);
    }

    #[test]
    fn environment_from_str_cases() {
        assert_eq!(
            "DEV".parse::<Environment>().unwrap(),
            Environment::Development
        );
        assert_eq!(
            "Production".parse::<Environment>().unwrap(),
            Environment::Production
        );
        assert!("nope".parse::<Environment>().is_err());
    }

    #[test]
    fn log_level_from_str_cases() {
        assert_eq!("warn".parse::<LogLevel>().unwrap(), LogLevel::Warning);
        assert_eq!("INFO".parse::<LogLevel>().unwrap(), LogLevel::Info);
        assert!("loud".parse::<LogLevel>().is_err());
    }

    #[test]
    fn parse_bool_covers_common_forms() {
        assert!(parse_bool("yes").unwrap());
        assert!(!parse_bool("OFF").unwrap());
        assert!(parse_bool("maybe").is_err());
    }
}
