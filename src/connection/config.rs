//! Database connection configuration.
//!
//! Port of `surql/connection/config.py`. Covers URL validation (remote
//! WebSocket/HTTP and embedded engines), namespace/database identifier
//! checks, timeout/retry defaults, and live-query gating.

use std::collections::HashMap;
use std::env;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::{Result, SurqlError};

/// Environment variable prefix used by [`ConnectionConfig::from_env`].
pub const ENV_PREFIX: &str = "SURQL_";

/// Protocol implied by the configured URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Protocol {
    /// `ws://`
    WebSocket,
    /// `wss://`
    WebSocketSecure,
    /// `http://`
    Http,
    /// `https://`
    Https,
    /// `mem://` or `memory://`
    Memory,
    /// `file://`
    File,
    /// `surrealkv://`
    SurrealKv,
}

impl Protocol {
    /// Return `true` when the protocol supports live queries (WebSocket or embedded).
    pub fn supports_live_queries(self) -> bool {
        !matches!(self, Self::Http | Self::Https)
    }

    /// Return `true` when the protocol runs in-process (no remote server).
    pub fn is_embedded(self) -> bool {
        matches!(self, Self::Memory | Self::File | Self::SurrealKv)
    }
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::WebSocket => "ws",
            Self::WebSocketSecure => "wss",
            Self::Http => "http",
            Self::Https => "https",
            Self::Memory => "memory",
            Self::File => "file",
            Self::SurrealKv => "surrealkv",
        };
        f.write_str(s)
    }
}

/// Database connection configuration.
///
/// Field names follow the Python port (`db_url`, `db_ns`, `db`, ...) for
/// wire-compatibility with env-var loading; convenient getters are
/// provided for the shorter aliases (`url`, `namespace`, `database`, ...).
///
/// ## Examples
///
/// ```
/// use surql::connection::ConnectionConfig;
///
/// let cfg = ConnectionConfig::builder()
///     .url("ws://localhost:8000/rpc")
///     .namespace("prod")
///     .database("app")
///     .build()
///     .unwrap();
/// assert_eq!(cfg.url(), "ws://localhost:8000/rpc");
/// assert_eq!(cfg.namespace(), "prod");
/// assert_eq!(cfg.database(), "app");
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectionConfig {
    /// SurrealDB connection URL. Aliases: `url`.
    pub db_url: String,
    /// Database namespace. Aliases: `namespace`.
    pub db_ns: String,
    /// Database name. Aliases: `database`.
    pub db: String,
    /// Authentication username. Aliases: `username`.
    pub db_user: Option<String>,
    /// Authentication password. Aliases: `password`.
    pub db_pass: Option<String>,
    /// Connection timeout in seconds. Aliases: `timeout`.
    pub db_timeout: f64,
    /// Maximum number of concurrent connections. Aliases: `max_connections`.
    pub db_max_connections: u32,
    /// Maximum retry attempts. Aliases: `retry_max_attempts`.
    pub db_retry_max_attempts: u32,
    /// Minimum retry wait time in seconds. Aliases: `retry_min_wait`.
    pub db_retry_min_wait: f64,
    /// Maximum retry wait time in seconds. Aliases: `retry_max_wait`.
    pub db_retry_max_wait: f64,
    /// Exponential backoff multiplier. Aliases: `retry_multiplier`.
    pub db_retry_multiplier: f64,
    /// Enable live query support. Requires WebSocket or embedded URL.
    pub enable_live_queries: bool,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            db_url: "ws://localhost:8000/rpc".into(),
            db_ns: "development".into(),
            db: "main".into(),
            db_user: None,
            db_pass: None,
            db_timeout: 30.0,
            db_max_connections: 10,
            db_retry_max_attempts: 3,
            db_retry_min_wait: 1.0,
            db_retry_max_wait: 10.0,
            db_retry_multiplier: 2.0,
            enable_live_queries: true,
        }
    }
}

impl ConnectionConfig {
    /// Start a builder with the default field values.
    pub fn builder() -> ConnectionConfigBuilder {
        ConnectionConfigBuilder::default()
    }

    /// Validate the configuration values according to the Python port's rules.
    pub fn validate(&self) -> Result<()> {
        validate_url(&self.db_url)?;
        validate_identifier(&self.db_ns, "namespace")?;
        validate_identifier(&self.db, "database")?;
        validate_numeric_range("timeout", self.db_timeout, 1.0, f64::INFINITY)?;
        validate_numeric_range(
            "max_connections",
            f64::from(self.db_max_connections),
            1.0,
            100.0,
        )?;
        validate_numeric_range(
            "retry_max_attempts",
            f64::from(self.db_retry_max_attempts),
            1.0,
            10.0,
        )?;
        validate_numeric_range("retry_min_wait", self.db_retry_min_wait, 0.1, f64::INFINITY)?;
        validate_numeric_range("retry_max_wait", self.db_retry_max_wait, 1.0, f64::INFINITY)?;
        validate_numeric_range(
            "retry_multiplier",
            self.db_retry_multiplier,
            1.0,
            f64::INFINITY,
        )?;
        if self.db_retry_max_wait <= self.db_retry_min_wait {
            return Err(SurqlError::Validation {
                reason: "db_retry_max_wait must be greater than db_retry_min_wait".into(),
            });
        }
        let proto = Self::detect_protocol(&self.db_url)?;
        if self.enable_live_queries && !proto.supports_live_queries() {
            return Err(SurqlError::Validation {
                reason: "Live queries require WebSocket (ws://, wss://) or embedded \
                     (mem://, memory://, file://, surrealkv://) connection"
                    .into(),
            });
        }
        Ok(())
    }

    /// Infer the [`Protocol`] used by this configuration's URL.
    pub fn protocol(&self) -> Result<Protocol> {
        Self::detect_protocol(&self.db_url)
    }

    fn detect_protocol(url: &str) -> Result<Protocol> {
        let trimmed = url.trim();
        if let Some(rest) = trimmed.strip_prefix("ws://") {
            if rest.is_empty() {
                return Err(SurqlError::Validation {
                    reason: "URL host must not be empty".into(),
                });
            }
            return Ok(Protocol::WebSocket);
        }
        if trimmed.starts_with("wss://") {
            return Ok(Protocol::WebSocketSecure);
        }
        if trimmed.starts_with("http://") {
            return Ok(Protocol::Http);
        }
        if trimmed.starts_with("https://") {
            return Ok(Protocol::Https);
        }
        if trimmed.starts_with("mem://") || trimmed.starts_with("memory://") {
            return Ok(Protocol::Memory);
        }
        if trimmed.starts_with("file://") {
            return Ok(Protocol::File);
        }
        if trimmed.starts_with("surrealkv://") {
            return Ok(Protocol::SurrealKv);
        }
        Err(SurqlError::Validation {
            reason: "URL must use one of: ws://, wss://, http://, https://, \
                 mem://, memory://, file://, surrealkv://"
                .into(),
        })
    }

    /// Load configuration from environment variables prefixed `SURQL_`.
    ///
    /// Recognised variables (case-insensitive): `SURQL_URL`,
    /// `SURQL_NAMESPACE`, `SURQL_DATABASE`, `SURQL_USERNAME`,
    /// `SURQL_PASSWORD`, `SURQL_TIMEOUT`, `SURQL_MAX_CONNECTIONS`,
    /// `SURQL_RETRY_MAX_ATTEMPTS`, `SURQL_RETRY_MIN_WAIT`,
    /// `SURQL_RETRY_MAX_WAIT`, `SURQL_RETRY_MULTIPLIER`,
    /// `SURQL_ENABLE_LIVE_QUERIES`.
    ///
    /// Missing values fall back to [`ConnectionConfig::default`]; on
    /// success, the built config is validated before return.
    pub fn from_env() -> Result<Self> {
        Self::from_env_with_prefix(ENV_PREFIX)
    }

    /// Load configuration from environment variables with a custom prefix
    /// (e.g. `SURQL_PRIMARY_` for named connections).
    pub fn from_env_with_prefix(prefix: &str) -> Result<Self> {
        let lookup = |key: &str| env::var(key).ok();
        Self::from_source_with_prefix(prefix, lookup)
    }

    /// Build a config from an arbitrary key lookup (used by [`Self::from_env`]
    /// and by tests to avoid process-wide env mutation).
    ///
    /// `lookup` is called with the fully-qualified variable name (case
    /// preserved). Missing values fall back to defaults.
    pub fn from_source_with_prefix<F>(prefix: &str, mut lookup: F) -> Result<Self>
    where
        F: FnMut(&str) -> Option<String>,
    {
        let mut cfg = Self::default();
        let p = prefix;

        if let Some(v) = lookup_with_aliases(&mut lookup, p, &["URL", "DB_URL"]) {
            cfg.db_url = v;
        }
        if let Some(v) = lookup_with_aliases(&mut lookup, p, &["NAMESPACE", "DB_NS"]) {
            cfg.db_ns = v;
        }
        if let Some(v) = lookup_with_aliases(&mut lookup, p, &["DATABASE", "DB"]) {
            cfg.db = v;
        }
        if let Some(v) = lookup_with_aliases(&mut lookup, p, &["USERNAME", "DB_USER"]) {
            cfg.db_user = Some(v);
        }
        if let Some(v) = lookup_with_aliases(&mut lookup, p, &["PASSWORD", "DB_PASS"]) {
            cfg.db_pass = Some(v);
        }
        if let Some(v) = lookup_with_aliases(&mut lookup, p, &["TIMEOUT", "DB_TIMEOUT"]) {
            cfg.db_timeout = parse_env("timeout", &v)?;
        }
        if let Some(v) =
            lookup_with_aliases(&mut lookup, p, &["MAX_CONNECTIONS", "DB_MAX_CONNECTIONS"])
        {
            cfg.db_max_connections = parse_env("max_connections", &v)?;
        }
        if let Some(v) = lookup_with_aliases(
            &mut lookup,
            p,
            &["RETRY_MAX_ATTEMPTS", "DB_RETRY_MAX_ATTEMPTS"],
        ) {
            cfg.db_retry_max_attempts = parse_env("retry_max_attempts", &v)?;
        }
        if let Some(v) =
            lookup_with_aliases(&mut lookup, p, &["RETRY_MIN_WAIT", "DB_RETRY_MIN_WAIT"])
        {
            cfg.db_retry_min_wait = parse_env("retry_min_wait", &v)?;
        }
        if let Some(v) =
            lookup_with_aliases(&mut lookup, p, &["RETRY_MAX_WAIT", "DB_RETRY_MAX_WAIT"])
        {
            cfg.db_retry_max_wait = parse_env("retry_max_wait", &v)?;
        }
        if let Some(v) =
            lookup_with_aliases(&mut lookup, p, &["RETRY_MULTIPLIER", "DB_RETRY_MULTIPLIER"])
        {
            cfg.db_retry_multiplier = parse_env("retry_multiplier", &v)?;
        }
        if let Some(v) = lookup_with_aliases(&mut lookup, p, &["ENABLE_LIVE_QUERIES"]) {
            cfg.enable_live_queries = parse_bool(&v)?;
        }

        cfg.validate()?;
        Ok(cfg)
    }

    /// Convenience: build from a pre-populated map (useful in tests).
    pub fn from_map_with_prefix(prefix: &str, map: &HashMap<String, String>) -> Result<Self> {
        Self::from_source_with_prefix(prefix, |k| map.get(k).cloned())
    }

    /// Alias for [`Self::db_url`].
    pub fn url(&self) -> &str {
        &self.db_url
    }

    /// Alias for [`Self::db_ns`].
    pub fn namespace(&self) -> &str {
        &self.db_ns
    }

    /// Alias for [`Self::db`].
    pub fn database(&self) -> &str {
        &self.db
    }

    /// Alias for [`Self::db_user`].
    pub fn username(&self) -> Option<&str> {
        self.db_user.as_deref()
    }

    /// Alias for [`Self::db_pass`].
    pub fn password(&self) -> Option<&str> {
        self.db_pass.as_deref()
    }

    /// Alias for [`Self::db_timeout`].
    pub fn timeout(&self) -> f64 {
        self.db_timeout
    }

    /// Alias for [`Self::db_max_connections`].
    pub fn max_connections(&self) -> u32 {
        self.db_max_connections
    }

    /// Alias for [`Self::db_retry_max_attempts`].
    pub fn retry_max_attempts(&self) -> u32 {
        self.db_retry_max_attempts
    }

    /// Alias for [`Self::db_retry_min_wait`].
    pub fn retry_min_wait(&self) -> f64 {
        self.db_retry_min_wait
    }

    /// Alias for [`Self::db_retry_max_wait`].
    pub fn retry_max_wait(&self) -> f64 {
        self.db_retry_max_wait
    }

    /// Alias for [`Self::db_retry_multiplier`].
    pub fn retry_multiplier(&self) -> f64 {
        self.db_retry_multiplier
    }
}

/// Builder for [`ConnectionConfig`].
#[derive(Debug, Clone, Default)]
pub struct ConnectionConfigBuilder {
    inner: Option<ConnectionConfig>,
}

macro_rules! setter {
    ($(#[$meta:meta])* $name:ident, $ty:ty, $field:ident) => {
        $(#[$meta])*
        pub fn $name(mut self, v: impl Into<$ty>) -> Self {
            let mut inner = self.inner.unwrap_or_default();
            inner.$field = v.into();
            self.inner = Some(inner);
            self
        }
    };
}

impl ConnectionConfigBuilder {
    setter!(
        /// Set the connection URL.
        url,
        String,
        db_url
    );
    setter!(
        /// Set the namespace.
        namespace,
        String,
        db_ns
    );
    setter!(
        /// Set the database name.
        database,
        String,
        db
    );

    /// Set authentication username.
    pub fn username(mut self, v: impl Into<String>) -> Self {
        let mut inner = self.inner.unwrap_or_default();
        inner.db_user = Some(v.into());
        self.inner = Some(inner);
        self
    }

    /// Set authentication password.
    pub fn password(mut self, v: impl Into<String>) -> Self {
        let mut inner = self.inner.unwrap_or_default();
        inner.db_pass = Some(v.into());
        self.inner = Some(inner);
        self
    }

    /// Set connection timeout in seconds.
    pub fn timeout(mut self, secs: f64) -> Self {
        let mut inner = self.inner.unwrap_or_default();
        inner.db_timeout = secs;
        self.inner = Some(inner);
        self
    }

    /// Set the maximum connection pool size.
    pub fn max_connections(mut self, n: u32) -> Self {
        let mut inner = self.inner.unwrap_or_default();
        inner.db_max_connections = n;
        self.inner = Some(inner);
        self
    }

    /// Set the maximum retry attempts.
    pub fn retry_max_attempts(mut self, n: u32) -> Self {
        let mut inner = self.inner.unwrap_or_default();
        inner.db_retry_max_attempts = n;
        self.inner = Some(inner);
        self
    }

    /// Set the minimum retry wait time in seconds.
    pub fn retry_min_wait(mut self, secs: f64) -> Self {
        let mut inner = self.inner.unwrap_or_default();
        inner.db_retry_min_wait = secs;
        self.inner = Some(inner);
        self
    }

    /// Set the maximum retry wait time in seconds.
    pub fn retry_max_wait(mut self, secs: f64) -> Self {
        let mut inner = self.inner.unwrap_or_default();
        inner.db_retry_max_wait = secs;
        self.inner = Some(inner);
        self
    }

    /// Set the retry backoff multiplier.
    pub fn retry_multiplier(mut self, m: f64) -> Self {
        let mut inner = self.inner.unwrap_or_default();
        inner.db_retry_multiplier = m;
        self.inner = Some(inner);
        self
    }

    /// Enable or disable live queries.
    pub fn enable_live_queries(mut self, on: bool) -> Self {
        let mut inner = self.inner.unwrap_or_default();
        inner.enable_live_queries = on;
        self.inner = Some(inner);
        self
    }

    /// Finalise the builder and validate the result.
    pub fn build(self) -> Result<ConnectionConfig> {
        let cfg = self.inner.unwrap_or_default();
        cfg.validate()?;
        Ok(cfg)
    }
}

/// A named [`ConnectionConfig`] for managing multiple databases.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NamedConnectionConfig {
    /// Connection name (e.g. `primary`, `replica`).
    pub name: String,
    /// Underlying connection configuration.
    pub config: ConnectionConfig,
}

impl NamedConnectionConfig {
    /// Load a named connection from environment variables using the
    /// prefix `SURQL_<NAME>_` (e.g. `SURQL_PRIMARY_URL`).
    pub fn from_env(name: &str) -> Result<Self> {
        let prefix = format!("{ENV_PREFIX}{}_", name.to_uppercase());
        let config = ConnectionConfig::from_env_with_prefix(&prefix)?;
        Ok(Self {
            name: name.to_lowercase(),
            config,
        })
    }

    /// Test-friendly variant: same as [`Self::from_env`] but with a custom lookup.
    pub fn from_source<F>(name: &str, lookup: F) -> Result<Self>
    where
        F: FnMut(&str) -> Option<String>,
    {
        let prefix = format!("{ENV_PREFIX}{}_", name.to_uppercase());
        let config = ConnectionConfig::from_source_with_prefix(&prefix, lookup)?;
        Ok(Self {
            name: name.to_lowercase(),
            config,
        })
    }
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

fn validate_url(url: &str) -> Result<()> {
    if url.is_empty() {
        return Err(SurqlError::Validation {
            reason: "URL cannot be empty".into(),
        });
    }
    let _ = ConnectionConfig::detect_protocol(url)?;
    Ok(())
}

fn validate_identifier(value: &str, context: &str) -> Result<()> {
    if value.is_empty() {
        return Err(SurqlError::Validation {
            reason: "Identifier cannot be empty".into(),
        });
    }
    let ok = value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    if !ok {
        return Err(SurqlError::Validation {
            reason: format!(
                "Identifier ({context}) must be alphanumeric with optional underscores/hyphens"
            ),
        });
    }
    Ok(())
}

fn validate_numeric_range(name: &str, value: f64, min: f64, max: f64) -> Result<()> {
    if value.is_nan() {
        return Err(SurqlError::Validation {
            reason: format!("{name} must be a finite number"),
        });
    }
    if value < min {
        return Err(SurqlError::Validation {
            reason: format!("{name} must be >= {min}"),
        });
    }
    if value > max {
        return Err(SurqlError::Validation {
            reason: format!("{name} must be <= {max}"),
        });
    }
    Ok(())
}

fn lookup_with_aliases<F>(lookup: &mut F, prefix: &str, keys: &[&str]) -> Option<String>
where
    F: FnMut(&str) -> Option<String>,
{
    for k in keys {
        let name = format!("{prefix}{k}");
        if let Some(v) = lookup(&name) {
            return Some(v);
        }
        let lower = name.to_lowercase();
        if let Some(v) = lookup(&lower) {
            return Some(v);
        }
    }
    None
}

fn parse_env<T: std::str::FromStr>(name: &str, raw: &str) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    raw.parse::<T>().map_err(|e| SurqlError::Validation {
        reason: format!("invalid {name}={raw:?}: {e}"),
    })
}

fn parse_bool(raw: &str) -> Result<bool> {
    match raw.trim().to_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(SurqlError::Validation {
            reason: format!("invalid boolean value {other:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        let cfg = ConnectionConfig::default();
        cfg.validate().unwrap();
        assert_eq!(cfg.url(), "ws://localhost:8000/rpc");
        assert_eq!(cfg.namespace(), "development");
        assert_eq!(cfg.database(), "main");
        assert!(cfg.enable_live_queries);
    }

    #[test]
    fn builder_overrides_fields() {
        let cfg = ConnectionConfig::builder()
            .url("wss://db.example.com/rpc")
            .namespace("prod")
            .database("app")
            .username("alice")
            .password("hunter2")
            .timeout(60.0)
            .build()
            .unwrap();
        assert_eq!(cfg.url(), "wss://db.example.com/rpc");
        assert_eq!(cfg.username(), Some("alice"));
        assert_eq!(cfg.password(), Some("hunter2"));
        assert!((cfg.timeout() - 60.0).abs() < f64::EPSILON);
    }

    #[test]
    fn rejects_empty_url() {
        let cfg = ConnectionConfig {
            db_url: String::new(),
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_unsupported_protocol() {
        let cfg = ConnectionConfig {
            db_url: "ftp://localhost".into(),
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn accepts_embedded_protocols() {
        for url in &[
            "mem://",
            "memory://",
            "file:///tmp/db.sdb",
            "surrealkv:///tmp/db.skv",
        ] {
            let cfg = ConnectionConfig {
                db_url: (*url).into(),
                ..Default::default()
            };
            cfg.validate().unwrap();
        }
    }

    #[test]
    fn rejects_live_queries_over_http() {
        let cfg = ConnectionConfig {
            db_url: "https://db.example.com/rpc".into(),
            enable_live_queries: true,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());

        let cfg_ok = ConnectionConfig {
            db_url: "https://db.example.com/rpc".into(),
            enable_live_queries: false,
            ..Default::default()
        };
        cfg_ok.validate().unwrap();
    }

    #[test]
    fn rejects_invalid_identifiers() {
        for bad in ["", "has space", "has/slash", "has!bang"] {
            let cfg = ConnectionConfig {
                db_ns: bad.into(),
                ..Default::default()
            };
            assert!(cfg.validate().is_err(), "ns {bad:?} should be invalid");
        }
    }

    #[test]
    fn retry_max_must_exceed_min() {
        let cfg = ConnectionConfig {
            db_retry_min_wait: 5.0,
            db_retry_max_wait: 3.0,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn protocol_detection() {
        let cases = [
            ("ws://localhost:8000", Protocol::WebSocket),
            ("wss://host/rpc", Protocol::WebSocketSecure),
            ("http://host", Protocol::Http),
            ("https://host", Protocol::Https),
            ("mem://", Protocol::Memory),
            ("memory://", Protocol::Memory),
            ("file:///tmp/db", Protocol::File),
            ("surrealkv:///tmp/db", Protocol::SurrealKv),
        ];
        for (url, proto) in cases {
            let cfg = ConnectionConfig {
                db_url: url.into(),
                enable_live_queries: proto.supports_live_queries(),
                ..Default::default()
            };
            cfg.validate().unwrap();
            assert_eq!(cfg.protocol().unwrap(), proto);
        }
    }

    #[test]
    fn protocol_helpers() {
        assert!(Protocol::WebSocket.supports_live_queries());
        assert!(Protocol::Memory.supports_live_queries());
        assert!(!Protocol::Http.supports_live_queries());
        assert!(!Protocol::Https.supports_live_queries());
        assert!(Protocol::Memory.is_embedded());
        assert!(!Protocol::WebSocket.is_embedded());
    }

    #[test]
    fn from_source_reads_vars() {
        let prefix = "SURQL_TEST_CFG_";
        let env: HashMap<String, String> = [
            ("URL", "wss://env.example/rpc"),
            ("NAMESPACE", "envns"),
            ("DATABASE", "envdb"),
            ("USERNAME", "envuser"),
            ("TIMEOUT", "45.5"),
            ("ENABLE_LIVE_QUERIES", "false"),
        ]
        .iter()
        .map(|(k, v)| (format!("{prefix}{k}"), (*v).to_string()))
        .collect();

        let cfg = ConnectionConfig::from_map_with_prefix(prefix, &env).unwrap();
        assert_eq!(cfg.url(), "wss://env.example/rpc");
        assert_eq!(cfg.namespace(), "envns");
        assert_eq!(cfg.database(), "envdb");
        assert_eq!(cfg.username(), Some("envuser"));
        assert!((cfg.timeout() - 45.5).abs() < f64::EPSILON);
        assert!(!cfg.enable_live_queries);
    }

    #[test]
    fn from_source_accepts_legacy_aliases() {
        let prefix = "SURQL_LEGACY_";
        let env: HashMap<String, String> = [
            ("DB_URL", "ws://legacy.example/rpc"),
            ("DB_NS", "legns"),
            ("DB", "legdb"),
            ("DB_USER", "leguser"),
            ("DB_PASS", "legpass"),
        ]
        .iter()
        .map(|(k, v)| (format!("{prefix}{k}"), (*v).to_string()))
        .collect();
        let cfg = ConnectionConfig::from_map_with_prefix(prefix, &env).unwrap();
        assert_eq!(cfg.url(), "ws://legacy.example/rpc");
        assert_eq!(cfg.namespace(), "legns");
        assert_eq!(cfg.database(), "legdb");
        assert_eq!(cfg.username(), Some("leguser"));
        assert_eq!(cfg.password(), Some("legpass"));
    }

    #[test]
    fn named_from_source_uses_prefix() {
        let prefix = "SURQL_PRIMARY_";
        let env: HashMap<String, String> = [
            ("URL", "ws://primary.example/rpc"),
            ("NAMESPACE", "pns"),
            ("DATABASE", "pdb"),
        ]
        .iter()
        .map(|(k, v)| (format!("{prefix}{k}"), (*v).to_string()))
        .collect();
        let named = NamedConnectionConfig::from_source("primary", |k| env.get(k).cloned()).unwrap();
        assert_eq!(named.name, "primary");
        assert_eq!(named.config.url(), "ws://primary.example/rpc");
    }

    #[test]
    fn serde_roundtrip() {
        let cfg = ConnectionConfig::default();
        let json = serde_json::to_string(&cfg).unwrap();
        let back: ConnectionConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(cfg, back);
    }
}
