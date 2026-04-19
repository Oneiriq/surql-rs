//! Environment configuration for multi-database orchestration.
//!
//! Port of `surql/orchestration/config.py`. Provides the
//! [`EnvironmentConfig`] value type, the [`EnvironmentRegistry`]
//! collection, and process-wide registry helpers
//! ([`get_registry`] / [`set_registry`] / [`configure_environments`] /
//! [`register_environment`]) mirroring the `ConnectionRegistry` pattern
//! established in [`crate::connection::registry`].

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::{Arc, OnceLock};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::connection::config::ConnectionConfig;
use crate::error::{Result, SurqlError};

/// Configuration for a single database environment.
///
/// Port of `surql.orchestration.config.EnvironmentConfig`.
///
/// ## Examples
///
/// ```
/// # #[cfg(feature = "orchestration")] {
/// use surql::connection::ConnectionConfig;
/// use surql::orchestration::EnvironmentConfig;
///
/// let cfg = ConnectionConfig::builder()
///     .url("ws://prod.example.com:8000")
///     .namespace("prod")
///     .database("main")
///     .build()
///     .unwrap();
/// let env = EnvironmentConfig::builder("production", cfg)
///     .priority(1)
///     .tag("prod")
///     .tag("critical")
///     .require_approval(true)
///     .build()
///     .unwrap();
/// assert_eq!(env.name, "production");
/// assert_eq!(env.priority, 1);
/// assert!(env.require_approval);
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnvironmentConfig {
    /// Environment name (e.g. `production`, `staging`).
    pub name: String,
    /// Database connection configuration.
    pub connection: ConnectionConfig,
    /// Deployment priority — lower value means higher priority.
    #[serde(default = "default_priority")]
    pub priority: u32,
    /// Tags for environment categorisation.
    #[serde(default)]
    pub tags: BTreeSet<String>,
    /// Require manual approval before deployment.
    #[serde(default)]
    pub require_approval: bool,
    /// Allow destructive migrations (e.g. `REMOVE TABLE`).
    #[serde(default = "default_allow_destructive")]
    pub allow_destructive: bool,
}

fn default_priority() -> u32 {
    100
}

fn default_allow_destructive() -> bool {
    true
}

impl EnvironmentConfig {
    /// Start a builder for a new [`EnvironmentConfig`].
    pub fn builder(
        name: impl Into<String>,
        connection: ConnectionConfig,
    ) -> EnvironmentConfigBuilder {
        EnvironmentConfigBuilder {
            name: name.into(),
            connection,
            priority: default_priority(),
            tags: BTreeSet::new(),
            require_approval: false,
            allow_destructive: default_allow_destructive(),
        }
    }

    fn validate_name(name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(SurqlError::Validation {
                reason: "environment name cannot be empty".into(),
            });
        }
        let ok = name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
        if !ok {
            return Err(SurqlError::Validation {
                reason: "environment name must be alphanumeric with optional underscores/hyphens"
                    .into(),
            });
        }
        Ok(())
    }

    /// Return `true` if this environment has the given tag.
    pub fn has_tag(&self, tag: &str) -> bool {
        self.tags.contains(tag)
    }
}

/// Builder for [`EnvironmentConfig`].
#[derive(Debug, Clone)]
pub struct EnvironmentConfigBuilder {
    name: String,
    connection: ConnectionConfig,
    priority: u32,
    tags: BTreeSet<String>,
    require_approval: bool,
    allow_destructive: bool,
}

impl EnvironmentConfigBuilder {
    /// Override the deployment priority.
    pub fn priority(mut self, value: u32) -> Self {
        self.priority = value;
        self
    }

    /// Add a tag.
    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.insert(tag.into());
        self
    }

    /// Replace the tag set with the supplied iterator.
    pub fn tags<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.tags = tags.into_iter().map(Into::into).collect();
        self
    }

    /// Toggle the `require_approval` flag.
    pub fn require_approval(mut self, value: bool) -> Self {
        self.require_approval = value;
        self
    }

    /// Toggle the `allow_destructive` flag.
    pub fn allow_destructive(mut self, value: bool) -> Self {
        self.allow_destructive = value;
        self
    }

    /// Finalise into an [`EnvironmentConfig`] after validation.
    ///
    /// # Errors
    ///
    /// Returns [`SurqlError::Validation`] when `name` is empty or contains
    /// characters other than ASCII alphanumerics, underscores, or hyphens.
    pub fn build(self) -> Result<EnvironmentConfig> {
        EnvironmentConfig::validate_name(&self.name)?;
        Ok(EnvironmentConfig {
            name: self.name,
            connection: self.connection,
            priority: self.priority,
            tags: self.tags,
            require_approval: self.require_approval,
            allow_destructive: self.allow_destructive,
        })
    }
}

/// Registry of environment configurations.
///
/// Clone-cheap: internal state is refcounted. Mirrors
/// [`crate::connection::registry::ConnectionRegistry`].
#[derive(Debug, Clone, Default)]
pub struct EnvironmentRegistry {
    inner: Arc<RegistryInner>,
}

#[derive(Debug, Default)]
struct RegistryInner {
    environments: RwLock<HashMap<String, EnvironmentConfig>>,
}

impl EnvironmentRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new environment.
    ///
    /// Replaces any previously-registered entry for the same name
    /// (matching Python's dict-assignment semantics).
    pub async fn register(&self, env: EnvironmentConfig) {
        let mut guard = self.inner.environments.write().await;
        guard.insert(env.name.clone(), env);
    }

    /// Register an environment with explicit fields.
    ///
    /// Convenience wrapper that mirrors the Python
    /// `EnvironmentRegistry.register_environment` signature.
    ///
    /// # Errors
    ///
    /// Returns [`SurqlError::Validation`] if the supplied `name` fails
    /// [`EnvironmentConfig`] validation.
    pub async fn register_environment(
        &self,
        name: impl Into<String>,
        connection: ConnectionConfig,
        priority: u32,
        tags: Option<BTreeSet<String>>,
        require_approval: bool,
        allow_destructive: bool,
    ) -> Result<()> {
        let mut builder = EnvironmentConfig::builder(name, connection)
            .priority(priority)
            .require_approval(require_approval)
            .allow_destructive(allow_destructive);
        if let Some(tags) = tags {
            builder = builder.tags(tags);
        }
        let env = builder.build()?;
        self.register(env).await;
        Ok(())
    }

    /// Unregister an environment. No-op if the name is not present.
    pub async fn unregister(&self, name: &str) {
        self.inner.environments.write().await.remove(name);
    }

    /// Fetch an environment by name.
    pub async fn get(&self, name: &str) -> Option<EnvironmentConfig> {
        self.inner.environments.read().await.get(name).cloned()
    }

    /// List registered environment names, sorted by ascending priority.
    pub async fn list(&self) -> Vec<String> {
        let envs = self.inner.environments.read().await;
        let mut sorted: Vec<&EnvironmentConfig> = envs.values().collect();
        sorted.sort_by_key(|e| (e.priority, e.name.clone()));
        sorted.into_iter().map(|e| e.name.clone()).collect()
    }

    /// Return every environment carrying the supplied tag.
    pub async fn get_by_tag(&self, tag: &str) -> Vec<EnvironmentConfig> {
        self.inner
            .environments
            .read()
            .await
            .values()
            .filter(|e| e.has_tag(tag))
            .cloned()
            .collect()
    }

    /// Total environment count.
    pub async fn len(&self) -> usize {
        self.inner.environments.read().await.len()
    }

    /// `true` when no environments are registered.
    pub async fn is_empty(&self) -> bool {
        self.inner.environments.read().await.is_empty()
    }

    /// Remove every environment.
    pub async fn clear(&self) {
        self.inner.environments.write().await.clear();
    }

    /// Load a registry from a JSON configuration file.
    ///
    /// The file format mirrors `EnvironmentRegistry.from_config_file` in
    /// the Python implementation:
    ///
    /// ```json
    /// {
    ///   "environments": [
    ///     {
    ///       "name": "production",
    ///       "connection": { "db_url": "...", "db_ns": "...", "db": "..." },
    ///       "priority": 1,
    ///       "tags": ["prod"],
    ///       "require_approval": true,
    ///       "allow_destructive": false
    ///     }
    ///   ]
    /// }
    /// ```
    ///
    /// A missing file yields an empty registry (matching Python).
    ///
    /// # Errors
    ///
    /// Returns [`SurqlError::Io`] when the file exists but cannot be
    /// read, [`SurqlError::Serialization`] when the JSON body cannot be
    /// parsed, or [`SurqlError::Validation`] when an environment entry
    /// has an invalid name.
    pub async fn from_config_file(path: &Path) -> Result<Self> {
        let registry = Self::new();
        if !path.exists() {
            return Ok(registry);
        }
        let body = std::fs::read_to_string(path)?;
        let config: FileConfig = serde_json::from_str(&body)?;
        for entry in config.environments {
            let env = EnvironmentConfig::builder(entry.name, entry.connection)
                .priority(entry.priority.unwrap_or_else(default_priority))
                .tags(entry.tags.unwrap_or_default())
                .require_approval(entry.require_approval.unwrap_or(false))
                .allow_destructive(
                    entry
                        .allow_destructive
                        .unwrap_or_else(default_allow_destructive),
                )
                .build()?;
            registry.register(env).await;
        }
        Ok(registry)
    }
}

/// JSON shape accepted by [`EnvironmentRegistry::from_config_file`].
#[derive(Debug, Clone, Deserialize)]
struct FileConfig {
    environments: Vec<FileEnvironment>,
}

#[derive(Debug, Clone, Deserialize)]
struct FileEnvironment {
    name: String,
    connection: ConnectionConfig,
    #[serde(default)]
    priority: Option<u32>,
    #[serde(default)]
    tags: Option<BTreeSet<String>>,
    #[serde(default)]
    require_approval: Option<bool>,
    #[serde(default)]
    allow_destructive: Option<bool>,
}

static GLOBAL: OnceLock<EnvironmentRegistry> = OnceLock::new();

fn global() -> &'static EnvironmentRegistry {
    GLOBAL.get_or_init(EnvironmentRegistry::new)
}

/// Handle to the process-wide [`EnvironmentRegistry`].
///
/// The first call initialises a fresh registry; subsequent calls return
/// a clone of the same handle.
pub fn get_registry() -> EnvironmentRegistry {
    global().clone()
}

/// Replace the process-wide registry.
///
/// Primarily useful in tests.
///
/// # Errors
///
/// Returns [`SurqlError::Registry`] when the global has already been
/// initialised — `OnceLock` can only be `set` once per process.
pub fn set_registry(registry: EnvironmentRegistry) -> Result<()> {
    GLOBAL.set(registry).map_err(|_| SurqlError::Registry {
        reason: "global environment registry is already initialised".into(),
    })
}

/// Replace the global registry with the contents of `config_path`.
///
/// Mirrors `surql.orchestration.config.configure_environments`.
///
/// # Errors
///
/// See [`EnvironmentRegistry::from_config_file`] and [`set_registry`].
pub async fn configure_environments(config_path: &Path) -> Result<()> {
    let registry = EnvironmentRegistry::from_config_file(config_path).await?;
    // If already set, swap contents instead of failing.
    match GLOBAL.get() {
        Some(existing) => {
            existing.clear().await;
            let snapshot = registry.inner.environments.read().await.clone();
            let mut target = existing.inner.environments.write().await;
            *target = snapshot;
            Ok(())
        }
        None => set_registry(registry),
    }
}

/// Register an environment in the global registry.
///
/// Mirrors the top-level `register_environment` helper from Python.
///
/// # Errors
///
/// Returns [`SurqlError::Validation`] when the name is invalid.
pub async fn register_environment(
    name: impl Into<String>,
    connection: ConnectionConfig,
    priority: u32,
    tags: Option<BTreeSet<String>>,
    require_approval: bool,
    allow_destructive: bool,
) -> Result<()> {
    get_registry()
        .register_environment(
            name,
            connection,
            priority,
            tags,
            require_approval,
            allow_destructive,
        )
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_connection(ns: &str, db: &str) -> ConnectionConfig {
        ConnectionConfig::builder()
            .url("ws://localhost:8000")
            .namespace(ns)
            .database(db)
            .build()
            .expect("valid connection config")
    }

    #[test]
    fn builder_rejects_empty_name() {
        let err = EnvironmentConfig::builder("", sample_connection("t", "a"))
            .build()
            .unwrap_err();
        assert!(matches!(err, SurqlError::Validation { .. }));
    }

    #[test]
    fn builder_rejects_invalid_characters() {
        let err = EnvironmentConfig::builder("prod env!", sample_connection("t", "a"))
            .build()
            .unwrap_err();
        assert!(matches!(err, SurqlError::Validation { .. }));
    }

    #[test]
    fn builder_accepts_hyphen_and_underscore() {
        let env = EnvironmentConfig::builder("prod_us-east", sample_connection("t", "a"))
            .priority(5)
            .tag("prod")
            .require_approval(true)
            .allow_destructive(false)
            .build()
            .expect("valid");
        assert_eq!(env.priority, 5);
        assert!(env.has_tag("prod"));
        assert!(env.require_approval);
        assert!(!env.allow_destructive);
    }

    #[tokio::test]
    async fn registry_register_and_get_roundtrip() {
        let registry = EnvironmentRegistry::new();
        let env = EnvironmentConfig::builder("staging", sample_connection("s", "a"))
            .priority(50)
            .tag("pre-prod")
            .build()
            .unwrap();
        registry.register(env.clone()).await;
        let fetched = registry.get("staging").await.expect("registered");
        assert_eq!(fetched.name, "staging");
        assert_eq!(fetched.priority, 50);
    }

    #[tokio::test]
    async fn registry_list_sorts_by_priority() {
        let registry = EnvironmentRegistry::new();
        for (name, priority) in [("c", 30), ("a", 10), ("b", 20)] {
            let env = EnvironmentConfig::builder(name, sample_connection("ns", name))
                .priority(priority)
                .build()
                .unwrap();
            registry.register(env).await;
        }
        let list = registry.list().await;
        assert_eq!(list, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn registry_by_tag_filters() {
        let registry = EnvironmentRegistry::new();
        let prod = EnvironmentConfig::builder("p", sample_connection("ns", "p"))
            .tag("prod")
            .build()
            .unwrap();
        let stg = EnvironmentConfig::builder("s", sample_connection("ns", "s"))
            .tag("stg")
            .build()
            .unwrap();
        registry.register(prod).await;
        registry.register(stg).await;
        let prods = registry.get_by_tag("prod").await;
        assert_eq!(prods.len(), 1);
        assert_eq!(prods[0].name, "p");
    }

    #[tokio::test]
    async fn register_environment_helper_validates() {
        let registry = EnvironmentRegistry::new();
        let err = registry
            .register_environment("", sample_connection("ns", "x"), 100, None, false, true)
            .await
            .unwrap_err();
        assert!(matches!(err, SurqlError::Validation { .. }));
    }

    #[tokio::test]
    async fn unregister_and_clear_work() {
        let registry = EnvironmentRegistry::new();
        let env = EnvironmentConfig::builder("x", sample_connection("ns", "x"))
            .build()
            .unwrap();
        registry.register(env).await;
        assert_eq!(registry.len().await, 1);
        registry.unregister("x").await;
        assert!(registry.is_empty().await);
        let env = EnvironmentConfig::builder("y", sample_connection("ns", "y"))
            .build()
            .unwrap();
        registry.register(env).await;
        registry.clear().await;
        assert!(registry.is_empty().await);
    }

    #[tokio::test]
    async fn from_config_file_missing_returns_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("does_not_exist.json");
        let registry = EnvironmentRegistry::from_config_file(&missing)
            .await
            .unwrap();
        assert!(registry.is_empty().await);
    }

    #[tokio::test]
    async fn from_config_file_parses_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("envs.json");
        let body = r#"{
            "environments": [
                {
                    "name": "production",
                    "connection": {
                        "db_url": "ws://localhost:8000",
                        "db_ns": "prod",
                        "db": "main",
                        "db_user": null,
                        "db_pass": null,
                        "db_timeout": 30.0,
                        "db_max_connections": 10,
                        "db_retry_max_attempts": 3,
                        "db_retry_min_wait": 1.0,
                        "db_retry_max_wait": 10.0,
                        "db_retry_multiplier": 2.0,
                        "enable_live_queries": true
                    },
                    "priority": 1,
                    "tags": ["prod", "critical"],
                    "require_approval": true,
                    "allow_destructive": false
                }
            ]
        }"#;
        std::fs::write(&path, body).unwrap();
        let registry = EnvironmentRegistry::from_config_file(&path).await.unwrap();
        assert_eq!(registry.len().await, 1);
        let env = registry.get("production").await.unwrap();
        assert_eq!(env.priority, 1);
        assert!(env.require_approval);
        assert!(!env.allow_destructive);
        assert!(env.has_tag("prod"));
        assert!(env.has_tag("critical"));
    }
}
