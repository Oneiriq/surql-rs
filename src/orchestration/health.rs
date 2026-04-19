//! Health checking for database instances.
//!
//! Port of `surql/orchestration/health.py`. Provides the
//! [`HealthStatus`] value, [`HealthCheck`] operator type, and the
//! convenience free functions [`check_environment_health`] and
//! [`verify_connectivity`].

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, error, info, warn};

use crate::connection::DatabaseClient;
use crate::error::{Result, SurqlError};
use crate::migration::MIGRATION_TABLE_NAME;
use crate::orchestration::environment::EnvironmentConfig;

/// Health status for a database instance.
///
/// Port of `surql.orchestration.health.HealthStatus`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthStatus {
    /// Environment name.
    pub environment: String,
    /// Overall health flag — `true` when the environment is usable.
    pub is_healthy: bool,
    /// `true` when the client could connect + run a trivial query.
    pub can_connect: bool,
    /// `true` when the migration history table exists.
    #[serde(default)]
    pub migration_table_exists: bool,
    /// Optional error message populated when the check failed.
    #[serde(default)]
    pub error: Option<String>,
}

impl HealthStatus {
    /// Build a status describing a successful health check.
    pub fn healthy(environment: impl Into<String>, migration_table_exists: bool) -> Self {
        Self {
            environment: environment.into(),
            is_healthy: true,
            can_connect: true,
            migration_table_exists,
            error: None,
        }
    }

    /// Build a status describing a failed health check.
    pub fn unhealthy(environment: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            environment: environment.into(),
            is_healthy: false,
            can_connect: false,
            migration_table_exists: false,
            error: Some(error.into()),
        }
    }
}

/// Health check operator.
///
/// Stateless — holds no configuration beyond the (implicit)
/// [`DatabaseClient`] opened per call. Mirrors
/// `surql.orchestration.health.HealthCheck`.
#[derive(Debug, Default, Clone, Copy)]
pub struct HealthCheck;

impl HealthCheck {
    /// Create a new health checker.
    pub fn new() -> Self {
        Self
    }

    /// Check a single environment end-to-end.
    ///
    /// # Errors
    ///
    /// Never propagates connection failures; they are reflected in the
    /// returned [`HealthStatus`]. [`SurqlError`] is only returned when
    /// a misconfigured environment prevents building a client at all.
    pub async fn check_environment(self, env: &EnvironmentConfig) -> Result<HealthStatus> {
        let client = match build_client(&env.connection) {
            Ok(client) => client,
            Err(err) => {
                warn!(environment = %env.name, error = %err, "health_client_build_failed");
                return Ok(HealthStatus::unhealthy(env.name.clone(), err.to_string()));
            }
        };

        let can_connect = check_connect(&client, &env.name).await;
        if !can_connect {
            let _ = client.disconnect().await;
            return Ok(HealthStatus {
                environment: env.name.clone(),
                is_healthy: false,
                can_connect: false,
                migration_table_exists: false,
                error: Some("Cannot connect to database".into()),
            });
        }

        let migration_table_exists = check_migration_table(&client, &env.name).await;
        let _ = client.disconnect().await;
        info!(
            environment = %env.name,
            migration_table_exists,
            "environment_health_checked"
        );
        Ok(HealthStatus::healthy(
            env.name.clone(),
            migration_table_exists,
        ))
    }

    /// Check whether the environment is reachable.
    ///
    /// Returns `Ok(true)` when a trivial query (`RETURN 1`) succeeds,
    /// `Ok(false)` otherwise. Configuration errors surface via
    /// [`SurqlError`].
    ///
    /// # Errors
    ///
    /// Propagates [`SurqlError::Connection`] variants raised while
    /// constructing the client (invalid URL, missing credentials).
    pub async fn check_connectivity(self, env: &EnvironmentConfig) -> Result<bool> {
        let client = build_client(&env.connection)?;
        let ok = check_connect(&client, &env.name).await;
        let _ = client.disconnect().await;
        Ok(ok)
    }

    /// Comprehensive schema integrity check (keyed by check name).
    ///
    /// Matches `HealthCheck.check_schema_integrity` in Python.
    ///
    /// # Errors
    ///
    /// See [`HealthCheck::check_connectivity`].
    pub async fn check_schema_integrity(
        self,
        env: &EnvironmentConfig,
    ) -> Result<HashMap<String, bool>> {
        let mut checks: HashMap<String, bool> = HashMap::new();
        checks.insert("connectivity".into(), false);
        checks.insert("migration_table".into(), false);

        let client = match build_client(&env.connection) {
            Ok(client) => client,
            Err(err) => {
                debug!(environment = %env.name, error = %err, "schema_integrity_client_failed");
                return Ok(checks);
            }
        };

        let connectivity = check_connect(&client, &env.name).await;
        checks.insert("connectivity".into(), connectivity);
        if !connectivity {
            let _ = client.disconnect().await;
            return Ok(checks);
        }

        let table_exists = check_migration_table(&client, &env.name).await;
        checks.insert("migration_table".into(), table_exists);
        let _ = client.disconnect().await;
        Ok(checks)
    }

    /// Check multiple environments serially.
    ///
    /// # Errors
    ///
    /// Returns the first [`SurqlError`] produced by
    /// [`HealthCheck::check_environment`]; all successful checks are
    /// discarded in that case.
    pub async fn verify_all_environments(
        self,
        environments: &[EnvironmentConfig],
    ) -> Result<HashMap<String, HealthStatus>> {
        let mut out = HashMap::new();
        for env in environments {
            let status = self.check_environment(env).await?;
            out.insert(env.name.clone(), status);
        }
        Ok(out)
    }
}

fn build_client(connection: &crate::connection::ConnectionConfig) -> Result<DatabaseClient> {
    DatabaseClient::new(connection.clone())
}

async fn check_connect(client: &DatabaseClient, environment: &str) -> bool {
    if let Err(err) = client.connect().await {
        warn!(environment = %environment, error = %err, "connectivity_check_failed");
        return false;
    }
    match client.query("RETURN 1").await {
        Ok(_) => {
            debug!(environment = %environment, "connectivity_check_passed");
            true
        }
        Err(err) => {
            warn!(environment = %environment, error = %err, "connectivity_check_failed");
            false
        }
    }
}

async fn check_migration_table(client: &DatabaseClient, environment: &str) -> bool {
    let surql = format!("SELECT * FROM {MIGRATION_TABLE_NAME} LIMIT 1");
    match client.query(&surql).await {
        Ok(value) => {
            debug!(environment = %environment, "migration_table_probe_ok");
            // SurrealDB returns an array-of-arrays; any Value other than null counts.
            !matches!(value, Value::Null)
        }
        Err(err) => {
            // Treat query errors (missing table) as "does not exist" rather than propagating.
            match err {
                SurqlError::Query { .. } => {
                    debug!(environment = %environment, "migration_table_not_found");
                    false
                }
                other => {
                    error!(environment = %environment, error = %other, "migration_table_check_error");
                    false
                }
            }
        }
    }
}

/// Convenience wrapper around [`HealthCheck::check_environment`].
///
/// # Errors
///
/// Propagates misconfiguration errors raised by the inner check.
pub async fn check_environment_health(env: &EnvironmentConfig) -> Result<HealthStatus> {
    HealthCheck::new().check_environment(env).await
}

/// Convenience wrapper around [`HealthCheck::check_connectivity`].
///
/// # Errors
///
/// Propagates misconfiguration errors raised by the inner check.
pub async fn verify_connectivity(env: &EnvironmentConfig) -> Result<bool> {
    HealthCheck::new().check_connectivity(env).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::ConnectionConfig;

    fn sample_env(name: &str) -> EnvironmentConfig {
        let cfg = ConnectionConfig::builder()
            .url("ws://127.0.0.1:65535")
            .namespace("ns")
            .database(name)
            .timeout(1.0)
            .retry_max_attempts(1)
            .retry_min_wait(0.1)
            .retry_max_wait(1.0)
            .build()
            .expect("config");
        EnvironmentConfig::builder(name, cfg).build().unwrap()
    }

    #[test]
    fn healthy_constructor_sets_flags() {
        let s = HealthStatus::healthy("prod", true);
        assert!(s.is_healthy);
        assert!(s.can_connect);
        assert!(s.migration_table_exists);
        assert!(s.error.is_none());
    }

    #[test]
    fn unhealthy_constructor_sets_error() {
        let s = HealthStatus::unhealthy("prod", "refused");
        assert!(!s.is_healthy);
        assert!(!s.can_connect);
        assert_eq!(s.error.as_deref(), Some("refused"));
    }

    #[tokio::test]
    async fn unreachable_host_reports_unhealthy() {
        let env = sample_env("unreach_host");
        let status = HealthCheck::new().check_environment(&env).await.unwrap();
        assert!(!status.is_healthy);
        assert!(!status.can_connect);
        assert!(!status.migration_table_exists);
        assert!(status.error.is_some());
    }

    #[tokio::test]
    async fn schema_integrity_on_unreachable_returns_false_checks() {
        let env = sample_env("unreach_integrity");
        let checks = HealthCheck::new()
            .check_schema_integrity(&env)
            .await
            .unwrap();
        assert_eq!(checks.get("connectivity"), Some(&false));
        assert_eq!(checks.get("migration_table"), Some(&false));
    }

    #[tokio::test]
    async fn verify_connectivity_false_on_unreachable() {
        let env = sample_env("unreach_verify");
        let ok = verify_connectivity(&env).await.unwrap();
        assert!(!ok);
    }

    #[tokio::test]
    async fn verify_all_environments_aggregates_results() {
        let envs = vec![sample_env("agg_a"), sample_env("agg_b")];
        let map = HealthCheck::new()
            .verify_all_environments(&envs)
            .await
            .unwrap();
        assert_eq!(map.len(), 2);
        for status in map.values() {
            assert!(!status.is_healthy);
        }
    }
}
