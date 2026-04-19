//! Deployment result and status types.
//!
//! Port of `surql/orchestration/strategy.py` (the `DeploymentStatus`
//! enum and `DeploymentResult` dataclass pieces). Kept in a dedicated
//! module to mirror the parity layout declared in issue #64.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Status of a single-environment deployment.
///
/// Mirrors `surql.orchestration.strategy.DeploymentStatus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentStatus {
    /// Deployment has been planned but has not yet started.
    Pending,
    /// Deployment is currently running.
    InProgress,
    /// Deployment succeeded.
    Success,
    /// Deployment failed.
    Failed,
    /// Deployment was rolled back after a later failure.
    RolledBack,
}

impl DeploymentStatus {
    /// Static string label (matches Python's enum values).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InProgress => "in_progress",
            Self::Success => "success",
            Self::Failed => "failed",
            Self::RolledBack => "rolled_back",
        }
    }
}

impl std::fmt::Display for DeploymentStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Result of deploying to a single environment.
///
/// Mirrors `surql.orchestration.strategy.DeploymentResult`.
///
/// ## Examples
///
/// ```
/// # #[cfg(feature = "orchestration")] {
/// use chrono::Utc;
/// use surql::orchestration::{DeploymentResult, DeploymentStatus};
///
/// let started = Utc::now();
/// let r = DeploymentResult::builder("prod", DeploymentStatus::Success, started)
///     .completed_at(Utc::now())
///     .execution_time_ms(42)
///     .migrations_applied(3)
///     .build();
/// assert_eq!(r.environment, "prod");
/// assert_eq!(r.status, DeploymentStatus::Success);
/// assert_eq!(r.migrations_applied, 3);
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentResult {
    /// Target environment name.
    pub environment: String,
    /// Outcome of the deployment.
    pub status: DeploymentStatus,
    /// When the deployment started.
    pub started_at: DateTime<Utc>,
    /// When the deployment finished (`None` when still in flight).
    pub completed_at: Option<DateTime<Utc>>,
    /// Error message captured when `status == Failed`.
    pub error: Option<String>,
    /// Wall-clock execution time in milliseconds.
    pub execution_time_ms: Option<u64>,
    /// Number of migrations actually applied.
    pub migrations_applied: usize,
}

impl DeploymentResult {
    /// Start a builder for a deployment result.
    pub fn builder(
        environment: impl Into<String>,
        status: DeploymentStatus,
        started_at: DateTime<Utc>,
    ) -> DeploymentResultBuilder {
        DeploymentResultBuilder {
            environment: environment.into(),
            status,
            started_at,
            completed_at: None,
            error: None,
            execution_time_ms: None,
            migrations_applied: 0,
        }
    }

    /// Duration between `started_at` and `completed_at`.
    ///
    /// Returns `None` when the deployment has not completed.
    pub fn duration(&self) -> Option<Duration> {
        let completed = self.completed_at?;
        let delta = completed.signed_duration_since(self.started_at);
        delta.to_std().ok()
    }

    /// Duration in seconds as a float, matching Python's
    /// `DeploymentResult.duration_seconds`.
    pub fn duration_seconds(&self) -> Option<f64> {
        self.duration().map(|d| d.as_secs_f64())
    }

    /// `true` when the deployment completed successfully.
    pub fn is_success(&self) -> bool {
        self.status == DeploymentStatus::Success
    }

    /// `true` when the deployment ended in failure.
    pub fn is_failed(&self) -> bool {
        self.status == DeploymentStatus::Failed
    }
}

/// Builder for [`DeploymentResult`].
#[derive(Debug, Clone)]
pub struct DeploymentResultBuilder {
    environment: String,
    status: DeploymentStatus,
    started_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
    error: Option<String>,
    execution_time_ms: Option<u64>,
    migrations_applied: usize,
}

impl DeploymentResultBuilder {
    /// Set the completion timestamp.
    pub fn completed_at(mut self, value: DateTime<Utc>) -> Self {
        self.completed_at = Some(value);
        self
    }

    /// Attach an error message.
    pub fn error(mut self, value: impl Into<String>) -> Self {
        self.error = Some(value.into());
        self
    }

    /// Set the measured wall-clock execution time in milliseconds.
    pub fn execution_time_ms(mut self, value: u64) -> Self {
        self.execution_time_ms = Some(value);
        self
    }

    /// Override the deployment status.
    pub fn status(mut self, value: DeploymentStatus) -> Self {
        self.status = value;
        self
    }

    /// Set the number of applied migrations.
    pub fn migrations_applied(mut self, value: usize) -> Self {
        self.migrations_applied = value;
        self
    }

    /// Finalise into a [`DeploymentResult`].
    pub fn build(self) -> DeploymentResult {
        DeploymentResult {
            environment: self.environment,
            status: self.status,
            started_at: self.started_at,
            completed_at: self.completed_at,
            error: self.error,
            execution_time_ms: self.execution_time_ms,
            migrations_applied: self.migrations_applied,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_as_str_matches_python() {
        assert_eq!(DeploymentStatus::Pending.as_str(), "pending");
        assert_eq!(DeploymentStatus::InProgress.as_str(), "in_progress");
        assert_eq!(DeploymentStatus::Success.as_str(), "success");
        assert_eq!(DeploymentStatus::Failed.as_str(), "failed");
        assert_eq!(DeploymentStatus::RolledBack.as_str(), "rolled_back");
    }

    #[test]
    fn status_serializes_as_snake_case() {
        let json = serde_json::to_string(&DeploymentStatus::InProgress).unwrap();
        assert_eq!(json, "\"in_progress\"");
    }

    #[test]
    fn builder_roundtrip_captures_fields() {
        let started = Utc::now();
        let completed = started + chrono::Duration::milliseconds(250);
        let r = DeploymentResult::builder("prod", DeploymentStatus::Success, started)
            .completed_at(completed)
            .execution_time_ms(250)
            .migrations_applied(4)
            .build();
        assert_eq!(r.environment, "prod");
        assert_eq!(r.migrations_applied, 4);
        assert!(r.is_success());
        assert!(!r.is_failed());
        let secs = r.duration_seconds().unwrap();
        assert!((secs - 0.250).abs() < 1e-9, "unexpected duration {secs}");
    }

    #[test]
    fn duration_none_without_completion() {
        let r = DeploymentResult::builder("prod", DeploymentStatus::InProgress, Utc::now()).build();
        assert!(r.duration().is_none());
        assert!(r.duration_seconds().is_none());
    }

    #[test]
    fn error_message_is_captured() {
        let r = DeploymentResult::builder("prod", DeploymentStatus::Failed, Utc::now())
            .error("boom")
            .build();
        assert_eq!(r.error.as_deref(), Some("boom"));
        assert!(r.is_failed());
    }
}
