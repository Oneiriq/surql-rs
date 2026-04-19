//! Canary deployment strategy.
//!
//! Deploys to a leading subset of environments first, then to the
//! remainder only if the canary batch succeeded. Port of
//! `surql.orchestration.strategy.CanaryStrategy`.

use async_trait::async_trait;
use tokio::task::JoinSet;
use tracing::{error, info};

use crate::error::{Result, SurqlError};
use crate::orchestration::coordinator::DeploymentPlan;
use crate::orchestration::environment::EnvironmentConfig;
use crate::orchestration::result::{DeploymentResult, DeploymentStatus};
use crate::orchestration::strategies::{
    deploy_to_environment, resolve_plan_environments, DeploymentStrategy,
};

/// Deploy to the first `canary_percentage` of environments, then the rest.
#[derive(Debug, Clone, Copy)]
pub struct CanaryStrategy {
    canary_percentage: f64,
}

impl Default for CanaryStrategy {
    fn default() -> Self {
        Self {
            canary_percentage: 10.0,
        }
    }
}

impl CanaryStrategy {
    /// Construct a canary strategy.
    ///
    /// # Errors
    ///
    /// Returns [`SurqlError::Validation`] when `canary_percentage` is
    /// outside the inclusive range `[1.0, 50.0]`.
    pub fn with_percentage(canary_percentage: f64) -> Result<Self> {
        if !(1.0..=50.0).contains(&canary_percentage) {
            return Err(SurqlError::Validation {
                reason: "canary_percentage must be between 1.0 and 50.0".into(),
            });
        }
        Ok(Self { canary_percentage })
    }

    /// Configured canary percentage.
    pub fn canary_percentage(&self) -> f64 {
        self.canary_percentage
    }
}

#[async_trait]
impl DeploymentStrategy for CanaryStrategy {
    async fn deploy(&self, plan: &DeploymentPlan) -> Result<Vec<DeploymentResult>> {
        info!(
            count = plan.environments.len(),
            canary_percentage = self.canary_percentage,
            "canary_deployment_started"
        );

        let envs = resolve_plan_environments(plan).await?;
        if envs.is_empty() {
            return Ok(Vec::new());
        }

        let canary_count = canary_slice(envs.len(), self.canary_percentage);
        let (canary, remaining) = envs.split_at(canary_count);
        let canary: Vec<EnvironmentConfig> = canary.to_vec();
        let remaining: Vec<EnvironmentConfig> = remaining.to_vec();

        info!(canary = canary.len(), "deploying_to_canary");
        let canary_results = fan_out(&canary, plan).await?;
        let failed = canary_results
            .iter()
            .any(|r| r.status == DeploymentStatus::Failed);
        if failed {
            error!("canary_deployment_failed");
            return Ok(canary_results);
        }

        info!(remaining = remaining.len(), "canary_successful_proceeding");
        let rest_results = fan_out(&remaining, plan).await?;
        let mut out = canary_results;
        out.extend(rest_results);
        Ok(out)
    }
}

fn canary_slice(total: usize, pct: f64) -> usize {
    // Mirrors py's `max(1, int(len(envs) * pct / 100))`.
    if total == 0 {
        return 0;
    }
    // Python's `int()` truncates toward zero; `f64 as usize` does the same
    // for non-negative finite values, which is what the API contract guarantees.
    #[allow(
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss
    )]
    let raw = (total as f64 * pct / 100.0) as usize;
    raw.max(1).min(total)
}

async fn fan_out(
    envs: &[EnvironmentConfig],
    plan: &DeploymentPlan,
) -> Result<Vec<DeploymentResult>> {
    if envs.is_empty() {
        return Ok(Vec::new());
    }
    let mut join = JoinSet::new();
    for (idx, env) in envs.iter().cloned().enumerate() {
        let plan = plan.clone();
        join.spawn(async move {
            let result = deploy_to_environment(&env, &plan).await;
            (idx, result)
        });
    }
    let mut buffer: Vec<Option<DeploymentResult>> = (0..envs.len()).map(|_| None).collect();
    while let Some(res) = join.join_next().await {
        match res {
            Ok((idx, result)) => buffer[idx] = Some(result),
            Err(join_err) => {
                return Err(SurqlError::Orchestration {
                    reason: format!("task join failed: {join_err}"),
                });
            }
        }
    }
    Ok(buffer.into_iter().flatten().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_out_of_range_percentage() {
        assert!(matches!(
            CanaryStrategy::with_percentage(0.5),
            Err(SurqlError::Validation { .. })
        ));
        assert!(matches!(
            CanaryStrategy::with_percentage(60.0),
            Err(SurqlError::Validation { .. })
        ));
    }

    #[test]
    fn accepts_boundary_percentages() {
        assert!(CanaryStrategy::with_percentage(1.0).is_ok());
        assert!(CanaryStrategy::with_percentage(50.0).is_ok());
    }

    #[test]
    fn canary_slice_matches_python_semantics() {
        assert_eq!(canary_slice(0, 10.0), 0);
        // int(10 * 10 / 100) == 1 -> max 1
        assert_eq!(canary_slice(10, 10.0), 1);
        // int(10 * 20 / 100) == 2
        assert_eq!(canary_slice(10, 20.0), 2);
        // Small percentage rounds down to 0, then clamped up to 1.
        assert_eq!(canary_slice(5, 1.0), 1);
        // Always at most `total`.
        assert_eq!(canary_slice(2, 50.0), 1);
    }
}
