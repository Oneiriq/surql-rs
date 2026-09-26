//! Canary deployment strategy.
//!
//! Deploys to a leading subset of environments first, then to the
//! remainder only if the canary batch succeeded. Port of
//! `surql.orchestration.strategy.CanaryStrategy`.

use async_trait::async_trait;
use tracing::{error, info};

use crate::error::{Result, SurqlError};
use crate::orchestration::coordinator::DeploymentPlan;
use crate::orchestration::result::{DeploymentResult, DeploymentStatus};
use crate::orchestration::strategies::concurrent::deploy_all;
use crate::orchestration::strategies::{resolve_plan_environments, DeploymentStrategy};

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
        let (canary, remaining) = envs
            .split_at_checked(canary_count)
            .unwrap_or((envs.as_slice(), &[]));

        info!(canary = canary.len(), "deploying_to_canary");
        let canary_results = deploy_all(canary, plan, None).await;
        let failed = canary_results
            .iter()
            .any(|r| r.status == DeploymentStatus::Failed);
        if failed {
            error!("canary_deployment_failed");
            return Ok(canary_results);
        }

        info!(remaining = remaining.len(), "canary_successful_proceeding");
        let rest_results = deploy_all(remaining, plan, None).await;
        let mut out = canary_results;
        out.extend(rest_results);
        Ok(out)
    }
}

/// Size of the canary batch: Python's `max(1, int(total * pct / 100))`,
/// capped at `total`.
///
/// `int()` truncates, so the count is the number of `n` in `1..=total`
/// with `n <= total * pct / 100`. Counting keeps the arithmetic in `f64`
/// without casting a float back to an integer.
fn canary_slice(total: usize, pct: f64) -> usize {
    let as_f64 = |n: usize| u32::try_from(n).map_or(f64::from(u32::MAX), f64::from);
    let limit = as_f64(total) * pct / 100.0;
    let raw = (1..=total).take_while(|n| as_f64(*n) <= limit).count();
    raw.max(1).min(total)
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
        assert_eq!(canary_slice(1, 50.0), 1);
        assert_eq!(canary_slice(3, 50.0), 1);
        assert_eq!(canary_slice(4, 50.0), 2);
        assert_eq!(canary_slice(200, 12.5), 25);
    }
}
