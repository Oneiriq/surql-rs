//! Parallel deployment strategy.
//!
//! Deploys the plan's migrations to every environment concurrently,
//! bounded by a capacity limiter. Port of
//! `surql.orchestration.strategy.ParallelStrategy`.

use async_trait::async_trait;
use tracing::info;

use crate::error::Result;
use crate::orchestration::coordinator::DeploymentPlan;
use crate::orchestration::result::DeploymentResult;
use crate::orchestration::strategies::concurrent::deploy_all;
use crate::orchestration::strategies::{resolve_plan_environments, DeploymentStrategy};

/// Deploy to every environment in parallel (fan-out with concurrency limit).
///
/// ## Examples
///
/// ```
/// # #[cfg(feature = "orchestration")] {
/// use surql::orchestration::strategies::ParallelStrategy;
///
/// let s = ParallelStrategy::with_max_concurrent(8);
/// assert_eq!(s.max_concurrent(), 8);
/// # }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ParallelStrategy {
    max_concurrent: usize,
}

impl Default for ParallelStrategy {
    fn default() -> Self {
        Self::with_max_concurrent(5)
    }
}

impl ParallelStrategy {
    /// Construct a new parallel strategy with the supplied concurrency.
    ///
    /// A value of `0` is coerced to `1` to avoid deadlock.
    pub fn with_max_concurrent(max_concurrent: usize) -> Self {
        Self {
            max_concurrent: max_concurrent.max(1),
        }
    }

    /// Current concurrency limit.
    pub fn max_concurrent(&self) -> usize {
        self.max_concurrent
    }
}

#[async_trait]
impl DeploymentStrategy for ParallelStrategy {
    async fn deploy(&self, plan: &DeploymentPlan) -> Result<Vec<DeploymentResult>> {
        info!(
            count = plan.environments.len(),
            max_concurrent = self.max_concurrent,
            "parallel_deployment_started"
        );

        let envs = resolve_plan_environments(plan).await?;
        Ok(deploy_all(&envs, plan, Some(self.max_concurrent)).await)
    }
}
