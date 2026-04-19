//! Parallel deployment strategy.
//!
//! Deploys the plan's migrations to every environment concurrently,
//! bounded by a capacity limiter. Port of
//! `surql.orchestration.strategy.ParallelStrategy`.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Semaphore;
use tracing::info;

use crate::error::Result;
use crate::orchestration::coordinator::DeploymentPlan;
use crate::orchestration::result::DeploymentResult;
use crate::orchestration::strategies::{
    deploy_to_environment, resolve_plan_environments, DeploymentStrategy,
};

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
        let limiter = Arc::new(Semaphore::new(self.max_concurrent));
        let plan = plan.clone();
        let mut handles = Vec::with_capacity(envs.len());

        for (idx, env) in envs.into_iter().enumerate() {
            let limiter = limiter.clone();
            let plan = plan.clone();
            handles.push(tokio::spawn(async move {
                let _permit = limiter
                    .acquire_owned()
                    .await
                    .expect("semaphore permits are never closed here");
                let result = deploy_to_environment(&env, &plan).await;
                (idx, result)
            }));
        }

        let mut buffer: Vec<Option<DeploymentResult>> = (0..handles.len()).map(|_| None).collect();
        for handle in handles {
            match handle.await {
                Ok((idx, result)) => buffer[idx] = Some(result),
                Err(join_err) => {
                    return Err(crate::error::SurqlError::Orchestration {
                        reason: format!("task join failed: {join_err}"),
                    });
                }
            }
        }

        Ok(buffer.into_iter().flatten().collect())
    }
}
