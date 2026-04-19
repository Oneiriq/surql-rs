//! Rolling (batched) deployment strategy.
//!
//! Deploys the plan's migrations in successive batches with a short
//! inter-batch sleep, stopping if any batch fails. Port of
//! `surql.orchestration.strategy.RollingStrategy`.

use std::time::Duration;

use async_trait::async_trait;
use tokio::task::JoinSet;
use tracing::{error, info};

use crate::error::Result;
use crate::orchestration::coordinator::DeploymentPlan;
use crate::orchestration::environment::EnvironmentConfig;
use crate::orchestration::result::{DeploymentResult, DeploymentStatus};
use crate::orchestration::strategies::{
    deploy_to_environment, resolve_plan_environments, DeploymentStrategy,
};

/// Deploy in fixed-size batches with a pause between them.
#[derive(Debug, Clone, Copy)]
pub struct RollingStrategy {
    batch_size: usize,
    batch_pause: Duration,
}

impl Default for RollingStrategy {
    fn default() -> Self {
        Self::with_batch_size(1)
    }
}

impl RollingStrategy {
    /// Construct a new rolling strategy with the supplied batch size.
    ///
    /// A `batch_size` of `0` is coerced to `1`.
    pub fn with_batch_size(batch_size: usize) -> Self {
        Self {
            batch_size: batch_size.max(1),
            batch_pause: Duration::from_secs(1),
        }
    }

    /// Override the default inter-batch pause (default = 1 second).
    pub fn with_batch_pause(mut self, pause: Duration) -> Self {
        self.batch_pause = pause;
        self
    }

    /// Configured batch size.
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
}

#[async_trait]
impl DeploymentStrategy for RollingStrategy {
    async fn deploy(&self, plan: &DeploymentPlan) -> Result<Vec<DeploymentResult>> {
        info!(
            count = plan.environments.len(),
            batch_size = self.batch_size,
            "rolling_deployment_started"
        );

        let envs = resolve_plan_environments(plan).await?;
        let mut out: Vec<DeploymentResult> = Vec::with_capacity(envs.len());

        let mut index = 0usize;
        let total = envs.len();
        let mut batch_num = 0usize;
        while index < total {
            let end = (index + self.batch_size).min(total);
            let batch: Vec<EnvironmentConfig> = envs[index..end].to_vec();
            batch_num += 1;
            info!(batch = batch_num, size = batch.len(), "deploying_batch");

            let mut join = JoinSet::new();
            for (local_idx, env) in batch.into_iter().enumerate() {
                let plan = plan.clone();
                join.spawn(async move {
                    let result = deploy_to_environment(&env, &plan).await;
                    (local_idx, result)
                });
            }

            let mut batch_results: Vec<Option<DeploymentResult>> =
                (0..(end - index)).map(|_| None).collect();
            while let Some(res) = join.join_next().await {
                match res {
                    Ok((local_idx, result)) => batch_results[local_idx] = Some(result),
                    Err(join_err) => {
                        return Err(crate::error::SurqlError::Orchestration {
                            reason: format!("task join failed: {join_err}"),
                        });
                    }
                }
            }

            let batch_results: Vec<DeploymentResult> =
                batch_results.into_iter().flatten().collect();
            let failed = batch_results
                .iter()
                .any(|r| r.status == DeploymentStatus::Failed);
            out.extend(batch_results);

            if failed {
                error!(batch = batch_num, "batch_failed_stopping");
                break;
            }

            index = end;
            if index < total && !self.batch_pause.is_zero() {
                tokio::time::sleep(self.batch_pause).await;
            }
        }

        Ok(out)
    }
}
