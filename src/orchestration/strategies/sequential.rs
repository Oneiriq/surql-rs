//! Sequential deployment strategy.
//!
//! Deploys the plan's migrations to each environment one after the
//! other, stopping on the first failure. Port of
//! `surql.orchestration.strategy.SequentialStrategy`.

use async_trait::async_trait;
use tracing::{info, warn};

use crate::error::Result;
use crate::orchestration::coordinator::DeploymentPlan;
use crate::orchestration::result::{DeploymentResult, DeploymentStatus};
use crate::orchestration::strategies::{
    deploy_to_environment, resolve_plan_environments, DeploymentStrategy,
};

/// Deploy sequentially, environment-by-environment.
///
/// Stops at the first failed environment to match Python semantics.
#[derive(Debug, Default, Clone, Copy)]
pub struct SequentialStrategy;

impl SequentialStrategy {
    /// Construct a new sequential strategy.
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl DeploymentStrategy for SequentialStrategy {
    async fn deploy(&self, plan: &DeploymentPlan) -> Result<Vec<DeploymentResult>> {
        info!(
            count = plan.environments.len(),
            "sequential_deployment_started"
        );
        let envs = resolve_plan_environments(plan).await?;
        let mut out = Vec::with_capacity(envs.len());
        for env in &envs {
            let result = deploy_to_environment(env, plan).await;
            let status = result.status;
            out.push(result);
            if status == DeploymentStatus::Failed {
                warn!(environment = %env.name, "sequential_deployment_stopped_on_failure");
                break;
            }
        }
        Ok(out)
    }
}
