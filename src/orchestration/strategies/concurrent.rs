//! Concurrent fan-out shared by the parallel, rolling, and canary
//! strategies.
//!
//! Every environment of a batch is deployed on its own task, and the
//! batch is only over when every task has finished. A task that panics
//! becomes a failed result for its environment instead of ending the
//! batch early: returning early would either leave the other deployments
//! running detached with their results thrown away (a spawned handle) or
//! abort them in the middle of a migration (a dropped `JoinSet`).

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::error;

use crate::orchestration::coordinator::DeploymentPlan;
use crate::orchestration::environment::EnvironmentConfig;
use crate::orchestration::result::{DeploymentResult, DeploymentStatus};
use crate::orchestration::strategies::deploy_to_environment;

/// Deploy `plan` to every environment in `envs` concurrently, at most
/// `max_concurrent` at a time when given, and return one result per
/// environment in the order of `envs`.
pub(crate) async fn deploy_all(
    envs: &[EnvironmentConfig],
    plan: &DeploymentPlan,
    max_concurrent: Option<usize>,
) -> Vec<DeploymentResult> {
    let limiter = max_concurrent.map(|n| Arc::new(Semaphore::new(n.max(1))));
    let mut tasks = JoinSet::new();
    let mut slot_of = HashMap::with_capacity(envs.len());
    for (slot, env) in envs.iter().cloned().enumerate() {
        let plan = plan.clone();
        let limiter = limiter.clone();
        let handle = tasks.spawn(async move {
            // The semaphore is never closed, so `acquire_owned` cannot
            // fail; if it ever did, deploying unthrottled is still correct.
            let _permit = match limiter {
                Some(limiter) => limiter.acquire_owned().await.ok(),
                None => None,
            };
            deploy_to_environment(&env, &plan).await
        });
        slot_of.insert(handle.id(), slot);
    }

    let mut results: Vec<Option<DeploymentResult>> = envs.iter().map(|_| None).collect();
    while let Some(joined) = tasks.join_next_with_id().await {
        let (id, result) = match joined {
            Ok((id, result)) => (id, Some(result)),
            Err(err) => {
                error!(error = %err, "deployment_task_failed");
                (err.id(), None)
            }
        };
        if let Some(entry) = slot_of.get(&id).and_then(|slot| results.get_mut(*slot)) {
            *entry = result;
        }
    }

    envs.iter()
        .zip(results)
        .map(|(env, result)| result.unwrap_or_else(|| task_lost(env)))
        .collect()
}

/// Result for an environment whose deployment task ended without
/// reporting (it panicked or was cancelled). What it applied is unknown,
/// so auto-rollback leaves it alone.
fn task_lost(env: &EnvironmentConfig) -> DeploymentResult {
    let now = Utc::now();
    DeploymentResult::builder(&env.name, DeploymentStatus::Failed, now)
        .completed_at(now)
        .error(
            "deployment task ended without a result; the migrations it applied are unknown, \
             check the environment's migration history",
        )
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::ConnectionConfig;
    use crate::orchestration::environment::EnvironmentRegistry;

    fn env(name: &str) -> EnvironmentConfig {
        let cfg = ConnectionConfig::builder()
            .url("ws://127.0.0.1:65535")
            .namespace("ns")
            .database(name)
            .build()
            .unwrap();
        EnvironmentConfig::builder(name, cfg).build().unwrap()
    }

    #[tokio::test]
    async fn returns_one_result_per_environment_in_order() {
        let envs = vec![env("c"), env("a"), env("b")];
        let plan = DeploymentPlan::builder(EnvironmentRegistry::new())
            .dry_run(true)
            .build();
        for limit in [None, Some(1), Some(8)] {
            let results = deploy_all(&envs, &plan, limit).await;
            let names: Vec<&str> = results.iter().map(|r| r.environment.as_str()).collect();
            assert_eq!(names, vec!["c", "a", "b"]);
        }
    }

    #[test]
    fn a_lost_task_is_a_failure_with_nothing_to_roll_back() {
        let result = task_lost(&env("prod"));
        assert_eq!(result.status, DeploymentStatus::Failed);
        assert!(result.applied_versions.is_empty());
    }
}
