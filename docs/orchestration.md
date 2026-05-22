# Orchestration

The `orchestration` module turns single-database migration deployment
into a planned, multi-environment rollout. It is the runtime
counterpart to the `migrate` CLI commands and is intended for
scenarios where the same schema lives in several databases (per-tenant
instances, dev / staging / prod tiers, blue / green pairs).

The module is gated behind the `orchestration` feature, which implies
`client`.

```toml
[dependencies]
oneiriq-surql = { version = "0.2", features = ["orchestration"] }
```

## Concepts

| Type                       | Role                                                                                                       |
|----------------------------|------------------------------------------------------------------------------------------------------------|
| `EnvironmentConfig`        | Connection details + metadata (`name`, `connection`, `priority`, `tags`, `require_approval`).              |
| `EnvironmentRegistry`      | Process-wide async registry of named environments.                                                         |
| `DeploymentPlan`           | An ordered list of environment names, the migrations to apply, and strategy parameters.                    |
| `DeploymentStrategy`       | Async trait with a single `deploy(plan)` method. Concrete strategies vary in fan-out and failure handling. |
| `DeploymentCoordinator`    | Wraps an `Arc<dyn DeploymentStrategy>` and runs a plan against the registry.                               |
| `DeploymentResult`         | Per-environment outcome (status, migrations applied, error, duration).                                     |
| `DeploymentStatus`         | `Pending`, `InProgress`, `Success`, `Failed`, `RolledBack`.                                                |
| `HealthCheck`              | Reachability probe + migration-table existence check for an `EnvironmentConfig`.                           |

## Built-in strategies

The `strategies` submodule exports four concrete `DeploymentStrategy`
implementations, selectable by name through `StrategyKind`:

- **Sequential** runs environments one at a time and short-circuits on
  the first failure. The safe default.
- **Parallel** fans out to every environment concurrently with a
  caller-supplied `max_concurrent` bound.
- **Rolling** deploys in fixed-size batches; the next batch starts
  only after the previous one finishes.
- **Canary** deploys to a percentage of environments first, then fans
  out to the remainder only if the canary subset succeeded.

`DeploymentCoordinator::with_strategy_label` is the convenience
constructor that resolves a `StrategyKind` plus its parameters
(`batch_size`, `canary_percentage`, `max_concurrent`) to the concrete
`Arc<dyn DeploymentStrategy>`.

## Quick start

```rust
use std::sync::Arc;
use surql::connection::ConnectionConfig;
use surql::orchestration::{
    DeploymentCoordinator, DeploymentPlan, EnvironmentConfig, EnvironmentRegistry,
    strategies::SequentialStrategy,
};

let registry = EnvironmentRegistry::new();

let dev = EnvironmentConfig::builder("dev", ConnectionConfig::default()).build()?;
let prod = EnvironmentConfig::builder("prod", ConnectionConfig::default())
    .require_approval(true)
    .priority(100)
    .build()?;

registry.register(dev).await;
registry.register(prod).await;

let coordinator = DeploymentCoordinator::new(
    registry.clone(),
    Arc::new(SequentialStrategy::new()),
);

let plan = DeploymentPlan::builder(registry)
    .environments(["dev", "prod"])
    .migrations(load_pending_migrations()?)
    .build()?;

let results = coordinator.deploy(&plan).await?;
for (env, outcome) in results {
    println!("{env}: {:?} ({} migrations)", outcome.status, outcome.migrations_applied);
}
```

`coordinator.deploy(&plan)` returns
`Result<HashMap<String, DeploymentResult>>`. Per-environment failures
are recorded in the map with `status = DeploymentStatus::Failed`; the
top-level `Result` only errors when environments cannot be resolved,
when the pre-flight health check fails, or when the strategy itself
raises a fatal error.

## Health checks

```rust
use surql::orchestration::{check_environment_health, verify_connectivity};

let env = registry.get("prod").await.expect("prod registered");

let status = check_environment_health(&env).await?;
if !status.is_healthy {
    eprintln!("{} unhealthy: {:?}", status.environment, status.error);
}

let reachable = verify_connectivity(&env).await?;
```

`HealthStatus` is a struct (not an enum) carrying `environment`,
`is_healthy`, `can_connect`, `migration_table_exists`, and an optional
`error` message. `check_environment_health` is the end-to-end probe;
`verify_connectivity` only confirms the client can open a session.

The coordinator will run `verify_all_environments` automatically when
the plan's `verify_health` flag is set; unhealthy environments abort
the deploy before any migration runs.
