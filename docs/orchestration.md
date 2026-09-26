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

| Type                       | Role                                                                                                             |
|----------------------------|------------------------------------------------------------------------------------------------------------------|
| `EnvironmentConfig`        | Connection details + metadata (`name`, `connection`, `priority`, `tags`, `require_approval`, `allow_destructive`). |
| `EnvironmentRegistry`      | Process-wide async registry of named environments.                                                               |
| `DeploymentPlan`           | An ordered list of environment names, the migrations to deploy, strategy parameters, and the `approved` flag.    |
| `DeploymentStrategy`       | Async trait with a single `deploy(plan)` method. Concrete strategies vary in fan-out and failure handling.       |
| `MigrationCoordinator`     | Wraps an `Arc<dyn DeploymentStrategy>` and runs a plan against the registry.                                     |
| `DeploymentResult`         | Per-environment outcome (status, `applied_versions`, `rolled_back_versions`, error, duration).                   |
| `DeploymentStatus`         | `Pending`, `InProgress`, `Success`, `Failed`, `RolledBack`.                                                      |
| `HealthCheck`              | Reachability probe + migration-table existence check for an `EnvironmentConfig`.                                 |

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

`MigrationCoordinator::with_strategy_label` is the convenience
constructor that resolves a `StrategyKind` plus its parameters
(`batch_size`, `canary_percentage`, `max_concurrent`) to the concrete
`Arc<dyn DeploymentStrategy>`. `deploy_to_environments(&plan)` does that
from the plan's own fields and deploys it.

The concurrent strategies wait for every deployment of a batch to finish
before deciding what to do next. A deployment task that panics is
reported as a failed environment (what it applied is unknown, so
auto-rollback leaves it alone); it never ends the batch early, which
would either leave other deployments running unobserved or abort them
in the middle of a migration.

## Quick start

```rust
use std::sync::Arc;
use surql::connection::ConnectionConfig;
use surql::migration::discover_migrations;
use surql::orchestration::{
    DeploymentPlan, EnvironmentConfig, EnvironmentRegistry, MigrationCoordinator,
    strategies::SequentialStrategy,
};

let registry = EnvironmentRegistry::new();

let dev = EnvironmentConfig::builder("dev", ConnectionConfig::default()).build()?;
let prod = EnvironmentConfig::builder("prod", ConnectionConfig::default())
    .require_approval(true)
    .allow_destructive(false)
    .priority(100)
    .build()?;

registry.register(dev).await;
registry.register(prod).await;

let coordinator = MigrationCoordinator::new(
    registry.clone(),
    Arc::new(SequentialStrategy::new()),
);

let plan = DeploymentPlan::builder(registry)
    .environments(["dev", "prod"])
    .migrations(discover_migrations(std::path::Path::new("migrations"))?)
    .approved(true) // prod requires approval
    .build();

let results = coordinator.deploy(&plan).await?;
for (env, outcome) in results {
    println!("{env}: {} applied {:?}", outcome.status, outcome.applied_versions);
}
```

`coordinator.deploy(&plan)` returns
`Result<HashMap<String, DeploymentResult>>`. Per-environment failures
are recorded in the map with `status = DeploymentStatus::Failed`; the
top-level `Result` only errors when environments cannot be resolved,
when an environment requires an approval the plan does not carry, when
the pre-flight health check fails, or when the strategy itself raises a
fatal error.

## What gets deployed

Hand the plan every migration you have; each environment gets only the
ones its `_migration_history` does not already record, in version order.
An environment already at v5 receives just v6 from a v1..v6 plan. The
versions actually applied are listed in `DeploymentResult::applied_versions`
(`migrations_applied` is their count). A migration whose statements or
commit fail stops that environment and marks it `Failed`; the migrations
after it are not attempted. An environment named twice in the plan is
deployed once.

A dry run does not connect to anything: it reports every plan migration
as an upper bound for each environment.

## Auto-rollback

With `auto_rollback` (the default), a failure in any environment reverts
what this deployment applied everywhere, newest first. Only
`applied_versions` are reverted: migrations an environment held before
the deployment are never touched. The outcome is recorded per
environment:

| Status        | Meaning                                                                                                   |
|---------------|-----------------------------------------------------------------------------------------------------------|
| `RolledBack`  | Deployed, then every migration it applied was reverted (`rolled_back_versions`).                          |
| `Success`     | Deployed and still deployed: the rollback did not start there, and `error` says why.                     |
| `Failed`      | Its own deployment failed (anything it applied first is reverted), or the rollback stopped partway.      |

A rollback does not start on an environment when a migration it would
revert has no `down` statements (running it would only delete the history
row), when the environment requires an approval the plan lacks, or when
the guards below refuse the `down` statements.

## Environment guards

- `require_approval: true` - the coordinator refuses the whole deployment
  before anything runs unless the plan is `approved`
  (`DeploymentPlan::builder(..).approved(true)`, or `surql orchestrate
  deploy --approve`). Dry runs need no approval.
- `allow_destructive: false` - a deployment whose pending `up` statements
  are destructive is refused on that environment before its first
  migration runs (the environment is `Failed`, which triggers
  auto-rollback elsewhere), and an auto-rollback whose `down` statements
  are destructive does not start there.

A statement is destructive when its leading keywords remove a table,
field, namespace, database, or bucket (`REMOVE` or `DROP`), change a
field's type (`ALTER FIELD ... TYPE`), or delete records (`DELETE`),
following the rollback safety analysis in `surql::migration::rollback`.

## Environments file

`EnvironmentRegistry::from_config_file` (and `configure_environments`)
load JSON like this:

```json
{
  "environments": [
    {
      "name": "production",
      "connection": { "db_url": "wss://db.example.com", "db_ns": "prod", "db": "main",
                      "db_user": "deploy", "db_pass": "..." },
      "priority": 1,
      "tags": ["prod"],
      "require_approval": true,
      "allow_destructive": false
    }
  ]
}
```

A connection needs `db_url`, `db_ns`, and `db` (or `url`, `namespace`,
`database`); the other connection fields (`db_user`, `db_pass`,
`db_timeout`, `db_max_connections`, `db_retry_*`, `enable_live_queries`,
or their names without `db_`) default. Every connection is validated.
Unknown keys and repeated environment names are errors, so a misspelt
`require_approval` cannot leave an environment unguarded.

`Debug` output of environments, registries, plans, and coordinators
never contains a password (and a `user:pass@` in a URL is redacted), so
`tracing::debug!(?plan)` is safe.

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
