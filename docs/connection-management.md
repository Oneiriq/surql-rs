# Connection management

`DatabaseClient` is the entry point to SurrealDB; the rest of the
`connection` module gives you ergonomic ways to share that client
across an async call tree, manage a named pool of connections, drive
authentication state, run live queries, and bracket work inside a
transaction.

Everything here requires one of the `client`, `client-rustls`, or
`client-wasm` features.

## Task-scoped current client

`connection::context` exposes a task-local "current database" handle
backed by `tokio::task_local!`. Code deep in a call tree can fetch the
active client without threading it through every function signature.

```rust
use std::sync::Arc;
use surql::connection::{connection_scope, get_db, DatabaseClient, ConnectionConfig};

let client = Arc::new(DatabaseClient::new(ConnectionConfig::default())?);
client.connect().await?;

connection_scope(client.clone(), async {
    let db = get_db()?;
    db.query("RETURN 1;").await?;
    Ok::<_, surql::SurqlError>(())
}).await?;
```

- `get_db` returns `SurqlError::Context` outside any scope.
- `has_db` returns `false` outside any scope.
- `connection_override` swaps the current value for the duration of an
  inner future; useful for per-request overrides.
- `set_db` / `clear_db` mutate the current scope's slot and fail
  outside a scope.

Spawned `tokio::spawn` tasks inherit the task-local automatically via
`TaskLocalFuture` provided you pass the future through one of the
`connection::context` helpers.

## Connection registry

`ConnectionRegistry` holds a name-to-`Arc<DatabaseClient>` map for code
paths that need to address several databases by name (orchestration,
multi-tenant batch jobs, the CLI).

```rust
use surql::connection::{set_registry, get_registry, ConnectionRegistry, ConnectionConfig};

let registry = ConnectionRegistry::new();

registry.register("dev",  ConnectionConfig::default(), /* connect */ true,  /* set_default */ true).await?;
registry.register("prod", ConnectionConfig::default(), /* connect */ false, /* set_default */ false).await?;

set_registry(registry)?;

let registry = get_registry();
let dev = registry.get("dev").await?;
```

The registry is cheap to clone because it stores `Arc<DatabaseClient>`
values internally. `register` returns the `Arc<DatabaseClient>` it
just inserted, so callers can hold a handle directly without a second
`get` call. The first registered connection auto-promotes to the
default when `set_default` is left `false` for every entry.

## Auth manager and token refresh

`AuthManager` wraps the client's session token and exposes the cached
`TokenAuth` returned by the last successful signin / signup. It is
`Clone`-cheap; the cached state is shared across clones through an
internal `Arc` so a CLI task and a background refresher can observe
the same value.

```rust
use surql::connection::{AuthManager, auth::RootCredentials};

let auth = AuthManager::new();
auth.signin(&client, &RootCredentials::new("root", "root")).await?;

if let Some(token) = auth.current_token().await {
    tracing::info!(token = %token.token, "session active");
}

if let Some(ty) = auth.auth_type().await {
    tracing::info!(?ty, "auth type");
}

if !auth.is_authenticated().await {
    auth.refresh(&client).await?;
}
```

`signin` accepts any `&dyn Credentials` (`RootCredentials`,
`NamespaceCredentials`, `DatabaseCredentials`, `ScopeCredentials`).
`authenticate(client, token)` adopts an externally-issued JWT;
`invalidate(client)` ends the session and drops the cached token;
`refresh(client)` re-applies the cached token against `client`
(useful after a reconnect because the v3 SDK has no dedicated refresh
endpoint).

## Streaming and live queries

`LiveQuery` is the typed subscription handle; `StreamingManager` holds
a registry of running subscriptions keyed by `SubscriptionId`.

```rust
use futures::StreamExt;
use surql::connection::{LiveQuery, StreamingManager};

let mut live: LiveQuery<User> = LiveQuery::start(&client, "user").await?;
while let Some(event) = live.next().await {
    handle_change(event?);
}

// Or hand off to the manager so the subscription can outlive the
// caller frame and be cancelled by id later.
let manager = StreamingManager::new();
let id = manager.spawn::<User, _>(&client, "user", |event| async move {
    handle_change(event);
}).await?;
manager.kill(id).await;
```

`manager.count`, `manager.ids`, and `manager.drain_all` give you the
operational surface needed to enumerate or tear down every active
subscription on a graceful exit.

## Transactions

`Transaction` is a server-side transaction handle opened with
`Transaction::begin(client)`. Statements queued via `execute` run
inside the same `BEGIN TRANSACTION` / `COMMIT TRANSACTION` block on
the server.

```rust
use surql::connection::Transaction;

let mut tx = Transaction::begin(&client).await?;
tx.execute("UPDATE user SET active = false WHERE last_seen < $cutoff;").await?;
tx.execute("CREATE audit SET reason = 'expired', count = 17;").await?;
tx.commit().await?;
```

Call `tx.rollback().await` instead of `commit` to discard the queued
statements. `tx.state()` returns `TransactionState::Active`,
`Committed`, or `RolledBack`; `tx.is_active()` is the boolean
shorthand.

Use `upsert_many_in_tx` from `surql::query::batch` when you want the
same atomicity for a batched `UPSERT` payload without writing the
SurrealQL yourself.
