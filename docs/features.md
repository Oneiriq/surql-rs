# Feature flags

`oneiriq-surql` is a large crate with several orthogonal subsystems.
Every optional dependency lives behind a feature flag so library-only
consumers (e.g. schema / migration tooling that renders SurrealQL but
never opens a socket) do not pull in `tokio`, `surrealdb`, `redis`, or
any binary-only dependencies.

## Summary

| Feature        | Default | Implies                | Pulls in                                                   | What you get                                                                 |
|----------------|---------|------------------------|------------------------------------------------------------|------------------------------------------------------------------------------|
| `client`       | yes     | -                      | `tokio`, `surrealdb` 3.x, `reqwest`, `futures`             | `DatabaseClient`, async CRUD, executor, graph helpers, transaction buffer.   |
| `cli`          | no      | `client`, `orchestration`, `settings` | `clap`, `tracing-subscriber`, `comfy-table`, `colored` | The `surql` binary (`migrate`, `schema`, `db`, `orchestrate`).              |
| `cache`        | no      | -                      | `tokio`, `async-trait`                                     | `MemoryCache` backend, `CacheManager`, global cache registry.                |
| `cache-redis`  | no      | `cache`                | `redis`                                                    | `RedisCache` backend for the cache manager.                                  |
| `settings`     | no      | -                      | `dotenvy`, `toml`                                          | Layered `Settings` / `SettingsBuilder` (env, `.env`, `Cargo.toml` metadata). |
| `orchestration`| no      | `client`               | `async-trait`                                              | Multi-database deployment strategies, environment registry, health checks.   |
| `watcher`      | no      | -                      | `notify`, `tokio`, `tokio-util`                            | Filesystem watcher for schema / migration hot-reload.                        |

## Picking a profile

### Library-only (render SurrealQL, no network)

```toml
[dependencies]
oneiriq-surql = { version = "0.2", default-features = false }
```

Gives you the schema DSL, migration renderer, query builder, and typed
result helpers. Everything under `types::`, `schema::`, `query::*`
(except `executor`, `crud`, `typed`, `graph`), `migration::models /
diff / generator / versioning / discovery`, and the parser remains
available.

### Default async client

```toml
[dependencies]
oneiriq-surql = "0.2"
```

Equivalent to `features = ["client"]`. Brings in `tokio`, `surrealdb`,
and the full async surface (`DatabaseClient`, `executor::fetch_all`,
`crud::*`, `graph::*`, `batch::*_many`, `Transaction`).

### CLI / service binary

```toml
[dependencies]
oneiriq-surql = { version = "0.2", features = ["cli"] }
```

Implies `client`, `orchestration`, and `settings`. Adds the `surql`
binary. See [CLI reference](cli.md).

### Cache layer

```toml
[dependencies]
oneiriq-surql = { version = "0.2", features = ["cache"] }
# or with Redis:
oneiriq-surql = { version = "0.2", features = ["cache-redis"] }
```

`cache` enables `MemoryCache` + `CacheManager`. `cache-redis`
additionally enables the `redis` backend (implies `cache`).

### Orchestration (standalone)

```toml
[dependencies]
oneiriq-surql = { version = "0.2", features = ["orchestration"] }
```

Pulls in the async client (`orchestration` requires live connections)
plus the `EnvironmentRegistry`, `MigrationCoordinator`, and the four
deployment strategies (sequential, parallel, rolling, canary).

### Settings

```toml
[dependencies]
oneiriq-surql = { version = "0.2", features = ["settings"] }
```

Enables the layered `Settings` loader (env vars, `.env`, `Cargo.toml`
`[package.metadata.surql]`). Pure dependency - does not require a
client.

### Schema watcher

```toml
[dependencies]
oneiriq-surql = { version = "0.2", features = ["watcher"] }
```

Filesystem watcher for schema / migration files - useful for
`cargo watch`-style development loops.

## Build-time guarantees

- `#![deny(missing_docs)]` is enforced on every feature combination.
- `#![forbid(unsafe_code)]` at the crate root.
- `cargo doc --no-deps --all-features` succeeds with `-D warnings`.
- `cargo clippy --all-targets --all-features -- -D warnings` is part
  of CI and the pre-push hook (see `CONTRIBUTING.md`).

## What's next

- [Installation](installation.md) - crate + binary install recipes.
- [CLI reference](cli.md) - subcommand tree.
- [Query UX helpers](query-ux.md) - `type_record`, `extract_many`,
  `aggregate_records`, `Query::execute`.
