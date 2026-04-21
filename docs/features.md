# Feature flags

`oneiriq-surql` is a large crate with several orthogonal subsystems.
Every optional dependency lives behind a feature flag so library-only
consumers (e.g. schema / migration tooling that renders SurrealQL but
never opens a socket) do not pull in `tokio`, `surrealdb`, `redis`, or
any binary-only dependencies.

## Summary

| Feature         | Default | Implies                | Pulls in                                                   | What you get                                                                 |
|-----------------|---------|------------------------|------------------------------------------------------------|------------------------------------------------------------------------------|
| `client`        | yes     | -                      | `tokio`, `surrealdb` 3.x (`native-tls`), `reqwest` (`default-tls`), `futures` | `DatabaseClient`, async CRUD, executor, graph helpers, transaction buffer. Uses the system `native-tls` stack (`openssl-sys` on Linux). |
| `client-rustls` | no      | -                      | `tokio`, `surrealdb` 3.x (`rustls`), `reqwest` (`rustls-tls-webpki-roots`), `futures` | Same surface as `client` but with pure-Rust TLS (no `openssl-sys`). See [Picking a TLS backend](#picking-a-tls-backend). |
| `cli`           | no      | `client`, `orchestration`, `settings` | `clap`, `tracing-subscriber`, `comfy-table`, `colored` | The `surql` binary (`migrate`, `schema`, `db`, `orchestrate`).              |
| `cache`         | no      | -                      | `tokio`, `async-trait`                                     | `MemoryCache` backend, `CacheManager`, global cache registry.                |
| `cache-redis`   | no      | `cache`                | `redis`                                                    | `RedisCache` backend for the cache manager.                                  |
| `settings`      | no      | -                      | `dotenvy`, `toml`                                          | Layered `Settings` / `SettingsBuilder` (env, `.env`, `Cargo.toml` metadata). |
| `orchestration` | no      | `client`               | `async-trait`                                              | Multi-database deployment strategies, environment registry, health checks.   |
| `watcher`       | no      | -                      | `notify`, `tokio`, `tokio-util`                            | Filesystem watcher for schema / migration hot-reload.                        |

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
`crud::*`, `graph::*`, `batch::*_many`, `Transaction`). Uses the
system `native-tls` stack (links `openssl-sys` on Linux,
Security.framework on macOS).

### Async client with rustls (no `openssl-sys`)

```toml
[dependencies]
oneiriq-surql = { version = "0.2", default-features = false, features = ["client-rustls"] }
```

Same API surface as `client`, but TLS is provided by `rustls` +
`webpki-roots` instead of `native-tls`. This is the right choice for
CI runners, Alpine / distroless containers, and any environment where
you do not want to install `libssl-dev` / link against the system
OpenSSL.

### Picking a TLS backend

Pick exactly one of `client` or `client-rustls`:

|                                | `client`                      | `client-rustls`                                   |
|--------------------------------|-------------------------------|---------------------------------------------------|
| TLS stack                      | `native-tls` (+ `openssl-sys` on Linux) | `rustls` + `webpki-roots`               |
| Needs `libssl-dev` at build    | yes (on Linux)                | no                                                |
| Uses OS trust store            | yes                           | no -- ships Mozilla webpki roots                  |
| Cross-compilation friendliness | depends on target OpenSSL     | portable, pure Rust                               |
| API surface                    | identical                     | identical                                         |
| Backwards compatible           | yes (default since 0.1)       | new in 0.2.2                                      |

Enabling both at once compiles, but doubles the TLS dependency set.
If you are consuming this crate from multiple workspace members,
pick one variant and hold it consistent via a workspace-level
feature flag.

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
