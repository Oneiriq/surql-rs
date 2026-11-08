# Installation

## Library

Add `surql` as a dependency:

```shell
cargo add oneiriq-surql
```

Or, in `Cargo.toml`:

```toml
[dependencies]
oneiriq-surql = "0.2"
```

## Feature flags

Short overview; the full matrix and recipes live on the
[Feature flags](features.md) page.

| Feature         | Default | What it adds                                              |
|-----------------|---------|-----------------------------------------------------------|
| `client`        | yes     | Async SurrealDB client (`tokio`, `surrealdb` 4.x) with `native-tls`. |
| `client-rustls` | no      | Same client surface but with pure-Rust TLS (no `openssl-sys`). |
| `client-grpc`   | no      | The gRPC transport (`grpc://`, `grpcs://`, SurrealDB 3.3+), on top of `client-rustls`. |
| `cli`           | no      | `surql` binary (implies `client`, `orchestration`, `settings`). |
| `cache`         | no      | In-process `MemoryCache` backend + `CacheManager`.        |
| `cache-redis`   | no      | Redis backend for the cache manager (implies `cache`).    |
| `settings`      | no      | Layered `Settings` / `SettingsBuilder`.                   |
| `orchestration` | no      | Multi-database deployment strategies + environment registry. |
| `watcher`       | no      | Filesystem watcher for schema / migration files.          |

```toml
[dependencies]
# library-only, no client
oneiriq-surql = { version = "0.2", default-features = false }

# binary + client
oneiriq-surql = { version = "0.2", features = ["cli"] }

# async client, pure-Rust TLS (no libssl-dev on the build host)
oneiriq-surql = { version = "0.2", default-features = false, features = ["client-rustls"] }
```

## CLI

```shell
cargo install oneiriq-surql --features cli
```

Subcommand reference: [CLI](cli.md).

## Requirements

- Rust 1.95 or newer.
- For the `client` feature: SurrealDB 3.0 or newer, plus a system
  TLS stack (`libssl-dev` on Linux, `Security.framework` on macOS).
- For the `client-rustls` feature: SurrealDB 3.0 or newer. No system
  TLS stack required.
- The client is the SurrealDB 4 Rust SDK (`surrealdb` 4.0), which
  connects to 3.x and 4.x servers alike. A 3.x SDK refuses a 4.0 server,
  so upgrade the crate before the server.
- Server versions: the crate is built and tested against SurrealDB 3.3,
  and a 3.0 server works with two exceptions. Values nested more than 16
  levels deep need 3.1 (`encoding::json::decode`). The 3.3 additions need
  3.3: `LIGHTWEIGHT` relations, `INLINE` caches and fields, the access
  `AUDIENCE` and `CONTEXT` clauses, `SELECT ... FOR UPDATE`, the
  `segment` tokenizer and the gRPC transport.
- Embedded engines (`mem://`, and `surrealkv://` with the SDK's
  `kv-surrealkv` feature) run SurrealDB 4.0. A 4.0 engine refuses a
  datastore that a 3.x release wrote unless the connection opts in with
  `ConnectionConfig::builder().experimental_upgrade_from_3x(true)`;
  without it, `connect` fails with `SurqlError::DatastoreUpgradeRefused`
  and leaves the datastore as it was. The upgrade is experimental in
  SurrealDB 4.0 and one-way, so back the datastore up first.
- SurrealDB 4 returns a statement without `ORDER BY` in no particular
  order. The crate's own reads that promise an order ask for it; a query
  you build yourself needs `order_by` wherever the order matters,
  including before `limit` and `offset`.

## What's next

- **[Quick Start](quickstart.md)** -- your first schema and migration.
- **[Schema Definition](schema.md)** -- the full schema DSL reference.
- **[Feature flags](features.md)** -- picking the right profile.
