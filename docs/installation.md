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
| `client`        | yes     | Async SurrealDB client (`tokio`, `surrealdb` 3.x).        |
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
```

## CLI

```shell
cargo install oneiriq-surql --features cli
```

Subcommand reference: [CLI](cli.md).

## Requirements

- Rust 1.90 or newer.
- For the `client` feature: SurrealDB 3.0 or newer.

## What's next

- **[Quick Start](quickstart.md)** -- your first schema and migration.
- **[Schema Definition](schema.md)** -- the full schema DSL reference.
- **[Feature flags](features.md)** -- picking the right profile.
