# Installation

## Library

Add `surql` as a dependency:

```shell
cargo add oneiriq-surql
```

Or, in `Cargo.toml`:

```toml
[dependencies]
oneiriq-surql = "0.1"
```

## Feature flags

| Feature        | Default | Description                                                                 |
|----------------|---------|-----------------------------------------------------------------------------|
| `client`       | yes     | Enables the async SurrealDB client (pulls in `tokio`, `surrealdb`, `reqwest`). |
| `cli`          | no      | Enables the `surql` binary (implies `client`, adds `clap` + `tracing-subscriber`). |
| `cache-redis`  | no      | Enables the Redis cache backend (adds `redis`).                             |

```toml
[dependencies]
oneiriq-surql = { version = "0.1", default-features = false }   # library-only, no client
# or
oneiriq-surql = { version = "0.1", features = ["cli"] }         # binary + client
```

## CLI

```shell
cargo install oneiriq-surql --features cli
```

## Requirements

- Rust 1.90 or newer.
- For the client feature: SurrealDB 2.0 or newer.

## What's next

- **[Quick Start](quickstart.md)** -- your first schema and migration.
- **[Schema Definition](schema.md)** -- the full schema DSL reference.
