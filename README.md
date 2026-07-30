# surql-rs

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.90%2B-orange)](https://www.rust-lang.org/)
[![SurrealDB](https://img.shields.io/badge/SurrealDB-3.0%2B-ff00a0)](https://surrealdb.com/)

A code-first database toolkit for [SurrealDB](https://surrealdb.com/). Define schemas, generate migrations, build queries, and perform typed CRUD -- all from Rust.

## Features

- **Code-First Migrations** - Schema changes defined in code with automatic migration generation (auto-diff, `.surql` file output with `-- @up` / `-- @down` sections, squash, hooks).
- **Type-Safe Query Builder** - Immutable fluent API with operator-typed `where_`, expression helpers, serde integration, and first-class `Query::execute` / `Query::select_expr`.
- **Query UX Helpers** - `type_record` / `type_thing`, `extract_many` / `has_result`, `aggregate_records` + `AggregateOpts` hoisted to the crate root.
- **Async-First** - Tokio-based client on `surrealdb` 3.x with connection pooling, retry logic, and buffered transactions.
- **Vector Search** - HNSW and MTREE index support with 8 distance metrics and EFC/M tuning.
- **Graph Traversal** - Native SurrealDB graph features with edge relationships (v3-compatible arrow chains).
- **Files & Buckets** - SurrealDB v3 object storage: code-first `DEFINE BUCKET` schema + migration diffing, a `FileRef` value type, and a runtime `client.bucket(name)` handle for put/get/head/delete/copy/rename/list.
- **Schema Visualization** - Mermaid, GraphViz, and ASCII diagrams with theming.
- **CLI Tools** - Full `surql` binary (`migrate`, `schema`, `db`, `bucket`, `orchestrate`) behind the `cli` feature.
- **Optional subsystems** - `cache` (memory + Redis), `settings`, `orchestration`, `watcher` feature flags.

## Quick Start

```shell
cargo add oneiriq-surql
```

Pure-Rust TLS (no `openssl-sys`, recommended for CI runners without
`libssl-dev`):

```shell
cargo add oneiriq-surql --no-default-features --features client-rustls
```

With the CLI:

```shell
cargo install oneiriq-surql --features cli
```

### TLS backends

- `client` (default) -- async client backed by `native-tls`; links
  `openssl-sys` on Linux, Security.framework on macOS.
- `client-rustls` -- same client surface, pure-Rust TLS via `rustls`
  + `webpki-roots`. No system OpenSSL needed. See
  [docs/features.md](docs/features.md#picking-a-tls-backend).

### Define a schema

```rust
use surql::schema::table::{table_schema, TableMode, unique_index};
use surql::schema::fields::{string_field, int_field, datetime_field};

let user_schema = table_schema("user")
    .mode(TableMode::Schemafull)
    .field(string_field("name"))
    .field(string_field("email").assertion("string::is::email($value)"))
    .field(int_field("age").assertion("$value >= 0 AND $value <= 150"))
    .field(datetime_field("created_at").default("time::now()").readonly(true))
    .index(unique_index("email_idx", &["email"]))
    .build()?;
```

### Execute a typed query

```rust
use surql::{DatabaseClient, type_record, extract_many};
use surql::connection::ConnectionConfig;
use surql::query::builder::Query;
use surql::query::expressions::{as_, count_all, math_mean};
use surql::types::operators::eq;

let client = DatabaseClient::new(ConnectionConfig::default())?;
client.connect().await?;

// First-class target: `type::record('user', 'alice')`.
let target = type_record("user", "alice").to_surql();

// Typed select projection + `.execute(&client)` on the builder.
let raw = Query::new()
    .select_expr(vec![
        as_(&count_all(), "total"),
        as_(&math_mean("score"), "mean_score"),
    ])
    .from_table("memory_entry")?
    .where_(&eq("status", "active"))
    .group_all()
    .execute(&client)
    .await?;

for row in extract_many(&raw) {
    println!("{row}");
}
```

### Aggregate with `AggregateOpts`

```rust
use surql::query::{aggregate_records, AggregateOpts};
use surql::query::expressions::{count_all, math_mean};
use surql::types::operators::eq;

let rows = aggregate_records(
    &client,
    "memory_entry",
    AggregateOpts {
        select: vec![
            ("total".into(), count_all()),
            ("mean_score".into(), math_mean("score")),
        ],
        where_: Some(eq("status", "active")),
        group_all: true,
        ..AggregateOpts::default()
    },
)
.await?;
```

### Files & buckets (SurrealDB v3 object storage)

Define a bucket in code, then read/write files through a typed handle. Bucket
and key are always passed as **bound** parameters (`type::file($bucket, $key)`)
— never string-interpolated — and binary payloads are bound as a native
`bytes` value, not base64.

```rust
use surql::schema::memory_bucket;          // or file_bucket / bucket_schema
use surql::query::FileData;

// Schema: `DEFINE BUCKET avatars BACKEND "memory";`
let bucket = memory_bucket("avatars");
client.query(&bucket.to_surql()?).await?;

// Runtime file ops via the client handle.
let files = client.bucket("avatars");
files.put("alice.png", FileData::bytes(image_bytes)).await?; // binary
files.put("note.txt", "hello").await?;                       // text
let text = files.get_text("note.txt").await?;
let bytes = files.get("alice.png").await?;
let _ = files.list().await?;
```

Buckets are an experimental, *hidden* v3 feature: it is **not** enabled by
`--allow-all`, and the `--allow-experimental files` *flag* form is broken (the
bare `files` argument is swallowed by the datastore positional). Enable it with
the environment variable instead:

```bash
SURREAL_CAPS_ALLOW_EXPERIMENTAL=files \
  surreal start --bind 127.0.0.1:8000 --user root --pass root --allow-all memory
```

The `bucket` CLI group and the `DatabaseClient::bucket` handle assume this.
File keys are returned in SurrealDB's **canonical** form — `FileRef::key()`
preserves the server's leading-slash key (`/a.txt`) verbatim, while
`FileRef`'s `Display` always renders a single-slash pointer (`bucket:/a.txt`).
Bucket definitions participate in schema diffing/migrations (`DEFINE` /
`ALTER` / `REMOVE BUCKET`) just like tables.

### Sessions

The Rust `surrealdb` crate (3.x) has **no multiplexed-session API**, so this
crate intentionally does not expose a `Session` type (unlike the Python /
TypeScript ports). For isolated namespace / database / auth contexts, use a
separate `DatabaseClient` per context — each owns its own connection and is
fully isolated. See the `surql::connection::session` module docs for details.

## Documentation

Full documentation at **[oneiriq.github.io/surql-rs](https://oneiriq.github.io/surql-rs/)**.

Selected pages:

- [Feature flags](https://oneiriq.github.io/surql-rs/features/) - picking a profile.
- [Query UX helpers](https://oneiriq.github.io/surql-rs/query-ux/) - the 0.2 crate-root helpers.
- [SurrealDB v3 patterns](https://oneiriq.github.io/surql-rs/v3-patterns/) - what changed rebasing on the 3.x driver.
- [CLI reference](https://oneiriq.github.io/surql-rs/cli/) - the full `surql` binary.
- [Upgrading 0.1 -> 0.2](https://oneiriq.github.io/surql-rs/migration/) - API deltas.

## Requirements

- Rust 1.90+
- SurrealDB 3.0+

## License

Apache License 2.0 - see [LICENSE](LICENSE).

## Python / TypeScript / Go

- **Python**: [surql-py](https://github.com/Oneiriq/surql-py) -- the original, reference implementation (Python 3.12+).
- **TypeScript / Deno / Node.js**: [surql](https://github.com/Oneiriq/surql) -- type-safe query builder and client.
- **Go**: [surql-go](https://github.com/Oneiriq/surql-go) -- Go port of this library, sharing the same schema + migration model.

## Support

- Documentation: [oneiriq.github.io/surql-rs](https://oneiriq.github.io/surql-rs/)
- Issues: [GitHub Issues](https://github.com/Oneiriq/surql-rs/issues)
- Changelog: [CHANGELOG.md](CHANGELOG.md)
