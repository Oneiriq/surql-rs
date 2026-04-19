# surql-rs

A code-first database toolkit for [SurrealDB](https://surrealdb.com/).
Define schemas, generate migrations, build queries, and perform typed CRUD
-- all from Rust.

> Rust port of [surql-py](https://github.com/Oneiriq/surql-py) (Python) and
> [@oneiriq/surql](https://github.com/Oneiriq/surql) (TypeScript / Deno).
> 1:1 feature parity is the target.

## Features

- **Code-First Migrations** -- Schema changes defined in code with automatic
  migration generation. Files use a portable `.surql` format with
  `-- @up` / `-- @down` section markers.
- **Type-Safe Query Builder** -- Immutable fluent API with operator-typed
  `where_`, expression helpers, serde integration, and first-class
  `Query::execute` / `Query::select_expr`.
- **Query UX Helpers** -- `type_record` / `type_thing`, `extract_many` /
  `has_result`, `aggregate_records` + `AggregateOpts` hoisted to the
  crate root for ergonomic imports.
- **Vector Search** -- HNSW and MTREE index support with 8 distance metrics
  and EFC/M tuning.
- **Graph Traversal** -- Native SurrealDB graph features with edge
  relationships and [v3-compatible arrow chains](v3-patterns.md).
- **Schema Visualization** -- Mermaid, GraphViz, and ASCII diagrams with
  modern / dark / forest / minimal themes.
- **Async-First** -- Tokio-based client on `surrealdb` 3.x with
  connection pooling, retry logic, and buffered transactions.
- **CLI Tools** -- Full `surql` binary (`migrate`, `schema`, `db`,
  `orchestrate`) under the `cli` feature.
- **Cache, Settings, Orchestration, Watcher** -- Opt-in feature flags
  for cross-cutting concerns. See [Feature flags](features.md).

## Quick Start

```shell
cargo add oneiriq-surql
```

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

## Documentation

- **[Installation](installation.md)** -- getting the crate installed.
- **[Quick Start](quickstart.md)** -- your first schema and migration.
- **[Feature flags](features.md)** -- picking the right profile.
- **[Schema Definition](schema.md)** -- the schema DSL in depth.
- **[Migrations](migrations.md)** -- diff-based migration generation, file
  format, and versioning.
- **[Query Builder](queries.md)** -- immutable fluent queries.
- **[Query UX helpers](query-ux.md)** -- `type_record`, `extract_many`,
  `aggregate_records`, `Query::execute`.
- **[Query Hints](query_hints.md)** -- INDEX / PARALLEL / TIMEOUT / FETCH /
  EXPLAIN hints.
- **[SurrealDB v3 patterns](v3-patterns.md)** -- what changed when
  rebasing on the 3.x driver.
- **[Visualization](visualization.md)** -- Mermaid / GraphViz / ASCII
  diagrams.
- **[CLI](cli.md)** -- the `surql` binary.
- **[Upgrading 0.1 -> 0.2](migration.md)** -- API deltas.
- **[Changelog](changelog.md)** -- release history.
- **[API reference](https://docs.rs/oneiriq-surql)** -- generated rustdoc.

## Sister projects

- **Python**: [surql-py](https://github.com/Oneiriq/surql-py)
- **TypeScript / Deno / Node.js**: [surql](https://github.com/Oneiriq/surql)
- **Go**: [surql-go](https://github.com/Oneiriq/surql-go)

## License

[Apache License 2.0](https://github.com/Oneiriq/surql-rs/blob/main/LICENSE).
