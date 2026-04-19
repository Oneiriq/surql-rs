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
  `where_`, expression helpers, and serde integration.
- **Vector Search** -- HNSW and MTREE index support with 8 distance metrics
  and EFC/M tuning.
- **Graph Traversal** -- Native SurrealDB graph features with edge
  relationships.
- **Schema Visualization** -- Mermaid, GraphViz, and ASCII diagrams with
  modern / dark / forest / minimal themes.
- **CLI Tools** -- Migrations, schema inspection, validation, database
  management *(planned)*.
- **Async-First** -- Tokio-based client with connection pooling and retry
  logic *(planned for 0.2)*.

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
- **[Schema Definition](schema.md)** -- the schema DSL in depth.
- **[Migrations](migrations.md)** -- diff-based migration generation, file
  format, and versioning.
- **[Query Builder](queries.md)** -- immutable fluent queries.
- **[Query Hints](query_hints.md)** -- INDEX / PARALLEL / TIMEOUT / FETCH /
  EXPLAIN hints.
- **[Visualization](visualization.md)** -- Mermaid / GraphViz / ASCII
  diagrams.
- **[CLI](cli.md)** -- the `surql` binary *(planned)*.
- **[Changelog](changelog.md)** -- release history.
- **[API reference](https://docs.rs/surql)** -- generated rustdoc.

## Sister projects

- **Python**: [surql-py](https://github.com/Oneiriq/surql-py)
- **TypeScript / Deno / Node.js**: [surql](https://github.com/Oneiriq/surql)
- **Go**: [surql-go](https://github.com/Oneiriq/surql-go)

## License

[Apache License 2.0](https://github.com/Oneiriq/surql-rs/blob/main/LICENSE).
