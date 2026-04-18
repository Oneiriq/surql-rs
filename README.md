# surql-rs

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.90%2B-orange)](https://www.rust-lang.org/)
[![SurrealDB](https://img.shields.io/badge/SurrealDB-2.0%2B-ff00a0)](https://surrealdb.com/)

A code-first database toolkit for [SurrealDB](https://surrealdb.com/). Define schemas, generate migrations, build queries, and perform typed CRUD -- all from Rust.

## Features

- **Code-First Migrations** - Schema changes defined in code with automatic migration generation (auto-diff + `.surql` file output with `-- @up` / `-- @down` sections)
- **Type-Safe Query Builder** - Immutable fluent API with operator-typed `where_`, expression helpers, and serde integration
- **Vector Search** - HNSW and MTREE index support with 8 distance metrics and EFC/M tuning
- **Graph Traversal** - Native SurrealDB graph features with edge relationships
- **Schema Visualization** - Mermaid, GraphViz, and ASCII diagrams with theming
- **CLI Tools** - Migrations, schema inspection, validation, database management *(planned)*
- **Async-First** - Tokio-based client with connection pooling and retry logic *(planned)*

## Quick Start

```shell
cargo add surql
```

With the CLI:

```shell
cargo install surql --features cli
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

Full documentation at **[oneiriq.github.io/surql-rs](https://oneiriq.github.io/surql-rs/)**.

## Requirements

- Rust 1.90+
- SurrealDB 2.0+

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
