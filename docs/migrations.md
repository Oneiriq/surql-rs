# Migrations

## File format

Migration files are plain `.surql` files with three section markers:

```text
-- @metadata
-- version: 20260418_193300
-- description: Add user table
-- depends_on: [20260418_180000]
-- @up
DEFINE TABLE user SCHEMAFULL;
DEFINE FIELD email ON TABLE user TYPE string;
-- @down
REMOVE TABLE IF EXISTS user;
```

The `version` pattern is `YYYYMMDD_HHMMSS`. Descriptions are slug-cased by
the generator. Every file is validated on load and includes a SHA-256
checksum for drift detection.

## Generating migrations

```rust
use surql::migration::generator::{
    create_blank_migration, generate_initial_migration,
    generate_migration_from_diffs,
};
use std::path::Path;

// Blank template
let m = create_blank_migration("add_log_table", "Add log table", Path::new("migrations"))?;

// Initial migration from a registry
let m = generate_initial_migration(&registry, Path::new("migrations"))?;

// From a precomputed diff
let m = generate_migration_from_diffs("rename_email", &diffs, Path::new("migrations"))?;
```

## Diffing

```rust
use surql::migration::diff::{diff_schemas, SchemaSnapshot};

let code = SchemaSnapshot::from_registry(&code_registry);
let db   = SchemaSnapshot { tables: db_tables, edges: db_edges };
let changes = diff_schemas(&code, &db);
```

## Discovery

```rust
use surql::migration::discovery::{discover_migrations, load_migration};
use std::path::Path;

let migrations = discover_migrations(Path::new("migrations"))?;
for m in &migrations {
    println!("{} {}", m.version, m.description);
}

let one = load_migration(Path::new("migrations/20260418_193300_add_user_table.surql"))?;
```

## Versioning + snapshots

```rust
use surql::migration::versioning::{
    create_snapshot, store_snapshot, load_snapshot, list_snapshots,
    compare_snapshots, VersionGraph,
};

let snap = create_snapshot(&registry, "after user table");
store_snapshot(&snap, Path::new("snapshots"))?;
let all = list_snapshots(Path::new("snapshots"))?;

let comparison = compare_snapshots(&all[0], &all[1]);
let mut graph = VersionGraph::new();
for s in all {
    graph.add(s);
}
```

## What's next

- **[Query Builder](queries.md)** -- immutable fluent queries for your
  migrated schema.
