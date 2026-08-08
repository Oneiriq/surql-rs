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

let code = SchemaSnapshot::from_all_parts(
    registry.tables().into_values(),
    registry.edges().into_values(),
    registry.buckets().into_values(),
);
let db = SchemaSnapshot::from_parts(db_tables, db_edges);
let changes = diff_schemas(&code, &db);
```

`from_parts` takes tables and edges, `from_all_parts` adds buckets, and
`SchemaSnapshot::new()` gives an empty one to fill field by field.
Prefer a constructor over a struct literal: the struct gains a kind of
definition from time to time, and a literal stops compiling when it
does.

### Against a live database

What a database holds can be read back and compared with what the code
declares. `parse_db_info` turns an `INFO FOR DB` response into a
`DatabaseInfo`, whose `tables`, `edges`, `buckets`, `analyzers` and
`accesses` are keyed by name:

```rust
use surql::schema::parser::{parse_db_info, parse_table_full};
use surql::migration::diff::{diff_schemas, SchemaSnapshot};

let info = parse_db_info(&client.query("INFO FOR DB").await?)?;
```

The composition is two levels, and which is which matters. `INFO FOR
DB` carries a table's mode and permissions but **no fields**, so a diff
built on it alone reports every table as fieldless and every column as
missing. `INFO FOR TABLE` carries the fields, indexes and events.
`parse_table_full` joins them:

```rust
// The `DEFINE TABLE ...` line from INFO FOR DB, and the INFO FOR
// TABLE response for the same table.
let define = info.tables.get("user").map(|t| t.to_surql()).unwrap_or_default();
let table = parse_table_full("user", &define, &info_for_table)?;
```

Assemble the tables you care about into a snapshot and diff it:

```rust
let live = SchemaSnapshot::from_all_parts(live_tables, live_edges, live_buckets);
let changes = diff_schemas(&code, &live);
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
