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
checksum; the checksum ignores a leading byte-order mark and `\r\n`
versus `\n` line endings, so a file hashes the same on every platform.
The checksum is recorded in the history table when the migration is
applied, and compared with the file from then on (see
[Edited migrations](#edited-migrations)).

Sections are split into statements on `;`, but only on a `;` that is
code: one inside a `--`, `//`, `#` or `/* */` comment, a `'…'` or `"…"`
string, a `` `…` `` or `⟨…⟩` quoted name, or a `{ }` / `( )` / `[ ]`
block does not end a statement, so `DEFINE FUNCTION` bodies and `FOR`
loops stay whole. A comment in front of a statement stays attached to
it; a piece holding only comments is not a statement. A leading UTF-8
byte-order mark is ignored.

`-- depends_on:` lists versions the migration must follow, and a
squashed migration carries `-- squashed-from:` (see below).

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

The generator never replaces an existing file. Versions have one-second
resolution, so a migration generated in a second some file in the
directory already uses gets the next free second instead. Line breaks in
a description are written as spaces, so a description cannot start a
section of its own.

## Applying and rolling back

```rust
use surql::migration::{migrate_up, migrate_down, get_migration_status, MigrateUpOptions};

let applied = migrate_up(&client, Path::new("migrations"), MigrateUpOptions::default()).await?;
let rolled_back = migrate_down(&client, Path::new("migrations"), 1).await?;
let report = get_migration_status(&client, Path::new("migrations")).await?;
```

Migrations apply in version order, with runs of digits compared as
numbers (`v9` before `v10`; timestamp versions order as strings do), and
each after the migrations it `depends_on`. A dependency cycle is an
error.

Each migration runs in one transaction together with its history
change: applying creates the `_migration_history` row for its version,
rolling back deletes it. The schema change and the history row therefore
commit or fail together. The row's record id is derived from the
version, so when two runners apply the same migration at once the
second one's transaction is rejected as a whole and its statements do
not run twice. Rolling back a migration that is not recorded as applied
fails without running its down body. A failed migration is reported as a
`Failed` status and stops the run; nothing of it was applied.

A migration with no `down` statements (a squashed or a blank one) is
refused when rolling back, rather than deleting its history row while
the schema stays.

### Edited migrations

A migration's file can change after it was applied, and the database
keeps the schema the old text produced. `get_migration_status` lists such
migrations in `report.modified` (the CLI's `migrate status` marks them
`modified`), and `migrate_up`, `surql migrate up` and orchestration
deploys refuse to run while there are any, naming each one: migrations
written after the edit may assume a schema the database does not have.

Either revert the edit, or, when it needs no applying (a comment, a
formatting change), accept it by recording the file's current checksum:

```rust
use surql::migration::{get_modified_migrations, rehash_migrations};

for m in get_modified_migrations(&client, Path::new("migrations")).await? {
    println!("{} ({}) changed since it was applied", m.version, m.path.display());
}
// Every modified migration, or pass the versions to accept.
rehash_migrations(&client, Path::new("migrations"), &[]).await?;
```

History rows written before checksums ignored line endings, or by the
Python port, hold the SHA-256 of the file's raw bytes. Those still match
on any checkout (either line ending, with or without a byte-order mark),
so upgrading does not report every existing migration as modified.

`create_rollback_plan` rolls back every applied migration newer than the
target, most recently applied first, and classifies the plan by what its
down statements can destroy (`analyze_statements` exposes the same
rules for any statement list): `REMOVE TABLE`, `REMOVE NAMESPACE`,
`REMOVE DATABASE`, `REMOVE BUCKET` and `DELETE` are `Danger`, `REMOVE
FIELD` and `ALTER FIELD … TYPE` are `Warning`. A plan that is not `Safe`
has `requires_approval` set, and `execute_rollback` refuses it until it
is approved:

```rust
use surql::migration::{create_rollback_plan, execute_rollback};

let plan = create_rollback_plan(&client, Path::new("migrations"), "20260418_180000").await?;
for issue in &plan.issues {
    println!("{}: {}", issue.safety, issue.description);
}
let result = execute_rollback(&client, plan.approve()).await?;
```

## Squashing

`squash_migrations` combines a range of migrations into one file whose
metadata lists the originals in `-- squashed-from:`. On a database that
applied the originals the squashed migration counts as applied (and on
one that recorded the squashed migration, so do its originals), so it is
never re-run. The squashed file has no down section, so it cannot be
rolled back; restore from a snapshot instead.

The optimiser (on by default) only removes definitions it can read, with
nothing in between that touches the same object: a plain `DEFINE`
followed by a `REMOVE` of the same table, field, index or event is
dropped as a pair, an earlier definition is dropped when a later one
`OVERWRITE`s it, and a later `IF NOT EXISTS` definition of an object
already defined is dropped. Data statements and anything it cannot parse
are never removed. The safety scan refuses a range containing a
`DELETE` unless `force` is set; comments in front of a statement do not
hide it.

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

`diff_schemas` returns the changes in an order that applies cleanly:
object additions first (functions, params, sequences, analyzers,
buckets), then every table and edge drop, then table changes, then edge
changes, and finally object drops in reverse. A changed index, event or
edge shape is re-defined with `OVERWRITE`, in both the forward and the
backward direction. Index and event changes are reported as
`DiffOperation::ModifyIndex` and `ModifyEvent`, both graded `Warning` by
the drift check.

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

let snap = create_snapshot(&registry, "20260418_193300", "after user table")?;
store_snapshot(&snap, Path::new("snapshots"))?;
let all = list_snapshots(Path::new("snapshots"))?;

let comparison = compare_snapshots(&all[0], &all[1]);
let mut graph = VersionGraph::new();
for m in &migrations {
    graph.add_version(m.clone(), None, None)?;
}
```

A snapshot is stored as `<version>.json`; a version that is not a plain
file name (`../x`, an absolute path) is rejected. `compare_snapshots`
reports tables, edges, accesses and buckets. The drift check compares
against the newest snapshot and fails if that file is corrupt rather
than falling back to an older one.

## Watching schema files

`SchemaWatcher::start` (feature `watcher`) must be called inside a Tokio
runtime; outside one it returns an error. It yields debounced
`DriftReport`s through a bounded channel: while the consumer is behind,
file events are coalesced rather than queued. `stop()` (or dropping the
watcher) ends the background task, after which the receiver yields
`None`.

## What's next

- **[Query Builder](queries.md)** -- immutable fluent queries for your
  migrated schema.
