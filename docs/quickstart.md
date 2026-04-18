# Quick Start

This walkthrough defines a small `user` table, generates an initial
migration, and inspects the resulting SurrealQL.

## 1. Define a schema

```rust
use surql::schema::fields::{datetime_field, int_field, string_field};
use surql::schema::registry::SchemaRegistry;
use surql::schema::table::{table_schema, unique_index, TableMode};
use surql::Result;

fn build_registry() -> Result<SchemaRegistry> {
    let user = table_schema("user")
        .mode(TableMode::Schemafull)
        .field(string_field("name"))
        .field(string_field("email").assertion("string::is::email($value)"))
        .field(int_field("age").assertion("$value >= 0 AND $value <= 150"))
        .field(
            datetime_field("created_at")
                .default("time::now()")
                .readonly(true),
        )
        .index(unique_index("email_idx", &["email"]))
        .build()?;

    let registry = SchemaRegistry::new();
    registry.register_table(user)?;
    Ok(registry)
}
```

## 2. Render SurrealQL

```rust
use surql::schema::sql::generate_schema_sql;

let registry = build_registry()?;
let statements = generate_schema_sql(
    Some(&registry.tables_map()),
    None,
    None,
    false, // if_not_exists
)?;
for stmt in &statements {
    println!("{stmt}");
}
```

Output:

```sql
DEFINE TABLE user SCHEMAFULL;
DEFINE FIELD name ON TABLE user TYPE string;
DEFINE FIELD email ON TABLE user TYPE string ASSERT string::is::email($value);
DEFINE FIELD age ON TABLE user TYPE int ASSERT $value >= 0 AND $value <= 150;
DEFINE FIELD created_at ON TABLE user TYPE datetime VALUE time::now() READONLY;
DEFINE INDEX email_idx ON TABLE user COLUMNS email UNIQUE;
```

## 3. Generate a migration file

```rust
use surql::migration::generator::generate_initial_migration;
use std::path::Path;

let migration = generate_initial_migration(&registry, Path::new("migrations"))?;
println!(
    "wrote migration {}: {}",
    migration.version, migration.description,
);
```

The resulting file uses the portable `.surql` format:

```text
-- @metadata
-- version: 20260418_193300
-- description: Initial schema
-- @up
DEFINE TABLE user SCHEMAFULL;
DEFINE FIELD name ON TABLE user TYPE string;
...
-- @down
REMOVE TABLE IF EXISTS user;
```

## 4. Diff code vs database

```rust
use surql::migration::diff::{diff_schemas, SchemaSnapshot};

let code_side = SchemaSnapshot::from_registry(&registry);
let db_side   = /* fetched via INFO FOR DB once the client lands */;
let diff = diff_schemas(&code_side, &db_side);
for op in diff {
    println!("{}: {}", op.operation, op.target_name);
}
```

## What's next

- **[Schema Definition](schema.md)** -- fields, tables, edges, indexes,
  events, access rules.
- **[Migrations](migrations.md)** -- the full migration lifecycle.
- **[Query Builder](queries.md)** -- immutable fluent queries.
