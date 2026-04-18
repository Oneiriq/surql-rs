# Schema Definition

The schema DSL is a code-first way to describe SurrealDB tables, edges,
fields, indexes, events, and access rules. Definitions render to `DEFINE`
statements and feed the migration generator + validator + visualizer.

## Tables

```rust
use surql::schema::fields::{string_field, int_field};
use surql::schema::table::{table_schema, TableMode, unique_index};

let user = table_schema("user")
    .mode(TableMode::Schemafull)
    .field(string_field("email").assertion("string::is::email($value)"))
    .field(int_field("age"))
    .index(unique_index("email_idx", &["email"]))
    .build()?;
```

`TableMode` has three variants: `Schemafull`, `Schemaless`, and `Drop`.

## Field types

| Helper               | SurrealDB type |
|----------------------|----------------|
| `string_field`       | `string`       |
| `int_field`          | `int`          |
| `float_field`        | `float`        |
| `bool_field`         | `bool`         |
| `datetime_field`     | `datetime`     |
| `duration_field`     | `duration`     |
| `decimal_field`      | `decimal`      |
| `object_field`       | `object`       |
| `array_field`        | `array`        |
| `record_field(t)`    | `record<t>`    |
| `computed_field`     | computed (`VALUE <expr>`) |

All field builders share the same chainable methods: `assertion`,
`default`, `value`, `readonly`, `flexible`, `permissions`.

## Indexes

- `index(name, cols)` -- standard
- `unique_index(name, cols)` -- UNIQUE
- `search_index(name, cols)` -- SEARCH
- `mtree_index(name, col, dimension, distance)` -- MTREE vector index
- `hnsw_index(name, col, dimension, distance, efc, m)` -- HNSW vector index

## Events

```rust
use surql::schema::table::event;

let new_user = event("new_user")
    .when("$event = 'CREATE'")
    .then("CREATE log SET table = 'user', id = $value.id");
```

## Edges

```rust
use surql::schema::edge::{edge_schema, EdgeMode, bidirectional_edge};

let likes = edge_schema("likes")
    .mode(EdgeMode::Relation)
    .from("user")
    .to("post")
    .build()?;
```

## Access (record + JWT)

```rust
use surql::schema::access::{access_schema, AccessType, JwtConfig};

let api = access_schema("api")
    .access_type(AccessType::Jwt)
    .jwt(JwtConfig::hs512_with_key("secret"))
    .duration_token("1h")
    .build()?;
```

## Registry + SQL generation

```rust
use surql::schema::registry::SchemaRegistry;
use surql::schema::sql::generate_schema_sql;

let registry = SchemaRegistry::new();
registry.register_table(user)?;
registry.register_edge(likes)?;

let stmts = generate_schema_sql(
    Some(&registry.tables_map()),
    Some(&registry.edges_map()),
    None,
    false, // if_not_exists
)?;
```

## What's next

- **[Migrations](migrations.md)** -- turning schema changes into migration
  files.
- **[Visualization](visualization.md)** -- Mermaid / GraphViz / ASCII
  diagrams from the registry.
