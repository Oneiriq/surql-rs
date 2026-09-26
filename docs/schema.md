# Schema Definition

The schema DSL is a code-first way to describe SurrealDB tables, edges,
fields, indexes, events, and access rules. Definitions render to `DEFINE`
statements and feed the migration generator + validator + visualizer.

## Tables

```rust
use surql::schema::{int_field, string_field, table_schema, unique_index, TableMode};

let user = table_schema("user")
    .with_mode(TableMode::Schemafull)
    .with_fields([
        string_field("email")
            .assertion("string::is::email($value)")
            .build_unchecked()?,
        int_field("age").build_unchecked()?,
    ])
    .with_indexes([unique_index("email_idx", ["email"])]);
```

`TableMode` has three variants: `Schemafull`, `Schemaless`, and `Drop`.

Table, field, index, and event names are rendered as SurrealQL names:
anything that is not a plain identifier, or is a reserved word, is
backtick-quoted (`` DEFINE FIELD `value` ON TABLE `select` ``), so a name can
never break out of the statement.

## Field types

| Helper               | SurrealDB type |
|----------------------|----------------|
| `string_field`       | `string`       |
| `int_field`          | `int`          |
| `float_field`        | `float`        |
| `bool_field`         | `bool`         |
| `datetime_field`     | `datetime`     |
| `object_field`       | `object`       |
| `array_field`        | `array`        |
| `record_field(n, Some(t))` | `record<t>` |
| `file_field`         | `file`         |
| `bytes_field`        | `bytes`        |
| `computed_field`     | computed (`VALUE <expr>`) |

All field builders share the same chainable methods: `assertion`,
`default`, `value`, `readonly`, `flexible`, `permissions`, `nullable`,
`reference`, `computed`.

### Nullable fields

`nullable(true)` wraps the rendered type in `option<...>`, so a
SCHEMAFULL column accepts `NONE`:

```rust
use surql::schema::{int_field, record_field, string_field};

int_field("size_bytes").nullable(true);              // TYPE option<int>
record_field("blob", Some("blob")).nullable(true);   // TYPE option<record<blob>>
string_field("digest").nullable(true);               // TYPE option<string>
```

`FieldDefinition::with_nullable(bool)` does the same on an already
built definition. The `INFO FOR TABLE` parser round-trips the wrapper
(the engine echoes it as `none | T`, nested generics included), so a
nullable column diffs against a live database as itself rather than as a
change.

## Permissions

Tables, edges, and fields take a per-action map. The key is an action
(`select`, `create`, `update`, and for tables `delete`), or several joined
with commas; the value is a `WHERE` expression, or `"NONE"` / `"FULL"` for
the fixed postures. Actions left out keep the engine default: `NONE` for a
table, `FULL` for a field.

```rust
use surql::schema::{string_field, table_schema};

let doc = table_schema("doc")
    .with_permissions([("select", "owner = $auth.id"), ("delete", "NONE")]);
// DEFINE TABLE doc SCHEMAFULL PERMISSIONS FOR delete NONE FOR select WHERE owner = $auth.id;

let (ssn, _) = string_field("ssn")
    .permissions([("select", "$auth.admin = true"), ("update", "NONE")])
    .build()?;
// DEFINE FIELD ssn ON TABLE person TYPE string
//   PERMISSIONS FOR select WHERE $auth.admin = true FOR update NONE;
```

Field permissions are rendered on the field's own `DEFINE FIELD`; a field
has no `delete` permission, and `build` refuses one.

The parser reads the engine's echo back into the same map, keeping only
actions that differ from the default, so a definition without permissions
compares equal to its echo and `PERMISSIONS FULL` reads back as `FULL` on
every action.

A bucket is different: it has one permission covering every file operation,
set as a clause body.

```rust
use surql::schema::bucket_schema;

let avatars = bucket_schema("avatars", "memory")
    .permissions("WHERE $auth.id != NONE")
    .build()?;
// DEFINE BUCKET avatars BACKEND "memory" PERMISSIONS WHERE $auth.id != NONE;
```

## Indexes

- `index(name, cols)` -- standard
- `unique_index(name, cols)` -- UNIQUE
- `search_index(name, [col])` / `bm25_index(name, [col], analyzer)` --
  FULLTEXT, exactly one column
- `mtree_index(name, col, dimension, distance, vector_type)` -- MTREE
- `hnsw_index(name, col, dimension, distance, vector_type, efc, m)` -- HNSW
- `diskann_index(name, col, dimension, distance, vector_type)` -- DISKANN

## Events

```rust
use surql::schema::event;

let new_user = event(
    "new_user",
    "$event = 'CREATE'",
    "CREATE log SET table = 'user', id = $value.id; UPDATE stats:users SET n += 1",
);
// DEFINE EVENT new_user ON TABLE user WHEN $event = 'CREATE'
//   THEN { CREATE log SET ...; UPDATE stats:users SET n += 1 };
```

The action always renders inside a `{ ... }` block, so a multi-statement
action stays part of the event instead of running once when the definition
is applied. The parser strips the block the engine echoes, so a parsed event
renders the same way again.

## Edges

```rust
use surql::schema::{edge_schema, typed_edge, EdgeMode};

let likes = typed_edge("likes", "user", "post");
// DEFINE TABLE likes TYPE RELATION FROM user TO post;

let loose = edge_schema("entity_relation").with_mode(EdgeMode::Schemafull);
```

The engine echoes the endpoints as `TYPE RELATION IN user OUT post`; the
parser reads both spellings, `a | b` lists, and backticked names.

## Access (record + JWT)

```rust
use surql::schema::{jwt_access, record_access, JwtConfig, RecordAccessConfig};

// Verify with a key; an HMAC key also signs the tokens the engine issues.
let api = jwt_access("api", JwtConfig::hs256("secret")).with_token("1h");

// Verify with a JWKS endpoint, and issue tokens with a private key.
let sso = jwt_access(
    "sso",
    JwtConfig::new("RS256")
        .with_url("https://auth.example.com/jwks")
        .with_issuer("<private key>"),
);
// DEFINE ACCESS sso ON DATABASE TYPE JWT URL 'https://auth.example.com/jwks'
//   WITH ISSUER ALGORITHM RS256 KEY '<private key>';

let account = record_access(
    "account",
    RecordAccessConfig::new()
        .with_signin("SELECT * FROM user WHERE email = $email")
        .with_jwt(JwtConfig::hs256("secret")),
);
```

A JWT configuration names exactly one of `key` and `url`; `issuer` is the
issuer signing key (`WITH ISSUER KEY`), not an issuer claim. Keys and every
other string are escaped as SurrealQL literals. The engine echoes symmetric
keys and issuer keys as `'[REDACTED]'`, and always echoes its default
durations; the parser reads `FOR TOKEN 1h` and `FOR SESSION NONE` back as
unset.

## Registry + SQL generation

```rust
use std::collections::BTreeMap;
use surql::schema::sql::generate_schema_sql;

let tables = BTreeMap::from([("user".to_string(), user)]);
let edges = BTreeMap::from([("likes".to_string(), likes)]);

let script = generate_schema_sql(Some(&tables), Some(&edges), false)?;
```

### Replacing definitions that already exist

`IF NOT EXISTS` creates a definition once and then never touches it, so
a schema that evolves needs the replacing form to bring an existing
database up to what the code declares. Every definition renders it:

```rust
use surql::schema::sql::generate_table_sql_overwrite;

// The table's own statement, replacing form.
let sql = user.to_surql_overwrite();

// The table and everything under it: fields, indexes, events.
let stmts = generate_table_sql_overwrite(&user);
```

`to_surql_overwrite` is on tables, fields, indexes, events, analyzers
and access methods. The ones that belong to a table take the table
name, because a field renders as `DEFINE FIELD ... ON <table>`:

```rust
let field_sql = email_field.to_surql_overwrite("user");
let index_sql = email_index.to_surql_overwrite("user");
```

Edges and access methods return a `Result`, because their rendering can
refuse; the rest return a `String`. `OVERWRITE` replaces the definition
and leaves the stored data alone.

## Reading a database back

`surql::schema::parser` turns `INFO FOR DB` / `INFO FOR TABLE` responses
back into the definitions above (`parse_db_info`, `parse_table_full`,
`parse_edge_info`). The engine rewrites what it stores; see
[SurrealDB v3 patterns, section 13](v3-patterns.md) for the echo shapes the
parser reads.

## What's next

- **[Migrations](migrations.md)** -- turning schema changes into migration
  files.
- **[Visualization](visualization.md)** -- Mermaid / GraphViz / ASCII
  diagrams from the registry.
