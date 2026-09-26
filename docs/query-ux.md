# Query UX helpers

The 0.2.0 "query-UX" wave (issue #78) added a set of first-class
helpers to the crate root that remove boilerplate around record
targeting, aggregate projections, and raw-result extraction. They live
alongside the existing builder - nothing was removed - so existing
0.1.x code keeps working.

All helpers are re-exported at the crate root:

```rust
use surql::{
    type_record, type_thing,          // crate::types::operators
    extract_one, extract_many,        // crate::query::results
    extract_scalar, has_result,
};
use surql::query::{AggregateOpts, aggregate_records, build_aggregate_query};
```

## `type_record` / `type_thing`

SurrealDB 3 renamed `type::thing(...)` to `type::record(...)`. Both
helpers return an [`Expression`] tagged as a function call.

### Before

```rust
let target = format!("type::record('task', '{}')", id.replace('\'', "\\'"));
let surql = format!("UPDATE {target} CONTENT $data");
```

### After

```rust
use surql::type_record;
use surql::query::crud::update_record_target;

let target = type_record("task", id).to_surql();
update_record_target(&client, &target, data).await?;
```

`type_thing(...)` is kept as an alias for the sibling ports and renders
`type::record(...)` too: SurrealDB 3 removed `type::thing` and rejects
it at parse time. The table and a string id are rendered as escaped
string literals.

Numeric ids are accepted directly:

```rust
let num = type_record("post", 42_i64);
assert_eq!(num.to_surql(), "type::record('post', 42)");
```

### Raw values go through typed channels

A `serde_json::Value` is always data. Serialising a `RecordRef` or
`SurrealFn` into JSON and passing it as a value renders an object literal
(`{ table: 'task', record_id: 'abc' }`), never the function call: the
renderer cannot tell such an object from untrusted input of the same
shape. To use one as SurrealQL, convert it into an `Expression`:

```rust
use surql::query::builder::Query;
use surql::query::expressions::time_now;
use surql::types::operators::eq_expr;
use surql::types::record_ref;

// In a WHERE clause: the `*_expr` comparisons take any Expression.
let q = Query::new()
    .select(None)
    .from_table("post")?
    .where_(eq_expr("author", record_ref("user", "alice")));

// In CREATE / UPSERT / RELATE content, or an UPDATE SET: `set_expr`.
let q = Query::new()
    .insert("post", data)?
    .set_expr("created_at", time_now())?
    .set_expr("author", record_ref("user", "alice").into())?;
```

`SurrealFn`, `RecordRef`, and `RecordID` all convert into an
`Expression` with `.into()`.

## `extract_many` / `has_result`

The existing `extract_result` / `extract_one` / `extract_scalar` /
`has_results` helpers cover the common response shapes, but Python and
TypeScript ports had an additional pair:

- `extract_many` - every record as boxed JSON `Value`s (ready to feed
  through `serde_json::from_value::<T>` without a wrapping step).
- `has_result` - singular alias of `has_results` matching the
  ergonomic naming used in the Python / TS ports.

### Before

```rust
use surql::query::results::extract_result;

let rows = extract_result(&raw);
let values: Vec<Value> = rows.into_iter().map(Value::Object).collect();
```

### After

```rust
use surql::{extract_many, has_result};

let values = extract_many(&raw);
if !has_result(&raw) { return Ok(None); }
```

Both handle the common response shapes, decided per statement:

- The Rust client's one-array-per-statement shape: `[[{...}, {...}]]`
- Flat arrays: `[{...}, {...}]`
- Nested `result` wrappers: `[{"result": [...]}]`

Each statement's rows are spread by exactly one level, so a row that is
itself an array (`SELECT VALUE tags`) stays one row, and a row with a
`result` field is kept whole. Rows that are not objects
(`SELECT VALUE name`) come back wrapped as `{"value": ...}`.

## `SurrealQL` function factories (snake_case)

`query::expressions` previously exposed camelCase-ish names like
`math_mean` / `math_sum`. The 0.2 wave added additional snake_case
factories so the full `string::*` / `math::*` / `type::*` / `time::*`
namespaces can be composed without manually concatenating strings:

```rust
use surql::query::expressions::{
    string_concat, string_len, string_lower, string_upper,
    math_abs, math_ceil, math_floor, math_round,
};

assert_eq!(string_upper("name").to_surql(), "string::uppercase(name)");
assert_eq!(math_round("price", 2).to_surql(), "math::round(price, 2)");
```

Every factory returns an [`Expression`] tagged
`ExpressionKind::Function` so it composes with `as_(...)`, `count_if`,
and the aggregate projection helpers without manual quoting.

## `Query::select_expr`

Passing rendered SurrealQL strings to `select(...)` obscured the typed
expression layer. `select_expr` takes any iterable of [`Expression`]
and handles rendering:

### Before

```rust
use surql::query::builder::Query;
use surql::query::expressions::{as_, count_all, math_mean};

let q = Query::new()
    .select(Some(vec![
        format!("{} AS total", count_all().to_surql()),
        format!("{} AS mean_strength", math_mean("strength").to_surql()),
    ]))
    .from_table("memory_entry")?;
```

### After

```rust
let q = Query::new()
    .select_expr(vec![
        as_(&count_all(), "total"),
        as_(&math_mean("strength"), "mean_strength"),
    ])
    .from_table("memory_entry")?
    .group_all();
```

Rendered SurrealQL:

```sql
SELECT count() AS total, math::mean(strength) AS mean_strength
  FROM memory_entry
  GROUP ALL
```

## `Query::execute`

Previously every builder call required routing through
`crate::query::executor::execute_query`:

### Before

```rust
use surql::query::executor::execute_query;

let raw = execute_query(&client, &q).await?;
```

### After

```rust
let raw = q.execute(&client).await?;
```

It is a thin async wrapper - identical behaviour, returns the raw
`serde_json::Value`. For typed deserialisation use
`executor::fetch_all` / `fetch_one` instead.

## `aggregate_records` + `AggregateOpts`

Stable-shaped aggregation without hand-rolled SurrealQL. Mirrors the
`surql-py` `AggregateOpts` struct.

### Before

```rust
let raw = client
    .query(
        "SELECT count() AS total, math::mean(score) AS mean_score \
         FROM memory_entry WHERE status = 'active' GROUP ALL",
    )
    .await?;
let rows = extract_result(&raw);
```

### After

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

Each row is a JSON object keyed by the aliases in
`AggregateOpts::select`. Aliases and `group_by` entries must be field
paths (`total`, `meta.count`). Pair with [`extract_scalar`] to pull a
single field out of the single-row `GROUP ALL` case:

```rust
use surql::extract_scalar;

let mean = extract_scalar(&rows[0], "mean_score", serde_json::json!(0.0));
```

`build_aggregate_query` returns the same `Query` without executing -
useful for unit tests that want to assert the rendered SurrealQL.

## What's next

- [v3 patterns](v3-patterns.md) - SurrealDB 3-specific SurrealQL
  shapes.
- [Migration 0.1 -> 0.2](migration.md) - upgrade notes and API
  deltas.
- [Query builder](queries.md) - end-to-end builder tour.
