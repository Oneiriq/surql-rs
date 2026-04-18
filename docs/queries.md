# Query Builder

The immutable fluent builder composes SurrealQL statements without
executing them. Every method returns a new `Query`; the input is never
mutated.

## SELECT

```rust
use surql::query::helpers::{from_table, select};
use surql::types::operators::{eq, gt};

let q = select(Some(vec!["name".into(), "email".into()]))
    .from_table("user")?
    .where_(&gt("age", 18))
    .where_(&eq("status", "active"))
    .order_by("created_at", "DESC")
    .limit(10);

println!("{}", q.to_surql());
```

## INSERT

```rust
use surql::query::helpers::insert;
use serde_json::json;

let q = insert(
    "user",
    [
        ("name", json!("Alice")),
        ("email", json!("alice@example.com")),
    ],
)?;
```

## UPDATE / UPSERT / DELETE / RELATE

```rust
use surql::query::helpers::{update, upsert, delete, relate};

let u = update("user:alice", [("status", json!("active"))])?;
let d = delete("user:bob")?;
let r = relate("user:alice", "likes", "post:1")?;
```

## Where

`where_` accepts:

- a `&str` (raw SurrealQL)
- a `String`
- any `&Operator` from `types::operators`
- a composed operator (`and_`, `or_`, `not_`)

```rust
use surql::types::operators::{and_, eq, gt};

let q = select(None)
    .from_table("user")?
    .where_(&and_(gt("age", 18), eq("status", "active")));
```

## Expressions

Expressions build typed SurrealQL fragments you can embed in select lists
or `where_` clauses.

```rust
use surql::query::expressions::{as_, concat, count, field, math_mean};

let sel = vec![
    field("id").to_surql(),
    as_(&math_mean("score"), "avg_score").to_surql(),
    as_(&count(None), "total").to_surql(),
];
```

## Hints

```rust
use surql::query::hints::{QueryHint, ParallelHint, TimeoutHint};

let q = q.hint(QueryHint::Parallel(ParallelHint::enabled()))
         .hint(QueryHint::Timeout(TimeoutHint::new(30.0)?));
```

Hints render as SurrealQL comments and are merged so that duplicates of
the same kind are collapsed to the latest value.

## Result wrappers

Once the async client lands, queries produce typed `QueryResult<T>` /
`RecordResult<T>` / `ListResult<T>` / `PaginatedResult<T>` values via the
result-extraction helpers in `query::results`.

## What's next

- **[Query Hints](query_hints.md)** -- every supported optimization hint.
- **[Visualization](visualization.md)** -- schema diagrams.
