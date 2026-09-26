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

## What is escaped, and what is raw

The builder splices names and values into SurrealQL text, so each kind of
input has one rule:

| Input | Rule |
|-------|------|
| Table and edge names | Must be identifiers (`[A-Za-z_][A-Za-z0-9_]*`), else a validation error. |
| Record-id targets (`from_table`, `update`, `upsert`, `delete`, `relate`, the CRUD and graph helpers) | Parsed and re-rendered as a `RecordID`: the key is escaped (`user:a-b` renders `user:⟨a-b⟩`, and `user:x; DELETE user` targets the record keyed `"x; DELETE user"`). Array, object, range, and generated keys (`user:[1, 2]`, `user:ulid()`) are refused unless quoted. |
| Fields in `order_by`, `group_by`, `fulltext_search`, `vector_search`, `similarity_score`, `SET` targets, `GraphQuery::select` / `fetch`, `AggregateOpts` aliases | Must be field paths (identifiers joined by `.`). |
| `search_score` and `reverse_traverse` names | Quoted as identifiers. |
| Data values (`insert`, `update`, `upsert`, `relate`, `set`, operator values) | Always literals. A JSON object is an object literal at any depth, whatever its keys; keys that are not identifiers are quoted. |
| Projections passed to `select`, string `WHERE` fragments, `join`, `traverse` paths, `Expression`s | Raw SurrealQL by design: never build them from untrusted text. |

`Query::to_surql` re-checks every name and target it renders, so a
`Query` assembled by setting its public fields directly follows the same
rules. Vector values and thresholds must be finite.

## Raw values: functions and record references

Because a JSON value is always data, a function call or record reference
goes in through an `Expression`:

```rust
use surql::query::expressions::time_now;
use surql::types::operators::{eq_expr, lt_expr};
use surql::types::record_ref;

// CREATE post CONTENT {title: 'hi', created_at: time::now()}
let q = insert("post", data)?.set_expr("created_at", time_now())?;

// WHERE author = type::record('user', 'alice') AND expires_at < time::now()
let q = select(None)
    .from_table("post")?
    .where_(eq_expr("author", record_ref("user", "alice")))
    .where_(lt_expr("expires_at", time_now()));
```

`set` / `set_expr` add `SET` assignments to an `UPDATE` and fields to the
`CONTENT` of a `CREATE` / `UPSERT` / `RELATE` (replacing a same-named
data key). `SurrealFn`, `RecordRef`, and `RecordID` convert into an
`Expression` with `.into()`.

## Graph traversal

`GraphQuery` renders one step per `out` / `r#in` / `both`. Without a
depth the step is a single hop onto the edge records; `Some(n)` is exactly
`n` hops to the records at the far end, spelled out hop by hop (the form
SurrealDB 3 accepts). `to(table)` narrows the last step's far end, in that
step's direction:

```rust
use surql::query::GraphQuery;

// SELECT * FROM user:alice->follows->?->follows->user LIMIT 10
let q = GraphQuery::new("user:alice").out("follows", Some(2)).to("user").limit(10)?;

// SELECT * FROM user:alice<-follows<-user
let q = GraphQuery::new("user:alice").r#in("follows", None).to("user");
```

Depths run from 1 to 32 (also the cap on `graph::shortest_path`'s
`max_depth`). `LIMIT` renders before `FETCH`, the order the engine
requires.

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
the same kind are collapsed to the latest value. The server ignores
comments, so hints label a statement without changing how it runs (see
[Query Hints](query_hints.md)).

## Result wrappers

Once the async client lands, queries produce typed `QueryResult<T>` /
`RecordResult<T>` / `ListResult<T>` / `PaginatedResult<T>` values via the
result-extraction helpers in `query::results`.

## What's next

- **[Query Hints](query_hints.md)** -- every supported optimization hint.
- **[Visualization](visualization.md)** -- schema diagrams.
