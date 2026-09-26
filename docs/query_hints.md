# Query Hints

Hints are typed annotations rendered as a `/* ... */` comment in front of
the statement. They are strongly typed so builder chains can dedup +
validate them before rendering.

**Hints do not change how a query runs.** SurrealDB discards comments
when it parses a statement, so the planner never sees them. They are
useful as labels carried in the statement text (logs, slow-query
captures, tooling that reads the SurrealQL). To change execution, use the
SurrealQL clause itself: `WITH INDEX`, `TIMEOUT`, `PARALLEL`, `EXPLAIN`,
or `FETCH`.

A hint can never end its comment early: `*/` inside a hint's text is
broken up when rendered, and `Query::to_surql` returns a validation error
for an `IndexHint` whose table or index is not an identifier.

## Kinds

| Hint            | Rendering                                 |
|-----------------|-------------------------------------------|
| `IndexHint`     | `/* USE INDEX table.index */` or `/* FORCE INDEX … */` |
| `ParallelHint`  | `/* PARALLEL ON */` / `OFF` / `/* PARALLEL N */`       |
| `TimeoutHint`   | `/* TIMEOUT Ns */`                        |
| `FetchHint`     | `/* FETCH EAGER */` / `LAZY` / `/* FETCH BATCH N */`   |
| `ExplainHint`   | `/* EXPLAIN */` or `/* EXPLAIN FULL */`   |

## Construction

```rust
use surql::query::hints::{
    ExplainHint, FetchHint, IndexHint, ParallelHint, TimeoutHint, QueryHint,
};

let idx = QueryHint::Index(IndexHint::new("user", "email_idx").force(true));
let par = QueryHint::Parallel(ParallelHint::with_workers(4)?);
let tmo = QueryHint::Timeout(TimeoutHint::new(30.0)?);
let fch = QueryHint::Fetch(FetchHint::batch(100)?);
let xpn = QueryHint::Explain(ExplainHint::full());
```

## Composition

Multiple hints can coexist; `merge_hints` collapses duplicates of the
same kind to the latest value (preserving insertion order of unique
kinds).

```rust
use surql::query::hints::{merge_hints, render_hints};

let hints = vec![tmo_a, par, tmo_b];       // two Timeout hints
let merged = merge_hints(hints);            // tmo_b wins
println!("{}", render_hints(&merged));      // space-joined comment string
```

## Validation

```rust
use surql::query::hints::validate_hint;

let errors = validate_hint(&idx, Some("user"));
assert!(errors.is_empty());

let errors = validate_hint(&idx, Some("post"));
assert_eq!(errors.len(), 1); // wrong table

let bad = QueryHint::Index(IndexHint::new("user", "email idx"));
assert_eq!(validate_hint(&bad, None).len(), 1); // not an identifier
```

## What's next

- **[Query Builder](queries.md)** -- chaining hints onto queries.
