# SurrealDB v3 patterns

The 0.1 -> 0.2 window rebased the crate on the `surrealdb` 3.x driver
(previously 2.x). Several of the SurrealQL shapes that worked on v2
are parse errors on v3. This page documents every call-site where
`surql` adapted, so consumers porting their own SurrealQL know what to
watch for.

## 1. Subprotocol handshake

`DatabaseClient` wraps `surrealdb::Surreal<surrealdb::engine::any::Any>`.
The `Any` engine picks the transport from the URL at runtime:

| URL scheme            | Engine                    |
|-----------------------|---------------------------|
| `ws://` / `wss://`    | WebSocket + RPC subprotocol |
| `http://` / `https://`| HTTP                      |
| `mem://`              | In-process                |
| `surrealkv://`        | `SurrealKV` embedded      |
| `file://` / `rocksdb://` | Local file stores      |

The v3 driver negotiates an RPC subprotocol on connect (v2 skipped the
handshake). `DatabaseClient::classify_surrealdb_error` specifically
recognises `subprotocol` in the error message and re-tags the failure
as [`SurqlError::Connection`] so retries use the connection back-off
schedule rather than the query back-off schedule.

```rust
let client = surql::DatabaseClient::new(config)?;
client.connect().await?;   // handshake happens here, not in ::new.
```

## 2. `type::thing` -> `type::record`

`type::thing(table, id)` was renamed to `type::record(table, id)` in
v3. The old name emits:

```text
Invalid function/constant path, did you maybe mean `type::record`
```

`surql` ships both helpers so existing queries keep working under
either server version:

- [`type_record`](query-ux.md#type_record-type_thing) - renders
  `type::record(...)`. Preferred on v3.
- [`type_thing`](query-ux.md#type_record-type_thing) - renders
  `type::thing(...)` verbatim. Use when a query plan relies on the
  literal function name matching.

The migration history recorder uses `type::record`:

```rust
// src/migration/history.rs
let surql = format!(
    "CREATE type::record('{table}', $id) SET {set};",
    table = MIGRATION_TABLE_NAME,
);
```

## 3. Datetime coercion

v3 rejects bare ISO-8601 strings for `datetime`-typed fields with:

```text
Expected `datetime` but found '...'
```

The fix is to keep the cast explicit in SurrealQL:

```rust
// src/migration/history.rs
let mut set = String::from(
    "version = $version, description = $description, \
     applied_at = <datetime> $applied_at, checksum = $checksum",
);
```

`types::coerce` provides the inverse - coerce arbitrary
`serde_json::Value`s into ISO-8601 strings suitable for this pattern.

## 4. Unrolled graph-traversal depth

v3 rejects the Python port's depth-templated traversal syntax:

```text
SELECT * FROM <from>->edge{d}-> WHERE id = <to>  -- parse error on v3
```

The trailing `->` leaves no target. `shortest_path` instead iterates
depths and unrolls the arrow chain with SurrealDB's `?` wildcard:

```rust
// src/query/graph.rs
for depth in 1..=max_depth {
    let mut path = String::new();
    for _ in 0..depth {
        write!(path, "->{edge_table}->?").unwrap();
    }
    let surql = format!(
        "SELECT * FROM {from_record}{path} WHERE id = {to_record} LIMIT 1"
    );
    // ...
}
```

Incoming edges use `FROM <record><-<edge>` rather than v2's
`FROM <-edge<-<record>`:

```rust
// src/query/graph.rs
Direction::In  => format!("SELECT count() FROM {record}<-{edge_table}"),
```

## 5. `UPSERT INTO <table> [...]` rejected

v3 requires a single target after `UPSERT`, not an array literal. The
Python port's bulk pattern:

```text
UPSERT INTO user [{id: 'user:a', ...}, {id: 'user:b', ...}];
```

is a parse error. `batch::upsert_many` iterates per record and emits:

```text
UPSERT <target> CONTENT $data
```

per row, with the payload bound as a variable. `build_upsert_query`
still emits the Python-compatible statement for logging / preview
output that needs byte-for-byte parity.

## 6. Buffered transactions

The v3 driver does **not** stream `BEGIN` / `COMMIT` / `CANCEL` as
separate `query()` calls - each `query()` is an isolated request, and
the server rejects a bare `COMMIT`. `Transaction::execute` buffers
statements client-side and `Transaction::commit` flushes them as a
single atomic request:

```text
BEGIN TRANSACTION;
...buffered statements...
COMMIT TRANSACTION;
```

`Transaction::rollback` simply drops the buffer without contacting the
server. See
[`crate::connection::transaction`](https://docs.rs/oneiriq-surql/latest/surql/connection/transaction/index.html)
for the full API.

Usage is unchanged from the v2 port:

```rust
let mut tx = client.begin_transaction().await?;
tx.execute("CREATE user SET name = 'Alice'").await?;
tx.execute("CREATE user SET name = 'Bob'").await?;
tx.commit().await?;
```

## 7. Structured `Token` on signin

v2 returned an opaque `Jwt`; v3 returns a structured `Token`.
`surql` transparently re-exports via the upstream
`surrealdb::opt::auth::Token`, so the [`AuthManager`] cache, refresh
loop, and the `connection::auth` credential types all operate on the
new type without requiring any caller changes.

## 8. `SurrealValue` envelope avoided

The typed-call envelope on v3 is the `SurrealValue` trait, which would
require `T: SurrealValue + Serialize + DeserializeOwned` bounds on
every typed helper. `surql` deliberately routes typed CRUD through raw
SurrealQL + `serde_json::Value`, keeping the public bound at just
`serde::Serialize + serde::de::DeserializeOwned`:

```rust
pub async fn fetch_one<T: DeserializeOwned>(
    client: &DatabaseClient,
    query: &Query,
) -> Result<Option<T>> { /* ... */ }
```

This keeps caller code identical between v2 and v3 and avoids a
`SurrealValue` derive on every schema record type.

## What's next

- [Query UX helpers](query-ux.md) - the 0.2 crate-root additions.
- [Migration 0.1 -> 0.2](migration.md) - upgrade notes.
- [API reference](https://docs.rs/oneiriq-surql) - generated rustdoc.
