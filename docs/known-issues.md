# Known issues

Open problems that are understood but not yet fixed, each with the fix it
would take. Fixed issues move to the [changelog](changelog.md).

## Queries

- **Deeply nested values cannot be inlined.** Values are rendered into
  the statement as literals, and the engine's parser refuses nesting
  deeper than its recursion limit (about 20 levels of objects and
  arrays), so such a value fails with "Exceeded query recursion depth
  limit". Binding values as query parameters instead of inlining them
  would lift the limit.

## Schema

- **`mtree_index` renders DDL the 3.x grammar rejects.** SurrealDB 3 has
  no `MTREE` index; use `hnsw_index` or `diskann_index`.
- **COUNT indexes are not modelled.** One read from the database parses
  as a standard index with no columns. The fix is an `IndexType::Count`.
- **Union field types read back as `any`.** A type such as
  `array<string> | int` parses as `FieldType::Any`.
- **Access definitions are not compared.** The engine echoes them with
  keys redacted, durations normalised (`24h` becomes `1d`) and implied
  algorithms, so comparing them would need that normalisation. Accesses
  are not diffed, so reconciling is unaffected.
- **Some event bodies still warn in validation.** The engine rewrites
  `IN` to `INSIDE` and respaces lists in event conditions, which
  `validate_schema` reports as a warning (never an error).

## Cache

- **Redis reconnects by dropping the connection.** A connection that
  fails with an I/O error is discarded and the next call opens a new
  one. redis's `ConnectionManager` would reconnect in the background, but
  needs its `connection-manager` feature.

## Dependencies

- **quick-xml advisories are carried.** RUSTSEC-2026-0194 and
  RUSTSEC-2026-0195 reach the lockfile through `object_store`'s `aws`
  backend, on the embedded-engine path only; `.cargo/audit.toml` records
  why they cannot be reached and when to remove the entries.
