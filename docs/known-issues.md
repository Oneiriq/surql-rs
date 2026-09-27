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
