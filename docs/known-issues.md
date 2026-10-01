# Known issues

Open problems that are understood but not yet fixed, each with the fix it
would take. Fixed issues move to the [changelog](changelog.md).

## Migrations

- **An index can be refused while SurrealDB 3.3 reclaims its table's
  document ids.** After an index is removed or overwritten, 3.3 can
  reclaim the table's shared document-id space in the background, and a
  `DEFINE INDEX` (other than `COUNT`) on that table that arrives once the
  reclaim has started fails with "The shared document-ID space for table `t` is
  still being reclaimed; retry DEFINE INDEX after cleanup completes". The
  migration is left unapplied (its transaction rolls back), the error
  names that cause, and running it again after the cleanup applies it.
  The fix would be for the executor to retry that one error after a
  delay; it could not be provoked on demand (200,000 records and repeated
  remove / define on a 3.3.0 server never hit it), so an untested retry
  was not added.

## Dependencies

- **quick-xml advisories are carried.** RUSTSEC-2026-0194 and
  RUSTSEC-2026-0195 reach the lockfile through `object_store` 0.13, whose
  `aws`, `gcp` and `azure` backends surrealdb-core 3.3 enables on every
  native build, on the embedded-engine path only; `.cargo/audit.toml`
  records why they cannot be reached. `object_store` 0.14 takes the fixed
  quick-xml (0.41), so the entries come out when surrealdb-core moves to
  it; nothing in this crate can select either.
