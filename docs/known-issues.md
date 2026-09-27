# Known issues

Open problems that are understood but not yet fixed, each with the fix it
would take. Fixed issues move to the [changelog](changelog.md).

## Migrations

- **An index can be refused while SurrealDB 3.3 reclaims its table's
  document ids.** After a `REMOVE INDEX` or a `DEFINE INDEX OVERWRITE`,
  3.3 cleans the table's shared document-id space up in the background,
  and a non-COUNT `DEFINE INDEX` on that table fails with "still being
  reclaimed" while the cleanup runs. The migration is left unapplied
  (its transaction rolls back) and applies once the cleanup finishes.
  The fix would be for the executor to retry that one error after a
  delay; it has not been seen outside a cleanup already in progress.

## Dependencies

- **quick-xml advisories are carried.** RUSTSEC-2026-0194 and
  RUSTSEC-2026-0195 reach the lockfile through `object_store` 0.13, whose
  `aws`, `gcp` and `azure` backends surrealdb-core 3.3 enables on every
  native build, on the embedded-engine path only; `.cargo/audit.toml`
  records why they cannot be reached. `object_store` 0.14 takes the fixed
  quick-xml (0.41), so the entries come out when surrealdb-core moves to
  it; nothing in this crate can select either.
