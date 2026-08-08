# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Record references (`DEFINE FIELD ... REFERENCE`).** `FieldDefinition`
  gained `reference: Option<ReferenceAction>` (`IGNORE` / `REJECT` /
  `CASCADE` / `UNSET`) and `computed: Option<String>`, with
  `FieldBuilder::reference` / `::computed` and the
  `reverse_reference_field(name, source)` constructor for the reverse half
  (`COMPUTED <~source`). `Query::reverse_traverse` and
  `query::references::reverse_reference_query` render the `<~table` /
  `<~table.{ a, b }` projection that reads incoming links back.

  The `REFERENCE` clause always spells out its `ON DELETE` action, because a
  bare `REFERENCE` is what `INFO FOR TABLE` echoes as `ON DELETE IGNORE`;
  emitting the short form would diff against the database forever. The
  parser reads both forms, and the assertion / default / value extractors
  now stop at `REFERENCE` and `COMPUTED`, which the engine emits *after*
  `ASSERT`.

  `FieldDefinition::validate` rejects what v3.0.5 rejects: `REFERENCE` on a
  nested field (`metadata.comics`) or on anything that is not a
  `record<table>` / `array<record<table>>` link, and `COMPUTED` beside
  `READONLY`, `VALUE`, or `DEFAULT`. Union types (`array<record<x>> |
  string`), which the engine also rejects, are not expressible here.

- **Background index builds (`DEFINE INDEX ... CONCURRENTLY`).**
  `IndexDefinition::with_concurrently` appends the directive, which lets a
  large index populate without blocking the statement.
  `info_for_index_surql(name, table)` renders the progress query and
  `IndexBuildStatus::from_info` reads the `{ building: { status, initial,
  pending, updated } }` answer, including through the array
  `DatabaseClient::query` wraps results in.

  v3.0.5 accepts `CONCURRENTLY` and then echoes the index back **without**
  it, so the parser deliberately reports `concurrently: false` and no
  comparison looks at the member. Storing it as parsed would make every
  background-built index diff forever.

- **`array<record<table>>` field types.** An ARRAY field with a
  `target_table` now renders `array<record<{target}>>` instead of a bare
  `array`, which is the shape a to-many reference needs. A `target_table`
  on an ARRAY field was previously carried but never rendered, so this
  changes emitted DDL for any definition that set both.

### Changed

- **Module splits to stay inside the 1000-LOC budget.** `FieldType` moved to
  `schema::field_type` and `IndexDefinition` (with `IndexType`, the distance
  and vector-type enums, and the `index` / `unique_index` / `search_index` /
  `bm25_index` / `mtree_index` / `hnsw_index` builders) moved to
  `schema::index`. Both are re-exported from their previous homes
  (`schema::fields`, `schema::table`), so no consumer path changes.


## [0.31.0] - 2026-08-07

### Added

- **The diff engine serves live-database reconciliation.** Analyzers
  join `DatabaseInfo`, `SchemaSnapshot`, and `diff_schemas` with
  their own parser, so the full schema a consumer declares can be
  compared against what a database actually holds.
  `parse_table_full` names the two-level composition (`INFO FOR DB`
  for mode and permissions, `INFO FOR TABLE` for fields, indexes,
  and events), because the database level alone yields fieldless
  tables, which is a trap for anyone diffing against it.


- **Nullable fields (`TYPE option<...>`).** `FieldBuilder::nullable(bool)` /
  `FieldDefinition::with_nullable(bool)` wrap the rendered type in
  `option<...>` (including record targets: `option<record<blob>>`) so a
  SCHEMAFULL column can accept `NONE`. The `INFO FOR TABLE` parser
  round-trips the wrapper, keeping migration diffing stable for nullable
  columns. Restores parity with surql-py (`nullable=True`, 1.5.8+) and the
  TS port; rendering for non-nullable fields is byte-identical to before.

- **Row-level filtering on the graph helpers (`conditions`).** `query::graph`
  gained a `conditions` argument on `traverse`, `traverse_raw`,
  `traverse_with_depth`, `get_outgoing_edges`, `get_incoming_edges`,
  `get_related_records`, and `shortest_path`. Each entry renders through
  `Query::where_` and multiple entries combine with `AND`, so a traversal can
  carry a tenant guard or any other row-level predicate. Restores parity with
  the sibling ports, which have accepted `conditions` on their graph helpers
  since surql-py 1.6.0.

  Previously these helpers emitted a bare `SELECT * FROM record->edge` with no
  filtering hook at all. A caller needing row-level isolation, a mandatory
  `WHERE tenant_id = ...` alongside engine-enforced `PERMISSIONS` for
  instance, could not express it, and had to abandon the helpers for a hand-rolled
  equality-filtered edge table.

- **`query::Condition`.** An owned `Raw(String) | Op(Operator)` carrier with
  `From` impls for `&str`, `String`, `&String`, `Operator`, and `&Operator`,
  plus `WhereCondition` for both `Condition` and `&Condition`. `WhereCondition`
  takes `self` by value and so cannot be used behind a trait object; `Condition`
  is what lets one slice mix raw fragments and operators, matching the
  `str | Operator` union the sibling ports accept.

### Changed

- **`DatabaseClient` clones now share ONE engine session.** The SDK
  mints a session per `Surreal` clone and announces it with lifecycle
  events the remote router can lose under concurrency, which surfaced
  as intermittent `Session not found` failures in any service that
  clones its client per request (an axum state extraction does
  exactly that). The inner handle now rides an `Arc`, so clones share
  the service session and session churn stops entirely. Code that
  wants an independent session asks for one: `caller_session` for a
  caller-bound session, `client.inner().clone()` for a raw one. Auth
  calls (`signin`, `authenticate`, `invalidate`) now act on the
  shared session, which is what a service almost always means; the
  previous per-clone isolation was an accident of the SDK's `Clone`.

- **BREAKING: graph helper signatures.** The seven helpers above take a new
  trailing `conditions: Option<&[Condition]>` parameter. Rust has no default
  arguments, so this follows the existing convention in this module of
  rendering a Python default argument as a required `Option<T>` parameter (as
  `create_relation`'s `data: Option<Value>` already does). Existing call sites
  migrate by passing `None`, which leaves the emitted SurrealQL unchanged.
  `count_related` is deliberately **not** included, because surql-py does not filter it
  either, and diverging would break the 1:1 contract.

- **Graph helpers compose through `Query` instead of `format!`.** Every
  `SELECT`-shaped helper now builds its statement with
  `Query::new().select(…).from_table(…).traverse(…)` rather than interpolating
  identifiers into a string. Statement construction moved into pure sync
  functions (`select_traversal_surql`, `count_related_surql`,
  `shortest_path_surql`, `depth_path`) that are unit-testable without a live
  client.

  `create_relation` and `remove_relation` are intentionally left hand-composed:
  `Query::relate` inlines its payload via `render_data_object`, whereas
  `create_relation` binds `CONTENT $data` as a variable, and routing it through
  the builder would inline caller payloads into the statement.

- **`shortest_path` emits parenthesised predicates.** Now that the identity
  check goes through the builder, the rendered clause is
  `WHERE (id = <to>) [AND (…)]` rather than `WHERE id = <to>`. Semantically
  identical; noted because it changes the exact statement text.

### Fixed

- **The guides document what this release adds.** Nullable fields and
  `OVERWRITE` rendering are in the schema guide, reading a live
  database back through `parse_db_info` and `parse_table_full` is in
  the migrations guide. Two examples that predate this release are
  corrected while passing: `SchemaSnapshot::from_registry` does not
  exist and never did, and a `SchemaSnapshot` struct literal stops
  compiling every time the type gains a kind of definition, so both
  now use the constructors. Every example in the changed pages was
  compiled against the crate rather than read over.

- **The dependency audit carries its two unfixable advisories in one
  place.** `.cargo/audit.toml` names each, what would have to change
  upstream for it to come out, and why it is safe to carry meanwhile.
  Both workflows now read that file instead of passing a flag with the
  reasoning written somewhere else. The file governs this repository's
  own audit and is not published with the crate: a consumer running
  `cargo audit` sees both advisories and makes their own call.

- **Diff results are now safe to apply to a live database.**
  Grouped permission actions (`FOR select, create ...`) compare
  equal to the engine's split echo instead of reporting a permanent
  false modification. Modify-class diffs render `OVERWRITE` forms:
  a modified field re-defines with `OVERWRITE`, and a permissions
  change carries the owning table's FULL definition, because a
  permissions-only `DEFINE TABLE` would silently reset the table's
  mode.

- **`OVERWRITE` rendering across the schema layer.** Tables, fields,
  indexes, events, analyzers, and access methods gain
  `to_surql_overwrite`, and `generate_table_sql_overwrite` renders a
  table's full statement set with it. `IF NOT EXISTS` creates and
  then never updates, so a consumer whose definitions evolve needs
  the replacing form to bring an existing database up to the code's
  schema; data is untouched, only definitions are replaced.

- **`DatabaseClient::caller_session` opens per-caller engine
  sessions.** A cloned SDK handle is its own session, so one
  connection can hold a root session and record-authenticated caller
  sessions side by side, with the engine applying `PERMISSIONS` to
  each session's actor. The method authenticates a record access
  token on a fresh clone, verifies the engine bound a record identity
  (refusing database-level tokens that `PERMISSIONS` would not
  filter), and returns a client whose drop ends the session.

- **Connection credentials reach embedded engines at build time.**
  `username`/`password` now construct embedded datastores with the
  root user in place, so anonymous sessions stop acting as owner and
  the engine is lockable from configuration alone. Remote engines
  keep the existing signin path.


- **`Query::set` / `set_expr` accept dotted paths.** `SET
  metadata.processing = {...}` is native SurrealDB nested assignment,
  and the schema layer already accepts dot notation for field
  definitions, but the update builder rejected any dotted target as an
  invalid identifier. Targets now validate per segment.

- **`FLEXIBLE` now renders immediately after the `TYPE` clause.** The
  previous trailing position (`... READONLY FLEXIBLE;`) is a parse error
  on SurrealDB v3 ("FLEXIBLE must be specified after TYPE"), so any
  schema combining a flexible object field with `READONLY`, `DEFAULT`,
  `VALUE`, or `ASSERT` failed to apply. Verified against v3.0.5: the
  after-TYPE position is accepted in every combination, including
  `option<object> FLEXIBLE`.

## [0.30.0] - 2026-07-29

### Added

- **Files & buckets (SurrealDB v3 object storage), full code-first depth.**
  - **Field types**: `FieldType::File` / `FieldType::Bytes` (emit `TYPE file` /
    `TYPE bytes`) plus `file_field(name)` / `bytes_field(name)` builders; the
    `INFO FOR ...` field parser round-trips both.
  - **Schema**: new `schema::bucket` module — `BucketDefinition` with
    `bucket_schema(name, backend)` / `memory_bucket(name)` /
    `file_bucket(name, path)` builders, rendering `DEFINE BUCKET
    [IF NOT EXISTS|OVERWRITE] … BACKEND "…" [READONLY] [PERMISSIONS …]
    [COMMENT "…"]`, `REMOVE BUCKET …`, and `ALTER BUCKET [IF EXISTS] …`
    (`READONLY`/`DROP READONLY`, `BACKEND`/`DROP BACKEND`, `PERMISSIONS`,
    `COMMENT`/`DROP COMMENT`). Exposed via `generate_bucket_sql[_with_options]`.
  - **Migrations**: `DiffOperation::{AddBucket, DropBucket, ModifyBucket}`,
    a `bucket` field on `SchemaDiff`, `diff_buckets(code, db)`, and bucket
    support threaded through `SchemaSnapshot`, `VersionedSnapshot`, the
    `SchemaRegistry` (`register_bucket` / `get_registered_buckets`), the
    initial-migration generator, drift detection, and `schema generate`.
  - **Parser**: `parse_bucket` reconstructs `BucketDefinition` from
    `INFO FOR DB` (`bu` / `buckets`) into `DatabaseInfo::buckets`.
  - **`FileRef` value type** (`types::file`): `{ bucket, key }` exposing
    SurrealDB's **canonical** key form — the key is stored *verbatim*
    (including the server's leading slash, e.g. `/a.txt`), while `Display`
    always renders a single-slash pointer (`bucket:/a.txt`) for any input.
    serde round-trips the structured form (`{ bucket, key: "/a.txt" }`) and
    accepts the `f"bucket:/key"` literal the SDK emits. The Rust SDK decodes
    `file` values (in `head`/`file::list`/record fields) straight to that
    literal, so — unlike the Python port — no `file::bucket`/`file::key`
    projection is needed.
  - **Runtime API**: `DatabaseClient::bucket(name)` returns a `Bucket` handle
    with `put` / `put_if_not_exists` / `get` / `get_text` / `exists` / `head` /
    `delete` / `copy` / `copy_if_not_exists` / `rename` /
    `rename_if_not_exists` / `list`. Every op uses the parameterised
    `type::file($bucket, $key)` constructor with **bound** params (never
    string-interpolated). Binary payloads bind as a native
    `surrealdb::types::Value::Bytes` via the new
    `DatabaseClient::query_with_surreal_vars` (no base64) — the JSON bind path
    cannot carry raw bytes. Data is accepted as a `FileData::Text|Bytes` enum.
  - **CLI**: new `surql bucket` group — `define` / `list` / `rm` plus file
    ops `put` / `get` / `delete` / `exists` / `files`.
  - Buckets require the server's `SURREAL_CAPS_ALLOW_EXPERIMENTAL=files`
    environment variable (the feature is hidden and not enabled by
    `--allow-all`; the `--allow-experimental files` flag form is broken). Live
    round-trip coverage is in `tests/integration_files.rs` (embedded probe +
    an `#[ignore]`d server test gated on `SURREAL_FILES_URL`), verified against
    SurrealDB 3.1.3.

- **Sessions documented as unsupported.** The Rust `surrealdb` crate has no
  multiplexed-session API, so `surql-rs` deliberately ships none (unlike the
  Python / TypeScript ports). The new `connection::session` module documents
  this and advises a separate `DatabaseClient` per isolated namespace/auth
  context.

## [0.29.0] - 2026-06-17

### Added

- **Full-text search (BM25) is now first-class — the sparse leg of hybrid
  retrieval.** Define a `DEFINE ANALYZER` in code with `analyzer(name)` /
  `standard_analyzer(name)` (`AnalyzerDefinition` + `Tokenizer` + `TokenFilter`,
  rendered via `generate_analyzer_sql` / `generate_analyzer_sql_with_options`);
  build a BM25-scored full-text index with `bm25_index(name, columns, analyzer)`
  (or `search_index(...).with_analyzer(...).with_bm25().with_highlights()`); and
  run the lexical query with `Query::fulltext_search(field, reference, query)` +
  `Query::search_score(reference, alias)`, or the `fulltext_search_query(...)`
  helper. Pair it with `vector_search` and fuse the two result orders by rank
  (Reciprocal Rank Fusion). Verified end-to-end against an embedded SurrealDB
  engine in `tests/integration_fulltext.rs`.

### Fixed

- **Full-text index now emits the SurrealDB 3.x `FULLTEXT` keyword.** The full-
  text index keyword was renamed from `SEARCH` to `FULLTEXT` in SurrealDB 3.0, so
  the previous output (`... SEARCH ANALYZER ascii`) was a parse error on v3.
  `IndexType::Search` / `search_index` / `IndexDefinition::to_surql*` and the
  migration diff now emit `FULLTEXT`, and the `INFO FOR TABLE` index parser
  recognises both spellings. See `docs/v3-patterns.md` §9 — including the note
  that the v3 streaming executor's full-text scan returns rows in BM25 relevance
  order but `search::score` is not plumbed through it (returns 0), so rank by the
  scan's natural order.

## [0.28.1] - 2026-06-12

### Fixed

- **Connect retries no longer mask the real failure behind "Already
  connected".** The SDK engine connects once per handle and rejects a second
  `connect`; after a partially-successful attempt (engine up, then credential
  signin or namespace selection failed), every retry died on that rejection,
  so the surfaced error was `Already connected` instead of the actual failure
  (e.g. `There was a problem with authentication`), and retries 2..n never
  re-attempted the failing step at all. `DatabaseClient` now tracks
  engine-level connection state: a retry — or a `connect` on an
  already-connected client (reconnect), which failed the same way — skips the
  engine connect and resumes at the step that failed.

## [0.28.0] - 2026-06-06

### Fixed

- **Table-level `PERMISSIONS` now render correctly.** `TableDefinition` emitted
  a malformed `DEFINE FIELD PERMISSIONS FOR {action} ON TABLE ...` per action,
  which SurrealDB rejects (`Unexpected token FOR`). Table permissions now render
  inline on the `DEFINE TABLE` statement (`... PERMISSIONS FOR select WHERE ...
  FOR create WHERE ...`), the only valid placement. Affects
  `to_surql_with_options` / `to_surql_all_with_options` and `generate_table_sql`.
- **Edge table `PERMISSIONS` were silently dropped.** `EdgeDefinition` ignored
  its `permissions` entirely; they now render inline on the
  `DEFINE TABLE ... TYPE RELATION` statement.
- **Migration diff renders a permissions change as valid SurrealQL.** A
  `ModifyPermissions` diff emitted the same malformed `DEFINE FIELD PERMISSIONS`
  form; it now emits a single `DEFINE TABLE <t> PERMISSIONS ...` statement. (A
  `SCHEMAFULL` table re-defined this way falls back to `SCHEMALESS`; full-mode
  fidelity is a follow-up once the diff carries the table mode.)

### Added

- **Expression-valued `UPDATE ... SET` for atomic read-modify-writes.**
  `Query::update_set` begins an `UPDATE <target> SET ...` whose assignments are
  supplied via `set` (literal) or `set_expr` (expression-valued), combinable
  with `where_` and `RETURN`. `Expression` now implements the standard
  arithmetic operators (`+ - * /` over anything `Into<Expression>`, with
  `From<i64|i32|f64>` for numeric literals), so a `SET` value can reference the
  row's current fields — e.g. `UPDATE t SET n = n + 1 WHERE ...` collapses a
  read-modify-write into one statement.
- **`is_none` / `is_not_none` operators** (`field IS NONE`) — the correct guard
  for an absent optional field, which SurrealDB reports as `NONE`, not `NULL`.
- `AccessDefinition::to_surql_with_options(if_not_exists)` and
  `generate_access_sql_with_options(access, if_not_exists)` to emit
  `DEFINE ACCESS IF NOT EXISTS ...` for idempotent re-application (e.g. a
  persistent store applying its schema on every connect).

## [0.2.7] - 2026-05-30

### Added

- Typed `record<table>` field emission. `FieldDefinition` gains a
  `target_table` field, and `record_field(name, Some("user"))`, the new
  `target_table(...)` builder setter, and `with_target_table(...)` all render
  `TYPE record<user>`. A canonical `type::record("X", $value)` coercion on a
  RECORD field is auto-lifted into `target_table` at build time, dropping the
  now redundant VALUE clause. The `DEFINE FIELD` parser reads `record<table>`
  back into `target_table` so typed records round-trip.

## [0.2.6] - 2026-05-22

Maintenance release focused on closing open security, dependency, and
CI-hygiene work. No public-API breaking changes.

### Security

- Bumped `openssl` from `0.10.79` to `0.10.80` via `cargo update`,
  closing [CVE-2026-45784](https://nvd.nist.gov/vuln/detail/CVE-2026-45784)
  (medium severity, potential out-of-bounds write in
  `CipherCtxRef::cipher_update_inplace` for AES-KW-PAD ciphers). The
  crate's default `client-rustls` backend never links `openssl`; the
  bump only affects consumers that opt into the `client` /
  `client-tls` feature.

### Fixed

- Daily Security Audit workflow no longer fails on every scheduled
  run. Replaced the deprecated Node.js 20
  `rustsec/audit-check@v2.0.0` action with a direct
  `taiki-e/install-action` + `cargo audit` invocation. The new step
  exits non-zero only on actual vulnerabilities; informational
  unmaintained warnings (atomic-polyfill, bincode 2.x) are surfaced as
  logs because they reach the dep graph transitively through
  `surrealdb` and are not actionable from this repo.

### Changed

- CI workflow (`ci.yml`) now runs the `stable` Rust toolchain only on
  push and pull-request triggers. `beta` toolchain coverage moved to
  the daily Nightly workflow so regressions still surface within 24
  hours without paying for two parallel jobs on every PR rev.
- Added `paths-ignore` filters to `ci.yml` and `coverage.yml` so pure
  documentation, LICENSE, or `.editorconfig` / `.gitignore` changes no
  longer trigger a full compile + clippy + test run. `docs.yml`
  already handles documentation rebuilds.
- Dependabot auto-merge workflow now uses
  `dependabot/fetch-metadata@v3` and
  `lewagon/wait-on-check-action@v1.7.0`, the latest stable majors of
  both actions.
- `docs/features.md` and `docs/migration.md` corrected: the default
  feature has been `client-rustls` since `0.2.3`, not `client`.

### Added

- `docs/connection-management.md` documents the task-scoped current
  client, `ConnectionRegistry`, `AuthManager`, `StreamingManager` /
  `LiveQuery`, and `Transaction`.
- `docs/caching.md` documents `CacheManager`, the `MemoryCache` and
  `RedisCache` backends, the `cached` / `cached_with` /
  `cache_key_for` helpers, and the invalidation surface.
- `docs/orchestration.md` documents `EnvironmentConfig` /
  `EnvironmentRegistry`, `DeploymentPlan`, `DeploymentCoordinator`,
  the four built-in `DeploymentStrategy` implementations (Sequential,
  Parallel, Rolling, Canary), `DeploymentResult`, and
  `check_environment_health` / `verify_connectivity`.
- `docs/migration.md` now carries an `Upgrading 0.2.5 -> 0.2.6`
  section.
- `mkdocs.yml` navigation surfaces the three new module pages under
  Guides.

## [0.2.5] - 2026-05-19

Brings the parser, RecordID, and batch surfaces to feature parity with
the surql-py 1.6.4 / 1.7.0 release window (and the sibling surql v1.5.0
TypeScript port). Also hardens the CI workflow set so PRs do not double-
run and the docs build no longer serialises every ref behind a single
queue.

### Added

- **`parse_edge_info(edge_name, info, define_table)`** in
  `surql::schema::parser` — counterpart to [`parse_table_info`] for
  graph-edge tables defined via `edge_schema` / [`EdgeDefinition`]. Edge
  mode is detected from the `DEFINE TABLE` statement: `TYPE RELATION`
  resolves to `EdgeMode::Relation`, `SCHEMAFULL` to `EdgeMode::Schemafull`,
  anything else to `EdgeMode::Schemaless`. `FROM <table>` and
  `TO <table>` are extracted independently so a malformed live
  definition that lost one clause surfaces as missing-endpoint drift
  instead of a parse failure. On `Relation`-mode edges the auto-emitted
  `in` and `out` field declarations SurrealDB stores are stripped on
  parse — they are implicit when `TYPE RELATION` is set, so the
  code-side `EdgeDefinition` does not declare them and round-trip diffs
  were flagging them as orphan additions. Per-action `PERMISSIONS`
  round-trip via the new `parse_table_permissions` helper.

- **`parse_table_permissions(definition)`** in
  `surql::schema::parser` — extracts the per-action `PERMISSIONS` rules
  from a `DEFINE TABLE` statement string. Returns `None` for the trivial
  `NONE` / `FULL` postures (the code-side helpers have no representation
  for those) and for definitions without a `PERMISSIONS` clause.
  Recognises the expanded form
  (`FOR select WHERE r1 FOR create WHERE r2 …`), the comma-joined form
  v3 emits when several actions share a rule
  (`FOR select, create, update, delete WHERE r`), and arbitrary mixes of
  both. The Rust `regex` crate does not support lookahead, so the body
  is split on `FOR` boundaries before applying the per-clause matcher —
  same per-action map shape the surql-py port produces, no lookahead.

- **`parse_table_info(name, info, define_table)`** — the optional third
  argument is the `DEFINE TABLE <name> ...` statement string, fetched
  from `INFO FOR DB`'s `tables.<name>` entry. SurrealDB v3's
  `INFO FOR TABLE` does *not* include the table-level `DEFINE TABLE`
  statement, so table mode and `PERMISSIONS` cannot be recovered from
  it alone. Without `define_table` the parser falls back to the legacy
  `tb` key inside the response (the v1 / v2 shape) and table mode
  defaults to `Schemaless` on v3.

- **`strip_brackets(value)`** in `surql::types`, re-exported from the
  crate root. SurrealDB v3 wraps record-id keys that contain anything
  other than `[A-Za-z_][A-Za-z0-9_]*` or pure digits in unicode angle
  brackets `⟨ … ⟩` (U+27E8 / U+27E9). Downstream consumers that wanted
  the bare `table:id` shape were calling
  `value.replace('⟨', "").replace('⟩', "")` themselves at every API
  boundary; `strip_brackets` centralises that strip and also accepts
  the legacy ASCII `< … >` form. `None` is passed through untouched so
  the helper is safe to apply unconditionally.

- **`upsert_many_in_tx(txn, table, items, conflict_fields)`** — atomic
  counterpart to [`upsert_many`]. Queues one
  `UPSERT <target> CONTENT { … }` statement per item on the supplied
  [`Transaction`] buffer; the per-record statements inherit the
  surrounding `BEGIN TRANSACTION` / `COMMIT TRANSACTION` framing so a
  single bad record rolls back the entire batch on commit instead of
  leaving the database half-seeded. `Transaction::execute` queues raw
  SQL without param bindings, so the CONTENT payload is rendered as a
  SurrealQL object literal (rather than `$data`-bound as it is in
  autocommit mode). Both `upsert_many` and `upsert_many_in_tx` accept
  an optional `conflict_fields` slice that emits an inline-value
  `WHERE … AND …` clause appended to each UPSERT.

### Fixed

- **`build_upsert_query` emitted `UPSERT INTO <table> [ {…}, {…} ]`**,
  which SurrealDB v3 rejects with a parse error — v3 wants a single
  record-id or table target after `UPSERT`, not an array literal. The
  renderer now emits one `UPSERT <target> CONTENT { … }` statement per
  item, joined by `;`, matching the surql-py 1.7.0 / surql 1.5.0
  shape that is portable across the sibling ports. The
  pre-0.2.5 source comment acknowledged the bug ("not valid SurrealDB
  v3 SurrealQL") but kept the broken shape for byte-for-byte parity
  with the older surql-py renderer; that parity bridge is no longer
  needed.

- **`build_upsert_query` `conflict_fields` emitted `WHERE field =
  $item.field`**, which has no `$item` binding in scope at the call
  site (and the rendered string is also fed verbatim to
  `Transaction::execute`, which queues raw SQL without binding params).
  The renderer now inlines the conflict values
  (`WHERE email = 'a@b.com' AND tenant = 'BFS'`), matching the surql
  1.5.0 fix.

- **`RecordID::Display` emitted ASCII `<id>` brackets** for ids that
  could not be rendered bare. SurrealDB v3 rejects ASCII `<` / `>` in
  record-id positions with
  `Unexpected token '<', expected a record-id key`; the output now uses
  the v3-correct unicode escape syntax `⟨id⟩` (U+27E8 / U+27E9).
  `RecordID::parse` accepts both forms on input so legacy wire payloads
  still round-trip cleanly. **Breaking** for callers that asserted on
  the exact `Display` output; the SQL shape is identical otherwise.

- **`RecordID::needs_angle_brackets` accepted leading-digit ids bare**
  (`chunk:1abc`). The pre-0.2.5 `simple_id_pattern` was
  `[A-Za-z0-9_]+`, which let `1abc` slip through and produced a literal
  v3 rejects with `Unexpected token`. The new `identifier_id_pattern`
  is `[A-Za-z_][A-Za-z0-9_]*`, with a separate allow-list for pure-
  digit strings (which v3 parses as integer-key ids and round-trips
  bare). Matches surql-py 1.7.0.

### Changed

- **`upsert_many` no longer routes through
  `UPSERT <table> CONTENT $data` for items that lack an `id` field**.
  The autocommit path always pins the target — `data.id` when present,
  `<table>` otherwise — and strips `id` from the bound payload so v3
  does not reject the duplicate field.

- **CI workflow set hardened against runaway runs**:
  - `docs.yml` switched from the global `group: pages` concurrency
    queue (which serialised every build + deploy across all refs and
    caused multi-day stalls when a long-running deploy held the queue)
    to a per-ref group with `cancel-in-progress: true`.
  - `ci.yml` and `coverage.yml` gained per-ref concurrency groups so
    rapid pushes to a PR cancel the in-progress run. The redundant
    `push: branches: ['release/**']` triggers were dropped — release
    branches only ever receive PRs that already fired the workflow via
    `pull_request`, so the push trigger was pure duplicated work.
  - `audit.yml`, `dep-review.yml`, and `pr-title.yml` gained per-ref
    concurrency groups so a sequence of PR edits cancels the in-progress
    lint and only the latest revision is checked.

### Verified

- `cargo fmt --all -- --check` — clean.
- `cargo clippy --lib --all-features --tests -- -D warnings` — clean.
- `cargo test --lib --no-default-features` — **927 passed, 0 failed**.
- `cargo test --lib --all-features` — **1088 passed, 0 failed**
  (baseline was 1066 on 0.2.4; +22 regression tests covering
  `parse_edge_info`, `parse_table_permissions`, `strip_brackets`,
  unicode-bracket `RecordID::Display`, and the per-record
  `build_upsert_query` shape).
- All integration tests compile.

## [0.2.4] - 2026-05-02

### Added

- `client-wasm` feature (Oneiriq/surql-rs#115). Wasm-friendly client
  surface that compiles cleanly to `wasm32-unknown-unknown`. Pulls
  `surrealdb` with `protocol-ws` + `kv-mem` only -- no `rustls` /
  `native-tls` / `reqwest`, since browsers terminate TLS at the
  WebSocket layer and `kv-mem` lets wasm callers run an embedded engine
  for local state and tests. Exposes the same
  `DatabaseClient` / `executor` / `crud` / `graph` / `batch` API as
  `client-rustls`.
- `[target.'cfg(target_arch = "wasm32")'.dependencies]` block in
  `Cargo.toml` that overrides `tokio` to the wasm-buildable subset
  (`sync`, `macros`, `rt`, `time`) and pulls
  `getrandom 0.3` with the `wasm_js` feature so `ulid` /
  `rand_core` link on wasm.
- `.cargo/config.toml` with the
  `--cfg=getrandom_backend="wasm_js"` rustflag required by
  `getrandom 0.3` on `wasm32-unknown-unknown` (the feature flag alone is
  insufficient -- see https://docs.rs/getrandom/0.3/#webassembly-support).
- `scripts/check-wasm.sh` -- canonical local + CI gate for the wasm
  build. On macOS auto-detects Homebrew LLVM so `cc-rs` can hand
  `ring 0.17`'s build script a wasm-capable clang (Apple's
  `/usr/bin/clang` has no wasm32 backend).

### Changed

- The optional `tokio` dependency moved from a top-level
  `[dependencies]` declaration with `features = ["full"]` to two
  target-specific declarations: native targets keep the historical
  `["full"]` feature set, while `wasm32-*` targets get
  `["sync", "macros", "rt", "time"]`. No source-level API changes.

### Fixed

- `cargo build --target wasm32-unknown-unknown -p oneiriq-surql --no-default-features --features client-wasm`
  now succeeds on a system with a wasm-capable clang in scope. Unblocks
  Oneiriq/pixel-stroke#236 (web-build of `pixel-stroke-persistence`).

## [0.2.3] - 2026-05-02

### Changed

- The default feature set is now `["client-rustls"]` (pure-Rust TLS).
  Previously the default was `["client"]`, which pulled
  `surrealdb/native-tls` and `reqwest/default-tls` and therefore
  `openssl-sys` into the dependency graph. The historical native-tls
  backend is still available via the `client` feature (now also exposed
  under the `client-tls` alias) for consumers that need the system
  OpenSSL stack.
- The `cli` and `orchestration` features now depend on `client-rustls`
  instead of `client` so that `cargo install oneiriq-surql --features cli`
  and other typical builds no longer compile against `openssl-sys`.

### Security

- Drops the `openssl-sys` transitive dependency from the default
  dependency graph, clearing the following Dependabot advisories on
  this crate's published default build:
  - rust-openssl: incorrect bounds assertion in AES key wrap (HIGH)
  - rust-openssl: unchecked callback length in PSK / cookie trampolines
    leaks adjacent memory to peer (HIGH)
  - rust-openssl: `MdCtxRef::digest_final()` writes past caller buffer
    with no length check (HIGH)
- Consumers who explicitly opt into `--features client` (or the
  `client-tls` alias) still link the system OpenSSL stack and remain
  subject to upstream `rust-openssl` advisories.

## [0.2.2] - 2026-04-21

### Added

- `client-rustls` feature (Oneiriq/surql-rs#97). Same surface as the
  default `client` feature, but with a pure-Rust TLS stack
  (`rustls` + `webpki-roots`) instead of `native-tls`. Enables
  building on runners that do not have `libssl-dev` / the system
  OpenSSL headers installed. See
  [docs/features.md](features.md#picking-a-tls-backend) for the
  trade-offs and [docs/migration.md](migration.md#6-switching-to-client-rustls-022)
  for a switching guide.

### Changed

- The `client` feature now explicitly selects `surrealdb/native-tls`
  and `reqwest/default-tls`. Behaviour is unchanged for existing
  consumers (the implicit TLS stack was already `native-tls`), but
  the TLS backend is no longer inherited from upstream defaults --
  it is pinned by the feature flag. No API changes.
- Optional `surrealdb` and `reqwest` dependencies are declared with
  `default-features = false` so the TLS backend is selected
  exclusively by `client` / `client-rustls`.

## [0.2.1] - 2026-04-18

### Documentation

- `docs/features.md` -- full feature-flag reference.
- `docs/query-ux.md` -- before / after walkthroughs for the 0.2
  crate-root helpers (`type_record`, `type_thing`, `extract_many`,
  `has_result`, `select_expr`, `execute`, `aggregate_records`).
- `docs/v3-patterns.md` -- SurrealDB v3-specific SurrealQL shapes
  (subprotocol handshake, `type::record` rename, datetime coercion,
  unrolled graph depth, rejected `UPSERT INTO [...]`, buffered
  transactions, `SurrealValue` avoidance).
- `docs/cli.md` -- full subcommand reference (replaces the pre-0.1
  "planned" placeholder).
- `docs/migration.md` -- 0.1.x -> 0.2.x upgrade notes.
- Updated README top-level example with `type_record`,
  `Query::select_expr`, `Query::execute`, `aggregate_records`.
- Updated `mkdocs.yml` nav with the new pages and `docs.rs/oneiriq-surql`
  reference link.
- Fixed pre-existing rustdoc intra-doc link warnings so
  `cargo doc --no-deps --all-features` succeeds under
  `RUSTDOCFLAGS="-D warnings"`.

No API changes.

## [0.1.0 - 0.2.0] see releases

### Added

- `migration::versioning` -- `VersionedSnapshot`, `VersionGraph`, and
  `compare_snapshots` for DAG-based migration history.
- `migration::generator` -- generate migration files (`generate_migration`,
  `generate_initial_migration`, `create_blank_migration`,
  `generate_migration_from_diffs`) with atomic writes and round-trip load.
- `migration::diff` -- schema diff engine (`diff_tables`, `diff_fields`,
  `diff_indexes`, `diff_events`, `diff_permissions`, `diff_edges`,
  `diff_schemas`).
- `migration::{models, discovery}` -- `.surql` file-format migrations with
  `-- @metadata` / `-- @up` / `-- @down` section markers and SHA-256 checksum.
- `schema::{visualize, themes, utils}` -- Mermaid / GraphViz / ASCII diagrams
  with modern / dark / forest / minimal themes.
- `schema::parser` -- parses SurrealDB `INFO FOR DB` / `INFO FOR TABLE`
  responses back into schema definitions.
- `schema::{validator, validator_utils}` -- cross-schema validation with
  severity-filtered reports.
- `schema::{sql, registry}` -- full DEFINE-statement composition and a
  thread-safe `SchemaRegistry`.
- `schema::{fields, table, edge, access}` -- code-first schema DSL.
- `query::{builder, helpers}` -- immutable `Query` with fluent chaining.
- `query::expressions` -- 25+ function builders and typed expression kinds.
- `query::{hints, results}` -- query optimization hints + typed result
  wrappers with raw-response extraction helpers.
- `connection::{config, auth}` -- connection configuration (URL / ns / db /
  timeouts / retry / live-queries gate) + auth credential types.
- `types::{operators, record_id, record_ref, surreal_fn, reserved, coerce}`
  -- operator enum + `RecordID<T>` with angle-bracket syntax + reserved-word
  checks + ISO-8601 datetime coercion.
- `error::SurqlError` -- unified error enum with `Context` chaining trait.

### Notes

This is a pre-release port of [surql-py](https://github.com/Oneiriq/surql-py)
targeting 1:1 feature parity. The runtime async client, CRUD executor, and
CLI land in the 0.1 -> 0.2 window.
