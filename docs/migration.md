# Upgrading 0.1.x -> 0.2.x

This page focuses on API deltas between the 0.1.0 feature-complete
release and the 0.2.0 "query-UX" wave. For the `.surql` migration
**file format** (which has not changed), see [Migrations](migrations.md).

## TL;DR

- **No removals.** Every 0.1.x public item is still present in 0.2.x.
- SurrealDB driver bumped to **3.x** (crate feature-gated). Several
  SurrealQL shapes were adjusted - see [v3 patterns](v3-patterns.md).
- New crate-root helpers: `type_record`, `type_thing`, `extract_many`,
  `has_result`.
- New builder methods: `Query::select_expr`, `Query::execute`.
- New aggregation API: `AggregateOpts`, `aggregate_records`,
  `build_aggregate_query`.
- Full-tree CLI now lands behind the `cli` feature.
- Pre-push hook + `CONTRIBUTING.md` added; no runtime effect.

## Step-by-step

### 1. Bump the dependency

```toml
[dependencies]
oneiriq-surql = "0.2"
```

The crate still installs from `crates.io` as `oneiriq-surql` and is
imported as `use surql::...`. No rename.

### 2. (Optional) drop boilerplate around v2-era helpers

None of the pre-0.2 helpers were removed, but many calls can be
shortened. See [Query UX helpers](query-ux.md) for the full
before/after matrix. Representative examples:

| 0.1.x                                                             | 0.2.x                                     |
|-------------------------------------------------------------------|-------------------------------------------|
| `execute_query(&client, &q).await?`                               | `q.execute(&client).await?`               |
| Hand-rendered `type::thing('task','...')`                         | `type_record("task", id).to_surql()`      |
| `extract_result(&raw).into_iter().map(Value::Object).collect()`   | `extract_many(&raw)`                      |
| Stringly-typed select list with manual `AS` concatenation         | `Query::select_expr(vec![as_(...), ...])` |
| Hand-rolled `SELECT count() ... GROUP ALL` + extraction glue      | `aggregate_records(client, "t", opts)`    |

### 3. v3-specific SurrealQL shapes

If your code contained raw SurrealQL passed to `client.query(...)`,
check the patterns in [v3 patterns](v3-patterns.md). The most common
breakages:

1. `type::thing(...)` - rename to `type::record(...)` (or use the
   `type_record` helper).
2. `UPSERT INTO <table> [...]` - switch to per-record `UPSERT
   <target> CONTENT $data` or call `batch::upsert_many`.
3. Incoming edges - write `FROM <record><-<edge>`, not
   `FROM <-<edge><-<record>`.
4. Variable-depth traversal - unroll to a fixed `->edge->?` chain per
   iteration instead of `->edge{d}->`.
5. Datetime inserts - keep the `<datetime> $var` cast visible in
   SurrealQL.

### 4. CLI migration

The CLI shipped partial in 0.1.x and is now complete in 0.2.0 under the
`cli` feature. Existing automation that called ad-hoc SurrealQL
through `surql db query` or `surql migrate up` keeps working
unchanged. New commands:

- `surql migrate squash <FROM> <TO>` - squash a contiguous range.
- `surql migrate validate [<VERSION>]` - structural validation.
- `surql schema visualize` / `surql schema export` / `surql schema
  inspect`.
- `surql orchestrate deploy|status|validate`.

See [CLI reference](cli.md) for the full tree.

### 5. Feature flags

0.2.x formalises the feature matrix:

| Feature        | 0.1.x                   | 0.2.x                                        |
|----------------|-------------------------|----------------------------------------------|
| `client`       | default                 | default                                      |
| `cli`          | partial (migrate only)  | full tree; implies `client` + `orchestration` + `settings` |
| `cache`        | new                     | `MemoryCache` backend + `CacheManager`       |
| `cache-redis`  | new                     | `RedisCache` backend (implies `cache`)       |
| `settings`     | new                     | layered `Settings` / `SettingsBuilder`       |
| `orchestration`| new                     | deployment strategies, environment registry  |
| `watcher`      | new                     | filesystem watcher                           |

See [Feature flags](features.md) for the detailed matrix.

## Deprecations

None in 0.2.x.

## Build / tooling changes

- CI and the new [`pre-push` hook](#pre-push-hook) require
  `cargo clippy --all-targets --all-features -- -D warnings`.
- Rustdoc now builds cleanly under `RUSTDOCFLAGS="-D warnings"`.

### Pre-push hook

Optional local hook that mirrors CI (`fmt`, `clippy`, `doc`,
`nextest`). Enable with:

```shell
git config --local core.hooksPath .githooks
```

Full steps in `CONTRIBUTING.md`.

## What's next

- [v3 patterns](v3-patterns.md) - the SurrealDB v3 shapes we adapted to.
- [Query UX helpers](query-ux.md) - detailed before/after walkthroughs.
- [Changelog](changelog.md) - dated release summary.
