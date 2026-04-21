# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.2] - 2026-04-21

### Added

- `client-rustls` feature (Oneiriq/surql-rs#97). Same surface as the
  default `client` feature, but with a pure-Rust TLS stack
  (`rustls` + `webpki-roots`) instead of `native-tls`. Enables
  building on runners that do not have `libssl-dev` / the system
  OpenSSL headers installed. See
  [docs/features.md](docs/features.md#picking-a-tls-backend) for the
  trade-offs and [docs/migration.md](docs/migration.md#6-switching-to-client-rustls-022)
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
