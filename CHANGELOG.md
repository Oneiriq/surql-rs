# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
