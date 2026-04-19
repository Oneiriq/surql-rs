# CLI

The `surql` binary ships under the `cli` feature flag. It is a thin
wrapper over the library - every side-effect is delegated to an
existing public function.

```shell
cargo install oneiriq-surql --features cli
```

## Global flags

Every subcommand accepts:

| Flag                 | Purpose                                                                 |
|----------------------|-------------------------------------------------------------------------|
| `--config <PATH>`    | Override automatic `Settings` discovery with a specific TOML file.      |
| `-v`, `--verbose`    | Emit extra diagnostic output for subcommands that support it.           |
| `--help`             | Standard clap help.                                                     |
| `--version`          | Print the crate version (propagated to every subcommand).               |

Without `--config`, the standard layered lookup runs: environment
variables, `.env`, then `Cargo.toml [package.metadata.surql]`.

## Exit codes

| Code | Meaning                                         |
|------|-------------------------------------------------|
| `0`  | Success.                                        |
| `1`  | Operation failure (`SurqlError` bubbled up).    |
| `2`  | Usage error (enforced by clap's argument parser). |

## Subcommand tree

```text
surql
|- version
|- db
|   |- init
|   |- ping
|   |- info      [--json]
|   |- reset     [--yes]
|   |- query     [<SURQL> | --file PATH]
|   |- version
|- migrate
|   |- up        [--target VERSION] [--dry-run]
|   |- down      [--target VERSION] [--dry-run]
|   |- status
|   |- history
|   |- create    <description> [--schema-dir PATH]
|   |- validate  [<VERSION>]
|   |- generate  [--from VERSION] [--to VERSION]
|   |- squash    <from> <to> [-o PATH] [--dry-run]
|- schema
|   |- show       [<TABLE>]
|   |- diff       [--from PATH] [--to PATH]
|   |- generate   [-o PATH]
|   |- sync       [--dry-run]
|   |- export     [-f json|yaml] [-o PATH]
|   |- tables
|   |- inspect    <TABLE>
|   |- validate
|   |- check
|   |- hook-config
|   |- watch
|   |- visualize  [--theme modern|dark|forest|minimal]
|   |             [-f mermaid|graphviz|ascii]
|   |             [-o PATH]
|- orchestrate
    |- deploy    [--plan PATH] [--strategy sequential|parallel|rolling|canary]
    |            [--environments LIST] [--dry-run]
    |- status    [--plan PATH]
    |- validate  [--plan PATH]
```

## `surql db`

Database utility commands. Each one opens a short-lived connection
from the resolved `Settings`.

- `surql db init` - ensure the `_migration_history` table exists.
- `surql db ping` - connect and issue `INFO FOR DB`.
- `surql db info [--json]` - print resolved namespace / database /
  URL. `--json` emits a machine-readable payload.
- `surql db reset [--yes]` - drop every table in the current database
  (prompts for confirmation unless `--yes` is passed).
- `surql db query [<SURQL>] [--file PATH]` - execute an inline or
  file-loaded SurrealQL statement and pretty-print the result.
- `surql db version` - print the server's `INFO FOR DB` version line.

## `surql migrate`

Wraps the migration runtime. Mirrors the `surql-py` `migrate` typer
group.

- `surql migrate up [--target VERSION] [--dry-run]` - apply pending
  migrations up to and including `--target` (defaults to the latest).
- `surql migrate down [--target VERSION] [--dry-run]` - roll back to
  and including `--target`.
- `surql migrate status` - show applied vs pending counts for the
  configured migrations directory.
- `surql migrate history` - dump the `_migration_history` rows.
- `surql migrate create <description> [--schema-dir PATH]` - scaffold
  a blank migration file with `-- @metadata` / `-- @up` / `-- @down`
  section markers. Slug-cases `<description>` for the filename.
- `surql migrate validate [<VERSION>]` - structural validation
  (section markers, version format, checksum). Optional single-version
  focus.
- `surql migrate generate [--from VERSION] [--to VERSION]` - render a
  diff-based migration by comparing two snapshot versions on disk.
- `surql migrate squash <FROM> <TO> [-o PATH] [--dry-run]` - squash a
  contiguous version range into one migration file.

## `surql schema`

Wraps the schema registry, parser, validator, visualiser, and
hook helpers.

- `surql schema show [<TABLE>]` - execute `INFO FOR DB`
  (or `INFO FOR TABLE <TABLE>`) and print JSON.
- `surql schema diff [--from PATH] [--to PATH]` - compare two
  snapshot files. Defaults (`from` = latest snapshot, `to` = live DB)
  allow bare `surql schema diff` for drift detection.
- `surql schema generate [-o PATH]` - emit the `DEFINE` SurrealQL for
  every registered table and edge.
- `surql schema sync [--dry-run]` - placeholder for code-to-database
  synchronisation (stub; will land in a follow-up).
- `surql schema export [-f json|yaml] [-o PATH]` - export the live
  schema to disk. `yaml` currently emits raw SurrealQL for parity with
  `surql-py`'s YAML output target.
- `surql schema tables` - list every table in the live database.
- `surql schema inspect <TABLE>` - show fields / indexes / events /
  permissions for a single table.
- `surql schema validate` - confirm the registered schema matches the
  live database.
- `surql schema check` - detect schema drift against the latest
  snapshot. Intended for CI.
- `surql schema hook-config` - emit a `.pre-commit-config.yaml`
  fragment running schema drift checks.
- `surql schema watch` - filesystem watcher (requires the `watcher`
  feature; shows a helpful message otherwise).
- `surql schema visualize [--theme modern|dark|forest|minimal] [-f
  mermaid|graphviz|ascii] [-o PATH]` - render the registry as a
  diagram. Defaults to Mermaid with the `modern` theme.

## `surql orchestrate`

Wraps `crate::orchestration`. Requires the `orchestration` feature
(implied by `cli`).

- `surql orchestrate deploy [--plan PATH] [--strategy
  sequential|parallel|rolling|canary] [--environments LIST]
  [--dry-run]` - apply migrations across environments declared in the
  plan file (default `environments.json`).
  `--environments` accepts a comma-separated subset; omitted, every
  registered environment is included.
- `surql orchestrate status [--plan PATH]` - show a health table for
  each environment in the plan.
- `surql orchestrate validate [--plan PATH]` - parse the plan and run
  connectivity checks without applying anything.

## What's next

- [Feature flags](features.md) - what each feature pulls in.
- [Installation](installation.md) - `cargo install` recipes.
- [Migrations](migrations.md) - migration file format and lifecycle.
