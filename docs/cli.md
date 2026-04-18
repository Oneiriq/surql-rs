# CLI

The `surql` binary is under active development. When it ships (0.2), it
will provide:

- `surql migrate create <description>` -- blank migration template.
- `surql migrate up [--steps N] [--dry-run]` -- apply pending migrations.
- `surql migrate down [--steps N] [--yes]` -- roll back.
- `surql migrate status` -- applied vs pending.
- `surql schema show [--format json|mermaid|graphviz|ascii]` -- inspect the
  running schema.
- `surql schema diff [--compare-live]` -- drift detection.
- `surql schema validate [--fail-on-drift]` -- CI validation.
- `surql db init` / `surql db reset` / `surql db health` -- management.

The target surface mirrors `surql-py`'s CLI. Track progress on
[GitHub Issues](https://github.com/Oneiriq/surql-rs/issues).
