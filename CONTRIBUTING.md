
## Local pre-push hook

This repo ships a `.githooks/pre-push` that runs the same checks GitHub Actions runs: `cargo fmt`, `clippy`, `cargo test --lib`, doc tests, the `client-rustls` build (and the check that it pulls no OpenSSL), the no-features build, `cargo audit`, the MSRV check on Rust 1.92, the `client-wasm` build, and `mkdocs build --strict`. Wire it up once per clone:

```bash
git config core.hooksPath .githooks
```

The gates that need an extra tool run when it is installed and otherwise print a `SKIPPED:` line naming what to install. To run all of them:

```bash
cargo install --locked cargo-audit
rustup toolchain install 1.92 --profile minimal
rustup target add wasm32-unknown-unknown   # plus a wasm-capable clang, see scripts/check-wasm.sh
pip install mkdocs-material mkdocs-minify-plugin
```

Integration tests (against a local `surrealdb/surrealdb:v3.0.5` container) are opt-in:

```bash
export SURQL_PRE_PUSH_INTEGRATION=1
docker run -d -p 8000:8000 --name surrealdb surrealdb/surrealdb:v3.0.5 start --user root --pass root memory
```

Bypass (rarely, only with authorisation):

```bash
git push --no-verify
```

## Fuzzing

`fuzz/` is a `cargo fuzz` crate with its own workspace. Its targets use the engine's own SurrealQL parser (`surrealdb-core`) as the oracle: rendered record ids, identifiers, string literals, and values must parse back as exactly one statement and as the input they were rendered from, and the `INFO` parsers must never panic on server text.

| Target | Checks |
|---|---|
| `record_id` | `RecordID` display parses to the same table and string key; `parse` inverts it |
| `ident` | `quote_ident` / `quote_str` read back unchanged and stay one statement |
| `value` | any JSON rendered by `quote_value_public` is one inert literal equal to the input |
| `schema_info` | every `schema::parser` entry point returns on arbitrary definition text |

libFuzzer needs Linux (or Docker) and the unstable sanitizer flags. Current nightlies do not compile `diskann-wide` 0.54 (a surrealdb-core dependency, E0283), so run cargo-fuzz on a stable toolchain with `RUSTC_BOOTSTRAP=1`. `-a` keeps debug assertions and overflow checks on in the optimised build:

```bash
cd fuzz
RUSTC_BOOTSTRAP=1 cargo +1.98 fuzz run -O -a value -- -max_total_time=600
```

The first build compiles the engine with instrumentation: allow about half an hour and 8 GB of memory.
