
## Local pre-push hook

This repo ships a `.githooks/pre-push` that runs the same checks GitHub Actions runs: `cargo fmt`, `clippy`, `cargo test --lib`, doc tests, the `client-rustls` build (and the check that it pulls no OpenSSL), `cargo audit`, the MSRV check on Rust 1.92, the `client-wasm` build, and `mkdocs build --strict`. Wire it up once per clone:

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
