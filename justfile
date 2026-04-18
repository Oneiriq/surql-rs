default:
    @just --list

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all --check

lint:
    cargo clippy --all-targets --all-features -- -D warnings

test:
    cargo test --all-features -- --nocapture

test-unit:
    cargo test --lib --all-features

test-doc:
    cargo test --doc --all-features

build:
    cargo build --all-features --release

check:
    just fmt-check
    just lint
    just test
    @echo "All checks passed."

bench:
    cargo bench

docs:
    RUSTDOCFLAGS="--document-private-items" cargo doc --all-features --no-deps --open

watch:
    cargo watch -x 'fmt' -x 'clippy --all-features -- -D warnings' -x 'test --all-features'

clean:
    cargo clean
