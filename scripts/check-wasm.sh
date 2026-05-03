#!/usr/bin/env bash
# Build `oneiriq-surql` for `wasm32-unknown-unknown` with the
# `client-wasm` feature flag.
#
# Invoke with no arguments. Honours `$CC_wasm32_unknown_unknown` /
# `$AR_wasm32_unknown_unknown` if already set; otherwise auto-detects
# Homebrew LLVM on macOS so `cargo build --target wasm32-unknown-unknown`
# can hand `cc-rs` a wasm-capable clang for `ring 0.17`'s build script.
#
# This is the canonical local + CI gate for Oneiriq/surql-rs#115.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# Prefer the rustup-managed toolchain when available; the
# rust-toolchain.toml pins `stable`, and rustup's `cargo` proxy honours
# it. Some macOS shells default to a Homebrew `rust` install that
# does not ship the `wasm32-unknown-unknown` target by default.
if [[ -d "$HOME/.cargo/bin" ]]; then
    export PATH="$HOME/.cargo/bin:$PATH"
fi

# Pick a wasm-capable C compiler if the caller has not already configured one.
if [[ -z "${CC_wasm32_unknown_unknown:-}" ]]; then
    if [[ "$(uname -s)" == "Darwin" ]]; then
        # Apple's `/usr/bin/clang` has no wasm32 backend. Prefer Homebrew
        # `llvm` (any version) when it is installed.
        for candidate in /opt/homebrew/opt/llvm/bin/clang \
                         /opt/homebrew/opt/llvm@21/bin/clang \
                         /opt/homebrew/opt/llvm@20/bin/clang \
                         /usr/local/opt/llvm/bin/clang; do
            if [[ -x "$candidate" ]]; then
                export CC_wasm32_unknown_unknown="$candidate"
                # Match the LLVM `ar` to the picked clang. cc-rs honours
                # `AR_wasm32_unknown_unknown`.
                ar_candidate="$(dirname "$candidate")/llvm-ar"
                if [[ -x "$ar_candidate" ]]; then
                    export AR_wasm32_unknown_unknown="$ar_candidate"
                fi
                echo "check-wasm: using $CC_wasm32_unknown_unknown for cc-rs"
                break
            fi
        done
    fi
fi

if [[ -z "${CC_wasm32_unknown_unknown:-}" ]]; then
    echo "check-wasm: no wasm-capable clang configured."
    echo "check-wasm: install one (e.g. \`brew install llvm\` on macOS)"
    echo "check-wasm: and set CC_wasm32_unknown_unknown / AR_wasm32_unknown_unknown."
fi

cargo build \
    --target wasm32-unknown-unknown \
    --no-default-features \
    --features client-wasm \
    -p oneiriq-surql \
    "$@"
