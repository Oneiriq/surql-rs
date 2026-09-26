//! `surql` CLI entry point.
//!
//! Thin binary wrapper: every command lives in [`surql::cli`].

use std::process::ExitCode;

fn main() -> ExitCode {
    surql::cli::run()
}
