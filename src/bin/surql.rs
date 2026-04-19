//! `surql` CLI entry point.
//!
//! Thin binary wrapper: every command lives in [`surql::cli`].

#![warn(clippy::all)]
#![deny(missing_docs)]

use std::process::ExitCode;

fn main() -> ExitCode {
    surql::cli::run()
}
