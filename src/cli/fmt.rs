//! Shared output helpers for the `surql` CLI.
//!
//! All terminal-facing formatting helpers live here so the individual
//! subcommand modules can focus on their business logic. Colours are
//! produced through [`colored`] and tables through [`comfy_table`];
//! tests disable colours globally to keep snapshot-friendly output.

use std::fmt::Display;

use colored::Colorize;
use comfy_table::{presets::UTF8_FULL, ContentArrangement, Table};

/// Print an informational message to stdout.
pub fn info(msg: impl Display) {
    println!("{}", msg);
}

/// Print a success message in green to stdout.
pub fn success(msg: impl Display) {
    println!("{}", format!("{msg}").green());
}

/// Print a warning message in yellow to stderr.
pub fn warn(msg: impl Display) {
    eprintln!("{}", format!("{msg}").yellow());
}

/// Print an error message in red to stderr.
pub fn error(msg: impl Display) {
    eprintln!("{}", format!("{msg}").red());
}

/// Build a pretty table using the default UTF-8 preset.
pub fn make_table() -> Table {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic);
    table
}

/// Emit a JSON serialisable value as pretty JSON to stdout.
///
/// # Errors
///
/// Returns [`serde_json::Error`] if the value cannot be serialised.
pub fn print_json<T: serde::Serialize>(value: &T) -> Result<(), serde_json::Error> {
    let s = serde_json::to_string_pretty(value)?;
    println!("{s}");
    Ok(())
}

/// Render a boolean status label (coloured `OK` / `FAIL`).
pub fn status_label(ok: bool) -> String {
    if ok {
        "OK".green().to_string()
    } else {
        "FAIL".red().to_string()
    }
}
