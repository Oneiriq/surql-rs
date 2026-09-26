//! Shared output helpers for the `surql` CLI.
//!
//! All terminal-facing formatting helpers live here so the individual
//! subcommand modules can focus on their business logic. Colours are
//! produced through [`colored`] and tables through [`comfy_table`];
//! tests disable colours globally to keep snapshot-friendly output.

use std::fmt::Display;
use std::io::{IsTerminal, Write};

use colored::Colorize;
use comfy_table::{presets::UTF8_FULL, ContentArrangement, Table};

use crate::error::{Result, SurqlError};

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
        .load_style(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic);
    table
}

/// Emit a JSON serialisable value as pretty JSON to stdout.
///
/// # Errors
///
/// Returns [`serde_json::Error`] if the value cannot be serialised.
pub fn print_json<T: serde::Serialize>(value: &T) -> std::result::Result<(), serde_json::Error> {
    let s = serde_json::to_string_pretty(value)?;
    println!("{s}");
    Ok(())
}

/// Ask the operator to confirm `action` before a destructive command runs.
///
/// Returns immediately when `yes` is set (the command's `--yes` flag).
/// Otherwise prints `<action>? [y/N]` to stderr and reads one line from
/// stdin; only `y` or `yes` proceeds. When stdin is not a terminal there
/// is nobody to ask, so the command is refused and `--yes` is required.
///
/// # Errors
///
/// Returns [`SurqlError::Validation`] when the action is refused or not
/// confirmed, and [`SurqlError::Io`] when stdin cannot be read.
pub fn confirm(action: &str, yes: bool) -> Result<()> {
    if yes {
        return Ok(());
    }
    let stdin = std::io::stdin();
    if !stdin.is_terminal() {
        return Err(SurqlError::Validation {
            reason: format!("refusing to {action} without confirmation: pass --yes"),
        });
    }
    eprint!("{action}? [y/N] ");
    std::io::stderr().flush()?;
    let mut answer = String::new();
    stdin.read_line(&mut answer)?;
    if is_yes(&answer) {
        Ok(())
    } else {
        Err(SurqlError::Validation {
            reason: format!("aborted: did not {action}"),
        })
    }
}

fn is_yes(answer: &str) -> bool {
    matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes")
}

/// Render a boolean status label (coloured `OK` / `FAIL`).
pub fn status_label(ok: bool) -> String {
    if ok {
        "OK".green().to_string()
    } else {
        "FAIL".red().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn confirm_with_yes_skips_the_prompt() {
        assert!(confirm("remove everything", true).is_ok());
    }

    #[test]
    fn only_y_or_yes_confirms() {
        for answer in ["y\n", "YES\r\n", " yes "] {
            assert!(is_yes(answer), "{answer:?}");
        }
        for answer in ["", "\n", "n", "no", "yess", "sure"] {
            assert!(!is_yes(answer), "{answer:?}");
        }
    }
}
