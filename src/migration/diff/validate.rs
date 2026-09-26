//! Safety checks for expressions the diff splices into SurrealQL: an event's
//! `WHEN` / `THEN`, and the default value a field backfill writes.

use crate::error::{Result, SurqlError};

/// Regex characters treated as safe in a default-value expression.
///
/// Preserved verbatim from the Python implementation to keep the validation
/// behaviour identical across runtimes.
const SAFE_DEFAULT_PATTERN: &str = concat!(
    r"^(",
    r"[a-zA-Z_][a-zA-Z0-9_]*(?:::[a-zA-Z_][a-zA-Z0-9_]*)*\([^;]*\)",
    r"|-?\d+(?:\.\d+)?",
    r"|true|false",
    r"|NONE|NULL",
    r"|'(?:[^'\\]|\\.)*'",
    r"|\$[a-zA-Z_][a-zA-Z0-9_]*",
    r")$",
);

/// The compiled [`SAFE_DEFAULT_PATTERN`]. `None` would mean the constant
/// failed to compile, which a unit test rules out; every default then reads
/// as unsafe rather than the process panicking.
pub(super) fn safe_default_regex() -> Option<&'static regex::Regex> {
    static RE: std::sync::OnceLock<Option<regex::Regex>> = std::sync::OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(SAFE_DEFAULT_PATTERN).ok())
        .as_ref()
}

/// Validate that an event expression has no injection patterns.
///
/// Mirrors `_validate_event_expression` in Python: rejects statement
/// separators (`;`) and SQL comments (`--`).
///
/// # Errors
///
/// Returns [`SurqlError::Validation`] when the expression contains a
/// banned pattern.
pub fn validate_event_expression(expr: &str, label: &str) -> Result<()> {
    let stripped = expr.trim();
    if stripped.contains("; ") || stripped.contains(";--") || stripped.ends_with(';') {
        return Err(SurqlError::Validation {
            reason: format!(
                "Unsafe event {label}: {expr:?}. Event {label}s must not contain statement separators."
            ),
        });
    }
    if stripped.contains("--") {
        return Err(SurqlError::Validation {
            reason: format!(
                "Unsafe event {label}: {expr:?}. Event {label}s must not contain SQL comments."
            ),
        });
    }
    Ok(())
}

/// Validate that a default-value expression is one of the allowlisted forms.
///
/// # Errors
///
/// Returns [`SurqlError::Validation`] when the expression does not match
/// the safe-default pattern.
pub fn validate_default_value(default: &str) -> Result<()> {
    let safe = safe_default_regex().is_some_and(|re| re.is_match(default.trim()));
    if !safe {
        return Err(SurqlError::Validation {
            reason: format!(
                "Unsafe default value expression: {default:?}. \
                 Defaults must be function calls, literals, or parameter references."
            ),
        });
    }
    Ok(())
}
