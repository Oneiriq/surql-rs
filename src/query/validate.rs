//! Name and target checks shared by every query renderer.
//!
//! A table, edge, field, or alias is spliced into SurrealQL as bare text,
//! so each one is either validated here (when the caller can be handed an
//! error) or quoted here (when it cannot). Record-id targets go through
//! [`RecordID`], which renders any key safely.

use crate::error::{Result, SurqlError};
use crate::types::escape::{is_identifier, quote_ident};
use crate::types::record_id::RecordID;

/// The deepest graph traversal the helpers render. Every hop is spelled
/// out (`->edge->?` per level), so the bound keeps statements short.
pub(crate) const MAX_GRAPH_DEPTH: u32 = 32;

/// Require `name` to be a bare identifier (`[A-Za-z_][A-Za-z0-9_]*`).
pub(crate) fn validate_identifier(name: &str, context: &str) -> Result<()> {
    if name.is_empty() {
        let capitalized = capitalize(context);
        return Err(SurqlError::Validation {
            reason: format!("{capitalized} cannot be empty"),
        });
    }
    if !is_identifier(name) {
        return Err(SurqlError::Validation {
            reason: format!(
                "Invalid {context}: {name:?}. Must contain only alphanumeric \
                 characters and underscores, and cannot start with a digit"
            ),
        });
    }
    Ok(())
}

/// Require `path` to be a field path: identifiers joined by `.`
/// (`metadata.processing`).
pub(crate) fn validate_field_path(path: &str, context: &str) -> Result<()> {
    if path.is_empty() {
        let capitalized = capitalize(context);
        return Err(SurqlError::Validation {
            reason: format!("{capitalized} cannot be empty"),
        });
    }
    path.split('.')
        .try_for_each(|segment| validate_identifier(segment, context))
}

/// Validate a `SET` assignment target, which may be a dotted path into
/// a nested object (`metadata.processing`) — SurrealDB assigns nested
/// fields natively, and the schema layer already accepts dot notation
/// for field definitions. Each segment validates as an identifier.
pub(crate) fn validate_set_target(field: &str) -> Result<()> {
    validate_field_path(field, "field name")
}

/// Quote each segment of a field path as an identifier, for sinks that
/// cannot report an error. Safe for any input; identical to the input for
/// an ordinary path.
pub(crate) fn quote_field_path(path: &str) -> String {
    path.split('.')
        .map(quote_ident)
        .collect::<Vec<_>>()
        .join(".")
}

/// Render a statement target: a table name, or a `table:key` record id.
///
/// A table must be an identifier. A record id is parsed with
/// [`RecordID::parse`] and re-rendered, so its key is escaped however it was
/// written: `user:x; DELETE user` targets the record keyed
/// `"x; DELETE user"` instead of running a second statement. Keys that are
/// already quoted (`⟨…⟩` or backticks) keep their meaning, and bare integer
/// keys stay integers.
pub(crate) fn render_target(target: &str) -> Result<String> {
    if target.contains(':') {
        parse_record(target).map(|id| id.to_string())
    } else {
        validate_identifier(target, "table name")?;
        Ok(target.to_owned())
    }
}

/// Parse a `table:key` record id.
///
/// An unquoted key SurrealQL would read as an array, object, range, or id
/// generator (`[…]`, `{…}`, `a..b`, `ulid()`) is rejected rather than
/// silently turned into a string key naming a different record.
pub(crate) fn parse_record(target: &str) -> Result<RecordID> {
    let Some((_, key)) = target.split_once(':') else {
        return Err(SurqlError::Validation {
            reason: format!("Invalid record ID {target:?}: expected table:id"),
        });
    };
    let key = key.trim();
    let quoted = key.starts_with(['⟨', '`', '<']);
    if !quoted && (key.starts_with(['[', '{']) || key.contains('(') || key.contains("..")) {
        return Err(SurqlError::Validation {
            reason: format!(
                "Unsupported record ID {target:?}: array, object, range, and generated \
                 keys cannot be used as a target; quote the key (table:⟨key⟩) to mean a \
                 string"
            ),
        });
    }
    RecordID::parse(target)
}

/// Render the record `id` of `table`: a bare key is joined to the table
/// (digits stay an integer key, as in SurrealQL), a qualified id must name
/// the same table.
pub(crate) fn record_in_table(table: &str, id: &str) -> Result<String> {
    validate_identifier(table, "table name")?;
    let record = if id.contains(':') {
        parse_record(id)?
    } else {
        parse_record(&format!("{table}:{id}"))?
    };
    if record.table() != table {
        return Err(SurqlError::Validation {
            reason: format!("Record ID {id:?} is not in table {table:?}"),
        });
    }
    Ok(record.to_string())
}

/// Require a graph depth in `1..=MAX_GRAPH_DEPTH`.
pub(crate) fn validate_depth(depth: u32) -> Result<()> {
    if depth == 0 || depth > MAX_GRAPH_DEPTH {
        return Err(SurqlError::Validation {
            reason: format!("Graph depth must be in 1..={MAX_GRAPH_DEPTH}, got {depth}"),
        });
    }
    Ok(())
}

/// Require every value to be finite: `NaN` and the infinities have no
/// place in a vector or a distance threshold.
pub(crate) fn validate_finite(values: &[f64], context: &str) -> Result<()> {
    match values.iter().find(|v| !v.is_finite()) {
        Some(bad) => Err(SurqlError::Validation {
            reason: format!("{} must be finite, got {bad}", capitalize(context)),
        }),
        None => Ok(()),
    }
}

fn capitalize(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) => c.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_paths_are_dotted_identifiers() {
        assert!(validate_field_path("a", "field").is_ok());
        assert!(validate_field_path("a.b_2", "field").is_ok());
        for bad in ["", "a..b", ".a", "a.", "a b", "a;b", "1a"] {
            assert!(validate_field_path(bad, "field").is_err(), "{bad:?}");
        }
    }

    #[test]
    fn quote_field_path_is_transparent_for_plain_paths() {
        assert_eq!(quote_field_path("meta.score"), "meta.score");
        assert_eq!(quote_field_path("a b.c"), "`a b`.c");
        assert_eq!(quote_field_path("x`; DELETE"), r"`x\`; DELETE`");
    }

    #[test]
    fn targets_render_tables_and_escaped_record_ids() {
        assert_eq!(render_target("user").unwrap(), "user");
        assert_eq!(render_target("user:alice").unwrap(), "user:alice");
        assert_eq!(render_target("post:123").unwrap(), "post:123");
        assert_eq!(render_target("post:⟨123⟩").unwrap(), "post:⟨123⟩");
        assert_eq!(render_target("a:⟨x-y⟩").unwrap(), "a:⟨x-y⟩");
        assert_eq!(
            render_target("user:x; DELETE user").unwrap(),
            "user:⟨x; DELETE user⟩"
        );
        assert!(render_target("user; DELETE user").is_err());
        assert!(render_target("1user:a").is_err());
        assert!(render_target(":a").is_err());
    }

    #[test]
    fn rendered_targets_are_stable() {
        for target in ["user:a", "user:⟨a-b⟩", "user:x⟩y", "user:-5", "user:⟨(⟩"] {
            let once = render_target(target).unwrap();
            assert_eq!(render_target(&once).unwrap(), once, "{target}");
        }
    }

    #[test]
    fn non_scalar_keys_are_refused() {
        for target in ["user:[1, 2]", "user:{a: 1}", "user:1..5", "user:ulid()"] {
            assert!(render_target(target).is_err(), "{target}");
        }
        assert_eq!(render_target("user:⟨ulid()⟩").unwrap(), "user:⟨ulid()⟩");
    }

    #[test]
    fn records_stay_in_their_table() {
        assert_eq!(record_in_table("user", "alice").unwrap(), "user:alice");
        assert_eq!(record_in_table("user", "7").unwrap(), "user:7");
        assert_eq!(record_in_table("user", "user:bob").unwrap(), "user:bob");
        assert!(record_in_table("user", "admin:root").is_err());
    }

    #[test]
    fn depth_and_finiteness_bounds() {
        assert!(validate_depth(0).is_err());
        assert!(validate_depth(1).is_ok());
        assert!(validate_depth(MAX_GRAPH_DEPTH).is_ok());
        assert!(validate_depth(MAX_GRAPH_DEPTH + 1).is_err());
        assert!(validate_finite(&[0.1, -2.0], "vector").is_ok());
        assert!(validate_finite(&[f64::NAN], "vector").is_err());
        assert!(validate_finite(&[f64::INFINITY], "vector").is_err());
    }
}
