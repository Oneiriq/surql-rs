//! Per-action `PERMISSIONS` clauses shared by tables, edges, and fields.
//!
//! A permission map goes from action (`select`, `create`, `update`,
//! `delete`, or a comma-joined group such as `"select, create"`) to a rule:
//! the literal `"NONE"` or `"FULL"` for the fixed postures, anything else a
//! `WHERE` expression passed to the engine verbatim. Actions left out keep
//! the engine default, which is `NONE` for a table and `FULL` for a field.
//!
//! The inverse lives in [`crate::schema::parser`], which reads the engine's
//! echo back into the same shape.

use std::collections::BTreeMap;

use crate::error::{Result, SurqlError};

/// Actions a table or edge `PERMISSIONS` clause may name.
pub(crate) const TABLE_ACTIONS: &[&str] = &["select", "create", "update", "delete"];

/// Actions a field `PERMISSIONS` clause may name: fields have no `delete`.
pub(crate) const FIELD_ACTIONS: &[&str] = &["select", "create", "update"];

/// Render one rule: `NONE`, `FULL`, or `WHERE <expr>`.
fn render_rule(rule: &str) -> String {
    let rule = rule.trim();
    if rule.eq_ignore_ascii_case("NONE") {
        "NONE".to_string()
    } else if rule.eq_ignore_ascii_case("FULL") {
        "FULL".to_string()
    } else {
        format!("WHERE {rule}")
    }
}

/// Render ` PERMISSIONS FOR <actions> <rule> ...`, or an empty string when
/// there is nothing to declare, so callers can append it unconditionally.
pub(crate) fn render_permissions_clause(permissions: Option<&BTreeMap<String, String>>) -> String {
    match permissions {
        Some(perms) if !perms.is_empty() => {
            let lines: Vec<String> = perms
                .iter()
                .map(|(actions, rule)| format!("FOR {} {}", actions.trim(), render_rule(rule)))
                .collect();
            format!(" PERMISSIONS {}", lines.join(" "))
        }
        _ => String::new(),
    }
}

/// Validate that every action in `permissions` is one of `allowed` and every
/// rule is non-empty. `owner` names the definition in the error.
pub(crate) fn validate_permissions(
    owner: &str,
    permissions: Option<&BTreeMap<String, String>>,
    allowed: &[&str],
) -> Result<()> {
    for (actions, rule) in permissions.into_iter().flatten() {
        for action in actions.split(',').map(str::trim) {
            if !allowed.iter().any(|a| a.eq_ignore_ascii_case(action)) {
                return Err(SurqlError::Validation {
                    reason: format!(
                        "{owner}: permission action {action:?} must be one of {}",
                        allowed.join(", ")
                    ),
                });
            }
        }
        if rule.trim().is_empty() {
            return Err(SurqlError::Validation {
                reason: format!("{owner}: permission for {actions:?} has an empty rule"),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn fixed_postures_render_as_keywords() {
        let perms = map(&[
            ("select", "$auth.admin"),
            ("create", "none"),
            ("update", "FULL"),
        ]);
        assert_eq!(
            render_permissions_clause(Some(&perms)),
            " PERMISSIONS FOR create NONE FOR select WHERE $auth.admin FOR update FULL"
        );
        assert_eq!(render_permissions_clause(None), "");
        assert_eq!(render_permissions_clause(Some(&BTreeMap::new())), "");
    }

    #[test]
    fn unknown_actions_and_empty_rules_are_refused() {
        let perms = map(&[("delete", "true")]);
        assert!(validate_permissions("field x", Some(&perms), FIELD_ACTIONS).is_err());
        assert!(validate_permissions("table x", Some(&perms), TABLE_ACTIONS).is_ok());
        let grouped = map(&[("select, create", "true")]);
        assert!(validate_permissions("table x", Some(&grouped), TABLE_ACTIONS).is_ok());
        let empty = map(&[("select", " ")]);
        assert!(validate_permissions("table x", Some(&empty), TABLE_ACTIONS).is_err());
    }
}
