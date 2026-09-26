//! Permission validation: each side resolved to one rule per action, with
//! the engine default filled in for every action a clause leaves out.

use std::collections::BTreeMap;

use super::normalize::canonical_expr;
use super::{ValidationResult, ValidationSeverity};

/// Actions a table (or edge) `PERMISSIONS` clause governs.
const TABLE_ACTIONS: &[&str] = &["select", "create", "update", "delete"];
/// Actions a field `PERMISSIONS` clause governs.
const FIELD_ACTIONS: &[&str] = &["select", "create", "update"];

/// Which actions a `PERMISSIONS` clause covers and what an action it leaves
/// out defaults to.
pub(super) struct PermissionScope {
    actions: &'static [&'static str],
    default: &'static str,
}

/// Tables and edges deny what their clause leaves out.
pub(super) const TABLE_PERMISSIONS: PermissionScope = PermissionScope {
    actions: TABLE_ACTIONS,
    default: "NONE",
};

/// Fields allow what their clause leaves out.
pub(super) const FIELD_PERMISSIONS: PermissionScope = PermissionScope {
    actions: FIELD_ACTIONS,
    default: "FULL",
};

/// Canonical form of one permission rule: `FULL` / `NONE` postures (also
/// spelled `WHERE true` / `WHERE false`) collapse to the keyword, anything
/// else is a normalised `WHERE` expression.
fn canonical_rule(rule: &str) -> String {
    let rule = rule.trim();
    let body = match rule.get(..6) {
        Some(prefix) if prefix.eq_ignore_ascii_case("WHERE ") => rule.get(6..).unwrap_or(""),
        _ => rule,
    }
    .trim();
    if body.eq_ignore_ascii_case("FULL") || body.eq_ignore_ascii_case("true") {
        "FULL".to_string()
    } else if body.eq_ignore_ascii_case("NONE") || body.eq_ignore_ascii_case("false") {
        "NONE".to_string()
    } else {
        canonical_expr(body)
    }
}

/// Resolve a permission map into one canonical rule per action: grouped keys
/// (`"select, update"`) are split, and every action the map leaves out takes
/// the scope's default.
fn effective_permissions(
    map: Option<&BTreeMap<String, String>>,
    scope: &PermissionScope,
) -> BTreeMap<String, String> {
    let mut out: BTreeMap<String, String> = scope
        .actions
        .iter()
        .map(|action| ((*action).to_string(), scope.default.to_string()))
        .collect();
    for (key, rule) in map.into_iter().flatten() {
        let rule = canonical_rule(rule);
        for action in key.split(',').map(str::trim).filter(|a| !a.is_empty()) {
            out.insert(action.to_ascii_lowercase(), rule.clone());
        }
    }
    out
}

fn render_permissions(effective: &BTreeMap<String, String>) -> String {
    effective
        .iter()
        .map(|(action, rule)| match rule.as_str() {
            "FULL" | "NONE" => format!("FOR {action} {rule}"),
            _ => format!("FOR {action} WHERE {rule}"),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub(super) fn compare_permissions(
    table: &str,
    field: Option<String>,
    message: &str,
    code: Option<&BTreeMap<String, String>>,
    db: Option<&BTreeMap<String, String>>,
    scope: &PermissionScope,
) -> Option<ValidationResult> {
    let code = effective_permissions(code, scope);
    let db = effective_permissions(db, scope);
    (code != db).then(|| {
        ValidationResult::new(
            ValidationSeverity::Error,
            table,
            field,
            message,
            Some(render_permissions(&code)),
            Some(render_permissions(&db)),
        )
    })
}
