//! Environment guards: `require_approval` and `allow_destructive`.
//!
//! [`EnvironmentConfig::require_approval`] and
//! [`EnvironmentConfig::allow_destructive`] are checked before an
//! orchestrated deployment applies anything to an environment and again
//! before an auto-rollback reverts anything there.

use crate::migration::{Migration, MigrationDirection};
use crate::orchestration::coordinator::DeploymentPlan;
use crate::orchestration::environment::EnvironmentConfig;

/// `true` when `env` needs an approval the plan does not carry.
pub(crate) fn needs_approval(env: &EnvironmentConfig, plan: &DeploymentPlan) -> bool {
    env.require_approval && !plan.approved
}

/// Every destructive statement that running `migrations` in `direction`
/// would execute, as `"<version> (<what it does>)"`.
pub(crate) fn destructive_statements(
    migrations: &[&Migration],
    direction: MigrationDirection,
) -> Vec<String> {
    migrations
        .iter()
        .flat_map(|m| {
            let statements = match direction {
                MigrationDirection::Up => &m.up,
                MigrationDirection::Down => &m.down,
            };
            statements
                .iter()
                .filter_map(|s| destructive_reason(s))
                .map(|reason| format!("{} ({reason})", m.version))
        })
        .collect()
}

/// What makes `statement` destructive, or `None` when it is not.
///
/// Follows the rollback safety analysis in [`crate::migration::rollback`]:
/// removing a table or a field and changing a field's type lose data. So
/// do removing a namespace, a database, or a bucket, and `DELETE`. The
/// statement is classified by its leading keywords, after any comment
/// lines.
fn destructive_reason(statement: &str) -> Option<&'static str> {
    let code = statement
        .lines()
        .map(str::trim)
        .skip_while(|line| {
            line.is_empty()
                || line.starts_with("--")
                || line.starts_with("//")
                || line.starts_with('#')
        })
        .collect::<Vec<_>>()
        .join(" ");
    let words: Vec<String> = code
        .split(|c: char| c.is_whitespace() || matches!(c, ';' | ',' | '(' | ')'))
        .filter(|w| !w.is_empty())
        .map(str::to_ascii_uppercase)
        .collect();
    let verb = words.first().map_or("", String::as_str);
    let object = words.get(1).map_or("", String::as_str);
    match (verb, object) {
        ("REMOVE" | "DROP", "TABLE") => Some("removes a table"),
        ("REMOVE" | "DROP", "FIELD") => Some("removes a field"),
        ("REMOVE" | "DROP", "NAMESPACE" | "NS") => Some("removes a namespace"),
        ("REMOVE" | "DROP", "DATABASE" | "DB") => Some("removes a database"),
        ("REMOVE" | "DROP", "BUCKET") => Some("removes a bucket"),
        ("DELETE", _) => Some("deletes records"),
        ("ALTER", "FIELD") if words.iter().any(|w| w == "TYPE") => Some("changes a field type"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_data_losing_statements() {
        for statement in [
            "REMOVE TABLE user",
            "remove table if exists user;",
            "REMOVE FIELD email ON TABLE user",
            "REMOVE NAMESPACE other",
            "REMOVE DATABASE main",
            "REMOVE BUCKET avatars",
            "DELETE user WHERE active = false",
            "ALTER FIELD age ON TABLE user TYPE string",
            "-- drop the old table\nREMOVE TABLE legacy;",
        ] {
            assert!(destructive_reason(statement).is_some(), "{statement}");
        }
    }

    #[test]
    fn leaves_additive_statements_alone() {
        for statement in [
            "DEFINE TABLE user SCHEMAFULL",
            "DEFINE TABLE post PERMISSIONS FOR select, create, update, delete WHERE true",
            "DEFINE FIELD delete ON TABLE audit TYPE bool",
            "REMOVE INDEX idx_email ON TABLE user",
            "REMOVE EVENT audit ON TABLE user",
            "UPDATE user SET active = true",
            "CREATE user:1 SET note = 'REMOVE TABLE user'",
            "",
        ] {
            assert!(destructive_reason(statement).is_none(), "{statement}");
        }
    }

    #[test]
    fn destructive_statements_reads_the_requested_direction() {
        let migration = Migration {
            version: "v1".into(),
            description: String::new(),
            path: std::path::PathBuf::new(),
            up: vec!["DEFINE TABLE t".into()],
            down: vec!["REMOVE TABLE t".into()],
            checksum: None,
            depends_on: vec![],
        };
        assert!(destructive_statements(&[&migration], MigrationDirection::Up).is_empty());
        assert_eq!(
            destructive_statements(&[&migration], MigrationDirection::Down),
            vec!["v1 (removes a table)".to_string()]
        );
    }
}
