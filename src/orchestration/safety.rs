//! Environment guards: `require_approval` and `allow_destructive`.
//!
//! [`EnvironmentConfig::require_approval`] and
//! [`EnvironmentConfig::allow_destructive`] are checked before an
//! orchestrated deployment applies anything to an environment and again
//! before an auto-rollback reverts anything there.

use crate::migration::{analyze_statements, Migration, MigrationDirection, RollbackSafety};
use crate::orchestration::coordinator::DeploymentPlan;
use crate::orchestration::environment::EnvironmentConfig;

/// `true` when `env` needs an approval the plan does not carry.
pub(crate) fn needs_approval(env: &EnvironmentConfig, plan: &DeploymentPlan) -> bool {
    env.require_approval && !plan.approved
}

/// Every destructive statement that running `migrations` in `direction`
/// would execute, as `"<version> (<what it does>)"`.
///
/// Destructive means what the rollback safety analysis
/// ([`analyze_statements`]) rates above safe: removing a table, a field, a
/// namespace, a database or a bucket, changing a field's type, and
/// `DELETE`, each read from its leading keywords after any comments.
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
            analyze_statements(&m.version, statements)
        })
        .filter(|issue| issue.safety != RollbackSafety::Safe)
        .map(|issue| format!("{} ({})", issue.migration, issue.description))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn migration(up: &[&str], down: &[&str]) -> Migration {
        Migration {
            version: "v1".into(),
            description: String::new(),
            path: std::path::PathBuf::new(),
            up: up.iter().map(|s| (*s).to_owned()).collect(),
            down: down.iter().map(|s| (*s).to_owned()).collect(),
            checksum: None,
            depends_on: vec![],
            squashed_from: vec![],
        }
    }

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
            let m = migration(&[statement], &[]);
            assert_eq!(
                destructive_statements(&[&m], MigrationDirection::Up).len(),
                1,
                "{statement}"
            );
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
            let m = migration(&[statement], &[]);
            assert!(
                destructive_statements(&[&m], MigrationDirection::Up).is_empty(),
                "{statement}"
            );
        }
    }

    #[test]
    fn destructive_statements_reads_the_requested_direction() {
        let m = migration(&["DEFINE TABLE t"], &["REMOVE TABLE t"]);
        assert_eq!(
            destructive_statements(&[&m], MigrationDirection::Up),
            [] as [std::string::String; 0]
        );
        let down = destructive_statements(&[&m], MigrationDirection::Down);
        assert_eq!(down.len(), 1);
        assert!(down[0].starts_with("v1 ("), "{down:?}");
    }
}
