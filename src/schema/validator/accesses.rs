//! Access validation: database-level `DEFINE ACCESS` definitions.

use std::collections::BTreeMap;

use super::{presence, ValidationResult, ValidationSeverity};
use crate::migration::diff::accesses_equal;
use crate::schema::access::AccessDefinition;

/// Validate code-defined access methods against those the database holds.
///
/// Each finding names the access as the `access:<name>` pseudo-field, with
/// an empty table. An access in code but not in the database is an error,
/// one only in the database a warning, and one whose definition differs an
/// error carrying both statements. Definitions compare through
/// [`accesses_equal`], so what the engine redacts (keys), fills in (the
/// default token duration, the verifier of a record access declared
/// without one) or respells (`24h` as `1d`) is not a difference; a changed
/// key cannot be seen at all.
///
/// ## Examples
///
/// ```
/// use surql::schema::validator::validate_accesses;
/// use surql::schema::{jwt_access, parse_access, JwtConfig};
///
/// let code = [jwt_access("api", JwtConfig::hs256("secret"))];
/// let db = [parse_access(
///     "api",
///     "DEFINE ACCESS api ON DATABASE TYPE JWT ALGORITHM HS256 KEY '[REDACTED]' \
///      WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE",
/// )
/// .unwrap()];
/// assert!(validate_accesses(&code, &db).is_empty());
/// assert_eq!(validate_accesses(&code, &[]).len(), 1);
/// ```
pub fn validate_accesses(
    code_accesses: &[AccessDefinition],
    db_accesses: &[AccessDefinition],
) -> Vec<ValidationResult> {
    let code: BTreeMap<&str, &AccessDefinition> =
        code_accesses.iter().map(|a| (a.name.as_str(), a)).collect();
    let db: BTreeMap<&str, &AccessDefinition> =
        db_accesses.iter().map(|a| (a.name.as_str(), a)).collect();
    let field = |name: &str| Some(format!("access:{name}"));
    let mut results = Vec::new();

    for name in code.keys().filter(|n| !db.contains_key(*n)) {
        results.push(presence(
            ValidationSeverity::Error,
            "",
            field(name),
            "Access defined in code but missing from database",
            true,
        ));
    }
    for name in db.keys().filter(|n| !code.contains_key(*n)) {
        results.push(presence(
            ValidationSeverity::Warning,
            "",
            field(name),
            "Access exists in database but not defined in code",
            false,
        ));
    }
    for (name, code_access) in &code {
        if let Some(db_access) = db.get(name) {
            if !accesses_equal(code_access, db_access) {
                results.push(ValidationResult::new(
                    ValidationSeverity::Error,
                    "",
                    field(name),
                    "Access definition mismatch",
                    code_access.to_surql().ok(),
                    Some(render_echo(db_access)),
                ));
            }
        }
    }
    results
}

/// The database side of a mismatch as a statement, falling back to its
/// debug form when it does not render (a redacted definition always does).
fn render_echo(access: &AccessDefinition) -> String {
    access.to_surql().unwrap_or_else(|_| format!("{access:?}"))
}
