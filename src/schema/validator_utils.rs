//! Validation result filtering, grouping, formatting, and summary helpers.
//!
//! Port of `surql/schema/validator_utils.py`. These helpers work on slices of
//! [`ValidationResult`] produced by [`crate::schema::validator::validate_schema`]
//! and provide convenience operations such as severity filtering, per-table
//! grouping, human-readable report formatting, and summary statistics.
//!
//! ## Examples
//!
//! ```
//! use surql::schema::validator::{ValidationResult, ValidationSeverity};
//! use surql::schema::validator_utils::{filter_errors, has_errors};
//!
//! let results = vec![
//!     ValidationResult::new(
//!         ValidationSeverity::Error,
//!         "user",
//!         Some("email".into()),
//!         "missing",
//!         None,
//!         None,
//!     ),
//!     ValidationResult::new(
//!         ValidationSeverity::Warning,
//!         "user",
//!         Some("name".into()),
//!         "default",
//!         None,
//!         None,
//!     ),
//! ];
//! assert!(has_errors(&results));
//! assert_eq!(filter_errors(&results).len(), 1);
//! ```

use std::collections::BTreeMap;

use super::validator::{ValidationResult, ValidationSeverity};

/// Return the subset of results matching the requested severity.
pub fn filter_by_severity(
    results: &[ValidationResult],
    severity: ValidationSeverity,
) -> Vec<ValidationResult> {
    results
        .iter()
        .filter(|r| r.severity == severity)
        .cloned()
        .collect()
}

/// Return only ERROR-severity results.
pub fn filter_errors(results: &[ValidationResult]) -> Vec<ValidationResult> {
    filter_by_severity(results, ValidationSeverity::Error)
}

/// Return only WARNING-severity results.
pub fn filter_warnings(results: &[ValidationResult]) -> Vec<ValidationResult> {
    filter_by_severity(results, ValidationSeverity::Warning)
}

/// Return `true` if any result has ERROR severity.
pub fn has_errors(results: &[ValidationResult]) -> bool {
    results
        .iter()
        .any(|r| r.severity == ValidationSeverity::Error)
}

/// Group validation results by their `table` attribute.
///
/// The returned map is keyed by table name. A [`BTreeMap`] is used so that
/// iteration order is stable — this matches the Python port's sorted report
/// output.
pub fn group_by_table(results: &[ValidationResult]) -> BTreeMap<String, Vec<ValidationResult>> {
    let mut grouped: BTreeMap<String, Vec<ValidationResult>> = BTreeMap::new();
    for result in results {
        grouped
            .entry(result.table.clone())
            .or_default()
            .push(result.clone());
    }
    grouped
}

/// Summary statistics for a slice of validation results.
///
/// Mirrors the dict returned by the Python `get_validation_summary` helper.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationSummary {
    /// Total number of results.
    pub total: usize,
    /// Number of ERROR-severity results.
    pub errors: usize,
    /// Number of WARNING-severity results.
    pub warnings: usize,
    /// Number of INFO-severity results.
    pub info: usize,
    /// Number of distinct tables with at least one result.
    pub tables_affected: usize,
    /// Whether any errors are present.
    pub has_errors: bool,
}

/// Produce summary statistics for a slice of validation results.
pub fn get_validation_summary(results: &[ValidationResult]) -> ValidationSummary {
    let errors = filter_errors(results).len();
    let warnings = filter_warnings(results).len();
    let info = filter_by_severity(results, ValidationSeverity::Info).len();
    let tables_affected = group_by_table(results).len();
    ValidationSummary {
        total: results.len(),
        errors,
        warnings,
        info,
        tables_affected,
        has_errors: errors > 0,
    }
}

/// Format validation results as a human-readable report.
///
/// When `include_info` is `false` (the default in the Python port), INFO
/// severities are stripped before grouping. If no significant results remain,
/// the helper emits a short sentinel message rather than an empty header.
pub fn format_validation_report(results: &[ValidationResult], include_info: bool) -> String {
    let mut lines: Vec<String> = Vec::new();

    if results.is_empty() {
        return "No schema validation issues found.".to_string();
    }

    let filtered: Vec<ValidationResult> = if include_info {
        results.to_vec()
    } else {
        results
            .iter()
            .filter(|r| r.severity != ValidationSeverity::Info)
            .cloned()
            .collect()
    };

    if filtered.is_empty() {
        return "No significant schema validation issues found.".to_string();
    }

    let grouped = group_by_table(&filtered);
    let error_count = filter_errors(&filtered).len();
    let warning_count = filter_warnings(&filtered).len();

    lines.push(format!(
        "Schema Validation Report: {} errors, {} warnings",
        error_count, warning_count,
    ));
    lines.push("=".repeat(60));

    for (table_name, table_results) in &grouped {
        lines.push(String::new());
        lines.push(format!("[{}]", table_name));
        for result in table_results {
            let icon = severity_icon(result.severity);
            let field_str = result
                .field
                .as_deref()
                .map_or(String::new(), |f| format!(".{}", f));
            lines.push(format!("  {} {}{}", icon, result.message, field_str));
            if result.code_value.is_some() || result.db_value.is_some() {
                let code = result.code_value.as_deref().unwrap_or("None");
                let db = result.db_value.as_deref().unwrap_or("None");
                lines.push(format!("      code: {}, db: {}", code, db));
            }
        }
    }

    lines.join("\n")
}

fn severity_icon(severity: ValidationSeverity) -> &'static str {
    match severity {
        ValidationSeverity::Error => "[!]",
        ValidationSeverity::Warning => "[~]",
        ValidationSeverity::Info => "[i]",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(severity: ValidationSeverity, table: &str, field: Option<&str>) -> ValidationResult {
        ValidationResult::new(
            severity,
            table,
            field.map(str::to_string),
            "msg",
            None,
            None,
        )
    }

    // -- filter_by_severity ----------------------------------------------------

    #[test]
    fn filter_errors_only() {
        let results = vec![
            mk(ValidationSeverity::Error, "user", Some("email")),
            mk(ValidationSeverity::Warning, "user", Some("name")),
            mk(ValidationSeverity::Info, "user", Some("age")),
        ];
        let filtered = filter_by_severity(&results, ValidationSeverity::Error);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].severity, ValidationSeverity::Error);
    }

    #[test]
    fn filter_warnings_only() {
        let results = vec![
            mk(ValidationSeverity::Error, "user", Some("email")),
            mk(ValidationSeverity::Warning, "user", Some("name")),
            mk(ValidationSeverity::Warning, "post", Some("title")),
        ];
        let filtered = filter_by_severity(&results, ValidationSeverity::Warning);
        assert_eq!(filtered.len(), 2);
        assert!(filtered
            .iter()
            .all(|r| r.severity == ValidationSeverity::Warning));
    }

    #[test]
    fn filter_empty_results() {
        let results: Vec<ValidationResult> = Vec::new();
        assert!(filter_by_severity(&results, ValidationSeverity::Error).is_empty());
    }

    #[test]
    fn filter_no_matches() {
        let results = vec![mk(ValidationSeverity::Error, "user", Some("email"))];
        assert!(filter_by_severity(&results, ValidationSeverity::Info).is_empty());
    }

    // -- filter_errors / filter_warnings --------------------------------------

    #[test]
    fn filter_errors_helper() {
        let results = vec![
            mk(ValidationSeverity::Error, "user", Some("email")),
            mk(ValidationSeverity::Warning, "user", Some("name")),
            mk(ValidationSeverity::Error, "post", Some("title")),
        ];
        let errors = filter_errors(&results);
        assert_eq!(errors.len(), 2);
        assert!(errors
            .iter()
            .all(|r| r.severity == ValidationSeverity::Error));
    }

    #[test]
    fn filter_warnings_helper() {
        let results = vec![
            mk(ValidationSeverity::Error, "user", Some("email")),
            mk(ValidationSeverity::Warning, "user", Some("name")),
            mk(ValidationSeverity::Warning, "post", Some("title")),
        ];
        let warnings = filter_warnings(&results);
        assert_eq!(warnings.len(), 2);
        assert!(warnings
            .iter()
            .all(|r| r.severity == ValidationSeverity::Warning));
    }

    // -- group_by_table --------------------------------------------------------

    #[test]
    fn group_by_table_basic() {
        let results = vec![
            mk(ValidationSeverity::Error, "user", Some("email")),
            mk(ValidationSeverity::Warning, "user", Some("name")),
            mk(ValidationSeverity::Error, "post", Some("title")),
        ];
        let grouped = group_by_table(&results);
        assert_eq!(grouped.len(), 2);
        assert!(grouped.contains_key("user"));
        assert!(grouped.contains_key("post"));
        assert_eq!(grouped["user"].len(), 2);
        assert_eq!(grouped["post"].len(), 1);
    }

    #[test]
    fn group_by_table_empty() {
        let results: Vec<ValidationResult> = Vec::new();
        assert!(group_by_table(&results).is_empty());
    }

    #[test]
    fn group_by_table_single_table() {
        let results = vec![
            mk(ValidationSeverity::Error, "user", Some("email")),
            mk(ValidationSeverity::Error, "user", Some("name")),
        ];
        let grouped = group_by_table(&results);
        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped["user"].len(), 2);
    }

    // -- has_errors ------------------------------------------------------------

    #[test]
    fn has_errors_true() {
        let results = vec![
            mk(ValidationSeverity::Error, "user", Some("email")),
            mk(ValidationSeverity::Warning, "user", Some("name")),
        ];
        assert!(has_errors(&results));
    }

    #[test]
    fn has_errors_false_with_warnings() {
        let results = vec![
            mk(ValidationSeverity::Warning, "user", Some("name")),
            mk(ValidationSeverity::Info, "user", Some("age")),
        ];
        assert!(!has_errors(&results));
    }

    #[test]
    fn has_errors_empty() {
        let results: Vec<ValidationResult> = Vec::new();
        assert!(!has_errors(&results));
    }

    // -- format_validation_report ----------------------------------------------

    #[test]
    fn format_report_no_issues() {
        let report = format_validation_report(&[], false);
        assert!(report.contains("No schema validation issues found"));
    }

    #[test]
    fn format_report_with_errors() {
        let results = vec![ValidationResult::new(
            ValidationSeverity::Error,
            "user",
            Some("email".into()),
            "Field type mismatch",
            Some("string".into()),
            Some("int".into()),
        )];
        let report = format_validation_report(&results, false);
        assert!(report.contains("Schema Validation Report"));
        assert!(report.contains("errors"));
        assert!(report.contains("user"));
        assert!(report.contains("email"));
    }

    #[test]
    fn format_report_excludes_info_by_default() {
        let results = vec![mk(ValidationSeverity::Info, "user", Some("name"))];
        let report = format_validation_report(&results, false);
        assert!(report.contains("No significant schema validation issues found"));
    }

    #[test]
    fn format_report_includes_info_when_requested() {
        let results = vec![mk(ValidationSeverity::Info, "user", Some("name"))];
        let report = format_validation_report(&results, true);
        assert!(report.contains("user"));
    }

    #[test]
    fn format_report_mixed_includes_both() {
        let results = vec![
            mk(ValidationSeverity::Error, "user", Some("email")),
            mk(ValidationSeverity::Warning, "post", None),
        ];
        let report = format_validation_report(&results, false);
        assert!(report.contains("[user]"));
        assert!(report.contains("[post]"));
        assert!(report.contains("[!]"));
        assert!(report.contains("[~]"));
    }

    #[test]
    fn format_report_values_included() {
        let results = vec![ValidationResult::new(
            ValidationSeverity::Error,
            "user",
            Some("email".into()),
            "Field type mismatch",
            Some("string".into()),
            Some("int".into()),
        )];
        let report = format_validation_report(&results, false);
        assert!(report.contains("code: string"));
        assert!(report.contains("db: int"));
    }

    // -- get_validation_summary ------------------------------------------------

    #[test]
    fn summary_with_mixed_results() {
        let results = vec![
            mk(ValidationSeverity::Error, "user", Some("email")),
            mk(ValidationSeverity::Error, "post", Some("title")),
            mk(ValidationSeverity::Warning, "user", Some("name")),
            mk(ValidationSeverity::Info, "user", Some("age")),
        ];
        let summary = get_validation_summary(&results);
        assert_eq!(summary.total, 4);
        assert_eq!(summary.errors, 2);
        assert_eq!(summary.warnings, 1);
        assert_eq!(summary.info, 1);
        assert_eq!(summary.tables_affected, 2);
        assert!(summary.has_errors);
    }

    #[test]
    fn summary_empty_results() {
        let summary = get_validation_summary(&[]);
        assert_eq!(summary.total, 0);
        assert_eq!(summary.errors, 0);
        assert_eq!(summary.warnings, 0);
        assert_eq!(summary.info, 0);
        assert_eq!(summary.tables_affected, 0);
        assert!(!summary.has_errors);
    }

    #[test]
    fn summary_no_errors() {
        let results = vec![mk(ValidationSeverity::Warning, "user", Some("name"))];
        let summary = get_validation_summary(&results);
        assert!(!summary.has_errors);
    }
}
