//! Field validation: every attribute a `DEFINE FIELD` renders.

use std::collections::BTreeMap;

use super::normalize::expr_eq;
use super::permissions::{compare_permissions, FIELD_PERMISSIONS};
use super::{presence, ValidationResult, ValidationSeverity};
use crate::migration::diff::type_eq;
use crate::schema::fields::{FieldDefinition, FieldType};

/// `true` for the `<parent>.*` (or `<parent>[*]`) child the engine defines on
/// its own beside a typed array `parent`.
fn is_engine_array_child(
    name: &str,
    code: &BTreeMap<&str, &FieldDefinition>,
    db: &BTreeMap<&str, &FieldDefinition>,
) -> bool {
    name.strip_suffix(".*")
        .or_else(|| name.strip_suffix("[*]"))
        .is_some_and(|parent| code.contains_key(parent) || db.contains_key(parent))
}

pub(super) fn compare_fields(
    table: &str,
    code_fields: &[FieldDefinition],
    db_fields: &[FieldDefinition],
) -> Vec<ValidationResult> {
    let code: BTreeMap<&str, &FieldDefinition> =
        code_fields.iter().map(|f| (f.name.as_str(), f)).collect();
    let db: BTreeMap<&str, &FieldDefinition> =
        db_fields.iter().map(|f| (f.name.as_str(), f)).collect();
    let mut results = Vec::new();

    for name in code.keys().filter(|n| !db.contains_key(*n)) {
        results.push(presence(
            ValidationSeverity::Error,
            table,
            Some((*name).to_string()),
            "Field defined in code but missing from database",
            true,
        ));
    }

    for name in db
        .keys()
        .filter(|n| !code.contains_key(*n) && !is_engine_array_child(n, &code, &db))
    {
        results.push(presence(
            ValidationSeverity::Warning,
            table,
            Some((*name).to_string()),
            "Field exists in database but not defined in code",
            false,
        ));
    }

    for (name, code_field) in &code {
        if let Some(db_field) = db.get(name) {
            results.extend(validate_field(table, code_field, db_field));
        }
    }

    results
}

/// The record target a field's `TYPE` clause actually carries: the renderer
/// only honours `target_table` on `record` and `array` fields.
fn effective_target(field: &FieldDefinition) -> Option<&str> {
    match field.field_type {
        FieldType::Record | FieldType::Array => field.target_table.as_deref(),
        _ => None,
    }
}

/// The `TYPE` clause a field renders, for reporting.
fn type_label(field: &FieldDefinition) -> String {
    let base = match (field.field_type, effective_target(field)) {
        _ if field.custom_type.is_some() => field.custom_type.clone().unwrap_or_default(),
        (FieldType::Record, Some(target)) => format!("record<{target}>"),
        (FieldType::Array, Some(target)) => format!("array<record<{target}>>"),
        (ty, _) => ty.as_str().to_string(),
    };
    if field.nullable {
        format!("option<{base}>")
    } else {
        base
    }
}

fn reference_label(field: &FieldDefinition) -> String {
    field.reference.map_or_else(
        || "none".to_string(),
        |action| format!("REFERENCE ON DELETE {}", action.as_str()),
    )
}

/// Validate a single field across code and database definitions.
///
/// Compares the type, nullability (`option<...>`), record target,
/// `REFERENCE` action, `COMPUTED` / `ASSERT` / `DEFAULT` / `VALUE`
/// expressions, `READONLY` / `FLEXIBLE` flags and permissions. Missing
/// permission actions count as the engine default for fields, `FULL`.
pub fn validate_field(
    table_name: &str,
    code_field: &FieldDefinition,
    db_field: &FieldDefinition,
) -> Vec<ValidationResult> {
    let mut results: Vec<ValidationResult> = field_checks(code_field, db_field)
        .into_iter()
        .filter(|check| check.differs)
        .map(|check| {
            ValidationResult::new(
                check.severity,
                table_name,
                Some(code_field.name.clone()),
                check.message,
                check.code,
                check.db,
            )
        })
        .collect();
    results.extend(compare_permissions(
        table_name,
        Some(code_field.name.clone()),
        "Field permissions mismatch",
        code_field.permissions.as_ref(),
        db_field.permissions.as_ref(),
        &FIELD_PERMISSIONS,
    ));
    results
}

/// One attribute comparison: whether the two sides differ, and how to
/// report it when they do.
struct Check {
    severity: ValidationSeverity,
    message: &'static str,
    differs: bool,
    code: Option<String>,
    db: Option<String>,
}

fn field_checks(code: &FieldDefinition, db: &FieldDefinition) -> Vec<Check> {
    let flag = |b: bool| Some(b.to_string());
    let mut checks = vec![
        Check {
            severity: ValidationSeverity::Error,
            message: "Field type mismatch",
            differs: code.field_type != db.field_type
                || !type_eq(code.custom_type.as_deref(), db.custom_type.as_deref()),
            code: Some(
                code.custom_type
                    .clone()
                    .unwrap_or_else(|| code.field_type.as_str().to_string()),
            ),
            db: Some(
                db.custom_type
                    .clone()
                    .unwrap_or_else(|| db.field_type.as_str().to_string()),
            ),
        },
        Check {
            severity: ValidationSeverity::Error,
            message: "Field nullability (option<...>) mismatch",
            differs: code.nullable != db.nullable,
            code: Some(type_label(code)),
            db: Some(type_label(db)),
        },
        Check {
            severity: ValidationSeverity::Error,
            message: "Field record target mismatch",
            differs: effective_target(code) != effective_target(db),
            code: Some(type_label(code)),
            db: Some(type_label(db)),
        },
        Check {
            severity: ValidationSeverity::Error,
            message: "Field REFERENCE mismatch",
            differs: code.reference != db.reference,
            code: Some(reference_label(code)),
            db: Some(reference_label(db)),
        },
    ];
    let expressions = [
        (
            "Field COMPUTED expression mismatch",
            &code.computed,
            &db.computed,
        ),
        ("Field assertion mismatch", &code.assertion, &db.assertion),
        ("Field default value mismatch", &code.default, &db.default),
        ("Field computed value mismatch", &code.value, &db.value),
    ];
    checks.extend(expressions.into_iter().map(|(message, c, d)| Check {
        severity: ValidationSeverity::Warning,
        message,
        differs: !expr_eq(c.as_deref(), d.as_deref()),
        code: c.clone(),
        db: d.clone(),
    }));
    checks.extend([
        Check {
            severity: ValidationSeverity::Info,
            message: "Field readonly flag mismatch",
            differs: code.readonly != db.readonly,
            code: flag(code.readonly),
            db: flag(db.readonly),
        },
        Check {
            severity: ValidationSeverity::Info,
            message: "Field flexible flag mismatch",
            differs: code.flexible != db.flexible,
            code: flag(code.flexible),
            db: flag(db.flexible),
        },
    ]);
    checks
}
