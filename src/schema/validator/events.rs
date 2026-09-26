//! Event validation: presence and the normalised `WHEN` / `THEN` bodies.

use std::collections::BTreeMap;

use super::normalize::canonical_event_body;
use super::{presence, ValidationResult, ValidationSeverity};
use crate::schema::table::EventDefinition;

pub(super) fn compare_events(
    table: &str,
    code_events: &[EventDefinition],
    db_events: &[EventDefinition],
) -> Vec<ValidationResult> {
    let code: BTreeMap<&str, &EventDefinition> =
        code_events.iter().map(|e| (e.name.as_str(), e)).collect();
    let db: BTreeMap<&str, &EventDefinition> =
        db_events.iter().map(|e| (e.name.as_str(), e)).collect();
    let mut results = Vec::new();

    for name in code.keys().filter(|n| !db.contains_key(*n)) {
        results.push(presence(
            ValidationSeverity::Error,
            table,
            Some(format!("event:{name}")),
            "Event defined in code but missing from database",
            true,
        ));
    }

    for name in db.keys().filter(|n| !code.contains_key(*n)) {
        results.push(presence(
            ValidationSeverity::Warning,
            table,
            Some(format!("event:{name}")),
            "Event exists in database but not defined in code",
            false,
        ));
    }

    for (name, code_event) in &code {
        let Some(db_event) = db.get(name) else {
            continue;
        };
        let pairs = [
            (
                "Event condition (WHEN) mismatch",
                &code_event.condition,
                &db_event.condition,
            ),
            (
                "Event action (THEN) mismatch",
                &code_event.action,
                &db_event.action,
            ),
        ];
        for (message, code_body, db_body) in pairs {
            if canonical_event_body(code_body) != canonical_event_body(db_body) {
                results.push(ValidationResult::new(
                    ValidationSeverity::Warning,
                    table,
                    Some(format!("event:{name}")),
                    message,
                    Some(code_body.clone()),
                    Some(db_body.clone()),
                ));
            }
        }
    }

    results
}
