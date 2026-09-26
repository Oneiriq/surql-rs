//! Migration files and the text tools that read them take arbitrary input.
//!
//! Loading a migration file, squash optimisation, the rollback analyser,
//! filename parsing and expression normalisation must return rather than
//! panic or hang. Squash optimisation accounts for every statement it is
//! given, and normalising an expression twice changes nothing.

#![no_main]

use std::path::PathBuf;

use libfuzzer_sys::fuzz_target;
use surql::migration::diff::normalize_expression;
use surql::migration::squash::optimize_statements;
use surql::migration::{
    analyze_statements, get_description_from_filename, get_version_from_filename,
    load_migration, validate_migration_name,
};

fn scratch_file() -> PathBuf {
    std::env::temp_dir().join(format!(
        "surql-fuzz-{}/20260101_000000_fuzz.surql",
        std::process::id()
    ))
}

fuzz_target!(|input: (String, String)| {
    let (text, filename) = input;

    let _ = validate_migration_name(&filename);
    let _ = get_version_from_filename(&filename);
    let _ = get_description_from_filename(&filename);

    let statements: Vec<String> = text.split('\n').map(str::to_owned).collect();
    let (kept, removed) = optimize_statements(&statements);
    assert!(
        kept.len() + removed <= statements.len(),
        "{} kept + {removed} removed from {} statements",
        kept.len(),
        statements.len()
    );
    let _ = analyze_statements("v1", &statements);

    let once = normalize_expression(&text);
    assert_eq!(normalize_expression(&once), once, "{text:?}");

    let path = scratch_file();
    if let Some(dir) = path.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    if std::fs::write(&path, text.as_bytes()).is_ok() {
        let _ = load_migration(&path);
    }
});
