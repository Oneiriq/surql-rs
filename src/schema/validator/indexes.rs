//! Index validation: kind, ordered columns, and the per-kind options with
//! the engine defaults filled in.

use std::collections::BTreeMap;

use super::{presence, ValidationResult, ValidationSeverity};
use crate::schema::index::{
    DISKANN_DEFAULT_ALPHA, DISKANN_DEFAULT_DEGREE, DISKANN_DEFAULT_L_BUILD,
};
use crate::schema::table::{
    DiskAnnDistanceType, HnswDistanceType, IndexDefinition, IndexType, MTreeDistanceType,
    MTreeVectorType,
};

/// HNSW `EFC` the engine stores when a definition leaves it out (verified
/// against the v3.0.5 `INFO FOR TABLE` echo).
const HNSW_DEFAULT_EFC: u32 = 150;
/// HNSW `M` the engine stores when a definition leaves it out.
const HNSW_DEFAULT_M: u32 = 12;
/// Analyzer a full-text index renders when none is set.
const FULLTEXT_DEFAULT_ANALYZER: &str = "ascii";

pub(super) fn compare_indexes(
    table: &str,
    code_indexes: &[IndexDefinition],
    db_indexes: &[IndexDefinition],
) -> Vec<ValidationResult> {
    let code: BTreeMap<&str, &IndexDefinition> =
        code_indexes.iter().map(|i| (i.name.as_str(), i)).collect();
    let db: BTreeMap<&str, &IndexDefinition> =
        db_indexes.iter().map(|i| (i.name.as_str(), i)).collect();
    let mut results = Vec::new();

    for name in code.keys().filter(|n| !db.contains_key(*n)) {
        results.push(presence(
            ValidationSeverity::Error,
            table,
            Some(format!("index:{name}")),
            "Index defined in code but missing from database",
            true,
        ));
    }

    for name in db.keys().filter(|n| !code.contains_key(*n)) {
        results.push(presence(
            ValidationSeverity::Warning,
            table,
            Some(format!("index:{name}")),
            "Index exists in database but not defined in code",
            false,
        ));
    }

    for (name, code_index) in &code {
        if let Some(db_index) = db.get(name) {
            results.extend(validate_index(table, code_index, db_index));
        }
    }

    results
}

/// Collects index findings under one `index:<name>` pseudo-field.
struct IndexReport<'a> {
    table: &'a str,
    field: String,
    results: Vec<ValidationResult>,
}

impl IndexReport<'_> {
    fn check<T: PartialEq>(
        &mut self,
        severity: ValidationSeverity,
        message: &str,
        code: T,
        db: T,
        render: impl Fn(T) -> Option<String>,
    ) {
        if code != db {
            self.results.push(ValidationResult::new(
                severity,
                self.table,
                Some(self.field.clone()),
                message,
                render(code),
                render(db),
            ));
        }
    }
}

/// Validate a single index across code and database definitions.
///
/// Compares the kind, the columns in order (a composite index on `a, b` is
/// not the index on `b, a`), and the options of each kind: vector
/// dimension, distance, element type and tuning (with the engine defaults
/// filled in for options the code leaves unset), and the full-text
/// analyzer, `BM25` and `HIGHLIGHTS`.
pub fn validate_index(
    table_name: &str,
    code_index: &IndexDefinition,
    db_index: &IndexDefinition,
) -> Vec<ValidationResult> {
    let mut report = IndexReport {
        table: table_name,
        field: format!("index:{}", code_index.name),
        results: Vec::new(),
    };
    let text = |s: &str| Some(s.to_string());
    let num = |n: Option<u32>| n.map(|n| n.to_string());

    report.check(
        ValidationSeverity::Error,
        "Index type mismatch",
        code_index.index_type.as_str(),
        db_index.index_type.as_str(),
        text,
    );
    report.check(
        ValidationSeverity::Error,
        "Index columns mismatch",
        code_index.columns.join(","),
        db_index.columns.join(","),
        Some,
    );

    let kind = match code_index.index_type {
        IndexType::Mtree => "MTREE",
        IndexType::Hnsw => "HNSW",
        IndexType::Diskann => "DISKANN",
        IndexType::Search => {
            check_fulltext(&mut report, code_index, db_index);
            return report.results;
        }
        IndexType::Unique | IndexType::Standard => return report.results,
    };

    report.check(
        ValidationSeverity::Error,
        &format!("{kind} index dimension mismatch"),
        code_index.dimension,
        db_index.dimension,
        num,
    );
    report.check(
        ValidationSeverity::Warning,
        &format!("{kind} index vector type mismatch"),
        code_index.vector_type.map(MTreeVectorType::as_str),
        db_index.vector_type.map(MTreeVectorType::as_str),
        |v| v.map(str::to_string),
    );

    match code_index.index_type {
        IndexType::Mtree => report.check(
            ValidationSeverity::Warning,
            "MTREE index distance metric mismatch",
            code_index.distance.map(MTreeDistanceType::as_str),
            db_index.distance.map(MTreeDistanceType::as_str),
            |v| v.map(str::to_string),
        ),
        IndexType::Hnsw => check_hnsw(&mut report, code_index, db_index),
        IndexType::Diskann => check_diskann(&mut report, code_index, db_index),
        _ => {}
    }

    report.results
}

fn check_hnsw(report: &mut IndexReport<'_>, code: &IndexDefinition, db: &IndexDefinition) {
    let num = |n: u32| Some(n.to_string());
    report.check(
        ValidationSeverity::Warning,
        "HNSW index distance metric mismatch",
        code.hnsw_distance.map(HnswDistanceType::as_str),
        db.hnsw_distance.map(HnswDistanceType::as_str),
        |v| v.map(str::to_string),
    );
    report.check(
        ValidationSeverity::Warning,
        "HNSW index EFC mismatch",
        code.efc.unwrap_or(HNSW_DEFAULT_EFC),
        db.efc.unwrap_or(HNSW_DEFAULT_EFC),
        num,
    );
    report.check(
        ValidationSeverity::Warning,
        "HNSW index M mismatch",
        code.m.unwrap_or(HNSW_DEFAULT_M),
        db.m.unwrap_or(HNSW_DEFAULT_M),
        num,
    );
}

fn check_diskann(report: &mut IndexReport<'_>, code: &IndexDefinition, db: &IndexDefinition) {
    let num = |n: u32| Some(n.to_string());
    report.check(
        ValidationSeverity::Warning,
        "DISKANN index distance metric mismatch",
        code.diskann_distance.map(DiskAnnDistanceType::as_str),
        db.diskann_distance.map(DiskAnnDistanceType::as_str),
        |v| v.map(str::to_string),
    );
    report.check(
        ValidationSeverity::Warning,
        "DISKANN index DEGREE mismatch",
        code.degree.unwrap_or(DISKANN_DEFAULT_DEGREE),
        db.degree.unwrap_or(DISKANN_DEFAULT_DEGREE),
        num,
    );
    report.check(
        ValidationSeverity::Warning,
        "DISKANN index L_BUILD mismatch",
        code.l_build.unwrap_or(DISKANN_DEFAULT_L_BUILD),
        db.l_build.unwrap_or(DISKANN_DEFAULT_L_BUILD),
        num,
    );
    report.check(
        ValidationSeverity::Warning,
        "DISKANN index ALPHA mismatch",
        code.alpha.as_deref().unwrap_or(DISKANN_DEFAULT_ALPHA),
        db.alpha.as_deref().unwrap_or(DISKANN_DEFAULT_ALPHA),
        |a| Some(a.to_string()),
    );
    report.check(
        ValidationSeverity::Warning,
        "DISKANN index HASHED_VECTOR mismatch",
        code.hashed_vector,
        db.hashed_vector,
        |b| Some(b.to_string()),
    );
}

fn check_fulltext(report: &mut IndexReport<'_>, code: &IndexDefinition, db: &IndexDefinition) {
    report.check(
        ValidationSeverity::Warning,
        "FULLTEXT index analyzer mismatch",
        code.analyzer
            .as_deref()
            .unwrap_or(FULLTEXT_DEFAULT_ANALYZER),
        db.analyzer.as_deref().unwrap_or(FULLTEXT_DEFAULT_ANALYZER),
        |a| Some(a.to_string()),
    );
    // v3 scores every full-text index with BM25 and echoes the clause even
    // when the definition left it out, so only a BM25 the code asks for and
    // the database lacks is a difference.
    if code.bm25 && !db.bm25 {
        report.check(
            ValidationSeverity::Warning,
            "FULLTEXT index BM25 mismatch",
            code.bm25,
            db.bm25,
            |b| Some(b.to_string()),
        );
    }
    report.check(
        ValidationSeverity::Warning,
        "FULLTEXT index HIGHLIGHTS mismatch",
        code.highlights,
        db.highlights,
        |b| Some(b.to_string()),
    );
}
