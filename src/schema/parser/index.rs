//! `DEFINE INDEX` parser.
//!
//! Extracts [`IndexDefinition`] values from SurrealDB `INFO FOR TABLE`
//! responses, including the vector-index variants (`UNIQUE`, `SEARCH`,
//! `MTREE`, `HNSW`). Split out of the monolithic `parser.rs` so each
//! submodule stays under the 1000-LOC budget; see parent [`super`] for
//! the public entry points.

use std::sync::OnceLock;

use regex::Regex;

use super::field::type_regex;
use super::regex_case_insensitive;
use crate::schema::table::{
    HnswDistanceType, IndexDefinition, IndexType, MTreeDistanceType, MTreeVectorType,
};

// --- Regex accessors ---------------------------------------------------------

fn columns_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        regex_case_insensitive(r"COLUMNS\s+([^;]+?)(?:UNIQUE|SEARCH|HNSW|MTREE|\s*;|\s*$)")
    })
}

fn fields_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        regex_case_insensitive(r"FIELDS\s+([^;]+?)(?:UNIQUE|SEARCH|HNSW|MTREE|\s*;|\s*$)")
    })
}

fn dimension_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"DIMENSION\s+(\d+)"))
}

fn distance_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"(?:DIST|DISTANCE)\s+(\w+)"))
}

fn efc_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"EFC\s+(\d+)"))
}

fn m_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"\bM\s+(\d+)"))
}

// --- Public parsers ----------------------------------------------------------

/// Parse every entry of an `ix` / `indexes` map.
pub fn parse_indexes(ix: &std::collections::BTreeMap<String, String>) -> Vec<IndexDefinition> {
    ix.iter()
        .filter_map(|(name, def)| parse_index(name, def))
        .collect()
}

/// Parse one `DEFINE INDEX` statement.
///
/// Returns `None` when the definition string is empty.
pub fn parse_index(name: &str, definition: &str) -> Option<IndexDefinition> {
    if definition.is_empty() {
        return None;
    }

    let mut columns = extract_index_columns(definition);
    if columns.is_empty() {
        columns = extract_index_fields(definition);
    }

    let index_type = extract_index_type(definition);

    let mut dimension = None;
    let mut distance = None;
    let mut vector_type = None;
    let mut hnsw_distance = None;
    let mut efc = None;
    let mut m = None;

    match index_type {
        IndexType::Mtree => {
            dimension = extract_dimension(definition);
            distance = extract_mtree_distance(definition);
            vector_type = extract_vector_type(definition);
        }
        IndexType::Hnsw => {
            dimension = extract_dimension(definition);
            vector_type = extract_vector_type(definition);
            hnsw_distance = extract_hnsw_distance(definition);
            efc = extract_hnsw_efc(definition);
            m = extract_hnsw_m(definition);
        }
        _ => {}
    }

    Some(IndexDefinition {
        name: name.to_string(),
        columns,
        index_type,
        dimension,
        distance,
        vector_type,
        hnsw_distance,
        efc,
        m,
    })
}

// --- Index extractors --------------------------------------------------------

fn split_cols(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn extract_index_columns(definition: &str) -> Vec<String> {
    columns_regex()
        .captures(definition)
        .and_then(|c| c.get(1))
        .map(|m| split_cols(m.as_str()))
        .unwrap_or_default()
}

fn extract_index_fields(definition: &str) -> Vec<String> {
    fields_regex()
        .captures(definition)
        .and_then(|c| c.get(1))
        .map(|m| split_cols(m.as_str()))
        .unwrap_or_default()
}

fn extract_index_type(definition: &str) -> IndexType {
    let upper = definition.to_uppercase();
    if upper.contains("UNIQUE") {
        IndexType::Unique
    } else if upper.contains("SEARCH") {
        IndexType::Search
    } else if upper.contains("HNSW") {
        IndexType::Hnsw
    } else if upper.contains("MTREE") {
        IndexType::Mtree
    } else {
        IndexType::Standard
    }
}

fn extract_dimension(definition: &str) -> Option<u32> {
    dimension_regex()
        .captures(definition)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse().ok())
}

fn extract_mtree_distance(definition: &str) -> Option<MTreeDistanceType> {
    let caps = distance_regex().captures(definition)?;
    let m = caps.get(1)?;
    match m.as_str().to_uppercase().as_str() {
        "COSINE" => Some(MTreeDistanceType::Cosine),
        "EUCLIDEAN" => Some(MTreeDistanceType::Euclidean),
        "MANHATTAN" => Some(MTreeDistanceType::Manhattan),
        "MINKOWSKI" => Some(MTreeDistanceType::Minkowski),
        _ => None,
    }
}

fn extract_hnsw_distance(definition: &str) -> Option<HnswDistanceType> {
    let caps = distance_regex().captures(definition)?;
    let m = caps.get(1)?;
    match m.as_str().to_uppercase().as_str() {
        "CHEBYSHEV" => Some(HnswDistanceType::Chebyshev),
        "COSINE" => Some(HnswDistanceType::Cosine),
        "EUCLIDEAN" => Some(HnswDistanceType::Euclidean),
        "HAMMING" => Some(HnswDistanceType::Hamming),
        "JACCARD" => Some(HnswDistanceType::Jaccard),
        "MANHATTAN" => Some(HnswDistanceType::Manhattan),
        "MINKOWSKI" => Some(HnswDistanceType::Minkowski),
        "PEARSON" => Some(HnswDistanceType::Pearson),
        _ => None,
    }
}

fn extract_vector_type(definition: &str) -> Option<MTreeVectorType> {
    // MTREE/HNSW `TYPE` clauses usually appear after `MTREE` / `HNSW`. Scan
    // every TYPE occurrence in case the first one is swallowed by the field
    // type clause (SurrealDB uses `TYPE` twice for these indexes).
    for caps in type_regex().captures_iter(definition) {
        let Some(m) = caps.get(1) else { continue };
        match m.as_str().to_uppercase().as_str() {
            "F64" => return Some(MTreeVectorType::F64),
            "F32" => return Some(MTreeVectorType::F32),
            "I64" => return Some(MTreeVectorType::I64),
            "I32" => return Some(MTreeVectorType::I32),
            "I16" => return Some(MTreeVectorType::I16),
            _ => {}
        }
    }
    None
}

fn extract_hnsw_efc(definition: &str) -> Option<u32> {
    efc_regex()
        .captures(definition)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse().ok())
}

fn extract_hnsw_m(definition: &str) -> Option<u32> {
    m_regex()
        .captures(definition)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse().ok())
}
