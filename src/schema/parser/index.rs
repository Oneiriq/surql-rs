//! `DEFINE INDEX` parser.
//!
//! Extracts [`IndexDefinition`] values from SurrealDB `INFO FOR TABLE`
//! responses, including the vector-index variants (`UNIQUE`, `SEARCH`,
//! `COUNT`, `HNSW`, `DISKANN`, and the pre-3.0 `MTREE`). Split out of the
//! monolithic `parser.rs` so each submodule stays under the 1000-LOC
//! budget; see parent [`super`] for the public entry points.
//!
//! The engine echoes `DEFINE INDEX <name> ON <table> FIELDS <a>, <b> [<kind>
//! <params>] [COMMENT …] [CONCURRENTLY]`, and a count index as `DEFINE
//! INDEX <name> ON <table> COUNT [WHERE <condition>] [COMMENT …]`. The
//! statement is read word by word after its head: the column list is the
//! comma-separated run after `FIELDS` / `COLUMNS`, and the index kind is the
//! word that follows it, so neither a table named `custom_fields` nor a
//! column named `year` or `research` can be mistaken for a clause.

use super::scan::{define_head, find_keyword_from, split_top_level, tokens, unquote_ident, Token};
use crate::schema::table::{
    DiskAnnDistanceType, HnswDistanceType, IndexDefinition, IndexType, MTreeDistanceType,
    MTreeVectorType,
};

// --- Public parsers ----------------------------------------------------------

/// Parse every entry of an `ix` / `indexes` map.
pub fn parse_indexes(ix: &std::collections::BTreeMap<String, String>) -> Vec<IndexDefinition> {
    ix.iter()
        .filter_map(|(name, def)| parse_index(name, def))
        .collect()
}

/// Parse one `DEFINE INDEX` statement.
///
/// A full-text index always reads back with `bm25` set: the engine scores
/// every `FULLTEXT` index with BM25 and always echoes `BM25(k1,b)`, whether
/// or not the statement asked for it.
///
/// Returns `None` when the definition string is empty.
pub fn parse_index(name: &str, definition: &str) -> Option<IndexDefinition> {
    if definition.trim().is_empty() {
        return None;
    }
    let body = define_head(definition, "INDEX", true).map_or(definition, |head| head.rest);
    let toks = tokens(body);
    let mut index = IndexDefinition::new(name, Vec::<String>::new());
    let mut i = 0;
    while let Some(token) = toks.get(i) {
        i += 1;
        if token.is("FIELDS") || token.is("COLUMNS") {
            let (columns, next) = read_columns(&toks, i);
            index.columns = columns;
            i = next;
        } else if token.is("UNIQUE") {
            index.index_type = IndexType::Unique;
        } else if token.is("FULLTEXT") || token.is("SEARCH") {
            index.index_type = IndexType::Search;
            i = read_fulltext(&toks, i, &mut index);
        } else if token.is("HNSW") {
            index.index_type = IndexType::Hnsw;
            i = read_vector(&toks, i, &mut index);
        } else if token.is("MTREE") {
            #[allow(deprecated)]
            let mtree = IndexType::Mtree;
            index.index_type = mtree;
            i = read_vector(&toks, i, &mut index);
        } else if token.is("DISKANN") {
            index.index_type = IndexType::Diskann;
            i = read_vector(&toks, i, &mut index);
        } else if token.is("COMMENT") {
            i += 1;
        } else if token.is("COUNT") {
            index.index_type = IndexType::Count;
            if toks.get(i).is_some_and(|t| t.is("WHERE")) {
                let (condition, next) = read_condition(body, &toks, i + 1);
                index.condition = condition;
                i = next;
            }
        }
    }
    // `CONCURRENTLY` is a build directive the engine drops from the
    // definition it echoes, so a parsed index is never concurrent. Reading
    // it back as `true` would make a background-built index look modified on
    // every reconcile.
    index.concurrently = false;
    Some(index)
}

// --- Index extractors --------------------------------------------------------

/// Read a count index's `WHERE` condition, starting at token `at`: the text
/// up to the next top-level `COMMENT` or `CONCURRENTLY`, or the end. Returns
/// the condition and the index of the first token after it.
fn read_condition(body: &str, toks: &[Token<'_>], at: usize) -> (Option<String>, usize) {
    let Some(start) = toks.get(at).map(|t| t.start) else {
        return (None, at);
    };
    let end = ["COMMENT", "CONCURRENTLY"]
        .iter()
        .filter_map(|keyword| find_keyword_from(body, keyword, start))
        .min()
        .unwrap_or(body.len());
    let condition = body
        .get(start..end)
        .unwrap_or_default()
        .trim()
        .trim_end_matches(';')
        .trim_end();
    let next = toks
        .iter()
        .position(|t| t.start >= end)
        .unwrap_or(toks.len());
    ((!condition.is_empty()).then(|| condition.to_string()), next)
}

/// Read the comma-separated column list starting at token `at`. Returns the
/// unquoted columns and the index of the first token after the list.
fn read_columns(toks: &[Token<'_>], at: usize) -> (Vec<String>, usize) {
    let mut raw = String::new();
    let mut i = at;
    while let Some(token) = toks.get(i) {
        let continues = raw.is_empty() || raw.ends_with(',') || token.text.starts_with(',');
        if !continues {
            break;
        }
        raw.push(' ');
        raw.push_str(token.text.trim_end_matches(';'));
        i += 1;
    }
    let columns = split_top_level(&raw, ',')
        .into_iter()
        .map(|column| unquote_ident(column.trim()))
        .filter(|column| !column.is_empty())
        .collect();
    (columns, i)
}

/// Read `ANALYZER <name> BM25[(k1,b)] HIGHLIGHTS` after `FULLTEXT`.
fn read_fulltext(toks: &[Token<'_>], at: usize, index: &mut IndexDefinition) -> usize {
    let mut i = at;
    while let Some(token) = toks.get(i) {
        let word = token.text.trim_end_matches(';');
        if token.is("ANALYZER") {
            index.analyzer = toks
                .get(i + 1)
                .map(|t| unquote_ident(t.text.trim_end_matches(';')))
                .filter(|a| !a.eq_ignore_ascii_case("ascii"));
            i += 2;
        } else if word
            .get(..4)
            .is_some_and(|head| head.eq_ignore_ascii_case("BM25"))
        {
            index.bm25 = true;
            i += 1;
        } else if token.is("HIGHLIGHTS") {
            index.highlights = true;
            i += 1;
        } else if token.is("VS") {
            i += 1;
        } else {
            break;
        }
    }
    i
}

/// Read the parameters of an `MTREE`, `HNSW`, or `DISKANN` index.
fn read_vector(toks: &[Token<'_>], at: usize, index: &mut IndexDefinition) -> usize {
    let mut i = at;
    while let Some(token) = toks.get(i) {
        let value = toks.get(i + 1).map(|t| t.text.trim_end_matches(';'));
        let number = || value.and_then(|v| v.parse::<u32>().ok());
        if token.is("DIMENSION") {
            index.dimension = number();
        } else if token.is("DIST") || token.is("DISTANCE") {
            let metric = value.unwrap_or("").to_ascii_uppercase();
            match index.index_type {
                IndexType::Hnsw => index.hnsw_distance = hnsw_distance(&metric),
                IndexType::Diskann => index.diskann_distance = diskann_distance(&metric),
                _ => index.distance = mtree_distance(&metric),
            }
        } else if token.is("TYPE") {
            index.vector_type = value.and_then(vector_type);
        } else if token.is("EFC") {
            index.efc = number();
        } else if token.is("M") {
            index.m = number();
        } else if token.is("DEGREE") {
            index.degree = number();
        } else if token.is("L_BUILD") {
            index.l_build = number();
        } else if token.is("ALPHA") {
            // The engine echoes a float literal with a trailing `f` suffix
            // (`ALPHA 1.2f`) and an integer bare (`ALPHA 2`); the suffix is
            // dropped so the stored value matches what code declares.
            index.alpha = value.map(|v| v.trim_end_matches(['f', 'F']).to_string());
        } else if token.is("M0") || token.is("LM") || token.is("CAPACITY") {
            // Engine-derived tuning the definition does not model.
        } else if token.is("HASHED_VECTOR") {
            index.hashed_vector = index.index_type == IndexType::Diskann;
            i += 1;
            continue;
        } else if token.is("EXTEND_CANDIDATES") || token.is("KEEP_PRUNED_CONNECTIONS") {
            i += 1;
            continue;
        } else {
            break;
        }
        i += 2;
    }
    i
}

fn mtree_distance(metric: &str) -> Option<MTreeDistanceType> {
    match metric {
        "COSINE" => Some(MTreeDistanceType::Cosine),
        "EUCLIDEAN" => Some(MTreeDistanceType::Euclidean),
        "MANHATTAN" => Some(MTreeDistanceType::Manhattan),
        "MINKOWSKI" => Some(MTreeDistanceType::Minkowski),
        _ => None,
    }
}

fn hnsw_distance(metric: &str) -> Option<HnswDistanceType> {
    match metric {
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

fn diskann_distance(metric: &str) -> Option<DiskAnnDistanceType> {
    match metric {
        "COSINE" => Some(DiskAnnDistanceType::Cosine),
        "COSINE_NORMALIZED" => Some(DiskAnnDistanceType::CosineNormalized),
        "EUCLIDEAN" => Some(DiskAnnDistanceType::Euclidean),
        "INNER_PRODUCT" => Some(DiskAnnDistanceType::InnerProduct),
        _ => None,
    }
}

fn vector_type(word: &str) -> Option<MTreeVectorType> {
    match word.to_ascii_uppercase().as_str() {
        "F64" => Some(MTreeVectorType::F64),
        "F32" => Some(MTreeVectorType::F32),
        "F16" => Some(MTreeVectorType::F16),
        "I64" => Some(MTreeVectorType::I64),
        "I32" => Some(MTreeVectorType::I32),
        "I16" => Some(MTreeVectorType::I16),
        "I8" => Some(MTreeVectorType::I8),
        "U8" => Some(MTreeVectorType::U8),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fulltext_params_are_not_columns() {
        let idx = parse_index(
            "ft",
            "DEFINE INDEX ft ON TABLE post FIELDS title, content FULLTEXT ANALYZER ascii \
             BM25(1.2,0.75)",
        )
        .unwrap();
        assert_eq!(idx.columns, ["title", "content"]);
        assert_eq!(idx.index_type, IndexType::Search);
        assert!(idx.bm25);
        assert!(idx.analyzer.is_none());
    }

    #[test]
    fn the_kind_comes_from_the_word_after_the_columns() {
        let idx = parse_index("yr", "DEFINE INDEX yr ON research_paper FIELDS year").unwrap();
        assert_eq!(idx.index_type, IndexType::Standard);
        assert_eq!(idx.columns, ["year"]);
        let idx = parse_index("r", "DEFINE INDEX r ON t FIELDS research").unwrap();
        assert_eq!(idx.columns, ["research"]);
        let idx = parse_index("i", "DEFINE INDEX i ON custom_fields FIELDS a").unwrap();
        assert_eq!(idx.columns, ["a"]);
        let idx = parse_index("bm25_idx", "DEFINE INDEX bm25_idx ON t FIELDS unique_code").unwrap();
        assert_eq!(idx.index_type, IndexType::Standard);
        assert!(!idx.bm25);
    }

    #[test]
    fn keyword_named_columns_and_comments_are_read_in_place() {
        let idx = parse_index("on", "DEFINE INDEX on ON nm2 FIELDS type, comment").unwrap();
        assert_eq!(idx.columns, ["type", "comment"]);
        let idx = parse_index(
            "u",
            "DEFINE INDEX u ON t FIELDS research, `select` UNIQUE COMMENT 'unique FIELDS x'",
        )
        .unwrap();
        assert_eq!(idx.columns, ["research", "select"]);
        assert_eq!(idx.index_type, IndexType::Unique);
    }

    #[test]
    fn highlights_and_a_named_analyzer_read_back() {
        let idx = parse_index(
            "s",
            "DEFINE INDEX s ON doc FIELDS content FULLTEXT ANALYZER text_en BM25(1.2,0.75) \
             HIGHLIGHTS",
        )
        .unwrap();
        assert_eq!(idx.analyzer.as_deref(), Some("text_en"));
        assert!(idx.highlights);
    }
}
