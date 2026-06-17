//! Full-text search analyzer definitions (`DEFINE ANALYZER`).
//!
//! A SurrealDB full-text index references an *analyzer* that turns
//! stored text and query text into comparable tokens — the lexical side of
//! hybrid (sparse + dense) retrieval. An analyzer is a tokenizer chain (how the
//! text is split) followed by a filter chain (how each token is normalised).
//!
//! This module renders the `DEFINE ANALYZER` statement from a typed
//! [`AnalyzerDefinition`], so callers define the analyzer in code rather than
//! hand-authoring SurrealQL — exactly as [`TableDefinition`](super::table::TableDefinition)
//! does for tables. Pair it with a BM25 [`search_index`](super::table::search_index)
//! (see [`super::table::bm25_index`]) and the
//! [`fulltext_search`](crate::query::builder::Query::fulltext_search) query
//! builder for end-to-end lexical recall.
//!
//! ## Examples
//!
//! ```
//! use surql::schema::{analyzer, TokenFilter, Tokenizer};
//!
//! let a = analyzer("text_en")
//!     .with_tokenizer(Tokenizer::Class)
//!     .with_filters([TokenFilter::Lowercase, TokenFilter::Ascii, TokenFilter::snowball("english")]);
//! assert_eq!(
//!     a.to_surql(),
//!     "DEFINE ANALYZER text_en TOKENIZERS class FILTERS lowercase,ascii,snowball(english);"
//! );
//! ```

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::error::{Result, SurqlError};

/// Tokenizer that splits text into terms before the filter chain runs.
///
/// Renders as the lowercase SurrealQL keyword used inside the
/// `TOKENIZERS ...` clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tokenizer {
    /// Split on whitespace (`blank`).
    Blank,
    /// Split on case transitions (`camelCase` -> `camel`, `Case`).
    Camel,
    /// Split on Unicode character-class transitions — letters, digits, and
    /// punctuation become separate tokens (`class`). The general-purpose
    /// default for prose and identifiers.
    Class,
    /// Split on punctuation (`punct`).
    Punct,
}

impl Tokenizer {
    /// Render as the SurrealQL keyword (`blank` / `camel` / `class` / `punct`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Blank => "blank",
            Self::Camel => "camel",
            Self::Class => "class",
            Self::Punct => "punct",
        }
    }
}

impl std::fmt::Display for Tokenizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A token filter that normalises or expands each token after tokenization.
///
/// Filters run in declaration order; each renders as the SurrealQL keyword (or
/// parameterised call) used inside the `FILTERS ...` clause.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenFilter {
    /// Fold accented / Unicode characters to their nearest ASCII equivalent
    /// (`ascii`).
    Ascii,
    /// Lowercase every token (`lowercase`).
    Lowercase,
    /// Uppercase every token (`uppercase`).
    Uppercase,
    /// Emit edge n-grams (prefixes) of length `min..=max` for prefix / typeahead
    /// matching (`edgengram(min,max)`).
    EdgeNgram(u32, u32),
    /// Emit n-grams of length `min..=max` (`ngram(min,max)`).
    Ngram(u32, u32),
    /// Reduce each token to its Snowball stem for the given language, e.g.
    /// `snowball(english)` — improves recall by matching word variants.
    Snowball(String),
}

impl TokenFilter {
    /// Construct a [`TokenFilter::Snowball`] for `language` (e.g. `"english"`).
    pub fn snowball(language: impl Into<String>) -> Self {
        Self::Snowball(language.into())
    }

    /// Construct a [`TokenFilter::EdgeNgram`] spanning `min..=max`.
    pub fn edge_ngram(min: u32, max: u32) -> Self {
        Self::EdgeNgram(min, max)
    }

    /// Construct a [`TokenFilter::Ngram`] spanning `min..=max`.
    pub fn ngram(min: u32, max: u32) -> Self {
        Self::Ngram(min, max)
    }

    /// Render as the SurrealQL filter keyword / call.
    pub fn to_surql(&self) -> String {
        match self {
            Self::Ascii => "ascii".to_string(),
            Self::Lowercase => "lowercase".to_string(),
            Self::Uppercase => "uppercase".to_string(),
            Self::EdgeNgram(min, max) => format!("edgengram({min},{max})"),
            Self::Ngram(min, max) => format!("ngram({min},{max})"),
            Self::Snowball(language) => format!("snowball({language})"),
        }
    }
}

impl std::fmt::Display for TokenFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_surql())
    }
}

/// Immutable `DEFINE ANALYZER` schema definition: a named tokenizer + filter
/// chain referenced by a full-text [`search_index`](super::table::search_index).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnalyzerDefinition {
    /// Analyzer name (referenced by a full-text index's `ANALYZER <name>` clause).
    pub name: String,
    /// Tokenizers applied, in order, to split the text.
    #[serde(default)]
    pub tokenizers: Vec<Tokenizer>,
    /// Filters applied, in order, to normalise each token.
    #[serde(default)]
    pub filters: Vec<TokenFilter>,
}

impl AnalyzerDefinition {
    /// Construct a new, empty analyzer (no tokenizers or filters yet).
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            tokenizers: Vec::new(),
            filters: Vec::new(),
        }
    }

    /// Append one tokenizer.
    pub fn with_tokenizer(mut self, tokenizer: Tokenizer) -> Self {
        self.tokenizers.push(tokenizer);
        self
    }

    /// Append several tokenizers.
    pub fn with_tokenizers<I>(mut self, tokenizers: I) -> Self
    where
        I: IntoIterator<Item = Tokenizer>,
    {
        self.tokenizers.extend(tokenizers);
        self
    }

    /// Append one filter.
    pub fn with_filter(mut self, filter: TokenFilter) -> Self {
        self.filters.push(filter);
        self
    }

    /// Append several filters.
    pub fn with_filters<I>(mut self, filters: I) -> Self
    where
        I: IntoIterator<Item = TokenFilter>,
    {
        self.filters.extend(filters);
        self
    }

    /// Validate the analyzer definition.
    ///
    /// Returns [`SurqlError::Validation`] when the name is empty.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(SurqlError::Validation {
                reason: "Analyzer name cannot be empty".into(),
            });
        }
        Ok(())
    }

    /// Render the `DEFINE ANALYZER` statement.
    pub fn to_surql(&self) -> String {
        self.to_surql_with_options(false)
    }

    /// Render the `DEFINE ANALYZER` statement, optionally with `IF NOT EXISTS`
    /// so it can be re-applied idempotently (e.g. a persistent store applying
    /// its schema on every connect). Empty tokenizer / filter chains omit their
    /// clause entirely.
    pub fn to_surql_with_options(&self, if_not_exists: bool) -> String {
        let ine = if if_not_exists { "IF NOT EXISTS " } else { "" };
        let mut sql = format!("DEFINE ANALYZER {ine}{name}", name = self.name);
        if !self.tokenizers.is_empty() {
            let toks = self
                .tokenizers
                .iter()
                .map(|t| t.as_str())
                .collect::<Vec<_>>()
                .join(",");
            write!(sql, " TOKENIZERS {toks}").expect("writing to String cannot fail");
        }
        if !self.filters.is_empty() {
            let filters = self
                .filters
                .iter()
                .map(TokenFilter::to_surql)
                .collect::<Vec<_>>()
                .join(",");
            write!(sql, " FILTERS {filters}").expect("writing to String cannot fail");
        }
        sql.push(';');
        sql
    }
}

/// Functional constructor for an empty [`AnalyzerDefinition`].
pub fn analyzer(name: impl Into<String>) -> AnalyzerDefinition {
    AnalyzerDefinition::new(name)
}

/// A sensible general-purpose analyzer for BM25 lexical recall: the `class`
/// tokenizer with `lowercase` + `ascii` filters. Add
/// [`TokenFilter::snowball`] for language-specific stemming.
pub fn standard_analyzer(name: impl Into<String>) -> AnalyzerDefinition {
    AnalyzerDefinition::new(name)
        .with_tokenizer(Tokenizer::Class)
        .with_filters([TokenFilter::Lowercase, TokenFilter::Ascii])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenizer_strings() {
        assert_eq!(Tokenizer::Blank.as_str(), "blank");
        assert_eq!(Tokenizer::Camel.as_str(), "camel");
        assert_eq!(Tokenizer::Class.as_str(), "class");
        assert_eq!(Tokenizer::Punct.as_str(), "punct");
    }

    #[test]
    fn tokenizer_display() {
        assert_eq!(format!("{}", Tokenizer::Class), "class");
    }

    #[test]
    fn tokenizer_serializes_lowercase() {
        assert_eq!(
            serde_json::to_string(&Tokenizer::Class).unwrap(),
            "\"class\""
        );
    }

    #[test]
    fn token_filter_renders() {
        assert_eq!(TokenFilter::Ascii.to_surql(), "ascii");
        assert_eq!(TokenFilter::Lowercase.to_surql(), "lowercase");
        assert_eq!(TokenFilter::Uppercase.to_surql(), "uppercase");
        assert_eq!(TokenFilter::edge_ngram(2, 10).to_surql(), "edgengram(2,10)");
        assert_eq!(TokenFilter::ngram(1, 3).to_surql(), "ngram(1,3)");
        assert_eq!(
            TokenFilter::snowball("english").to_surql(),
            "snowball(english)"
        );
    }

    #[test]
    fn token_filter_display_matches_surql() {
        assert_eq!(
            format!("{}", TokenFilter::snowball("german")),
            "snowball(german)"
        );
    }

    #[test]
    fn analyzer_minimal_renders_name_only() {
        assert_eq!(analyzer("plain").to_surql(), "DEFINE ANALYZER plain;");
    }

    #[test]
    fn analyzer_renders_tokenizers_and_filters() {
        let a = analyzer("text_en")
            .with_tokenizers([Tokenizer::Class, Tokenizer::Camel])
            .with_filters([TokenFilter::Lowercase, TokenFilter::Ascii]);
        assert_eq!(
            a.to_surql(),
            "DEFINE ANALYZER text_en TOKENIZERS class,camel FILTERS lowercase,ascii;"
        );
    }

    #[test]
    fn analyzer_if_not_exists() {
        let a = standard_analyzer("std");
        assert_eq!(
            a.to_surql_with_options(true),
            "DEFINE ANALYZER IF NOT EXISTS std TOKENIZERS class FILTERS lowercase,ascii;"
        );
    }

    #[test]
    fn standard_analyzer_is_class_lowercase_ascii() {
        let a = standard_analyzer("std");
        assert_eq!(a.tokenizers, vec![Tokenizer::Class]);
        assert_eq!(a.filters, vec![TokenFilter::Lowercase, TokenFilter::Ascii]);
    }

    #[test]
    fn analyzer_with_snowball() {
        let a = standard_analyzer("text_en").with_filter(TokenFilter::snowball("english"));
        assert_eq!(
            a.to_surql(),
            "DEFINE ANALYZER text_en TOKENIZERS class FILTERS lowercase,ascii,snowball(english);"
        );
    }

    #[test]
    fn analyzer_validate_rejects_empty_name() {
        let mut a = analyzer("x");
        a.name = String::new();
        assert!(a.validate().is_err());
    }

    #[test]
    fn analyzer_round_trips_through_serde() {
        let a = standard_analyzer("text_en").with_filter(TokenFilter::snowball("english"));
        let json = serde_json::to_string(&a).unwrap();
        let back: AnalyzerDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(a, back);
    }
}
