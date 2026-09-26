//! Parse `DEFINE ANALYZER` echoes back into [`AnalyzerDefinition`]s.
//!
//! The engine reports analyzers in `INFO FOR DB` as definition
//! strings; round-tripping them into the typed form lets schema
//! diffing compare code against database for analyzers the way it
//! already does for tables.

use super::scan::{clause, clauses, define_head, split_top_level, Shape};
use crate::error::{Result, SurqlError};
use crate::schema::analyzer::{AnalyzerDefinition, TokenFilter, Tokenizer};

const ANALYZER_CLAUSES: &[(&str, Shape)] = &[
    ("FUNCTION", Shape::Expr),
    ("TOKENIZERS", Shape::Expr),
    ("FILTERS", Shape::Expr),
    ("COMMENT", Shape::Str),
];

fn invalid(name: &str, detail: &str) -> SurqlError {
    SurqlError::Validation {
        reason: format!("analyzer {name}: {detail}"),
    }
}

fn parse_tokenizer(name: &str, raw: &str) -> Result<Tokenizer> {
    match raw.to_ascii_lowercase().as_str() {
        "blank" => Ok(Tokenizer::Blank),
        "camel" => Ok(Tokenizer::Camel),
        "class" => Ok(Tokenizer::Class),
        "punct" => Ok(Tokenizer::Punct),
        other => Err(invalid(name, &format!("unknown tokenizer {other:?}"))),
    }
}

fn parse_filter(name: &str, raw: &str) -> Result<TokenFilter> {
    let raw = raw.to_ascii_lowercase();
    let raw = raw.as_str();
    if let Some(args) = raw
        .strip_prefix("snowball(")
        .and_then(|r| r.strip_suffix(')'))
    {
        return Ok(TokenFilter::snowball(args.trim()));
    }
    for (prefix, ngram) in [("edgengram(", true), ("ngram(", false)] {
        if let Some(args) = raw.strip_prefix(prefix).and_then(|r| r.strip_suffix(')')) {
            let mut parts = args.split(',').map(str::trim);
            let (min, max) = (parts.next(), parts.next());
            let parse = |v: Option<&str>| {
                v.and_then(|v| v.parse::<u32>().ok())
                    .ok_or_else(|| invalid(name, &format!("bad ngram bounds {args:?}")))
            };
            let (min, max) = (parse(min)?, parse(max)?);
            return Ok(if ngram {
                TokenFilter::edge_ngram(min, max)
            } else {
                TokenFilter::ngram(min, max)
            });
        }
    }
    match raw {
        "ascii" => Ok(TokenFilter::Ascii),
        "lowercase" => Ok(TokenFilter::Lowercase),
        "uppercase" => Ok(TokenFilter::Uppercase),
        other => Err(invalid(name, &format!("unknown filter {other:?}"))),
    }
}

/// Parse one `DEFINE ANALYZER` definition string.
///
/// Clauses are read after the `DEFINE ANALYZER <name>` head and outside
/// quotes, so an analyzer named `blog_filters` or a `COMMENT 'FILTERS x'`
/// cannot open a clause. The engine echoes keywords in upper case
/// (`TOKENIZERS BLANK,CLASS FILTERS LOWERCASE, SNOWBALL(ENGLISH)`); both
/// cases read the same.
pub fn parse_analyzer(name: &str, definition: &str) -> Result<AnalyzerDefinition> {
    let mut analyzer = AnalyzerDefinition::new(name);
    let body = define_head(definition, "ANALYZER", false).map_or(definition, |head| head.rest);
    let found = clauses(body, ANALYZER_CLAUSES);
    // Filters carry parenthesised arguments with commas inside, so the
    // split respects depth.
    let list = |keyword: &str| -> Vec<&str> {
        clause(&found, keyword)
            .map(|body| {
                split_top_level(body, ',')
                    .into_iter()
                    .map(str::trim)
                    .filter(|item| !item.is_empty())
                    .collect()
            })
            .unwrap_or_default()
    };

    let tokenizers = list("TOKENIZERS")
        .into_iter()
        .map(|t| parse_tokenizer(name, t))
        .collect::<Result<Vec<_>>>()?;
    let filters = list("FILTERS")
        .into_iter()
        .map(|f| parse_filter(name, f))
        .collect::<Result<Vec<_>>>()?;
    analyzer = analyzer.with_tokenizers(tokenizers).with_filters(filters);
    Ok(analyzer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_the_full_shape() {
        let rendered = "DEFINE ANALYZER copal_text TOKENIZERS class FILTERS \
                        lowercase,snowball(english);";
        let parsed = parse_analyzer("copal_text", rendered).unwrap();
        assert_eq!(parsed.tokenizers, vec![Tokenizer::Class]);
        assert_eq!(
            parsed.filters,
            vec![TokenFilter::Lowercase, TokenFilter::snowball("english")]
        );
    }

    #[test]
    fn parses_ngram_arguments() {
        let parsed = parse_analyzer(
            "t",
            "DEFINE ANALYZER t TOKENIZERS blank FILTERS edgengram(2,10);",
        )
        .unwrap();
        assert_eq!(parsed.filters, vec![TokenFilter::edge_ngram(2, 10)]);
    }

    #[test]
    fn a_multibyte_name_does_not_panic() {
        // `ŉ` upper-cases to the three-byte `ʼN`, which used to shift the
        // byte offsets used to slice the original string.
        let db = crate::schema::parser::parse_db_info(&serde_json::json!({
            "az": { "a": "DEFINE ANALYZER `ŉŉŉŉŉŉŉŉŉŉŉŉ` TOKENIZERS blank" }
        }))
        .unwrap();
        assert_eq!(db.analyzers["a"].tokenizers, vec![Tokenizer::Blank]);
    }

    #[test]
    fn keywords_inside_the_name_or_a_comment_are_not_clauses() {
        // Exact 3.0.5 echo.
        let parsed = parse_analyzer(
            "blog_filters",
            "DEFINE ANALYZER blog_filters TOKENIZERS BLANK,CLASS FILTERS LOWERCASE, \
             SNOWBALL(ENGLISH), EDGENGRAM(2,10) COMMENT 'FILTERS x'",
        )
        .unwrap();
        assert_eq!(parsed.tokenizers, vec![Tokenizer::Blank, Tokenizer::Class]);
        assert_eq!(
            parsed.filters,
            vec![
                TokenFilter::Lowercase,
                TokenFilter::snowball("english"),
                TokenFilter::edge_ngram(2, 10),
            ]
        );
    }

    #[test]
    fn unknown_pieces_refuse() {
        assert!(parse_analyzer("t", "DEFINE ANALYZER t TOKENIZERS mystery;").is_err());
        assert!(parse_analyzer("t", "DEFINE ANALYZER t FILTERS mystery;").is_err());
    }
}
