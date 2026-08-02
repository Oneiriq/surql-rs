//! Parse `DEFINE ANALYZER` echoes back into [`AnalyzerDefinition`]s.
//!
//! The engine reports analyzers in `INFO FOR DB` as definition
//! strings; round-tripping them into the typed form lets schema
//! diffing compare code against database for analyzers the way it
//! already does for tables.

use crate::error::{Result, SurqlError};
use crate::schema::analyzer::{AnalyzerDefinition, TokenFilter, Tokenizer};

fn invalid(name: &str, detail: &str) -> SurqlError {
    SurqlError::Validation {
        reason: format!("analyzer {name}: {detail}"),
    }
}

fn parse_tokenizer(name: &str, raw: &str) -> Result<Tokenizer> {
    match raw {
        "blank" => Ok(Tokenizer::Blank),
        "camel" => Ok(Tokenizer::Camel),
        "class" => Ok(Tokenizer::Class),
        "punct" => Ok(Tokenizer::Punct),
        other => Err(invalid(name, &format!("unknown tokenizer {other:?}"))),
    }
}

fn parse_filter(name: &str, raw: &str) -> Result<TokenFilter> {
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
pub fn parse_analyzer(name: &str, definition: &str) -> Result<AnalyzerDefinition> {
    let mut analyzer = AnalyzerDefinition::new(name);
    let upper = definition.to_uppercase();

    let clause = |keyword: &str| -> Option<String> {
        let start = upper.find(keyword)? + keyword.len();
        let rest = &definition[start..];
        let end = ["TOKENIZERS", "FILTERS", "COMMENT"]
            .iter()
            .filter_map(|k| upper[start..].find(k))
            .min()
            .unwrap_or(rest.len());
        Some(rest[..end].trim().trim_end_matches(';').trim().to_owned())
    };

    if let Some(list) = clause("TOKENIZERS") {
        let tokenizers = list
            .split(',')
            .map(|t| parse_tokenizer(name, t.trim()))
            .collect::<Result<Vec<_>>>()?;
        analyzer = analyzer.with_tokenizers(tokenizers);
    }
    if let Some(list) = clause("FILTERS") {
        // Filters carry parenthesised arguments with commas inside, so
        // the split respects depth.
        let mut filters = Vec::new();
        let mut depth = 0usize;
        let mut current = String::new();
        for ch in list.chars() {
            match ch {
                '(' => {
                    depth += 1;
                    current.push(ch);
                }
                ')' => {
                    depth = depth.saturating_sub(1);
                    current.push(ch);
                }
                ',' if depth == 0 => {
                    filters.push(current.trim().to_owned());
                    current.clear();
                }
                _ => current.push(ch),
            }
        }
        if !current.trim().is_empty() {
            filters.push(current.trim().to_owned());
        }
        let filters = filters
            .iter()
            .map(|f| parse_filter(name, f))
            .collect::<Result<Vec<_>>>()?;
        analyzer = analyzer.with_filters(filters);
    }
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
    fn unknown_pieces_refuse() {
        assert!(parse_analyzer("t", "DEFINE ANALYZER t TOKENIZERS mystery;").is_err());
        assert!(parse_analyzer("t", "DEFINE ANALYZER t FILTERS mystery;").is_err());
    }
}
