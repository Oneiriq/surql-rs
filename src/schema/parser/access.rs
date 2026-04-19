//! `DEFINE ACCESS` parser.
//!
//! Extracts [`AccessDefinition`] values (JWT + RECORD variants) from
//! SurrealDB `INFO FOR DB` responses. Split out of the monolithic
//! `parser.rs` so each submodule stays under the 1000-LOC budget; see
//! parent [`super`] for the public entry points.

use std::sync::OnceLock;

use regex::Regex;

use super::regex_case_insensitive;
use crate::schema::access::{AccessDefinition, AccessType, JwtConfig, RecordAccessConfig};

// --- Regex accessors ---------------------------------------------------------

fn algorithm_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"ALGORITHM\s+(\w+)"))
}

fn key_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"(?s)KEY\s+'([^']*)'"))
}

fn url_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"(?s)URL\s+'([^']*)'"))
}

fn issuer_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"(?s)WITH\s+ISSUER\s+'([^']*)'"))
}

fn signup_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        regex_case_insensitive(r"(?s)SIGNUP\s+\((.+?)\)(?:\s+SIGNIN|\s+DURATION|\s*;|\s*$)")
    })
}

fn signin_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        regex_case_insensitive(r"(?s)SIGNIN\s+\((.+?)\)(?:\s+SIGNUP|\s+DURATION|\s*;|\s*$)")
    })
}

fn session_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"FOR\s+SESSION\s+(\w+)"))
}

fn token_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"FOR\s+TOKEN\s+(\w+)"))
}

fn access_type_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| regex_case_insensitive(r"TYPE\s+(JWT|RECORD)"))
}

// --- Public parser -----------------------------------------------------------

/// Parse one `DEFINE ACCESS` statement.
///
/// Returns `None` when the access type cannot be determined.
pub fn parse_access(name: &str, definition: &str) -> Option<AccessDefinition> {
    if definition.is_empty() {
        return None;
    }
    let access_type = extract_access_type(definition)?;

    let mut acc = AccessDefinition {
        name: name.to_string(),
        access_type,
        jwt: None,
        record: None,
        duration_session: None,
        duration_token: None,
    };

    match access_type {
        AccessType::Jwt => {
            let algorithm = extract_algorithm(definition).unwrap_or_else(|| "HS256".into());
            acc.jwt = Some(JwtConfig {
                algorithm,
                key: extract_single_quoted(key_regex(), definition),
                url: extract_single_quoted(url_regex(), definition),
                issuer: extract_single_quoted(issuer_regex(), definition),
            });
        }
        AccessType::Record => {
            let signup = signup_regex()
                .captures(definition)
                .and_then(|c| c.get(1))
                .map(|m| m.as_str().trim().to_string());
            let signin = signin_regex()
                .captures(definition)
                .and_then(|c| c.get(1))
                .map(|m| m.as_str().trim().to_string());
            acc.record = Some(RecordAccessConfig { signup, signin });
        }
    }

    acc.duration_session = session_regex()
        .captures(definition)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string());
    acc.duration_token = token_regex()
        .captures(definition)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string());

    Some(acc)
}

// --- Access extractors -------------------------------------------------------

fn extract_access_type(definition: &str) -> Option<AccessType> {
    let caps = access_type_regex().captures(definition)?;
    match caps.get(1)?.as_str().to_uppercase().as_str() {
        "JWT" => Some(AccessType::Jwt),
        "RECORD" => Some(AccessType::Record),
        _ => None,
    }
}

fn extract_algorithm(definition: &str) -> Option<String> {
    algorithm_regex()
        .captures(definition)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string())
}

fn extract_single_quoted(re: &Regex, definition: &str) -> Option<String> {
    re.captures(definition)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string())
}
