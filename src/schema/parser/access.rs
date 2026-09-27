//! `DEFINE ACCESS` parser.
//!
//! Extracts [`AccessDefinition`] values (JWT + RECORD variants) from
//! SurrealDB `INFO FOR DB` responses. Split out of the monolithic
//! `parser.rs` so each submodule stays under the 1000-LOC budget; see
//! parent [`super`] for the public entry points.
//!
//! The engine echoes
//!
//! ```text
//! DEFINE ACCESS api ON DATABASE TYPE JWT ALGORITHM RS256 KEY 'pub'
//!   WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE
//! DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNUP (CREATE …) SIGNIN (SELECT …)
//!   WITH JWT ALGORITHM HS256 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]'
//!   DURATION FOR TOKEN 15m, FOR SESSION 1d
//! ```
//!
//! Symmetric keys and every issuer key come back as `'[REDACTED]'`, so a
//! parsed secret never equals the one the code declared.

use super::scan::{
    clauses, define_head, split_top_level, string_literal, strip_group, tokens, Shape,
};
use crate::schema::access::{AccessDefinition, AccessType, JwtConfig, RecordAccessConfig};

/// The clauses after `TYPE JWT` / `TYPE RECORD`, read flat: `WITH ISSUER`
/// and `WITH JWT` switch which key the following `ALGORITHM` / `KEY` set.
const ACCESS_CLAUSES: &[(&str, Shape)] = &[
    ("SIGNUP", Shape::Expr),
    ("SIGNIN", Shape::Expr),
    ("WITH", Shape::Expr),
    ("ALGORITHM", Shape::Expr),
    ("KEY", Shape::Expr),
    ("URL", Shape::Expr),
    ("AUDIENCE", Shape::Expr),
    ("AUTHENTICATE", Shape::Expr),
    ("CONTEXT", Shape::Expr),
    ("DURATION", Shape::Expr),
    ("COMMENT", Shape::Str),
];

/// The engine's token duration when none is declared.
const DEFAULT_TOKEN_DURATION: &str = "1h";

// --- Public parser -----------------------------------------------------------

/// Parse one `DEFINE ACCESS` statement.
///
/// Durations at the engine default (`FOR TOKEN 1h`, `FOR SESSION NONE`,
/// both always echoed) read back as `None`, which is what a definition that
/// never set them holds. For an HMAC algorithm the engine issues tokens with
/// the verification key, and echoes that as `WITH ISSUER KEY`; that implied
/// issuer reads back as `None` too.
///
/// Returns `None` when the access type cannot be determined.
pub fn parse_access(name: &str, definition: &str) -> Option<AccessDefinition> {
    if definition.is_empty() {
        return None;
    }
    let rest = define_head(definition, "ACCESS", false).map_or(definition, |head| head.rest);
    let toks = tokens(rest);
    let type_at = toks.iter().position(|t| t.is("TYPE"))?;
    let kind = toks.get(type_at + 1)?;
    let access_type = if kind.is("JWT") {
        AccessType::Jwt
    } else if kind.is("RECORD") {
        AccessType::Record
    } else {
        return None;
    };
    let after_type = rest.get(kind.end..).unwrap_or("");
    let found = clauses(after_type, ACCESS_CLAUSES);

    let mut jwt = JwtParts::default();
    let mut signup = None;
    let mut signin = None;
    let mut durations = (None, None);
    let mut authenticate = None;
    let mut context = None;
    let mut issuer_scope = false;
    for c in &found {
        match c.keyword {
            "WITH" => {
                issuer_scope = c.body.eq_ignore_ascii_case("ISSUER");
                jwt.declared |= c.body.eq_ignore_ascii_case("JWT");
            }
            "ALGORITHM" if issuer_scope => jwt.issuer_algorithm = Some(c.body.to_string()),
            "ALGORITHM" => jwt.algorithm = Some(c.body.to_string()),
            "KEY" if issuer_scope => jwt.issuer = Some(literal(c.body)),
            "KEY" => jwt.key = Some(literal(c.body)),
            "URL" => jwt.url = Some(literal(c.body)),
            other => {
                issuer_scope = false;
                match other {
                    "SIGNUP" => signup = Some(unwrap_parens(c.body)),
                    "SIGNIN" => signin = Some(unwrap_parens(c.body)),
                    "AUDIENCE" => {
                        jwt.audience = split_top_level(c.body, ',')
                            .into_iter()
                            .map(|a| literal(a.trim()))
                            .collect();
                    }
                    "AUTHENTICATE" => authenticate = Some(c.body.to_string()),
                    "CONTEXT" => context = Some(c.body.to_string()),
                    "DURATION" => durations = parse_durations(c.body),
                    _ => {}
                }
            }
        }
    }

    let mut acc = AccessDefinition {
        name: name.to_string(),
        access_type,
        jwt: None,
        record: None,
        duration_session: durations.0,
        duration_token: durations.1,
        authenticate,
        context,
    };
    match access_type {
        AccessType::Jwt => acc.jwt = Some(jwt.into_config()),
        AccessType::Record => {
            acc.record = Some(RecordAccessConfig {
                signup,
                signin,
                jwt: jwt.declared.then(|| jwt.into_config()),
            });
        }
    }
    Some(acc)
}

// --- Access extractors -------------------------------------------------------

/// The JWT verifier and issuer pieces seen so far.
#[derive(Default)]
struct JwtParts {
    declared: bool,
    algorithm: Option<String>,
    key: Option<String>,
    url: Option<String>,
    issuer_algorithm: Option<String>,
    issuer: Option<String>,
    audience: Vec<String>,
}

impl JwtParts {
    fn into_config(self) -> JwtConfig {
        let algorithm = self
            .algorithm
            .or(self.issuer_algorithm)
            .unwrap_or_else(|| "HS256".to_string());
        let symmetric = algorithm.to_ascii_uppercase().starts_with("HS");
        JwtConfig {
            issuer: self.issuer.filter(|_| !(symmetric && self.key.is_some())),
            algorithm,
            key: self.key,
            url: self.url,
            audience: self.audience,
        }
    }
}

/// A string literal's text, or the raw expression when it is not one.
fn literal(body: &str) -> String {
    string_literal(body).unwrap_or_else(|| body.to_string())
}

/// The expression inside `( … )`, or the body as written.
fn unwrap_parens(body: &str) -> String {
    strip_group(body, '(').unwrap_or(body).to_string()
}

/// Read `FOR TOKEN 15m, FOR SESSION 1d` into `(session, token)`, dropping
/// the values the engine reports for an unset duration.
fn parse_durations(body: &str) -> (Option<String>, Option<String>) {
    let mut session = None;
    let mut token = None;
    for part in split_top_level(body, ',') {
        let words = tokens(part);
        let [for_word, which, value, ..] = words.as_slice() else {
            continue;
        };
        if !for_word.is("FOR") {
            continue;
        }
        let value = value.text.trim_end_matches(';').to_string();
        if which.is("SESSION") {
            session = Some(value).filter(|v| !v.eq_ignore_ascii_case("NONE"));
        } else if which.is("TOKEN") {
            token = Some(value).filter(|v| v != DEFAULT_TOKEN_DURATION);
        }
    }
    (session, token)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migration::diff::accesses_equal;

    #[test]
    fn record_signin_survives_a_following_jwt_clause() {
        // Exact 3.0.5 echo.
        let acc = parse_access(
            "a4",
            "DEFINE ACCESS a4 ON DATABASE TYPE RECORD SIGNUP (CREATE user SET a = 1) SIGNIN \
             (SELECT * FROM user WHERE a = 1) WITH JWT ALGORITHM HS256 KEY '[REDACTED]' WITH \
             ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 15m, FOR SESSION 1d",
        )
        .unwrap();
        let rec = acc.record.unwrap();
        assert_eq!(rec.signup.as_deref(), Some("CREATE user SET a = 1"));
        assert_eq!(
            rec.signin.as_deref(),
            Some("SELECT * FROM user WHERE a = 1")
        );
        let jwt = rec.jwt.unwrap();
        assert_eq!(jwt.algorithm, "HS256");
        assert_eq!(jwt.key.as_deref(), Some("[REDACTED]"));
        assert!(jwt.issuer.is_none());
        assert_eq!(acc.duration_token.as_deref(), Some("15m"));
        assert_eq!(acc.duration_session.as_deref(), Some("1d"));
    }

    #[test]
    fn default_durations_read_back_as_unset() {
        let acc = parse_access(
            "a6",
            "DEFINE ACCESS a6 ON DATABASE TYPE JWT URL 'https://x/jwks' DURATION FOR SESSION NONE",
        )
        .unwrap();
        assert!(acc.duration_session.is_none());
        assert!(acc.duration_token.is_none());
        assert_eq!(acc.jwt.unwrap().url.as_deref(), Some("https://x/jwks"));
        let acc = parse_access(
            "a1",
            "DEFINE ACCESS a1 ON DATABASE TYPE JWT ALGORITHM HS256 KEY '[REDACTED]' WITH ISSUER \
             KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE",
        )
        .unwrap();
        assert!(acc.duration_token.is_none());
    }

    #[test]
    fn an_asymmetric_issuer_key_is_kept() {
        let acc = parse_access(
            "a3",
            "DEFINE ACCESS a3 ON DATABASE TYPE JWT ALGORITHM RS256 KEY 'pubkey' WITH ISSUER KEY \
             '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE",
        )
        .unwrap();
        let jwt = acc.jwt.unwrap();
        assert_eq!(jwt.algorithm, "RS256");
        assert_eq!(jwt.key.as_deref(), Some("pubkey"));
        assert_eq!(jwt.issuer.as_deref(), Some("[REDACTED]"));
    }

    #[test]
    fn escaped_keys_are_unescaped() {
        let acc = parse_access(
            "k",
            r#"DEFINE ACCESS k ON DATABASE TYPE JWT ALGORITHM RS256 KEY "it's \\ key""#,
        )
        .unwrap();
        assert_eq!(acc.jwt.unwrap().key.as_deref(), Some(r"it's \ key"));
    }

    // The SurrealDB 3.3 echoes of what this crate renders, and how they
    // compare with the definitions that produced them.

    #[test]
    fn audience_does_not_run_into_the_key() {
        let echo = parse_access(
            "j2",
            "DEFINE ACCESS j2 ON DATABASE TYPE JWT ALGORITHM RS256 KEY 'pub' AUDIENCE 'api', \
             'web' DURATION FOR SESSION 1w",
        )
        .unwrap();
        let jwt = echo.jwt.clone().unwrap();
        assert_eq!(jwt.key.as_deref(), Some("pub"));
        assert_eq!(jwt.audience, vec!["api".to_string(), "web".to_string()]);
        assert_eq!(echo.duration_session.as_deref(), Some("1w"));

        let code = crate::schema::jwt_access(
            "j2",
            JwtConfig::new("RS256")
                .with_key("pub")
                .with_audience(["api", "web"]),
        )
        .with_session("168h");
        assert!(accesses_equal(&code, &echo));
        let other = crate::schema::jwt_access(
            "j2",
            JwtConfig::new("RS256")
                .with_key("pub")
                .with_audience(["api"]),
        )
        .with_session("168h");
        assert!(!accesses_equal(&other, &echo));
    }

    #[test]
    fn authenticate_and_context_read_back() {
        let echo = parse_access(
            "r2",
            "DEFINE ACCESS r2 ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM user) WITH JWT \
             ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' AUTHENTICATE \
             $auth.enabled = true CONTEXT { tenant: 'x' } DURATION FOR TOKEN 1h, FOR SESSION 4w2d",
        )
        .unwrap();
        assert_eq!(echo.authenticate.as_deref(), Some("$auth.enabled = true"));
        assert_eq!(echo.context.as_deref(), Some("{ tenant: 'x' }"));
        assert_eq!(echo.duration_session.as_deref(), Some("4w2d"));

        // Declared without a verifier: the engine's implied HS512 one and
        // the respelled durations are no difference.
        let code = crate::schema::record_access(
            "r2",
            RecordAccessConfig::new().with_signin("SELECT * FROM user"),
        )
        .with_authenticate("$auth.enabled = true")
        .with_context("{tenant:'x'}")
        .with_session("30d");
        assert_eq!(
            code.to_surql().unwrap(),
            "DEFINE ACCESS r2 ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM user) \
             AUTHENTICATE $auth.enabled = true CONTEXT {tenant:'x'} DURATION FOR SESSION 30d;"
        );
        assert!(accesses_equal(&code, &echo));
        assert!(!accesses_equal(
            &code.clone().with_context("{ tenant: 'y' }"),
            &echo
        ));
        let declared = crate::schema::record_access(
            "r2",
            RecordAccessConfig::new()
                .with_signin("SELECT * FROM user")
                .with_jwt(JwtConfig::new("RS256").with_key("pub")),
        )
        .with_authenticate("$auth.enabled = true")
        .with_context("{tenant:'x'}")
        .with_session("30d");
        assert!(!accesses_equal(&declared, &echo));
    }

    #[test]
    fn durations_compare_by_length() {
        let echo = parse_access(
            "j1",
            "DEFINE ACCESS j1 ON DATABASE TYPE JWT ALGORITHM HS256 KEY '[REDACTED]' WITH ISSUER \
             KEY '[REDACTED]' DURATION FOR TOKEN 1h30m, FOR SESSION NONE",
        )
        .unwrap();
        let code = crate::schema::jwt_access("j1", JwtConfig::hs256("secret")).with_token("90m");
        assert!(accesses_equal(&code, &echo));
        assert!(accesses_equal(&code.clone().with_session("NONE"), &echo));
        assert!(!accesses_equal(&code.clone().with_token("1h"), &echo));
        assert!(!accesses_equal(&code.with_session("1d"), &echo));
    }
}
