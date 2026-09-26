//! Access control schema definitions.
//!
//! Port of `surql/schema/access.py`. Provides [`AccessDefinition`] and its
//! supporting enums/config structs for emitting `DEFINE ACCESS` statements.
//!
//! These credential config types ([`JwtConfig`], [`RecordAccessConfig`]) are
//! deliberately distinct from the connection-auth credentials defined in
//! [`crate::connection::auth`]. The connection credentials describe how a
//! *client* signs in to SurrealDB; the types here describe what SurrealDB
//! should accept *from* clients via `DEFINE ACCESS`.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::error::{Result, SurqlError};
use crate::types::escape::{quote_ident, quote_str};

/// The JWT algorithms `DEFINE ACCESS ... ALGORITHM` accepts.
const ALGORITHMS: &[&str] = &[
    "EDDSA", "ES256", "ES384", "ES512", "HS256", "HS384", "HS512", "PS256", "PS384", "PS512",
    "RS256", "RS384", "RS512",
];

/// Access type used in `DEFINE ACCESS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum AccessType {
    /// JWT-verified bearer tokens.
    Jwt,
    /// Record-based access (SIGNUP / SIGNIN expressions).
    Record,
}

impl AccessType {
    /// Render as SurrealQL keyword (`JWT` / `RECORD`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Jwt => "JWT",
            Self::Record => "RECORD",
        }
    }
}

impl std::fmt::Display for AccessType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Immutable JWT access configuration for `DEFINE ACCESS ... TYPE JWT`.
///
/// Mirrors the engine's grammar: tokens are verified either with a key
/// (`ALGORITHM <alg> KEY '<key>'`) or with keys fetched from a JWKS endpoint
/// (`URL '<url>'`), never both, and the engine can optionally issue tokens
/// of its own (`WITH ISSUER KEY '<key>'`).
///
/// ## Examples
///
/// ```
/// use surql::schema::{jwt_access, JwtConfig};
///
/// let verify_only = jwt_access("api", JwtConfig::new("RS256").with_key("<public key>"));
/// assert_eq!(
///     verify_only.to_surql().unwrap(),
///     "DEFINE ACCESS api ON DATABASE TYPE JWT ALGORITHM RS256 KEY '<public key>';"
/// );
///
/// let jwks = jwt_access(
///     "sso",
///     JwtConfig::new("RS256")
///         .with_url("https://auth.example.com/jwks")
///         .with_issuer("<private key>"),
/// );
/// assert_eq!(
///     jwks.to_surql().unwrap(),
///     "DEFINE ACCESS sso ON DATABASE TYPE JWT URL 'https://auth.example.com/jwks' \
///      WITH ISSUER ALGORITHM RS256 KEY '<private key>';"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JwtConfig {
    /// JWT signing algorithm (e.g. `HS256`, `RS256`). With a JWKS [`url`]
    /// the engine takes the verification algorithm from the key set, so this
    /// only applies to the [`issuer`] key.
    ///
    /// [`url`]: Self::url
    /// [`issuer`]: Self::issuer
    #[serde(default = "JwtConfig::default_algorithm")]
    pub algorithm: String,
    /// Verification key: the shared secret for an HMAC algorithm, the public
    /// key for an asymmetric one. Mutually exclusive with [`url`](Self::url).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub key: Option<String>,
    /// JWKS endpoint the engine fetches verification keys from. Mutually
    /// exclusive with [`key`](Self::key).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub url: Option<String>,
    /// Issuer signing key (`WITH ISSUER KEY`): the key the engine signs the
    /// tokens it issues with. For an HMAC algorithm the verification key
    /// already serves, so leave this unset; for an asymmetric one it is the
    /// private key paired with the public [`key`](Self::key). Without it a
    /// JWT access method only verifies externally issued tokens.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub issuer: Option<String>,
}

impl JwtConfig {
    fn default_algorithm() -> String {
        "HS256".into()
    }

    /// Construct an HS256 JWT configuration with only a key.
    pub fn hs256(key: impl Into<String>) -> Self {
        Self {
            algorithm: "HS256".into(),
            key: Some(key.into()),
            url: None,
            issuer: None,
        }
    }

    /// Construct a JWT configuration with the given algorithm.
    pub fn new(algorithm: impl Into<String>) -> Self {
        Self {
            algorithm: algorithm.into(),
            key: None,
            url: None,
            issuer: None,
        }
    }

    /// Set the verification key.
    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Set the JWKS URL.
    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Set the issuer signing key (`WITH ISSUER KEY`).
    pub fn with_issuer(mut self, issuer: impl Into<String>) -> Self {
        self.issuer = Some(issuer.into());
        self
    }

    /// Validate the configuration against the engine's grammar.
    ///
    /// Returns [`SurqlError::Validation`] for an unknown algorithm, or when
    /// the configuration names neither or both of [`key`](Self::key) and
    /// [`url`](Self::url).
    pub fn validate(&self) -> Result<()> {
        if !ALGORITHMS
            .iter()
            .any(|alg| alg.eq_ignore_ascii_case(&self.algorithm))
        {
            return Err(SurqlError::Validation {
                reason: format!(
                    "Unknown JWT algorithm {:?}; expected one of {}",
                    self.algorithm,
                    ALGORITHMS.join(", ")
                ),
            });
        }
        match (&self.key, &self.url) {
            (Some(_), None) | (None, Some(_)) => Ok(()),
            (None, None) => Err(SurqlError::Validation {
                reason: "JWT access needs a verification key or a JWKS url".into(),
            }),
            (Some(_), Some(_)) => Err(SurqlError::Validation {
                reason: "JWT access takes a verification key or a JWKS url, not both".into(),
            }),
        }
    }

    /// Render `ALGORITHM <alg> KEY '<key>'` or `URL '<url>'`, then the
    /// optional `WITH ISSUER` clause. The engine names the issuer algorithm
    /// only when the verifier (a JWKS url) does not already fix it.
    fn to_clause(&self) -> String {
        let algorithm = self.algorithm.to_ascii_uppercase();
        let mut sql = match (&self.key, &self.url) {
            (Some(key), _) => format!("ALGORITHM {algorithm} KEY {}", quote_str(key)),
            (None, Some(url)) => format!("URL {}", quote_str(url)),
            (None, None) => format!("ALGORITHM {algorithm}"),
        };
        if let Some(issuer) = &self.issuer {
            if self.key.is_some() {
                let _ = write!(sql, " WITH ISSUER KEY {}", quote_str(issuer));
            } else {
                let _ = write!(
                    sql,
                    " WITH ISSUER ALGORITHM {algorithm} KEY {}",
                    quote_str(issuer)
                );
            }
        }
        sql
    }
}

/// `true` for a SurrealQL duration literal (`24h`, `1h30m`) or `NONE`.
fn is_duration(text: &str) -> bool {
    !text.is_empty() && text.chars().all(|c| c.is_ascii_alphanumeric() || c == 'µ')
}

impl Default for JwtConfig {
    fn default() -> Self {
        Self {
            algorithm: Self::default_algorithm(),
            key: None,
            url: None,
            issuer: None,
        }
    }
}

/// Immutable record-access configuration for `DEFINE ACCESS ... TYPE RECORD`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordAccessConfig {
    /// SurrealQL expression that runs on signup.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub signup: Option<String>,
    /// SurrealQL expression that runs on signin.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub signin: Option<String>,
    /// JWT verifier for externally minted record tokens (`WITH JWT
    /// ...`). With this set, tokens signed with the declared key and
    /// carrying an `id` claim authenticate as record sessions
    /// directly; no signup or signin flow is required.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub jwt: Option<JwtConfig>,
}

impl RecordAccessConfig {
    /// Construct a new, empty record-access configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the signup expression.
    pub fn with_signup(mut self, signup: impl Into<String>) -> Self {
        self.signup = Some(signup.into());
        self
    }

    /// Set the signin expression.
    pub fn with_signin(mut self, signin: impl Into<String>) -> Self {
        self.signin = Some(signin.into());
        self
    }

    /// Attach a JWT verifier for externally minted record tokens.
    pub fn with_jwt(mut self, jwt: JwtConfig) -> Self {
        self.jwt = Some(jwt);
        self
    }
}

/// Immutable `DEFINE ACCESS` schema definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessDefinition {
    /// Access definition name.
    pub name: String,
    /// Access type (JWT / RECORD).
    #[serde(rename = "type")]
    pub access_type: AccessType,
    /// JWT configuration (required when `access_type == Jwt`).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub jwt: Option<JwtConfig>,
    /// Record configuration (required when `access_type == Record`).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub record: Option<RecordAccessConfig>,
    /// Session duration (e.g. `24h`, `7d`).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub duration_session: Option<String>,
    /// Token duration (e.g. `15m`).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub duration_token: Option<String>,
}

impl AccessDefinition {
    /// Construct a new JWT access definition.
    pub fn jwt(name: impl Into<String>, config: JwtConfig) -> Self {
        Self {
            name: name.into(),
            access_type: AccessType::Jwt,
            jwt: Some(config),
            record: None,
            duration_session: None,
            duration_token: None,
        }
    }

    /// Construct a new record-access definition.
    pub fn record(name: impl Into<String>, config: RecordAccessConfig) -> Self {
        Self {
            name: name.into(),
            access_type: AccessType::Record,
            jwt: None,
            record: Some(config),
            duration_session: None,
            duration_token: None,
        }
    }

    /// Set the session duration.
    pub fn with_session(mut self, duration: impl Into<String>) -> Self {
        self.duration_session = Some(duration.into());
        self
    }

    /// Set the token duration.
    pub fn with_token(mut self, duration: impl Into<String>) -> Self {
        self.duration_token = Some(duration.into());
        self
    }

    /// Validate the access definition.
    ///
    /// Returns [`SurqlError::Validation`] when:
    /// - the name is empty;
    /// - the access type is `Jwt` and no [`JwtConfig`] is set;
    /// - the access type is `Record` and no [`RecordAccessConfig`] is set;
    /// - a [`JwtConfig`] fails [`JwtConfig::validate`];
    /// - a duration is not a SurrealQL duration literal.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(SurqlError::Validation {
                reason: "Access name cannot be empty".into(),
            });
        }
        match (self.access_type, &self.jwt, &self.record) {
            (AccessType::Jwt, None, _) => {
                return Err(SurqlError::Validation {
                    reason: "JWT access type requires jwt config".into(),
                });
            }
            (AccessType::Jwt, Some(jwt), _) => jwt.validate()?,
            (AccessType::Record, _, None) => {
                return Err(SurqlError::Validation {
                    reason: "RECORD access type requires record config".into(),
                });
            }
            (AccessType::Record, _, Some(record)) => {
                if let Some(jwt) = &record.jwt {
                    jwt.validate()?;
                }
            }
        }
        for duration in [&self.duration_session, &self.duration_token]
            .into_iter()
            .flatten()
        {
            if !is_duration(duration) {
                return Err(SurqlError::Validation {
                    reason: format!("Access {:?}: {duration:?} is not a duration", self.name),
                });
            }
        }
        Ok(())
    }

    /// Render the `DEFINE ACCESS` statement.
    ///
    /// Validates the definition first; returns an error if validation fails.
    pub fn to_surql(&self) -> Result<String> {
        self.to_surql_with_options(false)
    }

    /// Render the `DEFINE ACCESS` statement, optionally with `IF NOT EXISTS` so
    /// it can be re-applied idempotently (e.g. on every connect to a persistent
    /// store). Validates the definition first.
    pub fn to_surql_with_options(&self, if_not_exists: bool) -> Result<String> {
        self.render_guard(if if_not_exists { "IF NOT EXISTS " } else { "" })
    }

    /// Render with `OVERWRITE`, replacing an existing definition while
    /// leaving stored data untouched. What schema evolution applies
    /// when a stored definition no longer matches the code.
    pub fn to_surql_overwrite(&self) -> Result<String> {
        self.render_guard("OVERWRITE ")
    }

    fn render_guard(&self, ine: &str) -> Result<String> {
        self.validate()?;
        let mut sql = format!(
            "DEFINE ACCESS {ine}{name} ON DATABASE TYPE {ty}",
            ine = ine,
            name = quote_ident(&self.name),
            ty = self.access_type.as_str(),
        );

        if let (AccessType::Jwt, Some(jwt)) = (self.access_type, &self.jwt) {
            sql.push(' ');
            sql.push_str(&jwt.to_clause());
        }

        if let (AccessType::Record, Some(record)) = (self.access_type, &self.record) {
            if let Some(signup) = &record.signup {
                let _ = write!(sql, " SIGNUP ({signup})");
            }
            if let Some(signin) = &record.signin {
                let _ = write!(sql, " SIGNIN ({signin})");
            }
            if let Some(jwt) = &record.jwt {
                let _ = write!(sql, " WITH JWT {}", jwt.to_clause());
            }
        }

        if self.duration_session.is_some() || self.duration_token.is_some() {
            let mut parts: Vec<String> = Vec::new();
            if let Some(session) = &self.duration_session {
                parts.push(format!("FOR SESSION {session}"));
            }
            if let Some(token) = &self.duration_token {
                parts.push(format!("FOR TOKEN {token}"));
            }
            let _ = write!(sql, " DURATION {}", parts.join(", "));
        }

        sql.push(';');
        Ok(sql)
    }
}

/// Builder for an [`AccessDefinition`] that defers JWT/record assignment.
#[derive(Debug, Clone)]
pub struct AccessSchemaBuilder {
    inner: AccessDefinition,
}

impl AccessSchemaBuilder {
    /// Set the JWT configuration (also sets `access_type` to `Jwt`).
    pub fn jwt(mut self, config: JwtConfig) -> Self {
        self.inner.access_type = AccessType::Jwt;
        self.inner.jwt = Some(config);
        self.inner.record = None;
        self
    }

    /// Set the record configuration (also sets `access_type` to `Record`).
    pub fn record(mut self, config: RecordAccessConfig) -> Self {
        self.inner.access_type = AccessType::Record;
        self.inner.record = Some(config);
        self.inner.jwt = None;
        self
    }

    /// Set the session duration.
    pub fn session(mut self, duration: impl Into<String>) -> Self {
        self.inner.duration_session = Some(duration.into());
        self
    }

    /// Set the token duration.
    pub fn token(mut self, duration: impl Into<String>) -> Self {
        self.inner.duration_token = Some(duration.into());
        self
    }

    /// Finalise the builder.
    pub fn build(self) -> Result<AccessDefinition> {
        self.inner.validate()?;
        Ok(self.inner)
    }
}

/// Functional constructor mirroring `surql.schema.access.access_schema`.
///
/// The returned builder requires a [`JwtConfig`] or [`RecordAccessConfig`] to
/// be attached before [`AccessSchemaBuilder::build`] will succeed.
pub fn access_schema(name: impl Into<String>, access_type: AccessType) -> AccessSchemaBuilder {
    AccessSchemaBuilder {
        inner: AccessDefinition {
            name: name.into(),
            access_type,
            jwt: None,
            record: None,
            duration_session: None,
            duration_token: None,
        },
    }
}

/// Convenience constructor for a JWT access definition.
pub fn jwt_access(name: impl Into<String>, config: JwtConfig) -> AccessDefinition {
    AccessDefinition::jwt(name, config)
}

/// Convenience constructor for a record-access definition.
pub fn record_access(name: impl Into<String>, config: RecordAccessConfig) -> AccessDefinition {
    AccessDefinition::record(name, config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn access_renders_overwrite() {
        let access = AccessDefinition::record(
            "caller",
            RecordAccessConfig::new().with_jwt(JwtConfig::hs256("secret")),
        );
        assert!(access
            .to_surql_overwrite()
            .unwrap()
            .starts_with("DEFINE ACCESS OVERWRITE caller ON DATABASE TYPE RECORD"));
    }

    #[test]
    fn record_access_renders_jwt_verifier() {
        let access = AccessDefinition::record(
            "caller",
            RecordAccessConfig::new().with_jwt(JwtConfig::hs256("secret")),
        )
        .with_session("1h");
        assert_eq!(
            access.to_surql().unwrap(),
            concat!(
                "DEFINE ACCESS caller ON DATABASE TYPE RECORD ",
                "WITH JWT ALGORITHM HS256 KEY 'secret' DURATION FOR SESSION 1h;"
            )
        );
    }

    #[test]
    fn access_type_strings() {
        assert_eq!(AccessType::Jwt.as_str(), "JWT");
        assert_eq!(AccessType::Record.as_str(), "RECORD");
    }

    #[test]
    fn access_type_display() {
        assert_eq!(format!("{}", AccessType::Jwt), "JWT");
    }

    #[test]
    fn access_type_serializes_uppercase() {
        let json = serde_json::to_string(&AccessType::Record).unwrap();
        assert_eq!(json, "\"RECORD\"");
    }

    #[test]
    fn jwt_config_default_algorithm() {
        let cfg = JwtConfig::default();
        assert_eq!(cfg.algorithm, "HS256");
        assert!(cfg.key.is_none());
    }

    #[test]
    fn jwt_config_hs256_helper() {
        let cfg = JwtConfig::hs256("secret");
        assert_eq!(cfg.algorithm, "HS256");
        assert_eq!(cfg.key.as_deref(), Some("secret"));
    }

    #[test]
    fn jwt_config_setters() {
        let cfg = JwtConfig::new("RS256")
            .with_url("https://auth.example.com/jwks")
            .with_issuer("private-key");
        assert_eq!(cfg.algorithm, "RS256");
        assert_eq!(cfg.url.as_deref(), Some("https://auth.example.com/jwks"));
        assert_eq!(cfg.issuer.as_deref(), Some("private-key"));
    }

    #[test]
    fn record_access_config_setters() {
        let cfg = RecordAccessConfig::new()
            .with_signup("CREATE user SET a = 1")
            .with_signin("SELECT * FROM user");
        assert_eq!(cfg.signup.as_deref(), Some("CREATE user SET a = 1"));
        assert_eq!(cfg.signin.as_deref(), Some("SELECT * FROM user"));
    }

    #[test]
    fn jwt_access_to_surql() {
        let a = jwt_access("api", JwtConfig::hs256("secret"));
        assert_eq!(
            a.to_surql().unwrap(),
            "DEFINE ACCESS api ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'secret';"
        );
    }

    #[test]
    fn jwt_access_with_url_and_issuer_follows_the_engine_grammar() {
        // `URL` replaces `ALGORITHM ... KEY`, and the issuer clause names a
        // key, not an issuer claim; the old `ALGORITHM RS256 URL ...
        // WITH ISSUER '...'` form was a parse error.
        let a = jwt_access(
            "api",
            JwtConfig::new("RS256")
                .with_url("https://auth.example.com/jwks")
                .with_issuer("private-key"),
        );
        assert_eq!(
            a.to_surql().unwrap(),
            "DEFINE ACCESS api ON DATABASE TYPE JWT URL 'https://auth.example.com/jwks' \
             WITH ISSUER ALGORITHM RS256 KEY 'private-key';"
        );
        let a = jwt_access(
            "api",
            JwtConfig::new("RS256")
                .with_key("public-key")
                .with_issuer("private-key"),
        );
        assert_eq!(
            a.to_surql().unwrap(),
            "DEFINE ACCESS api ON DATABASE TYPE JWT ALGORITHM RS256 KEY 'public-key' \
             WITH ISSUER KEY 'private-key';"
        );
    }

    #[test]
    fn jwt_config_needs_exactly_one_verifier_and_a_known_algorithm() {
        assert!(jwt_access("api", JwtConfig::new("RS256"))
            .to_surql()
            .is_err());
        assert!(jwt_access(
            "api",
            JwtConfig::new("RS256").with_key("k").with_url("https://x")
        )
        .to_surql()
        .is_err());
        assert!(jwt_access(
            "api",
            JwtConfig::new("RS256; REMOVE TABLE user").with_key("k")
        )
        .to_surql()
        .is_err());
        assert!(record_access(
            "r",
            RecordAccessConfig::new().with_jwt(JwtConfig::new("HS256"))
        )
        .to_surql()
        .is_err());
    }

    #[test]
    fn keys_and_names_are_escaped() {
        let a = jwt_access("my-api", JwtConfig::hs256(r"it's \ secret"));
        assert_eq!(
            a.to_surql().unwrap(),
            r"DEFINE ACCESS `my-api` ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'it\'s \\ secret';"
        );
        let bad = jwt_access("api", JwtConfig::hs256("k")).with_session("1h; REMOVE TABLE x");
        assert!(bad.to_surql().is_err());
    }

    #[test]
    fn access_to_surql_if_not_exists() {
        let a = jwt_access("api", JwtConfig::hs256("secret"));
        let sql = a.to_surql_with_options(true).unwrap();
        assert!(sql.starts_with("DEFINE ACCESS IF NOT EXISTS api ON DATABASE"));
        // Default stays without the guard.
        assert!(a
            .to_surql()
            .unwrap()
            .starts_with("DEFINE ACCESS api ON DATABASE"));
    }

    #[test]
    fn record_access_to_surql() {
        let a = record_access(
            "user_auth",
            RecordAccessConfig::new()
                .with_signup("CREATE user SET ...")
                .with_signin("SELECT * FROM user WHERE ..."),
        );
        let sql = a.to_surql().unwrap();
        assert!(sql.contains("TYPE RECORD"));
        assert!(sql.contains("SIGNUP (CREATE user SET ...)"));
        assert!(sql.contains("SIGNIN (SELECT * FROM user WHERE ...)"));
    }

    #[test]
    fn duration_clause_renders() {
        let a = jwt_access("api", JwtConfig::hs256("secret"))
            .with_session("24h")
            .with_token("15m");
        let sql = a.to_surql().unwrap();
        assert!(sql.contains("DURATION FOR SESSION 24h, FOR TOKEN 15m"));
    }

    #[test]
    fn duration_session_only_renders() {
        let a = jwt_access("api", JwtConfig::hs256("secret")).with_session("7d");
        let sql = a.to_surql().unwrap();
        assert!(sql.contains("DURATION FOR SESSION 7d"));
        assert!(!sql.contains("FOR TOKEN"));
    }

    #[test]
    fn duration_token_only_renders() {
        let a = jwt_access("api", JwtConfig::hs256("secret")).with_token("1h");
        let sql = a.to_surql().unwrap();
        assert!(sql.contains("DURATION FOR TOKEN 1h"));
        assert!(!sql.contains("FOR SESSION"));
    }

    #[test]
    fn validate_rejects_empty_name() {
        let mut a = jwt_access("api", JwtConfig::hs256("secret"));
        a.name = String::new();
        assert!(a.validate().is_err());
    }

    #[test]
    fn validate_rejects_jwt_without_config() {
        let mut a = jwt_access("api", JwtConfig::hs256("secret"));
        a.jwt = None;
        assert!(a.validate().is_err());
    }

    #[test]
    fn validate_rejects_record_without_config() {
        let mut a = record_access("user_auth", RecordAccessConfig::new());
        a.record = None;
        assert!(a.validate().is_err());
    }

    #[test]
    fn access_schema_builder_jwt() {
        let a = access_schema("api", AccessType::Jwt)
            .jwt(JwtConfig::hs256("secret"))
            .session("24h")
            .token("15m")
            .build()
            .unwrap();
        assert_eq!(a.access_type, AccessType::Jwt);
        assert_eq!(a.duration_session.as_deref(), Some("24h"));
    }

    #[test]
    fn access_schema_builder_record() {
        let a = access_schema("user_auth", AccessType::Record)
            .record(RecordAccessConfig::new().with_signup("CREATE user"))
            .build()
            .unwrap();
        assert_eq!(a.access_type, AccessType::Record);
        assert_eq!(
            a.record.as_ref().unwrap().signup.as_deref(),
            Some("CREATE user")
        );
    }

    #[test]
    fn access_schema_builder_missing_config_fails() {
        let err = access_schema("api", AccessType::Jwt).build().unwrap_err();
        assert!(matches!(err, SurqlError::Validation { .. }));
    }

    #[test]
    fn access_schema_builder_swap_jwt_to_record() {
        let a = access_schema("x", AccessType::Jwt)
            .jwt(JwtConfig::hs256("s"))
            .record(RecordAccessConfig::new().with_signup("CREATE user"))
            .build()
            .unwrap();
        assert_eq!(a.access_type, AccessType::Record);
        assert!(a.jwt.is_none());
    }

    #[test]
    fn clone_and_eq() {
        let a = jwt_access("api", JwtConfig::hs256("secret"));
        assert_eq!(a.clone(), a);
    }
}
