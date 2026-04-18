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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JwtConfig {
    /// JWT signing algorithm (e.g. `HS256`, `RS256`).
    #[serde(default = "JwtConfig::default_algorithm")]
    pub algorithm: String,
    /// Symmetric key for HMAC algorithms.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub key: Option<String>,
    /// JWKS endpoint URL for key discovery.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub url: Option<String>,
    /// Expected token issuer claim.
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

    /// Set the symmetric key.
    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Set the JWKS URL.
    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Set the expected issuer.
    pub fn with_issuer(mut self, issuer: impl Into<String>) -> Self {
        self.issuer = Some(issuer.into());
        self
    }
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
    /// - the access type is `Record` and no [`RecordAccessConfig`] is set.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(SurqlError::Validation {
                reason: "Access name cannot be empty".into(),
            });
        }
        match self.access_type {
            AccessType::Jwt if self.jwt.is_none() => Err(SurqlError::Validation {
                reason: "JWT access type requires jwt config".into(),
            }),
            AccessType::Record if self.record.is_none() => Err(SurqlError::Validation {
                reason: "RECORD access type requires record config".into(),
            }),
            _ => Ok(()),
        }
    }

    /// Render the `DEFINE ACCESS` statement.
    ///
    /// Validates the definition first; returns an error if validation fails.
    pub fn to_surql(&self) -> Result<String> {
        self.validate()?;
        let mut sql = format!(
            "DEFINE ACCESS {name} ON DATABASE TYPE {ty}",
            name = self.name,
            ty = self.access_type.as_str(),
        );

        if let (AccessType::Jwt, Some(jwt)) = (self.access_type, &self.jwt) {
            write!(sql, " ALGORITHM {}", jwt.algorithm).expect("writing to String cannot fail");
            if let Some(key) = &jwt.key {
                write!(sql, " KEY '{}'", key).expect("writing to String cannot fail");
            }
            if let Some(url) = &jwt.url {
                write!(sql, " URL '{}'", url).expect("writing to String cannot fail");
            }
            if let Some(iss) = &jwt.issuer {
                write!(sql, " WITH ISSUER '{}'", iss).expect("writing to String cannot fail");
            }
        }

        if let (AccessType::Record, Some(record)) = (self.access_type, &self.record) {
            if let Some(signup) = &record.signup {
                write!(sql, " SIGNUP ({})", signup).expect("writing to String cannot fail");
            }
            if let Some(signin) = &record.signin {
                write!(sql, " SIGNIN ({})", signin).expect("writing to String cannot fail");
            }
        }

        if self.duration_session.is_some() || self.duration_token.is_some() {
            let mut parts: Vec<String> = Vec::new();
            if let Some(session) = &self.duration_session {
                parts.push(format!("FOR SESSION {}", session));
            }
            if let Some(token) = &self.duration_token {
                parts.push(format!("FOR TOKEN {}", token));
            }
            write!(sql, " DURATION {}", parts.join(", "))
                .expect("writing to String cannot fail");
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
            .with_issuer("example");
        assert_eq!(cfg.algorithm, "RS256");
        assert_eq!(cfg.url.as_deref(), Some("https://auth.example.com/jwks"));
        assert_eq!(cfg.issuer.as_deref(), Some("example"));
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
    fn jwt_access_with_url_and_issuer() {
        let a = jwt_access(
            "api",
            JwtConfig::new("RS256")
                .with_url("https://auth.example.com/jwks")
                .with_issuer("https://auth.example.com"),
        );
        let sql = a.to_surql().unwrap();
        assert!(sql.contains("URL 'https://auth.example.com/jwks'"));
        assert!(sql.contains("WITH ISSUER 'https://auth.example.com'"));
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
