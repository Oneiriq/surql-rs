//! Authentication types for SurrealDB connections.
//!
//! Port of `surql/connection/auth.py`. Covers the four SurrealDB auth
//! levels (root / namespace / database / scope) plus JWT token auth, as
//! pure data types. The Python `AuthManager` ties these to the async
//! client; that wrapper lands with the runtime client in a later PR.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// SurrealDB authentication level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthType {
    /// Server root user.
    Root,
    /// Namespace user.
    Namespace,
    /// Database user.
    Database,
    /// Scope/record-level user.
    Scope,
}

impl std::fmt::Display for AuthType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Root => "root",
            Self::Namespace => "namespace",
            Self::Database => "database",
            Self::Scope => "scope",
        };
        f.write_str(s)
    }
}

/// Trait implemented by every credential type so the runtime client can
/// serialise them to the SurrealDB SDK's `signin`/`signup` payload.
pub trait Credentials {
    /// SurrealDB auth level this credential targets.
    fn auth_type(&self) -> AuthType;

    /// Flatten the credential to a JSON object for the SurrealDB SDK.
    fn to_signin_payload(&self) -> Map<String, Value>;
}

/// Root-level credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootCredentials {
    /// Root username.
    pub username: String,
    /// Root password.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub password: Option<String>,
}

impl RootCredentials {
    /// Construct root credentials.
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: Some(password.into()),
        }
    }
}

impl Credentials for RootCredentials {
    fn auth_type(&self) -> AuthType {
        AuthType::Root
    }

    fn to_signin_payload(&self) -> Map<String, Value> {
        let mut m = Map::new();
        m.insert("username".into(), Value::String(self.username.clone()));
        if let Some(p) = &self.password {
            m.insert("password".into(), Value::String(p.clone()));
        }
        m
    }
}

/// Namespace-level credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceCredentials {
    /// Target namespace.
    pub namespace: String,
    /// Namespace username.
    pub username: String,
    /// Namespace password.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub password: Option<String>,
}

impl NamespaceCredentials {
    /// Construct namespace credentials.
    pub fn new(
        namespace: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            username: username.into(),
            password: Some(password.into()),
        }
    }
}

impl Credentials for NamespaceCredentials {
    fn auth_type(&self) -> AuthType {
        AuthType::Namespace
    }

    fn to_signin_payload(&self) -> Map<String, Value> {
        let mut m = Map::new();
        m.insert("namespace".into(), Value::String(self.namespace.clone()));
        m.insert("username".into(), Value::String(self.username.clone()));
        if let Some(p) = &self.password {
            m.insert("password".into(), Value::String(p.clone()));
        }
        m
    }
}

/// Database-level credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseCredentials {
    /// Target namespace.
    pub namespace: String,
    /// Target database.
    pub database: String,
    /// Database username.
    pub username: String,
    /// Database password.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub password: Option<String>,
}

impl DatabaseCredentials {
    /// Construct database credentials.
    pub fn new(
        namespace: impl Into<String>,
        database: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            database: database.into(),
            username: username.into(),
            password: Some(password.into()),
        }
    }
}

impl Credentials for DatabaseCredentials {
    fn auth_type(&self) -> AuthType {
        AuthType::Database
    }

    fn to_signin_payload(&self) -> Map<String, Value> {
        let mut m = Map::new();
        m.insert("namespace".into(), Value::String(self.namespace.clone()));
        m.insert("database".into(), Value::String(self.database.clone()));
        m.insert("username".into(), Value::String(self.username.clone()));
        if let Some(p) = &self.password {
            m.insert("password".into(), Value::String(p.clone()));
        }
        m
    }
}

/// Scope-level (record access) credentials.
///
/// `variables` holds the scope-defined fields (commonly `email`,
/// `password`, etc.). They are flattened into the top-level payload at
/// signin time to match the SDK contract.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScopeCredentials {
    /// Target namespace.
    pub namespace: String,
    /// Target database.
    pub database: String,
    /// Access/scope name.
    pub access: String,
    /// Scope-defined variables (stored sorted for deterministic output).
    #[serde(default)]
    pub variables: BTreeMap<String, Value>,
}

impl ScopeCredentials {
    /// Construct scope credentials (with an empty variable set).
    pub fn new(
        namespace: impl Into<String>,
        database: impl Into<String>,
        access: impl Into<String>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            database: database.into(),
            access: access.into(),
            variables: BTreeMap::new(),
        }
    }

    /// Attach a scope variable (e.g. `"email"`, `"password"`).
    pub fn with(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.variables.insert(key.into(), value.into());
        self
    }
}

impl Credentials for ScopeCredentials {
    fn auth_type(&self) -> AuthType {
        AuthType::Scope
    }

    fn to_signin_payload(&self) -> Map<String, Value> {
        let mut m = Map::new();
        m.insert("namespace".into(), Value::String(self.namespace.clone()));
        m.insert("database".into(), Value::String(self.database.clone()));
        m.insert("access".into(), Value::String(self.access.clone()));
        for (k, v) in &self.variables {
            m.insert(k.clone(), v.clone());
        }
        m
    }
}

/// Pre-existing JWT token authentication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenAuth {
    /// JWT authentication token.
    pub token: String,
}

impl TokenAuth {
    /// Construct token auth.
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn auth_type_display() {
        assert_eq!(AuthType::Root.to_string(), "root");
        assert_eq!(AuthType::Namespace.to_string(), "namespace");
        assert_eq!(AuthType::Database.to_string(), "database");
        assert_eq!(AuthType::Scope.to_string(), "scope");
    }

    #[test]
    fn root_payload() {
        let creds = RootCredentials::new("root", "secret");
        let p = creds.to_signin_payload();
        assert_eq!(p.get("username").unwrap(), &json!("root"));
        assert_eq!(p.get("password").unwrap(), &json!("secret"));
        assert_eq!(creds.auth_type(), AuthType::Root);
    }

    #[test]
    fn namespace_payload() {
        let creds = NamespaceCredentials::new("prod", "u", "p");
        let p = creds.to_signin_payload();
        assert_eq!(p.get("namespace").unwrap(), &json!("prod"));
        assert_eq!(p.get("username").unwrap(), &json!("u"));
        assert_eq!(p.get("password").unwrap(), &json!("p"));
        assert_eq!(creds.auth_type(), AuthType::Namespace);
    }

    #[test]
    fn database_payload() {
        let creds = DatabaseCredentials::new("prod", "app", "u", "p");
        let p = creds.to_signin_payload();
        assert_eq!(p.get("namespace").unwrap(), &json!("prod"));
        assert_eq!(p.get("database").unwrap(), &json!("app"));
        assert_eq!(p.get("username").unwrap(), &json!("u"));
        assert_eq!(p.get("password").unwrap(), &json!("p"));
        assert_eq!(creds.auth_type(), AuthType::Database);
    }

    #[test]
    fn scope_payload_flattens_variables() {
        let creds = ScopeCredentials::new("prod", "app", "user")
            .with("email", "a@example.com")
            .with("password", "secret");
        let p = creds.to_signin_payload();
        assert_eq!(p.get("namespace").unwrap(), &json!("prod"));
        assert_eq!(p.get("database").unwrap(), &json!("app"));
        assert_eq!(p.get("access").unwrap(), &json!("user"));
        assert_eq!(p.get("email").unwrap(), &json!("a@example.com"));
        assert_eq!(p.get("password").unwrap(), &json!("secret"));
        assert_eq!(creds.auth_type(), AuthType::Scope);
    }

    #[test]
    fn token_auth_debug_does_not_panic() {
        let t = TokenAuth::new("eyJhbGciOiJIUzI1NiJ9.abc");
        // Ensure Debug is implemented; this will print the token since
        // we keep Serialize for wire use. Users should avoid logging.
        let _ = format!("{t:?}");
    }

    #[test]
    fn auth_type_serde() {
        let json = serde_json::to_string(&AuthType::Root).unwrap();
        assert_eq!(json, "\"root\"");
        let back: AuthType = serde_json::from_str("\"scope\"").unwrap();
        assert_eq!(back, AuthType::Scope);
    }

    #[test]
    fn root_credentials_skip_missing_password() {
        let creds = RootCredentials {
            username: "root".into(),
            password: None,
        };
        let json = serde_json::to_string(&creds).unwrap();
        assert!(!json.contains("password"));
    }
}
