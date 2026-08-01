//! Per-caller engine sessions over one connection.
//!
//! `DatabaseClient::caller_session` clones the SDK handle, which
//! mints an independent engine session, and binds a record access
//! token to it. The engine then filters rows and fields by
//! `PERMISSIONS` for that session while the parent client keeps full
//! authority.
//!
//! Enforcement follows the ACTOR, and engine credentials only decide
//! what anonymous sessions may do: a record session is constrained
//! even on an engine built without credentials, where anonymous
//! sessions act as owner. Both engine states are covered here, and
//! the `username`/`password` connection settings now reach embedded
//! engines at build time so the locked state is reachable at all.

#![cfg(any(feature = "client", feature = "client-rustls"))]

use base64::Engine as _;
use serde_json::Value;
use sha2::{Digest, Sha256};

use surql::connection::{ConnectionConfig, DatabaseClient};

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// HMAC-SHA256 by hand: the crate's sha2 is 0.11, whose digest traits
/// the published hmac crate does not speak yet, and twelve lines beat
/// a second sha2 version in the tree.
fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut block = [0u8; 64];
    if key.len() > 64 {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let inner: Vec<u8> = block.iter().map(|b| b ^ 0x36).collect();
    let outer: Vec<u8> = block.iter().map(|b| b ^ 0x5c).collect();
    let inner_hash = Sha256::digest([inner.as_slice(), message].concat());
    Sha256::digest([outer.as_slice(), &inner_hash].concat()).into()
}

/// A hand-rolled HS256 JWT, enough for the engine to verify.
fn jwt(secret: &str, claims: &Value) -> String {
    let header = b64(serde_json::json!({ "alg": "HS256", "typ": "JWT" })
        .to_string()
        .as_bytes());
    let payload = b64(claims.to_string().as_bytes());
    let signing_input = format!("{header}.{payload}");
    let signature = b64(&hmac_sha256(secret.as_bytes(), signing_input.as_bytes()));
    format!("{signing_input}.{signature}")
}

fn caller_claims(namespace: &str, access: &str, id: Option<&str>) -> Value {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut claims = serde_json::json!({
        "iss": "surql-tests",
        "iat": now,
        "exp": now + 3600,
        "ns": namespace,
        "db": namespace,
        "ac": access,
        "tn": "acme",
    });
    if let Some(id) = id {
        claims["id"] = Value::String(id.to_owned());
    }
    claims
}

async fn connected(namespace: &str, credentialed: bool) -> DatabaseClient {
    let mut builder = ConnectionConfig::builder()
        .url("mem://")
        .namespace(namespace)
        .database(namespace);
    if credentialed {
        builder = builder.username("root").password("root");
    }
    let client = DatabaseClient::new(builder.build().expect("valid config")).expect("constructs");
    client.connect().await.expect("connects");
    client
}

async fn define_guarded_table(client: &DatabaseClient) {
    client
        .query(
            "DEFINE ACCESS caller ON DATABASE TYPE RECORD \
             WITH JWT ALGORITHM HS256 KEY 'session-secret' DURATION FOR SESSION 1h;",
        )
        .await
        .expect("access defines");
    client
        .query(
            "DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select WHERE tenant = $token.tn \
             FOR create, update, delete NONE; \
             CREATE doc SET tenant = 'acme', body = 'ours'; \
             CREATE doc SET tenant = 'rival', body = 'theirs';",
        )
        .await
        .expect("table defines and seeds");
}

fn rows(result: &Value, statement: usize) -> &Vec<Value> {
    result
        .get(statement)
        .and_then(|v| v.as_array())
        .expect("statement result is an array")
}

async fn assert_engine_filters(root: &DatabaseClient, namespace: &str) {
    let token = jwt(
        "session-secret",
        &caller_claims(namespace, "caller", Some("caller_ident:ck1")),
    );
    let caller = root.caller_session(&token).await.expect("session binds");

    let seen = caller.query("SELECT * FROM doc;").await.expect("select");
    let seen = rows(&seen, 0);
    assert_eq!(seen.len(), 1, "table permissions filter rows: {seen:?}");
    assert_eq!(seen[0]["tenant"], "acme");

    let all = root.query("SELECT * FROM doc;").await.expect("select");
    assert_eq!(
        rows(&all, 0).len(),
        2,
        "the parent session keeps full authority"
    );

    // A refused write returns empty rows with NO error; the count
    // proves nothing landed.
    let written = caller
        .query("CREATE doc SET tenant = 'acme', body = 'sneaky';")
        .await
        .expect("refused writes do not error");
    assert!(
        rows(&written, 0).is_empty(),
        "write was admitted: {written:?}"
    );
    let count = root
        .query("SELECT count() FROM doc GROUP ALL;")
        .await
        .expect("count");
    assert_eq!(rows(&count, 0)[0]["count"], 2);

    // The caller session ends with its client; the parent stays up.
    drop(caller);
    root.query("SELECT * FROM doc;")
        .await
        .expect("parent session survives");
}

#[tokio::test]
async fn caller_session_is_engine_filtered() {
    let root = connected("sessions", true).await;
    define_guarded_table(&root).await;
    assert_engine_filters(&root, "sessions").await;
}

/// Enforcement follows the actor. Credentials lock the anonymous
/// front door; the record session is filtered either way.
#[tokio::test]
async fn caller_session_enforces_on_credential_less_engines() {
    let root = connected("open_sessions", false).await;
    define_guarded_table(&root).await;
    assert_engine_filters(&root, "open_sessions").await;
}

#[tokio::test]
async fn caller_session_refuses_tokens_without_record_identity() {
    let root = connected("unbound_sessions", true).await;
    define_guarded_table(&root).await;

    // A record access token without an `id` claim dies at
    // authenticate: the engine has no record to bind.
    let idless = jwt(
        "session-secret",
        &caller_claims("unbound_sessions", "caller", None),
    );
    assert!(root.caller_session(&idless).await.is_err());

    // A plain JWT access method authenticates fine and yields a
    // database-level session, which `PERMISSIONS` clauses do not
    // filter. That is exactly what the identity check refuses.
    root.query(
        "DEFINE ACCESS system ON DATABASE TYPE JWT \
         ALGORITHM HS256 KEY 'session-secret' DURATION FOR SESSION 1h;",
    )
    .await
    .expect("jwt access defines");
    let system = jwt(
        "session-secret",
        &caller_claims("unbound_sessions", "system", None),
    );
    let err = root.caller_session(&system).await.expect_err("must refuse");
    assert!(
        err.to_string().contains("no record identity"),
        "unexpected error: {err}"
    );
}
