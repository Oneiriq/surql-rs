//! Unit tests for [`DatabaseClient`], most against an embedded `mem://`
//! engine.

use std::sync::OnceLock;

use surrealdb::types::{AuthError, NotAllowedError};

use super::*;
use crate::connection::auth::RootCredentials;
use crate::connection::auth_manager::AuthManager;

/// The root password of every embedded engine in this file, drawn once per
/// run so no credential here is a literal.
fn test_password() -> &'static str {
    static PASSWORD: OnceLock<String> = OnceLock::new();
    PASSWORD.get_or_init(|| ulid::Ulid::generate().to_string())
}

#[test]
fn new_validates_config() {
    let cfg = ConnectionConfig::default();
    let client = DatabaseClient::new(cfg).expect("valid default config");
    assert!(!client.is_connected());
}

/// Regression: the derived `Debug` printed the config's password (and
/// the SDK handle); a client is routinely logged.
#[test]
fn debug_redacts_the_config_secrets() {
    let url_secret = ulid::Ulid::generate().to_string();
    let client = DatabaseClient::new(ConnectionConfig {
        db_url: format!("ws://svc:{url_secret}@db.example/rpc"),
        db_user: Some("svc".into()),
        db_pass: Some(test_password().to_owned()),
        ..Default::default()
    })
    .unwrap();
    let shown = format!("{client:?}");
    assert!(!shown.contains(test_password()), "Debug shows the password");
    assert!(!shown.contains(&url_secret), "Debug shows URL userinfo");
}

#[test]
fn new_rejects_invalid_config() {
    let bad = ConnectionConfig {
        db_url: "ftp://nope".into(),
        ..Default::default()
    };
    assert!(DatabaseClient::new(bad).is_err());
}

#[test]
fn flatten_rows_typed_handles_wrapped_and_flat_shapes() {
    #[derive(serde::Deserialize, Debug, PartialEq)]
    struct Row {
        name: String,
    }
    let wrapped = serde_json::json!([
        { "result": [{ "name": "alice" }, { "name": "bob" }] }
    ]);
    let rows: Vec<Row> = flatten_rows_typed(&wrapped).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].name, "alice");

    let flat = serde_json::json!([[{ "name": "carol" }]]);
    let rows: Vec<Row> = flatten_rows_typed(&flat).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "carol");
}

/// Regression: rows were flattened recursively, so a row with a `result`
/// field was replaced by that field's value (and vanished when it was
/// null), and array-valued rows were spliced into their elements.
#[test]
fn flatten_rows_typed_spreads_exactly_one_level() {
    let raw = serde_json::json!([
        [
            { "id": "job:1", "result": "ok" },
            { "id": "job:2", "result": null },
            { "result": 5 }
        ],
        null
    ]);
    let rows: Vec<Value> = flatten_rows_typed(&raw).unwrap();
    assert_eq!(
        rows,
        vec![
            serde_json::json!({ "id": "job:1", "result": "ok" }),
            serde_json::json!({ "id": "job:2", "result": null }),
            serde_json::json!({ "result": 5 }),
        ]
    );

    // `SELECT VALUE tags FROM post`: every row is itself an array.
    let raw = serde_json::json!([[[1, 2], [3]]]);
    let rows: Vec<Vec<i64>> = flatten_rows_typed(&raw).unwrap();
    assert_eq!(rows, vec![vec![1, 2], vec![3]]);

    // The legacy envelope is unwrapped at statement level only, and only
    // when it has nothing but envelope keys.
    let raw = serde_json::json!([
        { "result": [{ "n": 1 }], "status": "OK", "time": "1ms" },
        { "result": 2, "note": "a row, not an envelope" }
    ]);
    let rows: Vec<Value> = flatten_rows_typed(&raw).unwrap();
    assert_eq!(
        rows,
        vec![
            serde_json::json!({ "n": 1 }),
            serde_json::json!({ "result": 2, "note": "a row, not an envelope" }),
        ]
    );
}

#[tokio::test]
async fn select_keeps_rows_with_a_result_field() {
    let client = DatabaseClient::new(root_mem_config("rows")).unwrap();
    client.connect().await.unwrap();
    client
        .query("CREATE job:1 SET result = 'ok'; CREATE job:2 SET result = NULL;")
        .await
        .unwrap();
    let rows: Vec<Value> = client.select("job").await.unwrap();
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert!(rows.iter().all(Value::is_object), "{rows:?}");
}

#[test]
fn first_row_typed_returns_none_for_empty_array() {
    let raw = serde_json::json!([[]]);
    let row: Option<Value> = first_row_typed(&raw).unwrap();
    assert!(row.is_none());
}

#[test]
fn payload_str_round_trip() {
    let creds = RootCredentials::new("root", test_password());
    let m = creds.to_signin_payload();
    assert_eq!(payload_str(&m, "username").unwrap(), "root");
    assert!(
        payload_str(&m, "password").is_ok_and(|p| p == test_password()),
        "the password round-trips"
    );
    assert!(payload_str(&m, "missing").is_err());
}

#[tokio::test]
async fn disconnect_when_never_connected_is_ok() {
    let client = DatabaseClient::new(ConnectionConfig::default()).unwrap();
    client.disconnect().await.unwrap();
    assert!(!client.is_connected());
}

/// Regression: a connect attempt that gets the engine up but fails a
/// later step (here: root signin) used to die on the SDK's "Already
/// connected" rejection on every retry, masking the real error behind
/// it. Credentials now initialise embedded engines at build time, so
/// the failing-signin state is constructed directly: the engine comes
/// up without the config's credentials, the way an external engine
/// with different credentials would look.
#[tokio::test]
async fn retries_surface_the_failing_step_not_already_connected() {
    let cfg = ConnectionConfig::builder()
        .url("mem://")
        .namespace("t")
        .database("t")
        .username("root")
        .password("wrong")
        .retry_max_attempts(3)
        .retry_min_wait(0.1)
        .retry_max_wait(1.0)
        .build()
        .unwrap();
    let client = DatabaseClient::new(cfg).unwrap();
    client.inner.connect("mem://".to_owned()).await.unwrap();
    *client.engine_connected.write().await = true;
    let err = client.connect().await.unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        !msg.contains("already connected"),
        "the signin failure must surface, not the engine re-connect: {err}"
    );
    assert!(!client.is_connected());
}

/// Regression: reconnecting an already-connected client used to fail the
/// same way (the entry disconnect only invalidates; the engine stays up,
/// so the fresh `connect` died on "Already connected").
#[tokio::test]
async fn reconnect_on_the_same_client_succeeds() {
    let cfg = ConnectionConfig::builder()
        .url("mem://")
        .namespace("t")
        .database("t")
        .retry_max_attempts(1)
        .build()
        .unwrap();
    let client = DatabaseClient::new(cfg).unwrap();
    client.connect().await.unwrap();
    assert!(client.is_connected());
    client.connect().await.unwrap();
    assert!(client.is_connected(), "reconnect lands back in service");
    // And the session still works.
    client.query("INFO FOR DB").await.unwrap();
}

#[tokio::test]
async fn operations_fail_when_not_connected() {
    let client = DatabaseClient::new(ConnectionConfig::default()).unwrap();
    let err = client.query("INFO FOR DB").await.unwrap_err();
    assert!(matches!(err, SurqlError::Connection { .. }));
}

#[test]
fn backoff_respects_bounds() {
    let cfg = ConnectionConfig {
        db_retry_min_wait: 0.5,
        db_retry_max_wait: 4.0,
        db_retry_multiplier: 2.0,
        ..Default::default()
    };
    let client = DatabaseClient::new(cfg).unwrap();
    let a1 = client.backoff_for(1);
    let a5 = client.backoff_for(5);
    assert!(a1 >= Duration::from_secs_f64(0.5));
    assert!(a5 <= Duration::from_secs_f64(4.0));
}

/// A long-lived session the engine expires must heal in place on a
/// client whose session holds the config credentials' authority: give
/// the config's own user one-second sessions, reconnect to start one,
/// let it expire, and the next query must succeed by replaying the
/// configured session -- the production incident (every request
/// failing "The session has expired" until a restart) in miniature.
/// On runs where the embedded engine declines to enforce the expiry
/// the query succeeds directly, so the test can never false-fail; the
/// runs that do enforce it exercise the whole replay path (the ws
/// integration suite exercises it deterministically).
#[tokio::test]
async fn expired_session_heals_on_a_config_credentialed_client() {
    let client = DatabaseClient::new(root_mem_config("heal")).unwrap();
    client.connect().await.unwrap();
    client
        .query(&format!(
            "DEFINE USER OVERWRITE root ON ROOT PASSWORD {} ROLES OWNER \
             DURATION FOR SESSION 1s;",
            crate::types::escape::quote_str(test_password())
        ))
        .await
        .unwrap();
    client.connect().await.unwrap();
    sleep(Duration::from_millis(2500)).await;
    client
        .query("INFO FOR DB")
        .await
        .expect("the expired session replays the config credentials and retries");
}

/// The replay guard is the security boundary: only a session whose
/// authority IS the config credentials may have them replayed. The
/// truth table runs every transition through the real methods on an
/// embedded engine, so the guard is pinned to what `connect`,
/// `signin`, `signup`, `authenticate`, `invalidate`, `disconnect` and
/// `caller_session` actually do, not to a hand-set field.
#[tokio::test]
async fn replay_guard_truth_table() {
    let service = DatabaseClient::new(root_mem_config("truth")).unwrap();
    assert!(
        !service.can_replay_session(),
        "never connected: no session to heal"
    );

    service.connect().await.unwrap();
    assert!(
        service.can_replay_session(),
        "config credentials + connect: the one shape that replays"
    );
    assert!(
        service.clone().can_replay_session(),
        "clones share the session, so they share the authority"
    );

    let token = service
        .signin(&RootCredentials::new("root", test_password()))
        .await
        .unwrap();
    assert!(
        !service.can_replay_session(),
        "signin gives the session another identity"
    );
    service.connect().await.unwrap();
    assert!(service.can_replay_session(), "connect restores it");

    let clone = service.clone();
    clone.authenticate(&token.token).await.unwrap();
    assert!(
        !service.can_replay_session(),
        "authenticate on ANY clone changes the shared session"
    );
    service.connect().await.unwrap();

    define_member_access(&service).await;
    let member = service
        .signup(&ScopeCredentials::new("truth", "truth", "member").with("name", "m"))
        .await
        .unwrap();
    assert!(!service.can_replay_session(), "signup, likewise");
    service.connect().await.unwrap();

    service.invalidate().await.unwrap();
    assert!(!service.can_replay_session(), "invalidate, likewise");
    service.connect().await.unwrap();

    let caller = service.caller_session(&member.token).await.unwrap();
    assert!(
        !caller.can_replay_session(),
        "a caller session never replays, even with config credentials present"
    );
    assert!(
        service.can_replay_session(),
        "a caller session is its own session: the parent keeps its authority"
    );

    service.disconnect().await.unwrap();
    assert!(!service.can_replay_session(), "disconnect ends it");

    let anonymous = DatabaseClient::new(ConnectionConfig {
        db_user: None,
        db_pass: None,
        ..root_mem_config("truth_anon")
    })
    .unwrap();
    anonymous.connect().await.unwrap();
    assert!(
        !anonymous.can_replay_session(),
        "no config credentials: nothing safe to replay"
    );
}

#[test]
fn session_expiry_is_read_from_the_request_error_only() {
    let structured = surrealdb::Error::not_allowed(
        "The session has expired".into(),
        NotAllowedError::Auth(AuthError::SessionExpired),
    );
    assert!(request_says_session_expired(&structured));
    let unstructured = surrealdb::Error::not_allowed("The session has expired".into(), None);
    assert!(request_says_session_expired(&unstructured));
    // A statement's own error text is never read as expiry.
    let thrown = surrealdb::Error::thrown("The session has expired".into());
    assert!(!request_says_session_expired(&thrown));
    let embedded = surrealdb::Error::not_allowed(
        "Found 'The session has expired' for field `note`".into(),
        None,
    );
    assert!(!request_says_session_expired(&embedded));
}

#[test]
fn only_a_lost_write_conflict_is_sent_again() {
    use surrealdb::types::QueryError;
    let structured = surrealdb::Error::query(
        "Transaction conflict".into(),
        QueryError::TransactionConflict,
    );
    assert!(is_retryable_conflict(&structured));
    // A peer that sends the engine's wording without the details.
    let worded = surrealdb::Error::query(
        "There was a problem with the key-value store: Transaction conflict: Write conflict,          retry the transaction. This transaction can be retried"
            .into(),
        None,
    );
    assert!(is_retryable_conflict(&worded));
    let not_executed = surrealdb::Error::query(
        "The query was not executed due to a failed transaction".into(),
        QueryError::NotExecuted,
    );
    assert!(!is_retryable_conflict(&not_executed));
    let thrown =
        surrealdb::Error::thrown("Transaction conflict: This transaction can be retried".into());
    assert!(!is_retryable_conflict(&thrown));
}

#[test]
fn render_target_accepts_tables_and_record_ids_only() {
    assert_eq!(render_target("user").unwrap(), "user");
    assert_eq!(render_target(" user ").unwrap(), "user");
    assert_eq!(render_target("select").unwrap(), "`select`");
    assert_eq!(render_target("user:alice").unwrap(), "user:alice");
    assert_eq!(render_target("post:42").unwrap(), "post:42");
    assert_eq!(render_target("user:⟨a-b⟩").unwrap(), "user:⟨a-b⟩");
    assert_eq!(
        render_target("user:x; REMOVE TABLE user").unwrap(),
        "user:⟨x; REMOVE TABLE user⟩"
    );
    assert_eq!(
        render_target("user:x⟩; REMOVE TABLE user").unwrap(),
        "user:`x⟩; REMOVE TABLE user`"
    );
    for bad in [
        "",
        "user; REMOVE TABLE user",
        "user WHERE true",
        "1user:a",
        ":a",
    ] {
        assert!(
            matches!(render_target(bad), Err(SurqlError::Validation { .. })),
            "{bad:?}"
        );
    }
}

#[test]
fn seconds_refuses_what_duration_cannot_hold() {
    assert_eq!(seconds(1.5).unwrap(), Duration::from_millis(1500));
    for bad in [f64::INFINITY, f64::NAN, 1e300, -1.0] {
        assert!(seconds(bad).is_err(), "{bad}");
    }
}

#[test]
fn surrealdb_error_maps_to_surql_error() {
    // In 3.x `surrealdb::Error` is a single struct with typed
    // variants exposed via predicate methods. Use the public
    // constructor helpers to synthesise representative cases and
    // assert they map onto the expected `SurqlError` variants.
    let thrown: SurqlError = surrealdb::Error::thrown("boom".into()).into();
    assert!(matches!(thrown, SurqlError::Query { .. }));

    let connection: SurqlError = surrealdb::Error::connection("down".into(), None).into();
    assert!(matches!(connection, SurqlError::Connection { .. }));

    let internal: SurqlError = surrealdb::Error::internal("boom".into()).into();
    assert!(matches!(internal, SurqlError::Database { .. }));
}

fn root_mem_config(ns: &str) -> ConnectionConfig {
    ConnectionConfig::builder()
        .url("mem://")
        .namespace(ns)
        .database(ns)
        .username("root")
        .password(test_password())
        .retry_max_attempts(1)
        .build()
        .unwrap()
}

/// A record access method whose sessions live one second, plus a
/// table only system users may read.
async fn define_member_access(client: &DatabaseClient) {
    client
        .query(
            "DEFINE ACCESS member ON DATABASE TYPE RECORD \
             SIGNUP (CREATE member SET name = $name) \
             SIGNIN (SELECT * FROM member WHERE name = $name) \
             DURATION FOR SESSION 1s; \
             DEFINE TABLE secret SCHEMALESS PERMISSIONS NONE; \
             CREATE secret SET v = 1;",
        )
        .await
        .unwrap();
}

/// Regression: the replay used to be a fixed per-client flag, so a
/// root-configured service that signed its shared session in as a
/// record user had that user's expired session silently replaced by
/// ROOT, which then ran the user's statement unfiltered. Whatever
/// the engine does with the expiry, the statement must never see a
/// row that `PERMISSIONS NONE` hides from the record user.
#[tokio::test]
async fn expired_record_session_never_replays_as_config_root() {
    let client = DatabaseClient::new(root_mem_config("escalate")).unwrap();
    client.connect().await.unwrap();
    define_member_access(&client).await;
    AuthManager::new()
        .signup(
            &client,
            &ScopeCredentials::new("escalate", "escalate", "member").with("name", "m"),
        )
        .await
        .unwrap();
    sleep(Duration::from_millis(2500)).await;
    if let Ok(rows) = client.query("SELECT * FROM secret;").await {
        assert_eq!(
            rows,
            serde_json::json!([[]]),
            "the record user's statement ran with root authority"
        );
    }
}

/// Regression: `connect` on a caller session signed it in with the
/// config credentials, handing the caller the service's authority.
#[tokio::test]
async fn connect_refuses_on_a_caller_session() {
    let root = DatabaseClient::new(root_mem_config("callerconnect")).unwrap();
    root.connect().await.unwrap();
    define_member_access(&root).await;
    let token = root
        .signup(
            &ScopeCredentials::new("callerconnect", "callerconnect", "member").with("name", "m"),
        )
        .await
        .unwrap();
    // The signup switched the shared session; put root back.
    root.connect().await.unwrap();
    let caller = root.caller_session(&token.token).await.unwrap();
    assert!(caller.connect().await.is_err());
    let seen = caller.query("SELECT * FROM secret;").await.unwrap();
    assert_eq!(seen, serde_json::json!([[]]));
}

/// Regression: the expiry check matched a substring of ANY error,
/// including a per-statement error that carries user data, and the
/// replay then re-ran every statement of the request.
#[tokio::test]
async fn statement_errors_never_trigger_a_replay() {
    let client = DatabaseClient::new(root_mem_config("noreplay")).unwrap();
    client.connect().await.unwrap();
    let err = client
        .query("CREATE counter SET n = 1; THROW 'The session has expired';")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("session has expired"), "{err}");
    let count = client
        .query("SELECT count() FROM counter GROUP ALL;")
        .await
        .unwrap();
    assert_eq!(count, serde_json::json!([[{ "count": 1 }]]));
}

/// The replay re-checks the authority under the identity lock: once a
/// signin on any clone has taken the session, a replay already on its
/// way declines instead of signing the config credentials back in over
/// the new identity.
#[tokio::test]
async fn replay_declines_once_the_session_changed_hands() {
    let service = DatabaseClient::new(root_mem_config("handover")).unwrap();
    service.connect().await.unwrap();
    assert!(
        service.replay_session().await.unwrap(),
        "config authority replays"
    );
    define_member_access(&service).await;
    service
        .clone()
        .signup(&ScopeCredentials::new("handover", "handover", "member").with("name", "m"))
        .await
        .unwrap();
    assert!(!service.replay_session().await.unwrap());
    let seen = service.query("SELECT * FROM secret;").await.unwrap();
    assert_eq!(seen, serde_json::json!([[]]), "still the member's session");
}

/// Regression: typed CRUD targets were spliced into the statement
/// verbatim, so a record key taken from user input ended the
/// statement and ran another.
#[tokio::test]
async fn crud_targets_cannot_inject_statements() {
    let client = DatabaseClient::new(root_mem_config("inject")).unwrap();
    client.connect().await.unwrap();
    client.query("CREATE user:a SET n = 1;").await.unwrap();
    let _ = client.select::<Value>("user:x; REMOVE TABLE user").await;
    let _ = client.delete::<Value>("user:x; REMOVE TABLE user").await;
    let rows: Vec<Value> = client.select("user").await.unwrap();
    assert_eq!(rows.len(), 1, "the injected REMOVE TABLE ran");
}

/// In a failed transaction the statements before the failure are "not
/// executed" too, with the engine's fixed sentence. When the failure is
/// itself a not-executed error with a cause (SurrealDB 3.3's refusal of a
/// `DEFINE INDEX` while document ids are reclaimed), that cause is what is
/// reported, not the first fixed sentence.
#[test]
fn the_not_executed_error_that_says_why_is_reported() {
    use super::convert::keep_not_executed;
    use surrealdb::types::QueryError;

    let not_executed =
        |message: &str| surrealdb::Error::query(message.to_owned(), QueryError::NotExecuted);
    let fixed = not_executed("The query was not executed due to a failed transaction");
    let cause = not_executed(
        "The shared document-ID space for table `t` is still being reclaimed; retry \
         DEFINE INDEX after cleanup completes",
    );
    let commit = not_executed("Cannot COMMIT: the transaction was aborted due to a prior error");

    let kept = [&fixed, &cause, &commit]
        .into_iter()
        .fold(None, keep_not_executed);
    let (says_why, err) = kept.expect("an error is kept");
    assert!(says_why);
    assert!(err.to_string().contains("still being reclaimed"), "{err}");

    let kept = [&fixed, &commit].into_iter().fold(None, keep_not_executed);
    let (says_why, err) = kept.expect("an error is kept");
    assert!(!says_why);
    assert!(err.to_string().contains("failed transaction"), "{err}");
}

/// SurrealDB 3.3 compacts an index in the background after writes to it,
/// and a write to the same index can lose a conflict to that pass. Rows are
/// written to a full-text index and deleted again at once, over and over,
/// each write a single statement: every conflict lost is sent again, and
/// every cycle completes.
#[tokio::test]
async fn a_lone_statement_that_lost_a_write_conflict_is_sent_again() {
    const ATTEMPTS: u32 = 10;
    let config = ConnectionConfig::builder()
        .url("mem://")
        .namespace("t")
        .database("t")
        .retry_max_attempts(ATTEMPTS)
        .retry_min_wait(0.1)
        .retry_max_wait(1.0)
        .build()
        .unwrap();
    let client = DatabaseClient::new(config).unwrap();
    client.connect().await.unwrap();
    client
        .query(
            "DEFINE ANALYZER words TOKENIZERS blank FILTERS lowercase;              DEFINE TABLE doc SCHEMALESS;              DEFINE INDEX doc_text ON doc FIELDS text FULLTEXT ANALYZER words BM25;",
        )
        .await
        .unwrap();
    for cycle in 0..20 {
        for row in 0..20 {
            client
                .query(&format!(
                    "UPSERT doc:c{cycle}r{row} SET title = 'routes', text = 'get the orders by id'"
                ))
                .await
                .unwrap();
        }
        client
            .query("DELETE doc WHERE title = 'routes'")
            .await
            .unwrap();
    }
    let left = client.query("SELECT * FROM doc").await.unwrap();
    assert_eq!(left, serde_json::json!([[]]));
}
