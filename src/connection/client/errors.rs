//! Mapping [`surrealdb::Error`] onto [`SurqlError`], and reading the few
//! engine errors the client reacts to.

use surrealdb::types::{AuthError, ConnectionError, NotAllowedError, QueryError};

use crate::error::SurqlError;

impl From<surrealdb::Error> for SurqlError {
    fn from(err: surrealdb::Error) -> Self {
        // 3.x unifies `Error` into a single struct with a `kind_str()`
        // discriminator and a human-readable message. Map the relevant
        // kinds onto the richer `SurqlError` taxonomy; fall back to a
        // substring match on the message for anything not yet modelled
        // in the typed details.
        classify_surrealdb_error(&err, err.to_string())
    }
}

fn classify_surrealdb_error(err: &surrealdb::Error, msg: String) -> SurqlError {
    if err.is_connection() {
        return SurqlError::Connection { reason: msg };
    }
    if err.is_query() || err.is_not_found() || err.is_not_allowed() || err.is_thrown() {
        return SurqlError::Query { reason: msg };
    }
    if err.is_serialization() {
        return SurqlError::Serialization { reason: msg };
    }
    let lowered = msg.to_lowercase();
    if lowered.contains("transaction") {
        return SurqlError::Transaction { reason: msg };
    }
    if lowered.contains("connect")
        || lowered.contains("not connected")
        || lowered.contains("websocket")
        || lowered.contains("timed out")
        || lowered.contains("subprotocol")
    {
        return SurqlError::Connection { reason: msg };
    }
    SurqlError::Database { reason: msg }
}

pub(super) fn connection_err(err: &surrealdb::Error) -> SurqlError {
    SurqlError::Connection {
        reason: err.to_string(),
    }
}

/// The SDK's rejection of a second `connect` on an already-connected handle.
/// Read from the structured details, with a message match for SDK paths
/// that report it without them.
pub(super) fn sdk_says_already_connected(err: &surrealdb::Error) -> bool {
    matches!(
        err.connection_details(),
        Some(ConnectionError::AlreadyConnected)
    ) || err.to_string().to_lowercase().contains("already connected")
}

/// The engine's refusal of a whole request because its authenticated
/// session has expired.
///
/// Only ever asked of the error a request as a whole failed with, never of
/// a per-statement result: a statement error carries user data (a `THROW`
/// message, the value a failed `ASSERT` rejected), so reading one as expiry
/// would let a stored value trigger a replay. The engine reports expiry as
/// a structured not-allowed error; the fallback, for a peer that sends the
/// kind without the details, is an exact match on the engine's fixed
/// message, never a substring.
pub(super) fn request_says_session_expired(err: &surrealdb::Error) -> bool {
    let structured = matches!(
        err.not_allowed_details(),
        Some(NotAllowedError::Auth(AuthError::SessionExpired))
    );
    let fixed_message = (err.is_not_allowed() || err.is_internal())
        && err
            .message()
            .trim()
            .eq_ignore_ascii_case("the session has expired");
    structured || fixed_message
}

/// A statement the engine refused because its transaction lost a write
/// conflict to another one. A conflicted transaction committed nothing, so
/// the statement can be sent again. SurrealDB 3.3 reports these on
/// ordinary single-statement writes under its optimistic engines (an
/// in-place replacement of indexed rows conflicted in two of five runs).
/// Read from the structured details, with a match on the engine's fixed
/// wording for a peer that sends the text without them.
pub(super) fn is_retryable_conflict(err: &surrealdb::Error) -> bool {
    if let Some(details) = err.query_details() {
        return matches!(details, QueryError::TransactionConflict);
    }
    // A statement's own `THROW` carries user text, never the engine's.
    if err.is_thrown() {
        return false;
    }
    let message = err
        .message()
        .trim()
        .trim_end_matches('.')
        .to_ascii_lowercase();
    message.contains("transaction conflict") && message.ends_with("this transaction can be retried")
}

/// A statement the engine skipped because another statement of the same
/// transaction failed. It carries no cause of its own, so it must never be
/// the error a caller sees when the failing statement's error is available.
/// Read from the structured details, with an exact match on the engine's
/// fixed message for a peer that sends the text without them.
pub(super) fn statement_was_not_executed(err: &surrealdb::Error) -> bool {
    matches!(err.query_details(), Some(QueryError::NotExecuted))
        || err
            .message()
            .trim()
            .to_ascii_lowercase()
            .starts_with("the query was not executed due to a failed transaction")
}

/// The engine's fixed not-executed messages: each says only that another
/// statement failed.
const UNINFORMATIVE_NOT_EXECUTED: [&str; 3] = [
    "the query was not executed due to a failed transaction",
    "the query was not executed due to a cancelled transaction",
    "cannot commit: the transaction was aborted due to a prior error",
];

/// `true` for a not-executed error that says why: one whose message is not
/// a fixed "another statement failed" sentence. SurrealDB 3.3 refuses a
/// `DEFINE INDEX` while the table's document ids are being reclaimed with
/// such an error, and in a transaction it sits among the fixed ones.
pub(super) fn not_executed_says_why(err: &surrealdb::Error) -> bool {
    let message = err
        .message()
        .trim()
        .trim_end_matches('.')
        .to_ascii_lowercase();
    !UNINFORMATIVE_NOT_EXECUTED.contains(&message.as_str())
}

pub(super) fn query_err(err: &surrealdb::Error) -> SurqlError {
    classify_surrealdb_error(err, err.to_string())
}
