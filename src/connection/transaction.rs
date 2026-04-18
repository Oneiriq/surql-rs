//! Database transactions.
//!
//! Port of `surql/connection/transaction.py`, adapted to the Rust SDK's
//! request model. The `surrealdb` crate (2.x) does **not** stream
//! individual `BEGIN` / `COMMIT` / `CANCEL` statements across separate
//! `query()` calls: each `query()` is an isolated request, and the
//! server rejects a bare `COMMIT`. Instead, every statement issued via
//! [`Transaction::execute`] is buffered client-side and flushed as a
//! single atomic `BEGIN … COMMIT` query when [`Transaction::commit`] is
//! called. [`Transaction::rollback`] simply drops the buffered
//! statements without contacting the server.
//!
//! SurrealDB does **not** support nested transactions; begin one at a
//! time.

use serde_json::Value;

use crate::connection::client::DatabaseClient;
use crate::error::{Result, SurqlError};

/// Lifecycle state of a [`Transaction`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// Transaction is open and accepting statements.
    Active,
    /// `COMMIT TRANSACTION` succeeded (all statements applied).
    Committed,
    /// Rolled back client-side; no statements were sent.
    RolledBack,
    /// A terminal operation failed; the transaction is no longer usable.
    Failed,
}

/// A SurrealDB transaction.
///
/// Create one with [`Transaction::begin`]. Buffered statements are
/// flushed atomically by [`Transaction::commit`]; [`Transaction::rollback`]
/// discards them.
#[derive(Debug)]
pub struct Transaction<'a> {
    client: &'a DatabaseClient,
    statements: Vec<String>,
    state: TransactionState,
}

impl<'a> Transaction<'a> {
    /// Begin a new transaction bound to `client`.
    #[allow(clippy::unused_async)]
    pub async fn begin(client: &'a DatabaseClient) -> Result<Transaction<'a>> {
        // Surface an early error if the client is not connected, so the
        // caller learns about it before issuing `execute` calls.
        if !client.is_connected() {
            return Err(SurqlError::Transaction {
                reason: "cannot begin transaction: client is not connected".into(),
            });
        }
        Ok(Self {
            client,
            statements: Vec::new(),
            state: TransactionState::Active,
        })
    }

    /// Current lifecycle state.
    pub fn state(&self) -> TransactionState {
        self.state
    }

    /// `true` if the transaction has not yet been committed or rolled back.
    pub fn is_active(&self) -> bool {
        self.state == TransactionState::Active
    }

    /// Queue a statement for execution inside the transaction.
    ///
    /// The statement is **not** executed until [`Transaction::commit`]
    /// is called. Returns [`serde_json::Value::Null`] on success; the
    /// actual result becomes available in `commit`'s response.
    #[allow(clippy::unused_async)]
    pub async fn execute(&mut self, surql: &str) -> Result<Value> {
        if !self.is_active() {
            return Err(SurqlError::Transaction {
                reason: format!("transaction is not active (state = {:?})", self.state),
            });
        }
        // Normalise the trailing semicolon so we can concatenate cleanly.
        let trimmed = surql.trim().trim_end_matches(';').to_owned();
        self.statements.push(trimmed);
        Ok(Value::Null)
    }

    /// Commit the transaction.
    ///
    /// Flushes all queued statements as a single
    /// `BEGIN TRANSACTION; …; COMMIT TRANSACTION;` request. Returns the
    /// array of per-statement results (minus the bookend
    /// `BEGIN` / `COMMIT` entries).
    pub async fn commit(mut self) -> Result<Value> {
        if !self.is_active() {
            return Err(SurqlError::Transaction {
                reason: format!("cannot commit in state {:?}", self.state),
            });
        }
        let mut surql = String::from("BEGIN TRANSACTION;\n");
        for stmt in &self.statements {
            surql.push_str(stmt);
            surql.push_str(";\n");
        }
        surql.push_str("COMMIT TRANSACTION;\n");

        match self.client.query(&surql).await {
            Ok(results) => {
                self.state = TransactionState::Committed;
                Ok(results)
            }
            Err(err) => {
                self.state = TransactionState::Failed;
                Err(SurqlError::Transaction {
                    reason: format!("commit failed: {err}"),
                })
            }
        }
    }

    /// Roll the transaction back without contacting the server.
    ///
    /// Since queued statements are buffered client-side until commit,
    /// there is nothing to undo server-side; this simply discards the
    /// buffer and marks the transaction as terminated.
    #[allow(clippy::unused_async)]
    pub async fn rollback(mut self) -> Result<()> {
        if !self.is_active() {
            return Err(SurqlError::Transaction {
                reason: format!("cannot rollback in state {:?}", self.state),
            });
        }
        self.statements.clear();
        self.state = TransactionState::RolledBack;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::config::ConnectionConfig;

    #[tokio::test]
    async fn begin_requires_connected_client() {
        let client = DatabaseClient::new(ConnectionConfig::default()).unwrap();
        let err = Transaction::begin(&client).await.unwrap_err();
        assert!(matches!(err, SurqlError::Transaction { .. }));
    }

    #[test]
    fn state_variants_are_distinct() {
        assert_ne!(TransactionState::Active, TransactionState::Committed);
        assert_ne!(TransactionState::RolledBack, TransactionState::Failed);
    }
}
