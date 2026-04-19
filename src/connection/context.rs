//! Task-scoped current-database context.
//!
//! Port of `surql/connection/context.py`. Python uses `contextvars` so the
//! current [`DatabaseClient`] automatically propagates down the async
//! call tree; the Rust equivalent is [`tokio::task_local!`], which gives
//! the same "logically-scoped global" with stricter ownership rules.
//!
//! The context holds an `Arc<DatabaseClient>` (not the owned client) so
//! overrides are cheap and the registry / caller always retains
//! ownership of the real handle.
//!
//! # Scoping rules
//!
//! * Outside any scope, [`get_db`] and [`has_db`] return
//!   [`SurqlError::Context`] / `false` respectively.
//! * [`connection_scope`] and [`connection_override`] push a new value
//!   for the duration of the wrapped future; callers inside that future
//!   (including spawned `tokio::spawn` tasks that inherit the task-local
//!   via `TaskLocalFuture`) will see the overridden value.
//! * [`set_db`] / [`clear_db`] mutate the **current** scope's slot. They
//!   fail outside a scope because `task_local!` values cannot be set
//!   without an enclosing `.scope(...)`.
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use surql::connection::{
//!     connection_scope, get_db, ConnectionConfig, DatabaseClient,
//! };
//!
//! # async fn run() -> surql::Result<()> {
//! let client = Arc::new(DatabaseClient::new(ConnectionConfig::default())?);
//! client.connect().await?;
//!
//! connection_scope(client.clone(), async {
//!     // Any code inside this future can fetch the scoped client.
//!     let db = get_db()?;
//!     db.query("RETURN 1;").await?;
//!     Ok::<_, surql::SurqlError>(())
//! })
//! .await?;
//! # Ok(()) }
//! ```

use std::cell::RefCell;
use std::future::Future;
use std::sync::Arc;

use crate::connection::client::DatabaseClient;
use crate::error::{Result, SurqlError};

tokio::task_local! {
    /// The currently-active database client for this async task.
    ///
    /// The outer [`RefCell`] lets [`set_db`] / [`clear_db`] mutate the
    /// slot in-place without requiring the caller to rebuild the whole
    /// `task_local` future. The inner [`Option`] is used because
    /// `set_db(None)` is a legitimate clear operation.
    static CURRENT_CLIENT: RefCell<Option<Arc<DatabaseClient>>>;
}

/// Return the current task-scoped database client.
///
/// # Errors
///
/// Returns [`SurqlError::Context`] when called outside a
/// [`connection_scope`] / [`connection_override`] block, or after an
/// explicit [`clear_db`] within that block.
pub fn get_db() -> Result<Arc<DatabaseClient>> {
    CURRENT_CLIENT
        .try_with(|slot| slot.borrow().clone())
        .map_err(|_| no_scope_error())?
        .ok_or_else(|| SurqlError::Context {
            reason: "no active database connection; use connection_scope() or set_db() first"
                .into(),
        })
}

/// Replace the current scope's client slot.
///
/// # Errors
///
/// Returns [`SurqlError::Context`] when called outside any scope
/// (`task_local!` slots cannot be set without an enclosing scope).
pub fn set_db(client: Arc<DatabaseClient>) -> Result<()> {
    CURRENT_CLIENT
        .try_with(|slot| {
            *slot.borrow_mut() = Some(client);
        })
        .map_err(|_| no_scope_error())
}

/// Clear the current scope's client slot.
///
/// # Errors
///
/// Returns [`SurqlError::Context`] when called outside any scope.
pub fn clear_db() -> Result<()> {
    CURRENT_CLIENT
        .try_with(|slot| {
            *slot.borrow_mut() = None;
        })
        .map_err(|_| no_scope_error())
}

/// Return `true` when a client is set in the current scope.
///
/// Returns `false` both when no scope exists and when the scope's slot
/// has been cleared.
pub fn has_db() -> bool {
    CURRENT_CLIENT
        .try_with(|slot| slot.borrow().is_some())
        .unwrap_or(false)
}

/// Run `fut` with `client` set as the current task-scoped database.
///
/// Mirrors py's `connection_scope(config)` asynccontextmanager but takes
/// a pre-built client rather than a config so the caller retains full
/// control over connection lifetime (matching the Rust "ownership is
/// explicit" idiom). Use [`crate::connection::ConnectionRegistry`] or
/// plain [`DatabaseClient::new`] + [`DatabaseClient::connect`] upstream.
///
/// On return, the previous scope's value (if any) is restored; on panic
/// inside `fut`, the `tokio::task_local!` machinery pops the frame the
/// same way.
pub async fn connection_scope<F, T>(client: Arc<DatabaseClient>, fut: F) -> T
where
    F: Future<Output = T>,
{
    CURRENT_CLIENT.scope(RefCell::new(Some(client)), fut).await
}

/// Run `fut` with `client` temporarily overriding the current scope's
/// database.
///
/// Identical to [`connection_scope`] at the task-local layer but named
/// separately for intent parity with py's `connection_override`. Inside
/// the wrapped future, [`get_db`] returns the override; when the future
/// resolves, the outer scope's value is restored automatically.
pub async fn connection_override<F, T>(client: Arc<DatabaseClient>, fut: F) -> T
where
    F: Future<Output = T>,
{
    connection_scope(client, fut).await
}

fn no_scope_error() -> SurqlError {
    SurqlError::Context {
        reason: "no active connection_scope on this task".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::ConnectionConfig;

    fn make_client() -> Arc<DatabaseClient> {
        Arc::new(DatabaseClient::new(ConnectionConfig::default()).expect("default config is valid"))
    }

    #[tokio::test]
    async fn get_db_outside_scope_errors() {
        let err = get_db().unwrap_err();
        assert!(matches!(err, SurqlError::Context { .. }));
        assert!(!has_db());
    }

    #[tokio::test]
    async fn set_db_outside_scope_errors() {
        let client = make_client();
        let err = set_db(client).unwrap_err();
        assert!(matches!(err, SurqlError::Context { .. }));
    }

    #[tokio::test]
    async fn clear_db_outside_scope_errors() {
        let err = clear_db().unwrap_err();
        assert!(matches!(err, SurqlError::Context { .. }));
    }

    #[tokio::test]
    async fn scope_sets_and_restores() {
        assert!(!has_db());
        let client = make_client();
        connection_scope(client.clone(), async {
            assert!(has_db());
            let got = get_db().expect("client in scope");
            assert!(Arc::ptr_eq(&got, &client));
        })
        .await;
        assert!(!has_db(), "scope must release the binding");
    }

    #[tokio::test]
    async fn override_swaps_inside_outer_scope() {
        let outer = make_client();
        let inner = make_client();
        connection_scope(outer.clone(), async {
            let got = get_db().unwrap();
            assert!(Arc::ptr_eq(&got, &outer));
            connection_override(inner.clone(), async {
                let got = get_db().unwrap();
                assert!(Arc::ptr_eq(&got, &inner));
            })
            .await;
            // Outer restored after the override completes.
            let got = get_db().unwrap();
            assert!(Arc::ptr_eq(&got, &outer));
        })
        .await;
    }

    #[tokio::test]
    async fn set_and_clear_inside_scope() {
        let first = make_client();
        let second = make_client();
        connection_scope(first.clone(), async {
            set_db(second.clone()).expect("set in scope");
            let got = get_db().unwrap();
            assert!(Arc::ptr_eq(&got, &second));
            clear_db().expect("clear in scope");
            assert!(!has_db());
            assert!(matches!(get_db().unwrap_err(), SurqlError::Context { .. }));
        })
        .await;
    }

    #[tokio::test]
    async fn scopes_are_isolated_across_tasks() {
        let a = make_client();
        let b = make_client();

        let task_a = {
            let a = a.clone();
            tokio::spawn(async move {
                connection_scope(a.clone(), async {
                    tokio::task::yield_now().await;
                    let got = get_db().unwrap();
                    assert!(Arc::ptr_eq(&got, &a));
                })
                .await;
            })
        };

        let task_b = {
            let b = b.clone();
            tokio::spawn(async move {
                connection_scope(b.clone(), async {
                    tokio::task::yield_now().await;
                    let got = get_db().unwrap();
                    assert!(Arc::ptr_eq(&got, &b));
                })
                .await;
            })
        };

        task_a.await.unwrap();
        task_b.await.unwrap();

        // Nothing leaked back into the parent.
        assert!(!has_db());
    }
}
