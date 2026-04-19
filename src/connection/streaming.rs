//! Live-query streaming.
//!
//! Port of `surql/connection/streaming.py`. Wraps the `surrealdb` SDK's
//! `LIVE SELECT` stream so callers get a plain [`futures::Stream`] of
//! deserialised notifications, and provides a [`StreamingManager`] that
//! owns the lifecycle of many concurrent live queries.
//!
//! Live queries require a WebSocket (`ws://` / `wss://`) or embedded
//! (`mem://`, `file://`, `surrealkv://`) connection. HTTP-mode clients
//! will get a [`SurqlError::Streaming`] at [`LiveQuery::start`] time.
//!
//! The underlying SDK stream sends `KILL` on drop, so dropping the
//! [`LiveQuery`] automatically releases the server-side subscription.
//! [`StreamingManager`] carries this guarantee across its whole pool: on
//! drop it kills every spawned task, and each task drops its
//! [`LiveQuery`] in turn.
//!
//! The 3.x SDK requires the notification payload type to implement
//! `surrealdb::types::SurrealValue`. The blanket impl for
//! `serde_json::Value` covers the common "untyped" case; users
//! wanting typed payloads must derive `SurrealValue` on their data
//! struct.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{Stream, StreamExt};
use surrealdb::method::QueryStream;
use surrealdb::types::SurrealValue;
use surrealdb::Notification;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use ulid::Ulid;

use crate::connection::client::DatabaseClient;
use crate::error::{Result, SurqlError};

/// A live-query subscription.
///
/// Iterate by polling the [`Stream`] impl:
///
/// ```no_run
/// use futures::StreamExt;
/// use serde_json::Value;
/// use surql::connection::{ConnectionConfig, DatabaseClient, LiveQuery};
///
/// # async fn run() -> surql::Result<()> {
/// let client = DatabaseClient::new(ConnectionConfig::default())?;
/// client.connect().await?;
/// // `serde_json::Value` implements `SurrealValue` out of the box,
/// // so it works as the payload type without any derive. For typed
/// // payloads, derive `surrealdb::types::SurrealValue` on your
/// // struct.
/// let mut live: LiveQuery<Value> = LiveQuery::start(&client, "user").await?;
/// while let Some(notification) = live.next().await {
///     let n = notification?;
///     println!("change: {:?}", n);
/// }
/// # Ok(()) }
/// ```
pub struct LiveQuery<T> {
    stream: QueryStream<Notification<T>>,
    _marker: PhantomData<T>,
}

impl<T> std::fmt::Debug for LiveQuery<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveQuery").finish_non_exhaustive()
    }
}

impl<T> LiveQuery<T>
where
    T: SurrealValue + Unpin + 'static,
{
    /// Start a `LIVE SELECT * FROM <target>` subscription.
    ///
    /// Fails with [`SurqlError::Streaming`] if the client's protocol
    /// does not support live queries (i.e. `http://` or `https://`).
    pub async fn start(client: &DatabaseClient, target: &str) -> Result<Self> {
        let proto = client.config().protocol()?;
        if !proto.supports_live_queries() {
            return Err(SurqlError::Streaming {
                reason: format!("live queries are not supported over {proto}"),
            });
        }

        let surql = format!("LIVE SELECT * FROM {target};");
        let mut response = client
            .inner()
            .query(surql)
            .await
            .map_err(|e| streaming_err(&e))?;
        let stream: QueryStream<Notification<T>> =
            response.stream(0).map_err(|e| streaming_err(&e))?;
        Ok(Self {
            stream,
            _marker: PhantomData,
        })
    }
}

impl<T> Stream for LiveQuery<T>
where
    T: SurrealValue + Unpin + 'static,
{
    type Item = Result<Notification<T>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Safety: we never move `stream` out of `self`; we just project to it.
        let this = self.get_mut();
        Pin::new(&mut this.stream)
            .poll_next(cx)
            .map(|opt| opt.map(|res| res.map_err(|e| streaming_err(&e))))
    }
}

fn streaming_err(err: &surrealdb::Error) -> SurqlError {
    SurqlError::Streaming {
        reason: err.to_string(),
    }
}

/// Unique handle for a subscription owned by [`StreamingManager`].
///
/// Returned by [`StreamingManager::spawn`]; pass back to
/// [`StreamingManager::kill`] to shut a subscription down early.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionId(Ulid);

impl SubscriptionId {
    fn new() -> Self {
        Self(Ulid::new())
    }

    /// String representation (ULID).
    pub fn as_str(self) -> String {
        self.0.to_string()
    }
}

impl std::fmt::Display for SubscriptionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Pool of live-query subscriptions with shared lifecycle.
///
/// Each [`StreamingManager::spawn`] call:
///
/// 1. Starts a new `LIVE SELECT` against `target`.
/// 2. Spawns a tokio task that polls the stream and dispatches every
///    notification to the supplied callback (sync or `async` via
///    `async move` closure).
/// 3. Stores the task's [`JoinHandle`] against a fresh
///    [`SubscriptionId`].
///
/// Dropping the manager aborts every spawned task; the
/// [`LiveQuery`] stored inside each task is dropped as part of the
/// abort, which issues `KILL` on the server.
///
/// # Example
///
/// ```no_run
/// use serde_json::Value;
/// use std::sync::Arc;
/// use surql::connection::{ConnectionConfig, DatabaseClient, StreamingManager};
///
/// # async fn run() -> surql::Result<()> {
/// let client = Arc::new(DatabaseClient::new(ConnectionConfig::default())?);
/// client.connect().await?;
/// let manager = StreamingManager::new();
/// let id = manager
///     .spawn::<Value, _>(&client, "user", |n| {
///         println!("change: {:?}", n.action);
///     })
///     .await?;
/// // ... do work ...
/// manager.kill(id).await;
/// # Ok(()) }
/// ```
pub struct StreamingManager {
    inner: Arc<StreamingManagerInner>,
}

impl std::fmt::Debug for StreamingManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingManager").finish_non_exhaustive()
    }
}

impl Default for StreamingManager {
    fn default() -> Self {
        Self::new()
    }
}

struct StreamingManagerInner {
    tasks: Mutex<HashMap<SubscriptionId, JoinHandle<()>>>,
}

impl StreamingManager {
    /// Construct an empty manager.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(StreamingManagerInner {
                tasks: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Start a live query and spawn a task that pipes its notifications
    /// to `callback`.
    ///
    /// The callback runs inside the spawned task on the current tokio
    /// runtime. Panics in the callback are caught by the runtime (and
    /// will abort that single subscription); error notifications from
    /// the SDK are logged via [`tracing::error!`] and swallowed so one
    /// decode failure does not tear down the whole pipe.
    ///
    /// # Errors
    ///
    /// Propagates any [`LiveQuery::start`] error (invalid protocol,
    /// query failure, etc.).
    pub async fn spawn<T, F>(
        &self,
        client: &DatabaseClient,
        target: &str,
        mut callback: F,
    ) -> Result<SubscriptionId>
    where
        T: SurrealValue + Unpin + Send + 'static,
        F: FnMut(Notification<T>) + Send + 'static,
    {
        let mut live: LiveQuery<T> = LiveQuery::start(client, target).await?;
        let id = SubscriptionId::new();
        let handle = tokio::spawn(async move {
            while let Some(item) = live.next().await {
                match item {
                    Ok(n) => callback(n),
                    Err(err) => {
                        tracing::error!(
                            target = "surql::connection::streaming",
                            "live query error: {err}"
                        );
                    }
                }
            }
        });

        self.inner.tasks.lock().await.insert(id, handle);
        Ok(id)
    }

    /// Kill a single subscription by id.
    ///
    /// Returns `true` when a matching subscription was found; `false`
    /// otherwise (unknown id or already-drained).
    pub async fn kill(&self, id: SubscriptionId) -> bool {
        if let Some(handle) = self.inner.tasks.lock().await.remove(&id) {
            handle.abort();
            // Wait for the abort to settle so the SDK's KILL flush
            // happens before we return; ignore the JoinError (AbortError
            // variant is expected).
            let _ = handle.await;
            true
        } else {
            false
        }
    }

    /// Number of live subscriptions currently managed.
    pub async fn count(&self) -> usize {
        self.inner.tasks.lock().await.len()
    }

    /// Return the set of known subscription ids (snapshot).
    pub async fn ids(&self) -> Vec<SubscriptionId> {
        self.inner.tasks.lock().await.keys().copied().collect()
    }

    /// Abort every managed subscription and clear the pool.
    pub async fn drain_all(&self) {
        let handles: Vec<JoinHandle<()>> = {
            let mut tasks = self.inner.tasks.lock().await;
            tasks.drain().map(|(_, h)| h).collect()
        };
        for h in handles {
            h.abort();
            let _ = h.await;
        }
    }
}

impl Drop for StreamingManager {
    fn drop(&mut self) {
        // Best-effort: abort every managed task synchronously. The
        // `LiveQuery` inside each task is dropped as part of the abort,
        // which sends `KILL` to the server.
        if let Ok(mut tasks) = self.inner.tasks.try_lock() {
            for (_, handle) in tasks.drain() {
                handle.abort();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::config::ConnectionConfig;

    #[tokio::test]
    async fn live_rejects_http_protocol() {
        let cfg = ConnectionConfig::builder()
            .url("http://localhost:8000")
            .enable_live_queries(false)
            .build()
            .unwrap();
        let client = DatabaseClient::new(cfg).unwrap();
        // Even though the client isn't connected, `start` should fail
        // early on protocol validation.
        let err = LiveQuery::<serde_json::Value>::start(&client, "user")
            .await
            .unwrap_err();
        assert!(matches!(err, SurqlError::Streaming { .. }));
    }

    #[tokio::test]
    async fn manager_starts_empty() {
        let m = StreamingManager::new();
        assert_eq!(m.count().await, 0);
        assert!(m.ids().await.is_empty());
        assert!(!m.kill(SubscriptionId::new()).await);
    }

    #[tokio::test]
    async fn spawn_surfaces_live_query_errors() {
        let cfg = ConnectionConfig::builder()
            .url("http://localhost:8000")
            .enable_live_queries(false)
            .build()
            .unwrap();
        let client = DatabaseClient::new(cfg).unwrap();
        let m = StreamingManager::new();
        let err = m
            .spawn::<serde_json::Value, _>(&client, "user", |_| {})
            .await
            .unwrap_err();
        assert!(matches!(err, SurqlError::Streaming { .. }));
        assert_eq!(m.count().await, 0);
    }

    #[tokio::test]
    async fn drain_all_empties_pool() {
        let m = StreamingManager::new();
        m.drain_all().await;
        assert_eq!(m.count().await, 0);
    }

    #[test]
    fn subscription_id_is_unique() {
        let a = SubscriptionId::new();
        let b = SubscriptionId::new();
        assert_ne!(a, b);
        assert!(!a.to_string().is_empty());
    }
}
