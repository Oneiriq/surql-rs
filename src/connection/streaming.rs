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

use crate::connection::client::{render_target, DatabaseClient};
use crate::error::{Result, SurqlError};
use crate::query::builder::{Condition, WhereCondition};

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
    /// The handle that ISSUED the `LIVE SELECT`, held for the stream's
    /// whole life.
    ///
    /// `Surreal::clone` gives each clone its own session id, and its
    /// `Drop` ends that session; a live query belongs to the session
    /// that started it. So a subscription opened through a borrowed
    /// handle goes quiet the moment the caller's clone drops, silently,
    /// with no error and no closed stream to explain it. Owning the
    /// exact handle that ran the statement is what keeps the session,
    /// and therefore the subscription, alive. A clone made afterwards
    /// would be a DIFFERENT session and would not help.
    _client: DatabaseClient,
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
    /// does not support live queries (i.e. `http://` or `https://`), and
    /// with [`SurqlError::Validation`] when `target` is not a table name
    /// or `table:id` record id (the rules on [`DatabaseClient::select`]).
    pub async fn start(client: &DatabaseClient, target: &str) -> Result<Self> {
        Self::start_where(client, target, Vec::<Condition>::new()).await
    }

    /// Start a `LIVE SELECT * FROM <target> WHERE ...` subscription.
    ///
    /// Conditions are wrapped in parentheses and joined with `AND`,
    /// matching [`Query`](crate::query::Query). The engine evaluates
    /// them per notification, so a subscriber sees only the rows it
    /// asked for; without this, every consumer of a shared table
    /// receives every row and has to discard the rest in application
    /// code, which is where tenant scoping goes wrong.
    ///
    /// ```no_run
    /// use serde_json::Value;
    /// use surql::connection::{ConnectionConfig, DatabaseClient, LiveQuery};
    /// use surql::types::operators::eq;
    ///
    /// # async fn run() -> surql::Result<()> {
    /// # let client = DatabaseClient::new(ConnectionConfig::default())?;
    /// let live: LiveQuery<Value> =
    ///     LiveQuery::start_where(&client, "file_event", [eq("tenant_id", "acme")]).await?;
    /// # Ok(()) }
    /// ```
    pub async fn start_where<C, I>(
        client: &DatabaseClient,
        target: &str,
        conditions: I,
    ) -> Result<Self>
    where
        C: WhereCondition,
        I: IntoIterator<Item = C>,
    {
        let proto = client.config().protocol()?;
        if !proto.supports_live_queries() {
            return Err(SurqlError::Streaming {
                reason: format!("live queries are not supported over {proto}"),
            });
        }

        let surql = render_live_select(target, conditions)?;
        // Clone BEFORE issuing the statement, and issue it through the
        // clone this struct keeps: the subscription belongs to whichever
        // session ran it.
        let owned = client.clone();
        let mut response = owned
            .inner()
            .query(surql)
            .await
            .map_err(|e| streaming_err(&e))?;
        let stream: QueryStream<Notification<T>> =
            response.stream(0).map_err(|e| streaming_err(&e))?;
        Ok(Self {
            stream,
            _client: owned,
            _marker: PhantomData,
        })
    }
}

/// Render the statement, split out so its shape is testable without a
/// connection. The target is a table name or record id, validated and
/// quoted like the typed CRUD targets (see [`DatabaseClient::select`]).
fn render_live_select<C, I>(target: &str, conditions: I) -> Result<String>
where
    C: WhereCondition,
    I: IntoIterator<Item = C>,
{
    let target = render_target(target)?;
    let clauses: Vec<String> = conditions
        .into_iter()
        .map(|c| format!("({})", c.to_condition()))
        .collect();
    Ok(if clauses.is_empty() {
        format!("LIVE SELECT * FROM {target};")
    } else {
        format!(
            "LIVE SELECT * FROM {target} WHERE {};",
            clauses.join(" AND ")
        )
    })
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
        Self(Ulid::generate())
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
///    notification to the supplied (synchronous) callback.
/// 3. Stores the task's [`JoinHandle`] against a fresh
///    [`SubscriptionId`], until the subscription is killed or its
///    stream ends.
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
        callback: F,
    ) -> Result<SubscriptionId>
    where
        T: SurrealValue + Unpin + Send + 'static,
        F: FnMut(Notification<T>) + Send + 'static,
    {
        self.spawn_where::<T, F, Condition, _>(client, target, Vec::new(), callback)
            .await
    }

    /// Start a filtered live query and spawn a task that pipes its
    /// notifications to `callback`. Conditions carry the semantics
    /// [`LiveQuery::start_where`] documents.
    pub async fn spawn_where<T, F, C, I>(
        &self,
        client: &DatabaseClient,
        target: &str,
        conditions: I,
        mut callback: F,
    ) -> Result<SubscriptionId>
    where
        T: SurrealValue + Unpin + Send + 'static,
        F: FnMut(Notification<T>) + Send + 'static,
        C: WhereCondition,
        I: IntoIterator<Item = C>,
    {
        let mut live: LiveQuery<T> = LiveQuery::start_where(client, target, conditions).await?;
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

        let mut tasks = self.inner.tasks.lock().await;
        prune_ended(&mut tasks);
        tasks.insert(id, handle);
        Ok(id)
    }

    /// Kill a single subscription by id.
    ///
    /// Returns `true` when a matching subscription was still running and
    /// has been stopped; `false` otherwise (unknown id, already killed or
    /// drained, or a subscription whose stream had already ended).
    pub async fn kill(&self, id: SubscriptionId) -> bool {
        let handle = self.inner.tasks.lock().await.remove(&id);
        match handle {
            Some(handle) if !handle.is_finished() => {
                handle.abort();
                // Wait for the abort to settle so the SDK's KILL flush
                // happens before we return; ignore the JoinError
                // (AbortError variant is expected).
                let _ = handle.await;
                true
            }
            _ => false,
        }
    }

    /// Number of live subscriptions currently managed. A subscription
    /// whose stream has ended (the server closed it, or the callback
    /// panicked) is no longer counted.
    pub async fn count(&self) -> usize {
        let mut tasks = self.inner.tasks.lock().await;
        prune_ended(&mut tasks);
        tasks.len()
    }

    /// Return the ids of the live subscriptions (snapshot).
    pub async fn ids(&self) -> Vec<SubscriptionId> {
        let mut tasks = self.inner.tasks.lock().await;
        prune_ended(&mut tasks);
        tasks.keys().copied().collect()
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

/// Forget the subscriptions whose task has finished: their stream ended,
/// so there is nothing left to count or kill.
fn prune_ended(tasks: &mut HashMap<SubscriptionId, JoinHandle<()>>) {
    tasks.retain(|_, handle| !handle.is_finished());
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
        assert_eq!(
            m.ids().await,
            [] as [crate::connection::streaming::SubscriptionId; 0]
        );
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

    /// The handle of a task that has already run to completion, standing
    /// in for a subscription whose stream ended.
    async fn ended_task() -> JoinHandle<()> {
        let handle = tokio::spawn(async {});
        while !handle.is_finished() {
            tokio::task::yield_now().await;
        }
        handle
    }

    /// Regression: a subscription whose stream had ended stayed in the
    /// pool, so `count` kept counting it and `kill` reported stopping it.
    #[tokio::test]
    async fn ended_subscriptions_are_neither_counted_nor_killed() {
        let m = StreamingManager::new();
        let a = SubscriptionId::new();
        m.inner.tasks.lock().await.insert(a, ended_task().await);
        assert!(!m.kill(a).await, "an ended subscription is not killed");

        let b = SubscriptionId::new();
        m.inner.tasks.lock().await.insert(b, ended_task().await);
        assert_eq!(m.count().await, 0);
        assert_eq!(
            m.ids().await,
            [] as [crate::connection::streaming::SubscriptionId; 0]
        );
    }

    #[tokio::test]
    async fn drain_all_empties_pool() {
        let m = StreamingManager::new();
        m.drain_all().await;
        assert_eq!(m.count().await, 0);
    }

    #[test]
    fn live_select_renders_its_where_clause() {
        use crate::types::operators::eq;

        assert_eq!(
            render_live_select("file_event", Vec::<Condition>::new()).unwrap(),
            "LIVE SELECT * FROM file_event;",
        );
        assert_eq!(
            render_live_select("file_event", [eq("tenant_id", "acme")]).unwrap(),
            "LIVE SELECT * FROM file_event WHERE (tenant_id = 'acme');",
        );
        // Parenthesised and AND-joined, matching Query.
        assert_eq!(
            render_live_select(
                "file_event",
                [
                    Condition::from(eq("tenant_id", "acme")),
                    Condition::from("dispatched = false"),
                ],
            )
            .unwrap(),
            "LIVE SELECT * FROM file_event WHERE (tenant_id = 'acme') AND (dispatched = false);",
        );
    }

    /// Regression: the target was spliced in verbatim, so a table name
    /// taken from input could close the `LIVE SELECT` and run another
    /// statement.
    #[test]
    fn live_select_target_cannot_inject_statements() {
        let err = render_live_select("user; REMOVE TABLE user", Vec::<Condition>::new());
        assert!(matches!(err, Err(SurqlError::Validation { .. })));
        assert_eq!(
            render_live_select("user:x; REMOVE TABLE user", Vec::<Condition>::new()).unwrap(),
            "LIVE SELECT * FROM user:⟨x; REMOVE TABLE user⟩;",
        );
    }

    #[test]
    fn subscription_id_is_unique() {
        let a = SubscriptionId::new();
        let b = SubscriptionId::new();
        assert_ne!(a, b);
        assert_ne!(a.to_string(), "");
    }
}
