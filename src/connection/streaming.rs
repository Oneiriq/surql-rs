//! Live-query streaming.
//!
//! Port of `surql/connection/streaming.py` (MVP). Wraps the
//! `surrealdb` SDK's `LIVE SELECT` stream so callers get a plain
//! [`futures::Stream`] of deserialized notifications.
//!
//! Live queries require a WebSocket (`ws://` / `wss://`) or embedded
//! (`mem://`, `file://`, `surrealkv://`) connection. HTTP-mode clients
//! will get a [`SurqlError::Streaming`] at [`LiveQuery::start`] time.
//!
//! The underlying SDK stream sends `KILL` on drop, so dropping the
//! [`LiveQuery`] automatically releases the server-side subscription.
//!
//! The 3.x SDK requires the notification payload type to implement
//! `surrealdb::types::SurrealValue`. The blanket impl for
//! `serde_json::Value` covers the common "untyped" case; users
//! wanting typed payloads must derive `SurrealValue` on their data
//! struct.

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use surrealdb::method::QueryStream;
use surrealdb::types::SurrealValue;
use surrealdb::Notification;

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
}
