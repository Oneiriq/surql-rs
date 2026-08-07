//! Live-query behaviour against the in-process `mem://` engine.
//!
//! Two properties matter to any consumer and neither is visible from the
//! statement text: the `WHERE` clause is evaluated by the engine before a
//! notification is delivered, and a subscription keeps working after the
//! client handle that opened it goes out of scope.

#![cfg(any(feature = "client", feature = "client-rustls"))]

use std::time::Duration;

use futures::StreamExt as _;
use serde_json::Value;

use surql::connection::{ConnectionConfig, DatabaseClient, LiveQuery};
use surql::types::operators::eq;

async fn memory_client(namespace: &str) -> DatabaseClient {
    let cfg = ConnectionConfig::builder()
        .url("mem://")
        .namespace(namespace)
        .database(namespace)
        .build()
        .expect("valid mem config");
    let client = DatabaseClient::new(cfg).expect("client constructs");
    client.connect().await.expect("connect to embedded engine");
    // A live query needs its table to exist first.
    client
        .inner()
        .query("DEFINE TABLE note SCHEMALESS;")
        .await
        .expect("table defines");
    client
}

async fn create(client: &DatabaseClient, tenant: &str) {
    client
        .inner()
        .query(format!("CREATE note SET tenant = '{tenant}';"))
        .await
        .expect("create succeeds");
}

/// The next row the feed delivers, or `None` if nothing arrives in time.
async fn next_within(
    feed: &mut LiveQuery<Value>,
    seconds: u64,
) -> Option<surrealdb::Notification<Value>> {
    tokio::time::timeout(Duration::from_secs(seconds), feed.next())
        .await
        .ok()
        .flatten()
        .map(|item| item.expect("notification decodes"))
}

#[tokio::test]
async fn the_engine_applies_the_where_clause_before_delivering() {
    let client = memory_client("it_live_where").await;
    let mut feed: LiveQuery<Value> =
        LiveQuery::start_where(&client, "note", [eq("tenant", "acme")])
            .await
            .expect("live query starts");

    create(&client, "rival").await;
    create(&client, "acme").await;

    let notification = next_within(&mut feed, 10)
        .await
        .expect("the acme row arrives");
    assert_eq!(notification.data["tenant"], "acme");

    // The rival row was written first and is not in this feed at all.
    assert!(
        next_within(&mut feed, 1).await.is_none(),
        "only the matching row is delivered",
    );
}

#[tokio::test]
async fn a_subscription_outlives_the_handle_that_opened_it() {
    let client = memory_client("it_live_scope").await;

    let mut feed: LiveQuery<Value> = {
        let scoped = client.clone();
        LiveQuery::start_where(&scoped, "note", [eq("tenant", "acme")])
            .await
            .expect("live query starts")
        // `scoped` drops here. A caller that opened the subscription
        // from a short-lived clone (a request handler's copy of shared
        // state, say) must still receive rows.
    };

    create(&client, "acme").await;

    let notification = next_within(&mut feed, 10)
        .await
        .expect("the row arrives after the opening handle is gone");
    assert_eq!(notification.data["tenant"], "acme");
}
