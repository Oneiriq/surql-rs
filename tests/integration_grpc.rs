//! The gRPC transport (`grpc://`, SurrealDB 3.3+) against a live server.
//!
//! Gated on the `client-grpc` feature and the `SURREAL_GRPC_URL` env var,
//! so `cargo test` stays green without a server:
//!
//! ```text
//! docker run -d -p 8000:8000 surrealdb/surrealdb:v3.3.0 start --user root --pass root memory
//! SURREAL_GRPC_URL=grpc://localhost:8000 SURREAL_USER=root SURREAL_PASS=root \
//!   cargo test --features client-grpc --test integration_grpc
//! ```

#![cfg(feature = "client-grpc")]

use std::env;

use serde_json::json;
use surql::connection::{ConnectionConfig, DatabaseClient, Protocol};

#[tokio::test]
async fn queries_run_over_grpc() {
    let Ok(url) = env::var("SURREAL_GRPC_URL") else {
        println!("skipped: SURREAL_GRPC_URL not set");
        return;
    };
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let cfg = ConnectionConfig::builder()
        .url(url)
        .namespace(format!("ns_grpc_{nanos}"))
        .database(format!("grpc_{nanos}"))
        .username(env::var("SURREAL_USER").unwrap_or_else(|_| "root".into()))
        .password(env::var("SURREAL_PASS").unwrap_or_else(|_| "root".into()))
        .timeout(10.0)
        .build()
        .expect("valid config");
    assert_eq!(cfg.protocol().unwrap(), Protocol::Grpc);
    let client = DatabaseClient::new(cfg).expect("client constructs");
    client.connect().await.expect("connect over gRPC");

    client
        .query("CREATE thing:a SET n = 1;")
        .await
        .expect("write over gRPC");
    let rows = client
        .query("SELECT VALUE n FROM thing:a;")
        .await
        .expect("read over gRPC");
    assert_eq!(rows, json!([[1]]));
    client.disconnect().await.unwrap();
}
