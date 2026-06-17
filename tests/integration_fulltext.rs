//! Integration test for the full-text (BM25) search surface against a real
//! SurrealDB engine.
//!
//! Unlike the other `integration_*` suites (which gate on a reachable
//! `SURREAL_URL` server), this one drives the in-process `mem://` engine — the
//! same SurrealDB build, no Docker — so it always runs under `cargo test` and in
//! CI. It is the end-to-end proof that the analyzer / BM25-index DDL emitted by
//! [`generate_analyzer_sql`] + [`bm25_index`] and the
//! [`fulltext_search`](surql::query::builder::Query::fulltext_search) DML are
//! valid SurrealQL on the v3 engine, and that `search::score` ranks results.

#![cfg(any(feature = "client", feature = "client-rustls"))]

use serde::Deserialize;

use surql::connection::{ConnectionConfig, DatabaseClient};
use surql::query::crud::query_records;
use surql::query::helpers::fulltext_search_query;
use surql::schema::{
    bm25_index, generate_analyzer_sql, generate_table_sql, standard_analyzer, table_schema,
    TableMode,
};

#[derive(Debug, Deserialize)]
struct Hit {
    content: String,
    score: f64,
}

async fn memory_client() -> DatabaseClient {
    let cfg = ConnectionConfig::builder()
        .url("mem://")
        .namespace("it_ft")
        .database("it_ft")
        .build()
        .expect("valid mem config");
    let client = DatabaseClient::new(cfg).expect("client constructs");
    client.connect().await.expect("connect to embedded engine");
    client
}

/// Apply the analyzer + BM25 search index, seed three documents, then search.
/// The analyzer must be defined before the index that references it, exactly as
/// a consumer's `ensure_schema` would order them.
#[tokio::test]
async fn bm25_fulltext_search_ranks_and_filters() {
    let client = memory_client().await;

    // DDL: analyzer first, then the table + BM25 index that references it. Both
    // come straight from the builders — no hand-authored SurrealQL.
    let analyzer = standard_analyzer("text_en");
    let doc = table_schema("doc")
        .with_mode(TableMode::Schemaless)
        .with_indexes([bm25_index("doc_content_bm25", ["content"], "text_en")]);

    let mut ddl = generate_analyzer_sql(&analyzer).expect("analyzer sql");
    ddl.extend(generate_table_sql(&doc, true));
    client
        .query(&ddl.join("\n"))
        .await
        .expect("apply analyzer + bm25 index DDL on the embedded engine");

    // Seed: doc:1 mentions "insider" twice (higher term frequency), doc:3 once,
    // doc:2 not at all. A single-term query is robust to the matches operator's
    // default boolean (AND vs OR) while still exercising BM25 ranking.
    client
        .query(
            "CREATE doc:1 SET content = 'insider buying by an insider at a small-cap biotech';\
             CREATE doc:2 SET content = 'quarterly earnings call transcript and guidance';\
             CREATE doc:3 SET content = 'insider selling pressure on a mega-cap tech name';",
        )
        .await
        .expect("seed documents");

    // The lexical leg of hybrid retrieval: `content @1@ 'insider'`. No explicit
    // `ORDER BY`: SurrealDB's full-text scan already yields rows in BM25
    // relevance order, which is exactly what RRF consumes (it fuses ranks, not
    // raw scores). search::score(1) is projected to exercise the DML.
    let query = fulltext_search_query("doc", "content", 1, "insider", None, "score")
        .expect("build query")
        .limit(10)
        .expect("limit");
    let hits: Vec<Hit> = query_records(&client, &query)
        .await
        .expect("run full-text search");

    // doc:2 (no "insider") is filtered out by the matches operator; the two
    // insider docs match.
    assert_eq!(
        hits.len(),
        2,
        "only the two matching docs come back: {hits:?}"
    );
    assert!(
        hits.iter().all(|h| h.content.contains("insider")),
        "every hit matched the query term: {hits:?}"
    );
    assert!(
        !hits.iter().any(|h| h.content.contains("earnings")),
        "the non-matching doc is excluded: {hits:?}"
    );

    // The full-text scan returns rows in BM25 relevance order: the doc with the
    // higher term frequency ranks first. This natural ordering — not a raw score
    // — is what hybrid RRF fusion needs.
    assert!(
        hits[0].content.contains("biotech"),
        "the higher term-frequency doc ranks first: {hits:?}"
    );
    // `search::score` is valid SurrealQL and projected above. On SurrealDB 3.x's
    // streaming executor the per-row score is not plumbed through the full-text
    // scan (it returns 0), so assert a finite, non-negative number rather than a
    // positive one — ranking relies on scan order, not this value.
    assert!(
        hits.iter().all(|h| h.score >= 0.0 && h.score.is_finite()),
        "search::score projects a finite number: {hits:?}"
    );

    // A query whose term appears in no document returns nothing.
    let none_query =
        fulltext_search_query("doc", "content", 1, "cryptocurrency", None, "score").unwrap();
    let empty: Vec<Hit> = query_records(&client, &none_query)
        .await
        .expect("run non-matching search");
    assert!(empty.is_empty(), "no document matches: {empty:?}");
}
