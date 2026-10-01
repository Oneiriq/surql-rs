use super::*;

#[test]
fn concurrently_renders_last() {
    let idx = unique_index("email_idx", ["email"]).with_concurrently(true);
    assert_eq!(
        idx.to_surql("user"),
        "DEFINE INDEX email_idx ON TABLE user COLUMNS email UNIQUE CONCURRENTLY;"
    );
}

#[test]
fn concurrently_is_off_by_default() {
    assert!(!index("i", ["a"]).concurrently);
    assert!(!index("i", ["a"]).to_surql("t").contains("CONCURRENTLY"));
}

#[test]
fn concurrently_composes_with_the_guards() {
    let idx = index("i", ["a"]).with_concurrently(true);
    assert_eq!(
        idx.to_surql_with_options("t", true),
        "DEFINE INDEX IF NOT EXISTS i ON TABLE t COLUMNS a CONCURRENTLY;"
    );
    assert_eq!(
        idx.to_surql_overwrite("t"),
        "DEFINE INDEX OVERWRITE i ON TABLE t COLUMNS a CONCURRENTLY;"
    );
}

#[test]
fn concurrently_renders_on_the_vector_forms() {
    let mtree = mtree_index("m", "v", 8, MTreeDistanceType::Cosine, MTreeVectorType::F32)
        .with_concurrently(true);
    assert!(mtree.to_surql("t").ends_with("TYPE F32 CONCURRENTLY;"));
    let hnsw = hnsw_index(
        "h",
        "v",
        8,
        HnswDistanceType::Cosine,
        MTreeVectorType::F32,
        Some(64),
        Some(8),
    )
    .with_concurrently(true);
    assert!(hnsw.to_surql("t").ends_with("M 8 CONCURRENTLY;"));
}

#[test]
fn concurrently_survives_serde_and_defaults_on_old_snapshots() {
    let idx = index("i", ["a"]).with_concurrently(true);
    let json = serde_json::to_string(&idx).unwrap();
    let back: IndexDefinition = serde_json::from_str(&json).unwrap();
    assert_eq!(idx, back);
    let legacy: IndexDefinition = serde_json::from_str(r#"{"name":"i","columns":["a"]}"#).unwrap();
    assert!(!legacy.concurrently);
}

#[test]
fn info_for_index_statement() {
    assert_eq!(
        info_for_index_surql("email_idx", "user"),
        "INFO FOR INDEX email_idx ON user;"
    );
}

#[test]
fn build_status_reads_the_indexing_shape() {
    let info = serde_json::json!({
        "building": { "initial": 100, "pending": 20, "status": "indexing", "updated": 4 }
    });
    let status = IndexBuildStatus::from_info(&info).expect("status");
    assert_eq!(status.status, "indexing");
    assert_eq!(status.initial, 100);
    assert_eq!(status.pending, 20);
    assert_eq!(status.updated, 4);
    assert!(!status.is_ready());
}

#[test]
fn build_status_reads_the_ready_shape_through_the_client_array() {
    let info = serde_json::json!([{ "building": { "status": "ready" } }]);
    let status = IndexBuildStatus::from_info(&info).expect("status");
    assert!(status.is_ready());
    assert_eq!(status.initial, 0);
}

#[test]
fn build_status_is_none_without_a_status() {
    assert!(IndexBuildStatus::from_info(&serde_json::json!({})).is_none());
}

#[test]
fn index_type_strings() {
    assert_eq!(IndexType::Unique.as_str(), "UNIQUE");
    assert_eq!(IndexType::Standard.as_str(), "INDEX");
    assert_eq!(IndexType::Mtree.as_str(), "MTREE");
    assert_eq!(IndexType::Hnsw.as_str(), "HNSW");
    assert_eq!(IndexType::Diskann.as_str(), "DISKANN");
}

#[test]
fn index_new_defaults_to_standard() {
    let idx = index("title_idx", ["title"]);
    assert_eq!(idx.index_type, IndexType::Standard);
}

#[test]
fn unique_index_to_surql() {
    let idx = unique_index("email_idx", ["email"]);
    assert_eq!(
        idx.to_surql("user"),
        "DEFINE INDEX email_idx ON TABLE user COLUMNS email UNIQUE;"
    );
}

#[test]
fn standard_index_to_surql() {
    let idx = index("title_idx", ["title"]);
    assert_eq!(
        idx.to_surql("post"),
        "DEFINE INDEX title_idx ON TABLE post COLUMNS title;"
    );
}

#[test]
fn search_index_to_surql() {
    let idx = search_index("content_search", ["title", "content"]);
    assert_eq!(
        idx.to_surql("post"),
        "DEFINE INDEX content_search ON TABLE post COLUMNS title, content FULLTEXT ANALYZER ascii;"
    );
}

#[test]
fn bm25_index_renders_analyzer_and_bm25() {
    let idx = bm25_index("content_bm25", ["content"], "text_en");
    assert_eq!(
        idx.to_surql("memory"),
        "DEFINE INDEX content_bm25 ON TABLE memory COLUMNS content FULLTEXT ANALYZER text_en BM25;"
    );
}

#[test]
fn search_index_with_analyzer_bm25_highlights() {
    let idx = search_index("s", ["content"])
        .with_analyzer("text_en")
        .with_bm25()
        .with_highlights();
    assert_eq!(
        idx.to_surql("doc"),
        "DEFINE INDEX s ON TABLE doc COLUMNS content FULLTEXT ANALYZER text_en BM25 HIGHLIGHTS;"
    );
}

#[test]
fn bm25_index_if_not_exists() {
    let idx = bm25_index("content_bm25", ["content"], "text_en");
    assert_eq!(
        idx.to_surql_with_options("memory", true),
        "DEFINE INDEX IF NOT EXISTS content_bm25 ON TABLE memory COLUMNS content \
         FULLTEXT ANALYZER text_en BM25;"
    );
}

#[test]
fn index_to_surql_if_not_exists() {
    let idx = unique_index("email_idx", ["email"]);
    assert_eq!(
        idx.to_surql_with_options("user", true),
        "DEFINE INDEX IF NOT EXISTS email_idx ON TABLE user COLUMNS email UNIQUE;"
    );
}

#[test]
fn index_validate_rejects_empty_name() {
    let mut idx = unique_index("x", ["a"]);
    idx.name = String::new();
    assert!(idx.validate().is_err());
}

#[test]
fn index_validate_rejects_empty_columns() {
    let idx = IndexDefinition::new("x", Vec::<String>::new()).with_type(IndexType::Unique);
    assert!(idx.validate().is_err());
}

/// SurrealDB 3 has no MTREE index; the statement is a parse error on
/// every 3.x engine, so no MTREE definition validates.
#[test]
fn index_validate_refuses_mtree() {
    for vt in [MTreeVectorType::F32, MTreeVectorType::F64] {
        let idx = mtree_index("x", "v", 8, MTreeDistanceType::Cosine, vt);
        let err = idx.validate().expect_err("SurrealDB 3 has no MTREE");
        assert!(
            err.to_string().contains("hnsw_index or diskann_index"),
            "{err}"
        );
    }
    assert!(IndexType::Mtree.is_removed());
    assert!(!IndexType::Hnsw.is_removed());
}

#[test]
fn count_index_renders_without_columns() {
    assert_eq!(
        count_index("n").to_surql("user"),
        "DEFINE INDEX n ON TABLE user COUNT;"
    );
    assert_eq!(
        count_index("n")
            .with_condition("active = true AND age > 18")
            .to_surql_overwrite("user"),
        "DEFINE INDEX OVERWRITE n ON TABLE user COUNT WHERE active = true AND age > 18;"
    );
    assert_eq!(
        count_index("n").with_concurrently(true).to_surql("user"),
        "DEFINE INDEX n ON TABLE user COUNT CONCURRENTLY;"
    );
    assert_eq!(IndexType::Count.as_str(), "COUNT");
}

#[test]
fn count_index_validation() {
    assert!(count_index("n").validate().is_ok());
    assert!(count_index("n").with_condition("a > 1").validate().is_ok());
    let with_columns = IndexDefinition::new("n", ["a"]).with_type(IndexType::Count);
    let err = with_columns.validate().expect_err("COUNT takes no columns");
    assert!(err.to_string().contains("takes no columns"), "{err}");
    let err = index("i", ["a"])
        .with_condition("a > 1")
        .validate()
        .expect_err("only COUNT takes a condition");
    assert!(err.to_string().contains("only a COUNT index"), "{err}");
}

#[test]
fn index_validate_hnsw_requires_dimension() {
    let idx = IndexDefinition::new("x", ["v"]).with_type(IndexType::Hnsw);
    assert!(idx.validate().is_err());
}

#[test]
fn index_validate_diskann_requires_dimension() {
    let mut idx = IndexDefinition::new("x", ["v"]).with_type(IndexType::Diskann);
    assert!(idx.validate().is_err());
    idx.dimension = Some(64);
    assert!(idx.validate().is_ok());
}

#[test]
fn index_validate_refuses_wide_types_on_diskann() {
    for vt in [
        MTreeVectorType::F64,
        MTreeVectorType::I64,
        MTreeVectorType::I32,
        MTreeVectorType::I16,
    ] {
        let idx = diskann_index("x", "v", 8, DiskAnnDistanceType::Cosine, vt);
        let err = idx.validate().expect_err("DISKANN refuses the wide types");
        assert!(err.to_string().contains("F32, F16, I8, or U8"), "{err}");
    }
    for vt in [
        MTreeVectorType::F32,
        MTreeVectorType::F16,
        MTreeVectorType::I8,
        MTreeVectorType::U8,
    ] {
        let idx = diskann_index("x", "v", 8, DiskAnnDistanceType::Cosine, vt);
        assert!(idx.validate().is_ok());
    }
}

#[test]
fn index_validate_refuses_a_foreign_metric_on_diskann() {
    let mut idx = diskann_index(
        "x",
        "v",
        8,
        DiskAnnDistanceType::Cosine,
        MTreeVectorType::F32,
    );
    idx.hnsw_distance = Some(HnswDistanceType::Manhattan);
    let err = idx.validate().expect_err("DISKANN refuses HNSW metrics");
    assert!(err.to_string().contains("diskann_distance"), "{err}");
}

#[test]
fn diskann_renders_the_defaults_even_when_unset() {
    // A hand-assembled definition with an empty tail still spells the
    // engine's defaults, per the echo discipline.
    let mut idx = IndexDefinition::new("pc", ["v"]).with_type(IndexType::Diskann);
    idx.dimension = Some(3);
    assert_eq!(
        idx.to_surql("t"),
        "DEFINE INDEX pc ON TABLE t COLUMNS v DISKANN DIMENSION 3 \
         DIST EUCLIDEAN TYPE F32 DEGREE 64 L_BUILD 100 ALPHA 1.2;"
    );
}

#[test]
fn with_alpha_stores_the_canonical_decimal() {
    let idx = diskann_index(
        "pc",
        "v",
        3,
        DiskAnnDistanceType::Cosine,
        MTreeVectorType::F32,
    )
    .with_alpha(1.5);
    assert_eq!(idx.alpha.as_deref(), Some("1.5"));
    assert_eq!(
        diskann_index(
            "pc",
            "v",
            3,
            DiskAnnDistanceType::Cosine,
            MTreeVectorType::F32
        )
        .with_alpha(2.0)
        .alpha
        .as_deref(),
        Some("2")
    );
}

#[test]
fn diskann_survives_serde_and_defaults_on_old_snapshots() {
    let idx = diskann_index(
        "pc",
        "v",
        3,
        DiskAnnDistanceType::CosineNormalized,
        MTreeVectorType::I8,
    )
    .with_degree(48)
    .with_hashed_vector(true);
    let json = serde_json::to_string(&idx).unwrap();
    let back: IndexDefinition = serde_json::from_str(&json).unwrap();
    assert_eq!(idx, back);
    // Stored snapshots and old contracts predate every DISKANN member;
    // they must keep deserialising with the members defaulted off.
    let legacy: IndexDefinition = serde_json::from_str(
        r#"{"name":"i","columns":["a"],"type":"HNSW","dimension":8,"efc":150}"#,
    )
    .unwrap();
    assert_eq!(legacy.index_type, IndexType::Hnsw);
    assert_eq!(legacy.diskann_distance, None);
    assert_eq!(legacy.degree, None);
    assert_eq!(legacy.l_build, None);
    assert_eq!(legacy.alpha, None);
    assert!(!legacy.hashed_vector);
}
