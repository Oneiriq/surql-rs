//! Unit tests for the query builder.

use super::*;
use crate::query::hints::{IndexHint, ParallelHint, TimeoutHint};
use crate::types::operators::{eq, gt};
use serde_json::Value;

fn data(pairs: &[(&str, Value)]) -> DataMap {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

// -----------------------------------------------------------------------
// Basic rendering
// -----------------------------------------------------------------------

#[test]
fn select_star_from_table() {
    let q = Query::new().select(None).from_table("user").unwrap();
    assert_eq!(q.to_surql().unwrap(), "SELECT * FROM user");
}

#[test]
fn select_projection_renders_comma_separated() {
    let q = Query::new()
        .select(Some(vec!["name".into(), "email".into()]))
        .from_table("user")
        .unwrap();
    assert_eq!(q.to_surql().unwrap(), "SELECT name, email FROM user");
}

#[test]
fn insert_renders_create_content() {
    let q = Query::new()
        .insert(
            "user",
            data(&[
                ("name", Value::String("Alice".into())),
                ("email", Value::String("alice@example.com".into())),
            ]),
        )
        .unwrap();
    // BTreeMap => alphabetical order: email, name.
    assert_eq!(
        q.to_surql().unwrap(),
        "CREATE user CONTENT {email: 'alice@example.com', name: 'Alice'}"
    );
}

#[test]
fn update_renders_set_clauses() {
    let q = Query::new()
        .update(
            "user:alice",
            data(&[("status", Value::String("active".into()))]),
        )
        .unwrap();
    assert_eq!(
        q.to_surql().unwrap(),
        "UPDATE user:alice SET status = 'active'"
    );
}

#[test]
fn update_set_expr_renders_atomic_guarded_update() {
    use crate::query::expressions::field;
    use crate::types::operators::{and_, eq, is_none};

    // An atomic, tombstone-guarded read-modify-write in one statement: the
    // increment references the row's own value, and the guard skips a row
    // that was forgotten (deleted_at set) between read and write.
    let q = Query::new()
        .update_set("memory:abc")
        .unwrap()
        .set_expr("reinforcement", field("reinforcement") + 1)
        .unwrap()
        .set("updated_at", "2026-06-06T00:00:00+00:00")
        .unwrap()
        .where_(and_(eq("tenant_id", "t"), is_none("deleted_at")))
        .return_after();
    assert_eq!(
        q.to_surql().unwrap(),
        "UPDATE memory:abc SET reinforcement = (reinforcement + 1), \
         updated_at = '2026-06-06T00:00:00+00:00' \
         WHERE ((tenant_id = 't') AND (deleted_at IS NONE)) RETURN AFTER"
    );
}

#[test]
fn upsert_renders_content_object() {
    let q = Query::new()
        .upsert(
            "user:alice",
            data(&[("status", Value::String("active".into()))]),
        )
        .unwrap();
    assert_eq!(
        q.to_surql().unwrap(),
        "UPSERT user:alice CONTENT {status: 'active'}"
    );
}

#[test]
fn delete_renders_record_id() {
    let q = Query::new().delete("user:alice").unwrap();
    assert_eq!(q.to_surql().unwrap(), "DELETE user:alice");
}

#[test]
fn delete_with_where() {
    let q = Query::new()
        .delete("user")
        .unwrap()
        .where_str("deleted_at IS NOT NULL");
    assert_eq!(
        q.to_surql().unwrap(),
        "DELETE user WHERE (deleted_at IS NOT NULL)"
    );
}

#[test]
fn relate_renders_arrow_chain() {
    let q = Query::new()
        .relate("likes", "user:alice", "post:123", None)
        .unwrap();
    assert_eq!(q.to_surql().unwrap(), "RELATE user:alice->likes->post:123");
}

#[test]
fn relate_with_data_renders_content() {
    let q = Query::new()
        .relate(
            "follows",
            "user:alice",
            "user:bob",
            Some(data(&[("since", Value::String("2024-01-01".into()))])),
        )
        .unwrap();
    assert_eq!(
        q.to_surql().unwrap(),
        "RELATE user:alice->follows->user:bob CONTENT {since: '2024-01-01'}"
    );
}

// -----------------------------------------------------------------------
// Fluent chaining
// -----------------------------------------------------------------------

#[test]
fn chaining_produces_full_select() {
    let q = Query::new()
        .select(Some(vec!["name".into(), "email".into()]))
        .from_table("user")
        .unwrap()
        .where_str("age > 18")
        .order_by("name", "ASC")
        .unwrap()
        .limit(10)
        .unwrap()
        .offset(20)
        .unwrap();
    assert_eq!(
        q.to_surql().unwrap(),
        "SELECT name, email FROM user WHERE (age > 18) ORDER BY name ASC LIMIT 10 START 20"
    );
}

#[test]
fn immutability_preserved_across_chain() {
    let base = Query::new().select(None).from_table("user").unwrap();
    let extended = base.clone().where_str("age > 18");
    assert_eq!(base.conditions, [] as [std::string::String; 0]);
    assert_eq!(extended.conditions.len(), 1);
    assert_eq!(base.to_surql().unwrap(), "SELECT * FROM user");
    assert_eq!(
        extended.to_surql().unwrap(),
        "SELECT * FROM user WHERE (age > 18)"
    );
}

// -----------------------------------------------------------------------
// WHERE variants
// -----------------------------------------------------------------------

#[test]
fn where_accepts_string_condition() {
    let q = Query::new()
        .select(None)
        .from_table("user")
        .unwrap()
        .where_str("age > 18");
    assert_eq!(q.to_surql().unwrap(), "SELECT * FROM user WHERE (age > 18)");
}

#[test]
fn where_accepts_operator_condition() {
    let q = Query::new()
        .select(None)
        .from_table("user")
        .unwrap()
        .where_(gt("age", 18));
    assert_eq!(q.to_surql().unwrap(), "SELECT * FROM user WHERE (age > 18)");
}

#[test]
fn multiple_where_conditions_join_with_and() {
    let q = Query::new()
        .select(None)
        .from_table("user")
        .unwrap()
        .where_(gt("age", 18))
        .where_(eq("status", "active"));
    assert_eq!(
        q.to_surql().unwrap(),
        "SELECT * FROM user WHERE (age > 18) AND (status = 'active')"
    );
}

// -----------------------------------------------------------------------
// ORDER / GROUP / LIMIT / OFFSET
// -----------------------------------------------------------------------

#[test]
fn order_by_desc_renders() {
    let q = Query::new()
        .select(None)
        .from_table("user")
        .unwrap()
        .order_by("created_at", "DESC")
        .unwrap();
    assert_eq!(
        q.to_surql().unwrap(),
        "SELECT * FROM user ORDER BY created_at DESC"
    );
}

#[test]
fn order_by_is_case_insensitive() {
    let q = Query::new()
        .select(None)
        .from_table("user")
        .unwrap()
        .order_by("name", "asc")
        .unwrap();
    assert!(q.to_surql().unwrap().contains("ORDER BY name ASC"));
}

#[test]
fn order_by_rejects_invalid_direction() {
    let err = Query::new()
        .select(None)
        .from_table("user")
        .unwrap()
        .order_by("name", "SIDEWAYS");
    assert!(matches!(err, Err(SurqlError::Validation { .. })));
}

#[test]
fn order_by_multiple_fields() {
    let q = Query::new()
        .select(None)
        .from_table("user")
        .unwrap()
        .order_by("last_name", "ASC")
        .unwrap()
        .order_by("first_name", "ASC")
        .unwrap();
    assert!(q
        .to_surql()
        .unwrap()
        .contains("ORDER BY last_name ASC, first_name ASC"));
}

#[test]
fn group_by_renders() {
    let q = Query::new()
        .select(Some(vec!["status".into(), "COUNT(*)".into()]))
        .from_table("user")
        .unwrap()
        .group_by(["status"]);
    assert_eq!(
        q.to_surql().unwrap(),
        "SELECT status, COUNT(*) FROM user GROUP BY status"
    );
}

#[test]
fn group_all_renders() {
    let q = Query::new()
        .select(Some(vec!["count()".into()]))
        .from_table("user")
        .unwrap()
        .group_all();
    assert_eq!(q.to_surql().unwrap(), "SELECT count() FROM user GROUP ALL");
}

#[test]
fn limit_and_offset_render_start() {
    let q = Query::new()
        .select(None)
        .from_table("user")
        .unwrap()
        .limit(10)
        .unwrap()
        .offset(5)
        .unwrap();
    assert_eq!(q.to_surql().unwrap(), "SELECT * FROM user LIMIT 10 START 5");
}

#[test]
fn negative_limit_rejected() {
    let err = Query::new().limit(-1);
    assert!(matches!(err, Err(SurqlError::Validation { .. })));
}

#[test]
fn negative_offset_rejected() {
    let err = Query::new().offset(-1);
    assert!(matches!(err, Err(SurqlError::Validation { .. })));
}

// -----------------------------------------------------------------------
// RETURN formats
// -----------------------------------------------------------------------

#[test]
fn return_diff_on_update() {
    let q = Query::new()
        .update("user:alice", data(&[("age", Value::from(30))]))
        .unwrap()
        .return_diff();
    assert_eq!(
        q.to_surql().unwrap(),
        "UPDATE user:alice SET age = 30 RETURN DIFF"
    );
}

#[test]
fn return_none_on_delete() {
    let q = Query::new().delete("user:alice").unwrap().return_none();
    assert_eq!(q.to_surql().unwrap(), "DELETE user:alice RETURN NONE");
}

#[test]
fn return_full_on_insert() {
    let q = Query::new()
        .insert("user", data(&[("name", Value::String("Alice".into()))]))
        .unwrap()
        .return_full();
    assert!(q.to_surql().unwrap().ends_with("RETURN FULL"));
}

#[test]
fn return_before_and_after() {
    let before = Query::new().delete("user:alice").unwrap().return_before();
    let after = Query::new().delete("user:alice").unwrap().return_after();
    assert!(before.to_surql().unwrap().contains("RETURN BEFORE"));
    assert!(after.to_surql().unwrap().contains("RETURN AFTER"));
}

// -----------------------------------------------------------------------
// Vector search
// -----------------------------------------------------------------------

#[test]
fn vector_search_without_threshold() {
    let q = Query::new()
        .select(None)
        .from_table("documents")
        .unwrap()
        .vector_search(
            "embedding",
            vec![0.1, 0.2, 0.3],
            10,
            VectorDistanceType::Cosine,
            None,
        )
        .unwrap();
    assert_eq!(
        q.to_surql().unwrap(),
        "SELECT * FROM documents WHERE embedding <|10,COSINE|> [0.1, 0.2, 0.3]"
    );
}

#[test]
fn vector_search_with_threshold() {
    let q = Query::new()
        .select(None)
        .from_table("documents")
        .unwrap()
        .vector_search(
            "embedding",
            vec![0.1, 0.2, 0.3],
            10,
            VectorDistanceType::Cosine,
            Some(0.7),
        )
        .unwrap();
    assert_eq!(
        q.to_surql().unwrap(),
        "SELECT * FROM documents WHERE embedding <|10,COSINE,0.7|> [0.1, 0.2, 0.3]"
    );
}

#[test]
fn vector_search_rejects_k_zero() {
    let err = Query::new()
        .select(None)
        .from_table("documents")
        .unwrap()
        .vector_search("embedding", vec![0.1], 0, VectorDistanceType::Cosine, None);
    assert!(matches!(err, Err(SurqlError::Validation { .. })));
}

#[test]
fn vector_search_rejects_empty_vector() {
    let err = Query::new()
        .select(None)
        .from_table("documents")
        .unwrap()
        .vector_search("embedding", vec![], 10, VectorDistanceType::Cosine, None);
    assert!(matches!(err, Err(SurqlError::Validation { .. })));
}

#[test]
fn indexed_vector_search_renders_the_effort_operand() {
    let q = Query::new()
        .select(None)
        .from_table("documents")
        .unwrap()
        .vector_search_indexed("embedding", vec![0.1, 0.2, 0.3], 10, 64)
        .unwrap();
    // An INTEGER second operand is what selects the index; the
    // metric form compares every row instead.
    assert_eq!(
        q.to_surql().unwrap(),
        "SELECT * FROM documents WHERE embedding <|10,64|> [0.1, 0.2, 0.3]"
    );

    for (k, ef) in [(0, 64), (10, 0)] {
        let err = Query::new()
            .select(None)
            .from_table("documents")
            .unwrap()
            .vector_search_indexed("embedding", vec![0.1], k, ef);
        assert!(
            matches!(err, Err(SurqlError::Validation { .. })),
            "{k} {ef}"
        );
    }
    let err = Query::new()
        .select(None)
        .from_table("documents")
        .unwrap()
        .vector_search_indexed("embedding", vec![], 10, 64);
    assert!(matches!(err, Err(SurqlError::Validation { .. })));
}

#[test]
fn similarity_score_adds_function_field() {
    let q = Query::new()
        .select(Some(vec!["id".into()]))
        .from_table("chunk")
        .unwrap()
        .similarity_score(
            "embedding",
            &[0.1, 0.2],
            VectorDistanceType::Cosine,
            "score",
        )
        .unwrap();
    let sql = q.to_surql().unwrap();
    assert!(sql.contains("vector::similarity::cosine(embedding, [0.1, 0.2]) AS score"));
}

#[test]
fn similarity_score_validates_names_and_values() {
    let base = Query::new().select(None).from_table("chunk").unwrap();
    let cosine = VectorDistanceType::Cosine;
    assert!(base
        .clone()
        .similarity_score("e) AS x FROM user; --", &[0.1], cosine, "s")
        .is_err());
    assert!(base
        .clone()
        .similarity_score("e", &[0.1], cosine, "s FROM user; --")
        .is_err());
    assert!(base
        .clone()
        .similarity_score("e", &[f64::NAN], cosine, "s")
        .is_err());
}

#[test]
fn non_finite_vectors_and_thresholds_are_refused() {
    let base = Query::new().select(None).from_table("doc").unwrap();
    let cosine = VectorDistanceType::Cosine;
    assert!(base
        .clone()
        .vector_search("e", vec![f64::INFINITY], 1, cosine, None)
        .is_err());
    assert!(base
        .clone()
        .vector_search("e", vec![0.1], 1, cosine, Some(f64::NAN))
        .is_err());
    assert!(base
        .clone()
        .vector_search_indexed("e", vec![f64::NEG_INFINITY], 1, 8)
        .is_err());
    // Hand-set values are re-checked when rendering.
    let mut q = base.vector_search("e", vec![0.1], 1, cosine, None).unwrap();
    q.vector_value = vec![f64::NAN];
    assert!(q.to_surql().is_err());
}

#[test]
fn set_expr_adds_raw_values_to_content() {
    use crate::query::expressions::time_now;
    use crate::types::record_ref;

    let q = Query::new()
        .insert("post", data(&[("title", Value::from("hi"))]))
        .unwrap()
        .set_expr("created_at", time_now())
        .unwrap()
        .set_expr("author", record_ref("user", "alice").into())
        .unwrap();
    assert_eq!(
        q.to_surql().unwrap(),
        "CREATE post CONTENT {title: 'hi', created_at: time::now(), \
         author: type::record('user', 'alice')}"
    );
    // An assignment replaces the same-named data key.
    let q = Query::new()
        .upsert("post:1", data(&[("n", Value::from(1))]))
        .unwrap()
        .set_expr("n", time_now())
        .unwrap();
    assert_eq!(
        q.to_surql().unwrap(),
        "UPSERT post:1 CONTENT {n: time::now()}"
    );
    // RELATE takes them as edge content, with or without a data map.
    let q = Query::new()
        .relate("likes", "user:a", "post:1", None)
        .unwrap()
        .set_expr("at", time_now())
        .unwrap();
    assert_eq!(
        q.to_surql().unwrap(),
        "RELATE user:a->likes->post:1 CONTENT {at: time::now()}"
    );
    // A nested path cannot be a CONTENT key.
    let q = Query::new()
        .insert("post", DataMap::new())
        .unwrap()
        .set("meta.n", 1)
        .unwrap();
    assert!(q.to_surql().is_err());
}

#[test]
fn hand_set_names_are_rechecked_when_rendering() {
    let mut q = Query::new().select(None).from_table("user").unwrap();
    q.table_name = Some("user; DELETE user".into());
    assert!(q.to_surql().is_err());

    let mut q = Query::new()
        .select(None)
        .from_table("user")
        .unwrap()
        .order_by("name", "ASC")
        .unwrap();
    q.order_fields[0].field = "name; DELETE user".into();
    assert!(q.to_surql().is_err());
    q.order_fields[0].field = "name".into();
    q.order_fields[0].direction = "ASC; DELETE user".into();
    assert!(q.to_surql().is_err());

    let mut q = Query::new()
        .relate("likes", "user:a", "post:1", None)
        .unwrap();
    q.relate_to = Some("post:1; DELETE post".into());
    assert_eq!(
        q.to_surql().unwrap(),
        "RELATE user:a->likes->post:⟨1; DELETE post⟩"
    );
    q.table_name = Some("likes; DELETE".into());
    assert!(q.to_surql().is_err());
}

#[test]
fn index_hints_are_checked_when_rendering() {
    let q = Query::new()
        .select(None)
        .from_table("user")
        .unwrap()
        .hint(QueryHint::Index(IndexHint::new(
            "user",
            "x */ DELETE user; /*",
        )));
    assert!(matches!(q.to_surql(), Err(SurqlError::Validation { .. })));
}

#[test]
fn record_targets_are_normalised() {
    let q = Query::new().delete("user:a-b").unwrap();
    assert_eq!(q.to_surql().unwrap(), "DELETE user:⟨a-b⟩");
    assert!(Query::new().delete("user:[1, 2]").is_err());
    assert!(Query::new().from_table("user; DELETE user").is_err());
}

// -----------------------------------------------------------------------
// Full-text search
// -----------------------------------------------------------------------

#[test]
fn fulltext_search_renders_match_operator() {
    let q = Query::new()
        .select(None)
        .from_table("memory")
        .unwrap()
        .fulltext_search("content", 1, "insider buying")
        .unwrap();
    assert_eq!(
        q.to_surql().unwrap(),
        "SELECT * FROM memory WHERE content @1@ 'insider buying'"
    );
}

#[test]
fn fulltext_search_with_score_and_order() {
    let q = Query::new()
        .select(None)
        .search_score(1, "score")
        .from_table("memory")
        .unwrap()
        .fulltext_search("content", 1, "form 4")
        .unwrap()
        .order_by("score", "DESC")
        .unwrap()
        .limit(5)
        .unwrap();
    assert_eq!(
        q.to_surql().unwrap(),
        "SELECT *, search::score(1) AS score FROM memory \
         WHERE content @1@ 'form 4' ORDER BY score DESC LIMIT 5"
    );
}

#[test]
fn fulltext_search_escapes_quotes() {
    let q = Query::new()
        .select(None)
        .from_table("memory")
        .unwrap()
        .fulltext_search("content", 0, "o'brien")
        .unwrap();
    assert_eq!(
        q.to_surql().unwrap(),
        "SELECT * FROM memory WHERE content @0@ 'o\\'brien'"
    );
}

#[test]
fn fulltext_search_rejects_empty_field() {
    let err = Query::new()
        .select(None)
        .from_table("memory")
        .unwrap()
        .fulltext_search("", 1, "x");
    assert!(matches!(err, Err(SurqlError::Validation { .. })));
}

#[test]
fn fulltext_search_rejects_empty_query() {
    let err = Query::new()
        .select(None)
        .from_table("memory")
        .unwrap()
        .fulltext_search("content", 1, "");
    assert!(matches!(err, Err(SurqlError::Validation { .. })));
}

#[test]
fn fulltext_and_vector_both_render_in_where() {
    let q = Query::new()
        .select(None)
        .from_table("memory")
        .unwrap()
        .vector_search(
            "embedding",
            vec![0.1, 0.2],
            5,
            VectorDistanceType::Cosine,
            None,
        )
        .unwrap()
        .fulltext_search("content", 1, "term")
        .unwrap();
    let sql = q.to_surql().unwrap();
    assert!(sql.contains("embedding <|5,COSINE|> [0.1, 0.2]"));
    assert!(sql.contains("content @1@ 'term'"));
    assert!(sql.contains(" AND "));
}

// -----------------------------------------------------------------------
// Hints
// -----------------------------------------------------------------------

#[test]
fn hint_prepends_comment() {
    let q = Query::new()
        .select(None)
        .from_table("user")
        .unwrap()
        .hint(QueryHint::Timeout(TimeoutHint::new(30.0).unwrap()));
    let sql = q.to_surql().unwrap();
    assert!(sql.starts_with("/* TIMEOUT 30s */"));
    assert!(sql.contains("SELECT * FROM user"));
}

#[test]
fn with_hints_composes_multiple() {
    let q = Query::new()
        .select(None)
        .from_table("user")
        .unwrap()
        .with_hints([
            QueryHint::Timeout(TimeoutHint::new(30.0).unwrap()),
            QueryHint::Parallel(ParallelHint::enabled()),
        ]);
    let sql = q.to_surql().unwrap();
    assert!(sql.contains("/* TIMEOUT 30s */"));
    assert!(sql.contains("/* PARALLEL ON */"));
}

#[test]
fn index_hint_references_table() {
    let q = Query::new()
        .select(None)
        .from_table("user")
        .unwrap()
        .hint(QueryHint::Index(IndexHint::new("user", "email_idx")));
    assert!(q
        .to_surql()
        .unwrap()
        .contains("/* USE INDEX user.email_idx */"));
}

// -----------------------------------------------------------------------
// Validation
// -----------------------------------------------------------------------

#[test]
fn expression_shaped_insert_data_stays_data() {
    let q = Query::new()
        .insert(
            "post",
            data(&[(
                "body",
                serde_json::json!({"expression": "1}; DELETE user; --"}),
            )]),
        )
        .unwrap();
    assert_eq!(
        q.to_surql().unwrap(),
        "CREATE post CONTENT {body: { expression: '1}; DELETE user; --' }}"
    );
}

#[test]
fn data_keys_set_directly_are_quoted() {
    let q = Query {
        operation: Some(Operation::Insert),
        table_name: Some("user".into()),
        insert_data: Some(data(&[("x: 1}; DELETE user; --", Value::from(1))])),
        ..Query::default()
    };
    assert_eq!(
        q.to_surql().unwrap(),
        "CREATE user CONTENT {'x: 1}; DELETE user; --': 1}"
    );
    let q = Query {
        operation: Some(Operation::Update),
        table_name: Some("user".into()),
        update_data: Some(data(&[("x = 1; DELETE user; --", Value::from(1))])),
        ..Query::default()
    };
    assert!(matches!(q.to_surql(), Err(SurqlError::Validation { .. })));
}

#[test]
fn record_targets_cannot_carry_a_second_statement() {
    let q = Query::new()
        .delete("user:x; REMOVE TABLE user; --")
        .unwrap();
    assert_eq!(
        q.to_surql().unwrap(),
        "DELETE user:⟨x; REMOVE TABLE user; --⟩"
    );
    let q = Query::new()
        .update_set("user:a; REMOVE TABLE user")
        .unwrap()
        .set("n", 1)
        .unwrap();
    assert_eq!(
        q.to_surql().unwrap(),
        "UPDATE user:⟨a; REMOVE TABLE user⟩ SET n = 1"
    );
    let q = Query::new()
        .relate(
            "likes",
            "user:a->likes->post:b; REMOVE TABLE post",
            "post:c",
            None,
        )
        .unwrap();
    assert_eq!(
        q.to_surql().unwrap(),
        "RELATE user:⟨a->likes->post:b; REMOVE TABLE post⟩->likes->post:c"
    );
}

#[test]
fn identifier_sinks_reject_injection() {
    let base = Query::new().select(None).from_table("user").unwrap();
    assert!(base.clone().order_by("name; DELETE user", "ASC").is_err());
    assert!(base
        .clone()
        .group_by(["status; DELETE user"])
        .to_surql()
        .is_err());
    assert!(base
        .clone()
        .fulltext_search("content; DELETE user", 1, "x")
        .is_err());
    assert!(base
        .clone()
        .vector_search(
            "e; DELETE user",
            vec![0.1],
            1,
            VectorDistanceType::Cosine,
            None
        )
        .is_err());
    let sql = base
        .clone()
        .search_score(1, "s FROM user; DELETE user; --")
        .to_surql()
        .unwrap();
    assert!(sql.contains("AS `s FROM user; DELETE user; --`"), "{sql}");
}

#[test]
fn invalid_table_name_rejected() {
    let err = Query::new().from_table("1user");
    assert!(matches!(err, Err(SurqlError::Validation { .. })));
}

#[test]
fn invalid_field_name_in_insert_rejected() {
    let err = Query::new().insert("user", data(&[("bad-field", Value::from(1))]));
    assert!(matches!(err, Err(SurqlError::Validation { .. })));
}

#[test]
fn invalid_edge_table_rejected() {
    let err = Query::new().relate("bad-edge", "user:a", "user:b", None);
    assert!(matches!(err, Err(SurqlError::Validation { .. })));
}

#[test]
fn empty_table_rejected() {
    let err = Query::new().from_table("");
    assert!(matches!(err, Err(SurqlError::Validation { .. })));
}

#[test]
fn to_surql_without_operation_errors() {
    let err = Query::new().to_surql();
    assert!(matches!(err, Err(SurqlError::Query { .. })));
}

#[test]
fn select_without_table_errors() {
    let err = Query::new().select(None).to_surql();
    assert!(matches!(err, Err(SurqlError::Query { .. })));
}

// -----------------------------------------------------------------------
// Traversal / join
// -----------------------------------------------------------------------

#[test]
fn set_accepts_dotted_paths_into_nested_objects() {
    let q = Query::new()
        .update_set("file:abc")
        .unwrap()
        .set(
            "metadata.processing",
            serde_json::json!({"verdict": "clean"}),
        )
        .unwrap()
        .to_surql()
        .unwrap();
    assert!(
        q.contains("SET metadata.processing = "),
        "nested assignment must render: {q}",
    );
    // Hostile segments still refuse.
    assert!(Query::new()
        .update_set("file:abc")
        .unwrap()
        .set("metadata.bad segment", 1)
        .is_err());
    assert!(Query::new()
        .update_set("file:abc")
        .unwrap()
        .set("metadata..double", 1)
        .is_err());
    assert!(Query::new()
        .update_set("file:abc")
        .unwrap()
        .set("", 1)
        .is_err());
}

#[test]
fn traverse_appends_path_to_from() {
    let q = Query::new()
        .select(None)
        .from_table("user:alice")
        .unwrap()
        .traverse("->likes->post");
    assert_eq!(
        q.to_surql().unwrap(),
        "SELECT * FROM user:alice->likes->post"
    );
}

#[test]
fn join_clause_appended() {
    let q = Query::new()
        .select(None)
        .from_table("user")
        .unwrap()
        .join("JOIN post ON user.id = post.author");
    assert!(q
        .to_surql()
        .unwrap()
        .contains("JOIN post ON user.id = post.author"));
}

// -----------------------------------------------------------------------
// Sub-feature 4: select_expr accepts typed Expressions
// -----------------------------------------------------------------------

#[test]
fn select_expr_renders_projection() {
    use crate::query::expressions::{as_, count_all, math_mean};

    let q = Query::new()
        .select_expr(vec![
            as_(&count_all(), "total"),
            as_(&math_mean("strength"), "mean"),
        ])
        .from_table("memory_entry")
        .unwrap()
        .group_all();

    assert_eq!(
        q.to_surql().unwrap(),
        "SELECT count() AS total, math::mean(strength) AS mean FROM memory_entry GROUP ALL",
    );
}

#[test]
fn select_expr_empty_falls_back_to_empty_list() {
    // Empty iterator yields no fields, so the default "*" (populated by
    // the non-expr `select(None)` helper) is NOT applied here; ensure
    // we still render a valid statement with just FROM.
    let q = Query::new()
        .select_expr(Vec::<crate::query::expressions::Expression>::new())
        .from_table("user")
        .unwrap();
    // Empty fields -> "*" by build_select's fallback.
    assert_eq!(q.to_surql().unwrap(), "SELECT * FROM user");
}
