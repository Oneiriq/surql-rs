use super::*;
use crate::schema::access::AccessDefinition;
use crate::schema::edge::{EdgeDefinition, EdgeMode};
use crate::schema::fields::{FieldDefinition, FieldType};
use crate::schema::registry::SchemaRegistry;
use crate::schema::table::table_schema;
use std::path::PathBuf;
use tempfile::tempdir;

// ----- helpers -----

fn tbl(name: &str) -> TableDefinition {
    table_schema(name)
}

fn tbl_with_field(name: &str, field: &str, ty: FieldType) -> TableDefinition {
    table_schema(name).with_fields([FieldDefinition::new(field, ty)])
}

fn edge(name: &str) -> EdgeDefinition {
    EdgeDefinition::new(name)
        .with_mode(EdgeMode::Relation)
        .with_from_table("a")
        .with_to_table("b")
}

fn access(name: &str) -> AccessDefinition {
    use crate::schema::access::JwtConfig;
    AccessDefinition::jwt(name, JwtConfig::hs256("secret"))
}

fn mig(version: &str) -> Migration {
    Migration {
        version: version.to_string(),
        description: "test".into(),
        path: PathBuf::from(format!("{version}.surql")),
        up: vec!["DEFINE TABLE t SCHEMAFULL;".into()],
        down: vec!["REMOVE TABLE t;".into()],
        checksum: Some("abc".into()),
        depends_on: vec![],
        squashed_from: vec![],
    }
}

// ----- VersionedSnapshot builder + checksum -----

#[test]
fn snapshot_builder_sets_version_and_description() {
    let s = VersionedSnapshot::builder("v1")
        .with_description("initial")
        .build();
    assert_eq!(s.version, "v1");
    assert_eq!(s.description, "initial");
    assert_ne!(s.checksum, "");
}

#[test]
fn snapshot_builder_with_timestamp_overrides_default() {
    let ts = DateTime::parse_from_rfc3339("2026-01-02T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let s = VersionedSnapshot::builder("v1").with_timestamp(ts).build();
    assert_eq!(s.timestamp, ts);
}

#[test]
fn snapshot_builder_defaults_empty_collections() {
    let s = VersionedSnapshot::builder("v1").build();
    assert!(s.tables.is_empty());
    assert!(s.edges.is_empty());
    assert!(s.accesses.is_empty());
}

#[test]
fn snapshot_builder_collects_by_name() {
    let s = VersionedSnapshot::builder("v1")
        .with_tables([tbl("user"), tbl("post")])
        .build();
    assert!(s.tables.contains_key("user"));
    assert!(s.tables.contains_key("post"));
}

#[test]
fn snapshot_filename_uses_json_extension() {
    let s = VersionedSnapshot::builder("20260102_120000").build();
    assert_eq!(s.filename(), "20260102_120000.json");
}

#[test]
fn snapshot_checksum_is_stable() {
    let a = VersionedSnapshot::builder("v1")
        .with_tables([tbl("user")])
        .build();
    let b = VersionedSnapshot::builder("v1")
        .with_tables([tbl("user")])
        .build();
    assert_eq!(a.checksum, b.checksum);
}

#[test]
fn snapshot_checksum_differs_when_content_differs() {
    let a = VersionedSnapshot::builder("v1")
        .with_tables([tbl("user")])
        .build();
    let b = VersionedSnapshot::builder("v1")
        .with_tables([tbl("post")])
        .build();
    assert_ne!(a.checksum, b.checksum);
}

#[test]
fn snapshot_migration_count_round_trip_is_preserved() {
    let s = VersionedSnapshot::builder("v1")
        .with_migration_count(7)
        .build();
    assert_eq!(s.migration_count, 7);
}

// ----- serde round-trip -----

#[test]
fn snapshot_serde_roundtrip() {
    let s = VersionedSnapshot::builder("v1")
        .with_description("initial")
        .with_tables([tbl_with_field("user", "email", FieldType::String)])
        .with_edges([edge("likes")])
        .with_accesses([access("user_access")])
        .with_migration_count(3)
        .build();
    let j = serde_json::to_string(&s).unwrap();
    let back: VersionedSnapshot = serde_json::from_str(&j).unwrap();
    assert_eq!(s, back);
}

// ----- create_snapshot -----

#[test]
fn create_snapshot_from_registry_captures_tables_and_edges() {
    let reg = SchemaRegistry::new();
    reg.register_table(tbl("user"));
    reg.register_edge(edge("likes"));
    let s = create_snapshot(&reg, "v1", "initial").unwrap();
    assert!(s.tables.contains_key("user"));
    assert!(s.edges.contains_key("likes"));
    assert_eq!(s.description, "initial");
}

#[test]
fn create_snapshot_rejects_empty_version() {
    let reg = SchemaRegistry::new();
    let err = create_snapshot(&reg, "   ", "desc").unwrap_err();
    assert!(matches!(err, SurqlError::Validation { .. }));
}

#[test]
fn create_snapshot_captures_buckets() {
    use crate::schema::bucket::memory_bucket;
    let reg = SchemaRegistry::new();
    reg.register_table(tbl("user"));
    reg.register_bucket(memory_bucket("avatars"));
    let s = create_snapshot(&reg, "v1", "initial").unwrap();
    assert!(s.buckets.contains_key("avatars"));
}

#[test]
fn snapshot_with_buckets_roundtrips_on_disk() {
    use crate::schema::bucket::memory_bucket;
    let dir = tempdir().unwrap();
    let s = VersionedSnapshot::builder("v1")
        .with_description("r")
        .with_tables([tbl("user")])
        .with_buckets([memory_bucket("avatars")])
        .build();
    let path = store_snapshot(&s, dir.path()).unwrap();
    let loaded = load_snapshot(&path).unwrap();
    assert_eq!(s, loaded);
    assert!(loaded.buckets.contains_key("avatars"));
}

#[test]
fn buckets_affect_checksum() {
    use crate::schema::bucket::memory_bucket;
    let without = VersionedSnapshot::builder("v1").build();
    let with = VersionedSnapshot::builder("v1")
        .with_buckets([memory_bucket("avatars")])
        .build();
    assert_ne!(without.checksum, with.checksum);
}

// ----- store_snapshot / load_snapshot round-trip -----

#[test]
fn store_and_load_snapshot_roundtrip() {
    let dir = tempdir().unwrap();
    let s = VersionedSnapshot::builder("v1")
        .with_description("r")
        .with_tables([tbl("user")])
        .build();
    let path = store_snapshot(&s, dir.path()).unwrap();
    assert!(path.exists());
    let loaded = load_snapshot(&path).unwrap();
    assert_eq!(s, loaded);
}

#[test]
fn store_snapshot_creates_missing_directory() {
    let dir = tempdir().unwrap();
    let nested = dir.path().join("snaps").join("nested");
    let s = VersionedSnapshot::builder("v1").build();
    store_snapshot(&s, &nested).unwrap();
    assert!(nested.join("v1.json").exists());
}

/// The file name came straight from the version, so `../x` or an
/// absolute path wrote outside the snapshot directory.
#[test]
fn store_snapshot_refuses_versions_that_leave_the_directory() {
    let dir = tempdir().unwrap();
    let inner = dir.path().join("snaps");
    let outside = dir.path().join("escaped");
    for version in [
        "../escaped",
        "..\\escaped",
        "a/b",
        "",
        "C:evil",
        outside.to_str().unwrap(),
    ] {
        let s = VersionedSnapshot::builder(version).build();
        let err = store_snapshot(&s, &inner).unwrap_err();
        assert!(matches!(err, SurqlError::Validation { .. }), "{version}");
    }
    assert!(!dir.path().join("escaped.json").exists());
    assert!(!inner.exists());
}

#[test]
fn load_snapshot_errors_for_missing_file() {
    let dir = tempdir().unwrap();
    let err = load_snapshot(&dir.path().join("nope.json")).unwrap_err();
    assert!(matches!(err, SurqlError::Io { .. }));
}

#[test]
fn load_snapshot_errors_for_invalid_json() {
    let dir = tempdir().unwrap();
    let p = dir.path().join("bad.json");
    std::fs::write(&p, b"not json").unwrap();
    let err = load_snapshot(&p).unwrap_err();
    assert!(matches!(err, SurqlError::Serialization { .. }));
}

// ----- list_snapshots -----

#[test]
fn list_snapshots_empty_for_missing_dir() {
    let dir = tempdir().unwrap();
    let missing = dir.path().join("absent");
    assert_eq!(
        list_snapshots(&missing).unwrap(),
        [] as [crate::migration::versioning::VersionedSnapshot; 0]
    );
}

#[test]
fn list_snapshots_returns_stored_snapshots_sorted() {
    let dir = tempdir().unwrap();
    for v in ["v2", "v1", "v3"] {
        let s = VersionedSnapshot::builder(v).build();
        store_snapshot(&s, dir.path()).unwrap();
    }
    let snaps = list_snapshots(dir.path()).unwrap();
    let versions: Vec<&str> = snaps.iter().map(|s| s.version.as_str()).collect();
    assert_eq!(versions, vec!["v1", "v2", "v3"]);
}

#[test]
fn list_snapshots_skips_non_json_files() {
    let dir = tempdir().unwrap();
    store_snapshot(&VersionedSnapshot::builder("v1").build(), dir.path()).unwrap();
    std::fs::write(dir.path().join("random.txt"), b"hi").unwrap();
    let snaps = list_snapshots(dir.path()).unwrap();
    assert_eq!(snaps.len(), 1);
    assert_eq!(snaps[0].version, "v1");
}

#[test]
fn list_snapshots_skips_invalid_json_files() {
    let dir = tempdir().unwrap();
    store_snapshot(&VersionedSnapshot::builder("v1").build(), dir.path()).unwrap();
    std::fs::write(dir.path().join("broken.json"), b"not json").unwrap();
    let snaps = list_snapshots(dir.path()).unwrap();
    assert_eq!(snaps.len(), 1);
}

// ----- VersionGraph: add / get / lookup -----

#[test]
fn graph_starts_empty() {
    let g = VersionGraph::new();
    assert!(g.is_empty());
    assert_eq!(g.len(), 0);
    assert!(g.root().is_none());
}

#[test]
fn graph_add_root_sets_root_pointer() {
    let mut g = VersionGraph::new();
    g.add_version(mig("v1"), None, None).unwrap();
    assert_eq!(g.root(), Some("v1"));
    assert_eq!(g.len(), 1);
}

#[test]
fn graph_add_duplicate_version_errors() {
    let mut g = VersionGraph::new();
    g.add_version(mig("v1"), None, None).unwrap();
    let err = g.add_version(mig("v1"), None, None).unwrap_err();
    assert!(matches!(err, SurqlError::Validation { .. }));
}

#[test]
fn graph_add_with_unknown_parent_errors() {
    let mut g = VersionGraph::new();
    let err = g.add_version(mig("v2"), Some("v1"), None).unwrap_err();
    assert!(matches!(err, SurqlError::Validation { .. }));
}

#[test]
fn graph_add_child_updates_parent_children_list() {
    let mut g = VersionGraph::new();
    g.add_version(mig("v1"), None, None).unwrap();
    g.add_version(mig("v2"), Some("v1"), None).unwrap();
    let parent = g.get("v1").unwrap();
    assert_eq!(parent.children, vec!["v2".to_string()]);
}

#[test]
fn graph_get_unknown_returns_none() {
    let g = VersionGraph::new();
    assert!(g.get("nope").is_none());
}

#[test]
fn graph_get_returns_attached_snapshot() {
    let mut g = VersionGraph::new();
    let snap = VersionedSnapshot::builder("v1").build();
    g.add_version(mig("v1"), None, Some(snap.clone())).unwrap();
    assert_eq!(g.get("v1").unwrap().snapshot.as_ref(), Some(&snap));
}

// ----- VersionGraph: remove -----

#[test]
fn graph_remove_unknown_errors() {
    let mut g = VersionGraph::new();
    let err = g.remove_version("ghost").unwrap_err();
    assert!(matches!(err, SurqlError::Validation { .. }));
}

#[test]
fn graph_remove_root_clears_root_pointer() {
    let mut g = VersionGraph::new();
    g.add_version(mig("v1"), None, None).unwrap();
    g.remove_version("v1").unwrap();
    assert!(g.root().is_none());
    assert!(g.is_empty());
}

#[test]
fn graph_remove_child_cleans_parent_children_list() {
    let mut g = VersionGraph::new();
    g.add_version(mig("v1"), None, None).unwrap();
    g.add_version(mig("v2"), Some("v1"), None).unwrap();
    g.remove_version("v2").unwrap();
    let parent = g.get("v1").unwrap();
    assert_eq!(parent.children, [] as [std::string::String; 0]);
}

#[test]
fn graph_remove_parent_detaches_children() {
    let mut g = VersionGraph::new();
    g.add_version(mig("v1"), None, None).unwrap();
    g.add_version(mig("v2"), Some("v1"), None).unwrap();
    g.remove_version("v1").unwrap();
    let child = g.get("v2").unwrap();
    assert!(child.parent.is_none());
}

// ----- VersionGraph: ancestors / descendants -----

#[test]
fn graph_ancestors_returns_chain_from_root() {
    let mut g = VersionGraph::new();
    g.add_version(mig("v1"), None, None).unwrap();
    g.add_version(mig("v2"), Some("v1"), None).unwrap();
    g.add_version(mig("v3"), Some("v2"), None).unwrap();
    assert_eq!(g.ancestors("v3"), vec!["v1".to_string(), "v2".to_string()]);
}

#[test]
fn graph_ancestors_empty_for_root() {
    let mut g = VersionGraph::new();
    g.add_version(mig("v1"), None, None).unwrap();
    assert_eq!(g.ancestors("v1"), [] as [std::string::String; 0]);
}

#[test]
fn graph_descendants_bfs_order() {
    let mut g = VersionGraph::new();
    g.add_version(mig("v1"), None, None).unwrap();
    g.add_version(mig("v2"), Some("v1"), None).unwrap();
    g.add_version(mig("v3"), Some("v1"), None).unwrap();
    g.add_version(mig("v4"), Some("v2"), None).unwrap();
    let descendants = g.descendants("v1");
    assert!(descendants.contains(&"v2".to_string()));
    assert!(descendants.contains(&"v3".to_string()));
    assert!(descendants.contains(&"v4".to_string()));
}

#[test]
fn graph_descendants_empty_for_leaf() {
    let mut g = VersionGraph::new();
    g.add_version(mig("v1"), None, None).unwrap();
    assert_eq!(g.descendants("v1"), [] as [std::string::String; 0]);
}

// ----- VersionGraph: path -----

#[test]
fn graph_path_forward() {
    let mut g = VersionGraph::new();
    g.add_version(mig("v1"), None, None).unwrap();
    g.add_version(mig("v2"), Some("v1"), None).unwrap();
    g.add_version(mig("v3"), Some("v2"), None).unwrap();
    let path = g.path("v1", "v3").unwrap();
    assert_eq!(path, vec!["v1".to_string(), "v2".to_string(), "v3".into()]);
}

#[test]
fn graph_path_backward() {
    let mut g = VersionGraph::new();
    g.add_version(mig("v1"), None, None).unwrap();
    g.add_version(mig("v2"), Some("v1"), None).unwrap();
    let path = g.path("v2", "v1").unwrap();
    assert_eq!(path, vec!["v2".to_string(), "v1".to_string()]);
}

#[test]
fn graph_path_to_self_is_single_node() {
    let mut g = VersionGraph::new();
    g.add_version(mig("v1"), None, None).unwrap();
    assert_eq!(g.path("v1", "v1"), Some(vec!["v1".to_string()]));
}

#[test]
fn graph_path_between_disconnected_roots_is_none() {
    let mut g = VersionGraph::new();
    g.add_version(mig("v1"), None, None).unwrap();
    // Detach v1 then add a new orphan root — their graphs are disjoint.
    g.remove_version("v1").unwrap();
    g.add_version(mig("a"), None, None).unwrap();
    g.add_version(mig("b"), None, None).unwrap();
    assert!(g.path("a", "b").is_none());
}

#[test]
fn graph_path_missing_endpoints_is_none() {
    let g = VersionGraph::new();
    assert!(g.path("x", "y").is_none());
}

#[test]
fn graph_versions_lists_all_in_version_order() {
    let mut g = VersionGraph::new();
    for v in ["v10", "v2", "v9", "v1"] {
        g.add_version(mig(v), None, None).unwrap();
    }
    assert_eq!(g.versions(), vec!["v1", "v2", "v9", "v10"]);
}

/// The checksum covers buckets, so two snapshots differing only in a
/// bucket compared as not identical with every list empty.
#[test]
fn compare_snapshots_reports_bucket_changes() {
    use crate::schema::bucket::memory_bucket;
    let from = VersionedSnapshot::builder("v1")
        .with_buckets([memory_bucket("old")])
        .build();
    let to = VersionedSnapshot::builder("v2")
        .with_buckets([memory_bucket("new")])
        .build();
    let diff = compare_snapshots(&from, &to);
    assert_eq!(diff.buckets_added, vec!["new"]);
    assert_eq!(diff.buckets_removed, vec!["old"]);
    assert!(!diff.is_identical());
}

// ----- compare_snapshots -----

#[test]
fn compare_snapshots_identical_checksum_match() {
    let s = VersionedSnapshot::builder("v1")
        .with_tables([tbl("user")])
        .build();
    let diff = compare_snapshots(&s, &s);
    assert!(diff.is_identical());
    assert!(diff.checksum_match);
}

#[test]
fn compare_snapshots_added_table() {
    let from = VersionedSnapshot::builder("v1").build();
    let to = VersionedSnapshot::builder("v2")
        .with_tables([tbl("user")])
        .build();
    let diff = compare_snapshots(&from, &to);
    assert_eq!(diff.tables_added, vec!["user"]);
    assert_eq!(diff.tables_removed, [] as [std::string::String; 0]);
    assert!(!diff.checksum_match);
}

#[test]
fn compare_snapshots_removed_table() {
    let from = VersionedSnapshot::builder("v1")
        .with_tables([tbl("user")])
        .build();
    let to = VersionedSnapshot::builder("v2").build();
    let diff = compare_snapshots(&from, &to);
    assert_eq!(diff.tables_removed, vec!["user"]);
}

#[test]
fn compare_snapshots_modified_table() {
    let from = VersionedSnapshot::builder("v1")
        .with_tables([tbl_with_field("user", "email", FieldType::String)])
        .build();
    let to = VersionedSnapshot::builder("v2")
        .with_tables([tbl_with_field("user", "email", FieldType::Int)])
        .build();
    let diff = compare_snapshots(&from, &to);
    assert_eq!(diff.tables_modified, vec!["user"]);
}

#[test]
fn compare_snapshots_added_and_removed_edges() {
    let from = VersionedSnapshot::builder("v1")
        .with_edges([edge("likes")])
        .build();
    let to = VersionedSnapshot::builder("v2")
        .with_edges([edge("follows")])
        .build();
    let diff = compare_snapshots(&from, &to);
    assert_eq!(diff.edges_added, vec!["follows"]);
    assert_eq!(diff.edges_removed, vec!["likes"]);
}

#[test]
fn compare_snapshots_access_diffs() {
    let from = VersionedSnapshot::builder("v1")
        .with_accesses([access("a")])
        .build();
    let to = VersionedSnapshot::builder("v2")
        .with_accesses([access("b")])
        .build();
    let diff = compare_snapshots(&from, &to);
    assert_eq!(diff.accesses_added, vec!["b"]);
    assert_eq!(diff.accesses_removed, vec!["a"]);
}

#[test]
fn compare_snapshots_sorts_output_vectors() {
    let from = VersionedSnapshot::builder("v1").build();
    let to = VersionedSnapshot::builder("v2")
        .with_tables([tbl("z"), tbl("a"), tbl("m")])
        .build();
    let diff = compare_snapshots(&from, &to);
    assert_eq!(diff.tables_added, vec!["a", "m", "z"]);
}

#[test]
fn compare_snapshots_end_to_end_mixed() {
    let from = VersionedSnapshot::builder("v1")
        .with_tables([tbl("user"), tbl("post")])
        .with_edges([edge("likes")])
        .build();
    let to = VersionedSnapshot::builder("v2")
        .with_tables([
            tbl_with_field("user", "email", FieldType::String),
            tbl("comment"),
        ])
        .with_edges([edge("follows")])
        .build();
    let diff = compare_snapshots(&from, &to);
    assert!(diff.tables_added.contains(&"comment".to_string()));
    assert!(diff.tables_removed.contains(&"post".to_string()));
    assert!(diff.tables_modified.contains(&"user".to_string()));
    assert!(diff.edges_added.contains(&"follows".to_string()));
    assert!(diff.edges_removed.contains(&"likes".to_string()));
    assert!(!diff.is_identical());
}
