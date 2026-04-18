//! Schema versioning and snapshot management.
//!
//! Port of `surql/migration/versioning.py`. Provides version tracking and
//! snapshot functionality for database schema evolution, enabling rich
//! version history and safe rollbacks.
//!
//! ## Deviations from Python
//!
//! * The Python module persists snapshots as rows in a `_schema_snapshot`
//!   table inside SurrealDB. The Rust port persists snapshots as JSON
//!   files on disk under a caller-supplied directory. This mirrors the
//!   broader Rust port's "migrations on disk" approach (see
//!   [`crate::migration::generator`]).
//! * The simple [`crate::migration::diff::SchemaSnapshot`] type (plain
//!   tables + edges container consumed by the diff API) is deliberately
//!   left unchanged. This module introduces a richer
//!   [`VersionedSnapshot`] that embeds the same table/edge material plus
//!   a version identifier, creation timestamp, human description,
//!   accesses, checksum, and migration count.
//! * [`create_snapshot`] takes a reference to a [`SchemaRegistry`]
//!   instead of an async database client because the registry is the
//!   authoritative source of code-defined schemas in the Rust port.
//!
//! ## Examples
//!
//! ```no_run
//! use std::path::Path;
//! use surql::migration::versioning::{create_snapshot, store_snapshot};
//! use surql::schema::SchemaRegistry;
//!
//! let registry = SchemaRegistry::new();
//! let snap = create_snapshot(&registry, "20260109_120000", "initial schema").unwrap();
//! store_snapshot(&snap, Path::new("./snapshots")).unwrap();
//! ```

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{Result, SurqlError};
use crate::migration::discovery::sha2_lite;
use crate::migration::models::Migration;
use crate::schema::access::AccessDefinition;
use crate::schema::edge::EdgeDefinition;
use crate::schema::registry::SchemaRegistry;
use crate::schema::table::TableDefinition;

// ---------------------------------------------------------------------------
// Snapshot types
// ---------------------------------------------------------------------------

/// Point-in-time snapshot of a database schema.
///
/// Captures the complete schema state (tables, edges, accesses) along with
/// a version identifier, creation timestamp, description, checksum, and
/// migration count. Used for version comparison and rollback operations.
///
/// ## Examples
///
/// ```
/// use surql::migration::versioning::VersionedSnapshot;
///
/// let snap = VersionedSnapshot::builder("20260109_120000")
///     .with_description("initial schema")
///     .build();
/// assert_eq!(snap.version, "20260109_120000");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionedSnapshot {
    /// Version identifier (typically a `YYYYMMDD_HHMMSS` timestamp).
    pub version: String,
    /// Snapshot creation time (UTC).
    pub timestamp: DateTime<Utc>,
    /// Optional human-readable description of this snapshot.
    #[serde(default)]
    pub description: String,
    /// Table definitions captured at this version, keyed by table name.
    #[serde(default)]
    pub tables: BTreeMap<String, TableDefinition>,
    /// Edge definitions captured at this version, keyed by edge name.
    #[serde(default)]
    pub edges: BTreeMap<String, EdgeDefinition>,
    /// Access definitions captured at this version, keyed by access name.
    #[serde(default)]
    pub accesses: BTreeMap<String, AccessDefinition>,
    /// SHA-256 hex digest of the serialised schema payload.
    pub checksum: String,
    /// Total number of migrations that had been applied at snapshot time.
    #[serde(default)]
    pub migration_count: u64,
}

impl VersionedSnapshot {
    /// Construct a builder for a versioned snapshot.
    pub fn builder(version: impl Into<String>) -> VersionedSnapshotBuilder {
        VersionedSnapshotBuilder::new(version)
    }

    /// Extension used for snapshot files on disk.
    pub const FILE_EXTENSION: &'static str = "json";

    /// Derive the canonical filename for this snapshot.
    pub fn filename(&self) -> String {
        format!("{}.{}", self.version, Self::FILE_EXTENSION)
    }
}

/// Builder for [`VersionedSnapshot`].
#[derive(Debug, Clone)]
pub struct VersionedSnapshotBuilder {
    version: String,
    timestamp: Option<DateTime<Utc>>,
    description: String,
    tables: BTreeMap<String, TableDefinition>,
    edges: BTreeMap<String, EdgeDefinition>,
    accesses: BTreeMap<String, AccessDefinition>,
    migration_count: u64,
}

impl VersionedSnapshotBuilder {
    /// Start a new builder rooted at the given version identifier.
    pub fn new(version: impl Into<String>) -> Self {
        Self {
            version: version.into(),
            timestamp: None,
            description: String::new(),
            tables: BTreeMap::new(),
            edges: BTreeMap::new(),
            accesses: BTreeMap::new(),
            migration_count: 0,
        }
    }

    /// Override the default timestamp (`Utc::now`).
    pub fn with_timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.timestamp = Some(ts);
        self
    }

    /// Set the human-readable description.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// Replace the tables map.
    pub fn with_tables<I>(mut self, tables: I) -> Self
    where
        I: IntoIterator<Item = TableDefinition>,
    {
        self.tables = tables.into_iter().map(|t| (t.name.clone(), t)).collect();
        self
    }

    /// Replace the edges map.
    pub fn with_edges<I>(mut self, edges: I) -> Self
    where
        I: IntoIterator<Item = EdgeDefinition>,
    {
        self.edges = edges.into_iter().map(|e| (e.name.clone(), e)).collect();
        self
    }

    /// Replace the accesses map.
    pub fn with_accesses<I>(mut self, accesses: I) -> Self
    where
        I: IntoIterator<Item = AccessDefinition>,
    {
        self.accesses = accesses.into_iter().map(|a| (a.name.clone(), a)).collect();
        self
    }

    /// Set the number of migrations that had been applied at snapshot time.
    pub fn with_migration_count(mut self, count: u64) -> Self {
        self.migration_count = count;
        self
    }

    /// Finalise the builder and compute a checksum over the schema payload.
    pub fn build(self) -> VersionedSnapshot {
        let timestamp = self.timestamp.unwrap_or_else(Utc::now);
        let checksum = compute_checksum(&self.tables, &self.edges, &self.accesses);
        VersionedSnapshot {
            version: self.version,
            timestamp,
            description: self.description,
            tables: self.tables,
            edges: self.edges,
            accesses: self.accesses,
            checksum,
            migration_count: self.migration_count,
        }
    }
}

/// Compute a deterministic SHA-256 checksum over the schema payload.
fn compute_checksum(
    tables: &BTreeMap<String, TableDefinition>,
    edges: &BTreeMap<String, EdgeDefinition>,
    accesses: &BTreeMap<String, AccessDefinition>,
) -> String {
    // BTreeMap iterates in key order, so serialising is deterministic.
    let payload = serde_json::json!({
        "tables": tables,
        "edges": edges,
        "accesses": accesses,
    });
    // `serde_json::to_vec` on a `Value` is infallible for owned data.
    let bytes = serde_json::to_vec(&payload).unwrap_or_default();
    sha2_lite::sha256_hex(&bytes)
}

// ---------------------------------------------------------------------------
// Version graph
// ---------------------------------------------------------------------------

/// Node in a [`VersionGraph`] representing a single schema version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionNode {
    /// Version identifier.
    pub version: String,
    /// Parent version, if any.
    pub parent: Option<String>,
    /// Migration associated with this version.
    pub migration: Migration,
    /// Optional snapshot captured at this version.
    pub snapshot: Option<VersionedSnapshot>,
    /// Child versions (reverse edges, populated as descendants are added).
    pub children: Vec<String>,
}

/// Directed acyclic graph of schema versions connected by migrations.
///
/// Tracks the complete migration history as a graph, supporting forward
/// and backward traversal for rollbacks and comparisons.
///
/// ## Examples
///
/// ```
/// use std::path::PathBuf;
/// use surql::migration::{Migration, versioning::VersionGraph};
///
/// let mut graph = VersionGraph::new();
/// let m = Migration {
///     version: "20260102_120000".into(),
///     description: "init".into(),
///     path: PathBuf::from("20260102_120000.surql"),
///     up: vec![],
///     down: vec![],
///     checksum: None,
///     depends_on: vec![],
/// };
/// graph.add_version(m, None, None);
/// assert_eq!(graph.len(), 1);
/// ```
#[derive(Debug, Clone, Default)]
pub struct VersionGraph {
    nodes: HashMap<String, VersionNode>,
    root: Option<String>,
}

impl VersionGraph {
    /// Construct an empty graph.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the number of nodes in the graph.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Return `true` when the graph has no nodes.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Return the root version, if one has been set.
    pub fn root(&self) -> Option<&str> {
        self.root.as_deref()
    }

    /// Add a version to the graph.
    ///
    /// # Errors
    ///
    /// Returns [`SurqlError::Validation`] when the version is already
    /// present in the graph or when `parent` is specified but unknown.
    pub fn add_version(
        &mut self,
        migration: Migration,
        parent: Option<&str>,
        snapshot: Option<VersionedSnapshot>,
    ) -> Result<()> {
        let version = migration.version.clone();
        if self.nodes.contains_key(&version) {
            return Err(SurqlError::Validation {
                reason: format!("version {version:?} already exists in graph"),
            });
        }
        if let Some(parent_version) = parent {
            if !self.nodes.contains_key(parent_version) {
                return Err(SurqlError::Validation {
                    reason: format!("parent version {parent_version:?} not found for {version:?}"),
                });
            }
        }

        if let Some(parent_version) = parent {
            if let Some(parent_node) = self.nodes.get_mut(parent_version) {
                parent_node.children.push(version.clone());
            }
        } else if self.root.is_none() {
            self.root = Some(version.clone());
        }

        self.nodes.insert(
            version.clone(),
            VersionNode {
                version,
                parent: parent.map(ToOwned::to_owned),
                migration,
                snapshot,
                children: Vec::new(),
            },
        );

        Ok(())
    }

    /// Remove a version from the graph.
    ///
    /// All references to this version (from the root pointer, from parent
    /// nodes' `children` lists, and from child nodes' `parent` fields)
    /// are cleaned up.
    ///
    /// # Errors
    ///
    /// Returns [`SurqlError::Validation`] when the version is not present.
    pub fn remove_version(&mut self, version: &str) -> Result<VersionNode> {
        let node = self
            .nodes
            .remove(version)
            .ok_or_else(|| SurqlError::Validation {
                reason: format!("version {version:?} not found in graph"),
            })?;

        if let Some(parent_version) = &node.parent {
            if let Some(parent_node) = self.nodes.get_mut(parent_version) {
                parent_node.children.retain(|c| c != version);
            }
        }
        for child_version in &node.children {
            if let Some(child_node) = self.nodes.get_mut(child_version) {
                child_node.parent = None;
            }
        }
        if self.root.as_deref() == Some(version) {
            self.root = None;
        }

        Ok(node)
    }

    /// Look up a node by version.
    pub fn get(&self, version: &str) -> Option<&VersionNode> {
        self.nodes.get(version)
    }

    /// Return a vector of all version identifiers in insertion order.
    pub fn versions(&self) -> Vec<&str> {
        self.nodes.keys().map(String::as_str).collect()
    }

    /// Return every ancestor of `version`, from root down to the immediate
    /// parent. Returns an empty vector for a missing or root-level version.
    pub fn ancestors(&self, version: &str) -> Vec<String> {
        let mut ancestors: Vec<String> = Vec::new();
        let mut current = version;
        while let Some(node) = self.nodes.get(current) {
            if let Some(parent) = &node.parent {
                ancestors.insert(0, parent.clone());
                current = parent;
            } else {
                break;
            }
        }
        ancestors
    }

    /// Return every descendant of `version` in BFS order.
    pub fn descendants(&self, version: &str) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        let Some(start) = self.nodes.get(version) else {
            return out;
        };
        let mut queue: VecDeque<String> = start.children.iter().cloned().collect();
        let mut visited: BTreeSet<String> = start.children.iter().cloned().collect();
        while let Some(current) = queue.pop_front() {
            out.push(current.clone());
            if let Some(node) = self.nodes.get(&current) {
                for child in &node.children {
                    if visited.insert(child.clone()) {
                        queue.push_back(child.clone());
                    }
                }
            }
        }
        out
    }

    /// BFS path between two versions, or `None` when no path exists.
    pub fn path(&self, from_version: &str, to_version: &str) -> Option<Vec<String>> {
        if !self.nodes.contains_key(from_version) || !self.nodes.contains_key(to_version) {
            return None;
        }
        if from_version == to_version {
            return Some(vec![from_version.to_string()]);
        }
        let mut queue: VecDeque<(String, Vec<String>)> = VecDeque::new();
        queue.push_back((from_version.to_string(), vec![from_version.to_string()]));
        let mut visited: BTreeSet<String> = BTreeSet::new();
        visited.insert(from_version.to_string());

        while let Some((current, path)) = queue.pop_front() {
            if current == to_version {
                return Some(path);
            }
            let Some(node) = self.nodes.get(&current) else {
                continue;
            };
            // Children (forward edges).
            for child in &node.children {
                if visited.insert(child.clone()) {
                    let mut next = path.clone();
                    next.push(child.clone());
                    queue.push_back((child.clone(), next));
                }
            }
            // Parent (backward edge).
            if let Some(parent) = &node.parent {
                if visited.insert(parent.clone()) {
                    let mut next = path.clone();
                    next.push(parent.clone());
                    queue.push_back((parent.clone(), next));
                }
            }
        }
        None
    }
}

// ---------------------------------------------------------------------------
// Snapshot creation, persistence, and comparison
// ---------------------------------------------------------------------------

/// Create a snapshot of the current contents of a [`SchemaRegistry`].
///
/// # Errors
///
/// Returns [`SurqlError::Validation`] when `version` is empty.
pub fn create_snapshot(
    registry: &SchemaRegistry,
    version: impl Into<String>,
    description: impl Into<String>,
) -> Result<VersionedSnapshot> {
    let version = version.into();
    if version.trim().is_empty() {
        return Err(SurqlError::Validation {
            reason: "snapshot version must not be empty".to_string(),
        });
    }

    let tables: BTreeMap<String, TableDefinition> = registry.tables().into_iter().collect();
    let edges: BTreeMap<String, EdgeDefinition> = registry.edges().into_iter().collect();

    let snapshot = VersionedSnapshot::builder(version)
        .with_description(description)
        .with_tables(tables.into_values())
        .with_edges(edges.into_values())
        .build();
    Ok(snapshot)
}

/// Store a snapshot as a pretty-printed JSON file inside `directory`.
///
/// The filename is `<version>.json`. The target directory is created if
/// it does not already exist.
///
/// # Errors
///
/// Returns [`SurqlError::Io`] or [`SurqlError::Serialization`] if the
/// directory cannot be created or the file cannot be written.
pub fn store_snapshot(snapshot: &VersionedSnapshot, directory: &Path) -> Result<PathBuf> {
    fs::create_dir_all(directory).map_err(|e| SurqlError::Io {
        reason: format!(
            "failed to create snapshot directory {}: {e}",
            directory.display(),
        ),
    })?;
    let path = directory.join(snapshot.filename());
    let payload = serde_json::to_vec_pretty(snapshot).map_err(|e| SurqlError::Serialization {
        reason: format!("failed to serialise snapshot {}: {e}", snapshot.version),
    })?;
    fs::write(&path, payload).map_err(|e| SurqlError::Io {
        reason: format!("failed to write snapshot file {}: {e}", path.display()),
    })?;
    Ok(path)
}

/// Load a snapshot from disk.
///
/// # Errors
///
/// Returns [`SurqlError::Io`] when the file cannot be read or
/// [`SurqlError::Serialization`] when the JSON payload is invalid.
pub fn load_snapshot(path: &Path) -> Result<VersionedSnapshot> {
    let bytes = fs::read(path).map_err(|e| SurqlError::Io {
        reason: format!("failed to read snapshot file {}: {e}", path.display()),
    })?;
    let snap: VersionedSnapshot =
        serde_json::from_slice(&bytes).map_err(|e| SurqlError::Serialization {
            reason: format!("failed to parse snapshot file {}: {e}", path.display()),
        })?;
    Ok(snap)
}

/// List every snapshot in `directory`, sorted by version identifier.
///
/// Invalid JSON files are silently skipped (matches Python's forgiving
/// `list_snapshots` which logs-and-continues on parse errors).
///
/// # Errors
///
/// Returns [`SurqlError::MigrationHistory`] when the directory cannot be
/// enumerated (missing directories are treated as empty rather than an
/// error, matching the Python behaviour).
pub fn list_snapshots(directory: &Path) -> Result<Vec<VersionedSnapshot>> {
    if !directory.exists() {
        return Ok(Vec::new());
    }
    let iter = fs::read_dir(directory).map_err(|e| SurqlError::MigrationHistory {
        reason: format!(
            "failed to read snapshot directory {}: {e}",
            directory.display(),
        ),
    })?;

    let mut snapshots: Vec<VersionedSnapshot> = Vec::new();
    for entry in iter {
        let entry = entry.map_err(|e| SurqlError::MigrationHistory {
            reason: format!("failed to read entry in {}: {e}", directory.display()),
        })?;
        let path = entry.path();
        if path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(str::to_ascii_lowercase)
            != Some(VersionedSnapshot::FILE_EXTENSION.to_string())
        {
            continue;
        }
        if let Ok(snap) = load_snapshot(&path) {
            snapshots.push(snap);
        }
    }
    snapshots.sort_by(|a, b| a.version.cmp(&b.version));
    Ok(snapshots)
}

/// Structured difference between two [`VersionedSnapshot`]s.
///
/// All "added / removed / modified" lists are sorted by name for stable
/// output regardless of input ordering.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotComparison {
    /// Table names present in the target snapshot but missing from the source.
    pub tables_added: Vec<String>,
    /// Table names present in the source snapshot but missing from the target.
    pub tables_removed: Vec<String>,
    /// Table names present in both but with different serialised content.
    pub tables_modified: Vec<String>,
    /// Edge names present in the target but missing from the source.
    pub edges_added: Vec<String>,
    /// Edge names present in the source but missing from the target.
    pub edges_removed: Vec<String>,
    /// Edge names present in both but with different serialised content.
    pub edges_modified: Vec<String>,
    /// Access names present in the target but missing from the source.
    pub accesses_added: Vec<String>,
    /// Access names present in the source but missing from the target.
    pub accesses_removed: Vec<String>,
    /// Access names present in both but with different serialised content.
    pub accesses_modified: Vec<String>,
    /// `true` when the two snapshot checksums are equal.
    pub checksum_match: bool,
}

impl SnapshotComparison {
    /// `true` when no differences were recorded and the checksums match.
    pub fn is_identical(&self) -> bool {
        self.tables_added.is_empty()
            && self.tables_removed.is_empty()
            && self.tables_modified.is_empty()
            && self.edges_added.is_empty()
            && self.edges_removed.is_empty()
            && self.edges_modified.is_empty()
            && self.accesses_added.is_empty()
            && self.accesses_removed.is_empty()
            && self.accesses_modified.is_empty()
            && self.checksum_match
    }
}

/// Compare two snapshots and return a structured diff.
///
/// The `from_version` snapshot is treated as the baseline; the
/// `to_version` snapshot as the target. Items present in `to` but not in
/// `from` are "added", and vice versa.
pub fn compare_snapshots(
    from_version: &VersionedSnapshot,
    to_version: &VersionedSnapshot,
) -> SnapshotComparison {
    let mut out = SnapshotComparison {
        checksum_match: from_version.checksum == to_version.checksum,
        ..SnapshotComparison::default()
    };

    compare_maps(
        &from_version.tables,
        &to_version.tables,
        &mut out.tables_added,
        &mut out.tables_removed,
        &mut out.tables_modified,
    );
    compare_maps(
        &from_version.edges,
        &to_version.edges,
        &mut out.edges_added,
        &mut out.edges_removed,
        &mut out.edges_modified,
    );
    compare_maps(
        &from_version.accesses,
        &to_version.accesses,
        &mut out.accesses_added,
        &mut out.accesses_removed,
        &mut out.accesses_modified,
    );
    out
}

fn compare_maps<T>(
    from_map: &BTreeMap<String, T>,
    to_map: &BTreeMap<String, T>,
    added: &mut Vec<String>,
    removed: &mut Vec<String>,
    modified: &mut Vec<String>,
) where
    T: PartialEq,
{
    let from_keys: BTreeSet<&String> = from_map.keys().collect();
    let to_keys: BTreeSet<&String> = to_map.keys().collect();

    for k in to_keys.difference(&from_keys) {
        added.push((*k).clone());
    }
    for k in from_keys.difference(&to_keys) {
        removed.push((*k).clone());
    }
    for k in from_keys.intersection(&to_keys) {
        if from_map.get(*k) != to_map.get(*k) {
            modified.push((*k).clone());
        }
    }
    added.sort();
    removed.sort();
    modified.sort();
}

#[cfg(test)]
mod tests {
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
        assert!(!s.checksum.is_empty());
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
        assert!(list_snapshots(&missing).unwrap().is_empty());
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
        assert!(parent.children.is_empty());
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
        assert!(g.ancestors("v1").is_empty());
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
        assert!(g.descendants("v1").is_empty());
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
    fn graph_versions_lists_all() {
        let mut g = VersionGraph::new();
        g.add_version(mig("v1"), None, None).unwrap();
        g.add_version(mig("v2"), Some("v1"), None).unwrap();
        let mut v = g.versions();
        v.sort_unstable();
        assert_eq!(v, vec!["v1", "v2"]);
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
        assert!(diff.tables_removed.is_empty());
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
}
