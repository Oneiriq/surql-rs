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
use std::path::{Component, Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{Result, SurqlError};
use crate::migration::discovery::{compare_versions, sha256_hex};
use crate::migration::models::Migration;
use crate::schema::access::AccessDefinition;
use crate::schema::bucket::BucketDefinition;
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
    /// Object-storage bucket definitions captured at this version, keyed by
    /// bucket name. Defaults to empty so snapshots written before bucket
    /// support still deserialise.
    #[serde(default)]
    pub buckets: BTreeMap<String, BucketDefinition>,
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
    buckets: BTreeMap<String, BucketDefinition>,
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
            buckets: BTreeMap::new(),
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

    /// Replace the buckets map.
    pub fn with_buckets<I>(mut self, buckets: I) -> Self
    where
        I: IntoIterator<Item = BucketDefinition>,
    {
        self.buckets = buckets.into_iter().map(|b| (b.name.clone(), b)).collect();
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
        let checksum = compute_checksum(&self.tables, &self.edges, &self.accesses, &self.buckets);
        VersionedSnapshot {
            version: self.version,
            timestamp,
            description: self.description,
            tables: self.tables,
            edges: self.edges,
            accesses: self.accesses,
            buckets: self.buckets,
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
    buckets: &BTreeMap<String, BucketDefinition>,
) -> String {
    // BTreeMap iterates in key order, so serialising is deterministic.
    let payload = serde_json::json!({
        "tables": tables,
        "edges": edges,
        "accesses": accesses,
        "buckets": buckets,
    });
    // `serde_json::to_vec` on a `Value` is infallible for owned data.
    let bytes = serde_json::to_vec(&payload).unwrap_or_default();
    sha256_hex(&bytes)
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
///     squashed_from: vec![],
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

    /// Return every version identifier, in version order (runs of digits
    /// compared as numbers, as migrations are ordered).
    pub fn versions(&self) -> Vec<&str> {
        let mut versions: Vec<&str> = self.nodes.keys().map(String::as_str).collect();
        versions.sort_by(|a, b| compare_versions(a, b));
        versions
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
    let buckets: BTreeMap<String, BucketDefinition> = registry.buckets().into_iter().collect();

    let snapshot = VersionedSnapshot::builder(version)
        .with_description(description)
        .with_tables(tables.into_values())
        .with_edges(edges.into_values())
        .with_buckets(buckets.into_values())
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
/// Returns [`SurqlError::Validation`] when the version cannot serve as a
/// file name inside `directory` (empty, or containing a path separator or
/// drive prefix, such as `../x` or an absolute path), and
/// [`SurqlError::Io`] or [`SurqlError::Serialization`] if the directory
/// cannot be created or the file cannot be written.
pub fn store_snapshot(snapshot: &VersionedSnapshot, directory: &Path) -> Result<PathBuf> {
    let filename = snapshot.filename();
    let is_single_name = !snapshot.version.trim().is_empty()
        && !snapshot.version.contains(['/', '\\', ':', '\0'])
        && matches!(
            Path::new(&filename)
                .components()
                .collect::<Vec<_>>()
                .as_slice(),
            [Component::Normal(_)]
        );
    if !is_single_name {
        return Err(SurqlError::Validation {
            reason: format!(
                "snapshot version {:?} cannot be used as a file name",
                snapshot.version
            ),
        });
    }
    fs::create_dir_all(directory).map_err(|e| SurqlError::Io {
        reason: format!(
            "failed to create snapshot directory {}: {e}",
            directory.display(),
        ),
    })?;
    let path = directory.join(filename);
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
    snapshots.sort_by(|a, b| compare_versions(&a.version, &b.version));
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
    /// Bucket names present in the target but missing from the source.
    #[serde(default)]
    pub buckets_added: Vec<String>,
    /// Bucket names present in the source but missing from the target.
    #[serde(default)]
    pub buckets_removed: Vec<String>,
    /// Bucket names present in both but with different content.
    #[serde(default)]
    pub buckets_modified: Vec<String>,
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
            && self.buckets_added.is_empty()
            && self.buckets_removed.is_empty()
            && self.buckets_modified.is_empty()
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
    compare_maps(
        &from_version.buckets,
        &to_version.buckets,
        &mut out.buckets_added,
        &mut out.buckets_removed,
        &mut out.buckets_modified,
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
mod tests;
