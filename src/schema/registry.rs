//! Global registry for code-defined table and edge schemas.
//!
//! Port of `surql/schema/registry.py`. Provides [`SchemaRegistry`] — a
//! process-wide singleton that tracks [`TableDefinition`] and
//! [`EdgeDefinition`] values registered from application code.
//!
//! The registered definitions can later be rendered with
//! [`crate::schema::generate_schema_sql`] to create the database schema, or
//! compared against the live database to detect drift.
//!
//! ## Thread safety
//!
//! Unlike the Python port (which uses `threading.Lock`), the Rust registry
//! stores its maps behind [`RwLock`]s so reads do not contend with each
//! other. The singleton itself lives in a [`OnceLock`].
//!
//! ## Examples
//!
//! ```
//! use surql::schema::{
//!     clear_registry, get_registered_tables, register_table, table_schema,
//! };
//!
//! clear_registry();
//! let user = register_table(table_schema("user"));
//! assert_eq!(user.name, "user");
//! assert!(get_registered_tables().contains_key("user"));
//! # clear_registry();
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{OnceLock, RwLock};

use super::edge::EdgeDefinition;
use super::table::TableDefinition;

/// Global registry for code-defined table and edge schemas.
///
/// Behaves as a singleton — always acquire it through [`get_registry`].
/// Methods take `&self` and use interior mutability ([`RwLock`]) to allow
/// concurrent reads and exclusive writes.
#[derive(Debug, Default)]
pub struct SchemaRegistry {
    tables: RwLock<HashMap<String, TableDefinition>>,
    edges: RwLock<HashMap<String, EdgeDefinition>>,
    schema_files: RwLock<Vec<PathBuf>>,
}

impl SchemaRegistry {
    /// Create a new, empty registry.
    ///
    /// Prefer [`get_registry`] for normal use — this constructor exists for
    /// isolated unit-testing of the registry type.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a table schema (replaces any existing entry with the same name).
    pub fn register_table(&self, table: TableDefinition) {
        let name = table.name.clone();
        if let Ok(mut guard) = self.tables.write() {
            guard.insert(name, table);
        }
    }

    /// Register an edge schema (replaces any existing entry with the same name).
    pub fn register_edge(&self, edge: EdgeDefinition) {
        let name = edge.name.clone();
        if let Ok(mut guard) = self.edges.write() {
            guard.insert(name, edge);
        }
    }

    /// Look up a registered table by name.
    pub fn get_table(&self, name: &str) -> Option<TableDefinition> {
        self.tables
            .read()
            .ok()
            .and_then(|guard| guard.get(name).cloned())
    }

    /// Look up a registered edge by name.
    pub fn get_edge(&self, name: &str) -> Option<EdgeDefinition> {
        self.edges
            .read()
            .ok()
            .and_then(|guard| guard.get(name).cloned())
    }

    /// Return a snapshot of all registered tables keyed by name.
    pub fn tables(&self) -> HashMap<String, TableDefinition> {
        self.tables
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    /// Return a snapshot of all registered edges keyed by name.
    pub fn edges(&self) -> HashMap<String, EdgeDefinition> {
        self.edges
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    /// Return the names of all registered tables.
    pub fn table_names(&self) -> Vec<String> {
        self.tables
            .read()
            .map(|guard| guard.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Return the names of all registered edges.
    pub fn edge_names(&self) -> Vec<String> {
        self.edges
            .read()
            .map(|guard| guard.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Number of registered tables.
    pub fn table_count(&self) -> usize {
        self.tables.read().map_or(0, |guard| guard.len())
    }

    /// Number of registered edges.
    pub fn edge_count(&self) -> usize {
        self.edges.read().map_or(0, |guard| guard.len())
    }

    /// Track a schema file that has been loaded.
    ///
    /// Duplicate paths are ignored (the list is de-duplicated on insert).
    pub fn add_schema_file(&self, path: impl AsRef<Path>) {
        let path = path.as_ref().to_path_buf();
        if let Ok(mut guard) = self.schema_files.write() {
            if !guard.iter().any(|p| p == &path) {
                guard.push(path);
            }
        }
    }

    /// Return a snapshot of all tracked schema file paths.
    pub fn schema_files(&self) -> Vec<PathBuf> {
        self.schema_files
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    /// Clear every registered table, edge, and schema file.
    pub fn clear(&self) {
        if let Ok(mut guard) = self.tables.write() {
            guard.clear();
        }
        if let Ok(mut guard) = self.edges.write() {
            guard.clear();
        }
        if let Ok(mut guard) = self.schema_files.write() {
            guard.clear();
        }
    }
}

/// Return the process-wide [`SchemaRegistry`] instance.
///
/// The returned reference has `'static` lifetime — it is safe to cache.
pub fn get_registry() -> &'static SchemaRegistry {
    static REGISTRY: OnceLock<SchemaRegistry> = OnceLock::new();
    REGISTRY.get_or_init(SchemaRegistry::new)
}

/// Register a table schema with the global registry and return it.
///
/// Convenience wrapper around [`SchemaRegistry::register_table`] that
/// returns the table for inline usage (`let t = register_table(...)`).
pub fn register_table(table: TableDefinition) -> TableDefinition {
    get_registry().register_table(table.clone());
    table
}

/// Register an edge schema with the global registry and return it.
///
/// Convenience wrapper around [`SchemaRegistry::register_edge`] that
/// returns the edge for inline usage.
pub fn register_edge(edge: EdgeDefinition) -> EdgeDefinition {
    get_registry().register_edge(edge.clone());
    edge
}

/// Clear every registered table, edge, and schema file in the global registry.
pub fn clear_registry() {
    get_registry().clear();
}

/// Snapshot of all tables registered in the global registry.
pub fn get_registered_tables() -> HashMap<String, TableDefinition> {
    get_registry().tables()
}

/// Snapshot of all edges registered in the global registry.
pub fn get_registered_edges() -> HashMap<String, EdgeDefinition> {
    get_registry().edges()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::edge::{edge_schema, typed_edge};
    use crate::schema::table::table_schema;
    use std::sync::{Arc, Barrier};
    use std::thread;

    // The `global` tests lock this mutex to serialise access to the process-wide
    // registry (individual tests run in parallel by default and would otherwise
    // stomp on each other's entries). `SchemaRegistry::new()` instances are
    // untouched by this.
    fn with_clean_global<F: FnOnce()>(f: F) {
        use std::sync::Mutex;
        static LOCK: Mutex<()> = Mutex::new(());
        let guard = LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        clear_registry();
        f();
        clear_registry();
        drop(guard);
    }

    // ---- SchemaRegistry (local instance) ----

    #[test]
    fn local_new_is_empty() {
        let r = SchemaRegistry::new();
        assert_eq!(r.table_count(), 0);
        assert_eq!(r.edge_count(), 0);
        assert!(r.tables().is_empty());
        assert!(r.edges().is_empty());
    }

    #[test]
    fn local_register_table_round_trip() {
        let r = SchemaRegistry::new();
        let t = table_schema("user");
        r.register_table(t.clone());
        assert_eq!(r.get_table("user"), Some(t));
        assert_eq!(r.table_count(), 1);
    }

    #[test]
    fn local_register_edge_round_trip() {
        let r = SchemaRegistry::new();
        let e = typed_edge("likes", "user", "post");
        r.register_edge(e.clone());
        assert_eq!(r.get_edge("likes"), Some(e));
        assert_eq!(r.edge_count(), 1);
    }

    #[test]
    fn local_get_missing_returns_none() {
        let r = SchemaRegistry::new();
        assert!(r.get_table("missing").is_none());
        assert!(r.get_edge("missing").is_none());
    }

    #[test]
    fn local_re_register_replaces_previous() {
        let r = SchemaRegistry::new();
        r.register_table(table_schema("user"));
        r.register_table(
            table_schema("user").with_mode(crate::schema::table::TableMode::Schemaless),
        );
        assert_eq!(r.table_count(), 1);
        assert_eq!(
            r.get_table("user").unwrap().mode,
            crate::schema::table::TableMode::Schemaless
        );
    }

    #[test]
    fn local_tables_returns_snapshot() {
        let r = SchemaRegistry::new();
        r.register_table(table_schema("a"));
        let snapshot = r.tables();
        r.register_table(table_schema("b"));
        assert_eq!(snapshot.len(), 1); // snapshot is independent
        assert_eq!(r.table_count(), 2);
    }

    #[test]
    fn local_table_names_and_edge_names() {
        let r = SchemaRegistry::new();
        r.register_table(table_schema("a"));
        r.register_table(table_schema("b"));
        r.register_edge(typed_edge("e1", "a", "b"));
        let mut tnames = r.table_names();
        tnames.sort();
        assert_eq!(tnames, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(r.edge_names(), vec!["e1".to_string()]);
    }

    #[test]
    fn local_clear_resets_everything() {
        let r = SchemaRegistry::new();
        r.register_table(table_schema("user"));
        r.register_edge(typed_edge("likes", "user", "post"));
        r.add_schema_file("schema.rs");
        r.clear();
        assert_eq!(r.table_count(), 0);
        assert_eq!(r.edge_count(), 0);
        assert!(r.schema_files().is_empty());
    }

    #[test]
    fn local_schema_files_are_unique() {
        let r = SchemaRegistry::new();
        r.add_schema_file("a.rs");
        r.add_schema_file("a.rs");
        r.add_schema_file("b.rs");
        let files = r.schema_files();
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn local_multiple_tables_round_trip() {
        let r = SchemaRegistry::new();
        r.register_table(table_schema("a"));
        r.register_table(table_schema("b"));
        r.register_table(table_schema("c"));
        assert_eq!(r.table_count(), 3);
        assert_eq!(r.tables().len(), 3);
    }

    #[test]
    fn local_edges_returns_snapshot() {
        let r = SchemaRegistry::new();
        r.register_edge(typed_edge("e1", "a", "b"));
        let snap = r.edges();
        r.register_edge(typed_edge("e2", "a", "b"));
        assert_eq!(snap.len(), 1);
        assert_eq!(r.edge_count(), 2);
    }

    #[test]
    fn local_empty_schema_file_list() {
        let r = SchemaRegistry::new();
        assert!(r.schema_files().is_empty());
    }

    // ---- Global singleton ----

    #[test]
    fn global_registry_is_stable() {
        let r1 = get_registry();
        let r2 = get_registry();
        assert!(std::ptr::eq(r1, r2));
    }

    #[test]
    fn global_register_table_roundtrip() {
        with_clean_global(|| {
            let t = register_table(table_schema("alpha"));
            assert_eq!(t.name, "alpha");
            assert!(get_registered_tables().contains_key("alpha"));
        });
    }

    #[test]
    fn global_register_edge_roundtrip() {
        with_clean_global(|| {
            let e = register_edge(typed_edge("rel", "alpha", "beta"));
            assert_eq!(e.name, "rel");
            assert!(get_registered_edges().contains_key("rel"));
        });
    }

    #[test]
    fn global_clear_registry_empties_everything() {
        with_clean_global(|| {
            register_table(table_schema("x"));
            register_edge(typed_edge("r", "x", "x"));
            clear_registry();
            assert_eq!(get_registry().table_count(), 0);
            assert_eq!(get_registry().edge_count(), 0);
        });
    }

    #[test]
    fn global_register_returns_input_value() {
        with_clean_global(|| {
            let t = table_schema("x");
            let returned = register_table(t.clone());
            assert_eq!(t, returned);
        });
    }

    #[test]
    fn global_register_edge_returns_input_value() {
        with_clean_global(|| {
            let e = edge_schema("r").with_from_table("x").with_to_table("y");
            let returned = register_edge(e.clone());
            assert_eq!(e, returned);
        });
    }

    // ---- Concurrency smoke test ----

    #[test]
    fn concurrent_registers_do_not_panic() {
        let registry = Arc::new(SchemaRegistry::new());
        let threads = 8_usize;
        let per_thread = 32_usize;
        let barrier = Arc::new(Barrier::new(threads));

        let mut handles = Vec::with_capacity(threads);
        for tid in 0..threads {
            let registry = Arc::clone(&registry);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                for i in 0..per_thread {
                    registry.register_table(table_schema(format!("t_{tid}_{i}")));
                    registry.register_edge(typed_edge(
                        format!("e_{tid}_{i}"),
                        format!("t_{tid}_{i}"),
                        "shared",
                    ));
                    // Read path exercised concurrently too.
                    let _ = registry.table_count();
                    let _ = registry.edges();
                }
            }));
        }

        for handle in handles {
            handle.join().expect("worker thread panicked");
        }

        assert_eq!(registry.table_count(), threads * per_thread);
        assert_eq!(registry.edge_count(), threads * per_thread);
    }
}
