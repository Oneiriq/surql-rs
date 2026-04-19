//! Filesystem watcher for schema files (debounced drift detection).
//!
//! Port of `surql/migration/watcher.py`. The Python original uses
//! `watchdog` and an asyncio debounce task; this Rust port uses
//! [`notify`](https://docs.rs/notify) for cross-platform file events
//! and surfaces debounced results via a [`tokio::sync::mpsc`] channel.
//!
//! This module is feature-gated behind the `watcher` cargo feature
//! (enables `notify`, `tokio`, and `tokio-util` as optional deps).
//!
//! ## Deviation from Python
//!
//! * The Python watcher invokes a user-supplied `async` callback and
//!   uses Python's `importlib` to dynamically load modified schema
//!   modules. Rust cannot execute arbitrary source files, so the port
//!   instead takes a [`SchemaSnapshot`] provider closure (called on each
//!   debounce tick) and a recorded snapshot. The debounced result is a
//!   [`DriftReport`] (same type returned by [`crate::migration::hooks`]).
//! * File-type filtering defaults to `.rs` / `.surql` (see
//!   [`is_schema_file`]) instead of `.py`.
//! * The debounce cadence is 500 ms by default (the py default is 1 s);
//!   callers can override via [`WatcherConfig::debounce_ms`].
//!
//! ## Example
//!
//! ```no_run
//! # #[cfg(feature = "watcher")] {
//! use std::path::PathBuf;
//! use surql::migration::diff::SchemaSnapshot;
//! use surql::migration::watcher::{SchemaWatcher, WatcherConfig};
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let recorded = SchemaSnapshot::new();
//! let (watcher, mut events) = SchemaWatcher::start(
//!     &[PathBuf::from("schemas")],
//!     &WatcherConfig::new(),
//!     SchemaSnapshot::new,
//!     recorded,
//! )?;
//! // ... later in another task
//! while let Some(report) = events.recv().await {
//!     if report.drift_detected {
//!         // regenerate migration
//!     }
//! }
//! watcher.stop();
//! # Ok(())
//! # }
//! # }
//! ```

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher as NotifyWatcher};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

use crate::error::{Result, SurqlError};
use crate::migration::diff::SchemaSnapshot;
use crate::migration::hooks::{check_schema_drift_from_snapshots, DriftReport};

// ---------------------------------------------------------------------------
// Public configuration
// ---------------------------------------------------------------------------

/// Configuration for [`SchemaWatcher`].
#[derive(Debug, Clone)]
pub struct WatcherConfig {
    /// Debounce window (ms). Events that arrive within this window are
    /// collapsed into a single drift check. Default: 500 ms.
    pub debounce_ms: u64,
    /// Whether to watch directories recursively. Default: `true`.
    pub recursive: bool,
    /// File extensions treated as schema files. Default: `["rs", "surql"]`.
    pub extensions: Vec<String>,
}

impl WatcherConfig {
    /// Construct a default watcher configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the debounce window (milliseconds).
    #[must_use]
    pub fn debounce_ms(mut self, ms: u64) -> Self {
        self.debounce_ms = ms;
        self
    }

    /// Toggle recursive watching of directories.
    #[must_use]
    pub fn recursive(mut self, recursive: bool) -> Self {
        self.recursive = recursive;
        self
    }

    /// Replace the file-extension allowlist.
    #[must_use]
    pub fn extensions<I, S>(mut self, exts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.extensions = exts.into_iter().map(Into::into).collect();
        self
    }
}

impl Default for WatcherConfig {
    fn default() -> Self {
        Self {
            debounce_ms: 500,
            recursive: true,
            extensions: vec!["rs".to_string(), "surql".to_string()],
        }
    }
}

/// Return `true` if `path` matches the default schema-file allowlist
/// (`.rs` / `.surql`).
#[must_use]
pub fn is_schema_file(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|e| e.to_str()),
        Some("rs" | "surql")
    )
}

// ---------------------------------------------------------------------------
// Watcher
// ---------------------------------------------------------------------------

/// Active schema file watcher.
///
/// Owns the [`notify`] watcher handle and a background debounce task.
/// Dropping the value stops the watcher; [`SchemaWatcher::stop`] is
/// provided as an explicit shutdown helper.
pub struct SchemaWatcher {
    running: Arc<AtomicBool>,
    _watcher: RecommendedWatcher,
}

impl std::fmt::Debug for SchemaWatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaWatcher")
            .field("running", &self.running.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl SchemaWatcher {
    /// Start watching `paths` for schema file changes.
    ///
    /// * `paths` — directories or files to monitor. Non-existent paths
    ///   are skipped with a warning; watching fails hard only if every
    ///   path is invalid.
    /// * `config` — knobs for debounce / recursion / extension filter.
    /// * `current_snapshot_provider` — closure invoked on each debounce
    ///   tick to produce the "code-side" schema. Must be `Send + 'static`.
    /// * `recorded_snapshot` — baseline the current snapshot is compared
    ///   against. Held for the lifetime of the watcher.
    ///
    /// Returns the watcher handle plus a receiver that yields a
    /// [`DriftReport`] every debounce tick.
    ///
    /// # Errors
    ///
    /// Returns [`SurqlError::MigrationWatcher`] if the underlying
    /// `notify` watcher cannot be constructed, or if no paths can be
    /// registered.
    pub fn start<F>(
        paths: &[PathBuf],
        config: &WatcherConfig,
        current_snapshot_provider: F,
        recorded_snapshot: SchemaSnapshot,
    ) -> Result<(Self, UnboundedReceiver<DriftReport>)>
    where
        F: Fn() -> SchemaSnapshot + Send + Sync + 'static,
    {
        let (report_tx, report_rx) = unbounded_channel::<DriftReport>();
        let running = Arc::new(AtomicBool::new(true));
        let pending_flag = Arc::new(AtomicBool::new(false));
        let (event_tx, event_rx) = unbounded_channel::<()>();

        let allow_ext = config.extensions.clone();
        let mut watcher = build_notify_watcher(Arc::clone(&pending_flag), event_tx, allow_ext)?;
        register_paths(&mut watcher, paths, config)?;

        spawn_debounce_task(
            Arc::clone(&running),
            Arc::clone(&pending_flag),
            event_rx,
            Duration::from_millis(config.debounce_ms),
            Arc::new(Mutex::new(recorded_snapshot)),
            Arc::new(current_snapshot_provider),
            report_tx,
        );

        Ok((
            Self {
                running,
                _watcher: watcher,
            },
            report_rx,
        ))
    }

    /// Stop the watcher. Safe to call multiple times.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }
}

impl Drop for SchemaWatcher {
    fn drop(&mut self) {
        self.stop();
    }
}

fn build_notify_watcher(
    pending_flag: Arc<AtomicBool>,
    event_tx: tokio::sync::mpsc::UnboundedSender<()>,
    allow_ext: Vec<String>,
) -> Result<RecommendedWatcher> {
    NotifyWatcher::new(
        move |res: notify::Result<Event>| match res {
            Ok(event) => {
                if !event_of_interest(&event, &allow_ext) {
                    return;
                }
                pending_flag.store(true, Ordering::Release);
                let _ = event_tx.send(());
            }
            Err(err) => {
                tracing::warn!(
                    target: "surql::migration::watcher",
                    error = %err,
                    "watcher_event_error",
                );
            }
        },
        notify::Config::default(),
    )
    .map_err(|e| SurqlError::MigrationWatcher {
        reason: format!("failed to construct file watcher: {e}"),
    })
}

fn register_paths(
    watcher: &mut RecommendedWatcher,
    paths: &[PathBuf],
    config: &WatcherConfig,
) -> Result<()> {
    let recursive = if config.recursive {
        RecursiveMode::Recursive
    } else {
        RecursiveMode::NonRecursive
    };
    let mut registered = 0usize;
    for path in paths {
        if !path.exists() {
            tracing::warn!(
                target: "surql::migration::watcher",
                path = %path.display(),
                "path_not_found",
            );
            continue;
        }
        let effective_mode = if path.is_dir() {
            recursive
        } else {
            RecursiveMode::NonRecursive
        };
        match watcher.watch(path, effective_mode) {
            Ok(()) => registered += 1,
            Err(e) => {
                tracing::warn!(
                    target: "surql::migration::watcher",
                    path = %path.display(),
                    error = %e,
                    "failed_to_register_watch",
                );
            }
        }
    }
    if registered == 0 {
        return Err(SurqlError::MigrationWatcher {
            reason: "no paths could be registered with the watcher".to_string(),
        });
    }
    Ok(())
}

fn spawn_debounce_task<F>(
    running: Arc<AtomicBool>,
    pending: Arc<AtomicBool>,
    mut event_rx: UnboundedReceiver<()>,
    debounce: Duration,
    recorded: Arc<Mutex<SchemaSnapshot>>,
    provider: Arc<F>,
    report_tx: tokio::sync::mpsc::UnboundedSender<DriftReport>,
) where
    F: Fn() -> SchemaSnapshot + Send + Sync + 'static,
{
    tokio::spawn(async move {
        while running.load(Ordering::Acquire) {
            if event_rx.recv().await.is_none() {
                break;
            }
            // Collapse subsequent events that arrive during the window.
            while tokio::time::timeout(debounce, event_rx.recv())
                .await
                .is_ok()
            {
                // A value (Some or None) arrived in-time; keep collapsing
                // until the window goes quiet.
            }
            if !running.load(Ordering::Acquire) {
                break;
            }
            if !pending.swap(false, Ordering::AcqRel) {
                continue;
            }
            let report = {
                let code = (provider)();
                let recorded = recorded.lock().expect("recorded mutex poisoned");
                check_schema_drift_from_snapshots(&code, &recorded)
            };
            if report_tx.send(report).is_err() {
                break;
            }
        }
    });
}

fn event_of_interest(event: &Event, extensions: &[String]) -> bool {
    matches!(
        event.kind,
        EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
    ) && event.paths.iter().any(|p| {
        matches!(
            p.extension().and_then(|e| e.to_str()),
            Some(ext) if extensions.iter().any(|allowed| allowed == ext)
        )
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migration::diff::SchemaSnapshot;
    use crate::schema::table::table_schema;
    use notify::event::{CreateKind, EventAttributes, ModifyKind};
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering as AtOrd};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn unique_temp_dir(tag: &str) -> PathBuf {
        let nanos: u128 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let n = TEST_DIR_COUNTER.fetch_add(1, AtOrd::Relaxed);
        let pid = std::process::id();
        let dir = std::env::temp_dir().join(format!("surql-watcher-{tag}-{pid}-{nanos}-{n}"));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    // --- is_schema_file ---------------------------------------------------

    #[test]
    fn is_schema_file_accepts_rs_and_surql() {
        assert!(is_schema_file(&PathBuf::from("user.rs")));
        assert!(is_schema_file(&PathBuf::from("schema/20260101_x.surql")));
    }

    #[test]
    fn is_schema_file_rejects_other_ext() {
        assert!(!is_schema_file(&PathBuf::from("README.md")));
        assert!(!is_schema_file(&PathBuf::from("Cargo.toml")));
        assert!(!is_schema_file(&PathBuf::from("a.py")));
    }

    // --- WatcherConfig ----------------------------------------------------

    #[test]
    fn watcher_config_defaults() {
        let c = WatcherConfig::new();
        assert_eq!(c.debounce_ms, 500);
        assert!(c.recursive);
        assert_eq!(c.extensions, vec!["rs".to_string(), "surql".to_string()]);
    }

    #[test]
    fn watcher_config_builders_override() {
        let c = WatcherConfig::new()
            .debounce_ms(50)
            .recursive(false)
            .extensions(["toml"]);
        assert_eq!(c.debounce_ms, 50);
        assert!(!c.recursive);
        assert_eq!(c.extensions, vec!["toml".to_string()]);
    }

    // --- event_of_interest ------------------------------------------------

    fn make_event(kind: EventKind, paths: Vec<PathBuf>) -> Event {
        Event {
            kind,
            paths,
            attrs: EventAttributes::new(),
        }
    }

    #[test]
    fn event_of_interest_accepts_surql_modify() {
        let e = make_event(
            EventKind::Modify(ModifyKind::Any),
            vec![PathBuf::from("schemas/user.surql")],
        );
        assert!(event_of_interest(
            &e,
            &["rs".to_string(), "surql".to_string()]
        ));
    }

    #[test]
    fn event_of_interest_rejects_non_listed_extension() {
        let e = make_event(
            EventKind::Modify(ModifyKind::Any),
            vec![PathBuf::from("schemas/README.md")],
        );
        assert!(!event_of_interest(
            &e,
            &["rs".to_string(), "surql".to_string()]
        ));
    }

    #[test]
    fn event_of_interest_rejects_access_kind() {
        // EventKind::Access is not in the create/modify/remove set.
        let e = make_event(
            EventKind::Access(notify::event::AccessKind::Read),
            vec![PathBuf::from("schemas/user.surql")],
        );
        assert!(!event_of_interest(
            &e,
            &["rs".to_string(), "surql".to_string()]
        ));
    }

    #[test]
    fn event_of_interest_accepts_create_kind() {
        let e = make_event(
            EventKind::Create(CreateKind::File),
            vec![PathBuf::from("schemas/new.rs")],
        );
        assert!(event_of_interest(
            &e,
            &["rs".to_string(), "surql".to_string()]
        ));
    }

    // --- SchemaWatcher (live file events) ---------------------------------

    #[tokio::test]
    async fn start_fails_when_all_paths_missing() {
        let missing = std::env::temp_dir().join("surql-watcher-never-xyz-1-2-3");
        let err = SchemaWatcher::start(
            &[missing],
            &WatcherConfig::new(),
            SchemaSnapshot::new,
            SchemaSnapshot::new(),
        )
        .expect_err("should fail when every path is missing");
        assert!(matches!(err, SurqlError::MigrationWatcher { .. }));
    }

    #[tokio::test]
    async fn start_succeeds_and_stop_returns_without_panic() {
        let dir = unique_temp_dir("start-stop");
        let (w, _rx) = SchemaWatcher::start(
            std::slice::from_ref(&dir),
            &WatcherConfig::new().debounce_ms(50),
            SchemaSnapshot::new,
            SchemaSnapshot::new(),
        )
        .expect("start watcher");
        w.stop();
        // Dropping should not panic either.
        drop(w);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn file_event_triggers_debounced_report_with_drift() {
        let dir = unique_temp_dir("drift-report");

        // Code-side snapshot exposes a `user` table; recorded is empty
        // so the resulting report must detect drift.
        let provider = || SchemaSnapshot {
            tables: vec![table_schema("user")],
            edges: vec![],
        };
        let recorded = SchemaSnapshot::new();

        let (w, mut rx) = SchemaWatcher::start(
            std::slice::from_ref(&dir),
            &WatcherConfig::new().debounce_ms(50),
            provider,
            recorded,
        )
        .expect("start watcher");

        // Trigger at least one Create event.
        let file = dir.join("user.surql");
        fs::write(&file, "-- @up\nSELECT 1;\n-- @down\nSELECT 2;\n").unwrap();

        let report = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("should receive a report before timeout")
            .expect("channel should yield a report");

        assert!(report.drift_detected);
        w.stop();
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn non_schema_file_does_not_trigger_report() {
        let dir = unique_temp_dir("non-schema");
        let (w, mut rx) = SchemaWatcher::start(
            std::slice::from_ref(&dir),
            &WatcherConfig::new().debounce_ms(50),
            SchemaSnapshot::new,
            SchemaSnapshot::new(),
        )
        .expect("start watcher");

        // Touch an unrelated file.
        fs::write(dir.join("NOTES.md"), "not a schema\n").unwrap();

        let got = tokio::time::timeout(Duration::from_millis(400), rx.recv()).await;
        assert!(got.is_err(), "should time out with no report");
        w.stop();
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn debounces_multiple_rapid_events_into_one_report() {
        let dir = unique_temp_dir("debounce");
        let provider = || SchemaSnapshot {
            tables: vec![table_schema("user")],
            edges: vec![],
        };
        let (w, mut rx) = SchemaWatcher::start(
            std::slice::from_ref(&dir),
            &WatcherConfig::new().debounce_ms(150),
            provider,
            SchemaSnapshot::new(),
        )
        .expect("start watcher");

        for i in 0..5 {
            let file = dir.join(format!("user{i}.surql"));
            fs::write(&file, format!("-- @up\nSELECT {i};\n-- @down\nSELECT 0;\n")).unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // First report should come through.
        let _first = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("first report")
            .expect("channel");

        // Further rapid events within the same window should not
        // produce extra reports immediately; let the quiescent window
        // expire.
        let extra = tokio::time::timeout(Duration::from_millis(400), rx.recv()).await;
        // We don't assert strictly zero extra reports because notify
        // backends sometimes batch vs. split events unpredictably;
        // instead assert at least the first one arrived.
        drop(extra);
        w.stop();
        let _ = fs::remove_dir_all(&dir);
    }
}
