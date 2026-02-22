#[cfg(target_os = "macos")]
pub mod contacts;
pub mod documents;
#[cfg(target_os = "macos")]
pub mod messages;
pub mod notes;
#[cfg(target_os = "macos")]
pub mod photos;
#[cfg(target_os = "macos")]
pub mod reminders;

use chrono::{DateTime, Duration, Utc};
use rusqlite::Connection;

use crate::config::Config;
use crate::connector::{Connector, ConnectorRegistry};
use crate::db;

/// Context passed to each connector's extract() method.
pub struct SyncContext {
    /// When Some, only fetch records modified after this timestamp.
    pub since: Option<DateTime<Utc>>,
    /// True if --full was explicitly requested.
    pub full: bool,
    /// When the current sync run started (used for soft-delete detection).
    pub started_at: DateTime<Utc>,
    /// Resume cursor JSON from a previous failed run (Feature 4).
    pub resume_cursor: Option<String>,
    /// The sync_runs row id for this run (Feature 4).
    pub sync_run_id: Option<i64>,
}

impl SyncContext {
    pub fn full_sync(started_at: DateTime<Utc>) -> Self {
        Self {
            since: None,
            full: true,
            started_at,
            resume_cursor: None,
            sync_run_id: None,
        }
    }

    pub fn incremental(since: DateTime<Utc>, started_at: DateTime<Utc>) -> Self {
        Self {
            since: Some(since),
            full: false,
            started_at,
            resume_cursor: None,
            sync_run_id: None,
        }
    }

    pub fn is_incremental(&self) -> bool {
        self.since.is_some() && !self.full
    }
}

/// Whether the sync was full or incremental.
pub enum SyncMode {
    Full,
    Incremental,
}

pub struct SyncResult {
    pub source: String,
    pub count: usize,
    pub status: SyncStatus,
    pub mode: SyncMode,
}

pub enum SyncStatus {
    Success,
    Skipped(String),
    Failed(String),
}

/// Sync all data sources.
pub fn sync_all(
    conn: &Connection,
    config: &Config,
    registry: &ConnectorRegistry,
    force_full: bool,
) -> Vec<SyncResult> {
    let connectors: Vec<&dyn Connector> = registry.all().iter().map(|c| c.as_ref()).collect();
    run_connectors(conn, config, &connectors, force_full)
}

/// Sync specific data sources.
pub fn sync_sources(
    conn: &Connection,
    config: &Config,
    sources: &[String],
    registry: &ConnectorRegistry,
    force_full: bool,
) -> Vec<SyncResult> {
    let filtered: Vec<&dyn Connector> = registry
        .all()
        .iter()
        .filter(|c| sources.iter().any(|s| s == c.name()))
        .map(|c| c.as_ref())
        .collect();

    if filtered.is_empty() {
        let names = registry.names();
        eprintln!("No matching sources. Available: {}", names.join(", "));
        return Vec::new();
    }

    run_connectors(conn, config, &filtered, force_full)
}

fn run_connectors(
    conn: &Connection,
    config: &Config,
    connectors: &[&dyn Connector],
    force_full: bool,
) -> Vec<SyncResult> {
    // Ensure sync_runs table exists (replaces ensure_metadata_table)
    if let Err(e) = db::ensure_sync_runs_table(conn) {
        eprintln!("Warning: could not create sync_runs table: {e}");
    }

    let mut results = Vec::new();

    for connector in connectors {
        let name = connector.name();

        // Capture sync start time with 60s buffer before extraction
        let sync_started_at = Utc::now() - Duration::seconds(60);

        // Determine sync mode: incremental if we have a last_sync and --full not requested
        let (mut ctx, mode) = if force_full {
            (SyncContext::full_sync(sync_started_at), SyncMode::Full)
        } else {
            match db::get_last_sync(conn, name) {
                Some(last) => (
                    SyncContext::incremental(last, sync_started_at),
                    SyncMode::Incremental,
                ),
                None => (SyncContext::full_sync(sync_started_at), SyncMode::Full),
            }
        };

        let mode_label = match mode {
            SyncMode::Full => "full",
            SyncMode::Incremental => "incremental",
        };
        print!("Syncing {name} ({mode_label})...");

        // Insert sync run record
        let run_id = match db::insert_sync_run(conn, name, &sync_started_at, mode_label) {
            Ok(id) => id,
            Err(e) => {
                eprintln!(" could not create sync run: {e}");
                continue;
            }
        };

        // Populate resume cursor from last failed run
        ctx.resume_cursor = db::get_last_resume_cursor(conn, name);
        ctx.sync_run_id = Some(run_id);

        match connector.extract(conn, config, &ctx) {
            Ok(count) => {
                println!(" {count} items");
                let _ = db::complete_sync_run(conn, run_id, "success", count, None);
                // Clear any stale resume cursors on success
                let _ = db::clear_resume_cursors(conn, name);

                results.push(SyncResult {
                    source: name.to_string(),
                    count,
                    status: SyncStatus::Success,
                    mode,
                });
            }
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("permission")
                    || msg.contains("not found")
                    || msg.contains("not available")
                    || msg.contains("disabled")
                {
                    println!(" skipped ({msg})");
                    let _ = db::complete_sync_run(conn, run_id, "skipped", 0, Some(&msg));
                    results.push(SyncResult {
                        source: name.to_string(),
                        count: 0,
                        status: SyncStatus::Skipped(msg),
                        mode,
                    });
                } else {
                    println!(" FAILED: {msg}");
                    let _ = db::complete_sync_run(conn, run_id, "failed", 0, Some(&msg));
                    results.push(SyncResult {
                        source: name.to_string(),
                        count: 0,
                        status: SyncStatus::Failed(msg),
                        mode,
                    });
                }
            }
        }
    }

    results
}

/// Print sync results summary.
pub fn print_summary(results: &[SyncResult]) {
    println!();
    println!("Sync Summary:");
    let mut total = 0;
    for r in results {
        let mode_tag = match r.mode {
            SyncMode::Full => "[full]",
            SyncMode::Incremental => "[incr]",
        };
        let status = match &r.status {
            SyncStatus::Success => format!("{:>8} items {mode_tag}", r.count),
            SyncStatus::Skipped(msg) => format!("skipped: {msg}"),
            SyncStatus::Failed(msg) => format!("FAILED: {msg}"),
        };
        println!("  {:<15} {status}", r.source);
        if matches!(r.status, SyncStatus::Success) {
            total += r.count;
        }
    }
    println!("  {:<15} {:>8} items", "TOTAL", total);
}
