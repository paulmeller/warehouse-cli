pub mod contacts;
pub mod documents;
pub mod messages;
pub mod monarch;
pub mod notes;
pub mod photos;
pub mod pocketsmith;
pub mod reminders;
pub mod twitter;

use anyhow::Result;
use rusqlite::Connection;

use crate::config::Config;

pub struct SyncResult {
    pub source: String,
    pub count: usize,
    pub status: SyncStatus,
}

pub enum SyncStatus {
    Success,
    Skipped(String),
    Failed(String),
}

type Extractor = fn(&Connection, &Config) -> Result<usize>;

fn get_extractors() -> Vec<(&'static str, Extractor)> {
    vec![
        ("contacts", contacts::extract as Extractor),
        ("imessages", messages::extract as Extractor),
        ("photos", photos::extract as Extractor),
        ("reminders", reminders::extract as Extractor),
        ("obsidian", notes::extract as Extractor),
        ("documents", documents::extract as Extractor),
        ("twitter", twitter::extract as Extractor),
        ("monarch", monarch::extract as Extractor),
        ("pocketsmith", pocketsmith::extract as Extractor),
    ]
}

/// Sync all data sources.
pub fn sync_all(conn: &Connection, config: &Config) -> Vec<SyncResult> {
    let extractors = get_extractors();
    run_extractors(conn, config, &extractors)
}

/// Sync specific data sources.
pub fn sync_sources(conn: &Connection, config: &Config, sources: &[String]) -> Vec<SyncResult> {
    let all = get_extractors();
    let filtered: Vec<_> = all
        .into_iter()
        .filter(|(name, _)| sources.iter().any(|s| s == name))
        .collect();

    if filtered.is_empty() {
        let names: Vec<&str> = get_extractors().iter().map(|(n, _)| *n).collect();
        eprintln!("No matching sources. Available: {}", names.join(", "));
        return Vec::new();
    }

    run_extractors(conn, config, &filtered)
}

fn run_extractors(
    conn: &Connection,
    config: &Config,
    extractors: &[(&str, Extractor)],
) -> Vec<SyncResult> {
    let mut results = Vec::new();

    for (name, extractor) in extractors {
        print!("Syncing {name}...");
        match extractor(conn, config) {
            Ok(count) => {
                println!(" {count} items");
                results.push(SyncResult {
                    source: name.to_string(),
                    count,
                    status: SyncStatus::Success,
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
                    results.push(SyncResult {
                        source: name.to_string(),
                        count: 0,
                        status: SyncStatus::Skipped(msg),
                    });
                } else {
                    println!(" FAILED: {msg}");
                    results.push(SyncResult {
                        source: name.to_string(),
                        count: 0,
                        status: SyncStatus::Failed(msg),
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
        let status = match &r.status {
            SyncStatus::Success => format!("{:>8} items", r.count),
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
