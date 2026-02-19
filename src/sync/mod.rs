pub mod contacts;
pub mod documents;
pub mod messages;
pub mod notes;
pub mod photos;
pub mod reminders;

use crate::config::Config;
use crate::connector::{Connector, ConnectorRegistry};
use rusqlite::Connection;

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

/// Sync all data sources.
pub fn sync_all(
    conn: &Connection,
    config: &Config,
    registry: &ConnectorRegistry,
) -> Vec<SyncResult> {
    let connectors: Vec<&dyn Connector> = registry.all().iter().map(|c| c.as_ref()).collect();
    run_connectors(conn, config, &connectors)
}

/// Sync specific data sources.
pub fn sync_sources(
    conn: &Connection,
    config: &Config,
    sources: &[String],
    registry: &ConnectorRegistry,
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

    run_connectors(conn, config, &filtered)
}

fn run_connectors(
    conn: &Connection,
    config: &Config,
    connectors: &[&dyn Connector],
) -> Vec<SyncResult> {
    let mut results = Vec::new();

    for connector in connectors {
        let name = connector.name();
        print!("Syncing {name}...");
        match connector.extract(conn, config) {
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
