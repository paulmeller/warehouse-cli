use anyhow::Result;
use rusqlite::Connection;
use std::path::PathBuf;

/// Get the audit database path: ~/.warehouse/audit.db
fn audit_db_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".warehouse")
        .join("audit.db")
}

/// Open the audit database, creating it if needed.
pub fn open_audit_db() -> Result<Connection> {
    let path = audit_db_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(&path)?;
    init_audit_schema(&conn)?;
    Ok(conn)
}

/// Initialize the audit schema.
fn init_audit_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL DEFAULT (datetime('now')),
            source TEXT NOT NULL,
            query TEXT,
            records_returned INTEGER NOT NULL DEFAULT 0,
            fields_redacted TEXT,
            blocked INTEGER NOT NULL DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp);
        CREATE INDEX IF NOT EXISTS idx_audit_source ON audit_log(source);",
    )?;
    Ok(())
}

/// Log a query to the audit trail.
#[allow(dead_code)]
pub fn log_query(
    source: &str,
    query: Option<&str>,
    records_returned: usize,
    blocked: bool,
    redacted_fields: Option<&[String]>,
) -> Result<()> {
    let settings = crate::config::load_settings();
    if !settings.audit_enabled {
        return Ok(());
    }

    let conn = open_audit_db()?;
    let redacted = redacted_fields.map(|f| f.join(", "));

    conn.execute(
        "INSERT INTO audit_log (source, query, records_returned, fields_redacted, blocked)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![
            source,
            query,
            records_returned as i64,
            redacted,
            blocked as i32,
        ],
    )?;

    Ok(())
}

/// Log multiple source queries at once (e.g., from a cross-source search).
pub fn log_search(
    query: &str,
    sources_queried: &[String],
    sources_blocked: &[String],
    records_returned: usize,
    redacted_fields: &std::collections::HashMap<String, Vec<String>>,
) -> Result<()> {
    let settings = crate::config::load_settings();
    if !settings.audit_enabled {
        return Ok(());
    }

    let conn = open_audit_db()?;

    for source in sources_queried {
        let redacted = redacted_fields.get(source).map(|f| f.join(", "));
        conn.execute(
            "INSERT INTO audit_log (source, query, records_returned, fields_redacted, blocked)
             VALUES (?1, ?2, ?3, ?4, 0)",
            rusqlite::params![source, query, records_returned as i64, redacted],
        )?;
    }

    for source in sources_blocked {
        conn.execute(
            "INSERT INTO audit_log (source, query, records_returned, fields_redacted, blocked)
             VALUES (?1, ?2, 0, NULL, 1)",
            rusqlite::params![source, query],
        )?;
    }

    Ok(())
}

/// Audit digest entry for a source.
#[allow(dead_code)]
struct DigestEntry {
    source: String,
    query_count: i64,
    total_records: i64,
    blocked_count: i64,
    redacted_fields: Option<String>,
}

/// Print audit digest for the last N days.
pub fn print_digest(days: u32, source_filter: Option<&str>, blocked_only: bool) -> Result<()> {
    let conn = open_audit_db()?;

    let cutoff = chrono::Local::now() - chrono::Duration::days(days as i64);
    let cutoff_str = cutoff.format("%Y-%m-%d").to_string();

    if blocked_only {
        print_blocked_queries(&conn, &cutoff_str)?;
        return Ok(());
    }

    if let Some(source) = source_filter {
        print_source_detail(&conn, source, &cutoff_str)?;
        return Ok(());
    }

    // General digest
    let mut stmt = conn.prepare(
        "SELECT
            source,
            COUNT(*) as query_count,
            SUM(records_returned) as total_records,
            SUM(blocked) as blocked_count,
            GROUP_CONCAT(DISTINCT fields_redacted) as redacted
         FROM audit_log
         WHERE timestamp >= ?1
         GROUP BY source
         ORDER BY query_count DESC",
    )?;

    let entries: Vec<DigestEntry> = stmt
        .query_map([&cutoff_str], |row| {
            Ok(DigestEntry {
                source: row.get("source")?,
                query_count: row.get("query_count")?,
                total_records: row.get::<_, Option<i64>>("total_records")?.unwrap_or(0),
                blocked_count: row.get::<_, Option<i64>>("blocked_count")?.unwrap_or(0),
                redacted_fields: row.get("redacted")?,
            })
        })?
        .filter_map(|r| r.ok())
        .collect();

    let total_queries: i64 = entries.iter().map(|e| e.query_count).sum();

    println!(
        "Last {} day{} \u{2014} {} queries",
        days,
        if days == 1 { "" } else { "s" },
        total_queries
    );
    println!();

    for entry in &entries {
        if entry.blocked_count == entry.query_count {
            // All queries were blocked
            println!(
                "  {:<14} {:>3} queries   (blocked \u{2014} access disabled)",
                entry.source, entry.query_count,
            );
        } else {
            let mut detail = format!("{} queries", entry.query_count);
            if let Some(ref redacted) = entry.redacted_fields {
                if !redacted.is_empty() {
                    detail.push_str(&format!("   ({} redacted on all)", redacted));
                }
            }
            println!("  {:<14} {}", entry.source, detail);
        }
    }

    // Show sources with no queries
    let queried_sources: Vec<&str> = entries.iter().map(|e| e.source.as_str()).collect();
    for source in crate::governance::ALL_SOURCES {
        if !queried_sources.contains(source) {
            let perm = crate::governance::get_source_permission(source);
            if !perm.access {
                println!(
                    "  {:<14}   0 queries   (blocked \u{2014} access disabled)",
                    source
                );
            } else {
                println!("  {:<14}   0 queries", source);
            }
        }
    }

    Ok(())
}

fn print_blocked_queries(conn: &Connection, cutoff: &str) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT timestamp, source, query
         FROM audit_log
         WHERE blocked = 1 AND timestamp >= ?1
         ORDER BY timestamp DESC
         LIMIT 100",
    )?;

    let rows: Vec<(String, String, Option<String>)> = stmt
        .query_map([cutoff], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })?
        .filter_map(|r| r.ok())
        .collect();

    if rows.is_empty() {
        println!("No blocked queries found.");
        return Ok(());
    }

    println!("Blocked queries:");
    println!();
    for (ts, source, query) in &rows {
        let q = query.as_deref().unwrap_or("<browse>");
        println!("  [{}] {} \u{2014} \"{}\"", ts, source, q);
    }

    Ok(())
}

#[allow(clippy::type_complexity)]
fn print_source_detail(conn: &Connection, source: &str, cutoff: &str) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT timestamp, query, records_returned, fields_redacted, blocked
         FROM audit_log
         WHERE source = ?1 AND timestamp >= ?2
         ORDER BY timestamp DESC
         LIMIT 50",
    )?;

    let rows: Vec<(String, Option<String>, i64, Option<String>, bool)> = stmt
        .query_map(rusqlite::params![source, cutoff], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, bool>(4)?,
            ))
        })?
        .filter_map(|r| r.ok())
        .collect();

    if rows.is_empty() {
        println!("No queries for '{}' in this period.", source);
        return Ok(());
    }

    println!("Queries for '{}':", source);
    println!();
    for (ts, query, count, redacted, blocked) in &rows {
        let q = query.as_deref().unwrap_or("<browse>");
        if *blocked {
            println!("  [{}] \"{}\" \u{2014} BLOCKED", ts, q);
        } else {
            let mut line = format!("  [{}] \"{}\" \u{2014} {} results", ts, q, count);
            if let Some(r) = redacted {
                if !r.is_empty() {
                    line.push_str(&format!(" (redacted: {})", r));
                }
            }
            println!("{}", line);
        }
    }

    Ok(())
}

/// Clean up old audit entries based on retention policy.
#[allow(dead_code)]
pub fn cleanup_audit(retention_days: u32) -> Result<usize> {
    let conn = open_audit_db()?;
    let cutoff = chrono::Local::now() - chrono::Duration::days(retention_days as i64);
    let cutoff_str = cutoff.format("%Y-%m-%dT%H:%M:%S").to_string();

    let deleted = conn.execute("DELETE FROM audit_log WHERE timestamp < ?1", [&cutoff_str])?;

    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn in_memory_audit() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_audit_schema(&conn).unwrap();
        conn
    }

    #[test]
    fn audit_schema_creates_table() {
        let conn = in_memory_audit();
        let exists: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='audit_log'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(exists);
    }

    #[test]
    fn audit_schema_idempotent() {
        let conn = in_memory_audit();
        init_audit_schema(&conn).unwrap(); // second call should not fail
    }

    #[test]
    fn audit_insert_and_query() {
        let conn = in_memory_audit();
        conn.execute(
            "INSERT INTO audit_log (source, query, records_returned, blocked)
             VALUES ('notes', 'test query', 5, 0)",
            [],
        )
        .unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM audit_log", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn audit_insert_blocked() {
        let conn = in_memory_audit();
        conn.execute(
            "INSERT INTO audit_log (source, query, records_returned, blocked)
             VALUES ('messages', 'search term', 0, 1)",
            [],
        )
        .unwrap();

        let blocked: bool = conn
            .query_row(
                "SELECT blocked FROM audit_log WHERE source = 'messages'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(blocked);
    }

    #[test]
    fn audit_insert_with_redacted_fields() {
        let conn = in_memory_audit();
        conn.execute(
            "INSERT INTO audit_log (source, query, records_returned, fields_redacted, blocked)
             VALUES ('contacts', 'alice', 3, 'phone, address', 0)",
            [],
        )
        .unwrap();

        let redacted: String = conn
            .query_row(
                "SELECT fields_redacted FROM audit_log WHERE source = 'contacts'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(redacted, "phone, address");
    }
}
