use anyhow::Result;
use chrono::{DateTime, Utc};
use rusqlite::Connection;

use crate::connector::ConnectorRegistry;

/// A row from the sync_runs table.
#[allow(dead_code)]
pub struct SyncRun {
    pub id: i64,
    pub connector_name: String,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub status: String,
    pub rows_synced: i64,
    pub error_message: Option<String>,
    pub sync_mode: String,
    pub resume_cursor: Option<String>,
}

/// Open the warehouse database.
pub fn open(db_path: &str) -> Result<Connection> {
    let conn = Connection::open(db_path)?;
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;
    Ok(conn)
}

/// Get row count for a table (returns 0 if table doesn't exist).
pub fn table_count(conn: &Connection, table: &str) -> i64 {
    // Validate table name to prevent injection (only allow alphanumeric + underscore)
    if !table.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return 0;
    }
    conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
        row.get(0)
    })
    .unwrap_or(0)
}

/// Initialize the FTS5 search schema from registered connectors.
pub fn init_search_schema(conn: &Connection, registry: &ConnectorRegistry) -> Result<()> {
    for connector in registry.all() {
        if let Some(sql) = connector.fts_schema_sql() {
            conn.execute_batch(sql)?;
        }
    }

    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS search_metadata (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );",
    )?;
    Ok(())
}

/// Ensure the search_metadata table exists (legacy, used by fallback test).
#[cfg(test)]
pub fn ensure_metadata_table(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS search_metadata (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );",
    )?;
    Ok(())
}

/// Get the last successful sync timestamp for a connector.
/// Checks sync_runs first (new), falls back to legacy search_metadata.
pub fn get_last_sync(conn: &Connection, name: &str) -> Option<DateTime<Utc>> {
    // Try sync_runs first (new)
    if table_exists(conn, "sync_runs") {
        if let Ok(s) = conn.query_row(
            "SELECT started_at FROM sync_runs
             WHERE connector_name = ?1 AND status = 'success'
             ORDER BY started_at DESC LIMIT 1",
            [name],
            |row| row.get::<_, String>(0),
        ) {
            if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
                return Some(dt.with_timezone(&Utc));
            }
        }
    }

    // Fall back to legacy search_metadata
    let key = format!("last_sync.{name}");
    conn.query_row(
        "SELECT value FROM search_metadata WHERE key = ?1",
        [&key],
        |row| row.get::<_, String>(0),
    )
    .ok()
    .and_then(|s| {
        DateTime::parse_from_rfc3339(&s)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    })
}

/// Record the last successful sync timestamp for a connector (legacy, used by fallback test).
#[cfg(test)]
pub fn set_last_sync(conn: &Connection, name: &str, ts: &DateTime<Utc>) -> Result<()> {
    let key = format!("last_sync.{name}");
    let value = ts.to_rfc3339();
    conn.execute(
        "INSERT INTO search_metadata(key, value, updated_at) VALUES (?1, ?2, CURRENT_TIMESTAMP)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP",
        rusqlite::params![key, value],
    )?;
    Ok(())
}

/// Ensure the sync_runs table exists.
pub fn ensure_sync_runs_table(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sync_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            connector_name TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            status TEXT NOT NULL DEFAULT 'running',
            rows_synced INTEGER DEFAULT 0,
            error_message TEXT,
            sync_mode TEXT NOT NULL,
            resume_cursor TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_sync_runs_connector
            ON sync_runs(connector_name, started_at DESC);",
    )?;
    Ok(())
}

/// Insert a new sync run with status='running'. Returns the run id.
pub fn insert_sync_run(
    conn: &Connection,
    connector_name: &str,
    started_at: &DateTime<Utc>,
    sync_mode: &str,
) -> Result<i64> {
    conn.execute(
        "INSERT INTO sync_runs (connector_name, started_at, status, sync_mode)
         VALUES (?1, ?2, 'running', ?3)",
        rusqlite::params![connector_name, started_at.to_rfc3339(), sync_mode],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Complete a sync run by setting ended_at, status, rows_synced, and error_message.
pub fn complete_sync_run(
    conn: &Connection,
    run_id: i64,
    status: &str,
    rows_synced: usize,
    error_message: Option<&str>,
) -> Result<()> {
    conn.execute(
        "UPDATE sync_runs SET ended_at = ?1, status = ?2, rows_synced = ?3, error_message = ?4
         WHERE id = ?5",
        rusqlite::params![
            Utc::now().to_rfc3339(),
            status,
            rows_synced as i64,
            error_message,
            run_id
        ],
    )?;
    Ok(())
}

/// Get recent sync run history.
pub fn get_sync_history(conn: &Connection, limit: usize) -> Result<Vec<SyncRun>> {
    // Table may not exist yet
    if !table_exists(conn, "sync_runs") {
        return Ok(Vec::new());
    }
    let mut stmt = conn.prepare(
        "SELECT id, connector_name, started_at, ended_at, status, rows_synced,
                error_message, sync_mode, resume_cursor
         FROM sync_runs ORDER BY started_at DESC LIMIT ?1",
    )?;
    let rows = stmt.query_map(rusqlite::params![limit as i64], |row| {
        Ok(SyncRun {
            id: row.get(0)?,
            connector_name: row.get(1)?,
            started_at: row.get(2)?,
            ended_at: row.get(3)?,
            status: row.get(4)?,
            rows_synced: row.get(5)?,
            error_message: row.get(6)?,
            sync_mode: row.get(7)?,
            resume_cursor: row.get(8)?,
        })
    })?;
    Ok(rows.filter_map(|r| r.ok()).collect())
}

/// Get column names of an existing table using PRAGMA table_info.
pub fn get_table_columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    // Validate table name
    if !table.chars().all(|c| c.is_alphanumeric() || c == '_') {
        anyhow::bail!("Invalid table name: {table}");
    }
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .filter_map(|r| r.ok())
        .collect();
    Ok(columns)
}

/// Update the resume_cursor for a sync run.
pub fn update_sync_cursor(conn: &Connection, run_id: i64, cursor_json: &str) -> Result<()> {
    conn.execute(
        "UPDATE sync_runs SET resume_cursor = ?1 WHERE id = ?2",
        rusqlite::params![cursor_json, run_id],
    )?;
    Ok(())
}

/// Get the resume cursor from the most recent failed run for a connector.
pub fn get_last_resume_cursor(conn: &Connection, connector_name: &str) -> Option<String> {
    if !table_exists(conn, "sync_runs") {
        return None;
    }
    conn.query_row(
        "SELECT resume_cursor FROM sync_runs
         WHERE connector_name = ?1 AND status = 'failed' AND resume_cursor IS NOT NULL
         ORDER BY started_at DESC LIMIT 1",
        [connector_name],
        |row| row.get::<_, String>(0),
    )
    .ok()
}

/// Clear resume cursors for a connector (after successful sync).
pub fn clear_resume_cursors(conn: &Connection, connector_name: &str) -> Result<()> {
    conn.execute(
        "UPDATE sync_runs SET resume_cursor = NULL
         WHERE connector_name = ?1 AND resume_cursor IS NOT NULL",
        [connector_name],
    )?;
    Ok(())
}

/// Check if a table exists in the database.
pub fn table_exists(conn: &Connection, table: &str) -> bool {
    conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        [table],
        |row| row.get::<_, i64>(0),
    )
    .unwrap_or(0)
        > 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn in_memory_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        conn
    }

    // ========== init_search_schema ==========

    fn test_registry() -> ConnectorRegistry {
        crate::connector::default_registry()
    }

    #[test]
    fn init_search_schema_creates_fts_tables() {
        let conn = in_memory_conn();
        let registry = test_registry();
        init_search_schema(&conn, &registry).unwrap();

        for table in &[
            "messages_fts",
            "notes_fts",
            "contacts_fts",
            "photos_fts",
            "documents_fts",
            "reminders_fts",
        ] {
            assert!(table_exists(&conn, table), "FTS table {table} should exist");
        }
    }

    #[test]
    fn init_search_schema_creates_mapping_tables() {
        let conn = in_memory_conn();
        let registry = test_registry();
        init_search_schema(&conn, &registry).unwrap();

        for table in &[
            "messages_fts_map",
            "notes_fts_map",
            "contacts_fts_map",
            "photos_fts_map",
            "documents_fts_map",
            "reminders_fts_map",
        ] {
            assert!(
                table_exists(&conn, table),
                "Mapping table {table} should exist"
            );
        }
    }

    #[test]
    fn init_search_schema_creates_metadata_table() {
        let conn = in_memory_conn();
        let registry = test_registry();
        init_search_schema(&conn, &registry).unwrap();
        assert!(table_exists(&conn, "search_metadata"));
    }

    #[test]
    fn init_search_schema_idempotent() {
        let conn = in_memory_conn();
        let registry = test_registry();
        init_search_schema(&conn, &registry).unwrap();
        // Should not fail on second call
        init_search_schema(&conn, &registry).unwrap();
        assert!(table_exists(&conn, "messages_fts"));
    }

    // ========== table_exists ==========

    #[test]
    fn table_exists_true_for_existing() {
        let conn = in_memory_conn();
        conn.execute_batch("CREATE TABLE test_table (id INTEGER)")
            .unwrap();
        assert!(table_exists(&conn, "test_table"));
    }

    #[test]
    fn table_exists_false_for_missing() {
        let conn = in_memory_conn();
        assert!(!table_exists(&conn, "nonexistent_table"));
    }

    // ========== table_count ==========

    #[test]
    fn table_count_empty_table() {
        let conn = in_memory_conn();
        conn.execute_batch("CREATE TABLE items (id INTEGER)")
            .unwrap();
        assert_eq!(table_count(&conn, "items"), 0);
    }

    #[test]
    fn table_count_with_rows() {
        let conn = in_memory_conn();
        conn.execute_batch("CREATE TABLE items (id INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO items VALUES (1)", []).unwrap();
        conn.execute("INSERT INTO items VALUES (2)", []).unwrap();
        conn.execute("INSERT INTO items VALUES (3)", []).unwrap();
        assert_eq!(table_count(&conn, "items"), 3);
    }

    #[test]
    fn table_count_missing_table() {
        let conn = in_memory_conn();
        assert_eq!(table_count(&conn, "nonexistent"), 0);
    }

    #[test]
    fn table_count_sql_injection_blocked() {
        let conn = in_memory_conn();
        // Attempt SQL injection through table name
        assert_eq!(table_count(&conn, "items; DROP TABLE items"), 0);
    }

    // ========== sync_runs ==========

    #[test]
    fn insert_and_complete_sync_run() {
        let conn = in_memory_conn();
        ensure_sync_runs_table(&conn).unwrap();
        let started = Utc::now();
        let run_id = insert_sync_run(&conn, "test_connector", &started, "full").unwrap();
        assert!(run_id > 0);

        complete_sync_run(&conn, run_id, "success", 42, None).unwrap();

        let row: (String, i64, Option<String>) = conn
            .query_row(
                "SELECT status, rows_synced, error_message FROM sync_runs WHERE id = ?1",
                [run_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(row.0, "success");
        assert_eq!(row.1, 42);
        assert!(row.2.is_none());
    }

    #[test]
    fn get_last_sync_from_sync_runs() {
        let conn = in_memory_conn();
        ensure_sync_runs_table(&conn).unwrap();

        let ts1 = Utc::now() - chrono::Duration::hours(2);
        let ts2 = Utc::now() - chrono::Duration::hours(1);

        let r1 = insert_sync_run(&conn, "myconn", &ts1, "full").unwrap();
        complete_sync_run(&conn, r1, "success", 10, None).unwrap();
        let r2 = insert_sync_run(&conn, "myconn", &ts2, "incremental").unwrap();
        complete_sync_run(&conn, r2, "success", 5, None).unwrap();

        let last = get_last_sync(&conn, "myconn").unwrap();
        // Should pick the most recent successful run (ts2)
        assert!((last - ts2).num_seconds().abs() < 2);
    }

    #[test]
    fn get_last_sync_falls_back_to_metadata() {
        let conn = in_memory_conn();
        ensure_metadata_table(&conn).unwrap();
        // Don't create sync_runs table — should fall back
        let ts = Utc::now();
        set_last_sync(&conn, "legacy", &ts).unwrap();

        let last = get_last_sync(&conn, "legacy").unwrap();
        assert!((last - ts).num_seconds().abs() < 2);
    }

    #[test]
    fn get_sync_history_returns_recent() {
        let conn = in_memory_conn();
        ensure_sync_runs_table(&conn).unwrap();

        for i in 0..5 {
            let ts = Utc::now() - chrono::Duration::hours(5 - i);
            let rid = insert_sync_run(&conn, "conn", &ts, "full").unwrap();
            complete_sync_run(&conn, rid, "success", (i * 10) as usize, None).unwrap();
        }

        let history = get_sync_history(&conn, 3).unwrap();
        assert_eq!(history.len(), 3);
        // Most recent first
        assert!(history[0].started_at > history[1].started_at);
    }

    #[test]
    fn get_table_columns_returns_names() {
        let conn = in_memory_conn();
        conn.execute_batch("CREATE TABLE test_cols (id INTEGER, name TEXT, age INTEGER)")
            .unwrap();
        let cols = get_table_columns(&conn, "test_cols").unwrap();
        assert_eq!(cols, vec!["id", "name", "age"]);
    }

    // ========== cursor persistence ==========

    #[test]
    fn cursor_persistence_and_retrieval() {
        let conn = in_memory_conn();
        ensure_sync_runs_table(&conn).unwrap();
        let ts = Utc::now();
        let rid = insert_sync_run(&conn, "notion", &ts, "full").unwrap();
        let cursor_json = r#"{"table":"notion_pages","cursor":"abc123","page":3}"#;
        update_sync_cursor(&conn, rid, cursor_json).unwrap();
        complete_sync_run(&conn, rid, "failed", 50, Some("timeout")).unwrap();

        let retrieved = get_last_resume_cursor(&conn, "notion");
        assert_eq!(retrieved.unwrap(), cursor_json);
    }

    #[test]
    fn cursor_cleared_on_success() {
        let conn = in_memory_conn();
        ensure_sync_runs_table(&conn).unwrap();
        let ts = Utc::now();
        let rid = insert_sync_run(&conn, "notion", &ts, "full").unwrap();
        update_sync_cursor(&conn, rid, r#"{"cursor":"old"}"#).unwrap();
        complete_sync_run(&conn, rid, "failed", 0, Some("err")).unwrap();

        // Simulate successful re-run clearing cursors
        clear_resume_cursors(&conn, "notion").unwrap();
        assert!(get_last_resume_cursor(&conn, "notion").is_none());
    }
}
