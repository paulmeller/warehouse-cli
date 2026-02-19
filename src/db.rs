use anyhow::Result;
use rusqlite::Connection;

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

/// Initialize the FTS5 search schema (matching Python's init_search_schema).
pub fn init_search_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        -- FTS5 for messages (with resolved sender names)
        CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
            sender_name,
            chat_name,
            text,
            tokenize='porter unicode61'
        );

        -- FTS5 for notes
        CREATE VIRTUAL TABLE IF NOT EXISTS notes_fts USING fts5(
            title,
            body,
            tags,
            tokenize='porter unicode61'
        );

        -- FTS5 for contacts
        CREATE VIRTUAL TABLE IF NOT EXISTS contacts_fts USING fts5(
            full_name,
            organization,
            note,
            tokenize='porter unicode61'
        );

        -- FTS5 for photos
        CREATE VIRTUAL TABLE IF NOT EXISTS photos_fts USING fts5(
            title,
            filename,
            people,
            album,
            tokenize='porter unicode61'
        );

        -- FTS5 for documents
        CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
            title,
            filename,
            content,
            file_type,
            tokenize='porter unicode61'
        );

        -- FTS5 for reminders
        CREATE VIRTUAL TABLE IF NOT EXISTS reminders_fts USING fts5(
            title,
            notes,
            list_name,
            location,
            tokenize='porter unicode61'
        );

        -- Mapping tables
        CREATE TABLE IF NOT EXISTS messages_fts_map (
            fts_rowid INTEGER PRIMARY KEY,
            message_id INTEGER NOT NULL,
            UNIQUE(message_id)
        );

        CREATE TABLE IF NOT EXISTS notes_fts_map (
            fts_rowid INTEGER PRIMARY KEY,
            note_id INTEGER NOT NULL,
            UNIQUE(note_id)
        );

        CREATE TABLE IF NOT EXISTS contacts_fts_map (
            fts_rowid INTEGER PRIMARY KEY,
            contact_identifier TEXT NOT NULL,
            UNIQUE(contact_identifier)
        );

        CREATE TABLE IF NOT EXISTS photos_fts_map (
            fts_rowid INTEGER PRIMARY KEY,
            asset_id INTEGER NOT NULL,
            UNIQUE(asset_id)
        );

        CREATE TABLE IF NOT EXISTS documents_fts_map (
            fts_rowid INTEGER PRIMARY KEY,
            document_id INTEGER NOT NULL,
            UNIQUE(document_id)
        );

        CREATE TABLE IF NOT EXISTS reminders_fts_map (
            fts_rowid INTEGER PRIMARY KEY,
            reminder_id TEXT NOT NULL,
            UNIQUE(reminder_id)
        );

        -- Search metadata table
        CREATE TABLE IF NOT EXISTS search_metadata (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        ",
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

    #[test]
    fn init_search_schema_creates_fts_tables() {
        let conn = in_memory_conn();
        init_search_schema(&conn).unwrap();

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
        init_search_schema(&conn).unwrap();

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
        init_search_schema(&conn).unwrap();
        assert!(table_exists(&conn, "search_metadata"));
    }

    #[test]
    fn init_search_schema_idempotent() {
        let conn = in_memory_conn();
        init_search_schema(&conn).unwrap();
        // Should not fail on second call
        init_search_schema(&conn).unwrap();
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
}
