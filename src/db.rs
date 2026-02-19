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

        -- FTS5 for Twitter bookmarks
        CREATE VIRTUAL TABLE IF NOT EXISTS bookmarks_fts USING fts5(
            text,
            author_handle,
            author_name,
            tokenize='porter unicode61'
        );

        -- FTS5 for Twitter likes
        CREATE VIRTUAL TABLE IF NOT EXISTS likes_fts USING fts5(
            text,
            author_handle,
            author_name,
            tokenize='porter unicode61'
        );

        -- FTS5 for transactions
        CREATE VIRTUAL TABLE IF NOT EXISTS transactions_fts USING fts5(
            merchant_name,
            category_name,
            notes,
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

        CREATE TABLE IF NOT EXISTS bookmarks_fts_map (
            fts_rowid INTEGER PRIMARY KEY,
            tweet_id TEXT NOT NULL,
            UNIQUE(tweet_id)
        );

        CREATE TABLE IF NOT EXISTS likes_fts_map (
            fts_rowid INTEGER PRIMARY KEY,
            tweet_id TEXT NOT NULL,
            UNIQUE(tweet_id)
        );

        CREATE TABLE IF NOT EXISTS transactions_fts_map (
            fts_rowid INTEGER PRIMARY KEY,
            transaction_id TEXT NOT NULL,
            source TEXT DEFAULT 'monarch',
            UNIQUE(transaction_id, source)
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
