use anyhow::Result;
use rusqlite::Connection;

use crate::connector::ConnectorRegistry;

#[cfg(test)]
use crate::db;

/// Rebuild all FTS5 indexes from registered connectors and return counts.
pub fn rebuild_all_fts(
    conn: &Connection,
    registry: &ConnectorRegistry,
) -> Result<Vec<(String, i64)>> {
    let mut counts = Vec::new();
    for connector in registry.all() {
        let count = connector.populate_fts(conn)?;
        counts.push((connector.name().to_string(), count));
    }
    Ok(counts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn setup_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        let registry = crate::connector::default_registry();
        db::init_search_schema(&conn, &registry).unwrap();
        conn
    }

    fn test_registry() -> ConnectorRegistry {
        crate::connector::default_registry()
    }

    // ========== Notes FTS ==========

    #[test]
    fn rebuild_fts_with_notes() {
        let conn = setup_db();
        conn.execute_batch(
            "CREATE TABLE obsidian_notes (
                id INTEGER PRIMARY KEY, title TEXT, body TEXT, content TEXT,
                file_path TEXT, created_at TEXT, modified_at TEXT, vault_name TEXT
            );
            CREATE TABLE obsidian_tags (note_id INTEGER, tag TEXT);
            INSERT INTO obsidian_notes (id, title, body) VALUES (1, 'Test Note', 'Body content');
            INSERT INTO obsidian_notes (id, title, body) VALUES (2, 'Another Note', 'More content');",
        ).unwrap();

        let registry = test_registry();
        let counts = rebuild_all_fts(&conn, &registry).unwrap();
        let notes_count = counts.iter().find(|(k, _)| k == "obsidian").unwrap().1;
        assert_eq!(notes_count, 2);
    }

    // ========== Contacts FTS ==========

    #[test]
    fn rebuild_fts_with_contacts() {
        let conn = setup_db();
        conn.execute_batch(
            "CREATE TABLE contacts (
                identifier TEXT PRIMARY KEY, given_name TEXT, family_name TEXT,
                organization TEXT, job_title TEXT, note TEXT, birthday TEXT, nickname TEXT
            );
            INSERT INTO contacts (identifier, given_name, family_name, organization) VALUES ('c1', 'Alice', 'Smith', 'Acme');
            INSERT INTO contacts (identifier, given_name, family_name) VALUES ('c2', 'Bob', 'Jones');
            INSERT INTO contacts (identifier, given_name, family_name) VALUES ('c3', 'Charlie', 'Brown');",
        ).unwrap();

        let registry = test_registry();
        let counts = rebuild_all_fts(&conn, &registry).unwrap();
        let contacts_count = counts.iter().find(|(k, _)| k == "contacts").unwrap().1;
        assert_eq!(contacts_count, 3);
    }

    // ========== Empty source tables ==========

    #[test]
    fn rebuild_fts_empty_source_tables() {
        let conn = setup_db();
        conn.execute_batch(
            "CREATE TABLE obsidian_notes (
                id INTEGER PRIMARY KEY, title TEXT, body TEXT, content TEXT,
                file_path TEXT, created_at TEXT, modified_at TEXT, vault_name TEXT
            );
            CREATE TABLE obsidian_tags (note_id INTEGER, tag TEXT);
            CREATE TABLE contacts (
                identifier TEXT PRIMARY KEY, given_name TEXT, family_name TEXT,
                organization TEXT, job_title TEXT, note TEXT, birthday TEXT, nickname TEXT
            );",
        )
        .unwrap();

        let registry = test_registry();
        let counts = rebuild_all_fts(&conn, &registry).unwrap();
        let notes_count = counts.iter().find(|(k, _)| k == "obsidian").unwrap().1;
        let contacts_count = counts.iter().find(|(k, _)| k == "contacts").unwrap().1;
        assert_eq!(notes_count, 0);
        assert_eq!(contacts_count, 0);
    }

    // ========== Missing source tables ==========

    #[test]
    fn rebuild_fts_missing_source_tables() {
        let conn = setup_db();
        let registry = test_registry();
        // No source tables created at all — should gracefully return 0
        let counts = rebuild_all_fts(&conn, &registry).unwrap();
        for (name, count) in &counts {
            assert_eq!(*count, 0, "{name} should be 0 with no source table");
        }
    }

    // ========== Documents FTS ==========

    #[test]
    fn rebuild_fts_with_documents() {
        let conn = setup_db();
        conn.execute_batch(
            "CREATE TABLE documents (
                id INTEGER PRIMARY KEY, title TEXT, filename TEXT, content TEXT,
                file_path TEXT, file_type TEXT, file_size INTEGER, modified_at TEXT
            );
            INSERT INTO documents (id, title, filename, content, file_type) VALUES (1, 'Report', 'report.pdf', 'Annual report content', '.pdf');
            INSERT INTO documents (id, title, filename, content, file_type) VALUES (2, 'Resume', 'resume.docx', 'Work experience', '.docx');",
        ).unwrap();

        let registry = test_registry();
        let counts = rebuild_all_fts(&conn, &registry).unwrap();
        let docs_count = counts.iter().find(|(k, _)| k == "documents").unwrap().1;
        assert_eq!(docs_count, 2);
    }

    // ========== Reminders FTS ==========

    #[test]
    fn rebuild_fts_with_reminders() {
        let conn = setup_db();
        conn.execute_batch(
            "CREATE TABLE reminders (
                id INTEGER PRIMARY KEY, reminder_id TEXT, title TEXT, notes TEXT,
                list_name TEXT, due_date TEXT, is_completed INTEGER, priority INTEGER,
                creation_date TEXT, completion_date TEXT, location TEXT
            );
            INSERT INTO reminders (id, reminder_id, title, list_name) VALUES (1, 'r1', 'Buy groceries', 'Shopping');
            INSERT INTO reminders (id, reminder_id, title, list_name) VALUES (2, 'r2', 'Call dentist', 'Health');",
        ).unwrap();

        let registry = test_registry();
        let counts = rebuild_all_fts(&conn, &registry).unwrap();
        let rem_count = counts.iter().find(|(k, _)| k == "reminders").unwrap().1;
        assert_eq!(rem_count, 2);
    }
}
