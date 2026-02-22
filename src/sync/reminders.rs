use anyhow::Result;
use rusqlite::{params, Connection, OpenFlags};

use crate::config::{self, Config};
use crate::connector::Connector;
use crate::db;
use crate::search;
use crate::sync::SyncContext;

pub struct RemindersConnector;

impl Connector for RemindersConnector {
    fn name(&self) -> &str {
        "reminders"
    }

    fn description(&self) -> &str {
        "Apple Reminders"
    }

    fn create_source_tables(&self, conn: &Connection) -> Result<()> {
        create_tables(conn)
    }

    fn extract(&self, conn: &Connection, config: &Config, ctx: &SyncContext) -> Result<usize> {
        extract(conn, config, ctx)
    }

    fn fts_schema_sql(&self) -> Option<&str> {
        Some(
            "CREATE VIRTUAL TABLE IF NOT EXISTS reminders_fts USING fts5(
                title,
                notes,
                list_name,
                location,
                tokenize='porter unicode61'
            );

            CREATE TABLE IF NOT EXISTS reminders_fts_map (
                fts_rowid INTEGER PRIMARY KEY,
                reminder_id TEXT NOT NULL,
                UNIQUE(reminder_id)
            );",
        )
    }

    fn populate_fts(&self, conn: &Connection) -> Result<i64> {
        if !db::table_exists(conn, "reminders") {
            return Ok(0);
        }

        let tx = conn.unchecked_transaction()?;
        tx.execute_batch("DELETE FROM reminders_fts; DELETE FROM reminders_fts_map;")?;

        tx.execute_batch(
            "INSERT INTO reminders_fts(rowid, title, notes, list_name, location)
            SELECT
                id,
                COALESCE(title, ''),
                COALESCE(notes, ''),
                COALESCE(list_name, ''),
                COALESCE(location, '')
            FROM reminders;

            INSERT INTO reminders_fts_map(fts_rowid, reminder_id)
            SELECT id, reminder_id FROM reminders;",
        )?;

        let count: i64 = tx.query_row("SELECT COUNT(*) FROM reminders_fts", [], |r| r.get(0))?;
        tx.commit()?;
        Ok(count)
    }

    fn governance_description(&self) -> &str {
        "Lists, due dates, and priorities."
    }

    fn governance_fields(&self) -> &[&str] {
        &["title", "notes", "list_name", "location"]
    }

    fn search_types(&self) -> Vec<(&str, &str)> {
        vec![("reminder", "reminders")]
    }

    fn search_fts(
        &self,
        conn: &Connection,
        search_type: &str,
        query: &str,
        options: &search::SearchOptions,
    ) -> Result<Vec<search::SearchResult>> {
        if search_type == "reminder" {
            search::search_reminders_fts(conn, query, options)
        } else {
            Ok(vec![])
        }
    }
}

const APPLE_TS: &str = "978307200";

/// Convert DateTime<Utc> to Apple seconds (for ZLASTMODIFIEDDATE).
fn to_apple_seconds(dt: &chrono::DateTime<chrono::Utc>) -> f64 {
    (dt.timestamp() as f64) - 978307200.0
}

/// Extract Reminders from macOS SQLite databases into warehouse.
pub fn extract(conn: &Connection, _config: &Config, ctx: &SyncContext) -> Result<usize> {
    let dbs = config::discover_reminders_databases();
    if dbs.is_empty() {
        anyhow::bail!("Reminders databases not found");
    }

    create_tables(conn)?;

    let tx = conn.unchecked_transaction()?;

    if !ctx.is_incremental() {
        tx.execute_batch(
            "DELETE FROM reminders;
             DELETE FROM reminder_lists;",
        )?;
    }

    let mut total_reminders = 0;
    let mut total_lists = 0;

    for db_path in &dbs {
        let src = match Connection::open_with_flags(db_path, OpenFlags::SQLITE_OPEN_READ_ONLY) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("  Warning: Cannot open {}: {e}", db_path.display());
                continue;
            }
        };

        // Check if this is a valid Reminders database
        let has_reminders: bool = src
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='ZREMCDREMINDER'",
                [],
                |r| r.get(0),
            )
            .unwrap_or(false);

        if !has_reminders {
            continue;
        }

        // Lists: always INSERT OR REPLACE without filtering
        let lists = extract_lists(&src, &tx)?;
        let reminders = extract_reminders(&src, &tx, ctx)?;
        total_lists += lists;
        total_reminders += reminders;
    }

    tx.commit()?;
    eprintln!("  lists: {total_lists}, reminders: {total_reminders}");
    Ok(total_reminders)
}

fn create_tables(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS reminder_lists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            calendar_id TEXT UNIQUE,
            name TEXT,
            color TEXT,
            source_name TEXT,
            source_type INTEGER,
            is_subscribed INTEGER,
            allows_modifications INTEGER,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS reminders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reminder_id TEXT UNIQUE,
            title TEXT,
            notes TEXT,
            url TEXT,
            location TEXT,
            is_completed INTEGER,
            priority INTEGER,
            creation_date TEXT,
            last_modified_date TEXT,
            completion_date TEXT,
            due_date TEXT,
            start_date TEXT,
            list_id TEXT,
            list_name TEXT,
            has_recurrence INTEGER,
            has_alarms INTEGER,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        ",
    )?;
    Ok(())
}

fn extract_lists(src: &Connection, dst: &Connection) -> Result<usize> {
    // Modern macOS uses ZREMCDBASELIST; older versions used ZREMCDCALENDAR
    let list_table = if table_exists(src, "ZREMCDBASELIST") {
        "ZREMCDBASELIST"
    } else if table_exists(src, "ZREMCDCALENDAR") {
        "ZREMCDCALENDAR"
    } else {
        return Ok(0);
    };

    // Column names differ between ZREMCDBASELIST (ZNAME) and ZREMCDCALENDAR (ZTITLE1)
    let name_col = if list_table == "ZREMCDBASELIST" {
        "ZNAME"
    } else {
        "ZTITLE1"
    };

    let sql = format!(
        "SELECT ZCKIDENTIFIER, {name_col} FROM {list_table} WHERE ZMARKEDFORDELETION = 0 OR ZMARKEDFORDELETION IS NULL"
    );

    let mut stmt = src.prepare(&sql)?;
    let mut insert =
        dst.prepare("INSERT OR REPLACE INTO reminder_lists (calendar_id, name) VALUES (?1, ?2)")?;

    let mut count = 0;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, Option<String>>(0)?,
            row.get::<_, Option<String>>(1)?,
        ))
    })?;

    for row in rows {
        let (id, name) = row?;
        insert.execute(params![id, name])?;
        count += 1;
    }
    Ok(count)
}

fn table_exists(conn: &Connection, name: &str) -> bool {
    conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
        [name],
        |r| r.get::<_, i64>(0),
    )
    .unwrap_or(0)
        > 0
}

fn extract_reminders(src: &Connection, dst: &Connection, ctx: &SyncContext) -> Result<usize> {
    // Determine list table and name column
    let (list_table, list_name_col) = if table_exists(src, "ZREMCDBASELIST") {
        ("ZREMCDBASELIST", "ZNAME")
    } else if table_exists(src, "ZREMCDCALENDAR") {
        ("ZREMCDCALENDAR", "ZTITLE1")
    } else {
        ("ZREMCDBASELIST", "ZNAME") // fallback
    };

    // ZREMCDREMINDER uses ZTITLE on modern macOS, ZTITLE1 on older
    let title_col = if has_column(src, "ZREMCDREMINDER", "ZTITLE") {
        "ZTITLE"
    } else {
        "ZTITLE1"
    };

    let incremental_filter = if ctx.is_incremental() {
        if let Some(ref since) = ctx.since {
            let apple_secs = to_apple_seconds(since);
            format!(" AND (r.ZLASTMODIFIEDDATE IS NOT NULL AND r.ZLASTMODIFIEDDATE > {apple_secs})")
        } else {
            String::new()
        }
    } else {
        String::new()
    };

    let sql = format!(
        "SELECT
            r.ZCKIDENTIFIER,
            r.{title_col},
            r.ZNOTES,
            r.ZCOMPLETED,
            r.ZPRIORITY,
            CASE WHEN r.ZCREATIONDATE IS NOT NULL
                THEN datetime(r.ZCREATIONDATE + {APPLE_TS}, 'unixepoch', 'localtime')
                ELSE NULL END,
            CASE WHEN r.ZLASTMODIFIEDDATE IS NOT NULL
                THEN datetime(r.ZLASTMODIFIEDDATE + {APPLE_TS}, 'unixepoch', 'localtime')
                ELSE NULL END,
            CASE WHEN r.ZCOMPLETIONDATE IS NOT NULL
                THEN datetime(r.ZCOMPLETIONDATE + {APPLE_TS}, 'unixepoch', 'localtime')
                ELSE NULL END,
            CASE WHEN r.ZDUEDATE IS NOT NULL
                THEN datetime(r.ZDUEDATE + {APPLE_TS}, 'unixepoch', 'localtime')
                ELSE NULL END,
            CASE WHEN r.ZSTARTDATE IS NOT NULL
                THEN datetime(r.ZSTARTDATE + {APPLE_TS}, 'unixepoch', 'localtime')
                ELSE NULL END,
            c.ZCKIDENTIFIER,
            c.{list_name_col}
         FROM ZREMCDREMINDER r
         LEFT JOIN {list_table} c ON r.ZLIST = c.Z_PK
         WHERE (r.ZMARKEDFORDELETION = 0 OR r.ZMARKEDFORDELETION IS NULL){incremental_filter}"
    );

    let mut stmt = src.prepare(&sql)?;
    let mut insert = dst.prepare(
        "INSERT OR REPLACE INTO reminders
         (reminder_id, title, notes,
          is_completed, priority,
          creation_date, last_modified_date, completion_date,
          due_date, start_date, list_id, list_name)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
    )?;

    let mut count = 0;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, Option<String>>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<i64>>(3)?,
            row.get::<_, Option<i64>>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, Option<String>>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, Option<String>>(8)?,
            row.get::<_, Option<String>>(9)?,
            row.get::<_, Option<String>>(10)?,
            row.get::<_, Option<String>>(11)?,
        ))
    })?;

    for row in rows {
        let r = row?;
        insert.execute(params![
            r.0, r.1, r.2, r.3, r.4, r.5, r.6, r.7, r.8, r.9, r.10, r.11
        ])?;
        count += 1;
    }
    Ok(count)
}

fn has_column(conn: &Connection, table: &str, column: &str) -> bool {
    let sql = format!("PRAGMA table_info({table})");
    let mut stmt = match conn.prepare(&sql) {
        Ok(s) => s,
        Err(_) => return false,
    };
    let rows = stmt.query_map([], |row| row.get::<_, String>(1));
    match rows {
        Ok(rows) => rows.flatten().any(|name| name == column),
        Err(_) => false,
    }
}
