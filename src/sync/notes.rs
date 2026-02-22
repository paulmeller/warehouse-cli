use anyhow::Result;
use regex::Regex;
use rusqlite::{params, Connection};
use std::collections::HashMap;
use std::path::Path;
use walkdir::WalkDir;

use crate::config::{self, Config};
use crate::connector::Connector;
use crate::db;
use crate::search;
use crate::sync::SyncContext;

pub struct NotesConnector;

impl Connector for NotesConnector {
    fn name(&self) -> &str {
        "obsidian"
    }

    fn description(&self) -> &str {
        "Obsidian markdown notes"
    }

    fn create_source_tables(&self, conn: &Connection) -> Result<()> {
        create_tables(conn)
    }

    fn extract(&self, conn: &Connection, config: &Config, ctx: &SyncContext) -> Result<usize> {
        extract(conn, config, ctx)
    }

    fn fts_schema_sql(&self) -> Option<&str> {
        Some(
            "CREATE VIRTUAL TABLE IF NOT EXISTS notes_fts USING fts5(
                title,
                body,
                tags,
                tokenize='porter unicode61'
            );

            CREATE TABLE IF NOT EXISTS notes_fts_map (
                fts_rowid INTEGER PRIMARY KEY,
                note_id INTEGER NOT NULL,
                UNIQUE(note_id)
            );",
        )
    }

    fn populate_fts(&self, conn: &Connection) -> Result<i64> {
        if !db::table_exists(conn, "obsidian_notes") {
            return Ok(0);
        }

        let tx = conn.unchecked_transaction()?;
        tx.execute_batch("DELETE FROM notes_fts; DELETE FROM notes_fts_map;")?;

        tx.execute_batch(
            "INSERT INTO notes_fts(rowid, title, body, tags)
            SELECT
                n.id,
                COALESCE(n.title, ''),
                COALESCE(n.body, ''),
                COALESCE(
                    (SELECT GROUP_CONCAT(tag, ' ')
                     FROM obsidian_tags
                     WHERE note_id = n.id),
                    ''
                )
            FROM obsidian_notes n;

            INSERT INTO notes_fts_map(fts_rowid, note_id)
            SELECT id, id FROM obsidian_notes;",
        )?;

        let count: i64 = tx.query_row("SELECT COUNT(*) FROM notes_fts", [], |r| r.get(0))?;
        tx.commit()?;
        Ok(count)
    }

    fn governance_source(&self) -> &str {
        "notes"
    }

    fn governance_description(&self) -> &str {
        "Your Obsidian vault \u{2014} notes, tags, and links."
    }

    fn governance_fields(&self) -> &[&str] {
        &["title", "body", "tags", "frontmatter"]
    }

    fn search_types(&self) -> Vec<(&str, &str)> {
        vec![("note", "notes")]
    }

    fn search_fts(
        &self,
        conn: &Connection,
        search_type: &str,
        query: &str,
        options: &search::SearchOptions,
    ) -> Result<Vec<search::SearchResult>> {
        if search_type == "note" {
            search::search_notes_fts(conn, query, options)
        } else {
            Ok(vec![])
        }
    }
}

/// Extract Obsidian notes from vault directories into warehouse.
pub fn extract(conn: &Connection, _config: &Config, ctx: &SyncContext) -> Result<usize> {
    let vaults = config::discover_obsidian_vaults();
    if vaults.is_empty() {
        anyhow::bail!("No Obsidian vaults not found");
    }

    create_tables(conn)?;

    let tx = conn.unchecked_transaction()?;

    if !ctx.is_incremental() {
        tx.execute_batch(
            "DELETE FROM obsidian_links;
             DELETE FROM obsidian_tags;
             DELETE FROM obsidian_notes;",
        )?;
    }

    let mut total = 0;
    for vault_path in &vaults {
        let vault_name = vault_path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown".into());

        let count = extract_vault(&tx, vault_path, &vault_name, ctx)?;
        eprintln!("  vault '{vault_name}': {count} notes");
        total += count;
    }

    tx.commit()?;
    Ok(total)
}

fn create_tables(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS obsidian_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vault_name TEXT NOT NULL,
            file_path TEXT NOT NULL,
            title TEXT,
            content TEXT,
            body TEXT,
            frontmatter_json TEXT,
            word_count INTEGER,
            char_count INTEGER,
            created_at TEXT,
            modified_at TEXT,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(vault_name, file_path)
        );

        CREATE TABLE IF NOT EXISTS obsidian_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            note_id INTEGER,
            tag TEXT NOT NULL,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (note_id) REFERENCES obsidian_notes(id)
        );

        CREATE TABLE IF NOT EXISTS obsidian_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_note_id INTEGER,
            target_title TEXT NOT NULL,
            _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (source_note_id) REFERENCES obsidian_notes(id)
        );
        ",
    )?;
    Ok(())
}

fn extract_vault(
    conn: &Connection,
    vault_path: &Path,
    vault_name: &str,
    ctx: &SyncContext,
) -> Result<usize> {
    let tag_re = Regex::new(r"(?:^|\s)#([a-zA-Z][a-zA-Z0-9_/-]*)").unwrap();
    let link_re = Regex::new(r"\[\[([^\]|]+)(?:\|[^\]]+)?\]\]").unwrap();
    let frontmatter_re = Regex::new(r"(?s)\A---\s*\n(.*?)\n---\s*\n").unwrap();

    let mut count = 0;

    for entry in WalkDir::new(vault_path)
        .into_iter()
        .filter_entry(|e| {
            let name = e.file_name().to_string_lossy();
            !name.starts_with('.') && name != "node_modules"
        })
        .filter_map(|e| e.ok())
    {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let ext = path.extension().and_then(|e| e.to_str());
        if ext != Some("md") {
            continue;
        }

        // In incremental mode, skip files not modified since last sync
        if ctx.is_incremental() {
            if let Some(ref since) = ctx.since {
                if let Ok(metadata) = std::fs::metadata(path) {
                    if let Ok(mtime) = metadata.modified() {
                        let file_modified: chrono::DateTime<chrono::Utc> = mtime.into();
                        if file_modified <= *since {
                            continue;
                        }
                    }
                }
            }
        }

        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => continue,
        };

        let rel_path = path
            .strip_prefix(vault_path)
            .unwrap_or(path)
            .to_string_lossy()
            .to_string();

        let title = path
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();

        // Parse frontmatter
        let (frontmatter_json, body) = if let Some(caps) = frontmatter_re.captures(&content) {
            let fm_text = caps.get(1).unwrap().as_str();
            let fm_json = parse_yaml_frontmatter(fm_text);
            let body_start = caps.get(0).unwrap().end();
            (Some(fm_json), content[body_start..].to_string())
        } else {
            (None, content.clone())
        };

        let word_count = body.split_whitespace().count() as i64;
        let char_count = body.len() as i64;

        // File timestamps
        let metadata = std::fs::metadata(path).ok();
        let created_at = metadata.as_ref().and_then(|m| m.created().ok()).map(|t| {
            chrono::DateTime::<chrono::Utc>::from(t)
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string()
        });
        let modified_at = metadata.as_ref().and_then(|m| m.modified().ok()).map(|t| {
            chrono::DateTime::<chrono::Utc>::from(t)
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string()
        });

        // Extract tags
        let mut tags = Vec::new();
        let mut seen_tags = std::collections::HashSet::new();

        // Tags from frontmatter
        if let Some(ref fm) = frontmatter_json {
            if let Ok(parsed) = serde_json::from_str::<HashMap<String, serde_json::Value>>(fm) {
                if let Some(serde_json::Value::Array(fm_tags)) = parsed.get("tags") {
                    for tag_val in fm_tags {
                        if let Some(tag) = tag_val.as_str() {
                            let tag = tag.trim_start_matches('#');
                            if seen_tags.insert(tag.to_string()) {
                                tags.push(tag.to_string());
                            }
                        }
                    }
                }
            }
        }

        // Inline tags
        for caps in tag_re.captures_iter(&body) {
            let tag = caps[1].to_string();
            if seen_tags.insert(tag.clone()) {
                tags.push(tag);
            }
        }

        // Extract wiki links
        let mut links = Vec::new();
        let mut seen_links = std::collections::HashSet::new();
        for caps in link_re.captures_iter(&body) {
            let target = caps[1].trim().to_string();
            if seen_links.insert(target.clone()) {
                links.push(target);
            }
        }

        // FK-safe upsert: check if note exists, then UPDATE or INSERT
        if ctx.is_incremental() {
            let existing_id: Option<i64> = conn
                .query_row(
                    "SELECT id FROM obsidian_notes WHERE vault_name = ?1 AND file_path = ?2",
                    params![vault_name, rel_path],
                    |row| row.get(0),
                )
                .ok();

            let note_id = if let Some(id) = existing_id {
                // UPDATE existing note, delete old tags/links first
                conn.execute("DELETE FROM obsidian_tags WHERE note_id = ?1", [id])?;
                conn.execute("DELETE FROM obsidian_links WHERE source_note_id = ?1", [id])?;
                conn.execute(
                    "UPDATE obsidian_notes SET title=?1, content=?2, body=?3, frontmatter_json=?4,
                     word_count=?5, char_count=?6, created_at=?7, modified_at=?8,
                     _extracted_at=CURRENT_TIMESTAMP
                     WHERE id=?9",
                    params![
                        title,
                        content,
                        body,
                        frontmatter_json,
                        word_count,
                        char_count,
                        created_at,
                        modified_at,
                        id,
                    ],
                )?;
                id
            } else {
                // INSERT new note
                conn.execute(
                    "INSERT INTO obsidian_notes
                     (vault_name, file_path, title, content, body, frontmatter_json,
                      word_count, char_count, created_at, modified_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    params![
                        vault_name,
                        rel_path,
                        title,
                        content,
                        body,
                        frontmatter_json,
                        word_count,
                        char_count,
                        created_at,
                        modified_at,
                    ],
                )?;
                conn.last_insert_rowid()
            };

            // Re-insert tags and links with stable note_id
            let mut insert_tag =
                conn.prepare_cached("INSERT INTO obsidian_tags (note_id, tag) VALUES (?1, ?2)")?;
            for tag in &tags {
                insert_tag.execute(params![note_id, tag])?;
            }

            let mut insert_link = conn.prepare_cached(
                "INSERT INTO obsidian_links (source_note_id, target_title) VALUES (?1, ?2)",
            )?;
            for link in &links {
                insert_link.execute(params![note_id, link])?;
            }
        } else {
            // Full sync: simple INSERT (table was already cleared)
            conn.execute(
                "INSERT INTO obsidian_notes
                 (vault_name, file_path, title, content, body, frontmatter_json,
                  word_count, char_count, created_at, modified_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    vault_name,
                    rel_path,
                    title,
                    content,
                    body,
                    frontmatter_json,
                    word_count,
                    char_count,
                    created_at,
                    modified_at,
                ],
            )?;

            let note_id = conn.last_insert_rowid();

            let mut insert_tag =
                conn.prepare_cached("INSERT INTO obsidian_tags (note_id, tag) VALUES (?1, ?2)")?;
            for tag in &tags {
                insert_tag.execute(params![note_id, tag])?;
            }

            let mut insert_link = conn.prepare_cached(
                "INSERT INTO obsidian_links (source_note_id, target_title) VALUES (?1, ?2)",
            )?;
            for link in &links {
                insert_link.execute(params![note_id, link])?;
            }
        }

        count += 1;
    }

    Ok(count)
}

/// Simple YAML frontmatter parser that converts to JSON.
fn parse_yaml_frontmatter(yaml_text: &str) -> String {
    let mut map = serde_json::Map::new();

    for line in yaml_text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((key, value)) = line.split_once(':') {
            let key = key.trim().to_string();
            let value = value.trim();

            if value.is_empty() {
                continue;
            }

            // Handle arrays like [a, b, c]
            if value.starts_with('[') && value.ends_with(']') {
                let inner = &value[1..value.len() - 1];
                let items: Vec<serde_json::Value> = inner
                    .split(',')
                    .map(|s| serde_json::Value::String(s.trim().trim_matches('"').to_string()))
                    .collect();
                map.insert(key, serde_json::Value::Array(items));
            } else {
                map.insert(key, serde_json::Value::String(value.to_string()));
            }
        }
    }

    serde_json::to_string(&map).unwrap_or_else(|_| "{}".into())
}
