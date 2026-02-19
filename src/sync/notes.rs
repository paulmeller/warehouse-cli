use anyhow::Result;
use regex::Regex;
use rusqlite::{params, Connection};
use std::collections::HashMap;
use std::path::Path;
use walkdir::WalkDir;

use crate::config::{self, Config};

/// Extract Obsidian notes from vault directories into warehouse.
pub fn extract(conn: &Connection, _config: &Config) -> Result<usize> {
    let vaults = config::discover_obsidian_vaults();
    if vaults.is_empty() {
        anyhow::bail!("No Obsidian vaults not found");
    }

    create_tables(conn)?;

    let tx = conn.unchecked_transaction()?;
    tx.execute_batch(
        "DELETE FROM obsidian_links;
         DELETE FROM obsidian_tags;
         DELETE FROM obsidian_notes;",
    )?;

    let mut total = 0;
    for vault_path in &vaults {
        let vault_name = vault_path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown".into());

        let count = extract_vault(&tx, vault_path, &vault_name)?;
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

fn extract_vault(conn: &Connection, vault_path: &Path, vault_name: &str) -> Result<usize> {
    let tag_re = Regex::new(r"(?:^|\s)#([a-zA-Z][a-zA-Z0-9_/-]*)").unwrap();
    let link_re = Regex::new(r"\[\[([^\]|]+)(?:\|[^\]]+)?\]\]").unwrap();
    let frontmatter_re = Regex::new(r"(?s)\A---\s*\n(.*?)\n---\s*\n").unwrap();

    let mut insert_note = conn.prepare(
        "INSERT INTO obsidian_notes
         (vault_name, file_path, title, content, body, frontmatter_json,
          word_count, char_count, created_at, modified_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
    )?;
    let mut insert_tag = conn.prepare(
        "INSERT INTO obsidian_tags (note_id, tag) VALUES (?1, ?2)",
    )?;
    let mut insert_link = conn.prepare(
        "INSERT INTO obsidian_links (source_note_id, target_title) VALUES (?1, ?2)",
    )?;

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
        let created_at = metadata
            .as_ref()
            .and_then(|m| m.created().ok())
            .map(|t| {
                chrono::DateTime::<chrono::Utc>::from(t)
                    .format("%Y-%m-%dT%H:%M:%S")
                    .to_string()
            });
        let modified_at = metadata
            .as_ref()
            .and_then(|m| m.modified().ok())
            .map(|t| {
                chrono::DateTime::<chrono::Utc>::from(t)
                    .format("%Y-%m-%dT%H:%M:%S")
                    .to_string()
            });

        insert_note.execute(params![
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
        ])?;

        let note_id = conn.last_insert_rowid();

        // Extract tags
        let mut seen_tags = std::collections::HashSet::new();

        // Tags from frontmatter
        if let Some(ref fm) = frontmatter_json {
            if let Ok(parsed) = serde_json::from_str::<HashMap<String, serde_json::Value>>(fm) {
                if let Some(serde_json::Value::Array(tags)) = parsed.get("tags") {
                    for tag_val in tags {
                        if let Some(tag) = tag_val.as_str() {
                            let tag = tag.trim_start_matches('#');
                            if seen_tags.insert(tag.to_string()) {
                                insert_tag.execute(params![note_id, tag])?;
                            }
                        }
                    }
                }
            }
        }

        // Inline tags
        for caps in tag_re.captures_iter(&body) {
            let tag = &caps[1];
            if seen_tags.insert(tag.to_string()) {
                insert_tag.execute(params![note_id, tag])?;
            }
        }

        // Extract wiki links
        let mut seen_links = std::collections::HashSet::new();
        for caps in link_re.captures_iter(&body) {
            let target = caps[1].trim().to_string();
            if seen_links.insert(target.clone()) {
                insert_link.execute(params![note_id, target])?;
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
