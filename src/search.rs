use anyhow::Result;
use regex::Regex;
use rusqlite::Connection;
use serde::Serialize;
use std::collections::HashMap;

pub const ALL_TYPES: &[&str] = &[
    "message", "note", "contact", "photo", "document", "reminder",
];

#[derive(Debug, Clone, Serialize)]
pub struct SearchResult {
    #[serde(rename = "type")]
    pub result_type: String,
    pub id: String,
    pub title: String,
    pub snippet: String,
    pub score: f64,
    pub metadata: HashMap<String, serde_json::Value>,
}

pub struct SearchOptions {
    pub types: Vec<String>,
    pub limit: usize,
    pub date_from: Option<String>,
    pub date_to: Option<String>,
    pub min_score: f64,
}

/// Escape a user query for FTS5 syntax.
pub fn escape_fts_query(query: &str) -> String {
    let query = query.trim();
    if query.is_empty() {
        return "\"\"".to_string();
    }

    let mut phrases = Vec::new();
    let mut remaining = query.to_string();

    // Extract quoted phrases
    let re = Regex::new(r#""([^"]+)""#).unwrap();
    for cap in re.captures_iter(query) {
        phrases.push(format!("\"{}\"", &cap[1]));
        remaining = remaining.replace(&cap[0], " ");
    }

    // Process remaining terms
    let mut terms = Vec::new();
    for token in remaining.split_whitespace() {
        if token.starts_with('-') && token.len() > 1 {
            let term: String = token[1..]
                .chars()
                .map(|c| if "*:^()~'".contains(c) { ' ' } else { c })
                .collect();
            let term = term.trim().to_string();
            if !term.is_empty() {
                terms.push(format!("NOT {term}"));
            }
        } else {
            let term: String = token
                .chars()
                .map(|c| if "*:^()~'".contains(c) { ' ' } else { c })
                .collect();
            let term = term.trim().to_string();
            if !term.is_empty() {
                terms.push(term);
            }
        }
    }

    let mut all_parts = phrases;
    all_parts.extend(terms);

    if all_parts.is_empty() {
        return "\"\"".to_string();
    }

    all_parts.join(" OR ")
}

/// Perform FTS-only search across all content types.
pub fn fts_search(
    conn: &Connection,
    query: &str,
    options: &SearchOptions,
) -> Result<Vec<SearchResult>> {
    let fts_query = escape_fts_query(query);
    let mut results = Vec::new();

    let types: Vec<&str> = options.types.iter().map(|s| s.as_str()).collect();

    if types.contains(&"message") {
        results.extend(search_messages_fts(conn, &fts_query, options)?);
    }
    if types.contains(&"note") {
        results.extend(search_notes_fts(conn, &fts_query, options)?);
    }
    if types.contains(&"contact") {
        results.extend(search_contacts_fts(conn, &fts_query)?);
    }
    if types.contains(&"photo") {
        results.extend(search_photos_fts(conn, &fts_query, options)?);
    }
    if types.contains(&"document") {
        results.extend(search_documents_fts(conn, &fts_query, options)?);
    }
    if types.contains(&"reminder") {
        results.extend(search_reminders_fts(conn, &fts_query, options)?);
    }

    // Sort by score descending
    results.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // Apply min_score filter
    results.retain(|r| r.score >= options.min_score);

    // Ensure type diversity
    results = ensure_type_diversity(results, &options.types, options.limit);

    // Limit
    results.truncate(options.limit);

    Ok(results)
}

/// Ensure each content type gets minimum representation in results.
///
/// Reserves slots for underrepresented types to prevent any single type
/// from dominating results. Uses a two-pass approach:
/// 1. Reserve min slots per type
/// 2. Fill remaining slots with highest-scored items
fn ensure_type_diversity(
    results: Vec<SearchResult>,
    types: &[String],
    limit: usize,
) -> Vec<SearchResult> {
    if types.len() <= 1 || results.is_empty() {
        return results;
    }

    // Group results by type
    let mut by_type: HashMap<String, Vec<&SearchResult>> = HashMap::new();
    for t in types {
        by_type.insert(t.clone(), Vec::new());
    }
    for r in &results {
        by_type.entry(r.result_type.clone()).or_default().push(r);
    }

    // Only count types that actually have results
    let active_types: Vec<&String> = types
        .iter()
        .filter(|t| by_type.get(*t).is_some_and(|v| !v.is_empty()))
        .collect();

    if active_types.len() <= 1 {
        return results;
    }

    let min_per_type = (limit / active_types.len() / 2).max(2);

    let mut diverse: Vec<SearchResult> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Pass 1: reserve min slots per type
    for t in &active_types {
        if let Some(type_results) = by_type.get(*t) {
            for r in type_results.iter().take(min_per_type) {
                let key = format!("{}:{}", r.result_type, r.id);
                if seen.insert(key) {
                    diverse.push((*r).clone());
                }
            }
        }
    }

    // Pass 2: fill remaining slots with highest-scored items
    let mut remaining = limit.saturating_sub(diverse.len());
    for r in &results {
        if remaining == 0 {
            break;
        }
        let key = format!("{}:{}", r.result_type, r.id);
        if seen.insert(key) {
            diverse.push(r.clone());
            remaining -= 1;
        }
    }

    // Re-sort by score
    diverse.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    diverse
}

fn search_messages_fts(
    conn: &Connection,
    query: &str,
    options: &SearchOptions,
) -> Result<Vec<SearchResult>> {
    let mut sql = String::from(
        "SELECT
            'message' as type,
            CAST(map.message_id AS TEXT) as id,
            COALESCE(ch.display_name, ch.chat_identifier, 'Chat') as title,
            snippet(messages_fts, 2, '**', '**', '...', 32) as snippet,
            bm25(messages_fts) as score,
            m.message_date,
            m.chat_id,
            m.is_from_me
        FROM messages_fts
        JOIN messages_fts_map map ON messages_fts.rowid = map.fts_rowid
        JOIN imessage_messages m ON map.message_id = m.message_id
        LEFT JOIN imessage_chats ch ON m.chat_id = ch.chat_id
        WHERE messages_fts MATCH ?1",
    );

    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(query.to_string())];

    if let Some(ref from) = options.date_from {
        sql.push_str(&format!(" AND m.message_date >= ?{}", params.len() + 1));
        params.push(Box::new(from.clone()));
    }
    if let Some(ref to) = options.date_to {
        sql.push_str(&format!(" AND m.message_date <= ?{}", params.len() + 1));
        params.push(Box::new(to.clone()));
    }

    sql.push_str(" ORDER BY bm25(messages_fts) LIMIT 50");

    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();

    match conn.prepare(&sql) {
        Ok(mut stmt) => {
            let rows = stmt.query_map(param_refs.as_slice(), |row| {
                let score: f64 = row.get("score")?;
                let date: Option<String> = row.get("message_date")?;
                let chat_id: Option<i64> = row.get("chat_id")?;
                let from_me: Option<bool> = row.get("is_from_me")?;
                let mut metadata = HashMap::new();
                if let Some(d) = date {
                    metadata.insert("date".into(), serde_json::Value::String(d));
                }
                if let Some(cid) = chat_id {
                    metadata.insert("chat_id".into(), serde_json::json!(cid));
                }
                if let Some(fm) = from_me {
                    metadata.insert("from_me".into(), serde_json::json!(fm));
                }
                Ok(SearchResult {
                    result_type: "message".into(),
                    id: row.get("id")?,
                    title: row.get("title")?,
                    snippet: row.get::<_, Option<String>>("snippet")?.unwrap_or_default(),
                    score: score.abs(),
                    metadata,
                })
            })?;
            Ok(rows.filter_map(|r| r.ok()).collect())
        }
        Err(_) => Ok(Vec::new()),
    }
}

fn search_notes_fts(
    conn: &Connection,
    query: &str,
    options: &SearchOptions,
) -> Result<Vec<SearchResult>> {
    let mut sql = String::from(
        "SELECT
            'note' as type,
            CAST(map.note_id AS TEXT) as id,
            n.title,
            snippet(notes_fts, 1, '**', '**', '...', 32) as snippet,
            bm25(notes_fts) as score,
            n.file_path,
            n.modified_at
        FROM notes_fts
        JOIN notes_fts_map map ON notes_fts.rowid = map.fts_rowid
        JOIN obsidian_notes n ON map.note_id = n.id
        WHERE notes_fts MATCH ?1",
    );

    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(query.to_string())];

    if let Some(ref from) = options.date_from {
        sql.push_str(&format!(" AND n.modified_at >= ?{}", params.len() + 1));
        params.push(Box::new(from.clone()));
    }
    if let Some(ref to) = options.date_to {
        sql.push_str(&format!(" AND n.modified_at <= ?{}", params.len() + 1));
        params.push(Box::new(to.clone()));
    }

    sql.push_str(" ORDER BY bm25(notes_fts) LIMIT 50");

    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();

    match conn.prepare(&sql) {
        Ok(mut stmt) => {
            let rows = stmt.query_map(param_refs.as_slice(), |row| {
                let score: f64 = row.get("score")?;
                let path: Option<String> = row.get("file_path")?;
                let modified: Option<String> = row.get("modified_at")?;
                let mut metadata = HashMap::new();
                if let Some(p) = path {
                    metadata.insert("path".into(), serde_json::Value::String(p));
                }
                if let Some(m) = modified {
                    metadata.insert("modified".into(), serde_json::Value::String(m));
                }
                Ok(SearchResult {
                    result_type: "note".into(),
                    id: row.get("id")?,
                    title: row
                        .get::<_, Option<String>>("title")?
                        .unwrap_or_else(|| "Untitled".into()),
                    snippet: row.get::<_, Option<String>>("snippet")?.unwrap_or_default(),
                    score: score.abs(),
                    metadata,
                })
            })?;
            Ok(rows.filter_map(|r| r.ok()).collect())
        }
        Err(_) => Ok(Vec::new()),
    }
}

fn search_contacts_fts(conn: &Connection, query: &str) -> Result<Vec<SearchResult>> {
    let sql = "SELECT
            'contact' as type,
            map.contact_identifier as id,
            COALESCE(c.given_name, '') || ' ' || COALESCE(c.family_name, '') as title,
            snippet(contacts_fts, 0, '**', '**', '...', 32) as snippet,
            bm25(contacts_fts) as score,
            c.organization,
            c.job_title
        FROM contacts_fts
        JOIN contacts_fts_map map ON contacts_fts.rowid = map.fts_rowid
        JOIN contacts c ON map.contact_identifier = c.identifier
        WHERE contacts_fts MATCH ?1
        ORDER BY bm25(contacts_fts)
        LIMIT 20";

    match conn.prepare(sql) {
        Ok(mut stmt) => {
            let rows = stmt.query_map([query], |row| {
                let score: f64 = row.get("score")?;
                let org: Option<String> = row.get("organization")?;
                let job: Option<String> = row.get("job_title")?;
                let mut metadata = HashMap::new();
                if let Some(o) = org {
                    metadata.insert("org".into(), serde_json::Value::String(o));
                }
                if let Some(j) = job {
                    metadata.insert("job_title".into(), serde_json::Value::String(j));
                }
                let title: String = row.get("title")?;
                Ok(SearchResult {
                    result_type: "contact".into(),
                    id: row.get("id")?,
                    title: if title.trim().is_empty() {
                        "Unknown".into()
                    } else {
                        title.trim().to_string()
                    },
                    snippet: row.get::<_, Option<String>>("snippet")?.unwrap_or_default(),
                    score: score.abs(),
                    metadata,
                })
            })?;
            Ok(rows.filter_map(|r| r.ok()).collect())
        }
        Err(_) => Ok(Vec::new()),
    }
}

fn search_photos_fts(
    conn: &Connection,
    query: &str,
    options: &SearchOptions,
) -> Result<Vec<SearchResult>> {
    let mut sql = String::from(
        "SELECT
            'photo' as type,
            CAST(map.asset_id AS TEXT) as id,
            COALESCE(a.title, a.filename) as title,
            snippet(photos_fts, 2, '**', '**', '...', 32) as snippet,
            bm25(photos_fts) as score,
            a.date_created,
            a.latitude,
            a.longitude
        FROM photos_fts
        JOIN photos_fts_map map ON photos_fts.rowid = map.fts_rowid
        JOIN photos_assets a ON map.asset_id = a.asset_id
        WHERE photos_fts MATCH ?1",
    );

    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(query.to_string())];

    if let Some(ref from) = options.date_from {
        sql.push_str(&format!(" AND a.date_created >= ?{}", params.len() + 1));
        params.push(Box::new(from.clone()));
    }
    if let Some(ref to) = options.date_to {
        sql.push_str(&format!(" AND a.date_created <= ?{}", params.len() + 1));
        params.push(Box::new(to.clone()));
    }

    sql.push_str(" ORDER BY bm25(photos_fts) LIMIT 30");

    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();

    match conn.prepare(&sql) {
        Ok(mut stmt) => {
            let rows = stmt.query_map(param_refs.as_slice(), |row| {
                let score: f64 = row.get("score")?;
                let date: Option<String> = row.get("date_created")?;
                let lat: Option<f64> = row.get("latitude")?;
                let lng: Option<f64> = row.get("longitude")?;
                let mut metadata = HashMap::new();
                if let Some(d) = date {
                    metadata.insert("date".into(), serde_json::Value::String(d));
                }
                if let Some(la) = lat {
                    metadata.insert("lat".into(), serde_json::json!(la));
                }
                if let Some(lo) = lng {
                    metadata.insert("lng".into(), serde_json::json!(lo));
                }
                Ok(SearchResult {
                    result_type: "photo".into(),
                    id: row.get("id")?,
                    title: row
                        .get::<_, Option<String>>("title")?
                        .unwrap_or_else(|| "Untitled".into()),
                    snippet: row.get::<_, Option<String>>("snippet")?.unwrap_or_default(),
                    score: score.abs(),
                    metadata,
                })
            })?;
            Ok(rows.filter_map(|r| r.ok()).collect())
        }
        Err(_) => Ok(Vec::new()),
    }
}

fn search_documents_fts(
    conn: &Connection,
    query: &str,
    options: &SearchOptions,
) -> Result<Vec<SearchResult>> {
    let mut sql = String::from(
        "SELECT
            'document' as type,
            CAST(map.document_id AS TEXT) as id,
            COALESCE(d.title, d.filename) as title,
            snippet(documents_fts, 2, '**', '**', '...', 32) as snippet,
            bm25(documents_fts) as score,
            d.file_path,
            d.file_type,
            d.modified_at
        FROM documents_fts
        JOIN documents_fts_map map ON documents_fts.rowid = map.fts_rowid
        JOIN documents d ON map.document_id = d.id
        WHERE documents_fts MATCH ?1",
    );

    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(query.to_string())];

    if let Some(ref from) = options.date_from {
        sql.push_str(&format!(" AND d.modified_at >= ?{}", params.len() + 1));
        params.push(Box::new(from.clone()));
    }
    if let Some(ref to) = options.date_to {
        sql.push_str(&format!(" AND d.modified_at <= ?{}", params.len() + 1));
        params.push(Box::new(to.clone()));
    }

    sql.push_str(" ORDER BY bm25(documents_fts) LIMIT 30");

    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();

    match conn.prepare(&sql) {
        Ok(mut stmt) => {
            let rows = stmt.query_map(param_refs.as_slice(), |row| {
                let score: f64 = row.get("score")?;
                let path: Option<String> = row.get("file_path")?;
                let ftype: Option<String> = row.get("file_type")?;
                let modified: Option<String> = row.get("modified_at")?;
                let mut metadata = HashMap::new();
                if let Some(p) = path {
                    metadata.insert("path".into(), serde_json::Value::String(p));
                }
                if let Some(ft) = ftype {
                    metadata.insert("file_type".into(), serde_json::Value::String(ft));
                }
                if let Some(m) = modified {
                    metadata.insert("modified".into(), serde_json::Value::String(m));
                }
                Ok(SearchResult {
                    result_type: "document".into(),
                    id: row.get("id")?,
                    title: row
                        .get::<_, Option<String>>("title")?
                        .unwrap_or_else(|| "Untitled".into()),
                    snippet: row.get::<_, Option<String>>("snippet")?.unwrap_or_default(),
                    score: score.abs(),
                    metadata,
                })
            })?;
            Ok(rows.filter_map(|r| r.ok()).collect())
        }
        Err(_) => Ok(Vec::new()),
    }
}

fn search_reminders_fts(
    conn: &Connection,
    query: &str,
    options: &SearchOptions,
) -> Result<Vec<SearchResult>> {
    let mut sql = String::from(
        "SELECT
            'reminder' as type,
            map.reminder_id as id,
            r.title,
            snippet(reminders_fts, 1, '**', '**', '...', 32) as snippet,
            bm25(reminders_fts) as score,
            r.list_name,
            r.due_date,
            r.is_completed,
            r.priority,
            r.location
        FROM reminders_fts
        JOIN reminders_fts_map map ON reminders_fts.rowid = map.fts_rowid
        JOIN reminders r ON map.reminder_id = r.reminder_id
        WHERE reminders_fts MATCH ?1",
    );

    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(query.to_string())];

    if let Some(ref from) = options.date_from {
        sql.push_str(&format!(
            " AND (r.due_date >= ?{0} OR r.creation_date >= ?{0})",
            params.len() + 1
        ));
        params.push(Box::new(from.clone()));
    }
    if let Some(ref to) = options.date_to {
        sql.push_str(&format!(
            " AND (r.due_date <= ?{0} OR r.creation_date <= ?{0})",
            params.len() + 1
        ));
        params.push(Box::new(to.clone()));
    }

    sql.push_str(" ORDER BY bm25(reminders_fts) LIMIT 30");

    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();

    match conn.prepare(&sql) {
        Ok(mut stmt) => {
            let rows = stmt.query_map(param_refs.as_slice(), |row| {
                let score: f64 = row.get("score")?;
                let list: Option<String> = row.get("list_name")?;
                let due: Option<String> = row.get("due_date")?;
                let completed: Option<bool> = row.get("is_completed")?;
                let priority: Option<i32> = row.get("priority")?;
                let location: Option<String> = row.get("location")?;
                let mut metadata = HashMap::new();
                if let Some(l) = list {
                    metadata.insert("list".into(), serde_json::Value::String(l));
                }
                if let Some(d) = due {
                    metadata.insert("due".into(), serde_json::Value::String(d));
                }
                if let Some(c) = completed {
                    metadata.insert("completed".into(), serde_json::json!(c));
                }
                if let Some(p) = priority {
                    metadata.insert("priority".into(), serde_json::json!(p));
                }
                if let Some(loc) = location {
                    metadata.insert("location".into(), serde_json::Value::String(loc));
                }
                Ok(SearchResult {
                    result_type: "reminder".into(),
                    id: row.get("id")?,
                    title: row
                        .get::<_, Option<String>>("title")?
                        .unwrap_or_else(|| "Untitled".into()),
                    snippet: row.get::<_, Option<String>>("snippet")?.unwrap_or_default(),
                    score: score.abs(),
                    metadata,
                })
            })?;
            Ok(rows.filter_map(|r| r.ok()).collect())
        }
        Err(_) => Ok(Vec::new()),
    }
}

// ========== Output Formatting ==========

pub fn format_text(results: &[SearchResult]) {
    if results.is_empty() {
        println!("No results found.");
        return;
    }

    for r in results {
        let score_pct = if r.score > 1.0 {
            (r.score * 100.0).min(100.0) as u32
        } else {
            (r.score * 100.0) as u32
        };

        println!();
        println!("[{}] {}", r.result_type.to_uppercase(), r.title);
        println!("Score: {score_pct}%");
        if !r.snippet.is_empty() {
            let display: String = r.snippet.chars().take(200).collect();
            println!("{display}");
        }

        let mut meta_parts = Vec::new();
        for key in &[
            "date",
            "modified",
            "due",
            "path",
            "org",
            "list",
            "file_type",
        ] {
            if let Some(val) = r.metadata.get(*key) {
                if let Some(s) = val.as_str() {
                    if !s.is_empty() {
                        meta_parts.push(format!("{}: {s}", key[..1].to_uppercase() + &key[1..]));
                    }
                }
            }
        }
        if !meta_parts.is_empty() {
            println!("  {}", meta_parts.join(" | "));
        }
    }
}

pub fn format_json(results: &[SearchResult]) -> Result<String> {
    Ok(serde_json::to_string_pretty(results)?)
}

pub fn format_csv(results: &[SearchResult]) -> String {
    let mut out = String::from("type,id,title,snippet,score,date\n");
    for r in results {
        let date = r
            .metadata
            .get("date")
            .or_else(|| r.metadata.get("modified"))
            .or_else(|| r.metadata.get("due"))
            .map(|v| v.as_str().unwrap_or("").to_string())
            .unwrap_or_default();
        let snippet: String = r.snippet.chars().take(100).collect();
        // Simple CSV escaping
        let title = r.title.replace('"', "\"\"");
        let snippet = snippet.replace('"', "\"\"");
        out.push_str(&format!(
            "{},\"{}\",\"{}\",\"{}\",{:.4},{}\n",
            r.result_type, r.id, title, snippet, r.score, date
        ));
    }
    out
}

pub fn format_markdown(results: &[SearchResult]) -> String {
    let mut lines = Vec::new();
    for r in results {
        let score_pct = if r.score <= 1.0 {
            (r.score * 100.0).min(100.0) as u32
        } else {
            (r.score * 10.0) as u32
        };
        lines.push(format!("## [{}] {}", r.result_type.to_uppercase(), r.title));
        lines.push(format!("**Score:** {score_pct}% | **ID:** {}", r.id));
        if !r.snippet.is_empty() {
            let display: String = r.snippet.chars().take(300).collect();
            lines.push(format!("\n{display}"));
        }
        let meta: Vec<String> = r
            .metadata
            .iter()
            .filter(|(_, v)| !v.is_null())
            .map(|(k, v)| {
                let val = match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                format!("**{k}:** {val}")
            })
            .collect();
        if !meta.is_empty() {
            lines.push(format!("\n_{}_", meta.join(" | ")));
        }
        lines.push(String::new());
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== escape_fts_query ==========

    #[test]
    fn escape_fts_query_empty_string() {
        assert_eq!(escape_fts_query(""), "\"\"");
    }

    #[test]
    fn escape_fts_query_whitespace_only() {
        assert_eq!(escape_fts_query("   "), "\"\"");
    }

    #[test]
    fn escape_fts_query_simple_terms() {
        let result = escape_fts_query("hello world");
        assert!(result.contains("hello"));
        assert!(result.contains("world"));
        assert!(result.contains(" OR "));
    }

    #[test]
    fn escape_fts_query_quoted_phrase() {
        let result = escape_fts_query("\"exact match\"");
        assert!(result.contains("\"exact match\""));
    }

    #[test]
    fn escape_fts_query_negation() {
        let result = escape_fts_query("-excluded");
        assert!(result.contains("NOT excluded"));
    }

    #[test]
    fn escape_fts_query_special_chars_stripped() {
        let result = escape_fts_query("test*query:with^special(chars)~more'stuff");
        assert!(!result.contains('*'));
        assert!(!result.contains(':'));
        assert!(!result.contains('^'));
        assert!(!result.contains('('));
        assert!(!result.contains(')'));
        assert!(!result.contains('~'));
        assert!(!result.contains('\''));
    }

    #[test]
    fn escape_fts_query_mixed_quoted_and_terms() {
        let result = escape_fts_query("\"hello world\" foo");
        assert!(result.contains("\"hello world\""));
        assert!(result.contains("foo"));
    }

    // ========== ensure_type_diversity ==========

    fn make_result(result_type: &str, id: &str, score: f64) -> SearchResult {
        SearchResult {
            result_type: result_type.into(),
            id: id.into(),
            title: format!("Title {id}"),
            snippet: String::new(),
            score,
            metadata: HashMap::new(),
        }
    }

    #[test]
    fn diversity_single_type_passthrough() {
        let results = vec![make_result("note", "1", 0.9), make_result("note", "2", 0.8)];
        let types = vec!["note".to_string()];
        let diverse = ensure_type_diversity(results.clone(), &types, 10);
        assert_eq!(diverse.len(), 2);
    }

    #[test]
    fn diversity_empty_results() {
        let results: Vec<SearchResult> = vec![];
        let types = vec!["note".to_string(), "contact".to_string()];
        let diverse = ensure_type_diversity(results, &types, 10);
        assert!(diverse.is_empty());
    }

    #[test]
    fn diversity_multi_type_balancing() {
        let mut results = Vec::new();
        for i in 0..10 {
            results.push(make_result("note", &format!("n{i}"), 0.9 - i as f64 * 0.01));
        }
        for i in 0..3 {
            results.push(make_result(
                "contact",
                &format!("c{i}"),
                0.5 - i as f64 * 0.01,
            ));
        }
        let types = vec!["note".to_string(), "contact".to_string()];
        let diverse = ensure_type_diversity(results, &types, 10);
        let contact_count = diverse
            .iter()
            .filter(|r| r.result_type == "contact")
            .count();
        // Contacts should get at least min_per_type representation
        assert!(contact_count >= 2);
    }

    // ========== format_csv ==========

    #[test]
    fn format_csv_basic() {
        let results = vec![make_result("note", "1", 0.95)];
        let csv = format_csv(&results);
        assert!(csv.starts_with("type,id,title,snippet,score,date\n"));
        assert!(csv.contains("note"));
        assert!(csv.contains("0.9500"));
    }

    #[test]
    fn format_csv_quote_escaping() {
        let mut r = make_result("note", "1", 0.5);
        r.title = "Title with \"quotes\"".into();
        let csv = format_csv(&[r]);
        assert!(csv.contains("\"\""));
    }

    // ========== format_markdown ==========

    #[test]
    fn format_markdown_score_display() {
        let results = vec![make_result("note", "1", 0.75)];
        let md = format_markdown(&results);
        assert!(md.contains("## [NOTE] Title 1"));
        assert!(md.contains("**Score:** 75%"));
        assert!(md.contains("**ID:** 1"));
    }

    #[test]
    fn format_markdown_with_metadata() {
        let mut r = make_result("note", "1", 0.5);
        r.metadata.insert(
            "path".into(),
            serde_json::Value::String("/some/path".into()),
        );
        let md = format_markdown(&[r]);
        assert!(md.contains("**path:** /some/path"));
    }

    // ========== FTS search integration tests ==========

    fn setup_notes_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        crate::db::init_search_schema(&conn).unwrap();

        // Create source tables
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS obsidian_notes (
                id INTEGER PRIMARY KEY,
                title TEXT,
                body TEXT,
                content TEXT,
                file_path TEXT,
                created_at TEXT,
                modified_at TEXT,
                vault_name TEXT
            );
            CREATE TABLE IF NOT EXISTS obsidian_tags (
                note_id INTEGER,
                tag TEXT
            );",
        )
        .unwrap();

        conn
    }

    fn seed_notes(conn: &Connection) {
        conn.execute(
            "INSERT INTO obsidian_notes (id, title, body, file_path, modified_at) VALUES (1, 'Rust Programming', 'Learning about ownership and borrowing in Rust', '/vault/rust.md', '2024-01-15')",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO obsidian_notes (id, title, body, file_path, modified_at) VALUES (2, 'Python Tips', 'Python decorators and generators explained', '/vault/python.md', '2024-01-16')",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO obsidian_notes (id, title, body, file_path, modified_at) VALUES (3, 'Meeting Notes', 'Discussed project timeline and Rust migration', '/vault/meeting.md', '2024-01-17')",
            [],
        ).unwrap();
    }

    fn setup_contacts_db(conn: &Connection) {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS contacts (
                identifier TEXT PRIMARY KEY,
                given_name TEXT,
                family_name TEXT,
                organization TEXT,
                job_title TEXT,
                note TEXT,
                birthday TEXT,
                nickname TEXT
            );",
        )
        .unwrap();
    }

    fn seed_contacts(conn: &Connection) {
        conn.execute(
            "INSERT INTO contacts (identifier, given_name, family_name, organization, job_title) VALUES ('c1', 'Alice', 'Smith', 'Acme Corp', 'Engineer')",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO contacts (identifier, given_name, family_name, organization, job_title) VALUES ('c2', 'Bob', 'Jones', 'Widgets Inc', 'Designer')",
            [],
        ).unwrap();
    }

    #[test]
    fn fts_search_notes_returns_matches() {
        let conn = setup_notes_db();
        seed_notes(&conn);
        crate::fts::rebuild_all_fts(&conn).unwrap();

        let options = SearchOptions {
            types: vec!["note".to_string()],
            limit: 10,
            date_from: None,
            date_to: None,

            min_score: 0.0,
        };
        let results = fts_search(&conn, "rust", &options).unwrap();
        assert!(!results.is_empty());
        assert!(results.iter().all(|r| r.result_type == "note"));
    }

    #[test]
    fn fts_search_contacts_returns_matches() {
        let conn = setup_notes_db();
        setup_contacts_db(&conn);
        seed_contacts(&conn);
        crate::fts::rebuild_all_fts(&conn).unwrap();

        let options = SearchOptions {
            types: vec!["contact".to_string()],
            limit: 10,
            date_from: None,
            date_to: None,

            min_score: 0.0,
        };
        let results = fts_search(&conn, "alice", &options).unwrap();
        assert!(!results.is_empty());
        assert!(results.iter().all(|r| r.result_type == "contact"));
    }

    #[test]
    fn fts_search_type_filtering() {
        let conn = setup_notes_db();
        setup_contacts_db(&conn);
        seed_notes(&conn);
        seed_contacts(&conn);
        crate::fts::rebuild_all_fts(&conn).unwrap();

        // Search for "rust" but only in contacts — should find nothing
        let options = SearchOptions {
            types: vec!["contact".to_string()],
            limit: 10,
            date_from: None,
            date_to: None,

            min_score: 0.0,
        };
        let results = fts_search(&conn, "rust", &options).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn fts_search_no_matches() {
        let conn = setup_notes_db();
        seed_notes(&conn);
        crate::fts::rebuild_all_fts(&conn).unwrap();

        let options = SearchOptions {
            types: vec!["note".to_string()],
            limit: 10,
            date_from: None,
            date_to: None,

            min_score: 0.0,
        };
        let results = fts_search(&conn, "xyznonexistent", &options).unwrap();
        assert!(results.is_empty());
    }
}
