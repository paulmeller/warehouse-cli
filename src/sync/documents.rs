use anyhow::Result;
use rusqlite::{params, Connection};
use std::collections::HashSet;
use std::io::Read;
use std::path::Path;
use walkdir::WalkDir;
use regex::Regex;

use crate::config::{self, Config};

/// Extract documents from configured directories into warehouse.
pub fn extract(conn: &Connection, config: &Config) -> Result<usize> {
    let dirs = config::discover_documents_directories();
    if dirs.is_empty() {
        anyhow::bail!("No document directories not found");
    }

    create_tables(conn)?;

    let doc_config = &config.documents;
    let extensions: HashSet<String> = doc_config.extensions.iter().cloned().collect();
    let max_size = doc_config.max_file_size_mb * 1024 * 1024;

    // Get existing file hashes for incremental sync
    let existing_hashes = get_existing_hashes(conn)?;

    let excluded_dirs: HashSet<&str> = [
        "node_modules",
        ".git",
        ".svn",
        "__pycache__",
        "venv",
        ".venv",
        "target",
        "build",
        "dist",
        ".next",
        ".cache",
    ]
    .into_iter()
    .collect();

    let mut count = 0;
    let mut skipped = 0;

    for dir in &dirs {
        for entry in WalkDir::new(dir)
            .into_iter()
            .filter_entry(|e| {
                let name = e.file_name().to_string_lossy();
                if doc_config.skip_hidden && name.starts_with('.') {
                    return false;
                }
                !excluded_dirs.contains(name.as_ref())
            })
            .filter_map(|e| e.ok())
        {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            let ext = path
                .extension()
                .map(|e| format!(".{}", e.to_string_lossy().to_lowercase()));
            let ext = match ext {
                Some(e) if extensions.contains(&e) => e,
                _ => continue,
            };

            let metadata = match std::fs::metadata(path) {
                Ok(m) => m,
                Err(_) => continue,
            };

            let file_size = metadata.len();
            if file_size > max_size || file_size == 0 {
                continue;
            }

            let file_hash = compute_file_hash(path);
            let path_str = path.to_string_lossy().to_string();

            // Skip if hash unchanged
            if let Some(ref hash) = file_hash {
                if existing_hashes.contains(hash) {
                    skipped += 1;
                    continue;
                }
            }

            let filename = path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();

            let title = path
                .file_stem()
                .map(|n| n.to_string_lossy().to_string());

            let content = extract_text_content(path, &ext);

            let modified_at = metadata
                .modified()
                .ok()
                .map(|t| {
                    chrono::DateTime::<chrono::Utc>::from(t)
                        .format("%Y-%m-%dT%H:%M:%S")
                        .to_string()
                });

            let created_at = metadata
                .created()
                .ok()
                .map(|t| {
                    chrono::DateTime::<chrono::Utc>::from(t)
                        .format("%Y-%m-%dT%H:%M:%S")
                        .to_string()
                });

            let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();

            conn.execute(
                "INSERT OR REPLACE INTO documents
                 (file_path, filename, title, content, file_type, file_size, file_hash,
                  created_at, modified_at, processed_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    path_str,
                    filename,
                    title,
                    content,
                    ext.trim_start_matches('.'),
                    file_size as i64,
                    file_hash,
                    created_at,
                    modified_at,
                    now,
                ],
            )?;
            count += 1;
        }
    }

    if skipped > 0 {
        eprintln!("  skipped {skipped} unchanged files");
    }
    Ok(count)
}

fn create_tables(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS documents (
            id INTEGER PRIMARY KEY,
            file_path TEXT UNIQUE,
            filename TEXT,
            title TEXT,
            content TEXT,
            file_type TEXT,
            file_size INTEGER,
            file_hash TEXT,
            author TEXT,
            created_at TEXT,
            modified_at TEXT,
            processed_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_documents_type ON documents(file_type);
        CREATE INDEX IF NOT EXISTS idx_documents_hash ON documents(file_hash);
        CREATE INDEX IF NOT EXISTS idx_documents_modified ON documents(modified_at);
        ",
    )?;
    Ok(())
}

fn get_existing_hashes(conn: &Connection) -> Result<HashSet<String>> {
    let mut hashes = HashSet::new();
    let has_table: bool = conn
        .query_row(
            "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='documents'",
            [],
            |r| r.get(0),
        )
        .unwrap_or(false);

    if !has_table {
        return Ok(hashes);
    }

    let mut stmt = conn.prepare("SELECT file_hash FROM documents WHERE file_hash IS NOT NULL")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    for row in rows {
        if let Ok(hash) = row {
            hashes.insert(hash);
        }
    }
    Ok(hashes)
}

/// Compute a simple hash of a file (first 8KB + size for speed).
fn compute_file_hash(path: &Path) -> Option<String> {
    let metadata = std::fs::metadata(path).ok()?;
    let size = metadata.len();

    let mut file = std::fs::File::open(path).ok()?;
    let mut buf = vec![0u8; 8192.min(size as usize)];
    let bytes_read = file.read(&mut buf).ok()?;
    buf.truncate(bytes_read);

    // Simple hash: combine size with a quick checksum of the first 8KB
    let checksum: u64 = buf.iter().enumerate().fold(0u64, |acc, (i, &b)| {
        acc.wrapping_add((b as u64).wrapping_mul(i as u64 + 1))
    });

    Some(format!("{:016x}{:016x}", size, checksum))
}

/// Extract text content from a file based on its extension.
fn extract_text_content(path: &Path, ext: &str) -> Option<String> {
    let result = match ext {
        ".txt" => std::fs::read_to_string(path).ok(),
        ".html" | ".htm" => std::fs::read_to_string(path).ok().map(|c| strip_html_tags(&c)),
        ".rtf" => std::fs::read_to_string(path).ok().map(|c| strip_rtf(&c)),
        ".pdf" => extract_pdf(path),
        ".docx" | ".doc" => extract_docx(path),
        ".pptx" | ".ppt" => extract_pptx(path),
        ".xlsx" | ".xls" => extract_xlsx(path),
        _ => None,
    };
    // Filter out empty/whitespace-only results
    result.filter(|s| !s.trim().is_empty())
}

/// Extract text from PDF using pdf-extract.
fn extract_pdf(path: &Path) -> Option<String> {
    let bytes = std::fs::read(path).ok()?;
    pdf_extract::extract_text_from_mem(&bytes).ok()
}

/// Extract text from DOCX (ZIP containing word/document.xml).
fn extract_docx(path: &Path) -> Option<String> {
    let file = std::fs::File::open(path).ok()?;
    let mut archive = zip::ZipArchive::new(file).ok()?;
    let mut content = archive.by_name("word/document.xml").ok()?;
    let mut xml = String::new();
    content.read_to_string(&mut xml).ok()?;
    Some(strip_xml_tags(&xml))
}

/// Extract text from PPTX (ZIP containing ppt/slides/slideN.xml).
fn extract_pptx(path: &Path) -> Option<String> {
    let file = std::fs::File::open(path).ok()?;
    let mut archive = zip::ZipArchive::new(file).ok()?;

    // Collect slide file names and sort them
    let mut slide_names: Vec<String> = (0..archive.len())
        .filter_map(|i| {
            let name = archive.by_index(i).ok()?.name().to_string();
            if name.starts_with("ppt/slides/slide") && name.ends_with(".xml") {
                Some(name)
            } else {
                None
            }
        })
        .collect();
    slide_names.sort();

    let mut texts = Vec::new();
    for name in &slide_names {
        if let Ok(mut entry) = archive.by_name(name) {
            let mut xml = String::new();
            if entry.read_to_string(&mut xml).is_ok() {
                let text = strip_xml_tags(&xml);
                if !text.trim().is_empty() {
                    texts.push(text);
                }
            }
        }
    }

    if texts.is_empty() {
        None
    } else {
        Some(texts.join("\n\n"))
    }
}

/// Extract text from XLSX using calamine.
fn extract_xlsx(path: &Path) -> Option<String> {
    use calamine::{Reader, open_workbook_auto};

    let mut workbook = open_workbook_auto(path).ok()?;
    let sheet_names: Vec<String> = workbook.sheet_names().to_vec();
    let mut texts = Vec::new();

    for name in &sheet_names {
        if let Ok(range) = workbook.worksheet_range(name) {
            let mut rows_text = Vec::new();
            for row in range.rows() {
                let cells: Vec<String> = row.iter().map(|c| c.to_string()).collect();
                let line = cells.join("\t");
                if !line.trim().is_empty() {
                    rows_text.push(line);
                }
            }
            if !rows_text.is_empty() {
                texts.push(format!("[{}]\n{}", name, rows_text.join("\n")));
            }
        }
    }

    if texts.is_empty() {
        None
    } else {
        Some(texts.join("\n\n"))
    }
}

/// Strip XML tags, keeping text content. Collapses whitespace.
fn strip_xml_tags(xml: &str) -> String {
    let re = Regex::new(r"<[^>]+>").unwrap();
    let text = re.replace_all(xml, " ");
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Basic HTML tag stripping.
fn strip_html_tags(html: &str) -> String {
    let mut result = String::with_capacity(html.len());
    let mut in_tag = false;
    for ch in html.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => result.push(ch),
            _ => {}
        }
    }
    // Collapse whitespace
    result.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Basic RTF content extraction (strip control words).
fn strip_rtf(rtf: &str) -> String {
    let mut result = String::new();
    let mut in_control = false;
    let mut brace_depth = 0;

    for ch in rtf.chars() {
        match ch {
            '{' => brace_depth += 1,
            '}' => brace_depth -= 1,
            '\\' => {
                in_control = true;
                continue;
            }
            ' ' | '\n' if in_control => {
                in_control = false;
                continue;
            }
            _ if in_control => continue,
            _ if brace_depth <= 1 => result.push(ch),
            _ => {}
        }
    }

    result
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}
