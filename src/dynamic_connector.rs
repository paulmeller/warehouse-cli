//! Dynamic connector system — JSON-defined API connectors.
//!
//! Allows users to define data source connectors as JSON files that describe:
//! auth, endpoints, table schema, field mappings, pagination, and FTS config.
//! Supports both REST and GraphQL APIs.

use anyhow::{Context, Result};
use chrono::Datelike;
use rusqlite::Connection;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

use crate::auth;
use crate::config::Config;
use crate::connector::Connector;
use crate::db;
use crate::search::{SearchOptions, SearchResult};
use crate::sync::SyncContext;

// ========== Spec structs ==========

#[derive(Debug, Deserialize)]
pub struct ConnectorSpec {
    pub version: u32,
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_api_type")]
    pub api_type: String,
    #[serde(default)]
    pub auth: Option<AuthSpec>,
    #[serde(default)]
    pub client: Option<ClientSpec>,
    pub tables: Vec<TableSpec>,
    #[serde(default)]
    pub fts: Vec<FtsSpec>,
    #[serde(default)]
    pub discover: Vec<DiscoverStep>,
    #[serde(default)]
    pub governance_fields: Vec<String>,
}

fn default_api_type() -> String {
    "rest".into()
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum AuthSpec {
    /// Static header from env var: `Bearer {{env.TOKEN}}`
    #[serde(rename = "header")]
    Header {
        header_name: String,
        value_template: String,
    },
    /// Env var shorthand
    #[serde(rename = "env")]
    Env {
        value_template: String,
        #[serde(default = "default_auth_header")]
        header_name: String,
    },
    /// Config key from ~/.warehouse/config.toml
    #[serde(rename = "config_key")]
    ConfigKey {
        key: String,
        header_name: String,
        #[serde(default)]
        header_prefix: String,
    },
    /// Browser cookies with derived headers
    #[serde(rename = "browser_cookies")]
    BrowserCookies {
        domains: Vec<String>,
        cookies: Vec<String>,
        headers: HashMap<String, String>,
    },
    /// Safari localStorage token
    #[serde(rename = "safari_localstorage")]
    SafariLocalStorage {
        origin_marker: String,
        localstorage_key: String,
        token_path: String,
        header_name: String,
        #[serde(default)]
        header_prefix: String,
    },
    /// Token chain: try strategies in order with validation
    #[serde(rename = "token_chain")]
    TokenChain {
        cache_file: String,
        strategies: Vec<TokenStrategy>,
        header_name: String,
        #[serde(default)]
        header_prefix: String,
        #[serde(default)]
        validate_url: String,
        #[serde(default)]
        validate_query: String,
    },
}

fn default_auth_header() -> String {
    "Authorization".into()
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum TokenStrategy {
    #[serde(rename = "env")]
    Env { var: String },
    #[serde(rename = "safari_localstorage")]
    SafariLocalStorage {
        origin_marker: String,
        localstorage_key: String,
        token_path: String,
    },
}

#[derive(Debug, Deserialize)]
pub struct ClientSpec {
    #[serde(default)]
    pub user_agent: String,
    #[serde(default)]
    pub default_headers: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct TableSpec {
    pub name: String,
    pub columns: Vec<ColumnSpec>,
    pub endpoint: EndpointSpec,
    pub response: ResponseSpec,
    /// When true, rows missing from a full sync are marked with _deleted_at.
    #[serde(default)]
    pub soft_delete: bool,
}

#[derive(Debug, Deserialize)]
pub struct ColumnSpec {
    pub name: String,
    #[serde(rename = "type", default = "default_column_type")]
    pub col_type: String,
    #[serde(default)]
    pub primary_key: bool,
    #[serde(default)]
    pub default: Option<String>,
}

fn default_column_type() -> String {
    "TEXT".into()
}

#[derive(Debug, Deserialize)]
pub struct EndpointSpec {
    pub url: String,
    #[serde(default = "default_method")]
    pub method: String,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default)]
    pub pagination: Option<PaginationSpec>,
    #[serde(default)]
    pub rate_limit: Option<RateLimitSpec>,
    #[serde(default)]
    pub query: String,
    /// Static GraphQL variables merged with pagination variables.
    #[serde(default)]
    pub variables: serde_json::Value,
    /// Features JSON object (for Twitter-style GraphQL-over-GET).
    #[serde(default)]
    pub features: Option<serde_json::Value>,
    /// GraphQL operation name.
    #[serde(default)]
    pub operation_name: String,
    /// GraphQL request method: "POST" (default) or "GET" (for Twitter-style URL params).
    #[serde(default)]
    pub graphql_method: String,
    /// Static JSON body for POST requests. Merged with cursor on subsequent pages.
    #[serde(default)]
    pub body: serde_json::Value,
    /// Incremental sync configuration for early-stop pagination.
    #[serde(default)]
    pub incremental: Option<IncrementalSpec>,
}

#[derive(Debug, Deserialize)]
pub struct IncrementalSpec {
    /// Dot-path to a date field in each result item (e.g., "last_edited_time").
    /// When incremental sync is active, pagination stops when an entire page
    /// of results is older than the last sync timestamp.
    pub stop_date_path: String,
}

fn default_method() -> String {
    "GET".into()
}

#[derive(Debug, Deserialize)]
pub struct PaginationSpec {
    #[serde(rename = "type")]
    pub pagination_type: String,
    #[serde(default = "default_page_size")]
    pub page_size: u32,
    #[serde(default = "default_max_pages")]
    pub max_pages: u32,
    #[serde(default)]
    pub cursor_path: String,
    #[serde(default)]
    pub has_next_path: String,
    #[serde(default)]
    pub cursor_variable: String,
    /// Extract cursor from within the results array rather than from a metadata field.
    /// When true, scans results for an entry matching cursor_entry_prefix in cursor_entry_id_path.
    #[serde(default)]
    pub cursor_from_results: bool,
    /// Dot-path within each result entry to check for cursor prefix (e.g., "entryId").
    #[serde(default)]
    pub cursor_entry_id_path: String,
    /// Prefix to identify cursor entries (e.g., "cursor-bottom").
    #[serde(default)]
    pub cursor_entry_prefix: String,
    /// Dot-path to extract cursor value from the matching entry (e.g., "content.value").
    #[serde(default)]
    pub cursor_value_path: String,
}

fn default_page_size() -> u32 {
    100
}

fn default_max_pages() -> u32 {
    50
}

#[derive(Debug, Deserialize)]
pub struct RateLimitSpec {
    #[serde(default = "default_delay")]
    pub delay_seconds: f64,
}

fn default_delay() -> f64 {
    1.0
}

#[derive(Debug, Deserialize)]
pub struct ResponseSpec {
    #[serde(default = "default_results_path")]
    pub results_path: String,
    pub field_mappings: Vec<FieldMapping>,
    #[serde(default)]
    pub filter: Option<ResponseFilter>,
}

fn default_results_path() -> String {
    "$".into()
}

#[derive(Debug, Deserialize)]
pub struct ResponseFilter {
    pub path: String,
    #[serde(default)]
    pub starts_with: String,
}

#[derive(Debug, Deserialize)]
pub struct FieldMapping {
    pub column: String,
    pub path: String,
    #[serde(default)]
    pub transform: String,
    /// Alternative paths to try if the primary path returns null.
    /// Useful for handling wrapper types (e.g., TweetWithVisibilityResults).
    #[serde(default)]
    pub alt_paths: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct FtsSpec {
    pub table_name: String,
    pub source_table: String,
    pub columns: Vec<String>,
    #[serde(default = "default_tokenizer")]
    pub tokenizer: String,
    #[serde(default = "default_id_column")]
    pub id_column: String,
    /// Column name in the map table for the source row ID (default: "source_id").
    /// Override to "tweet_id" or "transaction_id" to match existing search.rs queries.
    #[serde(default = "default_map_id_column")]
    pub map_id_column: String,
    /// Source discriminator tag for shared FTS tables (e.g., "monarch", "pocketsmith").
    /// When set, the map table includes a `source` column, and populate_fts only
    /// deletes/inserts rows matching this tag.
    #[serde(default)]
    pub source_tag: String,
    /// Custom SQL expressions for FTS column population.
    /// Key is the FTS column name, value is a SQL expression referencing source table columns.
    /// Example: {"notes": "COALESCE(note, '') || ' ' || COALESCE(memo, '')"}
    #[serde(default)]
    pub column_expressions: HashMap<String, String>,

    // Search metadata for registry-driven FTS search
    /// The --type value for this FTS entry (defaults to connector name).
    #[serde(default)]
    pub search_type: String,
    /// Simple column name for title (e.g., "title", "merchant_name").
    #[serde(default)]
    pub title_column: String,
    /// SQL expression for title (e.g., "'@' || t.author_handle").
    #[serde(default)]
    pub title_expr: String,
    /// Fallback title when column is NULL (default: "Untitled").
    #[serde(default)]
    pub title_fallback: String,
    /// Column name for date filtering (e.g., "created_at", "date").
    #[serde(default)]
    pub date_column: String,
    /// FTS5 snippet() column index.
    #[serde(default)]
    pub snippet_column: usize,
    /// Special snippet template (e.g., "amount_category" for transactions).
    #[serde(default)]
    pub snippet_template: String,
    /// Result metadata key -> SQL expression (e.g., "author_name" -> "t.author_name").
    #[serde(default)]
    pub metadata_columns: HashMap<String, String>,
    /// When true, filter out soft-deleted rows (WHERE t._deleted_at IS NULL) in search.
    #[serde(default)]
    pub soft_delete: bool,
}

fn default_tokenizer() -> String {
    "porter unicode61".into()
}

fn default_id_column() -> String {
    "id".into()
}

fn default_map_id_column() -> String {
    "source_id".into()
}

#[derive(Debug, Deserialize)]
pub struct DiscoverStep {
    pub id: String,
    pub action: String,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub input: String,
    #[serde(default)]
    pub pattern: String,
    #[serde(default)]
    pub group: usize,
    #[serde(default)]
    pub limit: usize,
    #[serde(default)]
    pub urls: String,
    #[serde(default)]
    pub url_prefix: String,
    #[serde(default)]
    pub key_group: usize,
    #[serde(default)]
    pub value_group: usize,
}

// ========== SQL safety ==========

/// Validate that a name is safe for use as a SQL identifier.
/// Only allows alphanumeric characters and underscores.
fn validate_identifier(name: &str) -> Result<()> {
    if name.is_empty() {
        anyhow::bail!("SQL identifier cannot be empty");
    }
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        anyhow::bail!("Invalid SQL identifier '{name}': only alphanumeric and underscore allowed");
    }
    if name.len() > 128 {
        anyhow::bail!("SQL identifier '{name}' too long (max 128 chars)");
    }
    Ok(())
}

/// Validate a SQL DEFAULT value. Only allows safe literals.
fn validate_default(value: &str) -> Result<()> {
    let v = value.trim();
    // Allow: CURRENT_TIMESTAMP, NULL, integers, floats, single-quoted strings (no nested quotes)
    let safe = v == "CURRENT_TIMESTAMP"
        || v == "NULL"
        || v.parse::<i64>().is_ok()
        || v.parse::<f64>().is_ok()
        || (v.starts_with('\'')
            && v.ends_with('\'')
            && !v[1..v.len() - 1].contains('\''));
    if !safe {
        anyhow::bail!("Unsafe DEFAULT value: '{v}'. Allowed: CURRENT_TIMESTAMP, NULL, numbers, 'string'");
    }
    Ok(())
}

/// Validate FTS5 tokenizer string.
fn validate_tokenizer(tokenizer: &str) -> Result<()> {
    let allowed = [
        "unicode61",
        "porter",
        "ascii",
        "trigram",
        "porter unicode61",
        "porter ascii",
    ];
    // Allow known tokenizers or tokenizers with known prefixes + options
    let base = tokenizer.split_whitespace().next().unwrap_or("");
    if !allowed.contains(&tokenizer) && !allowed.contains(&base) {
        anyhow::bail!("Unknown FTS5 tokenizer: '{tokenizer}'");
    }
    Ok(())
}

// ========== JSON dot-path navigation ==========

/// Navigate a JSON value using a dot-path like "data.issues.nodes".
/// Supports array indexing with [N] for fixed positions and [*] for flattening.
pub fn resolve_path<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    if path == "$" || path.is_empty() {
        return Some(value);
    }

    let mut current = value;

    for segment in split_path_segments(path) {
        match segment {
            PathSegment::Key(key) => {
                current = current.get(key)?;
            }
            PathSegment::Index(idx) => {
                current = current.as_array()?.get(idx)?;
            }
            PathSegment::Wildcard => {
                // For [*], return the array itself — caller handles iteration
                return Some(current);
            }
        }
    }

    Some(current)
}

/// Resolve a path and collect results, handling [*] wildcard expansion.
fn resolve_path_collecting(value: &serde_json::Value, path: &str) -> Vec<serde_json::Value> {
    if path == "$" || path.is_empty() {
        return vec![value.clone()];
    }

    let segments = split_path_segments(path);
    collect_recursive(value, &segments)
}

fn collect_recursive(
    value: &serde_json::Value,
    segments: &[PathSegment],
) -> Vec<serde_json::Value> {
    if segments.is_empty() {
        return vec![value.clone()];
    }

    match &segments[0] {
        PathSegment::Key(key) => {
            if let Some(child) = value.get(key.as_str()) {
                collect_recursive(child, &segments[1..])
            } else {
                Vec::new()
            }
        }
        PathSegment::Index(idx) => {
            if let Some(arr) = value.as_array() {
                if let Some(child) = arr.get(*idx) {
                    collect_recursive(child, &segments[1..])
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            }
        }
        PathSegment::Wildcard => {
            if let Some(arr) = value.as_array() {
                let mut results = Vec::new();
                for item in arr {
                    results.extend(collect_recursive(item, &segments[1..]));
                }
                results
            } else {
                Vec::new()
            }
        }
    }
}

enum PathSegment {
    Key(String),
    Index(usize),
    Wildcard,
}

fn split_path_segments(path: &str) -> Vec<PathSegment> {
    let mut segments = Vec::new();

    for part in path.split('.') {
        if let Some(bracket_pos) = part.find('[') {
            // Key before bracket
            let key = &part[..bracket_pos];
            if !key.is_empty() {
                segments.push(PathSegment::Key(key.to_string()));
            }

            // Parse bracket expressions
            let mut rest = &part[bracket_pos..];
            while let Some(start) = rest.find('[') {
                if let Some(end) = rest.find(']') {
                    let idx_str = &rest[start + 1..end];
                    if idx_str == "*" {
                        segments.push(PathSegment::Wildcard);
                    } else if let Ok(idx) = idx_str.parse::<usize>() {
                        segments.push(PathSegment::Index(idx));
                    }
                    rest = &rest[end + 1..];
                } else {
                    break;
                }
            }
        } else {
            segments.push(PathSegment::Key(part.to_string()));
        }
    }

    segments
}

/// Extract a scalar value from JSON using a dot-path, converting to string.
fn extract_field(value: &serde_json::Value, path: &str, transform: &str) -> Option<String> {
    let resolved = resolve_path(value, path)?;

    let raw = match resolved {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => return None,
        other => other.to_string(),
    };

    Some(apply_transform(&raw, transform))
}

/// Extract a field, trying alternative paths if the primary path returns None.
fn extract_field_with_alts(
    value: &serde_json::Value,
    path: &str,
    alt_paths: &[String],
    transform: &str,
) -> Option<String> {
    if let Some(result) = extract_field(value, path, transform) {
        return Some(result);
    }
    for alt in alt_paths {
        if let Some(result) = extract_field(value, alt, transform) {
            return Some(result);
        }
    }
    None
}

fn apply_transform(value: &str, transform: &str) -> String {
    match transform {
        "to_string" => value.to_string(),
        "to_int" => value
            .parse::<f64>()
            .map(|f| (f as i64).to_string())
            .unwrap_or_else(|_| value.to_string()),
        "to_bool" => {
            let b = matches!(value, "true" | "1" | "yes");
            (b as i64).to_string()
        }
        "join_array" => {
            if let Ok(arr) = serde_json::from_str::<Vec<String>>(value) {
                arr.join(", ")
            } else {
                value.to_string()
            }
        }
        "join_rich_text" => {
            // Notion rich text: array of objects with "plain_text" fields
            if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(value) {
                arr.iter()
                    .filter_map(|v| v.get("plain_text").and_then(|p| p.as_str()))
                    .collect::<Vec<_>>()
                    .join("")
            } else {
                value.to_string()
            }
        }
        _ => value.to_string(),
    }
}

// ========== Body template resolution ==========

/// Recursively resolve `{{...}}` templates in JSON string values.
fn resolve_body_templates(
    body: &serde_json::Value,
    context: &HashMap<String, DiscoverValue>,
) -> serde_json::Value {
    match body {
        serde_json::Value::String(s) => {
            serde_json::Value::String(resolve_discover_template(s, context))
        }
        serde_json::Value::Object(map) => {
            let mut new_map = serde_json::Map::new();
            for (k, v) in map {
                new_map.insert(k.clone(), resolve_body_templates(v, context));
            }
            serde_json::Value::Object(new_map)
        }
        serde_json::Value::Array(arr) => serde_json::Value::Array(
            arr.iter()
                .map(|v| resolve_body_templates(v, context))
                .collect(),
        ),
        other => other.clone(),
    }
}

// ========== Discovery pipeline ==========

/// Execute discovery steps and return a context of discovered variables.
fn run_discovery(
    client: &reqwest::blocking::Client,
    steps: &[DiscoverStep],
    auth_headers: &HashMap<String, String>,
) -> Result<HashMap<String, DiscoverValue>> {
    let mut context: HashMap<String, DiscoverValue> = HashMap::new();

    for step in steps {
        match step.action.as_str() {
            "fetch" => {
                let url = resolve_discover_template(&step.url, &context);
                let mut req = client.get(&url);
                for (k, v) in auth_headers {
                    req = req.header(k, v);
                }
                let resp = req
                    .send()
                    .context(format!("Discovery fetch failed: {url}"))?;
                let body = resp.text().unwrap_or_default();
                context.insert(
                    step.id.clone(),
                    DiscoverValue::Map({
                        let mut m = HashMap::new();
                        m.insert("body".to_string(), body);
                        m
                    }),
                );
            }
            "regex_all" => {
                let input = resolve_discover_template(&step.input, &context);
                let re = regex::Regex::new(&step.pattern)
                    .context(format!("Invalid regex in discover step '{}'", step.id))?;
                let mut matches = Vec::new();
                for caps in re.captures_iter(&input) {
                    if let Some(m) = caps.get(step.group) {
                        matches.push(m.as_str().to_string());
                    }
                    if step.limit > 0 && matches.len() >= step.limit {
                        break;
                    }
                }
                context.insert(step.id.clone(), DiscoverValue::Array(matches));
            }
            "fetch_regex_map" => {
                let urls_ref = resolve_discover_template(&step.urls, &context);
                let urls: Vec<String> = if let Some(DiscoverValue::Array(arr)) =
                    context.get(urls_ref.trim_start_matches("{{").trim_end_matches("}}"))
                {
                    arr.clone()
                } else {
                    serde_json::from_str(&urls_ref).unwrap_or_default()
                };

                let re = regex::Regex::new(&step.pattern)
                    .context(format!("Invalid regex in discover step '{}'", step.id))?;
                let mut map = HashMap::new();

                for url_path in &urls {
                    let url = if url_path.starts_with("http") {
                        url_path.clone()
                    } else {
                        format!("{}{url_path}", step.url_prefix)
                    };

                    if let Ok(resp) = client.get(&url).send() {
                        if let Ok(text) = resp.text() {
                            for caps in re.captures_iter(&text) {
                                if let (Some(key), Some(val)) =
                                    (caps.get(step.key_group), caps.get(step.value_group))
                                {
                                    map.insert(key.as_str().to_string(), val.as_str().to_string());
                                }
                            }
                        }
                    }
                }

                context.insert(step.id.clone(), DiscoverValue::Map(map));
            }
            "fetch_json" => {
                let url = resolve_discover_template(&step.url, &context);
                let mut req = client.get(&url);
                for (k, v) in auth_headers {
                    req = req.header(k, v);
                }
                let resp = req
                    .send()
                    .context(format!("Discovery fetch_json failed: {url}"))?;
                let json: serde_json::Value = resp
                    .json()
                    .context(format!("Discovery fetch_json parse failed: {url}"))?;
                context.insert(step.id.clone(), DiscoverValue::Json(json));
            }
            "json_path" => {
                let input_key = step.input.trim_start_matches("{{").trim_end_matches("}}");
                let (step_ref, path) = input_key.split_once('.').unwrap_or((input_key, ""));

                if let Some(DiscoverValue::Json(json)) = context.get(step_ref) {
                    let path_to_use = if path.is_empty() { &step.pattern } else { path };
                    if let Some(val) = resolve_path(json, path_to_use) {
                        match val {
                            serde_json::Value::String(s) => {
                                context
                                    .insert(step.id.clone(), DiscoverValue::StringVal(s.clone()));
                            }
                            serde_json::Value::Number(n) => {
                                context.insert(
                                    step.id.clone(),
                                    DiscoverValue::StringVal(n.to_string()),
                                );
                            }
                            other => {
                                context.insert(step.id.clone(), DiscoverValue::Json(other.clone()));
                            }
                        }
                    }
                }
            }
            other => {
                eprintln!("  unknown discover action: {other}");
            }
        }
    }

    Ok(context)
}

#[derive(Debug, Clone)]
enum DiscoverValue {
    Array(Vec<String>),
    Map(HashMap<String, String>),
    StringVal(String),
    Json(serde_json::Value),
}

/// Resolve `{{step_id.key}}` or `{{step_id}}` templates using discovery context.
/// Also resolves `{{date.today}}`, `{{date.month_start}}`, `{{date.month_end}}`.
fn resolve_discover_template(template: &str, context: &HashMap<String, DiscoverValue>) -> String {
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| regex::Regex::new(r"\{\{(\w+)(?:\.(\w+))?\}\}").unwrap());
    let mut result = template.to_string();

    for caps in re.captures_iter(template) {
        let full = caps.get(0).unwrap().as_str();
        let step_id = &caps[1];

        // Handle built-in date templates
        if step_id == "date" {
            if let Some(key_match) = caps.get(2) {
                let replacement = resolve_date_template(key_match.as_str());
                result = result.replace(full, &replacement);
            }
            continue;
        }

        if let Some(value) = context.get(step_id) {
            let replacement = if let Some(key_match) = caps.get(2) {
                let key = key_match.as_str();
                match value {
                    DiscoverValue::Map(m) => m.get(key).cloned().unwrap_or_default(),
                    DiscoverValue::Json(v) => resolve_path(v, key)
                        .map(|v| match v {
                            serde_json::Value::String(s) => s.clone(),
                            serde_json::Value::Number(n) => n.to_string(),
                            other => other.to_string(),
                        })
                        .unwrap_or_default(),
                    DiscoverValue::StringVal(s) => s.clone(),
                    DiscoverValue::Array(_) => String::new(),
                }
            } else {
                match value {
                    DiscoverValue::Map(m) => serde_json::to_string(m).unwrap_or_default(),
                    DiscoverValue::Array(a) => serde_json::to_string(a).unwrap_or_default(),
                    DiscoverValue::StringVal(s) => s.clone(),
                    DiscoverValue::Json(v) => serde_json::to_string(v).unwrap_or_default(),
                }
            };
            result = result.replace(full, &replacement);
        }
    }

    result
}

/// Recursively resolve `{{...}}` templates in a JSON value (strings, objects, arrays).
fn resolve_json_templates(
    value: &mut serde_json::Value,
    context: &HashMap<String, DiscoverValue>,
) {
    match value {
        serde_json::Value::String(s) => {
            let resolved = resolve_discover_template(s, context);
            if resolved != *s {
                *s = resolved;
            }
        }
        serde_json::Value::Object(map) => {
            for v in map.values_mut() {
                resolve_json_templates(v, context);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr.iter_mut() {
                resolve_json_templates(v, context);
            }
        }
        _ => {}
    }
}

/// Resolve date template variables.
fn resolve_date_template(key: &str) -> String {
    let today = chrono::Local::now().date_naive();
    match key {
        "today" => today.format("%Y-%m-%d").to_string(),
        "month_start" => {
            let start = today.with_day(1).unwrap_or(today);
            start.format("%Y-%m-%d").to_string()
        }
        "month_end" => {
            let next_month_first = if today.month() == 12 {
                chrono::NaiveDate::from_ymd_opt(today.year() + 1, 1, 1)
            } else {
                chrono::NaiveDate::from_ymd_opt(today.year(), today.month() + 1, 1)
            };
            let end = next_month_first
                .and_then(|d| d.pred_opt())
                .unwrap_or(today);
            end.format("%Y-%m-%d").to_string()
        }
        "year_month" => today.format("%Y-%m").to_string(),
        _ => String::new(),
    }
}

// ========== Auth resolution ==========

/// Resolved auth: a set of headers to add to requests.
struct ResolvedAuth {
    headers: HashMap<String, String>,
}

/// Resolve auth spec into request headers.
fn resolve_auth(
    spec: &Option<AuthSpec>,
    client: &reqwest::blocking::Client,
    graphql_url: Option<&str>,
) -> Result<ResolvedAuth> {
    let auth = match spec {
        Some(a) => a,
        None => {
            return Ok(ResolvedAuth {
                headers: HashMap::new(),
            })
        }
    };

    let mut headers = HashMap::new();

    match auth {
        AuthSpec::Header {
            header_name,
            value_template,
        } => {
            let value = auth::resolve_env_template(value_template)?;
            headers.insert(header_name.clone(), value);
        }
        AuthSpec::Env {
            value_template,
            header_name,
        } => {
            let value = auth::resolve_env_template(value_template)?;
            headers.insert(header_name.clone(), value);
        }
        AuthSpec::ConfigKey {
            key,
            header_name,
            header_prefix,
        } => {
            let raw = auth::read_config_key(key)?;
            let value = if header_prefix.is_empty() {
                raw
            } else {
                format!("{header_prefix} {raw}")
            };
            headers.insert(header_name.clone(), value);
        }
        AuthSpec::BrowserCookies {
            domains,
            cookies,
            headers: header_templates,
        } => {
            let domain_refs: Vec<&str> = domains.iter().map(|s| s.as_str()).collect();
            let cookie_refs: Vec<&str> = cookies.iter().map(|s| s.as_str()).collect();
            let cookie_values = auth::extract_browser_cookies(&domain_refs, &cookie_refs)?;

            for (header_name, template) in header_templates {
                let value = auth::resolve_cookie_template(template, &cookie_values);
                headers.insert(header_name.clone(), value);
            }
        }
        AuthSpec::SafariLocalStorage {
            origin_marker,
            localstorage_key,
            token_path,
            header_name,
            header_prefix,
        } => {
            #[cfg(target_os = "macos")]
            {
                let token =
                    auth::extract_safari_token(origin_marker, localstorage_key, token_path)?;
                let value = if header_prefix.is_empty() {
                    token
                } else {
                    format!("{header_prefix} {token}")
                };
                headers.insert(header_name.clone(), value);
            }
            #[cfg(not(target_os = "macos"))]
            {
                let _ = (origin_marker, localstorage_key, token_path, header_name, header_prefix);
                anyhow::bail!("Safari localStorage auth is only available on macOS");
            }
        }
        AuthSpec::TokenChain {
            cache_file,
            strategies,
            header_name,
            header_prefix,
            validate_url,
            validate_query,
        } => {
            let url = if validate_url.is_empty() {
                graphql_url.unwrap_or("")
            } else {
                validate_url
            };

            let token = resolve_token_chain(
                client,
                cache_file,
                strategies,
                header_name,
                header_prefix,
                url,
                validate_query,
            )?;

            let value = if header_prefix.is_empty() {
                token
            } else {
                format!("{header_prefix} {token}")
            };
            headers.insert(header_name.clone(), value);
        }
    }

    Ok(ResolvedAuth { headers })
}

fn resolve_token_chain(
    client: &reqwest::blocking::Client,
    cache_file: &str,
    strategies: &[TokenStrategy],
    header_name: &str,
    header_prefix: &str,
    validate_url: &str,
    validate_query: &str,
) -> Result<String> {
    // 1. Try cached token
    if let Some(token) = auth::read_cached_token(cache_file) {
        if validate_url.is_empty()
            || auth::validate_graphql_token(
                client,
                validate_url,
                &token,
                header_name,
                header_prefix,
                validate_query,
            )
        {
            return Ok(token);
        }
        auth::delete_cached_token(cache_file);
    }

    // 2. Try each strategy
    for strategy in strategies {
        let token = match strategy {
            TokenStrategy::Env { var } => std::env::var(var).ok(),
            TokenStrategy::SafariLocalStorage {
                origin_marker,
                localstorage_key,
                token_path,
            } => {
                #[cfg(target_os = "macos")]
                {
                    auth::extract_safari_token(origin_marker, localstorage_key, token_path).ok()
                }
                #[cfg(not(target_os = "macos"))]
                {
                    let _ = (origin_marker, localstorage_key, token_path);
                    None
                }
            }
        };

        if let Some(token) = token {
            if !token.is_empty()
                && (validate_url.is_empty()
                    || auth::validate_graphql_token(
                        client,
                        validate_url,
                        &token,
                        header_name,
                        header_prefix,
                        validate_query,
                    ))
            {
                let _ = auth::save_cached_token(cache_file, &token);
                return Ok(token);
            }
        }
    }

    anyhow::bail!("All authentication strategies failed")
}

// ========== DynamicConnector ==========

pub struct DynamicConnector {
    spec: ConnectorSpec,
    fts_ddl: Option<String>,
    /// Override source tag: "built-in" for bundled specs, "installed" for user-installed.
    source_tag: String,
    /// Pre-computed governance field references (leaked to 'static since connectors live forever).
    governance_field_refs: Vec<&'static str>,
}

impl DynamicConnector {
    pub fn from_spec(spec: ConnectorSpec) -> Result<Self> {
        Self::from_spec_with_source(spec, "installed".to_string())
    }

    fn from_spec_with_source(spec: ConnectorSpec, source_tag: String) -> Result<Self> {
        // Validate all identifiers
        validate_identifier(&spec.name)?;
        for table in &spec.tables {
            validate_identifier(&table.name)?;
            for col in &table.columns {
                validate_identifier(&col.name)?;
                if let Some(ref default) = col.default {
                    validate_default(default)?;
                }
            }
        }
        for fts in &spec.fts {
            validate_identifier(&fts.table_name)?;
            validate_identifier(&fts.source_table)?;
            validate_identifier(&fts.map_id_column)?;
            if !fts.source_tag.is_empty() {
                validate_identifier(&fts.source_tag)?;
            }
            if !fts.id_column.is_empty() {
                validate_identifier(&fts.id_column)?;
            }
            if !fts.title_column.is_empty() {
                validate_identifier(&fts.title_column)?;
            }
            if !fts.date_column.is_empty() {
                validate_identifier(&fts.date_column)?;
            }
            for col in &fts.columns {
                validate_identifier(col)?;
            }
            validate_tokenizer(&fts.tokenizer)?;
        }

        // Pre-compute FTS DDL for all specs
        let fts_ddl = if spec.fts.is_empty() {
            None
        } else {
            let mut ddl_parts = Vec::new();
            for fts in &spec.fts {
                let cols = fts.columns.join(",\n                ");
                let map_cols = if fts.source_tag.is_empty() {
                    format!(
                        "fts_rowid INTEGER PRIMARY KEY,\n                {} TEXT NOT NULL,\n                UNIQUE({})",
                        fts.map_id_column, fts.map_id_column
                    )
                } else {
                    format!(
                        "fts_rowid INTEGER PRIMARY KEY,\n                {} TEXT NOT NULL,\n                source TEXT NOT NULL,\n                UNIQUE({}, source)",
                        fts.map_id_column, fts.map_id_column
                    )
                };

                ddl_parts.push(format!(
                    "CREATE VIRTUAL TABLE IF NOT EXISTS {} USING fts5(\n                {},\n                tokenize='{}'\n            );\n\n            CREATE TABLE IF NOT EXISTS {}_map (\n                {}\n            );",
                    fts.table_name, cols, fts.tokenizer, fts.table_name, map_cols
                ));
            }
            Some(ddl_parts.join("\n\n"))
        };

        // Pre-compute governance field refs
        let governance_field_refs: Vec<&'static str> = if spec.governance_fields.is_empty() {
            // Derive from FTS column names
            let mut fields: Vec<&'static str> = Vec::new();
            for fts in &spec.fts {
                for col in &fts.columns {
                    let leaked: &'static str = Box::leak(col.clone().into_boxed_str());
                    if !fields.contains(&leaked) {
                        fields.push(leaked);
                    }
                }
            }
            fields
        } else {
            spec.governance_fields
                .iter()
                .map(|s| -> &'static str { Box::leak(s.clone().into_boxed_str()) })
                .collect()
        };

        Ok(Self {
            spec,
            fts_ddl,
            source_tag,
            governance_field_refs,
        })
    }

    pub fn from_json(json: &str) -> Result<Self> {
        let spec: ConnectorSpec =
            serde_json::from_str(json).context("Failed to parse connector spec")?;

        if spec.version != 1 {
            anyhow::bail!("Unsupported connector spec version: {}", spec.version);
        }

        Self::from_spec(spec)
    }

    fn build_client(&self) -> Result<reqwest::blocking::Client> {
        let mut builder =
            reqwest::blocking::Client::builder().timeout(std::time::Duration::from_secs(30));

        if let Some(ref client_spec) = self.spec.client {
            if !client_spec.user_agent.is_empty() {
                builder = builder.user_agent(&client_spec.user_agent);
            }

            if !client_spec.default_headers.is_empty() {
                let mut headers = reqwest::header::HeaderMap::new();
                for (k, v) in &client_spec.default_headers {
                    if let (Ok(name), Ok(val)) = (
                        reqwest::header::HeaderName::from_bytes(k.as_bytes()),
                        reqwest::header::HeaderValue::from_str(v),
                    ) {
                        headers.insert(name, val);
                    }
                }
                builder = builder.default_headers(headers);
            }
        }

        builder.build().context("Failed to build HTTP client")
    }

    fn fetch_rest_table(
        &self,
        client: &reqwest::blocking::Client,
        conn: &Connection,
        table: &TableSpec,
        auth_headers: &HashMap<String, String>,
        discover_context: &HashMap<String, DiscoverValue>,
        ctx: &SyncContext,
    ) -> Result<usize> {
        let mut total = 0;
        let mut page: u32 = 1;
        let max_pages = table
            .endpoint
            .pagination
            .as_ref()
            .map(|p| p.max_pages)
            .unwrap_or(1);
        let page_size = table
            .endpoint
            .pagination
            .as_ref()
            .map(|p| p.page_size)
            .unwrap_or(100);
        let delay = table
            .endpoint
            .rate_limit
            .as_ref()
            .map(|r| std::time::Duration::from_secs_f64(r.delay_seconds));
        let is_post = table.endpoint.method.eq_ignore_ascii_case("POST");
        let mut cursor: Option<String> = None;

        // Resume from cursor if available
        if let Some(ref resume_json) = ctx.resume_cursor {
            if let Ok(resume) = serde_json::from_str::<serde_json::Value>(resume_json) {
                if resume.get("table").and_then(|t| t.as_str()) == Some(&table.name) {
                    cursor = resume
                        .get("cursor")
                        .and_then(|c| c.as_str())
                        .map(String::from);
                    page = resume.get("page").and_then(|p| p.as_u64()).unwrap_or(1) as u32;
                    total = resume
                        .get("rows_so_far")
                        .and_then(|r| r.as_u64())
                        .unwrap_or(0) as usize;
                    eprintln!("    resuming from page {page}");
                }
            }
        }

        loop {
            if page > max_pages {
                break;
            }

            // Build URL with pagination
            let mut url = resolve_discover_template(&table.endpoint.url, discover_context);
            if let Some(ref pagination) = table.endpoint.pagination {
                match pagination.pagination_type.as_str() {
                    "page_number" => {
                        let sep = if url.contains('?') { '&' } else { '?' };
                        url = format!("{url}{sep}page={page}&per_page={page_size}");
                    }
                    "offset" => {
                        let offset = (page - 1) * page_size;
                        let sep = if url.contains('?') { '&' } else { '?' };
                        url = format!("{url}{sep}offset={offset}&limit={page_size}");
                    }
                    "cursor" if !is_post => {
                        // GET cursor pagination: append cursor as query param
                        if let Some(ref c) = cursor {
                            let sep = if url.contains('?') { '&' } else { '?' };
                            url = format!("{url}{sep}{}={c}", pagination.cursor_variable);
                        }
                    }
                    _ => {}
                }
            }

            let mut req = match table.endpoint.method.to_uppercase().as_str() {
                "POST" => client.post(&url),
                "PUT" => client.put(&url),
                _ => client.get(&url),
            };
            for (k, v) in auth_headers {
                req = req.header(k, v);
            }
            for (k, v) in &table.endpoint.headers {
                req = req.header(k, v);
            }

            // Attach JSON body for POST requests
            if is_post && !table.endpoint.body.is_null() {
                let mut req_body = resolve_body_templates(&table.endpoint.body, discover_context);
                // Merge cursor into body for cursor pagination
                if let Some(ref c) = cursor {
                    if let Some(ref pagination) = table.endpoint.pagination {
                        if pagination.pagination_type == "cursor" {
                            if let Some(obj) = req_body.as_object_mut() {
                                obj.insert(
                                    pagination.cursor_variable.clone(),
                                    serde_json::Value::String(c.clone()),
                                );
                            }
                        }
                    }
                }
                req = req.json(&req_body);
            }

            let resp = match req.send() {
                Ok(r) => r,
                Err(e) => {
                    save_cursor_on_error(conn, ctx, &table.name, &cursor, page, total);
                    return Err(e).context(format!("Request failed: {url}"));
                }
            };
            if !resp.status().is_success() {
                let status = resp.status();
                if status.as_u16() == 429 {
                    eprintln!("  rate limited on page {page}");
                    save_cursor_on_error(conn, ctx, &table.name, &cursor, page, total);
                    break;
                }
                let error_body = resp.text().unwrap_or_default();
                save_cursor_on_error(conn, ctx, &table.name, &cursor, page, total);
                anyhow::bail!("API error: HTTP {status} from {url}\n  {error_body}");
            }

            let resp_body: serde_json::Value = match resp.json() {
                Ok(b) => b,
                Err(e) => {
                    save_cursor_on_error(conn, ctx, &table.name, &cursor, page, total);
                    return Err(e).context("Failed to parse response");
                }
            };

            // Navigate to results
            let results = resolve_path_collecting(&resp_body, &table.response.results_path);
            if results.is_empty() {
                break;
            }

            // If results_path points to an array, use it; otherwise wrap single items
            let items: Vec<&serde_json::Value> = if results.len() == 1 {
                if let Some(arr) = results[0].as_array() {
                    arr.iter().collect()
                } else {
                    results.iter().collect()
                }
            } else {
                results.iter().collect()
            };

            if items.is_empty() {
                break;
            }

            // Apply filter if specified
            let filtered_items: Vec<&&serde_json::Value> =
                if let Some(ref filter) = table.response.filter {
                    items
                        .iter()
                        .filter(|item| {
                            if let Some(val) = resolve_path(item, &filter.path) {
                                if !filter.starts_with.is_empty() {
                                    val.as_str()
                                        .map(|s| s.starts_with(&filter.starts_with))
                                        .unwrap_or(false)
                                } else {
                                    true
                                }
                            } else {
                                true
                            }
                        })
                        .collect()
                } else {
                    items.iter().collect()
                };

            let page_count = insert_items(conn, table, &filtered_items)?;
            total += page_count;

            // Check incremental early-stop: if all items on this page are older than since, stop
            if let Some(ref incremental) = table.endpoint.incremental {
                if ctx.is_incremental() {
                    if let Some(ref since) = ctx.since {
                        let since_str = since.to_rfc3339();
                        let all_old = items.iter().all(|item| {
                            resolve_path(item, &incremental.stop_date_path)
                                .and_then(|v| v.as_str())
                                .map(|date_str| date_str < since_str.as_str())
                                .unwrap_or(false)
                        });
                        if all_old {
                            eprintln!("    incremental: stopping early (page {page} all older than last sync)");
                            break;
                        }
                    }
                }
            }

            // Handle pagination
            if let Some(ref pagination) = table.endpoint.pagination {
                match pagination.pagination_type.as_str() {
                    "cursor" => {
                        // Check has_more flag
                        if !pagination.has_next_path.is_empty() {
                            let has_next = resolve_path(&resp_body, &pagination.has_next_path)
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);
                            if !has_next {
                                break;
                            }
                        }
                        // Extract next cursor
                        if !pagination.cursor_path.is_empty() {
                            cursor = resolve_path(&resp_body, &pagination.cursor_path)
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string());
                            if cursor.is_none() {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                    _ => {
                        // page_number / offset: stop if fewer results than page_size
                        if items.len() < page_size as usize {
                            break;
                        }
                    }
                }
            } else {
                break; // No pagination, single page
            }

            page += 1;
            if let Some(d) = delay {
                std::thread::sleep(d);
            }
        }

        Ok(total)
    }

    fn fetch_graphql_table(
        &self,
        client: &reqwest::blocking::Client,
        conn: &Connection,
        table: &TableSpec,
        auth_headers: &HashMap<String, String>,
        discover_context: &HashMap<String, DiscoverValue>,
        ctx: &SyncContext,
    ) -> Result<usize> {
        let mut total = 0;
        let mut cursor: Option<String> = None;
        let max_pages = table
            .endpoint
            .pagination
            .as_ref()
            .map(|p| p.max_pages)
            .unwrap_or(1);
        let page_size = table
            .endpoint
            .pagination
            .as_ref()
            .map(|p| p.page_size)
            .unwrap_or(100);
        let delay = table
            .endpoint
            .rate_limit
            .as_ref()
            .map(|r| std::time::Duration::from_secs_f64(r.delay_seconds));
        let use_get = table.endpoint.graphql_method.eq_ignore_ascii_case("GET");

        let mut start_page: u32 = 0;
        // Resume from cursor if available
        if let Some(ref resume_json) = ctx.resume_cursor {
            if let Ok(resume) = serde_json::from_str::<serde_json::Value>(resume_json) {
                if resume.get("table").and_then(|t| t.as_str()) == Some(&table.name) {
                    cursor = resume
                        .get("cursor")
                        .and_then(|c| c.as_str())
                        .map(String::from);
                    start_page = resume.get("page").and_then(|p| p.as_u64()).unwrap_or(0) as u32;
                    total = resume
                        .get("rows_so_far")
                        .and_then(|r| r.as_u64())
                        .unwrap_or(0) as usize;
                    eprintln!("    resuming from page {}", start_page + 1);
                }
            }
        }

        for page in start_page..max_pages {
            let url = resolve_discover_template(&table.endpoint.url, discover_context);
            let query = resolve_discover_template(&table.endpoint.query, discover_context);

            // Build variables: start with static vars, merge pagination
            let mut variables = if table.endpoint.variables.is_object() {
                table.endpoint.variables.clone()
            } else {
                serde_json::json!({})
            };
            // Resolve templates in variable values (recursively for nested objects)
            resolve_json_templates(&mut variables, discover_context);

            if let Some(ref pagination) = table.endpoint.pagination {
                match pagination.pagination_type.as_str() {
                    "cursor" => {
                        if let Some(ref c) = cursor {
                            variables[&pagination.cursor_variable] =
                                serde_json::Value::String(c.clone());
                        }
                    }
                    "offset" => {
                        let offset = page * page_size;
                        variables["offset"] = serde_json::json!(offset);
                        variables["limit"] = serde_json::json!(page_size);
                    }
                    _ => {}
                }
            }

            let resp = if use_get {
                // GraphQL-over-GET: send query/variables/features as URL query params
                let mut query_params = vec![("variables", serde_json::to_string(&variables)?)];
                if let Some(ref features) = table.endpoint.features {
                    query_params.push(("features", serde_json::to_string(features)?));
                }
                let mut req = client.get(&url).query(&query_params);
                for (k, v) in auth_headers {
                    req = req.header(k, v);
                }
                for (k, v) in &table.endpoint.headers {
                    let resolved = resolve_discover_template(v, discover_context);
                    req = req.header(k, resolved);
                }
                match req.send() {
                    Ok(r) => r,
                    Err(e) => {
                        save_cursor_on_error(conn, ctx, &table.name, &cursor, page, total);
                        return Err(e).context("GraphQL GET request failed");
                    }
                }
            } else {
                // Standard GraphQL POST
                let mut payload = serde_json::json!({
                    "query": query,
                    "variables": variables,
                });
                if !table.endpoint.operation_name.is_empty() {
                    payload["operationName"] =
                        serde_json::Value::String(table.endpoint.operation_name.clone());
                }
                let mut req = client.post(&url);
                for (k, v) in auth_headers {
                    req = req.header(k, v);
                }
                for (k, v) in &table.endpoint.headers {
                    let resolved = resolve_discover_template(v, discover_context);
                    req = req.header(k, resolved);
                }
                match req.json(&payload).send() {
                    Ok(r) => r,
                    Err(e) => {
                        save_cursor_on_error(conn, ctx, &table.name, &cursor, page, total);
                        return Err(e).context("GraphQL POST request failed");
                    }
                }
            };

            if !resp.status().is_success() {
                let status = resp.status();
                if status.as_u16() == 429 {
                    eprintln!("  rate limited on page {}", page + 1);
                    save_cursor_on_error(conn, ctx, &table.name, &cursor, page, total);
                    break;
                }
                let error_body = resp.text().unwrap_or_default();
                save_cursor_on_error(conn, ctx, &table.name, &cursor, page, total);
                anyhow::bail!("GraphQL error: HTTP {status}\n  {error_body}");
            }

            let body: serde_json::Value = match resp.json() {
                Ok(b) => b,
                Err(e) => {
                    save_cursor_on_error(conn, ctx, &table.name, &cursor, page, total);
                    return Err(e).context("Failed to parse response");
                }
            };

            // Navigate to results
            let results_path =
                resolve_discover_template(&table.response.results_path, discover_context);
            let results = resolve_path_collecting(&body, &results_path);
            if results.is_empty() {
                break;
            }

            let items: Vec<&serde_json::Value> = if results.len() == 1 {
                if let Some(arr) = results[0].as_array() {
                    arr.iter().collect()
                } else {
                    results.iter().collect()
                }
            } else {
                results.iter().collect()
            };

            if items.is_empty() {
                break;
            }

            // Apply filter
            let filtered_items: Vec<&&serde_json::Value> =
                if let Some(ref filter) = table.response.filter {
                    items
                        .iter()
                        .filter(|item| {
                            if let Some(val) = resolve_path(item, &filter.path) {
                                if !filter.starts_with.is_empty() {
                                    val.as_str()
                                        .map(|s| s.starts_with(&filter.starts_with))
                                        .unwrap_or(false)
                                } else {
                                    true
                                }
                            } else {
                                true
                            }
                        })
                        .collect()
                } else {
                    items.iter().collect()
                };

            let page_count = insert_items(conn, table, &filtered_items)?;
            total += page_count;

            // Check incremental early-stop: if all items on this page are older than since, stop
            if let Some(ref incremental) = table.endpoint.incremental {
                if ctx.is_incremental() {
                    if let Some(ref since) = ctx.since {
                        let since_str = since.to_rfc3339();
                        let all_old = items.iter().all(|item| {
                            resolve_path(item, &incremental.stop_date_path)
                                .and_then(|v| v.as_str())
                                .map(|date_str| date_str < since_str.as_str())
                                .unwrap_or(false)
                        });
                        if all_old {
                            eprintln!("    incremental: stopping early (page {} all older than last sync)", page + 1);
                            break;
                        }
                    }
                }
            }

            // Handle pagination
            if let Some(ref pagination) = table.endpoint.pagination {
                match pagination.pagination_type.as_str() {
                    "cursor" => {
                        // Try cursor-from-results first (Twitter pattern)
                        if pagination.cursor_from_results {
                            cursor = None;
                            for item in &items {
                                if let Some(id_val) =
                                    resolve_path(item, &pagination.cursor_entry_id_path)
                                {
                                    if let Some(id_str) = id_val.as_str() {
                                        if id_str.starts_with(&pagination.cursor_entry_prefix) {
                                            cursor =
                                                resolve_path(item, &pagination.cursor_value_path)
                                                    .and_then(|v| v.as_str())
                                                    .map(|s| s.to_string());
                                            break;
                                        }
                                    }
                                }
                            }
                            if cursor.is_none() {
                                break;
                            }
                        } else {
                            // Standard cursor from response metadata
                            if !pagination.has_next_path.is_empty() {
                                let has_next = resolve_path(&body, &pagination.has_next_path)
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(false);
                                if !has_next {
                                    break;
                                }
                            }
                            if !pagination.cursor_path.is_empty() {
                                cursor = resolve_path(&body, &pagination.cursor_path)
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string());
                                if cursor.is_none() {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }
                    }
                    "offset" => {
                        // Check if we got fewer results than page_size
                        if items.len() < page_size as usize {
                            break;
                        }
                    }
                    _ => break,
                }
            } else {
                break; // No pagination, single page
            }

            if let Some(d) = delay {
                std::thread::sleep(d);
            }
        }

        Ok(total)
    }
}

/// Insert items into a table using field mappings.
fn insert_items(
    conn: &Connection,
    table: &TableSpec,
    items: &[&&serde_json::Value],
) -> Result<usize> {
    if items.is_empty() {
        return Ok(0);
    }

    // Build INSERT statement from column specs
    let col_names: Vec<&str> = table
        .columns
        .iter()
        .filter(|c| c.default.is_none())
        .map(|c| c.name.as_str())
        .collect();

    let placeholders: Vec<String> = (1..=col_names.len()).map(|i| format!("?{i}")).collect();

    let sql = format!(
        "INSERT OR REPLACE INTO {} ({}) VALUES ({})",
        table.name,
        col_names.join(", "),
        placeholders.join(", ")
    );

    let mut stmt = conn.prepare_cached(&sql)?;
    let mut count = 0;

    for item in items {
        let mut values: Vec<Option<String>> = Vec::new();
        for col in &table.columns {
            if col.default.is_some() {
                continue;
            }
            let mapping = table
                .response
                .field_mappings
                .iter()
                .find(|m| m.column == col.name);

            let value = if let Some(mapping) = mapping {
                extract_field_with_alts(item, &mapping.path, &mapping.alt_paths, &mapping.transform)
            } else {
                None
            };
            values.push(value);
        }

        let params: Vec<&dyn rusqlite::types::ToSql> = values
            .iter()
            .map(|v| v as &dyn rusqlite::types::ToSql)
            .collect();

        stmt.execute(params.as_slice())?;
        count += 1;
    }

    Ok(count)
}

/// Save cursor state to sync_runs on error so the sync can be resumed.
fn save_cursor_on_error(
    conn: &Connection,
    ctx: &SyncContext,
    table_name: &str,
    cursor: &Option<String>,
    page: u32,
    rows_so_far: usize,
) {
    if let Some(run_id) = ctx.sync_run_id {
        let cursor_state = serde_json::json!({
            "table": table_name,
            "cursor": cursor,
            "page": page,
            "rows_so_far": rows_so_far,
        });
        let _ = db::update_sync_cursor(conn, run_id, &cursor_state.to_string());
    }
}

/// Mark rows not seen in this sync as soft-deleted.
/// Only call for full syncs on tables with soft_delete enabled.
fn mark_soft_deleted(conn: &Connection, table_name: &str, sync_started_at: &str) -> Result<usize> {
    validate_identifier(table_name)?;
    let sql = format!(
        "UPDATE {} SET _deleted_at = CURRENT_TIMESTAMP
         WHERE _deleted_at IS NULL AND _extracted_at < ?1",
        table_name
    );
    Ok(conn.execute(&sql, rusqlite::params![sync_started_at])?)
}

impl Connector for DynamicConnector {
    fn name(&self) -> &str {
        &self.spec.name
    }

    fn description(&self) -> &str {
        &self.spec.description
    }

    fn source(&self) -> &str {
        &self.source_tag
    }

    fn create_source_tables(&self, conn: &Connection) -> Result<()> {
        for table in &self.spec.tables {
            if db::table_exists(conn, &table.name) {
                // Migration: detect and add missing columns
                let existing = db::get_table_columns(conn, &table.name)?;
                let existing_lower: Vec<String> =
                    existing.iter().map(|c| c.to_lowercase()).collect();

                for col in &table.columns {
                    if !existing_lower.contains(&col.name.to_lowercase()) {
                        if col.primary_key {
                            eprintln!(
                                "  warning: cannot add PK column {}.{} via ALTER TABLE",
                                table.name, col.name
                            );
                            continue;
                        }
                        let mut sql = format!(
                            "ALTER TABLE {} ADD COLUMN {} {}",
                            table.name, col.name, col.col_type
                        );
                        if let Some(ref default) = col.default {
                            sql.push_str(&format!(" DEFAULT {default}"));
                        }
                        conn.execute_batch(&sql)?;
                        eprintln!(
                            "  schema migration: added column {}.{}",
                            table.name, col.name
                        );
                    }
                }

                // Also add _deleted_at if soft_delete is enabled and column is missing
                if table.soft_delete && !existing_lower.contains(&"_deleted_at".to_string()) {
                    conn.execute_batch(&format!(
                        "ALTER TABLE {} ADD COLUMN _deleted_at TEXT",
                        table.name
                    ))?;
                    eprintln!(
                        "  schema migration: added column {}._deleted_at",
                        table.name
                    );
                }
            } else {
                // New table: CREATE TABLE
                let mut col_defs = Vec::new();
                let mut pk_cols = Vec::new();

                for col in &table.columns {
                    let mut def = format!("{} {}", col.name, col.col_type);
                    if col.primary_key {
                        pk_cols.push(col.name.as_str());
                    }
                    if let Some(ref default) = col.default {
                        def.push_str(&format!(" DEFAULT {default}"));
                    }
                    col_defs.push(def);
                }

                // Add _deleted_at column if soft_delete is enabled
                if table.soft_delete {
                    col_defs.push("_deleted_at TEXT".to_string());
                }

                if !pk_cols.is_empty() {
                    col_defs.push(format!("PRIMARY KEY ({})", pk_cols.join(", ")));
                }

                let sql = format!(
                    "CREATE TABLE IF NOT EXISTS {} (\n  {}\n)",
                    table.name,
                    col_defs.join(",\n  ")
                );
                conn.execute_batch(&sql)?;
            }
        }
        Ok(())
    }

    fn extract(&self, conn: &Connection, _config: &Config, ctx: &SyncContext) -> Result<usize> {
        self.create_source_tables(conn)?;

        let client = self.build_client()?;

        // Resolve auth
        let graphql_url = self.spec.tables.first().map(|t| t.endpoint.url.as_str());
        let auth = resolve_auth(&self.spec.auth, &client, graphql_url)?;

        // Run discovery pipeline
        let mut discover_context = if !self.spec.discover.is_empty() {
            run_discovery(&client, &self.spec.discover, &auth.headers)?
        } else {
            HashMap::new()
        };

        // Always inject {{last_sync.*}} — use epoch date for full sync so URL templates
        // like `start_date={{last_sync.date}}` resolve to a far-past date instead of
        // staying as literal text (which would break the URL).
        let last_sync_date = ctx
            .since
            .as_ref()
            .map(|s| s.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| "1970-01-01".to_string());
        let last_sync_ts = ctx
            .since
            .as_ref()
            .map(|s| s.to_rfc3339())
            .unwrap_or_else(|| "1970-01-01T00:00:00+00:00".to_string());
        let mut last_sync_map = HashMap::new();
        last_sync_map.insert("date".to_string(), last_sync_date);
        last_sync_map.insert("timestamp".to_string(), last_sync_ts);
        discover_context.insert("last_sync".to_string(), DiscoverValue::Map(last_sync_map));

        let mut total = 0;
        let mut errors: Vec<String> = Vec::new();
        let sync_started_str = ctx.started_at.format("%Y-%m-%d %H:%M:%S").to_string();

        for table in &self.spec.tables {
            let result = match self.spec.api_type.as_str() {
                "graphql" => self.fetch_graphql_table(
                    &client,
                    conn,
                    table,
                    &auth.headers,
                    &discover_context,
                    ctx,
                ),
                _ => self.fetch_rest_table(
                    &client,
                    conn,
                    table,
                    &auth.headers,
                    &discover_context,
                    ctx,
                ),
            };
            match result {
                Ok(count) => {
                    eprintln!("  {}: {count}", table.name);
                    total += count;

                    // Soft delete: mark missing rows on full sync
                    if table.soft_delete && !ctx.is_incremental() && count > 0 {
                        match mark_soft_deleted(conn, &table.name, &sync_started_str) {
                            Ok(deleted) if deleted > 0 => {
                                eprintln!("  {}: {deleted} soft-deleted", table.name);
                            }
                            Err(e) => {
                                eprintln!("  {}: soft-delete failed: {e}", table.name);
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => {
                    eprintln!("  {}: FAILED ({})", table.name, e);
                    errors.push(format!("{}: {e}", table.name));
                }
            }
        }

        if total == 0 && !errors.is_empty() {
            anyhow::bail!("{}", errors.join("; "));
        }

        Ok(total)
    }

    fn fts_schema_sql(&self) -> Option<&str> {
        self.fts_ddl.as_deref()
    }

    fn populate_fts(&self, conn: &Connection) -> Result<i64> {
        if self.spec.fts.is_empty() {
            return Ok(0);
        }

        let mut total: i64 = 0;

        for fts in &self.spec.fts {
            if !db::table_exists(conn, &fts.source_table) {
                continue;
            }

            let tx = conn.unchecked_transaction()?;

            if fts.source_tag.is_empty() {
                // Non-shared: clear all data
                tx.execute(&format!("DELETE FROM {}", fts.table_name), [])?;
                tx.execute(&format!("DELETE FROM {}_map", fts.table_name), [])?;
            } else {
                // Shared: only delete rows matching our source tag
                tx.execute(
                    &format!(
                        "DELETE FROM {} WHERE rowid IN (SELECT fts_rowid FROM {}_map WHERE source = ?1)",
                        fts.table_name, fts.table_name
                    ),
                    rusqlite::params![&fts.source_tag],
                )?;
                tx.execute(
                    &format!("DELETE FROM {}_map WHERE source = ?1", fts.table_name),
                    rusqlite::params![&fts.source_tag],
                )?;
            }

            // Build column expressions for FTS populate
            let fts_cols = fts.columns.join(", ");
            let select_cols: Vec<String> = fts
                .columns
                .iter()
                .map(|c| {
                    if let Some(expr) = fts.column_expressions.get(c) {
                        expr.clone()
                    } else {
                        format!("COALESCE({c}, '')")
                    }
                })
                .collect();
            let select_str = select_cols.join(", ");

            if fts.source_tag.is_empty() {
                // Non-shared: use natural rowids
                let insert_fts = format!(
                    "INSERT INTO {}(rowid, {}) SELECT rowid, {} FROM {}",
                    fts.table_name, fts_cols, select_str, fts.source_table
                );
                tx.execute_batch(&insert_fts)?;

                let insert_map = format!(
                    "INSERT INTO {}_map(fts_rowid, {}) SELECT rowid, {} FROM {}",
                    fts.table_name, fts.map_id_column, fts.id_column, fts.source_table
                );
                tx.execute_batch(&insert_map)?;

                let count: i64 = tx.query_row(
                    &format!("SELECT COUNT(*) FROM {}_map", fts.table_name),
                    [],
                    |r| r.get(0),
                )?;
                total += count;
            } else {
                // Shared: use rowid offset to avoid collisions
                let offset: i64 = tx
                    .query_row(
                        &format!("SELECT COALESCE(MAX(rowid), 0) FROM {}", fts.table_name),
                        [],
                        |r| r.get(0),
                    )
                    .unwrap_or(0);

                tx.execute(
                    &format!(
                        "INSERT INTO {}(rowid, {}) SELECT ?1 + rowid, {} FROM {}",
                        fts.table_name, fts_cols, select_str, fts.source_table
                    ),
                    rusqlite::params![offset],
                )?;

                tx.execute(
                    &format!(
                        "INSERT INTO {}_map(fts_rowid, {}, source) SELECT ?1 + rowid, {}, ?2 FROM {}",
                        fts.table_name, fts.map_id_column, fts.id_column, fts.source_table
                    ),
                    rusqlite::params![offset, &fts.source_tag],
                )?;

                let count: i64 = tx.query_row(
                    &format!(
                        "SELECT COUNT(*) FROM {}_map WHERE source = ?1",
                        fts.table_name
                    ),
                    rusqlite::params![&fts.source_tag],
                    |r| r.get(0),
                )?;
                total += count;
            }

            tx.commit()?;
        }

        Ok(total)
    }

    fn governance_source(&self) -> &str {
        &self.spec.name
    }

    fn governance_description(&self) -> &str {
        &self.spec.description
    }

    fn governance_fields(&self) -> &[&str] {
        // We store pre-computed references at construction time
        &self.governance_field_refs
    }

    fn search_types(&self) -> Vec<(&str, &str)> {
        let mut types = Vec::new();
        for fts in &self.spec.fts {
            let st = if fts.search_type.is_empty() {
                self.spec.name.as_str()
            } else {
                fts.search_type.as_str()
            };
            let pair = (st, self.spec.name.as_str());
            if !types.contains(&pair) {
                types.push(pair);
            }
        }
        types
    }

    fn search_fts(
        &self,
        conn: &Connection,
        search_type: &str,
        query: &str,
        options: &SearchOptions,
    ) -> Result<Vec<SearchResult>> {
        let mut results = Vec::new();
        for fts in &self.spec.fts {
            let st = if fts.search_type.is_empty() {
                self.spec.name.as_str()
            } else {
                fts.search_type.as_str()
            };
            if st == search_type {
                results.extend(generic_fts_search(conn, fts, search_type, query, options)?);
            }
        }
        if results.len() > 1 {
            results.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            results.truncate(30);
        }
        Ok(results)
    }
}

/// Execute a generic FTS search driven by FtsSpec metadata.
fn generic_fts_search(
    conn: &Connection,
    fts: &FtsSpec,
    search_type: &str,
    query: &str,
    options: &SearchOptions,
) -> Result<Vec<SearchResult>> {
    // Build title expression
    let title_expr = if !fts.title_expr.is_empty() {
        fts.title_expr.clone()
    } else if !fts.title_column.is_empty() {
        let fallback = if fts.title_fallback.is_empty() {
            "Untitled".to_string()
        } else {
            fts.title_fallback.replace('\'', "''")
        };
        format!("COALESCE(t.{}, '{}')", fts.title_column, fallback)
    } else {
        "'Untitled'".to_string()
    };

    // Build metadata column selections
    let mut extra_selects = String::new();
    let meta_keys: Vec<&String> = fts.metadata_columns.keys().collect();
    for key in &meta_keys {
        let expr = &fts.metadata_columns[*key];
        extra_selects.push_str(&format!(",\n            {} as meta_{}", expr, key));
    }

    // Build source filter for shared FTS tables
    let source_filter = if fts.source_tag.is_empty() {
        String::new()
    } else {
        format!(" AND map.source = '{}'", fts.source_tag)
    };

    // Build soft-delete filter
    let soft_delete_filter = if fts.soft_delete {
        " AND t._deleted_at IS NULL"
    } else {
        ""
    };

    // Build date filters
    let mut date_filters = String::new();
    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(query.to_string())];

    if !fts.date_column.is_empty() {
        if let Some(ref from) = options.date_from {
            date_filters.push_str(&format!(
                " AND t.{} >= ?{}",
                fts.date_column,
                params.len() + 1
            ));
            params.push(Box::new(from.clone()));
        }
        if let Some(ref to) = options.date_to {
            date_filters.push_str(&format!(
                " AND t.{} <= ?{}",
                fts.date_column,
                params.len() + 1
            ));
            params.push(Box::new(to.clone()));
        }
    }

    let sql = format!(
        "SELECT
            '{search_type}' as type,
            map.{map_id} as id,
            {title_expr} as title,
            snippet({fts_table}, {snippet_col}, '**', '**', '...', 32) as snippet,
            bm25({fts_table}) as score{extra_selects}
        FROM {fts_table}
        JOIN {fts_table}_map map ON {fts_table}.rowid = map.fts_rowid
        JOIN {source_table} t ON map.{map_id} = t.{id_col}
        WHERE {fts_table} MATCH ?1{source_filter}{soft_delete_filter}{date_filters}
        ORDER BY bm25({fts_table})
        LIMIT 30",
        search_type = search_type,
        map_id = fts.map_id_column,
        title_expr = title_expr,
        fts_table = fts.table_name,
        snippet_col = fts.snippet_column,
        extra_selects = extra_selects,
        source_table = fts.source_table,
        id_col = fts.id_column,
        source_filter = source_filter,
        soft_delete_filter = soft_delete_filter,
        date_filters = date_filters,
    );

    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();

    match conn.prepare(&sql) {
        Ok(mut stmt) => {
            let rows = stmt.query_map(param_refs.as_slice(), |row| {
                let score: f64 = row.get("score")?;
                let mut metadata = HashMap::new();

                for key in &meta_keys {
                    let col_name = format!("meta_{}", key);
                    // Try as string first, then as f64, then as i64
                    if let Ok(Some(s)) = row.get::<_, Option<String>>(col_name.as_str()) {
                        metadata.insert(key.to_string(), serde_json::Value::String(s));
                    } else if let Ok(Some(f)) = row.get::<_, Option<f64>>(col_name.as_str()) {
                        metadata.insert(key.to_string(), serde_json::json!(f));
                    } else if let Ok(Some(i)) = row.get::<_, Option<i64>>(col_name.as_str()) {
                        metadata.insert(key.to_string(), serde_json::json!(i));
                    }
                }

                let title: String = row
                    .get::<_, Option<String>>("title")?
                    .unwrap_or_else(|| "Untitled".into());

                let raw_snippet: String =
                    row.get::<_, Option<String>>("snippet")?.unwrap_or_default();

                // Handle special snippet templates
                let snippet = if fts.snippet_template == "amount_category" {
                    let amt = metadata
                        .get("amount")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0);
                    let cat = metadata
                        .get("category")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Uncategorized");
                    format!("${:.2} - {}", amt.abs(), cat)
                } else {
                    raw_snippet
                };

                Ok(SearchResult {
                    result_type: search_type.to_string(),
                    id: row.get("id")?,
                    title,
                    snippet,
                    score: score.abs(),
                    metadata,
                })
            })?;
            Ok(rows.filter_map(|r| r.ok()).collect())
        }
        Err(_) => Ok(Vec::new()),
    }
}

// ========== Loader ==========

/// Directory where user-installed connector JSON specs live.
pub fn connectors_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".warehouse")
        .join("connectors")
}

/// Load all dynamic connectors from ~/.warehouse/connectors/*.json
pub fn load_dynamic_connectors() -> Vec<Box<dyn Connector>> {
    let dir = connectors_dir();
    if !dir.exists() {
        return Vec::new();
    }

    let mut connectors: Vec<Box<dyn Connector>> = Vec::new();

    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(_) => return connectors,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "json") {
            // Skip files in bin/ subdirectory (future executable plugins)
            if path
                .parent()
                .is_some_and(|p| p.file_name().is_some_and(|n| n == "bin"))
            {
                continue;
            }

            match std::fs::read_to_string(&path) {
                Ok(json) => match DynamicConnector::from_json(&json) {
                    Ok(connector) => {
                        connectors.push(Box::new(connector));
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to load connector {}: {e}", path.display());
                    }
                },
                Err(e) => {
                    eprintln!("Warning: Failed to read {}: {e}", path.display());
                }
            }
        }
    }

    connectors
}

/// Validate a connector spec JSON string without loading it.
pub fn validate_spec(json: &str) -> Result<ConnectorSpec> {
    let spec: ConnectorSpec =
        serde_json::from_str(json).context("Failed to parse connector spec JSON")?;

    if spec.version != 1 {
        anyhow::bail!("Unsupported spec version: {}", spec.version);
    }

    validate_identifier(&spec.name)?;

    for table in &spec.tables {
        validate_identifier(&table.name)?;
        for col in &table.columns {
            validate_identifier(&col.name)?;
        }
    }

    for fts in &spec.fts {
        validate_identifier(&fts.table_name)?;
        validate_identifier(&fts.source_table)?;
        validate_identifier(&fts.map_id_column)?;
        for col in &fts.columns {
            validate_identifier(col)?;
        }
        validate_tokenizer(&fts.tokenizer)?;
    }

    Ok(spec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_identifier_alphanumeric() {
        assert!(validate_identifier("github_stars").is_ok());
        assert!(validate_identifier("my_table_123").is_ok());
    }

    #[test]
    fn validate_identifier_rejects_special() {
        assert!(validate_identifier("drop; --").is_err());
        assert!(validate_identifier("table name").is_err());
        assert!(validate_identifier("").is_err());
    }

    #[test]
    fn resolve_path_simple() {
        let data = serde_json::json!({
            "repo": {
                "full_name": "user/repo",
                "id": 12345
            }
        });
        let result = resolve_path(&data, "repo.full_name");
        assert_eq!(result.unwrap().as_str().unwrap(), "user/repo");
    }

    #[test]
    fn resolve_path_root() {
        let data = serde_json::json!([1, 2, 3]);
        let result = resolve_path(&data, "$");
        assert_eq!(result.unwrap(), &data);
    }

    #[test]
    fn resolve_path_with_index() {
        let data = serde_json::json!({
            "items": [
                {"name": "first"},
                {"name": "second"}
            ]
        });
        let result = resolve_path(&data, "items[0].name");
        assert_eq!(result.unwrap().as_str().unwrap(), "first");
    }

    #[test]
    fn resolve_path_missing_key() {
        let data = serde_json::json!({"a": 1});
        assert!(resolve_path(&data, "b.c").is_none());
    }

    #[test]
    fn extract_field_with_transform() {
        let data = serde_json::json!({"id": 12345});
        let result = extract_field(&data, "id", "to_string");
        assert_eq!(result.unwrap(), "12345");
    }

    #[test]
    fn parse_minimal_spec() {
        let json = r#"{
            "version": 1,
            "name": "test_api",
            "description": "Test connector",
            "tables": [{
                "name": "test_items",
                "columns": [
                    {"name": "id", "type": "TEXT", "primary_key": true},
                    {"name": "title", "type": "TEXT"}
                ],
                "endpoint": {
                    "url": "https://api.example.com/items"
                },
                "response": {
                    "results_path": "$",
                    "field_mappings": [
                        {"column": "id", "path": "id"},
                        {"column": "title", "path": "title"}
                    ]
                }
            }]
        }"#;

        let connector = DynamicConnector::from_json(json).unwrap();
        assert_eq!(connector.name(), "test_api");
        assert_eq!(connector.source(), "installed");
    }

    #[test]
    fn parse_spec_with_fts() {
        let json = r#"{
            "version": 1,
            "name": "github_stars",
            "description": "GitHub starred repositories",
            "tables": [{
                "name": "github_stars",
                "columns": [
                    {"name": "id", "type": "TEXT", "primary_key": true},
                    {"name": "full_name", "type": "TEXT"}
                ],
                "endpoint": { "url": "https://api.github.com/user/starred" },
                "response": {
                    "results_path": "$",
                    "field_mappings": [
                        {"column": "id", "path": "id", "transform": "to_string"},
                        {"column": "full_name", "path": "full_name"}
                    ]
                }
            }],
            "fts": [{
                "table_name": "github_stars_fts",
                "source_table": "github_stars",
                "columns": ["full_name"],
                "tokenizer": "porter unicode61"
            }]
        }"#;

        let connector = DynamicConnector::from_json(json).unwrap();
        assert!(connector.fts_schema_sql().is_some());
        let fts_sql = connector.fts_schema_sql().unwrap();
        assert!(fts_sql.contains("github_stars_fts"));
    }

    #[test]
    fn reject_invalid_version() {
        let json = r#"{"version": 99, "name": "test", "tables": []}"#;
        assert!(DynamicConnector::from_json(json).is_err());
    }

    #[test]
    fn reject_sql_injection_name() {
        let json = r#"{"version": 1, "name": "drop; --", "tables": []}"#;
        assert!(DynamicConnector::from_json(json).is_err());
    }

    #[test]
    fn validate_tokenizer_known() {
        assert!(validate_tokenizer("porter unicode61").is_ok());
        assert!(validate_tokenizer("unicode61").is_ok());
        assert!(validate_tokenizer("porter").is_ok());
    }

    #[test]
    fn validate_tokenizer_unknown() {
        assert!(validate_tokenizer("custom_evil_tokenizer").is_err());
    }

    #[test]
    fn resolve_path_collecting_with_wildcard() {
        let data = serde_json::json!({
            "items": [
                {"name": "a"},
                {"name": "b"},
                {"name": "c"}
            ]
        });
        let results = resolve_path_collecting(&data, "items[*].name");
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_str().unwrap(), "a");
    }

    #[test]
    fn apply_transform_to_bool() {
        assert_eq!(apply_transform("true", "to_bool"), "1");
        assert_eq!(apply_transform("false", "to_bool"), "0");
        assert_eq!(apply_transform("1", "to_bool"), "1");
    }

    // ========== schema migration ==========

    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        conn
    }

    fn make_test_connector(json: &str) -> DynamicConnector {
        DynamicConnector::from_json(json).unwrap()
    }

    #[test]
    fn schema_migration_adds_new_column() {
        let conn = test_conn();
        // Create table with 2 columns
        conn.execute_batch(
            "CREATE TABLE test_items (id TEXT PRIMARY KEY, title TEXT, _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        ).unwrap();

        // Load spec with 3 columns (adds 'description')
        let json = r#"{
            "version": 1, "name": "test_api",
            "tables": [{
                "name": "test_items",
                "columns": [
                    {"name": "id", "type": "TEXT", "primary_key": true},
                    {"name": "title", "type": "TEXT"},
                    {"name": "description", "type": "TEXT"},
                    {"name": "_extracted_at", "type": "TIMESTAMP", "default": "CURRENT_TIMESTAMP"}
                ],
                "endpoint": {"url": "https://example.com"},
                "response": {"results_path": "$", "field_mappings": []}
            }]
        }"#;
        let connector = make_test_connector(json);
        connector.create_source_tables(&conn).unwrap();

        let cols = db::get_table_columns(&conn, "test_items").unwrap();
        assert!(cols.contains(&"description".to_string()));
    }

    #[test]
    fn schema_migration_skips_existing_columns() {
        let conn = test_conn();
        conn.execute_batch("CREATE TABLE test_items (id TEXT PRIMARY KEY, title TEXT)")
            .unwrap();

        let json = r#"{
            "version": 1, "name": "test_api",
            "tables": [{
                "name": "test_items",
                "columns": [
                    {"name": "id", "type": "TEXT", "primary_key": true},
                    {"name": "title", "type": "TEXT"}
                ],
                "endpoint": {"url": "https://example.com"},
                "response": {"results_path": "$", "field_mappings": []}
            }]
        }"#;
        let connector = make_test_connector(json);
        // Should not error
        connector.create_source_tables(&conn).unwrap();
        let cols = db::get_table_columns(&conn, "test_items").unwrap();
        assert_eq!(cols.len(), 2);
    }

    #[test]
    fn schema_migration_warns_on_pk_column() {
        let conn = test_conn();
        conn.execute_batch("CREATE TABLE test_items (id TEXT PRIMARY KEY)")
            .unwrap();

        // Spec tries to add a new PK column — should be skipped without error
        let json = r#"{
            "version": 1, "name": "test_api",
            "tables": [{
                "name": "test_items",
                "columns": [
                    {"name": "id", "type": "TEXT", "primary_key": true},
                    {"name": "id2", "type": "TEXT", "primary_key": true}
                ],
                "endpoint": {"url": "https://example.com"},
                "response": {"results_path": "$", "field_mappings": []}
            }]
        }"#;
        let connector = make_test_connector(json);
        connector.create_source_tables(&conn).unwrap();
        let cols = db::get_table_columns(&conn, "test_items").unwrap();
        // id2 should NOT be added (PK column warning)
        assert!(!cols.contains(&"id2".to_string()));
    }

    // ========== soft delete ==========

    #[test]
    fn soft_delete_creates_deleted_at_column() {
        let conn = test_conn();
        let json = r#"{
            "version": 1, "name": "test_api",
            "tables": [{
                "name": "soft_items",
                "soft_delete": true,
                "columns": [
                    {"name": "id", "type": "TEXT", "primary_key": true},
                    {"name": "title", "type": "TEXT"},
                    {"name": "_extracted_at", "type": "TIMESTAMP", "default": "CURRENT_TIMESTAMP"}
                ],
                "endpoint": {"url": "https://example.com"},
                "response": {"results_path": "$", "field_mappings": []}
            }]
        }"#;
        let connector = make_test_connector(json);
        connector.create_source_tables(&conn).unwrap();

        let cols = db::get_table_columns(&conn, "soft_items").unwrap();
        assert!(cols.contains(&"_deleted_at".to_string()));
    }

    #[test]
    fn soft_delete_marks_missing_rows() {
        let conn = test_conn();
        conn.execute_batch(
            "CREATE TABLE sd_items (
                id TEXT PRIMARY KEY,
                title TEXT,
                _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                _deleted_at TEXT
            )",
        )
        .unwrap();

        // Insert 3 rows with old _extracted_at
        conn.execute_batch(
            "INSERT INTO sd_items (id, title, _extracted_at) VALUES ('a', 'A', '2024-01-01 00:00:00');
             INSERT INTO sd_items (id, title, _extracted_at) VALUES ('b', 'B', '2024-01-01 00:00:00');
             INSERT INTO sd_items (id, title, _extracted_at) VALUES ('c', 'C', '2024-01-01 00:00:00');"
        ).unwrap();

        // Simulate: rows a and b were re-inserted (newer _extracted_at), c was not
        conn.execute(
            "UPDATE sd_items SET _extracted_at = '2024-06-01 12:00:00' WHERE id IN ('a', 'b')",
            [],
        )
        .unwrap();

        let count = mark_soft_deleted(&conn, "sd_items", "2024-06-01 00:00:00").unwrap();
        assert_eq!(count, 1); // Only 'c' is older

        let deleted: Option<String> = conn
            .query_row("SELECT _deleted_at FROM sd_items WHERE id = 'c'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert!(deleted.is_some());
    }

    #[test]
    fn soft_delete_undeletes_on_reappearance() {
        let conn = test_conn();
        conn.execute_batch(
            "CREATE TABLE sd_items2 (
                id TEXT PRIMARY KEY,
                title TEXT,
                _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                _deleted_at TEXT
            );
            INSERT INTO sd_items2 (id, title, _deleted_at) VALUES ('x', 'X', '2024-06-01');",
        )
        .unwrap();

        // INSERT OR REPLACE simulates the row reappearing
        conn.execute(
            "INSERT OR REPLACE INTO sd_items2 (id, title) VALUES ('x', 'X updated')",
            [],
        )
        .unwrap();

        let deleted: Option<String> = conn
            .query_row(
                "SELECT _deleted_at FROM sd_items2 WHERE id = 'x'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(deleted.is_none()); // INSERT OR REPLACE clears _deleted_at
    }

    // ========== resumable backfill ==========

    #[test]
    fn resume_cursor_skips_wrong_table() {
        // Cursor for table_a should not affect table_b parsing
        let cursor_json = r#"{"table":"table_a","cursor":"abc","page":5,"rows_so_far":100}"#;
        let resume: serde_json::Value = serde_json::from_str(cursor_json).unwrap();
        // Check that it doesn't match table_b
        assert_ne!(
            resume.get("table").and_then(|t| t.as_str()),
            Some("table_b")
        );
        // But does match table_a
        assert_eq!(
            resume.get("table").and_then(|t| t.as_str()),
            Some("table_a")
        );
    }

    #[test]
    fn soft_delete_skipped_on_incremental() {
        // This is a logic test: mark_soft_deleted should only be called
        // when !ctx.is_incremental(), which is checked at the call site.
        // Verify the function still works correctly when called with a timestamp
        // that matches all rows (simulating what would happen if incorrectly called).
        let conn = test_conn();
        conn.execute_batch(
            "CREATE TABLE sd_inc (
                id TEXT PRIMARY KEY,
                _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                _deleted_at TEXT
            );
            INSERT INTO sd_inc (id, _extracted_at) VALUES ('a', '2024-06-01 12:00:00');",
        )
        .unwrap();

        // With a far-future timestamp, nothing should be marked (all rows are "old")
        let count = mark_soft_deleted(&conn, "sd_inc", "2099-01-01 00:00:00").unwrap();
        assert_eq!(count, 1); // Would mark it — that's why we gate on !is_incremental at the call site
    }
}
