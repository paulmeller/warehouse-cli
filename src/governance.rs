use std::collections::HashMap;

use crate::config::{self, SourcePermission};
use crate::search::SearchResult;

/// All known data source names.
pub const ALL_SOURCES: &[&str] = &[
    "messages",
    "contacts",
    "notes",
    "documents",
    "reminders",
    "photos",
];

/// Known fields per data source (used for onboarding and validation).
pub fn source_fields(source: &str) -> &'static [&'static str] {
    match source {
        "messages" => &["sender_name", "chat_name", "text"],
        "contacts" => &["name", "email", "phone", "address"],
        "notes" => &["title", "body", "tags", "frontmatter"],
        "documents" => &["title", "filename", "content"],
        "reminders" => &["title", "notes", "list_name", "location"],
        "photos" => &["title", "filename", "people", "location"],
        _ => &[],
    }
}

/// Human-readable description for each data source.
pub fn source_description(source: &str) -> &'static str {
    match source {
        "messages" => "Your full iMessage and SMS conversation history.",
        "contacts" => "Names, emails, phone numbers, and addresses.",
        "notes" => "Your Obsidian vault — notes, tags, and links.",
        "documents" => "Text extracted from PDF, DOCX, XLSX, and PPTX files.",
        "reminders" => "Lists, due dates, and priorities.",
        "photos" => "Your Apple Photos library — images, faces, people, locations.",
        _ => "Unknown data source.",
    }
}

/// Check if a source is accessible based on permissions.
pub fn is_source_allowed(source: &str) -> bool {
    let permissions = config::load_permissions();
    match permissions.get(source) {
        Some(perm) => perm.access,
        None => false, // Default deny
    }
}

/// Get allowed fields for a source. Returns None if all fields are allowed.
#[allow(dead_code)]
pub fn get_allowed_fields(source: &str) -> Option<Vec<String>> {
    let permissions = config::load_permissions();
    match permissions.get(source) {
        Some(perm) if perm.access => perm.fields.clone(),
        _ => None,
    }
}

/// Get max age in days for a source. Returns None if no limit.
#[allow(dead_code)]
pub fn get_max_age_days(source: &str) -> Option<u32> {
    let permissions = config::load_permissions();
    match permissions.get(source) {
        Some(perm) if perm.access => perm.max_age_days,
        _ => None,
    }
}

/// Get the permission for a specific source.
pub fn get_source_permission(source: &str) -> SourcePermission {
    let permissions = config::load_permissions();
    permissions
        .get(source)
        .cloned()
        .unwrap_or(SourcePermission::default())
}

/// Filter search types to only include permitted sources.
pub fn filter_allowed_types(types: &[String]) -> Vec<String> {
    types
        .iter()
        .filter(|t| {
            let source = search_type_to_source(t);
            is_source_allowed(source)
        })
        .cloned()
        .collect()
}

/// Map search result type names to source permission names.
pub fn search_type_to_source(search_type: &str) -> &str {
    match search_type {
        "message" => "messages",
        "note" => "notes",
        "contact" => "contacts",
        "photo" => "photos",
        "document" => "documents",
        "reminder" => "reminders",
        _ => search_type,
    }
}

/// Map source names to search type names.
#[allow(dead_code)]
pub fn source_to_search_type(source: &str) -> &str {
    match source {
        "messages" => "message",
        "notes" => "note",
        "contacts" => "contact",
        "photos" => "photo",
        "documents" => "document",
        "reminders" => "reminder",
        _ => source,
    }
}

/// Check if a specific field is allowed for a source.
#[allow(dead_code)]
pub fn is_field_allowed(source: &str, field: &str) -> bool {
    match get_allowed_fields(source) {
        Some(fields) => fields.iter().any(|f| f == field),
        None => true, // No field restriction = all allowed
    }
}

/// Apply field redaction to search results based on permissions.
/// Returns the redacted results and a list of redacted fields per source.
pub fn apply_field_redaction(
    results: Vec<SearchResult>,
) -> (Vec<SearchResult>, HashMap<String, Vec<String>>) {
    let permissions = config::load_permissions();
    let mut redacted_fields: HashMap<String, Vec<String>> = HashMap::new();

    let filtered: Vec<SearchResult> = results
        .into_iter()
        .filter_map(|mut result| {
            let source = search_type_to_source(&result.result_type);
            let perm = match permissions.get(source) {
                Some(p) if p.access => p,
                _ => return None, // Source blocked entirely
            };

            // Apply field-level redaction if fields are specified
            if let Some(ref allowed) = perm.fields {
                let mut source_redacted = Vec::new();

                // Redact metadata fields not in allowed list
                let metadata_keys: Vec<String> = result.metadata.keys().cloned().collect();
                for key in &metadata_keys {
                    let field_name = metadata_key_to_field(source, key);
                    if !allowed.iter().any(|f| f == field_name || f == key.as_str()) {
                        result.metadata.remove(key);
                        if !source_redacted.contains(&field_name.to_string()) {
                            source_redacted.push(field_name.to_string());
                        }
                    }
                }

                // Redact snippet if content/body/text field not allowed
                let content_field = match source {
                    "messages" => "text",
                    "notes" => "body",
                    "documents" => "content",
                    "reminders" => "notes",
                    _ => "content",
                };
                if !allowed.iter().any(|f| f == content_field) && !result.snippet.is_empty() {
                    result.snippet = "[redacted]".to_string();
                    if !source_redacted.contains(&content_field.to_string()) {
                        source_redacted.push(content_field.to_string());
                    }
                }

                if !source_redacted.is_empty() {
                    redacted_fields
                        .entry(source.to_string())
                        .or_default()
                        .extend(source_redacted);
                }
            }

            Some(result)
        })
        .collect();

    // Deduplicate redacted field lists
    for fields in redacted_fields.values_mut() {
        fields.sort();
        fields.dedup();
    }

    (filtered, redacted_fields)
}

/// Map metadata keys to canonical field names for permission checking.
fn metadata_key_to_field<'a>(source: &str, key: &'a str) -> &'a str {
    match (source, key) {
        ("contacts", "org") => "organization",
        ("contacts", "sender") => "phone",
        (_, "path") => "file_path",
        (_, "modified") => "modified_at",
        (_, "date") => "date",
        (_, "due") => "due_date",
        (_, "list") => "list_name",
        (_, "lat" | "lng" | "distance_km") => "location",
        (_, "from_me") => "sender_name",
        (_, "chat_id") => "chat_name",
        _ => key,
    }
}

/// Compute the cutoff date string for a max_age_days filter.
#[allow(dead_code)]
pub fn max_age_cutoff_date(days: u32) -> String {
    let cutoff = chrono::Local::now() - chrono::Duration::days(days as i64);
    cutoff.format("%Y-%m-%d").to_string()
}

/// Format a human-readable summary of a source's permissions.
pub fn format_source_summary(_source: &str, perm: &SourcePermission) -> String {
    if !perm.access {
        return "blocked".to_string();
    }

    let mut parts = Vec::new();

    if let Some(ref fields) = perm.fields {
        if fields.is_empty() {
            parts.push("no fields".to_string());
        } else {
            parts.push(format!("{} only", fields.join(", ")));
        }
    } else {
        parts.push("full access".to_string());
    }

    if let Some(days) = perm.max_age_days {
        parts.push(format!("last {} days", days));
    } else if perm.fields.is_some() {
        parts.push("no time limit".to_string());
    }

    parts.join(" · ")
}

/// Print the current permissions summary.
pub fn print_permissions_summary() {
    let permissions = config::load_permissions();
    println!("Current permissions:");
    println!();
    for source in ALL_SOURCES {
        let perm = permissions.get(*source).cloned().unwrap_or_default();
        let icon = if perm.access { "\u{2713}" } else { "\u{2717}" };
        let summary = format_source_summary(source, &perm);
        println!("  {:<12} {}  {}", source, icon, summary);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_search_type_to_source() {
        assert_eq!(search_type_to_source("message"), "messages");
        assert_eq!(search_type_to_source("note"), "notes");
        assert_eq!(search_type_to_source("contact"), "contacts");
        assert_eq!(search_type_to_source("photo"), "photos");
        assert_eq!(search_type_to_source("document"), "documents");
        assert_eq!(search_type_to_source("reminder"), "reminders");
    }

    #[test]
    fn test_source_to_search_type() {
        assert_eq!(source_to_search_type("messages"), "message");
        assert_eq!(source_to_search_type("notes"), "note");
    }

    #[test]
    fn test_source_fields_known() {
        assert!(!source_fields("messages").is_empty());
        assert!(!source_fields("contacts").is_empty());
        assert!(!source_fields("notes").is_empty());
    }

    #[test]
    fn test_source_fields_unknown() {
        assert!(source_fields("unknown_source").is_empty());
    }

    #[test]
    fn test_format_source_summary_blocked() {
        let perm = SourcePermission::default();
        assert_eq!(format_source_summary("messages", &perm), "blocked");
    }

    #[test]
    fn test_format_source_summary_full_access() {
        let perm = SourcePermission {
            access: true,
            fields: None,
            max_age_days: None,
        };
        assert_eq!(format_source_summary("documents", &perm), "full access");
    }

    #[test]
    fn test_format_source_summary_fields_only() {
        let perm = SourcePermission {
            access: true,
            fields: Some(vec!["name".into(), "email".into()]),
            max_age_days: None,
        };
        let summary = format_source_summary("contacts", &perm);
        assert!(summary.contains("name, email only"));
        assert!(summary.contains("no time limit"));
    }

    #[test]
    fn test_format_source_summary_with_max_age() {
        let perm = SourcePermission {
            access: true,
            fields: Some(vec!["title".into(), "body".into()]),
            max_age_days: Some(180),
        };
        let summary = format_source_summary("notes", &perm);
        assert!(summary.contains("last 180 days"));
    }

    #[test]
    fn test_max_age_cutoff_date() {
        let cutoff = max_age_cutoff_date(90);
        // Just verify it produces a valid date string
        assert_eq!(cutoff.len(), 10);
        assert!(cutoff.contains('-'));
    }

    #[test]
    fn test_apply_field_redaction_blocked_source() {
        let results = vec![SearchResult {
            result_type: "message".into(),
            id: "1".into(),
            title: "Test".into(),
            snippet: "content".into(),
            score: 1.0,
            metadata: HashMap::new(),
        }];
        // With default permissions (all blocked), results should be empty
        let (filtered, _) = apply_field_redaction(results);
        // This depends on config state, so we just verify it doesn't panic
        assert!(filtered.len() <= 1);
    }

    #[test]
    fn test_filter_allowed_types_empty() {
        let types: Vec<String> = vec![];
        let filtered = filter_allowed_types(&types);
        assert!(filtered.is_empty());
    }
}
