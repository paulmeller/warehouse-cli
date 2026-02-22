//! Shared auth services for connectors.
//!
//! Provides reusable authentication strategies that can be used by both
//! built-in Rust connectors and dynamic JSON connectors:
//! - Token caching with restricted file permissions
//! - Safari localStorage extraction (UTF-16LE)
//! - Browser cookie extraction (via cookies.rs)
//! - Environment variable resolution
//! - Config key resolution
//! - Token chain with fallback strategies and validation

use anyhow::{Context, Result};
use std::path::PathBuf;

// ========== Token caching ==========

/// Path to a cached token file under ~/.warehouse/
pub fn cached_token_path(name: &str) -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".warehouse")
        .join(name)
}

/// Read a cached token, returning None if missing or empty.
pub fn read_cached_token(name: &str) -> Option<String> {
    let path = cached_token_path(name);
    std::fs::read_to_string(&path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Save a token to cache with restricted permissions (0600).
pub fn save_cached_token(name: &str, token: &str) -> Result<()> {
    let path = cached_token_path(name);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&path, token)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    }

    Ok(())
}

/// Delete a cached token file.
pub fn delete_cached_token(name: &str) {
    let path = cached_token_path(name);
    let _ = std::fs::remove_file(&path);
}

// ========== Safari localStorage (macOS only) ==========

/// Find a Safari localStorage database by scanning origin directories
/// for a key marker (e.g., "monarchDeviceUUID").
///
/// Safari stores localStorage in SQLite databases under:
/// ~/Library/Containers/com.apple.Safari/Data/Library/WebKit/WebsiteData/Default/
pub fn find_safari_localstorage(origin_marker: &str) -> Option<PathBuf> {
    let home = dirs::home_dir()?;
    let base =
        home.join("Library/Containers/com.apple.Safari/Data/Library/WebKit/WebsiteData/Default");

    if !base.exists() {
        return None;
    }

    let entries = std::fs::read_dir(&base).ok()?;
    for entry in entries.flatten() {
        let origin_dir = entry.path();
        if !origin_dir.is_dir() {
            continue;
        }

        let dir_name = origin_dir.file_name()?.to_string_lossy().to_string();
        let ls_path = origin_dir
            .join(&dir_name)
            .join("LocalStorage")
            .join("localstorage.sqlite3");

        if !ls_path.exists() {
            continue;
        }

        let conn = rusqlite::Connection::open_with_flags(
            &ls_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .ok()?;

        let found: bool = conn
            .query_row(
                &format!(
                    "SELECT COUNT(*) > 0 FROM ItemTable WHERE key = '{}'",
                    origin_marker.replace('\'', "''")
                ),
                [],
                |r| r.get(0),
            )
            .unwrap_or(false);

        if found {
            return Some(ls_path);
        }
    }

    None
}

/// Read a value from a Safari localStorage SQLite database.
/// Safari stores values as UTF-16LE encoded bytes.
pub fn read_localstorage_value(db_path: &PathBuf, key: &str) -> Result<String> {
    let conn =
        rusqlite::Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .context("Failed to open localStorage database")?;

    let value_bytes: Vec<u8> = conn
        .query_row(
            "SELECT value FROM ItemTable WHERE key = ?1",
            rusqlite::params![key],
            |r| r.get(0),
        )
        .context(format!("Key '{key}' not found in localStorage"))?;

    // Decode UTF-16LE
    let u16_chars: Vec<u16> = value_bytes
        .chunks_exact(2)
        .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
        .collect();

    String::from_utf16(&u16_chars).context("Failed to decode UTF-16LE localStorage value")
}

/// Extract a token from Safari localStorage by navigating a JSON dot-path.
///
/// For example, with key="persist:root" and token_path="user.token":
/// 1. Read persist:root from localStorage (JSON)
/// 2. Get the "user" field (which is itself a JSON string)
/// 3. Parse it and get the "token" field
pub fn extract_safari_token(
    origin_marker: &str,
    localstorage_key: &str,
    token_path: &str,
) -> Result<String> {
    let db_path = find_safari_localstorage(origin_marker).ok_or_else(|| {
        anyhow::anyhow!(
            "Safari localStorage not found for origin marker '{origin_marker}'. \
             Log into the web app in Safari first."
        )
    })?;

    let raw_value = read_localstorage_value(&db_path, localstorage_key).context(format!(
        "Could not read '{localstorage_key}' from localStorage"
    ))?;

    // Navigate dot-path through potentially nested JSON strings
    let parts: Vec<&str> = token_path.split('.').collect();
    let mut current: serde_json::Value =
        serde_json::from_str(&raw_value).context("Failed to parse localStorage value as JSON")?;

    for (i, part) in parts.iter().enumerate() {
        // If the current value is a string, try to parse it as JSON
        if let Some(s) = current.as_str() {
            current = serde_json::from_str(s).context(format!(
                "Failed to parse nested JSON at path segment '{part}'"
            ))?;
        }
        current = current.get(part).cloned().ok_or_else(|| {
            let path_so_far = parts[..=i].join(".");
            anyhow::anyhow!("Key '{path_so_far}' not found in localStorage data")
        })?;
    }

    // Final value might be a string
    if let Some(s) = current.as_str() {
        Ok(s.to_string())
    } else {
        Ok(current.to_string())
    }
}

// ========== Browser cookies ==========

/// Extract specific cookies from browsers for given domains.
/// Returns a map of cookie_name -> cookie_value.
pub fn extract_browser_cookies(
    domains: &[&str],
    cookie_names: &[&str],
) -> Result<std::collections::HashMap<String, String>> {
    let results = crate::cookies::scoop_cookies(domains);
    let mut cookies = std::collections::HashMap::new();

    for name in cookie_names {
        if let Some((browser, cookie)) = crate::cookies::find_cookie(&results, name) {
            if cookies.is_empty() {
                eprintln!("  using cookies from {browser}");
            }
            cookies.insert(name.to_string(), cookie.value.clone());
        }
    }

    if cookies.len() < cookie_names.len() {
        let missing: Vec<&&str> = cookie_names
            .iter()
            .filter(|n| !cookies.contains_key(**n))
            .collect();
        anyhow::bail!(
            "Missing cookies: {}. Log in to the site in a browser.",
            missing
                .iter()
                .map(|n| n.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    Ok(cookies)
}

// ========== Template resolution ==========

/// Resolve `{{env.VAR_NAME}}` templates in a string.
pub fn resolve_env_template(template: &str) -> Result<String> {
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| regex::Regex::new(r"\{\{env\.([^}]+)\}\}").unwrap());
    let mut result = template.to_string();

    for caps in re.captures_iter(template) {
        let full_match = caps.get(0).unwrap().as_str();
        let var_name = &caps[1];
        let value = std::env::var(var_name)
            .context(format!("Environment variable '{var_name}' not set"))?;
        result = result.replace(full_match, &value);
    }

    Ok(result)
}

/// Resolve `{{cookies.NAME}}` templates using a cookie map.
pub fn resolve_cookie_template(
    template: &str,
    cookies: &std::collections::HashMap<String, String>,
) -> String {
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| regex::Regex::new(r"\{\{cookies\.([^}]+)\}\}").unwrap());
    let mut result = template.to_string();

    for caps in re.captures_iter(template) {
        let full_match = caps.get(0).unwrap().as_str();
        let cookie_name = &caps[1];
        if let Some(value) = cookies.get(cookie_name) {
            result = result.replace(full_match, value);
        }
    }

    result
}

// ========== GraphQL token validation ==========

/// Validate a token by making a lightweight GraphQL query.
pub fn validate_graphql_token(
    client: &reqwest::blocking::Client,
    url: &str,
    token: &str,
    header_name: &str,
    header_prefix: &str,
    query: &str,
) -> bool {
    let auth_value = if header_prefix.is_empty() {
        token.to_string()
    } else {
        format!("{header_prefix} {token}")
    };

    let payload = serde_json::json!({ "query": query });

    let resp = client
        .post(url)
        .header(header_name, &auth_value)
        .json(&payload)
        .send();

    match resp {
        Ok(r) => r.status().is_success(),
        Err(_) => false,
    }
}

/// Read a dotted config key from the loaded Config's TOML representation.
/// For example, "pocketsmith.api_key" reads config.pocketsmith.api_key.
pub fn read_config_key(key: &str) -> Result<String> {
    let config = crate::config::load_config();
    let toml_value = toml::Value::try_from(&config).context("Failed to serialize config")?;

    let mut current = &toml_value;
    for part in key.split('.') {
        current = current.get(part).ok_or_else(|| {
            anyhow::anyhow!("Config key '{key}' not found (missing segment '{part}')")
        })?;
    }

    match current {
        toml::Value::String(s) => Ok(s.clone()),
        other => Ok(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_env_template_no_vars() {
        let result = resolve_env_template("Bearer static-token").unwrap();
        assert_eq!(result, "Bearer static-token");
    }

    #[test]
    fn resolve_env_template_with_var() {
        std::env::set_var("TEST_WAREHOUSE_TOKEN", "abc123");
        let result = resolve_env_template("Bearer {{env.TEST_WAREHOUSE_TOKEN}}").unwrap();
        assert_eq!(result, "Bearer abc123");
        std::env::remove_var("TEST_WAREHOUSE_TOKEN");
    }

    #[test]
    fn resolve_env_template_missing_var() {
        let result = resolve_env_template("{{env.NONEXISTENT_VAR_12345}}");
        assert!(result.is_err());
    }

    #[test]
    fn resolve_cookie_template_works() {
        let mut cookies = std::collections::HashMap::new();
        cookies.insert("ct0".to_string(), "token123".to_string());
        cookies.insert("auth_token".to_string(), "auth456".to_string());

        let result = resolve_cookie_template(
            "auth_token={{cookies.auth_token}}; ct0={{cookies.ct0}}",
            &cookies,
        );
        assert_eq!(result, "auth_token=auth456; ct0=token123");
    }

    #[test]
    fn cached_token_path_under_warehouse() {
        let path = cached_token_path("test_token");
        assert!(path.to_string_lossy().contains(".warehouse"));
        assert!(path.to_string_lossy().ends_with("test_token"));
    }
}
