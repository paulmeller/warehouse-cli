//! Extract cookies from installed browsers.
//!
//! Platform support:
//! - **macOS:** Chrome, Brave, Edge, Arc, Firefox, Safari (Keychain + AES-CBC)
//! - **Windows:** Firefox only (Chromium DPAPI decryption not yet implemented)
//! - **Linux:** Firefox only (Chromium Secret Service decryption not yet implemented)

use anyhow::{Context, Result};
use std::path::PathBuf;

/// A browser cookie.
#[derive(Debug, Clone)]
pub struct Cookie {
    pub name: String,
    pub value: String,
    pub domain: String,
}

/// Try all browsers and return cookies matching the given domains.
/// Domains should include leading dot for subdomain matching (e.g., ".x.com").
pub fn scoop_cookies(domains: &[&str]) -> Vec<(String, Vec<Cookie>)> {
    let mut results = Vec::new();

    // Chromium-based browsers (macOS only for now — Windows DPAPI not yet implemented)
    #[cfg(target_os = "macos")]
    {
        let chromium_browsers: &[(&str, &str, &str)] = &[
            ("Chrome", "Google/Chrome", "Chrome Safe Storage"),
            ("Brave", "BraveSoftware/Brave-Browser", "Brave Safe Storage"),
            ("Edge", "Microsoft Edge", "Microsoft Edge Safe Storage"),
            ("Arc", "Arc/User Data", "Arc Safe Storage"),
            ("Chromium", "Chromium", "Chromium Safe Storage"),
            ("Vivaldi", "Vivaldi", "Vivaldi Safe Storage"),
            ("Opera", "com.operasoftware.Opera", "Opera Safe Storage"),
        ];

        for (name, dir_name, keychain_service) in chromium_browsers {
            match read_chromium_cookies(dir_name, keychain_service, domains) {
                Ok(cookies) if !cookies.is_empty() => {
                    results.push((name.to_string(), cookies));
                }
                _ => {}
            }
        }
    }

    // Firefox (cross-platform — plain SQLite, just different paths)
    match read_firefox_cookies(domains) {
        Ok(cookies) if !cookies.is_empty() => {
            results.push(("Firefox".to_string(), cookies));
        }
        _ => {}
    }

    // Safari (macOS only)
    #[cfg(target_os = "macos")]
    match read_safari_cookies(domains) {
        Ok(cookies) if !cookies.is_empty() => {
            results.push(("Safari".to_string(), cookies));
        }
        _ => {}
    }

    results
}

/// Find a cookie by name from scoop results, returning (browser_name, cookie).
pub fn find_cookie<'a>(
    results: &'a [(String, Vec<Cookie>)],
    name: &str,
) -> Option<(&'a str, &'a Cookie)> {
    for (browser, cookies) in results {
        if let Some(cookie) = cookies.iter().find(|c| c.name == name) {
            return Some((browser, cookie));
        }
    }
    None
}

// ========== Chromium-based browsers (macOS) ==========

#[cfg(target_os = "macos")]
fn chromium_cookies_path(dir_name: &str) -> Option<PathBuf> {
    let home = dirs::home_dir()?;
    let base = home.join("Library/Application Support").join(dir_name);

    // Try Default profile first, then Profile 1, etc.
    for profile in &["Default", "Profile 1", "Profile 2", "Profile 3"] {
        let path = base.join(profile).join("Cookies");
        if path.exists() {
            return Some(path);
        }
    }
    None
}

#[cfg(target_os = "macos")]
fn get_keychain_password(service: &str) -> Result<String> {
    let output = std::process::Command::new("/usr/bin/security")
        .args(["find-generic-password", "-w", "-s", service, "-a", service])
        .output()
        .context("Failed to run security command")?;

    if !output.status.success() {
        // Try with just the service name as account (some browsers differ)
        let output2 = std::process::Command::new("/usr/bin/security")
            .args([
                "find-generic-password",
                "-w",
                "-s",
                service,
                "-a",
                &service.replace(" Safe Storage", ""),
            ])
            .output()
            .context("Failed to run security command")?;

        if !output2.status.success() {
            anyhow::bail!("Keychain password not found for {service}");
        }
        return Ok(String::from_utf8(output2.stdout)?.trim().to_string());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

#[cfg(target_os = "macos")]
fn derive_chromium_key(password: &str) -> Result<Vec<u8>> {
    use pbkdf2::pbkdf2_hmac;
    use sha1::Sha1;

    let salt = b"saltysalt";
    let iterations = 1003;
    let mut key = vec![0u8; 16]; // AES-128
    pbkdf2_hmac::<Sha1>(password.as_bytes(), salt, iterations, &mut key);
    Ok(key)
}

#[cfg(target_os = "macos")]
fn decrypt_chromium_value(encrypted: &[u8], key: &[u8]) -> Option<String> {
    use aes::cipher::{block_padding::Pkcs7, BlockDecryptMut, KeyIvInit};

    // Chrome on macOS prefixes encrypted values with "v10" (3 bytes)
    if encrypted.len() < 4 || &encrypted[..3] != b"v10" {
        // Might be unencrypted
        return String::from_utf8(encrypted.to_vec()).ok();
    }

    let ciphertext = &encrypted[3..];
    let iv = [b' '; 16]; // Chrome uses 16 spaces as IV on macOS

    type Aes128CbcDec = cbc::Decryptor<aes::Aes128>;

    let decryptor = Aes128CbcDec::new_from_slices(key, &iv).ok()?;
    let mut buf = ciphertext.to_vec();
    let plaintext = decryptor.decrypt_padded_mut::<Pkcs7>(&mut buf).ok()?;
    String::from_utf8(plaintext.to_vec()).ok()
}

#[cfg(target_os = "macos")]
fn read_chromium_cookies(
    dir_name: &str,
    keychain_service: &str,
    domains: &[&str],
) -> Result<Vec<Cookie>> {
    let cookies_path = chromium_cookies_path(dir_name)
        .ok_or_else(|| anyhow::anyhow!("Cookies database not found"))?;

    let password = get_keychain_password(keychain_service)?;
    let key = derive_chromium_key(&password)?;

    // Copy to temp file to avoid locked database
    let temp = tempfile::NamedTempFile::new()?;
    std::fs::copy(&cookies_path, temp.path())?;

    let conn = rusqlite::Connection::open_with_flags(
        temp.path(),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;

    let placeholders: Vec<String> = domains
        .iter()
        .enumerate()
        .map(|(i, _)| format!("host_key LIKE ?{}", i + 1))
        .collect();
    let where_clause = placeholders.join(" OR ");
    let sql = format!("SELECT name, encrypted_value, host_key FROM cookies WHERE {where_clause}");
    let params: Vec<String> = domains.iter().map(|d| format!("%{d}")).collect();
    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(param_refs.as_slice(), |row| {
        let name: String = row.get(0)?;
        let encrypted: Vec<u8> = row.get(1)?;
        let domain: String = row.get(2)?;
        Ok((name, encrypted, domain))
    })?;

    let mut cookies = Vec::new();
    for row in rows.flatten() {
        let (name, encrypted, domain) = row;
        if let Some(value) = decrypt_chromium_value(&encrypted, &key) {
            if !value.is_empty() {
                cookies.push(Cookie {
                    name,
                    value,
                    domain,
                });
            }
        }
    }

    Ok(cookies)
}

// ========== Firefox (cross-platform) ==========

fn firefox_cookies_path() -> Option<PathBuf> {
    let home = dirs::home_dir()?;

    let profiles_dir = if cfg!(target_os = "macos") {
        home.join("Library/Application Support/Firefox/Profiles")
    } else if cfg!(target_os = "windows") {
        home.join("AppData/Roaming/Mozilla/Firefox/Profiles")
    } else {
        home.join(".mozilla/firefox")
    };

    if !profiles_dir.exists() {
        return None;
    }

    // Find the default profile (usually ends with .default-release or .default)
    let mut entries: Vec<_> = std::fs::read_dir(&profiles_dir)
        .ok()?
        .flatten()
        .filter(|e| e.path().is_dir())
        .collect();

    // Sort to prefer .default-release over .default
    entries.sort_by(|a, b| {
        let a_name = a.file_name().to_string_lossy().to_string();
        let b_name = b.file_name().to_string_lossy().to_string();
        let a_prio = if a_name.contains("default-release") {
            0
        } else if a_name.contains("default") {
            1
        } else {
            2
        };
        let b_prio = if b_name.contains("default-release") {
            0
        } else if b_name.contains("default") {
            1
        } else {
            2
        };
        a_prio.cmp(&b_prio)
    });

    for entry in entries {
        let cookies_path = entry.path().join("cookies.sqlite");
        if cookies_path.exists() {
            return Some(cookies_path);
        }
    }
    None
}

fn read_firefox_cookies(domains: &[&str]) -> Result<Vec<Cookie>> {
    let cookies_path =
        firefox_cookies_path().ok_or_else(|| anyhow::anyhow!("Firefox cookies not found"))?;

    // Copy to temp file to avoid locked database
    let temp = tempfile::NamedTempFile::new()?;
    std::fs::copy(&cookies_path, temp.path())?;

    let conn = rusqlite::Connection::open_with_flags(
        temp.path(),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;

    let placeholders: Vec<String> = domains
        .iter()
        .enumerate()
        .map(|(i, _)| format!("host LIKE ?{}", i + 1))
        .collect();
    let where_clause = placeholders.join(" OR ");
    let sql = format!("SELECT name, value, host FROM moz_cookies WHERE {where_clause}");
    let params: Vec<String> = domains.iter().map(|d| format!("%{d}")).collect();
    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(param_refs.as_slice(), |row| {
        Ok(Cookie {
            name: row.get(0)?,
            value: row.get(1)?,
            domain: row.get(2)?,
        })
    })?;

    Ok(rows.flatten().filter(|c| !c.value.is_empty()).collect())
}

// ========== Safari (macOS only) ==========

#[cfg(target_os = "macos")]
fn read_safari_cookies(domains: &[&str]) -> Result<Vec<Cookie>> {
    let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Cannot find home directory"))?;

    // Modern macOS: Safari is sandboxed in a container
    let container_path =
        home.join("Library/Containers/com.apple.Safari/Data/Library/Cookies/Cookies.binarycookies");
    // Legacy path (older macOS versions)
    let legacy_path = home.join("Library/Cookies/Cookies.binarycookies");

    let cookies_path = if container_path.exists() {
        container_path
    } else if legacy_path.exists() {
        legacy_path
    } else {
        anyhow::bail!("Safari cookies file not found");
    };

    let data = std::fs::read(&cookies_path)
        .context("Cannot read Safari cookies (Full Disk Access required)")?;

    parse_safari_binary_cookies(&data, domains)
}

#[cfg(target_os = "macos")]
fn parse_safari_binary_cookies(data: &[u8], domains: &[&str]) -> Result<Vec<Cookie>> {
    if data.len() < 8 || &data[0..4] != b"cook" {
        anyhow::bail!("Not a valid Safari cookies file");
    }

    let num_pages = u32::from_be_bytes([data[4], data[5], data[6], data[7]]) as usize;
    if data.len() < 8 + num_pages * 4 {
        anyhow::bail!("Truncated Safari cookies file");
    }

    // Read page sizes
    let mut page_sizes = Vec::with_capacity(num_pages);
    for i in 0..num_pages {
        let offset = 8 + i * 4;
        let size = u32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        page_sizes.push(size);
    }

    let mut cookies = Vec::new();
    let mut page_offset = 8 + num_pages * 4;

    for page_size in &page_sizes {
        if page_offset + page_size > data.len() {
            break;
        }

        let page = &data[page_offset..page_offset + page_size];
        if let Ok(page_cookies) = parse_safari_page(page, domains) {
            cookies.extend(page_cookies);
        }

        page_offset += page_size;
    }

    Ok(cookies)
}

#[cfg(target_os = "macos")]
fn parse_safari_page(page: &[u8], domains: &[&str]) -> Result<Vec<Cookie>> {
    if page.len() < 8 {
        anyhow::bail!("Page too small");
    }

    // Page header: 4 bytes magic (0x00000100), 4 bytes cookie count
    let num_cookies = u32::from_le_bytes([page[4], page[5], page[6], page[7]]) as usize;

    if page.len() < 8 + num_cookies * 4 {
        anyhow::bail!("Truncated page");
    }

    // Cookie offsets
    let mut cookie_offsets = Vec::with_capacity(num_cookies);
    for i in 0..num_cookies {
        let off = 8 + i * 4;
        let cookie_off =
            u32::from_le_bytes([page[off], page[off + 1], page[off + 2], page[off + 3]]) as usize;
        cookie_offsets.push(cookie_off);
    }

    let mut cookies = Vec::new();

    for &cookie_off in &cookie_offsets {
        if cookie_off + 48 > page.len() {
            continue;
        }

        let rec = &page[cookie_off..];
        if rec.len() < 48 {
            continue;
        }

        // Cookie record layout (all little-endian):
        // 0: size (4), 1: flags (4), 2: padding (4)
        // 12: url_offset (4), 16: name_offset (4), 20: path_offset (4), 24: value_offset (4)
        let url_off = u32::from_le_bytes([rec[16], rec[17], rec[18], rec[19]]) as usize;
        let name_off = u32::from_le_bytes([rec[20], rec[21], rec[22], rec[23]]) as usize;
        let value_off = u32::from_le_bytes([rec[28], rec[29], rec[30], rec[31]]) as usize;

        let read_str = |offset: usize| -> Option<String> {
            if offset >= rec.len() {
                return None;
            }
            let end = rec[offset..].iter().position(|&b| b == 0)?;
            String::from_utf8(rec[offset..offset + end].to_vec()).ok()
        };

        let domain = match read_str(url_off) {
            Some(d) => d,
            None => continue,
        };
        let name = match read_str(name_off) {
            Some(n) => n,
            None => continue,
        };
        let value = match read_str(value_off) {
            Some(v) => v,
            None => continue,
        };

        // Check domain match
        let domain_matches = domains
            .iter()
            .any(|d| domain.ends_with(d) || domain == d.trim_start_matches('.'));

        if domain_matches && !value.is_empty() {
            cookies.push(Cookie {
                name,
                value,
                domain,
            });
        }
    }

    Ok(cookies)
}
