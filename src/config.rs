use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Config directory: ~/.warehouse/
fn config_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".warehouse")
}

/// Config file path: ~/.warehouse/config.toml
pub fn config_file_path() -> PathBuf {
    config_dir().join("config.toml")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub discovery: DiscoveryConfig,
    #[serde(default)]
    pub paths: PathsConfig,
    #[serde(default)]
    pub documents: DocumentsConfig,
    #[serde(default)]
    pub twitter: TwitterConfig,
    #[serde(default)]
    pub monarch: MonarchConfig,
    #[serde(default)]
    pub pocketsmith: PocketSmithConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            discovery: DiscoveryConfig::default(),
            paths: PathsConfig::default(),
            documents: DocumentsConfig::default(),
            twitter: TwitterConfig::default(),
            monarch: MonarchConfig::default(),
            pocketsmith: PocketSmithConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub scan_onedrive: bool,
    #[serde(default = "default_true")]
    pub scan_dropbox: bool,
    #[serde(default = "default_true")]
    pub scan_icloud: bool,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            scan_onedrive: true,
            scan_dropbox: true,
            scan_icloud: true,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PathsConfig {
    #[serde(default)]
    pub obsidian_vaults: Vec<String>,
    #[serde(default)]
    pub photos_dirs: Vec<String>,
    #[serde(default)]
    pub documents_dirs: Vec<String>,
    #[serde(default)]
    pub database: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentsConfig {
    #[serde(default = "default_backend")]
    pub backend: String,
    #[serde(default = "default_extensions")]
    pub extensions: Vec<String>,
    #[serde(default = "default_max_size")]
    pub max_file_size_mb: u64,
    #[serde(default = "default_true")]
    pub skip_hidden: bool,
}

impl Default for DocumentsConfig {
    fn default() -> Self {
        Self {
            backend: "markitdown".into(),
            extensions: default_extensions(),
            max_file_size_mb: 50,
            skip_hidden: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TwitterConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_chrome")]
    pub cookie_source: String,
    #[serde(default = "default_true")]
    pub extract_bookmarks: bool,
    #[serde(default = "default_true")]
    pub extract_likes: bool,
    #[serde(default)]
    pub extract_mentions: bool,
    #[serde(default = "default_true")]
    pub incremental: bool,
    #[serde(default = "default_page_delay")]
    pub page_delay_seconds: f64,
    #[serde(default = "default_max_pages")]
    pub max_pages: u32,
    #[serde(default)]
    pub user_screen_name: String,
}

impl Default for TwitterConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            cookie_source: "chrome".into(),
            extract_bookmarks: true,
            extract_likes: true,
            extract_mentions: false,
            incremental: true,
            page_delay_seconds: 2.0,
            max_pages: 50,
            user_screen_name: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonarchConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_safari")]
    pub browser: String,
    #[serde(default = "default_true")]
    pub extract_accounts: bool,
    #[serde(default = "default_true")]
    pub extract_transactions: bool,
    #[serde(default = "default_true")]
    pub extract_budgets: bool,
    #[serde(default = "default_true")]
    pub extract_recurring: bool,
    #[serde(default = "default_365")]
    pub transaction_days: u32,
    #[serde(default = "default_true")]
    pub incremental: bool,
    #[serde(default = "default_page_delay")]
    pub page_delay_seconds: f64,
    #[serde(default = "default_max_pages")]
    pub max_pages: u32,
}

impl Default for MonarchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            browser: "safari".into(),
            extract_accounts: true,
            extract_transactions: true,
            extract_budgets: true,
            extract_recurring: true,
            transaction_days: 365,
            incremental: true,
            page_delay_seconds: 1.0,
            max_pages: 500,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PocketSmithConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub api_key: String,
    #[serde(default = "default_true")]
    pub extract_accounts: bool,
    #[serde(default = "default_true")]
    pub extract_transactions: bool,
    #[serde(default = "default_true")]
    pub extract_categories: bool,
    #[serde(default = "default_365")]
    pub transaction_days: u32,
    #[serde(default = "default_true")]
    pub incremental: bool,
    #[serde(default = "default_half_sec")]
    pub page_delay_seconds: f64,
    #[serde(default = "default_max_pages")]
    pub max_pages: u32,
}

impl Default for PocketSmithConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            api_key: String::new(),
            extract_accounts: true,
            extract_transactions: true,
            extract_categories: true,
            transaction_days: 365,
            incremental: true,
            page_delay_seconds: 0.5,
            max_pages: 500,
        }
    }
}

// Serde default helpers
fn default_true() -> bool { true }
fn default_backend() -> String { "markitdown".into() }
fn default_chrome() -> String { "chrome".into() }
fn default_safari() -> String { "safari".into() }
fn default_page_delay() -> f64 { 2.0 }
fn default_half_sec() -> f64 { 0.5 }
fn default_max_pages() -> u32 { 500 }
fn default_365() -> u32 { 365 }
fn default_extensions() -> Vec<String> {
    vec![
        ".pdf", ".docx", ".doc", ".pptx", ".ppt",
        ".xlsx", ".xls", ".txt", ".rtf", ".html", ".htm",
    ]
    .into_iter()
    .map(String::from)
    .collect()
}
fn default_max_size() -> u64 { 50 }

/// Load config from ~/.warehouse/config.toml, merging with defaults.
pub fn load_config() -> Config {
    let path = config_file_path();
    if path.exists() {
        match std::fs::read_to_string(&path) {
            Ok(content) => match toml::from_str(&content) {
                Ok(cfg) => return cfg,
                Err(e) => {
                    eprintln!("Warning: Failed to parse config {}: {e}", path.display());
                }
            },
            Err(e) => {
                eprintln!("Warning: Failed to read config {}: {e}", path.display());
            }
        }
    }
    Config::default()
}

/// Get the warehouse database path.
pub fn get_warehouse_db_path() -> String {
    let cfg = load_config();
    if !cfg.paths.database.is_empty() {
        return expand_path(&cfg.paths.database)
            .to_string_lossy()
            .to_string();
    }
    config_dir()
        .join("warehouse.db")
        .to_string_lossy()
        .to_string()
}

/// Expand ~ in paths.
pub fn expand_path(path: &str) -> PathBuf {
    if path.starts_with("~/") || path == "~" {
        if let Some(home) = dirs::home_dir() {
            return home.join(&path[2..]);
        }
    }
    PathBuf::from(path)
}

/// Create default config file if it doesn't exist.
pub fn save_default_config() -> anyhow::Result<PathBuf> {
    let dir = config_dir();
    std::fs::create_dir_all(&dir)?;

    let path = config_file_path();
    if !path.exists() {
        let default_toml = r#"# Personal Data Warehouse Configuration
# Paths can be absolute or use ~ for home directory

[discovery]
# Enable auto-discovery of data sources
enabled = true
scan_onedrive = true
scan_dropbox = true
scan_icloud = true

[paths]
# Override or add to discovered paths (optional)
# obsidian_vaults = ["~/Documents/MyVault", "~/Dropbox/Notes"]
# photos_dirs = ["~/Pictures", "D:/Photos"]
# documents_dirs = ["~/Documents", "~/Downloads"]

# Database location (empty = ~/.warehouse/warehouse.db)
# database = "~/warehouse.db"

[documents]
# Document extraction settings
# backend options: "lightweight", "markitdown" (recommended), "docling"
backend = "markitdown"
extensions = [".pdf", ".docx", ".doc", ".pptx", ".ppt", ".xlsx", ".xls", ".txt", ".rtf", ".html", ".htm"]
max_file_size_mb = 50
skip_hidden = true
"#;
        std::fs::write(&path, default_toml)?;
        println!("Created default config at {}", path.display());
    }

    Ok(path)
}

/// Get current platform.
pub fn get_platform() -> &'static str {
    if cfg!(target_os = "macos") {
        "macos"
    } else if cfg!(target_os = "windows") {
        "windows"
    } else {
        "linux"
    }
}

/// Discover Obsidian vault directories.
pub fn discover_obsidian_vaults() -> Vec<PathBuf> {
    let cfg = load_config();
    let mut vaults = Vec::new();

    // User-configured vaults
    for vault_path in &cfg.paths.obsidian_vaults {
        let expanded = expand_path(vault_path);
        if expanded.exists() {
            vaults.push(expanded);
        }
    }

    if !cfg.discovery.enabled {
        return vaults;
    }

    let home = match dirs::home_dir() {
        Some(h) => h,
        None => return vaults,
    };

    let mut search_paths = Vec::new();

    if cfg!(target_os = "macos") {
        if cfg.discovery.scan_icloud {
            search_paths.push(
                home.join("Library/Mobile Documents/iCloud~md~obsidian/Documents"),
            );
        }
        search_paths.push(home.join("Documents"));
        if cfg.discovery.scan_dropbox {
            search_paths.push(home.join("Dropbox"));
        }
    } else if cfg!(target_os = "windows") {
        search_paths.push(home.join("Documents"));
        if cfg.discovery.scan_onedrive {
            search_paths.push(home.join("OneDrive/Documents"));
        }
        if cfg.discovery.scan_dropbox {
            search_paths.push(home.join("Dropbox"));
        }
    } else {
        search_paths.push(home.join("Documents"));
        if cfg.discovery.scan_dropbox {
            search_paths.push(home.join("Dropbox"));
        }
    }

    for base in &search_paths {
        if !base.exists() {
            continue;
        }
        // Check if base itself is a vault
        if base.join(".obsidian").exists() && !vaults.contains(base) {
            vaults.push(base.clone());
        }
        // One level deep
        if let Ok(entries) = std::fs::read_dir(base) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() && path.join(".obsidian").exists() && !vaults.contains(&path) {
                    vaults.push(path.clone());
                }
                // Two levels deep
                if path.is_dir() {
                    if let Ok(sub_entries) = std::fs::read_dir(&path) {
                        for sub_entry in sub_entries.flatten() {
                            let sub_path = sub_entry.path();
                            if sub_path.is_dir()
                                && sub_path.join(".obsidian").exists()
                                && !vaults.contains(&sub_path)
                            {
                                vaults.push(sub_path);
                            }
                        }
                    }
                }
            }
        }
    }

    vaults
}

/// Get iMessages database path (macOS only).
pub fn get_imessages_db_path() -> Option<PathBuf> {
    if !cfg!(target_os = "macos") {
        return None;
    }
    let path = dirs::home_dir()?.join("Library/Messages/chat.db");
    path.exists().then_some(path)
}

/// Get Apple Photos database path (macOS only).
pub fn get_photos_db_path() -> Option<PathBuf> {
    if !cfg!(target_os = "macos") {
        return None;
    }
    let path = dirs::home_dir()?
        .join("Pictures/Photos Library.photoslibrary/database/Photos.sqlite");
    path.exists().then_some(path)
}

/// Discover Apple Reminders databases (macOS only).
pub fn discover_reminders_databases() -> Vec<PathBuf> {
    if !cfg!(target_os = "macos") {
        return Vec::new();
    }
    let home = match dirs::home_dir() {
        Some(h) => h,
        None => return Vec::new(),
    };
    let stores_dir = home.join(
        "Library/Group Containers/group.com.apple.reminders/Container_v1/Stores",
    );
    if !stores_dir.exists() {
        return Vec::new();
    }
    match std::fs::read_dir(&stores_dir) {
        Ok(entries) => entries
            .flatten()
            .filter(|e| {
                e.path()
                    .extension()
                    .is_some_and(|ext| ext == "sqlite")
            })
            .map(|e| e.path())
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// Discover document directories.
pub fn discover_documents_directories() -> Vec<PathBuf> {
    let cfg = load_config();
    let mut dirs_list = Vec::new();

    for dir_path in &cfg.paths.documents_dirs {
        let expanded = expand_path(dir_path);
        if expanded.exists() {
            dirs_list.push(expanded);
        }
    }

    if !cfg.discovery.enabled {
        return dirs_list;
    }

    let home = match dirs::home_dir() {
        Some(h) => h,
        None => return dirs_list,
    };

    let add_if_exists = |dirs: &mut Vec<PathBuf>, path: PathBuf| {
        if path.exists() && !dirs.contains(&path) {
            dirs.push(path);
        }
    };

    if cfg!(target_os = "macos") {
        add_if_exists(&mut dirs_list, home.join("Documents"));
        if cfg.discovery.scan_icloud {
            add_if_exists(
                &mut dirs_list,
                home.join("Library/Mobile Documents/com~apple~CloudDocs"),
            );
        }
        if cfg.discovery.scan_dropbox {
            add_if_exists(&mut dirs_list, home.join("Dropbox"));
        }
    } else if cfg!(target_os = "windows") {
        add_if_exists(&mut dirs_list, home.join("Documents"));
        if cfg.discovery.scan_onedrive {
            add_if_exists(&mut dirs_list, home.join("OneDrive/Documents"));
        }
        if cfg.discovery.scan_dropbox {
            add_if_exists(&mut dirs_list, home.join("Dropbox"));
        }
    } else {
        add_if_exists(&mut dirs_list, home.join("Documents"));
        if cfg.discovery.scan_dropbox {
            add_if_exists(&mut dirs_list, home.join("Dropbox"));
        }
    }

    dirs_list
}

/// Print discovered data sources.
pub fn print_discovered_sources() {
    let platform = get_platform();
    let path = config_file_path();
    println!("Platform: {platform}");
    println!("Config file: {} (exists: {})", path.display(), path.exists());
    println!();

    println!("Obsidian Vaults:");
    for vault in discover_obsidian_vaults() {
        println!("  - {}", vault.display());
    }

    println!("\nDocument Directories:");
    for dir in discover_documents_directories() {
        println!("  - {}", dir.display());
    }

    if cfg!(target_os = "macos") {
        println!("\niMessages DB:");
        match get_imessages_db_path() {
            Some(p) => println!("  - {}", p.display()),
            None => println!("  - Not found"),
        }

        println!("\nPhotos DB:");
        match get_photos_db_path() {
            Some(p) => println!("  - {}", p.display()),
            None => println!("  - Not found"),
        }

        println!("\nReminders DBs:");
        for db in discover_reminders_databases() {
            println!("  - {}", db.display());
        }
    }

    println!("\nWarehouse DB:");
    println!("  - {}", get_warehouse_db_path());
}

#[allow(dead_code)]
/// Apple epoch offset: seconds between Unix epoch (1970) and Apple epoch (2001).
pub const APPLE_EPOCH_OFFSET: i64 = 978307200;

/// Convert Apple Core Data timestamp to ISO string.
#[allow(dead_code)]
pub fn apple_timestamp_to_iso(timestamp: f64) -> Option<String> {
    if timestamp == 0.0 {
        return None;
    }
    // Handle nanoseconds format
    let ts = if timestamp > 1e12 {
        timestamp / 1e9
    } else {
        timestamp
    };
    let unix_ts = ts as i64 + APPLE_EPOCH_OFFSET;
    chrono::DateTime::from_timestamp(unix_ts, 0)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S").to_string())
}
