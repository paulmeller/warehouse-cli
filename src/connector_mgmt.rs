//! Connector management commands: add, remove, list, info.

use anyhow::{Context, Result};

use crate::config;
use crate::connector;
use crate::dynamic_connector;

/// `warehouse connector list` — show all connectors with source tags.
pub fn cmd_connector_list() -> Result<()> {
    let registry = connector::default_registry();
    println!("Available connectors:");
    println!();
    for c in registry.all() {
        let fts = if c.fts_schema_sql().is_some() {
            "search"
        } else {
            "sync only"
        };
        let source = c.source();
        println!("  {:<20} {} [{source}] [{fts}]", c.name(), c.description());
    }
    println!();
    println!("{} connector(s) registered", registry.all().len());
    Ok(())
}

/// `warehouse connector add <url>` — download, validate, and install a connector spec.
pub fn cmd_connector_add(url: &str) -> Result<()> {
    // Enforce HTTPS
    if !url.starts_with("https://") {
        anyhow::bail!("Only HTTPS URLs are allowed for security. Got: {url}");
    }

    eprintln!("Downloading connector spec from {url}...");

    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    let resp = client
        .get(url)
        .send()
        .context("Failed to download connector spec")?;

    if !resp.status().is_success() {
        anyhow::bail!("Download failed: HTTP {}", resp.status());
    }

    let json = resp.text().context("Failed to read response body")?;

    // Validate
    let spec = dynamic_connector::validate_spec(&json)?;
    eprintln!("Validated: {} — {}", spec.name, spec.description);

    // Check for conflicts with built-in connectors
    let registry = connector::default_registry();
    if let Some(existing) = registry.get(&spec.name) {
        if existing.source() == "built-in" {
            anyhow::bail!(
                "Cannot install '{}': conflicts with built-in connector",
                spec.name
            );
        }
    }

    // Save to connectors directory
    let dir = dynamic_connector::connectors_dir();
    std::fs::create_dir_all(&dir)?;

    let file_path = dir.join(format!("{}.json", spec.name));
    if file_path.exists() {
        eprintln!("Replacing existing connector: {}", spec.name);
    }

    // Write pretty-printed for readability
    let pretty: serde_json::Value = serde_json::from_str(&json)?;
    let formatted = serde_json::to_string_pretty(&pretty)?;
    std::fs::write(&file_path, formatted)?;

    println!(
        "Installed connector '{}' at {}",
        spec.name,
        file_path.display()
    );

    // Auto-enable permissions if the user has permissions configured
    if config::permissions_configured() {
        let mut cfg = config::load_config();
        let perm = cfg.permissions.entry(spec.name.clone()).or_default();
        if !perm.access {
            perm.access = true;
            config::save_config(&cfg)?;
            println!("Enabled '{}' in permissions.", spec.name);
        }
    }

    println!("Run `warehouse sync {}` to fetch data.", spec.name);
    Ok(())
}

/// `warehouse connector remove <name>` — uninstall a dynamic connector.
pub fn cmd_connector_remove(name: &str) -> Result<()> {
    let dir = dynamic_connector::connectors_dir();
    let file_path = dir.join(format!("{name}.json"));

    if !file_path.exists() {
        anyhow::bail!("Connector '{name}' not found in {}", dir.display());
    }

    std::fs::remove_file(&file_path)?;
    println!("Removed connector '{name}'");
    Ok(())
}

/// `warehouse connector info <name>` — show details of a connector.
pub fn cmd_connector_info(name: &str) -> Result<()> {
    let registry = connector::default_registry();

    let connector = registry.get(name).ok_or_else(|| {
        anyhow::anyhow!("Connector '{name}' not found. Run `warehouse connector list` to see available connectors.")
    })?;

    println!("Name:        {}", connector.name());
    println!("Description: {}", connector.description());
    println!("Source:      {}", connector.source());
    println!(
        "FTS:         {}",
        if connector.fts_schema_sql().is_some() {
            "yes"
        } else {
            "no"
        }
    );

    // If it's a dynamic connector, show the spec file path
    if connector.source() == "installed" {
        let dir = dynamic_connector::connectors_dir();
        let file_path = dir.join(format!("{name}.json"));
        if file_path.exists() {
            println!("Spec file:   {}", file_path.display());

            // Show table info from spec
            if let Ok(json) = std::fs::read_to_string(&file_path) {
                if let Ok(spec) = dynamic_connector::validate_spec(&json) {
                    println!();
                    println!("Tables:");
                    for table in &spec.tables {
                        let cols: Vec<&str> =
                            table.columns.iter().map(|c| c.name.as_str()).collect();
                        println!("  {} ({})", table.name, cols.join(", "));
                    }
                    if !spec.fts.is_empty() {
                        println!();
                        for fts in &spec.fts {
                            println!("FTS index: {} -> {}", fts.table_name, fts.source_table);
                            println!("  columns: {}", fts.columns.join(", "));
                            if !fts.source_tag.is_empty() {
                                println!("  source tag: {}", fts.source_tag);
                            }
                        }
                    }
                    if let Some(ref auth) = spec.auth {
                        println!();
                        let auth_type = match auth {
                            dynamic_connector::AuthSpec::Header { .. } => "header",
                            dynamic_connector::AuthSpec::Env { .. } => "env",
                            dynamic_connector::AuthSpec::ConfigKey { .. } => "config_key",
                            dynamic_connector::AuthSpec::BrowserCookies { .. } => "browser_cookies",
                            dynamic_connector::AuthSpec::SafariLocalStorage { .. } => {
                                "safari_localstorage"
                            }
                            dynamic_connector::AuthSpec::TokenChain { .. } => "token_chain",
                        };
                        println!("Auth type:   {auth_type}");
                    }
                }
            }
        }
    }

    Ok(())
}
