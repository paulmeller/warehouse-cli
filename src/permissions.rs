use std::io::{self, BufRead, Write};

use anyhow::Result;

use crate::config::{self, SourcePermission};
use crate::connector::ConnectorRegistry;
use crate::governance;

/// Run the interactive onboarding flow.
pub fn run_onboarding(registry: &ConnectorRegistry) -> Result<()> {
    let mut config = config::load_config();

    println!("Welcome to Warehouse.");
    println!();
    println!(
        "Warehouse lets AI agents search your personal data \u{2014} but only what you allow."
    );
    println!(
        "Let's set up your permissions. You can change these any time with: warehouse permissions"
    );
    println!();

    for source in registry.all_sources() {
        let existing = config.permissions.get(source).cloned();
        let perm = prompt_source_permission(source, existing.as_ref(), registry)?;
        config.permissions.insert(source.to_string(), perm);
    }

    // Show summary
    println!();
    println!("All done. Here's your configuration:");
    println!();
    for source in registry.all_sources() {
        if let Some(perm) = config.permissions.get(source) {
            let icon = if perm.access { "\u{2713}" } else { "\u{2717}" };
            let summary = governance::format_source_summary(source, perm);
            println!("  {:<12} {}  {}", source, icon, summary);
        }
    }
    println!();

    // Confirm save
    print!("Save these settings? [Y/n] ");
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().lock().read_line(&mut input)?;
    let input = input.trim().to_lowercase();

    if input.is_empty() || input == "y" || input == "yes" {
        config::save_config(&config)?;
        println!();
        println!(
            "Configuration saved. Run `warehouse permissions` to review or change at any time."
        );
    } else {
        println!("Aborted. No changes saved.");
    }

    Ok(())
}

/// Prompt the user for a single source's permission.
fn prompt_source_permission(
    source: &str,
    existing: Option<&SourcePermission>,
    registry: &ConnectorRegistry,
) -> Result<SourcePermission> {
    let description = registry.source_description(source);
    let title = source[..1].to_uppercase() + &source[1..];

    println!(
        "\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}\u{2501}"
    );
    println!();
    println!("  {}", title);
    println!("  {}", description);
    println!();

    // Show current value if re-running
    if let Some(perm) = existing {
        let summary = governance::format_source_summary(source, perm);
        println!("  Current: {}", summary);
        println!();
    }

    println!("  [Y] Yes    \u{2014} full access");
    println!("  [M] Maybe  \u{2014} configure what agents can see");
    println!("  [N] No     \u{2014} blocked (default)");
    println!();

    loop {
        print!("  > ");
        io::stdout().flush()?;
        let mut input = String::new();
        io::stdin().lock().read_line(&mut input)?;
        let input = input.trim().to_lowercase();

        match input.as_str() {
            "y" | "yes" => {
                return Ok(SourcePermission {
                    access: true,
                    fields: None,
                    max_age_days: None,
                });
            }
            "m" | "maybe" => {
                return prompt_granular_permission(source, existing, registry);
            }
            "" | "n" | "no" => {
                return Ok(SourcePermission::default());
            }
            _ => {
                println!("  Please enter Y, M, or N.");
            }
        }
    }
}

/// Prompt for granular field and time permissions.
fn prompt_granular_permission(
    source: &str,
    existing: Option<&SourcePermission>,
    registry: &ConnectorRegistry,
) -> Result<SourcePermission> {
    let fields = registry.source_fields(source);

    // Field selection
    println!();
    println!("  Which fields can agents see?");
    println!("  (enter field numbers separated by commas, or 'all')");
    println!();

    let existing_fields = existing.and_then(|p| p.fields.as_ref());

    for (i, field) in fields.iter().enumerate() {
        let checked = match existing_fields {
            Some(ef) => ef.iter().any(|f| f == field),
            None => true, // Default to checked for new setup
        };
        let mark = if checked { "\u{2713}" } else { " " };
        println!("    [{}] {} {}", mark, i + 1, field);
    }
    println!();

    print!("  Fields> ");
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().lock().read_line(&mut input)?;
    let input = input.trim().to_lowercase();

    let selected_fields: Option<Vec<String>> = if input == "all" || input.is_empty() {
        None // All fields
    } else {
        let selected: Vec<String> = input
            .split(',')
            .filter_map(|s| {
                let s = s.trim();
                // Try parsing as number first
                if let Ok(num) = s.parse::<usize>() {
                    if num >= 1 && num <= fields.len() {
                        return Some(fields[num - 1].to_string());
                    }
                }
                // Try as field name
                if fields.contains(&s) {
                    return Some(s.to_string());
                }
                None
            })
            .collect();
        if selected.is_empty() {
            None
        } else {
            Some(selected)
        }
    };

    // Time range
    println!();
    println!("  How far back can agents see?");
    println!();
    println!("  [A] All history");
    println!("  [R] Recent only  \u{2014} last 90 days");
    println!("  [C] Custom");
    println!("  [S] Skip         \u{2014} no time limit");
    println!();

    let max_age_days = loop {
        print!("  > ");
        io::stdout().flush()?;
        let mut input = String::new();
        io::stdin().lock().read_line(&mut input)?;
        let input = input.trim().to_lowercase();

        match input.as_str() {
            "a" | "all" | "" | "s" | "skip" => break None,
            "r" | "recent" => break Some(90),
            "c" | "custom" => {
                print!("  How many days back? ");
                io::stdout().flush()?;
                let mut days_input = String::new();
                io::stdin().lock().read_line(&mut days_input)?;
                match days_input.trim().parse::<u32>() {
                    Ok(days) if days > 0 => break Some(days),
                    _ => println!("  Please enter a positive number."),
                }
            }
            _ => println!("  Please enter A, R, C, or S."),
        }
    };

    Ok(SourcePermission {
        access: true,
        fields: selected_fields,
        max_age_days,
    })
}

/// Enable a specific source.
pub fn enable_source(source: &str, registry: &ConnectorRegistry) -> Result<()> {
    validate_source_name(source, registry)?;
    let mut config = config::load_config();
    let perm = config.permissions.entry(source.to_string()).or_default();
    perm.access = true;
    config::save_config(&config)?;
    println!("Enabled access to '{}'.", source);
    Ok(())
}

/// Disable a specific source.
pub fn disable_source(source: &str, registry: &ConnectorRegistry) -> Result<()> {
    validate_source_name(source, registry)?;
    let mut config = config::load_config();
    let perm = config.permissions.entry(source.to_string()).or_default();
    perm.access = false;
    config::save_config(&config)?;
    println!("Disabled access to '{}'.", source);
    Ok(())
}

/// Set fields for a source.
pub fn set_fields(source: &str, fields_str: &str, registry: &ConnectorRegistry) -> Result<()> {
    validate_source_name(source, registry)?;
    let mut config = config::load_config();
    let perm = config
        .permissions
        .entry(source.to_string())
        .or_insert_with(|| SourcePermission {
            access: true,
            fields: None,
            max_age_days: None,
        });

    if fields_str == "all" {
        perm.fields = None;
        println!("Set '{}' to all fields.", source);
    } else {
        let known = registry.source_fields(source);
        let fields: Vec<String> = fields_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|f| known.contains(&f.as_str()) || !f.is_empty())
            .collect();
        println!("Set '{}' fields to: {}", source, fields.join(", "));
        perm.fields = Some(fields);
    }

    perm.access = true; // Setting fields implies enabling
    config::save_config(&config)?;
    Ok(())
}

/// Set max age for a source.
pub fn set_max_age(source: &str, max_age: &str, registry: &ConnectorRegistry) -> Result<()> {
    validate_source_name(source, registry)?;
    let mut config = config::load_config();
    let perm = config
        .permissions
        .entry(source.to_string())
        .or_insert_with(|| SourcePermission {
            access: true,
            fields: None,
            max_age_days: None,
        });

    if max_age == "none" {
        perm.max_age_days = None;
        println!("Removed time limit for '{}'.", source);
    } else {
        let days: u32 = max_age.parse().map_err(|_| {
            anyhow::anyhow!(
                "Invalid max-age value '{}'. Use a number of days or 'none'.",
                max_age
            )
        })?;
        perm.max_age_days = Some(days);
        println!("Set '{}' max age to {} days.", source, days);
    }

    perm.access = true;
    config::save_config(&config)?;
    Ok(())
}

/// Reset all permissions to defaults (deny-all).
pub fn reset_permissions() -> Result<()> {
    print!("This will reset all permissions to deny-all. Continue? [y/N] ");
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().lock().read_line(&mut input)?;
    let input = input.trim().to_lowercase();

    if input == "y" || input == "yes" {
        let mut config = config::load_config();
        config.permissions.clear();
        config::save_config(&config)?;
        println!("All permissions reset to defaults (deny-all).");
    } else {
        println!("Aborted.");
    }

    Ok(())
}

/// Validate that a source name is known.
fn validate_source_name(source: &str, registry: &ConnectorRegistry) -> Result<()> {
    let all = registry.all_sources();
    if all.contains(&source) {
        Ok(())
    } else {
        anyhow::bail!(
            "Unknown source '{}'. Available sources: {}",
            source,
            all.join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_registry() -> ConnectorRegistry {
        crate::connector::default_registry()
    }

    #[test]
    fn validate_known_sources() {
        let registry = test_registry();
        assert!(validate_source_name("messages", &registry).is_ok());
        assert!(validate_source_name("contacts", &registry).is_ok());
        assert!(validate_source_name("notes", &registry).is_ok());
        assert!(validate_source_name("documents", &registry).is_ok());
        assert!(validate_source_name("reminders", &registry).is_ok());
        assert!(validate_source_name("photos", &registry).is_ok());
    }

    #[test]
    fn validate_unknown_source() {
        let registry = test_registry();
        assert!(validate_source_name("unknown", &registry).is_err());
        assert!(validate_source_name("", &registry).is_err());
    }
}
