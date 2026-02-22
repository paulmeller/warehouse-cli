#[allow(dead_code)]
mod audit;
mod auth;
mod browse;
mod cli;
mod config;
mod connector;
mod connector_mgmt;
#[allow(dead_code)]
mod cookies;
mod db;
mod dynamic_connector;
mod fts;
#[allow(dead_code)]
mod governance;
mod permissions;
mod schedule;
mod search;
mod sync;

use anyhow::Result;
use clap::Parser;

use cli::{Cli, Commands};

fn main() -> Result<()> {
    let cli = Cli::parse();
    let db_path = cli.resolve_db_path();

    match cli.command {
        Commands::Init => cmd_init(&db_path),
        Commands::Index => cmd_index(&db_path),
        Commands::Search(args) => cmd_search(&db_path, args),
        Commands::Status => cmd_status(&db_path),
        Commands::Config(sub) => cmd_config(sub),
        Commands::Messages(args) => cmd_browse_messages(&db_path, args),
        Commands::Notes(args) => cmd_browse_notes(&db_path, args),
        Commands::Contacts(args) => cmd_browse_contacts(&db_path, args),
        Commands::Documents(args) => cmd_browse_documents(&db_path, args),
        Commands::Reminders(args) => cmd_browse_reminders(&db_path, args),
        Commands::Photos(args) => cmd_browse_photos(&db_path, args),
        Commands::Person(args) => cmd_person(&db_path, args),
        Commands::Timeline(args) => cmd_timeline(&db_path, args),
        Commands::Recent => cmd_recent(&db_path),
        Commands::Context(args) => cmd_context(&db_path, args),
        Commands::Show(args) => cmd_show(&db_path, args),
        Commands::Sync(args) => cmd_sync(&db_path, args),
        Commands::Schedule(sub) => cmd_schedule(sub),
        Commands::Doctor => cmd_doctor(),
        Commands::Setup => cmd_setup(&db_path),
        Commands::Connector(sub) => cmd_connector(sub),
        Commands::Permissions(sub) => cmd_permissions(sub),
        Commands::Audit(args) => cmd_audit(args),
    }
}

fn cmd_init(db_path: &str) -> Result<()> {
    if !std::path::Path::new(db_path).exists() {
        anyhow::bail!("Database not found: {db_path}");
    }
    let conn = db::open(db_path)?;
    let registry = connector::default_registry();
    db::init_search_schema(&conn, &registry)?;
    println!("Search schema initialized.");
    Ok(())
}

fn cmd_index(db_path: &str) -> Result<()> {
    let conn = db::open(db_path)?;
    let registry = connector::default_registry();
    db::init_search_schema(&conn, &registry)?;
    println!("Building FTS5 indexes...");
    let counts = fts::rebuild_all_fts(&conn, &registry)?;
    println!();
    println!("Indexed:");
    for (name, count) in &counts {
        println!("  {name:12} {count:>10}");
    }
    println!("Done!");
    Ok(())
}

fn cmd_search(db_path: &str, args: cli::SearchArgs) -> Result<()> {
    let conn = db::open(db_path)?;
    let registry = connector::default_registry();

    // Validate and resolve search types
    let requested_types: Vec<String> = if args.types.is_empty() {
        registry
            .all_search_types()
            .iter()
            .map(|s| s.to_string())
            .collect()
    } else {
        // Validate requested types at runtime
        let valid_types = registry.all_search_types();
        for t in &args.types {
            if !valid_types.contains(&t.as_str()) {
                anyhow::bail!(
                    "Unknown search type '{}'. Available types: {}",
                    t,
                    valid_types.join(", ")
                );
            }
        }
        args.types
    };

    // Build search options — permission checks happen inside fts_search per-connector
    let options = search::SearchOptions {
        types: requested_types.clone(),
        limit: args.limit,
        date_from: args.date_from,
        date_to: args.date_to,
        min_score: args.min_score,
    };

    let results = search::fts_search(&conn, &args.query, &options, &registry)?;

    // Apply field redaction
    let (results, redacted_fields) = if config::permissions_configured() {
        governance::apply_field_redaction(results, &registry)
    } else {
        (results, std::collections::HashMap::new())
    };

    // Log to audit trail
    let queried_sources: Vec<String> = requested_types
        .iter()
        .filter_map(|t| registry.search_type_to_source(t))
        .map(|s| s.to_string())
        .collect();
    let blocked_sources: Vec<String> = if config::permissions_configured() {
        queried_sources
            .iter()
            .filter(|s| !governance::is_source_allowed(s))
            .cloned()
            .collect()
    } else {
        vec![]
    };
    let _ = audit::log_search(
        &args.query,
        &queried_sources,
        &blocked_sources,
        results.len(),
        &redacted_fields,
    );

    match args.format.as_str() {
        "json" => println!("{}", search::format_json(&results)?),
        "csv" => print!("{}", search::format_csv(&results)),
        "markdown" => print!("{}", search::format_markdown(&results)),
        _ => search::format_text(&results),
    }
    Ok(())
}

fn cmd_status(db_path: &str) -> Result<()> {
    if !std::path::Path::new(db_path).exists() {
        println!("Database: {db_path} (not found)");
        return Ok(());
    }
    let conn = db::open(db_path)?;
    let registry = connector::default_registry();
    println!("Database: {db_path}");
    println!();

    // Built-in source tables
    let tables = [
        ("contacts", "contacts"),
        ("messages", "imessage_messages"),
        ("notes", "obsidian_notes"),
        ("photos", "photos_assets"),
        ("documents", "documents"),
        ("reminders", "reminders"),
    ];

    println!("Source data:");
    for (label, table) in &tables {
        let count = db::table_count(&conn, table);
        if count > 0 {
            println!("  {label:20} {count:>10}");
        }
    }

    // Dynamic connector source tables
    for connector in registry.all() {
        if connector.source() == "built-in"
            && connector.name() != "contacts"
            && connector.name() != "imessages"
            && connector.name() != "obsidian"
            && connector.name() != "photos"
            && connector.name() != "documents"
            && connector.name() != "reminders"
        {
            // This is a dynamic built-in connector - check its tables
            let name = connector.name();
            // Try common table patterns
            let count = db::table_count(&conn, &format!("{}_transactions", name));
            if count > 0 {
                println!("  {name:20} {count:>10}");
            }
        }
    }

    println!();
    println!("FTS indexes:");
    // Collect FTS tables from all connectors
    let mut seen_fts: Vec<String> = Vec::new();
    for connector in registry.all() {
        if let Some(fts_sql) = connector.fts_schema_sql() {
            // Extract table names from FTS DDL
            for line in fts_sql.lines() {
                if line.contains("CREATE VIRTUAL TABLE") {
                    if let Some(name) = line.split_whitespace().nth(5) {
                        let name = name.trim();
                        if !seen_fts.contains(&name.to_string()) {
                            let count = db::table_count(&conn, name);
                            if count > 0 {
                                // Use connector governance source as label
                                let label = connector.governance_source();
                                println!("  {label:20} {count:>10}");
                            }
                            seen_fts.push(name.to_string());
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

fn cmd_config(sub: cli::ConfigSubcommand) -> Result<()> {
    match sub {
        cli::ConfigSubcommand::Show => {
            let cfg = config::load_config();
            println!("Config file: {}", config::config_file_path().display());
            println!();
            println!("{}", toml::to_string_pretty(&cfg)?);
        }
        cli::ConfigSubcommand::Sources => {
            config::print_discovered_sources();
        }
        cli::ConfigSubcommand::Init => {
            let path = config::save_default_config()?;
            println!("Config file: {}", path.display());
        }
    }
    Ok(())
}

fn cmd_browse_messages(db_path: &str, args: cli::MessagesArgs) -> Result<()> {
    let conn = db::open(db_path)?;
    let results = browse::browse_messages(&conn, &args)?;
    browse::print_results(&results, &args.format, args.limit);
    Ok(())
}

fn cmd_browse_notes(db_path: &str, args: cli::NotesArgs) -> Result<()> {
    let conn = db::open(db_path)?;
    let results = browse::browse_notes(&conn, &args)?;
    browse::print_results(&results, &args.format, args.limit);
    Ok(())
}

fn cmd_browse_contacts(db_path: &str, args: cli::ContactsArgs) -> Result<()> {
    let conn = db::open(db_path)?;
    let results = browse::browse_contacts(&conn, &args)?;
    browse::print_results(&results, &args.format, args.limit);
    Ok(())
}

fn cmd_browse_documents(db_path: &str, args: cli::DocumentsArgs) -> Result<()> {
    let conn = db::open(db_path)?;
    let results = browse::browse_documents(&conn, &args)?;
    browse::print_results(&results, &args.format, args.limit);
    Ok(())
}

fn cmd_browse_reminders(db_path: &str, args: cli::RemindersArgs) -> Result<()> {
    let conn = db::open(db_path)?;
    let results = browse::browse_reminders(&conn, &args)?;
    browse::print_results(&results, &args.format, args.limit);
    Ok(())
}

fn cmd_browse_photos(db_path: &str, args: cli::PhotosArgs) -> Result<()> {
    let conn = db::open(db_path)?;
    let results = browse::browse_photos(&conn, &args)?;
    browse::print_results(&results, &args.format, args.limit);
    Ok(())
}

fn cmd_person(db_path: &str, args: cli::PersonArgs) -> Result<()> {
    let conn = db::open(db_path)?;
    let data = browse::person_view(&conn, &args.name, args.limit)?;

    match args.format.as_str() {
        "json" => println!("{}", serde_json::to_string_pretty(&data)?),
        _ => browse::print_person_text(&data),
    }
    Ok(())
}

fn cmd_timeline(db_path: &str, args: cli::TimelineArgs) -> Result<()> {
    let conn = db::open(db_path)?;
    let days = if args.week { 7 } else { 1 };
    let date = args
        .date
        .unwrap_or_else(|| chrono::Local::now().format("%Y-%m-%d").to_string());
    let data = browse::timeline_view(&conn, &date, days, args.limit)?;

    match args.format.as_str() {
        "json" => println!("{}", serde_json::to_string_pretty(&data)?),
        _ => browse::print_timeline_text(&data),
    }
    Ok(())
}

fn cmd_recent(db_path: &str) -> Result<()> {
    let conn = db::open(db_path)?;
    let data = browse::recent_activity(&conn, 5)?;
    browse::print_recent_text(&data);
    Ok(())
}

fn cmd_context(db_path: &str, args: cli::ContextArgs) -> Result<()> {
    let conn = db::open(db_path)?;
    let results = browse::message_context(&conn, &args.message_id, args.before, args.after)?;
    for r in &results {
        let marker = if r.id == args.message_id {
            ">>>"
        } else {
            "   "
        };
        let date = r
            .metadata
            .get("date")
            .map(|v| v.as_str().unwrap_or(""))
            .unwrap_or("");
        println!("{marker} [{date}] {}: {}", r.title, r.snippet);
    }
    Ok(())
}

fn cmd_show(db_path: &str, args: cli::ShowArgs) -> Result<()> {
    let conn = db::open(db_path)?;
    let parts: Vec<&str> = args.item.splitn(2, ':').collect();
    if parts.len() != 2 {
        anyhow::bail!("Usage: warehouse show <type>:<id> (e.g., note:123)");
    }
    let content = browse::get_full_content(&conn, parts[0], parts[1])?;
    match content {
        Some(data) => println!("{}", serde_json::to_string_pretty(&data)?),
        None => println!("Not found: {}", args.item),
    }
    Ok(())
}

fn cmd_sync(db_path: &str, args: cli::SyncArgs) -> Result<()> {
    // Create DB directory if needed
    if let Some(parent) = std::path::Path::new(db_path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    let conn = db::open(db_path)?;

    // Handle --history flag
    if args.history {
        let runs = db::get_sync_history(&conn, 20)?;
        if runs.is_empty() {
            println!("No sync history found.");
            return Ok(());
        }

        match args.format.as_str() {
            "json" => {
                let json_runs: Vec<serde_json::Value> = runs
                    .iter()
                    .map(|r| {
                        serde_json::json!({
                            "id": r.id,
                            "connector": r.connector_name,
                            "started_at": r.started_at,
                            "ended_at": r.ended_at,
                            "status": r.status,
                            "rows_synced": r.rows_synced,
                            "error_message": r.error_message,
                            "sync_mode": r.sync_mode,
                        })
                    })
                    .collect();
                println!("{}", serde_json::to_string_pretty(&json_runs)?);
            }
            _ => {
                println!(
                    "{:<4} {:<16} {:<13} {:<8} {:>8} {:<25}",
                    "ID", "Connector", "Mode", "Status", "Rows", "Started"
                );
                println!("{}", "-".repeat(78));
                for r in &runs {
                    // Truncate started_at to just date+time
                    let started = if r.started_at.len() > 19 {
                        &r.started_at[..19]
                    } else {
                        &r.started_at
                    };
                    println!(
                        "{:<4} {:<16} {:<13} {:<8} {:>8} {:<25}",
                        r.id, r.connector_name, r.sync_mode, r.status, r.rows_synced, started
                    );
                    if let Some(ref err) = r.error_message {
                        let truncated = if err.len() > 70 {
                            format!("{}...", &err[..67])
                        } else {
                            err.clone()
                        };
                        println!("     error: {truncated}");
                    }
                }
            }
        }
        return Ok(());
    }

    let cfg = config::load_config();
    let registry = connector::default_registry();

    let results = if args.sources.is_empty() {
        sync::sync_all(&conn, &cfg, &registry, args.full)
    } else {
        sync::sync_sources(&conn, &cfg, &args.sources, &registry, args.full)
    };

    match args.format.as_str() {
        "json" => {
            let json_results: Vec<serde_json::Value> = results
                .iter()
                .map(|r| {
                    serde_json::json!({
                        "source": r.source,
                        "count": r.count,
                        "status": match &r.status {
                            sync::SyncStatus::Success => "success",
                            sync::SyncStatus::Skipped(_) => "skipped",
                            sync::SyncStatus::Failed(_) => "failed",
                        },
                    })
                })
                .collect();
            println!("{}", serde_json::to_string_pretty(&json_results)?);
        }
        _ => sync::print_summary(&results),
    }
    Ok(())
}

fn cmd_schedule(sub: cli::ScheduleSubcommand) -> Result<()> {
    match sub {
        cli::ScheduleSubcommand::Install(args) => {
            if let Some(time) = args.daily {
                schedule::install_daily(&time)
            } else if let Some(hours) = args.every {
                schedule::install_interval(hours)
            } else {
                anyhow::bail!("Specify --daily HH:MM or --every N")
            }
        }
        cli::ScheduleSubcommand::Remove => schedule::remove(),
        cli::ScheduleSubcommand::Status => schedule::status(),
        cli::ScheduleSubcommand::Logs(args) => schedule::logs(args.lines),
    }
}

fn cmd_doctor() -> Result<()> {
    println!("Warehouse Doctor");
    println!("================");
    println!();

    let platform = config::get_platform();
    println!("Platform: {platform}");

    let db_path = config::get_warehouse_db_path();
    let db_exists = std::path::Path::new(&db_path).exists();
    println!("Database: {db_path} (exists: {db_exists})");

    let cfg_path = config::config_file_path();
    println!(
        "Config: {} (exists: {})",
        cfg_path.display(),
        cfg_path.exists()
    );

    println!();
    println!("Data sources:");

    // macOS-only data sources
    #[cfg(target_os = "macos")]
    {
        // iMessages
        match config::get_imessages_db_path() {
            Some(p) => println!("  iMessages: {} (OK)", p.display()),
            None => println!("  iMessages: not found (requires Full Disk Access)"),
        }

        // Photos
        match config::get_photos_db_path() {
            Some(p) => println!("  Photos: {} (OK)", p.display()),
            None => println!("  Photos: not found"),
        }

        // Reminders
        let reminders = config::discover_reminders_databases();
        if reminders.is_empty() {
            println!("  Reminders: no databases found");
        } else {
            println!("  Reminders: {} database(s) (OK)", reminders.len());
        }
    }

    // Cross-platform data sources

    // Obsidian
    let vaults = config::discover_obsidian_vaults();
    if vaults.is_empty() {
        println!("  Obsidian: no vaults found");
    } else {
        for v in &vaults {
            println!("  Obsidian: {} (OK)", v.display());
        }
    }

    // Documents
    let doc_dirs = config::discover_documents_directories();
    if doc_dirs.is_empty() {
        println!("  Documents: no directories found");
    } else {
        println!("  Documents: {} directory(ies) (OK)", doc_dirs.len());
    }

    if db_exists {
        println!();
        println!("Database contents:");
        let conn = db::open(&db_path)?;
        let tables = [
            ("contacts", "contacts"),
            ("messages", "imessage_messages"),
            ("notes", "obsidian_notes"),
            ("photos", "photos_assets"),
            ("documents", "documents"),
            ("reminders", "reminders"),
        ];
        for (label, table) in &tables {
            let count = db::table_count(&conn, table);
            if count > 0 {
                println!("  {label:20} {count:>10}");
            }
        }
    }

    println!();
    println!("All checks complete.");
    Ok(())
}

fn cmd_setup(db_path: &str) -> Result<()> {
    println!("Warehouse Setup");
    println!("===============");
    println!();

    // Step 1: Create config if needed
    let cfg_path = config::config_file_path();
    if !cfg_path.exists() {
        println!("Creating default config...");
        config::save_default_config()?;
    } else {
        println!("Config: {} (exists)", cfg_path.display());
    }

    // Step 2: Create DB directory
    if let Some(parent) = std::path::Path::new(db_path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Step 3: Sync
    println!();
    println!("Syncing data sources...");
    let conn = db::open(db_path)?;
    let cfg = config::load_config();
    let registry = connector::default_registry();
    let results = sync::sync_all(&conn, &cfg, &registry, false);
    sync::print_summary(&results);

    // Step 4: Build FTS indexes
    println!();
    println!("Building FTS5 indexes...");
    db::init_search_schema(&conn, &registry)?;
    let counts = fts::rebuild_all_fts(&conn, &registry)?;
    println!("Indexed:");
    for (name, count) in &counts {
        println!("  {name:12} {count:>10}");
    }

    // Step 5: Offer to install popular API connectors
    println!();
    println!("Optional: install API connectors for cloud services.");
    println!("These are downloaded from the warehouse-connectors gallery.");
    println!();

    let gallery_connectors = [
        ("pocketsmith", "PocketSmith — accounts, categories, transactions"),
        ("monarch", "Monarch Money — accounts, transactions, budgets"),
        ("twitter", "Twitter/X — bookmarks and likes"),
        ("notion", "Notion — pages and databases"),
    ];

    let gallery_base =
        "https://raw.githubusercontent.com/paulmeller/warehouse-connectors/main/connectors";

    for (name, description) in &gallery_connectors {
        // Skip if already installed
        let installed_path = dynamic_connector::connectors_dir().join(format!("{name}.json"));
        if installed_path.exists() {
            println!("  {description} [already installed]");
            continue;
        }

        print!("  Install {description}? [y/N] ");
        use std::io::Write;
        std::io::stdout().flush()?;

        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        let input = input.trim().to_lowercase();

        if input == "y" || input == "yes" {
            let url = format!("{gallery_base}/{name}.json");
            match connector_mgmt::cmd_connector_add(&url) {
                Ok(()) => {}
                Err(e) => eprintln!("  Warning: Failed to install {name}: {e}"),
            }
        }
    }

    println!();
    println!("Setup complete! Try:");
    println!("  warehouse search \"your query\"");
    println!("  warehouse status");
    println!("  warehouse doctor");
    Ok(())
}

fn cmd_connector(sub: cli::ConnectorSubcommand) -> Result<()> {
    match sub {
        cli::ConnectorSubcommand::List => connector_mgmt::cmd_connector_list(),
        cli::ConnectorSubcommand::Add(args) => connector_mgmt::cmd_connector_add(&args.url),
        cli::ConnectorSubcommand::Remove(args) => connector_mgmt::cmd_connector_remove(&args.name),
        cli::ConnectorSubcommand::Info(args) => connector_mgmt::cmd_connector_info(&args.name),
    }
}

fn cmd_permissions(sub: cli::PermissionsSubcommand) -> Result<()> {
    let registry = connector::default_registry();
    match sub {
        cli::PermissionsSubcommand::Show => {
            governance::print_permissions_summary(&registry);
        }
        cli::PermissionsSubcommand::Enable(args) => {
            permissions::enable_source(&args.source, &registry)?;
        }
        cli::PermissionsSubcommand::Disable(args) => {
            permissions::disable_source(&args.source, &registry)?;
        }
        cli::PermissionsSubcommand::Set(args) => {
            if let Some(ref fields) = args.fields {
                permissions::set_fields(&args.source, fields, &registry)?;
            }
            if let Some(ref max_age) = args.max_age {
                permissions::set_max_age(&args.source, max_age, &registry)?;
            }
            if args.fields.is_none() && args.max_age.is_none() {
                anyhow::bail!("Specify --fields or --max-age. Example:\n  warehouse permissions set contacts --fields name,email\n  warehouse permissions set notes --max-age 90");
            }
        }
        cli::PermissionsSubcommand::Reset => {
            permissions::reset_permissions()?;
        }
        cli::PermissionsSubcommand::Setup => {
            permissions::run_onboarding(&registry)?;
        }
    }
    Ok(())
}

fn cmd_audit(args: cli::AuditArgs) -> Result<()> {
    let days = if args.week { 7 } else { args.days.unwrap_or(7) };
    let registry = connector::default_registry();

    audit::print_digest(days, args.source.as_deref(), args.blocked, &registry)?;
    Ok(())
}
