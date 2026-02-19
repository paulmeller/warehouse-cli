mod browse;
mod cli;
mod config;
mod db;
mod fts;
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
    }
}

fn cmd_init(db_path: &str) -> Result<()> {
    if !std::path::Path::new(db_path).exists() {
        anyhow::bail!("Database not found: {db_path}");
    }
    let conn = db::open(db_path)?;
    db::init_search_schema(&conn)?;
    println!("Search schema initialized.");
    Ok(())
}

fn cmd_index(db_path: &str) -> Result<()> {
    let conn = db::open(db_path)?;
    db::init_search_schema(&conn)?;
    println!("Building FTS5 indexes...");
    let counts = fts::rebuild_all_fts(&conn)?;
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
    let options = search::SearchOptions {
        types: if args.types.is_empty() {
            search::ALL_TYPES.iter().map(|s| s.to_string()).collect()
        } else {
            args.types
        },
        limit: args.limit,
        date_from: args.date_from,
        date_to: args.date_to,
        contact: args.contact,
        min_score: args.min_score,
    };

    let results = search::fts_search(&conn, &args.query, &options)?;

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
    println!("Database: {db_path}");
    println!();

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

    println!();
    println!("FTS indexes:");
    let fts_tables = [
        ("messages_fts", "messages"),
        ("notes_fts", "notes"),
        ("contacts_fts", "contacts"),
        ("photos_fts", "photos"),
        ("documents_fts", "documents"),
        ("reminders_fts", "reminders"),
    ];
    for (table, label) in &fts_tables {
        let count = db::table_count(&conn, table);
        if count > 0 {
            println!("  {label:20} {count:>10}");
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
    let date = args.date.unwrap_or_else(|| chrono::Local::now().format("%Y-%m-%d").to_string());
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
        let marker = if r.id == args.message_id { ">>>" } else { "   " };
        let date = r.metadata.get("date").map(|v| v.as_str().unwrap_or("")).unwrap_or("");
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
    let cfg = config::load_config();

    let results = if args.sources.is_empty() {
        sync::sync_all(&conn, &cfg)
    } else {
        sync::sync_sources(&conn, &cfg, &args.sources)
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

    // Obsidian
    let vaults = config::discover_obsidian_vaults();
    if vaults.is_empty() {
        println!("  Obsidian: no vaults found");
    } else {
        for v in &vaults {
            println!("  Obsidian: {} (OK)", v.display());
        }
    }

    // Reminders
    let reminders = config::discover_reminders_databases();
    if reminders.is_empty() {
        println!("  Reminders: no databases found");
    } else {
        println!("  Reminders: {} database(s) (OK)", reminders.len());
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
    let results = sync::sync_all(&conn, &cfg);
    sync::print_summary(&results);

    // Step 4: Build FTS indexes
    println!();
    println!("Building FTS5 indexes...");
    db::init_search_schema(&conn)?;
    let counts = fts::rebuild_all_fts(&conn)?;
    println!("Indexed:");
    for (name, count) in &counts {
        println!("  {name:12} {count:>10}");
    }

    println!();
    println!("Setup complete! Try:");
    println!("  warehouse search \"your query\"");
    println!("  warehouse status");
    println!("  warehouse doctor");
    Ok(())
}
