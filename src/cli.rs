use clap::{Parser, Subcommand};

use crate::config;

#[derive(Parser)]
#[command(name = "warehouse", about = "Personal data warehouse - search & browse")]
#[command(after_help = "Quick examples:\n  \
    warehouse search \"meeting notes\"\n  \
    warehouse reminders\n  \
    warehouse messages --contact \"Sarah\"\n  \
    warehouse notes --tag \"project\"\n  \
    warehouse contacts --search \"Smith\"\n  \
    warehouse documents --type pdf\n  \
    warehouse person \"John\"\n  \
    warehouse timeline --week")]
pub struct Cli {
    /// Database path (overrides config)
    #[arg(long, global = true, env = "WAREHOUSE_DB")]
    pub db: Option<String>,

    #[command(subcommand)]
    pub command: Commands,
}

impl Cli {
    pub fn resolve_db_path(&self) -> String {
        if let Some(ref db) = self.db {
            return db.clone();
        }
        config::get_warehouse_db_path()
    }
}

#[derive(Subcommand)]
pub enum Commands {
    /// Initialize search schema (FTS5 tables)
    Init,

    /// Build FTS5 indexes from source data
    Index,

    /// Search across all content
    Search(SearchArgs),

    /// Show database status and counts
    Status,

    /// Configuration management
    #[command(subcommand)]
    Config(ConfigSubcommand),

    /// Browse messages
    Messages(MessagesArgs),

    /// Browse notes
    Notes(NotesArgs),

    /// Browse contacts
    Contacts(ContactsArgs),

    /// Browse documents
    Documents(DocumentsArgs),

    /// Browse reminders
    Reminders(RemindersArgs),

    /// Browse/search photos
    Photos(PhotosArgs),

    /// View everything about a person
    Person(PersonArgs),

    /// View activity timeline
    Timeline(TimelineArgs),

    /// Recent activity dashboard
    Recent,

    /// Show message context (surrounding messages)
    Context(ContextArgs),

    /// Show full content of an item
    Show(ShowArgs),

    /// Sync data from all or specific sources
    Sync(SyncArgs),

    /// Manage sync schedule (LaunchAgent)
    #[command(subcommand)]
    Schedule(ScheduleSubcommand),

    /// Check system requirements and data sources
    Doctor,

    /// First-time setup (sync + index)
    Setup,
}

#[derive(Subcommand)]
pub enum ConfigSubcommand {
    /// Show current configuration
    Show,
    /// Show discovered data sources
    Sources,
    /// Create default config file
    Init,
}

#[derive(Parser)]
pub struct SearchArgs {
    /// Search query
    pub query: String,

    /// Number of results
    #[arg(short = 'n', long, default_value_t = 10)]
    pub limit: usize,

    /// Filter by content type
    #[arg(short = 't', long = "type", value_parser = [
        "message", "note", "contact", "photo", "document",
        "reminder", "bookmark", "like", "transaction"
    ])]
    pub types: Vec<String>,

    /// Start date (YYYY-MM-DD)
    #[arg(long = "from")]
    pub date_from: Option<String>,

    /// End date (YYYY-MM-DD)
    #[arg(long = "to")]
    pub date_to: Option<String>,

    /// Filter messages by contact name
    #[arg(long)]
    pub contact: Option<String>,

    /// Minimum score threshold
    #[arg(long, default_value_t = 0.0)]
    pub min_score: f64,

    /// Output format
    #[arg(long, default_value = "text", value_parser = ["text", "json", "csv", "markdown"])]
    pub format: String,
}

#[derive(Parser)]
pub struct MessagesArgs {
    /// Filter by contact name
    #[arg(long)]
    pub contact: Option<String>,

    /// Start date (YYYY-MM-DD)
    #[arg(long = "from")]
    pub date_from: Option<String>,

    /// End date (YYYY-MM-DD)
    #[arg(long = "to")]
    pub date_to: Option<String>,

    /// Only sent messages
    #[arg(long)]
    pub from_me: bool,

    /// Search message content
    #[arg(long)]
    pub search: Option<String>,

    /// Sort by: date, contact
    #[arg(long, default_value = "date")]
    pub sort: String,

    /// Reverse sort order
    #[arg(long)]
    pub reverse: bool,

    /// Number of results
    #[arg(short = 'n', long, default_value_t = 20)]
    pub limit: usize,

    /// Output format
    #[arg(long, default_value = "text", value_parser = ["text", "json", "markdown"])]
    pub format: String,
}

#[derive(Parser)]
pub struct NotesArgs {
    /// Filter by vault name
    #[arg(long)]
    pub vault: Option<String>,

    /// Filter by tag
    #[arg(long)]
    pub tag: Option<String>,

    /// Search in content
    #[arg(long)]
    pub search: Option<String>,

    /// Modified after date (YYYY-MM-DD)
    #[arg(long = "from")]
    pub date_from: Option<String>,

    /// Modified before date (YYYY-MM-DD)
    #[arg(long = "to")]
    pub date_to: Option<String>,

    /// Sort by: modified, created, title
    #[arg(long, default_value = "modified")]
    pub sort: String,

    /// Reverse sort order
    #[arg(long)]
    pub reverse: bool,

    /// Number of results
    #[arg(short = 'n', long, default_value_t = 20)]
    pub limit: usize,

    /// Output format
    #[arg(long, default_value = "text", value_parser = ["text", "json", "markdown"])]
    pub format: String,
}

#[derive(Parser)]
pub struct ContactsArgs {
    /// Search by name
    #[arg(long)]
    pub search: Option<String>,

    /// Filter by organization
    #[arg(long)]
    pub org: Option<String>,

    /// Only contacts with email
    #[arg(long)]
    pub has_email: bool,

    /// Only contacts with phone
    #[arg(long)]
    pub has_phone: bool,

    /// Sort by: name, org
    #[arg(long, default_value = "name")]
    pub sort: String,

    /// Reverse sort order
    #[arg(long)]
    pub reverse: bool,

    /// Number of results
    #[arg(short = 'n', long, default_value_t = 50)]
    pub limit: usize,

    /// Output format
    #[arg(long, default_value = "text", value_parser = ["text", "json", "markdown"])]
    pub format: String,
}

#[derive(Parser)]
pub struct DocumentsArgs {
    /// Filter by file type (pdf, docx, etc.)
    #[arg(long = "type")]
    pub file_type: Option<String>,

    /// Search in content
    #[arg(long)]
    pub search: Option<String>,

    /// Modified after date (YYYY-MM-DD)
    #[arg(long = "from")]
    pub date_from: Option<String>,

    /// Modified before date (YYYY-MM-DD)
    #[arg(long = "to")]
    pub date_to: Option<String>,

    /// Sort by: modified, size, name, type
    #[arg(long, default_value = "modified")]
    pub sort: String,

    /// Reverse sort order
    #[arg(long)]
    pub reverse: bool,

    /// Number of results
    #[arg(short = 'n', long, default_value_t = 20)]
    pub limit: usize,

    /// Output format
    #[arg(long, default_value = "text", value_parser = ["text", "json", "markdown"])]
    pub format: String,
}

#[derive(Parser)]
pub struct RemindersArgs {
    /// Include completed reminders
    #[arg(long)]
    pub all: bool,

    /// Only completed reminders
    #[arg(long)]
    pub completed: bool,

    /// Filter by list name
    #[arg(long)]
    pub list: Option<String>,

    /// Due today
    #[arg(long)]
    pub due_today: bool,

    /// Due within 7 days
    #[arg(long)]
    pub due_week: bool,

    /// Overdue reminders
    #[arg(long)]
    pub overdue: bool,

    /// Filter by priority (high, medium, low)
    #[arg(long)]
    pub priority: Option<String>,

    /// Sort by: due, priority, created, title
    #[arg(long, default_value = "due")]
    pub sort: String,

    /// Reverse sort order
    #[arg(long)]
    pub reverse: bool,

    /// Number of results
    #[arg(short = 'n', long, default_value_t = 20)]
    pub limit: usize,

    /// Output format
    #[arg(long, default_value = "text", value_parser = ["text", "json", "markdown"])]
    pub format: String,
}

#[derive(Parser)]
pub struct PhotosArgs {
    /// Person name to search for
    pub name: Option<String>,

    /// Photos near location (lat,lng)
    #[arg(long)]
    pub near: Option<String>,

    /// Search radius in km
    #[arg(long, default_value_t = 10.0)]
    pub radius: f64,

    /// After date (YYYY-MM-DD)
    #[arg(long = "from")]
    pub date_from: Option<String>,

    /// Before date (YYYY-MM-DD)
    #[arg(long = "to")]
    pub date_to: Option<String>,

    /// Sort by: date, name
    #[arg(long, default_value = "date")]
    pub sort: String,

    /// Reverse sort order
    #[arg(long)]
    pub reverse: bool,

    /// Number of results
    #[arg(short = 'n', long, default_value_t = 20)]
    pub limit: usize,

    /// Output format
    #[arg(long, default_value = "text", value_parser = ["text", "json", "markdown"])]
    pub format: String,
}

#[derive(Parser)]
pub struct PersonArgs {
    /// Person name
    pub name: String,

    /// Items per category
    #[arg(short = 'n', long, default_value_t = 5)]
    pub limit: usize,

    /// Output format
    #[arg(long, default_value = "text", value_parser = ["text", "json"])]
    pub format: String,
}

#[derive(Parser)]
pub struct TimelineArgs {
    /// Show past 7 days
    #[arg(long)]
    pub week: bool,

    /// Specific date (YYYY-MM-DD)
    #[arg(long)]
    pub date: Option<String>,

    /// Items per category
    #[arg(short = 'n', long, default_value_t = 10)]
    pub limit: usize,

    /// Output format
    #[arg(long, default_value = "text", value_parser = ["text", "json"])]
    pub format: String,
}

#[derive(Parser)]
pub struct ContextArgs {
    /// Message ID to get context for
    pub message_id: String,

    /// Messages before
    #[arg(short = 'b', long, default_value_t = 5)]
    pub before: usize,

    /// Messages after
    #[arg(short = 'a', long, default_value_t = 5)]
    pub after: usize,
}

#[derive(Parser)]
pub struct ShowArgs {
    /// Item to show (format: type:id, e.g., note:123)
    pub item: String,
}

#[derive(Parser)]
pub struct SyncArgs {
    /// Specific sources to sync (e.g., contacts photos imessages)
    pub sources: Vec<String>,

    /// Output format
    #[arg(long, default_value = "text", value_parser = ["text", "json"])]
    pub format: String,
}

#[derive(Subcommand)]
pub enum ScheduleSubcommand {
    /// Install daily sync schedule
    Install(ScheduleInstallArgs),
    /// Remove sync schedule
    Remove,
    /// Show schedule status
    Status,
    /// View sync logs
    Logs(ScheduleLogsArgs),
}

#[derive(Parser)]
pub struct ScheduleInstallArgs {
    /// Daily sync time (HH:MM)
    #[arg(long)]
    pub daily: Option<String>,

    /// Sync interval in hours
    #[arg(long)]
    pub every: Option<u32>,
}

#[derive(Parser)]
pub struct ScheduleLogsArgs {
    /// Number of log lines to show
    #[arg(short = 'n', long, default_value_t = 50)]
    pub lines: usize,
}
