use anyhow::Result;
use rusqlite::Connection;

use crate::config::Config;
use crate::search;
#[cfg(target_os = "macos")]
use crate::sync::{contacts, messages, photos, reminders};
use crate::sync::{documents, notes, SyncContext};

/// Trait that all data source connectors must implement.
///
/// Both built-in connectors (contacts, messages, etc.) and dynamic JSON
/// connectors implement this same interface.
pub trait Connector: Send + Sync {
    /// Unique identifier for this connector (e.g., "contacts", "imessages").
    fn name(&self) -> &str;

    /// Human-readable description shown in `warehouse connector list`.
    fn description(&self) -> &str;

    /// Source tag: "built-in" for compiled connectors, "installed" for dynamic JSON connectors.
    fn source(&self) -> &str {
        "built-in"
    }

    /// Create source tables in the warehouse database.
    fn create_source_tables(&self, conn: &Connection) -> Result<()>;

    /// Extract data from the source into the warehouse database.
    /// Returns the number of primary items extracted.
    fn extract(&self, conn: &Connection, config: &Config, ctx: &SyncContext) -> Result<usize>;

    /// SQL to create FTS5 virtual table(s) and mapping table(s).
    /// Return None if this connector doesn't support full-text search.
    fn fts_schema_sql(&self) -> Option<&str>;

    /// Populate FTS index from source tables.
    /// Returns the number of items indexed.
    fn populate_fts(&self, conn: &Connection) -> Result<i64>;

    /// Governance source name for permissions.
    /// Override when connector name differs from governance source
    /// (e.g., "imessages" -> "messages", "obsidian" -> "notes").
    fn governance_source(&self) -> &str {
        self.name()
    }

    /// Human-readable description for permissions UI.
    fn governance_description(&self) -> &str {
        self.description()
    }

    /// Fields exposed by this source (for permissions UI).
    fn governance_fields(&self) -> &[&str] {
        &[]
    }

    /// Search types this connector handles (for --type flag).
    /// Returns list of (search_type, governance_source) pairs.
    fn search_types(&self) -> Vec<(&str, &str)> {
        vec![]
    }

    /// Execute FTS search. Returns empty vec if not handled.
    fn search_fts(
        &self,
        _conn: &Connection,
        _search_type: &str,
        _query: &str,
        _options: &search::SearchOptions,
    ) -> Result<Vec<search::SearchResult>> {
        Ok(vec![])
    }
}

/// Registry of all available connectors.
pub struct ConnectorRegistry {
    connectors: Vec<Box<dyn Connector>>,
}

impl ConnectorRegistry {
    pub fn new() -> Self {
        Self {
            connectors: Vec::new(),
        }
    }

    /// Register a connector. Connectors are synced/indexed in registration order.
    pub fn register(&mut self, connector: Box<dyn Connector>) {
        self.connectors.push(connector);
    }

    /// Get a connector by name.
    pub fn get(&self, name: &str) -> Option<&dyn Connector> {
        self.connectors
            .iter()
            .find(|c| c.name() == name)
            .map(|c| c.as_ref())
    }

    /// All registered connectors.
    pub fn all(&self) -> &[Box<dyn Connector>] {
        &self.connectors
    }

    /// Names of all registered connectors.
    pub fn names(&self) -> Vec<&str> {
        self.connectors.iter().map(|c| c.name()).collect()
    }

    /// All governance source names (deduplicated, ordered).
    pub fn all_sources(&self) -> Vec<&str> {
        let mut sources: Vec<&str> = Vec::new();
        for c in &self.connectors {
            let src = c.governance_source();
            if !sources.contains(&src) {
                sources.push(src);
            }
        }
        sources
    }

    /// All search type names (deduplicated).
    pub fn all_search_types(&self) -> Vec<&str> {
        let mut types: Vec<&str> = Vec::new();
        for c in &self.connectors {
            for (st, _) in c.search_types() {
                if !types.contains(&st) {
                    types.push(st);
                }
            }
        }
        types
    }

    /// Map a search type to its governance source name.
    pub fn search_type_to_source(&self, search_type: &str) -> Option<&str> {
        for c in &self.connectors {
            for (st, src) in c.search_types() {
                if st == search_type {
                    return Some(src);
                }
            }
        }
        None
    }

    /// Get connectors that handle a given search type.
    pub fn connectors_for_search_type(&self, search_type: &str) -> Vec<&dyn Connector> {
        self.connectors
            .iter()
            .filter(|c| c.search_types().iter().any(|(st, _)| *st == search_type))
            .map(|c| c.as_ref())
            .collect()
    }

    /// Get governance fields for a source.
    pub fn source_fields(&self, source: &str) -> Vec<&str> {
        for c in &self.connectors {
            if c.governance_source() == source {
                let fields = c.governance_fields();
                if !fields.is_empty() {
                    return fields.to_vec();
                }
            }
        }
        vec![]
    }

    /// Get description for a source.
    pub fn source_description(&self, source: &str) -> &str {
        for c in &self.connectors {
            if c.governance_source() == source {
                return c.governance_description();
            }
        }
        "Unknown data source."
    }
}

/// Build the default registry with all built-in connectors plus dynamic connectors.
pub fn default_registry() -> ConnectorRegistry {
    let mut registry = ConnectorRegistry::new();

    // Built-in connectors: macOS-only data sources
    #[cfg(target_os = "macos")]
    {
        registry.register(Box::new(contacts::ContactsConnector));
        registry.register(Box::new(messages::MessagesConnector));
        registry.register(Box::new(photos::PhotosConnector));
        registry.register(Box::new(reminders::RemindersConnector));
    }

    // Built-in connectors: cross-platform
    registry.register(Box::new(notes::NotesConnector));
    registry.register(Box::new(documents::DocumentsConnector));

    // User-installed connectors from ~/.warehouse/connectors/*.json
    for connector in crate::dynamic_connector::load_dynamic_connectors() {
        registry.register(connector);
    }

    registry
}
