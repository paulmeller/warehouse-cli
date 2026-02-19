use anyhow::Result;
use rusqlite::Connection;

use crate::config::Config;
use crate::sync::{contacts, documents, messages, notes, photos, reminders};

/// Trait that all data source connectors must implement.
///
/// Both built-in connectors (contacts, messages, etc.) and future external
/// plugins implement this same interface. There is no distinction between
/// "built-in" and "plugin" — they are all connectors.
pub trait Connector: Send + Sync {
    /// Unique identifier for this connector (e.g., "contacts", "imessages").
    fn name(&self) -> &str;

    /// Human-readable description shown in `warehouse connectors`.
    fn description(&self) -> &str;

    /// Create source tables in the warehouse database.
    fn create_source_tables(&self, conn: &Connection) -> Result<()>;

    /// Extract data from the source into the warehouse database.
    /// Returns the number of primary items extracted.
    fn extract(&self, conn: &Connection, config: &Config) -> Result<usize>;

    /// SQL to create FTS5 virtual table(s) and mapping table(s).
    /// Return None if this connector doesn't support full-text search.
    fn fts_schema_sql(&self) -> Option<&str>;

    /// Populate FTS index from source tables.
    /// Returns the number of items indexed.
    fn populate_fts(&self, conn: &Connection) -> Result<i64>;
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
}

/// Build the default registry with all built-in connectors.
pub fn default_registry() -> ConnectorRegistry {
    let mut registry = ConnectorRegistry::new();
    registry.register(Box::new(contacts::ContactsConnector));
    registry.register(Box::new(messages::MessagesConnector));
    registry.register(Box::new(photos::PhotosConnector));
    registry.register(Box::new(reminders::RemindersConnector));
    registry.register(Box::new(notes::NotesConnector));
    registry.register(Box::new(documents::DocumentsConnector));
    registry
}
