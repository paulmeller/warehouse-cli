# Warehouse

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A fast CLI tool that consolidates your personal data into a single searchable database. Warehouse extracts data from macOS apps and local files, indexes everything with SQLite FTS5 full-text search, and lets you search and browse across all your data from the terminal.

## Supported Data Sources

- **iMessages** — full conversation history with contact resolution
- **Contacts** — phones, emails, addresses, social profiles
- **Photos** — Apple Photos library (assets, faces, people, locations)
- **Notes** — Obsidian vaults with frontmatter, tags, and link extraction
- **Documents** — PDF, DOCX, XLSX, PPTX text extraction
- **Reminders** — lists, due dates, priorities

## Installation

### Homebrew (recommended)

```bash
brew tap paulmeller/tap
brew install warehouse
```

### From release

Download the latest binary from [Releases](https://github.com/paulmeller/warehouse-cli/releases):

```bash
# Apple Silicon (M1/M2/M3/M4)
curl -L https://github.com/paulmeller/warehouse-cli/releases/latest/download/warehouse-macos-arm64.tar.gz | tar xz
sudo mv warehouse /usr/local/bin/

# Intel Mac
curl -L https://github.com/paulmeller/warehouse-cli/releases/latest/download/warehouse-macos-x86_64.tar.gz | tar xz
sudo mv warehouse /usr/local/bin/
```

### From source

```bash
cargo install --path .
```

## Quick Start

```bash
# Initialize configuration
warehouse init

# Sync your data sources
warehouse sync

# Search across everything
warehouse search "meeting notes"
```

## Key Commands

```bash
# Full-text search with ranked results
warehouse search "query"

# Browse specific data types
warehouse messages --contact "Sarah"
warehouse contacts --search "Smith"
warehouse notes --tag "project"
warehouse documents --type pdf
warehouse reminders

# Person-centric view — everything about one person
warehouse person "John"

# Timeline — recent activity across all sources
warehouse timeline --week
```

## Requirements

- **macOS** (data source extraction relies on macOS-specific databases)
- **Full Disk Access** permission is required for iMessages, Contacts, and Reminders extraction (System Settings > Privacy & Security > Full Disk Access)

## Configuration

Warehouse stores its configuration at `~/.warehouse/config.toml`. Run `warehouse init` to generate a default config with auto-discovered paths for your data sources.

## License

MIT — see [LICENSE](LICENSE) for details.
