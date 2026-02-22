# Warehouse

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**The safe way to give AI access to your personal data.**

Warehouse consolidates your personal data — messages, contacts, photos, notes, documents, finances — into a single searchable SQLite database with governed access. Default-deny permissions, field-level redaction, and a full audit trail let you give AI agents the personal context they need without handing over the keys to everything.

## Why

AI agents like OpenClaw and NanoClaw are incredible. They can manage your messages, calendar, and files autonomously. But giving an agent raw access to your personal data is risky — and the industry is still figuring out how to do it safely. As agent capabilities grow, so does the need for a governed layer between your data and the tools that access it.

Warehouse is the governed data layer between your personal data and any AI agent:

- **You** decide which sources, fields, and time ranges are accessible
- **The agent** gets filtered, redacted results — it never knows what was withheld
- **The audit trail** logs every query, every blocked access, every redacted field

Works with OpenClaw, NanoClaw, Claude Code, or any agent that can execute shell commands.

## Data Sources

### Built-in (native Rust extractors)

| Source | What's extracted |
|--------|-----------------|
| **iMessages** | Full conversation history with sender/contact resolution |
| **Contacts** | Phones, emails, addresses, social profiles |
| **Photos** | Apple Photos library — assets, faces, people, locations |
| **Notes** | Obsidian vaults with frontmatter, tags, and link extraction |
| **Documents** | PDF, DOCX, XLSX, PPTX full-text extraction |
| **Reminders** | Lists, due dates, priorities, completion status |

### Popular connectors ([gallery](https://github.com/paulmeller/warehouse-connectors))

Install API connectors from the community gallery — or `warehouse setup` will offer them interactively.

| Source | What's extracted | Install |
|--------|-----------------|---------|
| **PocketSmith** | Accounts, categories, transactions | `warehouse connector add https://raw.githubusercontent.com/paulmeller/warehouse-connectors/main/connectors/pocketsmith.json` |
| **Monarch Money** | Accounts, transactions, recurring, budgets | `warehouse connector add https://raw.githubusercontent.com/paulmeller/warehouse-connectors/main/connectors/monarch.json` |
| **Twitter/X** | Bookmarks and likes via GraphQL | `warehouse connector add https://raw.githubusercontent.com/paulmeller/warehouse-connectors/main/connectors/twitter.json` |
| **Notion** | Pages and databases via REST API | `warehouse connector add https://raw.githubusercontent.com/paulmeller/warehouse-connectors/main/connectors/notion.json` |
| **GitHub** | Starred repos | `warehouse connector add https://raw.githubusercontent.com/paulmeller/warehouse-connectors/main/connectors/github.json` |
| **Hacker News** | Top stories (no auth) | `warehouse connector add https://raw.githubusercontent.com/paulmeller/warehouse-connectors/main/connectors/hackernews.json` |

### Add your own

Drop a JSON spec into `~/.warehouse/connectors/` to connect any REST or GraphQL API. No code required — define auth, endpoints, table schema, field mappings, pagination, and FTS config in a single file. See the [connector authoring guide](https://github.com/paulmeller/warehouse-connectors/blob/main/AUTHORING.md). Install from a URL:

```bash
warehouse connector add https://example.com/my-connector.json
```

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
# First-time setup: sync all sources + build search indexes
warehouse setup

# Or step by step:
warehouse sync                   # extract data from all sources
warehouse index                  # build FTS5 search indexes
warehouse search "meeting notes" # search across everything
```

## Commands

### Search & browse

```bash
# Full-text search with BM25 ranked results
warehouse search "quarterly review" --type notes,documents
warehouse search "dinner" --from 2025-01-01 --format json

# Browse specific data types
warehouse messages --contact "Sarah" --from 2025-06-01
warehouse contacts --search "Smith" --has-email
warehouse notes --tag "project" --vault "Work"
warehouse documents --type pdf
warehouse reminders --overdue
warehouse photos "John" --from 2025-01-01

# Person-centric view — everything about one person
warehouse person "John"

# Timeline — recent activity across all sources
warehouse timeline --week
warehouse recent
```

### Sync & connectors

```bash
# Sync all sources (incremental by default)
warehouse sync

# Sync specific sources
warehouse sync contacts imessages notion

# Force full re-sync
warehouse sync --full

# View sync history
warehouse sync --history

# Manage connectors
warehouse connector list
warehouse connector add https://example.com/connector.json
warehouse connector info pocketsmith
```

### Governance

```bash
# Interactive permission setup (default-deny)
warehouse permissions setup

# Manage per-source access
warehouse permissions show
warehouse permissions enable contacts
warehouse permissions disable photos
warehouse permissions set contacts --fields name,email
warehouse permissions set messages --max-age 90

# View audit trail
warehouse audit --week
warehouse audit --source messages --blocked
```

### System

```bash
warehouse status    # database counts
warehouse doctor    # check data sources and requirements
warehouse config show
warehouse config sources
```

## Governance Model

| Layer | What it controls | Example |
|-------|-----------------|---------|
| **Source-level** | Block entire data sources | Photos: off |
| **Field-level** | Whitelist specific fields | Contacts: name, email only |
| **Time-based** | Restrict to recent data | Messages: last 90 days |
| **Audit trail** | Log every query | Who searched what, when, what was redacted |
| **Default deny** | Fail-safe baseline | Nothing accessible until explicitly enabled |

Permissions are stored in `~/.warehouse/permissions.toml`. The audit trail lives in a separate `audit.db` — isolated from the main database so it can't be tampered with.

## Sync Architecture

Warehouse operates as a local ELT pipeline:

- **Incremental by default** — subsequent syncs only fetch new/changed data
- **Run history** — every sync is tracked in `sync_runs` with status, row counts, and timing (`warehouse sync --history`)
- **Resumable backfill** — if a paginated API sync fails mid-way, the next run resumes from where it left off
- **Schema migration** — when a connector spec adds new columns, existing tables are automatically migrated via `ALTER TABLE`
- **Soft delete detection** — for full-result-set endpoints, rows that disappear from the API are marked with `_deleted_at` rather than being lost

## Requirements

- **macOS** — built-in extractors use macOS-specific databases (the governance/search layer is portable)
- **Full Disk Access** — required for iMessages, Contacts, and Reminders extraction (System Settings > Privacy & Security > Full Disk Access)
- API connectors require their respective API keys/tokens configured in `~/.warehouse/config.toml`

## Configuration

Configuration lives at `~/.warehouse/config.toml`. Run `warehouse config init` to generate a default config with auto-discovered paths for your data sources.

```bash
warehouse config show      # display current config
warehouse config sources   # show discovered data sources
warehouse doctor           # full system check
```

## How It Works

```
macOS apps ──┐
Cloud APIs ──┤  warehouse sync  ──▶  SQLite DB  ──▶  warehouse search
Local files ─┘    (extract)          (normalize)     (govern + query)
                                         │
                                   warehouse index
                                    (FTS5 indexes)
```

All data is stored as text/metadata in SQLite — no binary files or blobs. Original files stay where they are. The database is a single portable file at `~/.warehouse/warehouse.db`.

## License

MIT — see [LICENSE](LICENSE) for details.
