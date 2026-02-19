# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run Commands

```bash
cargo build                  # debug build
cargo build --release        # optimized build (LTO, stripped)
cargo run -- <subcommand>    # run in dev
cargo test                   # run all tests
cargo test <test_name>       # run a single test
cargo clippy                 # lint
cargo fmt                    # format
```

## What This Is

Warehouse is a macOS-focused CLI tool that consolidates personal data (iMessages, contacts, photos, Obsidian notes, documents, reminders, Twitter, financial data) into a single SQLite database with FTS5 full-text search. The primary workflow is: `sync` (extract from sources) → `index` (build FTS5 indexes) → `search`/`browse`.

## Architecture

**Entry flow:** `main.rs` parses CLI via clap (`cli.rs`), opens the SQLite DB (`db.rs`), and dispatches to one of ~18 subcommands.

**Core modules:**
- `db.rs` — SQLite connection setup, FTS5 schema creation, mapping tables between FTS virtual tables and source tables
- `config.rs` — TOML config at `~/.warehouse/config.toml`, platform-aware auto-discovery of data sources
- `search.rs` — FTS5 query execution with BM25 scoring, query escaping, result diversity enforcement (prevents one content type from dominating), multiple output formats (text/JSON/CSV/markdown)
- `fts.rs` — Rebuilds all 9 FTS indexes from source tables, manages FTS↔source rowid mappings
- `browse.rs` — Direct SQL browsing (not FTS) with filtering, person-centric views, timelines
- `schedule.rs` — macOS LaunchAgent plist generation for background sync

**Sync subsystem (`sync/`):** Pluggable extractor architecture. Each data source has its own module:
- `messages.rs` — iMessage `chat.db`, resolves senders against contacts
- `contacts.rs` — macOS AddressBook (phones, emails, addresses, social profiles)
- `notes.rs` — Obsidian vault discovery, frontmatter/tags/links extraction
- `photos.rs` — Apple Photos library (assets, faces, people, locations)
- `reminders.rs` — Apple Reminders (lists, due dates, priorities)
- `documents.rs` — PDF/DOCX/XLSX/PPTX text extraction with pluggable backends (lightweight, markitdown, docling)
- `twitter.rs` — Bookmarks/likes via browser cookies, incremental sync
- `monarch.rs` — Monarch Money financial data via browser automation
- `pocketsmith.rs` — PocketSmith financial data via REST API

**Database design:** Source tables (e.g. `imessage_messages`) are populated by sync. FTS5 virtual tables (e.g. `messages_fts`) plus mapping tables (`messages_fts_map`) decouple search indexes from source data. A `search_metadata` table tracks index timestamps.

## Key Conventions

- Error handling uses `anyhow::Result` throughout
- SQLite is bundled via rusqlite's `bundled` feature (no system SQLite dependency)
- Date handling includes Apple epoch conversion (2001 base year) for macOS databases
- FTS5 queries are escaped to remove special characters (`*:^()~'`)
- Sync gracefully skips unavailable sources rather than failing
- macOS Full Disk Access permission is required for iMessages, Contacts, and Reminders extraction

## CI

Release workflow (`.github/workflows/release.yml`) triggers on `v*` tags, builds for macOS arm64 and x86_64, creates GitHub release with tarballs and SHA256 checksums.
