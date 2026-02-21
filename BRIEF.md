# Warehouse — Product Brief (DRAFT)

> Working document for website, README, HN/PH/Discord launch copy.
> Not shipped as-is — this is the source of truth for messaging.

---

## One-liner

Warehouse gives your Claw a memory — sync your macOS personal data into a
single searchable SQLite database.

## The problem

Claws are powerful but context-starved. Out of the box, a Claw on your Mac
mini knows nothing about you — your messages, contacts, notes, photos,
documents, reminders are all locked in separate macOS databases and app
silos. Without personal context, your Claw is just a generic agent.

Meanwhile, getting data *out* of macOS apps is annoyingly hard. iMessage
uses an undocumented SQLite schema with Apple-epoch timestamps. Contacts
live in an AddressBook database with a non-obvious structure. Photos
metadata is spread across multiple tables. Every source has its own format,
quirks, and permission requirements.

## What Warehouse does

Warehouse extracts, normalizes, and indexes your personal data:

- **iMessages** — full history with sender/contact resolution
- **Contacts** — phones, emails, addresses, social profiles
- **Photos** — Apple Photos library (assets, faces, people, locations)
- **Notes** — Obsidian vaults with frontmatter, tags, and links
- **Documents** — PDF, DOCX, XLSX, PPTX full-text extraction
- **Reminders** — lists, due dates, priorities

Everything lands in a single SQLite database with FTS5 full-text search
indexes. BM25-ranked results. Query across all sources or filter by type.
JSON, CSV, markdown, or plain text output.

```
warehouse sync      # extract from all sources
warehouse index     # build FTS5 search indexes
warehouse search "dinner plans last week"
```

## Why now — the Claw angle

Claws are the new orchestration layer on top of AI agents. They handle
scheduling, tool calls, persistence, and context management. But they need
something to orchestrate *over*.

Warehouse is the **data layer** for your personal Claw:

- **Structured retrieval** — your Claw calls `warehouse search` as a tool
  to answer questions grounded in your actual data
- **Always fresh** — `warehouse sync` on a schedule (LaunchAgent support
  built in) keeps the database current
- **Local-first** — everything stays on your machine, in a SQLite file you
  own. No cloud sync, no API keys for data access, no trust assumptions.
- **Claw-agnostic** — works with OpenClaw, NanoClaw, or any agent framework
  that can shell out or read SQLite. Not locked to any orchestrator.
- **Auditable** — small Rust codebase (~3K LOC). You or your AI agent can
  read the entire thing.

The combination is: Warehouse gives the Claw eyes into your data. The Claw
gives Warehouse a brain and a schedule.

## Who this is for

**Primary:** Developers and tinkerers setting up a personal Claw (OpenClaw,
NanoClaw, etc.) on a Mac mini or similar always-on Mac. They want their
agent to have personal context without handing their data to a cloud
service.

**Secondary:** Power users who want fast, unified search across their macOS
data from the terminal, independent of any Claw setup.

## Key differentiators

1. **macOS-native extraction** — handles the hard, undocumented parts
   (Apple epoch conversion, AddressBook schema, Photos library structure)
2. **Single SQLite file** — no server, no daemon, no Docker. One file that
   any tool can read.
3. **FTS5 with BM25** — real ranked search, not just string matching.
   Result diversity enforcement prevents one content type from dominating.
4. **Small and auditable** — Rust, ~3K LOC, zero runtime dependencies
   beyond the binary. If you're worried about supply chain risk with your
   Claw (you should be), the data layer should be something you can
   actually verify.
5. **Pluggable document extraction** — lightweight built-in, or swap in
   markitdown/docling for heavier formats.

## What Warehouse is NOT

- Not a Claw itself — no agent loop, no LLM calls, no scheduling logic
- Not cross-platform — macOS data sources only (Linux/Windows would need
  different extractors for different apps)
- Not a sync service — one-directional extract, no write-back to sources
- Not a RAG pipeline — no embeddings, no vector search. FTS5 keyword
  search. Embeddings could be added later but the simple thing works
  surprisingly well.

## Competitive / adjacent landscape

| Tool | Relationship |
|------|-------------|
| OpenClaw | Claw that Warehouse can plug into as a data source/tool |
| NanoClaw | Same — lighter weight, containerized, arguably better fit |
| Apple Spotlight | Searches but can't export, no API, no structured access |
| Rewind.ai / Recall | Cloud-dependent screen recording, different approach |
| Khoj | Self-hosted AI search — has its own extraction. More opinionated, bigger. |
| ripgrep/fzf | Text search tools, not personal data extraction |

## Launch channels & tone

**README / Website:** Technical, concise, show-don't-tell. Lead with the
Claw use case, keep the standalone CLI use case as secondary. Code
examples. Architecture diagram showing Warehouse as the data layer.

**HackerNews:** "Show HN" format. Lead with the personal insight — "I was
setting up a Claw on my Mac mini and realized it had no access to any of
my personal data." Emphasize small codebase, local-first, SQLite
simplicity. HN loves small focused tools that do one thing well.

**Product Hunt:** More accessible framing. "Give your AI assistant a
memory." Focus on the outcome (ask your Claw about your messages, notes,
photos) rather than the implementation.

**OpenClaw Discord / Claw communities:** Position as infrastructure.
"If you're running a Claw on macOS, here's the missing data layer."
Practical integration guide. Show how to wire warehouse as a tool.

---

## Open questions

- [ ] Should we ship a `warehouse serve` HTTP endpoint for Claws that
      prefer HTTP over shell exec? (Simple read-only JSON API over the
      SQLite DB)
- [ ] MCP (Model Context Protocol) server — would let Claws that support
      MCP discover warehouse as a tool automatically. Probably worth doing.
- [ ] Should the README lead with the Claw narrative or keep it balanced
      with standalone use? Risk of dating the copy if "Claw" hype fades.
- [ ] Demo video / GIF — what's the most compelling 30-second demo?
      Probably: sync → search → Claw asking a personal question and getting
      a grounded answer.
- [ ] Pricing / monetization — currently MIT/free. Stay that way? Or is
      there a paid tier (managed sync, more sources, cloud backup)?
