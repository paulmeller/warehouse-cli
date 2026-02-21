# Warehouse — Product Brief (DRAFT v2)

> Working document for website, README, HN/PH/Discord launch copy.
> Not shipped as-is — this is the source of truth for messaging.

---

## One-liner

The safe way to give AI access to your personal data.

## The problem

AI agents need personal context to be useful. But giving an agent access
to your messages, contacts, notes, photos, and documents is terrifying —
and it should be.

Today you have two options:

1. **Give your agent raw access to everything.** Most setups just hand the
   agent a database path or a file system mount. No controls, no audit
   trail, no way to know what it accessed or what it did with it.

2. **Give your agent nothing.** Keep it generic. It can't help with
   anything personal. Safe but useless.

Warehouse is the third option: **managed, governed access to your personal
data.** You decide exactly what your agent can see, at what granularity,
and you get a full audit trail of every query it makes.

## What Warehouse does

### 1. Extract & normalize

Warehouse pulls data out of macOS apps and normalizes it into a single
SQLite database:

- **iMessages** — full history with sender/contact resolution
- **Contacts** — phones, emails, addresses, social profiles
- **Photos** — Apple Photos library (assets, faces, people, locations)
- **Notes** — Obsidian vaults with frontmatter, tags, and links
- **Documents** — PDF, DOCX, XLSX, PPTX full-text extraction
- **Reminders** — lists, due dates, priorities

### 2. Govern access

Before anything can read your data, you configure permissions:

```bash
warehouse permissions setup    # interactive onboarding
```

For each data source, you choose:
- **Full access** — the agent sees everything
- **Restricted** — field-level control (e.g. contacts: name and email
  only, no phone or address) and time-based limits (e.g. messages from
  the last 90 days only)
- **Blocked** — the agent can't see this source at all

**Default is deny-all.** Nothing is accessible until you explicitly allow
it. This is the opposite of how most agent setups work.

### 3. Search with redaction

When your agent (or you) searches, governance rules are enforced
automatically:

```bash
warehouse search "dinner plans"
```

- Blocked sources are excluded from results
- Restricted fields are redacted before results are returned
- Time-limited sources only return data within the allowed window
- Every query is logged to a separate audit database

### 4. Audit everything

```bash
warehouse audit --week           # what was accessed in the last 7 days
warehouse audit --source messages  # just message queries
warehouse audit --blocked        # show denied access attempts
```

The audit trail records: what was queried, which sources were searched,
which were blocked, how many records were returned, and which fields
were redacted. Stored in a separate `audit.db` so it can't be tampered
with through the main database.

## The governance model

| Layer | What it controls | Example |
|-------|-----------------|---------|
| Source-level | Block entire data sources | Photos: off |
| Field-level | Whitelist specific fields | Contacts: name, email only |
| Time-based | Restrict to recent data | Messages: last 90 days |
| Audit trail | Log every query | Who searched what, when, what was redacted |
| Default deny | Fail-safe baseline | Nothing accessible until explicitly enabled |

This is the same kind of layered access control you'd expect from an
enterprise data platform, applied to your personal data on a single
machine.

## How agents use Warehouse

Agents interact with Warehouse through two interfaces:

1. **CLI** — the agent calls `warehouse search`, `warehouse messages`,
   etc. as shell commands. Works with any agent that can execute commands.

2. **Agent skill** — a skill definition (to be created) that wraps the
   CLI into a structured tool the agent can call natively. The skill
   describes the available commands, parameters, and output formats so
   the agent doesn't need to guess at CLI syntax.

Both interfaces go through the same governance layer. The agent never
touches the SQLite database directly — every query is mediated by
Warehouse's permission and redaction logic.

**What the agent experiences:**

- It calls `warehouse search "dinner plans"` and gets back filtered,
  redacted results — it never knows what was withheld
- Blocked sources simply don't appear in results
- Restricted fields are removed before the agent sees them
- The agent doesn't need to know about the governance layer at all

**What you experience:**

- `warehouse audit --week` shows you exactly what the agent queried
- You can tighten or loosen access at any time without reconfiguring
  the agent
- You can see every blocked access attempt and every redacted field

This works with any agent framework — OpenClaw, NanoClaw, Claude Code,
custom setups. No lock-in. If it can shell out or use a skill, it works.

## Who this is for

**Primary:** People setting up personal AI agents (Claws, Claude, custom
setups) who want their agent to have personal context but aren't
comfortable giving it raw, ungoverned access to their data.

**Secondary:** Privacy-conscious power users who want fast, unified search
across their macOS data with explicit control over what's queryable.

## Key differentiators

1. **Default-deny governance** — the only personal data tool that starts
   from "block everything" and requires explicit opt-in per source, per
   field, per time range
2. **Full audit trail** — separate audit database logs every query, every
   blocked access, every redacted field. You always know what was accessed.
3. **Field-level redaction** — not just "can the agent see contacts" but
   "can the agent see contact *phone numbers*." Granular enough to be
   actually useful for privacy.
4. **Local-only, single SQLite file** — no server, no cloud, no API keys.
   Your data stays on your machine in a file you own.
5. **Small and auditable codebase** — Rust, ~3K LOC, zero runtime
   dependencies beyond the binary. If you're trusting it with your
   personal data, you should be able to read the code. You can.
6. **Pluggable connector architecture** — trait-based plugin system. Each
   data source is a connector that implements a standard interface. Add
   new sources without touching the core.

## What Warehouse is NOT

- Not a Claw / agent — no agent loop, no LLM calls, no autonomous actions
- Not a RAG pipeline — no embeddings, no vector search. FTS5 keyword
  search. Simple, fast, predictable.
- Not cross-platform — macOS data sources only (the extractors are
  macOS-specific; the governance/search layer is portable)
- Not a sync service — read-only extraction, no write-back to sources

## Competitive / adjacent landscape

| Tool | How Warehouse differs |
|------|----------------------|
| Raw SQLite access | No governance, no audit, no redaction |
| Apple Spotlight | No API, no structured access, no agent integration |
| Rewind.ai / Recall | Cloud-dependent, screen recording, no field-level control |
| Khoj | Self-hosted AI search — bigger, more opinionated, no governance layer |
| MCP servers | Usually all-or-nothing access, no field redaction or audit |

## Launch channels & tone

**README / Website:** Lead with the trust/governance story. "The safe way
to give AI access to your personal data." Show the permission model and
audit trail prominently. Technical but accessible — this isn't just for
hardcore developers, it's for anyone who's nervous about AI + personal data.

**HackerNews:** "Show HN" — lead with the problem. "I wanted my AI agent
to know about my messages and contacts, but I wasn't comfortable giving
it raw database access. So I built a governed data layer with default-deny
permissions and a full audit trail." HN will appreciate: small Rust
codebase, local-first, SQLite, no cloud dependencies, principled security
model.

**Product Hunt:** "Your AI assistant needs your data. But do you trust it?
Warehouse gives AI agents governed, audited access to your personal data —
with controls you'd expect from enterprise software, running on your own
machine."

**OpenClaw Discord / Claw communities:** Position as the missing safety
layer. "Before you give your Claw access to your iMessages, consider what
'access' means. Warehouse lets you control exactly which sources, which
fields, and which time ranges your Claw can see — and logs every query it
makes." Include practical setup: install, permissions setup, add the agent
skill, done.

---

## Open questions

- [ ] Agent skill definition — what commands/parameters does the skill
      expose? Likely: search, messages, contacts, notes, person, timeline.
      Needs to feel natural to the agent without leaking implementation.
- [ ] Demo strategy — what's the most compelling 30-second demo? Probably:
      permissions setup → agent query → audit showing what was accessed and
      what was redacted.
- [ ] Should we position this as "for Claws" specifically or "for AI
      agents" broadly? Claw-specific is more timely, agent-generic is more
      durable.
- [ ] Config-as-code — should permission configs be shareable / version-
      controllable? "Here's my recommended permission profile for a
      personal assistant Claw."
