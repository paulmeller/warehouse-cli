# Connector Format Comparison: Warehouse vs Airbyte vs Fivetran

This document compares Warehouse's JSON connector spec format with the connector protocols used by Airbyte and Fivetran.

## Overview

| Aspect | Warehouse | Airbyte | Fivetran |
|--------|-----------|---------|----------|
| **Connector definition** | Declarative JSON spec file | Imperative code in Docker container | Imperative Python code |
| **Runtime** | Single Rust binary parses JSON | Docker container per connector | Python process via gRPC |
| **Data transport** | Direct SQLite writes | Line-delimited JSON on STDOUT | gRPC stream |
| **Schema declaration** | In-spec `tables[].columns` | JSONSchema via `discover` command | `schema()` function return value |
| **Incremental sync** | Pagination early-stop + soft delete | Cursor-based STATE messages | State dict with cursors + checkpoints |
| **Target** | Local SQLite + FTS5 | Any destination connector | Fivetran-managed warehouse |

## Architecture Comparison

### Warehouse: Declarative JSON Spec

Warehouse connectors are **purely declarative**. A single JSON file describes everything the runtime needs to extract data from an API:

```json
{
  "version": 1,
  "name": "github_stars",
  "description": "GitHub starred repositories",
  "api_type": "rest",
  "auth": {
    "type": "env",
    "value_template": "{{env.GITHUB_TOKEN}}",
    "header_name": "Authorization"
  },
  "tables": [{
    "name": "github_stars",
    "columns": [
      {"name": "id", "type": "INTEGER", "primary_key": true},
      {"name": "full_name", "type": "TEXT"},
      {"name": "description", "type": "TEXT"},
      {"name": "language", "type": "TEXT"},
      {"name": "stargazers_count", "type": "INTEGER"}
    ],
    "endpoint": {
      "url": "https://api.github.com/user/starred",
      "method": "GET",
      "pagination": {"type": "page_number", "page_size": 100, "max_pages": 50}
    },
    "response": {
      "results_path": "$",
      "field_mappings": [
        {"column": "id", "path": "id"},
        {"column": "full_name", "path": "full_name"},
        {"column": "description", "path": "description"},
        {"column": "language", "path": "language"},
        {"column": "stargazers_count", "path": "stargazers_count"}
      ]
    }
  }],
  "fts": [{
    "table_name": "github_stars_fts",
    "source_table": "github_stars",
    "columns": ["full_name", "description"],
    "tokenizer": "porter unicode61",
    "search_type": "github_stars",
    "title_column": "full_name"
  }]
}
```

The Rust runtime handles all HTTP execution, pagination, authentication, response parsing, and SQLite insertion. No user code runs.

### Airbyte: Docker Container Protocol

Airbyte connectors are **Docker images** that implement the Airbyte Protocol — a line-delimited JSON message protocol on STDOUT/STDIN. Connectors are full programs (typically Python, Java, or any language) that must implement four commands:

```bash
# 1. Advertise configuration schema
connector spec
# Output: {"type": "SPEC", "spec": {"connectionSpecification": {<JSONSchema>}}}

# 2. Validate credentials
connector check --config config.json
# Output: {"type": "CONNECTION_STATUS", "connectionStatus": {"status": "SUCCEEDED"}}

# 3. Discover available streams/tables
connector discover --config config.json
# Output: {"type": "CATALOG", "catalog": {"streams": [...]}}

# 4. Extract data
connector read --config config.json --catalog configured_catalog.json --state state.json
# Output: one JSON message per line:
#   {"type": "RECORD", "record": {"stream": "users", "data": {"id": 1, "name": "Alice"}, "emitted_at": 1623861660}}
#   {"type": "STATE", "state": {"type": "STREAM", "stream": {"stream_descriptor": {"name": "users"}, "stream_state": {"cursor": "2025-01-15"}}}}
#   {"type": "LOG", "log": {"level": "INFO", "message": "Synced 500 records"}}
```

Each message is a self-typed JSON envelope. The Airbyte platform orchestrates the source → destination pipeline.

### Fivetran: Python SDK with gRPC

Fivetran connectors are **Python functions** using the `fivetran_connector_sdk` package. They communicate with the Fivetran platform over gRPC:

```python
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log

def schema(configuration):
    return [
        {
            "table": "github_stars",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "full_name": "STRING",
                "description": "STRING",
                "language": "STRING",
                "stargazers_count": "INT"
            }
        }
    ]

def update(configuration, state):
    cursor = state.get("last_starred_at", "2000-01-01T00:00:00Z")
    for repo in fetch_starred_repos(since=cursor):
        op.upsert("github_stars", {
            "id": repo["id"],
            "full_name": repo["full_name"],
            "description": repo["description"],
            "language": repo["language"],
            "stargazers_count": repo["stargazers_count"]
        })
        cursor = max(cursor, repo["starred_at"])
    op.checkpoint({"last_starred_at": cursor})

connector = Connector(update=update, schema=schema)
```

The SDK handles gRPC serialization, checkpointing durability, and delivery to the Fivetran-managed destination warehouse.

## Detailed Comparison

### 1. Connector Authoring

| | Warehouse | Airbyte | Fivetran |
|---|-----------|---------|----------|
| **Language** | JSON (no code) | Any (Python, Java, Go, etc.) | Python only |
| **Skill required** | JSON editing + API knowledge | Full programming + Docker | Python programming |
| **Packaging** | `.json` file in `~/.warehouse/connectors/` | Docker image | Python module deployed to Fivetran |
| **Install** | `warehouse connector add <url>` | Docker pull via UI/API | `fivetran deploy` CLI |
| **LOC for simple connector** | ~40-60 lines of JSON | ~200-500 lines of code + Dockerfile | ~50-100 lines of Python |

Warehouse's declarative approach means no code to write or debug for standard REST/GraphQL APIs. The tradeoff is that custom extraction logic (complex transforms, multi-step auth flows, non-standard APIs) requires extending the spec format rather than writing arbitrary code.

### 2. Schema Declaration

**Warehouse** — Schema is embedded in the connector spec as `tables[].columns`:
```json
"columns": [
  {"name": "id", "type": "INTEGER", "primary_key": true},
  {"name": "title", "type": "TEXT"},
  {"name": "created_at", "type": "TEXT", "default": "CURRENT_TIMESTAMP"}
]
```
Types map directly to SQLite types: `TEXT`, `INTEGER`, `REAL`, `BLOB`. Schema is static — defined at connector authoring time. The runtime creates tables with `CREATE TABLE IF NOT EXISTS`.

**Airbyte** — Schema is discovered at runtime via the `discover` command, which returns a catalog using JSONSchema:
```json
{
  "streams": [{
    "name": "users",
    "json_schema": {
      "type": "object",
      "properties": {
        "id": {"type": "integer"},
        "name": {"type": "string"},
        "email": {"type": ["string", "null"]}
      }
    },
    "supported_sync_modes": ["full_refresh", "incremental"],
    "source_defined_cursor": true,
    "default_cursor_field": ["updated_at"]
  }]
}
```
The ConfiguredAirbyteCatalog then specifies which streams to sync and how. Schema is dynamic and can change between syncs.

**Fivetran** — Schema is declared in the `schema()` function:
```python
def schema(configuration):
    return [{
        "table": "users",
        "primary_key": ["id"],
        "columns": {"id": "INT", "name": "STRING", "email": "STRING"}
    }]
```
If primary keys are omitted, Fivetran adds a hidden `_fivetran_id` column.

### 3. Data Extraction & Transport

**Warehouse** — The runtime drives extraction entirely. The JSON spec declares endpoints, pagination rules, and field mappings. The runtime:
1. Resolves auth headers (env vars, config keys, browser cookies, token chains)
2. Makes HTTP requests with configured method/headers/body
3. Navigates the response JSON using dot-path expressions (e.g., `data.items[*].name`)
4. Applies transforms (`to_string`, `to_int`, `join_array`, `join_rich_text`)
5. Writes rows directly into SQLite via `INSERT OR REPLACE`

There is no intermediate message format — data goes straight from HTTP response to SQLite.

**Airbyte** — Connectors emit RECORD messages to STDOUT. Each record is a JSON envelope:
```json
{"type": "RECORD", "record": {"stream": "users", "data": {"id": 1, "name": "Alice"}, "emitted_at": 1623861660}}
```
The platform reads STDOUT and routes records to the destination connector, which writes them to the target system. The line-delimited JSON protocol means source and destination are fully decoupled.

**Fivetran** — Connectors call SDK operations that stream records over gRPC:
```python
op.upsert("users", {"id": 1, "name": "Alice"})
op.update("users", {"id": 1, "name": "Alice Updated"})
op.delete("users", {"id": 1})
```
Each call sends one row through the gRPC stream. The Fivetran platform handles writing to the destination.

### 4. Incremental Sync & State

**Warehouse** — Two mechanisms:
- **Pagination early-stop**: The `incremental.stop_date_path` field specifies a date path in results. When all items on a page are older than the last sync, pagination stops.
- **Soft delete**: When `soft_delete: true`, rows missing from a full sync are marked with `_deleted_at` timestamp rather than physically deleted.
- **Resume cursors**: Failed paginated syncs save cursor/page/rows-so-far as JSON in `sync_runs.resume_cursor`, allowing resume without re-fetching.

State is tracked internally in the `sync_runs` table — connector specs don't manage state explicitly.

**Airbyte** — Connectors emit STATE messages that the platform persists:
```json
{"type": "STATE", "state": {"type": "STREAM", "stream": {"stream_descriptor": {"name": "users"}, "stream_state": {"cursor_field": ["updated_at"], "cursor": "2025-01-15T00:00:00Z"}}}}
```
State types: `STREAM` (per-stream, preferred), `GLOBAL` (cross-stream), `LEGACY` (deprecated). The platform passes saved state back to the connector on the next sync via `--state state.json`.

**Fivetran** — Connectors manage state through the `state` dict parameter and `checkpoint()`:
```python
def update(configuration, state):
    cursor = state.get("last_updated", "2000-01-01")
    # ... fetch and upsert records ...
    op.checkpoint({"last_updated": new_cursor})
```
Checkpoints are atomic — Fivetran persists data and state together. Recommended cadence: every ~10k records or ~10 minutes. On failure, the next sync resumes from the last checkpoint with at-least-once delivery.

### 5. Response Parsing / Field Mapping

**Warehouse** — Uses a declarative dot-path mapping system:
```json
"field_mappings": [
  {"column": "author_name", "path": "post.author.display_name"},
  {"column": "tags", "path": "metadata.tags", "transform": "join_array"},
  {"column": "id", "path": "legacy_id", "alt_paths": ["item.id", "data.identifier"]}
]
```
Features: nested dot-path navigation, array wildcards (`[*]`), transforms, alternative fallback paths, response filtering. The runtime handles all JSON traversal.

**Airbyte** — Connectors are responsible for their own response parsing. The Low-Code CDK provides a YAML-based declarative layer with similar concepts (record selectors, field transformations), but most connectors use custom Python/Java code to parse API responses and emit RECORD messages.

**Fivetran** — Connectors parse responses in Python code. There is no declarative mapping layer — developers write standard Python to transform API responses into row dictionaries passed to `op.upsert()`.

### 6. Authentication

**Warehouse** — Auth is declarative in the spec, with multiple strategies:
- `env` — Token from environment variable
- `config_key` — Token from `~/.warehouse/config.toml`
- `header` — Static header value
- `browser_cookies` — Extract auth from browser cookie stores (macOS-specific)
- `safari_localstorage` — Token from Safari localStorage
- `token_chain` — Try multiple strategies in sequence with URL-based validation

**Airbyte** — Auth is configured via the connector's `spec` JSONSchema and handled in connector code. The platform manages OAuth refresh flows for supported connectors.

**Fivetran** — Auth credentials are passed via the `configuration` dict. OAuth and other flows are managed by the Fivetran platform when available. Custom connectors receive secrets as configuration values.

### 7. Full-Text Search

This is **unique to Warehouse**. Both Airbyte and Fivetran are pure data movement tools — they don't provide search capabilities.

Warehouse connector specs include an `fts` section that configures SQLite FTS5 indexes:
```json
"fts": [{
  "table_name": "github_stars_fts",
  "source_table": "github_stars",
  "columns": ["full_name", "description", "topics"],
  "tokenizer": "porter unicode61",
  "search_type": "github_stars",
  "title_column": "full_name",
  "date_column": "starred_at",
  "snippet_column": 1
}]
```
This means adding a new data source automatically makes it searchable via `warehouse search`.

### 8. Governance & Permissions

Also **unique to Warehouse**. The connector spec can declare `governance_fields` that are exposed to the permission system:
```json
"governance_fields": ["title", "body", "author", "date"]
```
This integrates with the `[permissions]` section of `config.toml` to enable field-level access control and time-based restrictions on search results.

Airbyte and Fivetran handle access control at the platform/warehouse level, not within the connector protocol.

## Summary: When Each Approach Fits

| Scenario | Best fit |
|----------|----------|
| **Personal data consolidation with local search** | Warehouse — declarative specs, FTS5, governance, all-local |
| **Standard REST/GraphQL API to warehouse** | Warehouse (if simple), Airbyte (if complex transforms needed) |
| **Enterprise data pipeline with monitoring** | Airbyte or Fivetran — mature orchestration, alerting, observability |
| **Non-HTTP sources** (databases, files, queues) | Airbyte or Fivetran — imperative code can use any client library |
| **Complex multi-step extraction logic** | Airbyte or Fivetran — arbitrary code vs. declarative limits |
| **No-code connector creation** | Warehouse — JSON-only, no programming required |
| **Managed infrastructure** | Fivetran — fully managed SaaS |
| **Self-hosted / open source** | Airbyte — open-source platform, or Warehouse for local use |

### Key Tradeoffs

**Warehouse's declarative approach:**
- (+) No code to write, test, or maintain for standard APIs
- (+) Integrated search and governance — not just data movement
- (+) Single binary, no Docker/Python dependencies
- (-) Limited to patterns the spec format supports (REST, GraphQL, specific pagination/auth types)
- (-) Custom transforms require extending the Rust runtime

**Airbyte's protocol approach:**
- (+) Any language, any data source, arbitrary complexity
- (+) Large connector catalog (hundreds of pre-built connectors)
- (+) Source/destination fully decoupled
- (-) Heavy infrastructure (Docker, Kubernetes, database for state)
- (-) More code to write for simple APIs

**Fivetran's SDK approach:**
- (+) Simple Python API, minimal boilerplate
- (+) Managed infrastructure, built-in monitoring
- (+) Atomic checkpointing with at-least-once delivery
- (-) Python only, vendor lock-in to Fivetran platform
- (-) No self-hosting option
