# Connector Format Comparison: Warehouse vs Airbyte vs Fivetran vs n8n

This document compares Warehouse's JSON connector spec format with the connector protocols used by Airbyte, Fivetran, and n8n.

## Overview

| Aspect | Warehouse | Airbyte | Fivetran | n8n |
|--------|-----------|---------|----------|-----|
| **Connector definition** | Declarative JSON spec file | Imperative code in Docker container | Imperative Python code | Declarative `routing` or imperative `execute()` (TypeScript) |
| **Runtime** | Single Rust binary parses JSON | Docker container per connector | Python process via gRPC | Node.js workflow engine |
| **Data transport** | Direct SQLite writes | Line-delimited JSON on STDOUT | gRPC stream | In-memory item arrays between nodes |
| **Schema declaration** | In-spec `tables[].columns` | JSONSchema via `discover` command | `schema()` function return value | Implicit (no formal schema) |
| **Incremental sync** | Pagination early-stop + soft delete | Cursor-based STATE messages | State dict with cursors + checkpoints | Workflow-level (external state management) |
| **Target** | Local SQLite + FTS5 | Any destination connector | Fivetran-managed warehouse | Any node (400+ integrations) |

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

### n8n: Declarative Routing or Programmatic Execute

n8n offers **two node-building styles**. For standard REST APIs, the recommended approach is **declarative** — you define a TypeScript class with a `routing` configuration instead of writing an `execute()` method. n8n's runtime automatically builds and sends HTTP requests based on the routing config:

```typescript
export class NasaPics implements INodeType {
  description: INodeTypeDescription = {
    displayName: 'NASA Pics',
    name: 'nasaPics',
    group: ['transform'],
    version: 1,
    inputs: [NodeConnectionType.Main],
    outputs: [NodeConnectionType.Main],
    credentials: [{ name: 'nasaPicsApi', required: true }],

    // Base request config — declarative
    requestDefaults: {
      baseURL: 'https://api.nasa.gov',
      headers: { Accept: 'application/json' },
    },

    properties: [
      {
        displayName: 'Resource',
        name: 'resource',
        type: 'options',
        options: [
          { name: 'Astronomy Picture of the Day', value: 'apod' },
          { name: 'Mars Rover Photos', value: 'marsRoverPhotos' },
        ],
        default: 'apod',
      },
      {
        displayName: 'Operation',
        name: 'operation',
        type: 'options',
        displayOptions: { show: { resource: ['apod'] } },
        options: [
          {
            name: 'Get',
            value: 'get',
            action: 'Get the astronomy picture of the day',
            // Declarative routing — no execute() needed
            routing: {
              request: { method: 'GET', url: '/planetary/apod' },
            },
          },
        ],
        default: 'get',
      },
      {
        displayName: 'Date',
        name: 'date',
        type: 'dateTime',
        default: '',
        // Field-level routing: how this value maps to the request
        routing: {
          send: { type: 'query', property: 'date' },
        },
      },
    ],
  };
  // No execute() method — n8n handles everything from the routing config
}
```

For complex logic (GraphQL, multi-step calls, custom transforms), n8n falls back to the **programmatic style** with a manual `execute()` method — similar to Airbyte/Fivetran's imperative approach.

Data flows between n8n nodes as arrays of `INodeExecutionData` items:
```json
[
  {"json": {"id": 1, "name": "Alice"}, "pairedItem": {"item": 0}},
  {"json": {"id": 2, "name": "Bob"}, "pairedItem": {"item": 1}}
]
```

## Detailed Comparison

### 1. Connector Authoring

| | Warehouse | Airbyte | Fivetran | n8n |
|---|-----------|---------|----------|-----|
| **Language** | JSON (no code) | Any (Python, Java, Go, etc.) | Python only | TypeScript |
| **Skill required** | JSON editing + API knowledge | Full programming + Docker | Python programming | TypeScript + n8n conventions |
| **Packaging** | `.json` file in `~/.warehouse/connectors/` | Docker image | Python module deployed to Fivetran | npm package |
| **Install** | `warehouse connector add <url>` | Docker pull via UI/API | `fivetran deploy` CLI | `npm install` into n8n instance |
| **LOC for simple connector** | ~40-60 lines of JSON | ~200-500 lines of code + Dockerfile | ~50-100 lines of Python | ~80-150 lines of TypeScript (declarative) |
| **Declarative option** | Yes (JSON-only) | Partial (Low-Code CDK / YAML) | No | Yes (`routing` config in TypeScript) |

Warehouse's declarative approach means no code to write or debug for standard REST/GraphQL APIs. n8n's declarative mode is the closest parallel — it also lets you define endpoints, auth, pagination, and field routing without an `execute()` method. The key difference is that Warehouse uses pure JSON (no programming language needed), while n8n's declarative config is embedded in TypeScript class definitions. Both fall short for complex extraction logic, where imperative code is needed.

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

**n8n** — Has **no formal schema declaration**. Data is untyped JSON flowing as `INodeExecutionData` items. Each item has a `json` property containing arbitrary key-value pairs. The node's `properties` array defines UI input fields (with types like `string`, `number`, `options`, `boolean`, `dateTime`), but these describe the node's parameters, not the shape of the output data. Downstream nodes receive whatever JSON the node produces.

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

**n8n** — Declarative nodes use `postReceive` actions to transform API responses:
```typescript
output: {
  postReceive: [
    { type: 'rootProperty', properties: { property: 'data.items' } },
    { type: 'filter', properties: { pass: '={{$responseItem.status === "active"}}' } },
    { type: 'setKeyValue', properties: { name: '={{$responseItem.name}}', value: '={{$responseItem.id}}' } },
    { type: 'limit', properties: { maxResults: '={{$parameter.limit}}' } },
  ],
}
```
This is a pipeline of transforms applied to the response — conceptually similar to Warehouse's `field_mappings` + `results_path` but expressed as an ordered array of typed operations rather than per-field dot-paths. n8n's expressions (`={{...}}`) enable inline JavaScript, giving more flexibility than Warehouse's fixed transform set (`to_string`, `join_array`, etc.).

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

**n8n** — Auth is declared in a separate credential class with an `authenticate` property that specifies injection targets:
```typescript
export class MyServiceApi implements ICredentialType {
  name = 'myServiceApi';
  properties = [
    { displayName: 'API Key', name: 'apiKey', type: 'string', typeOptions: { password: true } },
  ];
  authenticate: IAuthenticate = {
    type: 'generic',
    properties: {
      headers: { 'X-API-Key': '={{$credentials.apiKey}}' },
    },
  };
}
```
Supports API key (header/query), HTTP Basic, Bearer token, OAuth1, and OAuth2 flows. Credentials are encrypted at rest and injected automatically into declarative routing requests — the node spec just references the credential by name. This is similar in spirit to Warehouse's auth spec but requires TypeScript instead of JSON.

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

## Warehouse vs n8n: Declarative Approaches Compared

Warehouse and n8n are the most similar in philosophy — both offer declarative connector definitions for REST APIs. Here's how they differ:

| Aspect | Warehouse JSON Spec | n8n Declarative Node |
|--------|---------------------|----------------------|
| **Format** | Pure JSON file | TypeScript class with `routing` config |
| **No programming language required** | Yes | No (TypeScript) |
| **Endpoint definition** | `endpoint.url` + `endpoint.method` | `routing.request.url` + `routing.request.method` |
| **Field → request mapping** | `response.field_mappings` (response → DB) | `routing.send` (UI field → request) |
| **Response → output mapping** | `results_path` + `field_mappings` with dot-paths | `postReceive` pipeline (rootProperty, filter, setKeyValue) |
| **Pagination** | `pagination.type` (page_number, offset, cursor) | `requestOperations.pagination` (offset, custom) |
| **Auth** | `auth` block in same JSON file | Separate credential TypeScript class |
| **Data destination** | SQLite (with FTS5 indexing) | Next node in workflow (in-memory) |
| **Scope** | Data extraction + storage + search | Workflow step (one action in a chain) |
| **Dynamic options** | `discover` pipeline | `loadOptions.routing` |
| **Transforms** | Fixed set: `to_string`, `to_int`, `join_array` | Inline JavaScript expressions: `={{...}}` |

The fundamental difference: Warehouse specs describe a **complete data pipeline** (auth → fetch → paginate → map → store → index), while n8n nodes describe a **single workflow step** (one API call that passes data to the next node). Warehouse handles state/sync/search internally; n8n delegates those concerns to the workflow.

## Summary: When Each Approach Fits

| Scenario | Best fit |
|----------|----------|
| **Personal data consolidation with local search** | Warehouse — declarative specs, FTS5, governance, all-local |
| **Standard REST/GraphQL API to warehouse** | Warehouse (if simple), Airbyte (if complex transforms needed) |
| **Enterprise data pipeline with monitoring** | Airbyte or Fivetran — mature orchestration, alerting, observability |
| **Visual workflow automation** | n8n — drag-and-drop node chaining with 400+ integrations |
| **Non-HTTP sources** (databases, files, queues) | Airbyte or Fivetran — imperative code can use any client library |
| **Complex multi-step extraction logic** | Airbyte, Fivetran, or n8n (programmatic mode) |
| **No-code connector creation** | Warehouse — JSON-only, no programming required |
| **Managed infrastructure** | Fivetran — fully managed SaaS |
| **Self-hosted / open source** | Airbyte, n8n, or Warehouse for local use |

### Key Tradeoffs

**Warehouse's declarative approach:**
- (+) No code to write, test, or maintain for standard APIs
- (+) Integrated search and governance — not just data movement
- (+) Single binary, no Docker/Python/Node.js dependencies
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

**n8n's hybrid approach:**
- (+) Declarative mode for simple REST APIs, programmatic fallback for complex logic
- (+) Visual workflow builder — chain nodes into multi-step pipelines
- (+) Large ecosystem (400+ built-in nodes) + self-hostable
- (+) Inline JavaScript expressions for flexible transforms
- (-) TypeScript required (not pure JSON like Warehouse)
- (-) No built-in schema enforcement or data persistence
- (-) Nodes are workflow steps, not standalone data pipelines — sync/state/storage are external concerns
