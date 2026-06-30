# Metadata And Provider Auth Storage

Cooldis needs durable metadata/config storage for state that is not conversation
context: provider catalogs, provider credentials, thread metadata, grants,
bindings, routing config, and similar control-plane records.

The local default should be an internal SQLite metadata store. Remote or larger
deployments can implement the same store traits with Postgres, Turso/libSQL, or
another relational backend. The runtime contract should not change just because
the backend changes.

## Pi Pattern To Keep

Pi separates provider metadata from provider credentials:

- `models.json` describes providers, endpoints, model records, headers, and
  provider compatibility.
- `auth.json` stores API keys and OAuth credentials.
- runtime API-key overrides are not persisted.
- auth status can report where credentials come from without exposing secret
  values.

Cooldis keeps that shape, but puts it inside a broader runtime-owned metadata
store with SQLite first:

```text
LlmProviderCatalogStore
  provider/model metadata, auth hints, headers, capability metadata

LlmProviderAuthStore
  provider credentials, redacted status, runtime auth resolution

LlmProviderAuthContext
  launch/turn runtime overrides plus environment snapshot

ThreadMetadataStore
  thread lifecycle records and topology for central control-plane lookup
```

The first concrete implementation is `SqliteMetadataStore`. App-server boot
opens one project-local SQLite store for provider catalog records and thread
lifecycle/topology records, plus one user-global SQLite store for credential
records. `SqliteLlmProviderStore` remains the provider table implementation,
but browser provider auth writes use the user auth store.

The public console defaults to:

```text
<project_root>/.cooldis/state/metadata.sqlite3
~/.cooldis/state/metadata.sqlite3
```

Seeding is idempotent and does not overwrite stored credentials, so a daemon or
app-server restart reloads the provider catalog from durable project metadata
before it constructs the provider runtime, then resolves credentials from the
user auth store.

## Built-In Seeds

Provider catalog seeds are ordinary metadata rows. Public examples should stay
provider-neutral unless a provider is part of a documented public integration. A
local OpenAI Chat Completions-compatible seed has this shape:

```text
provider_id: local-openai-compatible
api: openai_chat_completions
base_url: https://api.example.invalid/v1
default model: example-chat-model
auth: stored credential or configured environment variable
```

The seed writes provider/model metadata only. Actual API keys stay in
`LlmProviderAuthStore` or the runtime/environment auth context.

Daemon config with no inline API key should select the catalog-backed provider
path. The runtime then resolves auth from the metadata store or process
environment using the priority order below.

## Auth Resolution

Provider auth resolution follows the Pi-inspired priority order:

1. runtime override, such as a launch-only key;
2. stored credential in the durable auth table;
3. environment variable;
4. provider catalog fallback, such as an explicit env reference or inline key.

Command-backed secrets are represented but not executed by default. A future
executor-backed resolver can add command execution with explicit policy,
timeouts, audit events, and cache semantics.

OAuth credential records exist as a durable shape, but refresh is not part of
the first implementation. Expired OAuth credentials currently fail closed rather
than silently re-authenticating.

## Thread Metadata

Thread topology is small, coordination-critical control-plane state. Locally,
`thread/start` persists `ThreadLifecycleRecord` rows through
`ThreadMetadataStore` into the project metadata database:

```text
thread_id
tenant_id
user_id
session_id
parent_thread_id
status
topology
metadata
created_at_ms / updated_at_ms
```

The table stores the full lifecycle record as JSON plus indexed tenant/user/
session and parent columns. That gives the local daemon/app-server a durable
central topology map without making chat history or context persistence part of
this first pass.

The Codex-shaped continuation surface is `thread/resume`: it attaches to a
resident runtime handle when one is already loaded, or loads the handle from this
metadata record plus the session/history store when only durable state remains.
`thread/loaded/list` is residency introspection, not the durable thread index.

Future provider-per-thread metadata should be stored in this project
control-plane database, but actual credentials stay in the user auth table.
Conversation context can use the session/history store locally and may move to a
different backend when the deployment shape needs it.

## Backend Direction

Local:

```text
<project_root>/.cooldis/state/metadata.sqlite3
  llm_provider_records
  thread_lifecycle_records
  future config/grant/capsule binding tables

~/.cooldis/state/metadata.sqlite3
  llm_provider_credentials
  named secret values
```

Remote:

```text
Postgres / Turso / relational adapter
  same store traits
  external leases/fencing where concurrent writers need it
```

The important boundary is that app-server workers read and write through store
interfaces. SQLite is the embedded default, not the architecture.
