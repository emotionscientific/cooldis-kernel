# Cooldis RPC Control Plane And Dev Chat

Cooldis owns an app-server control plane with a Codex-shaped transport so local
clients can exercise the Cooldis kernel without importing Codex as a runtime
dependency. The app-server copies the wire shape that the Codex remote client
expects and routes execution through `CooldisSupervisor` / `RuntimeHost`.

The app-server is protocol, transport, and event projection. It is not a second
kernel.

`cooldis-mcp-server` reuses this daemon/app-server control plane from the MCP
side. MCP is therefore another projection over `CooldisSupervisor` and
`RuntimeHost`, not an independent scheduler. See
[Cooldis MCP Server](mcp-server.md).

For Cooldis-owned clients, the `thread/start` control-plane method accepts the
runtime `topology` object and the `parentThreadId` shorthand used by MCP. These
fields are Cooldis extensions over the Codex-shaped transport; bit-for-bit Codex
client parity is not the target of that topology surface.

## Current Surfaces

`cooldis rpc` starts the Cooldis app-server on a Unix socket:

```sh
cargo run --bin cooldis -- rpc --listen unix:///tmp/cooldis.sock
```

It can also listen on a TCP WebSocket address:

```sh
cargo run --bin cooldis -- rpc --listen ws://127.0.0.1:49200/rpc
```

The TCP WebSocket listener is loopback-only until the auth/origin policy from
[issue #108](https://github.com/emotionscientific/cooldis/issues/108) lands. Both
paths accept WebSocket frames, handle Codex-style JSON-RPC without requiring a
`jsonrpc` field, and expose the V1 method subset needed by Codex remote clients.
The direct app-server command currently uses the deterministic local/offline
provider. The TCP WebSocket listener also serves `GET /healthz` and `GET
/readyz` with a small JSON `200 OK` response.

`cooldis dev chat` starts a private app server on a temporary Unix socket and
connects Cooldis' owned Codex-shaped test client to it:

```sh
cargo run --bin cooldis -- dev chat
```

With no prompt, `cooldis dev chat` opens a small line REPL:

```text
Cooldis chat. Type /quit to exit.
> hello
assistant> ...
```

With a prompt argument, it sends one turn and exits:

```sh
cargo run --bin cooldis -- dev chat "hello from cooldis"
```

`cooldis dev rpc` connects to a running daemon's WebSocket app server instead
of starting a private one. It is useful for protocol debugging and for checking
the live daemon state from scripts:

```sh
cargo run --bin cooldis -- dev rpc call thread/list
cargo run --bin cooldis -- dev rpc call thread/read '{"threadId":"...","includeTurns":false}'
```

By default it connects to `ws://127.0.0.1:49200/rpc`. Pass `--url` for another
running WebSocket endpoint, or `--config` to read `daemon.app_server.listen`
from a `cooldis.toml`:

```sh
cargo run --bin cooldis -- dev rpc turn --new "hello from the daemon"
cargo run --bin cooldis -- dev rpc turn --thread <thread-id> --json "resume here"
cargo run --bin cooldis -- dev rpc tail --thread <thread-id> --url ws://127.0.0.1:49200/rpc
```

## Provider Config

The dev chat command can point its private app-server runtime at a live
provider endpoint. Put non-secret settings in a local `cooldis.json`:

```json
{
  "chat": {
    "provider": "openai",
    "base_url": "https://api.openai.com",
    "api_key_env": "OPENAI_API_KEY",
    "model": "gpt-4.1-mini",
    "stream": true,
    "max_tokens": 4096
  }
}
```

Then run interactive chat:

```sh
cargo run --bin cooldis -- dev chat
```

Or pass the provider config on the command line:

```sh
cargo run --bin cooldis -- dev chat \
  --provider openai \
  --base-url https://api.openai.com \
  --api-key-env OPENAI_API_KEY \
  --env-file /path/to/local/.env \
  --model gpt-4.1-mini
```

It can also talk directly to OpenAI Chat Completions-compatible endpoints:

```json
{
  "chat": {
    "provider": "openai_chat_completions",
    "base_url": "https://api.openai.com",
    "api_key_env": "OPENAI_API_KEY",
    "model": "gpt-4.1-mini",
    "stream": false,
    "max_tokens": 4096
  }
}
```

Anthropic Messages-compatible endpoints use the Anthropic provider shape:

```json
{
  "chat": {
    "provider": "anthropic",
    "base_url": "https://api.anthropic.com",
    "api_key_env": "ANTHROPIC_API_KEY",
    "model": "claude-sonnet-4-5-20250929",
    "stream": true,
    "max_tokens": 4096
  }
}
```

AWS Bedrock Anthropic uses the Bedrock Runtime `InvokeModel` path and AWS
SigV4 credentials:

```json
{
  "chat": {
    "provider": "anthropic_bedrock",
    "region": "us-east-1",
    "model": "global.anthropic.claude-sonnet-4-5-20250929-v1:0",
    "stream": false,
    "max_tokens": 4096
  }
}
```

The Sonnet 4.5 Bedrock raw model ID is
`anthropic.claude-sonnet-4-5-20250929-v1:0`, but Bedrock accounts can require
an inference profile ID. Cooldis defaults to the documented global profile,
`global.anthropic.claude-sonnet-4-5-20250929-v1:0`; use a regional profile
such as `us.anthropic.claude-sonnet-4-5-20250929-v1:0` when data routing
requires it. `anthropic_bedrock` reads credentials from `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`, and
`AWS_BEDROCK_REGION`/`AWS_REGION`/`AWS_DEFAULT_REGION`. For the local shared
1Password item, run the child command through `scripts/with-bedrock-env.sh`.
Streaming is disabled for this path until Cooldis has an AWS event-stream
decoder for `InvokeModelWithResponseStream`.

Provider config resolution is:

- `--config <file>`, otherwise `./cooldis.json` if it exists;
- command-line flags override the config file;
- `--env-file <file>`, `COOLDIS_CHAT_ENV_FILE`, otherwise `./.env`;
- base URLs resolve from `base_url`, provider-specific local env, or command
  line flags;
- keys resolve from `api_key`, `api_key_env`, or provider-standard env vars
  such as `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, and AWS Bedrock env vars;
- models resolve from `model`, command line flags, or provider-specific local
  env;
- `max_tokens` defaults to `4096`;
- streaming defaults to enabled except for `anthropic_bedrock`, which defaults
  to non-streaming.

Keep real secrets in the environment or a local ignored env file, not in
committed `cooldis.json`.

The app-server also opens the internal metadata store at
`state_home/metadata.sqlite3` on startup. It seeds provider catalog records,
resolves catalog-backed provider credentials, and persists thread
lifecycle/topology records for local `thread/start`, fork paths, and
kernel-spawned child threads created through `cooldis-threads`. Plain OpenAI Compatible
config can use that catalog-backed provider path instead of requiring the
endpoint shape to be rebuilt from config each time.

## Thread Residency

Cooldis follows the Codex-shaped command surface here: the public continuation
operation is `thread/resume`. A resume request attaches to a resident thread when
one is already loaded, or loads the thread from durable metadata/session state
when only the stored thread id remains.

`thread/loaded/list` is introspection for currently resident runtime handles. It
is useful for tests and clients that want to reconnect to already-loaded work,
but it is not a durable thread index and does not imply a public
`thread/load`/`thread/unload` pair. Unloading idle, unobserved handles is an
internal residency policy.

Restored resident threads stream through `turn/start` the same way freshly
started threads do. A client that reconnects after a daemon restart can issue a
turn against the loaded thread id and expect the usual item delta/completion and
turn completion notifications.

## Thread Forks

`thread/fork` is the clone-style fork. Params are
`{ "threadId": "...", "checkpointId": "...", "ephemeral": false }`, with
`checkpointId` optional. When omitted, the app-server creates a checkpoint at
the source thread's current active leaf and forks from that point. When supplied,
the checkpoint must already be loaded for the requested source thread.

The result includes the normal `thread` object plus a `fork` provenance object:
`{ "mode": "clone", "parentThreadId": "...", "checkpointId": "...",
"sourceCut": { "threadId": "...", "checkpointId": "...", "leafEntryId": "...",
"streamId": "thread:...", "streamToSequence": null } }`. Clone forks copy the
checkpoint branch into a new child branch; later parent and child turns diverge
without sharing active leaves.

`thread/rebindFork` is the borrowed-prefix/reference fork. It starts a child
thread bound to a new agent manifest and records a `ThreadBaseRef` so context
reads inherit the source prefix instead of copying the source entries into the
child store. Its result also includes `fork.sourceCut`, with
`{ "mode": "reference", ... }` plus the bound `agentRef` and `manifestHash`.

`parentThreadId` remains the topology/control parent. `forkedFromId` is kept as
a compatibility lineage alias on thread objects. New clients should use
`fork.sourceCut` as the exact split point. Policy-level
`AdmissionDecision::Fork` is still not wired through the daemon bridge in this
release.

## Manifest Starts

`thread/start` accepts `agentRef` for a published `agent://...` ref. The
app-server resolves aliases from its configured agent registry root
(`.cooldis/agents` by default) and operation refs from its configured operation
registry root (`.cooldis/operations` by default), selects a declared model
profile against the configured provider surface, applies allowed
`runtimeOverrides`, and records the immutable manifest ref in thread lifecycle
metadata. Relative agent registry roots and operation registry roots are
resolved against the configured runtime `cwd`, matching the root reported by
`config/read`.

At startup the app-server also publishes a kernel-synthesized default manifest
as `agent://cooldis/default@latest`. Its envelope is the configured provider,
model, working directory, and streaming support. If capsule bindings are
configured with `global_operation_names`, or `load_all_active_when_unbound`,
startup synthesis resolves the active local operation records and emits pinned
`bash_tool` rows into that default manifest. Bare starts therefore bind a normal
manifest whose bind receipt names the exact operation artifacts. When those
binding knobs are configured, the effective operation registry root must contain
the named records. With no binding config, an absent default operation registry
root behaves like an empty registry.

All `thread/start` calls now bind either the explicit `agentRef` or that default
manifest. Legacy explicit parameters lower as follows:

- `model` and `modelProvider` are profile selectors only. They must match
  exactly one declared `model_profiles` row in the bound manifest; no-match and
  ambiguous selectors are rejected with the declared profiles listed in the
  error.
- `cwd` lowers to `runtimeOverrides.defaultCwd` and is checked against the
  bound manifest's runtime override allowlist.
- non-empty `capsuleBindings.operationNames` is rejected. Operations must be
  declared by the manifest and pass through publish/bind receipts, not injected
  at start time.

Manifest-backed starts emit `manifest.compile.completed` followed by
`manifest.bind.completed` on the thread event stream before the first turn. Both
events are discharged and include provenance. An `@latest` start includes the
alias resolution receipt in the compile event payload.

## Thinking Config

`thread/start` and `turn/start` accept an optional `thinking` object:

```json
{ "type": "effort", "effort": "low" }
```

```json
{ "type": "budget", "budgetTokens": 4096 }
```

```json
{ "type": "disabled" }
```

Valid effort values are `low`, `medium`, `high`, `xhigh`, and `max`.
Unknown `type` values, missing required fields, non-`u32` budgets, and unknown
effort values fail the request instead of falling back silently.

Precedence is turn, then thread, then daemon default. A `turn/start` value
applies only to that turn. A `thread/start` value is stored on the thread and is
reported as `thread.thinking` from `thread/read` and `thread/list`; unset
thread-level thinking is reported as `null`.

Provider capability errors stay on the turn error path. For example, budget
thinking against an OpenAI Responses provider continues to fail closed as a turn
error rather than being prevalidated at `thread/start`. OpenAI Chat Completions
maps `low`, `medium`, and `high` effort values to `reasoning_effort`; budget
thinking and unsupported effort values fail closed on that provider path.

## Query Methods

### `agent/list`

Params: none.

Result: `{ "data": [...], "cursor": null }`. Each entry includes `name`,
`version`, `refUri`, `manifestHash`, optional `title` and `summary`,
`defaultModelProfile` (`id`, `providerRef`, `modelRef`), `toolIds`,
`aliases` (`alias`, `version`), and `publishedAtMs`.

An empty configured agent registry returns an empty `data` array. Registry I/O
or record decoding failures return JSON-RPC errors. Alias entries are reported
only when the alias resolves to an existing version record.

### `agent/read`

Params: `{ "ref": "agent://name@version-or-alias" }`.

Result: the stored published agent record JSON. When the ref resolves through an
alias, the response also includes `aliasResolutionReceipt`.

Unknown, malformed, or unreadable refs fail with a JSON-RPC error instead of an
empty success.

### `operation/list`

Params: none.

Result: `{ "data": [...], "cursor": null }`. Each entry is projected from the
configured operation registry and includes the record's `name`,
`activeArtifactHash`, optional `summary`, `manifest`, `projections`,
`interface`, `capabilityGrants`, `metadata`, `source`, and `build`.

If no operation registry root is configured, or if the conventional default root
is absent, the method returns an empty `data` array. Registry I/O or record
decoding failures return JSON-RPC errors.

### `model/list`

Params: none.

Result: the existing `{ "data": [...], "nextCursor": null }` envelope. Local
and direct provider configs return the configured provider/model identity.
Catalog-backed configs return the configured provider's model records and append
the configured default when the catalog omits it. Exactly one entry has
`isDefault: true`.

A missing catalog provider or invalid provider metadata returns a JSON-RPC
error during app-server setup or method handling.

### `mcpSource/list`

Params: none.

Result: `{ "data": [...], "nextCursor": null }`. Each entry is a redacted remote
MCP source record: `name`, `transport`, `url`, redacted `auth`, redacted
`headers`, `include_tools`, timeout/output caps, and the latest
`discovered_tools` snapshot when discovery has run.

`mcpServerStatus/list` is kept as a compatibility alias for this read model.

### `mcpSource/read`

Params: `{ "name": "arcade" }`.

Result: `{ "source": { ... } }` with the same redacted source shape as
`mcpSource/list`. Unknown source names fail with a JSON-RPC error.

### `mcpSource/upsert`

Params: `{ "name": "...", "transport": "mcp-http" | "mcp-sse", "url": "...",
"bearerSecret": "optional.secret.ref", "bearerToken": "optional write-only
token", "headers": [{ "name": "...", "value": "..." }], "includeTools": [...],
"timeoutMs": 30000, "maxOutputBytes": 1048576 }`.

Result: `{ "source": { ... } }`. When `bearerToken` is supplied, the daemon
stores it in the local secret store and records only a secret ref on the MCP
source. If `bearerSecret` is omitted, the daemon creates
`mcp.<source>.bearer`.

### `mcpSource/discover`

Params: `{ "name": "arcade" }`.

Result: `{ "source": { ... } }` after connecting to the configured MCP endpoint,
running `initialize` plus `tools/list`, and storing the discovered tool snapshot.

### `mcpSource/delete`

Params: `{ "name": "arcade" }`.

Result: `{ "deleted": true | false }`.

### `mcpSource/testTool`

Params: `{ "name": "arcade", "tool": "search", "arguments": { ... } }`.

Result: `{ "toolName": "...", "content": [...], "contentText": "...",
"isError": false }`. This is a setup/test call for UI flows; granting a source
to an agent still happens through a manifest `protocol_tool_import`.

### `mcpSource/manifestPatch`

Params: `{ "name": "arcade", "importId": "arcade", "agentRef":
"agent://researcher@latest" }`. `importId` and `agentRef` are optional.

Result: `{ "source": { ... }, "serverRef": "mcp://arcade", "toml": "...",
"tool": { ... }, "diagnostics": [...] }`. The method previews the bare
`protocol_tool_import` fragment needed to attach a verified source to an agent.
When `agentRef` is supplied, diagnostics report non-fatal collisions such as an
existing tool id or an existing import of the same `serverRef`. This method does
not edit, publish, or republish agent manifests.

### `thread/list`

Params: none.

Result: `{ "data": [...], "nextCursor": null, "backwardsCursor": null }`.
Entries include root threads, app-server-created child threads, and child
threads spawned by `cooldis-threads/thread_spawn`. Child entries populate both
`parentThreadId` and the compatibility `forkedFromId` field with the spawning
thread id. Root threads report both fields as `null`.

Kernel-spawned child threads are registered in the metadata store before their
first submitted turn. Their `thread/events/list` stream is therefore addressable
by child id and includes the manifest compile/bind receipts recorded during
child binding.

### `thread/events/list`

Params:
`{ "threadId": "...", "stream": "thread", "cursor": "...", "streamCursor": {...}, "limit": 100, "kinds": [...] }`.
Only `threadId` is required. `stream` defaults to `"thread"` and may be
`"thread"`, `"control"`, or `"derived:<name>"`. `limit` defaults to `100` and
is clamped to the range `1..=500`. `kinds` filters by exact event-kind strings.
`cursor` is the legacy opaque next-sequence token. `streamCursor` is the
canonical `cooldis.stream.cursor/1` object and is verified against stream id,
sequence, and event id before replay. A request may pass either `cursor` or
`streamCursor`, but not both.

Result: `{ "data": [...], "cursor": "..." | null, "streamCursor": {...} | null }`.
Events are returned in append order. Each entry is the canonical
`cooldis.stream.record/1` envelope and also carries the compatibility aliases
`eventId` and `atMs`. `cursor` and `streamCursor` are returned when more records
may be available.

Unknown thread ids and malformed cursors fail with JSON-RPC errors. A valid
thread with no matching events returns an empty `data` array and null cursors.

### `thread/couplings/list`

Params: `{ "threadId": "...", "limit": 100 }`.
Only `threadId` is required. `limit` defaults to `100` and is clamped to
`1..=500`.

Result:
`{ "data": [...], "nextCursor": null, "agentRef": "...", "manifestHash": "...", "bindEventId": "..." }`.
V1 projects bound couplings from the latest `manifest.bind.completed` receipt
for the thread. Each row includes id, inferred role, trigger kind/match,
source streams/kinds, sink stream/kinds, function ref, artifact hash, optional
operation name, grants, budget, and config hash. This is an inspection surface:
it reads the immutable bind receipt and does not mutate a runtime hook list.

### `thread/approvals/list`

Params: `{ "threadId": "...", "limit": 100 }`.
Only `threadId` is required. `limit` defaults to `100` and is clamped to
`1..=500`.

Result: `{ "data": [...], "nextCursor": null }`. V1 projects pending approval
inspection from open `tool.call.suspended` control facts that carry an
`approval_id`; terminal `tool.call.decision` facts close the pending approval.
Each entry includes the approval id, suspended event id, optional request event
id, turn id, call id, snapshot id, reason, and `status = "pending"`.

This is a read-only inspection surface. It does not claim Telegram, Slack,
email, or web HITL completion. Use `approval/resolve` when an initialized
control-plane client needs to witness an abstract approval decision.

### `approval/resolve`

Params:
`{ "threadId": "...", "approvalId": "...", "decision": "approved", "reason": "..." }`.
`decision` is `"approved"` or `"denied"`; `reason` is optional.

Result:
`{ "status": "resolved" | "already_resolved", "approvalId": "...", "decision": "approved", "approved": true, "reason": "...", "snapshotId": "...", "eventId": "...", "streamId": "...", "sequence": 1, "createdAtMs": 0 }`.

V1 resolves only approval ids that are currently visible as open
approval-bearing `tool.call.suspended` facts. The method appends a witnessed
`approval.resolved` event to the thread control stream. Repeating the same
decision is idempotent and returns `status = "already_resolved"` with the
original event id; conflicting duplicate decisions fail closed.

This is still an abstract write surface, not channel-specific HITL completion:
it does not deliver through Telegram, Slack, email, or web UI, and it does not
directly resume the turn. Resolution controllers may consume
`approval.resolved` and discharge `tool.call.decision`; the scheduler can then
append `turn.resumed`.

### `thread/waiting/list`

Params: `{ "threadId": "...", "limit": 100 }`.
Only `threadId` is required. `limit` defaults to `100` and is clamped to
`1..=500`.

Result: `{ "data": [...], "nextCursor": null }`. V1 projects waiting state from
durable `turn.waiting` control facts plus pending `tool.call.suspended` facts
that do not already have a matching active `turn.waiting` row. `turn.resumed`
or `turn.completed` closes a waiting turn; terminal tool decisions close
pending tool suspensions.

### `thread/debug/export`

Params:
`{ "threadId": "...", "streams": ["thread", "control"], "includeThread": true, "maxEventsPerStream": 5000, "redact": true }`.
Only `threadId` is required. `streams` defaults to `["thread", "control"]` and
accepts the same selectors as `thread/events/list`. `maxEventsPerStream`
defaults to `5000` and is clamped to `1..=10000`.

Result:
`{ "schema": "cooldis.debug.thread_export/1", "threadId": "...", "backend": ..., "ackClasses": ..., "redaction": ..., "thread": ..., "streams": [...], "receipts": [...] }`.
The V1 local backend identity is `kind = "sqlite"` with the session store path
and ack classes `local_committed` plus `query_projected`. Each stream entry
includes `selector`, `streamId`, `backend`, `ackClasses`, `range`, `data`,
`eventCount`, `truncated`, `cursor`, and `streamCursor`; `cursor` is the legacy
opaque continuation token and `streamCursor` is the canonical
`cooldis.stream.cursor/1` continuation cursor when the export is truncated.
`range` carries start/tail sequence, legacy cursor tokens, and
`lastExportedStreamCursor` / `tailStreamCursor` evidence for replay/debug.
`data` uses the canonical
`cooldis.stream.record/1` event envelope. `receipts` is a compact index of
exported discharged events by stream id, sequence, event id, kind, origin, and
payload schema.

Redaction is on by default and recursively replaces values for secret-shaped
JSON keys such as `api_key`, `token`, `secret`, `authorization`, `password`, and
bearer credentials. The `redaction.redactedKeys` array records which key names
were redacted in the bundle.

This method is the V1 operator evidence bundle surface. It is not a browser-safe
subscription stream and does not replace the authority stream store.

### `thread/read`

Params: `{ "threadId": "...", "includeTurns": true }`. `includeTurns` defaults
to `true`; set it to `false` to read only thread metadata.

Result: `{ "thread": { ... } }`. The thread object matches `thread/list` entries
and includes `turns` when requested. It also includes `thinking`, which is the
effective thread-level config or `null` when unset. Restored threads derive
`preview` from the first persisted user text when no live preview is resident.

Each turn includes `id`, `items`, `itemsView`, `status`, `error`, `startedAt`,
`completedAt`, and `durationMs`. Persisted history is projected in chronological
turn order from user and assistant text messages. User items use
`{ "id": "...", "type": "userMessage", "content": [{ "type": "text", "text": "..." }] }`.
Agent items use the same text content shape with
`type: "agentMessage"` and also include the compatibility `text` field used by
live item notifications. Thinking items use the same content shape with
`type: "agentThinking"` and a compatibility `text` field. Thinking content is
not folded into `agentMessage` text. Completed persisted turns include
`status: "completed"` and `completedAt` when assistant or thinking content is
recorded.

## Protocol Scope

The V1 app-server implements the Codex TUI-critical request subset:

- `initialize` and `initialized`;
- `account/read`;
- `agent/list`, `agent/read`, `operation/list`, `model/list`;
- `mcpSource/list`, `mcpSource/read`, `mcpSource/upsert`,
  `mcpSource/discover`, `mcpSource/delete`, `mcpSource/testTool`,
  `mcpSource/manifestPatch`;
- `thread/start`, `thread/resume`, `thread/fork`, `thread/read`,
  `thread/list`, `thread/loaded/list`, `thread/events/list`,
  `thread/couplings/list`, `thread/approvals/list`, `thread/waiting/list`,
  `thread/debug/export`;
- `approval/resolve`;
- `thread/name/set`, `thread/metadata/update`, `thread/compact/start`,
  `thread/unsubscribe`;
- `turn/start`, `turn/steer`, `turn/interrupt`.

Schema-valid friendly stubs keep the client calm for catalog/config surfaces
such as `skills/list`, `plugin/list`, `hooks/list`,
`account/rateLimits/read`, `config/read`, and `configRequirements/read`.
`mcpServerStatus/list` remains as a compatibility alias for `mcpSource/list`.
`config/read` additionally reports the app-server's
absolute working directory as `config.cwd`, so clients can discover a real
root for the filesystem read methods without out-of-band configuration.

Unsupported noncritical methods return JSON-RPC `-32601` with the method name.

Runtime events are projected back into Codex-shaped notifications, including:

- `thread/started`, `thread/status/changed`, and `thread/closed`;
- `turn/started`, `turn/completed`, and `error`;
- `item/agentMessage/delta` for streamed assistant text;
- `item/agentThinking/delta` for streamed thinking text;
- final `item/completed` messages for completed assistant output.

## Verification

Run the normal Rust suite:

```sh
cargo test
```

Run the cheap app-server smoke:

```sh
cargo run --bin cooldis-app-server-smoke
```

Run the workbench query-surface smoke:

```sh
cargo run --bin cooldis-workbench-smoke
```

Run the focused MCP server tests:

```sh
cargo test mcp_server
```

Run the cheap process-backed live smoke:

```sh
cargo run --bin cooldis-live-smoke
```

Run a one-shot local/offline chat proof:

```sh
cargo run --bin cooldis -- dev chat "hello from local chat"
```

Run an OpenAI Responses-compatible chat proof with a local env file:

```sh
cargo run --bin cooldis -- dev chat \
  --provider openai \
  --base-url https://api.openai.com \
  --api-key-env OPENAI_API_KEY \
  --env-file /path/to/local/.env \
  --model gpt-4.1-mini \
  "Reply with exactly COOL_CHAT_OPENAI_OK and no other text."
```

The expected response is exactly:

```text
COOL_CHAT_OPENAI_OK
```

Run an OpenAI Chat Completions-compatible proof:

```sh
cargo run --bin cooldis -- dev chat \
  --provider openai_chat_completions \
  --base-url https://api.openai.com \
  --api-key-env OPENAI_API_KEY \
  --no-stream \
  "Reply with exactly COOL_CHAT_COMPLETIONS_OK and no other text."
```

Run an Anthropic Messages proof:

```sh
cargo run --bin cooldis -- dev chat \
  --provider anthropic \
  --api-key-env ANTHROPIC_API_KEY \
  --model claude-sonnet-4-5-20250929 \
  "Reply with exactly COOL_CHAT_ANTHROPIC_OK and no other text."
```

Run an Anthropic Bedrock proof:

```sh
scripts/with-bedrock-env.sh cargo run --bin cooldis -- dev chat \
  --provider anthropic_bedrock \
  --model global.anthropic.claude-sonnet-4-5-20250929-v1:0 \
  --no-stream \
  "Reply with exactly COOL_CHAT_BEDROCK_OK and no other text."
```
