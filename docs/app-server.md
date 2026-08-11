# Verlet RPC Control Plane

Verlet owns an app-server control plane with a Codex-shaped transport so local
clients can exercise the Verlet kernel without importing Codex as a runtime
dependency. The app-server copies the wire shape that the Codex remote client
expects and routes execution through `VerletSupervisor` / `RuntimeHost`.

The app-server is protocol, transport, and event projection. It is not a second
kernel.

`verlet-mcp-server` reuses this daemon/app-server control plane from the MCP
side. MCP is therefore another projection over `VerletSupervisor` and
`RuntimeHost`, not an independent scheduler. See
[Verlet MCP Server](mcp-server.md).

For Verlet-owned clients, the `thread/start` control-plane method accepts the
runtime `topology` object and the `parentThreadId` shorthand used by MCP. These
fields are Verlet extensions over the Codex-shaped transport; bit-for-bit Codex
client parity is not the target of that topology surface.

## Current Surfaces

### Standalone RPC quick start

Bootstrap an operator into an explicit state home before starting a standalone
TCP WebSocket server. The identity command prints the token once on a line in
the form `token <value>`. Copy only `<value>` for the client command below. The
`$HOME`-anchored state home identifies the same store from both terminals,
regardless of their working directories:

```sh
verlet identity bootstrap operator:quick-start \
  --display "Quick-start operator" \
  --state-home "$HOME/.verlet/rpc-quick-start-state"
```

Start the server in one terminal:

```sh
verlet rpc \
  --listen ws://127.0.0.1:49200/rpc \
  --state-home "$HOME/.verlet/rpc-quick-start-state"
```

Then call it from another terminal, replacing `<token>` with the token printed
by `identity bootstrap`:

```sh
VERLET_APP_SERVER_TOKEN="<token>" \
  verlet debug rpc call thread/list
```

Both transports authenticate every connection before any method dispatch; see
[Authentication](#authentication). A TCP WebSocket client must present a
bearer token. A same-uid Unix socket peer needs no token in local mode. Both
paths accept WebSocket frames, handle
Codex-style JSON-RPC without requiring a `jsonrpc` field, and expose the V1
method subset needed by Codex remote clients.
The direct app-server command currently uses the deterministic local/offline
provider. The TCP WebSocket listener also serves `GET /healthz` and `GET
/readyz` with a small JSON `200 OK` response.

`verlet rpc` also accepts `--runtime-home` and `--cwd`. Without
`--state-home`, it creates a fresh per-process temporary state home and prints
that path at startup. Use an explicit state home when minting a credential for
a TCP WebSocket client. A Unix socket server can be started with:

```sh
verlet rpc \
  --listen unix:///tmp/verlet.sock \
  --state-home "$HOME/.verlet/rpc-quick-start-state"
```

`verlet console` starts the same loopback app-server shape for local browser
operation, binds `127.0.0.1:<port>`, serves the bundled Svelte console from `/`,
and keeps JSON-RPC on `/rpc`. Each console process generates a session token,
injects it into `index.html`, and rejects `/rpc` WebSocket upgrades that do not
present the token through `Sec-WebSocket-Protocol`. Query-string tokens are not
accepted.

`verlet chat` starts the bundled terminal console. It launches a private app
server on a temporary Unix socket by default, or attaches to an existing
endpoint with `--attach`:

```sh
cargo run --bin verlet -- chat
cargo run --bin verlet -- chat --attach unix:///tmp/verlet.sock
```

With a prompt argument, it opens the terminal console and submits that prompt:

```sh
cargo run --bin verlet -- chat "hello from verlet"
```

`verlet debug rpc` connects to a running standalone or daemon WebSocket app
server instead of starting a private one. It is useful for protocol debugging
and for checking live state from scripts. Export the credential first so each
command presents it:

```sh
export VERLET_APP_SERVER_TOKEN="<token>"
verlet debug rpc call thread/list
verlet debug rpc call thread/read '{"threadId":"...","includeTurns":false}'
```

By default it connects to `ws://127.0.0.1:49200/rpc`. Pass `--url` for another
running WebSocket endpoint, or `--config` to read `daemon.app_server.listen`
from a `verlet.toml`:

```sh
verlet debug rpc turn --new "hello from the daemon"
verlet debug rpc turn --thread <thread-id> --json "resume here"
verlet debug rpc tail --thread <thread-id> --url ws://127.0.0.1:49200/rpc
```

`verlet debug bind` answers why a thread has its effective configuration by
projecting its recorded `manifest.compile.completed` and
`manifest.bind.completed` receipts. It uses the same WebSocket endpoint
selection as `debug rpc`, and it can inspect the same thread offline from the
SQLite journal without a daemon:

```sh
cargo run --bin verlet -- debug bind <thread-id>
cargo run --bin verlet -- debug bind <thread-id> --json --url ws://127.0.0.1:49200/rpc
cargo run --bin verlet -- debug bind <thread-id> --journal .verlet/state/session_history.sqlite3
```

The command never resolves the manifest again or consults current daemon
defaults to fill old receipt gaps. Legacy origins that were not recorded print
as `[unrecorded]`.

## Authentication

Every RPC WebSocket connection resolves to a principal before any method is
dispatched, on both transports. There is no unauthenticated fallthrough: a
connection that fails to authenticate receives a uniform `401 Unauthorized`,
opens no JSON-RPC session, and is witnessed as a rejection. The standalone
app-server's unauthenticated `/healthz` and `/readyz` HTTP probes do not expose
RPC. The multi-instance host exposes only `/healthz`; see
[Config-driven multi-instance host](#config-driven-multi-instance-host). The
full design is ADR 0008 (`docs/adr/0008-identity-plane-v0.md`, including its
as-shipped addendum).

### Modes

`[daemon.identity]` in the daemon config used by `verlet daemon run` selects
the mode (see `daemon/daemon_config.rs`):

- `local` (default when the section is absent): a synthesized single-operator
  identity is projected into the app server, and a Unix-socket peer whose uid
  matches the daemon's effective uid resolves to that operator without a
  token, so `verlet` CLI usage on the host keeps working with no migration.
  The socket file is chmod `0o600` in every mode.
- `managed`: requires explicit non-blank `tenant_id` and `console_principal`;
  the daemon hard-fails at startup otherwise. It does not synthesize or
  validate an active principal record at startup. Bootstrap the configured
  principal in the daemon's state home before starting it. Peer mapping is
  disabled; every connection presents a credential.

The standalone TCP WebSocket listener remains loopback-only in both modes.
`verlet host run` separately permits an explicit `allow_non_loopback = true`
opt-in for a credential-authenticated private-network listener; loopback stays
the default. Standalone `verlet rpc` and `verlet console` construct local-mode
app servers; the console is a private local surface, not a client for a running
managed daemon.

### Config-driven multi-instance host

`verlet host run --config <host.toml>` boots multiple managed instances behind
one TCP listener. Each instance owns a standard `InstanceRoots::under(root)`
layout (`runtime`, `state`, `user-state`, `agents`, `blobs`, and `skills`) and
has its own identity authority. The host route table contains credential
digests only; raw access tokens do not belong in the host config, host command
line, host process environment, or logs.

```toml
[listen]
addr = "[::]:7900"
allow_non_loopback = true

[[instance]]
id = "orch"
root = "/data/instances/orch"
cwd = "/data/instances/orch/workspace"
tenant_id = "tenant-orch"
console_principal = "operator:orch"
hook_shell = "/bin/sh"
clock = true
route_digests = ["sha256:<64 lowercase hex characters>"]

[instance.provider]
provider = "bifrost_openai"
base_url = "http://bifrost.railway.internal:8080"
api_key_env = "VERLET_HOST_ORCH_PROVIDER_KEY"
model = "openai/gpt-5"
```

`listen.addr` must be a TCP socket address. `allow_non_loopback` defaults to
`false`; set it explicitly only when the authenticated listener is meant to be
reachable over a private network. Every instance requires a unique printable
`id`, absolute `root`, `cwd`, and `hook_shell`, non-blank `tenant_id` and
`console_principal`, non-overlapping roots (including aliases through existing
symlinked parents), and globally unique route digests in the exact
`sha256:<64 lowercase hex>` form printed by `identity mint`. Each instance runs
its own `clock.tick` route by default; set `clock = false` in that instance's
table to opt out. `local_offline` is the provider-free test/smoke mode. A
`bifrost_openai` provider requires `base_url`, `model`, and the name of a
non-empty environment variable in `api_key_env`; the variable value is read
once at boot and injected only into that instance.

Provision each instance before starting the host. Use the state home under its
configured root, capture tokens in the client/orchestrator secret store, and
copy only the printed digest into `host.toml`:

```sh
verlet identity bootstrap operator:orch \
  --display "Orch operator" \
  --state-home /data/instances/orch/state

verlet identity declare adapter:gateway \
  --kind adapter \
  --display "Gateway adapter" \
  --declared-by operator:orch \
  --state-home /data/instances/orch/state

verlet identity mint adapter:gateway \
  --minted-by operator:orch \
  --state-home /data/instances/orch/state
# stdout includes both:
# token <store-this-client-side>
# token_digest=sha256:<put-this-in-route_digests>

verlet host run --config /etc/verlet/host.toml
```

The host validates every entry and resolves every provider environment variable
before starting any instance. After boot it prints one liveness line containing
only the instance count and bound address. `GET /healthz` on the same listener
returns an unauthenticated empty `200 OK` for deployment probes; no other host
path bypasses authentication. SIGTERM and SIGINT drain listener, connection,
and instance clock tasks, shut down all instances, and exit successfully unless
shutdown fails.

### Bootstrap and credentials

Identity commands edit the offline session store, so stop the daemon first and
pass the same state home configured at `[daemon.runtime].state_home`:

```toml
[daemon.identity]
mode = "managed"
tenant_id = "tenant-acme"
console_principal = "operator:root"

[daemon.runtime]
state_home = "/var/lib/verlet/state"

[daemon.app_server]
listen = "ws://127.0.0.1:49200/rpc"
```

```sh
verlet identity bootstrap operator:root \
  --display "Root operator" \
  --state-home /var/lib/verlet/state

verlet identity declare adapter:gateway \
  --kind adapter \
  --display "Gateway adapter" \
  --declared-by operator:root \
  --state-home /var/lib/verlet/state

verlet identity mint adapter:gateway \
  --minted-by operator:root \
  --state-home /var/lib/verlet/state

verlet daemon config validate --config verlet.toml
verlet daemon run --config verlet.toml
```

`bootstrap` declares the first operator and mints its credential atomically.
`mint` prints the new token exactly once followed by
`token_digest=sha256:<hex>`; only the digest is stored. Capture the operator and
adapter tokens in the deployment's secret store. `identity list`
shows redacted records, and `revoke-credential` / `revoke-principal` retire
them. Revocation takes effect at the next connection; live sessions are not
torn down.

Verlet-owned daemon clients read the bearer token from the environment. For
example, an operator can connect the debug client with:

```sh
VERLET_APP_SERVER_TOKEN="$OPERATOR_TOKEN" \
  verlet debug rpc call thread/list --config verlet.toml
```

An adapter connects to `/rpc` with `Authorization: Bearer <adapter-token>` and
can call only Ingress methods (`turn/start` and `ingress/submit`) on an existing
operator-created thread. A missing,
unknown, expired, or revoked credential is refused during the upgrade with
`401 Unauthorized`. An authenticated principal that calls a method above its
authority receives JSON-RPC error `-32003`.

### Token carriers

- `Authorization: Bearer <token>` on the WebSocket upgrade, both transports.
  Header name and scheme are case-insensitive.
- The console uses the WebSocket subprotocol carrier: the client offers
  `verlet-console-token.<token>` and the server echoes exactly that
  recognized protocol. Query-parameter tokens are not accepted (they leak into
  logs). The console credential itself is minted by the server at
  construction: at most one is active per state home, a recorded predecessor
  is revoked at startup, and the server retires its own credential on graceful
  shutdown.
- MCP, ACP, and debug-RPC clients read `VERLET_APP_SERVER_TOKEN` from the
  environment (an explicit config field overrides it) and act with exactly the
  authority of the principal that token resolves to.

The pre-authentication handshake is bounded: a 10-second per-stage deadline
and an 8 KiB header cap, failing closed.

### Authorization

Every dispatched method has an authority class, checked once at the top of the
dispatcher against the principal's kind:

- `Host`: methods that execute commands, access the filesystem, touch provider
  or secret configuration, or construct/reconstruct runtime bindings
  (`command/*`, `fs/*`, the stateful `modelProvider/*` methods,
  `mcpSource/*`, `thread/start`, `thread/resume`, `approval/resolve`, ...).
  Operator-only.
- `Interactive`: reads and conversational control on existing threads
  (`thread/read`, `thread/list`, `stream/read`, `turn/steer`, `model/select`,
  `mandate/*`, ...).
- `Ingress`: input delivery. `turn/start` and `ingress/submit` are the only
  dispatched methods an adapter credential can call. They may
  lazily reconstruct a thread from its committed metadata (replaying the
  operator's standing grant), but adapter callers cannot supply the `cwd`,
  `model`, or `thinking` turn controls.

The normative method-to-class table is `DISPATCH_METHOD_AUTHORITY_CLASSES` in
`adapters/app_server/connection.rs`, pinned by an exhaustive drift test.
Unknown methods fail closed to Host. A refused request gets JSON-RPC error
`-32003` with a generic message that does not reveal whether the method
exists.

### What gets witnessed

Durable identity records (SQLite, same store as session history) capture:

- session open and close, with principal, surface, and credential reference;
- every failed authentication and every authority-class refusal, with the
  reason;
- every host-authority effect (the `HOST_EFFECT_METHODS` subset: command
  execution, filesystem mutations, provider/secret access, runtime
  construction, client-stream append, approval release), as a row naming
  session, principal, method, and timestamp, written before the effect runs. A
  failed witness write blocks the effect.

Ingress submitted over an authenticated RPC session is stamped
`via="caller:{session_id}"`, pointing at the witnessed session record.

### `getAuthStatus`

Params: `{ "includeToken": false, "refreshToken": false }`; both fields are
optional compatibility inputs and do not reveal a credential.

Result:
`{ "authMethod": null, "authToken": null, "requiresOpenaiAuth": false, "principalId": "operator:root", "kind": "operator" }`.
`principalId` and `kind` identify the resolved principal for this authenticated
session; `kind` is `operator` or `adapter`.

## Provider Config

The chat command can point its private app-server runtime at a live
provider endpoint. Put non-secret settings in a local `verlet.json`:

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
cargo run --bin verlet -- chat
```

Or pass the provider config on the command line:

```sh
cargo run --bin verlet -- chat \
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

AWS Bedrock Anthropic uses the Bedrock Runtime `InvokeModel` and
`InvokeModelWithResponseStream` paths with AWS SigV4 credentials:

```json
{
  "chat": {
    "provider": "anthropic_bedrock",
    "region": "us-east-1",
    "model": "global.anthropic.claude-sonnet-4-5-20250929-v1:0",
    "stream": true,
    "max_tokens": 4096
  }
}
```

The Sonnet 4.5 Bedrock raw model ID is
`anthropic.claude-sonnet-4-5-20250929-v1:0`, but Bedrock accounts can require
an inference profile ID. Verlet defaults to the documented global profile,
`global.anthropic.claude-sonnet-4-5-20250929-v1:0`; use a regional profile
such as `us.anthropic.claude-sonnet-4-5-20250929-v1:0` when data routing
requires it. `anthropic_bedrock` reads credentials from `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`, and
`AWS_BEDROCK_REGION`/`AWS_REGION`/`AWS_DEFAULT_REGION`. For the local shared
1Password item, run the child command through `scripts/with-bedrock-env.sh`.
Streaming uses `InvokeModelWithResponseStream` and decodes AWS
`application/vnd.amazon.eventstream` frames into the same Anthropic Messages
stream events as the native Anthropic adapter.

Provider config resolution is:

- `--config <file>`, otherwise `./verlet.json` if it exists;
- command-line flags override the config file;
- `--env-file <file>`, `VERLET_CHAT_ENV_FILE`, otherwise `./.env`;
- base URLs resolve from `base_url`, provider-specific local env, or command
  line flags;
- keys resolve from `api_key`, `api_key_env`, or provider-standard env vars
  such as `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, and AWS Bedrock env vars;
- models resolve from `model`, command line flags, or provider-specific local
  env;
- `max_tokens` defaults to `4096`;
- streaming defaults to enabled for hosted providers, including
  `anthropic_bedrock`; local/offline mode remains non-streaming.

Keep real secrets in the environment or a local ignored env file, not in
committed `verlet.json`.

The app-server opens project metadata at `state_home/metadata.sqlite3` and user
metadata at `user_state_home/metadata.sqlite3` on startup. Project metadata owns
provider catalog rows, MCP source records, and thread lifecycle/topology records
for local `thread/start`, fork paths, and kernel-spawned child threads created
through `verlet-threads`. User metadata owns provider credentials and named
secret values. Plain OpenAI Compatible config can use the catalog-backed
provider path while resolving API keys from the user auth store.

## Thread Residency

Verlet follows the Codex-shaped command surface here: the public continuation
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

## Thread Handle Dispatch

`thread/spawn` accepts `{ "threadId", "taskName", "message", "agentRef"?,
"placement"?, "workspace"?, "dispatchId"? }`. Because placement and workspace
authority attach to a manifest bind, either override requires `agentRef`;
supplying one without it is invalid params. The optional `dispatchId` is
generated when omitted. A retry with the same identity folds the parent control
stream and returns the original
`{ "handle": { "kind": "thread", "id": "..." }, "dispatchId": "..." }`
alongside the child thread fields; it does not append another spawn request or
start another child. The durable request continues to carry that identity in
its existing `correlation_id` field, as specified by ADR 0006.

`thread/submit` accepts `{ "threadId", "message", "dispatchId"? }`. Its local
lane binds the dispatch identity to the target turn reservation. For a remotely
placed child it instead lands the same identity in that child's store-hosted
queue; the child pulls it through the sync endpoint and admits it through its
own durable ingress lane. In both placements a retry returns the same turn
identity without injecting a second queued input, and a different payload under
the same identity is rejected.

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
without sharing active leaves. A workspace-bound source copies the exact
resolved workspace receipt metadata into the fork checkpoint, so the child
inherits the same guest path, canonical host path, and mode rather than
consulting a newer daemon default. The child also receives its own compile and
bind receipt pair; recovery never treats copied lifecycle metadata as authority
without that child-local witness.

`thread/rebindFork` is the borrowed-prefix/reference fork. It starts a child
thread bound to a new agent manifest and records a `ThreadBaseRef` so context
reads inherit the source prefix instead of copying the source entries into the
child store. Its result also includes `fork.sourceCut`, with
`{ "mode": "reference", ... }` plus the bound `agentRef` and `manifestHash`.

`parentThreadId` remains the topology/control parent. `forkedFromId` is kept as
a compatibility lineage alias on thread objects. New clients should use
`fork.sourceCut` as the exact split point. Daemon IO routes can also select
`fork_on_new_dm`; the bridge uses the same checkpoint fork lineage and records
the fork with `thread.spawned`.

## Manifest Starts

`thread/start` accepts `agentRef` for a published `agent://...` ref. The
app-server resolves aliases from its configured agent registry root
(`.verlet/agents` by default) and operation refs from its configured operation
registry root (`.verlet/operations` by default), selects a declared model
profile against the configured provider surface, applies allowed
`runtimeOverrides`, and records the immutable manifest ref in thread lifecycle
metadata. Relative agent registry roots and operation registry roots are
resolved against the configured runtime `cwd`, matching the root reported by
`config/read`.

`thread/start` parses an optional operator placement binding:
`{"placement":{"target":"local","executor_ref":null,"config":{}}}`. The
same additive `placement` parameter is accepted by manifest-binding
`thread/spawn` and `thread/rebindFork` calls. It overrides
`daemon.runtime.placement`; existing callers that omit it keep using the daemon
default. The model-visible `thread_spawn` tool remains
`{task_name, message, agent_ref}` and cannot select placement or a workspace.

The same methods accept an additive operator workspace override:
`{"workspace":{"hostPath":"/absolute/host/tree","mode":"rw"}}`. It overrides
`daemon.runtime.workspace`, but only when the selected manifest declares a
`[workspace]` requirement. Required-but-unbound and undeclared-but-supplied
bindings both fail closed, and `ro` cannot satisfy a manifest whose `min_mode`
is `rw`. Workspace is a binding-plane field only: it is not included in the
content-addressed manifest and is not exposed as a model-facing tool argument.
Local workspace bindings cannot be combined with `remote` or `sandbox`
placement in this slice.

The resolved mount is installed into the per-thread VFS at the declared guest
path. Bash remains `virtual_only`; no host-route shell is enabled. The existing
`HostFileSystem` backend canonicalizes the root, canonicalizes an existing path
or its nearest existing ancestor before I/O, rejects paths that resolve outside
the root, and refuses write-through of a symlink leaf. Read-write host mounts
also serialize operations for the same directory, including mounts reached
through filesystem aliases, and reject mutations of multiply-linked files so a
pre-existing hard link cannot write an inode outside the mount. Mount
construction rechecks the witnessed canonical root before installing the VFS
entry. Absolute virtual paths and `..` components therefore remain rooted in
the mounted tree, while symlinks to outside targets fail. Skill resources
retain their separate read-only `/skills` mount.

Remote execution in this slice is deliberately a conductor-to-child operation:
only `thread/spawn` executes a `remote` binding, and only after this daemon
generation has bound and started its configured sync endpoint. `thread/start`,
daemon routes, and `thread/rebindFork` reject a remote binding rather than
silently starting it in the local runtime. `sandbox` remains fail-closed.

At startup the app-server also publishes a kernel-synthesized default manifest
as `agent://verlet/default@latest`. Its envelope is the configured provider,
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
- `runtimeOverrides.maxToolRounds` accepts a positive integer or
  `"unlimited"` and is checked against the manifest's `max_tool_rounds`
  override allowlist entry. With no manifest value or override, the provider
  loop keeps its default cap of eight batches per turn.
- non-empty `capsuleBindings.operationNames` is rejected. Operations must be
  declared by the manifest and pass through publish/bind receipts, not injected
  at start time.

Manifest-backed starts atomically emit `manifest.compile.completed`,
`manifest.bind.completed`, and exactly one witnessed `placement.decision` on
the thread event stream before the first turn. The placement fact is derived
from the same effective binding stored on the bind receipt, including defaulted
local, so the receipt cannot commit without its witness. The compile and bind
events are discharged and include provenance. An `@latest` start includes the
alias resolution receipt in the compile event payload. A remote child records
the same bind receipt and unchanged `placement.decision` in its local stream
before that stream converges to the parent store. Without a served sync backend,
`remote` retains the missing-capability error; `sandbox` always does.
When present, the effective workspace mount is part of that same
`manifest.bind.completed` payload and atomic append. Thread lifecycle metadata
stores the resolved mount for factory reconstruction. Missing or corrupt
workspace metadata, and metadata that is absent from or disagrees with the
active durable bind receipt, recovers as unbound and cannot fall through to the
daemon's current default. The witness check runs before runtime construction, so
untrusted lifecycle metadata is never mounted first and re-witnessed later. A
manifest that requires the mount consequently remains unloadable until
explicitly rebound through a new bind path.

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

At startup, the app-server synthesizes first-party kernel operation records into
the configured registry: `verlet-threads`, `verlet-schedule`,
`verlet-process`, and `verlet-notify`. The default manifest binds the
thread-control package only; agents that need other first-party operations bind
the corresponding `op://...@sha256:<record-hash>` explicitly.

### `model/list`

Params: none.

Result: `{ "data": [...], "nextCursor": null }`. The list composes every model
from every provider in the project metadata store with catalog metadata (the
checked-in models.dev snapshot plus its last valid background refresh) for the
same providers; project metadata wins when the same provider/model pair appears
in both sources. Catalog providers without a store record do not appear here;
the setup wizard discovers them through `modelProvider/catalog`. The
launch-configured provider/model is appended only while it is the active
selection, absent from the composed list, and either explicitly requested at
launch or backed by a store record; the default offline echo pair therefore
stays hidden on a fresh install, though submitting turns against it still
works.
Each entry includes `providerId`, `model`, `displayName`, `authStatus`
(`configured`, `env`, or `missing`), and `active`. Compatibility fields such
as `id` and `isDefault` remain present; `isDefault` follows the session's active
selection.

Auth status uses the same user credential store and environment resolution as
`verlet auth status`. The active selection is process-local runtime state.

### `model/select`

Params: `{ "providerId": "wafer", "model": "wafer-model" }`.

Result: `{ "active": { "providerId": "wafer", "model": "wafer-model" } }`.
The pair must exist in `model/list`, and its provider must have a stored
credential or satisfied environment credential. Validation and provider-client
construction complete before the selection changes, so a failed call changes
nothing and returns JSON-RPC `-32602` with the provider or credential problem.
Store-backed selection supports OpenAI Chat Completions, OpenAI Responses, and
Anthropic Messages provider APIs. The `openai-codex` provider id always uses
the dedicated ChatGPT-plan OAuth client regardless of its stored API value.
Its resolved `baseUrl` must be the canonical
`https://chatgpt.com/backend-api/codex/responses` endpoint; loopback URLs with
the same path are accepted for local endpoint tests.

Selection applies to turns started after the call. A turn already running keeps
the endpoint it resolved at its own start, including subsequent tool rounds.
Selection is not persisted: restarting the app-server restores the provider and
model from launch configuration.

### `modelProvider/list`

Params: none.

Result: `{ "data": [...], "nextCursor": null }`. Each entry is a redacted model
provider endpoint record: `providerId`, `api`, `baseUrl`, optional
`displayName`, redacted `auth`, `authHeader`, redacted `headers`, `models`,
`metadata`, timestamps, `configuredAuth` from the user auth store, and
`isActiveProvider`. Model rows include `modelId`, optional model-level
`api`/`baseUrl`, token limits, input modalities, redacted headers, metadata,
and `isDefault` when it matches the runtime-active model.

### `modelProvider/read`

Params: `{ "providerId": "wafer" }`.

Result: `{ "provider": { ... } }` with the same redacted shape as
`modelProvider/list`. Unknown provider ids fail with a JSON-RPC error.

### `modelProvider/upsert`

Params: `{ "provider": { "providerId": "...", "api": "open_ai_chat_completions",
"baseUrl": "https://...", "displayName": "...", "auth": { "type": "env",
"name": "PROVIDER_API_KEY" }, "authHeader": true, "headers": { "X-Provider":
{ "type": "literal", "value": "..." } }, "models": [...], "metadata": {} } }`.

Result: `{ "provider": { ... } }` with the redacted stored record. This method
creates or replaces provider metadata only. It rejects inline API keys and
command-backed auth or header values; use `modelProvider/auth/set` for stored
credentials. A catalog provider backing the active model cannot be replaced;
select another provider first so the active endpoint and stored metadata cannot
diverge.

### `modelProvider/delete`

Params: `{ "providerId": "wafer" }`.

Result: `{ "deleted": true, "providerId": "wafer" }`. Deleting a provider also
removes any user-stored credential for the same provider id and clears stale
project-store credential rows if present. A catalog provider backing the active
model cannot be deleted until another provider is selected.

### `modelProvider/auth/status`

Params: `{ "providerId": "wafer" }`, or `{}` to list all provider auth statuses.

Result: `{ "auth": { ... } | null, "data": [...], "nextCursor": null }`.
Entries report `providerId`, optional `displayName`, whether a credential is
configured, its non-secret source/label, and whether the provider uses an auth
header. Providers stored with `auth: none` (keyless local endpoints) report
`configured: true` with source `none`. Credential values are never returned.

A single-provider query for a catalog provider with no store record answers
`configured: false` instead of not-found, so clients can probe providers
before the first `modelProvider/auth/set`. Ids known to neither the store nor
the catalog keep the not-found error.

### `modelProvider/auth/set`

Params: `{ "providerId": "wafer", "apiKey": "..." }`.

Result: `{ "auth": { ... } }` with the same redacted auth status returned by
`modelProvider/auth/status`. The method rejects OAuth-only providers
(`openai-codex`); use `modelProvider/auth/setOAuth`.

When the provider id exists in the catalog but has no store record yet, the
record is created on demand from the catalog template (base URL, API family,
display name, and non-deprecated catalog models with the default marked)
before the credential is stored, so connecting a catalog provider needs no
prior `modelProvider/upsert`. If storing the credential or rebuilding the
active endpoint fails, the created record is rolled back.

### `modelProvider/auth/setOAuth`

Params: `{ "providerId": "openai-codex", "access": "...", "refresh": "...", "expiresAtMs": 1720000000000, "accountId": "...", "email": "..." }`.
`accountId` and `email` are optional. Access and refresh tokens must be
non-empty. The method accepts only OAuth-backed providers; use
`modelProvider/auth/set` for API-key providers.

Result: `{ "auth": { ... } }` with the same redacted auth status returned by
`modelProvider/auth/status`. Token and account values are never returned.

`modelProvider/auth/set`, `modelProvider/auth/setOAuth`, and
`modelProvider/auth/delete` serialize with `model/select`. When they target the
active catalog provider, the app-server eagerly rebuilds and atomically replaces
the future-turn endpoint. If rebuilding fails, the previous credential is
restored; an in-flight turn keeps its existing snapshot.

`modelProvider/auth/delete` also removes the provider record itself when the
record was created on demand from the catalog and still matches today's
template, returning the catalog entry to its unconfigured state. A record the
user modified, and any custom record, survives credential deletion.

### `modelProvider/catalog`

Params: none (an empty object is accepted).

Result: `{ "providers": [...] }`. A read-only merge of the models.dev provider
catalog (checked-in snapshot plus its last valid background refresh) with
provider-store state; it is the chat setup wizard's data source and never
writes provider records or credentials. Each entry carries `providerId`,
`displayName`, `baseUrl`, `api`, `authKind` (`api_key`, or `oauth` for
`openai-codex`), `envVars`, `docUrl`, `modelCount`, `defaultModel` (the store
record's default-flagged model when a record exists, else the first catalog
model in sorted order), `configured`/`authSource` (`stored`, `env`, `oauth`,
`none` for keyless records, or null)/`authLabel` from the same auth-status
resolution as `modelProvider/auth/status`, `custom` (true for store records
without a catalog entry), and `active` (the provider of the runtime-active
model). Configured providers sort first, then the active provider, then
alphabetical display names. Credential values are never returned.

Provider base URLs, API families, and auth kinds ship only in the reviewed
checked-in snapshot. The background refresh updates model metadata and
non-endpoint provider display metadata for providers already in that snapshot.
Providers found only by the refresh do not enter the catalog provider view.
At startup, records materialized by older catalog versions are reconciled
before endpoint construction: reviewed provider endpoints are re-pinned, and
refresh-only provider records are retired. Their separately stored credentials
are preserved so an operator can recover them through an explicit custom
provider configuration.

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
"isError": false }`. This is a setup/test call for UI flows; attaching a source
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

### `ingress/submit`

Params:
`{ "threadId": "...", "input": [...], "delivery": { "deliveryId": "...", "attempt": 1, "metadata": {} }, "dedupeKey": { "scope": "...", "key": "..." }, "correlationId": "...", "tier": "attested" }`.
`threadId`, `input`, and `delivery` are required. `input` has the same array
shape as `turn/start`. `tier` defaults to `attested`; `recorded` is reserved
until the foreign-harness lane exists.

Result:
`{ "ingressEventId": "...", "deduped": false, "admission": { "decision": "queue", "admissible": true } }`.
The boundary builds an attributed ADR 0007 envelope, witnesses
`io.ingress.received`, durably appends `admission.decided`, and only then
schedules the turn. Redelivery dedupes on the envelope's effective key and
returns the original ingress event id without scheduling another turn.

### `stream/append`

Params:
`{ "stream": "client:orch:placement", "records": [{ "kind": "placement.bound", "payloadSchema": "verlet.orch.placement.bound/1", "payload": {} }], "expectedSequence": 1 }`.
The `client:` stream id, non-empty record batch, lowercase dotted kind, and
declared non-`cooldis.*` schema id are validated before any append.
`expectedSequence` is optional and fences the client stream's next sequence.

Result:
`{ "streamId": "client:orch:placement", "records": [{ "eventId": "...", "sequence": 1 }] }`.
The batch is atomic and principal-attributed. A stale sequence fence returns
JSON-RPC error `-32004` with data `{ "expected": 1, "actual": 2 }`. A daemon
whose placement lease has been superseded returns `-32005`, message
`journal lease epoch is stale`, and data
`{ "streamId": "client:orch:placement", "presentedEpoch": 7, "minimumEpoch": 8 }`;
the orchestrator must treat that as a rehome signal rather than retrying the
stale daemon. This is a Host effect, so the session host-effect witness must
commit before validation or append execution begins.

### `stream/read`

Params:
`{ "stream": "client:orch:placement", "streamCursor": {...}, "limit": 100, "kinds": ["placement.bound"] }`.
Only `client:` stream ids are accepted. `limit` defaults to `100` and is
clamped to `1..=500`; `kinds` matches the client-declared kind exactly. A
cursor is verified against the requested stream id, sequence, and event id.

Result: `{ "data": [...], "streamCursor": {...} | null }`. Each row is a
`cooldis.stream.record/1` envelope whose `kind`, `payload_schema`, and
`payload` are the client-declared values, with the carrier's sequence and event
id. `principal_id` identifies the writer. Client streams never schedule work
and are excluded from startup recovery scans.

### `thread/list`

Params: none.

Result: `{ "data": [...], "nextCursor": null, "backwardsCursor": null }`.
Entries include root threads, app-server-created child threads, and child
threads spawned by `verlet-threads/thread_spawn`. Child entries populate both
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

#### Resumable receipts retrieval

Metering and run-outcome consumers enumerate threads with `thread/list`, then
tail each thread through `thread/events/list` with a per-stream
`streamCursor`. Filter for `session.entry.appended` (assistant-message usage),
`turn.completed` (turn outcomes), and `io.egress.delivered` /
`io.egress.failed` (egress receipts). Persist the consumer's cursor map as an
ordinary record in its own `client:` stream. Ephemeral `turn/usage`
notifications are display hints, not the metering source of truth.

### `mandate/start`

Params:
`{ "threadId": "...", "schedule": { "interval": { "every_ms": 60000 } }, "maxOccurrences": 3, "catchUp": "skip_missed", "inputTemplate": "Continue with the reminder.", "expiresAt": "2026-07-04T20:00:00Z" }`.
Only `threadId` and `schedule` are required. The schedule union is externally
tagged: `{ "cron": { "expr": "0 9 * * *", "tz": "America/Los_Angeles" } }`,
`{ "interval": { "every_ms": 60000 } }`, or
`{ "at": { "when": "2026-07-04T18:00:00Z" } }`. `catchUp` defaults to
`"skip_missed"` and may also be `"coalesce_missed"`. `expiresAt` is an
optional absolute RFC3339 UTC instant. An already expired mandate is rejected;
after a live mandate passes that instant, its next continuation request is
rejected and the lapse is witnessed on the control stream.

Result:
`{ "mandateEventId": "...", "streamId": "control:<threadId>", "sequence": 1 }`.
The method appends a witnessed `mandate.started` fact to the thread control
stream. Cron expressions are parsed before append, cron time zones must be
IANA names, interval schedules must be at least 60 seconds, and `at` schedules
in the past are rejected unless `catchUp = "coalesce_missed"`.

### `mandate/list`

Params: `{ "threadId": "..." }`.

Result:
`{ "data": [{ "mandateEventId": "...", "mandateId": "...", "threadId": "...", "schedule": { "interval": { "every_ms": 60000 } }, "maxOccurrences": 3, "catchUp": "skip_missed", "inputTemplate": "Continue with the reminder.", "createdAtMs": 0, "streamId": "control:<threadId>", "sequence": 1 }], "nextCursor": null }`.
The projection folds active `mandate.started` facts minus matching
`mandate.revoked` facts from the thread control stream.

### `mandate/revoke`

Params: `{ "threadId": "...", "mandateEventId": "..." }`.

Result:
`{ "status": "revoked" | "already_revoked", "mandateEventId": "...", "revokedEventId": "...", "streamId": "control:<threadId>", "sequence": 2 }`.
The method appends a witnessed `mandate.revoked` fact linked to the start event.
Revoking an already revoked mandate is an idempotent no-op success and returns
the original revoke event id.

### `thread/couplings/list`

Params: `{ "threadId": "...", "limit": 100 }`.
Only `threadId` is required. `limit` defaults to `100` and is clamped to
`1..=500`.

Result:
`{ "data": [...], "nextCursor": null, "agentRef": "...", "manifestHash": "...", "bindEventId": "..." }`.
V1 projects bound couplings from the latest `manifest.bind.completed` receipt
for the thread. Each row includes id, inferred role, trigger kind/match,
source streams/kinds, sink stream/kinds, function ref, artifact hash, optional
operation name, budget, and config hash. This is an inspection surface:
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

This method is the V1 support evidence bundle surface. It is not a browser-safe
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
- `account/read`, `getAuthStatus`;
- `agent/list`, `agent/read`, `operation/list`, `model/list`;
- `mcpSource/list`, `mcpSource/read`, `mcpSource/upsert`,
  `mcpSource/discover`, `mcpSource/delete`, `mcpSource/testTool`,
  `mcpSource/manifestPatch`;
- `thread/start`, `thread/spawn`, `thread/submit`, `thread/resume`, `thread/fork`, `thread/read`,
  `thread/list`, `thread/loaded/list`, `thread/events/list`,
  `thread/couplings/list`, `thread/approvals/list`, `thread/waiting/list`,
  `thread/debug/export`;
- `mandate/start`, `mandate/revoke`, `mandate/list`;
- `approval/resolve`;
- `ingress/submit`, `stream/append`, `stream/read`;
- `thread/name/set`, `thread/metadata/update`, `thread/compact/start`,
  `thread/unsubscribe`;
- `turn/start`, `turn/steer`, `turn/interrupt`.

Schema-valid friendly stubs keep the client calm for catalog/config surfaces
such as `skills/list`, `plugin/list`, `hooks/list`,
`account/rateLimits/read`, `config/read`, and `configRequirements/read`.
`hooks/list` returns `witnessing: true` to report that mutating host debug hook
outcomes are witnessed before they take effect; it does not expose a manifest
hook catalog.
`mcpServerStatus/list` remains as a compatibility alias for `mcpSource/list`.
`config/read` additionally reports the app-server's
absolute working directory as `config.cwd`, so clients can discover a real
root for the filesystem read methods without out-of-band configuration.

Unsupported noncritical methods return JSON-RPC `-32601` with the method name.

Runtime events are projected back into Codex-shaped notifications, including:

- `thread/started`, `thread/status/changed`, and `thread/closed`;
- `thread/resync/started`, `thread/resynced`, and `thread/resync/failed` for
  explicit broadcast-lag recovery;
- `turn/started`, `turn/completed`, and `error`;
- `item/agentMessage/delta` for streamed assistant text;
- `item/agentThinking/delta` for streamed thinking text;
- final `item/completed` messages for completed assistant output.

### Broadcast lag recovery

Thread runtime events use a bounded broadcast channel. If a watcher falls
behind, it does not silently skip the lost events. It first emits:

```json
{
  "method": "thread/resync/started",
  "params": {
    "threadId": "...",
    "reason": "broadcastLag",
    "laggedEvents": 76
  }
}
```

Clients must treat incremental thread notifications as non-authoritative after
this marker. Once the runtime reaches a quiescent `idle`, `stopped`, or `failed`
status, the watcher rebuilds the active turn from durable session messages and
emits one replacement snapshot:

```json
{
  "method": "thread/resynced",
  "params": {
    "threadId": "...",
    "reason": "broadcastLag",
    "laggedEvents": 76,
    "thread": { "id": "...", "turns": [] }
  }
}
```

`params.thread` has the same full shape as `thread/read` with
`includeTurns: true`. `laggedEvents` is the saturated total reported by the
broadcast receiver for the lag episode. Clients should replace their local
thread projection with this snapshot before applying later incremental
notifications.

If durable truth cannot be read, recovery fails closed and the watcher emits
`thread/resync/failed` with
`{ threadId, reason: "broadcastLag", laggedEvents, error: { code:
"resync_failed", message } }` instead of claiming that the incomplete
projection is synchronized. The watcher then stops; a client can explicitly
resume or re-read the thread after the underlying error is resolved.

## Verification

Run the normal Rust suite:

```sh
cargo test
```

Run the cheap app-server smoke:

```sh
cargo run --bin verlet-app-server-smoke
```

Run the workbench query-surface smoke:

```sh
cargo run --bin verlet-workbench-smoke
```

Run the focused MCP server tests:

```sh
cargo test mcp_server
```

Run a one-shot local/offline chat proof:

```sh
cargo run --bin verlet -- chat "hello from local chat"
```

Run an OpenAI Responses-compatible chat proof with a local env file:

```sh
cargo run --bin verlet -- chat \
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
cargo run --bin verlet -- chat \
  --provider openai_chat_completions \
  --base-url https://api.openai.com \
  --api-key-env OPENAI_API_KEY \
  --no-stream \
  "Reply with exactly COOL_CHAT_COMPLETIONS_OK and no other text."
```

Run an Anthropic Messages proof:

```sh
cargo run --bin verlet -- chat \
  --provider anthropic \
  --api-key-env ANTHROPIC_API_KEY \
  --model claude-sonnet-4-5-20250929 \
  "Reply with exactly COOL_CHAT_ANTHROPIC_OK and no other text."
```

Run an Anthropic Bedrock proof:

```sh
scripts/with-bedrock-env.sh cargo run --bin verlet -- chat \
  --provider anthropic_bedrock \
  --model global.anthropic.claude-sonnet-4-5-20250929-v1:0 \
  "Reply with exactly COOL_CHAT_BEDROCK_OK and no other text."
```
