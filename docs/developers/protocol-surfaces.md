# Protocol Surfaces

Verlet exposes one native control plane and two compatibility adapters.

- **Verlet RPC** is the native daemon and app-server control plane.
- **ACP** lets ACP hosts launch Verlet as a stdio coding agent.
- **MCP** has two directions: external MCP clients can use Verlet through
  `verlet-mcp-server`, and Verlet can register remote MCP servers as tool
  sources for agents.

These surfaces share the same runtime. They do not create separate schedulers.
Manifest binding, provider configuration, operation registries, attachments,
durable thread state, and receipts stay in Verlet.

The examples below assume installed binaries. From a source checkout, replace
`verlet ...` with `cargo run --bin verlet -- ...`, and replace adapter
binaries with `cargo run --bin verlet-acp-agent -- ...` or
`cargo run --bin verlet-mcp-server -- ...`.

## Surface Map

| Surface | Use it when | Boundary | Runtime lowering |
| --- | --- | --- | --- |
| Verlet RPC | You are building a Verlet-native client, workbench, daemon integration, or test harness | WebSocket or Unix-socket JSON-RPC | `thread/*`, `turn/*`, `agent/*`, `operation/*`, `mcpSource/*`, `fs/*`, `command/exec` |
| ACP agent | An ACP host or editor wants to run Verlet beside other ACP-compatible coding agents | stdio JSON-RPC | `session/new` becomes `thread/start`; `session/prompt` becomes `turn/start`; `session/cancel` becomes `turn/interrupt` |
| MCP server | An MCP client wants to call Verlet | stdio MCP | Fixed MCP tools proxy to Verlet RPC methods |
| MCP source registration | A Verlet agent should use a remote MCP server | CLI or RPC setup methods | Source records are stored, discovered, tested, and later attached by manifest `protocol_tool_import` |

## Native RPC

The native RPC surface is the most complete protocol surface. It exposes thread
lifecycle, turn control, manifest-backed starts, MCP source setup, model and
config introspection, filesystem helpers, and command execution.

### Dry Path

The dry path uses the deterministic local provider. It is useful for protocol
checks and docs examples because it does not need a model API key.

Start a temporary WebSocket RPC server:

```sh
ROOT="$(mktemp -d /tmp/verlet-rpc.XXXXXX)"
URL="ws://127.0.0.1:49200/rpc"
mkdir -p "$ROOT/workspace"

verlet rpc \
  --listen "$URL" \
  --runtime-home "$ROOT/runtime" \
  --state-home "$ROOT/state" \
  --cwd "$ROOT/workspace"
```

In another terminal, call the daemon:

```sh
URL="ws://127.0.0.1:49200/rpc"
verlet debug rpc call thread/list --url "$URL"
verlet debug rpc turn --new --url "$URL" "hello from rpc"
```

Expected dry response text starts with `local:`.

### Live Provider Path

The live path uses `verlet daemon run` with a provider config. This example is
provider-neutral and uses an OpenAI Chat Completions-compatible endpoint:

```toml
[daemon.runtime]
cwd = "."
runtime_home = ".verlet/runtime"
state_home = ".verlet/state"

[daemon.app_server]
listen = "ws://127.0.0.1:49200/rpc"

[daemon.provider]
provider = "openai_chat_completions"
base_url = "https://api.openai.com"
api_key_env = "OPENAI_API_KEY"
model = "gpt-4.1-mini"
stream = true
max_tokens = 4096
```

Run the daemon:

```sh
verlet daemon run --config verlet.toml
```

Then submit a real model-backed turn:

```sh
verlet debug rpc turn --new \
  --url ws://127.0.0.1:49200/rpc \
  "Reply with exactly: VERLET_RPC_LIVE_OK"
```

## ACP Agent

`verlet-acp-agent` is for ACP hosts. It is intentionally narrower than native
RPC. It can create sessions, submit prompts, stream updates, cancel active
prompts, close sessions, and expose session-local config options.

ACP does not own provider credentials, operation registry mutation, sandbox
policy, or Verlet permission policy.

### Dry Path

Start a local RPC server on a Unix socket:

```sh
ROOT="$(mktemp -d /tmp/verlet-acp.XXXXXX)"
SOCKET="/tmp/verlet-acp.sock"
rm -f "$SOCKET"
mkdir -p "$ROOT/workspace"

verlet rpc \
  --listen "unix://$SOCKET" \
  --runtime-home "$ROOT/runtime" \
  --state-home "$ROOT/state" \
  --cwd "$ROOT/workspace"
```

Configure an ACP host to launch the adapter:

```json
{
  "protocol": "acp",
  "transport": {
    "type": "stdio",
    "command": "verlet-acp-agent",
    "args": [
      "--socket",
      "/tmp/verlet-acp.sock",
      "--cwd",
      "/path/to/workspace",
      "--timeout-ms",
      "30000"
    ]
  }
}
```

From a source checkout, the process smoke exercises
`initialize -> session/new -> session/prompt -> session/close` over the real
stdio binary:

```sh
cargo test -p verlet --test acp_agent_process_smoke
```

### Live Provider Path

Use the same ACP adapter command, but point it at a provider-backed daemon:

```sh
# Use a daemon config whose app-server listen value is:
# listen = "unix:///tmp/verlet-acp.sock"
verlet daemon run --config verlet.toml

verlet-acp-agent \
  --socket /tmp/verlet-acp.sock \
  --cwd /path/to/workspace \
  --timeout-ms 180000
```

The ACP host still sends `initialize`, `session/new`, and `session/prompt`.
Only the daemon provider config changes. A good live smoke prompt is:

```text
Reply with exactly this single token and no other text: VERLET_ACP_LIVE_OK
```

## MCP Server

Run `verlet-mcp-server` when an external MCP client should use Verlet.
The server speaks MCP over stdio and proxies fixed tools to the Verlet daemon.

### Dry Path

Start a local daemon on a Unix socket:

```sh
ROOT="$(mktemp -d /tmp/verlet-mcp.XXXXXX)"
SOCKET="/tmp/verlet-mcp.sock"
rm -f "$SOCKET"
mkdir -p "$ROOT/workspace"

verlet rpc \
  --listen "unix://$SOCKET" \
  --runtime-home "$ROOT/runtime" \
  --state-home "$ROOT/state" \
  --cwd "$ROOT/workspace"
```

Configure an MCP client:

```toml
[mcp_servers.verlet]
command = "verlet-mcp-server"
args = ["--socket", "/tmp/verlet-mcp.sock"]
```

From a source checkout, run the MCP server contract tests:

```sh
cargo test -p verlet mcp_server --lib
```

The MCP tools include:

- `verlet_daemon_status`
- `verlet_thread_start`
- `verlet_thread_list`
- `verlet_thread_read`
- `verlet_turn_start`
- `verlet_turn_wait`
- `verlet_turn_interrupt`
- `verlet_prompt`
- `verlet_command_exec`
- `verlet_capsule_binding_set`
- `verlet_capsule_binding_delete`
- `verlet_capsule_binding_list`
- `verlet_capsule_binding_resolve`

### Live Provider Path

Use the same MCP server command against a provider-backed daemon:

```sh
# Use a daemon config whose app-server listen value is:
# listen = "unix:///tmp/verlet-mcp.sock"
verlet daemon run --config verlet.toml
verlet-mcp-server --socket /tmp/verlet-mcp.sock
```

An MCP client can then call the `verlet_prompt` tool with a real model prompt:

```json
{
  "name": "verlet_prompt",
  "arguments": {
    "message": "Reply with exactly: VERLET_MCP_LIVE_OK"
  }
}
```

## MCP Source Registration

Use MCP source registration when Verlet should use someone else's MCP server.
This is the opposite direction from `verlet-mcp-server`.

Registering a source does not attach it to every agent. Agent manifests still
opt into remote MCP sources with `protocol_tool_import`.

### Dry Path

Store, inspect, and remove a source record without contacting a remote server:

```sh
ROOT="$(mktemp -d /tmp/verlet-mcp-source.XXXXXX)"

verlet tool source add docs-demo \
  --kind mcp-http \
  --url http://127.0.0.1:9/mcp \
  --include-tool search \
  --state-home "$ROOT/state"

verlet tool source list --json --state-home "$ROOT/state"
verlet tool source show docs-demo --json --state-home "$ROOT/state"
verlet tool source remove docs-demo --state-home "$ROOT/state"
```

The equivalent RPC setup methods are:

```text
mcpSource/list
mcpSource/read
mcpSource/upsert
mcpSource/discover
mcpSource/delete
mcpSource/testTool
mcpSource/manifestPatch
```

### Live Remote-MCP Path

For a real remote MCP server, the CLI can add the source, discover its tools,
and inspect the stored record:

```sh
verlet tool source add search \
  --kind mcp-http \
  --url https://example.com/mcp \
  --bearer-secret mcp.search.bearer \
  --include-tool search

verlet tool source discover search
verlet tool source show search --json
```

Use the native RPC methods for setup-time tool testing and manifest patch
preview:

```text
mcpSource/upsert
-> mcpSource/discover
-> mcpSource/testTool
-> mcpSource/manifestPatch
-> publish an agent manifest with protocol_tool_import
```

## Choosing A Surface

Use native RPC when you need Verlet runtime control. Use ACP when an editor or
agent host expects an ACP agent process. Use `verlet-mcp-server` when an MCP
client should call Verlet. Use MCP source registration when a Verlet agent
should call a remote MCP server.

When testing a protocol integration, run both paths:

- dry/local path first, to prove framing, process launch, and JSON shape;
- live-provider path second, to prove the real model lifecycle and streaming
  behavior.
