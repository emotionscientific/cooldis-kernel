# Protocol Surfaces

Cooldis exposes one native control plane and two compatibility adapters.

- **Cooldis RPC** is the native daemon and app-server control plane.
- **ACP** lets ACP hosts launch Cooldis as a stdio coding agent.
- **MCP** has two directions: external MCP clients can use Cooldis through
  `cooldis-mcp-server`, and Cooldis can register remote MCP servers as tool
  sources for agents.

These surfaces share the same runtime. They do not create separate schedulers.
Manifest binding, provider configuration, operation registries, grants,
durable thread state, and receipts stay in Cooldis.

The examples below assume installed binaries. From a source checkout, replace
`cooldis ...` with `cargo run --bin cooldis -- ...`, and replace adapter
binaries with `cargo run --bin cooldis-acp-agent -- ...` or
`cargo run --bin cooldis-mcp-server -- ...`.

## Surface Map

| Surface | Use it when | Boundary | Runtime lowering |
| --- | --- | --- | --- |
| Cooldis RPC | You are building a Cooldis-native client, workbench, daemon integration, or test harness | WebSocket or Unix-socket JSON-RPC | `thread/*`, `turn/*`, `agent/*`, `operation/*`, `mcpSource/*`, `fs/*`, `command/exec` |
| ACP agent | An ACP host or editor wants to run Cooldis beside other ACP-compatible coding agents | stdio JSON-RPC | `session/new` becomes `thread/start`; `session/prompt` becomes `turn/start`; `session/cancel` becomes `turn/interrupt` |
| MCP server | An MCP client wants to call Cooldis | stdio MCP | Fixed MCP tools proxy to Cooldis RPC methods |
| MCP source registration | A Cooldis agent should use a remote MCP server | CLI or RPC setup methods | Source records are stored, discovered, tested, and later attached by manifest `protocol_tool_import` |

## Native RPC

The native RPC surface is the most complete protocol surface. It exposes thread
lifecycle, turn control, manifest-backed starts, MCP source setup, model and
config introspection, filesystem helpers, and command execution.

### Dry Path

The dry path uses the deterministic local provider. It is useful for protocol
checks and docs examples because it does not need a model API key.

Start a temporary WebSocket RPC server:

```sh
ROOT="$(mktemp -d /tmp/cooldis-rpc.XXXXXX)"
URL="ws://127.0.0.1:49200/rpc"
mkdir -p "$ROOT/workspace"

cooldis rpc \
  --listen "$URL" \
  --runtime-home "$ROOT/runtime" \
  --state-home "$ROOT/state" \
  --cwd "$ROOT/workspace"
```

In another terminal, call the daemon:

```sh
URL="ws://127.0.0.1:49200/rpc"
cooldis debug rpc call thread/list --url "$URL"
cooldis debug rpc turn --new --url "$URL" "hello from rpc"
```

Expected dry response text starts with `local:`.

### Live Provider Path

The live path uses `cooldis daemon run` with a provider config. This example is
provider-neutral and uses an OpenAI Chat Completions-compatible endpoint:

```toml
[daemon.runtime]
cwd = "."
runtime_home = ".cooldis/runtime"
state_home = ".cooldis/state"

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
cooldis daemon run --config cooldis.toml
```

Then submit a real model-backed turn:

```sh
cooldis debug rpc turn --new \
  --url ws://127.0.0.1:49200/rpc \
  "Reply with exactly: COOLDIS_RPC_LIVE_OK"
```

## ACP Agent

`cooldis-acp-agent` is for ACP hosts. It is intentionally narrower than native
RPC. It can create sessions, submit prompts, stream updates, cancel active
prompts, close sessions, and expose session-local config options.

ACP does not own provider credentials, operation registry mutation, sandbox
policy, or Cooldis permission policy.

### Dry Path

Start a local RPC server on a Unix socket:

```sh
ROOT="$(mktemp -d /tmp/cooldis-acp.XXXXXX)"
SOCKET="/tmp/cooldis-acp.sock"
rm -f "$SOCKET"
mkdir -p "$ROOT/workspace"

cooldis rpc \
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
    "command": "cooldis-acp-agent",
    "args": [
      "--socket",
      "/tmp/cooldis-acp.sock",
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
cargo test -p cooldis --test acp_agent_process_smoke
```

### Live Provider Path

Use the same ACP adapter command, but point it at a provider-backed daemon:

```sh
# Use a daemon config whose app-server listen value is:
# listen = "unix:///tmp/cooldis-acp.sock"
cooldis daemon run --config cooldis.toml

cooldis-acp-agent \
  --socket /tmp/cooldis-acp.sock \
  --cwd /path/to/workspace \
  --timeout-ms 180000
```

The ACP host still sends `initialize`, `session/new`, and `session/prompt`.
Only the daemon provider config changes. A good live smoke prompt is:

```text
Reply with exactly this single token and no other text: COOLDIS_ACP_LIVE_OK
```

## MCP Server

Run `cooldis-mcp-server` when an external MCP client should use Cooldis.
The server speaks MCP over stdio and proxies fixed tools to the Cooldis daemon.

### Dry Path

Start a local daemon on a Unix socket:

```sh
ROOT="$(mktemp -d /tmp/cooldis-mcp.XXXXXX)"
SOCKET="/tmp/cooldis-mcp.sock"
rm -f "$SOCKET"
mkdir -p "$ROOT/workspace"

cooldis rpc \
  --listen "unix://$SOCKET" \
  --runtime-home "$ROOT/runtime" \
  --state-home "$ROOT/state" \
  --cwd "$ROOT/workspace"
```

Configure an MCP client:

```toml
[mcp_servers.cooldis]
command = "cooldis-mcp-server"
args = ["--socket", "/tmp/cooldis-mcp.sock"]
```

From a source checkout, run the MCP server contract tests:

```sh
cargo test -p cooldis mcp_server --lib
```

The MCP tools include:

- `cooldis_daemon_status`
- `cooldis_thread_start`
- `cooldis_thread_list`
- `cooldis_thread_read`
- `cooldis_turn_start`
- `cooldis_turn_wait`
- `cooldis_turn_interrupt`
- `cooldis_prompt`
- `cooldis_command_exec`
- `cooldis_capsule_binding_set`
- `cooldis_capsule_binding_delete`
- `cooldis_capsule_binding_list`
- `cooldis_capsule_binding_resolve`

### Live Provider Path

Use the same MCP server command against a provider-backed daemon:

```sh
# Use a daemon config whose app-server listen value is:
# listen = "unix:///tmp/cooldis-mcp.sock"
cooldis daemon run --config cooldis.toml
cooldis-mcp-server --socket /tmp/cooldis-mcp.sock
```

An MCP client can then call the `cooldis_prompt` tool with a real model prompt:

```json
{
  "name": "cooldis_prompt",
  "arguments": {
    "message": "Reply with exactly: COOLDIS_MCP_LIVE_OK"
  }
}
```

## MCP Source Registration

Use MCP source registration when Cooldis should use someone else's MCP server.
This is the opposite direction from `cooldis-mcp-server`.

Registering a source does not grant it to every agent. Agent manifests still
opt into remote MCP sources with `protocol_tool_import`.

### Dry Path

Store, inspect, and remove a source record without contacting a remote server:

```sh
ROOT="$(mktemp -d /tmp/cooldis-mcp-source.XXXXXX)"

cooldis tool source add docs-demo \
  --kind mcp-http \
  --url http://127.0.0.1:9/mcp \
  --include-tool search \
  --state-home "$ROOT/state"

cooldis tool source list --json --state-home "$ROOT/state"
cooldis tool source show docs-demo --json --state-home "$ROOT/state"
cooldis tool source remove docs-demo --state-home "$ROOT/state"
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
cooldis tool source add search \
  --kind mcp-http \
  --url https://example.com/mcp \
  --bearer-secret mcp.search.bearer \
  --include-tool search

cooldis tool source discover search
cooldis tool source show search --json
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

Use native RPC when you need Cooldis runtime control. Use ACP when an editor or
agent host expects an ACP agent process. Use `cooldis-mcp-server` when an MCP
client should call Cooldis. Use MCP source registration when a Cooldis agent
should call a remote MCP server.

When testing a protocol integration, run both paths:

- dry/local path first, to prove framing, process launch, and JSON shape;
- live-provider path second, to prove the real model lifecycle and streaming
  behavior.
