# Verlet ACP Agent

`verlet-acp-agent` lets an Agent Client Protocol host launch Verlet as a
stdio ACP agent. It is an interoperability adapter over the Verlet daemon
app-server. It is not the Verlet runtime contract and it does not replace the
app-server API.

```text
ACP host
-> verlet-acp-agent over stdio JSON-RPC
-> Verlet daemon app-server socket
-> manifest-bound Verlet thread
```

## Install

From a source checkout:

```sh
cargo install --path crates/verlet-kernel --bin verlet
cargo install --path crates/verlet-kernel --bin verlet-acp-agent
```

For source-tree development, run the binary through Cargo:

```sh
cargo run --bin verlet-acp-agent -- --version
```

Installed binaries expose stable host-facing identity:

```sh
verlet-acp-agent --version
```

The ACP `initialize` response reports:

```json
{
  "agentInfo": {
    "name": "verlet-acp-agent",
    "title": "Verlet ACP Agent",
    "version": "0.1.0"
  }
}
```

The version value is the installed Verlet package version.

## Start Verlet

`verlet-acp-agent` connects to an already-running Verlet daemon or app-server
Unix socket. Start one in another terminal:

```sh
verlet rpc --listen unix:///tmp/verlet.sock
```

From a source checkout:

```sh
cargo run --bin verlet -- rpc --listen unix:///tmp/verlet.sock
```

For model-backed sessions, configure the daemon provider the same way native
Verlet app-server clients do. Provider auth, model catalogs, operation
registries, and manifest publishing remain Verlet-owned setup.

## Run The Adapter

The adapter speaks ACP on stdin/stdout and writes diagnostics to stderr:

```sh
verlet-acp-agent --listen unix:///tmp/verlet.sock
```

Equivalent socket and environment forms:

```sh
verlet-acp-agent --socket /tmp/verlet.sock
VERLET_DAEMON_LISTEN=unix:///tmp/verlet.sock verlet-acp-agent
VERLET_DAEMON_SOCKET=/tmp/verlet.sock verlet-acp-agent
```

Useful options:

```sh
verlet-acp-agent \
  --listen unix:///tmp/verlet.sock \
  --agent-ref agent://researcher@latest \
  --cwd /path/to/workspace \
  --timeout-ms 30000
```

`--agent-ref` selects a published Verlet agent manifest for every ACP
`session/new` handled by this process. If omitted, the app-server uses its
default manifest path. `--cwd` sets the default working directory for sessions;
ACP `session/new.cwd` can override it per session. The cwd lowers to the
Verlet manifest runtime `defaultCwd` override and is still subject to manifest
and app-server policy.

## Local Smoke

From the source checkout, this launches a real app-server, starts the
`verlet-acp-agent` binary over stdio, creates an ACP session, submits a prompt,
and closes the session:

```sh
cargo test -p verlet --test acp_agent_process_smoke
```

For the full project gate:

```sh
scripts/check-pre-push.sh
```

## Generic ACP Stdio Config

ACP hosts use different config field names. The command/args shape is the
portable part:

```json
{
  "agents": {
    "verlet": {
      "protocol": "acp",
      "transport": {
        "type": "stdio",
        "command": "verlet-acp-agent",
        "args": [
          "--listen",
          "unix:///tmp/verlet.sock",
          "--agent-ref",
          "agent://researcher@latest",
          "--cwd",
          "/path/to/workspace"
        ]
      }
    }
  }
}
```

For source-tree development, replace the command with Cargo:

```json
{
  "protocol": "acp",
  "transport": {
    "type": "stdio",
    "command": "cargo",
    "args": [
      "run",
      "--quiet",
      "--bin",
      "verlet-acp-agent",
      "--",
      "--listen",
      "unix:///tmp/verlet.sock"
    ]
  }
}
```

Cargo build output must stay on stderr; ACP JSON-RPC messages must be the only
stdout content.

## AgentOS / Rivet-Style Descriptor

Use the host's actual package schema. This descriptor shape shows the fields a
host package should carry without depending on any specific AgentOS runtime:

```json
{
  "name": "verlet",
  "displayName": "Verlet",
  "protocol": "acp",
  "transport": "stdio",
  "command": "verlet-acp-agent",
  "args": [
    "--listen",
    "unix:///tmp/verlet.sock",
    "--agent-ref",
    "agent://researcher@latest"
  ],
  "env": {},
  "defaultCwd": "/path/to/workspace",
  "agentInfo": {
    "name": "verlet-acp-agent",
    "title": "Verlet ACP Agent"
  }
}
```

The host package should not embed provider secrets. Point the adapter at a
Verlet daemon whose provider and registry state were configured through
Verlet.

## Limits

ACP is intentionally narrower than the Verlet app-server API:

- ACP can create sessions, submit prompts, cancel or close sessions, and expose
  session-local model and thinking selectors.
- ACP cannot mutate provider secrets, tenant identity, operation registry state,
  placement policy, or sandbox policy.
- ACP `mcpServers` input is not a silent tool-injection path; tool authority
  still comes from Verlet manifest binding, configured MCP source records, and
  grants.
- ACP permission bridging is deferred until the Verlet permission coupling
  outlet exists. See
  [ACP: design permission coupling outlet](https://github.com/emotionscientific/cooldis/issues/153).

For the lower-level protocol mapping, see
[ACP Thread Projection](acp-thread-projection.md).
