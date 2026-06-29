# Cooldis ACP Agent

`cooldis-acp-agent` lets an Agent Client Protocol host launch Cooldis as a
stdio ACP agent. It is an interoperability adapter over the Cooldis daemon
app-server. It is not the Cooldis runtime contract and it does not replace the
app-server API.

```text
ACP host
-> cooldis-acp-agent over stdio JSON-RPC
-> Cooldis daemon app-server socket
-> manifest-bound Cooldis thread
```

## Install

From a source checkout:

```sh
cargo install --path crates/cooldis-kernel --bin cooldis
cargo install --path crates/cooldis-kernel --bin cooldis-acp-agent
```

For source-tree development, run the binary through Cargo:

```sh
cargo run --bin cooldis-acp-agent -- --version
```

Installed binaries expose stable host-facing identity:

```sh
cooldis-acp-agent --version
```

The ACP `initialize` response reports:

```json
{
  "agentInfo": {
    "name": "cooldis-acp-agent",
    "title": "Cooldis ACP Agent",
    "version": "0.1.0"
  }
}
```

The version value is the installed Cooldis package version.

## Start Cooldis

`cooldis-acp-agent` connects to an already-running Cooldis daemon or app-server
Unix socket. Start one in another terminal:

```sh
cooldis rpc --listen unix:///tmp/cooldis.sock
```

From a source checkout:

```sh
cargo run --bin cooldis -- rpc --listen unix:///tmp/cooldis.sock
```

For model-backed sessions, configure the daemon provider the same way native
Cooldis app-server clients do. Provider auth, model catalogs, operation
registries, and manifest publishing remain Cooldis-owned setup.

## Run The Adapter

The adapter speaks ACP on stdin/stdout and writes diagnostics to stderr:

```sh
cooldis-acp-agent --listen unix:///tmp/cooldis.sock
```

Equivalent socket and environment forms:

```sh
cooldis-acp-agent --socket /tmp/cooldis.sock
COOLDIS_DAEMON_LISTEN=unix:///tmp/cooldis.sock cooldis-acp-agent
COOLDIS_DAEMON_SOCKET=/tmp/cooldis.sock cooldis-acp-agent
```

Useful options:

```sh
cooldis-acp-agent \
  --listen unix:///tmp/cooldis.sock \
  --agent-ref agent://researcher@latest \
  --cwd /path/to/workspace \
  --timeout-ms 30000
```

`--agent-ref` selects a published Cooldis agent manifest for every ACP
`session/new` handled by this process. If omitted, the app-server uses its
default manifest path. `--cwd` sets the default working directory for sessions;
ACP `session/new.cwd` can override it per session. The cwd lowers to the
Cooldis manifest runtime `defaultCwd` override and is still subject to manifest
and app-server policy.

## Local Smoke

From the source checkout, this launches a real app-server, starts the
`cooldis-acp-agent` binary over stdio, creates an ACP session, submits a prompt,
and closes the session:

```sh
cargo test -p cooldis --test acp_agent_process_smoke
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
    "cooldis": {
      "protocol": "acp",
      "transport": {
        "type": "stdio",
        "command": "cooldis-acp-agent",
        "args": [
          "--listen",
          "unix:///tmp/cooldis.sock",
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
      "cooldis-acp-agent",
      "--",
      "--listen",
      "unix:///tmp/cooldis.sock"
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
  "name": "cooldis",
  "displayName": "Cooldis",
  "protocol": "acp",
  "transport": "stdio",
  "command": "cooldis-acp-agent",
  "args": [
    "--listen",
    "unix:///tmp/cooldis.sock",
    "--agent-ref",
    "agent://researcher@latest"
  ],
  "env": {},
  "defaultCwd": "/path/to/workspace",
  "agentInfo": {
    "name": "cooldis-acp-agent",
    "title": "Cooldis ACP Agent"
  }
}
```

The host package should not embed provider secrets. Point the adapter at a
Cooldis daemon whose provider and registry state were configured through
Cooldis.

## Limits

ACP is intentionally narrower than the Cooldis app-server API:

- ACP can create sessions, submit prompts, cancel or close sessions, and expose
  session-local model and thinking selectors.
- ACP cannot mutate provider secrets, tenant identity, operation registry state,
  placement policy, or sandbox policy.
- ACP `mcpServers` input is not a silent tool-injection path; tool authority
  still comes from Cooldis manifest binding, configured MCP source records, and
  grants.
- ACP permission bridging is deferred until the Cooldis permission coupling
  outlet exists. See
  [ACP: design permission coupling outlet](https://github.com/emotionscientific/cooldis/issues/153).

For the lower-level protocol mapping, see
[ACP Thread Projection](acp-thread-projection.md).
