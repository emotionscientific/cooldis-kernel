# Verlet CLI

`verlet` is the local runtime command. The public surface is intentionally
small at the root: everyday users start the browser console, chat in the
terminal, initialize agent projects, publish tools, agents, and blob resources,
or run the RPC server. Lower-level protocol probes live under `debug`.

## Help Model

```sh
verlet --help
verlet commands
verlet help chat
verlet help tool manual
man verlet
```

Bare `verlet` prints the same concise starting surface as `verlet --help`.
It points new users to the browser console, terminal chat, and agent project
initializer. Use `verlet commands` for the full canonical command list,
subcommand help for exact syntax and options, and `verlet(1)` for the durable
command-family overview.

## Main Commands

```sh
verlet console
verlet chat
verlet init <name>
verlet coupling init <name>
verlet agent init|plan|publish|list|show|run
verlet blob publish
verlet tool build|list|publish|run|source|manual
verlet auth status|set|delete
verlet secret import|set|list|status|delete
```

`verlet console` starts the bundled browser console on loopback, serves the UI
and `/rpc` from the same listener, prints the URLs, and opens the browser by
default.

`verlet chat` starts the bundled terminal console. By default it launches a
private local app-server; with `--attach` it connects to an existing
`unix://...` or `ws://.../rpc` endpoint.

`verlet coupling init <name>` scaffolds a Rust Wasm coupling package using the
SDK `#[coupling]` macro, fixture JSON, schemas, and a native testkit test. The
generated package is validated with `verlet tool build --package`.

`verlet auth` stores local model-provider credentials in the user metadata
store. `verlet secret` stores named runtime secrets used by tools and adapters.
Both redact stored values in status and list output.

## Advanced Commands

```sh
verlet rpc --listen <unix://PATH|ws://HOST:PORT[/rpc]> [--runtime-home <path>] [--state-home <path>] [--cwd <path>]
verlet debug bind <thread-id> [--json] [--url <ws-url> | --config <path> | --journal <db>]
verlet debug rpc call <method> [PARAMS_JSON]
verlet debug rpc turn (--thread <id> | --new) <text>
verlet debug rpc tail --thread <id>
verlet daemon run|config|service
```

`verlet rpc` is the app-server control plane for scripts, local hosts, MCP,
ACP, and other clients. Without `--state-home`, it uses a fresh temporary state
home. TCP WebSocket clients supply a token through
`VERLET_APP_SERVER_TOKEN`; see the standalone quick start and Authentication
section in [Verlet RPC Control Plane](app-server.md). `verlet debug bind`
explains the effective model,
placement, workspace, runtime, tool, coupling, grant, skill, and context
envelope strictly from recorded compile and bind receipts. It reads a running
daemon through `thread/events/list`, or a stopped daemon's SQLite journal with
`--journal`; `--json` emits the full receipt projection. `verlet debug rpc` is
protocol tooling for maintainers and smoke tests; it is not the default user
console.

## Release Contract

Release archives must include the `verlet`, `verlet-acp-agent`, and
`verlet-mcp-server` binaries, `share/verlet/console/*`, and
`share/man/man1/verlet.1`. Package smoke tests assert that the manual renders
and the canonical help pages work for `console`, `chat`, `auth`, `tool manual`,
`rpc`, and `debug rpc`.
