# Cooldis CLI

`cooldis` is the local runtime command. The public surface is intentionally
small at the root: everyday users start the browser console, chat in the
terminal, initialize agent projects, publish tools, agents, and blob resources,
or run the RPC server. Lower-level protocol probes live under `debug`.

## Help Model

```sh
cooldis --help
cooldis commands
cooldis help chat
cooldis help tool manual
man cooldis
```

Bare `cooldis` prints the same concise starting surface as `cooldis --help`.
It points new users to the browser console, terminal chat, and agent project
initializer. Use `cooldis commands` for the full canonical command list,
subcommand help for exact syntax and options, and `cooldis(1)` for the durable
command-family overview.

## Main Commands

```sh
cooldis console
cooldis chat
cooldis init <name>
cooldis coupling init <name>
cooldis agent init|plan|publish|list|show|run
cooldis blob publish
cooldis tool build|list|publish|run|source|manual
cooldis auth status|set|delete
cooldis secret import|set|list|status|delete
```

`cooldis console` starts the bundled browser console on loopback, serves the UI
and `/rpc` from the same listener, prints the URLs, and opens the browser by
default.

`cooldis chat` starts the bundled terminal console. By default it launches a
private local app-server; with `--attach` it connects to an existing
`unix://...` or `ws://.../rpc` endpoint.

`cooldis coupling init <name>` scaffolds a Rust Wasm coupling package using the
SDK `#[coupling]` macro, fixture JSON, schemas, and a native testkit test. The
generated package is validated with `cooldis tool build --package`.

`cooldis auth` stores local model-provider credentials in the user metadata
store. `cooldis secret` stores named runtime secrets used by tools and adapters.
Both redact stored values in status and list output.

## Advanced Commands

```sh
cooldis rpc --listen <unix://PATH|ws://HOST:PORT[/rpc]> [--cwd <path>]
cooldis debug bind <thread-id> [--json] [--url <ws-url> | --config <path> | --journal <db>]
cooldis debug rpc call <method> [PARAMS_JSON]
cooldis debug rpc turn (--thread <id> | --new) <text>
cooldis debug rpc tail --thread <id>
cooldis daemon run|config|service
```

`cooldis rpc` is the app-server control plane for scripts, local hosts, MCP,
ACP, and other clients. `cooldis debug bind` explains the effective model,
placement, workspace, runtime, tool, coupling, grant, skill, and context
envelope strictly from recorded compile and bind receipts. It reads a running
daemon through `thread/events/list`, or a stopped daemon's SQLite journal with
`--journal`; `--json` emits the full receipt projection. `cooldis debug rpc` is
protocol tooling for maintainers and smoke tests; it is not the default user
console.

## Release Contract

Release archives must include the `cooldis`, `cooldis-acp-agent`, and
`cooldis-mcp-server` binaries, `share/cooldis/console/*`, and
`share/man/man1/cooldis.1`. Package smoke tests assert that the manual renders
and the canonical help pages work for `console`, `chat`, `auth`, `tool manual`,
`rpc`, and `debug rpc`.
