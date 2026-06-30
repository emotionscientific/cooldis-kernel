# Cooldis CLI

`cooldis` is the local runtime command. The public surface is intentionally
small at the root: everyday users start the browser console, chat in the
terminal, initialize agent projects, publish tools and agents, or run the RPC
server. Lower-level protocol probes live under `debug`.

## Help Model

```sh
cooldis --help
cooldis commands
cooldis help chat
cooldis help tool manual
```

Bare `cooldis` prints the same concise help as `cooldis --help`. Use
`cooldis commands` for the full canonical command list.

## Main Commands

```sh
cooldis console
cooldis chat
cooldis init <name>
cooldis agent init|plan|publish|list|show|run
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

`cooldis auth` stores local model-provider credentials in the user metadata
store. `cooldis secret` stores named runtime secrets used by tools and adapters.
Both redact stored values in status and list output.

## Advanced Commands

```sh
cooldis rpc --listen <unix://PATH|ws://HOST:PORT[/rpc]> [--cwd <path>]
cooldis debug rpc call <method> [PARAMS_JSON]
cooldis debug rpc turn (--thread <id> | --new) <text>
cooldis debug rpc tail --thread <id>
cooldis daemon run|config|service
```

`cooldis rpc` is the app-server control plane for scripts, local hosts, MCP,
ACP, and other clients. `cooldis debug rpc` is protocol tooling for maintainers
and smoke tests; it is not the default user console.

## Release Contract

Release archives must include the `cooldis`, `cooldis-acp-agent`, and
`cooldis-mcp-server` binaries plus `share/cooldis/console/*`. Package smoke
tests assert that the canonical help pages work for `console`, `chat`, `auth`,
`tool manual`, `rpc`, and `debug rpc`.
