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
verlet serve
verlet chat
verlet init <name>
verlet coupling init <name>
verlet agent init|plan|publish|list|show|run
verlet blob publish
verlet tool build|list|publish|run|source|manual
verlet auth status|set|delete
verlet secret import|set|list|status|delete
verlet identity bootstrap|declare|mint|revoke-credential|revoke-principal|list
```

`verlet console` starts the bundled browser console on loopback, serves the UI
and `/rpc` from the same listener, prints the URLs, and opens the browser by
default.

`verlet chat` starts the bundled terminal console as a client of the project
instance. `--attach` remains an explicit override for a specific `unix://...`
or `ws://.../rpc` endpoint.

`verlet coupling init <name>` scaffolds a Rust Wasm coupling package using the
SDK `#[coupling]` macro, fixture JSON, schemas, and a native testkit test. The
generated package is validated with `verlet tool build --package`.

`verlet auth` manages model-provider credentials through the running instance.
`verlet tool source` manages project MCP sources through that same RPC surface.
`verlet secret` manages the user secret store through the instance; import reads
the named environment variable in the client and sends its value only over the
local Unix socket. `verlet identity bootstrap` is the sole offline identity
operation and requires the server to be stopped. The other identity commands
act through the authenticated operator connection. Secret status/list output
and identity list output do not expose bearer values.

## Server And Client Commands

`verlet serve` runs the app-server and configured IO routes in the foreground.
It idles out only when `serve --idle-timeout <duration>` or
`daemon.idle_timeout` is configured. `verlet console` runs the same app-server
in-process with the browser UI and a loopback WebSocket listener, and never
idles out.

`verlet auth`, `verlet secret`, non-bootstrap `verlet identity`, `verlet tool
source`, the secret-bearing path of `verlet tool run`, and `verlet chat` are
clients. They read the endpoint record for their scope and connect to its Unix
socket. Project commands use the same project and state-root resolution as
`verlet console`. User-scoped commands first check the current project instance
because that instance also owns the user metadata root, then check the
user-root endpoint. An explicit identity `--state-home` instead names the
server/session state root, preserving the bootstrap flag's meaning. Outside a
Verlet project, a user-scoped command starts its server from the Verlet home
with project and user state sharing the user state root, and creates nothing
under the arbitrary current directory.

When no matching instance is reachable, a client starts `verlet serve`
detached, writes its stdout and stderr to `<state_root>/serve.log`, waits up to
15 seconds for the endpoint, and continues. Auto-started servers use a 10 minute
idle timeout unless `daemon.idle_timeout` selects another human duration such
as `2s` or `30m`. A new Unix or WebSocket RPC connection resets the timer.
Threads running turns with no attached client still count as idle, so their
durable records survive when the server exits.

## Advanced Commands

```sh
verlet rpc --listen <unix://PATH|ws://HOST:PORT[/rpc]> [--runtime-home <path>] [--state-home <path>] [--cwd <path>]
verlet debug bind <thread-id> [--json] [--url <unix://path|ws-url> | --config <path> | --journal <db>]
verlet debug journal [--thread <thread-id>] [--kind <kind>] [--from-sequence <n>] [--to-sequence <n>] [--json] [--url <unix://path|ws-url> | --config <path> | --journal <db>]
verlet debug rpc call <method> [PARAMS_JSON]
verlet debug rpc turn (--thread <id> | --new) <text>
verlet debug rpc tail --thread <id>
verlet serve [--config <path>] [--idle-timeout <duration>]
verlet daemon config|service
```

`verlet rpc` is the app-server control plane for scripts, local hosts, MCP,
ACP, and other clients. Without `--state-home`, it uses a fresh temporary state
home. TCP WebSocket clients supply a token through
`VERLET_APP_SERVER_TOKEN`; see the standalone quick start and Authentication
section in [Verlet RPC Control Plane](app-server.md). `verlet debug bind`
explains the effective model,
placement, workspace, runtime, tool attachment, coupling, skill, and context
envelope strictly from recorded compile and bind receipts. It reads a running
daemon through `thread/events/list`, or a stopped daemon's Turso journal with
`--journal`; `--json` emits the full receipt projection. `verlet debug journal`
lists raw event records with optional thread, kind, and inclusive sequence
filters. Without `--journal` it reads through the live owner RPC. With
`--journal` it opens a cold Turso store read-only and refuses a store held by a
live owner. Sequence bounds must be positive and apply to each stream's local
sequence. Without `--thread`, one range can therefore return records from many
streams with the same sequence numbers. `--json` emits an array of raw records.
With no `--url` or `--config`, all three debug commands discover the project
instance from the working directory and connect to the listener in its
`endpoint.json`, including a Unix socket. `--url` accepts `unix://path` and
`ws://host:port[/rpc]`; `--config` retains its WebSocket-listener behavior. If
no instance endpoint record exists, the debug client falls back to
`ws://127.0.0.1:49200/rpc`. A stale record names its path and points to the cold
`debug journal --journal` lane. `verlet debug rpc` is protocol tooling for
maintainers and smoke tests; it is not the default user console.

## Release Contract

Release archives must include the `verlet`, `verlet-acp-agent`, and
`verlet-mcp-server` binaries, `share/verlet/console/*`, and
`share/man/man1/verlet.1`. Package smoke tests assert that the manual renders
and the canonical help pages work for `console`, `chat`, `auth`, `tool manual`,
`rpc`, and `debug rpc`.
