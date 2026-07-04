# Cooldis Daemon

`cooldis daemon` is the foreground shape for the future `cooldisd` service. It
does not install launchd or systemd units implicitly; service files are printed
or installed only through explicit user commands.

The daemon config format is TOML:

```toml
[daemon.runtime]
cwd = "."
runtime_home = ".cooldis/runtime"
state_home = ".cooldis/state"

[daemon.app_server]
# Optional. The default uses XDG_RUNTIME_DIR when available, otherwise a
# user-scoped state directory such as ~/Library/Application Support/cooldis/run
# on macOS or ~/.local/state/cooldis/run on Linux.
listen = "unix://.cooldis/run/cooldis.sock"

[daemon.registries]
operations = ".cooldis/operations"
agents = ".cooldis/agents"

[daemon.operations]
load_all_active_when_unbound = false
global_operation_names = []

[daemon.provider]
provider = "openai"
base_url = "https://api.openai.com"
api_key_env = "OPENAI_API_KEY"
model = "gpt-4.1-mini"
stream = true
max_tokens = 4096
env_file = ".env"

[daemon.io.ingress.persistence]
mode = "durable_queue"
queue_name = "cooldis-ingress"
visibility_timeout_secs = 30

[daemon.io.ingress.queue]
sqlite_path = ".cooldis/queue/ingress.sqlite"

[[daemon.io.routes]]
id = "chat-tui"
kind = "websocket.tui"
enabled = true
policy = "steer_when_active"
threading = "selected_thread"

[[daemon.io.routes]]
id = "telegram-main"
kind = "telegram.bot"
enabled = true
policy = "queue_per_conversation"
threading = "per_conversation"

[daemon.io.routes.telegram]
listen = "127.0.0.1:9000"
path = "/telegram"
secret_token_env = "TELEGRAM_WEBHOOK_SECRET"
bot_token_env = "TELEGRAM_BOT_TOKEN"
```

Operation-backed agent manifests, including examples such as
`examples/agents/researcher/`, resolve against the operation registry root
`.cooldis/operations` under the daemon runtime `cwd` by default. If that
default root is absent, the daemon starts cleanly and the operation registry is
empty until records are published. `daemon.registries.operations` overrides the
operation registry root, and `daemon.registries.agents` overrides where
published agent manifests live; when they are unset, the daemon keeps the
app-server defaults `.cooldis/operations` and `.cooldis/agents`.

`daemon.operations` controls which operation records are lowered into the
kernel-synthesized default manifest for bare `thread/start` calls. Set
`global_operation_names` to named records that should always be available, or
set `load_all_active_when_unbound = true` to load every active record from the
effective operation registry root when no thread-specific binding is present.

For an OpenAI Chat Completions-compatible gateway, use the Chat Completions
provider with explicit `base_url`, key, and model settings:

```toml
[daemon.provider]
provider = "openai_chat_completions"
base_url = "https://api.openai.com"
api_key_env = "OPENAI_API_KEY"
model = "gpt-4.1-mini"
stream = false
max_tokens = 4096
```

For an Anthropic Messages-compatible endpoint, use the Anthropic provider
shape:

```toml
[daemon.provider]
provider = "anthropic"
base_url = "https://api.anthropic.com"
api_key_env = "ANTHROPIC_API_KEY"
model = "claude-sonnet-4-5-20250929"
stream = true
max_tokens = 4096
```

For AWS Bedrock Anthropic through `InvokeModel` and
`InvokeModelWithResponseStream`, use AWS credentials from the environment:

```toml
[daemon.provider]
provider = "anthropic_bedrock"
region = "us-east-1"
model = "global.anthropic.claude-sonnet-4-5-20250929-v1:0"
stream = true
max_tokens = 4096
```

For local lossy mode:

```toml
[daemon.io.ingress.persistence]
mode = "best_effort_direct"
```

Current commands:

```sh
cooldis daemon config validate --config cooldis.toml
cooldis daemon service print --target launchd --config cooldis.toml
cooldis daemon service install --target launchd --config cooldis.toml
cooldis daemon service uninstall --target launchd
cooldis daemon service print --target systemd --config cooldis.toml
cooldis daemon service install --target systemd --config cooldis.toml
cooldis daemon service uninstall --target systemd
cooldis daemon run --config cooldis.toml
```

`daemon service install` writes a user-level launchd plist or systemd unit. It
does not load, enable, start, or stop the service automatically.

`daemon run` starts the Cooldis app-server with the configured
provider and starts enabled IO routes. Telegram routes bind the configured HTTP
webhook listener, normalize updates through `cooldis-io-telegram`, submit them
to either the durable pgqrs/SQLite queue or the direct runtime bridge, and can
deliver visible assistant messages through Telegram `sendMessage` when a bot
token is configured.

## Lifecycle Boundary For Local V1 Use

The app-server runs in three shapes. They share one implementation; they
differ in who owns the process, the socket, and the state directories.

1. **Ephemeral, per-command** — `cooldis chat` starts a private in-process
   app-server on a throwaway Unix socket under `/tmp`
   and tear it down on exit. Nothing outside the command should attach to
   it; its socket path is not stable.
2. **Standalone control plane** — `cooldis rpc --listen ...` runs the
   app-server in the foreground on a user-chosen Unix socket or
   loopback WebSocket address, with no IO routes. This is the shape a local
   client (the workbench, a script, the smoke bins) attaches to during
   development. The process owns the socket for its lifetime; stopping it
   is `SIGINT`/`SIGTERM`, and there is no hot handoff.
3. **Daemon** — `cooldis daemon run` is the standalone shape plus the
   configured provider and IO routes, with the socket defaulting to the
   user-scoped run directory. This is the V1 long-lived local shape; the
   service files from `daemon service install` wrap exactly this command.

The boundary rules for V1:

- **One writer per state home.** Exactly one app-server process may own a
  given runtime/state home at a time. Running two shapes against the same
  state directories is unsupported; the ephemeral chat shape avoids this by
  using isolated temp state.
- **State outlives the process; subscriptions do not.** Threads, turns, and
  events persist in the state home (published agent records live in the
  separate agent registry root, `.cooldis/agents` by default) and are
  reloaded on the next start — kill/restart/resume is a supported, tested
  path. Live notification subscriptions are in-memory only: a reconnecting
  client must re-list state and re-subscribe; there is no notification
  replay.
- **Lifecycle ownership is explicit.** Most local clients attach to a socket
  that a user (or the OS service manager) already started. Connection
  refused means "start the daemon", and those clients should say so rather
  than spawning processes themselves. A desktop client may offer an explicit
  user-level managed profile that starts `cooldis daemon run`/`cooldis rpc`
  from the system-installed runtime, records that it owns that child process,
  and stops only that process after asking or applying a remembered quit
  preference. External local sockets and remote endpoints are attach-only.
- **Loopback or Unix socket only.** TCP listen addresses must be loopback;
  there is no authentication layer in V1, so the OS user boundary is the
  security boundary.

`cooldis-mcp-server` can attach to the daemon app-server socket and expose the
same runtime as MCP stdio tools for Codex and other MCP clients:

```sh
cooldis-mcp-server --listen unix://.cooldis/run/cooldis.sock
```

See [Cooldis MCP Server](mcp-server.md) for tool names, Codex config, and V1
limits.

Verification coverage for the daemon lane includes:

```sh
cargo test --test daemon_smoke
cargo test daemon_io::tests::queue_worker_processes_envelope_after_queue_and_bridge_restart
```

The first smoke starts the real `cooldis daemon run` binary on a configured
Unix socket and drives it with the Cooldis-owned Codex TUI remote client. The
second queues an ingress envelope into SQLite, drops the first queue/bridge,
reopens the queue, and proves the restarted worker can submit it into the
runtime.
