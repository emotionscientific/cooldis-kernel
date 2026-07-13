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

[daemon.runtime.placement]
# Optional. Absent means local. Keep the daemon default local when root
# threads are started through thread/start; select remote on thread/spawn.
target = "local"

[daemon.runtime.workspace]
# Optional bind-plane default for manifests that declare [workspace].
# Relative host paths resolve against this config file's directory.
host_path = "../living-app"
mode = "rw" # "ro" or "rw"

[daemon.app_server]
# Optional. The default uses XDG_RUNTIME_DIR when available, otherwise a
# user-scoped state directory such as ~/Library/Application Support/cooldis/run
# on macOS or ~/.local/state/cooldis/run on Linux.
listen = "unix://.cooldis/run/cooldis.sock"

[daemon.sync]
# Optional. Omit `listen` to keep the store-primary sync endpoint disabled.
# TCP uses the app-server listen grammar; this binds an HTTP endpoint even
# though the address selector is written with ws://.
listen = "ws://127.0.0.1:9443"
lease_ttl_secs = 60

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
agent_ref = "agent://karl-dev@latest"
egress_retry = { max_attempts = 5, base_backoff_ms = 500 }

[daemon.io.routes.content_policies]
"telegram.message_reaction" = "observe_only"

[daemon.io.routes.telegram]
listen = "127.0.0.1:9000"
path = "/telegram"
secret_token_env = "TELEGRAM_WEBHOOK_SECRET"
bot_token_env = "TELEGRAM_BOT_TOKEN"

[[daemon.io.routes]]
id = "clock-main"
kind = "clock.tick"
enabled = true
```

Common route keys:

| Key | Meaning |
| --- | --- |
| `id` | Stable route id used in IO receipts and egress state. |
| `kind` | Route adapter kind such as `telegram.bot` or `clock.tick`. |
| `enabled` | Starts the route when true; disabled routes are parsed but not started. |
| `policy` | Admission policy such as `queue_per_conversation`, `interrupt_on_new_dm`, or `fork_on_new_dm`. |
| `content_policies` | Optional map from adapter-stamped event content kind to a policy override. Values use the same vocabulary as `policy`. |
| `threading` | Scope selector such as `per_conversation`, `per_actor`, or `route_single_thread`. |
| `agent_ref` | Optional published manifest ref, for example `agent://karl-dev@latest`. The daemon requires an `agent://` ref and fails startup if the ref does not resolve in the effective `daemon.registries.agents` root. Publish missing refs with `cooldis agent publish`. |
| `egress_retry` | Per-route delivery retry limits for projected assistant output. |

`daemon.runtime.placement` is the default manifest-bind placement. It accepts
`target = "local" | "remote" | "sandbox"`, optional `executor_ref`, and an
optional executor-specific `config` table. An additive app-server bind override
takes precedence over this default. `remote` opens only after the configured
`daemon.sync` listener has successfully bound and the generation-local process
executor is installed. With no served sync listener it retains the byte-stable
`requires the remote EventStore backend capability` error. `sandbox` remains
fail-closed everywhere. This slice executes remote bindings only through
`thread/spawn`; `thread/start`, daemon-route binding, and `thread/rebindFork`
reject a remote binding instead of falling back to local execution.

`daemon.runtime.workspace` is the default operator binding for an abstract
manifest `[workspace]` requirement. It supplies the machine-local `host_path`
and concrete `mode = "ro" | "rw"`; a bind-time app-server `workspace`
parameter takes precedence. A requiring manifest fails closed when neither is
present, and a configured or requested binding is rejected when the manifest
did not declare a workspace. The selected mode must meet the manifest's
`min_mode` floor. The bind receipt records the canonical host path, guest path,
and mode. That receipt metadata—not the current daemon default—is used for
resume and clone forks, so a restart cannot silently re-point an existing
thread. Workspace mounts are local-placement only in this slice.

## Store-primary sync endpoint

`daemon.sync` enables the daemon-owned door to its EventStore. It is disabled
when `listen` is absent, which preserves local-only daemon behavior. When
enabled, the daemon serves authenticated HTTP push, verified-cursor pull,
queue-delivery acknowledgement, and lease renewal routes over the configured
socket. TCP listeners use the same
address parser as the app-server (`ws://HOST:PORT[/rpc]`); the sync service
itself is ordinary HTTP and logs its effective `http://` address. Unix socket
listeners use `unix://PATH`; relative socket paths resolve against the config
file's directory, like `daemon.app_server.listen`.

The V1 server is close-per-response and content-length-only. It caps headers at
64 KiB and 128 fields, request bodies at 8 MiB, concurrent request tasks at
128, and total request handling at 30 seconds. Transient listener accept
failures retry with bounded backoff instead of stopping the endpoint.

The built-in TCP listener is loopback-only because the V1 service does not
terminate TLS. Expose it to another machine only through an authenticated TLS
proxy or private tunnel terminating on the daemon host. Bearer credentials are
minted with stream leases by the dispatch path; there is no static bearer-token
config field. Tokens belong only in the HTTP `Authorization: Bearer ...` header
and must not appear in URLs or logs.

### Enable remote placement end to end

Keep the app-server and sync listeners local to the daemon, configure the
provider credentials exactly as for a local run, then select `remote` on the
manifest-bound child spawn:

```toml
[daemon.app_server]
listen = "unix://.cooldis/run/cooldis.sock"

[daemon.sync]
listen = "ws://127.0.0.1:9443"
lease_ttl_secs = 60

[daemon.provider]
provider = "openai"
api_key_env = "OPENAI_API_KEY"
model = "gpt-4.1-mini"
```

```json
{
  "method": "thread/spawn",
  "params": {
    "threadId": "<local-parent-thread-id>",
    "taskName": "remote-worker",
    "message": "run remotely",
    "agentRef": "agent://cooldis/default@latest",
    "placement": { "target": "remote" },
    "dispatchId": "stable-submit-identity"
  }
}
```

The daemon grants two independent exact-prefix authorities at dispatch: one
credential can pull and acknowledge only `sync-ingress:<child-id>`, and the
other can push only `thread:<child-id>`. The one-time bearer values cross to
the child over its inherited stdin pipe; they are never written to config,
command-line arguments, URLs, or logs. Provider credentials follow the normal
daemon provider flow and are inherited by the child process. The child owns a
separate local state root, admits queue rows through its durable ingress lane,
and pushes its stream back through `daemon.sync`. The parent folds the returned
stream and the existing handle-ingress adapter turns its first terminal event
into the parent turn.

Every push follows credential, prefix-scope, credential/lease binding, then
atomic lease-and-sequence fencing. Rejections are durably witnessed in redacted
daemon-owned state before a rejection response is sent. Pull performs
credential and prefix-scope authorization plus verified cursor replay, without
a write lease fence. The child appends locally first; endpoint downtime changes
propagation lag, not the local append result. Fence loss and divergent history
are terminal, while transport failures are retried by the child loop with
bounded backoff.

`content_policies` applies only to envelopes whose content is
`Event { kind, .. }`; plain text, commands, and metadata use the route's
`policy`. A `coalesce_bursts` override requires the route to define
`coalesce_bursts`, just like the route-level policy. For Telegram routes, map
`"telegram.message_reaction" = "observe_only"` to witness those webhook updates
without starting turns, or map that content kind to another route policy when
the route should wake the agent.

Telegram only delivers `message_reaction` updates when the webhook is
registered with that update kind:

```sh
curl -X POST "https://api.telegram.org/bot$TELEGRAM_BOT_TOKEN/setWebhook" \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://alice.example.com/telegram",
    "secret_token": "'"$TELEGRAM_WEBHOOK_SECRET"'",
    "allowed_updates": [
      "message",
      "edited_message",
      "channel_post",
      "edited_channel_post",
      "callback_query",
      "message_reaction"
    ]
  }'
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
to either the durable pgqrs/SQLite queue or the direct runtime bridge, and
start a per-route egress projector. The projector reads bound thread streams
from a persisted cursor, delivers visible assistant messages through Telegram
`sendMessage` when a bot token is configured, records delivered/failed receipts
in the journal, and stores exhausted envelopes in
`cooldis_daemon_egress_dead_letters` in the route's queue SQLite database.
`egress_retry.max_attempts` and `egress_retry.base_backoff_ms` configure the
bounded exponential retry loop; the defaults are `5` and `500`.

Clock routes are daemon-owned ingress adapters. Configure one
`kind = "clock.tick"` route per daemon; schedules are not route config and live
in `mandate.started` control-stream events. The route scans control streams for
active mandates, computes deterministic due occurrences, enqueues due ticks
through the same durable ingress queue as Telegram, and the queue worker admits
them as witnessed `timer.fired` events on the subject thread's control stream.
In this daemon slice the clock route observes mandate changes by rescanning
control streams on a 30-second poll instead of subscribing to append
notifications.

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
cargo test -p cooldis clock_route
```

The first smoke starts the real `cooldis daemon run` binary on a configured
Unix socket and drives it with the Cooldis-owned Codex TUI remote client. The
second queues an ingress envelope into SQLite, drops the first queue/bridge,
reopens the queue, and proves the restarted worker can submit it into the
runtime.
