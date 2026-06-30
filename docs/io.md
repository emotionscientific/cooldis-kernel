# Cooldis IO

Cooldis IO is the boundary where external protocols enter and leave the
runtime. It is broader than a messaging gateway: Telegram, local CLI input,
websocket TUI sessions, webhooks, cron ticks, GitHub events, email, product
queues, and future event buses should all pass through the same conceptual
surface.

The V1 implementation lives inside `cooldis daemon run`. The important split is
architectural:

```text
protocol adapter
-> ingress envelope
-> queue / dedupe
-> resolver
-> admission policy
-> app-server / kernel bridge
-> egress envelope
-> protocol delivery
```

The protocol-neutral contracts live in `crates/cooldis-io-core`.
The durable ingress queue wrapper lives in `crates/cooldis-io-pgqrs`.
The daemon-owned runtime bridge lives in `crates/cooldis-kernel` so it can call
`CooldisSupervisor` without making adapter crates depend on the kernel.

## Terms

- **Protocol adapter**: understands one wire protocol, such as Telegram
  webhooks, a websocket TUI, local CLI stdin/stdout, or generic HTTP webhooks.
- **Ingress envelope**: normalized inbound event with source, conversation,
  actor, content, attachments, metadata, and a protocol-provided dedupe key.
- **Queue / dedupe**: durable admission buffer that enforces idempotency,
  ordering, retry, and dead-letter behavior before events touch the runtime.
- **Resolver**: maps external source/conversation/actor identity into a
  `ThreadAddress` and optional provider policy.
- **Admission policy**: chooses whether the event should queue, steer,
  interrupt, fork, observe, or reject.
- **Kernel bridge**: maps IO decisions onto the runtime app-server and Cooldis
  kernel calls.
- **Egress projector**: maps runtime events into protocol-neutral outbound
  envelopes.
- **Egress adapter**: delivers outbound envelopes through the original protocol
  or another configured destination.

## Daemon Topology

For V1, IO and the runtime app-server should be one daemon process:

```text
cooldisd
  IO layer
    protocols
    queue / dedupe
    resolver
    admission policy
    egress projector
  runtime app-server
    thread/start
    turn/start
    steer
    interrupt
    events
  kernel
    CooldisSupervisor
    RuntimeHost
    provider/tool/runtime adapters
```

This keeps local deployment simple while preserving boundaries that can be split
later. A future production deployment can move protocol adapters or queue
workers into separate processes without changing envelope contracts.

## Protocol Packages

Protocol packages should be separately testable and compiled into the daemon by
feature or by the product binary:

```text
crates/cooldis-io-core
crates/cooldis-io-cli
crates/cooldis-io-websocket
crates/cooldis-io-webhook
crates/cooldis-io-telegram
```

`crates/cooldis-io-telegram` is the first concrete adapter crate. It parses
Telegram Bot API updates into IO envelopes and builds `sendMessage` requests
from visible egress messages. The daemon owns webhook HTTP routing, durable
queue storage, tenant resolution, and admission policy.

`crates/cooldis-io-pgqrs` wraps `pgqrs` behind the core queue traits. The spike
uses a local SQLite DSN for restart/resume proof. Managed deployments can enable
the crate's `postgres` feature and point the same wrapper at a Postgres DSN
later.

## Ingress Persistence

Ingress persistence is an explicit route setting, not a protocol-adapter
decision. The default should be durable queue mode:

```toml
[daemon.io.ingress.persistence]
mode = "durable_queue"
queue_name = "cooldis-ingress"
visibility_timeout_secs = 30
```

In this mode, inbound envelopes are written to the shared ingress queue before a
worker resolves and admits them into the kernel. This is the right shape for
webhooks, managed deployments, server restarts, and zero-downtime upgrades.

Local or purely interactive routes can opt into a lossy direct path:

```toml
[daemon.io.ingress.persistence]
mode = "best_effort_direct"
```

`best_effort_direct` bypasses durable queue storage entirely. It avoids a local
SQLite/Postgres queue growing forever during development, but any in-flight
message can be lost if the daemon exits between protocol receipt and runtime
submission.

Runtime hotswap should start as config-level hotswap:

```toml
[[daemon.io.routes]]
id = "telegram-main"
kind = "telegram.bot"
enabled = true
policy = "interrupt_on_new_dm"
threading = "per_conversation"

[daemon.io.routes.telegram]
listen = "127.0.0.1:9000"
path = "/telegram"
secret_token_env = "TELEGRAM_WEBHOOK_SECRET"
bot_token_env = "TELEGRAM_BOT_TOKEN"

[[daemon.io.routes]]
id = "chat-tui"
kind = "websocket.tui"
enabled = true
policy = "steer_when_active"
threading = "selected_thread"

[daemon.io.routes.ingress.persistence]
mode = "best_effort_direct"
```

Dynamic code plugins can come later as out-of-process adapters that speak the
same envelope shape over JSON-RPC, websocket, or another small transport.

## Telegram Example

A Telegram adapter should own Telegram-specific parsing and delivery only:

```text
Telegram Update
-> cooldis-io-telegram
-> IngressEnvelope {
     source.protocol = "telegram.bot",
     source.instance_id = "main",
     conversation.external_conversation_id = "telegram:chat:<chat_id>",
     conversation.external_thread_id = "<topic_id>",
     actor.external_actor_id = "telegram:user:<user_id>",
     dedupe_key = "telegram.bot:main:update:<update_id>",
     content = Text(...)
   }
-> shared IO queue / resolver / admission
```

The adapter should not decide whether the event queues, steers, or interrupts.
It can provide hints in metadata, but policy owns the final decision.

Egress is symmetric:

```text
EgressEnvelope {
  target.source.protocol = "telegram.bot",
  target.conversation.external_conversation_id = "telegram:chat:<chat_id>",
  kind = AssistantMessage(...)
}
-> cooldis-io-telegram
-> sendMessage / editMessage / sendDocument / sendChatAction
```

## Policy Examples

- `queue_per_conversation`: every event appends behind the active turn.
- `steer_when_active`: if a turn is running, new user text becomes steering;
  otherwise it starts a queued turn.
- `interrupt_on_new_dm`: new direct messages cancel the active turn and replace
  it.
- `observe_system_events`: record webhook/cron events without waking the model.
- `reject_when_dedupe_seen`: acknowledge repeated protocol updates without
  touching the runtime.

Product deployments can add richer resolvers and policies for auth, billing,
quotas, frontend ledger projection, model routing, and durable queue semantics
without forking the kernel.
