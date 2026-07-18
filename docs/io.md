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

## Adapter Envelope Contract

Every admissible ingress envelope carries two typed facts defined by
[ADR 0007](adr/0007-adapter-envelope-contract-v0.md):

- `delivery: IoDelivery` names the external delivery. Its `delivery_id` is the
  Telegram update id, webhook or queue message id, or the clock occurrence
  `"{mandate_event_id}:{occurrence_index}"`. An explicit `dedupe_key` wins;
  otherwise the effective key is derived by scoping `delivery_id` to the
  envelope source.
- `principal: IoPrincipal` records the tenant and principal on whose authority
  the event acts, plus how that attribution was established. Self-attributing
  sources such as the clock stamp the mandate holder at construction. Protocol
  adapters leave it unset, and the daemon route binding stamps it before
  admission. External actor identity is provenance, not authority.

The contract is enforced twice. Production submit sinks reject an envelope
without a non-empty delivery identity and effective dedupe key before it can be
queued or applied. After target resolution, the admission boundary requires
principal attribution and exact agreement between `principal.tenant_id` and
the resolved target tenant. An attribution validation failure is admitted only
as a witnessed, terminal Reject outcome; the durable queue completes it and
redelivery folds to the settled reject. Resolver and other transient bridge
errors remain retryable.

The optional wire fields are a bounded upgrade ramp, not a second producer
contract. When a pre-contract queued envelope is leased with `dedupe_key` but
without `delivery`, the daemon deterministically uses the dedupe key's `key` as
`delivery_id`; the explicit dedupe key still wins, so its effective key remains
byte-identical across the upgrade. An envelope with neither identity is
terminally rejected. New producers must always set `delivery` directly.

## Terms

- **Protocol adapter**: understands one wire protocol, such as Telegram
  webhooks, a websocket TUI, local CLI stdin/stdout, or generic HTTP webhooks.
- **Ingress envelope**: normalized inbound event with source, conversation,
  actor, content, attachments, delivery provenance, principal attribution,
  metadata, and a redelivery dedupe identity.
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

`clock.tick` is the daemon-owned clock ingress adapter. It does not carry
schedules in route config; it reads active `mandate.started` minus
`mandate.revoked` facts from thread control streams and turns due occurrences
into durable ingress envelopes. The queue worker admits those envelopes as
witnessed `timer.fired` control-stream events with a payload that names the
source `mandate_event_id`, `scheduled_for`, deterministic `occurrence_index`,
and whether the fire was a recovery `catch_up`.
`std::schedule.cron` renders the mandate `input_template` as a plain string for
the continuation turn input; the only supported substitution is
`{scheduled_for}`.

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

For routes using `threading = "per_conversation"`, the conversation-to-thread
binding is durable. Route startup restores durable bindings before accepting
ingress, so a conversation resumes the same thread after a daemon restart;
runtime threads not yet resident in the fresh supervisor are loaded lazily on
first use. If durable thread history exists and that lifecycle load fails, the
stale binding is replaced by a fresh, durably bound thread before ingress is
submitted. A binding with no thread history is instead an incomplete initial
route reservation: a failed kernel start leaves that row in place, and the next
attempt retries the same reserved id.

Durable queue apply uses the leased `IngressEnvelope.id` as its idempotency key.
The resolved thread's control stream owns the envelope outcome lifecycle. Here
`io.ingress` names that lifecycle, not the component or stream that produced the
envelope. After `io.ingress.received` and `admission.decided`, the daemon appends
an expected-tail fenced `io.ingress.claimed` event before cancellation or turn
submission. The typed claim names every covered envelope, its ingress witnesses,
the admission event, and exactly one intended outcome. Queue and steer claims
carry the reserved `turn_id`, submission mode, and input digest. Interrupt claims
carry the replacement turn ID when present, cancellation reason, and input
digest. Fork claims written by this version carry the child turn key, a
preallocated child thread id, and the input digest. The claim is the durable
child reservation required by ADR 0004 Decision 4. Only after the parent
control stream accepts that claim does the daemon checkpoint the parent,
find-or-create that exact child, append `thread.spawned`, bind egress to the
child, and submit the child turn. Racing applies fold the same parent claim, so
the loser performs no fork effects and cannot allocate a second child.

After the reserved submission is sent, the daemon waits for execution evidence
and then appends `io.ingress.settled`. Evidence is per intent. A queue or
interrupt-replacement claim needs the earliest executing-side turn-trace event
carrying its turn ID, and the executing side's own input persistence does not
qualify: the canonical earliest evidence is the context compile receipt, which
names its turn. A steer claim settles on its persisted steer input entry,
because durable consumption of the input is the steer outcome whether the
running turn accepted it or the idle thread recorded and rejected it.
`turn.submitted` is never evidence because it is the submitting side's
apply-time record. A settle cites its claim and evidence and records whether
execution or recovery settled it. Only then does the queue worker complete the
lease. The derived thread-stream `turn.submitted` record still carries target
context for the egress projector and cites the control stream ingress
witnesses, but it no longer owns ingress idempotency. A fork settles against its
parent control-stream `thread.spawned` evidence, whose typed fork payload carries
the claim event ID.

Before appending a claim, the daemon commits an ownership record keyed by the
envelope's protocol dedupe key to the shared ingress SQLite state (ADR 0004,
Decision 3). The record names the control stream selected by that attempt and
is staged in its own transaction before the attempt is admitted to claim.
Overlapping attempts may stage candidate ownership rows, but claim admission
serializes a
global fold across every recorded owner stream and retains only the winning
owner, so workers that resolved different routes cannot each append a claim.
An ownership record with no claim is a tombstone: the next attempt may
supersede it and claim on the then-current route. Settle does not clear
ownership. The ownership row is deleted only when its `cooldis_ingress_dedupe`
row ages out, so late redelivery still finds a settled claim.

Redelivery resolves its fold target ownership-first and current-route second.
When ownership names a stream, the daemon folds that stream even if the active
conversation route now points at a fork child. If the ownership fold contains
no claim, fresh apply continues on the current route and supersedes the
tombstone before claiming. The ADR 0003 fork exception remains narrower and
unchanged: only when no owned outcome exists does a redelivered fork envelope
walk parent ancestry and honor fork-intent outcomes found there; non-fork
claims remain per-stream scoped. The daemon keeps one durable active binding
per conversation scope, claimed atomically at first contact, so racing first
deliveries share a single control stream. A settled claim is terminal and
dedupes without repeating a receipt, admission decision, cancellation, or
submission. An unsettled turn claim checks the thread journal for
executing-side evidence. Evidence settles the claim as recovery; no evidence
re-submits the same turn ID and then settles.
Supervisor reservation is idempotent on turn ID, turn input persistence adopts
the existing entry for a replayed turn ID, and cancellation of an absent or
finished turn is a witnessed no-op. Interrupt recovery re-runs cancellation and
then applies the same rule to its replacement turn. Observe and reject claims
are appended with their settle in one fenced batch. Their settles have no
execution evidence and record `settled_by = execution`, so both outcomes are
terminal at claim time. A redelivered observe or reject dedupes without another
ingress witness or admission decision, and the queue worker completes its lease.
A lone observe or reject claim cannot result from a valid append and is reported
as corrupt history instead of being recovered.

Fork recovery reuses the child named by a matching `thread.spawned`, completes
binding and child submit, and settles. If no matching spawn exists, recovery
uses the child id reserved by the claim. It adopts that child's durable start
identity and original checkpoint ancestry when creation completed before the
crash; otherwise it creates the reserved id from a recovery-time checkpoint.
Either path appends exactly one `thread.spawned`, so the checkpoint clone stays
one ancestry batch and the creation-before-spawn cut cannot orphan a child.

For a new per-conversation root, the daemon preallocates a thread id and claims
the existing durable initial-route row before calling the kernel start path.
Contenders find-or-start only the id selected by that row. A losing candidate
therefore writes no `thread_started`, manifest, turn, or other thread history;
an interrupted winner can be adopted by a later attempt using the same
reservation.

This claim/settle protocol closes the process-death window between durable
intent and volatile submission without changing the outbound send/receipt
ambiguity described below. `best_effort_direct` remains lossy and does not use
the durable outcome protocol.

Runtime hotswap should start as config-level hotswap:

```toml
[[daemon.io.routes]]
id = "telegram-main"
kind = "telegram.bot"
enabled = true
policy = "interrupt_on_new_dm"
threading = "per_conversation"
agent_ref = "agent://karl-dev@latest"

[daemon.io.routes.content_policies]
"telegram.message_reaction" = "observe_only"

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
coalesce_bursts = { window_ms = 750, max_batch = 8 }

[daemon.io.routes.ingress.persistence]
mode = "best_effort_direct"

[[daemon.io.routes]]
id = "clock-main"
kind = "clock.tick"
enabled = true
```

Enabled Telegram routes must set either `secret_token` or
`secret_token_env`; daemon config validation rejects a listener without one.
The daemon authenticates `X-Telegram-Bot-Api-Secret-Token` before route or
payload processing. `bot_token`/`bot_token_env` remains optional for
ingress-only routes.

Dynamic code plugins can come later as out-of-process adapters that speak the
same envelope shape over JSON-RPC, websocket, or another small transport.

## Clock Tick Example

Clock ticks use the same queue and worker as other durable ingress. A due
mandate occurrence becomes:

```text
clock route
-> IngressEnvelope {
     source.protocol = "clock.tick",
     source.instance_id = "clock-main",
     conversation.external_conversation_id = "thread:<thread_id>",
     conversation.kind = "system",
     delivery.delivery_id = "<mandate_event_id>:<occurrence_index>",
     dedupe_key = "clock.tick:clock-main:<mandate_event_id>:<occurrence_index>",
     principal = {
       tenant_id = "<mandate tenant>",
       principal_id = "<mandate user>",
       via = "mandate:<mandate_event_id>"
     },
     content = Event {
       kind = "timer.fired",
       payload = {
         mandate_event_id,
         scheduled_for,
         occurrence_index,
         catch_up
       }
     }
   }
-> shared IO queue / dedupe
-> daemon bridge
-> control:<thread_id> timer.fired
```

The dedupe key is `(mandate_event_id, occurrence_index)` scoped to the clock
route, so a daemon crash after enqueue but before the route observes success
does not double-fire the control event on restart. If the daemon was down at
the due instant, `coalesce_missed` fires one recovery tick with
`catch_up = true`; `skip_missed` advances to the next occurrence.

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

Telegram `message_reaction` updates become ordinary ingress events, not a new
journal event kind:

```text
Telegram message_reaction Update
-> IngressEnvelope {
     source.protocol = "telegram.bot",
     metadata.telegram_update_kind = "message_reaction",
     metadata.telegram_message_id = "<reacted-to_message_id>",
     content = Event {
       kind = "telegram.message_reaction",
       payload = {
         message_id,
         old_reaction,
         new_reaction
       }
     }
   }
-> io.ingress.received
-> admission.decided
```

`old_reaction` and `new_reaction` preserve Telegram `ReactionType` objects.
Known `emoji` and `custom_emoji` reactions are typed by the adapter; unknown
reaction variants stay as opaque JSON so the whole update can still be
witnessed.

The adapter should not decide whether the event queues, steers, or interrupts.
It can provide hints in metadata, but policy owns the final decision.
The bridge can override the route default with `content_policies`, keyed by the
adapter-stamped event content kind. For example:

```toml
[daemon.io.routes.content_policies]
"telegram.message_reaction" = "observe_only"
```

Only `IngressContent::Event { kind, .. }` selects these overrides. Ordinary
messages and metadata continue to use the route's `policy` field.

Telegram only sends reaction updates to webhooks that opt into
`message_reaction` in `allowed_updates` when calling `setWebhook`:

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

`EgressKind::PlatformAction` carries protocol-neutral route actions:

```text
PlatformAction {
  action = "typing" | "reaction" | "sticker" | ...
  payload = <JSON object>
}
```

Protocol adapters map known actions to their wire API and reject unknown actions.
The Telegram adapter maps `typing` to `sendChatAction`, `reaction` with
`message_id` and `emoji` to `setMessageReaction`, and `sticker` with `file_id`
to `sendSticker`.

`EgressKind::Silence { reason }` is a witnessed decline. It produces no wire
call; receipt/event recording can still say the agent intentionally chose not to
reply.

Routes can project inline assistant text tags into platform actions before
delivery. The tag grammar belongs in daemon TOML with the product route, not in
the kernel:

```toml
[[daemon.io.routes]]
id = "telegram-main"
kind = "telegram.bot"
agent_ref = "agent://karl-dev@latest"
egress_projection = [
  { pattern = '\[reaction:(?P<emoji>[^\]]+)\]', action = "reaction" },
  { pattern = '\[sticker:(?P<file_id>[^\]]+)\]', action = "sticker" },
  { pattern = '\[no_response\]', action = "silence" },
]
typing_simulation = { chars_per_second = 25 }
```

Rules are regular expressions with named groups. Matched spans are stripped from
the assistant text; named groups become the action payload. A message such as
`hello[reaction:👍] friend` becomes an `AssistantMessage` with `hello friend`
plus a `PlatformAction { action = "reaction", payload.emoji = "👍" }`. A message
containing only `[sticker:<id>]` becomes a sticker action with `payload.file_id`.
A message containing only `[no_response]` becomes one `Silence` envelope and no
text envelope. Invalid regexes fail daemon config validation with the rule
index.

`typing_simulation` is off by default. When enabled, the daemon emits a
`typing` platform action before a text envelope and sleeps by
`text_length / chars_per_second`, capped at 8 seconds.

## Durable Egress Projection

The daemon does not deliver replies from a per-turn in-memory watcher. Each
enabled route owns an egress projector task. The projector reads the route's
bound thread event streams from a persisted cursor stored in the same SQLite
state as the ingress queue. It pairs target context from provenance-bearing
`turn.submitted` ingress records with user and assistant
`session.entry.appended` records, projects assistant output through the route's
`egress_projection` rules, picks up `io.egress.requested` events directly, and
calls the route adapter. The control-stream `io.ingress.received` record remains
the sole ingress receipt; the projector does not consume a second receipt from
the thread stream.

`io.egress.requested` currently has no in-kernel producer. Producers are
boundary clients that append the event through the control plane and future
published operations that can be granted the relevant stream access.

Supervisor spawn uses the same requested/projector grammar for a kernel-local
effect. `std::supervisor.spawn` does not start a child thread during the
coupling fold; it emits `thread.spawn.requested` on the parent control stream
and, when configured to block, a parent `turn.waiting` fact. The thread-spawn
projector consumes that request, re-checks the parent thread's bound
`threads.spawn` grant, starts the child through the same thread/turn kernel
path as `cooldis-threads` `thread_spawn`, and witnesses `thread.spawned`.
`std::supervisor.child_completion` remains the paired fold for routed child
completion facts back into the parent continuation.

Successful delivery appends an `io.egress.delivered` event to the thread
journal with the route id, egress kind, external message id returned by the
adapter, and attempt count. Exhausted delivery appends `io.egress.failed` with
`dead_lettered = true` and writes the envelope to
`cooldis_daemon_egress_dead_letters` in the same SQLite database. Dead letters
are inspectable state only in V1; automatic replay is intentionally out of
scope.

Retries are per route:

```toml
[[daemon.io.routes]]
id = "telegram-main"
kind = "telegram.bot"
egress_retry = { max_attempts = 5, base_backoff_ms = 500 }
```

The backoff is exponential from `base_backoff_ms` until `max_attempts` is
exhausted. `EgressKind::Silence` performs no wire call and is immediately
witnessed as `io.egress.delivered` with `egress_kind = "silence"`.

Delivery idempotency is journal-first. The projector dedupes on
`source_event_id + envelope_index` and consults existing
`io.egress.delivered` / `io.egress.failed` receipts before sending on restart.
This gives a send-at-most-once bias for text after recovery. There is still one
honest duplicate window: if the daemon crashes after Telegram accepts a message
but before the delivered receipt is appended, the recovered projector cannot
distinguish that accepted send from a never-sent envelope and may send that one
envelope again.

## Admission Policies

- `queue_per_conversation`: every event appends behind the active turn.
- `steer_when_active`: if a turn is running, new user text becomes steering;
  otherwise it starts a queued turn.
- `interrupt_on_new_dm`: new direct messages cancel the active turn and replace
  it.
- `fork_on_new_dm`: new direct messages fork the resolved source thread through
  `thread/fork`, then submit the incoming text to the child thread. Durable queue
  admission first claims the envelope on the parent's control stream. The claim
  reserves its child thread id before the checkpoint, child creation, lineage
  witness, egress binding, and child submit. The parent control stream witnesses
  lineage with `thread.spawned`, the existing `fork.sourceCut` shape, and the
  optional `fork.claim_event_id` recovery join used by durable ingress.
- `coalesce_bursts`: durable queue workers batch inbound messages from the same
  route/source/external conversation before admission. Configure it per route
  with `coalesce_bursts = { window_ms = 750, max_batch = 8 }`. The first
  message starts the window, later messages in the same window join in arrival
  order, and the worker admits one merged text envelope when `window_ms`
  expires or `max_batch` is reached. Coalescing is best-effort within a single
  drain window: a burst that spans drain windows may be admitted as multiple
  batches, and `max_batch` bounds only a single drain's batch.
- `observe_system_events`: record webhook/cron events without waking the model.
- `reject_when_dedupe_seen`: acknowledge repeated protocol updates without
  touching the runtime.

`coalesce_bursts` is a queue-worker hold, so it runs before the route's normal
admission action. A route can set `policy = "steer_when_active"` and
`coalesce_bursts = { ... }`; the worker first admits one merged envelope, then
`steer_when_active` decides whether that merged envelope queues or steers. If
the daemon exits while a batch is held, pgqrs visibility expiry makes the held
messages visible again and the recovered worker admits them once as a batch.

Every admitted path emits `admission.decided`. Coalesced admissions use
`decision = "coalesce"` and list every source `io.ingress.received` event in
`source_ingress_event_ids`; the thread stream receives exactly one derived
`turn.submitted` record for the merged envelope. Its provenance cites those
control-stream witnesses, and its target context lets egress projection pair
one inbound context with the eventual assistant entry.

Product deployments can add richer resolvers and policies for auth, billing,
quotas, frontend ledger projection, model routing, and durable queue semantics
without forking the kernel.
