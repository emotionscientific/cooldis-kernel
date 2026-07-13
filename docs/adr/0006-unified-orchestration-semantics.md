# ADR 0006: Unified Orchestration Semantics

Status: accepted (design ratified by the anchor 2026-07-11/12 via EMO-407
addenda and EMO-409; no new event kinds required, so nothing here is
ratification-gated — see Event-kind inventory)
Date: 2026-07-12

## Context

Work that outlives the turn that invoked it — a spawned child thread, a host
process, a remote coding agent — is today plumbed through per-kind machinery:
`thread_spawn`'s `block_parent` waiting record and child-completion couplings,
the process manager's exec/write/poll/terminate verbs, and (outside this repo)
actor-wrapper callback shims whose child reports back by invoking a tool. The
lexicon closed this on
2026-07-11 by naming one primitive: a **handle** is a durable reference to
work in flight, returned by a call in place of its value; the thread and
process verb families are per-kind surfaces over it. Its law: a
handle-returning call declares a dispatch identity and is idempotent on it,
and every handle reaches exactly one witnessed terminal outcome carrying
provenance to the originating call.

This ADR specifies the mechanism behind that law for the kernel: the terminal
envelope delivered for every handle, dispatch-identity semantics, the push
wiring that turns a child's terminal outcome into a parent turn, the placement
binding, intra-turn steer delivery, and the store-mediated cross-runtime
protocol for remotely placed children. The governing decisions were ratified
on the EMO-407/EMO-409 trail and are restated here, not
re-litigated. Three prior ADRs are load-bearing: ADR 0001 (stream schema and
the frozen event-kind vocabulary), ADR 0003 (the durable ingress outcome
protocol — claim/settle), and ADR 0005 (the storage engine whose sync
protocol carries the remote lane).

Vocabulary note: per the crank docket's held item D-12, the word for what a
handle reaches is **terminal outcome**, never "settle" — `settle` remains
scoped to ingress-envelope outcomes (ADR 0003) unless the anchor widens it.
Section "D-12 evaluation" below is the requested input to that ruling.

## Decision

### The handle contract

A handle-returning call — `thread_spawn`, `process_exec`, and any future
long-running operation — has three parts:

1. **Dispatch identity.** Every handle-returning call carries a
   `dispatch_id`. Conductor calls (app-server RPC) supply it explicitly;
   model-initiated calls get it injected by the tool router from the
   provider tool-call id — there is no model-visible idempotency field. The
   call is idempotent on it: a retried call folds existing dispatch state
   and returns the original handle, never a second execution. For threads
   this formalizes the existing `ThreadSpawnRequestedPayload.correlation_id`.
   The projector already copies that value into the witnessed
   `thread.spawned` JSON and uses it in its fenced duplicate fold over
   discharged `thread.spawn.requested` claims plus spawned/failure records.
   Lifecycle completion currently joins to `thread.spawned` through
   `ThreadJoinedPayload.spawned_event_id`, not through a correlation-id
   lookup. The implementation renames nothing on the request wire —
   `dispatch_id` is carried in the existing `correlation_id` field — and
   retains that spawned-event provenance leg through completion.
2. **The handle value.** The call returns `{handle_kind, handle_id,
   dispatch_id}` — a thread id or process id plus the identity that minted
   it. The model-facing projection addresses the handle by `task_name`
   alias, not by raw id (see Alias surface).
3. **Exactly one terminal outcome.** The handle's typed terminal envelope is
   delivered to the consumer exactly once through the push lane below. Pull
   verbs (`thread_wait`, `thread_status`, `process_poll`)
   remain as strict-policy primitives for workflow joins; they are folds
   over the same durable state and are never the agent-facing default.

### The terminal envelope

The terminal value of any handle, one schema for all handle kinds:

```text
HandleTerminalEnvelope {
  dispatch_id                     identity of the originating call
  handle       { kind, id }       thread | process
  outcome      completed | failed | cancelled
  outcome_reason                  optional; carries detail (timeout, budget,
                                  exit status) without widening the vocabulary
  result                          schema-typed value (validated against
                                  result_schema_id when present)
  result_schema_id                optional schema identity for result
  artifact_refs                   content-addressed references produced
  usage                           RuntimeUsage totals (threads; absent for
                                  processes)
  retryable                       whether re-dispatch with a fresh identity
                                  is a sensible caller move
}
```

`escalated` is not an outcome (anchor, 2026-07-11): an escalation is
ordinary child model output the parent reads. There is no anti-quit or
loop-termination machinery for sub-agents; the parent keeps tabs. The
richer `RuntimeTerminalState` (stopped, timed-out) maps into `failed` with
`outcome_reason` carrying the detail — the envelope's outcome vocabulary is
deliberately closed at three.

### Push wiring: terminal outcome as ingress

The ratified rule: the terminal envelope arrives as **witnessed ingress on
the parent thread and triggers a parent turn**. This is one lane for all
handle kinds and all placements, and it is built entirely from surfaces
that already exist:

1. **Terminal-outcome fold (child side).** A thread's terminal outcome is a
   fold over its own stream (lifecycle terminal status plus the closing
   turn's records); a process's terminal outcome is the process manager's
   terminal snapshot. Neither needs a new witnessed kind: for threads the
   stream already carries the evidence; for processes the ingress witness
   below is the first durable record, which is sufficient because the
   envelope is the only fact a consumer may act on.
2. **The handle adapter (an ingress source).** When the runtime observes a
   handle it minted reaching terminal state, it emits an `IngressEnvelope`
   whose content is `IngressContent::Event { kind:
   "cooldis.handle.outcome/1", payload: <HandleTerminalEnvelope> }`, with
   `IoDedupeKey { scope: "cooldis.handle.outcome/1", key: dispatch_id }`.
   This is the actual `IoDedupeKey { scope, key }` wire shape. The envelope
   enters the same durable queue as every other ingress;
   `IngressContent::Event` is preserved by the queue codec and is admitted by
   the existing event route.
3. **Resolution by the spawn-time binding.** At dispatch, the runtime
   records a durable handle binding: this dispatch delivers its terminal
   envelope to that consumer thread (the analog of the durable conversation binding that resolves
   protocol ingress). The resolver maps the terminal envelope to the
   parent thread deterministically from that binding.
4. **Admission, claim, and settle under ADR 0003.** The terminal
   envelope goes through admission (default policy: queue a turn), then the
   claim/settle protocol. Exactly-once delivery of the terminal outcome to
   the parent is therefore not new machinery: it is the ingress queue's
   dedupe key plus the existing uniqueness law "at most one claim per
   ingress envelope id, every claim settles exactly once."
5. **The parent turn.** The admitted turn's input is the envelope,
   assembled as witnessed context. The parent wakes because its child
   finished — push-first — with the envelope in hand.

Backpressure and subscription need no new declaration surface: the queue is
the backpressure (terminal envelopes are ordinary queued ingress,
subject to the same lease/retry/dead-letter discipline), and the
subscription is the spawn-time handle binding — a parent is "subscribed" to
exactly the handles it dispatched, by construction. A trigger declaration in
the manifest coupling sense is not required for the default path; couplings
MAY additionally select on the terminal-outcome content kind for chaotic
reactions, under the existing trigger law.

`block_parent` survives as the strict-policy join: it is now specified as
"await the handle's terminal envelope" rather than a bespoke completion
fold, and `thread_wait` folds the same state.

### Placement

Per the lexicon law: placement attaches at the binding — or the conductor
boundary call that creates one — never inline in a model-visible tool call.
Concretely:

- The bind surface gains an optional **placement binding**: `{ target:
  PlacementTarget (local | remote | sandbox), executor_ref, config }`. It
  rides `AgentManifestBindReceipt` as an optional field (absent = local),
  so every run's effective placement is in its bind receipt. The manifest
  itself carries no placement — a manifest is portable by construction; the
  daemon config supplies deployment defaults, and operator surfaces
  (app-server RPC, daemon config) may set or override at bind time.
- The LLM tool projection of `thread_spawn` stays `{task_name, message,
  agent_ref}`. The model names what runs and under which contract; the
  deployment decides where. The current control-decision surface already
  defines `PlacementTarget::{Local, Remote, Sandbox}` and folds witnessed
  `placement.decision` controller facts for an invocation, defaulting to
  `Local` when no fact exists. The binding implementation reuses that
  witnessed kind for the selected bind-time target; it must append a fact for
  the effective target, including a defaulted local target, rather than
  treating the current in-memory default return as a witness.
- Compatibility constraint: `manifest.bind.completed` is a durable payload.
  The placement field is optional with a serde default, and the
  implementation ticket carries the standing decode-compat requirement — a
  seeded raw legacy payload must decode and fold.

A v2-deferred item stays deferred: an abstract, capability-gated isolation
REQUIREMENT hint from the manifest (never an endpoint) — not in this
design.

### Alias surface

Model-visible surfaces speak aliases; receipts speak hashes.

- `task_name` is the model-facing address of a child handle: the model
  spawns with a `task_name`, steers and reads by `task_name`; the runtime
  resolves it to the handle id and the resolution is receipted. Raw thread
  and process ids appear in receipts and the journal, not in the tool
  projection.
- Rolled-in defaults: the default conductor path requires no
  manifest/binding/placement vocabulary. `agent_ref` defaults to the bound
  default-manifest alias; placement defaults from daemon config; a bare
  `thread_spawn {task_name, message}` is a complete call. Customization is
  ordinary alias publishing under the existing alias law (aliases resolve
  to immutable records and produce receipts — `AgentAliasRecord` /
  `AgentAliasResolutionReceipt` are the existing carriers).

### Intra-turn steer delivery

Interrupt, durable queue, and follow-up are already native; the gap is that
a steer admitted while a turn is mid-flight waits for the next turn. The
design point (ratified scope: no new verb) is a delivery point in the turn
loop:

- At each **tool-round boundary** — after a round of tool results is
  complete and before the next model request is assembled — the turn loop
  folds newly admitted steer entries for this thread and assembles them
  into the next model request as witnessed context.
- Durable semantics are ADR 0003's, unchanged: a steer claim settles on the
  persisted steer input entry, and durable consumption is the steer
  outcome. Intra-turn delivery changes WHEN the persisted entry reaches the
  model (this tool-round boundary instead of next turn), not what is
  recorded. A steer that misses the last boundary of a turn is delivered at
  the next turn exactly as today.
- No new verb: the surface is unchanged (`thread_submit` with steer
  admission), and the delivery point is turn-loop mechanics under the
  existing assembly law (assembly selects and arranges; the steer entry was
  already witnessed).

### Cross-runtime protocol (store-primary)

Remote placement makes the parent runtime a conductor of the child: one
contract — the boundary surface — at every placement. The correctness-
bearing channel is the store, never a socket (anchor, 2026-07-11): the
child persists its stream to a store the parent's daemon hosts, and every
cross-runtime interaction is witnessed ingress on the receiving side,
correlated by handle.

Requirements on the remote/shared EventStore backend (the "missing lego,"
spun out as its own implementation ticket):

1. **Daemon-owned endpoint.** The parent daemon embeds the sync-server
   protocol and serves it from the daemon process. Two findings from the Turso adoption make this
   non-negotiable rather than preferred: the engine takes an exclusive
   per-process file lock (a second local open of a live store is refused —
   ADR 0005 decision 5), and its logical sync path supplies no Cooldis
   stream-lease token or expected-tail fence. The current pre-1.0 client uses
   `POST /pull-updates` for a protobuf page stream and `POST /v2/pipeline` for
   Hrana JSON/SQL push; the daemon-owned endpoint must add the Cooldis fence
   before accepting the latter. There is no lawful topology in which a remote
   runtime opens the parent's store files; the endpoint is the only door, and
   the daemon's attestation
   authority sits at it. Considered alternative: an object-store segment
   log (append-only stream segments under a manifest pointer in S3-class
   storage). Viable, and it stays the documented fallback if the embedded
   sync protocol proves unstable pre-1.0. S3-class stores support conditional
   object replacement, but they do not supply an append-log or queue contract;
   the segment-manifest compare-and-swap, lease fencing, and queue semantics
   would still need a coordination authority. The daemon endpoint would still
   exist, and embedding the protocol the engine already speaks is the smaller
   system. ADR 0005's format-compatible rollback claim covers local WAL-mode
   files; rollback of this remote sync topology is deferred to the remote
   EventStore implementation ticket and must not be inferred from that local
   guarantee.
2. **Single-writer lease per thread stream.** Every thread stream has at
   most one live propagator. The child leases its own stream's write
   authority; the lease is granted at dispatch, carried with reservation
   lineage, and **enforced at push time by the daemon endpoint** — a push
   bearing a stale lease is rejected fail-closed (this is the fence the
   engine does not provide). Two propagators never both advance one
   thread.
3. **Scoped write credentials per stream prefix.** The child's credential
   authorizes exactly its own thread-stream prefix (the Pi pattern). The
   sandbox holds no authority beyond the streams it owns.
4. **Store-hosted durable ingress queue.** Parent-to-child submits and
   steers ride a durable ingress queue hosted in the store, not a socket.
   `thread_submit` to a remote child therefore carries dispatch identity
   with the same law as spawn: a retried submit folds the existing queue
   entry and never double-injects.
5. **Push, status, wait = folds and tails over the store.** The parent's
   handle adapter tails the store; the child's terminal event landing in
   the store IS the push (it becomes the terminal envelope through the
   same lane as local children). `thread_status`/`thread_wait` fold the
   same streams. With the current Turso sync implementation, a held
   connection can retain its pre-pull snapshot; the parent tail must reopen
   or checkpoint after each pull before folding the new revision. Crash
   recovery is stream re-lease: the sandbox is
   disposable compute, and a replacement propagator resumes from the
   durable stream under a fresh lease with lineage to the old one.
6. **The live lane is optional and non-correctness-bearing.** A WebSocket
   over a signed sandbox URL with stream-cursor reconnect may carry
   low-latency UI streaming. It is a latency optimization only; nothing
   correctness-bearing may depend on a live connection between runtimes,
   and every fact it carries is also in the store.

**Attestation (triaged case 1, anchor-ratified 2026-07-12).** There is no
attestation federation. A remote child's attestations arrive at the parent
as ordinary witnessed boundary content: the parent daemon attests its own
ingestion (the ingress witness at its endpoint) and never re-attests or
countersigns the child runtime's internal receipts. "One attestation
authority" keeps its existing per-runtime scope — each runtime's stream is
truth for its own threads, and no existing word's scope moves. Cross-daemon
trust (verifying another daemon's attestations) is deferred until a second
authority actually exists; it can be added later without reordering
anything decided here.

### Event-kind inventory

This design introduces **no new event kinds**. The frozen vocabulary
(cooldis.events/0.3) already carries: `thread.spawn.requested` /
`thread.spawned` / `thread.joined` (dispatch and join), `placement.decision`
(placement resolution), `io.ingress.received` / `admission.decided` /
`io.ingress.claimed` / `io.ingress.settled` (the terminal-outcome delivery
lane's ingress protocol, per ADR 0003), and turn-trace kinds (child-side
evidence). The terminal envelope is ingress CONTENT (`IngressContent::Event` with content kind
`cooldis.handle.outcome/1`), not a new stream event kind — content kinds are
not ratification-gated vocabulary. If implementation surfaces a genuine need
for a witnessed process-terminal kind on a stream, it queues for the docket
per the frozen-vocabulary rule; the design as specified does not need one.

### D-12 evaluation (input to the anchor's ruling)

D-12 asks whether a handle's terminal outcome should be a `settle` — i.e.
whether the claim/settle machinery gains a second, non-ingress subject. The
pre-registered flip conditions, evaluated against this design:

- **Second non-ingress subject with the same hard laws?** Structurally the
  laws rhyme (exactly-once, provenance to origin, terminal dedupe). But as
  designed, the handle's exactly-once delivery RIDES the existing ingress
  claim/settle: the terminal envelope is an ingress envelope, and the
  protocol fact that makes it exactly-once is `io.ingress.settled` itself.
  There is no second protocol instance to name — the child-side terminal
  outcome is a fold over already-witnessed records, not a new fenced fact.
- **Closed common payload?** No. The ingress outcome intent
  (turn/fork/interrupt/observe/reject) and the handle envelope
  (completed/failed/cancelled + result) do not share a payload shape; a
  common `settle` would be a union with no common consumer.
- **Cross-domain selector need?** Status/wait folds select by dispatch id
  over handle state; ingress recovery folds select by envelope id over
  claims. No consumer selects across both domains.

Recommendation: **do not widen `settle`.** The design dissolves the second
subject instead of creating it — the handle's terminal outcome is content
carried by the one existing settle protocol, so "terminal outcome" stays a
distinct word for a distinct thing (a value), while `settle` keeps naming
the protocol fact. If a future handle kind cannot ride the ingress lane,
D-12 re-opens with that evidence.

## Consequences

- One terminal-outcome delivery lane for all handle kinds and placements;
  `block_parent`, `thread_wait`, and the process verbs become folds/policies
  over it rather than parallel machinery.
- The ingress queue and ADR 0003 do the exactly-once work; no new
  idempotency protocol enters the kernel.
- `dispatch_id` formalizes `correlation_id` semantics; the wire field is
  unchanged.
- The bind receipt gains an optional placement binding (decode-compat
  gated); `placement.decision` gains its binding-resolution use.
- The remote EventStore backend + stream lease is the enabling
  implementation ticket for remote placement; until it lands, placement
  targets other than local fail closed at bind.
- Skeleton types land with this ADR: `DispatchId`, `HandleKind`,
  `HandleId`, `HandleTerminalOutcome`, `HandleTerminalEnvelope` in
  `cooldis-runtime-contracts`; `AgentManifestPlacementBinding` on the bind
  receipt in `cooldis-kernel`.

## Implementation follow-up tickets

Filed as EMO-418 through EMO-424 (drafted with this ADR, not started):

1. **EMO-418 — thread handle lane.** Caller-supplied dispatch identity,
   idempotent spawn fold over `thread.spawn.requested`, handle return value;
   `thread_submit` under the same law.
2. **EMO-419 — terminal outcome as ingress.** The handle adapter, the
   durable spawn-time handle binding, push-triggered parent turn,
   `block_parent`/`thread_wait` respecified as folds; crash-window tests cut
   inside the emission window.
3. **EMO-420 — process handles.** Dispatch identity on `process_exec`;
   process terminal snapshots feed the same adapter and envelope.
4. **EMO-421 — placement resolution at bind.** Daemon defaults, bind-time
   override, witnessed effective target (including defaulted local), fail
   closed for non-local targets until an executor exists.
5. **EMO-422 — remote EventStore backend.** Daemon-owned endpoint, scoped
   credentials, single-writer lease with reservation lineage enforced at
   push time, store-hosted submit/steer queue, fold/tail refresh, stream
   re-lease recovery; carries the sync-maturity and remote-rollback gates.
6. **EMO-423 — intra-turn steer delivery.** The tool-round-boundary drain,
   with next-turn fallback preserved and no new verb.
7. **EMO-424 — alias surface.** `task_name` addressing, rolled-in defaults,
   receipted resolution, no raw ids in tool projections.

The optional live streaming lane (cursor-resumable WebSocket for UI) is
deliberately not ticketed until the store-primary path is proven; when it
is, the ticket must demonstrate that disconnecting it cannot affect
correctness.

## Out of scope

- Implementation of the envelope/idempotency plumbing, the handle adapter,
  the remote EventStore backend, and the sync protocol (follow-up tickets).
- Loop-termination / anti-quit conditions for sub-agents (deferred; the
  parent keeps tabs).
- The ephemeral-sandbox executor and fs-substrate decision (next gap).
- The actor wrapper (retained as the vendored-harness shim; unchanged).
- Widening `settle` (D-12 — anchor territory; this ADR only supplies the
  evaluation).
