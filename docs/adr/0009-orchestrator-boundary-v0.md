# ADR 0009: Orchestrator boundary v0 (envelope submission, client streams, receipts recipe)

Status: accepted (architect, 2026-08-05). Design source: the orchestrator
design pack (workspace plan, 2026-08) and Linear EMO-532. Builds on ADR 0007
(adapter envelope contract) and ADR 0008 (identity plane).

## Context

The orchestrator is a separate product process that drives this daemon as a
client: it submits work, records its own orchestration state (fleet
membership, placement bindings, run outcomes) into the store it is bundled
with, and reads run receipts for metering. Its governing law is that it is a
client of the record, never a second store: it owns no private database.

What the boundary already provides:

- Invocation by published reference: `thread/start` accepts `agentRef`
  (`agent://{namespace}/{name}@{version}`), Host class, host-effect
  witnessed. Nothing new is needed for reference-based invocation.
- Witnessed RPC ingress: `turn/start` appends `io.ingress.received` to the
  control stream with the session principal
  (`record_rpc_ingress_received`).
- Resumable per-stream reads: `thread/events/list` with
  `cooldis.stream.cursor/1`.
- Boundary authentication and authority classes (ADR 0008).

What is missing: a typed envelope submission surface (ADR 0007 envelopes
currently enter only through in-process adapters), and any way for a client
to append its own witnessed records to the store.

## Decision 1: `ingress/submit` (authority class: Ingress)

A new request method that submits an ADR 0007 envelope to a thread. This is
the invocation verb adapters and the orchestrator's schedule runner both
use; `turn/start` remains the interactive caller lane.

Params (camelCase on the wire):

```jsonc
{
  "threadId": "…",              // required
  "input": …,                    // required; same shape turn/start accepts
  "delivery": {                  // required (ADR 0007 D1)
    "deliveryId": "…",
    "attempt": 1,                // optional
    "metadata": { "…": "…" }     // optional, string map
  },
  "dedupeKey": { "scope": "…", "key": "…" },  // optional; else derived from deliveryId
  "correlationId": "…",          // optional; orchestrator's join key
  "tier": "attested"             // optional, default "attested"
}
```

Semantics:

- The server builds the `IngressEnvelope` itself: principal from the
  session (`via: "caller:{session_id}"`, per ADR 0008 D6), witnessed
  `io.ingress.received` on `control:<threadId>`, then admission before any
  scheduling (the existing law: the admission append must succeed before
  enqueueing).
- Dedupe rides `effective_dedupe_key()`. A duplicate submission returns
  the original ingress event id with `"deduped": true` and schedules
  nothing. Submission is therefore safe to retry.
- `correlationId` and `tier` are stamped into envelope metadata
  (`correlation_id`, `guarantee_tier`). `tier` accepts only `"attested"`
  in v0; `"recorded"` is reserved for the foreign-harness lane and is
  rejected with a validation error until that lane exists. The field
  exists now so the schema never breaks.
- Result: `{ "ingressEventId": "…", "deduped": false, "admission":
  { "decision": "queue", "admissible": true } }`.
- This is a new turn-entry surface: it registers in `TURN_ENTRY_SURFACES`
  (as `app-server-envelope-ingress`) with an end-to-end fixture, or the
  admission coverage ratchet fails.

`ingress/*` already classifies as Ingress in both authority classifiers, so
adapter-kind principals can use it; that is the point.

## Decision 2: client streams (`stream/append`, `stream/read`)

A client stream is a store stream owned by a boundary client rather than by
a thread lifecycle. Stream id shape: `client:<name>`, where `<name>` is one
or more `[a-z0-9][a-z0-9-]*` segments joined by `:` (example:
`client:orch:fleet`). Records in client streams are witnessed (they enter
from outside the runtime), carry the writing principal, and never schedule
work: no admission records, no membership in `TURN_ENTRY_SURFACES`, and
recovery/ingress sweeps skip the `client:` prefix exactly as they skip
`sync-ingress:`.

### Storage encoding (amended 2026-08-05, first implementation round)

Two typed store contracts rule out storing client records as first-class
kinds: `ThreadCoordinates.thread_id` is a UUID-typed `ThreadId`, and
`NewEventRecord.kind` is the closed `EventKind` enum whose payload schema
derives from the kind (the frozen-vocabulary law; correct, and kept).
Client records are therefore stored under one new kernel carrier kind:

- `EventKind::ClientRecordAppended`, kind string `client.record.appended`,
  payload schema `cooldis.event.client.record.appended/1`. Payload:
  `{ client_kind, client_schema, principal_id, body }`, where
  `client_kind` / `client_schema` are the declared values from
  `stream/append` and `body` is the client payload, opaque to the kernel.
- The wire contract of `stream/append` / `stream/read` is unchanged:
  `stream/read` unwraps carrier events and returns records whose `kind`
  and payload schema are the client-declared values (store sequence and
  event id come from the carrier). The `kinds` filter matches
  `client_kind`.
- Store coordinates: `thread_id` = UUIDv5 of the full stream id under the
  fixed namespace `530827e2-57cf-405e-9ca7-bb08b18c1ab0` (deterministic,
  collision-free, appears in coordinates only; streams are addressed by
  stream id everywhere). Tenant from the session identity.

One frozen kind added through the normal registry path; the client cohort
lives in payload data, exactly where "declared, never interpreted"
belongs.

### `stream/append` (authority class: Host; host-effect witnessed)

```jsonc
{
  "stream": "client:orch:placement",
  "records": [
    { "kind": "placement.bound",
      "payloadSchema": "verlet.orch.placement.bound/1",
      "payload": { … } }
  ],
  "expectedSequence": 42        // optional fence: next sequence must equal this
}
```

- Validation: stream id grammar as above; `kind` is lowercase dotted
  (`[a-z]+(\.[a-z_]+)+`); `payloadSchema` matches
  `[a-z][a-z0-9.-]*/[0-9]+` and must NOT be in the kernel's reserved
  cohort (`cooldis.*`): client cohorts declare their own ids
  (the orchestrator uses `verlet.orch.*`). Payloads are otherwise opaque
  to the kernel; the declared schema id is recorded, not interpreted.
- The batch appends atomically. With `expectedSequence` it uses the
  store's fenced append; a stale expectation fails closed with a
  dedicated error carrying `{ "expected": …, "actual": … }` so writers
  get compare-and-set semantics (this is what the orchestrator's
  placement lease fence rides on).
- Result: `{ "streamId": "…", "records": [ { "eventId": "…",
  "sequence": … } ] }`.
- Host class: only operator principals write client streams in v0.
  Because it durably mutates the store outside a thread lifecycle, it is
  a host-effect method (witnessed before execution, blocked if the
  witness write fails).

### `stream/read` (authority class: Interactive)

```jsonc
{ "stream": "client:orch:placement",
  "streamCursor": { … },        // optional cooldis.stream.cursor/1
  "limit": 100,                  // 1..=500
  "kinds": ["placement.bound"] } // optional exact-match filter
```

Result mirrors `thread/events/list`: `{ "data": [ … ], "streamCursor":
{ … } }` with `cooldis.stream.record/1` envelopes. `stream/read` accepts
only `client:` stream ids; thread-owned streams stay behind
`thread/events/list` and its authorization story.

## Decision 3: receipts retrieval is a recipe, not a method

Metering and run-outcome consumers need turn outcomes, usage, and egress
receipts. All of these are already durable on thread streams:
`session.entry.appended` carries per-assistant-message usage,
`turn.completed` marks outcomes, and `io.egress.delivered` /
`io.egress.failed` are the egress receipts. The v0 retrieval contract is
therefore: enumerate threads (`thread/list`), then tail each thread stream
with `thread/events/list` using a `kinds` filter and per-stream cursors.
The consumer persists its cursor map in its own client stream.

No new kernel surface, no new kernel truth. This is O(threads) per poll
and per-user daemons hold few threads; a store-level global cursor would
require amending the ADR 0001 cursor schema and is deferred until a
deployment actually outgrows the recipe. Ephemeral `turn/usage`
notifications remain a live-display convenience, not a metering source.

## Decision 4: authorization posture

v0 authorization stays class-level (ADR 0008): operators append, anyone
Interactive reads, on daemons the product itself operates. The per-method /
per-stream grant algebra that ADR 0008 deferred "until a real ask arrives"
now has its real ask (a receipts consumer that should read receipts and
nothing else); it stays out of this ADR and gets its own issue, because a
grant algebra designed overnight would be a worse law than no law.

## Consequences

- The orchestrator's store client switches its reads to `stream/read` and
  gains a working `append`; its stream names adopt the `client:` prefix
  (`client:orch:fleet`, `client:orch:placement`, `client:orch:runs`).
- One Host, one Interactive, and one Ingress method join the dispatcher, the
  authority tables, the second classifier in the identity plane, the
  drift test, `docs/app-server.md`, and the public API coverage ledger.
- The schedule runner and platform adapters compose from `ingress/submit`
  plus existing mandate methods; no further kernel surface is expected
  for the tenant-zero migration.
- Reserved-cohort enforcement (`cooldis.*` rejected at `stream/append`)
  keeps client cohorts and kernel cohorts from ever colliding.

## Out of scope

The foreign-harness (`recorded` tier) execution lane; the grant algebra;
a global receipts cursor; any orchestrator-side client code (lives in its
own repo).
