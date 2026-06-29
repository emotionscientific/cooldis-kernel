# ADR 0001: Cooldis Stream Schema V1

Status: accepted for V1 RC target
Date: 2026-06-23

## Context

Cooldis already has the canon word `stream`: an append-only, totally ordered
sequence of events owned by a scope. Current code has `EventRecord`,
`EventOrigin`, `EventProvenance`, `EventStore`, and the SQLite event table, but
the V1 release candidate needs a written stream schema contract that backend
adapters, fixtures, RPC surfaces, export bundles, docs, and future SDK
bindings can all point at.

This ADR makes the stream schema the runtime event truth for V1. Storage
backends may differ, but they do not get to redefine what an event means.

## Decision

Cooldis V1 ships one logical stream schema:

```text
cooldis.stream.record/1
```

Every backend adapter must preserve this record contract. Backend-specific
schemas, headers, tables, and indexes are implementation details. The kernel
owns event meaning, ordering, origin, provenance, receipts, and resume
semantics.

There are two schema layers:

```text
stream record envelope
  stable fields every event record carries

event-kind payload schema
  kind-specific payload for session entries, receipts, tool calls, approvals,
  activations, terminal facts, and future events
```

The envelope is V1's common ABI. Payload schemas are frozen per event-kind
version and validated by the shared schema engine.

## Canonical Record Envelope

The JSON form is the portable shape. Binary encodings may exist later, but they
must round-trip to this envelope without changing meaning.

```json
{
  "schema": "cooldis.stream.record/1",
  "event_id": "018f0000-0000-7000-8000-000000000001",
  "stream_id": "thread:018f0000-0000-7000-8000-000000000010",
  "sequence": 1,
  "coordinates": {
    "tenant_id": "local",
    "user_id": "default",
    "session_id": "default",
    "thread_id": "018f0000-0000-7000-8000-000000000010"
  },
  "created_at_ms": 1771718400000,
  "kind": "turn.submitted",
  "origin": "witnessed",
  "payload_schema": "cooldis.event.turn.submitted/1",
  "trace_context": {
    "trace_id": "5b8aa5a2d2c872e8321cf37308d69df2",
    "span_id": "051581bf3cb55c13"
  },
  "provenance": {},
  "payload": {}
}
```

Envelope fields:

| Field | Meaning |
| --- | --- |
| `schema` | Literal schema id for the record envelope. V1 uses `cooldis.stream.record/1`. |
| `event_id` | Globally unique immutable event id. UUIDv7 is the V1 reference form. |
| `stream_id` | The stream that owns the event. Per-stream order is authoritative. |
| `sequence` | Contiguous 1-based per-stream sequence assigned by the store on accepted append. |
| `coordinates` | Runtime scope coordinates used for authorization, filtering, and replay. |
| `created_at_ms` | Runtime-observed wall-clock time in Unix epoch milliseconds. Not an ordering source. |
| `kind` | Frozen event-kind string. Unknown kinds fail closed. |
| `origin` | `witnessed` or `discharged`. |
| `payload_schema` | Schema id for the kind-specific payload. |
| `trace_context` | Optional correlation metadata for observability and external trace propagation. |
| `provenance` | Empty only for witnessed events. Required and non-empty for discharged events. |
| `payload` | Kind-specific JSON payload. Secrets do not belong here. |

Allowed future envelope fields must be additive. Removing or changing the
meaning of an envelope field is a breaking schema change and requires
`cooldis.stream.record/2`.

## Coordinates

Coordinates are the common routing and authorization scope carried by V1
records:

```json
{
  "tenant_id": "local",
  "user_id": "default",
  "session_id": "default",
  "thread_id": "018f0000-0000-7000-8000-000000000010"
}
```

`thread_id` is required for thread-owned streams. Tenant, user, and session
coordinates may be deployment defaults in local mode, but must be explicit in
the record. Stream grants and selectors operate on these coordinates plus
`stream_id`, `kind`, and sequence ranges.

## Origin And Provenance

`origin` is a layer-0 truth field:

```text
witnessed    the world or runtime boundary did it
discharged   a coupling, projection, controller, assembler, or runtime
             function produced it
```

Witnessed events may have empty provenance. Discharged events must carry
non-empty provenance. A store that accepts a discharged event without
provenance is invalid.

V1 provenance shape:

```json
{
  "source_streams": ["thread:..."],
  "source_event_ids": ["018f0000-0000-7000-8000-000000000020"],
  "source_ranges": [
    {
      "stream_id": "thread:...",
      "from_sequence": 1,
      "to_sequence": 12
    }
  ],
  "discharged_by": "coupling://cooldis.context/default@sha256:...",
  "function": "cooldis.context.compile/1",
  "config_hash": "sha256:..."
}
```

`source_range` singular is a compatibility alias while current code migrates to
`source_ranges`. New schema fixtures should use `source_ranges`.

Provenance must be sufficient to locate the source facts and the function or
coupling version that produced the discharge. Receipts are discharged events;
they are never recomputed under newer code.

## Trace Context And Correlation

The stream record may carry observability correlation, but correlation is not
authority. A trace id helps Datadog, OpenTelemetry, browser logs, provider
spans, and export bundles join records. It does not decide whether a worker may
continue a thread.

V1 correlation shape:

```json
{
  "trace_id": "5b8aa5a2d2c872e8321cf37308d69df2",
  "span_id": "051581bf3cb55c13",
  "parent_span_id": "0000000000000000",
  "traceparent": "00-5b8aa5a2d2c872e8321cf37308d69df2-051581bf3cb55c13-01",
  "tracestate": "dd=s:1",
  "activation_id": "act_...",
  "attempt_id": "att_..."
}
```

`traceparent` and `tracestate` follow the W3C Trace Context shape when a record
crosses an HTTP or protocol boundary. `activation_id` and `attempt_id` are
Cooldis coordinates for durable work and must not be replaced by tracing
concepts.

Trace correlation fields are selector and routing inputs. They are not source
ranges, not provenance, and not leases.

## Payload Schemas

Event-kind payload schemas are named:

```text
cooldis.event.<event-kind>/1
```

Examples:

```text
cooldis.event.session.entry.appended/1
cooldis.event.manifest.bind.completed/1
cooldis.event.context.compile.completed/1
cooldis.event.context.summary.completed/1
cooldis.event.context.read_plan.set/1
cooldis.event.coupling.run.completed/1
cooldis.event.activation.suspended/1
```

Payload schemas are versioned, validated by the shared schema engine, and
fixture-backed. A new optional field may be added to a payload schema version
only when older readers can ignore it without changing receipt meaning.
Otherwise a new payload schema version is required.

## Authority, Telemetry, And Projection Streams

V1 intentionally distinguishes runtime truth from observability output.
Traditional systems usually send model traces to an LLM observability backend
and runtime traces/logs to an APM system such as Datadog. Cooldis may generate
both, but neither is allowed to replace the authority stream.

Use these lanes:

| Lane | Source | Destination | Authority for resume? |
| --- | --- | --- | --- |
| Authority stream | Witnessed and discharged Cooldis events | Stream store, export bundle, audit surfaces | Yes |
| Model trace projection | Model call and tool-call events | OpenTelemetry GenAI spans/events, LLM observability tools | No |
| Runtime trace projection | Process, RPC, storage, provider, and worker activity | OpenTelemetry traces/logs/metrics, Datadog, local logs | No |
| UI projection | Browser-safe thread/event projection | SSE, React hooks, app-server subscriptions | No |

Authority events are never sampled away. Telemetry and UI projections may be
sampled, redacted, aggregated, or dropped under policy. If an observed fact can
change continuation, retry, approval, billing, or audit, it must enter an
authority stream as a witnessed or discharged event before it affects the next
activation.

Model traces are especially sensitive. Prompt text, model outputs, tool
arguments, tool results, retrieval documents, and system instructions may be
represented as hashes, handles, redacted snippets, or private projection
records. Raw content is opt-in and belongs behind an explicit retention and
access policy.

## Stream Identity And Granularity

V1 canonical streams are scoped by runtime meaning, not by storage technology.

Required stream families:

| Stream family | Default id shape | Purpose |
| --- | --- | --- |
| Thread primary stream | `thread:<thread_id>` | User turns, model/tool lifecycle, context receipts, terminal states. |
| Control stream | `control:<scope>` | Approval, policy, routing, demotion, and controller facts when not thread-local. |
| Registry stream | `registry:<scope>` | Publish, bind, alias, schema, and package receipts when the deployment needs durable registry audit. |
| Operation stream | `operation:<operation_ref or invocation_id>` | Long-running operation facts when they outlive a turn or need independent fanout. |

Most V1 local deployments can persist only thread streams plus registry/bind
receipts needed by the golden path. Remote and managed deployments may split
control, operation, and audit streams when fanout, retention, or grant scope
differs.

## Routing And Aggregation

The stream router is a projection/controller layer over accepted records. It
must route from envelope fields, not by spelunking arbitrary child payloads.
Valid V1 routing keys include:

```text
schema
stream_id
coordinates.tenant_id / project-derived scope / thread_id
kind
origin
payload_schema
created_at_ms
trace_context.trace_id
provenance.discharged_by
```

Routing profiles decide destination and retention:

```text
resume-required facts      -> stream store, export bundle
model trace projections    -> OTel GenAI / LLM observability
runtime trace projections  -> OTel traces/logs/metrics / APM
browser-safe projections   -> SSE/app-server subscribers
analytics aggregates       -> metrics/read models
```

Aggregates are not truth unless they are discharged back to an authority stream
with provenance. For example, a Datadog token-cost dashboard is a view. A
budget-exhausted decision is an authority event whose payload may include the
aggregate that justified it.

The default router should be boring: all authority events go to the durable
stream store; derived telemetry exporters are opt-in, redacted by default, and
allowed to fail without corrupting the run unless the deployment explicitly
requires an export receipt.

## Append Contract

V1 stream stores must provide atomic per-stream batch append:

```text
append_batch(stream_id, expected, records) -> append_ack
```

The store assigns contiguous sequence numbers. A batch either appends all
records in order or appends none.

Append expectations:

| Expectation | Meaning |
| --- | --- |
| `any_tail` | Append at current tail. Legal only for single-writer or externally fenced cases. |
| `match_sequence(n)` | Append only if the current tail sequence is `n`. |
| `fencing_token(t)` | Append only if token `t` is current for the serialization key. |
| `match_event(id)` | Append only if the current tail event id matches `id`. |

Distributed writes that advance serialized thread state must use either
`match_sequence`, `fencing_token`, or both. Blind `any_tail` is not acceptable
for multi-writer thread evolution.

Append acknowledgement:

```json
{
  "stream_id": "thread:...",
  "start_sequence": 13,
  "end_sequence": 15,
  "tail_sequence": 15,
  "tail_event_id": "018f...",
  "acks": ["local_committed", "stream_committed"]
}
```

## Read, Follow, And Cursor Contract

Read is replay:

```text
read(stream_id, from_cursor, selector) -> ordered records
```

Follow is live replay:

```text
follow(stream_id, from_cursor, selector) -> ordered records plus future records
```

V1 cursors are per-stream:

```json
{
  "schema": "cooldis.stream.cursor/1",
  "stream_id": "thread:...",
  "sequence": 12,
  "event_id": "018f..."
}
```

`sequence` is the resume point. `event_id` is included to detect accidental
stream replacement, migration mistakes, or backend bugs. A read from cursor
returns records strictly after the cursor unless the API explicitly requests
inclusive replay.

Selectors may filter by kind, origin, coordinates, and sequence range, but
filtering must not reorder records.

## Context Projection Read Plans

Context assembly is an event-sourced projection. It is a stateless,
deterministic reducer over:

```text
accepted stream events + pinned refs + read plan + assembler config + budget
```

It does not summarize, remember, mutate cursors, or load ambient code. Anything
model-visible that was created by a model or other chaotic function must already
exist as a discharged event before assembly reads it.

Use event-sourcing vocabulary at this boundary:

| Term | Cooldis meaning |
| --- | --- |
| stream position / cursor | Replay point for one stream. |
| frontier | The tail position frozen for this projection run. |
| read plan | The range plan a projection reducer uses to read raw events, discharged checkpoint events, or drops. |
| projection reducer | The deterministic context assembler. |
| projection receipt | The `context.compile.completed` event proving the reducer output. |
| summary checkpoint | A lossy discharged event covering a source range; not a lossless aggregate snapshot. |

The human shorthand for a read plan may look slice-like:

```text
thread[:80] + summary(thread[80:140]) + thread[140:frontier]
```

The canonical ABI is structured data. The stream layer never parses slice
syntax. A read plan lowers to cursor-bounded ranges and stream event
references:

```json
{
  "schema": "cooldis.context.read_plan/1",
  "name": "history.default",
  "source_stream": "thread:018f0000-0000-7000-8000-000000000010",
  "frontier": "compile_frontier",
  "entries": [
    {
      "kind": "raw_range",
      "range": {
        "from": "start",
        "to": { "sequence": 80, "event_id": "018f..." }
      }
    },
    {
      "kind": "event_ref",
      "covers": {
        "from": { "sequence": 80, "event_id": "018f..." },
        "to": { "sequence": 140, "event_id": "018f..." }
      },
      "event_id": "018f0000-0000-7000-8000-000000000099",
      "event_role": "summary_checkpoint"
    },
    {
      "kind": "raw_range",
      "range": {
        "from": { "sequence": 140, "event_id": "018f..." },
        "to": "frontier"
      }
    }
  ]
}
```

Ranges are cursor intervals: `from` is exclusive, `to` is inclusive. `start`
means before the first event in the stream. `frontier` is symbolic only inside
the read plan; a context compile receipt must record the resolved frontier
cursor for every stream it consumed.

Read plan entry kinds for V1:

| Kind | Meaning |
| --- | --- |
| `raw_range` | The reducer may select source events in the range directly. |
| `event_ref` | The reducer uses a discharged event, such as a summary checkpoint, for the covered range. |
| `drop_range` | The range is intentionally excluded and the receipt must say why. |

Immutable pinned/static context sources, such as published prompt resources,
AGENTS files, resource packages, and immutable tool contracts, do not need a
stream read plan. They enter through resource or manifest bind receipts and are
selected by digest/ref, not loaded ambiently from application code. Mutable or
policy-driven instruction material is different: V1 may witness it as a
`context.summary.completed` checkpoint and select it with a
`context.read_plan.set` entry whose `event_role` is `instruction_checkpoint`.
The context compile receipt must record the selected ref or stream checkpoint,
digest, assembler version, and segment hash.

Compaction does not implicitly move a cursor. A summarizer projection discharges
`context.summary.completed` with the compacted text in the event payload and
provenance over the covered source range. The content-addressed pointer belongs
to the compaction operation, coupling, and config that produced the text; it is
not an off-stream pointer to the summary text. A controller may separately
discharge `context.read_plan.set` to make a named plan such as
`history.default` point at that summary event. Assembly then resolves the latest
granted read plan by name, freezes exact cursors and selected records, and emits
`context.compile.completed`.

The `context.summary.completed` payload is the discharged compacted output.
Provenance remains the authority for why the output exists; it carries the
content-addressed operation or coupling ref, function name, version, config hash,
and covered source ranges. The payload carries the model-visible text and
queryable facts assembly needs:

```json
{
  "schema": "cooldis.event.context.summary.completed/1",
  "role": "summary_checkpoint",
  "covered_ranges": [
    {
      "stream_id": "thread:018f0000-0000-7000-8000-000000000010",
      "from": "start",
      "to": { "sequence": 140, "event_id": "018f..." }
    }
  ],
  "content": {
    "kind": "inline_text",
    "sha256": "sha256:...",
    "text": "..."
  },
  "supersedes_event_ids": []
}
```

The hash in `content.sha256` is an integrity hash for the discharged text, not a
substitute authority location. Redacted UI, export, or telemetry surfaces may
replace the text with a handle, but the authority stream record remains the
source for resume. A model-backed summarizer also emits witnessed model-call
events; the summary text itself is discharged.

The `context.read_plan.set` payload is a named policy fact:

```json
{
  "schema": "cooldis.event.context.read_plan.set/1",
  "name": "history.default",
  "scope": "thread",
  "applies_to": {
    "pipeline_id": "default",
    "source_id": "history"
  },
  "read_plan": {
    "schema": "cooldis.context.read_plan/1",
    "name": "history.default",
    "source_stream": "thread:...",
    "frontier": "compile_frontier",
    "entries": []
  },
  "supersedes_event_id": "018f..."
}
```

The latest accepted `context.read_plan.set` for `(scope, name, pipeline_id,
source_id)` is the named checkpoint/read-plan state. V1 does not need a separate
milestone primitive: a simple positional milestone is a one-entry read plan, and
a fancy middle-compaction or swap is a multi-entry read plan. If future UX wants
to display "milestones", it can project them from read-plan events.

V1's default runtime path remains the simple cache-friendly read plan:
pinned/static sources, optional discharged memory or summary events, then an
append-stable raw tail. The read plan schema is intentionally more general so
future assemblers can compact a middle range, keep an earlier raw prefix, or
swap between equivalent discharged summary events without changing the stream
cursor contract.

The implemented V1 memory lane consumes this general shape conservatively:
`std::memory.extract` discharges `context.summary.completed` records into
`derived:memory`, `std::memory.recall` emits a `context.read_plan.set` into
`derived:context`, and provider runtime assembly reduces the latest
`pipeline_id = "context.memory"` read plan into a deterministic
`<memory_context>` block before model dispatch. History/default read plans stay
recorded as authority facts; replacing raw history ranges with summary entries
is a later assembler extension.

The implemented V1 dynamic-instruction lane uses the same reducer shape:
`std::prompt.dynamic_instructions` discharges instruction text as
`context.summary.completed`, emits a `context.read_plan.set` with
`pipeline_id = "context.instructions"`, and provider runtime assembly reduces
the latest plan into a deterministic `<instruction_context>` block before model
dispatch.

Pins are separate from read plans. A pin is a publish/bind fact that accepts a
witnessed external contract or resource as immutable, content-addressed runtime
input. Context assembly consumes pins by ref and digest through resource or
manifest bind receipts; it does not invent a `context.pin` event for V1. If a
mutable external prompt, tool contract, or file should become static context, it
must first be published or bound as a pinned resource/contract, or it must be
witnessed as an instruction checkpoint and selected by an explicit read plan.

## Destination Acknowledgements

The logical stream has one meaning, but deployments may attach multiple
destinations with different tradeoffs. V1 names acknowledgement classes so the
runtime can say what has actually been proven.

| Ack | Meaning |
| --- | --- |
| `local_committed` | Durable enough for local process restart. |
| `query_projected` | Visible in relational indexes or read models. |
| `stream_committed` | Accepted by the ordered durable stream backend. |
| `broadcast_visible` | Available to live subscribers. |
| `archived` | Written to cold replay or export storage. |

The release gate may require different ack thresholds for different lanes. A
local-only fixture lane may require `local_committed`. A remote serverless
resume proof should require `stream_committed`.

## Backend Mapping

Backends are destination adapters. They do not define semantics.

The V1 RC implementation target is SQLite first. S2, Turso/libSQL, Postgres,
and PlanetScale are adapter lanes behind the same schema contract; they should
not become release blockers for the first golden path.

### SQLite

SQLite is the embedded reference adapter:

- one local database per runtime state home;
- `event_records` table stores the envelope;
- sequence assigned transactionally as `MAX(sequence) + 1` per `stream_id`;
- native follow is optional; polling is acceptable;
- provides `local_committed`;
- may also host query projections for local dev.

### Relational Backends

Postgres, PlanetScale, and similar non-embedded relational databases should ship
reference schemas, migration examples, and conformance tests. Operators may map
the envelope onto their existing tenant, org, partition, or schema topology.

Cooldis decides the logical record contract. The operator may decide physical
table layout, partitioning, colocated app data, and operational migrations.

### Turso And LibSQL

Turso/libSQL is the hot relational and local-first lane:

- database granularity defaults to deployment, tenant, or project boundary,
  not one database per thread;
- event envelope rows remain the contract;
- relational indexes and read models are useful for query, debug, and UI;
- sync or embedded-replica behavior is an adapter guarantee, not a new truth
  category.

Turso is a good fit for `local_committed` plus `query_projected`, and may also
participate in remote durability as its sync story matures.

### S2

S2 is the stream-native durable partner lane after the SQLite reference adapter
has proven the schema contract:

- basin per Cooldis deployment or environment;
- prefix per tenant or project;
- primary S2 stream per Cooldis thread by default;
- sibling streams for control, operation, audit, or projection only when
  fanout, grant scope, or retention differs;
- S2 sequence numbers map to `sequence`;
- Cooldis event fields map to S2 record headers plus canonical body;
- S2 follow/read maps to Cooldis `follow` and replay.

S2 is the preferred future shape for `stream_committed`, `broadcast_visible`,
cold replay, pubsub-backed UI surfaces, and disposable serverless workers. It
is not part of the V1 RC gate unless a separate partner proof is deliberately
enabled.

## Serverless And Rollover Consequence

The record schema makes process lifetime irrelevant when a deployment has a
durable stream store. A worker may run in Lambda, Trigger.dev, a container, an
edge worker, or a local daemon as long as it can:

1. read the bound manifest/package/schema refs;
2. read stream records from a cursor;
3. acquire a lease or append with a fencing token when advancing serialized
   state;
4. append terminal, suspended, failed, completed, or continuation facts; and
5. resume from those facts in a later process.

Invocation boundaries are placement. Stream records are truth.

## Security And Redaction

Secrets never enter stream payloads, provenance, headers, cursors, export
bundles, or live-lane evidence. Event payloads may reference secret handles or
credential source names that are safe to display, but never raw values.

Browser clients should not receive raw authority streams by default. UI packages
consume browser-safe projection streams or RPC/SSE surfaces derived from the
canonical stream.

## External Compatibility Notes

This ADR intentionally does not make OpenTelemetry, Datadog, CloudEvents, S2,
or any other external format the Cooldis authority schema. It does keep the
mapping cheap:

- W3C Trace Context maps to `trace_context.traceparent` and
  `trace_context.tracestate`.
- OpenTelemetry traces/logs/metrics are projections from accepted records. GenAI
  spans/events map from model-call and tool-call records under the configured
  content policy.
- Datadog-style log/trace correlation maps through trace id, span id, service,
  env, and version attributes produced by the telemetry projection.
- CloudEvents delivery can map `event_id` to `id`, `kind` to `type`,
  `stream_id` to `source`, `created_at_ms` to `time`, and the Cooldis envelope
  or payload to `data`.

These are surfaces over the stream schema. They help other systems understand
Cooldis records, but they do not govern replay, resume, grants, or receipts.

## Implementation Consequences

Current V1 RC implementation status:

- `crates/cooldis-runtime-contracts::schema` provides the shared fail-closed
  JSON Schema subset used by runtime contracts.
- The shared schema engine supports string `type` declarations and array
  `type` unions, including nullable fields. V1 export schemas use that support
  to remain strict with `additionalProperties: false` while still representing
  absent rows, cursors, and range endpoints explicitly as `null`.
- Tool argument validation and tool-package schema/fixture validation use the
  shared schema engine, so caller-facing tools and Stream Schema V1 do not drift
  into separate validators.
- `EventRecord::to_stream_record_v1` emits `cooldis.stream.record/1` envelopes.
- `StreamCursorV1` emits `cooldis.stream.cursor/1`; `EventStore::read_events_after_cursor`
  verifies the cursor stream id, sequence, and event id before replaying records
  strictly after that position. SQLite proves this across reopen.
- `stream_schema_registry_v1` validates the V1 envelope,
  `cooldis.stream.cursor/1`, `cooldis.stream.backend_capabilities/1`,
  `cooldis.stream.append_ack/1`, `cooldis.stream.routing_decision/1`,
  `cooldis.context.read_plan/1`, and the first context discharge payload
  schemas.
- `EventKind::payload_schema_id` freezes payload schema ids for the current
  event vocabulary.
- SQLite event rows persist the V1 stream `schema` and `payload_schema`
  identity columns, migrate legacy rows forward, and fail closed if a stored
  payload schema no longer matches the event kind. `config_hash` is a
  provenance field, not an event-table column; discharged coupling receipts now
  carry function identity and config hash separately.
- `thread/events/list` exposes canonical stream envelopes over the app-server
  RPC surface, with legacy `eventId` and `atMs` aliases for clients. It returns
  both the legacy opaque `cursor` and canonical `streamCursor`; requests that
  pass `streamCursor` use verified `EventStore::read_events_after_cursor`
  replay.
- `thread/debug/export` returns a default-redacted
  `cooldis.debug.thread_export/1` evidence bundle over selected
  thread/control/derived streams, including SQLite backend identity, ack
  classes, legacy range cursor tokens, canonical `cooldis.stream.cursor/1`
  continuation/tail evidence, discharged-event receipt summaries, and redaction
  evidence. The bundle is registered in `stream_schema_registry_v1` and frozen
  in `contracts/debug_thread_export_v1.json`.
- `cooldis-manifest-e2e-smoke` asserts the default V1 golden path can restart,
  resume, then export `cooldis.stream.record/1` thread records and manifest
  compile/bind receipt schemas from the same SQLite-backed thread.
- `crates/cooldis-kernel/tests/fixtures/contracts/stream_schema_v1.json` freezes
  representative compile, summary checkpoint, read-plan-set, and
  compile-after-drop-range envelopes, canonical cursors, a SQLite-local append
  acknowledgement, SQLite-local backend capabilities, and the default V1
  routing decisions derived from their envelope fields.
- `StreamBackendCapabilitiesV1` freezes
  `cooldis.stream.backend_capabilities/1`: backend kind, storage scope, ack
  classes, and feature booleans. The V1 SQLite reference declares local
  embedded authority storage with query projection and verified cursor replay;
  it explicitly does not claim expected-tail fencing, live follow, broadcast
  visibility, or cold archive behavior.
- `StreamAppendAckV1` freezes `cooldis.stream.append_ack/1`: stream id,
  contiguous start/end/tail sequence, tail event id, and the ack classes
  actually proven. The V1 local fixture uses `local_committed` and
  `query_projected`; remote backends may later add `stream_committed`,
  `broadcast_visible`, or `archived`.
- `StreamRecordEnvelopeV1::route_decision_v1` and
  `EventRecord::route_decision_v1` provide the first stream router/exporter
  contract. The decision records the envelope routing keys and route profiles
  (`authority_store`, `export_bundle`, `model_trace`, `runtime_trace`,
  `browser_safe_projection`, and `analytics_aggregate`) without reading child
  payload internals.
- Context compile receipts now include `cooldis.context.read_plan/1`; the
  fixture covers raw ranges, summary `event_ref` entries, explicit
  `drop_range` entries, and retained-tail raw ranges. Compaction emits
  `context.summary.completed` and `context.read_plan.set` authority events
  before the legacy session compaction entry.
- `std::context.truncate` now freezes a fixture-backed `drop_range` plus
  retained-tail `raw_range` read plan, proving V1 segment maps without an
  ambient process cursor.
- `std::context.summarize` now freezes a fixture-backed
  `context.summary.completed` plus `context.read_plan.set` pair, proving that
  compacted model-visible summary text is an authority discharge in the stream.
- `std::prompt.steer` now freezes fixture-backed continuation and
  instruction-read-plan control facts, keeping prompt steering visible in the
  authority stream instead of hiding it in application callbacks.

This ADR creates or sharpens remaining V1 work:

- Keep public event and receipt fixtures against `cooldis.stream.record/1`
  green as the vocabulary grows.
- Keep context compile fixtures covering raw ranges, resolved summary refs,
  dropped ranges, and retained tails as the read-plan vocabulary grows.
- Keep V1 payload fixtures for `context.summary.completed` and
  `context.read_plan.set` fail-closed through the shared schema engine as their
  payloads get richer.
- Extend the current `EventStore` shape toward a `StreamStore` contract with
  append expectations and follow semantics. The V1 cursor envelope, append-ack
  envelope, ack classes, backend capability envelope, and verified replay
  helper are in place; append fencing, live follow, and additional adapter
  capability records remain future StreamStore work.
- Extend the stream router/exporter contract from the default pure classifier
  into destination adapters and export receipts for model traces, runtime
  traces, UI streams, and analytics aggregates. Those projections must continue
  to derive from accepted records without becoming the source of truth.
- Require restart/resume tests to prove continuation from stream facts, not
  process memory.
- Treat S2/Turso/Postgres/PlanetScale as adapters behind the same schema, not
  independent runtimes.

## Non-Goals

- Global ordering across all streams.
- Cross-stream transactions as a V1 baseline.
- Raw browser access to kernel authority streams.
- TS/Python SDK generation in V1.
- Provider-native JSON as canonical history.
- Workflow-engine deterministic replay semantics.
- Storage backends inventing event meanings.

## Open Follow-Ups

- Exact Rust trait names: keep `EventStore` as compatibility wrapper or promote
  `StreamStore` as the public trait.
- Whether `activation.*` events are named in V1 fixtures or left as a deferred
  payload family for the async coupling work.
- How much of S2 and Turso is V1 implementation versus documented adapter
  direction.
