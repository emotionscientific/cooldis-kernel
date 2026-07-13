# Standard Operations

Cooldis V1 treats the standard library as published records, not application
imports. Agent manifests should point at operation refs and grants; the runtime
then decides whether a record runs as Wasm, a kernel-native package, a process
placement, or a future remote placement.

The currently implemented release-gated slice is:

- thread/turn control: `cooldis-threads` as a kernel-native package;
- process/command contract: `cooldis-process` as a kernel-native package
  published at startup and dispatchable only when host-process authority is
  explicitly bound;
- notify/channel intent: `cooldis-notify` as a kernel-native reference package
  that records channel intent without delivering to external channels;
- source/search building blocks: `http-fetch`, `file-read`, and `json-query`
  as first-party Wasm packages;
- local operation secrets: `cooldis secret` plus manifest secret resolution for
  operation packages that declare `secret:<name>`;
- coupling templates: the V1 catalog plus runtime-executable reference
  implementations for async queue, completion callback, context spill,
  context truncation, memory extract/recall preview, permission tool-gating,
  schedule/retry/deadletter, and dynamic instruction checkpoints.

The remaining V1 stdlib work is tracked separately: channel-specific delivery
adapters for Slack, Telegram, email, web, or other HITL surfaces. The abstract
approval gate is executable and emits durable control facts; channel delivery is
not hidden inside that template. Retrieval beyond the sequence-selected
`std::memory.recall` preview is a later source selector or operation concern,
not an additional V1 coupling template id.

Kernel-native records live in the same operation registry as Wasm packages, but
their `runtime.kind` is `kernel` and their artifact hash is computed from the
canonical serialized tool contract. User-facing `cooldis tool build` and
`cooldis tool publish` reject `runtime.kind = "kernel"`; only startup synthesis
may publish those records. The kernel package contracts are validated with the
same shared schema engine used by tool packages and stream fixtures.

## Packages

### `cooldis-threads`

Runtime kind: `kernel`

`cooldis-threads` is synthesized at app-server startup when an operation
registry root is configured. It publishes five thread-control operations:

| Operation | Required Capability | Purpose |
| --- | --- | --- |
| `thread_spawn` | `threads.spawn` | Start a supervised child thread and submit its first message. |
| `thread_submit` | `threads.control` | Steer a child addressed by its parent-scoped task name. |
| `thread_wait` | `threads.read` | Wait for a child addressed by task name to settle. |
| `thread_status` | `threads.read` | Report status for a child addressed by task name. |
| `thread_cancel` | `threads.control` | Cancel a child addressed by task name. |

`thread_spawn` input:

```json
{
  "task_name": "worker",
  "message": "inspect this file",
  "agent_ref": "agent://cooldis/default@latest"
}
```

`agent_ref` is optional. Without it, the app-server resolves the synthesized
`agent://cooldis/default@latest` alias and records the ordinary alias-resolution,
compile, and bind receipts on the child. Placement comes from daemon config, so
the two-field `{task_name, message}` form is complete. Every spawned child is a
first-class thread: the app-server registers its lifecycle/topology record
before the first child turn, `thread/list` includes it with `parentThreadId` set
to the spawning thread, and `thread/events/list` can query the child event
stream.

Within one parent, `task_name` is a durable reservation. Retrying with the same
provider tool-call identity folds to the original handle. Reusing the name with
a different dispatch identity is rejected, including after the first child has
completed; another parent may independently use the same name. Resolution folds
the existing `thread.spawn.requested` and `thread.spawned` records and returns an
internal resolution receipt. It does not use a process-local alias map.

Model-visible results for spawn, submit, wait, status, and cancel contain only
`operation`, `task_name`, and `status`. Thread ids, handle ids, event ids, turn
ids, interaction ids, and dispatch ids remain available in runtime receipts and
the journal, not in these tool results.

`thread_submit` input:

```json
{
  "task_name": "worker",
  "message": "continue"
}
```

`thread_wait` input:

```json
{
  "task_name": "worker",
  "timeout_ms": 1000
}
```

`thread_status` input:

```json
{
  "task_name": "worker"
}
```

`thread_cancel` input:

```json
{
  "task_name": "worker"
}
```

`thread_wait` waits for the target to settle; it does not remove the target from
the thread query surface. `thread_cancel` cancels and shuts down the scoped
target thread, leaving its lifecycle record in the terminal `stopped` status.

Manifests receive these operations only through declared tool rows and grants.
The default manifest declares them as `direct_tool` rows when the registry root
exists and its child-thread policy allows it. A manifest with
`allow_child_agents = false` may bind read/control rows, but binding a
`threads.spawn` row fails closed.

### `cooldis-process`

Runtime kind: `kernel`

`cooldis-process` is synthesized at app-server startup when an operation
registry root is configured. It publishes four process-handle operations:

| Operation | Required Capability | Purpose |
| --- | --- | --- |
| `process_exec` | `process.spawn` | Start a host command process and return its first snapshot. |
| `process_poll` | `process.read` | Poll an existing process handle. |
| `process_write` | `process.write` | Write base64 stdin bytes to a process handle. |
| `process_terminate` | `process.control` | Terminate a process handle. |

The contract uses the same process snapshot vocabulary as the app-server
`command/exec` surface: stable process id, status, backend, label,
exit code when known, stdout/stderr, truncation flags, and event count.

`process_exec` accepts an optional `dispatch_id` and generates one when absent.
The durable observe-only dispatch witness is settled before backend startup;
retrying that identity returns its original live handle, while a witness whose
live registry entry is gone fails closed. Poll and wait projections read that
same manager snapshot, whose terminal entry remains until outcome ingress is
acknowledged. App-server streaming `command/exec` uses camelCase `dispatchId`
and `threadId` to bind the handle settlement consumer.

V1 intentionally does not add `cooldis-process` to the default manifest and
`load_all_active_when_unbound` skips it. Host process authority must be declared
explicitly by a manifest or registry binding. When a manifest binds these rows,
the provider runtime mounts a kernel dispatcher over `AsyncExecutionManager` and
`HostBashLiveBackend`, so direct-tool aliases can start, poll, write to, and
terminate host process handles through the stable package ABI. The app-server
`command/exec` RPC remains a separate browser-facing projection with camelCase
fields; `cooldis-process` emits the package receipt shape above.

### `cooldis-notify`

Runtime kind: `kernel`

`cooldis-notify` is synthesized at app-server startup when an operation
registry root is configured. It publishes two reference operations:

| Operation | Required Capability | Purpose |
| --- | --- | --- |
| `notify_preview` | `notify.preview` | Normalize notification intent without delivering it to a channel. |
| `channel_emit` | `channel.emit` | Record channel egress intent for an explicit external delivery adapter. |

`notify_preview` input:

```json
{
  "channel": "email",
  "subject": "Build complete",
  "body": "The build finished successfully.",
  "severity": "info"
}
```

`channel_emit` input:

```json
{
  "channel": "slack",
  "message": "Ready for review",
  "thread_id": "..."
}
```

Both operations return `status = "recorded"`, `delivery = "not_sent"`, and
`channel_decision_required = true`. This package is intentionally a V1
reference boundary: it proves that manifests can bind notify/channel intent as
schema-validated kernel operations, while Telegram, Slack, email, HITL, and
other channel-specific delivery adapters remain explicit future operations.

### `http-fetch`

Operation: `http_fetch`

Input:

```json
{
  "url": "https://example.com/data.json",
  "headers": {"accept": "application/json"},
  "timeoutMs": 5000,
  "maxResponseBytes": 262144
}
```

Output:

```json
{
  "status": 200,
  "headers": {"content-type": "application/json"},
  "bodyText": "{\"ok\":true}",
  "truncated": false
}
```

`http_fetch` is GET-only. It declares no package-time capability because the
origin is caller input. The HTTP broker checks the concrete request at call
time. Grant the concrete origin with `net.http:GET:<origin>` for public
destinations or `net.http.private:GET:<origin>` for loopback/private
destinations. Origin patterns may use `*`, such as `net.http:GET:https://*`;
public wildcards never cross into the private namespace. Denials return a JSON
`error` object instead of trapping the operation.

### `file-read`

Operation: `file_read`

Input:

```json
{
  "path": "/workspace/input.txt",
  "offsetBytes": 0,
  "maxBytes": 262144
}
```

Output:

```json
{
  "content": "hello",
  "bytesRead": 5,
  "eof": true
}
```

The operation reads only through the host-provided VFS read ABI. Missing files
and denied paths return a JSON `error` object with empty content.

### `json-query`

Operation: `json_query`

Input:

```json
{
  "json": {"items": [{"name": "Ada"}]},
  "pointer": "/items/0/name"
}
```

Output:

```json
{
  "found": true,
  "value": "Ada"
}
```

The pointer follows RFC 6901. A pointer that is neither empty nor slash-prefixed
returns `InvalidArgument`.

## Seeding A Registry

Run:

```bash
scripts/seed-ops.sh
```

The optional first argument selects a registry root:

```bash
scripts/seed-ops.sh /tmp/cooldis-operations
```

The script builds and publishes `http-fetch`, `file-read`, and `json-query`
with `cooldis tool publish --package`. Re-running it against the same
registry root is idempotent.

## Coupling Templates

Couplings are V1 event-stream edges. They are not application imports and they
are not hidden hooks; a coupling template names the source stream/kinds, the
sink stream/kinds, the role (`controller` or `projection`), and the maturity of
the implementation surface. The frozen catalog lives in
`cooldis.coupling.template_catalog/1` and is release-gated by
`contracts/coupling_template_catalog_v1.json`.

`cooldis init <name>` writes the same catalog to
`components/couplings.toml` so a fresh folder shows the intended stdlib shape
without requiring a TS/Python DSL binding.

The first reference executor is `StdlibCouplingExecutor`. It covers the
must-have templates `std::queue.task`, `std::queue.completion_callback`, and
`std::context.spill`; fixtures freeze the queue payloads in
`contracts/stdlib_queue_couplings.json` and the spill payloads in
`contracts/stdlib_context_spill_coupling.json`. It also covers the optional
context/memory/permission/schedule/retry/failure-inspection templates
`std::context.truncate`, `std::memory.extract`, `std::memory.recall`,
`std::prompt.dynamic_instructions`, `std::permission.tool_gate`,
`std::schedule.cron`, `std::retry.with_budget`, and
`std::failure.deadletter`, frozen in their matching `contracts/stdlib_*`
fixtures.

When a manifest-backed thread starts through the app-server, the app-server
persists the full post-bind `BoundCouplingSet` in thread lifecycle metadata.
`RuntimeServices` decodes that set and runs the supported stdlib couplings
after kernel boundary events are appended. Custom couplings remain bound and
inspectable, but they are not auto-executed unless a runtime executor supports
their template id.

Manifest coupling ids accept the canonical stdlib template id spelling, so a
folder-first agent can declare `id = "std::context.spill"` without a language
SDK or an adapter-specific alias.

The executor is deliberately small: `std::queue.task` turns a witnessed
`turn.submitted` event into a durable `turn.waiting` control fact, and
`std::queue.completion_callback` consumes the watched queue coupling's
`coupling.run.completed` receipt and emits either `loop.completed` or, when
configured with `on_completed = "request_continuation"`,
`turn.continue.requested`. A completion callback binding should include
`trigger_match.coupling_id` for the watched coupling so it does not trigger on
its own run receipts.

`std::context.spill` consumes `context.compile.completed`, emits
`context.summary.completed` with the compacted text in the stream payload, and
emits `context.read_plan.set` selecting that summary checkpoint for the covered
range. The scheduler supports preallocated discharge ids so the read plan can
point at the exact summary event id instead of using an out-of-band content
handle.

`std::context.truncate` consumes `context.compile.completed` and emits a
single `context.read_plan.set` control fact. The read plan explicitly marks the
old prefix as `drop_range` and keeps the bounded tail as `raw_range`; it is a
segment-map discharge over the stream, not an implicit cursor hidden in process
memory.

`std::context.summarize` consumes `context.compile.completed` or
`turn.completed`, emits `context.summary.completed` with the summarized text in
the event payload, and emits `context.read_plan.set` selecting that summary
checkpoint. The summary is witnessed as a discharged event with provenance and
content hash; the reference does not store model-visible summary text behind an
off-stream pointer.

`std::memory.extract` consumes `turn.completed` or `tool.call.completed` and
projects an observational memory checkpoint into `derived:memory` using the
same `context.summary.completed` payload contract. The extracted text remains
inside the event payload, provenance points back to the triggering boundary
event, and the payload is marked `memory_kind = "observation"`. This is a
preview discharge, not a hidden mutable memory store.

`std::memory.recall` consumes `turn.submitted` or `context.compile.completed`,
reads selected `context.summary.completed` checkpoints from `derived:memory`,
and emits a `context.read_plan.set` fact into `derived:context`. The read plan
references memory checkpoint event ids with `event_role = "memory_checkpoint"`;
it does not duplicate memory text or call a hidden retrieval service. Before
provider dispatch, `RuntimeServices` reduces the latest `context.memory` read
plan into a deterministic `<memory_context>` block, so recall is visible through
the same context assembly path as other model-visible facts. The V1 reference
is sequence-selected and bounded by `max_events`; semantic/vector ranking
remains a later source selector or operation concern.

`std::prompt.steer` consumes `turn.completed` or `approval.resolved` and emits
explicit control facts. In `request_continuation` mode it writes
`turn.continue.requested` with the next turn input. In `set_read_plan` mode it
writes `context.read_plan.set` selecting an existing instruction checkpoint by
event id. It does not load prompt text from application code or hide steering
inside a callback.

`std::failure.deadletter` consumes typed failed or blocked control facts and
projects them into `derived:deadletter` for inspection. Its reference binding
uses `trigger_match.status = "failed"` when watching `coupling.run.failed`
events, because the projection itself reuses the frozen `coupling.run.failed`
kind on a different stream. Already-deadlettered payloads are ignored by the
executor as a defensive no-op.

`std::retry.with_budget` consumes a typed `coupling.run.failed` fact and either
emits `turn.continue.requested` with the next explicit attempt or
`loop.budget_exhausted` when the configured attempt limit has been reached. The
reference config names `max_attempts`, `parent_turn_id`, `loop_id`,
`next_turn_input`, and optional `retryable_error_classes`; it never retries by
replaying an old operation implicitly.

`std::permission.approval_gate` consumes `tool.call.requested` and emits a
paired `approval.requested` plus `tool.call.suspended` into the control stream.
The approval request is an abstract durable fact with `approval_id`, subject,
tool name, request event id, reason, and resume token. It does not deliver a
Slack/Telegram/email/web message and it does not resolve the approval; those are
separate control-plane/channel surfaces.

V1 must-have templates:

| Template | Role | Maturity | Runtime executable | Purpose |
| --- | --- | --- | --- | --- |
| `std::queue.task` | controller | reference_only | yes | Turn a witnessed event into durable queued work; reference executor emits `turn.waiting`. |
| `std::queue.completion_callback` | controller | reference_only | yes | Continue, notify, or terminate after queued work completes; reference executor emits `loop.completed` or `turn.continue.requested`. |
| `std::context.spill` | projection | reference_only | yes | Discharge over-budget context into `context.summary.completed` and `context.read_plan.set`. |

Optional executable reference templates:

| Template | Role | Maturity | Runtime executable | Purpose |
| --- | --- | --- | --- | --- |
| `std::context.truncate` | controller | kernel_backed | yes | Emit a read plan with explicit `drop_range` plus retained raw tail. |
| `std::context.summarize` | projection | kernel_backed | yes | Emit a summary checkpoint plus read plan over a witnessed source range. |
| `std::memory.extract` | projection | reference_only | yes | Project completed turns or tool calls into observational memory checkpoints on `derived:memory`. |
| `std::memory.recall` | projection | reference_only | yes | Project selected memory checkpoints into a context read plan on `derived:context`. |
| `std::prompt.steer` | controller | reference_only | yes | Emit explicit continuation or instruction-read-plan control facts. |
| `std::prompt.dynamic_instructions` | projection | reference_only | yes | Project versioned instruction checkpoints into context assembly. |
| `std::permission.approval_gate` | controller | reference_only | yes | Emit paired `approval.requested` and `tool.call.suspended` facts for abstract approval. |
| `std::permission.tool_gate` | controller | kernel_backed | yes | Emit allow/rewrite/deny/wait control facts for tool calls. |
| `std::schedule.cron` | controller | reference_only | yes | Convert witnessed `timer.fired` occurrences for schedule mandates into bounded continuation or budget-exhausted facts. |
| `std::supervisor.spawn` | controller | kernel_backed | yes | Discharge `thread.spawn.requested` plus optional parent `turn.waiting`; the thread-spawn projector performs the child-thread effect. |
| `std::supervisor.child_completion` | controller | kernel_backed | yes | Join a routed child-completion fact into a parent continuation or terminal fact. |
| `std::retry.with_budget` | controller | reference_only | yes | Convert typed failure facts into explicit continuation or budget-exhausted control facts. |
| `std::failure.deadletter` | projection | reference_only | yes | Project failed or blocked control facts into `derived:deadletter` for inspection. |

`runtime_executable = true` means `RuntimeServices` can auto-schedule the
template through `StdlibCouplingExecutor` after the triggering boundary event is
appended. `maturity` is still about product completeness: a reference-only
template can be executable as a V1 reference implementation without claiming the
last word on the abstraction.

Kernel-backed templates already have a matching primitive or scheduler surface:
`std::context.truncate`, `std::context.summarize`,
`std::permission.tool_gate`, `std::supervisor.spawn`, and
`std::supervisor.child_completion`; all have executable V1 references today.
`std::supervisor.spawn` is pure at coupling time: it validates the bound
`threads.spawn` grant, discharges `thread.spawn.requested`, and lets the
durable thread-spawn projector perform the local child-thread effect and
witness `thread.spawned`.
`std::supervisor.child_completion` deliberately consumes a completion fact that
has already been routed into the supervising stream/control plane; cross-thread
or cross-host child event routing remains outside the V1 stdlib claim.
`std::permission.approval_gate` is a reference executable template for the
abstract approval fact path. Channel ingress/egress templates remain marked
`channel_decision_required` so V1 does not imply a completed Telegram, Slack,
email, or web approval loop before that interface is chosen.

## V1 Boundary

The guest SDK currently exposes HTTP request, VFS file-open/read/close, source
read, and sink write calls. The implemented V1 source/search slice
intentionally does not include file write, directory listing, non-GET HTTP, or
product-specific operations.

HITL is not claimed as a completed channel-specific stdlib package in V1. V1
does expose `thread/approvals/list` and `thread/waiting/list` inspection over
durable control facts, plus `approval/resolve` for witnessing an abstract
approval decision into the control stream. Channel binding, delivery through a
specific Slack/Telegram/email/web surface, and automatic approval-to-resume
completion remain behind a concrete interface decision.
