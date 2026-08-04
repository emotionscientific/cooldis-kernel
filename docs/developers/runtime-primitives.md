# Runtime Primitives

Verlet is a standalone Rust runtime workspace. Product logic belongs outside the
runtime.

## Core Primitives

The runtime owns:

- multi-tenant host and supervisor;
- thread lifecycle, subthread relationships, events, cancellation, resume, and
  shutdown;
- provider adapters and canonical model history;
- virtual bash and VFS;
- sandbox and process-shaped execution;
- operation ABI contracts;
- tool publishing and operation registry;
- CLI, RPC, MCP, HTTP, and model-tool projections;
- daemon and app-server surfaces.

## Boundary Rule

Product systems configure and call Verlet. They should not become part of the
runtime workspace.

Auth products, billing, dashboards, invite flows, Railway deployment, and
app-specific ledgers belong in the product repo or adapter layer.

## Contract First

Runtime changes should start from the contract surface:

- ABI;
- thread lifecycle;
- operation registry;
- provider adapter;
- daemon/RPC;
- VFS;
- projection.

Add or update focused contract tests before broad implementation.

## External Interruption During Turns

Turn input has three interruption tiers. `queue` is passive and starts after the
active turn. `steer` is delivered at a tool-round boundary: it does not cancel a
running provider request or tool call, and completed tool results remain in
history. `interrupt` is the only tier that fires the active turn's cancellation
token. Every running tool invocation receives a child of that token.

The bound manifest value `runtime.cancellation_grace_ms` is measured from the
instant the token fires; when absent, the runtime uses five seconds. Tool
invocations run in owned tasks so stopping the turn loop cannot drop them in the
middle of a durable write. A call that settles after its token fires but within
grace records `cancelled_acknowledged`. A call still running at grace records
`cancelled_exceeded_grace` when its detached task eventually settles. That
completion may follow the turn's terminal cancellation record. Calls witnessed
as requested but blocked behind batch hold edges settle immediately as
`cancelled_acknowledged` without launching.

On resume, the runtime reads the complete cancelled child-turn window and
fail-closes any requested call that still has no completion as
`cancelled_exceeded_grace`. Process-backed tools terminate their OS process
group on cancellation and retain the partial stdout and stderr they observed.
