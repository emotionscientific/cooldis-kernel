# Runtime Primitives

Cooldis is a standalone Rust runtime workspace. Product logic belongs outside the
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

Product systems configure and call Cooldis. They should not become part of the
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
