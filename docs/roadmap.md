# Roadmap

Verlet V1 is the runtime-primitives release. V2 is the serverless agent
platform direction.

## V1 Direction

V1 proves local packageable agents with durable execution.

Core proofs:

- build and publish ABI-backed operations;
- publish and run an agent manifest that pins those operations;
- run a durable local agent through CLI, daemon, RPC, ACP, and MCP surfaces;
- record events and receipts that explain tool calls, provider calls, and
  runtime decisions;
- package release binaries for supported targets;
- keep provider credentials, runtime secrets, and public examples separated.

## V2 Direction

V2 adds the product platform around the kernel:

- managed cloud placement;
- package and tool registries;
- private package sharing;
- richer scheduling and trigger surfaces;
- stateful harness export, import, and diff;
- stronger provider, sandbox, observability, and policy adapter boundaries;
- cloud promotion from local records;
- production ownership surfaces for events, secrets, quotas, audit, and
  cancellation.

## Discipline

```text
Code builds primitives.
Manifests compose agents.
Runtime enforces authority.
Release gates prove the public surface.
```
