# Cooldis

Cooldis is an open serverless agent platform.

Define the agent, not the app around it.

Build agents locally. Publish them to managed cloud. Install agents like
packages. Govern them like infrastructure. Keep the agent definition portable so
the managed platform is acceleration, not lock-in.

Cooldis treats an agent as a declarative unit: describe the behavior, tools,
resources, secrets, permissions, context policy, and placement needs before the
agent runs. The runtime turns that declaration into something it can inspect,
grant, run, resume, publish, revoke, and audit.

## Why Care

Serious agents are not hard because prompts are hard. They are hard because every
agent turns into a small backend: deployment, tools, secrets, permissions,
runtime state, logs, failures, retries, rollback, and a place to run.

Cooldis gives that backend shape to the runtime. You focus on the agent: what it
does, what tools it can use, what resources it can see, and what powers it is
allowed to exercise.

## What You Can Do

- Build an agent locally from a manifest.
- Attach tools, resources, secrets, and runtime grants.
- Publish static prompt and context files as immutable blob resources.
- Publish local skill directories, author manifests against a package name, and
  receive a content hash in the bind receipt.
- Opt into conventional workspace skill discovery and retain the exact bind
  witness across resume/fork while later workspace reads remain live.
- Run the agent through a governed runtime instead of a one-off app server.
- Inspect events, tool calls, receipts, and artifacts.
- Resume work from durable records.
- Start idempotent process handles whose terminal outcomes re-enter the owning
  thread through durable ingress.
- Place manifest-bound child threads in separate local processes through the
  daemon's authenticated store-backed queue and stream-sync surface.
- Publish the same declared agent shape toward managed placement when the cloud
  path is ready.

## Declarative Agent Shape

```text
agent declaration
+ capability bindings
+ resources
+ context pipeline
+ couplings and grants
+ placement requirements
-> governed agent runtime
```

The runtime owns lifecycle, permissions, tool visibility, operation projection,
events, cancellation, resume, and audit records. Product code can configure and
call Cooldis without becoming the runtime.

## Mental Models

If you know Vercel, think of Cooldis as the local-to-managed path for agents:
define locally, inspect the runtime shape, publish when ready, observe what
happened, and keep the source definition portable.

If you know Terraform or Dockerfiles, the familiar part is declaration. Declare
the agent, tools, resources, grants, behavior, and placement instead of hiding
that shape inside application code. Cooldis turns the declaration into a running
agent across local and future cloud runtimes.

If you know package managers, the destination is installable agents and tools:
fetch a versioned agent or tool package, inspect the powers it asks for, grant
what you approve, and run it under policy.

## Current Status

Cooldis is experimental. The repository is focused on V1 runtime primitives:
agent manifests, operation publishing, ABI contracts, macro-authored custom Wasm
couplings, witnessed OpenAPI operation imports, offline coupling replay,
local runtime execution, provider adapters,
virtual bash, VFS-backed oversized-output spill receipts,
skill-package resources with bind-time name pinning, witnessed workspace skill
discovery without a second mount, bind-plane local host-workspace mounts,
daemon/RPC surfaces, daemon-embedded store-primary
stream propagation, store-backed remote child placement, and the proof path for
packageable local agents.

The managed cloud, public package registry, private marketplace, and stateful
harness product layer are V2 direction.

## Read Next

- [Getting Started](getting-started.md)
- [Declarative Agents](concepts/declarative-agents.md)
- [Local To Managed Deployment](concepts/local-to-managed.md)
- [Permissions And Governance](concepts/permissions-and-governance.md)
- [Runtime Primitives](developers/runtime-primitives.md)
- [Daemon And Remote Placement](daemon.md)
- [Protocol Surfaces](developers/protocol-surfaces.md)
- [OpenAPI Operation Imports](openapi-adapter.md)
- [How Cooldis Is Tested](how-cooldis-is-tested.md)
- [Roadmap](roadmap.md)
