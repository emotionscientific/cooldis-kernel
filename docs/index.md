# Verlet

Verlet is an open serverless agent platform.

Define the agent, not the app around it.

Build agents locally. Publish them to managed cloud. Install agents like
packages. Govern them like infrastructure. Keep the agent definition portable so
the managed platform is acceleration, not lock-in.

Verlet treats an agent manifest as a declarative unit and preset: describe model
profiles, policies, runtime defaults, workspace and placement needs, context,
and proposed tool/resource bindings before the agent runs. At start, the
runtime expands the preset into an opening sequence of recorded attachments.
The thread's toolset is the fold of those binding events, not a standing
manifest document.

## Why Care

Serious agents are not hard because prompts are hard. They are hard because every
agent turns into a small backend: deployment, tools, secrets, permissions,
runtime state, logs, failures, retries, rollback, and a place to run.

Verlet gives that backend shape to the runtime. You focus on the agent: what it
does, what tools it can use, what resources it can see, and what powers it is
allowed to exercise.

## What You Can Do

- Build an agent locally from a manifest.
- Attach tools and resources, with explicit secret and private-network config.
- Publish static prompt and context files as immutable blob resources.
- Publish local skill directories, author manifests against a package name, and
  receive a content hash in the bind receipt.
- Import conventional external `SKILL.md` directories into ordinary skill and
  blob records with explicit script degradation and inert hook configuration.
- Opt into conventional workspace skill discovery and retain the exact bind
  witness across resume/fork while later workspace reads remain live.
- Run the agent through a governed runtime instead of a one-off app server.
- Inspect events, tool calls, receipts, artifacts, and the effective bind
  envelope recorded for a thread.
- Resume work by re-folding the thread's durable records, without re-binding or
  consulting the current agent registry.
- Start idempotent process handles whose terminal outcomes re-enter the owning
  thread through durable ingress.
- Place manifest-bound child threads in separate local processes through the
  daemon's authenticated store-backed queue and stream-sync surface.
- Publish the same declared agent shape toward managed placement when the cloud
  path is ready.

## Declarative Agent Shape

```text
manifest preset
+ model profiles, policies, runtime defaults, workspace, and context
+ proposed tools, resources, couplings, and attachment config
-> bind expansion
-> recorded binding.attached / binding.detached history
-> folded toolset and governed runtime
```

The runtime owns lifecycle, permissions, tool visibility, operation projection,
events, cancellation, resume, and audit records. Product code can configure and
call Verlet without becoming the runtime.

## Mental Models

If you know Vercel, think of Verlet as the local-to-managed path for agents:
define locally, inspect the runtime shape, publish when ready, observe what
happened, and keep the source definition portable.

If you know Terraform or Dockerfiles, the familiar part is declaration. Declare
the agent, tools, resources, attachments, behavior, and placement instead of hiding
that shape inside application code. Verlet turns the declaration into a running
agent across local and future cloud runtimes.

If you know package managers, the destination is installable agents and tools:
fetch a versioned agent or tool package, inspect its attachments and package
capabilities, and run it under policy.

## Current Status

Verlet is experimental. The repository is focused on V1 runtime primitives:
agent manifests, operation publishing, ABI contracts, macro-authored custom Wasm
couplings, witnessed OpenAPI operation imports, offline coupling replay,
local runtime execution, provider adapters including OpenAI Codex access through
a user's ChatGPT plan,
virtual bash, VFS-backed oversized-output spill receipts,
skill-package resources with bind-time name pinning, witnessed workspace skill
discovery without a second mount, bind-plane local host-workspace mounts,
daemon/RPC surfaces, daemon-embedded store-primary
stream propagation, store-backed remote child placement, and the proof path for
packageable local agents. Each running app-server instance publishes an endpoint
record beside its owned state roots so local clients can discover its Unix
socket without scanning ports or guessing paths.

The managed cloud, public package registry, private marketplace, and stateful
harness product layer are V2 direction.

The runtime also exposes a multi-tenant host facade and `verlet host run`: one
listener selects an instance from an explicit credential-digest route, while
the selected instance remains responsible for authentication, witnessing, and
its own default mandate clock and shutdown boundary. Non-loopback
private-network binds require an explicit config opt-in; see
[RPC Control Plane](app-server.md#config-driven-multi-instance-host).

## Read Next

- [Getting Started](getting-started.md)
- [Declarative Agents](concepts/declarative-agents.md)
- [Local To Managed Deployment](concepts/local-to-managed.md)
- [Permissions And Governance](concepts/permissions-and-governance.md)
- [Runtime Primitives](developers/runtime-primitives.md)
- [Chat Console](chat.md)
- [Provider Setup](provider-setup.md)
- [Daemon And Remote Placement](daemon.md)
- [Protocol Surfaces](developers/protocol-surfaces.md)
- [Threat Model](threat-model.md)
- [Frozen Format IDs](format-ids.md)
- [OpenAPI Operation Imports](openapi-adapter.md)
- [How Verlet Is Tested](how-verlet-is-tested.md)
- [Roadmap](roadmap.md)
