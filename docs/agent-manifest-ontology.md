# Agent Manifest Ontology

Status: design note for the AgentManifest epic.

An agent manifest is a declarative composition of versioned, publishable
artifacts. It should describe what an agent is allowed to be and do, while the
thread records what actually happened.

Typed V1 schema note: `crates/cooldis-kernel/src/agent/manifest_schema.rs` is
the source of truth for the shipped V1 manifest shape. The registry layer reads
that typed form and stores publish records; future design notes should map back
to the Rust schema before changing accepted keys.

Lexicon note: the default manifest is the kernel-synthesized
`agent://cooldis/default` record published at app-server startup. A thread that
starts without an agent ref and without explicit envelope params binds that
manifest, so thread lineage always has a manifest ref even for the plain local
start path.

```text
AgentManifest
  versioned declaration of composition, powers, context, resources, couplings,
  reserved hooks, policies, and runtime defaults

Thread
  live or persisted execution state: turns, history, event log, checkpoints,
  active streams, process handles, pending tool calls, and runtime-local state
```

The manifest may declare policies for history, events, resources, and tools. It
must not embed live thread state.

## Versioned Artifact Principle

Everything a manifest names should resolve to a versioned publishable artifact
or a scoped runtime reference with an explicit version policy.

Examples:

```text
op://data/csv_profile@sha256:...
skill://release-review@sha256:...
resource://artifact/sha256:...
assembler://cooldis/naive-assembly@0
agent://release-verifier@0.3.1
```

Mutable names can exist for ergonomics, but they should lower to immutable
records at publish or run time. In a folder-first agent project,
`prompts/system.md` lowers to a blob resource and is wired into the `identity`
static context source:

```text
agent publish
  resolve mutable refs
  validate grants and compatibility
  store immutable manifest record
  emit publish receipt with resolved versions and hashes
```

This keeps agent definitions portable, inspectable, rollbackable, and safe to
resume after host restart.

### Operation References

Agent tool rows address published operation record artifacts with one of two
content-addressed forms:

```text
op://<record>@sha256:<hash>
op://<record>/<operation>@sha256:<hash>
```

The single-segment form binds the whole record, so bind-time grants must cover
every operation declared by that record. The two-segment form selects one
operation within the record; bind-time grants are checked against that
operation's required capabilities, and the thread catalog exposes only the
selected operation for that binding. In both forms, the hash addresses the
record artifact, not a per-operation artifact.

## Components

### Identity

The manifest's own durable identity.

```text
name
version
description
labels
publisher metadata
compatibility range
```

### Model

Provider and model policy. Raw secrets should not live here; use credential
references.

```text
provider
model
adapter/protocol
params
fallback policy
credential refs
```

### Harness Is Emergent

Harness is not a manifest component.

It is the name for the assembled execution envelope produced by the tangible
manifest components:

```text
model profiles
tools
resources
skills
context pipelines
couplings
hooks (reserved; host debug only)
policies and grants
topology
IO
persistence
runtime defaults
```

Cooldis may export, diff, or inspect a resolved "portable harness" as a useful
read model, but an agent author should not declare `harness = ...` as a separate
primitive. Codex, Pi, Claude, local app-server, or a sandbox are runtime adapters
or placements, not harnesses.

### Tools

Published callable capabilities available to the agent.

```text
tool refs
operation names
aliases/surfaces
schemas
required grants
availability scope
tool routing rules
```

Tools should be referenced as published artifacts. The manifest should not copy
tool artifacts into itself.

First-party kernel tools are still operation artifacts. For example,
`cooldis-threads/thread_spawn` is bound as
`op://cooldis-threads/thread_spawn@sha256:<record-hash>` and requires the
`threads.spawn` grant. The manifest binds published operation records by
artifact hash rather than copying tool implementations into itself.

### Resources

Static or external artifacts made available to the agent, context builder, or
tools.

```text
blobs
files
artifacts
skills
prompt packs
templates
datasets
schemas
indexes
```

Resource declarations say that something exists and may be used. They do not by
themselves make the resource model-visible or writable.

### Skills

A skill is a published markdown resource package. In V1 it is declared as a
`[[resources]]` row with `kind = "skill"` and a content-addressed
`skill://<package>@sha256:<hash>` ref. The `[skills]` manifest section remains
reserved until skills need their own grants or executable entrypoints.

```text
publish lane: cooldis skill publish <dir>
registry: .cooldis/skills
package shape: <name>/SKILL.md files
metadata: optional frontmatter name, description, trigger_hint
fallbacks: name from dirname, description from first non-heading line
```

At bind, the skill package is loaded from the registry, the package digest is
recorded in the bind receipt, and the kernel renders a deterministic static
index:

```text
<skill name> — <description>
```

The index is materialized as a pinned `kernel://assembler/static` context
segment. Skill bodies are not all pinned into model context; they are mounted
read-only in the thread VFS at `/skills/<name>.md`, where existing virtual bash
commands such as `cat` or `view` can read them. Skill resources grant no
ambient host authority.

### Context

How model-visible context is assembled: pipelines of independent sources,
each pairing an assembler ref with a selector and a budget share. The kernel
performs the deterministic merge, the final budget fit, and the receipt.

```text
system prompt refs
pipelines and sources (assembler ref, selector, budget share)
compaction policy
attachment policy
token budget policy
canonicalization rules
resource inclusion rules
history selection rules
record selection over discharged events
```

Context pipelines consume resources, discharged events, and thread history.
Blob access belongs to resources and grants; blob inclusion in a prompt
belongs to the pipeline.

### Observations And Memory

Observations are discharged events: records produced by couplings
(projections and controllers) over event streams or resources, living in
streams beside everything else.

```text
event selectors
projection functions
provenance rules
retention rules
retrieval selectors
```

Observations carry provenance back to their source events and resources.
They never replace or rewrite the append-only event stream they were
produced from. Declared couplings may use built-in `std::` executors or custom
Wasm operations referenced by pinned `op://<record>/<operation>@sha256:<hash>`
refs. Custom coupling capsules are pure compute: the invocation carries trigger
and selected source events plus config, and the guest may only propose discharge
events. The kernel validates the declared sink, applies stream grants, enforces
budgets/depth, and stamps discharged provenance. HTTP, VFS, secrets, and other
effectful imports stay tool capabilities rather than coupling authority.

### Hooks

Lifecycle and interception points.

```text
pre_user_message
post_user_message
pre_context_build
post_context_build
pre_model_call
post_model_call
pre_tool_use
post_tool_use
on_error
on_compaction
on_thread_start
on_thread_resume
```

Hooks are host-scope debug tooling, not a manifest authority surface. The
manifest `[hooks]` table remains reserved; runtime control that must be replayed
or audited belongs in witnessed couplings.

### Policies And Grants

The authority boundary.

```text
capability grants
deny/allow rules
budget limits
network limits
filesystem limits
resource read/write grants
tool approval rules
child-thread creation rules
```

The model cannot grant itself new powers. Publish and start must validate that
declared tools, resources, couplings, and effects are allowed.

### Topology And Delegation

How this agent may create or interact with child threads.

```text
allowed child manifest refs
child-thread operation policy
parent/child scope inheritance
branch/fork policy
concurrency limits
supervisor behavior
```

Child agents should also resolve to versioned manifests. Inline child manifests
are acceptable for local authoring but should compile to immutable records for
durable publish.

### IO Interfaces

External surfaces over the same runtime object.

```text
CLI surface
MCP surface
app-server surface
webhook/chat/event ingress
egress adapters
stream/event subscriptions
```

These are surfaces, not separate authority layers.

### Persistence

Where durable agent-adjacent state lives.

```text
thread store policy
event stream and discharged-record store policy
artifact store refs
tool binding refs
agent manifest refs
resume behavior
```

The manifest declares persistence policy. Stores contain the actual records.

### Build And Provenance

How referenced artifacts are built or verified.

```text
builder kind: cargo | nix | npm | none
source refs
lockfile/hash refs
artifact hashes
publish receipts
resolved dependency graph
```

Nix belongs here as an optional builder/provenance backend. It should not be the
canonical manifest language.

### Runtime Defaults

Defaults used when starting a thread from the manifest.

```text
initial turn template
default cwd/session/project
timeout policy
cancellation behavior
logging/trace level
streaming defaults
allowed runtime overrides
```

### Thread Start Lowering

Every app-server `thread/start` binds either an explicit `agentRef` or the
kernel-synthesized default manifest. Legacy-looking start parameters are
interpreted only against that bound manifest:

- `model` and `modelProvider` select exactly one declared `model_profiles` row.
  If no declared profile matches, or the selector is ambiguous, the start fails
  closed and the error lists the declared profiles.
- `cwd` lowers to `runtimeOverrides.defaultCwd`, so the manifest's runtime
  override allowlist decides whether it may change the effective working
  directory.
- non-empty `capsuleBindings.operationNames` is rejected. Tool/operation
  authority comes from manifest tool rows, published operation refs, and bind
  receipts, not start-time operation injection.

The default manifest may contain synthesized `bash_tool` rows for daemon
operation-binding config (`global_operation_names` and
`load_all_active_when_unbound`). Those rows are pinned to the active artifact
hashes resolved at startup, so a bare thread's bind receipt shows the exact
operations it received. If those config knobs are set, the daemon needs an
operation registry root so synthesis can resolve the declared rows; with no
operation-binding config, registries remain optional.

Thread-control tools follow the same rule. At startup, a daemon with an
operation registry root publishes the kernel-native `cooldis-threads` record and
the default manifest declares five `direct_tool` rows pinned to it:
`thread_spawn`, `thread_submit`, `thread_wait`, `thread_status`, and
`thread_cancel`. Those rows carry the `threads.spawn`, `threads.control`, and
`threads.read` grants required by the package. A manifest with
`allow_child_agents = false` and no thread rows receives no thread-control
tools, including inside virtual bash. If such a manifest declares a
`thread_spawn` row, bind fails because that row requires `threads.spawn`.

## Example Shape

```toml
[agent]
name = "release-verifier"
version = "0.3.1"

[[model_profiles]]
id = "default"
provider_ref = "provider://local/default"
model_ref = "model://local/default"

[[tools]]
type = "direct_tool"
id = "csv_profile"
tool_name = "csv_profile"
operation_ref = "op://data/csv_profile@sha256:..."
grants = []

[[resources]]
name = "release-review"
kind = "skill"
ref = "skill://release-review@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
mount = "context"
mode = "read"

[[resources]]
name = "release-playbook"
kind = "blob"
ref = "resource://artifact/sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
mount = "context"
mode = "read"

[[context.pipelines]]
id = "default"

[[context.pipelines.sources]]
id = "playbook"
assembler = "kernel://assembler/static"
input = "release-playbook"
pinned = true

[[context.pipelines.sources]]
id = "memory"
assembler = "kernel://assembler/record-select"
select = { kind = ["user_preference"], scope = "thread" }
budget_share = 0.1

[[context.pipelines.sources]]
id = "history"
assembler = "kernel://assembler/anchored-window"
select = { stream = "thread", read_plan = "history.default", fallback = "start" }
budget_share = "rest"

# [hooks] is reserved. Use witnessed couplings for replayable control.

[policies]
network = "deny"
filesystem = "vfs"
allow_child_agents = true

[[delegates]]
name = "test-runner"
ref = "agent://test-runner@0.2.0"

[builder]
kind = "nix"
target = ".#agent-tools"

[runtime]
default_cwd = "workspace"
streaming = true
```

## Boundary Summary

```text
resources + policies/grants
  control access to blobs, skills, files, artifacts, datasets, and schemas

context pipelines
  decide what becomes model-visible, when, and how

tools/effects
  perform capability-checked work over resources and runtime surfaces

thread state
  records execution; it is not embedded into the manifest
```
