# Agent Manifest Ontology

Status: design note for the AgentManifest epic.

An agent manifest is a declarative composition of versioned, publishable
artifacts. It should describe what an agent is allowed to be and do, while the
thread records what actually happened.

Typed V1 schema note: `crates/cooldis-agent/src/manifest_schema.rs` is
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
static context source, including when the manifest declares an explicit
`[context]` pipeline whose `identity` static source leaves `input` unset:

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
primitive. Things like an external agent CLI (Codex, Claude), Pi, a local
app-server, or a sandbox are runtime adapters or placements, not harnesses.

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

Every `bash_tool`, `direct_tool`, and `protocol_tool_import` row may declare an
effect class:

```toml
effect_class = "idempotent" # pure | idempotent | at-most-once
```

The field defaults to `at-most-once`. `pure` and `idempotent` authorize a
re-execution after an interrupted invocation; `at-most-once` instead produces
a witnessed conservative failure. A recorded outcome is reused for every
class only when its argument fingerprint and bound manifest snapshot match.
Fingerprints are SHA-256 hashes of the canonical JSON tool name and arguments.
Journal events written before fingerprints existed retain their legacy request
event and call-id reuse behavior. Pinned protocol imports copy their declared
class into the bind receipt; dynamic tool-universe calls remain
`at-most-once`.

Every `grants` array on `bash_tool`, `direct_tool`,
`protocol_tool_import`, and coupling rows accepts either the existing bare
capability string or an expiring object:

```toml
grants = [
  "fs.read:/workspace",
  { capability = "net:https://example.com", expires_at = "2026-07-16T20:00:00Z" },
]
```

`expires_at` is an absolute RFC3339 UTC instant; duration shorthand is not
accepted. A bare string has no expiry and retains the legacy serialized and
content-addressed manifest shape. If any grant on a tool row has already
expired at bind, the whole tool row is omitted from the presented surface and
the bind receipt records the lapsed grant and exclusion. Authority remains
live after bind: form snapshots remain stable for a running turn, but the next
invocation after a grant expires fails closed and names the capability and
expiry. A later bind with a fresh grant is the only way to restore that power.

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

Skills have two declarative lanes. Workspace discovery gives local coding
agents conventional-directory ergonomics. Published packages give manifests an
immutable registry lane that is portable outside one workspace. The lanes may
be used together, but skill names must be unique across both.

Workspace discovery is off by default:

```toml
[workspace]
guest_path = "/workspace"
min_mode = "rw"

[skills]
discover = true
path = ".agents/skills" # optional default
```

`skills.path` is relative to the resolved workspace root and cannot contain a
`..` component or control characters. Enabling discovery without a `[workspace]`
requirement is invalid. At bind, the kernel traverses
`<workspace>/<skills.path>` once for conventional `<skill>/SKILL.md` entries and
applies the same frontmatter and fallback parsing as package publication.
Symlinked skill directories are not followed; the discovery root and any
symlinked `SKILL.md` target must resolve inside the witnessed workspace, and the
opened file identity is checked again before its contents are parsed. A missing
or empty directory is a valid, witnessed empty discovery because the scope is
user-provisioned.

Discovery does not create a skill mount. The files remain in the live workspace
and the injected index points to workspace-relative `SKILL.md` paths that
existing workspace bash/read surfaces can open. Mounting a bind-time snapshot
beside that live tree would expose two versions of one workspace scope and
violate workspace law.

The bind receipt's `skill_discovery` field records the normalized discovery
path and, for every entry, its name, description, workspace-relative path, and
content SHA-256. Resume and fork rehydrate that witness and its deterministic
index without traversing the directory again. A later workspace read may see
edited content; this drift is allowed and provable by comparing the bind
witness with later read receipts. Cooldis witnesses workspace state at bind; it
does not police subsequent edits.

The published-package lane is declared as a `[[resources]]` row with
`kind = "skill"`. The ref may be floating as `skill://<package>` or pinned as
`skill://<package>@sha256:<hash>`.

```text
publish lane: cooldis skill publish <dir>
registry: .cooldis/skills
package shape: <name>/SKILL.md files
metadata: optional frontmatter name, description, trigger_hint
fallbacks: name from dirname, description from first non-heading line
output: pinned skill://<package>@sha256:<hash> and floating skill://<package>
```

Conventional external `SKILL.md` directories can be compiled into this same
published-package lane with `cooldis skill import`. The importer appends direct
markdown references, publishes assets as blob resources, makes omitted scripts
visible as deterministic degradation text, and leaves hook/MCP configuration
inert. See [Cooldis Agent CLI — Importing external skill
directories](agent-cli.md#importing-external-skill-directories) for the conversion
and dry-run contract.

Authors and manifests speak names; receipts speak hashes. At bind, a floating
ref resolves the active local registry record once, and the pinned ref and
package digest are recorded in the bind receipt. A pinned ref loads that exact
immutable version without consulting the active record. Existing bound threads
keep their witnessed version across resume and fork; a later bind resolves the
then-current active version. Unknown names and duplicate names within or across
registry packages and workspace discovery fail closed. The kernel renders a
deterministic package index:

```text
<skill name> — <description>
```

The index is materialized as a pinned `kernel://assembler/static` context
segment. Skill bodies are not all pinned into model context; they are mounted
read-only in the thread VFS at `/skills/<name>.md`, where existing virtual bash
commands such as `cat` or `view` can read them. Skill resources grant no
ambient host authority.

The workspace-discovery index adds the workspace-relative body path:

```text
<skill name> — <description> — <skills.path>/<directory>/SKILL.md
```

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
belongs to the pipeline. When `prompts/system.md` exists, declaring an
explicit `input` on the `identity` static source is rejected because prompt
provenance would be ambiguous; either leave `input` unset for folder-first
lowering or move the file out of `prompts/system.md` and point at a declared
resource explicitly.

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
declared tools, resources, couplings, and effects are allowed. Coupling grants
use the same bare-string or expiring-object form as tool grants. An expired
coupling row is excluded at bind; a coupling grant that lapses later fails
before the next source read, executor invocation, or sink write, with the lapse
recorded on the control stream.

### Admission

There is one admission law: no turn entry is scheduled unless the admissible set
and selected decision have first been recorded. Turn-starting surfaces project
that law as `admission.decided` on the thread's control stream before the
runtime command is enqueued. Daemon ingress records its route policy context
(`route_id`, route policy hash, admissible route decisions, and source
`io.ingress.received` event ids) and then schedules through the already-admitted
runtime path so it cannot double emit.

Non-daemon turn-starting surfaces use the `surface:<name>` route convention.
The app-server RPC path records the surface of the initialized client:
`surface:mcp-adapter`, `surface:acp-adapter`, and `surface:debug-rpc` for the
bundled adapters, `surface:app-server-rpc` for any other client. The surface
label is attribution and provenance only; admission authority is the same
declared surface policy for all of them. Direct `RuntimeHost::submit*` records
`surface:host-submit`; kernel thread-to-thread submission records
`surface:kernel-thread-submit`. Their policy hash is the canonical hash of the
declared trivial surface policy, not an empty string. The current journal
schema has queue/steer/interrupt/fork/observe/reject/coalesce decision values,
so the trivial admitted surface policy lowers to `decision = "queue"` with
`admissible = ["queue"]`. App-server RPC first witnesses the input as
`io.ingress.received` and names that event in `source_ingress_event_ids`;
direct host submit has no ingress event, so its source list is intentionally
empty. CLI chat submits through the operator RPC path and inherits the
app-server surface admission record. The registry of turn-entry surfaces and
the coverage ratchet over them live in `kernel/admission.rs`.

In-loop continuations use the same admission law through the continuation gate:
`turn.continuation.accepted` or `turn.continuation.rejected` records the
decision over the requested continuation. Those event kinds remain distinct for
now. Event-kind unification between continuation decisions and
`admission.decided` is a named roadmap item, not part of the current schema.

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

### Workspace Requirement

A manifest may declare one abstract host-workspace requirement without naming a
machine-local host path:

```toml
[workspace]
guest_path = "/work"
min_mode = "rw" # optional; defaults to "ro"
```

`guest_path` is an absolute, normalized virtual path. `/` and the `/skills`
subtree are reserved. `min_mode` is the least authority the agent needs. The operator's
daemon default or bind-time RPC override supplies the concrete host directory
and `ro`/`rw` mode on the binding plane. The override wins over the default;
the bind fails if either side is missing or if an undeclared mount is supplied.
The effective canonical host path, guest path, and mode live in the bind receipt
and thread lifecycle metadata, never in the content-addressed manifest.

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
max_tool_rounds = 64
```

`max_tool_rounds` limits model/tool batches per turn. When omitted, the
runtime default is `8`, preserving the existing tool-loop behavior. A positive
integer raises or lowers the cap; `max_tool_rounds = "unlimited"` is the
explicit opt-in sentinel for no round cap. Unlimited rounds remain bounded by
the turn's time, token, byte, and cancellation budgets. Reaching a finite cap
fails the turn; it never truncates tool results silently.

`max_tool_rounds` is also a deny-by-default runtime override key. A caller may
set it only when `[runtime.overrides].allow` contains `"max_tool_rounds"`.

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
