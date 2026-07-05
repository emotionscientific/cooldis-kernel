# Cooldis Agent CLI

Status: V1 publish and local run slice.

`cooldis agent` is the declaration and publication lane for agent manifests. It
is distinct from:

- `cooldis tool`, which builds and publishes capability artifacts;
- `cooldis rpc`, which exposes the control plane to clients and sandboxes.

## Commands

```sh
cooldis init release-verifier
cooldis agent plan release-verifier/cooldis.agent.toml \
  --operations-registry-root .cooldis/operations
cooldis agent publish release-verifier/cooldis.agent.toml \
  --operations-registry-root .cooldis/operations
cooldis blob publish release-verifier/prompts/system.md --name identity
cooldis agent list
cooldis agent show agent://release-verifier@0.1.0
cooldis agent run agent://release-verifier@latest --input "check the branch"
```

`cooldis init <name>` is the V1 folder-first entrypoint. It creates:

```text
<name>/
  cooldis.agent.toml
  prompts/system.md
  components/operations.toml
  components/couplings.toml
  operations/README.md
```

The generated manifest is valid immediately for planning. `prompts/system.md`
is the folder-first system prompt: `agent plan` and `agent publish` lower it to
an immutable blob resource and wire it into the `identity` static context source
when that source has no explicit input. `components/couplings.toml`
contains the frozen V1 coupling template ids: async queue, completion callback,
context spill/truncate/summarize, memory preview, schedule/retry/deadletter,
permission/control, prompt steering, and channel ingress/egress. Custom
operation packages live under `operations/` and should be published first; then
add their pinned `op://...@sha256:<hash>` refs to `cooldis.agent.toml` before
publishing the agent. `cooldis agent init --out path.toml` keeps the old
single-manifest file form for compatibility.

`plan` is the dry-run boundary for agent records. It parses the source manifest,
validates the identity envelope, resolves the canonical JSON shape, computes
source and manifest hashes, and prints the publish summary. For a folder-first
project, it may publish `prompts/system.md` into the idempotent blob registry so
the preview includes the same `context_source: identity -> resource://...`
binding that `publish` will store. It does not write an agent record. If an
operations registry is locatable, `plan` verifies each `op://` tool row against
that registry and annotates the `resolved_ref:` line with `[verified]`. Without
an operations registry, it still succeeds and reports those refs as
`[unverified-offline]`.

`publish` reruns the same resolution path and writes a durable local record under
`.cooldis/agents`. The active record lives in `records/<name>.json`; immutable
version records live in `versions/<name>/<version>.json`. Republish of the same
name and version is allowed only when the manifest hash is identical. When a
folder-first prompt is lowered, the publish receipt pins the blob hash through
the resolved blob resource and the `identity` context source.

`publish` is a registry oracle for `op://` refs. Every operation tool row must
name a local operation record, pin a published version hash, select either the
whole record or a declared operation, and declare grants that cover the selected
operation requirements. Use `--operations-registry-root <dir>` when the
operations registry is not the conventional `.cooldis/operations` root. Missing
or fabricated operation refs reject before the agent record is written.

`cooldis blob publish <file> [--registry-root .cooldis/blobs] [--name <name>]`
publishes an arbitrary file as an immutable blob artifact and prints the
`resource://artifact/sha256:<hash>` ref. Re-publishing the same content returns
the same digest and is a no-op. Agent manifests consume blob artifacts through
`[[resources]] kind = "blob"` rows and static context sources whose `input`
names the resource.

`list` and `show` inspect published records from the local registry. They are the
minimum discovery surface required once publication exists.

`run` starts a manifest-backed app-server thread from a published `agent://...`
ref, sends one input turn, prints the assistant output, then prints the manifest
compile and bind receipt event ids. Use `--registry-root <dir>` to point at a
non-default local agent registry.

The blob registry root derives from the agent registry root everywhere agents
are resolved: `<registry-root>/blobs`, or the sibling `blobs` directory when
the agent root's basename is `agents` (so the conventional `.cooldis/agents`
pairs with `.cooldis/blobs`). `agent plan`, `agent publish`, and `agent run`
all share this derivation; an explicit app-server `blob_registry_root` override
still wins.

## Current Manifest Shape

The V1 parser requires identity and at least one model profile:

```toml
[agent]
name = "release-verifier"
version = "0.1.0"
description = "Checks a release branch."
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[runtime]
default_cwd = "."
streaming = false
```

The raw resolved manifest is preserved as canonical JSON in the published record
so later schema expansion can add more provider refs, operation refs, resources,
policies, grants, context defaults, and runtime defaults without changing the
publication boundary.

## Removal Semantics

Plain `delete` is not part of this slice. A published agent record is a durable
artifact. The later removal surface should distinguish:

- `unpublish`: withdraw an active alias or discovery pointer while keeping
  immutable records available for audit and resume;
- `gc`: remove unreferenced local blobs or records when a local registry is being
  cleaned intentionally.
