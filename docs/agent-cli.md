# Cooldis Agent CLI

Status: V1 publish and local run slice.

`cooldis agent` is the declaration and publication lane for agent manifests. It
is distinct from:

- `cooldis tool`, which builds and publishes capability artifacts;
- `cooldis thread`, which will address live execution instances;
- `cooldis rpc`, which exposes the control plane to clients and sandboxes.

## Commands

```sh
cooldis init release-verifier
cooldis agent plan release-verifier/cooldis.agent.toml \
  --operations-registry-root .cooldis/operations
cooldis agent publish release-verifier/cooldis.agent.toml \
  --operations-registry-root .cooldis/operations
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

The generated manifest is valid immediately for planning, while the prompt and
component files make the intended project graph explicit. `components/couplings.toml`
contains the frozen V1 coupling template ids: async queue, completion callback,
context spill/truncate/summarize, memory preview, schedule/retry/deadletter,
permission/control, prompt steering, and channel ingress/egress. Custom
operation packages live under `operations/` and should be published first; then
replace the placeholder `op://...@sha256:000...` refs in `cooldis.agent.toml`
before publishing the agent. `cooldis agent init --out path.toml` keeps the old
single-manifest file form for compatibility.

`plan` is the dry-run boundary. It parses the source manifest, validates the
identity envelope, resolves the canonical JSON shape, computes source and
manifest hashes, and prints the publish summary. It writes nothing. If an
operations registry is locatable, `plan` verifies each `op://` tool row against
that registry and annotates the `resolved_ref:` line with `[verified]`. Without
an operations registry, it still succeeds and reports those refs as
`[unverified-offline]`.

`publish` reruns the same resolution path and writes a durable local record under
`.cooldis/agents`. The active record lives in `records/<name>.json`; immutable
version records live in `versions/<name>/<version>.json`. Republish of the same
name and version is allowed only when the manifest hash is identical.

`publish` is a registry oracle for `op://` refs. Every operation tool row must
name a local operation record, pin a published version hash, select either the
whole record or a declared operation, and declare grants that cover the selected
operation requirements. Use `--operations-registry-root <dir>` when the
operations registry is not the conventional `.cooldis/operations` root. Missing
or fabricated operation refs reject before the agent record is written.

`list` and `show` inspect published records from the local registry. They are the
minimum discovery surface required once publication exists.

`run` starts a manifest-backed app-server thread from a published `agent://...`
ref, sends one input turn, prints the assistant output, then prints the manifest
compile and bind receipt event ids. Use `--registry-root <dir>` to point at a
non-default local agent registry.

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
