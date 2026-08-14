# Verlet Agent CLI

Status: V1 publish and local run slice.

`verlet agent` is the declaration and publication lane for agent manifests. It
is distinct from:

- `verlet tool`, which builds and publishes capability artifacts;
- `verlet rpc`, which exposes the control plane to clients and sandboxes.

## Commands

```sh
verlet init release-verifier
verlet agent plan release-verifier/verlet.agent.toml \
  --operations-registry-root .verlet/operations
verlet agent publish release-verifier/verlet.agent.toml \
  --resolve-ops --operations-registry-root .verlet/operations
verlet blob publish release-verifier/prompts/system.md --name identity
verlet skill publish release-verifier/skills
verlet skill import ~/.agents/skills/release-checker --dry-run
verlet agent list
verlet agent versions release-verifier
verlet agent diff release-verifier --from 0.1.0:authored --to 0.1.0:resolved
verlet agent show agent://release-verifier@0.1.0
verlet agent run agent://release-verifier@latest --input "check the branch"
```

`verlet init <name>` is the V1 folder-first entrypoint. It creates:

```text
<name>/
  verlet.agent.toml
  prompts/system.md
  components/operations.toml
  components/couplings.toml
  operations/README.md
```

The generated manifest is valid immediately for planning. `prompts/system.md`
is the folder-first system prompt: `agent plan` and `agent publish` lower it to
an immutable blob resource and wire it into the `identity` static context source
when that source has no explicit input. This works for the synthesized default
pipeline and for an explicit `[context]` pipeline that leaves the `identity`
static source input unset so other sources can declare their own
`budget_share`. If `prompts/system.md` exists and the explicit `identity`
source also declares an `input`, planning and publishing reject it as ambiguous:
drop the input to use folder-first lowering, or move the file out of
`prompts/system.md` and point at a declared resource explicitly.
`components/couplings.toml` contains the frozen V1 coupling template ids: async
queue, completion callback, context spill/truncate/summarize, memory preview,
schedule/retry/deadletter, permission/control, prompt steering, and channel
ingress/egress. Custom operation packages live under `operations/` and should
be published first; then add pinned `op://...@sha256:<hash>` refs to
`verlet.agent.toml`, or use `verlet agent publish --resolve-ops` to rewrite
`op://name` and `op://name@latest` authoring refs to the active pinned hash
before publishing the agent. `verlet agent init --out <dir>` selects the
folder-first project directory. Single-manifest file output is not supported.

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
`.verlet/agents`. The active record lives in `records/<name>.json`; immutable
version records live in `versions/<name>/<version>.json`. Republish of the same
name and version is allowed only when the manifest hash is identical. When a
folder-first prompt is lowered, the publish receipt pins the blob hash through
the resolved blob resource and the `identity` context source. Each new record
retains the authored TOML verbatim beside the canonical resolved manifest; the
source and manifest hashes identify those two forms. Legacy records remain
readable but do not gain an authored form retroactively.

`publish` is a registry oracle for `op://` refs. Every operation tool row must
name a local operation record, pin a published version hash, select either the
whole record or a declared operation. Use `--operations-registry-root <dir>` when the
operations registry is not the conventional `.verlet/operations` root. Missing
or fabricated operation refs reject before the agent record is written. Passing
`--resolve-ops` is an authoring convenience only: unpinned `op://name` and
`op://name@latest` rows are resolved against the local operations registry's
active published record, the manifest file is rewritten to
`op://name@sha256:<hash>` before normal publish verification runs, and each
rewrite is printed. Published agent records and stored manifests always carry
pinned operation refs; runtime never looks up `@latest` for operation tools.

`verlet blob publish <file> [--registry-root .verlet/blobs] [--name <name>]`
publishes an arbitrary file as an immutable blob artifact and prints the
`resource://artifact/sha256:<hash>` ref. Re-publishing the same content returns
the same digest and is a no-op. Agent manifests consume blob artifacts through
`[[resources]] kind = "blob"` rows and static context sources whose `input`
names the resource.

`verlet skill publish <dir> [--registry-root .verlet/skills] [--name <package>]`
publishes the directory's `<skill>/SKILL.md` entries into the local skill
registry and prints both `skill://<package>@sha256:<hash>` and
`skill://<package>`. Identical content reuses the same immutable version;
changed content advances the active name while preserving prior versions.
Manifests may use either ref. Authors and manifests speak names; receipts speak
hashes: a floating name resolves once at bind and the bind receipt witnesses
the pinned ref. Pinned refs never follow the active name.

### Importing external skill directories

`verlet skill import <dir> [--registry-root .verlet/skills]
[--blob-registry-root .verlet/blobs] [--name <package>] [--dry-run]` is the
publisher-side converter for one conventional skill directory whose
`SKILL.md` is at its root. It produces only existing registry records:

- root `SKILL.md` frontmatter and body become one skill entry;
- direct `references/*.md` files are appended to that entry under deterministic
  `Imported references` sections, keeping the registry's one-entry shape;
- recursive `assets/**` files publish through the blob registry and appear in
  the output as `resource://artifact/sha256:<hash>` refs;
- recursive `scripts/**` files are not converted or published. Their sorted
  paths are written into an `Import degradation` body section and into the
  imported entry description, so the existing bind-time model-visible skill
  index also states what is unavailable;
- hook and MCP configuration-shaped files are ignored and reported. Import
  never turns their content into standing authority;
- other files, including package-root files and nested/non-markdown reference
  files, are not converted and are reported as skipped.

The command prints pinned and floating skill refs, every blob ref, and a
ready-to-paste manifest fragment containing `[[resources]]` rows for the pinned
skill package and blobs. Re-importing identical content reuses the same skill
and blob hashes without creating new immutable versions. `--dry-run` computes
and prints that same plan without creating either registry root. Symlinks in the
input tree are rejected instead of followed.

Publication is the portable registry lane. Local agents may instead opt into
bind-time workspace discovery with `[skills] discover = true` and an optional
workspace-relative `path` (default `.agents/skills`); this does not publish or
mount the files. See [Agent Manifest Ontology — Skills](agent-manifest-ontology.md#skills)
for the two-lane model, no-mount rule, and durable witness semantics.

`list` and `show` inspect published records from the local registry. `versions
<name>` lists immutable versions by `published_at_ms`, not by the author-declared
version string. Its text output includes an RFC3339 publication time, declared
version, and manifest hash; `--json` also exposes the source hash.

`diff <name> --from <version>[:authored|:resolved] --to
<version>[:authored|:resolved]` compares two immutable snapshots structurally.
The form defaults to `resolved`; `authored` parses the retained TOML through the
manifest schema before comparison. Comparing authored and resolved forms of one
version shows fields filled during resolution, including folder-first prompt
lowering. Changes are JSON-pointer path ordered and reported as added, removed,
or changed. `--json` emits the raw change list. Authored comparison fails
when the retained source cannot be decoded as a current manifest.

`run` starts a manifest-backed app-server thread from a published `agent://...`
ref, sends one input turn, prints the assistant output, then prints the manifest
compile and bind receipt event ids. Use `--registry-root <dir>` to point at a
non-default local agent registry.

The blob registry root derives from the agent registry root everywhere agents
are resolved: `<registry-root>/blobs`, or the sibling `blobs` directory when
the agent root's basename is `agents` (so the conventional `.verlet/agents`
pairs with `.verlet/blobs`). `agent plan`, `agent publish`, and `agent run`
all share this derivation; an explicit app-server `blob_registry_root` override
still wins.

## Current Manifest Shape

The V1 parser requires identity and at least one model profile:

```toml
[agent]
name = "release-verifier"
version = "0.1.0"
description = "Checks a release branch."
kind = "verlet.agent-manifest"
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
policies, attachment config, context defaults, and runtime defaults without changing the
publication boundary.

## Removal Semantics

Plain `delete` is not part of this slice. A published agent record is a durable
artifact. The later removal surface should distinguish:

- `unpublish`: withdraw an active alias or discovery pointer while keeping
  immutable records available for audit and resume;
- `gc`: remove unreferenced local blobs or records when a local registry is being
  cleaned intentionally.
