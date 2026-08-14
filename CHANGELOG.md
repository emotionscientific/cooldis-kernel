# Changelog

## Unreleased

### Compatibility paths removed

- Persisted `cooldis.agent-manifest` records and manifests that still carry
  grant-string authority payloads are rejected. Republish those records with
  the current Verlet version. Persisted agent records missing current required
  fields and default-agent records in the pre-rename namespace are rejected
  with the same guidance.
- Thread streams with manifest bind receipts but no `binding.attached` or
  `binding.detached` events no longer derive a toolset from receipts or cached
  metadata. Start a new thread. Streams with neither receipts nor binding
  events still resume with an empty toolset.
- The app-server no longer reconstructs a manifest tool-use instruction from
  old operation metadata.
- Bare `tool://` agent tool refs are rejected. Use `op://` for published
  operations or `mcp://` for configured MCP sources.
- `verlet init` and `verlet agent init` now write folder-first projects only.
  `--out` must name a project directory.
- The pre-rename console WebSocket token subprotocol is no longer accepted.
- Frozen durable-record identifiers in `docs/format-ids.md` are unchanged.

## v0.4.0 (2026-08-13)

### The binding model

Tool authority is now event-sourced. This release completes the redesign
started in v0.3.x: the grant-string layer is gone, and a thread's toolset is
determined by its own recorded history.

- Attachment is the gate. A tool is available to a thread because an
  attach event is recorded in that thread's stream; detaching it is another
  recorded event. There are no grant strings and no per-call
  set-membership checks.
- Bindings are journaled. `binding.attached` and `binding.detached` events
  (`cooldis.events/0.4`) carry the binder's provenance and the resolved
  principal, and they are appended in the same fenced batch as the bind
  receipt. The record can now answer "when did this agent get this tool."
- Toolsets derive from the stream. The catalog and the operation router are
  views over the thread's binding history. Every `tool.call.requested`
  receipt cites the attach event that admitted the operation.
- Resume replays the record. Resuming a thread reconstructs its toolset and
  runtime configuration from the thread's own stream and durable receipts.
  It loads no registry, compares no manifest hashes, and appends no events.
  Threads that were previously stranded at resume by a republished or
  deleted agent record now resume and keep working, and their tool calls
  still cite the original attach event.
- Secret injection and private-network enforcement moved to attachment
  configuration, where the real gate is.

The agent manifest remains as a preset: a declared opening sequence of
attach events for thread start, carrying its non-authority payload
(model profile, runtime settings) as before. `manifest.bind.completed`
receipts and all frozen `cooldis.*` format identifiers
(`docs/format-ids.md`) are unchanged.

### Rename compat removal

The v0.3.x cooldis-to-verlet deprecation window is over. Removed:

- the `cooldis` shim binary; archives and installers ship `verlet`,
  `verlet-acp-agent`, and `verlet-mcp-server`
- `COOLDIS_*` environment fallbacks; only canonical `VERLET_*` names are
  read
- `cooldis.agent-manifest` kind acceptance; the accepted kind is
  `verlet.agent-manifest`
- unpinned `op://cooldis-*` package aliases and `cooldis_*` MCP tool
  aliases
- legacy `cooldis.toml` / `cooldis.json` / `.cooldis` config and state
  discovery

Preserved forever: the frozen durable-record format identifiers
(`cooldis.events/*`, `cooldis.event.*`, see `docs/format-ids.md`), pinned
old-name `op://...@sha256` refs against preserved records, and every
historical receipt. Old streams remain readable; resume works through the
compatibility lanes for records written before the binding events existed.

### Also in this release

- Hooks that exit without reading stdin complete normally; the exit status
  and stdout decide the outcome.
- Concurrent resumes of the same thread share one runtime build.
- The name lint tightened: allowlist entries that existed only for the
  removed compat lanes are gone.

## v0.3.2 (2026-08-08)

### Changes

- Rebuilt the `verlet chat` terminal console on the tuika UI framework:
  streaming markdown answers, distinct thinking rows, live tool and command
  output cells with wrap-then-elide previews, a slash-command popup with tab
  completion, a multiline composer with paste and history recall, and
  scrollback that follows the tail only while at the bottom. The console
  remains a pure RPC client of the app-server; the UI lives in the new
  `verlet-chat` crate.
- Kernel operation dispatchers are now immutable at registration and
  resolved per thread through dispatch overlays, removing the mutable
  shared-registry slot.
- Journal appends are fenced by the placement-lease epoch, so a superseded
  host can no longer write after its lease moves.
- Added a Nix flake for the dev shell and binary packaging.
- Documented the frozen `cooldis.*` format identifiers in
  `docs/format-ids.md`; the repository name lint now closes Rust source over
  an explicit frozen-identifier allowlist.

### Deprecations

- Renamed the kernel packages `cooldis-threads`, `cooldis-schedule`,
  `cooldis-process`, and `cooldis-notify` to their `verlet-*` forms. Unpinned
  old `op://` refs and bare package names resolve to the new records with a
  deprecation warning through v0.3.x. Pinned old refs continue to address an
  already-persisted old-name record and hash; they are not redirected, and a
  fresh host without that record returns the normal not-found error. The old
  aliases are scheduled for removal in v0.4.0.
- Agent manifests now emit and prefer `verlet.agent-manifest`. The former
  `cooldis.agent-manifest` kind remains accepted through v0.3.x and is
  scheduled for removal in v0.4.0. Already-persisted agent records are not
  rewritten.

## v0.3.1 (2026-08-05)

### Changes

- Added the v0 orchestrator boundary on the app-server (ADR 0009): a
  record-client surface for external orchestrators, with the client-stream
  boundary documented.
- The daemon egress drain now maintains incremental per-thread views instead
  of replaying the full journal on every drain.
- The frozen V1 stream schema registry is cached instead of being rebuilt on
  each validation.

## v0.3.0 (2026-08-04)

### Deprecations

- Renamed the Cooldis kernel, crates, and primary binary to Verlet. The
  `cooldis` binary name, `COOLDIS_*` environment variables, old config
  filenames, and old state-directory names remain compatible in v0.3.0 with
  deprecation warnings; removal is no earlier than v0.4.0. Release archives,
  installer, and the Homebrew formula now use the `verlet` name.

### Changes

- Fork ingress claims now settle correctly when a store append fails
  ambiguously: reconciliation reads the authoritative event stream, adopts
  the entry if the write landed, and retries once only when no durable
  record exists. Previously a single failed append could leave a fork claim
  durably orphaned so the child thread never started.
- RPC client errors from the standalone credential path render without
  internal component naming, and the credential setup is documented.
- Bumped bashkit from 0.9.0 to 0.14.3.
