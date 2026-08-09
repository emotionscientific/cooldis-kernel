# Changelog

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
