# Changelog

## Unreleased

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
