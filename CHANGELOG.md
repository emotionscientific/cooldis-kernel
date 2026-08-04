# Changelog

## v0.3.0 (2026-08-04)

- Renamed the Cooldis kernel, crates, and primary binary to Verlet. The
  `cooldis` binary name, `COOLDIS_*` environment variables, old config
  filenames, and old state-directory names remain compatible in v0.3.0 with
  deprecation warnings; removal is no earlier than v0.4.0. Release archives,
  installer, and the Homebrew formula now use the `verlet` name.
- Fork ingress claims now settle correctly when a store append fails
  ambiguously: reconciliation reads the authoritative event stream, adopts
  the entry if the write landed, and retries once only when no durable
  record exists. Previously a single failed append could leave a fork claim
  durably orphaned so the child thread never started.
- RPC client errors from the standalone credential path render without
  internal component naming, and the credential setup is documented.
- Bumped bashkit from 0.9.0 to 0.14.3.
