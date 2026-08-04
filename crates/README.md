# Crates

Rust packages live here. The repository root is a workspace wrapper; individual
crate manifests own package-level dependencies and tests.

Current layout:

- `verlet-kernel/`: the main package, still named `verlet`, with the runtime
  kernel, CLI binaries, smoke binaries, and integration tests.
- `verlet-abi/`: operation ABI contracts, values, grants, ports, and effect
  claims shared by host and guest surfaces.
- `verlet-agent/`: agent manifest and thread contract types that do not need
  the full kernel.
- `verlet-guest-sdk/`: guest-side SDK for Wasm ABI operations.
- `verlet-history/` and `verlet-history-sqlite/`: history/event contracts and
  SQLite persistence.
- `verlet-io-core/`: protocol-neutral IO envelope, resolver, admission, and
  egress contracts for future daemon/protocol adapters.
- `verlet-io-pgqrs/`: pgqrs-backed durable ingress queue spike with local
  SQLite and future Postgres support.
- `verlet-io-telegram/`: Telegram Bot API update normalization and egress
  delivery built on `verlet-io-core`.
- `verlet-metadata/`: provider catalog/auth metadata and secret storage.
- `verlet-operations/`: operation projections, tool-package contracts,
  in-memory operation registry, durable operation records, blob store, and
  scoped operation binding records.
- `verlet-process/`: shared process/event/result handle for bash, Wasm, and
  bridge streams.
- `verlet-provider/`: provider wire adapters and replay normalization.
- `verlet-runtime-contracts/`: runtime identity, event, lifecycle, and thread
  contract types shared across packages.
- `verlet-vbash/`: virtual-bash contracts, Bashkit harness, apply_patch, and
  operation shell builtins.
- `verlet-vfs/`: scoped VFS, host mounts, object-store mounts, and writeback.
- `verlet-wasm/`: Wasm artifact/config, manifest validation, operation
  invocation, HTTP/VFS imports, and Wasmtime execution.
