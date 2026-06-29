# Crates

Rust packages live here. The repository root is a workspace wrapper; individual
crate manifests own package-level dependencies and tests.

Current layout:

- `cooldis-kernel/`: the main package, still named `cooldis`, with the runtime
  kernel, CLI binaries, smoke binaries, and integration tests.
- `cooldis-abi/`: operation ABI contracts, values, grants, ports, and effect
  claims shared by host and guest surfaces.
- `cooldis-agent/`: agent manifest and thread contract types that do not need
  the full kernel.
- `cooldis-guest-sdk/`: guest-side SDK for Wasm ABI operations.
- `cooldis-history/` and `cooldis-history-sqlite/`: history/event contracts and
  SQLite persistence.
- `cooldis-io-core/`: protocol-neutral IO envelope, resolver, admission, and
  egress contracts for future daemon/protocol adapters.
- `cooldis-io-pgqrs/`: pgqrs-backed durable ingress queue spike with local
  SQLite and future Postgres support.
- `cooldis-io-telegram/`: Telegram Bot API update normalization and egress
  delivery built on `cooldis-io-core`.
- `cooldis-metadata/`: provider catalog/auth metadata and secret storage.
- `cooldis-operations/`: operation projections, tool-package contracts,
  in-memory operation registry, durable operation records, blob store, and
  scoped operation binding records.
- `cooldis-process/`: shared process/event/result handle for bash, Wasm, and
  bridge streams.
- `cooldis-provider/`: provider wire adapters and replay normalization.
- `cooldis-runtime-contracts/`: runtime identity, event, lifecycle, and thread
  contract types shared across packages.
- `cooldis-vbash/`: virtual-bash contracts, Bashkit harness, apply_patch, and
  operation shell builtins.
- `cooldis-vfs/`: scoped VFS, host mounts, object-store mounts, and writeback.
- `cooldis-wasm/`: Wasm artifact/config, manifest validation, operation
  invocation, HTTP/VFS imports, and Wasmtime execution.
