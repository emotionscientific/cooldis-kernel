# Repository Map

Cooldis is a standalone Rust runtime workspace. The repository root is for
workspace-wide contracts, docs, scripts, proto sketches, and shared artifacts.
Rust packages live under `crates/`.

```text
.
├── Cargo.toml                  # workspace manifest
├── Cargo.lock                  # workspace lockfile
├── crates/
│   ├── cooldis-kernel/         # package name: cooldis
│   ├── cooldis-guest-sdk/      # Wasm guest SDK
│   ├── cooldis-io-core/        # protocol-neutral IO contracts
│   ├── cooldis-io-pgqrs/       # durable ingress queue spike
│   └── cooldis-io-telegram/    # Telegram protocol adapter crate
├── docs/                       # hostable public docs
├── proto/                      # bridge wire-contract sketches
├── scripts/                    # repo-native verification and hooks
└── scratch/                    # ignored local investigations
```

## Kernel Package

The main crate remains named `cooldis`, but its package source lives in
`crates/cooldis-kernel/`.

```text
crates/cooldis-kernel/
├── Cargo.toml
├── src/
│   ├── lib.rs                  # public module map and flat exports
│   ├── bin/                    # thin user-facing binary entrypoints
│   ├── cli/                    # Cooldis CLI implementation and unit tests
│   ├── kernel/                 # lifecycle, history, metadata stores, supervisor, context, compaction
│   ├── agent/                  # agent tools, hooks, permissions, tool routing
│   ├── adapters/               # Codex, provider, TUI, app-server, MCP surfaces
│   ├── capabilities/           # ABI, bridge, process handles, VFS, bash, Wasm
│   ├── operations/             # build helper, kernel packages, plugin catalog, shims
│   └── daemon/                 # daemon config and IO workers
└── tests/                      # kernel integration tests, smoke entrypoints, and fixtures
```

External callers can use either the domain namespaces, such as
`cooldis::kernel::runtime_host::RuntimeHost`, or the existing flat re-exports,
such as `cooldis::RuntimeHost`.

## Boundary Rule

Cooldis owns runtime primitives: tenants, thread lifecycle, events, history,
runtime adapters, ABI contracts, virtual bash, VFS, provider adapters,
tool publishing, daemon IO, and the Cooldis app-server.

Product logic belongs elsewhere: auth, billing, invites, dashboards, Railway
deployment, product ledgers, and user-facing policy are outside this runtime
workspace.

Future TypeScript clients, product adapters, long-running workers, or product
apps should get real package/service directories only when they exist. Until
then, keep their shape in docs and tests rather than empty top-level roots.
