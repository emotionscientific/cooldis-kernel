# Repository Map

Verlet is a standalone Rust runtime workspace. The repository root is for
workspace-wide contracts, docs, scripts, proto sketches, and shared artifacts.
Rust packages live under `crates/`.

```text
.
├── Cargo.toml                  # workspace manifest
├── Cargo.lock                  # workspace lockfile
├── crates/
│   ├── verlet-kernel/         # package name: verlet
│   ├── verlet-guest-sdk/      # Wasm guest SDK
│   ├── verlet-io-core/        # protocol-neutral IO contracts
│   ├── verlet-io-pgqrs/       # durable ingress queue spike
│   └── verlet-io-telegram/    # Telegram protocol adapter crate
├── docs/                       # hostable public docs
├── proto/                      # bridge wire-contract sketches
├── scripts/                    # repo-native verification and hooks
└── scratch/                    # ignored local investigations
```

## Kernel Package

The main crate remains named `verlet`, but its package source lives in
`crates/verlet-kernel/`.

```text
crates/verlet-kernel/
├── Cargo.toml
├── src/
│   ├── lib.rs                  # public module map and flat exports
│   ├── bin/                    # thin user-facing binary entrypoints
│   ├── cli/                    # Verlet CLI implementation and unit tests
│   ├── kernel/                 # lifecycle, history, metadata stores, supervisor, context, compaction
│   ├── agent/                  # agent tools, hooks, permissions, tool routing
│   ├── adapters/               # provider, TUI, app-server, MCP, and ACP surfaces
│   ├── capabilities/           # ABI, bridge, process handles, VFS, bash, Wasm
│   ├── operations/             # build helper, kernel packages, plugin catalog, shims
│   └── daemon/                 # daemon config and IO workers
└── tests/                      # kernel integration tests, smoke entrypoints, and fixtures
```

External callers can use either the domain namespaces, such as
`verlet::kernel::runtime_host::RuntimeHost`, or the existing flat re-exports,
such as `verlet::RuntimeHost`.

## Boundary Rule

Verlet owns runtime primitives: tenants, thread lifecycle, events, history,
runtime adapters, ABI contracts, virtual bash, VFS, provider adapters,
tool publishing, daemon IO, and the Verlet app-server.

Product logic belongs elsewhere: auth, billing, invites, dashboards, Railway
deployment, product ledgers, and user-facing policy are outside this runtime
workspace.

Future TypeScript clients, product adapters, long-running workers, or product
apps should get real package/service directories only when they exist. Until
then, keep their shape in docs and tests rather than empty top-level roots.
