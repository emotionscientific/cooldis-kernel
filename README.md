# Cooldis

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/emotionscientific/cooldis-kernel)
[![Verify](https://github.com/emotionscientific/cooldis-kernel/actions/workflows/verify.yml/badge.svg)](https://github.com/emotionscientific/cooldis-kernel/actions/workflows/verify.yml)
[![Release](https://github.com/emotionscientific/cooldis-kernel/actions/workflows/release.yml/badge.svg)](https://github.com/emotionscientific/cooldis-kernel/actions/workflows/release.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Status: experimental](https://img.shields.io/badge/status-experimental-yellow.svg)](README.md)

> Status: Cooldis is experimental. APIs, behavior, and release packaging may
> change before a stable public release.

## What Is Cooldis

Cooldis is an open serverless agent platform.

At the kernel level, Cooldis is a Rust runtime for declared agent workloads. An
agent declaration describes the model profile, tools, resources, secrets,
permissions, context policy, and runtime defaults before execution. Cooldis
turns that declaration into governed runtime work: capability grants, tool
visibility, provider access, operation dispatch, event records, cancellation,
resume, and audit receipts.

Cooldis is not an agent, a graph framework, a provider SDK, or a product app.
Product systems configure and call Cooldis. The kernel owns the runtime
contracts those systems depend on.

## What We Mean By Kernel, Runtime, Harness

"Kernel" is an overloaded word, so here is how this repository uses it: the
privileged core that owns mechanism and refuses to own policy. The test for
what belongs in it is simple: if changing a thing could make the system's
audit receipts lie, it is compiled into the kernel (event ordering,
provenance, fail-closed grants, budgets, receipts), and everything else lives
above it, named, versioned, and swappable. The kernel does not think, does not
prompt, and does not orchestrate. (So: not "kernel" as in a Jupyter kernel,
which is an evaluator, and not "kernel" as in an orchestration SDK.) The
**runtime** is that kernel in motion: the running system that
executes turns and witnesses facts; when we say "the runtime did it," we mean
an event whose authority is the system's own attestation, not some function's
output. And a **harness** — the industry's word for everything wrapped around
a model — is here not something you hand-write but something the system
resolves: an agent's effective execution envelope, derived from its
declarations and grants, exportable and diffable like a lockfile.

## Repository Scope

This repository contains the standalone Cooldis kernel workspace:

- Runtime primitives: tenant hosts, thread lifecycle, history, events,
  cancellation, resume, and supervisor routing.
- Agent and operation contracts: manifests, tool publication, operation ABI,
  coupling ABI, grants, command projections, and Wasm operation support.
- Runtime surfaces: CLI, daemon, app-server RPC, MCP, ACP, provider adapters,
  virtual bash, VFS, process handles, and daemon-embedded store-primary stream
  sync with scoped credentials and single-writer leases.
- Release and verification tooling for tagged binary releases.

The product layer is outside this repository. Auth products, billing,
dashboards, invite flows, deployment orchestration, and app-specific ledgers
belong in product repos or adapters.

For the detailed repository map, see [docs/repository-map.md](docs/repository-map.md).

## Related Repositories

- [cooldis-console](https://github.com/emotionscientific/cooldis-console):
  desktop/web console and product-side client for the Cooldis app-server
  protocol.

When checked out as sibling repositories, the console usually lives at
`../cooldis-console`.

## Current Status

The V1 work is focused on runtime primitives:

- agent manifest planning, publishing, listing, showing, and local running;
- operation publication and ABI-backed invocation;
- blob resource publication and folder-first prompt lowering;
- skill-package publication with floating author refs pinned in bind receipts,
  static indexes, and read-only VFS bodies;
- publisher-side import of conventional `SKILL.md` directories into existing
  skill/blob records with model-visible script degradation and inert hooks;
- opt-in workspace skill discovery with bind-time hashes, path-bearing static
  indexes, and durable resume/fork witnesses without a second skill mount;
- manifest-declared workspace requirements with operator-bound read-only or
  read-write host directories mounted through the virtual VFS;
- custom Wasm coupling execution, macro scaffolding, and offline replay for
  declared event folds;
- local runtime execution through provider, virtual bash, and Wasm paths,
  including thread-VFS `/spill` receipts for oversized bash and process output;
- daemon, RPC, MCP, ACP, and CLI projections over the same kernel contracts;
- daemon-hosted remote child placement: store-backed ingress, separate local
  child processes, and local-first stream propagation fenced by durable scoped
  credentials and leases;
- release packaging for `cooldis`, `cooldis-acp-agent`, and
  `cooldis-mcp-server`.

Managed cloud placement, a public package registry, marketplace flows, and
stateful product harnesses are future direction. See
[docs/roadmap.md](docs/roadmap.md).

## Documentation

- [Docs index](docs/README.md): documentation map.
- [Overview](docs/index.md): product category and current status.
- [Getting started](docs/getting-started.md): local run and inspection path.
- [CLI](docs/cli.md): canonical commands, help model, and receipt-backed bind
  inspection.
- [Runtime primitives](docs/developers/runtime-primitives.md): kernel-owned
  surfaces and boundaries.
- [ABI](docs/abi.md): operation boundary and host/guest contract.
- [Wasm operation dev kit](docs/wasm-operation-dev-kit.md): Rust guest
  macros, fixtures, custom coupling authoring, and replay.
- [OpenAPI adapter](docs/openapi-adapter.md): import a witnessed REST contract
  into ordinary published operations without SDK generation.
- [Agent CLI](docs/agent-cli.md): manifest authoring and local agent commands.
- [Agent manifest ontology](docs/agent-manifest-ontology.md): manifest shape
  and deferred fields.
- [Command contracts](docs/command-contracts.md): command/manual projection
  rules.
- [Chat console](docs/chat.md): local terminal console over the app-server RPC
  boundary.
- [Provider adapters](docs/provider-adapters.md): agent loop boundary.
- [Daemon and RPC](docs/app-server.md): app-server control plane.
- [Threat model](docs/threat-model.md): stable threat IDs, current status, and
  deterministic mitigation guards.
- [Testing guidelines](docs/testing-guidelines.md): runtime test expectations.
- [Release guide](RELEASE.md): tags, targets, package smoke tests, and
  installer artifacts.

## Running Locally

Use the repository scripts as the source of truth:

```sh
scripts/verify.sh
cargo test --workspace --all-targets --locked
```

For the first local path, see [docs/getting-started.md](docs/getting-started.md).
For release packaging, tag checks, and async release publishing, see
[RELEASE.md](RELEASE.md).

## Contributing

Cooldis is not accepting public contributions yet. Please read
[CONTRIBUTING.md](CONTRIBUTING.md) before opening issues, and do not open
unsolicited pull requests. Report suspected vulnerabilities through the private
process in [SECURITY.md](SECURITY.md).

## License

Cooldis is licensed under the [Apache License, Version 2.0](LICENSE).
