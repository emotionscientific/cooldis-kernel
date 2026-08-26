# Verlet

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/emotionscientific/verlet-kernel)
[![Verify](https://github.com/emotionscientific/verlet-kernel/actions/workflows/verify.yml/badge.svg)](https://github.com/emotionscientific/verlet-kernel/actions/workflows/verify.yml)
[![Release](https://github.com/emotionscientific/verlet-kernel/actions/workflows/release.yml/badge.svg)](https://github.com/emotionscientific/verlet-kernel/actions/workflows/release.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Status: experimental](https://img.shields.io/badge/status-experimental-yellow.svg)](README.md)

> Status: Verlet Kernel is experimental. APIs, behavior, and release
> packaging may change before a stable public release. It is the open-source
> engine under Verlet Cloud, the managed agents service.

## What Is Verlet

Verlet Kernel is a declarative harness that grew into a complete runtime for
agent workloads, written in Rust.

You declare an agent before anything runs. Its manifest is a preset: it carries
model profiles, policies, runtime defaults, workspace requirements, and the
context pipeline, and it proposes tools and resources for binding. When a
thread starts, the kernel expands that preset and records the opening
`binding.attached` events. Those recorded attachments, rather than a standing
manifest document, establish tool authority.

It serves heterogeneous workloads on one machine. Agents, workflows, and
sub-agents run on the same execution machinery and differ only in continuation
policy: a workflow follows a fixed script, an agent chooses its next step.
Bring a harness with you or declare one here; either way the kernel owns
dispatch, authority, and the record.

The engine is modular and recomposes without changing meaning. Embed it in a
process, run it as a daemon, drive workflows with it: however it is assembled,
it is the same machine, with the same behavior and the same event stream. And
because an agent is a declaration plus content-addressed tools and explicit
attachment config, an agent is packageable: exportable, diffable, and reproducible on
another machine.

Everything is event-sourced. Every action is an event in an append-only
stream, and state is a fold over the stream. Durability, replay, resume,
audit, and observability are properties of the storage model, not features
added on top.

A thread's current toolset is the fold of its `binding.attached` and
`binding.detached` history. Resume re-folds that same stream and reconstructs
runtime mounting configuration from persisted metadata and durable recorded
facts. It does not re-bind the manifest or require a current registry record or
matching manifest hash.

Verlet is not an agent, a graph framework, a provider SDK, or a product app.
Product systems configure and call Verlet. The kernel owns the runtime
contracts those systems depend on.

## What We Mean By Kernel, Runtime, Harness

"Kernel" is an overloaded word, so here is how this repository uses it: the
privileged core that owns mechanism and refuses to own policy. The test for
what belongs in it is simple: if changing a thing could make the system's
audit receipts lie, it is compiled into the kernel (event ordering,
provenance, fail-closed attachment, budgets, receipts), and everything else lives
above it, named, versioned, and swappable. The kernel does not think, does not
prompt, and does not orchestrate. (So: not "kernel" as in a Jupyter kernel,
which is an evaluator, and not "kernel" as in an orchestration SDK.) The
**runtime** is that kernel in motion: the running system that
executes turns and witnesses facts; when we say "the runtime did it," we mean
an event whose authority is the system's own attestation, not some function's
output. And a **harness** (the industry's word for everything wrapped around
a model) is here not something you hand-write but something the system
resolves: an agent's effective execution envelope, derived from its
declarations and attachments, exportable and diffable like a lockfile.

## Repository Scope

This repository contains the standalone Verlet kernel workspace:

- Runtime primitives: tenant hosts, thread lifecycle, history, events,
  cancellation, resume, and supervisor routing.
- Agent and operation contracts: manifests, tool publication, operation ABI,
  coupling ABI, attachment config, command projections, and Wasm operation support.
- Runtime surfaces: CLI, daemon, app-server RPC, MCP, ACP, provider adapters,
  virtual bash, VFS, process handles, and daemon-embedded store-primary stream
  sync with scoped credentials and single-writer leases.
- Release and verification tooling for tagged binary releases.

The product layer is outside this repository. Auth products, billing,
dashboards, invite flows, deployment orchestration, and app-specific ledgers
belong in product repos or adapters.

For the detailed repository map, see [docs/repository-map.md](docs/repository-map.md).

## The Console

The web console lives in this repository at [apps/console](apps/console) and
ships inside the kernel release as `verlet console`. See the
[CLI server and client command model](docs/cli.md#server-and-client-commands)
for foreground and endpoint-discovered use.

## Current Status

The V1 work is focused on runtime primitives:

- an embeddable multi-tenant host facade and `verlet host run` entry point with
  one authenticated listener, credential-digest-to-instance selection,
  instance-owned authentication, default mandate clocks, and independently
  drainable lifecycles;
- agent manifest planning, publishing, listing, showing, and local running;
- operation publication and ABI-backed invocation;
- local tool-kit installation that lowers member packages into ordinary
  content-addressed operation publishes plus one removable installed-kit record;
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
- OpenAI Codex backend access through browser or device OAuth against a user's
  ChatGPT plan, with credentials isolated in the user provider store;
- daemon, RPC, MCP, ACP, and CLI projections over the same kernel contracts;
- daemon-hosted remote child placement: store-backed ingress, separate local
  child processes, and local-first stream propagation fenced by durable scoped
  credentials and leases;
- release packaging for `verlet`, `verlet-acp-agent`, and
  `verlet-mcp-server`.

A managed service, Verlet Cloud, runs on top of this kernel and is concierge
today. Self-serve cloud placement, a public package registry, marketplace
flows, and stateful product harnesses are future direction. See
[docs/roadmap.md](docs/roadmap.md).

## How This Is Built

Coding agents are used extensively in the development of Verlet. Several
different models draft, implement, review, and cross-examine each other's
claims, and a human architect makes the design decisions and reviews what
lands. This is what lets a very small team build a project of this size at
this speed, and it is why fixes land quickly after they are found.

The design intention is held fixed in a separate, public formalism:
[verlet-formalism](https://github.com/emotionscientific/verlet-formalism)
carries the laws and the lexicon the code is written against. Every
primitive is named there before it is implemented, and the codebase is
scanned against it on a regular cadence. When a scan finds code and canon
disagreeing, either the code is fixed or the canon is struck, with a commit
that says which.

Treat the repository accordingly. It is experimental, it moves fast, and
the record of why it moved is in the commit history and the formalism.

## Documentation

- [Docs index](docs/README.md): documentation map.
- [Agents in Version Control](https://emotionscientific.github.io/verlet-kernel/primer/agents-in-version-control.html):
  plain-language primer on the runtime, with a prompt per section to ask a
  coding agent in this checkout ([PDF](docs/primer/agents-in-version-control.pdf)).
- [Overview](docs/index.md): product category and current status.
- [Getting started](docs/getting-started.md): local run and inspection path.
- [CLI](docs/cli.md): canonical commands, help model, and receipt-backed bind
  inspection.
- [Runtime primitives](docs/developers/runtime-primitives.md): kernel-owned
  surfaces and boundaries.
- [ABI](docs/abi.md): operation boundary and host/guest contract.
- [Wasm operation dev kit](docs/wasm-operation-dev-kit.md): Rust guest
  macros, fixtures, tool kits, custom coupling authoring, and replay.
- [OpenAPI adapter](docs/openapi-adapter.md): import a witnessed REST contract
  into ordinary published operations without SDK generation.
- [Agent CLI](docs/agent-cli.md): manifest authoring and local agent commands.
- [Agent manifest ontology](docs/agent-manifest-ontology.md): manifest shape
  and deferred fields.
- [Command contracts](docs/command-contracts.md): command/manual projection
  rules.
- [Chat console](docs/chat.md): local terminal console over the app-server RPC
  boundary. First run opens an in-TUI provider setup window; see
  [Provider setup](docs/provider-setup.md).
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

### Local Linux verification

Run the same full verification suite in a pinned Debian Linux container before
shipping a change that may behave differently across operating systems:

```sh
scripts/verify-linux.sh
```

The default is native `linux/arm64` on Apple Silicon. It covers the Linux OS
class of path, filesystem, glibc, epoll, signal, and timing bugs; CI's x86_64
leg remains the architecture authority. Use `scripts/verify-linux.sh --amd64`
only when reproducing an architecture-specific failure because emulation is
substantially slower.

The first run downloads the Rust 1.97.1 Bookworm image (the stable toolchain
used by CI when this lane was added), installs its toolchain, and builds the
full workspace. Later runs reuse Cargo, Rustup, and architecture-specific
Cargo-lane state from the `verlet-verify-linux` Docker volume, but still
execute every verification step. Concurrent wrapper runs serialize access to
that shared volume. Docker Desktop should have at least 12 GB of memory under
Settings > Resources; the wrapper warns and reduces Cargo to two build jobs
below that limit. If cache corruption is suspected, reset it with
`docker volume rm verlet-verify-linux`; the next run will be cold.

For the first local path, see [docs/getting-started.md](docs/getting-started.md).
For release packaging, tag checks, and async release publishing, see
[RELEASE.md](RELEASE.md).

## Contributing

Verlet is not accepting public contributions yet. Please read
[CONTRIBUTING.md](CONTRIBUTING.md) before opening issues, and do not open
unsolicited pull requests. Report suspected vulnerabilities through the private
process in [SECURITY.md](SECURITY.md).

## License

Verlet is licensed under the [Apache License, Version 2.0](LICENSE).
