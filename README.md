# Verlet

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/emotionscientific/verlet-kernel)
[![Verify](https://github.com/emotionscientific/verlet-kernel/actions/workflows/verify.yml/badge.svg)](https://github.com/emotionscientific/verlet-kernel/actions/workflows/verify.yml)
[![Release](https://github.com/emotionscientific/verlet-kernel/actions/workflows/release.yml/badge.svg)](https://github.com/emotionscientific/verlet-kernel/actions/workflows/release.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Status: experimental](https://img.shields.io/badge/status-experimental-yellow.svg)](README.md)

> Status: Verlet is experimental. APIs, behavior, and release packaging may
> change before a stable public release. It is the open-source engine under
> Verlet Cloud, the managed agents service.

## What Verlet Is

Verlet is an open-source runtime for AI agents, written in Rust.

You describe an agent in a manifest: which model it uses, which tools it may
call, which files, secrets, and network hosts it may reach, and what limits it
runs under. Verlet runs the agent from that description.

While the agent runs, Verlet writes every step to an append-only record: each
user message, each model reply, each tool call, each permission decision, and
each change to the agent's own setup. That record is the agent's only state.
Restarting, replaying, auditing, and rolling back all work by reading it.

The same runtime runs on a laptop, as a background daemon, or embedded in
another program. It behaves the same way in each place and produces the same
record.

## The Problem It Solves

Every serious agent turns into a small backend. It needs tools, secrets,
permissions, durable state, logs, retries, a place to run, and a way to
recover when it crashes. Most teams rebuild that backend for each agent, and
the parts that matter for trust are the parts that get skipped.

Three questions come up the first time an agent does something expensive:

- **What did it do, and why?** Most agent stacks keep a prompt log and a
  separate application log. Neither one explains the other.
- **What could it have done, and who allowed it?** If tool access lives in
  prompts and config files, nobody can answer this after the fact.
- **Can it change itself safely?** Agents that edit their own tools, prompts,
  or memory in place break often, because nothing records the change and
  nothing can undo it.

Verlet's answer is to put the agent in version control. The agent's definition
and its whole history live on one record. Acting is the same thing as
committing. A tool the agent was never given does not exist for it. Every
outside effect passes through one checkpoint, and the decision made there is
written down before the effect happens. When the agent changes its own setup,
that change goes through the same checkpoint as everything else.

## Start Here: The Primer

**[Agents in Version Control](https://emotionscientific.github.io/verlet-kernel/primer/agents-in-version-control.html)**
is a plain-language primer on what an agent runtime has to get right, and how
Verlet does it. It is the recommended first read, before any other page in this
repository. Also available as a [PDF](docs/primer/agents-in-version-control.pdf).

It is written for an engineer or product owner deciding what to build agents
on, or checking an agent product's claims. It uses one running example (a
support agent that can refund money) and introduces each term as the answer to
a problem already on the table. It also shows a real excerpt of the record, so
you can see what "everything is written down" looks like in practice.

Each section of the primer ends with a prompt you can paste into a coding agent
opened in this checkout. The agent answers from the code, so you can check the
primer's claims against the repository yourself.

## Try It

Install the CLI with Homebrew:

```sh
brew install emotionscientific/tap/verlet
```

Or with the release installer:

```sh
curl -fsSL https://github.com/emotionscientific/verlet-kernel/releases/latest/download/install.sh | sh
```

Then open the local console:

```sh
verlet chat
```

On the first run there is no model provider yet. The console opens a setup
window where you connect one: an API key, a ChatGPT-plan login, or a custom
endpoint. After that you have a running agent, and every turn you take with it
is on the record.

The intended local loop is: write a manifest, plan it, publish it, bind tools
and resources, run it, and inspect the events, receipts, and artifacts it
produced. [Getting started](docs/getting-started.md) walks that path, and the
docs mark the parts that are still being built as reserved or partial.

## How It Works, In One Screen

- **Manifest.** A folder that declares the agent: model profiles, policies,
  runtime defaults, workspace needs, context pipeline, and the tools and
  resources it proposes to use. It is checked into git like any other source.
- **Operation.** An executable contract with typed input, typed output, and
  declared effects. Published operations are content-addressed, so a name
  always resolves to one exact artifact. A tool is the model-visible face of
  an operation.
- **Binding.** When a thread starts, the kernel expands the manifest and
  records an attach event for each tool and resource. The agent's current
  toolset is computed by reading those attach and detach events. Nothing
  else grants tool access.
- **Thread and turn.** A thread is one durable line of work with its own
  history. A turn is one round of that work, from a submitted message to a
  quiet state or a budget limit.
- **Record.** Every thread is an append-only stream of events. Events are
  either witnessed (the runtime saw the world do something) or produced (a
  component computed something, and says from which inputs).
- **Receipt.** A recorded explanation of something the runtime resolved: which
  name resolved to which artifact, which attachment made a tool visible, which
  policy decision allowed an action, and what the model was shown.
- **Checkpoint.** Every continuation, whether a model call, a tool call, a child
  thread, or the end of a turn, passes one admission point. A policy decides
  what is allowed next. A fixed script makes a workflow. Letting the model
  choose makes an agent. Both run on the same record.
- **Placement.** Where a thread runs is a separate decision from what it may
  do. A thread can move between machines without changing its authority.

The [kernel invariants](docs/kernel-invariants.md) page states these rules
precisely. The [formalism](https://github.com/emotionscientific/verlet-formalism)
repository holds the laws and the lexicon the code is written against.

## Who It Is For

- Engineers choosing what to build agents on, who want durable state,
  replay, and an audit trail without writing that layer themselves.
- Teams that must answer "what did the agent do, what could it have done, and
  who allowed it" from data rather than from memory.
- Product builders who want the runtime contracts owned by something they can
  inspect. The product layer (auth, billing, dashboards, deployment) stays in
  product code and calls Verlet.
- Compliance and governance products, which can consume the record as
  evidence and plug policy decisions into the checkpoint.

## What Is In This Repository

- Runtime primitives: tenant hosts, thread lifecycle, history, events,
  cancellation, resume, and supervisor routing.
- Agent and operation contracts: manifests, tool publication, operation ABI,
  coupling ABI, attachment config, command projections, and Wasm operation
  support.
- Runtime surfaces: CLI, daemon, app-server RPC, MCP, ACP, provider adapters,
  virtual bash, virtual filesystem, process handles, and daemon-embedded
  stream sync with scoped credentials and single-writer leases.
- The web console at [apps/console](apps/console), shipped inside the kernel
  release as `verlet console`.
- Release and verification tooling for tagged binary releases.

For the detailed map, see [docs/repository-map.md](docs/repository-map.md).

## Current Status

The current release is v0.5.1. The work so far is the runtime core. Today the
kernel can:

- plan, publish, list, show, and run agent manifests locally;
- publish content-addressed operations and invoke them through the ABI,
  including Wasm operations and custom Wasm couplings with offline replay;
- install local tool kits, including a checked-in file-tools kit with a guest
  `/workspace` root backed by a witnessed host mount;
- publish prompt and context files as immutable resources, and publish skill
  directories (including conventional `SKILL.md` folders) with bind-time name
  pinning;
- mount operator-approved host directories into a thread through the virtual
  filesystem, read-only or read-write;
- run threads against model providers, virtual bash, and Wasm, with oversized
  output spilled to receipts on the thread's filesystem;
- connect to OpenAI Codex through a ChatGPT-plan login, with credentials kept
  in the user's provider store;
- expose the same contracts over the CLI, a daemon, RPC, MCP, and ACP;
- keep single-owner state homes with a `verlet debug journal` command for
  sanctioned forensics;
- place child threads in separate local processes through the daemon's
  authenticated, store-backed queue;
- run as a multi-tenant host with one authenticated listener and per-instance
  authentication;
- ship as `verlet`, `verlet-acp-agent`, and `verlet-mcp-server` binaries for
  macOS and Linux.

In progress: a pluggable policy router with held approvals and resume, stable
evidence export for outside governance systems, and managed-cloud hardening.
Verlet Cloud runs on this kernel and is concierge today. Self-serve cloud
placement, a public package registry, and marketplace flows are future
direction. See [docs/roadmap.md](docs/roadmap.md).

## Words This Repository Uses

"Kernel" is an overloaded word. Here it means the privileged core that owns
mechanism and refuses to own policy. The test for what belongs in it: if
changing a thing could make the audit receipts lie, it is compiled into the
kernel. That covers event ordering, provenance, attachment, budgets, and
receipts. Everything else lives above the kernel, named, versioned, and
swappable. The kernel does not think, does not prompt, and does not
orchestrate. It is not a Jupyter kernel and it is not an orchestration SDK.

The **runtime** is the kernel in motion: the running system that executes
turns and witnesses facts. When the docs say "the runtime did it," they mean an
event whose authority is the system's own attestation.

A **harness** is the industry's word for everything wrapped around a model.
Here you do not hand-write it. The system resolves it from the agent's
declarations and attachments, and you can export and diff it like a lockfile.

## How This Is Built

Coding agents are used extensively in the development of Verlet. Several
different models draft, implement, review, and cross-examine each other's
claims. A human architect makes the design decisions and reviews what lands.
This is what lets a very small team build a project of this size at this speed,
and it is why fixes land quickly after they are found.

The design intention is held fixed in a separate, public formalism:
[verlet-formalism](https://github.com/emotionscientific/verlet-formalism)
carries the laws and the lexicon the code is written against. Every primitive
is named there before it is implemented, and the codebase is scanned against it
on a regular cadence. When a scan finds code and canon disagreeing, either the
code is fixed or the canon is struck, with a commit that says which.

Treat the repository accordingly. It is experimental, it moves fast, and the
record of why it moved is in the commit history and the formalism.

## Documentation

- [Agents in Version Control](https://emotionscientific.github.io/verlet-kernel/primer/agents-in-version-control.html):
  the primer. Read this first ([PDF](docs/primer/agents-in-version-control.pdf)).
- [Docs index](docs/README.md): the full documentation map.
- [Overview](docs/index.md): product category and current status.
- [Getting started](docs/getting-started.md): local run and inspection path.
- [Kernel invariants](docs/kernel-invariants.md): the runtime rules every
  other page relies on.
- [CLI](docs/cli.md): canonical commands, help model, receipt-backed bind
  inspection, and sanctioned journal forensics.
- [Chat console](docs/chat.md) and [Provider setup](docs/provider-setup.md):
  the local terminal console and connecting a model provider.
- [Agent CLI](docs/agent-cli.md) and
  [Agent manifest ontology](docs/agent-manifest-ontology.md): manifest
  authoring, commands, and manifest shape.
- [Runtime primitives](docs/developers/runtime-primitives.md): kernel-owned
  surfaces and boundaries.
- [ABI](docs/abi.md): operation boundary and host/guest contract.
- [Wasm operation dev kit](docs/wasm-operation-dev-kit.md): Rust guest macros,
  fixtures, tool kits, custom coupling authoring, and replay.
- [OpenAPI adapter](docs/openapi-adapter.md): import a witnessed REST contract
  as ordinary published operations.
- [Command contracts](docs/command-contracts.md): command and manual
  projection rules.
- [Provider adapters](docs/provider-adapters.md): the agent loop boundary.
- [Daemon and RPC](docs/app-server.md): the app-server control plane.
- [Threat model](docs/threat-model.md): stable threat IDs, current status, and
  deterministic mitigation guards.
- [Testing guidelines](docs/testing-guidelines.md) and
  [How Verlet is tested](docs/how-verlet-is-tested.md).
- [Release guide](RELEASE.md): tags, targets, package smoke tests, and
  installer artifacts.

## Building And Verifying From Source

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
class of path, filesystem, glibc, epoll, signal, and timing bugs. CI's x86_64
leg remains the per-PR architecture backstop. The release preflight runs
`scripts/verify-linux.sh --amd64` before tagging. Run that lane on demand for
changes involving pointer width, atomics, SIMD, architecture-conditional
dependencies, or Wasm runtime internals. Emulation is substantially slower.

The first run downloads the Rust 1.97.1 Bookworm image (the stable toolchain
used by CI when this lane was added), installs its toolchain, and builds the
full workspace. Later runs reuse Cargo, Rustup, and architecture-specific
Cargo-lane state from the `verlet-verify-linux` Docker volume, but still
execute every verification step. Concurrent wrapper runs serialize access to
that shared volume. Docker Desktop should have at least 12 GB of memory under
Settings > Resources; the wrapper warns and reduces Cargo to two build jobs
below that limit. If cache corruption is suspected, reset it with
`docker volume rm verlet-verify-linux`; the next run will be cold.

For release packaging, tag checks, and async release publishing, see
[RELEASE.md](RELEASE.md).

## Contributing

Verlet is not accepting public contributions yet. Please read
[CONTRIBUTING.md](CONTRIBUTING.md) before opening issues, and do not open
unsolicited pull requests. Report suspected vulnerabilities through the private
process in [SECURITY.md](SECURITY.md).

## License

Verlet is licensed under the [Apache License, Version 2.0](LICENSE).
