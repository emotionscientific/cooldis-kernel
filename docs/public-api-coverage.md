# Public API Coverage

This ledger tracks public Verlet surfaces that humans, coding agents, model
agents, or external clients can invoke. It is intentionally explicit: a missing
row is a documentation gap, and a row with no manual projection is a man-page
gap.

The source of truth remains the runtime contract:

```text
ABI operation contract -> lawful projections -> visible bindings -> grants
```

Command syntax, MCP shape, HTTP routes, and model-tool schemas are projections.
They may improve caller ergonomics, but they may not change authority, required
inputs, durable effects, or output semantics.

## Coverage States

- `covered`: canonical doc exists and the current help/man page is sufficient
  for this slice.
- `partial`: canonical doc exists, but generated or richer manual projection is
  still missing.
- `gap`: known public surface needs a canonical doc, a manual projection, or
  both.
- `reserved`: named in help or docs, but not yet an implemented V1 public
  surface.

## CLI And Command Surfaces

| Surface | Canonical contract doc | Help/man projection | Status | Gap |
| --- | --- | --- | --- | --- |
| `verlet` | [README](../README.md), [Verlet Docs](README.md) | `verlet --help`, `verlet(1)` | covered | Root help is the concise start surface; `verlet commands`, subcommand help, and the authored manual provide the durable command inventory and overview. |
| `verlet init`, `verlet agent init` | [Verlet Agent CLI](agent-cli.md) | `verlet init --help`, `verlet agent init --help` | partial | Folder-first project graph implemented; add generated synopsis/options/output/exit-status manual. |
| `verlet coupling init` | [Rust Wasm Operation Dev Kit](wasm-operation-dev-kit.md), [ABI](abi.md) | `verlet coupling init --help` | partial | Scaffolds a macro-authored Wasm coupling package with native testkit and fixture proof; generated command manual remains follow-up work. |
| `verlet agent plan` | [Verlet Agent CLI](agent-cli.md) | `verlet agent plan --help` | partial | Document JSON/text output shape and validation exit statuses. |
| `verlet agent publish` | [Verlet Agent CLI](agent-cli.md) | `verlet agent publish --help` | partial | Document registry writes, idempotency, and failure statuses in man form. |
| `verlet agent list` | [Verlet Agent CLI](agent-cli.md) | `verlet agent list --help` | partial | Document listing output and registry-root behavior in man form. |
| `verlet agent versions` | [Verlet Agent CLI](agent-cli.md) | `verlet agent versions --help` | partial | Lists immutable snapshots in publication order with text and JSON projections; generated manual output remains follow-up work. |
| `verlet agent diff` | [Verlet Agent CLI](agent-cli.md) | `verlet agent diff --help` | partial | Diffs authored and resolved snapshots structurally with text and JSON projections; generated manual output remains follow-up work. |
| `verlet agent show` | [Verlet Agent CLI](agent-cli.md) | `verlet agent show --help` | partial | Document JSON record schema pointer in man form. |
| `verlet agent run` | [Verlet Agent CLI](agent-cli.md), [V1 Release Candidate Gate](v1-release-candidate.md) | `verlet agent run --help` | partial | Runs one manifest-backed local app-server turn and prints manifest receipt event ids; generated manual output remains follow-up work. |
| `verlet blob publish` | [Verlet Agent CLI](agent-cli.md) | `verlet blob publish --help` | partial | Publishes immutable blob resources for folder-first prompts and explicit static context inputs; generated manual output remains follow-up work. |
| `verlet import build`, `verlet import publish` | [OpenAPI To ABI Operation Adapter](openapi-adapter.md), [Rust Wasm Operation Dev Kit](wasm-operation-dev-kit.md) | `verlet import --help` | covered | V1 verifies a vendored JSON spec witness, renders deterministic Wasm, and publishes the multi-operation record through the normal capability gate. |
| `verlet coupling run --replay` | [Rust Wasm Operation Dev Kit](wasm-operation-dev-kit.md), [ABI](abi.md) | `verlet coupling run --help` | partial | Dry-runs a bound coupling artifact against recorded thread events, prints proposed discharges, and reports quota/budget blocks; generated manual output remains follow-up work. |
| `verlet tool build` | [Rust Wasm Operation Dev Kit](wasm-operation-dev-kit.md), [Verlet Tool Maker Skill](../skills/verlet-tool-maker/SKILL.md), [ABI](abi.md) | `verlet tool build --help` | partial | Package build emits a V0 receipt and runs fixtures; generate a richer manual/JSON receipt projection with artifact, config, and validation outputs next. |
| `verlet tool list` | [Tool Publish Storage](publish-storage.md), [Verlet Agent Maker Skill](../skills/verlet-agent-maker/SKILL.md) | `verlet tool list --help` | partial | Lists active operation records and full active artifact hashes; generated manual output remains follow-up work. |
| `verlet tool publish` | [Tool Publish Storage](publish-storage.md), [Rust Wasm Operation Dev Kit](wasm-operation-dev-kit.md), [Verlet Tool Maker Skill](../skills/verlet-tool-maker/SKILL.md) | `verlet tool publish --help` | partial | Package publish persists the accepted interface; generate manual with registry writes, grants, and replacement semantics. |
| `verlet tool run` | [ABI](abi.md), [Rust Wasm Operation Dev Kit](wasm-operation-dev-kit.md) | `verlet tool run --help` | partial | Generate manual with stdin/stdout/stderr, mounts, JSON output, and exit taxonomy. |
| `verlet tool manual` | [Command Contracts](command-contracts.md), [V1 Release Candidate Gate](v1-release-candidate.md) | `verlet tool manual <published-tool> [operation] [--json]` | partial | Published package operation manuals are implemented; first-party CLI command manuals and imported MCP manuals remain follow-up work. |
| `verlet tool source` | [Verlet MCP Server](mcp-server.md), [Secret Management](secret-management.md) | `verlet tool source --help` | partial | Remote MCP source add/list/show/remove is implemented; generate source-record and imported-tool manual pages with grants, discovery receipts, and failure taxonomy. |
| `verlet skill publish` | [Agent Manifest Ontology](agent-manifest-ontology.md), [Verlet Agent CLI](agent-cli.md), [Command Contracts](command-contracts.md) | `verlet skill publish --help` | partial | Publishes deterministic markdown skill packages into `.verlet/skills`, prints pinned and floating refs, and supports bind-time name resolution with pinned receipts; generated man page and JSON publish receipt projection remain follow-up work. |
| `verlet skill import` | [Agent Manifest Ontology](agent-manifest-ontology.md), [Verlet Agent CLI](agent-cli.md), [Command Contracts](command-contracts.md) | `verlet skill import --help` | partial | Compiles a conventional external skill directory into existing skill/blob records, reports inert scripts, hook-shaped files, and skipped files, prints manifest rows, and supports a write-free dry run; generated man page and JSON receipt projection remain follow-up work. |
| `verlet auth` | [Metadata And Provider Auth Storage](provider-storage.md), [Secret Management](secret-management.md) | `verlet auth --help` | partial | Local model-provider credential set/status/delete is implemented with redacted output; richer provider setup docs remain follow-up work. |
| `verlet secret` | [Secret Management](secret-management.md) | `verlet secret --help` | partial | Local import/set/list/status/delete is implemented with redacted values; generate per-command manual pages and richer grant receipt docs. |
| `verlet rpc` | [RPC Control Plane](app-server.md) | `verlet rpc --help` | partial | Add generated endpoint/method manual and wire examples. |
| `verlet chat` | [Chat Console](chat.md), [RPC Control Plane](app-server.md), [Provider Adapter Surface](provider-adapters.md) | `verlet chat --help` | partial | Bundled local terminal console over app-server RPC; richer generated manual remains follow-up work. |
| `verlet debug rpc` | [RPC Control Plane](app-server.md) | `verlet debug rpc --help` | partial | Protocol debug client for running daemon WebSocket endpoints; formal man page remains follow-up work. |
| `verlet debug bind` | [RPC Control Plane](app-server.md), [CLI](cli.md) | `verlet debug bind --help` | partial | Receipt-only effective bind explanation over live `thread/events/list` or an offline SQLite journal; formal man page remains follow-up work. |
| `verlet daemon run` | [Verlet Daemon](daemon.md), [Verlet IO](io.md) | `verlet daemon --help` | partial | Add generated daemon run/config/service man pages. |
| `verlet daemon config validate` | [Verlet Daemon](daemon.md) | `verlet daemon --help` | partial | Document config schema and validation errors in man form. |
| `verlet daemon service print` | [Verlet Daemon](daemon.md) | `verlet daemon --help` | partial | Document generated launchd/systemd output and side-effect boundary. |
| `verlet daemon service install` | [Verlet Daemon](daemon.md) | `verlet daemon --help` | partial | Document filesystem writes and non-starting behavior. |
| `verlet daemon service uninstall` | [Verlet Daemon](daemon.md) | `verlet daemon --help` | partial | Document target selection, labels, and idempotency. |
| `verlet console` | [RPC Control Plane](app-server.md) | `verlet console --help` | partial | Bundled local browser console served by the kernel app-server with static assets and token-gated `/rpc`; generated command manual remains follow-up work. |

## Runtime Contracts And Projections

| Surface | Canonical contract doc | Help/man projection | Status | Gap |
| --- | --- | --- | --- | --- |
| ABI operation contract | [ABI](abi.md) | n/a | covered | Keep projection law linked from every command-contract doc. |
| ABI coupling contract | [ABI](abi.md), [Agent Manifest Ontology](agent-manifest-ontology.md), [Rust Wasm Operation Dev Kit](wasm-operation-dev-kit.md) | `verlet coupling init` scaffold | covered | Custom Wasm couplings use versioned invocation/discharge JSON, pure-compute host imports, and SDK macro-authored exports; generated authoring/manual pages remain follow-up work. |
| Command contracts | [Command Contracts](command-contracts.md) | `verlet tool manual`; issue [#104](https://github.com/emotionscientific/cooldis/issues/104) | partial | Tool-package operation manuals, generated fallback manuals, and JSON projection are implemented; generated first-party CLI command man pages remain. |
| Virtual-bash commands | [Command Contracts](command-contracts.md), [ABI](abi.md) | `man <command>` inside agent turns | partial | Live operation shell commands expose a thread-visible command contract with `verlet run` origin, IO, capability, exit-status fields, and retrievable `/spill` receipts for oversized bash and process streams; richer package-authored docs still need to flow into the runtime registry. |
| MCP compatibility ingress | [Verlet MCP Server](mcp-server.md), [Command Contracts](command-contracts.md) | MCP tool descriptions | partial | Imported tools should compile into reviewed command contracts where useful. |
| RPC orchestrator boundary | [RPC Control Plane](app-server.md), [ADR 0009](adr/0009-orchestrator-boundary-v0.md) | app-server method docs | covered | `ingress/submit` provides attributed, deduplicated, admission-gated envelope delivery; `stream/append` and `stream/read` provide witnessed client records with sequence- and placement-epoch-fenced atomic append, a stable stale-lease rehome error, and verified cursors. |
| RPC receipts retrieval | [RPC Control Plane](app-server.md), [ADR 0009](adr/0009-orchestrator-boundary-v0.md) | `thread/list`, `thread/events/list`, and client-stream cursor recipe | covered | Metering and run-outcome consumers tail durable usage, turn-outcome, and egress-receipt events and persist their cursor map in a client stream; no second receipts truth is introduced. |
| First-party MCP client imports | [Verlet MCP Server](mcp-server.md), [Command Contracts](command-contracts.md) | `verlet tool source --help`; future command contract/manual projection | partial | Persisted remote source records and discovered snapshots exist; imported tools still need richer grants, receipts, and manual projection. |
| RPC thread methods | [RPC Control Plane](app-server.md) | app-server method docs | partial | `thread/start` and `thread/rebindFork` apply allow-listed manifest runtime overrides, including `max_tool_rounds`; manifest-bound `thread/spawn` resolves fresh bound runtime metadata. `thread/spawn` remote placement and idempotent `thread/submit` use the store-hosted child queue when sync is served. Generated method-level manual/reference table remains follow-up work. |
| Process handle operations and streaming `command/exec` | [Standard Operations](standard-operations.md), [ADR 0006](adr/0006-unified-orchestration-semantics.md) | operation schemas and app-server method docs | partial | Dispatch identity, durable settlement ingress, and pull/control folds are implemented; generated method-level manuals remain follow-up work. |
| RPC query/control methods | [RPC Control Plane](app-server.md) | app-server method docs, `verlet-workbench-smoke` | partial | Agent, operation, model, thread event, coupling, approval, and waiting query methods are implemented; `approval/resolve` witnesses abstract approval decisions. Generated endpoint/manual pages remain follow-up work. |
| First-party thread tools | [Standard Operations](standard-operations.md), [Unified Orchestration Semantics](adr/0006-unified-orchestration-semantics.md) | Published `cooldis-threads` operation manuals and virtual-bash commands | covered | Spawn defaults through the bound default-manifest alias; spawn, submit/steer, wait, status, and cancel address children by parent-scoped `task_name`, while raw ids remain in durable receipts and the journal. |
| Agent manifests | [Agent Manifest Ontology](agent-manifest-ontology.md), [Verlet Agent CLI](agent-cli.md), [V1 Release Candidate Gate](v1-release-candidate.md) | `verlet agent * --help` | partial | Typed workspace binding, opt-in no-mount workspace skill discovery, finite-or-unlimited `max_tool_rounds`, and absolute UTC expiry on tool and coupling grants are documented with durable bind witnesses; a generated schema reference remains follow-up work. |
| Daemon config | [Verlet Daemon](daemon.md), [Verlet IO](io.md) | `verlet daemon config validate --help` | partial | Placement and workspace defaults, store-primary `[daemon.sync]`, Telegram, and `clock.tick` route config are documented; add versioned config schema reference. |
| Daemon store-primary sync HTTP | [Verlet Daemon](daemon.md), [ADR 0006](adr/0006-unified-orchestration-semantics.md) | `[daemon.sync]` config validation | covered | V1 push, verified-cursor pull, queue acknowledgement, lease renewal, and process-backed remote child placement are projected over a loopback HTTP or Unix listener; the live WebSocket lane remains deliberately absent. |
| Provider adapters | [Provider Adapter Surface](provider-adapters.md) | config/help docs | partial | Keep provider config examples generic and outside product logic. |
| Secret references | [Secret Management](secret-management.md) | `verlet secret --help` | partial | Local SQLite secret refs and redacted CLI status are implemented; provider-specific secret manager adapters and richer grant receipts remain V2. |
| Identity/RBAC adapters | Identity/RBAC adapter boundary | n/a | reserved | V2 adapter boundary; V1 remains local principals and explicit grants. |
| Python/WASIX operation source | [ABI](abi.md), [Command Contracts](command-contracts.md) | future `verlet tool build --runtime python-wasix` and generated command/manual projection | reserved | V2 authoring lane for local Python functions with declared dependencies; interface remains the operation contract, with Wasmer/WASIX as a package/process placement. |

## Update Rule

When adding or changing an invokable surface:

1. Add or update the canonical contract doc.
2. Add or update the help/man projection.
3. Add or update this ledger with `covered`, `partial`, `gap`, or `reserved`.
4. Run `cargo test --workspace --all-targets --locked`.

This keeps documentation gaps and man-page gaps visible in the same quality lane
as runtime contract tests.
