# Public API Coverage

This ledger tracks public Cooldis surfaces that humans, coding agents, model
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
| `cooldis` | [README](../README.md), [Cooldis Docs](README.md) | `cooldis --help` | partial | Generate a model-facing `man cooldis` page from the command contract. |
| `cooldis init`, `cooldis agent init` | [Cooldis Agent CLI](agent-cli.md) | `cooldis init --help`, `cooldis agent init --help` | partial | Folder-first project graph implemented; add generated synopsis/options/output/exit-status manual. |
| `cooldis coupling init` | [Rust Wasm Operation Dev Kit](wasm-operation-dev-kit.md), [ABI](abi.md) | `cooldis coupling init --help` | partial | Scaffolds a macro-authored Wasm coupling package with native testkit and fixture proof; generated command manual remains follow-up work. |
| `cooldis agent plan` | [Cooldis Agent CLI](agent-cli.md) | `cooldis agent plan --help` | partial | Document JSON/text output shape and validation exit statuses. |
| `cooldis agent publish` | [Cooldis Agent CLI](agent-cli.md) | `cooldis agent publish --help` | partial | Document registry writes, idempotency, and failure statuses in man form. |
| `cooldis agent list` | [Cooldis Agent CLI](agent-cli.md) | `cooldis agent list --help` | partial | Document listing output and registry-root behavior in man form. |
| `cooldis agent show` | [Cooldis Agent CLI](agent-cli.md) | `cooldis agent show --help` | partial | Document JSON record schema pointer in man form. |
| `cooldis agent run` | [Cooldis Agent CLI](agent-cli.md), [V1 Release Candidate Gate](v1-release-candidate.md) | `cooldis agent run --help` | partial | Runs one manifest-backed local app-server turn and prints manifest receipt event ids; generated manual output remains follow-up work. |
| `cooldis blob publish` | [Cooldis Agent CLI](agent-cli.md) | `cooldis blob publish --help` | partial | Publishes immutable blob resources for folder-first prompts and explicit static context inputs; generated manual output remains follow-up work. |
| `cooldis tool build` | [Rust Wasm Operation Dev Kit](wasm-operation-dev-kit.md), [Cooldis Tool Maker Skill](../skills/cooldis-tool-maker/SKILL.md), [ABI](abi.md) | `cooldis tool build --help` | partial | Package build emits a V0 receipt and runs fixtures; generate a richer manual/JSON receipt projection with artifact, config, and validation outputs next. |
| `cooldis tool list` | [Tool Publish Storage](publish-storage.md), [Cooldis Agent Maker Skill](../skills/cooldis-agent-maker/SKILL.md) | `cooldis tool list --help` | partial | Lists active operation records and full active artifact hashes; generated manual output remains follow-up work. |
| `cooldis tool publish` | [Tool Publish Storage](publish-storage.md), [Rust Wasm Operation Dev Kit](wasm-operation-dev-kit.md), [Cooldis Tool Maker Skill](../skills/cooldis-tool-maker/SKILL.md) | `cooldis tool publish --help` | partial | Package publish persists the accepted interface; generate manual with registry writes, grants, and replacement semantics. |
| `cooldis tool run` | [ABI](abi.md), [Rust Wasm Operation Dev Kit](wasm-operation-dev-kit.md) | `cooldis tool run --help` | partial | Generate manual with stdin/stdout/stderr, mounts, JSON output, and exit taxonomy. |
| `cooldis tool manual` | [Command Contracts](command-contracts.md), [V1 Release Candidate Gate](v1-release-candidate.md) | `cooldis tool manual <published-tool> [operation] [--json]` | partial | Published package operation manuals are implemented; first-party CLI command manuals and imported MCP manuals remain follow-up work. |
| `cooldis tool source` | [Cooldis MCP Server](mcp-server.md), [Secret Management](secret-management.md) | `cooldis tool source --help` | partial | Remote MCP source add/list/show/remove is implemented; generate source-record and imported-tool manual pages with grants, discovery receipts, and failure taxonomy. |
| `cooldis skill publish` | [Agent Manifest Ontology](agent-manifest-ontology.md), [Command Contracts](command-contracts.md) | `cooldis skill publish --help` | partial | Publishes deterministic markdown skill packages into `.cooldis/skills`; generated man page and JSON receipt projection remain follow-up work. |
| `cooldis auth` | [Metadata And Provider Auth Storage](provider-storage.md), [Secret Management](secret-management.md) | `cooldis auth --help` | partial | Local model-provider credential set/status/delete is implemented with redacted output; richer provider setup docs remain follow-up work. |
| `cooldis secret` | [Secret Management](secret-management.md) | `cooldis secret --help` | partial | Local import/set/list/status/delete is implemented with redacted values; generate per-command manual pages and richer grant receipt docs. |
| `cooldis rpc` | [RPC Control Plane](app-server.md) | `cooldis rpc --help` | partial | Add generated endpoint/method manual and wire examples. |
| `cooldis chat` | [Chat Console](chat.md), [RPC Control Plane](app-server.md), [Provider Adapter Surface](provider-adapters.md) | `cooldis chat --help` | partial | Bundled local terminal console over app-server RPC; richer generated manual remains follow-up work. |
| `cooldis debug rpc` | [RPC Control Plane](app-server.md) | `cooldis debug rpc --help` | partial | Protocol debug client for running daemon WebSocket endpoints; formal man page remains follow-up work. |
| `cooldis daemon run` | [Cooldis Daemon](daemon.md), [Cooldis IO](io.md) | `cooldis daemon --help` | partial | Add generated daemon run/config/service man pages. |
| `cooldis daemon config validate` | [Cooldis Daemon](daemon.md) | `cooldis daemon --help` | partial | Document config schema and validation errors in man form. |
| `cooldis daemon service print` | [Cooldis Daemon](daemon.md) | `cooldis daemon --help` | partial | Document generated launchd/systemd output and side-effect boundary. |
| `cooldis daemon service install` | [Cooldis Daemon](daemon.md) | `cooldis daemon --help` | partial | Document filesystem writes and non-starting behavior. |
| `cooldis daemon service uninstall` | [Cooldis Daemon](daemon.md) | `cooldis daemon --help` | partial | Document target selection, labels, and idempotency. |
| `cooldis console` | [RPC Control Plane](app-server.md) | `cooldis console --help` | partial | Bundled local browser console served by the kernel app-server with static assets and token-gated `/rpc`; generated command manual remains follow-up work. |

## Runtime Contracts And Projections

| Surface | Canonical contract doc | Help/man projection | Status | Gap |
| --- | --- | --- | --- | --- |
| ABI operation contract | [ABI](abi.md) | n/a | covered | Keep projection law linked from every command-contract doc. |
| ABI coupling contract | [ABI](abi.md), [Agent Manifest Ontology](agent-manifest-ontology.md), [Rust Wasm Operation Dev Kit](wasm-operation-dev-kit.md) | `cooldis coupling init` scaffold | covered | Custom Wasm couplings use versioned invocation/discharge JSON, pure-compute host imports, and SDK macro-authored exports; generated authoring/manual pages remain follow-up work. |
| Command contracts | [Command Contracts](command-contracts.md) | `cooldis tool manual`; issue [#104](https://github.com/emotionscientific/cooldis/issues/104) | partial | Tool-package operation manuals, generated fallback manuals, and JSON projection are implemented; generated first-party CLI command man pages remain. |
| Virtual-bash commands | [Command Contracts](command-contracts.md), [ABI](abi.md) | `man <command>` inside agent turns | partial | Live operation shell commands expose a thread-visible command contract with `cooldis run` origin, IO, capability, and exit-status fields; richer package-authored docs still need to flow into the runtime registry. |
| MCP compatibility ingress | [Cooldis MCP Server](mcp-server.md), [Command Contracts](command-contracts.md) | MCP tool descriptions | partial | Imported tools should compile into reviewed command contracts where useful. |
| First-party MCP client imports | [Cooldis MCP Server](mcp-server.md), [Command Contracts](command-contracts.md) | `cooldis tool source --help`; future command contract/manual projection | partial | Persisted remote source records and discovered snapshots exist; imported tools still need richer grants, receipts, and manual projection. |
| RPC thread methods | [RPC Control Plane](app-server.md) | app-server method docs | partial | Thread thinking params, precedence, and item stream projection are documented; generated method-level manual/reference table remains follow-up work. |
| RPC query/control methods | [RPC Control Plane](app-server.md) | app-server method docs, `cooldis-workbench-smoke` | partial | Agent, operation, model, thread event, coupling, approval, and waiting query methods are implemented; `approval/resolve` witnesses abstract approval decisions. Generated endpoint/manual pages remain follow-up work. |
| Agent manifests | [Cooldis Agent CLI](agent-cli.md), [V1 Release Candidate Gate](v1-release-candidate.md) | `cooldis agent * --help` | partial | Add schema reference once manifest fields expand beyond identity. |
| Daemon config | [Cooldis Daemon](daemon.md), [Cooldis IO](io.md) | `cooldis daemon config validate --help` | partial | Telegram and `clock.tick` route config is documented; add versioned config schema reference. |
| Provider adapters | [Provider Adapter Surface](provider-adapters.md) | config/help docs | partial | Keep provider config examples generic and outside product logic. |
| Secret references | [Secret Management](secret-management.md) | `cooldis secret --help` | partial | Local SQLite secret refs and redacted CLI status are implemented; provider-specific secret manager adapters and richer grant receipts remain V2. |
| Identity/RBAC adapters | Identity/RBAC adapter boundary | n/a | reserved | V2 adapter boundary; V1 remains local principals and explicit grants. |
| Python/WASIX operation source | [ABI](abi.md), [Command Contracts](command-contracts.md) | future `cooldis tool build --runtime python-wasix` and generated command/manual projection | reserved | V2 authoring lane for local Python functions with declared dependencies; interface remains the operation contract, with Wasmer/WASIX as a package/process placement. |

## Update Rule

When adding or changing an invokable surface:

1. Add or update the canonical contract doc.
2. Add or update the help/man projection.
3. Add or update this ledger with `covered`, `partial`, `gap`, or `reserved`.
4. Run `cargo test --workspace --all-targets --locked`.

This keeps documentation gaps and man-page gaps visible in the same quality lane
as runtime contract tests.
