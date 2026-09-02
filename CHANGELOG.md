# Changelog

## Unreleased

### Release tooling

- The primer source and build script now live in the repository. Local verify
  and CI check that the committed HTML matches the source.
- Added `scripts/release.sh` and `just release <tag>` as the maintainer release
  button. It bumps the workspace version, rolls the changelog, runs the local
  gates, lands a release PR, tags the merge, waits for GitHub publishing,
  verifies the published installer, updates Homebrew, and writes a receipt.
- Removed `scripts/release-async.sh`. Releases now use the synchronous,
  resumable release button.
- Moved the Homebrew tap dispatch out of the GitHub release workflow and into
  the maintainer release button, which uses the maintainer's `gh` login and
  waits for the tap update.
- The model catalog refresh script can write to a selected output path so the
  release preflight can compare a temporary refresh without changing the
  checked-in snapshot.
- The installer and release manifest now default to the current
  `emotionscientific/verlet-kernel` repository.

### State storage

- State database filenames are now `session_history.turso` and
  `metadata.turso`. This is a compatibility break with no migration shim. Old
  state homes remain stale and are not opened by current Verlet releases.
- Added `verlet debug journal` for raw event record inspection. It supports
  thread, kind, and inclusive sequence filters, reads a live store through the
  owner RPC, and permits a direct read-only Turso open only for a cold store.

### Tool kits

- Kit tools can declare a derived model-facing input surface over their
  authored ABI envelope. The Pi tools now expose only their nested argument
  schema to models, while `root` is bound to the guest `/workspace` path. The
  default manifest declares that read-write mount and the app-server binds its
  configured cwd as the witnessed host directory. Surface schemas are closed
  JSON objects, bound values are validated when bindings are created or
  replayed, and surface operations are not also exposed through raw
  virtual-bash operation commands.
- Pi-kit file search now skips non-UTF-8, NUL-containing, and oversized files
  during grep walks, skips Unix special files and per-file read failures during
  directory walks, bounds grep, read, edit, and find ignore-file reads to 8 MiB,
  rejects mismatched file and directory path kinds through structured tool
  errors, and keeps native and wasm output envelopes identical for those cases.
  Directly named grep files still surface read errors, and directly named
  special files are rejected before opening so FIFOs never wait for a peer.
  The default Wasm fuel budget is now 10 billion: measured file processing
  costs about 154 fuel per byte, so a maximal read fits several times over and
  multi-file grep walks across tens of MiB complete while runaway guests remain
  bounded to seconds of CPU.

### Fixes

- Terminally failed turns now append one bounded, body-free `turn.failed`
  outcome to the thread journal with provider attribution and retry count.
- The serve thread-handle ingress adapter now backs off persistent history-store
  failures to a 60 second retry interval, logs only retry state transitions,
  reports degradation, and reports recovery without exiting the serve process.

## v0.5.1 (2026-08-26)

### Tool kits

- Added the kit surface: a kit is a directory of tool packages with a
  `verlet.kit.toml` manifest declaring the tool set it exposes.
  `verlet kit install <kit-dir>` builds and proves each member package,
  publishes the operations content-addressed, and writes one installed-kit
  record under `.verlet/kits/`. The daemon's default manifest synthesizes
  `direct_tool` rows from installed-kit records at startup, so installed
  tools appear after the next daemon restart.
- Added the pi kit under `agent-tools/`: read, write, edit, find, and grep
  file tools compiled to wasm, exposed to models under their plain names.
  `scripts/build-pi-kit-dist.sh` builds the distributable kit directory.
- A tool package can now declare the model-facing tool name for an
  operation (`[operations.mcp] tool_name`); the declared name overlays the
  stored projection on every derivation lane.
- `verlet chat` setup gained a "Tool kits" step: an "Install tools" row on
  the setup home screen, and a one-shot first-run offer after the first
  model selection when the recommended kit is not installed. Installs run
  the same pipeline as `verlet kit install` against the project instance;
  attached sessions are directed to run the install on the instance host.

### Dependencies

- bashkit 0.14.3 to 0.17.1, tuika 0.8.0 to 0.11.1, turso and turso_core
  0.7.0-pre.19 to 0.8.0-pre.7.
- The virtual shell's `yaml` builtin is gone: bashkit removed it, and its
  `yq` replacement needs a bashkit feature this build does not enable. The
  `yaml` name is no longer reserved and can be claimed by an operation.
- The virtual shell gained bashkit's new builtins (bzip2 family), tighter
  resource bounds on heavy scripts, and stricter parsing (malformed `$(...)`
  fails closed). The reserved-command list is now pinned to bashkit's real
  builtin set by a test, and shell grammar keywords (`time`, `if`, `case`,
  ...) are reserved too: operations can no longer claim names the shell
  parses as syntax.
- Shell output is bounded at the text boundary: non-UTF-8 bytes from a
  shell pipeline can no longer expand past the 64 MiB retention ceiling,
  and truncation reported by nested operation and external-proxy outputs
  now survives into the final command result instead of being dropped.

### Fixes

- The Linux verification lane no longer kills its own container: trace-ab
  terminates child process groups with a direct killpg syscall, and lane
  diagnostics name the known SIGKILL causes.
- Cargo lane locks are keyed so containers cannot see stale locks from
  another container's run.
- Socket-release assertions retry to absorb close-notify latency.

## v0.5.0 (2026-08-24)

### Control plane

- Added Host-authority `secret/list`, `secret/status`, `secret/set`,
  `secret/delete`, Unix-only `secret/resolve`, and `identity/list`,
  `identity/declare`, Unix-only `identity/mint`, and `identity/revoke` app-server
  methods. Secret, non-bootstrap identity, and secret-bearing `tool run` CLI
  paths now use the owning instance instead of opening its stores directly.

### Compatibility paths removed

- `identity declare`, `identity mint`, and identity revocation commands no
  longer accept `--declared-by`, `--minted-by`, or `--revoked-by`. The acting
  principal is always the authenticated operator connection. `identity
  bootstrap` remains the only offline identity command.
- `verlet daemon run` was removed. Use the top-level `verlet serve` command;
  daemon config validation and service management remain under `verlet daemon`.
- `verlet chat` no longer starts a private app-server. It discovers the project
  instance and uses the same detached, idle-bounded server startup as other
  client commands when nothing is running.

- Persisted `cooldis.agent-manifest` records and manifests that still carry
  grant-string authority payloads are rejected. Republish those records with
  the current Verlet version. Persisted agent records missing current required
  fields and default-agent records in the pre-rename namespace are rejected
  with the same guidance.
- Thread streams whose manifest bind receipts declare operation bindings but
  have no `binding.attached` or `binding.detached` events no longer derive a
  toolset from receipts or cached metadata. Start a new thread. Current
  receipt-only streams that declare an empty operation toolset, and streams
  with neither receipts nor binding events, still resume with an empty
  toolset.
- A persisted thread lifecycle with a completely empty event stream is
  rejected at runtime construction with "start a new thread" guidance. Only
  the unpersisted start boundary may build from the context binding plan;
  injected runtime factories without a journal also use that explicit plan.
- Invalid, unwitnessed, or mismatched persisted workspace metadata is rejected
  instead of being stripped during daemon lazy load and resumed unbound.
- Binding projection no longer recognizes the short-lived EMO-584 complete
  snapshot replay shape. Attachments remain active by event id until an
  explicit `binding.detached` event retires them.
- Tool recovery no longer treats a missing recorded request fingerprint as a
  wildcard match for a current fingerprint.
- Queued ingress no longer synthesizes delivery witnesses from older dedupe
  metadata. Unwitnessed envelopes fail validation; the current route principal
  is still attached from the declared route identity.
- The unused `RuntimeKernelControl::spawn_subagent` API alias was removed; use
  the witnessed child-spawn surface.
- The app-server no longer reconstructs a manifest tool-use instruction from
  old operation metadata.
- Bare `tool://` agent tool refs are rejected. Use `op://` for published
  operations or `mcp://` for configured MCP sources.
- `verlet init` and `verlet agent init` now write folder-first projects only.
  `--out` must name a project directory.
- The pre-rename console WebSocket token subprotocol is no longer accepted.
- Frozen durable-record identifiers in `docs/format-ids.md` are unchanged.

## v0.4.0 (2026-08-13)

### The binding model

Tool authority is now event-sourced. This release completes the redesign
started in v0.3.x: the grant-string layer is gone, and a thread's toolset is
determined by its own recorded history.

- Attachment is the gate. A tool is available to a thread because an
  attach event is recorded in that thread's stream; detaching it is another
  recorded event. There are no grant strings and no per-call
  set-membership checks.
- Bindings are journaled. `binding.attached` and `binding.detached` events
  (`cooldis.events/0.4`) carry the binder's provenance and the resolved
  principal, and they are appended in the same fenced batch as the bind
  receipt. The record can now answer "when did this agent get this tool."
- Toolsets derive from the stream. The catalog and the operation router are
  views over the thread's binding history. Every `tool.call.requested`
  receipt cites the attach event that admitted the operation.
- Resume replays the record. Resuming a thread reconstructs its toolset and
  runtime configuration from the thread's own stream and durable receipts.
  It loads no registry, compares no manifest hashes, and appends no events.
  Threads that were previously stranded at resume by a republished or
  deleted agent record now resume and keep working, and their tool calls
  still cite the original attach event.
- Secret injection and private-network enforcement moved to attachment
  configuration, where the real gate is.

The agent manifest remains as a preset: a declared opening sequence of
attach events for thread start, carrying its non-authority payload
(model profile, runtime settings) as before. `manifest.bind.completed`
receipts and all frozen `cooldis.*` format identifiers
(`docs/format-ids.md`) are unchanged.

### Rename compat removal

The v0.3.x cooldis-to-verlet deprecation window is over. Removed:

- the `cooldis` shim binary; archives and installers ship `verlet`,
  `verlet-acp-agent`, and `verlet-mcp-server`
- `COOLDIS_*` environment fallbacks; only canonical `VERLET_*` names are
  read
- `cooldis.agent-manifest` kind acceptance; the accepted kind is
  `verlet.agent-manifest`
- unpinned `op://cooldis-*` package aliases and `cooldis_*` MCP tool
  aliases
- legacy `cooldis.toml` / `cooldis.json` / `.cooldis` config and state
  discovery

Preserved forever: the frozen durable-record format identifiers
(`cooldis.events/*`, `cooldis.event.*`, see `docs/format-ids.md`), pinned
old-name `op://...@sha256` refs against preserved records, and every
historical receipt. Old streams remain readable; resume works through the
compatibility lanes for records written before the binding events existed.

### Also in this release

- Hooks that exit without reading stdin complete normally; the exit status
  and stdout decide the outcome.
- Concurrent resumes of the same thread share one runtime build.
- The name lint tightened: allowlist entries that existed only for the
  removed compat lanes are gone.

## v0.3.2 (2026-08-08)

### Changes

- Rebuilt the `verlet chat` terminal console on the tuika UI framework:
  streaming markdown answers, distinct thinking rows, live tool and command
  output cells with wrap-then-elide previews, a slash-command popup with tab
  completion, a multiline composer with paste and history recall, and
  scrollback that follows the tail only while at the bottom. The console
  remains a pure RPC client of the app-server; the UI lives in the new
  `verlet-chat` crate.
- Kernel operation dispatchers are now immutable at registration and
  resolved per thread through dispatch overlays, removing the mutable
  shared-registry slot.
- Journal appends are fenced by the placement-lease epoch, so a superseded
  host can no longer write after its lease moves.
- Added a Nix flake for the dev shell and binary packaging.
- Documented the frozen `cooldis.*` format identifiers in
  `docs/format-ids.md`; the repository name lint now closes Rust source over
  an explicit frozen-identifier allowlist.

### Deprecations

- Renamed the kernel packages `cooldis-threads`, `cooldis-schedule`,
  `cooldis-process`, and `cooldis-notify` to their `verlet-*` forms. Unpinned
  old `op://` refs and bare package names resolve to the new records with a
  deprecation warning through v0.3.x. Pinned old refs continue to address an
  already-persisted old-name record and hash; they are not redirected, and a
  fresh host without that record returns the normal not-found error. The old
  aliases are scheduled for removal in v0.4.0.
- Agent manifests now emit and prefer `verlet.agent-manifest`. The former
  `cooldis.agent-manifest` kind remains accepted through v0.3.x and is
  scheduled for removal in v0.4.0. Already-persisted agent records are not
  rewritten.

## v0.3.1 (2026-08-05)

### Changes

- Added the v0 orchestrator boundary on the app-server (ADR 0009): a
  record-client surface for external orchestrators, with the client-stream
  boundary documented.
- The daemon egress drain now maintains incremental per-thread views instead
  of replaying the full journal on every drain.
- The frozen V1 stream schema registry is cached instead of being rebuilt on
  each validation.

## v0.3.0 (2026-08-04)

### Deprecations

- Renamed the Cooldis kernel, crates, and primary binary to Verlet. The
  `cooldis` binary name, `COOLDIS_*` environment variables, old config
  filenames, and old state-directory names remain compatible in v0.3.0 with
  deprecation warnings; removal is no earlier than v0.4.0. Release archives,
  installer, and the Homebrew formula now use the `verlet` name.

### Changes

- Fork ingress claims now settle correctly when a store append fails
  ambiguously: reconciliation reads the authoritative event stream, adopts
  the entry if the write landed, and retries once only when no durable
  record exists. Previously a single failed append could leave a fork claim
  durably orphaned so the child thread never started.
- RPC client errors from the standalone credential path render without
  internal component naming, and the credential setup is documented.
- Bumped bashkit from 0.9.0 to 0.14.3.
