# Verlet Chat

`verlet chat` is the bundled local terminal console for operating a Verlet
app-server session. It is intentionally an RPC client over the app-server
boundary, not a privileged runtime path. By default it launches a private local
app-server; with `--attach` it connects to an existing endpoint.

```text
verlet chat [PROMPT] [--config <file>] [--cwd <path>]
verlet chat [PROMPT] --attach <unix://path|ws://host:port[/rpc]>
verlet chat [PROMPT] --provider <provider> [--model <model>] ...
```

## Architecture

The UI lives in the `verlet-chat` crate (`crates/verlet-chat`), built on
[tuika](https://github.com/everruns/tuika) and ported from its `codex`
example. Its `App` core is a synchronous state machine that consumes typed
`ChatEvent`s and emits typed `Action`s, with no RPC or async code, so the whole
state surface is testable without a terminal. A thin async runner owns the
terminal session and multiplexes input, host events, and animation ticks.

The kernel side (`crates/verlet-kernel/src/cli/chat.rs`) hosts the async
driver: it owns the JSON-RPC client, translates app-server notifications into
`ChatEvent`s, and executes `Action`s as RPC calls. Chat therefore remains a
pure client of the app-server; other frontends can replace it without touching
the runtime.

tuika is pinned to an exact version in the workspace manifest: it is pre-1.0
and minor releases may break API, so upgrades are deliberate, reviewed
changes.

## Model Catalog

Model metadata comes from a small models.dev snapshot checked into the kernel
for the `anthropic`, `openai`, and `openai-codex` providers. The app-server can
therefore list models with no network access. On startup it also schedules a
background models.dev refresh, capped at once per 24 hours, and stores the last
valid normalized response under the same user state home as the provider
metadata store. A valid cached response overlays the built-in snapshot; a
refresh or cache failure silently falls back to the built-in data and never
blocks chat.

The refresh endpoint defaults to `https://models.dev/api.json`. Set
`VERLET_MODEL_CATALOG_URL` before starting the app-server to use a compatible
endpoint instead; setting it to an empty or whitespace-only value disables
refresh entirely. Catalog prices are informational model metadata only; they
are not the authority for cloud metering.

## Included Surface

- Full-screen transcript with streaming markdown answers, thinking rows,
  and tool/command cells that stream output live and collapse long output to
  a middle-elided preview with the exit status.
- Multiline composer (Enter submits, Shift+Enter or Ctrl+J inserts a
  newline, paste is bracketed), with Up-arrow history recall on an empty
  composer.
- Slash-command popup with filtering and Tab completion:
  `/help`, `/quit`, `/q`, `/interrupt`, `/clear`, `/status`, `/new`,
  `/sessions`, `/resume <thread-id>`, `/rename <name>`, `/fork`, `/compact`,
  and `/models`.
- Working indicator with elapsed time while a turn is in flight; Esc or
  Ctrl+C interrupts, Ctrl+C on an idle session quits, Ctrl+D quits.
- Footer with key hints, the thread short id, and the turn state; banner
  with version, connection mode, cwd, and the app-server's runtime-active model.
- PgUp/PgDn scrollback that sticks to the tail when at the bottom.
- Dark theme by default; `NO_COLOR=1` drops to the terminal's own colors.

## Known Limits

Approvals are not surfaced in the TUI yet (the app-server exposes them via
polling only). A broadcast-lag resync shows a notice rather than rebuilding
the transcript. The console does not implement shell escape, mouse interaction
beyond wheel scrolling, file mentions, external editor handoff, export/copy,
an orchestrator view, or an interactive model picker. Runtime model switching
is available to RPC clients through `model/select`; the banner and `/models`
status reflect the active entry returned by `model/list`. A switch affects the
next turn and is reset when the app-server restarts. Those remaining UI surfaces
are separate product decisions. The goal is a credible default local console
while preserving the app-server RPC boundary.
