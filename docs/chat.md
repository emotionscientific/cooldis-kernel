# Verlet Chat

`verlet chat` is the bundled local terminal console for operating a Verlet
app-server session. It is intentionally an RPC client over the app-server
boundary, not a privileged runtime path. By default it discovers the project
instance and auto-starts an idle-bounded `verlet serve` when needed. With
`--attach` it connects to the specified endpoint.

```text
verlet chat [PROMPT] [--config <file>] [--cwd <path>]
verlet chat [PROMPT] --attach <unix://path|ws://host:port[/rpc]>
```

## OpenAI Codex With A ChatGPT Plan

The `openai-codex` provider authenticates through OpenAI and charges usage to
the signed-in user's ChatGPT plan. It does not use an OpenAI API key.

```sh
verlet auth login openai-codex
verlet auth status openai-codex
verlet chat
```

Login normally opens a browser and listens for the registered OAuth callback on
`127.0.0.1:1455`. Use the device-code flow on a remote or headless machine, or
when that port is unavailable:

```sh
verlet auth login openai-codex --device
```

Credentials are stored in the user provider store. `verlet auth status
openai-codex` shows redacted configuration status, and `verlet auth delete
openai-codex` signs the local Verlet installation out. Verlet refreshes expiring
tokens automatically.

The same setup is available inside the TUI through `/setup` (see
[Provider Setup](provider-setup.md)). The setup window lists configured
providers, offers the full provider catalog and a custom-provider form,
accepts pasted API keys for key-based providers, and offers browser or
device-code sign-in for `openai-codex`. Browser sign-in opens on the machine
running `verlet chat` and listens on that machine's localhost callback, even
when the chat was started with `--attach`; the OAuth flow is client-side and
only the completed credential is sent to the attached app-server. When no
provider is configured, the window opens automatically at startup.

An attached app-server must support `modelProvider/auth/setOAuth` to complete
in-TUI OpenAI Codex sign-in (use a kernel build containing this chat credential
wiring or newer). Device-code sign-in is the appropriate choice when the chat
client machine is headless or cannot open a browser.

The built-in seed guarantees `gpt-5.6-sol`, `gpt-5.6-terra`, and
`gpt-5.6-luna`; `gpt-5.6-sol` is the default. The merged model catalog can add
other current Codex-plan models without replacing those baseline rows.

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

Provider and model metadata comes from a models.dev snapshot checked into the
kernel: every OpenAI-compatible and Anthropic-compatible provider the upstream
data can derive (about 160), plus the static `openai-codex` entry. The
app-server can therefore list providers and models with no network access. On
startup it also schedules a background models.dev refresh, capped at once per
24 hours, and stores the last valid normalized response under the same user
state home as the provider metadata store. A valid cached response overlays
the built-in snapshot; a refresh or cache failure silently falls back to the
built-in data and never blocks chat.

The refresh endpoint defaults to `https://models.dev/api.json`. Set
`VERLET_MODEL_CATALOG_URL` before starting the app-server to use a compatible
endpoint instead; setting it to an empty or whitespace-only value disables
refresh entirely. Catalog prices are informational model metadata only; they
are not the authority for cloud metering.

Before a release, regenerate the checked-in snapshot with
`scripts/update-model-catalog.sh` and review the resulting diff by hand:
catalog base URLs decide where credentials are sent, so a changed or added
URL is trust-bearing. The script writes the snapshot through the same Rust
normalization the runtime refresh uses (HTTPS required for remote hosts,
plain HTTP only for loopback), so regeneration is byte-stable for unchanged
upstream data.

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
  `/models`, `/setup`, and `/providers`.
- `/setup` (alias `/providers`) opens the provider setup window: a centered
  modal with a configured-provider overview, a searchable catalog picker, a
  custom-provider form, and credential entry. Pasted keys cross the app-server
  RPC boundary; OpenAI Codex browser/device OAuth runs in the chat client
  process before the completed credential is sent to the server. See
  [Provider Setup](provider-setup.md).
- `/models` opens a modal picker backed by a fresh `model/list` request.
  Selecting a row calls `model/select`; missing credentials are reported by
  the app-server without changing the active model. While a modal window is
  open, it owns keyboard input and Esc dismisses it one level at a time.
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
or an orchestrator view. Runtime model switching is also available to other RPC
clients through `model/select`; the banner and `/models` picker reflect the
active entry returned by `model/list`. A switch affects the next turn and is
reset when the app-server restarts. Those remaining UI surfaces are separate
product decisions. The goal is a credible default local console while
preserving the app-server RPC boundary.
