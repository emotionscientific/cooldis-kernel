# Cooldis Chat

`cooldis chat` is the bundled local terminal console for operating a Cooldis
app-server session. It is intentionally an RPC client over the app-server
boundary, not a privileged runtime path. By default it launches a private local
app-server; with `--attach` it connects to an existing endpoint.

```text
cooldis chat [PROMPT] [--config <file>] [--cwd <path>]
cooldis chat [PROMPT] --attach <unix://path|ws://host:port[/rpc]>
cooldis chat [PROMPT] --provider <provider> [--model <model>] ...
```

## Included Surface

- Transcript pane with compact rows for user, assistant, system/status,
  lifecycle, thinking summary, and error output.
- Multiline composer with paste, cursor movement, backspace/delete, Enter
  submit, and modified Enter newline handling where the terminal reports it.
- Status line with connection mode, cwd, model/provider, thread id/name, and
  turn state.
- Basic semantic colors for user, assistant, system, errors, thinking, and
  muted metadata. `NO_COLOR=1` disables semantic color.
- Slash commands:
  `/help`, `/quit`, `/q`, `/interrupt`, `/clear`, `/status`, `/new`,
  `/sessions`, `/resume <thread-id>`, `/rename <name>`, `/fork`, `/compact`,
  and `/models`.

## Known Limits

The V1 chat console deliberately does not implement shell escape or
`command/exec`, full themes, mouse support, file mentions, autocomplete,
external editor handoff, export/copy, OpenTUI/web frontend mode, or rich tool
detail panels. Those remain separate product decisions. The current goal is a
credible default local console while preserving the app-server RPC boundary so
other frontends can replace it.
