# Trace A/B harness

`verlet-trace-ab` runs the same prompt against pinned Pi 0.70.2 and a running
Verlet app server, normalizes both traces to `cooldis.trace.common/1` JSONL,
and writes a terminal-friendly side-by-side diff. The converters and diff are
offline; only `run` invokes `npx` and model providers.

## Run an A/B

Start a Verlet daemon whose provider and coding-agent manifest point at the
same provider route and model that Pi will use. The manifest must declare an
`rw` workspace at `/work` (or its chosen guest path) and allow the
`max_tool_rounds` runtime override.

```sh
cargo run -p verlet-trace-ab -- run \
  --prompt-file /tmp/task.txt \
  --workspace /absolute/path/to/seed-workspace \
  --output /tmp/trace-ab-run \
  --provider shared-provider \
  --model shared-model \
  --verlet-agent-ref agent://coding/trace-ab@latest \
  --verlet-url ws://127.0.0.1:49200/rpc \
  --max-tool-rounds 64
```

`--provider` and `--model` are passed unchanged to both Pi and Verlet. Configure
Pi's model registry and the Verlet agent profile so those names resolve to the
same endpoint. The runner rejects an existing non-empty output directory,
copies the seed into separate `pi-workspace` and `verlet-workspace` trees, and
leaves the original untouched. Copies omit `.git`, preserve file modes and
workspace-internal symlinks, rewrite absolute internal symlinks to the clone,
and reject links that escape the seed workspace.

The output directory contains:

- `pi.session.jsonl`, `pi.rpc.jsonl`, and `pi.common.jsonl`;
- `verlet.export.json`, `verlet.rpc.jsonl`, and `verlet.common.jsonl`;
- `diff.txt`, plus stderr logs and both resulting workspaces.

Both sides are attempted even when one fails. The command exits nonzero after
writing every artifact it could recover, including a partial Pi session or a
Verlet export collected after a failed turn.

Pi is launched exactly as
`npx --yes @mariozechner/pi-coding-agent@0.70.2 --mode rpc`; the harness sends
LF-delimited `prompt` and `get_state` commands. Verlet uses the existing
`debug rpc call thread/start`, `debug rpc turn`, and
`debug rpc call thread/debug/export` paths. No daemon, runtime, or store API is
added by this tool.

## Convert or diff offline

```sh
cargo run -p verlet-trace-ab -- convert-pi \
  --input session.jsonl --output pi.common.jsonl

cargo run -p verlet-trace-ab -- convert-verlet \
  --input verlet.export.json --output verlet.common.jsonl

cargo run -p verlet-trace-ab -- diff \
  --pi pi.common.jsonl --verlet verlet.common.jsonl \
  --output diff.txt
```

Common-form lines include source metadata, assistant messages, tool calls, tool
results, turn boundaries, compactions, and explicit `unmapped` records. Tokens
and latency are populated only when the source has them. Raw Pi entries and
Verlet event envelopes remain under `details`; Verlet thread projection,
stream metadata, provenance, context-compile boundaries, redaction metadata,
and export receipt indexes are retained there as well. The summary's `unmapped`
count makes future or unsupported source shapes visible.

## Read the diff

Rows align first by turn and then by model/tool round. `CALL` is an ordinary
tool invocation, `FAIL` is a failed result, and `RETRY` is the next edit-class
tool call in that turn after a failed `edit` or `apply_patch`. The summary shows
turns, rounds, calls by tool, total tokens, trace wall time, edit failures, edit
retries, and unmapped records. Missing timing is shown as `n/a`, not zero. The
retry marker is deliberately structural: it does not infer whether two patches
are semantically identical.

The Pi fixture in `tests/fixtures/pi-session.jsonl` is a content-sanitized
excerpt of a real persisted Pi session with a failed edit and successful retry.
The Verlet fixture follows the checked-in `cooldis.debug.thread_export/1`
contract and durable event payloads.
