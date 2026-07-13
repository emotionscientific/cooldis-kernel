# Command Contracts

Command contracts are the Unix-shaped projection of Cooldis operations. They
exist so humans, coding agents, model agents, virtual bash, MCP shims, and future
HTTP/API projections can share one boring, inspectable contract.

The standard is:

```text
A Cooldis command behaves like a boring Unix command unless it declares
otherwise.

argv + stdin + explicit env + cwd
-> stdout + stderr + exit status + declared effects
```

The ABI operation contract remains the source of truth. A command contract
changes syntax, not authority. It may make an operation easier to call from a
shell, but it may not widen capabilities, hide effects, change output semantics,
or invent ambient host access.

## Design Laws

1. **Stdout is compositional data.** Anything intended to be piped, redirected,
   parsed, or compared belongs on stdout.
2. **Stderr is diagnostics and events.** Progress, warnings, structured event
   JSONL, and human diagnostics go to stderr.
3. **Exit status controls flow.** `if command`, `command && next`, `command ||
   recover`, `set -e`, and loops must have stable meaning.
4. **Inputs are declared.** Positional args, flags, stdin, env, cwd, files,
   resources, and secrets must be visible in the contract.
5. **Effects are declared.** Durable writes, network requests, secret use, VFS
   mutations, subprocesses, and remote calls must be represented as required
   capabilities or effect ports.
6. **Batching is not the default.** Prefer shell repetition, pipes, JSONL, or
   file inputs unless atomicity, transactionality, or performance requires a
   batch object.
7. **Text and JSON are explicit modes.** Human text is allowed. Machine output
   should have `--json`, JSONL, or schema-backed stdout.
8. **No ambient Unix.** Full POSIX is not implied. Cooldis supports a
   process-shaped subset: argv, env, cwd, stdin, stdout, stderr, exit status,
   cancellation, and scoped VFS.

## Oversized Tool Output

The bash and process tool projections plan stdout and stderr independently. A
stream at or below its configured byte ceiling remains inline. An oversized
stream writes its retained raw bytes to the thread VFS at
`/spill/<call-id>.<stream>.txt`, where `<stream>` is `stdout` or `stderr`. The
tool result keeps the truncation flag, replaces the inline stream with a 16 KiB
head preview and retrieval pointer, and adds a typed receipt containing `path`,
`total_bytes`, `preview_bytes`, and an additive `retention_truncated` flag.
Capture retains at most 64 MiB per stream. If a source exceeds that retention
ceiling, the spill contains the retained 64 MiB prefix and both its pointer text
and receipt state that the source was retention-truncated.
The default in-memory `/spill` root is capped at 128 MiB, enough for one
retention-sized stdout and stderr pair. Further writes degrade to the emergency
stub rather than growing thread memory without bound.

Use ordinary virtual bash to retrieve the artifact. `cat` can redirect the
complete file, while `head -c` and `sed` can read bounded ranges without
repeating an oversized result:

```sh
cat /spill/<call-id>.stdout.txt > /workspace/full-output.txt
head -c 4096 /spill/<call-id>.stdout.txt
sed -n '200,260p' /spill/<call-id>.stderr.txt
```

`/spill` is reserved from manifest workspace mounts. Spill files have the same
session lifecycle as the thread VFS and require no host cleanup. If the VFS
write is unavailable, the tool call still completes with a labeled, bounded
head-and-tail emergency stub instead of silently discarding the whole stream.

## Contract Fields

A complete command contract should be able to generate `--help`, `man`, model
tool descriptions, MCP compatibility descriptions, and JSON reference output.

```yaml
name: data profile
operation: data.csv_profile
summary: Profile CSV text.
stability: experimental

usage:
  - data profile [--json] [--has-header] [FILE]
  - data profile --stdin --json

arguments:
  - name: file
    position: 1
    required: false
    kind: virtual_path
    meaning: CSV file to read from scoped VFS.

flags:
  - name: --stdin
    kind: boolean
    meaning: Read CSV text from stdin.
  - name: --json
    kind: boolean
    meaning: Emit schema-backed JSON to stdout.
  - name: --has-header
    kind: boolean
    meaning: Treat first row as a header row.

stdin:
  mode: optional
  format: text/csv
  meaning: CSV bytes when --stdin is set or no file is supplied.

stdout:
  default_format: text
  json_format: application/json
  schema: schemas/csv_profile.output.json

stderr:
  format: text or application/jsonl events
  meaning: Diagnostics, progress, warnings, and non-compositional events.

exit_status:
  0: success
  1: runtime failure
  2: usage error
  3: capability denied
  4: validation error
  5: upstream or transport error
  6: timeout
  7: cancelled

capabilities: []
effects: []
examples:
  - command: data profile --stdin --json < people.csv
```

V1 package manifests only store a small subset of this shape. The standard is
the target for `ToolCommandContract`, generated manuals, and package validation.

## Exit Status Taxonomy

Use stable categories so bash control flow is reliable:

| Code | Meaning |
| --- | --- |
| `0` | Success. Stdout contains the promised data. |
| `1` | Runtime failure that is not more specifically classified. |
| `2` | Usage error: bad flags, missing args, malformed invocation. |
| `3` | Capability denied or grant missing. |
| `4` | Input validation failed after parsing. |
| `5` | Upstream, transport, or dependency failure. |
| `6` | Timeout. |
| `7` | Cancelled by caller/runtime. |

Commands may define more specific nonzero codes, but these meanings should stay
reserved across official Cooldis surfaces.

## Bash Control Flow

Official command projections must work in normal shell control flow:

```sh
if data profile --stdin --json < people.csv > profile.json; then
  jq '.columns | length' profile.json
else
  echo "profile failed" >&2
fi
```

Pipes and redirection should compose when the declared formats match:

```sh
cat people.csv | data profile --stdin --json | jq '.row_count'
```

Looping should replace fake batch APIs unless the batch has real semantics:

```sh
while read -r query; do
  search search "$query" --json
done < queries.txt
```

## Manual Projection

The same contract should project into:

```text
cooldis <command> --help                 short human help
cooldis tool manual <published-tool> [operation] structured human/agent reference
cooldis tool manual <published-tool> --json      token-efficient machine reference
virtual bash: man <command>              thread-visible live command contract
ToolDefinition.description               compact model-facing description
MCP description/schema                   compatibility projection
```

Manual text can clarify behavior, but cannot grant authority or contradict the
ABI contract. Capability and effect sections should be generated from accepted
contracts where possible.

The V0 implementation stores an operation manual in each accepted
`ToolInterfaceContract` produced by `cooldis tool build --package`. If a package
omits the caller-facing description or fixtures, build emits warnings and
generates a fallback manual from the ABI projection, schemas, required
capabilities, command binding, and fixtures that do exist.

Virtual bash `man <command>` is intentionally narrower: it reads the live
operation projection mounted in that thread and reports usage, stdin/stdout
kinds, required capabilities, and exit-status semantics. Operator envelope
details such as source path, registry root, transport, and secret refs belong in
tool config/show surfaces, not in `man`.
