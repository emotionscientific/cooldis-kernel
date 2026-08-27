# Agent tools

Standalone implementations of the model-facing file tools: `read`, `write`,
`edit`, `find`, `grep`. The `find` implementation remains in the
`tool-glob` crate. Each crate compiles on its own and exports both the
tool logic and its model-facing contract (name, description, JSON schema,
effect class), so the surface the model sees ships with the implementation.

Rules for every crate in this folder:

- **No direct `std::fs` in tool logic.** All filesystem access goes through
  the `ToolFs` trait in `tool-core`. Backends (real filesystem, verlet-vfs,
  wasm ABI imports) are supplied by the embedder. This is what keeps the
  native-vs-wasm packaging decision open: the tool cores compile to
  `wasm32-unknown-unknown` unchanged.
- **Match Pi's addressable truncation.** `read` head-truncates at 2,000 lines
  or 50 KiB; `grep` and `find` head-truncate at 50 KiB. Their exact notices
  tell the model to continue with `offset`, raise the limit, or refine the
  query. `tool_core::MAX_RESULT_BYTES` remains a 4 MiB record/wire backstop.
- **Deterministic.** Same inputs + same filesystem state = same output,
  byte for byte, on every backend. No wall clock, no randomness, no
  environment reads.
- **Contract next to code.** `contract()` in each crate is the single source
  of the tool's name, Pi-compatible description, and input schema. The
  `tool-glob` crate's model-facing contract is named `find`.

Semantics are ported from Pi (`reference/pi-mono`, coding-agent tools) with
the deviations recorded in each crate's doc comments. The `bash` sixth tool
is not here: vbash is its own subsystem. Image viewing is deliberately not
part of `read`; it will be a separate optional package.

Paths apply only Pi's authority-preserving normalization inside the granted
root: one leading `@` is stripped and Unicode space variants become ASCII
spaces. Tilde and file URLs that cannot resolve as literal paths inside the
root are treated as not found; they never introduce ambient home or URL
authority.

## Wasm operation lane

`wasm/read`, `wasm/write`, `wasm/edit`, and `wasm/search` package the five
tool cores as Verlet operation ABI modules. The search module exposes both
`find` and `grep`. Every operation accepts the same JSON object as the native
CLI harness:

```json
{"root":"/workspace","args":{"path":"notes.txt"}}
```

The Pi package manifests are surface-declared: direct model tool rows derive
their input schema from the nested `args` schema, while the host binds `root`
to the guest `/workspace` path at attach time. The default manifest declares
that read-write mount without a host path; the app-server bind layer supplies
its configured cwd as the witnessed host directory. Those schemas reject
unknown model arguments, and the same operations are withheld from raw
virtual-bash operation commands. CLI and fixture calls remain envelope-shaped
because those callers act as the host.

It returns exactly one CLI envelope, either `{"ok": <tool output>}` or
`{"error": <text>}`. Tool and argument errors remain successful ABI calls so
the caller receives the Pi-compatible error text. Only source and sink
transport failures use a non-OK ABI status. Read, find, and grep need only an
attached VFS. Write and edit also declare and require the `fs.write` grant.

Known wasm-lane divergence: the guest ABI reports symlinks as kind `Other`
(neither file nor directory), so the walker skips symlinked file entries that
the native `StdFs` lane includes. Both walkers skip symlinked directories.
Unix sockets, FIFOs, block devices, and character devices are also reported as
`Other` by host-backed VFS mounts and skipped by both walkers. Direct read
rejects symlink paths that the native lane follows when the target remains
inside its root. The ABI exposes no follow-symlink distinction; resolving this
needs a host-side change, tracked separately.

Each module directory is a standalone Cargo workspace with its own lockfile,
a `cdylib` library target, and `panic = "abort"` in the release profile. This
is the crate layout accepted by `verlet tool build`. Point a tool package's
`runtime.module_path` at one of these directories, then build the package:

```sh
verlet tool build --package path/to/verlet.tool.toml
```

For a direct source build of one module, use its standalone manifest:

```sh
cargo build --manifest-path agent-tools/wasm/read/Cargo.toml \
  --target wasm32-unknown-unknown --release
```

## Designed divergences from Pi

- **Confinement.** Every filesystem operation remains inside the explicit
  `ToolFs` root, including after benign path normalization and symlink checks.
- **Determinism.** Shared traversal and result order are lexicographically
  deterministic instead of preserving `fd`/`rg` process order.
- **In-process engines.** `globset`, `grep-regex`, and `grep-searcher` replace
  spawned or downloaded `fd`/`rg` binaries.
- **4 MiB backstop.** `MAX_RESULT_BYTES` still protects structured receipts,
  edit details, the record, and the wire after Pi's smaller text truncators.
- **Cancellation layer.** Cancellation is enforced by the Verlet runtime
  boundary rather than synchronous tool-core arguments.
- **Mutation-queue deferral.** Same-path write/edit serialization belongs to
  the kernel invocation layer and is deferred until the wasm integration
  stage.
- **Bounded text scanning.** Grep skips files that are not valid UTF-8 text,
  contain NUL, exceed the shared 8 MiB per-file read limit, have a non-regular
  stat kind, or fail when read after a directory walk selected them. The last
  case covers races and backend kinds that cannot be classified before open.
  A directly named single-file grep still returns its read error. Read returns
  a structured error for files over the limit; offset/limit cannot bypass the
  ceiling because the current ABI has no seek or ranged-read operation. Find
  skips special entries and ignore-rule files that are oversized or fail while
  being read. Edit preserves whole-file atomicity under the same ceiling and
  returns a structured error without changing or silently skipping an
  oversized target. Pi delegates grep to `rg` without this size ceiling and its
  read and grep context paths can load whole files. The Wasm fuel budget is a
  runaway guard, sized from the measured guest cost of about 154 fuel per file
  byte so an 8 MiB read fits several times over and multi-file grep walks across
  tens of MiB complete while runaway guests remain bounded to seconds of CPU.
