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
