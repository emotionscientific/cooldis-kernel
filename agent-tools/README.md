# Agent tools

Standalone implementations of the model-facing file tools: `read`, `write`,
`edit`, `glob`, `grep`. Each crate compiles on its own and exports both the
tool logic and its model-facing contract (name, description, JSON schema,
effect class), so the surface the model sees ships with the implementation.

Rules for every crate in this folder:

- **No direct `std::fs` in tool logic.** All filesystem access goes through
  the `ToolFs` trait in `tool-core`. Backends (real filesystem, verlet-vfs,
  wasm ABI imports) are supplied by the embedder. This is what keeps the
  native-vs-wasm packaging decision open: the tool cores compile to
  `wasm32-unknown-unknown` unchanged.
- **Assume the lossless spill coupling exists.** Tools do not implement
  clever truncation or continuation messaging. Oversized output is the
  system's job: the runtime spills the full payload and hands the model an
  addressable reference. Tools enforce only a dumb per-result byte cap
  (`tool_core::MAX_RESULT_BYTES`) to protect the record and the wire.
- **Deterministic.** Same inputs + same filesystem state = same output,
  byte for byte, on every backend. No wall clock, no randomness, no
  environment reads.
- **Contract next to code.** `contract()` in each crate is the single source
  of the tool's name, description, and input schema. Manifests may attach a
  tool under a different name (e.g. `glob` attached as `find` for Pi
  parity), but the schema and semantics come from here.

Semantics are ported from Pi (`reference/pi-mono`, coding-agent tools) with
the deviations recorded in each crate's doc comments. The `bash` sixth tool
is not here: vbash is its own subsystem. Image viewing is deliberately not
part of `read`; it will be a separate optional package.
