# Wasm Counter Coupling

Minimal custom coupling guest for the `cooldis.coupling.invocation/0.1` ABI.

The `fold_counter` operation reads a `CouplingInvocation`, counts the selected
source events, and proposes one derived discharge every `config.every` matching
events. The kernel still owns sink validation, provenance stamping, depth, and
budget enforcement.

Build:

```sh
cargo build --release --target wasm32-unknown-unknown --manifest-path examples/wasm-counter-coupling/Cargo.toml
```
