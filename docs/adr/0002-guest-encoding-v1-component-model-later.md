# ADR 0002: Guest Encoding V1, Component Model Later

Status: accepted
Date: 2026-07-08

## Context

The V1 guest ABI is core Wasm with JSON over linear memory. Guests export
`alloc`, `dealloc`, and versioned operation entry points; the host shuttles
serialized envelopes across the boundary. `cooldis-guest-sdk` already provides
typed envelope structs and read/write helpers, but the exports and dispatch
ceremony are still hand-written, costing roughly fifty lines per operation.

The WebAssembly component model and WIT are the ecosystem's typed,
language-neutral successor to exactly this kind of hand-rolled boundary.
Anyone evaluating Cooldis will reasonably ask why the runtime is not built on
it already, and whether adopting Cooldis today means betting against where
Wasm is going.

It does not. This ADR records the plan so nobody has to guess.

## Decision

V1 keeps the core Wasm JSON-over-linear-memory encoding. The component model
is the named successor, and the migration path is designed in now rather than
discovered later:

1. **The SDK macro layer is the compatibility boundary.** The frozen,
   user-facing authoring contract is a plain typed Rust function wrapped by
   an SDK attribute, with serde-derivable input/output types and one error
   enum:

   ```rust
   use cooldis_guest_sdk::prelude::*;

   #[coupling]
   fn extract(window: TurnWindow) -> Result<MemoryFacts, CouplingError> {
       // plain Rust; natively cargo-testable, no Wasm host required
   }
   ```

   The same layer covers operation guests (`#[operation]`) and coupling
   guests (`#[coupling]`); both reuse the Wasm operation call path.

2. **The wire encoding is a private detail of the macro expansion.** Once the
   SDK lands, guest source never hand-writes `alloc`/`dealloc`/`invoke`
   exports; a guest that does is out of contract and gets no compatibility
   promise.

3. **Moving to components swaps the macro expansion and the host executor.**
   It changes no guest source, no manifest, and no recorded operation
   contract. Guest authors recompile; they do not migrate.

## Why not components now

- The expensive half is host-side: the component runtime API, resource
  tables, and canonical ABI plumbing. That work buys no new capability for
  the operations the runtime executes today.
- WASI 0.2/0.3 and async component semantics are still moving targets.
- Operation payloads are stream events and versioned JSON envelopes already;
  JSON at the boundary is not a bottleneck for V1 workloads.
- The runtime earns its keep by running real agents now. Encoding elegance
  does not outrank that.

## Migration triggers

Any one of these promotes the component migration from plan to active work:

- a real polyglot guest need (a non-Rust coupling worth shipping);
- component tooling and WASI stabilizing enough that the host executor swap
  is a bounded task;
- a typed-interface requirement the JSON envelope cannot express without
  growing a parallel schema layer.

## Consequences

- The `cooldis-guest-sdk` macro layer must land before third-party guest
  authoring is documented as supported, so no external code ever couples to
  the raw encoding.
- [ABI](../abi.md) and the [operation dev kit](../wasm-operation-dev-kit.md)
  describe the SDK contract as the authoring surface and reference this ADR
  for the encoding trajectory.
- Fixture and conformance suites exercise guests through the SDK contract,
  not the raw envelope, so they survive the swap unchanged.
