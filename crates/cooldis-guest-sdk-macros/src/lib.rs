//! Attribute macros for Cooldis guest authoring.
//!
//! These attributes are the compatibility boundary described in ADR 0002
//! (`docs/adr/0002-guest-encoding-v1-component-model-later.md`): the
//! user-facing contract is a plain typed Rust function, and the wire
//! encoding (today JSON over linear memory, later the component model) is a
//! private detail of the expansion. Guest source must never hand-write the
//! raw `alloc`/`dealloc`/`invoke` exports.
//!
//! Expansion is implemented in ticket 0063. Until then, applying either
//! attribute is a compile error at the use site so an unfinished macro can
//! never silently produce a guest with no exports.

use proc_macro::TokenStream;

/// Marks a function as a Cooldis operation guest entry point.
///
/// Contract (frozen; see ADR 0002):
///
/// ```ignore
/// #[operation]
/// fn search(input: SearchInput) -> Result<SearchOutput, GuestError> { .. }
///
/// // or, when the operation needs host powers (HTTP, events, cancellation):
/// #[operation]
/// fn fetch(ctx: &mut OperationContext, input: FetchInput) -> Result<FetchOutput, GuestError> { .. }
/// ```
///
/// `input` deserializes from the operation's JSON input; the `Ok` value
/// serializes to its JSON output; `GuestError` maps to the error envelope.
/// The expansion generates the ABI exports, the operation-manifest entry
/// wiring, and the envelope encode/decode.
#[proc_macro_attribute]
pub fn operation(_attr: TokenStream, item: TokenStream) -> TokenStream {
    unimplemented_stub("operation", item)
}

/// Marks a function as a Cooldis coupling guest entry point.
///
/// Contract (frozen; see ADR 0002):
///
/// ```ignore
/// #[coupling]
/// fn fold(ctx: CouplingContext) -> Result<Discharge, GuestError> { .. }
/// ```
///
/// `CouplingContext` gives typed access to the trigger event, selected
/// source events, and manifest config
/// (`cooldis.coupling.invocation/0.1`); `Discharge` builds the proposed
/// events of `cooldis.coupling.discharge/0.1`. Couplings are pure compute:
/// the expansion imports no effectful host powers.
#[proc_macro_attribute]
pub fn coupling(_attr: TokenStream, item: TokenStream) -> TokenStream {
    unimplemented_stub("coupling", item)
}

fn unimplemented_stub(name: &str, item: TokenStream) -> TokenStream {
    let mut out: TokenStream = format!(
        "compile_error!(\"#[{name}] expansion is not implemented yet (ticket 0063)\");"
    )
    .parse()
    .expect("stub compile_error tokens parse");
    out.extend(item);
    out
}
