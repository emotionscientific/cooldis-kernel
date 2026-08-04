//! Attribute macros for Verlet guest authoring.
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
use quote::{format_ident, quote};
use syn::{Error, FnArg, ItemFn, PatType, ReturnType, Type, parse_macro_input, spanned::Spanned};

/// Marks a function as a Verlet operation guest entry point.
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
pub fn operation(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    match expand_operation(attr, input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Marks a function as a Verlet coupling guest entry point.
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
pub fn coupling(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    match expand_coupling(attr, input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn expand_operation(attr: TokenStream, item: ItemFn) -> Result<proc_macro2::TokenStream, Error> {
    reject_attr("operation", attr)?;
    validate_common("operation", &item)?;
    let name = item.sig.ident.to_string();
    let ident = &item.sig.ident;
    let invoke_ident = format_ident!("__verlet_guest_sdk_invoke_{ident}");
    let operation_id = 1u32;
    let inputs = item.sig.inputs.iter().collect::<Vec<_>>();
    let (invoke_fn, call_expr, manifest_events) = match inputs.as_slice() {
        [FnArg::Typed(input)] => {
            let input_ty = &input.ty;
            (
                quote! {
                    fn #invoke_ident(
                        source: ::verlet_guest_sdk::Source,
                        output: ::verlet_guest_sdk::Sink,
                    ) -> ::core::result::Result<(), ::verlet_guest_sdk::GuestError> {
                        let input = ::verlet_guest_sdk::__private::read_json_input::<#input_ty>(source)?;
                        let output_value = #ident(input)?;
                        ::verlet_guest_sdk::__private::write_json_output(output, &output_value)
                    }
                },
                quote! {
                    #invoke_ident(
                        ::verlet_guest_sdk::Source(source),
                        ::verlet_guest_sdk::Sink(output),
                    )
                },
                quote! {},
            )
        }
        [first, FnArg::Typed(input)] if is_mut_ref_argument(first) => {
            let input_ty = &input.ty;
            (
                quote! {
                    fn #invoke_ident(
                        invocation: ::verlet_guest_sdk::Invocation,
                        source: ::verlet_guest_sdk::Source,
                        output: ::verlet_guest_sdk::Sink,
                        events: ::verlet_guest_sdk::EventSink,
                    ) -> ::core::result::Result<(), ::verlet_guest_sdk::GuestError> {
                        let input = ::verlet_guest_sdk::__private::read_json_input::<#input_ty>(source)?;
                        let mut ctx = ::verlet_guest_sdk::OperationContext::new(invocation, events);
                        let output_value = #ident(&mut ctx, input)?;
                        ::verlet_guest_sdk::__private::write_json_output(output, &output_value)
                    }
                },
                quote! {
                    #invoke_ident(
                        ::verlet_guest_sdk::Invocation(invocation),
                        ::verlet_guest_sdk::Source(source),
                        ::verlet_guest_sdk::Sink(output),
                        ::verlet_guest_sdk::EventSink(events),
                    )
                },
                quote! {.jsonl_events()},
            )
        }
        [first, _] => {
            return Err(Error::new(
                first.span(),
                "#[operation] only supports fn(input) or fn(&mut OperationContext, input)",
            ));
        }
        _ => {
            return Err(Error::new(
                item.sig.inputs.span(),
                "#[operation] only supports fn(input) or fn(&mut OperationContext, input)",
            ));
        }
    };

    Ok(quote! {
        #item

        #invoke_fn

        #[unsafe(no_mangle)]
        pub extern "C" fn __verlet_describe_module__(sink: u32) -> i32 {
            let manifest = ::verlet_guest_sdk::OperationManifest::new(vec![
                ::verlet_guest_sdk::OperationDefinition::new(#operation_id, #name)
                    .json_input()
                    .json_output()
                    #manifest_events,
            ]);
            ::verlet_guest_sdk::__private::status_from_guest_result(
                ::verlet_guest_sdk::__private::write_manifest(
                    ::verlet_guest_sdk::Sink(sink),
                    &manifest,
                ),
            )
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn __verlet_call_operation__(
            operation: u32,
            invocation: u32,
            source: u32,
            output: u32,
            events: u32,
        ) -> i32 {
            match operation {
                #operation_id => ::verlet_guest_sdk::__private::status_from_guest_result(#call_expr),
                _ => ::verlet_guest_sdk::STATUS_NOT_FOUND,
            }
        }
    })
}

fn expand_coupling(attr: TokenStream, item: ItemFn) -> Result<proc_macro2::TokenStream, Error> {
    reject_attr("coupling", attr)?;
    validate_common("coupling", &item)?;
    if item.sig.inputs.len() != 1 {
        return Err(Error::new(
            item.sig.inputs.span(),
            "#[coupling] only supports fn(CouplingContext)",
        ));
    }
    let Some(FnArg::Typed(PatType { ty, .. })) = item.sig.inputs.first() else {
        return Err(Error::new(
            item.sig.inputs.span(),
            "#[coupling] only supports fn(CouplingContext)",
        ));
    };
    let name = item.sig.ident.to_string();
    let ident = &item.sig.ident;
    let invoke_ident = format_ident!("__verlet_guest_sdk_invoke_{ident}");
    let operation_id = 1u32;

    Ok(quote! {
        #item

        fn #invoke_ident(
            source: ::verlet_guest_sdk::Source,
            output: ::verlet_guest_sdk::Sink,
        ) -> ::core::result::Result<(), ::verlet_guest_sdk::GuestError> {
            let ctx: #ty = ::verlet_guest_sdk::__private::read_coupling_context(source)?;
            let discharge = #ident(ctx)?;
            ::verlet_guest_sdk::__private::write_coupling_discharge_output(output, discharge)
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn __verlet_describe_module__(sink: u32) -> i32 {
            let manifest = ::verlet_guest_sdk::OperationManifest::new(vec![
                ::verlet_guest_sdk::OperationDefinition::new(#operation_id, #name)
                    .json_input()
                    .json_output(),
            ]);
            ::verlet_guest_sdk::__private::status_from_guest_result(
                ::verlet_guest_sdk::__private::write_manifest(
                    ::verlet_guest_sdk::Sink(sink),
                    &manifest,
                ),
            )
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn __verlet_call_operation__(
            operation: u32,
            _invocation: u32,
            source: u32,
            output: u32,
            _events: u32,
        ) -> i32 {
            match operation {
                #operation_id => ::verlet_guest_sdk::__private::status_from_guest_result(
                    #invoke_ident(
                        ::verlet_guest_sdk::Source(source),
                        ::verlet_guest_sdk::Sink(output),
                    ),
                ),
                _ => ::verlet_guest_sdk::STATUS_NOT_FOUND,
            }
        }
    })
}

fn reject_attr(name: &str, attr: TokenStream) -> Result<(), Error> {
    if attr.is_empty() {
        Ok(())
    } else {
        Err(Error::new(
            proc_macro2::Span::call_site(),
            format!("#[{name}] does not accept arguments in the frozen v1 contract"),
        ))
    }
}

fn validate_common(name: &str, item: &ItemFn) -> Result<(), Error> {
    if item.sig.constness.is_some() {
        return Err(Error::new(
            item.sig.constness.span(),
            format!("#[{name}] does not support const functions"),
        ));
    }
    if item.sig.asyncness.is_some() {
        return Err(Error::new(
            item.sig.asyncness.span(),
            format!("#[{name}] does not support async functions"),
        ));
    }
    if item.sig.unsafety.is_some() {
        return Err(Error::new(
            item.sig.unsafety.span(),
            format!("#[{name}] does not support unsafe functions"),
        ));
    }
    if !matches!(item.sig.output, ReturnType::Type(_, _)) {
        return Err(Error::new(
            item.sig.output.span(),
            format!("#[{name}] functions must return Result<_, GuestError>"),
        ));
    }
    Ok(())
}

fn is_mut_ref_argument(arg: &FnArg) -> bool {
    let FnArg::Typed(input) = arg else {
        return false;
    };
    matches!(
        input.ty.as_ref(),
        Type::Reference(reference) if reference.mutability.is_some()
    )
}
