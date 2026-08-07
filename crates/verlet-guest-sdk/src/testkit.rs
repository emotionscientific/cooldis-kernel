//! Native test harness for guest functions.
//!
//! Guest functions are plain Rust, so they run under `cargo test` on the
//! host with no Wasm build and no kernel: hand them an invocation envelope
//! and assert on the discharge. This tests the guest's logic; byte-level
//! ABI conformance of the macro expansion is covered by the fixture runs of
//! the dev kit, not here.

/// Invoke a `#[coupling]` function natively.
pub fn invoke_coupling<F>(
    f: F,
    invocation: crate::CouplingInvocation,
) -> Result<crate::CouplingDischarge, crate::contract::GuestError>
where
    F: FnOnce(
        crate::contract::CouplingContext,
    ) -> Result<crate::contract::Discharge, crate::contract::GuestError>,
{
    f(crate::contract::CouplingContext::from_invocation(
        invocation,
    ))
    .map(crate::contract::Discharge::into_coupling_discharge)
}

/// Invoke a pure `#[operation]` function natively with a JSON input value.
pub fn invoke_operation<F, In, Out>(
    f: F,
    input: serde_json::Value,
) -> Result<Out, crate::contract::GuestError>
where
    F: FnOnce(In) -> Result<Out, crate::contract::GuestError>,
    In: serde::de::DeserializeOwned,
{
    let input = serde_json::from_value(input)
        .map_err(|err| crate::contract::GuestError::BadInput(format!("operation input: {err}")))?;
    f(input)
}

/// Parse a `cooldis.coupling.invocation/0.1` fixture (the same JSON the dev
/// kit's fixture runs feed the real artifact).
pub fn invocation_from_fixture_json(
    json: &str,
) -> Result<crate::CouplingInvocation, crate::contract::GuestError> {
    let invocation =
        crate::CouplingInvocation::from_json_slice(json.as_bytes()).map_err(|err| {
            crate::contract::GuestError::BadInput(format!("fixture invocation: {err}"))
        })?;
    if invocation.abi != crate::COUPLING_INVOCATION_ABI {
        return Err(crate::contract::GuestError::BadInput(format!(
            "fixture abi {:?} is not {COUPLING_INVOCATION_ABI:?}",
            invocation.abi,
            COUPLING_INVOCATION_ABI = crate::COUPLING_INVOCATION_ABI
        )));
    }
    Ok(invocation)
}

/// Load and parse a `cooldis.coupling.invocation/0.1` fixture from disk.
pub fn invocation_from_fixture_file(
    path: impl AsRef<std::path::Path>,
) -> Result<crate::CouplingInvocation, crate::contract::GuestError> {
    let path = path.as_ref();
    let json = std::fs::read_to_string(path).map_err(|err| {
        crate::contract::GuestError::BadInput(format!(
            "fixture invocation {}: {err}",
            path.display()
        ))
    })?;
    crate::testkit::invocation_from_fixture_json(&json)
}

#[cfg(test)]
mod tests {

    fn fixture() -> String {
        serde_json::json!({
            "abi": crate::COUPLING_INVOCATION_ABI,
            "trigger_event": {
                "id": "evt-3",
                "stream_id": "conversation:t-1",
                "sequence": 3,
                "kind": "turn.completed",
                "origin": "witnessed",
                "payload": {}
            },
            "selected_events": [],
            "config": {"every": 1},
            "invocation_meta": {
                "coupling_id": "test.counter",
                "thread_id": "t-1",
                "depth": 0
            }
        })
        .to_string()
    }

    #[test]
    fn coupling_runs_natively_without_wasm_host() {
        let invocation = crate::testkit::invocation_from_fixture_json(&fixture()).unwrap();
        let discharge = crate::testkit::invoke_coupling(
            |ctx| {
                assert_eq!(ctx.trigger().kind, "turn.completed");
                assert_eq!(ctx.meta().thread_id, "t-1");
                crate::contract::Discharge::empty().event(
                    "derived:counter",
                    "placement.decision",
                    serde_json::json!({"count": 1}),
                )
            },
            invocation,
        )
        .unwrap();
        assert_eq!(discharge.events.len(), 1);
        assert_eq!(discharge.events[0].stream, "derived:counter");
    }

    #[test]
    fn fixture_abi_mismatch_is_bad_input() {
        let bad = fixture().replace("coupling.invocation/0.1", "coupling.invocation/9.9");
        assert!(matches!(
            crate::testkit::invocation_from_fixture_json(&bad),
            Err(crate::contract::GuestError::BadInput(_))
        ));
    }
}
