const FOLD_COUNTER_ID: u32 = 1;

#[unsafe(no_mangle)]
pub extern "C" fn __verlet_describe_module__(sink: u32) -> i32 {
    let manifest =
        verlet_guest_sdk::OperationManifest::new(vec![verlet_guest_sdk::OperationDefinition {
            id: FOLD_COUNTER_ID,
            name: "fold_counter".to_string(),
            input: verlet_guest_sdk::OperationValueKind::Json,
            output: verlet_guest_sdk::OperationValueKind::Json,
            events: verlet_guest_sdk::OperationEventKind::None,
            mode: verlet_guest_sdk::OperationMode::Sync,
            required_capabilities: Vec::new(),
        }]);
    let bytes = match manifest.to_json_vec() {
        Ok(bytes) => bytes,
        Err(_) => return verlet_guest_sdk::STATUS_INVALID_ARGUMENT,
    };
    status(verlet_guest_sdk::write_sink(verlet_guest_sdk::Sink(sink), &bytes).map(|_| ()))
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
        FOLD_COUNTER_ID => status(fold_counter(
            verlet_guest_sdk::Source(source),
            verlet_guest_sdk::Sink(output),
        )),
        _ => verlet_guest_sdk::STATUS_NOT_FOUND,
    }
}

fn fold_counter(
    source: verlet_guest_sdk::Source,
    output: verlet_guest_sdk::Sink,
) -> Result<(), verlet_guest_sdk::StatusCode> {
    let invocation = verlet_guest_sdk::read_coupling_invocation(source)?;
    if invocation.abi != verlet_guest_sdk::COUPLING_INVOCATION_ABI {
        return Err(verlet_guest_sdk::StatusCode::InvalidArgument);
    }
    let every = invocation
        .config
        .get("every")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(3)
        .max(1);
    let sink_stream = invocation
        .config
        .get("sink_stream")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("derived:counter");
    let sink_kind = invocation
        .config
        .get("sink_kind")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("placement.decision");
    let count = invocation.selected_events.len() as u64;
    let events = if count > 0 && count % every == 0 {
        vec![verlet_guest_sdk::CouplingDischargeEvent {
            stream: sink_stream.to_string(),
            kind: sink_kind.to_string(),
            payload: serde_json::json!({
                "schema": "cooldis.example.counter_fold/1",
                "count": count,
                "trigger_event_id": invocation.trigger_event.id,
                "coupling_id": invocation.invocation_meta.coupling_id,
            }),
            provenance: None,
        }]
    } else {
        Vec::new()
    };
    verlet_guest_sdk::write_coupling_discharge(
        output,
        &verlet_guest_sdk::CouplingDischarge::new(events),
    )
    .map(|_| ())
}

fn status(result: Result<(), verlet_guest_sdk::StatusCode>) -> i32 {
    match result {
        Ok(()) => verlet_guest_sdk::STATUS_OK,
        Err(err) => err.as_raw(),
    }
}
