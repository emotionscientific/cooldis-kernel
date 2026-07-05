use cooldis_guest_sdk::{
    COUPLING_INVOCATION_ABI, CouplingDischarge, CouplingDischargeEvent, OperationDefinition,
    OperationEventKind, OperationManifest, OperationMode, OperationValueKind,
    STATUS_INVALID_ARGUMENT, STATUS_NOT_FOUND, STATUS_OK, Sink, Source, StatusCode,
    read_coupling_invocation, write_coupling_discharge, write_sink,
};
use serde_json::json;

const FOLD_COUNTER_ID: u32 = 1;

#[unsafe(no_mangle)]
pub extern "C" fn __cooldis_describe_module__(sink: u32) -> i32 {
    let manifest = OperationManifest::new(vec![OperationDefinition {
        id: FOLD_COUNTER_ID,
        name: "fold_counter".to_string(),
        input: OperationValueKind::Json,
        output: OperationValueKind::Json,
        events: OperationEventKind::None,
        mode: OperationMode::Sync,
        required_capabilities: Vec::new(),
    }]);
    let bytes = match manifest.to_json_vec() {
        Ok(bytes) => bytes,
        Err(_) => return STATUS_INVALID_ARGUMENT,
    };
    status(write_sink(Sink(sink), &bytes).map(|_| ()))
}

#[unsafe(no_mangle)]
pub extern "C" fn __cooldis_call_operation__(
    operation: u32,
    _invocation: u32,
    source: u32,
    output: u32,
    _events: u32,
) -> i32 {
    match operation {
        FOLD_COUNTER_ID => status(fold_counter(Source(source), Sink(output))),
        _ => STATUS_NOT_FOUND,
    }
}

fn fold_counter(source: Source, output: Sink) -> Result<(), StatusCode> {
    let invocation = read_coupling_invocation(source)?;
    if invocation.abi != COUPLING_INVOCATION_ABI {
        return Err(StatusCode::InvalidArgument);
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
        vec![CouplingDischargeEvent {
            stream: sink_stream.to_string(),
            kind: sink_kind.to_string(),
            payload: json!({
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
    write_coupling_discharge(output, &CouplingDischarge::new(events)).map(|_| ())
}

fn status(result: Result<(), StatusCode>) -> i32 {
    match result {
        Ok(()) => STATUS_OK,
        Err(err) => err.as_raw(),
    }
}
