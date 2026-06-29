use cooldis_guest_sdk::{
    OperationDefinition, OperationEventKind, OperationManifest, OperationMode, OperationValueKind,
    STATUS_INVALID_ARGUMENT, STATUS_NOT_FOUND, STATUS_OK, Sink, Source, StatusCode, read_source,
    write_sink,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const JSON_QUERY_ID: u32 = 1;

#[unsafe(no_mangle)]
pub extern "C" fn __cooldis_describe_module__(sink: u32) -> i32 {
    let manifest = OperationManifest::new(vec![OperationDefinition {
        id: JSON_QUERY_ID,
        name: "json_query".to_string(),
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
        JSON_QUERY_ID => status(json_query(Source(source), Sink(output))),
        _ => STATUS_NOT_FOUND,
    }
}

#[derive(Deserialize)]
struct JsonQueryInput {
    json: Value,
    pointer: String,
}

#[derive(Serialize)]
struct JsonQueryOutput {
    found: bool,
    value: Value,
}

fn json_query(source: Source, output: Sink) -> Result<(), StatusCode> {
    let input: JsonQueryInput = serde_json::from_slice(&read_all_source(source)?)
        .map_err(|_| StatusCode::InvalidArgument)?;
    if !input.pointer.is_empty() && !input.pointer.starts_with('/') {
        return Err(StatusCode::InvalidArgument);
    }
    let output_value = match input.json.pointer(&input.pointer) {
        Some(value) => JsonQueryOutput {
            found: true,
            value: value.clone(),
        },
        None => JsonQueryOutput {
            found: false,
            value: Value::Null,
        },
    };
    write_json(output, &output_value)
}

fn read_all_source(source: Source) -> Result<Vec<u8>, StatusCode> {
    let mut output = Vec::new();
    let mut buffer = [0u8; 4096];
    loop {
        let n = read_source(source, &mut buffer)?;
        if n == 0 {
            break;
        }
        output.extend_from_slice(&buffer[..n]);
        if n < buffer.len() {
            break;
        }
    }
    Ok(output)
}

fn write_json(output: Sink, value: &impl Serialize) -> Result<(), StatusCode> {
    let bytes = serde_json::to_vec(value).map_err(|_| StatusCode::InvalidArgument)?;
    write_sink(output, &bytes)?;
    Ok(())
}

fn status(result: Result<(), StatusCode>) -> i32 {
    match result {
        Ok(()) => STATUS_OK,
        Err(err) => err.as_raw(),
    }
}
