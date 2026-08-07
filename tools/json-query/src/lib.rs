const JSON_QUERY_ID: u32 = 1;

#[unsafe(no_mangle)]
pub extern "C" fn __verlet_describe_module__(sink: u32) -> i32 {
    let manifest =
        verlet_guest_sdk::OperationManifest::new(vec![verlet_guest_sdk::OperationDefinition {
            id: JSON_QUERY_ID,
            name: "json_query".to_string(),
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
        JSON_QUERY_ID => status(json_query(
            verlet_guest_sdk::Source(source),
            verlet_guest_sdk::Sink(output),
        )),
        _ => verlet_guest_sdk::STATUS_NOT_FOUND,
    }
}

#[derive(serde::Deserialize)]
struct JsonQueryInput {
    json: serde_json::Value,
    pointer: String,
}

#[derive(serde::Serialize)]
struct JsonQueryOutput {
    found: bool,
    value: serde_json::Value,
}

fn json_query(
    source: verlet_guest_sdk::Source,
    output: verlet_guest_sdk::Sink,
) -> Result<(), verlet_guest_sdk::StatusCode> {
    let input: JsonQueryInput = serde_json::from_slice(&read_all_source(source)?)
        .map_err(|_| verlet_guest_sdk::StatusCode::InvalidArgument)?;
    if !input.pointer.is_empty() && !input.pointer.starts_with('/') {
        return Err(verlet_guest_sdk::StatusCode::InvalidArgument);
    }
    let output_value = match input.json.pointer(&input.pointer) {
        Some(value) => JsonQueryOutput {
            found: true,
            value: value.clone(),
        },
        None => JsonQueryOutput {
            found: false,
            value: serde_json::Value::Null,
        },
    };
    write_json(output, &output_value)
}

fn read_all_source(
    source: verlet_guest_sdk::Source,
) -> Result<Vec<u8>, verlet_guest_sdk::StatusCode> {
    let mut output = Vec::new();
    let mut buffer = [0u8; 4096];
    loop {
        let n = verlet_guest_sdk::read_source(source, &mut buffer)?;
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

fn write_json(
    output: verlet_guest_sdk::Sink,
    value: &impl serde::Serialize,
) -> Result<(), verlet_guest_sdk::StatusCode> {
    let bytes =
        serde_json::to_vec(value).map_err(|_| verlet_guest_sdk::StatusCode::InvalidArgument)?;
    verlet_guest_sdk::write_sink(output, &bytes)?;
    Ok(())
}

fn status(result: Result<(), verlet_guest_sdk::StatusCode>) -> i32 {
    match result {
        Ok(()) => verlet_guest_sdk::STATUS_OK,
        Err(err) => err.as_raw(),
    }
}
