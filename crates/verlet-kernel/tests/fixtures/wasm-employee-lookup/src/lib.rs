const LOOKUP_ID: u32 = 1;

#[unsafe(no_mangle)]
pub extern "C" fn __verlet_describe_module__(sink: u32) -> i32 {
    let manifest =
        verlet_guest_sdk::OperationManifest::new(vec![verlet_guest_sdk::OperationDefinition {
            id: LOOKUP_ID,
            name: "lookup".to_string(),
            input: verlet_guest_sdk::OperationValueKind::Json,
            output: verlet_guest_sdk::OperationValueKind::Json,
            events: verlet_guest_sdk::OperationEventKind::Jsonl,
            mode: verlet_guest_sdk::OperationMode::Sync,
            required_capabilities: vec!["net.http.private".to_string()],
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
    invocation: u32,
    source: u32,
    output: u32,
    events: u32,
) -> i32 {
    match operation {
        LOOKUP_ID => status(lookup_employee(
            verlet_guest_sdk::Invocation(invocation),
            verlet_guest_sdk::Source(source),
            verlet_guest_sdk::Sink(output),
            verlet_guest_sdk::EventSink(events),
        )),
        _ => verlet_guest_sdk::STATUS_NOT_FOUND,
    }
}

#[derive(serde::Deserialize)]
struct EmployeeLookupInput {
    base_url: String,
    employee_id: String,
}

#[derive(serde::Deserialize)]
struct EmployeeServiceRecord {
    employee_id: String,
    name: String,
    department: String,
}

#[derive(serde::Serialize)]
struct EmployeeLookupOutput {
    employee_id: String,
    name: String,
    department: String,
    source_status: u16,
}

fn lookup_employee(
    invocation: verlet_guest_sdk::Invocation,
    source: verlet_guest_sdk::Source,
    output: verlet_guest_sdk::Sink,
    events: verlet_guest_sdk::EventSink,
) -> Result<(), verlet_guest_sdk::StatusCode> {
    let input: EmployeeLookupInput = serde_json::from_slice(&read_all_source(source)?)
        .map_err(|_| verlet_guest_sdk::StatusCode::InvalidArgument)?;
    if input.base_url.trim().is_empty() || input.employee_id.trim().is_empty() {
        return Err(verlet_guest_sdk::StatusCode::InvalidArgument);
    }

    let url = format!(
        "{}/employee/{}",
        input.base_url.trim_end_matches('/'),
        input.employee_id
    );
    let request = verlet_guest_sdk::HttpRequest::new("GET", url)
        .header("accept", "application/json")
        .timeout_ms(1000)
        .max_response_bytes(4096)
        .to_json_vec()
        .map_err(|_| verlet_guest_sdk::StatusCode::InvalidArgument)?;
    let verlet_guest_sdk::HttpResponseSources { metadata, body } =
        verlet_guest_sdk::http_request(invocation, &request, &[], events)?;
    let response: verlet_guest_sdk::HttpResponse =
        serde_json::from_slice(&read_all_source(metadata)?)
            .map_err(|_| verlet_guest_sdk::StatusCode::TransportError)?;
    if response.status != 200 {
        return Err(verlet_guest_sdk::StatusCode::TransportError);
    }
    let record: EmployeeServiceRecord = serde_json::from_slice(&read_all_source(body)?)
        .map_err(|_| verlet_guest_sdk::StatusCode::TransportError)?;
    let output_value = EmployeeLookupOutput {
        employee_id: record.employee_id,
        name: record.name,
        department: record.department,
        source_status: response.status,
    };
    write_json(output, &output_value)
}

fn read_all_source(
    source: verlet_guest_sdk::Source,
) -> Result<Vec<u8>, verlet_guest_sdk::StatusCode> {
    let mut output = Vec::new();
    let mut buffer = [0u8; 1024];
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
