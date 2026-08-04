use verlet_guest_sdk::{
    EventSink, HttpRequest, HttpResponse, HttpResponseSources, Invocation, OperationDefinition,
    OperationEventKind, OperationManifest, OperationMode, OperationValueKind,
    STATUS_INVALID_ARGUMENT, STATUS_NOT_FOUND, STATUS_OK, Sink, Source, StatusCode, http_request,
    read_source, write_sink,
};
use serde::{Deserialize, Serialize};

const LOOKUP_ID: u32 = 1;

#[unsafe(no_mangle)]
pub extern "C" fn __verlet_describe_module__(sink: u32) -> i32 {
    let manifest = OperationManifest::new(vec![OperationDefinition {
        id: LOOKUP_ID,
        name: "lookup".to_string(),
        input: OperationValueKind::Json,
        output: OperationValueKind::Json,
        events: OperationEventKind::Jsonl,
        mode: OperationMode::Sync,
        required_capabilities: vec!["net.http.private".to_string()],
    }]);
    let bytes = match manifest.to_json_vec() {
        Ok(bytes) => bytes,
        Err(_) => return STATUS_INVALID_ARGUMENT,
    };
    status(write_sink(Sink(sink), &bytes).map(|_| ()))
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
            Invocation(invocation),
            Source(source),
            Sink(output),
            EventSink(events),
        )),
        _ => STATUS_NOT_FOUND,
    }
}

#[derive(Deserialize)]
struct EmployeeLookupInput {
    base_url: String,
    employee_id: String,
}

#[derive(Deserialize)]
struct EmployeeServiceRecord {
    employee_id: String,
    name: String,
    department: String,
}

#[derive(Serialize)]
struct EmployeeLookupOutput {
    employee_id: String,
    name: String,
    department: String,
    source_status: u16,
}

fn lookup_employee(
    invocation: Invocation,
    source: Source,
    output: Sink,
    events: EventSink,
) -> Result<(), StatusCode> {
    let input: EmployeeLookupInput =
        serde_json::from_slice(&read_all_source(source)?).map_err(|_| StatusCode::InvalidArgument)?;
    if input.base_url.trim().is_empty() || input.employee_id.trim().is_empty() {
        return Err(StatusCode::InvalidArgument);
    }

    let url = format!(
        "{}/employee/{}",
        input.base_url.trim_end_matches('/'),
        input.employee_id
    );
    let request = HttpRequest::new("GET", url)
        .header("accept", "application/json")
        .timeout_ms(1000)
        .max_response_bytes(4096)
        .to_json_vec()
        .map_err(|_| StatusCode::InvalidArgument)?;
    let HttpResponseSources { metadata, body } =
        http_request(invocation, &request, &[], events)?;
    let response: HttpResponse = serde_json::from_slice(&read_all_source(metadata)?)
        .map_err(|_| StatusCode::TransportError)?;
    if response.status != 200 {
        return Err(StatusCode::TransportError);
    }
    let record: EmployeeServiceRecord = serde_json::from_slice(&read_all_source(body)?)
        .map_err(|_| StatusCode::TransportError)?;
    let output_value = EmployeeLookupOutput {
        employee_id: record.employee_id,
        name: record.name,
        department: record.department,
        source_status: response.status,
    };
    write_json(output, &output_value)
}

fn read_all_source(source: Source) -> Result<Vec<u8>, StatusCode> {
    let mut output = Vec::new();
    let mut buffer = [0u8; 1024];
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
