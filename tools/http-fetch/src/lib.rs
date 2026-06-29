use cooldis_guest_sdk::{
    EventSink, HttpRequest, HttpResponse, Invocation, OperationDefinition, OperationEventKind,
    OperationManifest, OperationMode, OperationValueKind, STATUS_INVALID_ARGUMENT,
    STATUS_NOT_FOUND, STATUS_OK, Sink, Source, StatusCode, http_request, read_source, write_sink,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const HTTP_FETCH_ID: u32 = 1;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 256 * 1024;

#[unsafe(no_mangle)]
pub extern "C" fn __cooldis_describe_module__(sink: u32) -> i32 {
    let manifest = OperationManifest::new(vec![OperationDefinition {
        id: HTTP_FETCH_ID,
        name: "http_fetch".to_string(),
        input: OperationValueKind::Json,
        output: OperationValueKind::Json,
        events: OperationEventKind::Jsonl,
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
    invocation: u32,
    source: u32,
    output: u32,
    events: u32,
) -> i32 {
    match operation {
        HTTP_FETCH_ID => status(http_fetch(
            Invocation(invocation),
            Source(source),
            Sink(output),
            EventSink(events),
        )),
        _ => STATUS_NOT_FOUND,
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct HttpFetchInput {
    url: String,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    timeout_ms: Option<u64>,
    max_response_bytes: Option<usize>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HttpFetchOutput {
    status: u16,
    headers: BTreeMap<String, String>,
    body_text: String,
    truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ErrorOutput>,
}

#[derive(Serialize)]
struct ErrorOutput {
    code: &'static str,
    message: &'static str,
}

fn http_fetch(
    invocation: Invocation,
    source: Source,
    output: Sink,
    events: EventSink,
) -> Result<(), StatusCode> {
    let input: HttpFetchInput = serde_json::from_slice(&read_all_source(source)?)
        .map_err(|_| StatusCode::InvalidArgument)?;
    let mut request = HttpRequest::new("GET", input.url);
    for (name, value) in input.headers {
        request = request.header(name, value);
    }
    if let Some(timeout_ms) = input.timeout_ms {
        request = request.timeout_ms(timeout_ms);
    }
    let max_response_bytes = input
        .max_response_bytes
        .unwrap_or(DEFAULT_MAX_RESPONSE_BYTES)
        .min(DEFAULT_MAX_RESPONSE_BYTES);
    request = request.max_response_bytes(max_response_bytes);
    let request_bytes = request
        .to_json_vec()
        .map_err(|_| StatusCode::InvalidArgument)?;

    let sources = match http_request(invocation, &request_bytes, &[], events) {
        Ok(sources) => sources,
        Err(err) => return write_json(output, &error_output(err)),
    };
    let metadata: HttpResponse = serde_json::from_slice(&read_all_source(sources.metadata)?)
        .map_err(|_| StatusCode::TransportError)?;
    let body = read_all_source(sources.body)?;
    let headers = metadata.headers.into_iter().collect();
    write_json(
        output,
        &HttpFetchOutput {
            status: metadata.status,
            headers,
            body_text: String::from_utf8_lossy(&body).into_owned(),
            truncated: metadata.truncated,
            error: None,
        },
    )
}

fn error_output(status: StatusCode) -> HttpFetchOutput {
    let (code, message) = match status {
        StatusCode::InvalidArgument => ("invalid_argument", "invalid HTTP fetch request"),
        StatusCode::CapabilityDenied => ("capability_denied", "HTTP request denied"),
        StatusCode::Timeout => ("timeout", "HTTP request timed out"),
        StatusCode::Cancelled => ("cancelled", "HTTP request cancelled"),
        _ => ("transport_error", "HTTP request failed"),
    };
    HttpFetchOutput {
        status: 0,
        headers: BTreeMap::new(),
        body_text: String::new(),
        truncated: false,
        error: Some(ErrorOutput { code, message }),
    }
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
