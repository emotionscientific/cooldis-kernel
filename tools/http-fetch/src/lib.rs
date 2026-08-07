const HTTP_FETCH_ID: u32 = 1;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 256 * 1024;

#[unsafe(no_mangle)]
pub extern "C" fn __verlet_describe_module__(sink: u32) -> i32 {
    let manifest =
        verlet_guest_sdk::OperationManifest::new(vec![verlet_guest_sdk::OperationDefinition {
            id: HTTP_FETCH_ID,
            name: "http_fetch".to_string(),
            input: verlet_guest_sdk::OperationValueKind::Json,
            output: verlet_guest_sdk::OperationValueKind::Json,
            events: verlet_guest_sdk::OperationEventKind::Jsonl,
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
    invocation: u32,
    source: u32,
    output: u32,
    events: u32,
) -> i32 {
    match operation {
        HTTP_FETCH_ID => status(http_fetch(
            verlet_guest_sdk::Invocation(invocation),
            verlet_guest_sdk::Source(source),
            verlet_guest_sdk::Sink(output),
            verlet_guest_sdk::EventSink(events),
        )),
        _ => verlet_guest_sdk::STATUS_NOT_FOUND,
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct HttpFetchInput {
    url: String,
    #[serde(default)]
    headers: std::collections::BTreeMap<String, String>,
    timeout_ms: Option<u64>,
    max_response_bytes: Option<usize>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct HttpFetchOutput {
    status: u16,
    headers: std::collections::BTreeMap<String, String>,
    body_text: String,
    truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ErrorOutput>,
}

#[derive(serde::Serialize)]
struct ErrorOutput {
    code: &'static str,
    message: &'static str,
}

fn http_fetch(
    invocation: verlet_guest_sdk::Invocation,
    source: verlet_guest_sdk::Source,
    output: verlet_guest_sdk::Sink,
    events: verlet_guest_sdk::EventSink,
) -> Result<(), verlet_guest_sdk::StatusCode> {
    let input: HttpFetchInput = serde_json::from_slice(&read_all_source(source)?)
        .map_err(|_| verlet_guest_sdk::StatusCode::InvalidArgument)?;
    let mut request = verlet_guest_sdk::HttpRequest::new("GET", input.url);
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
        .map_err(|_| verlet_guest_sdk::StatusCode::InvalidArgument)?;

    let sources = match verlet_guest_sdk::http_request(invocation, &request_bytes, &[], events) {
        Ok(sources) => sources,
        Err(err) => return write_json(output, &error_output(err)),
    };
    let metadata: verlet_guest_sdk::HttpResponse =
        serde_json::from_slice(&read_all_source(sources.metadata)?)
            .map_err(|_| verlet_guest_sdk::StatusCode::TransportError)?;
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

fn error_output(status: verlet_guest_sdk::StatusCode) -> HttpFetchOutput {
    let (code, message) = match status {
        verlet_guest_sdk::StatusCode::InvalidArgument => {
            ("invalid_argument", "invalid HTTP fetch request")
        }
        verlet_guest_sdk::StatusCode::CapabilityDenied => {
            ("capability_denied", "HTTP request denied")
        }
        verlet_guest_sdk::StatusCode::Timeout => ("timeout", "HTTP request timed out"),
        verlet_guest_sdk::StatusCode::Cancelled => ("cancelled", "HTTP request cancelled"),
        _ => ("transport_error", "HTTP request failed"),
    };
    HttpFetchOutput {
        status: 0,
        headers: std::collections::BTreeMap::new(),
        body_text: String::new(),
        truncated: false,
        error: Some(ErrorOutput { code, message }),
    }
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
