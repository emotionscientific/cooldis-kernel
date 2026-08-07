const FILE_READ_ID: u32 = 1;
const DEFAULT_MAX_BYTES: usize = 256 * 1024;

#[unsafe(no_mangle)]
pub extern "C" fn __verlet_describe_module__(sink: u32) -> i32 {
    let manifest =
        verlet_guest_sdk::OperationManifest::new(vec![verlet_guest_sdk::OperationDefinition {
            id: FILE_READ_ID,
            name: "file_read".to_string(),
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
        FILE_READ_ID => status(file_read(
            verlet_guest_sdk::Source(source),
            verlet_guest_sdk::Sink(output),
        )),
        _ => verlet_guest_sdk::STATUS_NOT_FOUND,
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct FileReadInput {
    path: String,
    #[serde(default)]
    offset_bytes: usize,
    max_bytes: Option<usize>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct FileReadOutput {
    content: String,
    bytes_read: usize,
    eof: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ErrorOutput>,
}

#[derive(serde::Serialize)]
struct ErrorOutput {
    code: &'static str,
    message: &'static str,
}

fn file_read(
    source: verlet_guest_sdk::Source,
    output: verlet_guest_sdk::Sink,
) -> Result<(), verlet_guest_sdk::StatusCode> {
    let input: FileReadInput = serde_json::from_slice(&read_all_source(source)?)
        .map_err(|_| verlet_guest_sdk::StatusCode::InvalidArgument)?;
    if !input.path.starts_with('/') {
        return Err(verlet_guest_sdk::StatusCode::InvalidArgument);
    }
    let max_bytes = input
        .max_bytes
        .unwrap_or(DEFAULT_MAX_BYTES)
        .min(DEFAULT_MAX_BYTES);
    let handle = match verlet_guest_sdk::open_file_read(&input.path) {
        Ok(handle) => handle,
        Err(err) => return write_json(output, &error_output(err)),
    };
    let read = read_file_to_vec(handle);
    let close = verlet_guest_sdk::close_file(handle);
    let bytes = match (read, close) {
        (Ok(bytes), Ok(())) => bytes,
        (Err(err), _) | (_, Err(err)) => return write_json(output, &error_output(err)),
    };
    let start = input.offset_bytes.min(bytes.len());
    let available = &bytes[start..];
    let take = available.len().min(max_bytes);
    let selected = &available[..take];
    write_json(
        output,
        &FileReadOutput {
            content: String::from_utf8_lossy(selected).into_owned(),
            bytes_read: selected.len(),
            eof: take == available.len(),
            error: None,
        },
    )
}

fn read_file_to_vec(
    handle: verlet_guest_sdk::FileHandle,
) -> Result<Vec<u8>, verlet_guest_sdk::StatusCode> {
    let mut output = Vec::new();
    let mut buffer = [0u8; 4096];
    loop {
        let n = verlet_guest_sdk::read_file(handle, &mut buffer)?;
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

fn error_output(status: verlet_guest_sdk::StatusCode) -> FileReadOutput {
    let (code, message) = match status {
        verlet_guest_sdk::StatusCode::InvalidArgument => {
            ("invalid_argument", "invalid file read request")
        }
        verlet_guest_sdk::StatusCode::NotFound => ("not_found", "file not found"),
        verlet_guest_sdk::StatusCode::CapabilityDenied => ("capability_denied", "file read denied"),
        verlet_guest_sdk::StatusCode::Cancelled => ("cancelled", "file read cancelled"),
        _ => ("transport_error", "file read failed"),
    };
    FileReadOutput {
        content: String::new(),
        bytes_read: 0,
        eof: true,
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
