use verlet_guest_sdk::{
    OperationDefinition, OperationEventKind, OperationManifest, OperationMode, OperationValueKind,
    STATUS_INVALID_ARGUMENT, STATUS_NOT_FOUND, STATUS_OK, Sink, Source, StatusCode, close_file,
    open_file_read, read_file, read_source, write_sink,
};
use serde::{Deserialize, Serialize};

const FILE_READ_ID: u32 = 1;
const DEFAULT_MAX_BYTES: usize = 256 * 1024;

#[unsafe(no_mangle)]
pub extern "C" fn __verlet_describe_module__(sink: u32) -> i32 {
    let manifest = OperationManifest::new(vec![OperationDefinition {
        id: FILE_READ_ID,
        name: "file_read".to_string(),
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
pub extern "C" fn __verlet_call_operation__(
    operation: u32,
    _invocation: u32,
    source: u32,
    output: u32,
    _events: u32,
) -> i32 {
    match operation {
        FILE_READ_ID => status(file_read(Source(source), Sink(output))),
        _ => STATUS_NOT_FOUND,
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FileReadInput {
    path: String,
    #[serde(default)]
    offset_bytes: usize,
    max_bytes: Option<usize>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FileReadOutput {
    content: String,
    bytes_read: usize,
    eof: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ErrorOutput>,
}

#[derive(Serialize)]
struct ErrorOutput {
    code: &'static str,
    message: &'static str,
}

fn file_read(source: Source, output: Sink) -> Result<(), StatusCode> {
    let input: FileReadInput = serde_json::from_slice(&read_all_source(source)?)
        .map_err(|_| StatusCode::InvalidArgument)?;
    if !input.path.starts_with('/') {
        return Err(StatusCode::InvalidArgument);
    }
    let max_bytes = input
        .max_bytes
        .unwrap_or(DEFAULT_MAX_BYTES)
        .min(DEFAULT_MAX_BYTES);
    let handle = match open_file_read(&input.path) {
        Ok(handle) => handle,
        Err(err) => return write_json(output, &error_output(err)),
    };
    let read = read_file_to_vec(handle);
    let close = close_file(handle);
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

fn read_file_to_vec(handle: verlet_guest_sdk::FileHandle) -> Result<Vec<u8>, StatusCode> {
    let mut output = Vec::new();
    let mut buffer = [0u8; 4096];
    loop {
        let n = read_file(handle, &mut buffer)?;
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

fn error_output(status: StatusCode) -> FileReadOutput {
    let (code, message) = match status {
        StatusCode::InvalidArgument => ("invalid_argument", "invalid file read request"),
        StatusCode::NotFound => ("not_found", "file not found"),
        StatusCode::CapabilityDenied => ("capability_denied", "file read denied"),
        StatusCode::Cancelled => ("cancelled", "file read cancelled"),
        _ => ("transport_error", "file read failed"),
    };
    FileReadOutput {
        content: String::new(),
        bytes_read: 0,
        eof: true,
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
