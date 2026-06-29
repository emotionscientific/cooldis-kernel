use cooldis_guest_sdk::{
    OperationDefinition, OperationEventKind, OperationManifest, OperationMode, OperationValueKind,
    STATUS_INVALID_ARGUMENT, STATUS_NOT_FOUND, STATUS_OK, Sink, Source, StatusCode, close_file,
    open_file_read, read_file, read_source, write_sink,
};

const CAT_ID: u32 = 1;
const TAIL_ID: u32 = 2;

#[unsafe(no_mangle)]
pub extern "C" fn __cooldis_describe_module__(sink: u32) -> i32 {
    let manifest =
        OperationManifest::new(vec![operation(CAT_ID, "cat"), operation(TAIL_ID, "tail")]);
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
        CAT_ID => status(cat(Source(source), Sink(output))),
        TAIL_ID => status(tail(Source(source), Sink(output))),
        _ => STATUS_NOT_FOUND,
    }
}

fn operation(id: u32, name: &str) -> OperationDefinition {
    OperationDefinition {
        id,
        name: name.to_string(),
        input: OperationValueKind::Text,
        output: OperationValueKind::Bytes,
        events: OperationEventKind::None,
        mode: OperationMode::Sync,
        required_capabilities: Vec::new(),
    }
}

fn cat(source: Source, output: Sink) -> Result<(), StatusCode> {
    let path = read_path(source)?;
    let handle = open_file_read(&path)?;
    let mut buffer = [0u8; 64];
    loop {
        let n = read_file(handle, &mut buffer)?;
        if n == 0 {
            break;
        }
        write_sink(output, &buffer[..n])?;
    }
    close_file(handle)
}

fn tail(source: Source, output: Sink) -> Result<(), StatusCode> {
    let path = read_path(source)?;
    let handle = open_file_read(&path)?;
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 64];
    loop {
        let n = read_file(handle, &mut buffer)?;
        if n == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..n]);
    }
    close_file(handle)?;

    let start = last_two_lines_start(&bytes);
    write_sink(output, &bytes[start..])?;
    Ok(())
}

fn read_path(source: Source) -> Result<String, StatusCode> {
    let mut buffer = [0u8; 512];
    let n = read_source(source, &mut buffer)?;
    core::str::from_utf8(&buffer[..n])
        .map(str::to_string)
        .map_err(|_| StatusCode::InvalidArgument)
}

fn last_two_lines_start(bytes: &[u8]) -> usize {
    let mut scan = bytes.len();
    if scan > 0 && bytes[scan - 1] == b'\n' {
        scan -= 1;
    }

    let mut lines = 0;
    while scan > 0 {
        scan -= 1;
        if bytes[scan] == b'\n' {
            lines += 1;
            if lines == 2 {
                return scan + 1;
            }
        }
    }
    0
}

fn status(result: Result<(), StatusCode>) -> i32 {
    match result {
        Ok(()) => STATUS_OK,
        Err(err) => err.as_raw(),
    }
}
