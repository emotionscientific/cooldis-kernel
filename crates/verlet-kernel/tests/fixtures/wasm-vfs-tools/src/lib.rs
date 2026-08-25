const CAT_ID: u32 = 1;
const TAIL_ID: u32 = 2;
const PUT_ID: u32 = 3;
const STAT_ID: u32 = 4;
const LS_ID: u32 = 5;

#[unsafe(no_mangle)]
pub extern "C" fn __verlet_describe_module__(sink: u32) -> i32 {
    let manifest = verlet_guest_sdk::OperationManifest::new(vec![
        operation(CAT_ID, "cat", &[]),
        operation(TAIL_ID, "tail", &[]),
        operation(PUT_ID, "put", &["fs.write"]),
        operation(STAT_ID, "stat", &[]),
        operation(LS_ID, "ls", &[]),
    ]);
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
        CAT_ID => status(cat(
            verlet_guest_sdk::Source(source),
            verlet_guest_sdk::Sink(output),
        )),
        TAIL_ID => status(tail(
            verlet_guest_sdk::Source(source),
            verlet_guest_sdk::Sink(output),
        )),
        PUT_ID => status(put(verlet_guest_sdk::Source(source))),
        STAT_ID => status(stat(
            verlet_guest_sdk::Source(source),
            verlet_guest_sdk::Sink(output),
        )),
        LS_ID => status(ls(
            verlet_guest_sdk::Source(source),
            verlet_guest_sdk::Sink(output),
        )),
        _ => verlet_guest_sdk::STATUS_NOT_FOUND,
    }
}

fn operation(
    id: u32,
    name: &str,
    required_capabilities: &[&str],
) -> verlet_guest_sdk::OperationDefinition {
    verlet_guest_sdk::OperationDefinition {
        id,
        name: name.to_string(),
        input: verlet_guest_sdk::OperationValueKind::Text,
        output: verlet_guest_sdk::OperationValueKind::Bytes,
        events: verlet_guest_sdk::OperationEventKind::None,
        mode: verlet_guest_sdk::OperationMode::Sync,
        required_capabilities: required_capabilities
            .iter()
            .map(|capability| (*capability).to_string())
            .collect(),
    }
}

fn cat(
    source: verlet_guest_sdk::Source,
    output: verlet_guest_sdk::Sink,
) -> Result<(), verlet_guest_sdk::StatusCode> {
    let path = read_path(source)?;
    let handle = verlet_guest_sdk::open_file_read(&path)?;
    let mut buffer = [0u8; 64];
    loop {
        let n = verlet_guest_sdk::read_file(handle, &mut buffer)?;
        if n == 0 {
            break;
        }
        verlet_guest_sdk::write_sink(output, &buffer[..n])?;
    }
    verlet_guest_sdk::close_file(handle)
}

fn tail(
    source: verlet_guest_sdk::Source,
    output: verlet_guest_sdk::Sink,
) -> Result<(), verlet_guest_sdk::StatusCode> {
    let path = read_path(source)?;
    let handle = verlet_guest_sdk::open_file_read(&path)?;
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 64];
    loop {
        let n = verlet_guest_sdk::read_file(handle, &mut buffer)?;
        if n == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..n]);
    }
    verlet_guest_sdk::close_file(handle)?;

    let start = last_two_lines_start(&bytes);
    verlet_guest_sdk::write_sink(output, &bytes[start..])?;
    Ok(())
}

fn put(source: verlet_guest_sdk::Source) -> Result<(), verlet_guest_sdk::StatusCode> {
    let input = read_all(source)?;
    let input: serde_json::Value = serde_json::from_slice(&input)
        .map_err(|_| verlet_guest_sdk::StatusCode::InvalidArgument)?;
    let path = input
        .get("path")
        .and_then(serde_json::Value::as_str)
        .ok_or(verlet_guest_sdk::StatusCode::InvalidArgument)?;
    let content = input
        .get("content")
        .and_then(serde_json::Value::as_str)
        .ok_or(verlet_guest_sdk::StatusCode::InvalidArgument)?
        .as_bytes();
    let parent = std::path::Path::new(path)
        .parent()
        .and_then(std::path::Path::to_str)
        .ok_or(verlet_guest_sdk::StatusCode::InvalidArgument)?;
    verlet_guest_sdk::mkdir(parent, true)?;
    let handle = verlet_guest_sdk::open_file_write(path)?;
    let split = content.len() / 2;
    verlet_guest_sdk::write_file(handle, &content[..split])?;
    verlet_guest_sdk::write_file(handle, &content[split..])?;
    verlet_guest_sdk::close_file(handle)
}

fn stat(
    source: verlet_guest_sdk::Source,
    output: verlet_guest_sdk::Sink,
) -> Result<(), verlet_guest_sdk::StatusCode> {
    let path = read_path(source)?;
    let stat = verlet_guest_sdk::stat_path(&path)?;
    let kind = match stat.kind {
        verlet_guest_sdk::FileKind::File => "file",
        verlet_guest_sdk::FileKind::Dir => "dir",
        verlet_guest_sdk::FileKind::Other => "other",
    };
    let bytes = serde_json::to_vec(&serde_json::json!({
        "kind": kind,
        "size": stat.size,
    }))
    .map_err(|_| verlet_guest_sdk::StatusCode::TransportError)?;
    verlet_guest_sdk::write_sink(output, &bytes)?;
    Ok(())
}

fn ls(
    source: verlet_guest_sdk::Source,
    output: verlet_guest_sdk::Sink,
) -> Result<(), verlet_guest_sdk::StatusCode> {
    let path = read_path(source)?;
    let entries = verlet_guest_sdk::list_dir(&path)?;
    let bytes =
        serde_json::to_vec(&entries).map_err(|_| verlet_guest_sdk::StatusCode::TransportError)?;
    verlet_guest_sdk::write_sink(output, &bytes)?;
    Ok(())
}

fn read_path(source: verlet_guest_sdk::Source) -> Result<String, verlet_guest_sdk::StatusCode> {
    let bytes = read_all(source)?;
    core::str::from_utf8(&bytes)
        .map(str::to_string)
        .map_err(|_| verlet_guest_sdk::StatusCode::InvalidArgument)
}

fn read_all(source: verlet_guest_sdk::Source) -> Result<Vec<u8>, verlet_guest_sdk::StatusCode> {
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 512];
    loop {
        let n = verlet_guest_sdk::read_source(source, &mut buffer)?;
        if n == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..n]);
    }
    Ok(bytes)
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

fn status(result: Result<(), verlet_guest_sdk::StatusCode>) -> i32 {
    match result {
        Ok(()) => verlet_guest_sdk::STATUS_OK,
        Err(err) => err.as_raw(),
    }
}
