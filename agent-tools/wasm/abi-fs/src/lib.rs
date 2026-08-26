//! `ToolFs` over the `cooldis_0.1` guest ABI.
//!
//! Adapts the agent-tools filesystem trait onto the host fs imports so the
//! tool cores run unchanged inside a wasm guest against the thread's
//! attached VFS. Confinement is the attachment itself: the host exposes only
//! the VFS world, so no path checks happen here (see the `ToolFs` trait
//! docs; the granted scope is the whole attached VFS). Mutating methods
//! additionally require the `fs.write` capability grant on the invocation;
//! without it the host denies the call and it surfaces as
//! [`verlet_tool_core::ToolFsError::Denied`].
//!
//! Native (non-wasm32) builds compile but every host call fails with a
//! transport error; the adapter is only meaningful inside a wasm guest.

/// [`verlet_tool_core::ToolFs`] backend over the guest ABI fs imports.
pub struct AbiFs {
    root: std::path::PathBuf,
}

impl AbiFs {
    /// `root` is the VFS directory that relative tool paths resolve
    /// against. The embedder passes it in the operation input; the runtime
    /// convention is `/workspace`.
    pub fn new(root: std::path::PathBuf) -> Self {
        Self { root }
    }

    /// Resolve a tool-supplied path onto the VFS: absolute paths pass
    /// through, relative paths join the root.
    fn resolve(&self, path: &std::path::Path) -> std::path::PathBuf {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.root.join(path)
        }
    }
}

/// Run one typed tool operation with the native CLI's input and output
/// envelopes. Source and sink failures remain ABI transport failures; input
/// and tool failures are written as successful `{"error": ...}` envelopes.
pub fn run_operation<Args, Output>(
    source: verlet_guest_sdk::Source,
    output: verlet_guest_sdk::Sink,
    run: fn(Args, &dyn verlet_tool_core::ToolFs) -> Result<Output, verlet_tool_core::ToolError>,
) -> Result<(), verlet_guest_sdk::StatusCode>
where
    Args: serde::de::DeserializeOwned,
    Output: serde::Serialize,
{
    #[derive(serde::Deserialize)]
    struct OperationInput<Args> {
        root: std::path::PathBuf,
        args: Args,
    }

    let bytes = verlet_guest_sdk::read_source_to_end(source)?;
    let input = match serde_json::from_slice::<OperationInput<Args>>(&bytes) {
        Ok(input) => input,
        Err(error) => return write_error(output, format!("invalid input JSON: {error}")),
    };
    let fs = AbiFs::new(input.root);
    write_result(output, run(input.args, &fs))
}

/// Run one tool operation whose CLI boundary has a raw-JSON parser hook.
pub fn run_operation_with_parser<Args, Output>(
    source: verlet_guest_sdk::Source,
    output: verlet_guest_sdk::Sink,
    parse_args: fn(serde_json::Value) -> Result<Args, String>,
    run: fn(Args, &dyn verlet_tool_core::ToolFs) -> Result<Output, verlet_tool_core::ToolError>,
) -> Result<(), verlet_guest_sdk::StatusCode>
where
    Output: serde::Serialize,
{
    #[derive(serde::Deserialize)]
    struct OperationInput {
        root: std::path::PathBuf,
        args: serde_json::Value,
    }

    let bytes = verlet_guest_sdk::read_source_to_end(source)?;
    let input = match serde_json::from_slice::<OperationInput>(&bytes) {
        Ok(input) => input,
        Err(error) => return write_error(output, format!("invalid input JSON: {error}")),
    };
    let args = match parse_args(input.args) {
        Ok(args) => args,
        Err(error) => return write_error(output, error),
    };
    let fs = AbiFs::new(input.root);
    write_result(output, run(args, &fs))
}

fn write_result<Output>(
    sink: verlet_guest_sdk::Sink,
    result: Result<Output, verlet_tool_core::ToolError>,
) -> Result<(), verlet_guest_sdk::StatusCode>
where
    Output: serde::Serialize,
{
    match result {
        Ok(output) => match serde_json::to_value(output) {
            Ok(output) => write_envelope(sink, "ok", output),
            Err(error) => write_error(sink, format!("failed to serialize result: {error}")),
        },
        Err(error) => write_error(sink, error.to_string()),
    }
}

fn write_error(
    sink: verlet_guest_sdk::Sink,
    error: String,
) -> Result<(), verlet_guest_sdk::StatusCode> {
    write_envelope(sink, "error", serde_json::Value::String(error))
}

fn write_envelope(
    sink: verlet_guest_sdk::Sink,
    key: &str,
    value: serde_json::Value,
) -> Result<(), verlet_guest_sdk::StatusCode> {
    let mut envelope = serde_json::Map::new();
    envelope.insert(key.to_owned(), value);
    let mut bytes = serde_json::to_vec(&serde_json::Value::Object(envelope))
        .map_err(|_| verlet_guest_sdk::StatusCode::TransportError)?;
    bytes.push(b'\n');
    let written = verlet_guest_sdk::write_sink(sink, &bytes)?;
    if written != bytes.len() {
        return Err(verlet_guest_sdk::StatusCode::TransportError);
    }
    Ok(())
}

impl verlet_tool_core::ToolFs for AbiFs {
    /// `open_file_read` + `read_file` chunks + `close_file`.
    /// Status mapping (all methods): `NotFound` ->
    /// [`verlet_tool_core::ToolFsError::NotFound`], `CapabilityDenied` ->
    /// `Denied`, everything else -> `Io` with the status in the message.
    /// A non-UTF-8 path is `Io` (host paths cross the ABI as UTF-8).
    fn read_file(&self, path: &std::path::Path) -> Result<Vec<u8>, verlet_tool_core::ToolFsError> {
        let resolved = self.resolve(path);
        let resolved = path_str(path, &resolved)?;
        let handle = verlet_guest_sdk::open_file_read(resolved)
            .map_err(|status| map_status(path, status))?;
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 1024];
        loop {
            match verlet_guest_sdk::read_file(handle, &mut buffer) {
                Ok(0) => break,
                Ok(read) => bytes.extend_from_slice(&buffer[..read]),
                Err(status) => {
                    let _ = verlet_guest_sdk::close_file(handle);
                    return Err(map_status(path, status));
                }
            }
        }
        verlet_guest_sdk::close_file(handle).map_err(|status| map_status(path, status))?;
        Ok(bytes)
    }

    /// `open_file_write` + `write_file` + `close_file`; the close is the
    /// commit point (whole-file replace). Parent directories are the tool
    /// core's job (`mkdir` first), matching the ABI contract.
    fn write_file(
        &self,
        path: &std::path::Path,
        content: &[u8],
    ) -> Result<(), verlet_tool_core::ToolFsError> {
        let resolved = self.resolve(path);
        let resolved = path_str(path, &resolved)?;
        let handle = verlet_guest_sdk::open_file_write(resolved)
            .map_err(|status| map_status(path, status))?;
        if let Err(status) = verlet_guest_sdk::write_file(handle, content) {
            // Deliberately NOT closed: `close_file` on a write handle is the
            // commit point, and `write_file` is all-or-nothing, so closing
            // here would commit an empty buffer (truncating the target as a
            // side effect of a failed write). A write handle dropped without
            // close is discarded by the host, leaving the file untouched.
            return Err(map_status(path, status));
        }
        verlet_guest_sdk::close_file(handle).map_err(|status| map_status(path, status))
    }

    /// Guest `mkdir` with the same `recursive` semantics.
    fn mkdir(
        &self,
        path: &std::path::Path,
        recursive: bool,
    ) -> Result<(), verlet_tool_core::ToolFsError> {
        let resolved = self.resolve(path);
        let resolved = path_str(path, &resolved)?;
        verlet_guest_sdk::mkdir(resolved, recursive).map_err(|status| map_status(path, status))
    }

    /// `stat_path`: kind `Dir` -> `is_dir`, kind `File` -> `is_file`,
    /// `Other` -> neither; size passes through.
    fn stat(
        &self,
        path: &std::path::Path,
    ) -> Result<verlet_tool_core::FileStat, verlet_tool_core::ToolFsError> {
        let resolved = self.resolve(path);
        let resolved = path_str(path, &resolved)?;
        let stat =
            verlet_guest_sdk::stat_path(resolved).map_err(|status| map_status(path, status))?;
        Ok(verlet_tool_core::FileStat {
            is_dir: stat.kind == verlet_guest_sdk::FileKind::Dir,
            is_file: stat.kind == verlet_guest_sdk::FileKind::File,
            size: stat.size,
        })
    }

    /// `list_dir`: entries map field-for-field (already name-sorted by the
    /// host, matching the deterministic walk order the tools rely on).
    fn read_dir(
        &self,
        path: &std::path::Path,
    ) -> Result<Vec<verlet_tool_core::DirEntry>, verlet_tool_core::ToolFsError> {
        let resolved = self.resolve(path);
        let resolved = path_str(path, &resolved)?;
        verlet_guest_sdk::list_dir(resolved)
            .map_err(|status| map_status(path, status))
            .map(|entries| {
                entries
                    .into_iter()
                    .map(|entry| verlet_tool_core::DirEntry {
                        name: entry.name,
                        is_dir: entry.is_dir,
                    })
                    .collect()
            })
    }

    /// `stat_path` with `NotFound` mapped to `Ok(false)`.
    fn exists(&self, path: &std::path::Path) -> Result<bool, verlet_tool_core::ToolFsError> {
        let resolved = self.resolve(path);
        let resolved = path_str(path, &resolved)?;
        match verlet_guest_sdk::stat_path(resolved) {
            Ok(_) => Ok(true),
            Err(verlet_guest_sdk::StatusCode::NotFound) => Ok(false),
            Err(status) => Err(map_status(path, status)),
        }
    }
}

fn path_str<'a>(
    path: &std::path::Path,
    resolved: &'a std::path::Path,
) -> Result<&'a str, verlet_tool_core::ToolFsError> {
    resolved.to_str().ok_or_else(|| {
        verlet_tool_core::ToolFsError::Io(format!(
            "filesystem path is not valid UTF-8: {}",
            path.display()
        ))
    })
}

fn map_status(
    path: &std::path::Path,
    status: verlet_guest_sdk::StatusCode,
) -> verlet_tool_core::ToolFsError {
    match status {
        verlet_guest_sdk::StatusCode::NotFound => {
            verlet_tool_core::ToolFsError::NotFound(path.to_path_buf())
        }
        verlet_guest_sdk::StatusCode::CapabilityDenied => {
            verlet_tool_core::ToolFsError::Denied(path.to_path_buf())
        }
        status => verlet_tool_core::ToolFsError::Io(format!(
            "filesystem operation for {} failed with status {status:?}",
            path.display()
        )),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn native_guest_transport_status_maps_to_io() {
        let fs = crate::AbiFs::new(std::path::PathBuf::from("/workspace"));

        let error =
            verlet_tool_core::ToolFs::read_file(&fs, std::path::Path::new("file.txt")).unwrap_err();

        assert!(matches!(error, verlet_tool_core::ToolFsError::Io(_)));
        assert!(error.to_string().contains("TransportError"));
    }
}
