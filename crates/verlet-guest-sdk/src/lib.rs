//! Guest-side helpers for Verlet ABI Wasm operations.
//!
//! This crate is intentionally small: it gives Rust guests the canonical
//! manifest/request shapes plus thin wrappers over the `cooldis_0.1` host ABI.
//! The blessed authoring pattern is a normal Rust `cdylib` crate compiled to
//! `wasm32-unknown-unknown`; see
//! `crates/verlet-kernel/tests/fixtures/wasm-vfs-tools` in the workspace for
//! the `cat` / `tail` fixture.

mod contract;
pub mod testkit;

// The re-exports below are a deliberate exception to the workspace's
// no-`pub use` rule, on the same footing as the engine re-exports in
// `verlet-sqlite`. This crate is the guest-authoring facade: a guest crate
// names `verlet_guest_sdk` and nothing else, so the attribute macros must
// reach it from here rather than through a second direct dependency on
// `verlet-guest-sdk-macros`. The `prelude` glob is likewise part of the
// published surface — the scaffold emitted by `verlet coupling init` opens
// with `use verlet_guest_sdk::prelude::*;` — so both are public API for
// crates outside this repo, not internal convenience.
pub use contract::{CouplingContext, Discharge, GuestError, OperationContext};
pub use verlet_guest_sdk_macros::{coupling, operation};

/// One-line import for guest crates:
/// `use verlet_guest_sdk::prelude::*;`
pub mod prelude {
    pub use crate::contract::{CouplingContext, Discharge, GuestError, OperationContext};
    pub use crate::{coupling, operation};
    pub use serde::{Deserialize, Serialize};
}

#[doc(hidden)]
pub mod __private {

    pub fn read_json_input<T: serde::de::DeserializeOwned>(
        source: crate::Source,
    ) -> Result<T, crate::GuestError> {
        let bytes = crate::read_source_to_end(source).map_err(crate::GuestError::Host)?;
        serde_json::from_slice(&bytes)
            .map_err(|err| crate::GuestError::BadInput(format!("operation input: {err}")))
    }

    pub fn write_json_output<T: serde::Serialize>(
        sink: crate::Sink,
        value: &T,
    ) -> Result<(), crate::GuestError> {
        let bytes = serde_json::to_vec(value)
            .map_err(|err| crate::GuestError::Internal(format!("operation output: {err}")))?;
        crate::write_sink(sink, &bytes).map_err(crate::GuestError::Host)?;
        Ok(())
    }

    pub fn write_manifest(
        sink: crate::Sink,
        manifest: &crate::OperationManifest,
    ) -> Result<(), crate::GuestError> {
        let bytes = manifest
            .to_json_vec()
            .map_err(|err| crate::GuestError::Internal(format!("operation manifest: {err}")))?;
        crate::write_sink(sink, &bytes).map_err(crate::GuestError::Host)?;
        Ok(())
    }

    pub fn read_coupling_context(
        source: crate::Source,
    ) -> Result<crate::CouplingContext, crate::GuestError> {
        let bytes = crate::read_source_to_end(source).map_err(crate::GuestError::Host)?;
        let invocation = crate::CouplingInvocation::from_json_slice(&bytes)
            .map_err(|err| crate::GuestError::BadInput(format!("coupling invocation: {err}")))?;
        if invocation.abi != crate::COUPLING_INVOCATION_ABI {
            return Err(crate::GuestError::BadInput(format!(
                "coupling invocation abi {:?} is not {COUPLING_INVOCATION_ABI:?}",
                invocation.abi,
                COUPLING_INVOCATION_ABI = crate::COUPLING_INVOCATION_ABI
            )));
        }
        Ok(crate::CouplingContext::from_invocation(invocation))
    }

    pub fn write_coupling_discharge_output(
        sink: crate::Sink,
        discharge: crate::Discharge,
    ) -> Result<(), crate::GuestError> {
        let bytes = discharge
            .into_coupling_discharge()
            .to_json_vec()
            .map_err(|err| crate::GuestError::Internal(format!("coupling discharge: {err}")))?;
        crate::write_sink(sink, &bytes).map_err(crate::GuestError::Host)?;
        Ok(())
    }

    pub fn status_from_guest_result(result: Result<(), crate::GuestError>) -> i32 {
        match result {
            Ok(()) => crate::STATUS_OK,
            Err(err) => status_from_guest_error(err),
        }
    }

    fn status_from_guest_error(err: crate::GuestError) -> i32 {
        match err {
            crate::GuestError::BadInput(_) => crate::STATUS_INVALID_ARGUMENT,
            crate::GuestError::Unsupported(_) => crate::STATUS_NOT_FOUND,
            crate::GuestError::Host(status) => status.as_raw(),
            crate::GuestError::Internal(_) => crate::StatusCode::TransportError.as_raw(),
        }
    }
}

/// ABI identifier for manifest-bearing Verlet Wasm operations.
pub const OPERATION_ABI: &str = "cooldis.operation/0.1";
/// ABI identifier for the host HTTP request import.
pub const HTTP_ABI: &str = "cooldis.net.http/0.1";
/// ABI identifier for custom coupling invocation JSON.
pub const COUPLING_INVOCATION_ABI: &str = "cooldis.coupling.invocation/0.1";
/// ABI identifier for custom coupling discharge JSON.
pub const COUPLING_DISCHARGE_ABI: &str = "cooldis.coupling.discharge/0.1";
/// Successful ABI status.
pub const STATUS_OK: i32 = 0;
/// Caller supplied invalid input or malformed JSON.
pub const STATUS_INVALID_ARGUMENT: i32 = 1;
/// Requested operation or host resource was not found.
pub const STATUS_NOT_FOUND: i32 = 2;
/// The host denied a requested capability.
pub const STATUS_CAPABILITY_DENIED: i32 = 3;
/// Guest or host transport failed.
pub const STATUS_TRANSPORT_ERROR: i32 = 4;
/// Operation timed out.
pub const STATUS_TIMEOUT: i32 = 5;
/// Invocation was cancelled.
pub const STATUS_CANCELLED: i32 = 6;
/// Source or file handle reached end of input.
pub const STATUS_EOF: i32 = 7;
/// File open mode for read-only VFS handles.
pub const FS_MODE_READ: u32 = 0;
/// File open mode for write VFS handles: bytes accumulate host-side and the
/// file is created or wholly replaced only when the handle is closed.
pub const FS_MODE_WRITE: u32 = 1;
#[cfg(target_arch = "wasm32")]
const STATUS_LEGACY_SOURCE_EOF: i32 = -1;

/// Status values returned by Verlet host imports and exports.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatusCode {
    /// The call succeeded.
    Ok,
    /// Input was malformed or semantically invalid.
    InvalidArgument,
    /// The requested operation, source, sink, or resource does not exist.
    NotFound,
    /// The host denied a declared or requested capability.
    CapabilityDenied,
    /// The host transport or guest boundary failed.
    TransportError,
    /// The host-side operation timed out.
    Timeout,
    /// The invocation was cancelled.
    Cancelled,
    /// The source or file is exhausted.
    Eof,
    /// A status code not known to this SDK version.
    Unknown(i32),
}

impl StatusCode {
    /// Convert a raw ABI status integer into a typed status.
    pub fn from_raw(status: i32) -> Self {
        match status {
            STATUS_OK => Self::Ok,
            STATUS_INVALID_ARGUMENT => Self::InvalidArgument,
            STATUS_NOT_FOUND => Self::NotFound,
            STATUS_CAPABILITY_DENIED => Self::CapabilityDenied,
            STATUS_TRANSPORT_ERROR => Self::TransportError,
            STATUS_TIMEOUT => Self::Timeout,
            STATUS_CANCELLED => Self::Cancelled,
            STATUS_EOF => Self::Eof,
            other => Self::Unknown(other),
        }
    }

    /// Convert this status into its raw ABI integer.
    pub fn as_raw(self) -> i32 {
        match self {
            Self::Ok => STATUS_OK,
            Self::InvalidArgument => STATUS_INVALID_ARGUMENT,
            Self::NotFound => STATUS_NOT_FOUND,
            Self::CapabilityDenied => STATUS_CAPABILITY_DENIED,
            Self::TransportError => STATUS_TRANSPORT_ERROR,
            Self::Timeout => STATUS_TIMEOUT,
            Self::Cancelled => STATUS_CANCELLED,
            Self::Eof => STATUS_EOF,
            Self::Unknown(status) => status,
        }
    }

    /// Treat `Ok` as success and every other status as an error.
    pub fn into_result(self) -> Result<(), Self> {
        match self {
            Self::Ok => Ok(()),
            err => Err(err),
        }
    }
}

/// Manifest exported by a Wasm operation guest.
#[derive(Clone, Debug, serde::Deserialize, PartialEq, Eq, serde::Serialize)]
pub struct OperationManifest {
    /// Manifest ABI version.
    pub abi: String,
    /// Operations provided by this guest module.
    pub operations: Vec<OperationDefinition>,
}

impl OperationManifest {
    /// Build a manifest for the current operation ABI.
    pub fn new(operations: Vec<OperationDefinition>) -> Self {
        Self {
            abi: OPERATION_ABI.to_string(),
            operations,
        }
    }

    /// Encode the manifest as JSON bytes for the ABI sink.
    pub fn to_json_vec(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
}

/// One operation entry in a Wasm guest manifest.
#[derive(Clone, Debug, serde::Deserialize, PartialEq, Eq, serde::Serialize)]
pub struct OperationDefinition {
    /// Stable numeric id used by `__verlet_call_operation__`.
    pub id: u32,
    /// Caller-facing operation name.
    pub name: String,
    #[serde(default)]
    /// Input value kind accepted by the operation.
    pub input: OperationValueKind,
    #[serde(default)]
    /// Output value kind written by the operation.
    pub output: OperationValueKind,
    #[serde(default)]
    /// Event stream format emitted by the operation.
    pub events: OperationEventKind,
    #[serde(default)]
    /// Operation execution mode.
    pub mode: OperationMode,
    #[serde(default)]
    /// Host capabilities the operation requires before dispatch.
    pub required_capabilities: Vec<String>,
}

/// Input envelope for a custom coupling Wasm guest.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CouplingInvocation {
    /// Invocation ABI version.
    pub abi: String,
    /// Event that fired this coupling invocation.
    pub trigger_event: CouplingInvocationEvent,
    #[serde(default)]
    /// Source events selected by the coupling declaration.
    pub selected_events: Vec<CouplingInvocationEvent>,
    #[serde(default)]
    /// Manifest-supplied coupling configuration.
    pub config: serde_json::Value,
    /// Invocation metadata supplied by the kernel.
    pub invocation_meta: CouplingInvocationMeta,
}

impl CouplingInvocation {
    /// Decode a coupling invocation from JSON bytes.
    pub fn from_json_slice(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// Event shape included in a coupling invocation.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CouplingInvocationEvent {
    /// Stable event id.
    pub id: String,
    /// Stream that contains the event.
    pub stream_id: String,
    /// Monotonic sequence number within the stream.
    pub sequence: i64,
    /// Event kind.
    pub kind: String,
    /// Origin label recorded by the kernel.
    pub origin: String,
    #[serde(default)]
    /// Event payload.
    pub payload: serde_json::Value,
}

/// Kernel metadata attached to a coupling invocation.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CouplingInvocationMeta {
    /// Coupling id from the manifest binding.
    pub coupling_id: String,
    /// Thread id where the coupling fired.
    pub thread_id: String,
    /// Coupling recursion depth.
    pub depth: u32,
}

/// Output envelope for a custom coupling Wasm guest.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CouplingDischarge {
    /// Discharge ABI version.
    pub abi: String,
    #[serde(default)]
    /// Proposed events for the kernel to validate and stamp.
    pub events: Vec<CouplingDischargeEvent>,
}

impl CouplingDischarge {
    /// Build a discharge envelope for the current coupling ABI.
    pub fn new(events: Vec<CouplingDischargeEvent>) -> Self {
        Self {
            abi: COUPLING_DISCHARGE_ABI.to_string(),
            events,
        }
    }

    /// Encode the discharge as JSON bytes for the ABI sink.
    pub fn to_json_vec(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
}

/// One event proposed by a coupling discharge.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CouplingDischargeEvent {
    /// Target event stream.
    pub stream: String,
    /// Target event kind.
    pub kind: String,
    #[serde(default)]
    /// Event payload.
    pub payload: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional guest-supplied provenance; the kernel stamps authoritative provenance.
    pub provenance: Option<serde_json::Value>,
}

impl OperationDefinition {
    /// Build a synchronous bytes-in/bytes-out operation definition.
    pub fn new(id: u32, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
            input: OperationValueKind::Bytes,
            output: OperationValueKind::Bytes,
            events: OperationEventKind::None,
            mode: OperationMode::Sync,
            required_capabilities: Vec::new(),
        }
    }

    /// Mark the operation input as JSON.
    pub fn json_input(mut self) -> Self {
        self.input = OperationValueKind::Json;
        self
    }

    /// Mark the operation output as JSON.
    pub fn json_output(mut self) -> Self {
        self.output = OperationValueKind::Json;
        self
    }

    /// Mark the operation event stream as JSON Lines.
    pub fn jsonl_events(mut self) -> Self {
        self.events = OperationEventKind::Jsonl;
        self
    }

    /// Set the operation execution mode.
    pub fn mode(mut self, mode: OperationMode) -> Self {
        self.mode = mode;
        self
    }

    /// Add a required host capability declaration.
    pub fn require(mut self, capability: impl Into<String>) -> Self {
        self.required_capabilities.push(capability.into());
        self
    }
}

/// Value encoding declared by an operation manifest entry.
#[derive(Clone, Debug, Default, serde::Deserialize, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationValueKind {
    /// Opaque bytes.
    #[default]
    Bytes,
    /// UTF-8 text.
    Text,
    /// JSON value.
    Json,
}

/// Event encoding declared by an operation manifest entry.
#[derive(Clone, Debug, Default, serde::Deserialize, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationEventKind {
    /// The operation does not emit events.
    #[default]
    None,
    /// The operation emits newline-delimited JSON events.
    Jsonl,
}

/// Execution mode declared by an operation manifest entry.
#[derive(Clone, Debug, Default, serde::Deserialize, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationMode {
    /// The operation completes during one host call.
    #[default]
    Sync,
    /// The operation may yield a host-managed process handle.
    Async,
    /// The operation may stream output or events over time.
    Streaming,
}

/// HTTP request envelope passed to the Verlet host import.
#[derive(Clone, Debug, serde::Deserialize, PartialEq, Eq, serde::Serialize)]
pub struct HttpRequest {
    /// HTTP ABI version.
    pub abi: String,
    /// HTTP method such as `GET` or `POST`.
    pub method: String,
    /// Absolute request URL.
    pub url: String,
    #[serde(default)]
    /// Literal headers to include with the request.
    pub headers: Vec<(String, String)>,
    #[serde(default)]
    /// Headers whose values are resolved from host-side secret names.
    pub secret_headers: Vec<(String, String)>,
    #[serde(default)]
    /// Secret-backed headers with a literal prefix such as `Bearer `.
    pub secret_header_prefixes: Vec<(String, String, String)>,
    #[serde(default)]
    /// Optional normalized input mapping interpreted by the HTTP host import.
    pub input_mapping: Option<serde_json::Value>,
    #[serde(default)]
    /// Return status, headers, body, and truncation as one JSON body source.
    pub response_envelope: bool,
    #[serde(default)]
    /// Optional timeout in milliseconds.
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    /// Optional response body size limit.
    pub max_response_bytes: Option<usize>,
}

impl HttpRequest {
    /// Build a request for the current HTTP ABI.
    pub fn new(method: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            abi: HTTP_ABI.to_string(),
            method: method.into(),
            url: url.into(),
            headers: Vec::new(),
            secret_headers: Vec::new(),
            secret_header_prefixes: Vec::new(),
            input_mapping: None,
            response_envelope: false,
            timeout_ms: None,
            max_response_bytes: None,
        }
    }

    /// Add a literal header.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Add a header whose value is loaded from a host-side secret.
    pub fn secret_header(mut self, name: impl Into<String>, secret: impl Into<String>) -> Self {
        self.secret_headers.push((name.into(), secret.into()));
        self
    }

    /// Set the request timeout in milliseconds.
    pub fn timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }

    /// Set the maximum response body bytes the host should return.
    pub fn max_response_bytes(mut self, max_response_bytes: usize) -> Self {
        self.max_response_bytes = Some(max_response_bytes);
        self
    }

    /// Encode the request as JSON bytes for the host import.
    pub fn to_json_vec(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
}

/// HTTP response metadata returned by the Verlet host import.
#[derive(Clone, Debug, serde::Deserialize, PartialEq, Eq, serde::Serialize)]
pub struct HttpResponse {
    /// HTTP ABI version.
    pub abi: String,
    /// Numeric HTTP status code.
    pub status: u16,
    /// Response headers.
    pub headers: Vec<(String, String)>,
    /// Whether the response body was truncated by the host limit.
    pub truncated: bool,
    /// Host-observed request duration in milliseconds.
    pub elapsed_ms: u64,
}

/// Host source handle for operation input or response data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Source(pub u32);

/// Host sink handle for operation output.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Sink(pub u32);

/// Host sink handle for JSONL operation events.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventSink(pub u32);

/// Host invocation handle for effectful operation calls.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Invocation(pub u32);

/// Host VFS file handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileHandle(pub u32);

/// Kind of filesystem entry reported by [`stat_path`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileKind {
    /// A regular file.
    File,
    /// A directory.
    Dir,
    /// Anything that is neither a regular file nor a directory.
    Other,
}

/// Metadata for a VFS path, decoded from the host's 16-byte stat record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileStat {
    /// What kind of entry the path names.
    pub kind: FileKind,
    /// Size in bytes for files; 0 for directories.
    pub size: u64,
}

/// One directory entry from [`list_dir`].
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct DirEntry {
    /// Entry name (name only, never a full path).
    pub name: String,
    /// Whether the entry is a directory.
    pub is_dir: bool,
}

/// Source handles returned by an HTTP host request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpResponseSources {
    /// Source containing serialized `HttpResponse` metadata.
    pub metadata: Source,
    /// Source containing response body bytes.
    pub body: Source,
}

/// Read one chunk from a host source into `buffer`.
pub fn read_source(source: Source, buffer: &mut [u8]) -> Result<usize, StatusCode> {
    call_source_read(source, buffer)
}

/// Read a host source until EOF or a short read.
pub fn read_source_to_end(source: Source) -> Result<Vec<u8>, StatusCode> {
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 1024];
    loop {
        let n = read_source(source, &mut buffer)?;
        if n == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..n]);
        if n < buffer.len() {
            break;
        }
    }
    Ok(bytes)
}

/// Read and decode a coupling invocation from a host source.
pub fn read_coupling_invocation(source: Source) -> Result<CouplingInvocation, StatusCode> {
    let bytes = read_source_to_end(source)?;
    CouplingInvocation::from_json_slice(&bytes).map_err(|_| StatusCode::InvalidArgument)
}

/// Write bytes to a host sink.
pub fn write_sink(sink: Sink, bytes: &[u8]) -> Result<usize, StatusCode> {
    call_sink_write(sink, bytes)
}

/// Encode and write a coupling discharge to a host sink.
pub fn write_coupling_discharge(
    sink: Sink,
    discharge: &CouplingDischarge,
) -> Result<usize, StatusCode> {
    let bytes = discharge
        .to_json_vec()
        .map_err(|_| StatusCode::InvalidArgument)?;
    write_sink(sink, &bytes)
}

/// Emit one JSONL event chunk to the operation event sink.
pub fn emit_event(
    invocation: Invocation,
    event_sink: EventSink,
    bytes: &[u8],
) -> Result<usize, StatusCode> {
    call_event_emit(invocation, event_sink, bytes)
}

/// Perform a host-mediated HTTP request.
pub fn http_request(
    invocation: Invocation,
    request: &[u8],
    body: &[u8],
    event_sink: EventSink,
) -> Result<HttpResponseSources, StatusCode> {
    call_http_request(invocation, request, body, event_sink)
}

/// Ask the host whether this invocation has been cancelled.
pub fn check_cancelled(invocation: Invocation) -> Result<(), StatusCode> {
    call_check_cancelled(invocation).into_result()
}

/// Open a read-only VFS file handle.
pub fn open_file_read(path: &str) -> Result<FileHandle, StatusCode> {
    call_fs_open(path, FS_MODE_READ)
}

/// Read bytes from a VFS file handle.
pub fn read_file(handle: FileHandle, buffer: &mut [u8]) -> Result<usize, StatusCode> {
    call_fs_read(handle, buffer)
}

/// Close a VFS file handle. For write handles this is the commit point: the
/// host replaces the whole file with the accumulated buffer, and the returned
/// status is the outcome of that write.
pub fn close_file(handle: FileHandle) -> Result<(), StatusCode> {
    call_fs_close(handle).into_result()
}

/// Open a write VFS file handle. Bytes passed to [`write_file`] accumulate
/// host-side; the file is created or wholly replaced only when
/// [`close_file`] commits the buffer. Parent directories are not created on
/// commit; create them first with [`mkdir`]. Requires the `fs.write`
/// capability grant in addition to an attached VFS.
pub fn open_file_write(path: &str) -> Result<FileHandle, StatusCode> {
    call_fs_open(path, FS_MODE_WRITE)
}

/// Append bytes to the pending buffer of a write VFS file handle.
/// All-or-nothing: on `Ok(())` the whole slice was appended.
pub fn write_file(handle: FileHandle, bytes: &[u8]) -> Result<(), StatusCode> {
    call_fs_write(handle, bytes).into_result()
}

/// Stat an absolute VFS path. `Err(StatusCode::NotFound)` doubles as the
/// existence check; there is no separate exists import.
pub fn stat_path(path: &str) -> Result<FileStat, StatusCode> {
    call_fs_stat(path)
}

/// List an absolute VFS directory, name-sorted (byte order). Drains the
/// host's listing source and decodes the JSON array of entries.
pub fn list_dir(path: &str) -> Result<Vec<DirEntry>, StatusCode> {
    let source = call_fs_list(path)?;
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 1024];
    loop {
        let n = call_source_read(source, &mut buffer)?;
        if n == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..n]);
    }
    serde_json::from_slice(&bytes).map_err(|_| StatusCode::TransportError)
}

/// Create a directory at an absolute VFS path. Requires the `fs.write`
/// capability grant in addition to an attached VFS.
pub fn mkdir(path: &str, recursive: bool) -> Result<(), StatusCode> {
    call_fs_mkdir(path, recursive).into_result()
}

#[cfg(target_arch = "wasm32")]
fn call_source_read(source: Source, buffer: &mut [u8]) -> Result<usize, StatusCode> {
    let mut len = buffer.len() as u32;
    let status = unsafe {
        imports::source_read(
            source.0,
            buffer.as_mut_ptr() as u32,
            &mut len as *mut u32 as u32,
        )
    };
    match status {
        STATUS_OK | STATUS_LEGACY_SOURCE_EOF => Ok(len as usize),
        other => Err(StatusCode::from_raw(other)),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn call_source_read(_source: Source, _buffer: &mut [u8]) -> Result<usize, StatusCode> {
    Err(StatusCode::TransportError)
}

#[cfg(target_arch = "wasm32")]
fn call_sink_write(sink: Sink, bytes: &[u8]) -> Result<usize, StatusCode> {
    let mut len = bytes.len() as u32;
    let status =
        unsafe { imports::sink_write(sink.0, bytes.as_ptr() as u32, &mut len as *mut u32 as u32) };
    StatusCode::from_raw(status).into_result()?;
    Ok(len as usize)
}

#[cfg(not(target_arch = "wasm32"))]
fn call_sink_write(_sink: Sink, _bytes: &[u8]) -> Result<usize, StatusCode> {
    Err(StatusCode::TransportError)
}

#[cfg(target_arch = "wasm32")]
fn call_event_emit(
    invocation: Invocation,
    event_sink: EventSink,
    bytes: &[u8],
) -> Result<usize, StatusCode> {
    let mut len = bytes.len() as u32;
    let status = unsafe {
        imports::event_emit(
            invocation.0,
            bytes.as_ptr() as u32,
            &mut len as *mut u32 as u32,
        )
    };
    StatusCode::from_raw(status).into_result()?;
    let _ = event_sink;
    Ok(len as usize)
}

#[cfg(not(target_arch = "wasm32"))]
fn call_event_emit(
    _invocation: Invocation,
    _event_sink: EventSink,
    _bytes: &[u8],
) -> Result<usize, StatusCode> {
    Err(StatusCode::TransportError)
}

#[cfg(target_arch = "wasm32")]
fn call_http_request(
    invocation: Invocation,
    request: &[u8],
    body: &[u8],
    event_sink: EventSink,
) -> Result<HttpResponseSources, StatusCode> {
    let mut out = [0u32; 2];
    let status = unsafe {
        imports::http_request(
            invocation.0,
            request.as_ptr() as u32,
            request.len() as u32,
            body.as_ptr() as u32,
            body.len() as u32,
            out.as_mut_ptr() as u32,
            event_sink.0,
        )
    };
    StatusCode::from_raw(status).into_result()?;
    Ok(HttpResponseSources {
        metadata: Source(out[0]),
        body: Source(out[1]),
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn call_http_request(
    _invocation: Invocation,
    _request: &[u8],
    _body: &[u8],
    _event_sink: EventSink,
) -> Result<HttpResponseSources, StatusCode> {
    Err(StatusCode::TransportError)
}

#[cfg(target_arch = "wasm32")]
fn call_check_cancelled(invocation: Invocation) -> StatusCode {
    StatusCode::from_raw(unsafe { imports::check_cancelled(invocation.0) })
}

#[cfg(not(target_arch = "wasm32"))]
fn call_check_cancelled(_invocation: Invocation) -> StatusCode {
    StatusCode::TransportError
}

#[cfg(target_arch = "wasm32")]
fn call_fs_open(path: &str, mode: u32) -> Result<FileHandle, StatusCode> {
    let mut handle = 0u32;
    let status = unsafe {
        imports::fs_open(
            path.as_ptr() as u32,
            path.len() as u32,
            mode,
            &mut handle as *mut u32 as u32,
        )
    };
    StatusCode::from_raw(status).into_result()?;
    Ok(FileHandle(handle))
}

#[cfg(not(target_arch = "wasm32"))]
fn call_fs_open(_path: &str, _mode: u32) -> Result<FileHandle, StatusCode> {
    Err(StatusCode::TransportError)
}

#[cfg(target_arch = "wasm32")]
fn call_fs_read(handle: FileHandle, buffer: &mut [u8]) -> Result<usize, StatusCode> {
    let mut len = buffer.len() as u32;
    let status = unsafe {
        imports::fs_read(
            handle.0,
            buffer.as_mut_ptr() as u32,
            &mut len as *mut u32 as u32,
        )
    };
    match StatusCode::from_raw(status) {
        StatusCode::Ok => Ok(len as usize),
        StatusCode::Eof => Ok(0),
        err => Err(err),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn call_fs_read(_handle: FileHandle, _buffer: &mut [u8]) -> Result<usize, StatusCode> {
    Err(StatusCode::TransportError)
}

#[cfg(target_arch = "wasm32")]
fn call_fs_close(handle: FileHandle) -> StatusCode {
    StatusCode::from_raw(unsafe { imports::fs_close(handle.0) })
}

#[cfg(not(target_arch = "wasm32"))]
fn call_fs_close(_handle: FileHandle) -> StatusCode {
    StatusCode::TransportError
}

#[cfg(target_arch = "wasm32")]
fn call_fs_write(handle: FileHandle, bytes: &[u8]) -> StatusCode {
    StatusCode::from_raw(unsafe {
        imports::fs_write(handle.0, bytes.as_ptr() as u32, bytes.len() as u32)
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn call_fs_write(_handle: FileHandle, _bytes: &[u8]) -> StatusCode {
    StatusCode::TransportError
}

#[cfg(target_arch = "wasm32")]
fn call_fs_stat(path: &str) -> Result<FileStat, StatusCode> {
    let mut record = [0u8; 16];
    let status = unsafe {
        imports::fs_stat(
            path.as_ptr() as u32,
            path.len() as u32,
            record.as_mut_ptr() as u32,
        )
    };
    StatusCode::from_raw(status).into_result()?;
    let kind = match u32::from_le_bytes(record[0..4].try_into().unwrap()) {
        0 => FileKind::File,
        1 => FileKind::Dir,
        _ => FileKind::Other,
    };
    let size = u64::from_le_bytes(record[8..16].try_into().unwrap());
    Ok(FileStat { kind, size })
}

#[cfg(not(target_arch = "wasm32"))]
fn call_fs_stat(_path: &str) -> Result<FileStat, StatusCode> {
    Err(StatusCode::TransportError)
}

#[cfg(target_arch = "wasm32")]
fn call_fs_list(path: &str) -> Result<Source, StatusCode> {
    let mut source = 0u32;
    let status = unsafe {
        imports::fs_list(
            path.as_ptr() as u32,
            path.len() as u32,
            &mut source as *mut u32 as u32,
        )
    };
    StatusCode::from_raw(status).into_result()?;
    Ok(Source(source))
}

#[cfg(not(target_arch = "wasm32"))]
fn call_fs_list(_path: &str) -> Result<Source, StatusCode> {
    Err(StatusCode::TransportError)
}

#[cfg(target_arch = "wasm32")]
fn call_fs_mkdir(path: &str, recursive: bool) -> StatusCode {
    StatusCode::from_raw(unsafe {
        imports::fs_mkdir(path.as_ptr() as u32, path.len() as u32, recursive as u32)
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn call_fs_mkdir(_path: &str, _recursive: bool) -> StatusCode {
    StatusCode::TransportError
}

#[cfg(target_arch = "wasm32")]
mod imports {
    #[link(wasm_import_module = "cooldis_0.1")]
    unsafe extern "C" {
        pub fn source_read(source: u32, ptr: u32, len_ptr: u32) -> i32;
        pub fn sink_write(sink: u32, ptr: u32, len_ptr: u32) -> i32;
        pub fn event_emit(invocation: u32, ptr: u32, len_ptr: u32) -> i32;
        pub fn http_request(
            invocation: u32,
            request_ptr: u32,
            request_len: u32,
            body_ptr: u32,
            body_len: u32,
            out_ptr: u32,
            event_sink: u32,
        ) -> i32;
        pub fn check_cancelled(invocation: u32) -> i32;
        pub fn fs_open(path_ptr: u32, path_len: u32, mode: u32, out_handle_ptr: u32) -> i32;
        pub fn fs_read(handle: u32, ptr: u32, len_ptr: u32) -> i32;
        pub fn fs_close(handle: u32) -> i32;
        pub fn fs_write(handle: u32, ptr: u32, len: u32) -> i32;
        pub fn fs_stat(path_ptr: u32, path_len: u32, out_ptr: u32) -> i32;
        pub fn fs_list(path_ptr: u32, path_len: u32, out_source_ptr: u32) -> i32;
        pub fn fs_mkdir(path_ptr: u32, path_len: u32, recursive: u32) -> i32;
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn manifest_serializes_to_verlet_operation_abi() {
        let manifest = crate::OperationManifest::new(vec![
            crate::OperationDefinition::new(1, "search")
                .json_input()
                .json_output()
                .jsonl_events()
                .require("net.http:POST:https://api.example.invalid")
                .require("secret:EXAMPLE_API_KEY"),
        ]);

        let value: serde_json::Value =
            serde_json::from_slice(&manifest.to_json_vec().unwrap()).unwrap();
        assert_eq!(value["abi"], crate::OPERATION_ABI);
        assert_eq!(value["operations"][0]["name"], "search");
        assert_eq!(value["operations"][0]["input"], "json");
        assert_eq!(value["operations"][0]["output"], "json");
        assert_eq!(value["operations"][0]["events"], "jsonl");
        assert_eq!(
            value["operations"][0]["required_capabilities"][1],
            "secret:EXAMPLE_API_KEY"
        );
    }

    #[test]
    fn http_request_serializes_to_verlet_http_abi() {
        let request = crate::HttpRequest::new("POST", "https://api.example.invalid/search")
            .header("content-type", "application/json")
            .secret_header("x-api-key", "EXAMPLE_API_KEY")
            .timeout_ms(5000)
            .max_response_bytes(2048);

        let value: serde_json::Value =
            serde_json::from_slice(&request.to_json_vec().unwrap()).unwrap();
        assert_eq!(value["abi"], crate::HTTP_ABI);
        assert_eq!(value["method"], "POST");
        assert_eq!(value["headers"][0][0], "content-type");
        assert_eq!(value["secret_headers"][0][1], "EXAMPLE_API_KEY");
    }

    #[test]
    fn coupling_discharge_serializes_to_verlet_coupling_abi() {
        let discharge = crate::CouplingDischarge::new(vec![crate::CouplingDischargeEvent {
            stream: "derived:counter".to_string(),
            kind: "placement.decision".to_string(),
            payload: serde_json::json!({"count": 3}),
            provenance: Some(serde_json::json!({"guest": "ignored"})),
        }]);

        let value: serde_json::Value =
            serde_json::from_slice(&discharge.to_json_vec().unwrap()).unwrap();
        assert_eq!(value["abi"], crate::COUPLING_DISCHARGE_ABI);
        assert_eq!(value["events"][0]["stream"], "derived:counter");
        assert_eq!(value["events"][0]["provenance"]["guest"], "ignored");
    }

    #[test]
    fn native_host_import_wrappers_fail_closed() {
        assert_eq!(
            crate::read_source(crate::Source(1), &mut [0u8; 4]).unwrap_err(),
            crate::StatusCode::TransportError
        );
        assert_eq!(
            crate::write_sink(crate::Sink(1), b"hello").unwrap_err(),
            crate::StatusCode::TransportError
        );
        assert_eq!(
            crate::check_cancelled(crate::Invocation(1)).unwrap_err(),
            crate::StatusCode::TransportError
        );
        assert_eq!(
            crate::open_file_read("/workspace/input.txt").unwrap_err(),
            crate::StatusCode::TransportError
        );
        assert_eq!(
            crate::read_file(crate::FileHandle(1), &mut [0u8; 4]).unwrap_err(),
            crate::StatusCode::TransportError
        );
        assert_eq!(
            crate::close_file(crate::FileHandle(1)).unwrap_err(),
            crate::StatusCode::TransportError
        );
    }
}
