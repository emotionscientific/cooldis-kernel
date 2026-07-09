//! Guest-side helpers for Cooldis ABI Wasm operations.
//!
//! This crate is intentionally small: it gives Rust guests the canonical
//! manifest/request shapes plus thin wrappers over the `cooldis_0.1` host ABI.
//! The blessed authoring pattern is a normal Rust `cdylib` crate compiled to
//! `wasm32-unknown-unknown`; see
//! `crates/cooldis-kernel/tests/fixtures/wasm-vfs-tools` in the workspace for
//! the `cat` / `tail` fixture.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

mod contract;
pub mod testkit;

pub use contract::{CouplingContext, Discharge, GuestError, OperationContext};
pub use cooldis_guest_sdk_macros::{coupling, operation};

/// One-line import for guest crates:
/// `use cooldis_guest_sdk::prelude::*;`
pub mod prelude {
    pub use crate::contract::{CouplingContext, Discharge, GuestError, OperationContext};
    pub use crate::{coupling, operation};
    pub use serde::{Deserialize, Serialize};
}

#[doc(hidden)]
pub mod __private {
    use serde::Serialize;
    use serde::de::DeserializeOwned;

    use crate::{
        COUPLING_INVOCATION_ABI, CouplingContext, Discharge, GuestError, OperationManifest, Sink,
        Source, StatusCode, read_source_to_end, write_sink,
    };

    pub fn read_json_input<T: DeserializeOwned>(source: Source) -> Result<T, GuestError> {
        let bytes = read_source_to_end(source).map_err(GuestError::Host)?;
        serde_json::from_slice(&bytes)
            .map_err(|err| GuestError::BadInput(format!("operation input: {err}")))
    }

    pub fn write_json_output<T: Serialize>(sink: Sink, value: &T) -> Result<(), GuestError> {
        let bytes = serde_json::to_vec(value)
            .map_err(|err| GuestError::Internal(format!("operation output: {err}")))?;
        write_sink(sink, &bytes).map_err(GuestError::Host)?;
        Ok(())
    }

    pub fn write_manifest(sink: Sink, manifest: &OperationManifest) -> Result<(), GuestError> {
        let bytes = manifest
            .to_json_vec()
            .map_err(|err| GuestError::Internal(format!("operation manifest: {err}")))?;
        write_sink(sink, &bytes).map_err(GuestError::Host)?;
        Ok(())
    }

    pub fn read_coupling_context(source: Source) -> Result<CouplingContext, GuestError> {
        let bytes = read_source_to_end(source).map_err(GuestError::Host)?;
        let invocation = crate::CouplingInvocation::from_json_slice(&bytes)
            .map_err(|err| GuestError::BadInput(format!("coupling invocation: {err}")))?;
        if invocation.abi != COUPLING_INVOCATION_ABI {
            return Err(GuestError::BadInput(format!(
                "coupling invocation abi {:?} is not {COUPLING_INVOCATION_ABI:?}",
                invocation.abi
            )));
        }
        Ok(CouplingContext::from_invocation(invocation))
    }

    pub fn write_coupling_discharge_output(
        sink: Sink,
        discharge: Discharge,
    ) -> Result<(), GuestError> {
        let bytes = discharge
            .into_coupling_discharge()
            .to_json_vec()
            .map_err(|err| GuestError::Internal(format!("coupling discharge: {err}")))?;
        write_sink(sink, &bytes).map_err(GuestError::Host)?;
        Ok(())
    }

    pub fn status_from_guest_result(result: Result<(), GuestError>) -> i32 {
        match result {
            Ok(()) => crate::STATUS_OK,
            Err(err) => status_from_guest_error(err),
        }
    }

    fn status_from_guest_error(err: GuestError) -> i32 {
        match err {
            GuestError::BadInput(_) => crate::STATUS_INVALID_ARGUMENT,
            GuestError::Unsupported(_) => crate::STATUS_NOT_FOUND,
            GuestError::Host(status) => status.as_raw(),
            GuestError::Internal(_) => StatusCode::TransportError.as_raw(),
        }
    }
}

/// ABI identifier for manifest-bearing Cooldis Wasm operations.
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
#[cfg(target_arch = "wasm32")]
const STATUS_LEGACY_SOURCE_EOF: i32 = -1;

/// Status values returned by Cooldis host imports and exports.
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
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
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
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct OperationDefinition {
    /// Stable numeric id used by `__cooldis_call_operation__`.
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
    pub config: JsonValue,
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
    pub payload: JsonValue,
}

/// Kernel metadata attached to a coupling invocation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CouplingInvocationMeta {
    /// Coupling id from the manifest binding.
    pub coupling_id: String,
    /// Thread id where the coupling fired.
    pub thread_id: String,
    /// Coupling recursion depth.
    pub depth: u32,
}

/// Output envelope for a custom coupling Wasm guest.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CouplingDischargeEvent {
    /// Target event stream.
    pub stream: String,
    /// Target event kind.
    pub kind: String,
    #[serde(default)]
    /// Event payload.
    pub payload: JsonValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional guest-supplied provenance; the kernel stamps authoritative provenance.
    pub provenance: Option<JsonValue>,
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
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
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
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationEventKind {
    /// The operation does not emit events.
    #[default]
    None,
    /// The operation emits newline-delimited JSON events.
    Jsonl,
}

/// Execution mode declared by an operation manifest entry.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
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

/// HTTP request envelope passed to the Cooldis host import.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
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

/// HTTP response metadata returned by the Cooldis host import.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
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

/// Close a VFS file handle.
pub fn close_file(handle: FileHandle) -> Result<(), StatusCode> {
    call_fs_close(handle).into_result()
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_serializes_to_cooldis_operation_abi() {
        let manifest = OperationManifest::new(vec![
            OperationDefinition::new(1, "search")
                .json_input()
                .json_output()
                .jsonl_events()
                .require("net.http:POST:https://api.example.invalid")
                .require("secret:EXAMPLE_API_KEY"),
        ]);

        let value: serde_json::Value =
            serde_json::from_slice(&manifest.to_json_vec().unwrap()).unwrap();
        assert_eq!(value["abi"], OPERATION_ABI);
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
    fn http_request_serializes_to_cooldis_http_abi() {
        let request = HttpRequest::new("POST", "https://api.example.invalid/search")
            .header("content-type", "application/json")
            .secret_header("x-api-key", "EXAMPLE_API_KEY")
            .timeout_ms(5000)
            .max_response_bytes(2048);

        let value: serde_json::Value =
            serde_json::from_slice(&request.to_json_vec().unwrap()).unwrap();
        assert_eq!(value["abi"], HTTP_ABI);
        assert_eq!(value["method"], "POST");
        assert_eq!(value["headers"][0][0], "content-type");
        assert_eq!(value["secret_headers"][0][1], "EXAMPLE_API_KEY");
    }

    #[test]
    fn coupling_discharge_serializes_to_cooldis_coupling_abi() {
        let discharge = CouplingDischarge::new(vec![CouplingDischargeEvent {
            stream: "derived:counter".to_string(),
            kind: "placement.decision".to_string(),
            payload: serde_json::json!({"count": 3}),
            provenance: Some(serde_json::json!({"guest": "ignored"})),
        }]);

        let value: serde_json::Value =
            serde_json::from_slice(&discharge.to_json_vec().unwrap()).unwrap();
        assert_eq!(value["abi"], COUPLING_DISCHARGE_ABI);
        assert_eq!(value["events"][0]["stream"], "derived:counter");
        assert_eq!(value["events"][0]["provenance"]["guest"], "ignored");
    }

    #[test]
    fn native_host_import_wrappers_fail_closed() {
        assert_eq!(
            read_source(Source(1), &mut [0u8; 4]).unwrap_err(),
            StatusCode::TransportError
        );
        assert_eq!(
            write_sink(Sink(1), b"hello").unwrap_err(),
            StatusCode::TransportError
        );
        assert_eq!(
            check_cancelled(Invocation(1)).unwrap_err(),
            StatusCode::TransportError
        );
        assert_eq!(
            open_file_read("/workspace/input.txt").unwrap_err(),
            StatusCode::TransportError
        );
        assert_eq!(
            read_file(FileHandle(1), &mut [0u8; 4]).unwrap_err(),
            StatusCode::TransportError
        );
        assert_eq!(
            close_file(FileHandle(1)).unwrap_err(),
            StatusCode::TransportError
        );
    }
}
