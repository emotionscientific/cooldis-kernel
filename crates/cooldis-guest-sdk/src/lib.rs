//! Guest-side helpers for Cooldis ABI Wasm operations.
//!
//! This crate is intentionally small: it gives Rust guests the canonical
//! manifest/request shapes plus thin wrappers over the `cooldis_0.1` host ABI.
//! The blessed authoring pattern is a normal Rust `cdylib` crate compiled to
//! `wasm32-unknown-unknown`; see
//! `crates/cooldis-kernel/tests/fixtures/wasm-vfs-tools` in the workspace for
//! the `cat` / `tail` fixture.

use serde::{Deserialize, Serialize};

pub const OPERATION_ABI: &str = "cooldis.operation/0.1";
pub const HTTP_ABI: &str = "cooldis.net.http/0.1";
pub const STATUS_OK: i32 = 0;
pub const STATUS_INVALID_ARGUMENT: i32 = 1;
pub const STATUS_NOT_FOUND: i32 = 2;
pub const STATUS_CAPABILITY_DENIED: i32 = 3;
pub const STATUS_TRANSPORT_ERROR: i32 = 4;
pub const STATUS_TIMEOUT: i32 = 5;
pub const STATUS_CANCELLED: i32 = 6;
pub const STATUS_EOF: i32 = 7;
pub const FS_MODE_READ: u32 = 0;
#[cfg(target_arch = "wasm32")]
const STATUS_LEGACY_SOURCE_EOF: i32 = -1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatusCode {
    Ok,
    InvalidArgument,
    NotFound,
    CapabilityDenied,
    TransportError,
    Timeout,
    Cancelled,
    Eof,
    Unknown(i32),
}

impl StatusCode {
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

    pub fn into_result(self) -> Result<(), Self> {
        match self {
            Self::Ok => Ok(()),
            err => Err(err),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct OperationManifest {
    pub abi: String,
    pub operations: Vec<OperationDefinition>,
}

impl OperationManifest {
    pub fn new(operations: Vec<OperationDefinition>) -> Self {
        Self {
            abi: OPERATION_ABI.to_string(),
            operations,
        }
    }

    pub fn to_json_vec(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct OperationDefinition {
    pub id: u32,
    pub name: String,
    #[serde(default)]
    pub input: OperationValueKind,
    #[serde(default)]
    pub output: OperationValueKind,
    #[serde(default)]
    pub events: OperationEventKind,
    #[serde(default)]
    pub mode: OperationMode,
    #[serde(default)]
    pub required_capabilities: Vec<String>,
}

impl OperationDefinition {
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

    pub fn json_input(mut self) -> Self {
        self.input = OperationValueKind::Json;
        self
    }

    pub fn json_output(mut self) -> Self {
        self.output = OperationValueKind::Json;
        self
    }

    pub fn jsonl_events(mut self) -> Self {
        self.events = OperationEventKind::Jsonl;
        self
    }

    pub fn mode(mut self, mode: OperationMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn require(mut self, capability: impl Into<String>) -> Self {
        self.required_capabilities.push(capability.into());
        self
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationValueKind {
    #[default]
    Bytes,
    Text,
    Json,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationEventKind {
    #[default]
    None,
    Jsonl,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationMode {
    #[default]
    Sync,
    Async,
    Streaming,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct HttpRequest {
    pub abi: String,
    pub method: String,
    pub url: String,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    #[serde(default)]
    pub secret_headers: Vec<(String, String)>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub max_response_bytes: Option<usize>,
}

impl HttpRequest {
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

    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    pub fn secret_header(mut self, name: impl Into<String>, secret: impl Into<String>) -> Self {
        self.secret_headers.push((name.into(), secret.into()));
        self
    }

    pub fn timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }

    pub fn max_response_bytes(mut self, max_response_bytes: usize) -> Self {
        self.max_response_bytes = Some(max_response_bytes);
        self
    }

    pub fn to_json_vec(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct HttpResponse {
    pub abi: String,
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub truncated: bool,
    pub elapsed_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Source(pub u32);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Sink(pub u32);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventSink(pub u32);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Invocation(pub u32);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileHandle(pub u32);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpResponseSources {
    pub metadata: Source,
    pub body: Source,
}

pub fn read_source(source: Source, buffer: &mut [u8]) -> Result<usize, StatusCode> {
    call_source_read(source, buffer)
}

pub fn write_sink(sink: Sink, bytes: &[u8]) -> Result<usize, StatusCode> {
    call_sink_write(sink, bytes)
}

pub fn emit_event(
    invocation: Invocation,
    event_sink: EventSink,
    bytes: &[u8],
) -> Result<usize, StatusCode> {
    call_event_emit(invocation, event_sink, bytes)
}

pub fn http_request(
    invocation: Invocation,
    request: &[u8],
    body: &[u8],
    event_sink: EventSink,
) -> Result<HttpResponseSources, StatusCode> {
    call_http_request(invocation, request, body, event_sink)
}

pub fn check_cancelled(invocation: Invocation) -> Result<(), StatusCode> {
    call_check_cancelled(invocation).into_result()
}

pub fn open_file_read(path: &str) -> Result<FileHandle, StatusCode> {
    call_fs_open(path, FS_MODE_READ)
}

pub fn read_file(handle: FileHandle, buffer: &mut [u8]) -> Result<usize, StatusCode> {
    call_fs_read(handle, buffer)
}

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
