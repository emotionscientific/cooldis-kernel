//! Shared surface for the standalone agent file tools.
//!
//! Every tool in `agent-tools/` is a pure function over a filesystem it
//! reaches only through [`ToolFs`]. The embedder picks the backend:
//!
//! - native: [`StdFs`] (feature `std-fs`) over the real filesystem,
//! - kernel: an adapter over the thread's verlet-vfs,
//! - wasm: an adapter over the guest ABI's fs imports.
//!
//! Tool crates must not depend on `std::fs`, tokio, or any host detail.
//! The trait is synchronous on purpose: wasm guests block on host calls,
//! and async hosts wrap tool runs in `spawn_blocking` at the boundary.

use std::path::{Path, PathBuf};

/// Hard cap on a single tool result, in bytes. Protects the record and the
/// wire, not the context window: context protection is the lossless spill
/// coupling's job, which is assumed present system-wide. Tools that hit
/// this cap return [`ToolError::ResultTooLarge`] rather than truncating
/// silently.
pub const MAX_RESULT_BYTES: usize = 4 * 1024 * 1024;

/// Minimal synchronous filesystem access for tool cores.
///
/// Backends enforce path confinement (workspace root, allowed roots):
/// a path outside the granted scope returns [`ToolFsError::Denied`].
/// Tool cores never re-check confinement.
pub trait ToolFs {
    fn read_file(&self, path: &Path) -> Result<Vec<u8>, ToolFsError>;
    fn write_file(&self, path: &Path, content: &[u8]) -> Result<(), ToolFsError>;
    /// Create a directory. `recursive` = `mkdir -p`.
    fn mkdir(&self, path: &Path, recursive: bool) -> Result<(), ToolFsError>;
    fn stat(&self, path: &Path) -> Result<FileStat, ToolFsError>;
    fn read_dir(&self, path: &Path) -> Result<Vec<DirEntry>, ToolFsError>;
    fn exists(&self, path: &Path) -> Result<bool, ToolFsError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileStat {
    pub is_dir: bool,
    pub is_file: bool,
    pub size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ToolFsError {
    #[error("not found: {0}")]
    NotFound(PathBuf),
    #[error("access denied: {0}")]
    Denied(PathBuf),
    #[error("{0}")]
    Io(String),
}

/// Errors a tool run can produce. The embedder renders these as the
/// tool-result error text the model sees; messages are written for the
/// model (actionable, no host paths beyond what the model supplied).
#[derive(Debug, thiserror::Error)]
pub enum ToolError {
    #[error(transparent)]
    Fs(#[from] ToolFsError),
    #[error("invalid arguments: {0}")]
    InvalidArgs(String),
    #[error("{0}")]
    Failed(String),
    #[error("result exceeds {MAX_RESULT_BYTES} bytes; narrow the request")]
    ResultTooLarge,
}

/// How a tool call composes with retries and replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EffectClass {
    /// No observable side effect. Safe to re-run any number of times.
    Pure,
    /// Re-running with identical arguments against the resulting state is
    /// a no-op (write with same content, edit already applied).
    Idempotent,
}

/// The model-facing contract of one tool. This is the single source of the
/// surface the model sees; packaging (native op, wasm op, remote executor)
/// carries it along unchanged.
#[derive(Clone, Debug)]
pub struct ToolContract {
    /// Default model-facing name. Manifests may attach under another name.
    pub name: &'static str,
    /// Model-facing description, verbatim.
    pub description: &'static str,
    /// JSON Schema for the arguments object.
    pub input_schema: serde_json::Value,
    pub effect_class: EffectClass,
}

/// Native backend over the real filesystem, for the standalone CLI bins
/// and tests. Confinement: paths are resolved under `root`; escapes deny.
#[cfg(feature = "std-fs")]
pub struct StdFs {
    pub root: PathBuf,
}

#[cfg(feature = "std-fs")]
impl ToolFs for StdFs {
    fn read_file(&self, _path: &Path) -> Result<Vec<u8>, ToolFsError> {
        todo!("EMO ticket: std::fs mapping with root confinement")
    }
    fn write_file(&self, _path: &Path, _content: &[u8]) -> Result<(), ToolFsError> {
        todo!("EMO ticket: std::fs mapping with root confinement")
    }
    fn mkdir(&self, _path: &Path, _recursive: bool) -> Result<(), ToolFsError> {
        todo!("EMO ticket: std::fs mapping with root confinement")
    }
    fn stat(&self, _path: &Path) -> Result<FileStat, ToolFsError> {
        todo!("EMO ticket: std::fs mapping with root confinement")
    }
    fn read_dir(&self, _path: &Path) -> Result<Vec<DirEntry>, ToolFsError> {
        todo!("EMO ticket: std::fs mapping with root confinement")
    }
    fn exists(&self, _path: &Path) -> Result<bool, ToolFsError> {
        todo!("EMO ticket: std::fs mapping with root confinement")
    }
}
