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
    fn read_file(&self, path: &std::path::Path) -> Result<Vec<u8>, ToolFsError>;
    fn write_file(&self, path: &std::path::Path, content: &[u8]) -> Result<(), ToolFsError>;
    /// Create a directory. `recursive` = `mkdir -p`.
    fn mkdir(&self, path: &std::path::Path, recursive: bool) -> Result<(), ToolFsError>;
    fn stat(&self, path: &std::path::Path) -> Result<FileStat, ToolFsError>;
    fn read_dir(&self, path: &std::path::Path) -> Result<Vec<DirEntry>, ToolFsError>;
    fn exists(&self, path: &std::path::Path) -> Result<bool, ToolFsError>;
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
    NotFound(std::path::PathBuf),
    #[error("access denied: {0}")]
    Denied(std::path::PathBuf),
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

/// Native backend over the real filesystem, for standalone CLI bins and tests.
///
/// Relative paths resolve beneath the configured root. Absolute paths are
/// accepted only when they name the root or one of its descendants. The root
/// and every existing path component are canonicalized before access, so both
/// lexical `..` escapes and symlinks that leave the root return
/// [`ToolFsError::Denied`]. Missing descendants are resolved only after their
/// nearest existing ancestor has passed the same check.
#[cfg(feature = "std-fs")]
pub struct StdFs {
    pub root: std::path::PathBuf,
}

#[cfg(feature = "std-fs")]
impl StdFs {
    pub fn new(root: impl AsRef<std::path::Path>) -> Result<Self, ToolFsError> {
        let root = root.as_ref().to_path_buf();
        let fs = Self {
            root: lexical_normalize(&root),
        };
        fs.canonical_root()?;
        Ok(fs)
    }

    fn canonical_root(&self) -> Result<std::path::PathBuf, ToolFsError> {
        if !self.root.is_absolute() {
            return Err(ToolFsError::Denied(self.root.clone()));
        }

        let canonical_root =
            std::fs::canonicalize(&self.root).map_err(|error| map_io_error(&self.root, error))?;
        let metadata =
            std::fs::metadata(&canonical_root).map_err(|error| map_io_error(&self.root, error))?;
        if !metadata.is_dir() {
            return Err(ToolFsError::Io(
                "filesystem root is not a directory".to_owned(),
            ));
        }
        Ok(canonical_root)
    }

    fn resolve(&self, path: &std::path::Path) -> Result<std::path::PathBuf, ToolFsError> {
        let root = lexical_normalize(&self.root);
        let canonical_root = self.canonical_root()?;
        let normalized = if path.is_absolute() {
            lexical_normalize(path)
        } else {
            lexical_normalize(&root.join(path))
        };
        let relative = normalized
            .strip_prefix(&root)
            .or_else(|_| normalized.strip_prefix(&canonical_root))
            .map_err(|_| ToolFsError::Denied(path.to_path_buf()))?;

        let components = relative.iter().collect::<Vec<_>>();
        let mut resolved = canonical_root.clone();
        for (index, component) in components.iter().enumerate() {
            resolved.push(component);
            match std::fs::symlink_metadata(&resolved) {
                Ok(_) => {
                    resolved = std::fs::canonicalize(&resolved)
                        .map_err(|error| map_io_error(path, error))?;
                    if !resolved.starts_with(&canonical_root) {
                        return Err(ToolFsError::Denied(path.to_path_buf()));
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    for missing in &components[index + 1..] {
                        resolved.push(missing);
                    }
                    return Ok(resolved);
                }
                Err(error) => return Err(map_io_error(path, error)),
            }
        }

        Ok(resolved)
    }
}

#[cfg(feature = "std-fs")]
impl ToolFs for StdFs {
    fn read_file(&self, path: &std::path::Path) -> Result<Vec<u8>, ToolFsError> {
        let resolved = self.resolve(path)?;
        std::fs::read(resolved).map_err(|error| map_io_error(path, error))
    }

    fn write_file(&self, path: &std::path::Path, content: &[u8]) -> Result<(), ToolFsError> {
        let resolved = self.resolve(path)?;
        std::fs::write(resolved, content).map_err(|error| map_io_error(path, error))
    }

    fn mkdir(&self, path: &std::path::Path, recursive: bool) -> Result<(), ToolFsError> {
        let resolved = self.resolve(path)?;
        let result = if recursive {
            std::fs::create_dir_all(&resolved)
        } else {
            std::fs::create_dir(&resolved)
        };
        result.map_err(|error| map_io_error(path, error))?;

        let canonical =
            std::fs::canonicalize(&resolved).map_err(|error| map_io_error(path, error))?;
        if !canonical.starts_with(self.canonical_root()?) {
            return Err(ToolFsError::Denied(path.to_path_buf()));
        }
        Ok(())
    }

    fn stat(&self, path: &std::path::Path) -> Result<FileStat, ToolFsError> {
        let resolved = self.resolve(path)?;
        let metadata = std::fs::metadata(resolved).map_err(|error| map_io_error(path, error))?;
        Ok(FileStat {
            is_dir: metadata.is_dir(),
            is_file: metadata.is_file(),
            size: metadata.len(),
        })
    }

    fn read_dir(&self, path: &std::path::Path) -> Result<Vec<DirEntry>, ToolFsError> {
        let resolved = self.resolve(path)?;
        let entries = std::fs::read_dir(resolved).map_err(|error| map_io_error(path, error))?;
        let mut result = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error| map_io_error(path, error))?;
            let file_type = entry
                .file_type()
                .map_err(|error| map_io_error(path, error))?;
            result.push(DirEntry {
                name: entry.file_name().to_string_lossy().into_owned(),
                is_dir: file_type.is_dir(),
            });
        }
        result.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(result)
    }

    fn exists(&self, path: &std::path::Path) -> Result<bool, ToolFsError> {
        let resolved = self.resolve(path)?;
        match std::fs::metadata(resolved) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(map_io_error(path, error)),
        }
    }
}

#[cfg(feature = "std-fs")]
fn lexical_normalize(path: &std::path::Path) -> std::path::PathBuf {
    let mut normalized = std::path::PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            std::path::Component::RootDir => normalized.push(component.as_os_str()),
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            std::path::Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

#[cfg(feature = "std-fs")]
fn map_io_error(path: &std::path::Path, error: std::io::Error) -> ToolFsError {
    match error.kind() {
        std::io::ErrorKind::NotFound => ToolFsError::NotFound(path.to_path_buf()),
        std::io::ErrorKind::PermissionDenied => ToolFsError::Denied(path.to_path_buf()),
        _ => ToolFsError::Io(error.to_string()),
    }
}

/// Run one tool's native JSON-over-stdin CLI surface over [`StdFs`].
///
/// Input has the shape `{"root":"/absolute/path","args":{...}}`. Exactly one
/// JSON object is written to stdout: `{"ok":...}` on success or
/// `{"error":"..."}` on failure. The returned process exit status is zero
/// for success and one for every input, filesystem, tool, or output error.
#[cfg(feature = "std-fs")]
pub fn run_cli<Args, Output>(
    run: fn(Args, &dyn ToolFs) -> Result<Output, ToolError>,
) -> std::process::ExitCode
where
    Args: serde::de::DeserializeOwned,
    Output: serde::Serialize,
{
    #[derive(serde::Deserialize)]
    struct CliInput<Args> {
        root: std::path::PathBuf,
        args: Args,
    }

    let mut input = String::new();
    if let Err(error) = std::io::Read::read_to_string(&mut std::io::stdin(), &mut input) {
        return write_cli_error(format!("failed to read stdin: {error}"));
    }
    let input = match serde_json::from_str::<CliInput<Args>>(&input) {
        Ok(input) => input,
        Err(error) => return write_cli_error(format!("invalid input JSON: {error}")),
    };
    let fs = match StdFs::new(&input.root) {
        Ok(fs) => fs,
        Err(error) => return write_cli_error(error.to_string()),
    };

    match run(input.args, &fs) {
        Ok(output) => match serde_json::to_value(output) {
            Ok(output) => {
                let mut result = serde_json::Map::new();
                result.insert("ok".to_owned(), output);
                write_cli_json(serde_json::Value::Object(result), true)
            }
            Err(error) => write_cli_error(format!("failed to serialize result: {error}")),
        },
        Err(error) => write_cli_error(error.to_string()),
    }
}

#[cfg(feature = "std-fs")]
fn write_cli_error(error: String) -> std::process::ExitCode {
    write_cli_json(serde_json::json!({"error": error}), false)
}

#[cfg(feature = "std-fs")]
fn write_cli_json(value: serde_json::Value, success: bool) -> std::process::ExitCode {
    let mut bytes = match serde_json::to_vec(&value) {
        Ok(bytes) => bytes,
        Err(_) => return std::process::ExitCode::FAILURE,
    };
    bytes.push(b'\n');
    if std::io::Write::write_all(&mut std::io::stdout(), &bytes).is_err() {
        return std::process::ExitCode::FAILURE;
    }
    if success {
        std::process::ExitCode::SUCCESS
    } else {
        std::process::ExitCode::FAILURE
    }
}

#[cfg(all(test, feature = "std-fs"))]
mod tests {
    #[test]
    fn parent_escape_is_denied() {
        let outer = tempfile::tempdir().unwrap();
        let root = outer.path().join("root");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(outer.path().join("outside.txt"), "secret").unwrap();
        let fs = crate::StdFs { root };

        let error =
            crate::ToolFs::read_file(&fs, std::path::Path::new("../outside.txt")).unwrap_err();
        assert!(matches!(error, crate::ToolFsError::Denied(_)));
    }

    #[cfg(unix)]
    #[test]
    fn symlink_escape_is_denied() {
        let outer = tempfile::tempdir().unwrap();
        let root = outer.path().join("root");
        let outside = outer.path().join("outside");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("secret.txt"), "secret").unwrap();
        std::os::unix::fs::symlink(&outside, root.join("link")).unwrap();
        let fs = crate::StdFs::new(&root).unwrap();

        let error =
            crate::ToolFs::read_file(&fs, std::path::Path::new("link/secret.txt")).unwrap_err();
        assert!(matches!(error, crate::ToolFsError::Denied(_)));
    }
}
