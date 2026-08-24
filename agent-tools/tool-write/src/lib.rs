//! `write` — create or overwrite a file, with an overwrite guard.
//!
//! Ported from Pi's write tool (`core/tools/write.ts`) with one deliberate
//! deviation: Pi's write overwrites unconditionally; ours refuses to
//! overwrite an existing file unless the model says so.
//!
//! Pinned semantics:
//! - Parent directories are created as needed (`mkdir -p`).
//! - If the target exists and `overwrite` is not `true`, the call fails
//!   with an error stating the file exists, its size, and that the model
//!   should read it first and pass `overwrite: true` to replace it. This
//!   is the stateless half of clobber protection; the stateful half
//!   ("was this path actually read this thread?") is a controller
//!   coupling on the record, out of scope here.
//! - Content is written exactly as given (no trailing-newline fixups).
//! - Result reports bytes written and whether a file was created or
//!   replaced.

#[derive(Clone, Debug, serde::Deserialize)]
pub struct WriteArgs {
    /// Path of the file to write (relative to the workspace root or
    /// absolute within the granted scope).
    pub path: std::path::PathBuf,
    /// Full file content.
    pub content: String,
    /// Must be `true` to replace an existing file. Default: false.
    #[serde(default)]
    pub overwrite: bool,
}

#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq)]
pub struct WriteOutput {
    pub bytes_written: u64,
    /// True when an existing file was replaced (only possible with
    /// `overwrite: true`).
    pub replaced: bool,
}

pub fn contract() -> verlet_tool_core::ToolContract {
    verlet_tool_core::ToolContract {
        name: "write",
        description: "Write content to a file, creating parent directories as \
                      needed. Fails if the file already exists unless overwrite \
                      is true; read the existing file before overwriting it.",
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Path of the file to write (relative or absolute)"},
                "content": {"type": "string", "description": "Full content to write"},
                "overwrite": {"type": "boolean", "description": "Set true to replace an existing file (default false)"}
            },
            "required": ["path", "content"]
        }),
        effect_class: verlet_tool_core::EffectClass::Idempotent,
    }
}

pub fn run(
    args: WriteArgs,
    fs: &dyn verlet_tool_core::ToolFs,
) -> Result<WriteOutput, verlet_tool_core::ToolError> {
    let replaced = fs.exists(&args.path)?;
    if replaced && !args.overwrite {
        let stat = fs.stat(&args.path)?;
        return Err(verlet_tool_core::ToolError::Failed(format!(
            "file {} exists ({} bytes); read it first and pass overwrite: true to replace it",
            args.path.display(),
            stat.size
        )));
    }

    if let Some(parent) = args.path.parent() {
        if !parent.as_os_str().is_empty() {
            fs.mkdir(parent, true)?;
        }
    }
    fs.write_file(&args.path, args.content.as_bytes())?;

    Ok(WriteOutput {
        bytes_written: u64::try_from(args.content.len()).unwrap_or(u64::MAX),
        replaced,
    })
}

#[cfg(test)]
mod tests {
    fn fs(root: &std::path::Path) -> verlet_tool_core::StdFs {
        verlet_tool_core::StdFs::new(root).unwrap()
    }

    fn args(path: &str, content: &str, overwrite: bool) -> crate::WriteArgs {
        crate::WriteArgs {
            path: std::path::PathBuf::from(path),
            content: content.to_owned(),
            overwrite,
        }
    }

    #[test]
    fn creates_a_file_and_nested_parents() {
        let root = tempfile::tempdir().unwrap();

        let output = crate::run(
            args("nested/deeper/file.txt", "exact content", false),
            &fs(root.path()),
        )
        .unwrap();

        assert_eq!(
            output,
            crate::WriteOutput {
                bytes_written: 13,
                replaced: false,
            }
        );
        assert_eq!(
            std::fs::read(root.path().join("nested/deeper/file.txt")).unwrap(),
            b"exact content"
        );
    }

    #[test]
    fn refuses_an_existing_file_without_overwrite() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "old").unwrap();

        let error = crate::run(args("file.txt", "new", false), &fs(root.path())).unwrap_err();

        assert_eq!(
            error.to_string(),
            "file file.txt exists (3 bytes); read it first and pass overwrite: true to replace it"
        );
        assert_eq!(std::fs::read(root.path().join("file.txt")).unwrap(), b"old");
    }

    #[test]
    fn replaces_an_existing_file_when_allowed() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "old").unwrap();

        let output = crate::run(args("file.txt", "replacement", true), &fs(root.path())).unwrap();

        assert_eq!(
            output,
            crate::WriteOutput {
                bytes_written: 11,
                replaced: true,
            }
        );
        assert_eq!(
            std::fs::read(root.path().join("file.txt")).unwrap(),
            b"replacement"
        );
    }

    #[test]
    fn reports_utf8_bytes_written() {
        let root = tempfile::tempdir().unwrap();

        let output = crate::run(args("unicode.txt", "é", false), &fs(root.path())).unwrap();

        assert_eq!(output.bytes_written, 2);
        assert!(!output.replaced);
    }
}
