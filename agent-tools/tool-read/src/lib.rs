//! `read` — line-range file access.
//!
//! Ported from Pi's read tool (`core/tools/read.ts`), slimmed per design:
//!
//! - **No image branch.** Image viewing is a separate optional package.
//! - **No smart truncation messaging.** The lossless spill coupling owns
//!   context protection. `read` keeps `offset`/`limit` because line
//!   addressing is the primitive that makes spill files (and any large
//!   file) walkable; the result reports the range returned and the total
//!   line count so the model can continue on its own.
//!
//! Pinned semantics (implementation ticket must match, with fixtures):
//! - `offset` is 1-indexed; `offset` past end of file is an error naming
//!   the file's total line count.
//! - No `limit` → to end of file, subject to `MAX_RESULT_BYTES` (error,
//!   not silent truncation, when exceeded — the model narrows with
//!   offset/limit).
//! - Content is returned verbatim (no line numbering, no tab expansion);
//!   UTF-8 with lossy replacement for invalid bytes.
//! - Trailer line `[lines {start}-{end} of {total}]` is appended after a
//!   blank line whenever the returned range is not the whole file.
//! - Line accounting follows Pi's newline split: an empty file is one empty
//!   logical line, and a terminal newline creates a final empty line.
//! - The result byte cap applies to the final rendered text, after lossy UTF-8
//!   conversion and after adding any partial-range trailer.

#[derive(Clone, Debug, serde::Deserialize)]
pub struct ReadArgs {
    /// Path to the file to read (relative to the workspace root or absolute
    /// within the granted scope).
    pub path: std::path::PathBuf,
    /// Line number to start reading from (1-indexed).
    pub offset: Option<u64>,
    /// Maximum number of lines to read.
    pub limit: Option<u64>,
}

#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq)]
pub struct ReadOutput {
    /// File content for the selected range, verbatim, plus the range
    /// trailer when partial.
    pub text: String,
    /// 1-indexed first line returned.
    pub start_line: u64,
    /// 1-indexed last line returned.
    pub end_line: u64,
    /// Total lines in the file.
    pub total_lines: u64,
}

pub fn contract() -> verlet_tool_core::ToolContract {
    verlet_tool_core::ToolContract {
        name: "read",
        description: "Read the contents of a text file. Returns the file verbatim. \
                      For large files use offset (1-indexed start line) and limit \
                      (max lines) to read in ranges; the result reports which lines \
                      of how many were returned.",
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Path to the file to read (relative or absolute)"},
                "offset": {"type": "number", "description": "Line number to start reading from (1-indexed)"},
                "limit": {"type": "number", "description": "Maximum number of lines to read"}
            },
            "required": ["path"]
        }),
        effect_class: verlet_tool_core::EffectClass::Pure,
    }
}

pub fn run(
    args: ReadArgs,
    fs: &dyn verlet_tool_core::ToolFs,
) -> Result<ReadOutput, verlet_tool_core::ToolError> {
    let offset = args.offset.unwrap_or(1);
    if offset == 0 {
        return Err(verlet_tool_core::ToolError::InvalidArgs(
            "offset must be at least 1".to_owned(),
        ));
    }
    if args.limit == Some(0) {
        return Err(verlet_tool_core::ToolError::InvalidArgs(
            "limit must be at least 1".to_owned(),
        ));
    }

    let bytes = fs.read_file(&args.path)?;
    let content = String::from_utf8_lossy(&bytes);
    let lines = content.split('\n').collect::<Vec<_>>();
    let total_lines = u64::try_from(lines.len()).unwrap_or(u64::MAX);

    if offset > total_lines {
        return Err(offset_past_end(offset, total_lines));
    }

    let start_index = usize::try_from(offset - 1)
        .map_err(|_| verlet_tool_core::ToolError::InvalidArgs("offset is too large".to_owned()))?;
    let available = lines.len() - start_index;
    let selected_count = match args.limit {
        Some(limit) => usize::try_from(limit).unwrap_or(usize::MAX).min(available),
        None => available,
    };
    let end_index = start_index + selected_count;
    let mut text = lines[start_index..end_index].join("\n");
    let end_line = u64::try_from(end_index).unwrap_or(u64::MAX);

    if offset != 1 || end_line != total_lines {
        if text.ends_with('\n') {
            text.push('\n');
        } else {
            text.push_str("\n\n");
        }
        text.push_str(&format!("[lines {offset}-{end_line} of {total_lines}]"));
    }
    if text.len() > verlet_tool_core::MAX_RESULT_BYTES {
        return Err(verlet_tool_core::ToolError::ResultTooLarge);
    }

    Ok(ReadOutput {
        text,
        start_line: offset,
        end_line,
        total_lines,
    })
}

fn offset_past_end(offset: u64, total_lines: u64) -> verlet_tool_core::ToolError {
    verlet_tool_core::ToolError::Failed(format!(
        "offset {offset} is past end of file ({total_lines} total lines)"
    ))
}

#[cfg(test)]
mod tests {
    fn fs(root: &std::path::Path) -> verlet_tool_core::StdFs {
        verlet_tool_core::StdFs::new(root).unwrap()
    }

    fn args(path: &str, offset: Option<u64>, limit: Option<u64>) -> crate::ReadArgs {
        crate::ReadArgs {
            path: std::path::PathBuf::from(path),
            offset,
            limit,
        }
    }

    #[test]
    fn reads_a_whole_file_verbatim() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "first\nsecond\n").unwrap();

        let output = crate::run(args("file.txt", None, None), &fs(root.path())).unwrap();

        assert_eq!(
            output,
            crate::ReadOutput {
                text: "first\nsecond\n".to_owned(),
                start_line: 1,
                end_line: 3,
                total_lines: 3,
            }
        );
    }

    #[test]
    fn reads_an_offset_and_limit_window_with_a_trailer() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "one\ntwo\nthree\nfour").unwrap();

        let output = crate::run(args("file.txt", Some(2), Some(2)), &fs(root.path())).unwrap();

        assert_eq!(
            output,
            crate::ReadOutput {
                text: "two\nthree\n\n[lines 2-3 of 4]".to_owned(),
                start_line: 2,
                end_line: 3,
                total_lines: 4,
            }
        );
    }

    #[test]
    fn rejects_an_offset_past_the_end_and_names_the_total() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "one\ntwo").unwrap();

        let error = crate::run(args("file.txt", Some(3), None), &fs(root.path())).unwrap_err();

        assert_eq!(
            error.to_string(),
            "offset 3 is past end of file (2 total lines)"
        );
    }

    #[test]
    fn reads_an_empty_file() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("empty.txt"), "").unwrap();

        let output = crate::run(args("empty.txt", None, None), &fs(root.path())).unwrap();

        assert_eq!(
            output,
            crate::ReadOutput {
                text: String::new(),
                start_line: 1,
                end_line: 1,
                total_lines: 1,
            }
        );
    }

    #[test]
    fn rejects_a_result_larger_than_the_cap() {
        let root = tempfile::tempdir().unwrap();
        let content = vec![b'x'; verlet_tool_core::MAX_RESULT_BYTES + 1];
        std::fs::write(root.path().join("large.txt"), content).unwrap();

        let error = crate::run(args("large.txt", None, None), &fs(root.path())).unwrap_err();

        assert!(matches!(error, verlet_tool_core::ToolError::ResultTooLarge));
    }

    #[test]
    fn replaces_invalid_utf8_lossily() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("invalid.txt"), [b'a', 0xff, b'\n']).unwrap();

        let output = crate::run(args("invalid.txt", None, None), &fs(root.path())).unwrap();

        assert_eq!(output.text, "a\u{fffd}\n");
    }

    #[test]
    fn rejects_zero_offset_and_limit() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "content").unwrap();
        let fs = fs(root.path());

        let offset_error = crate::run(args("file.txt", Some(0), None), &fs).unwrap_err();
        let limit_error = crate::run(args("file.txt", None, Some(0)), &fs).unwrap_err();

        assert_eq!(
            offset_error.to_string(),
            "invalid arguments: offset must be at least 1"
        );
        assert_eq!(
            limit_error.to_string(),
            "invalid arguments: limit must be at least 1"
        );
    }
}
