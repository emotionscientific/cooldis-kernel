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

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use verlet_tool_core::{EffectClass, ToolContract, ToolError, ToolFs};

#[derive(Clone, Debug, Deserialize)]
pub struct ReadArgs {
    /// Path to the file to read (relative to the workspace root or absolute
    /// within the granted scope).
    pub path: PathBuf,
    /// Line number to start reading from (1-indexed).
    pub offset: Option<u64>,
    /// Maximum number of lines to read.
    pub limit: Option<u64>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
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

pub fn contract() -> ToolContract {
    ToolContract {
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
        effect_class: EffectClass::Pure,
    }
}

pub fn run(_args: ReadArgs, _fs: &dyn ToolFs) -> Result<ReadOutput, ToolError> {
    todo!("EMO ticket: port Pi read semantics per module docs")
}
