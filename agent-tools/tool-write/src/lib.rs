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

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use verlet_tool_core::{EffectClass, ToolContract, ToolError, ToolFs};

#[derive(Clone, Debug, Deserialize)]
pub struct WriteArgs {
    /// Path of the file to write (relative to the workspace root or
    /// absolute within the granted scope).
    pub path: PathBuf,
    /// Full file content.
    pub content: String,
    /// Must be `true` to replace an existing file. Default: false.
    #[serde(default)]
    pub overwrite: bool,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct WriteOutput {
    pub bytes_written: u64,
    /// True when an existing file was replaced (only possible with
    /// `overwrite: true`).
    pub replaced: bool,
}

pub fn contract() -> ToolContract {
    ToolContract {
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
        effect_class: EffectClass::Idempotent,
    }
}

pub fn run(_args: WriteArgs, _fs: &dyn ToolFs) -> Result<WriteOutput, ToolError> {
    todo!("EMO ticket: implement per module docs")
}
