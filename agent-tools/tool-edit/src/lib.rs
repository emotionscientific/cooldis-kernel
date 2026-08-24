//! `edit` — replace exact text spans in one file.
//!
//! Ported from Pi's edit tool (`core/tools/edit.ts`, `edit-diff.ts`).
//! This is the tool with the tricky logic; the implementation ticket ports
//! Pi's test fixtures alongside it.
//!
//! Pinned semantics:
//! - `edits` apply as one atomic batch: all succeed or the file is
//!   untouched.
//! - Each `old_text` must match exactly once. Match strategy per Pi:
//!   exact match first, then a normalized pass (NFKC, smart quotes to
//!   ASCII, en/em dashes to hyphens, trailing whitespace stripped per
//!   line) that must still be unique.
//! - Zero matches, multiple matches, or overlapping edit spans are
//!   errors naming the offending `old_text` (first 100 chars).
//! - `old_text == new_text` is an error ("no change").
//! - Result returns a unified diff of the applied change; the model uses
//!   it to confirm the edit landed where intended.
//! - Line endings: file's existing endings are preserved; `old_text`
//!   matching normalizes CRLF to LF for comparison.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use verlet_tool_core::{EffectClass, ToolContract, ToolError, ToolFs};

#[derive(Clone, Debug, Deserialize)]
pub struct EditArgs {
    /// Path of the file to edit (relative to the workspace root or
    /// absolute within the granted scope).
    pub path: PathBuf,
    /// Replacements to apply as one atomic batch.
    pub edits: Vec<Edit>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Edit {
    /// Text to find. Must match exactly one location in the file.
    pub old_text: String,
    /// Replacement text.
    pub new_text: String,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct EditOutput {
    /// Unified diff of the applied change.
    pub diff: String,
    pub edits_applied: u32,
}

pub fn contract() -> ToolContract {
    ToolContract {
        name: "edit",
        description: "Edit a file by replacing exact text. Each old_text must \
                      match exactly one location; all edits in one call apply \
                      atomically. Returns a diff of the change. Read the file \
                      first and copy old_text exactly, including whitespace.",
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Path of the file to edit (relative or absolute)"},
                "edits": {
                    "type": "array",
                    "description": "Replacements to apply atomically",
                    "items": {
                        "type": "object",
                        "properties": {
                            "old_text": {"type": "string", "description": "Text to find (must be unique in the file)"},
                            "new_text": {"type": "string", "description": "Replacement text"}
                        },
                        "required": ["old_text", "new_text"]
                    },
                    "minItems": 1
                }
            },
            "required": ["path", "edits"]
        }),
        effect_class: EffectClass::Idempotent,
    }
}

pub fn run(_args: EditArgs, _fs: &dyn ToolFs) -> Result<EditOutput, ToolError> {
    todo!("EMO ticket: port Pi edit semantics + fixtures per module docs")
}
