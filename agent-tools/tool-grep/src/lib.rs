//! `grep` — search file contents for a pattern.
//!
//! Pi's version spawns `rg --json` (auto-downloading ripgrep when
//! missing). Ours runs ripgrep's engine in-process — the `grep-regex` and
//! `grep-searcher` crates search over any reader — fed by the same
//! `ToolFs` walk as `tool-glob`. No spawned binaries, no downloads, and
//! the whole reason this exists as a function tool is preserved: the
//! pattern arrives as a JSON string, no shell quoting, and the output is
//! bounded `file:line` rows.
//!
//! Pinned semantics (mirroring Pi's flags):
//! - Regex by default; `literal: true` = fixed-string. `ignore_case`
//!   as named. Same walk rules as `tool-glob` (gitignore, hidden files
//!   included, `.git/` skipped), with optional `glob` file filter.
//! - Output rows: `path:line: text` with root-relative paths; `context`
//!   adds N lines before/after with `path:line- text` separators, blocks
//!   joined by `--` lines (rg's grouped format).
//! - Match lines longer than 500 chars are clipped with `…`.
//! - `limit` (default 100) caps match count; search stops early when
//!   reached and the result carries `limit_reached`.
//! - Binary files (NUL in first 8KB) are skipped.
//! - Deterministic file order (same as glob's sort), so identical state
//!   yields identical output on every backend.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use verlet_tool_core::{EffectClass, ToolContract, ToolError, ToolFs};

pub const DEFAULT_LIMIT: u64 = 100;
pub const MAX_LINE_CHARS: usize = 500;

#[derive(Clone, Debug, Deserialize)]
pub struct GrepArgs {
    /// Search pattern (regex, or literal string with `literal: true`).
    pub pattern: String,
    /// Directory or file to search (default: workspace root).
    pub path: Option<PathBuf>,
    /// Filter files by glob pattern, e.g. `*.ts`.
    pub glob: Option<String>,
    #[serde(default)]
    pub ignore_case: bool,
    #[serde(default)]
    pub literal: bool,
    /// Lines of context before and after each match (default 0).
    pub context: Option<u64>,
    /// Maximum number of matches (default 100).
    pub limit: Option<u64>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct GrepOutput {
    /// Rendered match rows (`path:line: text`).
    pub text: String,
    pub match_count: u64,
    pub limit_reached: bool,
}

pub fn contract() -> ToolContract {
    ToolContract {
        name: "grep",
        description: "Search file contents for a pattern. Returns matching lines \
                      with file paths and line numbers. Regex by default; set \
                      literal for exact strings. Respects .gitignore. Stops at \
                      the match limit and says so.",
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "description": "Search pattern (regex or literal string)"},
                "path": {"type": "string", "description": "Directory or file to search (default: current directory)"},
                "glob": {"type": "string", "description": "Filter files by glob pattern, e.g. '*.ts' or '**/*.spec.ts'"},
                "ignore_case": {"type": "boolean", "description": "Case-insensitive search (default: false)"},
                "literal": {"type": "boolean", "description": "Treat pattern as literal string instead of regex (default: false)"},
                "context": {"type": "number", "description": "Number of lines to show before and after each match (default: 0)"},
                "limit": {"type": "number", "description": "Maximum number of matches to return (default: 100)"}
            },
            "required": ["pattern"]
        }),
        effect_class: EffectClass::Pure,
    }
}

pub fn run(_args: GrepArgs, _fs: &dyn ToolFs) -> Result<GrepOutput, ToolError> {
    todo!("EMO ticket: grep-searcher over ToolFs walk per module docs")
}
