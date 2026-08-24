//! `glob` — find files by glob pattern (Pi calls this `find`).
//!
//! Pi's version shells out to `fd`; ours walks through [`ToolFs`] with
//! `globset` matching so every backend (native, vfs, wasm) traverses
//! identically. Gitignore awareness comes from parsing `.gitignore`
//! files with the `ignore` crate's buffer-based gitignore module during
//! the walk — not from the `ignore` crate's walker, which is welded to
//! `std::fs`.
//!
//! Pinned semantics:
//! - Pattern syntax: `globset` defaults with `**` support, matched
//!   against paths relative to the search root, `/`-separated.
//! - Respects `.gitignore` (per directory, nested, plus root
//!   `.git/info/exclude`); hidden files are included; `.git/` is always
//!   skipped.
//! - Results are root-relative `/`-separated paths, sorted
//!   lexicographically (Pi sorts by mtime for interactive use; we pin
//!   deterministic order instead — recorded deviation).
//! - `limit` (default 1000) caps results; the result carries a
//!   `limit_reached` flag so the model knows the listing is partial.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use verlet_tool_core::{EffectClass, ToolContract, ToolError, ToolFs};

pub const DEFAULT_LIMIT: u64 = 1000;

#[derive(Clone, Debug, Deserialize)]
pub struct GlobArgs {
    /// Glob pattern, e.g. `*.ts`, `**/*.json`, `src/**/*.spec.ts`.
    pub pattern: String,
    /// Directory to search in (default: workspace root).
    pub path: Option<PathBuf>,
    /// Maximum number of results (default 1000).
    pub limit: Option<u64>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct GlobOutput {
    /// Root-relative `/`-separated paths, sorted.
    pub paths: Vec<String>,
    pub limit_reached: bool,
}

pub fn contract() -> ToolContract {
    ToolContract {
        name: "glob",
        description: "Search for files by glob pattern, e.g. '*.ts' or \
                      '**/*.spec.ts'. Returns matching file paths relative to \
                      the search directory, sorted. Respects .gitignore.",
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "description": "Glob pattern to match files, e.g. '*.ts', '**/*.json', or 'src/**/*.spec.ts'"},
                "path": {"type": "string", "description": "Directory to search in (default: current directory)"},
                "limit": {"type": "number", "description": "Maximum number of results (default: 1000)"}
            },
            "required": ["pattern"]
        }),
        effect_class: EffectClass::Pure,
    }
}

pub fn run(_args: GlobArgs, _fs: &dyn ToolFs) -> Result<GlobOutput, ToolError> {
    todo!("EMO ticket: ToolFs walk + globset + gitignore per module docs")
}
