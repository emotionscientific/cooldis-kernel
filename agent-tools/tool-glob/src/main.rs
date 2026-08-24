//! Standalone CLI: JSON args on stdin -> JSON result on stdout.
//! Doubles as the executor-side implementation for remote serving.
//! Input: {"root": "/abs/dir", "args": {...GlobArgs}}

fn main() -> std::process::ExitCode {
    verlet_tool_core::run_cli(verlet_tool_glob::run)
}
