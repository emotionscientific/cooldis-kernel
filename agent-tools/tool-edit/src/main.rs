//! Standalone CLI: JSON args on stdin -> JSON result on stdout.
//! Doubles as the executor-side implementation for remote serving.
//! Input: {"root": "/abs/dir", "args": {...EditArgs}}

fn main() -> std::process::ExitCode {
    verlet_tool_core::run_cli_with_parser(verlet_tool_edit::parse_cli_args, verlet_tool_edit::run)
}
