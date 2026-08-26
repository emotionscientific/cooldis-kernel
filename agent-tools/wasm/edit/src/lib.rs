//! Wasm guest module exposing the Pi-parity edit tool over the Verlet
//! operation ABI (`cooldis_0.1`).
//!
//! Operation input mirrors the native CLI harness stdin: JSON
//! `{"root": <vfs dir>, "args": <tool args>}`. Output mirrors the CLI
//! envelope: `{"ok": <tool output>}` on success, `{"error": <text>}` on a
//! tool-level failure, including malformed args. The ABI status stays OK
//! whenever an envelope can express the outcome, so the model sees the
//! tool's error text verbatim (Pi parity); non-OK statuses are reserved
//! for transport-level failures (unreadable source, sink write failure).

const EDIT_ID: u32 = 1;

#[unsafe(no_mangle)]
pub extern "C" fn __verlet_describe_module__(sink: u32) -> i32 {
    let manifest = verlet_guest_sdk::OperationManifest::new(vec![
        verlet_guest_sdk::OperationDefinition::new(EDIT_ID, "edit")
            .json_input()
            .json_output()
            .require("fs.write"),
    ]);
    let bytes = match manifest.to_json_vec() {
        Ok(bytes) => bytes,
        Err(_) => return verlet_guest_sdk::STATUS_INVALID_ARGUMENT,
    };
    status(verlet_guest_sdk::write_sink(verlet_guest_sdk::Sink(sink), &bytes).map(|_| ()))
}

#[unsafe(no_mangle)]
pub extern "C" fn __verlet_call_operation__(
    operation: u32,
    _invocation: u32,
    source: u32,
    output: u32,
    _events: u32,
) -> i32 {
    match operation {
        EDIT_ID => status(run_edit(
            verlet_guest_sdk::Source(source),
            verlet_guest_sdk::Sink(output),
        )),
        _ => verlet_guest_sdk::STATUS_NOT_FOUND,
    }
}

/// Drain the source, parse `{"root", "args"}` with `args` kept as raw
/// JSON, run it through `verlet_tool_edit::parse_cli_args` (Pi'"'"'s
/// prepare/coerce/validate pipeline; its validation envelope is the error
/// text), then `verlet_tool_edit::run` over
/// [`verlet_tool_abi_fs::AbiFs`] rooted at `root`, and write the CLI
/// envelope to the output sink.
fn run_edit(
    source: verlet_guest_sdk::Source,
    output: verlet_guest_sdk::Sink,
) -> Result<(), verlet_guest_sdk::StatusCode> {
    verlet_tool_abi_fs::run_operation_with_parser(
        source,
        output,
        verlet_tool_edit::parse_cli_args,
        verlet_tool_edit::run,
    )
}

fn status(result: Result<(), verlet_guest_sdk::StatusCode>) -> i32 {
    match result {
        Ok(()) => verlet_guest_sdk::STATUS_OK,
        Err(code) => code.as_raw(),
    }
}
