//! Wasm guest module exposing the Pi-parity write tool over the Verlet
//! operation ABI (`cooldis_0.1`).
//!
//! Operation input mirrors the native CLI harness stdin: JSON
//! `{"root": <vfs dir>, "args": <tool args>}`. Output mirrors the CLI
//! envelope: `{"ok": <tool output>}` on success, `{"error": <text>}` on a
//! tool-level failure, including malformed args. The ABI status stays OK
//! whenever an envelope can express the outcome, so the model sees the
//! tool's error text verbatim (Pi parity); non-OK statuses are reserved
//! for transport-level failures (unreadable source, sink write failure).

const WRITE_ID: u32 = 1;

#[unsafe(no_mangle)]
pub extern "C" fn __verlet_describe_module__(sink: u32) -> i32 {
    let manifest = verlet_guest_sdk::OperationManifest::new(vec![
        verlet_guest_sdk::OperationDefinition::new(WRITE_ID, "write")
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
        WRITE_ID => status(run_write(
            verlet_guest_sdk::Source(source),
            verlet_guest_sdk::Sink(output),
        )),
        _ => verlet_guest_sdk::STATUS_NOT_FOUND,
    }
}

/// Drain the source, parse `{"root", "args"}`, run
/// `verlet_tool_write::run` over [`verlet_tool_abi_fs::AbiFs`] rooted at
/// `root`, and write the CLI envelope to the output sink.
fn run_write(
    source: verlet_guest_sdk::Source,
    output: verlet_guest_sdk::Sink,
) -> Result<(), verlet_guest_sdk::StatusCode> {
    verlet_tool_abi_fs::run_operation(source, output, verlet_tool_write::run)
}

fn status(result: Result<(), verlet_guest_sdk::StatusCode>) -> i32 {
    match result {
        Ok(()) => verlet_guest_sdk::STATUS_OK,
        Err(code) => code.as_raw(),
    }
}
