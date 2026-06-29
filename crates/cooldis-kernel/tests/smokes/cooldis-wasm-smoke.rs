use bashkit::{FileSystem, InMemoryFs};
use cooldis::{
    CooldisVfs, RustWasmBuildOptions, WasmRuntimeArtifact, WasmRuntimeConfig, WasmRuntimeFactory,
    build_rust_wasm_module,
};
use std::path::Path;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("wasm-vfs-tools");
    let build = build_rust_wasm_module(RustWasmBuildOptions::new(fixture_dir))?;

    let workspace = Arc::new(InMemoryFs::new());
    workspace
        .write_file(Path::new("/input.txt"), b"hello from compiled Rust wasm\n")
        .await?;

    let vfs = Arc::new(CooldisVfs::new(Arc::new(InMemoryFs::new())));
    vfs.mount("/workspace", workspace)?;

    let factory = WasmRuntimeFactory::new(
        WasmRuntimeConfig::new(WasmRuntimeArtifact::path(build.artifact_path)).with_vfs(vfs),
    )?;
    let output = factory
        .invoke_operation_bytes("cat", b"/workspace/input.txt".to_vec())
        .await?;
    let text = String::from_utf8(output.output)?;

    if text != "hello from compiled Rust wasm\n" {
        return Err(format!("unexpected wasm output: {text:?}").into());
    }

    println!("cooldis wasm smoke ok: {}", text.trim());
    Ok(())
}
