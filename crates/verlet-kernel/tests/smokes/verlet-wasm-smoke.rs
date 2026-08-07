use bashkit::FileSystem as _;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let fixture_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("wasm-vfs-tools");
    let build = verlet::build_rust_wasm_module(verlet::RustWasmBuildOptions::new(fixture_dir))?;

    let workspace = std::sync::Arc::new(bashkit::InMemoryFs::new());
    workspace
        .write_file(
            std::path::Path::new("/input.txt"),
            b"hello from compiled Rust wasm\n",
        )
        .await?;

    let vfs = std::sync::Arc::new(verlet::VerletVfs::new(std::sync::Arc::new(
        bashkit::InMemoryFs::new(),
    )));
    vfs.mount("/workspace", workspace)?;

    let factory = verlet::WasmRuntimeFactory::new(
        verlet::WasmRuntimeConfig::new(verlet::WasmRuntimeArtifact::path(build.artifact_path))
            .with_vfs(vfs),
    )?;
    let output = factory
        .invoke_operation_bytes("cat", b"/workspace/input.txt".to_vec())
        .await?;
    let text = String::from_utf8(output.output)?;

    if text != "hello from compiled Rust wasm\n" {
        return Err(format!("unexpected wasm output: {text:?}").into());
    }

    println!("verlet wasm smoke ok: {}", text.trim());
    Ok(())
}
