#[test]
fn parses_cargo_json_wasm_artifact_path() {
    let stdout = br#"{"reason":"compiler-artifact","filenames":["/tmp/verlet.d","/tmp/verlet_wasm_vfs_tools.wasm"]}
{"reason":"build-finished","success":true}
"#;

    assert_eq!(
        crate::operations::operation_builder::find_wasm_artifact_path(stdout),
        Some(std::path::PathBuf::from("/tmp/verlet_wasm_vfs_tools.wasm"))
    );
}
