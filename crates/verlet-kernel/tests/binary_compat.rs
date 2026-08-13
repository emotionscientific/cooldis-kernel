#[test]
fn legacy_binary_target_and_source_are_absent() {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let cargo_toml = std::fs::read_to_string(manifest_dir.join("Cargo.toml")).unwrap();

    assert!(!cargo_toml.contains(concat!("name = \"cool", "dis\"")));
    assert!(
        !manifest_dir
            .join(concat!("src/bin/cool", "dis.rs"))
            .exists()
    );
}
