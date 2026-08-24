#![cfg(feature = "cli")]

#[test]
fn cli_globs_from_its_confined_root() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("nested")).unwrap();
    std::fs::write(root.path().join("nested/file.txt"), "content").unwrap();
    let input = serde_json::json!({
        "root": root.path(),
        "args": {"pattern": "**/*.txt"}
    });

    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_tool-glob"))
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    std::io::Write::write_all(
        &mut child.stdin.take().unwrap(),
        serde_json::to_string(&input).unwrap().as_bytes(),
    )
    .unwrap();
    let output = child.wait_with_output().unwrap();

    assert!(output.status.success(), "{:?}", output);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({
            "ok": {
                "paths": ["nested/file.txt"],
                "limit_reached": false
            }
        })
    );
}
