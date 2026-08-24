#![cfg(feature = "cli")]

#[test]
fn cli_writes_to_its_confined_root() {
    let root = tempfile::tempdir().unwrap();
    let input = serde_json::json!({
        "root": root.path(),
        "args": {"path": "nested/file.txt", "content": "exact content"}
    });

    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_tool-write"))
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
        serde_json::json!({"ok": {"bytes_written": 13, "replaced": false}})
    );
    assert_eq!(
        std::fs::read(root.path().join("nested/file.txt")).unwrap(),
        b"exact content"
    );
}
