#![cfg(feature = "cli")]

#[test]
fn cli_edits_a_file_within_its_confined_root() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("file.txt"), "hello\nworld\n").unwrap();
    let input = serde_json::json!({
        "root": root.path(),
        "args": {
            "path": "file.txt",
            "edits": [{"old_text": "world", "new_text": "there"}]
        }
    });

    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_tool-edit"))
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

    assert!(output.status.success(), "{output:?}");
    let value = serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap();
    assert_eq!(value["ok"]["edits_applied"], 1);
    assert!(value["ok"]["diff"].as_str().unwrap().contains("+there"));
    assert_eq!(
        std::fs::read_to_string(root.path().join("file.txt")).unwrap(),
        "hello\nthere\n"
    );
}
