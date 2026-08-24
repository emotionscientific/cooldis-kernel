#![cfg(feature = "cli")]

#[test]
fn cli_reads_from_its_confined_root() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("file.txt"), "hello\nworld\n").unwrap();
    let input = serde_json::json!({
        "root": root.path(),
        "args": {"path": "file.txt"}
    });

    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_tool-read"))
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
                "text": "hello\nworld\n",
                "start_line": 1,
                "end_line": 3,
                "total_lines": 3
            }
        })
    );
}

#[test]
fn cli_returns_json_and_exit_one_for_a_tool_error() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("file.txt"), "one line").unwrap();
    let input = serde_json::json!({
        "root": root.path(),
        "args": {"path": "file.txt", "offset": 2}
    });

    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_tool-read"))
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

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({
            "error": "Offset 2 is beyond end of file (1 lines total)"
        })
    );
}
