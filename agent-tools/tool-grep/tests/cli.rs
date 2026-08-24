#![cfg(feature = "cli")]

#[test]
fn cli_greps_from_its_confined_root() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("file.txt"), "first\nneedle\n").unwrap();
    let input = serde_json::json!({
        "root": root.path(),
        "args": {"pattern": "needle"}
    });

    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_tool-grep"))
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
                "text": "file.txt:2: needle",
                "match_count": 1,
                "limit_reached": false
            }
        })
    );
}
