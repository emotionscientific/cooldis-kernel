use std::path::PathBuf;
use std::process::Command;

#[test]
fn cooldis_cli_builds_rust_source_and_runs_cat_tail() {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let module_path = repo.join("tests/fixtures/wasm-vfs-tools");
    let mount = format!(
        "/workspace={}",
        module_path.join("testdata").to_string_lossy()
    );

    let cat = run_cooldis([
        "tool",
        "run",
        "--module-path",
        module_path.to_str().unwrap(),
        "cat",
        "--input",
        "/workspace/input.txt",
        "--mount",
        &mount,
    ]);
    assert_eq!(cat, "alpha\nbeta\ngamma from cooldis vfs\n");

    let tail = run_cooldis([
        "tool",
        "run",
        "--module-path",
        module_path.to_str().unwrap(),
        "tail",
        "--input",
        "/workspace/tail.txt",
        "--mount",
        &mount,
    ]);
    assert_eq!(tail, "four\nfive\n");
}

fn run_cooldis<const N: usize>(args: [&str; N]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_cooldis"))
        .args(args)
        .output()
        .expect("failed to run cooldis cli");
    assert!(
        output.status.success(),
        "cooldis cli failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("cooldis output should be utf8")
}
