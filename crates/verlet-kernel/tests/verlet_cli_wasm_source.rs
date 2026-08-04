use std::path::PathBuf;
use std::process::Command;
use uuid::Uuid;

#[test]
fn verlet_cli_builds_rust_source_and_runs_cat_tail() {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let module_path = repo.join("tests/fixtures/wasm-vfs-tools");
    let mount = format!(
        "/workspace={}",
        module_path.join("testdata").to_string_lossy()
    );

    let cat = run_verlet([
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
    assert_eq!(cat, "alpha\nbeta\ngamma from verlet vfs\n");

    let tail = run_verlet([
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

#[test]
fn verlet_coupling_init_scaffold_tests_builds_and_validates_package() {
    let temp = std::env::temp_dir().join(format!("verlet-coupling-init-{}", Uuid::now_v7()));
    std::fs::create_dir_all(&temp).unwrap();
    let scaffold = temp.join("counter-coupling");

    run_verlet([
        "coupling",
        "init",
        "counter-coupling",
        "--out",
        scaffold.to_str().unwrap(),
    ]);
    run_command(&scaffold, "cargo", &["test"]);
    run_command(
        &scaffold,
        "cargo",
        &[
            "build",
            "--locked",
            "--release",
            "--target",
            "wasm32-unknown-unknown",
        ],
    );
    run_verlet_in(
        &scaffold,
        ["tool", "build", "--package", "verlet.tool.toml"],
    );
}

fn run_verlet<const N: usize>(args: [&str; N]) -> String {
    run_verlet_in(&PathBuf::from("."), args)
}

fn run_verlet_in<const N: usize>(current_dir: &PathBuf, args: [&str; N]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_verlet"))
        .current_dir(current_dir)
        .args(args)
        .output()
        .expect("failed to run verlet cli");
    assert!(
        output.status.success(),
        "verlet cli failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("verlet output should be utf8")
}

fn run_command(current_dir: &PathBuf, program: &str, args: &[&str]) -> String {
    let output = Command::new(program)
        .current_dir(current_dir)
        .args(args)
        .output()
        .unwrap_or_else(|err| panic!("failed to run {program}: {err}"));
    assert!(
        output.status.success(),
        "{program} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("command output should be utf8")
}
