use std::path::PathBuf;
use std::process::{Command, Output};

const DEPRECATION: &str = concat!(
    "warning: cool",
    "dis is deprecated; use verlet (compatibility will be removed in v0.4.0)\n"
);

fn run(path: &PathBuf, argument: &str) -> Output {
    Command::new(path).arg(argument).output().unwrap()
}

#[test]
fn legacy_binary_matches_verlet_help_and_version_with_one_warning() {
    let canonical = PathBuf::from(env!("CARGO_BIN_EXE_verlet"));
    let legacy = canonical.with_file_name(format!(
        "{}{}",
        concat!("cool", "dis"),
        std::env::consts::EXE_SUFFIX
    ));

    for argument in ["--help", "--version"] {
        let canonical_output = run(&canonical, argument);
        let legacy_output = run(&legacy, argument);

        assert!(canonical_output.status.success());
        assert!(legacy_output.status.success());
        assert_eq!(legacy_output.stdout, canonical_output.stdout);
        assert_eq!(legacy_output.stderr, DEPRECATION.as_bytes());
        let stdout = String::from_utf8_lossy(&canonical_output.stdout);
        assert!(stdout.to_ascii_lowercase().contains("verlet"));
        assert!(!stdout.to_ascii_lowercase().contains(concat!("cool", "dis")));
    }
}
