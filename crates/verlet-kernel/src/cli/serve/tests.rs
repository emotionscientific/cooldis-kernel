#[test]
fn parse_serve_accepts_config_roots_and_idle_timeout() {
    let parsed = crate::cli::serve::parse_serve_args(
        [
            "--config",
            "/tmp/verlet.toml",
            "--cwd",
            "/tmp/project",
            "--runtime-home",
            "/tmp/runtime",
            "--state-home",
            "/tmp/state",
            "--user-state-home",
            "/tmp/user-state",
            "--idle-timeout",
            "2s",
        ]
        .into_iter()
        .map(std::ffi::OsString::from)
        .collect(),
    )
    .unwrap();

    assert_eq!(
        parsed.config_path.as_deref(),
        Some(std::path::Path::new("/tmp/verlet.toml"))
    );
    assert_eq!(parsed.idle_timeout, Some(std::time::Duration::from_secs(2)));
}
