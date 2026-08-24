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
    assert!(!parsed.no_idle_timeout);
}

#[test]
fn parse_serve_internal_no_idle_timeout_rejects_duration_override() {
    let parsed =
        crate::cli::serve::parse_serve_args(vec![std::ffi::OsString::from("--no-idle-timeout")])
            .unwrap();
    assert!(parsed.no_idle_timeout);
    assert_eq!(parsed.idle_timeout, None);

    let error = crate::cli::serve::parse_serve_args(
        ["--no-idle-timeout", "--idle-timeout", "2s"]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("cannot be used together"));
}
