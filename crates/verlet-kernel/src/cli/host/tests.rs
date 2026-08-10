//! EMO-564 acceptance-shaped unit tests for the host CLI. Config
//! parse/validate cases run here; the end-to-end boot test lives in
//! `tests/host_facade.rs` beside the facade suite.

static PROCESS_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

struct TestEnv {
    name: String,
    prior: Option<std::ffi::OsString>,
}

impl TestEnv {
    fn set(name: String, value: &str) -> Self {
        Self::set_os(name, std::ffi::OsString::from(value))
    }

    fn set_os(name: String, value: std::ffi::OsString) -> Self {
        let prior = std::env::var_os(&name);
        // SAFETY: host config tests serialize their process-environment
        // mutations with `PROCESS_ENV_LOCK`, and this guard restores state.
        unsafe { std::env::set_var(&name, value) };
        Self { name, prior }
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        // SAFETY: see `TestEnv::set`; the serializing lock outlives this guard.
        unsafe {
            match &self.prior {
                Some(value) => std::env::set_var(&self.name, value),
                None => std::env::remove_var(&self.name),
            }
        }
    }
}

struct TestConfigFile {
    root: std::path::PathBuf,
    path: std::path::PathBuf,
}

impl TestConfigFile {
    fn write(label: &str, contents: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "verlet-host-config-{label}-{}",
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("host.toml");
        std::fs::write(&path, contents).unwrap();
        Self { root, path }
    }
}

impl Drop for TestConfigFile {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}

fn local_instance(id: &str, root: &std::path::Path) -> String {
    let route_digest = crate::daemon::identity::identity_token_digest(id);
    format!(
        r#"
[[instance]]
id = "{id}"
root = "{}"
cwd = "{}"
tenant_id = "tenant-{id}"
console_principal = "operator:{id}"
hook_shell = "/bin/sh"
route_digests = ["{route_digest}"]

[instance.provider]
provider = "local_offline"
"#,
        root.join(id).display(),
        root.join("workspace").display(),
    )
}

fn load_error(label: &str, contents: &str) -> String {
    let file = TestConfigFile::write(label, contents);
    super::load_host_run_config(&file.path)
        .unwrap_err()
        .to_string()
}

fn direct_local_instance(
    id: &str,
    root: std::path::PathBuf,
    cwd: &std::path::Path,
) -> super::HostInstanceConfig {
    super::HostInstanceConfig {
        id: id.to_string(),
        root,
        cwd: cwd.to_path_buf(),
        tenant_id: format!("tenant-{id}"),
        console_principal: format!("operator:{id}"),
        hook_shell: "/bin/sh".to_string(),
        route_digests: Vec::new(),
        provider: super::HostInstanceProviderConfig {
            provider: "local_offline".to_string(),
            base_url: None,
            api_key_env: None,
            model: None,
            resolved_api_key: None,
        },
    }
}

#[test]
fn parse_minimal_config_with_defaults() {
    let root = std::env::temp_dir().join(format!("verlet-host-minimal-{}", uuid::Uuid::now_v7()));
    let config = format!(
        "[listen]\naddr = \"127.0.0.1:0\"\n{}",
        local_instance("first", &root)
    );
    let file = TestConfigFile::write("minimal", &config);

    let loaded = super::load_host_run_config(&file.path).unwrap();

    assert_eq!(loaded.listen.addr, "127.0.0.1:0");
    assert!(!loaded.listen.allow_non_loopback);
    assert_eq!(loaded.instance.len(), 1);
    assert_eq!(loaded.instance[0].id, "first");
}

#[test]
fn host_run_args_require_only_a_config_path() {
    let path =
        super::parse_host_run_args(vec!["--config".into(), "/tmp/host.toml".into()]).unwrap();
    assert_eq!(path, std::path::PathBuf::from("/tmp/host.toml"));
    assert!(
        super::parse_host_run_args(Vec::new())
            .unwrap_err()
            .to_string()
            .contains("requires --config")
    );
    let error = super::parse_host_run_args(vec![
        "--token".into(),
        "argument-secret-that-must-not-be-echoed".into(),
    ])
    .unwrap_err()
    .to_string();
    assert!(error.contains("unknown host run argument"), "{error}");
    assert!(!error.contains("argument-secret-that-must-not-be-echoed"));
}

#[test]
fn reject_duplicate_instance_ids() {
    let root = std::env::temp_dir().join(format!("verlet-host-duplicate-{}", uuid::Uuid::now_v7()));
    let config = format!(
        "[listen]\naddr = \"127.0.0.1:0\"\n{}{}",
        local_instance("same", &root.join("one")),
        local_instance("same", &root.join("two")),
    );

    let error = load_error("duplicate-id", &config);

    assert!(
        error.contains("instance id \"same\" is duplicated"),
        "{error}"
    );
}

#[test]
fn reject_duplicate_route_digest_across_instances() {
    let root = std::env::temp_dir().join(format!("verlet-host-routes-{}", uuid::Uuid::now_v7()));
    let mut second = local_instance("second", &root);
    let first_digest = crate::daemon::identity::identity_token_digest("first");
    let second_digest = crate::daemon::identity::identity_token_digest("second");
    second = second.replace(&second_digest, &first_digest);
    let config = format!(
        "[listen]\naddr = \"127.0.0.1:0\"\n{}{}",
        local_instance("first", &root),
        second,
    );

    let error = load_error("duplicate-route", &config);

    assert!(
        error.contains("route_digests[0] duplicates instance \"first\".route_digests[0]"),
        "{error}"
    );
}

#[test]
fn reject_duplicate_route_digest_within_one_instance() {
    let root = std::env::temp_dir().join(format!(
        "verlet-host-routes-within-{}",
        uuid::Uuid::now_v7()
    ));
    let digest = crate::daemon::identity::identity_token_digest("first");
    let instance = local_instance("first", &root).replace(
        &format!("route_digests = [\"{digest}\"]"),
        &format!("route_digests = [\"{digest}\", \"{digest}\"]"),
    );
    let config = format!("[listen]\naddr = \"127.0.0.1:0\"\n{instance}");

    let error = load_error("duplicate-route-within", &config);

    assert!(
        error.contains("route_digests[1] duplicates instance \"first\".route_digests[0]"),
        "{error}"
    );
}

#[test]
fn reject_non_digest_route_without_echoing_a_pasted_token() {
    let root =
        std::env::temp_dir().join(format!("verlet-host-route-token-{}", uuid::Uuid::now_v7()));
    let raw_token = "verlet-secret-token-that-must-not-be-echoed";
    let digest = crate::daemon::identity::identity_token_digest("first");
    let instance = local_instance("first", &root).replace(&digest, raw_token);
    let config = format!("[listen]\naddr = \"127.0.0.1:0\"\n{instance}");

    let error = load_error("route-token", &config);

    assert!(
        error.contains("route_digests[0] must be a sha256 digest"),
        "{error}"
    );
    assert!(!error.contains(raw_token), "raw token leaked in {error}");
}

#[test]
fn reject_route_digest_shape_variants_not_emitted_by_identity_mint() {
    let root =
        std::env::temp_dir().join(format!("verlet-host-route-shape-{}", uuid::Uuid::now_v7()));
    let valid = crate::daemon::identity::identity_token_digest("first");
    for (label, invalid) in [
        ("missing-prefix", "a".repeat(64)),
        ("short", format!("sha256:{}", "a".repeat(63))),
        ("uppercase", format!("sha256:{}", "A".repeat(64))),
    ] {
        let instance = local_instance("first", &root).replace(&valid, &invalid);
        let config = format!("[listen]\naddr = \"127.0.0.1:0\"\n{instance}");
        let error = load_error(label, &config);
        assert!(
            error.contains("route_digests[0] must be a sha256 digest"),
            "{label}: {error}"
        );
        assert!(!error.contains(&invalid), "{label} leaked in {error}");
    }
}

#[test]
fn reject_relative_root_cwd_or_hook_shell() {
    let config = r#"
[listen]
addr = "127.0.0.1:0"

[[instance]]
id = "relative"
root = "relative-root"
cwd = "relative-cwd"
tenant_id = "tenant"
console_principal = "operator"
hook_shell = "bin/sh"

[instance.provider]
provider = "local_offline"
"#;

    let error = load_error("relative-paths", config);

    for field in ["root", "cwd", "hook_shell"] {
        assert!(error.contains(field), "missing {field} in {error}");
        assert!(error.contains("must be absolute"), "{error}");
    }
}

#[test]
fn reject_blank_tenant_or_console_principal() {
    let root = std::env::temp_dir().join(format!("verlet-host-identity-{}", uuid::Uuid::now_v7()));
    let config = format!(
        r#"
[listen]
addr = "127.0.0.1:0"

[[instance]]
id = "blank"
root = "{}"
cwd = "{}"
tenant_id = "  "
console_principal = "\t"
hook_shell = "/bin/sh"

[instance.provider]
provider = "local_offline"
"#,
        root.join("instance").display(),
        root.join("workspace").display(),
    );

    let error = load_error("blank-identity", &config);

    assert!(error.contains("tenant_id must be non-blank"), "{error}");
    assert!(
        error.contains("console_principal must be non-blank"),
        "{error}"
    );
}

#[test]
fn bifrost_provider_requires_base_url_key_env_and_model() {
    let root = std::env::temp_dir().join(format!("verlet-host-bifrost-{}", uuid::Uuid::now_v7()));
    let config = format!(
        r#"
[listen]
addr = "127.0.0.1:0"

[[instance]]
id = "bifrost"
root = "{}"
cwd = "{}"
tenant_id = "tenant"
console_principal = "operator"
hook_shell = "/bin/sh"

[instance.provider]
provider = "bifrost_openai"
"#,
        root.join("instance").display(),
        root.join("workspace").display(),
    );

    let error = load_error("bifrost-required", &config);

    for field in ["base_url", "api_key_env", "model"] {
        assert!(error.contains(field), "missing {field} in {error}");
    }
}

#[test]
fn bifrost_key_env_must_resolve_at_load_time() {
    let _env_lock = PROCESS_ENV_LOCK.lock().unwrap();
    let env_name = format!("VERLET_HOST_MISSING_{}", uuid::Uuid::now_v7());
    // SAFETY: this test serializes its process-environment mutation.
    unsafe { std::env::remove_var(&env_name) };
    let root = std::env::temp_dir().join(format!("verlet-host-key-env-{}", uuid::Uuid::now_v7()));
    let config = format!(
        r#"
[listen]
addr = "127.0.0.1:0"

[[instance]]
id = "bifrost"
root = "{}"
cwd = "{}"
tenant_id = "tenant"
console_principal = "operator"
hook_shell = "/bin/sh"

[instance.provider]
provider = "bifrost_openai"
base_url = "https://bifrost.example.test"
api_key_env = "{env_name}"
model = "openai/test"
"#,
        root.join("instance").display(),
        root.join("workspace").display(),
    );

    let error = load_error("missing-key-env", &config);

    assert!(error.contains(&env_name), "{error}");
    assert!(error.contains("did not resolve"), "{error}");
}

#[cfg(unix)]
#[test]
fn non_unicode_key_env_error_does_not_format_secret_bytes() {
    use std::os::unix::ffi::OsStringExt as _;

    let _env_lock = PROCESS_ENV_LOCK.lock().unwrap();
    let env_name = format!("VERLET_HOST_NON_UNICODE_{}", uuid::Uuid::now_v7());
    let secret_prefix = "provider-secret-prefix-that-must-not-appear";
    let mut secret = secret_prefix.as_bytes().to_vec();
    secret.push(0xff);
    let _env = TestEnv::set_os(env_name.clone(), std::ffi::OsString::from_vec(secret));
    let root =
        std::env::temp_dir().join(format!("verlet-host-non-unicode-{}", uuid::Uuid::now_v7()));
    let config = format!(
        r#"
[listen]
addr = "127.0.0.1:0"

[[instance]]
id = "bifrost"
root = "{}"
cwd = "{}"
tenant_id = "tenant"
console_principal = "operator"
hook_shell = "/bin/sh"

[instance.provider]
provider = "bifrost_openai"
base_url = "https://bifrost.example.test"
api_key_env = "{env_name}"
model = "openai/test"
"#,
        root.join("instance").display(),
        root.join("workspace").display(),
    );

    let error = load_error("non-unicode-key-env", &config);

    assert!(error.contains(&env_name), "{error}");
    assert!(error.contains("did not resolve"), "{error}");
    assert!(
        !error.contains(secret_prefix),
        "provider key leaked in {error}"
    );
}

#[test]
fn bifrost_key_is_resolved_only_once_during_load() {
    let _env_lock = PROCESS_ENV_LOCK.lock().unwrap();
    let env_name = format!("VERLET_HOST_KEY_{}", uuid::Uuid::now_v7());
    let _env = TestEnv::set(env_name.clone(), "original-key");
    let root = std::env::temp_dir().join(format!("verlet-host-key-once-{}", uuid::Uuid::now_v7()));
    let config = format!(
        r#"
[listen]
addr = "127.0.0.1:0"

[[instance]]
id = "bifrost"
root = "{}"
cwd = "{}"
tenant_id = "tenant"
console_principal = "operator"
hook_shell = "/bin/sh"

[instance.provider]
provider = "bifrost_openai"
base_url = "https://bifrost.example.test"
api_key_env = "{env_name}"
model = "openai/test"
"#,
        root.join("instance").display(),
        root.join("workspace").display(),
    );
    let file = TestConfigFile::write("key-once", &config);
    let loaded = super::load_host_run_config(&file.path).unwrap();
    assert!(!format!("{loaded:?}").contains("original-key"));
    // SAFETY: the module lock serializes this mutation and `_env` restores it.
    unsafe { std::env::set_var(&env_name, "changed-after-load") };

    let (_, hosted) = super::hosted_instance_config(&loaded.instance[0]).unwrap();
    assert!(!format!("{hosted:?}").contains("original-key"));
    let auth = hosted.instance_environment.provider_auth.resolve();
    assert!(!format!("{auth:?}").contains("original-key"));
    assert_eq!(
        auth.runtime_api_keys
            .get(verlet::adapters::app_server::APP_SERVER_BIFROST_PROVIDER)
            .map(String::as_str),
        Some("original-key")
    );

    drop(hosted);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn validation_failure_after_key_resolution_never_contains_key_bytes() {
    let _env_lock = PROCESS_ENV_LOCK.lock().unwrap();
    let env_name = format!("VERLET_HOST_KEY_REDACTION_{}", uuid::Uuid::now_v7());
    let secret = "provider-secret-bytes-that-must-never-appear";
    let _env = TestEnv::set(env_name.clone(), secret);
    let root = std::env::temp_dir().join(format!(
        "verlet-host-key-redaction-{}",
        uuid::Uuid::now_v7()
    ));
    let config = format!(
        r#"
[listen]
addr = "not-a-socket-address"

[[instance]]
id = "bifrost"
root = "{}"
cwd = "{}"
tenant_id = "tenant"
console_principal = "operator"
hook_shell = "/bin/sh"

[instance.provider]
provider = "bifrost_openai"
base_url = "https://bifrost.example.test"
api_key_env = "{env_name}"
model = "openai/test"
"#,
        root.join("instance").display(),
        root.join("workspace").display(),
    );

    let error = load_error("key-redaction", &config);

    assert!(error.contains("listen.addr"), "{error}");
    assert!(!error.contains(secret), "provider key leaked in {error}");
}

#[test]
fn unknown_provider_name_is_an_error() {
    let root = std::env::temp_dir().join(format!("verlet-host-provider-{}", uuid::Uuid::now_v7()));
    let config = local_instance("first", &root).replace("local_offline", "mystery_provider");
    let config = format!("[listen]\naddr = \"127.0.0.1:0\"\n{config}");

    let error = load_error("unknown-provider", &config);

    assert!(
        error.contains("unknown provider \"mystery_provider\""),
        "{error}"
    );
}

#[test]
fn unknown_toml_field_is_an_error() {
    let root = std::env::temp_dir().join(format!("verlet-host-unknown-{}", uuid::Uuid::now_v7()));
    let config = format!(
        "[listen]\naddr = \"127.0.0.1:0\"\nunknown = true\n{}",
        local_instance("first", &root),
    );

    let error = load_error("unknown-field", &config);

    assert!(error.contains("unknown field `unknown`"), "{error}");
}

#[test]
fn reject_overlapping_instance_roots() {
    let root = std::env::temp_dir().join(format!("verlet-host-overlap-{}", uuid::Uuid::now_v7()));
    let config = format!(
        "[listen]\naddr = \"127.0.0.1:0\"\n{}{}",
        local_instance("first", &root),
        local_instance("second", &root.join("first")),
    );

    let error = load_error("overlapping-roots", &config);

    assert!(error.contains("instance roots overlap"), "{error}");
}

#[cfg(unix)]
#[test]
fn reject_overlap_through_a_symlinked_existing_parent() {
    let root = std::env::temp_dir().join(format!(
        "verlet-host-symlink-overlap-{}",
        uuid::Uuid::now_v7()
    ));
    let first_root = root.join("first");
    std::fs::create_dir_all(&first_root).unwrap();
    std::os::unix::fs::symlink(&first_root, root.join("first-alias")).unwrap();
    let config = format!(
        "[listen]\naddr = \"127.0.0.1:0\"\n{}{}",
        local_instance("first", &root),
        local_instance("second", &root.join("first-alias")),
    );

    let error = load_error("symlink-overlap", &config);

    assert!(error.contains("instance roots overlap"), "{error}");
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn listener_accepts_literal_ipv4_and_ipv6_binds_but_rejects_hostnames() {
    for (label, addr) in [("ipv4", "0.0.0.0:7900"), ("ipv6", "[::]:7900")] {
        let root = std::env::temp_dir().join(format!(
            "verlet-host-listen-{label}-{}",
            uuid::Uuid::now_v7()
        ));
        let config = format!(
            "[listen]\naddr = \"{addr}\"\n{}",
            local_instance(label, &root)
        );
        let file = TestConfigFile::write(label, &config);
        assert_eq!(
            super::load_host_run_config(&file.path).unwrap().listen.addr,
            addr
        );
    }

    let root = std::env::temp_dir().join(format!(
        "verlet-host-listen-hostname-{}",
        uuid::Uuid::now_v7()
    ));
    let config = format!(
        "[listen]\naddr = \"localhost:7900\"\n{}",
        local_instance("hostname", &root)
    );
    let error = load_error("hostname-listener", &config);
    assert!(
        error.contains("listen.addr must be a TCP socket address"),
        "{error}"
    );
}

#[test]
fn reject_empty_instance_list_and_invalid_listener() {
    let error = load_error("empty", "[listen]\naddr = \"not-an-address\"\n");

    assert!(error.contains("at least one [[instance]]"), "{error}");
    assert!(error.contains("listen.addr"), "{error}");
}

#[tokio::test]
async fn mid_boot_failure_shuts_down_started_instances_and_releases_roots() {
    let root = std::env::temp_dir().join(format!("verlet-host-cleanup-{}", uuid::Uuid::now_v7()));
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let first = direct_local_instance("duplicate", root.join("first"), &workspace);
    let second = direct_local_instance("duplicate", root.join("second"), &workspace);
    let config = super::VerletHostRunConfig {
        listen: super::HostListenConfig {
            addr: "127.0.0.1:0".to_string(),
            allow_non_loopback: false,
        },
        instance: vec![first.clone(), second.clone()],
    };

    let error = super::serve_until_shutdown(config, async { Ok(()) })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("already exists"), "{error}");

    let (_, first_successor) = super::hosted_instance_config(&first).unwrap();
    let (_, second_successor) = super::hosted_instance_config(&second).unwrap();
    drop(first_successor);
    drop(second_successor);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn pending_config_failure_releases_reservations_before_listener_bind() {
    let root = std::env::temp_dir().join(format!(
        "verlet-host-pending-cleanup-{}",
        uuid::Uuid::now_v7()
    ));
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let first = direct_local_instance("first", root.join("first"), &workspace);
    let second = direct_local_instance(
        "second",
        root.join("first").join("runtime").join("nested"),
        &workspace,
    );
    let port = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = port.local_addr().unwrap();
    drop(port);
    let config = super::VerletHostRunConfig {
        listen: super::HostListenConfig {
            addr: addr.to_string(),
            allow_non_loopback: false,
        },
        instance: vec![first.clone(), second],
    };

    let error = super::serve_until_shutdown(config, async { Ok(()) })
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("overlaps reserved root"),
        "{error}"
    );

    let rebound = tokio::net::TcpListener::bind(addr).await.unwrap();
    drop(rebound);
    let (_, successor) = super::hosted_instance_config(&first).unwrap();
    drop(successor);
    std::fs::remove_dir_all(root).unwrap();
}
