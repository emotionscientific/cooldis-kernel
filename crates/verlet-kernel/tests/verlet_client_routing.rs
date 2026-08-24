#[path = "support/model_catalog.rs"]
mod model_catalog_test_support;

#[cfg(unix)]
#[tokio::test]
async fn console_serves_concurrent_auth_and_tool_source_clients() {
    let root = TestRoot::new("verlet-client-routing");
    let project = root.path().join("project");
    let user_home = root.path().join("user-home");
    let assets = root.path().join("console-assets");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::create_dir_all(&assets).unwrap();
    std::fs::write(
        assets.join("index.html"),
        "<!doctype html><title>routing test</title>",
    )
    .unwrap();

    let mut console_command = tokio::process::Command::new(env!("CARGO_BIN_EXE_verlet"));
    model_catalog_test_support::disable_for_tokio_command(&mut console_command);
    let mut console = console_command
        .args(["console", "--no-open", "--cwd"])
        .arg(&project)
        .env("VERLET_HOME", &user_home)
        .env("VERLET_CONSOLE_ASSET_DIR", &assets)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let endpoint = wait_for_endpoint(&project.join(".verlet/state"), Some(&mut console)).await;

    let auth = run_client(&project, &user_home, ["auth", "status", "openai-codex"]);
    let sources = run_client(&project, &user_home, ["tool", "source", "list"]);
    let (auth, sources) = tokio::join!(auth, sources);
    assert_success("auth status", &auth);
    assert_success("tool source list", &sources);
    assert_eq!(
        verlet::adapters::app_server::instance::resolve_instance_endpoint(
            &project.join(".verlet/state")
        )
        .unwrap()
        .pid,
        endpoint.pid
    );
    assert!(!project.join(".verlet/state/serve.log").exists());

    let other_project = root.path().join("other-project");
    std::fs::create_dir_all(&other_project).unwrap();
    let conflict = run_client(&other_project, &user_home, ["tool", "source", "list"]).await;
    assert!(!conflict.status.success());
    let conflict = String::from_utf8(conflict.stderr).unwrap();
    assert!(
        conflict.contains(&format!("pid {}", endpoint.pid)),
        "{conflict}"
    );
    assert!(
        conflict.contains(&endpoint.unix_socket.display().to_string()),
        "{conflict}"
    );

    console.start_kill().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(30), console.wait())
        .await
        .expect("console did not stop")
        .unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn auth_client_auto_spawns_serve_and_idle_shutdown_removes_endpoint() {
    let root = TestRoot::new("verlet-client-auto-spawn");
    let project = root.path().join("project");
    let user_home = root.path().join("user-home");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(
        project.join("verlet.toml"),
        "[daemon]\nidle_timeout = \"2s\"\n\n[daemon.provider]\nprovider = \"local\"\n",
    )
    .unwrap();

    let auth = run_client(&project, &user_home, ["auth", "status", "openai-codex"]).await;
    assert_success("auto-spawned auth status", &auth);

    let state_root = project.join(".verlet/state");
    let endpoint = wait_for_endpoint(&state_root, None).await;
    assert_ne!(endpoint.pid, std::process::id());
    assert!(state_root.join("serve.log").is_file());
    wait_for_endpoint_removal(&state_root).await;
    wait_for_endpoint_removal(&user_home.join("state")).await;
}

#[cfg(unix)]
#[tokio::test]
async fn concurrent_auto_spawn_preserves_log_and_winner_endpoint() {
    let root = TestRoot::new("verlet-client-auto-spawn-race");
    let project = root.path().join("project");
    let user_home = root.path().join("user-home");
    let state_root = project.join(".verlet/state");
    std::fs::create_dir_all(&state_root).unwrap();
    std::fs::write(state_root.join("serve.log"), "preexisting log marker\n").unwrap();
    std::fs::write(
        project.join("verlet.toml"),
        "[daemon]\nidle_timeout = \"500ms\"\n\n[daemon.provider]\nprovider = \"local\"\n",
    )
    .unwrap();

    let first = run_client(&project, &user_home, ["auth", "status", "openai-codex"]);
    let second = run_client(&project, &user_home, ["tool", "source", "list"]);
    let (first, second) = tokio::join!(first, second);
    assert_success("concurrent auth status", &first);
    assert_success("concurrent tool source list", &second);

    let endpoint = wait_for_endpoint(&state_root, None).await;
    assert!(
        std::os::unix::net::UnixStream::connect(&endpoint.unix_socket).is_ok(),
        "winning endpoint socket is not connectable"
    );
    let log = std::fs::read_to_string(state_root.join("serve.log")).unwrap();
    assert!(log.starts_with("preexisting log marker\n"), "{log}");

    let third = run_client(&project, &user_home, ["tool", "source", "list"]).await;
    assert_success("client after losing spawn exited", &third);
    wait_for_endpoint_removal(&state_root).await;
    wait_for_endpoint_removal(&user_home.join("state")).await;
}

#[cfg(unix)]
#[tokio::test]
async fn simultaneous_serve_loser_exits_zero() {
    let root = TestRoot::new("verlet-serve-start-race");
    let project = root.path().join("project");
    let user_home = root.path().join("user-home");
    let state_root = project.join(".verlet/state");
    let runtime_home = project.join(".verlet/runtime");
    std::fs::create_dir_all(&project).unwrap();

    let run = || async {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_verlet"));
        model_catalog_test_support::disable_for_tokio_command(&mut command);
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            command
                .args(["serve", "--idle-timeout", "300ms", "--cwd"])
                .arg(&project)
                .arg("--runtime-home")
                .arg(&runtime_home)
                .arg("--state-home")
                .arg(&state_root)
                .arg("--user-state-home")
                .arg(user_home.join("state"))
                .current_dir(&project)
                .stdin(std::process::Stdio::null())
                .output(),
        )
        .await
        .expect("serve process timed out")
        .unwrap()
    };

    let (first, second) = tokio::join!(run(), run());
    assert_success("first simultaneous serve", &first);
    assert_success("second simultaneous serve", &second);
    assert!(!state_root.join("endpoint.json").exists());
    assert!(!user_home.join("state/endpoint.json").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn service_managed_serve_ignores_configured_idle_timeout() {
    let root = TestRoot::new("verlet-service-no-idle");
    let project = root.path().join("project");
    let user_home = root.path().join("user-home");
    std::fs::create_dir_all(&project).unwrap();
    let config = project.join("verlet.toml");
    std::fs::write(
        &config,
        "[daemon]\nidle_timeout = \"100ms\"\n\n[daemon.runtime]\ncwd = \".\"\nruntime_home = \".verlet/runtime\"\nstate_home = \".verlet/state\"\n\n[daemon.provider]\nprovider = \"local\"\n",
    )
    .unwrap();

    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_verlet"));
    model_catalog_test_support::disable_for_tokio_command(&mut command);
    let mut serve = command
        .args(["serve", "--no-idle-timeout", "--config"])
        .arg(&config)
        .env("VERLET_HOME", &user_home)
        .current_dir(&project)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    wait_for_endpoint(&project.join(".verlet/state"), Some(&mut serve)).await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        serve.try_wait().unwrap().is_none(),
        "service-managed serve exited on configured idle timeout"
    );
    serve.start_kill().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(30), serve.wait())
        .await
        .expect("service-managed serve did not stop")
        .unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn auth_outside_project_starts_user_home_server_without_caller_state() {
    let root = TestRoot::new("verlet-auth-outside-project");
    let directory = root.path().join("arbitrary-directory");
    let user_home = root.path().join("user-home");
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::create_dir_all(&user_home).unwrap();
    std::fs::write(
        user_home.join("config.toml"),
        "[daemon]\nidle_timeout = \"500ms\"\n",
    )
    .unwrap();

    let output = run_client(&directory, &user_home, ["auth", "status", "openai-codex"]).await;
    assert_success("auth status outside a project", &output);
    assert!(!directory.join(".verlet").exists());

    let state_root = user_home.join("state");
    let endpoint = wait_for_endpoint(&state_root, None).await;
    assert_ne!(endpoint.pid, std::process::id());
    assert!(state_root.join("serve.log").is_file());
    assert!(!user_home.join("projects/home").exists());
    wait_for_endpoint_removal(&state_root).await;
}

async fn run_client<const N: usize>(
    project: &std::path::Path,
    user_home: &std::path::Path,
    args: [&str; N],
) -> std::process::Output {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_verlet"));
    model_catalog_test_support::disable_for_tokio_command(&mut command);
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        command
            .args(args)
            .current_dir(project)
            .env("HOME", user_home)
            .env("VERLET_HOME", user_home)
            .stdin(std::process::Stdio::null())
            .output(),
    )
    .await
    .expect("client command timed out")
    .unwrap()
}

fn assert_success(label: &str, output: &std::process::Output) {
    assert!(
        output.status.success(),
        "{label} failed with {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

async fn wait_for_endpoint(
    state_root: &std::path::Path,
    mut child: Option<&mut tokio::process::Child>,
) -> verlet::adapters::app_server::instance::InstanceEndpoint {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        if let Some(endpoint) =
            verlet::adapters::app_server::instance::resolve_instance_endpoint(state_root)
        {
            return endpoint;
        }
        if let Some(child) = child.as_deref_mut()
            && let Some(status) = child.try_wait().unwrap()
        {
            panic!("server exited before endpoint publication: {status}");
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for endpoint {}",
            state_root.join("endpoint.json").display()
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

async fn wait_for_endpoint_removal(state_root: &std::path::Path) {
    let endpoint_path = state_root.join("endpoint.json");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        if !endpoint_path.exists() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for endpoint removal {}; serve log:\n{}",
            endpoint_path.display(),
            std::fs::read_to_string(state_root.join("serve.log")).unwrap_or_default()
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

struct TestRoot(std::path::PathBuf);

impl TestRoot {
    fn new(prefix: &str) -> Self {
        Self(std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::now_v7().simple())))
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
