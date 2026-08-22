#[path = "support/model_catalog.rs"]
mod model_catalog_test_support;

#[tokio::test]
async fn serve_replaces_daemon_run_without_an_alias() {
    let serve = tokio::process::Command::new(env!("CARGO_BIN_EXE_verlet"))
        .args(["serve", "--help"])
        .output()
        .await
        .unwrap();
    assert!(serve.status.success());
    assert!(String::from_utf8_lossy(&serve.stdout).contains("verlet serve"));

    let daemon_run = tokio::process::Command::new(env!("CARGO_BIN_EXE_verlet"))
        .args(["daemon", "run"])
        .output()
        .await
        .unwrap();
    assert!(!daemon_run.status.success());
    assert!(
        String::from_utf8_lossy(&daemon_run.stderr).contains("unknown daemon subcommand \"run\"")
    );
}

#[tokio::test]
async fn serve_serves_codex_remote_on_configured_unix_socket() {
    let smoke_id = uuid::Uuid::now_v7().simple().to_string();
    let root = std::path::Path::new("/tmp").join(format!("cdisd-{}", &smoke_id[..12]));
    std::fs::create_dir_all(&root).unwrap();
    let socket = root.join("run/verlet.sock");
    let config_path = root.join("verlet.toml");
    write_daemon_config(&config_path, &root, &socket);

    let server = ServeChild::spawn(&config_path).await;
    let mut client = connect_daemon_client(&socket).await;

    let account = client.account_read().await.unwrap();
    assert_eq!(account["requiresOpenaiAuth"], false);
    let models = client.model_list().await.unwrap();
    assert!(matches!(models["data"].as_array(), Some(models) if !models.is_empty()));

    let completed = client
        .run_prompt("daemon smoke", std::time::Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        completed.assistant_text.contains("daemon smoke"),
        "assistant text did not echo prompt: {:?}",
        completed.assistant_text
    );
    assert!(
        completed
            .notifications
            .iter()
            .any(|notification| { notification.method == "item/agentMessage/delta" })
    );
    assert!(
        completed
            .notifications
            .iter()
            .any(|notification| { notification.method == "turn/completed" })
    );

    client.close().await.unwrap();
    server.stop().await;
    let _ = std::fs::remove_dir_all(root);
}

#[cfg(unix)]
#[tokio::test]
async fn console_endpoint_record_refuses_serve_then_becomes_stale_after_sigkill() {
    let smoke_id = uuid::Uuid::now_v7().simple().to_string();
    let root = std::path::Path::new("/tmp").join(format!("cdisep-{}", &smoke_id[..12]));
    let assets = root.join("console-assets");
    let user_home = root.join("user-home");
    std::fs::create_dir_all(&assets).unwrap();
    std::fs::write(
        assets.join("index.html"),
        "<!doctype html><title>test</title>",
    )
    .unwrap();
    let socket = root.join("run/verlet.sock");
    let config_path = root.join("verlet.toml");
    write_daemon_config(&config_path, &root, &socket);

    let mut console = tokio::process::Command::new(env!("CARGO_BIN_EXE_verlet"));
    model_catalog_test_support::disable_for_tokio_command(&mut console);
    let mut console = console
        .arg("console")
        .arg("--no-open")
        .arg("--cwd")
        .arg(&root)
        .arg("--config")
        .arg(&config_path)
        .env("VERLET_HOME", &user_home)
        .env("VERLET_CONSOLE_ASSET_DIR", &assets)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let console_pid = console.id().unwrap();
    let state_home = root.join("state");
    let endpoint = wait_for_endpoint_record(&state_home, &mut console).await;
    let canonical_state_home = std::fs::canonicalize(&state_home).unwrap();
    assert_eq!(endpoint.pid, console_pid);
    assert!(endpoint.unix_socket.is_absolute());
    assert_eq!(
        endpoint.unix_socket,
        canonical_state_home.join("verlet.sock")
    );
    assert!(endpoint.unix_socket.exists());
    let stream = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        tokio::net::UnixStream::connect(&endpoint.unix_socket),
    )
    .await
    .expect("console endpoint socket did not accept promptly")
    .unwrap();
    drop(stream);
    assert!(
        endpoint
            .ws_url
            .as_deref()
            .is_some_and(|url| url.starts_with("ws://127.0.0.1:"))
    );

    let mut serve = tokio::process::Command::new(env!("CARGO_BIN_EXE_verlet"));
    model_catalog_test_support::disable_for_tokio_command(&mut serve);
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        serve
            .arg("serve")
            .arg("--config")
            .arg(&config_path)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output(),
    )
    .await
    .expect("serve contention did not finish promptly")
    .unwrap();
    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    let expected = format!(
        "instance already running for {}, pid {}, socket {}",
        canonical_state_home.display(),
        endpoint.pid,
        endpoint.unix_socket.display()
    );
    assert!(
        stderr.contains(&expected),
        "expected {expected:?} in serve stderr {stderr:?}"
    );
    assert!(!stderr.contains("File is locked"));

    console.start_kill().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(30), console.wait())
        .await
        .expect("console did not exit after SIGKILL")
        .unwrap();
    assert!(
        state_home
            .join(verlet::adapters::app_server::ENDPOINT_RECORD_NAME)
            .is_file()
    );
    assert_eq!(
        verlet::adapters::app_server::instance::resolve_instance_endpoint(&state_home),
        None
    );
    let _ = std::fs::remove_dir_all(root);
}

fn write_daemon_config(
    config_path: &std::path::Path,
    root: &std::path::Path,
    socket: &std::path::Path,
) {
    let text = format!(
        r#"
[daemon.runtime]
cwd = "{}"
runtime_home = "runtime"
state_home = "state"

[daemon.app_server]
listen = "unix://{}"

[daemon.provider]
provider = "local"
"#,
        escape_toml_string(&std::env::current_dir().unwrap().display().to_string()),
        escape_toml_string(&socket.display().to_string()),
    );
    std::fs::write(config_path, text).unwrap();
    assert!(root.exists());
}

fn escape_toml_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

async fn connect_daemon_client(
    socket: &std::path::Path,
) -> verlet::adapters::operator_client::OperatorClient<tokio::net::UnixStream> {
    let mut last_error = None;
    for _ in 0..1_500 {
        if socket.exists() {
            match verlet::adapters::operator_client::OperatorClient::connect_unix(
                socket,
                verlet::adapters::operator_client::OperatorConnectConfig {
                    client_name: "verlet-daemon-smoke".to_string(),
                    ..verlet::adapters::operator_client::OperatorConnectConfig::default()
                },
            )
            .await
            {
                Ok(client) => return client,
                Err(err) => last_error = Some(err.to_string()),
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!(
        "timed out waiting for daemon socket {}; last connect error: {}",
        socket.display(),
        last_error.unwrap_or_else(|| "socket did not appear".to_string())
    );
}

async fn wait_for_endpoint_record(
    state_home: &std::path::Path,
    child: &mut tokio::process::Child,
) -> verlet::adapters::app_server::instance::InstanceEndpoint {
    for _ in 0..1_500 {
        if let Some(endpoint) =
            verlet::adapters::app_server::instance::resolve_instance_endpoint(state_home)
        {
            return endpoint;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("server exited before writing its endpoint record: {status}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!(
        "timed out waiting for endpoint record {}",
        state_home
            .join(verlet::adapters::app_server::ENDPOINT_RECORD_NAME)
            .display()
    );
}

struct ServeChild {
    child: Option<tokio::process::Child>,
}

impl ServeChild {
    async fn spawn(config_path: &std::path::Path) -> Self {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_verlet"));
        model_catalog_test_support::disable_for_tokio_command(&mut command);
        let child = command
            .arg("serve")
            .arg("--config")
            .arg(config_path)
            .env(
                "VERLET_HOME",
                config_path.parent().unwrap().join("user-home"),
            )
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        Self { child: Some(child) }
    }

    async fn stop(mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.start_kill();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(30), child.wait()).await;
        }
    }
}

impl Drop for ServeChild {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.start_kill();
        }
    }
}
