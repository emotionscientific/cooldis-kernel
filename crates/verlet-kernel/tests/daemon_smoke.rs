#[tokio::test]
async fn daemon_run_serves_codex_remote_on_configured_unix_socket() {
    let smoke_id = uuid::Uuid::now_v7().simple().to_string();
    let root = std::path::Path::new("/tmp").join(format!("cdisd-{}", &smoke_id[..12]));
    std::fs::create_dir_all(&root).unwrap();
    let socket = root.join("run/verlet.sock");
    let config_path = root.join("verlet.toml");
    write_daemon_config(&config_path, &root, &socket);

    let daemon = DaemonChild::spawn(&config_path).await;
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
    daemon.stop().await;
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
) -> verlet::adapters::codex_tui::CodexTuiTestClient<tokio::net::UnixStream> {
    let mut last_error = None;
    for _ in 0..1_500 {
        if socket.exists() {
            match verlet::adapters::codex_tui::CodexTuiTestClient::connect_unix(
                socket,
                verlet::adapters::codex_tui::CodexTuiConnectConfig {
                    client_name: "verlet-daemon-smoke".to_string(),
                    ..verlet::adapters::codex_tui::CodexTuiConnectConfig::default()
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

struct DaemonChild {
    child: Option<tokio::process::Child>,
}

impl DaemonChild {
    async fn spawn(config_path: &std::path::Path) -> Self {
        let child = tokio::process::Command::new(env!("CARGO_BIN_EXE_verlet"))
            .arg("daemon")
            .arg("run")
            .arg("--config")
            .arg(config_path)
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

impl Drop for DaemonChild {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.start_kill();
        }
    }
}
