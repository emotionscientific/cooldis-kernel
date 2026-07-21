use cooldis::{CodexTuiConnectConfig, CodexTuiTestClient};
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::{Child, Command};
use uuid::Uuid;

#[tokio::test]
async fn daemon_run_serves_codex_remote_on_configured_unix_socket() {
    let smoke_id = Uuid::now_v7().simple().to_string();
    let root = Path::new("/tmp").join(format!("cdisd-{}", &smoke_id[..12]));
    std::fs::create_dir_all(&root).unwrap();
    let socket = root.join("run/cooldis.sock");
    let config_path = root.join("cooldis.toml");
    write_daemon_config(&config_path, &root, &socket);

    let daemon = DaemonChild::spawn(&config_path).await;
    let mut client = connect_daemon_client(&socket).await;

    let account = client.account_read().await.unwrap();
    assert_eq!(account["requiresOpenaiAuth"], false);
    let models = client.model_list().await.unwrap();
    assert!(matches!(models["data"].as_array(), Some(models) if !models.is_empty()));

    let completed = client
        .run_prompt("daemon smoke", Duration::from_secs(5))
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

fn write_daemon_config(config_path: &Path, root: &Path, socket: &Path) {
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

async fn connect_daemon_client(socket: &Path) -> CodexTuiTestClient<tokio::net::UnixStream> {
    let mut last_error = None;
    for _ in 0..150 {
        if socket.exists() {
            match CodexTuiTestClient::connect_unix(
                socket,
                CodexTuiConnectConfig {
                    client_name: "cooldis-daemon-smoke".to_string(),
                    ..CodexTuiConnectConfig::default()
                },
            )
            .await
            {
                Ok(client) => return client,
                Err(err) => last_error = Some(err.to_string()),
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "timed out waiting for daemon socket {}; last connect error: {}",
        socket.display(),
        last_error.unwrap_or_else(|| "socket did not appear".to_string())
    );
}

struct DaemonChild {
    child: Option<Child>,
}

impl DaemonChild {
    async fn spawn(config_path: &Path) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_cooldis"))
            .arg("daemon")
            .arg("run")
            .arg("--config")
            .arg(config_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        Self { child: Some(child) }
    }

    async fn stop(mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.start_kill();
            let _ = tokio::time::timeout(Duration::from_secs(30), child.wait()).await;
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
