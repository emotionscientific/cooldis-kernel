use tokio::io::AsyncBufReadExt as _;
use verlet::EventStore as _;
use verlet::daemon::identity::IdentityAuthority as _;

static RPC_PROCESS_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn rpc_cli_startup_names_websocket_state_home_and_credential_path() {
    let _process_guard = RPC_PROCESS_TEST_LOCK.lock().await;
    let root = std::env::temp_dir().join(format!(
        "cdis-rpc-startup-ws-{}",
        uuid::Uuid::now_v7().simple()
    ));
    let state_home = root.join("state");
    let runtime_home = root.join("runtime");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let listen = format!("ws://{}/rpc", unused_loopback_addr());

    let lines = rpc_startup_lines(&listen, &state_home, &runtime_home, &workspace).await;

    assert_eq!(lines[0], format!("verlet rpc listening on {listen}"));
    assert_eq!(
        lines[1],
        format!("verlet rpc state home: {}", state_home.display())
    );
    assert_eq!(
        lines[2],
        "Before starting this server, mint a bearer token with `verlet identity` against this state home; WebSocket clients pass that token in VERLET_APP_SERVER_TOKEN."
    );

    let _ = std::fs::remove_dir_all(root);
}

#[cfg(unix)]
#[tokio::test]
async fn rpc_cli_startup_names_unix_state_home_and_peer_authentication() {
    let _process_guard = RPC_PROCESS_TEST_LOCK.lock().await;
    let root = std::env::temp_dir().join(format!(
        "cdis-rpc-startup-unix-{}",
        uuid::Uuid::now_v7().simple()
    ));
    let state_home = root.join("state");
    let runtime_home = root.join("runtime");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let socket = root.join("rpc.sock");
    let listen = format!("unix://{}", socket.display());

    let lines = rpc_startup_lines(&listen, &state_home, &runtime_home, &workspace).await;

    assert_eq!(lines[0], format!("verlet rpc listening on {listen}"));
    assert_eq!(
        lines[1],
        format!("verlet rpc state home: {}", state_home.display())
    );
    assert_eq!(lines[2], "Same-uid Unix socket peers need no token.");

    let _ = std::fs::remove_dir_all(root);
}

async fn rpc_startup_lines(
    listen: &str,
    state_home: &std::path::Path,
    runtime_home: &std::path::Path,
    workspace: &std::path::Path,
) -> Vec<String> {
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_verlet"))
        .args([
            "rpc",
            "--listen",
            listen,
            "--state-home",
            state_home.to_str().unwrap(),
            "--runtime-home",
            runtime_home.to_str().unwrap(),
            "--cwd",
            workspace.to_str().unwrap(),
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("failed to spawn verlet rpc");
    let stderr = child
        .stderr
        .take()
        .expect("verlet rpc stderr should be piped");
    let mut reader = tokio::io::BufReader::new(stderr);
    let lines = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let mut lines = Vec::new();
        while lines.len() < 3 {
            let mut line = String::new();
            let bytes = reader
                .read_line(&mut line)
                .await
                .expect("failed to read verlet rpc startup output");
            assert_ne!(
                bytes, 0,
                "verlet rpc exited before startup output completed"
            );
            let line = line.trim_end();
            if line.starts_with("verlet rpc listening on ")
                || line.starts_with("verlet rpc state home: ")
                || line.starts_with("Before starting this server, mint ")
                || line == "Same-uid Unix socket peers need no token."
            {
                lines.push(line.to_string());
            }
        }
        lines
    })
    .await
    .expect("timed out waiting for verlet rpc startup output");
    child.kill().await.expect("failed to stop verlet rpc");
    lines
}

#[tokio::test]
async fn debug_rpc_cli_calls_and_streams_turns_over_websocket() {
    let _process_guard = RPC_PROCESS_TEST_LOCK.lock().await;
    let root = TestRoot::new("cdis-debug-rpc");
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let server = DebugRpcServer::start(root.path(), &workspace).await;
    let url = server.url();

    let call = run_verlet(
        ["debug", "rpc", "call", "thread/list", "--url", url.as_str()],
        Some(&server.token),
    )
    .await;
    assert_success(&call);
    let thread_list: serde_json::Value = serde_json::from_slice(&call.stdout).unwrap();
    assert!(thread_list["data"].as_array().is_some());

    let first = run_verlet(
        [
            "debug",
            "rpc",
            "turn",
            "--new",
            "--json",
            "first debug rpc turn",
            "--url",
            url.as_str(),
        ],
        Some(&server.token),
    )
    .await;
    assert_success(&first);
    let thread_id = String::from_utf8(first.stderr.clone())
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .to_string();
    assert!(!thread_id.is_empty());
    let jsonl = jsonl_lines(&first.stdout);
    assert!(
        jsonl
            .iter()
            .all(|notification| notification["params"]["threadId"].as_str() == Some(&thread_id))
    );
    let delta_text = agent_delta_text(&jsonl);
    let completed_text = completed_turn_text(&jsonl);
    assert_eq!(delta_text, completed_text);
    assert!(
        completed_text.contains("first debug rpc turn"),
        "completed text did not include prompt: {completed_text:?}"
    );

    let live_bind = run_verlet(
        [
            "debug",
            "bind",
            thread_id.as_str(),
            "--json",
            "--url",
            url.as_str(),
        ],
        Some(&server.token),
    )
    .await;
    assert_success(&live_bind);
    let live_explanation: serde_json::Value = serde_json::from_slice(&live_bind.stdout).unwrap();
    assert_eq!(live_explanation["thread_id"], thread_id);
    assert_eq!(live_explanation["model"]["origin"], "manifest-default");

    let missing_thread_id = uuid::Uuid::now_v7().to_string();
    let missing_bind = run_verlet(
        [
            "debug",
            "bind",
            missing_thread_id.as_str(),
            "--url",
            url.as_str(),
        ],
        Some(&server.token),
    )
    .await;
    assert!(!missing_bind.status.success());
    assert!(
        String::from_utf8_lossy(&missing_bind.stderr).contains("thread not found"),
        "missing-thread error was not preserved: {}",
        String::from_utf8_lossy(&missing_bind.stderr)
    );

    let resumed = run_verlet(
        [
            "debug",
            "rpc",
            "turn",
            "--thread",
            thread_id.as_str(),
            "second debug rpc turn",
            "--url",
            url.as_str(),
        ],
        Some(&server.token),
    )
    .await;
    assert_success(&resumed);
    let resumed_stdout = String::from_utf8(resumed.stdout).unwrap();
    assert!(
        resumed_stdout.contains("second debug rpc turn"),
        "resumed turn output did not include prompt: {resumed_stdout:?}"
    );
    let store = verlet::SqliteSessionStore::open(root.path().join("state/session_history.sqlite3"))
        .await
        .unwrap();
    let control_events = store
        .read_events(
            &verlet::EventStreamId::new(format!("control:{thread_id}")),
            None,
        )
        .await
        .unwrap();
    let thread_events = store
        .read_events(
            &verlet::EventStreamId::new(format!("thread:{thread_id}")),
            None,
        )
        .await
        .unwrap();
    assert_admission_precedes_execution(&control_events, &thread_events, "surface:debug-rpc");
    drop(store);

    server.stop().await;
    let journal = root.path().join("state/session_history.sqlite3");
    let offline_bind = run_verlet(
        [
            "debug",
            "bind",
            thread_id.as_str(),
            "--json",
            "--journal",
            journal.to_str().unwrap(),
        ],
        None,
    )
    .await;
    assert_success(&offline_bind);
    let offline_explanation: serde_json::Value =
        serde_json::from_slice(&offline_bind.stdout).unwrap();
    assert_eq!(offline_explanation, live_explanation);
}

#[tokio::test]
async fn debug_rpc_cli_renders_rpc_client_errors_without_internal_names() {
    let _process_guard = RPC_PROCESS_TEST_LOCK.lock().await;
    let closed_url = "ws://127.0.0.1:0/rpc";
    let closed = run_verlet(
        ["debug", "rpc", "call", "thread/list", "--url", closed_url],
        None,
    )
    .await;
    assert!(!closed.status.success());
    let closed_error = String::from_utf8(closed.stderr).unwrap();
    assert!(
        closed_error.starts_with(&format!(
            "verlet: failed to connect to the Verlet RPC endpoint `{closed_url}`:"
        )),
        "unexpected closed-port error: {closed_error:?}"
    );
    assert!(!closed_error.contains("runtime factory failed"));
    assert!(!closed_error.contains("Codex TUI"));

    let root = TestRoot::new("cdis-debug-rpc-errors");
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let server = DebugRpcServer::start(root.path(), &workspace).await;
    let url = server.url();

    let unauthorized = run_verlet(
        ["debug", "rpc", "call", "thread/list", "--url", url.as_str()],
        None,
    )
    .await;
    assert!(!unauthorized.status.success());
    let unauthorized_error = String::from_utf8(unauthorized.stderr).unwrap();
    assert!(
        unauthorized_error.starts_with(&format!(
            "verlet: failed to connect to the Verlet RPC endpoint `{url}`:"
        )),
        "unexpected unauthorized error: {unauthorized_error:?}"
    );
    assert!(
        unauthorized_error.contains("401"),
        "unauthorized error did not preserve HTTP status: {unauthorized_error:?}"
    );

    let refused = run_verlet(
        ["debug", "rpc", "call", "thread/list", "--url", url.as_str()],
        Some(&server.adapter_token),
    )
    .await;
    assert!(!refused.status.success());
    let refused_error = String::from_utf8(refused.stderr).unwrap();
    assert_eq!(
        refused_error,
        "verlet: request `thread/list` was refused: request is not authorized for this principal\n"
    );

    server.stop().await;
}

fn assert_admission_precedes_execution(
    control_events: &[verlet::EventRecord],
    thread_events: &[verlet::EventRecord],
    route_id: &str,
) {
    let admission = control_events
        .iter()
        .find(|event| {
            event.kind == verlet::EventKind::AdmissionDecided
                && event.payload["route_id"] == route_id
        })
        .expect("control stream missing expected admission.decided");
    let executed = thread_events
        .iter()
        .find(|event| {
            event.kind == verlet::EventKind::SessionEntryAppended
                && event.payload["runtime_kind"] != "thread_started"
        })
        .expect("thread stream missing executed turn session entry");
    assert!(
        (
            admission.created_at_ms,
            admission.stream_id.to_string(),
            admission.sequence.get(),
            admission.id.to_string(),
        ) < (
            executed.created_at_ms,
            executed.stream_id.to_string(),
            executed.sequence.get(),
            executed.id.to_string(),
        ),
        "admission.decided must precede executed turn session entry"
    );
}

struct DebugRpcServer {
    addr: std::net::SocketAddr,
    token: String,
    adapter_token: String,
    task: Option<tokio::task::JoinHandle<verlet::VerletResult<()>>>,
}

impl DebugRpcServer {
    async fn start(root: &std::path::Path, workspace: &std::path::Path) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let listen = verlet::AppServerListenAddr::WebSocket(addr);
        let mut config = verlet::VerletAppServerConfig::local(listen, workspace);
        config.runtime_home = root.join("runtime");
        config.state_home = root.join("state");
        let app = verlet::VerletAppServer::new_local(config).await.unwrap();
        let store = verlet::SqliteSessionStore::open(app.session_store_path())
            .await
            .unwrap();
        let authority = verlet::daemon::identity::SqliteIdentityAuthority::new(
            store,
            std::sync::Arc::new(verlet::SystemDaemonClock),
            None,
        )
        .await
        .unwrap();
        let principal = verlet::daemon::identity::PrincipalId::new(app.user_id());
        let token = authority
            .mint_credential(&principal, &principal, None)
            .await
            .unwrap()
            .1;
        let adapter = verlet::daemon::identity::PrincipalId::new("adapter:debug-rpc-error-test");
        authority
            .declare_principal(
                &principal,
                &adapter,
                verlet::daemon::identity::PrincipalKind::Adapter,
                "Debug RPC error test adapter",
            )
            .await
            .unwrap();
        let adapter_token = authority
            .mint_credential(&principal, &adapter, None)
            .await
            .unwrap()
            .1;
        let task = tokio::spawn(async move { app.serve_websocket_listener(listener).await });
        let server = Self {
            addr,
            token,
            adapter_token,
            task: Some(task),
        };
        wait_for_websocket(&format!("ws://{addr}/rpc"), &server.token).await;
        server
    }

    fn url(&self) -> String {
        format!("ws://{}/rpc", self.addr)
    }

    async fn stop(mut self) {
        let task = self.task.take().expect("debug RPC server task missing");
        task.abort();
        let _ = task.await;
    }
}

impl Drop for DebugRpcServer {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
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

fn unused_loopback_addr() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

async fn wait_for_websocket(url: &str, token: &str) {
    let mut last_error = None;
    for _ in 0..1_500 {
        match verlet::CodexTuiTestClient::connect_websocket(
            url,
            verlet::CodexTuiConnectConfig {
                client_name: "verlet-debug-rpc-test-wait".to_string(),
                bearer_token: Some(token.to_string()),
                ..verlet::CodexTuiConnectConfig::default()
            },
        )
        .await
        {
            Ok(mut client) => {
                client.close().await.unwrap();
                return;
            }
            Err(err) => {
                last_error = Some(err.to_string());
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
    }
    panic!(
        "timed out waiting for debug rpc websocket {url}; last error: {}",
        last_error.unwrap_or_else(|| "none".to_string())
    );
}

async fn run_verlet<const N: usize>(args: [&str; N], token: Option<&str>) -> std::process::Output {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_verlet"));
    command.args(args).stdin(std::process::Stdio::null());
    if let Some(token) = token {
        command.env("VERLET_APP_SERVER_TOKEN", token);
    } else {
        command.env_remove("VERLET_APP_SERVER_TOKEN");
    }
    command.output().await.unwrap()
}

fn assert_success(output: &std::process::Output) {
    assert!(
        output.status.success(),
        "command failed: status={:?}\nstdout={}\nstderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn jsonl_lines(bytes: &[u8]) -> Vec<serde_json::Value> {
    String::from_utf8_lossy(bytes)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn agent_delta_text(notifications: &[serde_json::Value]) -> String {
    notifications
        .iter()
        .filter(|notification| notification["method"].as_str() == Some("item/agentMessage/delta"))
        .filter_map(|notification| notification["params"]["delta"].as_str())
        .collect::<Vec<_>>()
        .join("")
}

fn completed_turn_text(notifications: &[serde_json::Value]) -> String {
    notifications
        .iter()
        .find(|notification| notification["method"].as_str() == Some("turn/completed"))
        .and_then(|notification| notification["params"]["turn"]["items"].as_array())
        .into_iter()
        .flatten()
        .filter(|item| item["type"].as_str() == Some("agentMessage"))
        .filter_map(item_text)
        .collect::<Vec<_>>()
        .join("")
}

fn item_text(item: &serde_json::Value) -> Option<&str> {
    item.get("text")
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            item.get("content")
                .and_then(serde_json::Value::as_array)
                .and_then(|content| content.first())
                .and_then(|content| content.get("text"))
                .and_then(serde_json::Value::as_str)
        })
}
