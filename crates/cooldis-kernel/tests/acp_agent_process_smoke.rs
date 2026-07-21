use cooldis::{
    AppServerListenAddr, CooldisAppServer, CooldisAppServerConfig, EventKind, EventStore,
    EventStreamId, SqliteSessionStore,
};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command as TokioCommand};
use uuid::Uuid;

#[test]
fn acp_agent_binary_reports_stable_version() {
    let output = Command::new(env!("CARGO_BIN_EXE_cooldis-acp-agent"))
        .arg("--version")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "cooldis-acp-agent --version failed: {output:?}"
    );
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!("cooldis-acp-agent {}\n", env!("CARGO_PKG_VERSION"))
    );
    assert!(
        output.stderr.is_empty(),
        "version should not write stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn acp_agent_process_smoke_runs_binary_over_stdio() {
    let root = PathBuf::from("/tmp").join(format!("cdis-acp-process-{}", Uuid::now_v7().simple()));
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let socket = root.join("app.sock");
    let listen = AppServerListenAddr::Unix(socket.clone());
    let mut app_config = CooldisAppServerConfig::local(listen.clone(), &workspace);
    app_config.runtime_home = root.join("runtime");
    app_config.state_home = root.join("state");
    app_config.agent_registry_root = root.join("agents");
    let app = CooldisAppServer::new_local(app_config).await.unwrap();
    let serve_task = tokio::spawn(async move { app.serve(listen).await });
    wait_for_socket(&socket).await;

    let mut agent = AcpAgentChild::spawn(&socket).await;
    let (mut stdin, stdout) = agent.take_stdio();
    let mut lines = BufReader::new(stdout).lines();

    send(
        &mut stdin,
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{},"clientInfo":{"name":"process-smoke","version":"1"}}}"#,
    )
    .await;
    let init = read_json_response(&mut lines, 1).await;
    assert_eq!(init["result"]["agentInfo"]["name"], "cooldis-acp-agent");
    assert_eq!(init["result"]["agentInfo"]["title"], "Cooldis ACP Agent");
    assert_eq!(
        init["result"]["agentInfo"]["version"],
        env!("CARGO_PKG_VERSION")
    );

    send(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "session/new",
            "params": {
                "cwd": workspace.display().to_string(),
            },
        })
        .to_string(),
    )
    .await;
    let session = read_json_response(&mut lines, 2).await;
    let session_id = session["result"]["sessionId"]
        .as_str()
        .expect("session id")
        .to_string();
    assert_eq!(session["result"]["cooldis"]["threadId"], session_id);

    send(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "session/prompt",
            "params": {
                "sessionId": session_id,
                "prompt": [{ "type": "text", "text": "process smoke" }],
            },
        })
        .to_string(),
    )
    .await;
    let update = loop {
        let message = read_json_message(&mut lines).await;
        if message["params"]["update"]["sessionUpdate"] == "agent_message_chunk" {
            break message;
        }
        assert!(
            message.get("id").and_then(Value::as_u64) != Some(3),
            "prompt response arrived before text update: {message}"
        );
    };
    assert_eq!(update["method"], "session/update", "{update}");
    assert_eq!(update["params"]["sessionId"], session_id);
    assert_eq!(
        update["params"]["update"]["content"],
        json!({ "type": "text", "text": "local:process smoke" })
    );
    let prompt = read_json_response(&mut lines, 3).await;
    assert_eq!(prompt["result"]["stopReason"], "end_turn", "{prompt}");
    assert_eq!(
        prompt["result"]["cooldis"]["assistantText"],
        "local:process smoke"
    );
    let store = SqliteSessionStore::open(root.join("state/session_history.sqlite3"))
        .await
        .unwrap();
    let control_events = store
        .read_events(&EventStreamId::new(format!("control:{session_id}")), None)
        .await
        .unwrap();
    let thread_events = store
        .read_events(&EventStreamId::new(format!("thread:{session_id}")), None)
        .await
        .unwrap();
    assert_admission_precedes_execution(&control_events, &thread_events, "surface:acp-adapter");

    send(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "session/close",
            "params": { "sessionId": session_id },
        })
        .to_string(),
    )
    .await;
    let close = read_json_response(&mut lines, 4).await;
    assert_eq!(close["result"], json!({}), "{close}");

    agent.stop().await;
    serve_task.abort();
    let _ = serve_task.await;
    let _ = std::fs::remove_dir_all(root);
}

fn assert_admission_precedes_execution(
    control_events: &[cooldis::EventRecord],
    thread_events: &[cooldis::EventRecord],
    route_id: &str,
) {
    let admission = control_events
        .iter()
        .find(|event| {
            event.kind == EventKind::AdmissionDecided && event.payload["route_id"] == route_id
        })
        .expect("control stream missing expected admission.decided");
    let executed = thread_events
        .iter()
        .find(|event| {
            event.kind == EventKind::SessionEntryAppended
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

async fn send<W>(writer: &mut W, message: &str)
where
    W: tokio::io::AsyncWrite + Unpin,
{
    writer.write_all(message.as_bytes()).await.unwrap();
    writer.write_all(b"\n").await.unwrap();
    writer.flush().await.unwrap();
}

async fn read_json_message<R>(lines: &mut tokio::io::Lines<BufReader<R>>) -> Value
where
    R: tokio::io::AsyncRead + Unpin,
{
    let deadline = tokio::time::sleep(Duration::from_secs(30));
    tokio::pin!(deadline);
    tokio::select! {
        _ = &mut deadline => panic!("timed out waiting for ACP JSON-RPC message"),
        line = lines.next_line() => {
            let line = line.unwrap().expect("agent closed stdout before message");
            serde_json::from_str(&line).unwrap()
        }
    }
}

async fn read_json_response<R>(lines: &mut tokio::io::Lines<BufReader<R>>, id: u64) -> Value
where
    R: tokio::io::AsyncRead + Unpin,
{
    let deadline = tokio::time::sleep(Duration::from_secs(30));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => panic!("timed out waiting for ACP JSON-RPC response id {id}"),
            line = lines.next_line() => {
                let line = line.unwrap().expect("agent closed stdout before response");
                let value: Value = serde_json::from_str(&line).unwrap();
                if value.get("id").and_then(Value::as_u64) == Some(id) {
                    return value;
                }
            }
        }
    }
}

async fn wait_for_socket(path: &Path) {
    for _ in 0..500 {
        if path.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {}", path.display());
}

struct AcpAgentChild {
    child: Option<Child>,
    stdin: Option<tokio::process::ChildStdin>,
    stdout: Option<tokio::process::ChildStdout>,
}

impl AcpAgentChild {
    async fn spawn(socket: &Path) -> Self {
        let mut child = TokioCommand::new(env!("CARGO_BIN_EXE_cooldis-acp-agent"))
            .arg("--socket")
            .arg(socket)
            .arg("--timeout-ms")
            .arg("10000")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().expect("agent stdin");
        let stdout = child.stdout.take().expect("agent stdout");
        Self {
            child: Some(child),
            stdin: Some(stdin),
            stdout: Some(stdout),
        }
    }

    fn take_stdio(&mut self) -> (tokio::process::ChildStdin, tokio::process::ChildStdout) {
        (
            self.stdin.take().expect("agent stdin"),
            self.stdout.take().expect("agent stdout"),
        )
    }

    async fn stop(mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.start_kill();
            let _ = tokio::time::timeout(Duration::from_secs(30), child.wait()).await;
        }
    }
}

impl Drop for AcpAgentChild {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.start_kill();
        }
    }
}
