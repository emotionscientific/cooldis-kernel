use cooldis::{
    AppServerListenAddr, CodexTuiConnectConfig, CodexTuiTestClient, CooldisAppServer,
    CooldisAppServerConfig, EventKind, EventStore, EventStreamId, SqliteSessionStore,
};
use serde_json::Value;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Output, Stdio};
use std::time::Duration;
use tokio::process::Command;
use tokio::task::JoinHandle;
use uuid::Uuid;

#[tokio::test]
async fn debug_rpc_cli_calls_and_streams_turns_over_websocket() {
    let root = PathBuf::from("/tmp").join(format!("cdis-debug-rpc-{}", Uuid::now_v7().simple()));
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let addr = unused_loopback_addr();
    let url = format!("ws://{addr}/rpc");
    let server = DebugRpcServer::start(&root, &workspace, addr).await;

    let call = run_cooldis(["debug", "rpc", "call", "thread/list", "--url", url.as_str()]).await;
    assert_success(&call);
    let thread_list: Value = serde_json::from_slice(&call.stdout).unwrap();
    assert!(thread_list["data"].as_array().is_some());

    let first = run_cooldis([
        "debug",
        "rpc",
        "turn",
        "--new",
        "--json",
        "first debug rpc turn",
        "--url",
        url.as_str(),
    ])
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

    let resumed = run_cooldis([
        "debug",
        "rpc",
        "turn",
        "--thread",
        thread_id.as_str(),
        "second debug rpc turn",
        "--url",
        url.as_str(),
    ])
    .await;
    assert_success(&resumed);
    let resumed_stdout = String::from_utf8(resumed.stdout).unwrap();
    assert!(
        resumed_stdout.contains("second debug rpc turn"),
        "resumed turn output did not include prompt: {resumed_stdout:?}"
    );
    let store = SqliteSessionStore::open(root.join("state/session_history.sqlite3"))
        .await
        .unwrap();
    let control_events = store
        .read_events(&EventStreamId::new(format!("control:{thread_id}")), None)
        .await
        .unwrap();
    let thread_events = store
        .read_events(&EventStreamId::new(format!("thread:{thread_id}")), None)
        .await
        .unwrap();
    assert_admission_precedes_execution(&control_events, &thread_events, "surface:debug-rpc");

    server.stop().await;
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

struct DebugRpcServer {
    task: JoinHandle<cooldis::CooldisResult<()>>,
}

impl DebugRpcServer {
    async fn start(root: &Path, workspace: &Path, addr: SocketAddr) -> Self {
        let listen = AppServerListenAddr::WebSocket(addr);
        let mut config = CooldisAppServerConfig::local(listen.clone(), workspace);
        config.runtime_home = root.join("runtime");
        config.state_home = root.join("state");
        let app = CooldisAppServer::new_local(config).await.unwrap();
        let task = tokio::spawn(async move { app.serve(listen).await });
        wait_for_websocket(&format!("ws://{addr}/rpc")).await;
        Self { task }
    }

    async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

async fn wait_for_websocket(url: &str) {
    let mut last_error = None;
    for _ in 0..100 {
        match CodexTuiTestClient::connect_websocket(
            url,
            CodexTuiConnectConfig {
                client_name: "cooldis-debug-rpc-test-wait".to_string(),
                ..CodexTuiConnectConfig::default()
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
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    panic!(
        "timed out waiting for debug rpc websocket {url}; last error: {}",
        last_error.unwrap_or_else(|| "none".to_string())
    );
}

async fn run_cooldis<const N: usize>(args: [&str; N]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_cooldis"))
        .args(args)
        .stdin(Stdio::null())
        .output()
        .await
        .unwrap()
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "command failed: status={:?}\nstdout={}\nstderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn jsonl_lines(bytes: &[u8]) -> Vec<Value> {
    String::from_utf8_lossy(bytes)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn agent_delta_text(notifications: &[Value]) -> String {
    notifications
        .iter()
        .filter(|notification| notification["method"].as_str() == Some("item/agentMessage/delta"))
        .filter_map(|notification| notification["params"]["delta"].as_str())
        .collect::<Vec<_>>()
        .join("")
}

fn completed_turn_text(notifications: &[Value]) -> String {
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

fn item_text(item: &Value) -> Option<&str> {
    item.get("text").and_then(Value::as_str).or_else(|| {
        item.get("content")
            .and_then(Value::as_array)
            .and_then(|content| content.first())
            .and_then(|content| content.get("text"))
            .and_then(Value::as_str)
    })
}

fn unused_loopback_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}
