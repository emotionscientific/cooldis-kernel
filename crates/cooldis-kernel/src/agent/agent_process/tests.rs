use super::*;
use crate::{
    PROCESS_EXEC_OPERATION, PROCESS_POLL_OPERATION, PROCESS_TERMINATE_OPERATION,
    PROCESS_WRITE_OPERATION, RuntimeHost, THREAD_WAIT_OPERATION, ThreadCoordinates, ThreadTopology,
    VirtualBashRuntimeFactory,
};
use base64::engine::general_purpose::STANDARD;
use std::path::PathBuf;
use std::sync::Arc;

#[tokio::test]
async fn kernel_thread_operations_spawn_wait_and_report_children() {
    let host = RuntimeHost::new(Arc::new(VirtualBashRuntimeFactory::default()));
    let root = host
        .start_thread(
            ThreadCoordinates::new("tenant", "user", "session"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let provider =
        KernelThreadOperationProvider::new(host.kernel_control(), root.context().clone());

    let spawn = provider
        .invoke_json(
            THREAD_SPAWN_OPERATION,
            json!({
                "task_name": "worker",
                "message": "echo tool-child",
            }),
        )
        .await
        .unwrap();
    let child_thread_id =
        parse_thread_id(spawn["thread_id"].as_str().unwrap(), "thread_id").unwrap();
    assert_eq!(spawn["operation"], "cooldis.thread_spawn");
    assert_eq!(
        spawn["parent_thread_id"].as_str().unwrap(),
        root.context().coordinates.thread_id.to_string()
    );

    let wait = provider
        .invoke_json(
            THREAD_WAIT_OPERATION,
            json!({
                "target_thread_id": child_thread_id.to_string(),
                "timeout_ms": 1_000,
            }),
        )
        .await
        .unwrap();
    assert_eq!(wait["operation"], "cooldis.thread_wait");
    assert_eq!(wait["timed_out"], false);
    assert!(
        wait["latest_output"]
            .as_str()
            .unwrap()
            .contains("tool-child")
    );

    let status = provider
        .invoke_json(THREAD_STATUS_OPERATION, json!({}))
        .await
        .unwrap();
    assert_eq!(status["operation"], "cooldis.thread_status");
    let child_ids = status["children"]
        .as_array()
        .unwrap()
        .iter()
        .map(|child| child["thread_id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(child_ids, vec![child_thread_id.to_string()]);

    provider
        .invoke_json(
            THREAD_CANCEL_OPERATION,
            json!({
                "target_thread_id": child_thread_id.to_string(),
            }),
        )
        .await
        .unwrap();
    host.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn kernel_thread_operations_reject_cross_session_targets() {
    let host = RuntimeHost::new(Arc::new(VirtualBashRuntimeFactory::default()));
    let root = host
        .start_thread(
            ThreadCoordinates::new("tenant", "user", "session-a"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let other = host
        .start_thread(
            ThreadCoordinates::new("tenant", "user", "session-b"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let provider =
        KernelThreadOperationProvider::new(host.kernel_control(), root.context().clone());

    let err = provider
        .invoke_json(
            THREAD_STATUS_OPERATION,
            json!({
                "target_thread_id": other.context().coordinates.thread_id.to_string(),
            }),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CooldisError::ThreadScopeMismatch { thread_id, .. }
            if thread_id == other.context().coordinates.thread_id
    ));
    host.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn kernel_process_operations_exec_and_poll_host_command() {
    let host = RuntimeHost::new(Arc::new(VirtualBashRuntimeFactory::default()));
    let root = host
        .start_thread(
            ThreadCoordinates::new("tenant", "user", "session"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let provider = KernelProcessOperationProvider::new(root.context().clone(), temp_cwd("exec"));

    let started = provider
        .invoke_json(
            PROCESS_EXEC_OPERATION,
            json!({
                "command": ["/bin/sh", "-c", "sleep 0.1; printf done"],
                "yield_time_ms": 1,
                "timeout_ms": 2_000,
                "output_bytes_cap": 4_096
            }),
        )
        .await
        .unwrap();
    assert_eq!(started["operation"], "cooldis.process_exec");
    assert_eq!(started["backend"], "host_bash");
    assert_eq!(started["status"], "running");
    let process_id = started["process_id"].as_str().unwrap().to_string();

    let polled = provider
        .invoke_json(
            PROCESS_POLL_OPERATION,
            json!({
                "process_id": process_id,
                "yield_time_ms": 1_000,
                "output_bytes_cap": 4_096
            }),
        )
        .await
        .unwrap();
    assert_eq!(polled["operation"], "cooldis.process_poll");
    assert_eq!(polled["backend"], "host_bash");
    assert_eq!(polled["status"], "completed");
    assert_eq!(polled["exit_code"], 0);
    assert_eq!(polled["stdout"], "done");
    assert!(polled.get("process_id").is_none());
    assert!(polled["event_count"].as_u64().unwrap() >= 2);

    host.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn kernel_process_operations_write_and_terminate_host_process() {
    let host = RuntimeHost::new(Arc::new(VirtualBashRuntimeFactory::default()));
    let root = host
        .start_thread(
            ThreadCoordinates::new("tenant", "user", "session"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let provider = KernelProcessOperationProvider::new(root.context().clone(), temp_cwd("stdin"));

    let started = provider
        .invoke_json(
            PROCESS_EXEC_OPERATION,
            json!({
                "command": ["/bin/cat"],
                "stream_stdin": true,
                "yield_time_ms": 1,
                "timeout_ms": 5_000,
                "output_bytes_cap": 4_096
            }),
        )
        .await
        .unwrap();
    assert_eq!(started["status"], "running");
    let process_id = started["process_id"].as_str().unwrap().to_string();

    let wrote = provider
        .invoke_json(
            PROCESS_WRITE_OPERATION,
            json!({
                "process_id": process_id,
                "delta_base64": base64::Engine::encode(&STANDARD, "hello\n"),
                "yield_time_ms": 500,
                "output_bytes_cap": 4_096
            }),
        )
        .await
        .unwrap();
    assert_eq!(wrote["operation"], "cooldis.process_write");
    assert_eq!(wrote["status"], "running");
    assert_eq!(wrote["stdout"], "hello\n");

    let process_id = wrote["process_id"].as_str().unwrap().to_string();
    let terminated = provider
        .invoke_json(
            PROCESS_TERMINATE_OPERATION,
            json!({
                "process_id": process_id,
                "reason": "test cleanup",
                "yield_time_ms": 1_000
            }),
        )
        .await
        .unwrap();
    assert_eq!(terminated["operation"], "cooldis.process_terminate");
    assert_eq!(terminated["status"], "cancelled");
    assert!(terminated.get("process_id").is_none());

    host.shutdown_all().await.unwrap();
}

fn temp_cwd(name: &str) -> PathBuf {
    let cwd = std::env::temp_dir().join(format!(
        "cooldis-kernel-process-{name}-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir_all(&cwd).unwrap();
    cwd
}
