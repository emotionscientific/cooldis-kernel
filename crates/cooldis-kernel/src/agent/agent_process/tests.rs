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
    assert_eq!(spawn["handle"]["kind"], "thread");
    assert_eq!(spawn["handle"]["id"], child_thread_id.to_string());
    assert!(spawn["dispatch_id"].as_str().is_some());
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
async fn retried_thread_submit_dispatch_enqueues_one_turn() {
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
    let arguments = json!({
        "target_thread_id": root.context().coordinates.thread_id.to_string(),
        "message": "echo submit-once",
        "dispatch_id": "submit-dispatch-1",
    });

    let first = provider
        .invoke_json(THREAD_SUBMIT_OPERATION, arguments.clone())
        .await
        .unwrap();
    let retry = provider
        .invoke_json(THREAD_SUBMIT_OPERATION, arguments)
        .await
        .unwrap();

    assert_eq!(first["dispatch_id"], "submit-dispatch-1");
    assert_eq!(retry["dispatch_id"], "submit-dispatch-1");
    assert_eq!(first["turn_id"], retry["turn_id"]);
    assert_eq!(first["interaction_id"], retry["interaction_id"]);
    tokio::task::yield_now().await;
    let session = root.session_context().await.unwrap();
    assert_eq!(
        session
            .messages
            .iter()
            .filter(|message| matches!(
                message,
                crate::CanonicalMessage::User { content, .. }
                    if content.iter().any(|content| matches!(
                        content,
                        crate::CanonicalContent::Text { text, .. }
                            if text == "echo submit-once"
                    ))
            ))
            .count(),
        1
    );

    host.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn thread_submit_dispatch_fold_is_scoped_to_the_target_thread() {
    let host = RuntimeHost::new(Arc::new(VirtualBashRuntimeFactory::default()));
    let caller = host
        .start_thread(
            ThreadCoordinates::new("tenant", "user", "session"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let first_target = host
        .start_thread(
            ThreadCoordinates::new("tenant", "user", "session"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let second_target = host
        .start_thread(
            ThreadCoordinates::new("tenant", "user", "session"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let control = host.kernel_control();
    let dispatch_id = DispatchId::new("shared-submit-dispatch");

    let first = control
        .submit_to_thread_with_dispatch(
            caller.context(),
            first_target.context().coordinates.thread_id,
            dispatch_id.clone(),
            TurnInput::text("echo first target"),
        )
        .await
        .unwrap();
    let retry = control
        .submit_to_thread_with_dispatch(
            caller.context(),
            first_target.context().coordinates.thread_id,
            dispatch_id.clone(),
            TurnInput::text("echo folded retry"),
        )
        .await
        .unwrap();
    let second = control
        .submit_to_thread_with_dispatch(
            caller.context(),
            second_target.context().coordinates.thread_id,
            dispatch_id,
            TurnInput::text("echo second target"),
        )
        .await
        .unwrap();

    assert_eq!(first, retry);
    assert_eq!(first.turn_id, second.turn_id);
    assert_ne!(first.interaction_id, second.interaction_id);
    for interaction_id in [first.interaction_id, second.interaction_id] {
        let interaction_id = uuid::Uuid::parse_str(&interaction_id.to_string()).unwrap();
        assert_eq!(interaction_id.get_version_num(), 8);
    }
    tokio::task::yield_now().await;
    let first_session = first_target.session_context().await.unwrap();
    let second_session = second_target.session_context().await.unwrap();
    assert!(first_session.messages.iter().any(|message| matches!(
        message,
        crate::CanonicalMessage::User { content, .. }
            if content.iter().any(|content| matches!(
                content,
                crate::CanonicalContent::Text { text, .. } if text == "echo first target"
            ))
    )));
    assert!(first_session.messages.iter().all(|message| !matches!(
        message,
        crate::CanonicalMessage::User { content, .. }
            if content.iter().any(|content| matches!(
                content,
                crate::CanonicalContent::Text { text, .. } if text == "echo folded retry"
            ))
    )));
    assert!(second_session.messages.iter().any(|message| matches!(
        message,
        crate::CanonicalMessage::User { content, .. }
            if content.iter().any(|content| matches!(
                content,
                crate::CanonicalContent::Text { text, .. } if text == "echo second target"
            ))
    )));

    host.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn compatibility_submit_without_dispatch_keeps_legacy_turn_identity() {
    let host = RuntimeHost::new(Arc::new(VirtualBashRuntimeFactory::default()));
    let root = host
        .start_thread(
            ThreadCoordinates::new("tenant", "user", "session"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();

    let receipt = host
        .kernel_control()
        .submit_to_thread(
            root.context(),
            root.context().coordinates.thread_id,
            None,
            TurnInput::text("echo compatibility submit"),
        )
        .await
        .unwrap();

    assert!(receipt.turn_id.starts_with("agent-process-v1-"));
    let interaction_id = uuid::Uuid::parse_str(&receipt.interaction_id.to_string()).unwrap();
    assert_eq!(interaction_id.get_version_num(), 7);
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
