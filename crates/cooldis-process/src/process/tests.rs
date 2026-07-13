use super::*;
use crate::OperationId;
use crate::{
    AsyncExecutionManager, AsyncProcessStartRequest, ExecutionDeadline, HostBashLiveBackend,
    ProcessSnapshotStatus,
};
use futures_util::stream;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn virtual_process_retains_output_and_exit_status() {
    let process = CooldisProcessHandle::from_virtual_command(
        "echo hi",
        VirtualCommandOutput {
            stdout: "hi\n".to_string(),
            stderr: "warn\n".to_string(),
            exit_code: 2,
            stdout_truncated: true,
            stderr_truncated: false,
        },
    );

    let output = process.output();

    assert_eq!(output.stdout_text_lossy(), "hi\n");
    assert_eq!(output.stderr_text_lossy(), "warn\n");
    assert_eq!(output.exit_code(), Some(2));
    assert!(!output.success());
    assert!(output.stdout_truncated);
    assert!(!output.stderr_truncated);
    assert_eq!(process.events().len(), 6);
}

#[test]
fn external_timeout_maps_to_terminal_process_state() {
    let request = ExternalCommandRequest {
        invocation: crate::ExternalCommandInvocation::Script("sleep 10".to_string()),
        executor: ExternalExecutorKind::HostBash,
        cwd: PathBuf::from("/workspace"),
        stdin: None,
        deadline: crate::ExecutionDeadline::from_now(std::time::Duration::from_millis(10)),
        max_output_bytes: 1024,
    };
    let process = CooldisProcessHandle::from_external_command(
        &request,
        ExternalCommandResult::new(VirtualCommandOutput {
            stdout: String::new(),
            stderr: "host bash exec timed out\n".to_string(),
            exit_code: 124,
            stdout_truncated: false,
            stderr_truncated: false,
        }),
    );

    let output = process.output();

    assert_eq!(output.exit_code(), Some(124));
    assert!(matches!(
        output.terminal,
        Some(CooldisProcessTerminalState::TimedOut { .. })
    ));
}

#[tokio::test]
async fn bridge_stream_is_live_and_retained() {
    let operation_id = OperationId::new();
    let process = CooldisProcessHandle::new(CooldisProcessBackend::Bridge, "bridge exec");
    let mut live = process.subscribe();
    let stream = stream::iter(vec![
        Ok(OperationEvent::Started { operation_id }),
        Ok(OperationEvent::Stdout {
            operation_id,
            bytes: b"hello".to_vec(),
        }),
        Ok(OperationEvent::Stderr {
            operation_id,
            bytes: b"warn".to_vec(),
        }),
        Ok(OperationEvent::Artifact {
            operation_id,
            artifact_id: "artifact-1".to_string(),
            path: Some(PathBuf::from("/tmp/report.json")),
            mime_type: Some("application/json".to_string()),
        }),
        Ok(OperationEvent::Cancelled {
            operation_id,
            reason: "stop".to_string(),
        }),
    ]);

    let join = process.attach_bridge_event_stream(Box::pin(stream));

    let started = live.recv().await.unwrap();
    assert!(matches!(
        started.kind,
        CooldisProcessEventKind::Started { .. }
    ));
    let stdout = live.recv().await.unwrap();
    assert!(matches!(
        stdout.kind,
        CooldisProcessEventKind::Stdout { ref bytes } if bytes == b"hello"
    ));
    join.await.unwrap().unwrap();

    let output = process.output();
    assert_eq!(output.stdout_text_lossy(), "hello");
    assert_eq!(output.stderr_text_lossy(), "warn");
    assert_eq!(output.artifacts.len(), 1);
    assert_eq!(output.artifacts[0].artifact_id, "artifact-1");
    assert!(matches!(
        output.terminal,
        Some(CooldisProcessTerminalState::Cancelled { reason }) if reason == "stop"
    ));
    assert_eq!(process.events().len(), 5);
}

#[tokio::test]
async fn async_manager_yields_running_handle_then_poll_completes_host_command() {
    let manager = AsyncExecutionManager::default();
    let backend = Arc::new(HostBashLiveBackend::default());
    let request = AsyncProcessStartRequest::host_command(
        vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "sleep 0.05; printf done".to_string(),
        ],
        std::env::current_dir().unwrap(),
    )
    .with_deadline(ExecutionDeadline::from_now(Duration::from_secs(1)))
    .with_yield_time(Duration::from_millis(5))
    .with_output_cap_bytes(1024)
    .retain_terminal_until_acknowledged();

    let started = manager.start(backend, request).await.unwrap();
    assert_eq!(started.snapshot.status, ProcessSnapshotStatus::Running);
    let process_id = started.snapshot.process_id.expect("running process id");

    let completed = manager
        .poll(process_id, Duration::from_secs(1), 1024)
        .await
        .unwrap();
    assert_eq!(completed.snapshot.status, ProcessSnapshotStatus::Completed);
    assert_eq!(completed.snapshot.process_id, Some(process_id));
    assert_eq!(String::from_utf8_lossy(&completed.snapshot.stdout), "done");
    assert_eq!(completed.snapshot.exit_code, Some(0));

    let repeated = manager.snapshot(process_id, 1024).await.unwrap();
    assert_eq!(repeated.snapshot, completed.snapshot);
    assert!(manager.acknowledge_terminal(process_id).await.unwrap());
    assert!(manager.snapshot(process_id, 1024).await.is_err());
}

#[tokio::test]
async fn async_manager_uses_preallocated_process_id() {
    let manager = AsyncExecutionManager::default();
    let process_id = CooldisProcessId::new();
    let request = AsyncProcessStartRequest::host_command(
        vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "sleep 0.05".to_string(),
        ],
        std::env::current_dir().unwrap(),
    )
    .with_process_id(process_id)
    .with_deadline(ExecutionDeadline::from_now(Duration::from_secs(1)))
    .with_yield_time(Duration::from_millis(1))
    .retain_terminal_until_acknowledged();

    let started = manager
        .start(Arc::new(HostBashLiveBackend), request)
        .await
        .unwrap();

    assert_eq!(started.snapshot.process_id, Some(process_id));
    manager
        .terminate(process_id, "test cleanup", Duration::from_secs(1), 1024)
        .await
        .unwrap();
    assert!(manager.acknowledge_terminal(process_id).await.unwrap());
}

#[tokio::test]
async fn async_manager_keeps_completed_handle_until_owner_acknowledges() {
    let manager = AsyncExecutionManager::default();
    let backend = Arc::new(HostBashLiveBackend::default());
    let first_request = AsyncProcessStartRequest::host_command(
        vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "sleep 0.05; printf first".to_string(),
        ],
        std::env::current_dir().unwrap(),
    )
    .with_deadline(ExecutionDeadline::from_now(Duration::from_secs(1)))
    .with_yield_time(Duration::from_millis(5))
    .with_output_cap_bytes(1024)
    .retain_terminal_until_acknowledged();

    let first_started = manager.start(backend.clone(), first_request).await.unwrap();
    let first_id = first_started
        .snapshot
        .process_id
        .expect("first process should still be running");
    tokio::time::sleep(Duration::from_millis(75)).await;

    let second_request = AsyncProcessStartRequest::host_command(
        vec!["/bin/sh".to_string(), "-c".to_string(), "true".to_string()],
        std::env::current_dir().unwrap(),
    )
    .with_deadline(ExecutionDeadline::from_now(Duration::from_secs(1)))
    .with_yield_time(Duration::from_secs(1))
    .with_output_cap_bytes(1024);
    let second_completed = manager.start(backend, second_request).await.unwrap();
    assert_eq!(
        second_completed.snapshot.status,
        ProcessSnapshotStatus::Completed
    );

    let first_completed = manager
        .poll(first_id, Duration::from_secs(1), 1024)
        .await
        .unwrap();
    assert_eq!(
        first_completed.snapshot.status,
        ProcessSnapshotStatus::Completed
    );
    assert_eq!(
        String::from_utf8_lossy(&first_completed.snapshot.stdout),
        "first"
    );
}

#[tokio::test]
async fn async_manager_writes_to_stdin_capable_host_command() {
    let manager = AsyncExecutionManager::default();
    let backend = Arc::new(HostBashLiveBackend::default());
    let request = AsyncProcessStartRequest::host_command(
        vec!["/bin/sh".to_string(), "-c".to_string(), "cat".to_string()],
        std::env::current_dir().unwrap(),
    )
    .pipe_stdin(true)
    .with_deadline(ExecutionDeadline::from_now(Duration::from_secs(2)))
    .with_yield_time(Duration::from_millis(5))
    .with_output_cap_bytes(1024)
    .retain_terminal_until_acknowledged();

    let started = manager.start(backend, request).await.unwrap();
    let process_id = started.snapshot.process_id.expect("running process id");
    let written = manager
        .write(
            process_id,
            b"ping\n".to_vec(),
            Duration::from_millis(100),
            1024,
        )
        .await
        .unwrap();

    assert_eq!(written.snapshot.status, ProcessSnapshotStatus::Running);
    assert!(String::from_utf8_lossy(&written.snapshot.stdout).contains("ping"));

    let terminated = manager
        .terminate(process_id, "test complete", Duration::from_secs(1), 1024)
        .await
        .unwrap();
    assert_eq!(terminated.snapshot.status, ProcessSnapshotStatus::Cancelled);
    assert!(terminated.snapshot.events.iter().any(|event| {
        matches!(
            &event.kind,
            CooldisProcessEventKind::Cancelled { reason } if reason == "test complete"
        )
    }));
    assert!(manager.acknowledge_terminal(process_id).await.unwrap());
}

#[tokio::test]
async fn async_manager_records_timeout_and_output_truncation() {
    let manager = AsyncExecutionManager::default();
    let backend = Arc::new(HostBashLiveBackend::default());
    let timeout_request = AsyncProcessStartRequest::host_command(
        vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "sleep 5".to_string(),
        ],
        std::env::current_dir().unwrap(),
    )
    .with_deadline(ExecutionDeadline::from_now(Duration::from_millis(25)))
    .with_yield_time(Duration::from_secs(1))
    .with_output_cap_bytes(1024);

    let timed_out = manager
        .start(backend.clone(), timeout_request)
        .await
        .unwrap();
    assert_eq!(timed_out.snapshot.status, ProcessSnapshotStatus::TimedOut);
    assert_eq!(timed_out.snapshot.exit_code, Some(124));
    let timed_out_id = timed_out.snapshot.process_id.unwrap();
    assert!(manager.snapshot(timed_out_id, 1024).await.is_err());

    let truncation_request = AsyncProcessStartRequest::host_command(
        vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "printf abcdef".to_string(),
        ],
        std::env::current_dir().unwrap(),
    )
    .with_deadline(ExecutionDeadline::from_now(Duration::from_secs(1)))
    .with_yield_time(Duration::from_secs(1))
    .with_output_cap_bytes(3);

    let truncated = manager.start(backend, truncation_request).await.unwrap();
    assert_eq!(truncated.snapshot.status, ProcessSnapshotStatus::Completed);
    assert_eq!(String::from_utf8_lossy(&truncated.snapshot.stdout), "abc");
    assert!(truncated.snapshot.stdout_truncated);
}
