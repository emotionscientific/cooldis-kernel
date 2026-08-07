#[test]
fn virtual_process_retains_output_and_exit_status() {
    let process = crate::process::VerletProcessHandle::from_virtual_command(
        "echo hi",
        crate::execution::VirtualCommandOutput {
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
    let request = crate::execution::ExternalCommandRequest {
        invocation: crate::execution::ExternalCommandInvocation::Script("sleep 10".to_string()),
        executor: crate::execution::ExternalExecutorKind::HostBash,
        cwd: std::path::PathBuf::from("/workspace"),
        stdin: None,
        deadline: crate::execution::ExecutionDeadline::from_now(std::time::Duration::from_millis(
            10,
        )),
        max_output_bytes: 1024,
    };
    let process = crate::process::VerletProcessHandle::from_external_command(
        &request,
        crate::execution::ExternalCommandResult::new(crate::execution::VirtualCommandOutput {
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
        Some(crate::process::VerletProcessTerminalState::TimedOut { .. })
    ));
}

#[tokio::test]
async fn bridge_stream_is_live_and_retained() {
    let operation_id = crate::bridge::OperationId::new();
    let process = crate::process::VerletProcessHandle::new(
        crate::process::VerletProcessBackend::Bridge,
        "bridge exec",
    );
    let mut live = process.subscribe();
    let stream = futures_util::stream::iter(vec![
        Ok(crate::bridge::OperationEvent::Started { operation_id }),
        Ok(crate::bridge::OperationEvent::Stdout {
            operation_id,
            bytes: b"hello".to_vec(),
        }),
        Ok(crate::bridge::OperationEvent::Stderr {
            operation_id,
            bytes: b"warn".to_vec(),
        }),
        Ok(crate::bridge::OperationEvent::Artifact {
            operation_id,
            artifact_id: "artifact-1".to_string(),
            path: Some(std::path::PathBuf::from("/tmp/report.json")),
            mime_type: Some("application/json".to_string()),
        }),
        Ok(crate::bridge::OperationEvent::Cancelled {
            operation_id,
            reason: "stop".to_string(),
        }),
    ]);

    let join = process.attach_bridge_event_stream(Box::pin(stream));

    let started = live.recv().await.unwrap();
    assert!(matches!(
        started.kind,
        crate::process::VerletProcessEventKind::Started { .. }
    ));
    let stdout = live.recv().await.unwrap();
    assert!(matches!(
        stdout.kind,
        crate::process::VerletProcessEventKind::Stdout { ref bytes } if bytes == b"hello"
    ));
    join.await.unwrap().unwrap();

    let output = process.output();
    assert_eq!(output.stdout_text_lossy(), "hello");
    assert_eq!(output.stderr_text_lossy(), "warn");
    assert_eq!(output.artifacts.len(), 1);
    assert_eq!(output.artifacts[0].artifact_id, "artifact-1");
    assert!(matches!(
        output.terminal,
        Some(crate::process::VerletProcessTerminalState::Cancelled { reason }) if reason == "stop"
    ));
    assert_eq!(process.events().len(), 5);
}

#[tokio::test]
async fn async_manager_yields_running_handle_then_poll_completes_host_command() {
    let manager = crate::live::AsyncExecutionManager::default();
    let backend = std::sync::Arc::new(crate::live::HostBashLiveBackend::default());
    let request = crate::live::AsyncProcessStartRequest::host_command(
        vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "sleep 0.05; printf done".to_string(),
        ],
        std::env::current_dir().unwrap(),
    )
    .with_deadline(crate::execution::ExecutionDeadline::from_now(
        std::time::Duration::from_secs(1),
    ))
    .with_yield_time(std::time::Duration::from_millis(5))
    .with_output_cap_bytes(1024)
    .retain_terminal_until_acknowledged();

    let started = manager.start(backend, request).await.unwrap();
    assert_eq!(
        started.snapshot.status,
        crate::live::ProcessSnapshotStatus::Running
    );
    let process_id = started.snapshot.process_id.expect("running process id");

    let completed = manager
        .poll(process_id, std::time::Duration::from_secs(1), 1024)
        .await
        .unwrap();
    assert_eq!(
        completed.snapshot.status,
        crate::live::ProcessSnapshotStatus::Completed
    );
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
    let manager = crate::live::AsyncExecutionManager::default();
    let process_id = crate::process::VerletProcessId::new();
    let request = crate::live::AsyncProcessStartRequest::host_command(
        vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "sleep 0.05".to_string(),
        ],
        std::env::current_dir().unwrap(),
    )
    .with_process_id(process_id)
    .with_deadline(crate::execution::ExecutionDeadline::from_now(
        std::time::Duration::from_secs(1),
    ))
    .with_yield_time(std::time::Duration::from_millis(1))
    .retain_terminal_until_acknowledged();

    let started = manager
        .start(
            std::sync::Arc::new(crate::live::HostBashLiveBackend),
            request,
        )
        .await
        .unwrap();

    assert_eq!(started.snapshot.process_id, Some(process_id));
    manager
        .terminate(
            process_id,
            "test cleanup",
            std::time::Duration::from_secs(1),
            1024,
        )
        .await
        .unwrap();
    assert!(manager.acknowledge_terminal(process_id).await.unwrap());
}

#[tokio::test]
async fn async_manager_keeps_completed_handle_until_owner_acknowledges() {
    let manager = crate::live::AsyncExecutionManager::default();
    let backend = std::sync::Arc::new(crate::live::HostBashLiveBackend::default());
    let first_request = crate::live::AsyncProcessStartRequest::host_command(
        vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "sleep 0.05; printf first".to_string(),
        ],
        std::env::current_dir().unwrap(),
    )
    .with_deadline(crate::execution::ExecutionDeadline::from_now(
        std::time::Duration::from_secs(1),
    ))
    .with_yield_time(std::time::Duration::from_millis(5))
    .with_output_cap_bytes(1024)
    .retain_terminal_until_acknowledged();

    let first_started = manager.start(backend.clone(), first_request).await.unwrap();
    let first_id = first_started
        .snapshot
        .process_id
        .expect("first process should still be running");
    let first_completed = manager
        .poll(first_id, std::time::Duration::from_secs(1), 1024)
        .await
        .unwrap();
    assert_eq!(
        first_completed.snapshot.status,
        crate::live::ProcessSnapshotStatus::Completed
    );
    assert_eq!(
        String::from_utf8_lossy(&first_completed.snapshot.stdout),
        "first"
    );

    let second_request = crate::live::AsyncProcessStartRequest::host_command(
        vec!["/bin/sh".to_string(), "-c".to_string(), "true".to_string()],
        std::env::current_dir().unwrap(),
    )
    .with_deadline(crate::execution::ExecutionDeadline::from_now(
        std::time::Duration::from_secs(1),
    ))
    .with_yield_time(std::time::Duration::from_secs(1))
    .with_output_cap_bytes(1024);
    let second_completed = manager.start(backend, second_request).await.unwrap();
    assert_eq!(
        second_completed.snapshot.status,
        crate::live::ProcessSnapshotStatus::Completed
    );

    let retained = manager.snapshot(first_id, 1024).await.unwrap();
    assert_eq!(retained.snapshot, first_completed.snapshot);
}

#[tokio::test]
async fn async_manager_writes_to_stdin_capable_host_command() {
    let manager = crate::live::AsyncExecutionManager::default();
    let backend = std::sync::Arc::new(crate::live::HostBashLiveBackend::default());
    let request = crate::live::AsyncProcessStartRequest::host_command(
        vec!["/bin/sh".to_string(), "-c".to_string(), "cat".to_string()],
        std::env::current_dir().unwrap(),
    )
    .pipe_stdin(true)
    .with_deadline(crate::execution::ExecutionDeadline::from_now(
        std::time::Duration::from_secs(2),
    ))
    .with_yield_time(std::time::Duration::from_millis(5))
    .with_output_cap_bytes(1024)
    .retain_terminal_until_acknowledged();

    let started = manager.start(backend, request).await.unwrap();
    let process_id = started.snapshot.process_id.expect("running process id");
    let written = manager
        .write(
            process_id,
            b"ping\n".to_vec(),
            std::time::Duration::from_millis(100),
            1024,
        )
        .await
        .unwrap();

    assert_eq!(
        written.snapshot.status,
        crate::live::ProcessSnapshotStatus::Running
    );
    assert!(String::from_utf8_lossy(&written.snapshot.stdout).contains("ping"));

    let terminated = manager
        .terminate(
            process_id,
            "test complete",
            std::time::Duration::from_secs(1),
            1024,
        )
        .await
        .unwrap();
    assert_eq!(
        terminated.snapshot.status,
        crate::live::ProcessSnapshotStatus::Cancelled
    );
    assert!(terminated.snapshot.events.iter().any(|event| {
        matches!(
            &event.kind,
            crate::process::VerletProcessEventKind::Cancelled { reason } if reason == "test complete"
        )
    }));
    assert!(manager.acknowledge_terminal(process_id).await.unwrap());
}

#[cfg(unix)]
#[tokio::test]
async fn host_process_termination_kills_term_ignoring_process_group_members() {
    let pid_file =
        std::env::temp_dir().join(format!("verlet-process-tree-{}.pid", uuid::Uuid::now_v7()));
    let script = format!(
        "trap '' TERM; /bin/sh -c 'trap \"\" TERM; echo $$ > {}; while :; do sleep 1; done' & wait",
        pid_file.display()
    );
    let manager = crate::live::AsyncExecutionManager::default();
    let request = crate::live::AsyncProcessStartRequest::host_command(
        vec!["/bin/sh".to_string(), "-c".to_string(), script],
        std::env::current_dir().unwrap(),
    )
    .with_deadline(crate::execution::ExecutionDeadline::from_now(
        std::time::Duration::from_secs(30),
    ))
    .with_yield_time(std::time::Duration::from_millis(10))
    .with_output_cap_bytes(1024)
    .retain_terminal_until_acknowledged();
    let started = manager
        .start(
            std::sync::Arc::new(crate::live::HostBashLiveBackend),
            request,
        )
        .await
        .unwrap();
    let process_id = started.snapshot.process_id.unwrap();

    let child_pid = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            // The shell creates the pid file before the echo's content lands;
            // keep polling until the content actually parses.
            if let Some(pid) = std::fs::read_to_string(&pid_file)
                .ok()
                .and_then(|pid| pid.trim().parse::<libc::pid_t>().ok())
            {
                break pid;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("term-ignoring process-tree child did not start");

    let terminated = manager
        .terminate(
            process_id,
            "interrupt",
            std::time::Duration::from_secs(1),
            1024,
        )
        .await
        .unwrap();
    let child_is_dead = unsafe {
        libc::kill(child_pid, 0) == -1
            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
    };
    if !child_is_dead {
        unsafe {
            libc::kill(child_pid, libc::SIGKILL);
        }
    }
    let _ = std::fs::remove_file(pid_file);

    assert_eq!(
        terminated.snapshot.status,
        crate::live::ProcessSnapshotStatus::Cancelled
    );
    assert!(child_is_dead, "process-group member survived termination");
}

#[cfg(unix)]
#[tokio::test]
async fn host_process_termination_still_works_after_the_group_leader_exits() {
    let root = std::env::temp_dir().join(format!(
        "verlet-live-reaped-leader-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let leader_file = root.join("leader.pid");
    let child_file = root.join("child.pid");
    let script = format!(
        "echo $$ > {}; (trap '' HUP TERM; while :; do sleep 1; done) & echo $! > {}; exit 0",
        leader_file.display(),
        child_file.display(),
    );
    let manager = crate::live::AsyncExecutionManager::default();
    let request = crate::live::AsyncProcessStartRequest::host_command(
        vec!["/bin/sh".to_string(), "-c".to_string(), script],
        std::env::current_dir().unwrap(),
    )
    .with_deadline(crate::execution::ExecutionDeadline::from_now(
        std::time::Duration::from_secs(30),
    ))
    .with_yield_time(std::time::Duration::from_millis(10))
    .with_output_cap_bytes(1024)
    .retain_terminal_until_acknowledged();
    let started = manager
        .start(
            std::sync::Arc::new(crate::live::HostBashLiveBackend),
            request,
        )
        .await
        .unwrap();
    let process_id = started.snapshot.process_id.unwrap();
    let (leader_pid, child_pid) = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let leader = std::fs::read_to_string(&leader_file)
                .ok()
                .and_then(|pid| pid.trim().parse::<libc::pid_t>().ok());
            let child = std::fs::read_to_string(&child_file)
                .ok()
                .and_then(|pid| pid.trim().parse::<libc::pid_t>().ok());
            if let (Some(leader), Some(child)) = (leader, child)
                && unsafe { libc::kill(leader, 0) } == -1
                && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
            {
                break (leader, child);
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("host process leader was not reaped before termination");

    let terminated = manager
        .terminate(
            process_id,
            "interrupt",
            std::time::Duration::from_secs(2),
            1024,
        )
        .await
        .unwrap();
    let child_is_dead = unsafe {
        libc::kill(child_pid, 0) == -1
            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
    };
    if !child_is_dead {
        unsafe {
            libc::kill(child_pid, libc::SIGKILL);
        }
    }
    let _ = std::fs::remove_dir_all(root);

    assert_eq!(
        terminated.snapshot.status,
        crate::live::ProcessSnapshotStatus::Cancelled
    );
    assert!(
        child_is_dead,
        "process-group member survived after leader {leader_pid} exited"
    );
}

#[tokio::test]
async fn async_manager_records_timeout_and_output_truncation() {
    let manager = crate::live::AsyncExecutionManager::default();
    let backend = std::sync::Arc::new(crate::live::HostBashLiveBackend::default());
    let timeout_request = crate::live::AsyncProcessStartRequest::host_command(
        vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "sleep 5".to_string(),
        ],
        std::env::current_dir().unwrap(),
    )
    .with_deadline(crate::execution::ExecutionDeadline::from_now(
        std::time::Duration::from_millis(25),
    ))
    .with_yield_time(std::time::Duration::from_secs(1))
    .with_output_cap_bytes(1024);

    let timed_out = manager
        .start(backend.clone(), timeout_request)
        .await
        .unwrap();
    assert_eq!(
        timed_out.snapshot.status,
        crate::live::ProcessSnapshotStatus::TimedOut
    );
    assert_eq!(timed_out.snapshot.exit_code, Some(124));
    let timed_out_id = timed_out.snapshot.process_id.unwrap();
    assert!(manager.snapshot(timed_out_id, 1024).await.is_err());

    let truncation_request = crate::live::AsyncProcessStartRequest::host_command(
        vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "printf abcdef".to_string(),
        ],
        std::env::current_dir().unwrap(),
    )
    .with_deadline(crate::execution::ExecutionDeadline::from_now(
        std::time::Duration::from_secs(1),
    ))
    .with_yield_time(std::time::Duration::from_secs(1))
    .with_output_cap_bytes(3);

    let truncated = manager.start(backend, truncation_request).await.unwrap();
    assert_eq!(
        truncated.snapshot.status,
        crate::live::ProcessSnapshotStatus::Completed
    );
    assert_eq!(String::from_utf8_lossy(&truncated.snapshot.stdout), "abc");
    assert!(truncated.snapshot.stdout_truncated);
}
