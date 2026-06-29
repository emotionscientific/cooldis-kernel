use super::*;
use crate::{InMemorySessionStore, ThreadCoordinates, ThreadEvent};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{Duration, timeout};

#[tokio::test]
async fn codex_cli_runtime_invokes_configured_binary_with_tenant_homes() {
    let temp = test_temp_dir("codex-cli-runtime");
    fs::create_dir_all(&temp).unwrap();
    let fake_codex = temp.join("codex");
    fs::write(
        &fake_codex,
        r#"#!/bin/sh
printf 'args='
printf '%s|' "$@"
printf '\nCODEX_HOME=%s\n' "$CODEX_HOME"
printf 'CODEX_SQLITE_HOME=%s\n' "$CODEX_SQLITE_HOME"
printf 'stdin='
cat
"#,
    )
    .unwrap();
    let mut permissions = fs::metadata(&fake_codex).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&fake_codex, permissions).unwrap();

    let factory = CodexCliRuntimeFactory::new(
        CodexRuntimeConfig::local(
            temp.join("tenant-a/codex-home"),
            temp.join("tenant-a/sqlite"),
            temp.join("workspace"),
        )
        .with_codex_bin(fake_codex)
        .with_model("gpt-test")
        .with_sandbox("read-only"),
    );
    let runtime = factory
        .build(&ThreadContext::root(ThreadCoordinates::new(
            "tenant_a",
            "user_1",
            "session_1",
        )))
        .await
        .unwrap();

    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = broadcast::channel(8);
    let (status_tx, _status_rx) = watch::channel(ThreadStatus::Starting);
    let cancellation = CancellationToken::new();
    let services = RuntimeServices::new(
        Arc::new(InMemorySessionStore::new()),
        crate::RuntimeExecutionPolicy::default(),
    );
    let context = ThreadContext::root(ThreadCoordinates::new("tenant_a", "user_1", "session_1"));
    let handle = tokio::spawn(runtime.run(
        context,
        services,
        command_rx,
        event_tx,
        status_tx,
        cancellation,
    ));

    command_tx
        .send(ThreadCommand::Submit {
            turn_id: "turn_1".to_string(),
            input: crate::TurnInput::text("hello codex"),
            mode: TurnSubmissionMode::Queue,
        })
        .await
        .unwrap();

    let user_mirror = next_mirror(&mut event_rx).await;
    assert_eq!(user_mirror.coordinates.tenant_id, "tenant_a");
    assert!(matches!(
        user_mirror.kind,
        SessionEntryKind::Message {
            message: CanonicalMessage::User { .. }
        }
    ));
    let assistant_mirror = next_mirror(&mut event_rx).await;
    assert_eq!(assistant_mirror.coordinates.tenant_id, "tenant_a");
    assert!(matches!(
        assistant_mirror.kind,
        SessionEntryKind::Message {
            message: CanonicalMessage::Assistant { .. }
        }
    ));
    let output = next_output(&mut event_rx).await;
    assert!(output.contains("args=exec|--json|--skip-git-repo-check|-C|"));
    assert!(output.contains("--model|gpt-test|"));
    assert!(output.contains("--sandbox|read-only|"));
    assert!(output.contains("read-only|-|"));
    assert!(output.contains("CODEX_HOME="));
    assert!(output.contains("tenant-a/codex-home"));
    assert!(output.contains("CODEX_SQLITE_HOME="));
    assert!(output.contains("tenant-a/sqlite"));
    assert!(output.contains("stdin=hello codex"));

    command_tx.send(ThreadCommand::Shutdown).await.unwrap();
    handle.await.unwrap();
    let _ = fs::remove_dir_all(temp);
}

async fn next_output(events: &mut broadcast::Receiver<ThreadEvent>) -> String {
    loop {
        let event = timeout(Duration::from_secs(5), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        if let ThreadEvent::Output { text, .. } = event {
            return text;
        }
    }
}

async fn next_mirror(events: &mut broadcast::Receiver<ThreadEvent>) -> crate::SessionEntry {
    loop {
        let event = timeout(Duration::from_secs(5), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        if let ThreadEvent::CanonicalMirror { entry, .. } = event {
            return entry;
        }
    }
}

fn test_temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}"))
}
