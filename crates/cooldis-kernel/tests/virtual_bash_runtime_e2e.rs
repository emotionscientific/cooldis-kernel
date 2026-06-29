use cooldis::{
    CanonicalContent, CanonicalMessage, CanonicalStopReason, CooldisSupervisor, ProviderApi,
    SessionEntryKind, TenantRegistration, TenantRuntimeContext, ThreadEvent, ThreadStartRequest,
    ThreadTopology, VirtualBashRuntimeConfig, VirtualBashRuntimeFactory, VirtualFile, VirtualMount,
};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::time::{Duration, timeout};

#[tokio::test]
async fn supervisor_runs_virtual_bash_with_configured_mounts_and_canonical_history() {
    let config = VirtualBashRuntimeConfig {
        cwd: PathBuf::from("/work"),
        mounts: vec![
            VirtualMount::writable("/work").with_file("seed.txt", "seed\n"),
            VirtualMount::readonly("/docs", vec![VirtualFile::new("guide.txt", "read me\n")]),
        ],
        ..VirtualBashRuntimeConfig::default()
    };
    let supervisor = CooldisSupervisor::new();
    supervisor
        .register_tenant(TenantRegistration {
            context: TenantRuntimeContext::local(
                "tenant-vbash",
                "/tmp/cooldis-e2e-runtime",
                "/tmp/cooldis-e2e-state",
            ),
            runtime_factory: Arc::new(VirtualBashRuntimeFactory::new(config)),
        })
        .await
        .unwrap();

    let thread = supervisor
        .start_thread(ThreadStartRequest {
            tenant_id: "tenant-vbash".to_string(),
            user_id: "user-1".to_string(),
            session_id: "session-1".to_string(),
            topology: ThreadTopology::root(),
            metadata: Default::default(),
        })
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    let script = "pwd && cat /work/seed.txt && echo changed > /work/new.txt \
                  && cat /docs/guide.txt && test ! -e /workspace && cat /work/new.txt";
    supervisor
        .submit_to(&thread.context().coordinates, "turn-1", script)
        .await
        .unwrap();

    let first_output = next_output(&mut events).await;
    assert!(first_output.contains("/work"));
    assert!(first_output.contains("seed"));
    assert!(first_output.contains("read me"));
    assert!(first_output.contains("changed"));
    assert!(!first_output.contains("[exit_code="));

    supervisor
        .submit_to(
            &thread.context().coordinates,
            "turn-2",
            "echo nope > /docs/guide.txt",
        )
        .await
        .unwrap();
    let second_output = next_output(&mut events).await;
    assert!(second_output.contains("read-only") || second_output.contains("denied"));
    assert!(second_output.contains("[exit_code="));

    let context = thread.session_context().await.unwrap();
    assert_eq!(context.messages.len(), 4);
    assert_eq!(canonical_text(&context.messages[0]), script);
    assert_eq!(canonical_text(&context.messages[1]), first_output);
    assert_eq!(
        canonical_text(&context.messages[2]),
        "echo nope > /docs/guide.txt"
    );
    assert_eq!(canonical_text(&context.messages[3]), second_output);

    let CanonicalMessage::Assistant {
        provider,
        api,
        model,
        stop_reason,
        ..
    } = &context.messages[1]
    else {
        panic!("expected first assistant mirror");
    };
    assert_eq!(provider, "cooldis");
    assert_eq!(api, &ProviderApi::Other("virtual_bash".to_string()));
    assert_eq!(model, "bashkit");
    assert_eq!(stop_reason, &CanonicalStopReason::EndTurn);

    assert!(context.entries.iter().all(|entry| {
        matches!(
            &entry.kind,
            SessionEntryKind::Message {
                message: CanonicalMessage::User { .. } | CanonicalMessage::Assistant { .. }
            }
        )
    }));
}

async fn next_output(events: &mut broadcast::Receiver<ThreadEvent>) -> String {
    timeout(Duration::from_secs(5), async {
        loop {
            match events.recv().await.unwrap() {
                ThreadEvent::Output { text, .. } => break text,
                ThreadEvent::Failed { message, .. } => panic!("thread failed: {message}"),
                _ => {}
            }
        }
    })
    .await
    .unwrap()
}

fn canonical_text(message: &CanonicalMessage) -> String {
    let content = match message {
        CanonicalMessage::User { content, .. }
        | CanonicalMessage::Assistant { content, .. }
        | CanonicalMessage::ToolResult { content, .. } => content,
    };
    content
        .iter()
        .map(|content| match content {
            CanonicalContent::Text { text, .. } => text.as_str(),
            _ => "",
        })
        .collect::<Vec<_>>()
        .join("")
}
