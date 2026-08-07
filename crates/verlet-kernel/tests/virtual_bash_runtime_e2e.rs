#[tokio::test]
async fn supervisor_runs_virtual_bash_with_configured_mounts_and_canonical_history() {
    let config = verlet::capabilities::execution::VirtualBashRuntimeConfig {
        cwd: std::path::PathBuf::from("/work"),
        mounts: vec![
            verlet_vbash::VirtualMount::writable("/work").with_file("seed.txt", "seed\n"),
            verlet_vbash::VirtualMount::readonly(
                "/docs",
                vec![verlet_vbash::VirtualFile::new("guide.txt", "read me\n")],
            ),
        ],
        ..verlet::capabilities::execution::VirtualBashRuntimeConfig::default()
    };
    let supervisor = verlet::kernel::supervisor::VerletSupervisor::new();
    supervisor
        .register_tenant(verlet::kernel::supervisor::TenantRegistration {
            context: verlet::kernel::supervisor::TenantRuntimeContext::local(
                "tenant-vbash",
                "/tmp/verlet-e2e-runtime",
                "/tmp/verlet-e2e-state",
            ),
            runtime_factory: std::sync::Arc::new(
                verlet::capabilities::execution::VirtualBashRuntimeFactory::new(config),
            ),
        })
        .await
        .unwrap();

    let thread = supervisor
        .start_thread(verlet::kernel::supervisor::ThreadStartRequest {
            tenant_id: "tenant-vbash".to_string(),
            user_id: "user-1".to_string(),
            session_id: "session-1".to_string(),
            topology: verlet_runtime_contracts::ThreadTopology::root(),
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

    let verlet_history::CanonicalMessage::Assistant {
        provider,
        api,
        model,
        stop_reason,
        ..
    } = &context.messages[1]
    else {
        panic!("expected first assistant mirror");
    };
    assert_eq!(provider, "verlet");
    assert_eq!(
        api,
        &verlet_history::ProviderApi::Other("virtual_bash".to_string())
    );
    assert_eq!(model, "bashkit");
    assert_eq!(stop_reason, &verlet_history::CanonicalStopReason::EndTurn);

    assert!(context.entries.iter().all(|entry| {
        matches!(
            &entry.kind,
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::User { .. }
                    | verlet_history::CanonicalMessage::Assistant { .. }
            }
        )
    }));
}

async fn next_output(
    events: &mut tokio::sync::broadcast::Receiver<
        verlet::kernel::runtime_host::runtime_api::ThreadEvent,
    >,
) -> String {
    tokio::time::timeout(tokio::time::Duration::from_secs(30), async {
        loop {
            match events.recv().await.unwrap() {
                verlet::kernel::runtime_host::runtime_api::ThreadEvent::Output { text, .. } => {
                    break text;
                }
                verlet::kernel::runtime_host::runtime_api::ThreadEvent::Failed {
                    message, ..
                } => panic!("thread failed: {message}"),
                _ => {}
            }
        }
    })
    .await
    .unwrap()
}

fn canonical_text(message: &verlet_history::CanonicalMessage) -> String {
    let content = match message {
        verlet_history::CanonicalMessage::User { content, .. }
        | verlet_history::CanonicalMessage::Assistant { content, .. }
        | verlet_history::CanonicalMessage::ToolResult { content, .. } => content,
    };
    content
        .iter()
        .map(|content| match content {
            verlet_history::CanonicalContent::Text { text, .. } => text.as_str(),
            _ => "",
        })
        .collect::<Vec<_>>()
        .join("")
}
