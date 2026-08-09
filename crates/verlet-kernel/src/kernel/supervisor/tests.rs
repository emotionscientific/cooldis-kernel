struct EchoRuntimeFactory;

#[async_trait::async_trait]
impl crate::kernel::runtime_host::runtime_api::AgentRuntimeFactory for EchoRuntimeFactory {
    async fn build(
        &self,
        _context: &verlet_runtime_contracts::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<
        Box<dyn crate::kernel::runtime_host::runtime_api::AgentRuntime>,
    > {
        Ok(Box::new(EchoRuntime))
    }
}

struct EchoRuntime;

#[async_trait::async_trait]
impl crate::kernel::runtime_host::runtime_api::AgentRuntime for EchoRuntime {
    async fn run(
        self: Box<Self>,
        context: verlet_runtime_contracts::ThreadContext,
        services: crate::kernel::runtime_host::runtime_services::RuntimeServices,
        mut commands: tokio::sync::mpsc::Receiver<
            crate::kernel::runtime_host::runtime_api::ThreadCommand,
        >,
        events: tokio::sync::broadcast::Sender<
            crate::kernel::runtime_host::runtime_api::ThreadEvent,
        >,
        status: tokio::sync::watch::Sender<verlet_runtime_contracts::ThreadStatus>,
        cancellation: tokio_util::sync::CancellationToken,
    ) {
        let thread_id = context.coordinates.thread_id;
        let coordinates = context.coordinates.clone();
        let _ =
            events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Started { context });
        let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => {
                    let _ = status.send(verlet_runtime_contracts::ThreadStatus::Stopped);
                    let _ = events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Stopped { thread_id });
                    break;
                }
                command = commands.recv() => {
                    match command {
                        Some(crate::kernel::runtime_host::runtime_api::ThreadCommand::Submit { turn_id, input, .. }) => {
                            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Running);
                            if let Ok(entry) = services.append_user_turn_input(&coordinates, &turn_id, &input).await {
                                let _ = events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::CanonicalMirror { thread_id, entry });
                            }
                            let _ = events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Output {
                                thread_id,
                                text: format!("{turn_id}:{}", input.text_projection()),
                            });
                            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
                        }
                        Some(crate::kernel::runtime_host::runtime_api::ThreadCommand::Cancel { reason }) => {
                            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Cancelling);
                            let _ = events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Signal {
                                thread_id,
                                signal: verlet_runtime_contracts::ThreadSignal::interrupt_cancel(&coordinates, reason.clone()),
                            });
                            let _ = events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Cancelled { thread_id, reason });
                            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
                        }
                        Some(crate::kernel::runtime_host::runtime_api::ThreadCommand::CancelTurn { .. }) => {}
                        Some(crate::kernel::runtime_host::runtime_api::ThreadCommand::Compact { .. }) => {
                            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
                        }
                        Some(crate::kernel::runtime_host::runtime_api::ThreadCommand::ResumeToolCall { .. }) => {
                            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
                        }
                        Some(crate::kernel::runtime_host::runtime_api::ThreadCommand::Shutdown) | None => {
                            let _ = events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Signal {
                                thread_id,
                                signal: verlet_runtime_contracts::ThreadSignal::shutdown(&coordinates),
                            });
                            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Stopped);
                            let _ = events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Stopped { thread_id });
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[derive(Default)]
struct GatedShutdownFactory {
    shutdown_received: std::sync::Arc<tokio::sync::Notify>,
    release_shutdown: std::sync::Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::runtime_api::AgentRuntimeFactory for GatedShutdownFactory {
    async fn build(
        &self,
        _context: &verlet_runtime_contracts::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<
        Box<dyn crate::kernel::runtime_host::runtime_api::AgentRuntime>,
    > {
        Ok(Box::new(GatedShutdownRuntime {
            shutdown_received: std::sync::Arc::clone(&self.shutdown_received),
            release_shutdown: std::sync::Arc::clone(&self.release_shutdown),
        }))
    }
}

struct GatedShutdownRuntime {
    shutdown_received: std::sync::Arc<tokio::sync::Notify>,
    release_shutdown: std::sync::Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::runtime_api::AgentRuntime for GatedShutdownRuntime {
    async fn run(
        self: Box<Self>,
        _context: verlet_runtime_contracts::ThreadContext,
        _services: crate::kernel::runtime_host::runtime_services::RuntimeServices,
        mut commands: tokio::sync::mpsc::Receiver<
            crate::kernel::runtime_host::runtime_api::ThreadCommand,
        >,
        _events: tokio::sync::broadcast::Sender<
            crate::kernel::runtime_host::runtime_api::ThreadEvent,
        >,
        status: tokio::sync::watch::Sender<verlet_runtime_contracts::ThreadStatus>,
        _cancellation: tokio_util::sync::CancellationToken,
    ) {
        let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
        while let Some(command) = commands.recv().await {
            if matches!(
                command,
                crate::kernel::runtime_host::runtime_api::ThreadCommand::Shutdown
            ) {
                self.shutdown_received.notify_one();
                self.release_shutdown.notified().await;
                let _ = status.send(verlet_runtime_contracts::ThreadStatus::Stopped);
                return;
            }
        }
    }
}

async fn supervisor() -> crate::kernel::supervisor::VerletSupervisor {
    supervisor_with_root(&unique_temp_dir("verlet-supervisor")).await
}

async fn supervisor_with_root(
    root: &std::path::Path,
) -> crate::kernel::supervisor::VerletSupervisor {
    let supervisor = crate::kernel::supervisor::VerletSupervisor::new();
    supervisor
        .register_tenant(crate::kernel::supervisor::TenantRegistration {
            context: tenant_context(root, "tenant_a"),
            runtime_factory: std::sync::Arc::new(EchoRuntimeFactory),
        })
        .await
        .unwrap();
    supervisor
        .register_tenant(crate::kernel::supervisor::TenantRegistration {
            context: tenant_context(root, "tenant_b"),
            runtime_factory: std::sync::Arc::new(EchoRuntimeFactory),
        })
        .await
        .unwrap();
    supervisor
}

fn tenant_context(
    root: &std::path::Path,
    tenant_id: &str,
) -> crate::kernel::supervisor::TenantRuntimeContext {
    crate::kernel::supervisor::TenantRuntimeContext::local(
        tenant_id,
        root.join(tenant_id).join("runtime"),
        root.join(tenant_id).join("state"),
    )
}

fn start_request(tenant_id: &str) -> crate::kernel::supervisor::ThreadStartRequest {
    start_request_with_topology(tenant_id, verlet_runtime_contracts::ThreadTopology::root())
}

fn start_request_with_topology(
    tenant_id: &str,
    topology: verlet_runtime_contracts::ThreadTopology,
) -> crate::kernel::supervisor::ThreadStartRequest {
    crate::kernel::supervisor::ThreadStartRequest {
        tenant_id: tenant_id.to_string(),
        user_id: "user_1".to_string(),
        session_id: "session_1".to_string(),
        topology,
        metadata: Default::default(),
    }
}

#[tokio::test]
async fn supervisor_routes_threads_by_tenant() {
    let root = unique_temp_dir("verlet-supervisor-routes");
    let supervisor = supervisor_with_root(&root).await;
    let a = supervisor
        .start_thread(start_request("tenant_a"))
        .await
        .unwrap();
    let b = supervisor
        .start_thread(start_request("tenant_b"))
        .await
        .unwrap();

    let mut a_events = a.subscribe_events();
    let mut b_events = b.subscribe_events();
    supervisor
        .submit("tenant_a", a.context().coordinates.thread_id, "a", "hello")
        .await
        .unwrap();
    supervisor
        .submit("tenant_b", b.context().coordinates.thread_id, "b", "world")
        .await
        .unwrap();

    assert_output(&mut a_events, "a:hello").await;
    assert_output(&mut b_events, "b:world").await;

    let snapshot = supervisor.snapshot().await;
    assert_eq!(snapshot.tenants.len(), 2);
    assert_eq!(snapshot.tenants[0].tenant_id, "tenant_a");
    assert_eq!(
        snapshot.tenants[0].config.runtime_home,
        root.join("tenant_a/runtime")
    );
    assert_eq!(snapshot.tenants[0].runtime.threads.len(), 1);
    assert_eq!(snapshot.tenants[1].tenant_id, "tenant_b");
    assert_eq!(
        snapshot.tenants[1].config.state_home,
        root.join("tenant_b/state")
    );
    assert_eq!(snapshot.tenants[1].runtime.threads.len(), 1);
    let _ = supervisor.shutdown_all().await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn supervisor_turn_submission_is_idempotent_on_turn_id() {
    let supervisor = supervisor().await;
    let thread = supervisor
        .start_thread(start_request("tenant_a"))
        .await
        .unwrap();
    let mut status = thread.subscribe_status();
    while *status.borrow() != verlet_runtime_contracts::ThreadStatus::Idle {
        status.changed().await.unwrap();
    }
    let mut events = thread.subscribe_events();

    supervisor
        .submit_to(&thread.context().coordinates, "turn-same", "hello")
        .await
        .unwrap();
    supervisor
        .submit_to(&thread.context().coordinates, "turn-same", "hello")
        .await
        .unwrap();

    assert_output(&mut events, "turn-same:hello").await;
    assert!(
        // tight-timeout: a duplicate output must remain absent after the idempotent submit
        tokio::time::timeout(tokio::time::Duration::from_millis(50), async {
            loop {
                if matches!(
                    events.recv().await,
                    Ok(crate::kernel::runtime_host::runtime_api::ThreadEvent::Output { .. })
                ) {
                    return;
                }
            }
        })
        .await
        .is_err(),
        "duplicate turn reservation submitted a second control effect"
    );
    assert_eq!(
        text_messages(&thread.session_context().await.unwrap()),
        vec!["hello"]
    );
}

#[tokio::test]
async fn dropped_turn_reservation_releases_turn_id() {
    let supervisor = supervisor().await;
    let thread = supervisor
        .start_thread(start_request("tenant_a"))
        .await
        .unwrap();
    let mut status = thread.subscribe_status();
    while *status.borrow() != verlet_runtime_contracts::ThreadStatus::Idle {
        status.changed().await.unwrap();
    }
    let mut events = thread.subscribe_events();
    let coordinates = &thread.context().coordinates;

    drop(
        supervisor
            .reserve_admitted_turn_to(
                coordinates,
                "turn-retry",
                crate::kernel::runtime_host::turn::TurnInput::text("first"),
                verlet_runtime_contracts::TurnSubmissionMode::Queue,
                None,
            )
            .await
            .unwrap(),
    );
    supervisor
        .submit_to(coordinates, "turn-retry", "second")
        .await
        .unwrap();

    assert_output(&mut events, "turn-retry:second").await;
}

#[tokio::test]
async fn cancelling_idle_thread_is_a_witnessed_no_op() {
    let supervisor = supervisor().await;
    let thread = supervisor
        .start_thread(start_request("tenant_a"))
        .await
        .unwrap();
    let mut status = thread.subscribe_status();
    while *status.borrow() != verlet_runtime_contracts::ThreadStatus::Idle {
        status.changed().await.unwrap();
    }
    let mut events = thread.subscribe_events();
    let prior_signal = thread.lifecycle_record().await.latest_signal_id;

    supervisor
        .cancel_at(&thread.context().coordinates, "already finished")
        .await
        .unwrap();

    assert_ne!(
        thread.lifecycle_record().await.latest_signal_id,
        prior_signal
    );
    assert_eq!(
        thread.status(),
        verlet_runtime_contracts::ThreadStatus::Idle
    );
    assert!(
        // tight-timeout: idle cancellation must not emit a runtime cancellation event
        tokio::time::timeout(tokio::time::Duration::from_millis(50), async {
            loop {
                if matches!(
                    events.recv().await,
                    Ok(crate::kernel::runtime_host::runtime_api::ThreadEvent::Cancelled { .. })
                ) {
                    return;
                }
            }
        })
        .await
        .is_err(),
        "idle cancellation reached the runtime instead of remaining a witnessed no-op"
    );
}

#[tokio::test]
async fn supervisor_runtime_contexts_keep_tenant_homes_and_stores_isolated() {
    let root = unique_temp_dir("verlet-tenant-context");
    let runtime_a = root.join("tenant-a/runtime");
    let state_a = root.join("tenant-a/state");
    let runtime_b = root.join("tenant-b/runtime");
    let state_b = root.join("tenant-b/state");
    let supervisor = crate::kernel::supervisor::VerletSupervisor::new();
    supervisor
        .register_tenant(crate::kernel::supervisor::TenantRegistration {
            context: crate::kernel::supervisor::TenantRuntimeContext::local(
                "tenant_a", &runtime_a, &state_a,
            ),
            runtime_factory: std::sync::Arc::new(EchoRuntimeFactory),
        })
        .await
        .unwrap();
    supervisor
        .register_tenant(crate::kernel::supervisor::TenantRegistration {
            context: crate::kernel::supervisor::TenantRuntimeContext::local(
                "tenant_b", &runtime_b, &state_b,
            ),
            runtime_factory: std::sync::Arc::new(EchoRuntimeFactory),
        })
        .await
        .unwrap();

    let a = supervisor
        .start_thread(start_request("tenant_a"))
        .await
        .unwrap();
    let b = supervisor
        .start_thread(start_request("tenant_b"))
        .await
        .unwrap();
    assert_ne!(
        a.context().coordinates.thread_id,
        b.context().coordinates.thread_id
    );
    let mut a_events = a.subscribe_events();
    let mut b_events = b.subscribe_events();

    supervisor
        .submit_to(&a.context().coordinates, "a", "from tenant a")
        .await
        .unwrap();
    supervisor
        .submit_to(&b.context().coordinates, "b", "from tenant b")
        .await
        .unwrap();
    assert_output(&mut a_events, "a:from tenant a").await;
    assert_output(&mut b_events, "b:from tenant b").await;

    assert_eq!(
        text_messages(&a.session_context().await.unwrap()),
        vec!["from tenant a"]
    );
    assert_eq!(
        text_messages(&b.session_context().await.unwrap()),
        vec!["from tenant b"]
    );

    let snapshot = supervisor.snapshot().await;
    let tenant_a = snapshot
        .tenants
        .iter()
        .find(|tenant| tenant.tenant_id == "tenant_a")
        .unwrap();
    let tenant_b = snapshot
        .tenants
        .iter()
        .find(|tenant| tenant.tenant_id == "tenant_b")
        .unwrap();
    assert_eq!(tenant_a.context.runtime_home, runtime_a);
    assert_eq!(tenant_a.context.state_home, state_a);
    assert_eq!(tenant_a.context.codex_home, runtime_a.join("codex-home"));
    assert_eq!(
        tenant_a.context.session_history_path,
        state_a.join("session_history.sqlite3")
    );
    assert_eq!(tenant_b.context.runtime_home, runtime_b);
    assert_eq!(tenant_b.context.state_home, state_b);
    assert_ne!(tenant_a.context.codex_home, tenant_b.context.codex_home);
    assert_ne!(
        tenant_a.context.session_history_path,
        tenant_b.context.session_history_path
    );
    assert!(tenant_a.context.codex_home.is_dir());
    assert!(tenant_a.context.session_history_path.is_file());
    assert!(tenant_b.context.session_history_path.is_file());

    let _ = supervisor.shutdown_all().await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn supervisor_supports_coordinate_addressed_submit_and_cancel() {
    let supervisor = supervisor().await;
    let thread = supervisor
        .start_thread(start_request("tenant_a"))
        .await
        .unwrap();
    let coordinates = thread.context().coordinates.clone();
    let mut events = thread.subscribe_events();

    supervisor
        .submit_to(&coordinates, "turn", "addressed")
        .await
        .unwrap();
    assert_output(&mut events, "turn:addressed").await;

    let prior_signal = thread.lifecycle_record().await.latest_signal_id;
    supervisor
        .cancel_at(&coordinates, "addressed cancel")
        .await
        .unwrap();
    assert_ne!(
        thread.lifecycle_record().await.latest_signal_id,
        prior_signal
    );
    assert_eq!(
        thread.status(),
        verlet_runtime_contracts::ThreadStatus::Idle
    );
}

#[tokio::test]
async fn supervisor_rejects_coordinate_scope_mismatch() {
    let supervisor = supervisor().await;
    let thread = supervisor
        .start_thread(start_request("tenant_a"))
        .await
        .unwrap();
    let mut wrong_user = thread.context().coordinates.clone();
    wrong_user.user_id = "other_user".to_string();

    let err = match supervisor
        .submit_to(&wrong_user, "turn", "should fail")
        .await
    {
        Ok(_) => panic!("submit_to unexpectedly succeeded"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        crate::kernel::runtime_host::VerletError::ThreadScopeMismatch {
            thread_id,
            ..
        } if thread_id == thread.context().coordinates.thread_id
    ));
}

#[tokio::test]
async fn supervisor_snapshot_groups_sessions() {
    let supervisor = supervisor().await;
    let root = supervisor
        .start_thread(start_request("tenant_a"))
        .await
        .unwrap();
    supervisor
        .start_thread(start_request_with_topology(
            "tenant_a",
            verlet_runtime_contracts::ThreadTopology::spawned_from(
                root.context().coordinates.thread_id,
            ),
        ))
        .await
        .unwrap();
    supervisor
        .start_thread(crate::kernel::supervisor::ThreadStartRequest {
            tenant_id: "tenant_a".to_string(),
            user_id: "user_1".to_string(),
            session_id: "session_2".to_string(),
            topology: verlet_runtime_contracts::ThreadTopology::root(),
            metadata: Default::default(),
        })
        .await
        .unwrap();

    let snapshot = supervisor.snapshot().await;
    let tenant_a = snapshot
        .tenants
        .iter()
        .find(|tenant| tenant.tenant_id == "tenant_a")
        .unwrap();
    assert_eq!(
        tenant_a.sessions,
        vec![
            crate::kernel::supervisor::SessionSnapshot {
                user_id: "user_1".to_string(),
                session_id: "session_1".to_string(),
                thread_count: 2,
            },
            crate::kernel::supervisor::SessionSnapshot {
                user_id: "user_1".to_string(),
                session_id: "session_2".to_string(),
                thread_count: 1,
            },
        ]
    );
}

#[tokio::test]
async fn supervisor_children_of_at_validates_parent_coordinates() {
    let supervisor = supervisor().await;
    let root = supervisor
        .start_thread(start_request("tenant_a"))
        .await
        .unwrap();
    let child = supervisor
        .start_thread(start_request_with_topology(
            "tenant_a",
            verlet_runtime_contracts::ThreadTopology::spawned_from(
                root.context().coordinates.thread_id,
            ),
        ))
        .await
        .unwrap();

    let children = supervisor
        .children_of_at(&root.context().coordinates)
        .await
        .unwrap();
    assert_eq!(children.len(), 1);
    assert_eq!(
        children[0].context().coordinates.thread_id,
        child.context().coordinates.thread_id
    );

    let mut wrong_session = root.context().coordinates.clone();
    wrong_session.session_id = "session_2".to_string();
    let err = match supervisor.children_of_at(&wrong_session).await {
        Ok(_) => panic!("children_of_at unexpectedly succeeded"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        crate::kernel::runtime_host::VerletError::ThreadScopeMismatch {
            thread_id,
            ..
        } if thread_id == root.context().coordinates.thread_id
    ));
}

#[tokio::test]
async fn supervisor_lifecycle_snapshot_and_checkpoint_use_records() {
    let supervisor = supervisor().await;
    let thread = supervisor
        .start_thread(start_request("tenant_a"))
        .await
        .unwrap();
    let checkpoint = supervisor
        .create_checkpoint_at(
            &thread.context().coordinates,
            None,
            Some("supervisor-checkpoint".to_string()),
            std::collections::BTreeMap::from([("product_key".to_string(), "opaque".to_string())]),
        )
        .await
        .unwrap();

    let snapshot = supervisor.lifecycle_snapshot().await;
    let tenant = snapshot
        .tenants
        .iter()
        .find(|tenant| tenant.tenant_id == "tenant_a")
        .unwrap();
    let record = tenant
        .records
        .iter()
        .find(|record| record.coordinates == thread.context().coordinates)
        .unwrap();
    assert_eq!(record.latest_checkpoint_id, Some(checkpoint.id));
    assert!(record.latest_signal_id.is_some());
}

#[tokio::test]
async fn supervisor_can_shutdown_one_tenant_without_stopping_others() {
    let supervisor = supervisor().await;
    let a = supervisor
        .start_thread(start_request("tenant_a"))
        .await
        .unwrap();
    let b = supervisor
        .start_thread(start_request("tenant_b"))
        .await
        .unwrap();

    let stopped = supervisor.shutdown_tenant("tenant_a").await.unwrap();

    assert_eq!(stopped, vec![a.context().coordinates.thread_id]);
    assert!(
        supervisor
            .get_thread("tenant_a", a.context().coordinates.thread_id)
            .await
            .is_err()
    );
    assert!(
        supervisor
            .get_thread("tenant_b", b.context().coordinates.thread_id)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn supervisor_only_releases_tenant_id_when_shutdown_unregisters_it() {
    let root = unique_temp_dir("verlet-supervisor-unregister");
    let supervisor = supervisor_with_root(&root).await;

    supervisor.shutdown_tenant("tenant_a").await.unwrap();
    let error = supervisor
        .register_tenant(crate::kernel::supervisor::TenantRegistration {
            context: tenant_context(&root, "tenant_a"),
            runtime_factory: std::sync::Arc::new(EchoRuntimeFactory),
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        crate::kernel::runtime_host::VerletError::TenantAlreadyExists(tenant_id)
            if tenant_id == "tenant_a"
    ));

    supervisor
        .shutdown_and_unregister_tenant("tenant_a")
        .await
        .unwrap();
    supervisor
        .register_tenant(crate::kernel::supervisor::TenantRegistration {
            context: tenant_context(&root, "tenant_a"),
            runtime_factory: std::sync::Arc::new(EchoRuntimeFactory),
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn unregistering_one_tenant_does_not_block_co_resident_tenant_work() {
    let root = unique_temp_dir("verlet-supervisor-unregister-isolation");
    let supervisor = crate::kernel::supervisor::VerletSupervisor::new();
    let gated = std::sync::Arc::new(GatedShutdownFactory::default());
    supervisor
        .register_tenant(crate::kernel::supervisor::TenantRegistration {
            context: tenant_context(&root, "tenant_a"),
            runtime_factory: gated.clone(),
        })
        .await
        .unwrap();
    supervisor
        .register_tenant(crate::kernel::supervisor::TenantRegistration {
            context: tenant_context(&root, "tenant_b"),
            runtime_factory: std::sync::Arc::new(EchoRuntimeFactory),
        })
        .await
        .unwrap();
    supervisor
        .start_thread(start_request("tenant_a"))
        .await
        .unwrap();

    let shutdown = {
        let supervisor = supervisor.clone();
        tokio::spawn(async move { supervisor.shutdown_and_unregister_tenant("tenant_a").await })
    };
    gated.shutdown_received.notified().await;

    let co_resident_start = {
        let supervisor = supervisor.clone();
        tokio::spawn(async move { supervisor.start_thread(start_request("tenant_b")).await })
    };
    for _ in 0..10_000 {
        if co_resident_start.is_finished() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        co_resident_start.is_finished(),
        "tenant_a shutdown held the supervisor registry lock across runtime teardown"
    );
    let tenant_b = co_resident_start.await.unwrap().unwrap();

    gated.release_shutdown.notify_one();
    shutdown.await.unwrap().unwrap();
    supervisor
        .shutdown_thread("tenant_b", tenant_b.context().coordinates.thread_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn supervisor_can_shutdown_all_tenants() {
    let supervisor = supervisor().await;
    supervisor
        .start_thread(start_request("tenant_a"))
        .await
        .unwrap();
    supervisor
        .start_thread(start_request("tenant_b"))
        .await
        .unwrap();

    let stopped = supervisor.shutdown_all().await.unwrap();

    assert_eq!(stopped.len(), 2);
    assert_eq!(stopped[0].0, "tenant_a");
    assert_eq!(stopped[0].1.len(), 1);
    assert_eq!(stopped[1].0, "tenant_b");
    assert_eq!(stopped[1].1.len(), 1);
    assert!(
        supervisor
            .snapshot()
            .await
            .tenants
            .iter()
            .all(|tenant| tenant.runtime.threads.is_empty())
    );
}

#[tokio::test]
async fn supervisor_rejects_unknown_tenant() {
    let supervisor = supervisor().await;
    let err = start_thread_err(&supervisor, start_request("missing")).await;
    assert!(
        matches!(err, crate::kernel::runtime_host::VerletError::TenantNotFound(tenant) if tenant == "missing")
    );
}

#[tokio::test]
async fn supervisor_rejects_cross_tenant_thread_topology() {
    let supervisor = supervisor().await;
    let source = supervisor
        .start_thread(start_request("tenant_a"))
        .await
        .unwrap();
    let source_thread_id = source.context().coordinates.thread_id;
    let err = start_thread_err(
        &supervisor,
        start_request_with_topology(
            "tenant_b",
            verlet_runtime_contracts::ThreadTopology::spawned_from(source_thread_id),
        ),
    )
    .await;
    assert!(
        matches!(err, crate::kernel::runtime_host::VerletError::RelatedThreadNotFound(id) if id == source_thread_id)
    );
}

#[tokio::test]
async fn supervisor_rejects_cross_session_thread_topology_inside_tenant() {
    let supervisor = supervisor().await;
    let source = supervisor
        .start_thread(start_request("tenant_a"))
        .await
        .unwrap();
    let source_thread_id = source.context().coordinates.thread_id;
    let err = start_thread_err(
        &supervisor,
        crate::kernel::supervisor::ThreadStartRequest {
            tenant_id: "tenant_a".to_string(),
            user_id: "user_1".to_string(),
            session_id: "session_2".to_string(),
            topology: verlet_runtime_contracts::ThreadTopology::spawned_from(source_thread_id),
            metadata: Default::default(),
        },
    )
    .await;
    assert!(matches!(
        err,
        crate::kernel::runtime_host::VerletError::RelatedThreadScopeMismatch {
            thread_id,
            ..
        } if thread_id == source_thread_id
    ));
}

async fn start_thread_err(
    supervisor: &crate::kernel::supervisor::VerletSupervisor,
    request: crate::kernel::supervisor::ThreadStartRequest,
) -> crate::kernel::runtime_host::VerletError {
    match supervisor.start_thread(request).await {
        Ok(_) => panic!("start_thread unexpectedly succeeded"),
        Err(err) => err,
    }
}

async fn assert_output(
    events: &mut tokio::sync::broadcast::Receiver<
        crate::kernel::runtime_host::runtime_api::ThreadEvent,
    >,
    expected: &str,
) {
    loop {
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(30), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        if let crate::kernel::runtime_host::runtime_api::ThreadEvent::Output { text, .. } = event {
            assert_eq!(text, expected);
            return;
        }
    }
}

fn text_messages(context: &verlet_history::SessionContext) -> Vec<String> {
    context
        .messages
        .iter()
        .map(|message| match message {
            verlet_history::CanonicalMessage::User { content, .. }
            | verlet_history::CanonicalMessage::Assistant { content, .. }
            | verlet_history::CanonicalMessage::ToolResult { content, .. } => content
                .iter()
                .find_map(|content| match content {
                    verlet_history::CanonicalContent::Text { text, .. } => Some(text.clone()),
                    _ => None,
                })
                .unwrap_or_default(),
        })
        .collect()
}

fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::now_v7()))
}
