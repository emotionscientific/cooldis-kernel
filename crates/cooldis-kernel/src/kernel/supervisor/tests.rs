use super::*;
use crate::{
    AgentRuntime, RuntimeServices, ThreadCommand, ThreadContext, ThreadEvent, ThreadSignal,
    ThreadStatus,
};
use async_trait::async_trait;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;

struct EchoRuntimeFactory;

#[async_trait]
impl AgentRuntimeFactory for EchoRuntimeFactory {
    async fn build(&self, _context: &ThreadContext) -> CooldisResult<Box<dyn AgentRuntime>> {
        Ok(Box::new(EchoRuntime))
    }
}

struct EchoRuntime;

#[async_trait]
impl AgentRuntime for EchoRuntime {
    async fn run(
        self: Box<Self>,
        context: ThreadContext,
        services: RuntimeServices,
        mut commands: mpsc::Receiver<ThreadCommand>,
        events: broadcast::Sender<ThreadEvent>,
        status: watch::Sender<ThreadStatus>,
        cancellation: CancellationToken,
    ) {
        let thread_id = context.coordinates.thread_id;
        let coordinates = context.coordinates.clone();
        let _ = events.send(ThreadEvent::Started { context });
        let _ = status.send(ThreadStatus::Idle);
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => {
                    let _ = status.send(ThreadStatus::Stopped);
                    let _ = events.send(ThreadEvent::Stopped { thread_id });
                    break;
                }
                command = commands.recv() => {
                    match command {
                        Some(ThreadCommand::Submit { turn_id, input, .. }) => {
                            let _ = status.send(ThreadStatus::Running);
                            if let Ok(entry) = services.append_user_turn_input(&coordinates, &input).await {
                                let _ = events.send(ThreadEvent::CanonicalMirror { thread_id, entry });
                            }
                            let _ = events.send(ThreadEvent::Output {
                                thread_id,
                                text: format!("{turn_id}:{}", input.text_projection()),
                            });
                            let _ = status.send(ThreadStatus::Idle);
                        }
                        Some(ThreadCommand::Cancel { reason }) => {
                            let _ = status.send(ThreadStatus::Cancelling);
                            let _ = events.send(ThreadEvent::Signal {
                                thread_id,
                                signal: ThreadSignal::interrupt_cancel(&coordinates, reason.clone()),
                            });
                            let _ = events.send(ThreadEvent::Cancelled { thread_id, reason });
                            let _ = status.send(ThreadStatus::Idle);
                        }
                        Some(ThreadCommand::Compact { .. }) => {
                            let _ = status.send(ThreadStatus::Idle);
                        }
                        Some(ThreadCommand::ResumeToolCall { .. }) => {
                            let _ = status.send(ThreadStatus::Idle);
                        }
                        Some(ThreadCommand::Shutdown) | None => {
                            let _ = events.send(ThreadEvent::Signal {
                                thread_id,
                                signal: ThreadSignal::shutdown(&coordinates),
                            });
                            let _ = status.send(ThreadStatus::Stopped);
                            let _ = events.send(ThreadEvent::Stopped { thread_id });
                            break;
                        }
                    }
                }
            }
        }
    }
}

async fn supervisor() -> CooldisSupervisor {
    supervisor_with_root(&unique_temp_dir("cooldis-supervisor")).await
}

async fn supervisor_with_root(root: &std::path::Path) -> CooldisSupervisor {
    let supervisor = CooldisSupervisor::new();
    supervisor
        .register_tenant(TenantRegistration {
            context: tenant_context(root, "tenant_a"),
            runtime_factory: Arc::new(EchoRuntimeFactory),
        })
        .await
        .unwrap();
    supervisor
        .register_tenant(TenantRegistration {
            context: tenant_context(root, "tenant_b"),
            runtime_factory: Arc::new(EchoRuntimeFactory),
        })
        .await
        .unwrap();
    supervisor
}

fn tenant_context(root: &std::path::Path, tenant_id: &str) -> TenantRuntimeContext {
    TenantRuntimeContext::local(
        tenant_id,
        root.join(tenant_id).join("runtime"),
        root.join(tenant_id).join("state"),
    )
}

fn start_request(tenant_id: &str) -> ThreadStartRequest {
    start_request_with_topology(tenant_id, ThreadTopology::root())
}

fn start_request_with_topology(tenant_id: &str, topology: ThreadTopology) -> ThreadStartRequest {
    ThreadStartRequest {
        tenant_id: tenant_id.to_string(),
        user_id: "user_1".to_string(),
        session_id: "session_1".to_string(),
        topology,
        metadata: Default::default(),
    }
}

#[tokio::test]
async fn supervisor_routes_threads_by_tenant() {
    let root = unique_temp_dir("cooldis-supervisor-routes");
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
async fn supervisor_runtime_contexts_keep_tenant_homes_and_stores_isolated() {
    let root = unique_temp_dir("cooldis-tenant-context");
    let runtime_a = root.join("tenant-a/runtime");
    let state_a = root.join("tenant-a/state");
    let runtime_b = root.join("tenant-b/runtime");
    let state_b = root.join("tenant-b/state");
    let supervisor = CooldisSupervisor::new();
    supervisor
        .register_tenant(TenantRegistration {
            context: TenantRuntimeContext::local("tenant_a", &runtime_a, &state_a),
            runtime_factory: Arc::new(EchoRuntimeFactory),
        })
        .await
        .unwrap();
    supervisor
        .register_tenant(TenantRegistration {
            context: TenantRuntimeContext::local("tenant_b", &runtime_b, &state_b),
            runtime_factory: Arc::new(EchoRuntimeFactory),
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
    assert_eq!(tenant_a.context.sqlite_home, state_a.join("sqlite"));
    assert_eq!(
        tenant_a.context.session_history_path,
        state_a.join("session_history.sqlite3")
    );
    assert_eq!(tenant_b.context.runtime_home, runtime_b);
    assert_eq!(tenant_b.context.state_home, state_b);
    assert_ne!(tenant_a.context.codex_home, tenant_b.context.codex_home);
    assert_ne!(tenant_a.context.sqlite_home, tenant_b.context.sqlite_home);
    assert_ne!(
        tenant_a.context.session_history_path,
        tenant_b.context.session_history_path
    );
    assert!(tenant_a.context.codex_home.is_dir());
    assert!(tenant_a.context.sqlite_home.is_dir());
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

    supervisor
        .cancel_at(&coordinates, "addressed cancel")
        .await
        .unwrap();
    assert_cancelled(&mut events, "addressed cancel").await;
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
        CooldisError::ThreadScopeMismatch {
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
            ThreadTopology::spawned_from(root.context().coordinates.thread_id),
        ))
        .await
        .unwrap();
    supervisor
        .start_thread(ThreadStartRequest {
            tenant_id: "tenant_a".to_string(),
            user_id: "user_1".to_string(),
            session_id: "session_2".to_string(),
            topology: ThreadTopology::root(),
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
            SessionSnapshot {
                user_id: "user_1".to_string(),
                session_id: "session_1".to_string(),
                thread_count: 2,
            },
            SessionSnapshot {
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
            ThreadTopology::spawned_from(root.context().coordinates.thread_id),
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
        CooldisError::ThreadScopeMismatch {
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
            BTreeMap::from([("product_key".to_string(), "opaque".to_string())]),
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
    assert!(matches!(err, CooldisError::TenantNotFound(tenant) if tenant == "missing"));
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
        start_request_with_topology("tenant_b", ThreadTopology::spawned_from(source_thread_id)),
    )
    .await;
    assert!(matches!(err, CooldisError::RelatedThreadNotFound(id) if id == source_thread_id));
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
        ThreadStartRequest {
            tenant_id: "tenant_a".to_string(),
            user_id: "user_1".to_string(),
            session_id: "session_2".to_string(),
            topology: ThreadTopology::spawned_from(source_thread_id),
            metadata: Default::default(),
        },
    )
    .await;
    assert!(matches!(
        err,
        CooldisError::RelatedThreadScopeMismatch {
            thread_id,
            ..
        } if thread_id == source_thread_id
    ));
}

async fn start_thread_err(
    supervisor: &CooldisSupervisor,
    request: ThreadStartRequest,
) -> CooldisError {
    match supervisor.start_thread(request).await {
        Ok(_) => panic!("start_thread unexpectedly succeeded"),
        Err(err) => err,
    }
}

async fn assert_output(events: &mut broadcast::Receiver<ThreadEvent>, expected: &str) {
    loop {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        if let ThreadEvent::Output { text, .. } = event {
            assert_eq!(text, expected);
            return;
        }
    }
}

async fn assert_cancelled(events: &mut broadcast::Receiver<ThreadEvent>, expected: &str) {
    loop {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        if let ThreadEvent::Cancelled { reason, .. } = event {
            assert_eq!(reason, expected);
            return;
        }
    }
}

fn text_messages(context: &crate::SessionContext) -> Vec<String> {
    context
        .messages
        .iter()
        .map(|message| match message {
            crate::CanonicalMessage::User { content, .. }
            | crate::CanonicalMessage::Assistant { content, .. }
            | crate::CanonicalMessage::ToolResult { content, .. } => content
                .iter()
                .find_map(|content| match content {
                    crate::CanonicalContent::Text { text, .. } => Some(text.clone()),
                    _ => None,
                })
                .unwrap_or_default(),
        })
        .collect()
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
}
