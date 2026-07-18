use super::*;
use crate::daemon::handle_ingress::ThreadHandleIngressAdapter;
use crate::test_support::FaultingIngressQueue;
use crate::{
    APP_SERVER_LOCAL_MODEL,
    APP_SERVER_LOCAL_PROVIDER,
    AgentLoopConfig,
    AgentLoopFactory,
    AgentRuntime,
    AgentRuntimeFactory,
    AppServerListenAddr,
    CanonicalContent,
    CanonicalStopReason,
    CanonicalUsage,
    // lexicon-allow: capsule - existing app-server manifest binding config type
    CapsuleBindingsConfig,
    CooldisAppServerConfig,
    CooldisDaemonClockRoute,
    DaemonClock,
    EventKind,
    EventStore,
    LocalAgentRegistry,
    MandateCatchUpPolicy,
    MandateSchedulePayload,
    MandateStartRequest,
    ProviderApi,
    ProviderClient,
    ProviderRequest,
    ProviderResponse,
    ProviderResult,
    PublishOperationRequest,
    PublishedOperationSource,
    RuntimeExecutionPolicy,
    RuntimeServices,
    SessionStore,
    StreamCursorV1,
    THREAD_AGENT_STATIC_CONTEXT_SEGMENTS_METADATA,
    TenantRegistration,
    TenantRuntimeContext,
    ThreadCommand,
    ThreadContext,
    ThreadEvent,
    ThreadSpawnedPayload,
    ThreadStatus,
    TimerFiredPayload,
    control_stream_id,
    revoke_mandate,
    start_mandate,
};
use chrono::{DateTime, TimeZone, Utc};
use cooldis_io_core::{
    ConversationKind, DeliveryReceipt, IngressContent, IoActor, IoConversation, IoDedupeKey,
    IoProtocolAdapter, IoProtocolCapabilities, IoSource, IoTarget,
};
use cooldis_io_pgqrs::{PgqrsIngressQueue, PgqrsQueueConfig, sqlite_dsn};
use cooldis_runtime_contracts::{
    DispatchId, HANDLE_OUTCOME_CONTENT_KIND, HandleTerminalEnvelope, HandleTerminalOutcome,
    RuntimeUsage,
};
use serde_json::json;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{Barrier, Mutex as TokioMutex, Notify, broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct CaptureSink {
    envelopes: Arc<TokioMutex<Vec<IngressEnvelope>>>,
}

struct BlockingSink {
    entered: Notify,
    release: Notify,
}

struct FailFirstCaptureSink {
    attempts: AtomicUsize,
    envelopes: TokioMutex<Vec<IngressEnvelope>>,
}

#[async_trait]
impl IngressSink for CaptureSink {
    async fn submit(&self, envelope: IngressEnvelope) -> IoResult<IngressAck> {
        let ack = IngressAck::accepted(&envelope);
        self.envelopes.lock().await.push(envelope);
        Ok(ack)
    }
}

#[async_trait]
impl IngressSink for BlockingSink {
    async fn submit(&self, envelope: IngressEnvelope) -> IoResult<IngressAck> {
        self.entered.notify_one();
        self.release.notified().await;
        Ok(IngressAck::accepted(&envelope))
    }
}

#[async_trait]
impl IngressSink for FailFirstCaptureSink {
    async fn submit(&self, envelope: IngressEnvelope) -> IoResult<IngressAck> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(IoError::Queue(
                "forced first settlement failure".to_string(),
            ));
        }
        let ack = IngressAck::accepted(&envelope);
        self.envelopes.lock().await.push(envelope);
        Ok(ack)
    }
}

struct CaptureEgress {
    sender: mpsc::UnboundedSender<EgressEnvelope>,
}

impl IoProtocolAdapter for CaptureEgress {
    fn kind(&self) -> &'static str {
        "telegram.bot"
    }

    fn capabilities(&self) -> IoProtocolCapabilities {
        IoProtocolCapabilities {
            ingress: false,
            egress: true,
            streaming: false,
            durable_offsets: false,
            attachments: false,
        }
    }
}

#[async_trait]
impl EgressAdapter for CaptureEgress {
    async fn deliver(&self, envelope: EgressEnvelope) -> IoResult<DeliveryReceipt> {
        self.sender.send(envelope.clone()).unwrap();
        Ok(DeliveryReceipt::delivered(&envelope, "capture"))
    }
}

#[derive(Clone)]
struct ScriptedEgress {
    calls: Arc<TokioMutex<Vec<EgressEnvelope>>>,
    failures: Arc<TokioMutex<VecDeque<String>>>,
    external_ids: Arc<TokioMutex<VecDeque<String>>>,
}

impl ScriptedEgress {
    fn new(failures: impl IntoIterator<Item = impl Into<String>>, external_ids: &[&str]) -> Self {
        Self {
            calls: Arc::new(TokioMutex::new(Vec::new())),
            failures: Arc::new(TokioMutex::new(
                failures.into_iter().map(Into::into).collect(),
            )),
            external_ids: Arc::new(TokioMutex::new(
                external_ids.iter().map(|id| id.to_string()).collect(),
            )),
        }
    }

    async fn calls(&self) -> Vec<EgressEnvelope> {
        self.calls.lock().await.clone()
    }
}

impl IoProtocolAdapter for ScriptedEgress {
    fn kind(&self) -> &'static str {
        "telegram.bot"
    }

    fn capabilities(&self) -> IoProtocolCapabilities {
        IoProtocolCapabilities {
            ingress: false,
            egress: true,
            streaming: false,
            durable_offsets: false,
            attachments: false,
        }
    }
}

#[async_trait]
impl EgressAdapter for ScriptedEgress {
    async fn deliver(&self, envelope: EgressEnvelope) -> IoResult<DeliveryReceipt> {
        self.calls.lock().await.push(envelope.clone());
        if let Some(error) = self.failures.lock().await.pop_front() {
            return Err(IoError::Delivery(error));
        }
        let fallback_id = {
            let calls = self.calls.lock().await;
            format!("message-{}", calls.len())
        };
        let external_id = self
            .external_ids
            .lock()
            .await
            .pop_front()
            .unwrap_or(fallback_id);
        Ok(DeliveryReceipt::delivered(&envelope, external_id))
    }
}

#[derive(Clone)]
struct ScriptedIngressQueue {
    state: Arc<TokioMutex<ScriptedIngressQueueState>>,
    block_next_complete: Arc<AtomicBool>,
    complete_started: Arc<Notify>,
    release_complete: Arc<Notify>,
}

struct ScriptedIngressQueueState {
    message_id: String,
    envelope: IngressEnvelope,
    attempt: u32,
    visible_at: tokio::time::Instant,
    completed: bool,
    complete_errors: VecDeque<String>,
    complete_calls: usize,
    retry_calls: usize,
}

impl ScriptedIngressQueue {
    fn new(
        message_id: impl Into<String>,
        envelope: IngressEnvelope,
        complete_errors: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            state: Arc::new(TokioMutex::new(ScriptedIngressQueueState {
                message_id: message_id.into(),
                envelope,
                attempt: 0,
                visible_at: tokio::time::Instant::now(),
                completed: false,
                complete_errors: complete_errors.into_iter().map(Into::into).collect(),
                complete_calls: 0,
                retry_calls: 0,
            })),
            block_next_complete: Arc::new(AtomicBool::new(false)),
            complete_started: Arc::new(Notify::new()),
            release_complete: Arc::new(Notify::new()),
        }
    }

    fn block_next_complete(&self) {
        self.block_next_complete.store(true, Ordering::SeqCst);
    }

    async fn wait_for_complete_started(&self) {
        self.complete_started.notified().await;
    }

    async fn completed(&self) -> bool {
        self.state.lock().await.completed
    }

    async fn complete_calls(&self) -> usize {
        self.state.lock().await.complete_calls
    }

    async fn retry_calls(&self) -> usize {
        self.state.lock().await.retry_calls
    }
}

#[async_trait]
impl IngressSink for ScriptedIngressQueue {
    async fn submit(&self, envelope: IngressEnvelope) -> IoResult<IngressAck> {
        let ack = IngressAck::accepted(&envelope);
        let mut state = self.state.lock().await;
        state.envelope = envelope;
        state.attempt = 0;
        state.visible_at = tokio::time::Instant::now();
        state.completed = false;
        Ok(ack)
    }
}

#[async_trait]
impl IngressQueueStore for ScriptedIngressQueue {
    async fn lease_ingress(
        &self,
        worker_id: &str,
        max_messages: usize,
        visibility_timeout_secs: u32,
    ) -> IoResult<Vec<LeasedIngressEnvelope>> {
        let mut state = self.state.lock().await;
        if max_messages == 0 || state.completed || state.visible_at > tokio::time::Instant::now() {
            return Ok(Vec::new());
        }
        state.attempt += 1;
        state.visible_at =
            tokio::time::Instant::now() + Duration::from_secs(visibility_timeout_secs.into());
        let mut leased =
            LeasedIngressEnvelope::new(state.message_id.clone(), state.envelope.clone());
        leased.attempt = state.attempt;
        leased.lease_owner = Some(worker_id.to_string());
        Ok(vec![leased])
    }

    async fn complete_ingress(&self, message_id: &str) -> IoResult<()> {
        {
            let mut state = self.state.lock().await;
            assert_eq!(message_id, state.message_id);
            state.complete_calls += 1;
            if let Some(error) = state.complete_errors.pop_front() {
                return Err(IoError::Queue(error));
            }
        }
        if self.block_next_complete.swap(false, Ordering::SeqCst) {
            self.complete_started.notify_one();
            self.release_complete.notified().await;
        }
        self.state.lock().await.completed = true;
        Ok(())
    }

    async fn hold_ingress_until(&self, message_id: &str, visible_at_ms: u64) -> IoResult<()> {
        let mut state = self.state.lock().await;
        assert_eq!(message_id, state.message_id);
        state.visible_at = tokio::time::Instant::now()
            + Duration::from_millis(visible_at_ms.saturating_sub(now_ms()));
        Ok(())
    }

    async fn retry_ingress(&self, message_id: &str, _reason: &str) -> IoResult<()> {
        let mut state = self.state.lock().await;
        assert_eq!(message_id, state.message_id);
        state.retry_calls += 1;
        state.visible_at = tokio::time::Instant::now();
        Ok(())
    }
}

fn test_envelope(text: &str) -> IngressEnvelope {
    IngressEnvelope::new(
        IoSource::new("telegram.bot", "main"),
        IoConversation::new("telegram:chat:123", ConversationKind::Direct),
        IngressContent::text(text),
        now_ms(),
    )
}

fn telegram_queue_envelope(text: &str) -> IngressEnvelope {
    telegram_queue_envelope_with_update(text, "999")
}

fn telegram_queue_envelope_with_update(text: &str, update_id: &str) -> IngressEnvelope {
    let source = IoSource::new("telegram.bot", "main");
    IngressEnvelope::new(
        source.clone(),
        IoConversation::new("telegram:chat:123", ConversationKind::Direct),
        IngressContent::text(text),
        now_ms(),
    )
    .with_actor(IoActor::new("telegram:user:42"))
    .with_dedupe_key(IoDedupeKey::for_source(
        &source,
        format!("update:{update_id}"),
    ))
    .with_metadata("cooldis_route_id", "main")
    .with_metadata("cooldis_route_policy", "queue_per_conversation")
    .with_metadata("telegram_message_id", "555")
}

fn event_envelope(kind: &str) -> IngressEnvelope {
    let source = IoSource::new("external.test", "main");
    IngressEnvelope::new(
        source.clone(),
        IoConversation::new("external:conversation:123", ConversationKind::Direct),
        IngressContent::Event {
            kind: kind.to_string(),
            payload: json!({
                "message_id": 556,
                "value": "event payload",
            }),
        },
        now_ms(),
    )
    .with_actor(IoActor::new("external:user:42"))
    .with_dedupe_key(IoDedupeKey::for_source(&source, format!("event:{kind}")))
    .with_metadata("external_message_id", "556")
}

fn coalesce_envelope(
    text: &str,
    update_id: &str,
    window_ms: u64,
    max_batch: usize,
) -> IngressEnvelope {
    telegram_queue_envelope_with_update(text, update_id)
        .with_metadata("cooldis_route_policy", "coalesce_bursts")
        .with_metadata("cooldis_coalesce_window_ms", window_ms.to_string())
        .with_metadata("cooldis_coalesce_max_batch", max_batch.to_string())
}

fn expired_coalesce_envelope(
    text: &str,
    update_id: &str,
    window_ms: u64,
    max_batch: usize,
) -> IngressEnvelope {
    let mut envelope = coalesce_envelope(text, update_id, window_ms, max_batch);
    envelope.received_at_ms = now_ms().saturating_sub(window_ms + 10);
    envelope
}

fn steer_coalesce_envelope(
    text: &str,
    update_id: &str,
    window_ms: u64,
    max_batch: usize,
) -> IngressEnvelope {
    let mut envelope = expired_coalesce_envelope(text, update_id, window_ms, max_batch)
        .with_metadata("cooldis_route_policy", "steer_when_active");
    envelope
        .metadata
        .insert("cooldis_coalesce_bursts".to_string(), "true".to_string());
    envelope
}

#[test]
fn coalesce_group_key_does_not_collapse_colon_bearing_components() {
    let mut left = coalesce_envelope("left", "9101", 20, 10)
        .with_metadata("cooldis_route_id", "a:b")
        .with_metadata("cooldis_route_threading", "f");
    left.source = IoSource::new("c", "d");
    left.conversation = IoConversation::new("e", ConversationKind::Direct);

    let mut right = coalesce_envelope("right", "9102", 20, 10)
        .with_metadata("cooldis_route_id", "a")
        .with_metadata("cooldis_route_threading", "f");
    right.source = IoSource::new("b", "c");
    right.conversation = IoConversation::new("d:e", ConversationKind::Direct);

    assert_ne!(coalesce_group_key(&left), coalesce_group_key(&right));
}

#[test]
fn coalesce_group_key_separates_per_actor_targets() {
    let first = coalesce_envelope("first", "9201", 20, 10)
        .with_metadata("cooldis_route_threading", "per_actor")
        .with_actor(IoActor::new("telegram:user:1"));
    let second = coalesce_envelope("second", "9202", 20, 10)
        .with_metadata("cooldis_route_threading", "per_actor")
        .with_actor(IoActor::new("telegram:user:2"));

    assert_ne!(coalesce_group_key(&first), coalesce_group_key(&second));
}

#[test]
fn coalesce_messages_sort_by_received_at_before_merging() {
    let mut early = coalesce_envelope("early", "9301", 20, 10);
    early.received_at_ms = 100;
    let mut late = coalesce_envelope("late", "9302", 20, 10);
    late.received_at_ms = 200;
    let mut messages = vec![
        LeasedIngressEnvelope::new("2", late),
        LeasedIngressEnvelope::new("1", early),
    ];

    sort_coalesce_messages(&mut messages);
    let merged = merged_coalesce_envelope(&messages).unwrap();

    assert_eq!(merged.content.text_projection(), "early\nlate");
}

fn observe_only_envelope(text: &str) -> IngressEnvelope {
    telegram_queue_envelope(text).with_metadata("cooldis_route_policy", "observe_only")
}

fn test_egress(text: &str) -> EgressEnvelope {
    let mut egress = EgressEnvelope::new(
        IoTarget {
            source: IoSource::new("telegram.bot", "main"),
            conversation: IoConversation::new("telegram:chat:123", ConversationKind::Direct),
            actor: None,
            metadata: BTreeMap::new(),
        },
        EgressKind::AssistantMessage {
            text: text.to_string(),
        },
        now_ms(),
    );
    egress
        .metadata
        .insert("telegram_message_id".to_string(), "555".to_string());
    egress
}

fn route_with_egress(
    egress_projection: Vec<crate::CooldisEgressProjectionRuleConfig>,
    typing_simulation: Option<crate::CooldisTypingSimulationConfig>,
) -> CooldisIoRouteConfig {
    route_with_egress_and_retry(
        egress_projection,
        typing_simulation,
        crate::CooldisEgressRetryConfig::default(),
    )
}

fn route_with_egress_and_retry(
    egress_projection: Vec<crate::CooldisEgressProjectionRuleConfig>,
    typing_simulation: Option<crate::CooldisTypingSimulationConfig>,
    egress_retry: crate::CooldisEgressRetryConfig,
) -> CooldisIoRouteConfig {
    CooldisIoRouteConfig {
        id: "main".to_string(),
        kind: "telegram.bot".to_string(),
        enabled: true,
        policy: None,
        content_policies: None,
        threading: None,
        agent_ref: None,
        coalesce_bursts: None,
        ingress: None,
        egress_projection,
        typing_simulation,
        egress_retry,
        telegram: None,
        metadata: BTreeMap::new(),
    }
}

fn test_root(name: &str) -> PathBuf {
    std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(format!("{name}-{}", uuid::Uuid::now_v7()))
}

async fn test_bridge() -> (
    CooldisDaemonIoBridge,
    mpsc::UnboundedReceiver<EgressEnvelope>,
    PathBuf,
) {
    let fixture_root = test_root("bridge");
    let (server, bridge, rx) = test_bridge_at_root(&fixture_root).await;
    let session_store_path = server.session_store_path().to_path_buf();
    (bridge, rx, session_store_path)
}

async fn test_server() -> CooldisAppServer {
    test_server_at_root(&test_root("server")).await
}

#[tokio::test]
async fn remote_queue_redelivery_enters_child_ingress_once() {
    const QUEUE_DELIVERY_CRASH_DST_SEED: u64 = 0x4300_0000_0000_0002;

    let root = test_root(&format!(
        "remote-queue-redelivery-{QUEUE_DELIVERY_CRASH_DST_SEED:016x}"
    ));
    let server = test_server_at_root(&root).await;
    let supervisor = server.supervisor();
    let child = supervisor
        .start_thread(ThreadStartRequest {
            tenant_id: server.tenant_id().to_string(),
            user_id: server.user_id().to_string(),
            session_id: "remote-child-session".to_string(),
            topology: ThreadTopology::root(),
            metadata: BTreeMap::new(),
        })
        .await
        .unwrap();
    let child_coordinates = child.context().coordinates.clone();
    let session_store_path = server.session_store_path().to_path_buf();
    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let source = IoSource::new("cooldis.remote", "ingress");
    let mut envelope = IngressEnvelope::new(
        source.clone(),
        IoConversation::new("remote-child", ConversationKind::System),
        IngressContent::text("deliver once"),
        now_ms(),
    )
    .with_dedupe_key(IoDedupeKey::for_source(&source, "dispatch-redelivery"));
    envelope.id = "remote-ingress-redelivery".to_string();
    let mut target = ResolvedIoTarget::new(
        ThreadAddress::new(
            server.tenant_id(),
            server.user_id(),
            child.context().coordinates.session_id.clone(),
        )
        .with_thread_id(child.context().coordinates.thread_id.to_string()),
    );
    target.create_thread_if_missing = false;
    let decision =
        AdmissionDecision::queue("remote-redelivery-turn", IoTurnInput::text("deliver once"));

    bridge
        .submit_durable_remote_envelope(envelope.clone(), target.clone(), decision.clone(), 1)
        .await
        .unwrap();
    // Seeded crash cut: admission committed, parent-side queue ack did not.
    // Drop the child generation and make its cold replacement re-present the
    // identical envelope against the same durable state.
    drop(bridge);
    drop(child);
    drop(supervisor);
    drop(server);
    let (restarted_server, restarted_bridge, _rx) = restarted_bridge_at_root(&root).await;
    restarted_bridge
        .submit_durable_remote_envelope(envelope, target, decision, 2)
        .await
        .unwrap();

    let store = SqliteSessionStore::open(&session_store_path).await.unwrap();
    let events = store
        .read_events(&EventStreamId::for_thread(&child_coordinates), None)
        .await
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::TurnSubmitted)
            .count(),
        1,
        "lost queue acknowledgement must not inject a second child turn"
    );
    let control = store
        .read_events(&control_stream_id(&child_coordinates), None)
        .await
        .unwrap();
    let admission =
        crate::kernel::admission::assert_admission_precedes_turn_records(&control, &events);
    assert_eq!(admission.payload["route_id"], "cooldis.remote:ingress");
    assert_eq!(
        control
            .iter()
            .filter(|event| event.kind == EventKind::IoIngressClaimed)
            .count(),
        1,
        "the durable ingress claim is the child-side dedupe authority"
    );
    drop(store);
    drop(restarted_bridge);
    drop(restarted_server);
    let _ = std::fs::remove_dir_all(root);
}

async fn test_server_at_root(fixture_root: &Path) -> CooldisAppServer {
    let socket_path = fixture_root.join("app-server.sock");
    let listen = AppServerListenAddr::parse(&format!("unix://{}", socket_path.display())).unwrap();
    let mut config = CooldisAppServerConfig::local(listen, std::env::current_dir().unwrap());
    config.runtime_home = fixture_root.join("runtime");
    config.state_home = fixture_root.join("state");
    config.user_state_home = fixture_root.join("user-state");
    apply_test_identity(&mut config, fixture_root);
    CooldisAppServer::new_local(config).await.unwrap()
}

async fn test_server_with_provider_at_root(
    fixture_root: &Path,
    provider_client: Arc<dyn ProviderClient>,
) -> CooldisAppServer {
    let socket_path = fixture_root.join("app-server-provider.sock");
    let listen = AppServerListenAddr::parse(&format!("unix://{}", socket_path.display())).unwrap();
    let mut config = CooldisAppServerConfig::local(listen, std::env::current_dir().unwrap());
    config.runtime_home = fixture_root.join("runtime");
    config.state_home = fixture_root.join("state");
    config.user_state_home = fixture_root.join("user-state");
    apply_test_identity(&mut config, fixture_root);
    let runtime_config = AgentLoopConfig::new(
        ProviderApi::Other(APP_SERVER_LOCAL_PROVIDER.to_string()),
        APP_SERVER_LOCAL_PROVIDER,
        APP_SERVER_LOCAL_MODEL,
    );
    let runtime_factory =
        crate::adapters::app_server::runtime_factory_from_provider_parts_with_app_paths(
            runtime_config,
            provider_client,
            // lexicon-allow: capsule - existing app-server config field
            config.capsule_bindings.clone(),
            None,
            &config,
        );
    CooldisAppServer::with_runtime_factory(config, runtime_factory)
        .await
        .unwrap()
}

async fn test_server_with_route_provider_at_root(
    fixture_root: &Path,
    workspace: &Path,
    agent_registry_root: &Path,
    operation_registry_root: &Path,
    client: Arc<RecordingRouteProviderClient>,
) -> CooldisAppServer {
    let socket_path = fixture_root.join("app-server-recording.sock");
    let listen = AppServerListenAddr::parse(&format!("unix://{}", socket_path.display())).unwrap();
    // lexicon-allow: capsule - existing app-server manifest binding config type
    let bindings = CapsuleBindingsConfig::default().with_registry_root(operation_registry_root);
    let mut config = CooldisAppServerConfig::local(listen, workspace)
        // lexicon-allow: capsule - existing app-server config method
        .with_capsule_bindings(bindings);
    config.runtime_home = fixture_root.join("runtime");
    config.state_home = fixture_root.join("state");
    config.user_state_home = fixture_root.join("user-state");
    config.agent_registry_root = agent_registry_root.to_path_buf();
    config.blob_registry_root =
        crate::default_blob_registry_root_for_agent_registry_root(agent_registry_root);
    apply_test_identity(&mut config, fixture_root);
    let runtime_config = AgentLoopConfig::new(
        ProviderApi::Other(APP_SERVER_LOCAL_PROVIDER.to_string()),
        APP_SERVER_LOCAL_PROVIDER,
        APP_SERVER_LOCAL_MODEL,
    );
    let provider_client: Arc<dyn ProviderClient> = client;
    let runtime_factory =
        crate::adapters::app_server::runtime_factory_from_provider_parts_with_app_paths(
            runtime_config,
            provider_client,
            // lexicon-allow: capsule - existing app-server config field
            config.capsule_bindings.clone(),
            None,
            &config,
        );
    CooldisAppServer::with_runtime_factory(config, runtime_factory)
        .await
        .unwrap()
}

async fn test_bridge_at_root(
    fixture_root: &Path,
) -> (
    CooldisAppServer,
    CooldisDaemonIoBridge,
    mpsc::UnboundedReceiver<EgressEnvelope>,
) {
    let server = test_server_at_root(fixture_root).await;
    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let (tx, rx) = mpsc::unbounded_channel();
    bridge
        .register_egress_adapter(
            "telegram.bot",
            "main",
            Arc::new(CaptureEgress { sender: tx }),
        )
        .await;
    (server, bridge, rx)
}

async fn restarted_bridge_at_root(
    fixture_root: &Path,
) -> (
    CooldisAppServer,
    CooldisDaemonIoBridge,
    mpsc::UnboundedReceiver<EgressEnvelope>,
) {
    let socket_path = fixture_root.join("app-server-restarted.sock");
    let listen = AppServerListenAddr::parse(&format!("unix://{}", socket_path.display())).unwrap();
    let mut config = CooldisAppServerConfig::local(listen, std::env::current_dir().unwrap());
    config.runtime_home = fixture_root.join("runtime");
    config.state_home = fixture_root.join("state");
    config.user_state_home = fixture_root.join("user-state");
    apply_test_identity(&mut config, fixture_root);
    let server = CooldisAppServer::new_local(config).await.unwrap();
    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let (tx, rx) = mpsc::unbounded_channel();
    bridge
        .register_egress_adapter(
            "telegram.bot",
            "main",
            Arc::new(CaptureEgress { sender: tx }),
        )
        .await;
    (server, bridge, rx)
}

fn apply_test_identity(config: &mut CooldisAppServerConfig, fixture_root: &Path) {
    let suffix = fixture_root
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("daemon-io");
    config.tenant_id = format!("app-server-{suffix}");
    config.user_id = format!("local-user-{suffix}");
}

#[derive(Default)]
struct RecordingRouteProviderClient {
    requests: StdMutex<Vec<ProviderRequest>>,
}

struct FailingRouteProviderClient;

impl RecordingRouteProviderClient {
    fn requests(&self) -> Vec<ProviderRequest> {
        self.requests.lock().unwrap().clone()
    }
}

#[derive(Default)]
struct BlockingRouteProviderClient {
    request_count: AtomicUsize,
    request_started: Notify,
    released: AtomicBool,
    release: Notify,
}

impl BlockingRouteProviderClient {
    async fn wait_for_requests(&self, count: usize) {
        loop {
            let started = self.request_started.notified();
            if self.request_count.load(Ordering::SeqCst) >= count {
                return;
            }
            started.await;
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release.notify_waiters();
    }
}

#[async_trait]
impl ProviderClient for BlockingRouteProviderClient {
    async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        self.request_count.fetch_add(1, Ordering::SeqCst);
        self.request_started.notify_waiters();
        while !self.released.load(Ordering::SeqCst) {
            let released = self.release.notified();
            if self.released.load(Ordering::SeqCst) {
                break;
            }
            released.await;
        }
        Ok(ProviderResponse {
            content: vec![CanonicalContent::text("blocking daemon route ok")],
            usage: CanonicalUsage {
                input_tokens: request.messages.len() as u64,
                output_tokens: 4,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
            },
            stop_reason: CanonicalStopReason::EndTurn,
        })
    }
}

#[async_trait]
impl ProviderClient for FailingRouteProviderClient {
    async fn complete(&self, _request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        Err(crate::ProviderError::Decode(
            "forced child provider failure".to_string(),
        ))
    }
}

#[async_trait]
impl ProviderClient for RecordingRouteProviderClient {
    async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        self.requests.lock().unwrap().push(request.clone());
        Ok(ProviderResponse {
            content: vec![CanonicalContent::text("daemon route ok")],
            usage: CanonicalUsage {
                input_tokens: request.messages.len() as u64,
                output_tokens: 3,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
            },
            stop_reason: CanonicalStopReason::EndTurn,
        })
    }
}

#[derive(Clone, Default)]
struct RuntimeBuildFailureProbe {
    reject_once: Arc<StdMutex<Option<ThreadId>>>,
    failures: Arc<StdMutex<Vec<ThreadId>>>,
}

impl RuntimeBuildFailureProbe {
    fn reject_once(&self, thread_id: ThreadId) {
        *self.reject_once.lock().unwrap() = Some(thread_id);
    }

    fn failures(&self) -> Vec<ThreadId> {
        self.failures.lock().unwrap().clone()
    }
}

struct SelectiveRuntimeFactory {
    inner: AgentLoopFactory,
    probe: RuntimeBuildFailureProbe,
}

#[async_trait]
impl AgentRuntimeFactory for SelectiveRuntimeFactory {
    async fn build(&self, context: &ThreadContext) -> CooldisResult<Box<dyn crate::AgentRuntime>> {
        let thread_id = context.coordinates.thread_id;
        let rejected = {
            let mut reject_once = self.probe.reject_once.lock().unwrap();
            if *reject_once == Some(thread_id) {
                reject_once.take();
                true
            } else {
                false
            }
        };
        if rejected {
            self.probe.failures.lock().unwrap().push(thread_id);
            return Err(CooldisError::RuntimeFactory(format!(
                "test rejected lifecycle load for {thread_id}"
            )));
        }
        self.inner.build(context).await
    }
}

async fn bridge_with_runtime_build_failure(
    fixture_root: &Path,
) -> (CooldisDaemonIoBridge, RuntimeBuildFailureProbe) {
    let tenant_id = format!("tenant-{}", uuid::Uuid::now_v7());
    let user_id = format!("user-{}", uuid::Uuid::now_v7());
    let runtime_config = AgentLoopConfig::new(
        ProviderApi::Other(APP_SERVER_LOCAL_PROVIDER.to_string()),
        APP_SERVER_LOCAL_PROVIDER,
        APP_SERVER_LOCAL_MODEL,
    );
    let client: Arc<dyn ProviderClient> = Arc::new(RecordingRouteProviderClient::default());
    let probe = RuntimeBuildFailureProbe::default();
    let runtime_factory = Arc::new(SelectiveRuntimeFactory {
        inner: AgentLoopFactory::new(runtime_config, client),
        probe: probe.clone(),
    });
    let supervisor = CooldisSupervisor::new();
    supervisor
        .register_tenant(TenantRegistration {
            context: TenantRuntimeContext::local(
                tenant_id.clone(),
                fixture_root.join("runtime"),
                fixture_root.join("state"),
            ),
            runtime_factory,
        })
        .await
        .unwrap();
    (
        CooldisDaemonIoBridge::new(
            supervisor,
            tenant_id,
            user_id,
            APP_SERVER_LOCAL_PROVIDER,
            APP_SERVER_LOCAL_MODEL,
            std::env::current_dir().unwrap(),
        ),
        probe,
    )
}

async fn bridge_with_execution_policy(
    fixture_root: &Path,
    execution_policy: RuntimeExecutionPolicy,
) -> CooldisDaemonIoBridge {
    let tenant_id = format!("tenant-{}", uuid::Uuid::now_v7());
    let user_id = format!("user-{}", uuid::Uuid::now_v7());
    let runtime_config = AgentLoopConfig::new(
        ProviderApi::Other(APP_SERVER_LOCAL_PROVIDER.to_string()),
        APP_SERVER_LOCAL_PROVIDER,
        APP_SERVER_LOCAL_MODEL,
    );
    let client: Arc<dyn ProviderClient> = Arc::new(RecordingRouteProviderClient::default());
    let runtime_factory = Arc::new(AgentLoopFactory::new(runtime_config, client));
    let context = TenantRuntimeContext::local(
        tenant_id.clone(),
        fixture_root.join("runtime"),
        fixture_root.join("state"),
    )
    .with_execution_policy(execution_policy);
    let session_store_path = context.session_history_path();
    let supervisor = CooldisSupervisor::new();
    supervisor
        .register_tenant(TenantRegistration {
            context,
            runtime_factory,
        })
        .await
        .unwrap();
    let mut bridge = CooldisDaemonIoBridge::new(
        supervisor,
        tenant_id,
        user_id,
        APP_SERVER_LOCAL_PROVIDER,
        APP_SERVER_LOCAL_MODEL,
        std::env::current_dir().unwrap(),
    );
    bridge.session_store_path = Some(session_store_path);
    bridge
}

#[derive(Default)]
struct UnresponsiveRuntimeState {
    running: Notify,
}

struct UnresponsiveRuntimeFactory {
    state: Arc<UnresponsiveRuntimeState>,
}

#[async_trait]
impl AgentRuntimeFactory for UnresponsiveRuntimeFactory {
    async fn build(&self, _context: &ThreadContext) -> CooldisResult<Box<dyn AgentRuntime>> {
        Ok(Box::new(UnresponsiveRuntime {
            state: Arc::clone(&self.state),
        }))
    }
}

struct UnresponsiveRuntime {
    state: Arc<UnresponsiveRuntimeState>,
}

#[async_trait]
impl AgentRuntime for UnresponsiveRuntime {
    async fn run(
        self: Box<Self>,
        context: ThreadContext,
        _services: RuntimeServices,
        mut commands: mpsc::Receiver<ThreadCommand>,
        events: broadcast::Sender<ThreadEvent>,
        status: watch::Sender<ThreadStatus>,
        _cancellation: CancellationToken,
    ) {
        let thread_id = context.coordinates.thread_id;
        let _ = events.send(ThreadEvent::Started { context });
        let _ = status.send(ThreadStatus::Idle);
        if matches!(commands.recv().await, Some(ThreadCommand::Submit { .. })) {
            let _ = status.send(ThreadStatus::Running);
            self.state.running.notify_one();
            std::future::pending::<()>().await;
        }
        let _ = events.send(ThreadEvent::Stopped { thread_id });
    }
}

async fn bridge_with_unresponsive_runtime(
    fixture_root: &Path,
) -> (CooldisDaemonIoBridge, Arc<UnresponsiveRuntimeState>) {
    let tenant_id = format!("tenant-{}", uuid::Uuid::now_v7());
    let user_id = format!("user-{}", uuid::Uuid::now_v7());
    let state = Arc::new(UnresponsiveRuntimeState::default());
    let runtime_factory = Arc::new(UnresponsiveRuntimeFactory {
        state: Arc::clone(&state),
    });
    let context = TenantRuntimeContext::local(
        tenant_id.clone(),
        fixture_root.join("runtime"),
        fixture_root.join("state"),
    )
    .with_execution_policy(RuntimeExecutionPolicy::default().with_cancel_grace_timeout_ms(10_000));
    let session_store_path = context.session_history_path();
    let supervisor = CooldisSupervisor::new();
    supervisor
        .register_tenant(TenantRegistration {
            context,
            runtime_factory,
        })
        .await
        .unwrap();
    let mut bridge = CooldisDaemonIoBridge::new(
        supervisor,
        tenant_id,
        user_id,
        APP_SERVER_LOCAL_PROVIDER,
        APP_SERVER_LOCAL_MODEL,
        std::env::current_dir().unwrap(),
    );
    bridge.session_store_path = Some(session_store_path);
    (bridge, state)
}

#[derive(Default)]
struct PersistedInputCutState {
    input_persisted: Notify,
}

struct PersistedInputCutRuntimeFactory {
    state: Arc<PersistedInputCutState>,
}

#[async_trait]
impl AgentRuntimeFactory for PersistedInputCutRuntimeFactory {
    async fn build(&self, _context: &ThreadContext) -> CooldisResult<Box<dyn AgentRuntime>> {
        Ok(Box::new(PersistedInputCutRuntime {
            state: Arc::clone(&self.state),
        }))
    }
}

struct PersistedInputCutRuntime {
    state: Arc<PersistedInputCutState>,
}

#[async_trait]
impl AgentRuntime for PersistedInputCutRuntime {
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
        if let Some(ThreadCommand::Submit { turn_id, input, .. }) = commands.recv().await {
            let _ = status.send(ThreadStatus::Running);
            let entry = services
                .append_user_turn_input(&coordinates, &turn_id, &input)
                .await
                .unwrap();
            let _ = events.send(ThreadEvent::CanonicalMirror { thread_id, entry });
            self.state.input_persisted.notify_one();
            cancellation.cancelled().await;
        }
        let _ = status.send(ThreadStatus::Stopped);
        let _ = events.send(ThreadEvent::Stopped { thread_id });
    }
}

#[derive(Default)]
struct FailOnceRuntimeState {
    failed: Notify,
}

struct FailOnceThenAgentLoopFactory {
    builds: AtomicUsize,
    state: Arc<FailOnceRuntimeState>,
    provider: AgentLoopFactory,
}

#[async_trait]
impl AgentRuntimeFactory for FailOnceThenAgentLoopFactory {
    async fn build(&self, context: &ThreadContext) -> CooldisResult<Box<dyn AgentRuntime>> {
        if self.builds.fetch_add(1, Ordering::SeqCst) == 0 {
            return Ok(Box::new(FailBeforeEvidenceRuntime {
                state: Arc::clone(&self.state),
            }));
        }
        self.provider.build(context).await
    }
}

struct FailBeforeEvidenceRuntime {
    state: Arc<FailOnceRuntimeState>,
}

#[async_trait]
impl AgentRuntime for FailBeforeEvidenceRuntime {
    async fn run(
        self: Box<Self>,
        context: ThreadContext,
        _services: RuntimeServices,
        mut commands: mpsc::Receiver<ThreadCommand>,
        events: broadcast::Sender<ThreadEvent>,
        status: watch::Sender<ThreadStatus>,
        _cancellation: CancellationToken,
    ) {
        let thread_id = context.coordinates.thread_id;
        let _ = events.send(ThreadEvent::Started { context });
        let _ = status.send(ThreadStatus::Idle);
        if commands.recv().await.is_some() {
            let _ = status.send(ThreadStatus::Failed);
            let _ = events.send(ThreadEvent::Failed {
                thread_id,
                message: "injected failure before execution evidence".to_string(),
            });
            self.state.failed.notify_one();
        }
    }
}

async fn bridge_with_runtime_factory_at_root(
    fixture_root: &Path,
    runtime_factory: Arc<dyn AgentRuntimeFactory>,
) -> CooldisDaemonIoBridge {
    let suffix = fixture_root
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("daemon-io");
    let tenant_id = format!("app-server-{suffix}");
    let user_id = format!("local-user-{suffix}");
    let context = TenantRuntimeContext::local(
        tenant_id.clone(),
        fixture_root.join("runtime"),
        fixture_root.join("state"),
    );
    let session_store_path = context.session_history_path();
    let supervisor = CooldisSupervisor::new();
    supervisor
        .register_tenant(TenantRegistration {
            context,
            runtime_factory,
        })
        .await
        .unwrap();
    let mut bridge = CooldisDaemonIoBridge::new(
        supervisor,
        tenant_id,
        user_id,
        APP_SERVER_LOCAL_PROVIDER,
        APP_SERVER_LOCAL_MODEL,
        std::env::current_dir().unwrap(),
    );
    bridge.session_store_path = Some(session_store_path);
    bridge
}

#[derive(Clone, Default)]
struct RuntimeBuildGateProbe {
    blocked_coordinates: Arc<StdMutex<Option<ThreadCoordinates>>>,
    matching_builds: Arc<AtomicUsize>,
    build_started: Arc<Notify>,
    release_first_build: Arc<Notify>,
}

impl RuntimeBuildGateProbe {
    fn block_first_build(&self, coordinates: ThreadCoordinates) {
        *self.blocked_coordinates.lock().unwrap() = Some(coordinates);
    }

    async fn wait_for_builds(&self, count: usize) {
        loop {
            let changed = self.build_started.notified();
            if self.matching_builds.load(Ordering::SeqCst) >= count {
                return;
            }
            changed.await;
        }
    }

    fn release(&self) {
        self.release_first_build.notify_one();
    }

    fn matching_builds(&self) -> usize {
        self.matching_builds.load(Ordering::SeqCst)
    }
}

struct GatedRuntimeFactory {
    inner: AgentLoopFactory,
    probe: RuntimeBuildGateProbe,
}

#[async_trait]
impl AgentRuntimeFactory for GatedRuntimeFactory {
    async fn build(&self, context: &ThreadContext) -> CooldisResult<Box<dyn crate::AgentRuntime>> {
        let matches = self
            .probe
            .blocked_coordinates
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|coordinates| coordinates == &context.coordinates);
        if matches {
            let build_index = self.probe.matching_builds.fetch_add(1, Ordering::SeqCst);
            self.probe.build_started.notify_one();
            if build_index == 0 {
                self.probe.release_first_build.notified().await;
            }
        }
        self.inner.build(context).await
    }
}

async fn bridge_with_runtime_build_gate(
    fixture_root: &Path,
) -> (CooldisDaemonIoBridge, RuntimeBuildGateProbe) {
    let tenant_id = format!("tenant-{}", uuid::Uuid::now_v7());
    let user_id = format!("user-{}", uuid::Uuid::now_v7());
    let runtime_config = AgentLoopConfig::new(
        ProviderApi::Other(APP_SERVER_LOCAL_PROVIDER.to_string()),
        APP_SERVER_LOCAL_PROVIDER,
        APP_SERVER_LOCAL_MODEL,
    );
    let client: Arc<dyn ProviderClient> = Arc::new(RecordingRouteProviderClient::default());
    let probe = RuntimeBuildGateProbe::default();
    let runtime_factory = Arc::new(GatedRuntimeFactory {
        inner: AgentLoopFactory::new(runtime_config, client),
        probe: probe.clone(),
    });
    let supervisor = CooldisSupervisor::new();
    supervisor
        .register_tenant(TenantRegistration {
            context: TenantRuntimeContext::local(
                tenant_id.clone(),
                fixture_root.join("runtime"),
                fixture_root.join("state"),
            ),
            runtime_factory,
        })
        .await
        .unwrap();
    (
        CooldisDaemonIoBridge::new(
            supervisor,
            tenant_id,
            user_id,
            APP_SERVER_LOCAL_PROVIDER,
            APP_SERVER_LOCAL_MODEL,
            std::env::current_dir().unwrap(),
        ),
        probe,
    )
}

async fn wait_for_provider_requests(client: &RecordingRouteProviderClient, count: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if client.requests().len() >= count {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {count} provider request(s)"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn publish_route_test_operation(registry_root: &Path) -> crate::PublishedOperationRecord {
    std::fs::create_dir_all(registry_root).unwrap();
    let wasm = wat::parse_str(route_test_operation_guest()).unwrap();
    let artifact_path = registry_root.join("lookup.wasm");
    std::fs::write(&artifact_path, wasm).unwrap();
    crate::LocalOperationRegistry::new(registry_root)
        .publish_artifact(PublishOperationRequest {
            name: "lookup".to_string(),
            artifact_path: artifact_path.clone(),
            source: PublishedOperationSource::Wasm {
                bin_path: artifact_path,
            },
            interface: None,
            capability_grants: Default::default(),
            metadata: Default::default(),
        })
        .await
        .unwrap()
}

fn publish_route_agent_manifest(
    root: &Path,
    agent_registry_root: &Path,
    operation_registry_root: &Path,
    operation_hash: &str,
) -> crate::PublishedAgentRecord {
    let project = root.join("daemon-route-runner");
    std::fs::create_dir_all(project.join("prompts")).unwrap();
    std::fs::write(
        project.join("prompts/system.md"),
        "You are the daemon route prompt runner.\n",
    )
    .unwrap();
    let manifest_path = project.join("cooldis.agent.toml");
    std::fs::write(
        &manifest_path,
        format!(
            r#"
[agent]
name = "daemon-route-runner"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[runtime]
default_cwd = "."
streaming = false

[[tools]]
type = "direct_tool"
id = "lookup"
tool_name = "lookup"
operation_ref = "op://lookup/lookup@sha256:{operation_hash}"
"#
        ),
    )
    .unwrap();
    LocalAgentRegistry::new(agent_registry_root)
        .publish_manifest_path_with_operation_registry(&manifest_path, operation_registry_root)
        .unwrap()
}

fn publish_route_agent_manifest_with_missing_blob(
    root: &Path,
    agent_registry_root: &Path,
) -> crate::PublishedAgentRecord {
    let project = root.join("daemon-missing-blob");
    std::fs::create_dir_all(&project).unwrap();
    let manifest_path = project.join("cooldis.agent.toml");
    std::fs::write(
        &manifest_path,
        format!(
            r#"
[agent]
name = "daemon-missing-blob"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[runtime]
default_cwd = "."
streaming = false

[[resources]]
name = "system_prompt"
kind = "blob"
ref = "resource://artifact/sha256:{}"

[context]
[[context.pipelines]]
id = "default"

[[context.pipelines.sources]]
id = "identity"
assembler = "kernel://assembler/static"
input = "system_prompt"
pinned = true
"#,
            "f".repeat(64)
        ),
    )
    .unwrap();
    LocalAgentRegistry::new(agent_registry_root)
        .publish_manifest_path(&manifest_path)
        .unwrap()
}

fn route_test_operation_guest() -> String {
    let manifest = serde_json::json!({
        "abi": "cooldis.operation/0.1",
        "operations": [{
            "id": 1,
            "name": "lookup",
            "input": "bytes",
            "output": "bytes",
            "events": "none",
            "mode": "sync",
            "required_capabilities": []
        }]
    })
    .to_string();
    format!(
        r#"
            (module
              (import "cooldis_0.1" "source_read" (func $source_read (param i32 i32 i32) (result i32)))
              (import "cooldis_0.1" "sink_write" (func $sink_write (param i32 i32 i32) (result i32)))
              (memory (export "memory") 1)
              (data (i32.const 4096) "{manifest}")
              (data (i32.const 8192) "lookup:")
              (func (export "__cooldis_describe_module__") (param $sink i32) (result i32)
                i32.const 0
                i32.const {manifest_len}
                i32.store
                local.get $sink
                i32.const 4096
                i32.const 0
                call $sink_write)
              (func (export "__cooldis_call_operation__")
                (param $op i32)
                (param $invocation i32)
                (param $source i32)
                (param $output i32)
                (param $events i32)
                (result i32)
                (local $n i32)
                local.get $op
                i32.const 1
                i32.ne
                if
                  i32.const 2
                  return
                end
                i32.const 0
                i32.const 1024
                i32.store
                local.get $source
                i32.const 1024
                i32.const 0
                call $source_read
                drop
                i32.const 0
                i32.load
                local.set $n
                i32.const 0
                i32.const 7
                i32.store
                local.get $output
                i32.const 8192
                i32.const 0
                call $sink_write
                drop
                i32.const 0
                local.get $n
                i32.store
                local.get $output
                i32.const 1024
                i32.const 0
                call $sink_write
                drop
                i32.const 0))
            "#,
        manifest = wat_bytes(manifest.as_bytes()),
        manifest_len = manifest.len(),
    )
}

fn wat_bytes(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| match byte {
            b'\n' => "\\0a".to_string(),
            b'\r' => "\\0d".to_string(),
            b'\t' => "\\09".to_string(),
            b'"' => "\\22".to_string(),
            b'\\' => "\\5c".to_string(),
            0x20..=0x7e => (*byte as char).to_string(),
            _ => format!("\\{byte:02x}"),
        })
        .collect()
}

async fn register_route_state(
    bridge: &CooldisDaemonIoBridge,
    route: &CooldisIoRouteConfig,
    db: &Path,
) {
    let source = test_envelope("").source;
    bridge
        .register_egress_route_config(&source.protocol, &source.instance_id, route)
        .await
        .unwrap();
    bridge
        .register_egress_state_sqlite_dsn(&source.protocol, &source.instance_id, sqlite_dsn(db))
        .await
        .unwrap();
}

async fn route_bindings(bridge: &CooldisDaemonIoBridge) -> Vec<BoundEgressThread> {
    let route_scope = test_envelope("").source.stable_scope();
    let state = bridge
        .egress_states
        .read()
        .await
        .get(&route_scope)
        .cloned()
        .expect("route state should be registered");
    state.bound_threads("main").unwrap()
}

fn insert_route_binding(
    state: &DaemonEgressState,
    scope_key: &str,
    coordinates: &ThreadCoordinates,
    updated_at_ms: i64,
) {
    let connection = state.lock_connection().unwrap();
    connection
        .execute(
            "INSERT INTO cooldis_daemon_egress_threads (
                route_id, source_scope, scope_key, tenant_id, user_id, session_id, thread_id, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                "main",
                test_envelope("").source.stable_scope(),
                scope_key,
                coordinates.tenant_id,
                coordinates.user_id,
                coordinates.session_id,
                coordinates.thread_id.to_string(),
                updated_at_ms,
            ],
        )
        .unwrap();
}

async fn start_thread_for_target(
    bridge: &CooldisDaemonIoBridge,
    target: &ResolvedIoTarget,
) -> ThreadCoordinates {
    bridge
        .supervisor
        .start_thread(ThreadStartRequest {
            tenant_id: target.address.tenant_id.clone(),
            user_id: target.address.user_id.clone(),
            session_id: target.address.session_id.clone(),
            topology: ThreadTopology::root(),
            metadata: BTreeMap::new(),
        })
        .await
        .unwrap()
        .context()
        .coordinates
        .clone()
}

async fn submit_and_wait_for_assistant_event(
    bridge: &CooldisDaemonIoBridge,
    text: &str,
) -> (String, String) {
    let receipt = bridge.submit_envelope(test_envelope(text)).await.unwrap();
    let thread_id = receipt.thread_id.expect("receipt should include thread id");
    let expected = format!("local:{text}");
    wait_for_assistant_text(bridge, &thread_id, &expected).await;
    (thread_id, expected)
}

async fn wait_for_assistant_text(bridge: &CooldisDaemonIoBridge, thread_id: &str, expected: &str) {
    let parsed = ThreadId::parse_str(thread_id).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let handle = bridge
            .supervisor
            .get_thread(&bridge.tenant_id, parsed)
            .await
            .unwrap();
        let context = handle.session_context().await.unwrap();
        if context.entries.iter().any(|entry| {
            matches!(
                assistant_text_from_entry(entry).as_deref(),
                Some(text) if text == expected
            )
        }) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for assistant text {expected:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn completed_thread_handle_fixture(
    name: &str,
) -> (
    PathBuf,
    CooldisAppServer,
    CooldisDaemonIoBridge,
    ThreadCoordinates,
    crate::AgentProcessSpawnReceipt,
) {
    thread_handle_fixture(
        name,
        Arc::new(RecordingRouteProviderClient::default()),
        true,
    )
    .await
}

async fn thread_handle_fixture(
    name: &str,
    client: Arc<dyn ProviderClient>,
    await_joined: bool,
) -> (
    PathBuf,
    CooldisAppServer,
    CooldisDaemonIoBridge,
    ThreadCoordinates,
    crate::AgentProcessSpawnReceipt,
) {
    let fixture_root = test_root(name);
    let server = test_server_with_provider_at_root(&fixture_root, client).await;
    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let parent = server
        .supervisor()
        .start_thread(ThreadStartRequest {
            tenant_id: server.tenant_id().to_string(),
            user_id: server.user_id().to_string(),
            session_id: format!("handle-settlement-{}", uuid::Uuid::now_v7()),
            topology: ThreadTopology::root(),
            metadata: BTreeMap::new(),
        })
        .await
        .unwrap();
    let parent_coordinates = parent.context().coordinates.clone();
    let control = server
        .supervisor()
        .kernel_control(server.tenant_id())
        .await
        .unwrap();
    let dispatch = control
        .dispatch_thread_spawn(
            parent.context(),
            DispatchId::new(format!("{name}-dispatch")),
            "worker".to_string(),
            "finish the child task".to_string(),
            None,
            None,
        )
        .await
        .unwrap();
    if await_joined {
        let store = SqliteSessionStore::open(server.session_store_path())
            .await
            .unwrap();
        wait_for_thread_joined(&store, &parent_coordinates).await;
    }
    (fixture_root, server, bridge, parent_coordinates, dispatch)
}

async fn wait_for_thread_joined(store: &SqliteSessionStore, parent: &ThreadCoordinates) {
    wait_for_thread_joined_count(store, parent, 1).await;
}

async fn wait_for_thread_joined_count(
    store: &SqliteSessionStore,
    parent: &ThreadCoordinates,
    expected: usize,
) {
    let control_stream = control_stream_id(parent);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let events = store.read_events(&control_stream, None).await.unwrap();
            if events
                .iter()
                .filter(|event| event.kind == EventKind::ThreadJoined)
                .count()
                >= expected
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("children did not durably reach thread.joined");
}

async fn handle_outcome_parent_inputs(
    store: &SqliteSessionStore,
    coordinates: &ThreadCoordinates,
) -> Vec<String> {
    store
        .build_context(coordinates)
        .await
        .unwrap()
        .messages
        .into_iter()
        .filter_map(|message| match message {
            crate::CanonicalMessage::User { content, .. } => Some(
                content
                    .into_iter()
                    .filter_map(|content| match content {
                        CanonicalContent::Text { text, .. } => Some(text),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join(""),
            ),
            _ => None,
        })
        .filter(|text| text.contains(HANDLE_OUTCOME_CONTENT_KIND))
        .collect()
}

#[tokio::test]
async fn completed_child_is_pushed_once_to_parent_with_dispatch_result_and_usage() {
    let (fixture_root, server, bridge, parent_coordinates, dispatch) =
        completed_thread_handle_fixture("handle-outcome-complete").await;
    let store = SqliteSessionStore::open(server.session_store_path())
        .await
        .unwrap();
    let capture = Arc::new(CaptureSink {
        envelopes: Arc::new(TokioMutex::new(Vec::new())),
    });
    let capture_adapter = ThreadHandleIngressAdapter::new(
        store.clone(),
        capture.clone() as Arc<dyn IngressSink>,
        &parent_coordinates.tenant_id,
        &parent_coordinates.user_id,
    );

    assert_eq!(capture_adapter.enqueue_ready_once().await.unwrap(), 1);
    let captured = capture.envelopes.lock().await.clone();
    assert_eq!(captured.len(), 1);
    let envelope = &captured[0];
    assert_eq!(
        envelope.dedupe_key,
        Some(IoDedupeKey::new(
            HANDLE_OUTCOME_CONTENT_KIND,
            dispatch.dispatch_id.to_string(),
        ))
    );
    let IngressContent::Event { kind, payload } = &envelope.content else {
        panic!("handle outcome must use event ingress content");
    };
    assert_eq!(kind, HANDLE_OUTCOME_CONTENT_KIND);
    let terminal: HandleTerminalEnvelope = serde_json::from_value(payload.clone()).unwrap();
    assert_eq!(terminal.dispatch_id, dispatch.dispatch_id);
    assert_eq!(terminal.handle, dispatch.handle);
    assert_eq!(terminal.outcome, HandleTerminalOutcome::Completed);
    assert_eq!(terminal.result, Some(json!("daemon route ok")));
    assert_eq!(
        terminal.usage,
        Some(RuntimeUsage {
            input_tokens: 1,
            output_tokens: 3,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
        })
    );
    assert!(terminal.outcome_reason.is_none());
    assert!(terminal.artifact_refs.is_empty());
    assert!(!terminal.retryable);
    assert_eq!(capture_adapter.enqueue_ready_once().await.unwrap(), 0);
    assert_eq!(capture.envelopes.lock().await.len(), 1);

    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(
            fixture_root.join("handle-ingress.sqlite"),
            "handle-outcome-complete",
        ))
        .await
        .unwrap(),
    );
    let adapter = ThreadHandleIngressAdapter::new(
        store.clone(),
        queue.clone() as Arc<dyn IngressSink>,
        &parent_coordinates.tenant_id,
        &parent_coordinates.user_id,
    );
    let control = server
        .supervisor()
        .kernel_control(server.tenant_id())
        .await
        .unwrap();
    let caller = server
        .supervisor()
        .get_thread_at(&parent_coordinates)
        .await
        .unwrap();
    let wait = control.wait_thread(caller.context(), dispatch.thread_id, Some(5_000));
    let delivery = async {
        let (first, duplicate) =
            tokio::join!(adapter.enqueue_ready_once(), adapter.enqueue_ready_once(),);
        assert_eq!(first.unwrap() + duplicate.unwrap(), 1);
        let worker = CooldisDaemonQueueWorker::new(
            queue.clone(),
            bridge,
            "handle-outcome-complete-worker",
            30,
        );
        assert_eq!(worker.drain_once().await.unwrap(), 1);
        assert_eq!(adapter.enqueue_ready_once().await.unwrap(), 0);
    };
    let (wait, ()) = tokio::join!(wait, delivery);
    let wait = wait.unwrap();
    assert!(!wait.timed_out);
    assert_eq!(wait.latest_output.as_deref(), Some("daemon route ok"));
    assert_eq!(
        handle_outcome_parent_inputs(&store, &parent_coordinates)
            .await
            .len(),
        1
    );
    let control_events = store
        .read_events(&control_stream_id(&parent_coordinates), None)
        .await
        .unwrap();
    assert_eq!(
        control_events
            .iter()
            .filter(|event| event.kind == EventKind::IoIngressClaimed)
            .count(),
        1
    );
    assert_eq!(
        control_events
            .iter()
            .filter(|event| event.kind == EventKind::IoIngressSettled)
            .count(),
        1
    );

    server.supervisor().shutdown_all().await.unwrap();
}

#[tokio::test]
async fn poisoned_control_stream_does_not_block_healthy_handle_settlement() {
    let (fixture_root, server, bridge, healthy_parent, healthy_dispatch) =
        completed_thread_handle_fixture("handle-outcome-poison-isolation").await;
    let store = SqliteSessionStore::open(server.session_store_path())
        .await
        .unwrap();
    let poisoned = server
        .supervisor()
        .start_thread(ThreadStartRequest {
            tenant_id: server.tenant_id().to_string(),
            user_id: server.user_id().to_string(),
            session_id: format!("poisoned-handle-stream-{}", uuid::Uuid::now_v7()),
            topology: ThreadTopology::root(),
            metadata: BTreeMap::new(),
        })
        .await
        .unwrap();
    let poisoned_coordinates = poisoned.context().coordinates.clone();
    store
        .append_events(
            &control_stream_id(&poisoned_coordinates),
            vec![NewEventRecord::discharged(
                poisoned_coordinates.clone(),
                EventKind::ThreadSpawned,
                json!({
                    "schema": EventKind::ThreadSpawned.payload_schema_id(),
                    "correlation_id": "poisoned-spawn-payload",
                    "parent_thread_id": poisoned_coordinates.thread_id.to_string()
                }),
                EventProvenance {
                    source_event_ids: vec![crate::EventRecordId::new()],
                    discharged_by: Some("test:poisoned-handle-stream".to_string()),
                    function: Some("poisoned_handle_stream/v1".to_string()),
                    ..EventProvenance::default()
                },
            )],
        )
        .await
        .unwrap();

    let capture = Arc::new(CaptureSink {
        envelopes: Arc::new(TokioMutex::new(Vec::new())),
    });
    let adapter = ThreadHandleIngressAdapter::new(
        store,
        capture.clone() as Arc<dyn IngressSink>,
        &healthy_parent.tenant_id,
        &healthy_parent.user_id,
    );

    assert_eq!(adapter.enqueue_ready_once().await.unwrap(), 1);
    let captured = capture.envelopes.lock().await;
    assert_eq!(captured.len(), 1);
    let IngressContent::Event { payload, .. } = &captured[0].content else {
        panic!("healthy settlement must use event ingress content");
    };
    let terminal: HandleTerminalEnvelope = serde_json::from_value(payload.clone()).unwrap();
    assert_eq!(terminal.dispatch_id, healthy_dispatch.dispatch_id);

    drop(captured);
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(
            fixture_root.join("poison-isolation-ingress.sqlite"),
            "handle-outcome-poison-isolation",
        ))
        .await
        .unwrap(),
    );
    let queue_adapter = ThreadHandleIngressAdapter::new(
        SqliteSessionStore::open(server.session_store_path())
            .await
            .unwrap(),
        queue.clone() as Arc<dyn IngressSink>,
        &healthy_parent.tenant_id,
        &healthy_parent.user_id,
    );
    assert_eq!(queue_adapter.enqueue_ready_once().await.unwrap(), 1);
    let worker =
        CooldisDaemonQueueWorker::new(queue, bridge, "handle-outcome-poison-isolation-worker", 30);
    assert_eq!(worker.drain_once().await.unwrap(), 1);
    let delivery_store = SqliteSessionStore::open(server.session_store_path())
        .await
        .unwrap();
    assert_eq!(
        handle_outcome_parent_inputs(&delivery_store, &healthy_parent)
            .await
            .len(),
        1
    );
    server.supervisor().shutdown_all().await.unwrap();
}

#[tokio::test]
async fn failed_settlement_submit_does_not_block_or_repeat_healthy_peer() {
    let (_fixture_root, server, _bridge, parent_coordinates, first_dispatch) =
        completed_thread_handle_fixture("handle-outcome-submit-isolation").await;
    let parent = server
        .supervisor()
        .get_thread_at(&parent_coordinates)
        .await
        .unwrap();
    let control = server
        .supervisor()
        .kernel_control(server.tenant_id())
        .await
        .unwrap();
    let second_dispatch = control
        .dispatch_thread_spawn(
            parent.context(),
            DispatchId::new("handle-outcome-submit-isolation-second"),
            "worker-two".to_string(),
            "finish the second child task".to_string(),
            None,
            None,
        )
        .await
        .unwrap();
    let store = SqliteSessionStore::open(server.session_store_path())
        .await
        .unwrap();
    wait_for_thread_joined_count(&store, &parent_coordinates, 2).await;
    let sink = Arc::new(FailFirstCaptureSink {
        attempts: AtomicUsize::new(0),
        envelopes: TokioMutex::new(Vec::new()),
    });
    let adapter = ThreadHandleIngressAdapter::new(
        store,
        sink.clone() as Arc<dyn IngressSink>,
        &parent_coordinates.tenant_id,
        &parent_coordinates.user_id,
    );

    assert_eq!(adapter.enqueue_ready_once().await.unwrap(), 1);
    assert_eq!(sink.envelopes.lock().await.len(), 1);
    assert_eq!(adapter.enqueue_ready_once().await.unwrap(), 1);
    let envelopes = sink.envelopes.lock().await;
    assert_eq!(envelopes.len(), 2);
    let dispatches = envelopes
        .iter()
        .map(|envelope| {
            let IngressContent::Event { payload, .. } = &envelope.content else {
                panic!("handle outcome must use event ingress content");
            };
            serde_json::from_value::<HandleTerminalEnvelope>(payload.clone())
                .unwrap()
                .dispatch_id
                .to_string()
        })
        .collect::<HashSet<_>>();
    assert_eq!(
        dispatches,
        HashSet::from([
            first_dispatch.dispatch_id.to_string(),
            second_dispatch.dispatch_id.to_string(),
        ])
    );

    drop(envelopes);
    server.supervisor().shutdown_all().await.unwrap();
}

#[tokio::test]
async fn failed_and_cancelled_children_project_detailed_handle_outcomes() {
    let (_fixture_root, failed_server, _bridge, failed_parent, failed_dispatch) =
        thread_handle_fixture(
            "handle-outcome-failed",
            Arc::new(FailingRouteProviderClient),
            true,
        )
        .await;
    let failed_store = SqliteSessionStore::open(failed_server.session_store_path())
        .await
        .unwrap();
    let failed_capture = Arc::new(CaptureSink {
        envelopes: Arc::new(TokioMutex::new(Vec::new())),
    });
    let failed_adapter = ThreadHandleIngressAdapter::new(
        failed_store,
        failed_capture.clone() as Arc<dyn IngressSink>,
        &failed_parent.tenant_id,
        &failed_parent.user_id,
    );
    assert_eq!(failed_adapter.enqueue_ready_once().await.unwrap(), 1);
    let failed_envelopes = failed_capture.envelopes.lock().await;
    let IngressContent::Event {
        payload: failed_payload,
        ..
    } = &failed_envelopes[0].content
    else {
        panic!("failed handle outcome must be event content");
    };
    let failed: HandleTerminalEnvelope = serde_json::from_value(failed_payload.clone()).unwrap();
    assert_eq!(failed.dispatch_id, failed_dispatch.dispatch_id);
    assert_eq!(failed.outcome, HandleTerminalOutcome::Failed);
    assert!(
        failed
            .outcome_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("forced child provider failure"))
    );
    assert!(failed.retryable);
    drop(failed_envelopes);
    failed_server.supervisor().shutdown_all().await.unwrap();

    let blocking = Arc::new(BlockingRouteProviderClient::default());
    let (_fixture_root, cancelled_server, _bridge, cancelled_parent, cancelled_dispatch) =
        thread_handle_fixture(
            "handle-outcome-cancelled",
            blocking.clone() as Arc<dyn ProviderClient>,
            false,
        )
        .await;
    blocking.wait_for_requests(1).await;
    let supervisor = cancelled_server.supervisor();
    let cancelled_child = cancelled_server
        .supervisor()
        .get_thread(cancelled_server.tenant_id(), cancelled_dispatch.thread_id)
        .await
        .unwrap();
    let mut cancelled_events = cancelled_child.subscribe_events();
    let cancelled_store = SqliteSessionStore::open(cancelled_server.session_store_path())
        .await
        .unwrap();
    let cancel = supervisor.cancel(
        cancelled_server.tenant_id(),
        cancelled_dispatch.thread_id,
        "parent cancelled child".to_string(),
    );
    let observe_cancelled = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if matches!(
                cancelled_events.recv().await.unwrap(),
                ThreadEvent::Cancelled { .. }
            ) {
                break;
            }
        }
        assert!(
            cancelled_store
                .read_events(&control_stream_id(&cancelled_parent), None)
                .await
                .unwrap()
                .iter()
                .any(|event| event.kind == EventKind::ThreadJoined),
            "a visible child cancellation must already have a durable terminal join"
        );
    });
    let (cancel, observed) = tokio::join!(cancel, observe_cancelled);
    cancel.unwrap();
    observed.expect("child did not publish its cancellation");
    wait_for_thread_joined(&cancelled_store, &cancelled_parent).await;
    let cancelled_capture = Arc::new(CaptureSink {
        envelopes: Arc::new(TokioMutex::new(Vec::new())),
    });
    let cancelled_adapter = ThreadHandleIngressAdapter::new(
        cancelled_store,
        cancelled_capture.clone() as Arc<dyn IngressSink>,
        &cancelled_parent.tenant_id,
        &cancelled_parent.user_id,
    );
    assert_eq!(cancelled_adapter.enqueue_ready_once().await.unwrap(), 1);
    let cancelled_envelopes = cancelled_capture.envelopes.lock().await;
    let IngressContent::Event {
        payload: cancelled_payload,
        ..
    } = &cancelled_envelopes[0].content
    else {
        panic!("cancelled handle outcome must be event content");
    };
    let cancelled: HandleTerminalEnvelope =
        serde_json::from_value(cancelled_payload.clone()).unwrap();
    assert_eq!(cancelled.dispatch_id, cancelled_dispatch.dispatch_id);
    assert_eq!(cancelled.outcome, HandleTerminalOutcome::Cancelled);
    assert_eq!(
        cancelled.outcome_reason.as_deref(),
        Some("parent cancelled child")
    );
    assert!(!cancelled.retryable);
    drop(cancelled_envelopes);
    blocking.release();
    cancelled_server.supervisor().shutdown_all().await.unwrap();
}

#[tokio::test]
async fn terminal_before_emission_fault_recovers_to_one_parent_turn() {
    let (fixture_root, server, bridge, parent_coordinates, _dispatch) =
        completed_thread_handle_fixture("handle-outcome-crash-window").await;
    let store = SqliteSessionStore::open(server.session_store_path())
        .await
        .unwrap();
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(
            fixture_root.join("handle-crash-window.sqlite"),
            "handle-outcome-crash-window",
        ))
        .await
        .unwrap(),
    );
    let faulting = Arc::new(FaultingIngressQueue::new(queue.clone()).fail_nth(
        "submit",
        1,
        "process cut after terminal observation before ingress emission",
    ));
    let cut_adapter = ThreadHandleIngressAdapter::new(
        store.clone(),
        faulting as Arc<dyn IngressSink>,
        &parent_coordinates.tenant_id,
        &parent_coordinates.user_id,
    );

    assert_eq!(cut_adapter.enqueue_ready_once().await.unwrap(), 0);
    drop(cut_adapter);

    let recovered = ThreadHandleIngressAdapter::new(
        store.clone(),
        queue.clone() as Arc<dyn IngressSink>,
        &parent_coordinates.tenant_id,
        &parent_coordinates.user_id,
    );
    assert_eq!(recovered.enqueue_ready_once().await.unwrap(), 1);
    assert_eq!(recovered.enqueue_ready_once().await.unwrap(), 0);
    let worker = CooldisDaemonQueueWorker::new(queue, bridge, "handle-outcome-recovery-worker", 30);
    assert_eq!(worker.drain_once().await.unwrap(), 1);
    assert_eq!(
        handle_outcome_parent_inputs(&store, &parent_coordinates)
            .await
            .len(),
        1
    );

    server.supervisor().shutdown_all().await.unwrap();
}

async fn egress_receipts(
    bridge: &CooldisDaemonIoBridge,
    thread_id: &str,
    kind: EventKind,
) -> Vec<crate::EventRecord> {
    let parsed = ThreadId::parse_str(thread_id).unwrap();
    let handle = bridge
        .supervisor
        .get_thread(&bridge.tenant_id, parsed)
        .await
        .unwrap();
    handle
        .read_thread_events(None)
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.kind == kind)
        .collect()
}

async fn egress_cursor(bridge: &CooldisDaemonIoBridge, thread_id: &str) -> Option<StreamCursorV1> {
    bridge
        .egress_cursor_for_thread("telegram.bot", "main", thread_id)
        .await
        .unwrap()
}

async fn only_thread_coordinates(bridge: &CooldisDaemonIoBridge) -> ThreadCoordinates {
    bridge
        .threads
        .lock()
        .await
        .values()
        .next()
        .cloned()
        .expect("admission should create a target thread")
}

async fn control_events_for(
    session_store_path: &Path,
    coordinates: &ThreadCoordinates,
) -> Vec<crate::EventRecord> {
    let session_store = crate::SqliteSessionStore::open(session_store_path)
        .await
        .unwrap();
    session_store
        .read_events(&control_stream_id(coordinates), None)
        .await
        .unwrap()
}

async fn thread_events_for(
    session_store_path: &Path,
    coordinates: &ThreadCoordinates,
) -> Vec<crate::EventRecord> {
    let session_store = crate::SqliteSessionStore::open(session_store_path)
        .await
        .unwrap();
    session_store
        .read_events(&crate::EventStreamId::for_thread(coordinates), None)
        .await
        .unwrap()
}

async fn assert_single_durable_ingress_turn(
    session_store_path: &Path,
    coordinates: &ThreadCoordinates,
    message_id: &str,
) {
    let control_events = control_events_for(session_store_path, coordinates).await;
    let mut thread_events = thread_events_for(session_store_path, coordinates).await;
    for _ in 0..100 {
        if thread_events
            .iter()
            .any(|event| event.kind == EventKind::TurnSubmitted)
        {
            break;
        }
        tokio::task::yield_now().await;
        thread_events = thread_events_for(session_store_path, coordinates).await;
    }
    assert_eq!(
        control_events
            .iter()
            .chain(&thread_events)
            .filter(|event| event.kind == EventKind::IoIngressReceived)
            .count(),
        1,
        "durable ingress redelivery must leave one receipt across all streams"
    );
    assert_eq!(
        thread_events
            .iter()
            .filter(|event| event.kind == EventKind::TurnSubmitted)
            .count(),
        1,
        "durable ingress redelivery must not submit a second turn"
    );
    let claims = control_events
        .iter()
        .filter(|event| {
            event.kind == EventKind::IoIngressClaimed
                && event.payload["ingress_envelope_ids"]
                    .as_array()
                    .is_some_and(|ids| ids.iter().any(|id| id.as_str() == Some(message_id)))
        })
        .collect::<Vec<_>>();
    assert_eq!(claims.len(), 1, "expected one durable ingress claim");
    let settles = control_events
        .iter()
        .filter(|event| {
            event.kind == EventKind::IoIngressSettled
                && event.payload["claim_event_id"].as_str()
                    == Some(claims[0].id.to_string().as_str())
        })
        .collect::<Vec<_>>();
    assert_eq!(settles.len(), 1, "expected one durable ingress settle");
    assert!(settles[0].payload["evidence_event_id"].as_str().is_some());
}

async fn user_texts_for(
    bridge: &CooldisDaemonIoBridge,
    coordinates: &ThreadCoordinates,
) -> Vec<String> {
    let handle = bridge.supervisor.get_thread_at(coordinates).await.unwrap();
    handle
        .session_context()
        .await
        .unwrap()
        .entries
        .iter()
        .filter_map(|entry| match &entry.kind {
            SessionEntryKind::Message {
                message: CanonicalMessage::User { content, .. },
            }
            | SessionEntryKind::CustomContextMessage {
                message: CanonicalMessage::User { content, .. },
            } => Some(text_from_canonical_content(content)),
            _ => None,
        })
        .collect()
}

async fn wait_for_user_text(
    bridge: &CooldisDaemonIoBridge,
    coordinates: &ThreadCoordinates,
    expected: &str,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if user_texts_for(bridge, coordinates)
            .await
            .contains(&expected.to_string())
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for user text {expected:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_user_text_containing(
    bridge: &CooldisDaemonIoBridge,
    coordinates: &ThreadCoordinates,
    expected: &str,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if user_texts_for(bridge, coordinates)
            .await
            .iter()
            .any(|text| text.contains(expected))
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for user text containing {expected:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn admission_source_ids(event: &crate::EventRecord) -> Vec<String> {
    event.payload["source_ingress_event_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap().to_string())
        .collect()
}

async fn drain_until_egress(
    bridge: &CooldisDaemonIoBridge,
    protocol: &str,
    instance_id: &str,
    expected: usize,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut drained = 0;
    loop {
        drained += bridge
            .drain_egress_once(protocol, instance_id)
            .await
            .unwrap();
        if drained >= expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {expected} egress source(s), drained {drained}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn egress_projection_strips_platform_action_tag_and_preserves_order() {
    let route = route_with_egress(
        vec![crate::CooldisEgressProjectionRuleConfig {
            pattern: r"\[sticker:(?P<file_id>[^\]]+)\]".to_string(),
            action: "sticker".to_string(),
        }],
        None,
    );
    let config = RouteEgressConfig::from_route(&route).unwrap();

    let projected = config.project(test_egress("hello[sticker:file-123] friend"));

    assert_eq!(projected.len(), 2);
    assert!(matches!(
        projected[0].kind,
        EgressKind::AssistantMessage { ref text } if text == "hello friend"
    ));
    assert!(matches!(
        projected[1].kind,
        EgressKind::PlatformAction { ref action, ref payload }
            if action == "sticker"
                && payload["file_id"] == "file-123"
                && payload["message_id"].is_null()
    ));
}

#[tokio::test]
async fn egress_projection_turns_no_response_tag_into_silence() {
    let route = route_with_egress(
        vec![crate::CooldisEgressProjectionRuleConfig {
            pattern: r"\[no_response\]".to_string(),
            action: "silence".to_string(),
        }],
        None,
    );
    let config = RouteEgressConfig::from_route(&route).unwrap();

    let projected = config.project(test_egress("[no_response]"));

    assert_eq!(projected.len(), 1);
    assert!(matches!(
        projected[0].kind,
        EgressKind::Silence { reason: None }
    ));
}

#[tokio::test]
async fn egress_projection_leaves_text_without_tags_unchanged() {
    let route = route_with_egress(
        vec![crate::CooldisEgressProjectionRuleConfig {
            pattern: r"\[sticker:(?P<file_id>[^\]]+)\]".to_string(),
            action: "sticker".to_string(),
        }],
        None,
    );
    let config = RouteEgressConfig::from_route(&route).unwrap();

    let projected = config.project(test_egress("plain answer"));

    assert_eq!(projected.len(), 1);
    assert!(matches!(
        projected[0].kind,
        EgressKind::AssistantMessage { ref text } if text == "plain answer"
    ));
}

#[tokio::test(start_paused = true)]
async fn typing_simulation_sends_typing_action_and_delays_text() {
    assert_eq!(typing_delay_for_text("abcd", 2), Duration::from_secs(2));
    assert_eq!(
        typing_delay_for_text("abcdefghi", 1),
        Duration::from_secs(8)
    );

    let (bridge, mut rx, _) = test_bridge().await;
    let route = route_with_egress(
        Vec::new(),
        Some(crate::CooldisTypingSimulationConfig {
            chars_per_second: 2,
        }),
    );
    bridge
        .register_egress_route_config("telegram.bot", "main", &route)
        .await
        .unwrap();

    let deliver = tokio::spawn({
        let bridge = bridge.clone();
        async move {
            bridge.deliver_egress(test_egress("abcd")).await;
        }
    });

    let typing = rx.recv().await.unwrap();
    assert!(matches!(
        typing.kind,
        EgressKind::PlatformAction { ref action, .. } if action == "typing"
    ));
    assert!(rx.try_recv().is_err());

    tokio::time::advance(Duration::from_millis(1_999)).await;
    tokio::task::yield_now().await;
    assert!(rx.try_recv().is_err());

    tokio::time::advance(Duration::from_millis(1)).await;
    let text = rx.recv().await.unwrap();
    assert!(matches!(
        text.kind,
        EgressKind::AssistantMessage { ref text } if text == "abcd"
    ));
    deliver.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn typing_simulation_is_off_by_default() {
    let (bridge, mut rx, _) = test_bridge().await;

    bridge.deliver_egress(test_egress("no typing")).await;

    let text = rx.recv().await.unwrap();
    assert!(matches!(
        text.kind,
        EgressKind::AssistantMessage { ref text } if text == "no typing"
    ));
    assert!(rx.try_recv().is_err());
}

#[derive(Clone)]
struct FakeClock {
    now: Arc<StdMutex<DateTime<Utc>>>,
}

impl FakeClock {
    fn new(now: DateTime<Utc>) -> Self {
        Self {
            now: Arc::new(StdMutex::new(now)),
        }
    }

    fn set(&self, now: DateTime<Utc>) {
        *self.now.lock().unwrap() = now;
    }
}

impl DaemonClock for FakeClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().unwrap()
    }
}

async fn start_clock_thread_with_mandate(
    server: &CooldisAppServer,
    catch_up: MandateCatchUpPolicy,
) -> (
    SqliteSessionStore,
    ThreadCoordinates,
    crate::MandateStartReceipt,
) {
    let handle = server
        .supervisor()
        .start_thread(ThreadStartRequest {
            tenant_id: server.tenant_id().to_string(),
            user_id: server.user_id().to_string(),
            session_id: format!("clock-{}", uuid::Uuid::now_v7()),
            topology: ThreadTopology::root(),
            metadata: BTreeMap::new(),
        })
        .await
        .unwrap();
    let coordinates = handle.context().coordinates.clone();
    let store = SqliteSessionStore::open(server.session_store_path())
        .await
        .unwrap();
    let receipt = start_mandate(
        &store,
        &coordinates,
        MandateStartRequest {
            schedule: MandateSchedulePayload::Interval { every_ms: 60_000 },
            max_occurrences: Some(3),
            catch_up: Some(catch_up),
            input_template: Some("wake".to_string()),
            snapshot_id: None,
            expires_at: None,
        },
        Utc::now(),
    )
    .await
    .unwrap();
    (store, coordinates, receipt)
}

fn event_time(event_ms: i64, offset_ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(event_ms + offset_ms)
        .single()
        .unwrap()
}

async fn timer_payloads(
    store: &SqliteSessionStore,
    coordinates: &ThreadCoordinates,
) -> Vec<TimerFiredPayload> {
    store
        .read_events(&control_stream_id(coordinates), None)
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.kind == EventKind::TimerFired)
        .map(|event| serde_json::from_value(event.payload).unwrap())
        .collect()
}

#[tokio::test]
async fn direct_sink_submits_ingress_to_runtime_and_emits_egress() {
    let (bridge, mut rx, _) = test_bridge().await;
    let db = test_root("direct-egress").join("io.sqlite");
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &db).await;

    let ack = bridge
        .direct_sink()
        .submit(test_envelope("hello direct"))
        .await
        .unwrap();

    assert!(ack.accepted);
    drain_until_egress(&bridge, "telegram.bot", "main", 1).await;
    let egress = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        egress.kind,
        EgressKind::AssistantMessage { ref text } if text.contains("hello direct")
    ));
}

#[tokio::test]
async fn route_agent_ref_binds_manifest_prompt_metadata_and_receipts() {
    let root = test_root("route-agent-binding");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = root.join("operations");
    let operation = publish_route_test_operation(&operation_registry_root).await;
    let agent_registry_root = root.join("agents");
    let agent = publish_route_agent_manifest(
        &root,
        &agent_registry_root,
        &operation_registry_root,
        &operation.active_artifact_hash,
    );
    let client = Arc::new(RecordingRouteProviderClient::default());
    let server = test_server_with_route_provider_at_root(
        &root,
        &workspace,
        &agent_registry_root,
        &operation_registry_root,
        client.clone(),
    )
    .await;
    let session_store_path = server.session_store_path().to_path_buf();
    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let mut route = route_with_egress(Vec::new(), None);
    route.agent_ref = Some("agent://daemon-route-runner@latest".to_string());
    let sink = RouteIngressSink::new(bridge.direct_sink(), &route);

    sink.submit(test_envelope("hello route")).await.unwrap();

    wait_for_provider_requests(&client, 1).await;
    let requests = client.requests();
    assert_eq!(
        requests[0].system[0].text,
        "You are the daemon route prompt runner.\n"
    );
    assert!(
        requests[0].system[1]
            .text
            .contains("You are running as agent://daemon-route-runner@0.1.0"),
        "{:?}",
        requests[0].system
    );

    let coordinates = only_thread_coordinates(&bridge).await;
    let handle = bridge.supervisor.get_thread_at(&coordinates).await.unwrap();
    let metadata = &handle.context().metadata;
    assert_eq!(
        metadata.get("cooldis.agent.ref_uri").map(String::as_str),
        Some("agent://daemon-route-runner@0.1.0")
    );
    assert_eq!(
        metadata
            .get(THREAD_AGENT_MANIFEST_HASH_METADATA)
            .map(String::as_str),
        Some(agent.manifest_hash.as_str())
    );
    let static_segments = metadata
        .get(THREAD_AGENT_STATIC_CONTEXT_SEGMENTS_METADATA)
        .expect("static context segment metadata should be stamped");
    let static_segments: Vec<serde_json::Value> = serde_json::from_str(static_segments).unwrap();
    assert_eq!(static_segments[0]["id"].as_str(), Some("identity"));
    assert!(
        static_segments[0]["content_sha256"]
            .as_str()
            .is_some_and(|hash| hash.starts_with("sha256:"))
    );

    let thread_events = thread_events_for(&session_store_path, &coordinates).await;
    assert!(
        thread_events
            .iter()
            .any(|event| event.kind == EventKind::ManifestBindCompleted)
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn route_agent_identity_survives_true_runtime_restart() {
    let root = test_root("route-agent-restart-identity");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = root.join("operations");
    let operation = publish_route_test_operation(&operation_registry_root).await;
    let agent_registry_root = root.join("agents");
    publish_route_agent_manifest(
        &root,
        &agent_registry_root,
        &operation_registry_root,
        &operation.active_artifact_hash,
    );
    let db = root.join("io.sqlite");
    let mut route = route_with_egress(Vec::new(), None);
    route.agent_ref = Some("agent://daemon-route-runner@latest".to_string());

    let first_client = Arc::new(RecordingRouteProviderClient::default());
    let first_server = test_server_with_route_provider_at_root(
        &root,
        &workspace,
        &agent_registry_root,
        &operation_registry_root,
        first_client.clone(),
    )
    .await;
    let first_bridge = CooldisDaemonIoBridge::from_app_server(&first_server);
    register_route_state(&first_bridge, &route, &db).await;
    RouteIngressSink::new(first_bridge.direct_sink(), &route)
        .submit(test_envelope("before restart"))
        .await
        .unwrap();
    wait_for_provider_requests(&first_client, 1).await;
    let coordinates = only_thread_coordinates(&first_bridge).await;
    drop(first_bridge);
    drop(first_server);

    let restarted_client = Arc::new(RecordingRouteProviderClient::default());
    let restarted_server = test_server_with_route_provider_at_root(
        &root,
        &workspace,
        &agent_registry_root,
        &operation_registry_root,
        restarted_client.clone(),
    )
    .await;
    let restarted = CooldisDaemonIoBridge::from_app_server(&restarted_server);
    register_route_state(&restarted, &route, &db).await;
    assert!(matches!(
        restarted.supervisor.get_thread_at(&coordinates).await,
        Err(CooldisError::ThreadNotFound(_))
    ));

    RouteIngressSink::new(restarted.direct_sink(), &route)
        .submit(test_envelope("after restart"))
        .await
        .unwrap();
    wait_for_provider_requests(&restarted_client, 1).await;

    let requests = restarted_client.requests();
    assert_eq!(
        requests[0].system[0].text,
        "You are the daemon route prompt runner.\n"
    );
    assert!(requests[0].tools.iter().any(|tool| tool.name == "lookup"));
    let reloaded = restarted
        .supervisor
        .get_thread_at(&coordinates)
        .await
        .unwrap();
    assert!(
        reloaded
            .context()
            .metadata
            .contains_key(THREAD_AGENT_STATIC_CONTEXT_SEGMENTS_METADATA)
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn fork_child_identity_survives_true_runtime_restart() {
    let root = test_root("route-fork-restart-identity");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = root.join("operations");
    let operation = publish_route_test_operation(&operation_registry_root).await;
    let agent_registry_root = root.join("agents");
    publish_route_agent_manifest(
        &root,
        &agent_registry_root,
        &operation_registry_root,
        &operation.active_artifact_hash,
    );
    let db = root.join("io.sqlite");
    let mut route = route_with_egress(Vec::new(), None);
    route.policy = Some("fork_on_new_dm".to_string());
    route.agent_ref = Some("agent://daemon-route-runner@latest".to_string());

    let first_client = Arc::new(RecordingRouteProviderClient::default());
    let first_server = test_server_with_route_provider_at_root(
        &root,
        &workspace,
        &agent_registry_root,
        &operation_registry_root,
        first_client.clone(),
    )
    .await;
    let first_bridge = CooldisDaemonIoBridge::from_app_server(&first_server);
    register_route_state(&first_bridge, &route, &db).await;
    RouteIngressSink::new(first_bridge.direct_sink(), &route)
        .submit(test_envelope("before fork restart"))
        .await
        .unwrap();
    wait_for_provider_requests(&first_client, 1).await;
    let child_coordinates = only_thread_coordinates(&first_bridge).await;
    let child = first_bridge
        .supervisor
        .get_thread_at(&child_coordinates)
        .await
        .unwrap();
    let expected_parent = child.context().parent_thread_id.unwrap();
    let expected_topology = child.context().topology.clone();
    drop(first_bridge);
    drop(first_server);

    let restarted_client = Arc::new(RecordingRouteProviderClient::default());
    let restarted_server = test_server_with_route_provider_at_root(
        &root,
        &workspace,
        &agent_registry_root,
        &operation_registry_root,
        restarted_client.clone(),
    )
    .await;
    let restarted = CooldisDaemonIoBridge::from_app_server(&restarted_server);
    register_route_state(&restarted, &route, &db).await;
    RouteIngressSink::new(restarted.direct_sink(), &route)
        .submit(test_envelope("after fork restart"))
        .await
        .unwrap();
    wait_for_provider_requests(&restarted_client, 1).await;

    let child = restarted
        .supervisor
        .get_thread_at(&child_coordinates)
        .await
        .unwrap();
    assert_eq!(child.context().parent_thread_id, Some(expected_parent));
    assert_eq!(child.context().topology, expected_topology);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn legacy_lazy_reload_fabricates_root_and_witnesses_every_fallback() {
    let root = test_root("legacy-reload-degraded");
    let (_server, bridge, _rx) = test_bridge_at_root(&root).await;
    let envelope = test_envelope("legacy reload");
    let target = bridge.resolve_target(&envelope).await.unwrap();
    let coordinates = ThreadCoordinates {
        tenant_id: target.address.tenant_id,
        user_id: target.address.user_id,
        session_id: target.address.session_id,
        thread_id: ThreadId::new(),
    };

    for expected_witnesses in 1..=2 {
        let handle = bridge
            .get_or_load_thread_handle(&coordinates)
            .await
            .unwrap();
        assert_eq!(handle.context().parent_thread_id, None);
        assert_eq!(handle.context().topology, ThreadTopology::root());
        assert!(handle.context().metadata.is_empty());
        bridge
            .supervisor
            .shutdown_thread_at(&coordinates)
            .await
            .unwrap();

        let events =
            thread_events_for(bridge.session_store_path.as_ref().unwrap(), &coordinates).await;
        let degraded = events
            .iter()
            .filter(|event| event.kind == EventKind::ThreadReloadDegraded)
            .collect::<Vec<_>>();
        assert_eq!(degraded.len(), expected_witnesses);
        let payload: crate::ThreadReloadDegradedPayload =
            serde_json::from_value(degraded.last().unwrap().payload.clone()).unwrap();
        assert_eq!(payload.thread_id, coordinates.thread_id);
        assert_eq!(
            payload.missing,
            vec!["topology", "parent_thread_id", "metadata"]
        );
        assert_eq!(payload.fallback, "fabricated_root");
    }
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn route_without_agent_ref_stays_unbound() {
    let root = test_root("route-without-agent-ref");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = root.join("operations");
    let agent_registry_root = root.join("agents");
    let client = Arc::new(RecordingRouteProviderClient::default());
    let server = test_server_with_route_provider_at_root(
        &root,
        &workspace,
        &agent_registry_root,
        &operation_registry_root,
        client.clone(),
    )
    .await;
    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let route = route_with_egress(Vec::new(), None);
    let sink = RouteIngressSink::new(bridge.direct_sink(), &route);

    sink.submit(test_envelope("hello unbound")).await.unwrap();

    wait_for_provider_requests(&client, 1).await;
    assert!(client.requests()[0].system.is_empty());
    let coordinates = only_thread_coordinates(&bridge).await;
    let handle = bridge.supervisor.get_thread_at(&coordinates).await.unwrap();
    assert!(
        !handle
            .context()
            .metadata
            .contains_key(THREAD_AGENT_MANIFEST_HASH_METADATA)
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn route_agent_ref_unknown_fails_with_registry_publish_hint() {
    let root = test_root("route-agent-missing");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = root.join("operations");
    let agent_registry_root = root.join("agents");
    let client = Arc::new(RecordingRouteProviderClient::default());
    let server = test_server_with_route_provider_at_root(
        &root,
        &workspace,
        &agent_registry_root,
        &operation_registry_root,
        client,
    )
    .await;
    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let mut route = route_with_egress(Vec::new(), None);
    route.agent_ref = Some("agent://missing-route-agent@latest".to_string());

    let err = bridge.validate_route_agent_ref(&route).await.unwrap_err();
    let message = err.to_string();
    assert!(message.contains("io.routes.main.agent_ref"));
    assert!(message.contains(&agent_registry_root.display().to_string()));
    assert!(message.contains("cooldis agent publish"));
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn route_agent_ref_missing_blob_fails_startup_validation() {
    let root = test_root("route-agent-missing-blob");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = root.join("operations");
    let agent_registry_root = root.join("agents");
    publish_route_agent_manifest_with_missing_blob(&root, &agent_registry_root);
    let client = Arc::new(RecordingRouteProviderClient::default());
    let server = test_server_with_route_provider_at_root(
        &root,
        &workspace,
        &agent_registry_root,
        &operation_registry_root,
        client,
    )
    .await;
    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let mut route = route_with_egress(Vec::new(), None);
    route.agent_ref = Some("agent://daemon-missing-blob@latest".to_string());

    let err = bridge.validate_route_agent_ref(&route).await.unwrap_err();
    let message = err.to_string();
    assert!(message.contains("io.routes.main.agent_ref"), "{message}");
    assert!(message.contains("did not bind"), "{message}");
    assert!(
        message.contains("blob resource \"system_prompt\""),
        "{message}"
    );
    assert!(message.contains("cooldis blob publish"), "{message}");
    assert!(bridge.threads.lock().await.is_empty());
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn fork_on_new_dm_child_inherits_route_agent_binding() {
    let root = test_root("route-agent-fork");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = root.join("operations");
    let operation = publish_route_test_operation(&operation_registry_root).await;
    let agent_registry_root = root.join("agents");
    let agent = publish_route_agent_manifest(
        &root,
        &agent_registry_root,
        &operation_registry_root,
        &operation.active_artifact_hash,
    );
    let client = Arc::new(RecordingRouteProviderClient::default());
    let server = test_server_with_route_provider_at_root(
        &root,
        &workspace,
        &agent_registry_root,
        &operation_registry_root,
        client.clone(),
    )
    .await;
    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let mut route = route_with_egress(Vec::new(), None);
    route.policy = Some("fork_on_new_dm".to_string());
    route.agent_ref = Some("agent://daemon-route-runner@latest".to_string());
    let sink = RouteIngressSink::new(bridge.direct_sink(), &route);

    sink.submit(test_envelope("fork route")).await.unwrap();

    wait_for_provider_requests(&client, 1).await;
    let child_coordinates = only_thread_coordinates(&bridge).await;
    let child = bridge
        .supervisor
        .get_thread_at(&child_coordinates)
        .await
        .unwrap();
    assert_eq!(
        child
            .context()
            .metadata
            .get(THREAD_AGENT_MANIFEST_HASH_METADATA)
            .map(String::as_str),
        Some(agent.manifest_hash.as_str())
    );
    let parent_thread_id = child
        .context()
        .parent_thread_id
        .expect("fork child should reference parent");
    let parent = bridge
        .supervisor
        .get_thread(&bridge.tenant_id, parent_thread_id)
        .await
        .unwrap();
    assert_eq!(
        parent
            .context()
            .metadata
            .get(THREAD_AGENT_MANIFEST_HASH_METADATA)
            .map(String::as_str),
        Some(agent.manifest_hash.as_str())
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(start_paused = true)]
async fn queue_worker_redelivery_after_complete_failure_does_not_duplicate_turn() {
    let fixture_root = test_root("queue-complete-redelivery");
    let (server, bridge, _rx) = test_bridge_at_root(&fixture_root).await;
    let session_store_path = server.session_store_path().to_path_buf();
    let egress_db = fixture_root.join("io.sqlite");
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    let envelope = telegram_queue_envelope("apply once after ack failure");
    let ingress_id = envelope.id.clone();
    let queue =
        ScriptedIngressQueue::new("message-redelivery", envelope, std::iter::empty::<&str>());
    let faulting_queue = Arc::new(FaultingIngressQueue::new(Arc::new(queue.clone())).fail_nth(
        "complete_ingress",
        1,
        "scripted complete failure",
    ));
    let worker = CooldisDaemonQueueWorker::new(
        faulting_queue.clone(),
        bridge.clone(),
        "worker-redelivery",
        30,
    );

    let err = worker.drain_once().await.unwrap_err();
    assert!(err.to_string().contains("scripted complete failure"));
    tokio::time::advance(Duration::from_secs(30)).await;
    assert_eq!(worker.drain_once().await.unwrap(), 1);

    assert!(queue.completed().await);
    assert_eq!(queue.complete_calls().await, 1);
    assert_eq!(faulting_queue.call_count("complete_ingress"), 2);
    let coordinates = only_thread_coordinates(&bridge).await;
    assert_single_durable_ingress_turn(&session_store_path, &coordinates, &ingress_id).await;
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test]
async fn racing_fork_applies_create_one_child_behind_one_parent_claim() {
    let fixture_root = test_root("fork-racing-applies");
    let (server, bridge, _rx) = test_bridge_at_root(&fixture_root).await;
    let session_store_path = server.session_store_path().to_path_buf();
    let egress_db = fixture_root.join("io.sqlite");
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    bridge
        .submit_envelope(test_envelope("seed the shared fork parent"))
        .await
        .unwrap();
    let parent_coordinates = only_thread_coordinates(&bridge).await;
    wait_for_user_text(&bridge, &parent_coordinates, "seed the shared fork parent").await;
    let competing_bridge = CooldisDaemonIoBridge::from_app_server(&server);
    register_route_state(
        &competing_bridge,
        &route_with_egress(Vec::new(), None),
        &egress_db,
    )
    .await;
    let envelope = telegram_queue_envelope("fork once under contention")
        .with_metadata("cooldis_route_policy", "fork_on_new_dm");

    let (first, second) = tokio::join!(
        bridge.submit_queued_envelope(envelope.clone(), 1),
        competing_bridge.submit_queued_envelope(envelope.clone(), 1)
    );
    first.unwrap();
    second.unwrap();
    assert_eq!(
        bridge.fork_claim_scan_count.load(Ordering::SeqCst)
            + competing_bridge
                .fork_claim_scan_count
                .load(Ordering::SeqCst),
        0,
        "fresh fork admission must not scan for recovery evidence under the scope lock"
    );

    let bindings = route_bindings(&bridge).await;
    assert_eq!(
        bindings.len(),
        2,
        "the parent should have exactly one child"
    );
    let mut control_events = Vec::new();
    let mut submitted = 0;
    for binding in &bindings {
        control_events.extend(control_events_for(&session_store_path, &binding.coordinates).await);
        submitted += thread_events_for(&session_store_path, &binding.coordinates)
            .await
            .iter()
            .filter(|event| {
                event.kind == EventKind::TurnSubmitted
                    && event.payload["ingress_envelope_id"].as_str() == Some(&envelope.id)
            })
            .count();
    }
    assert_eq!(
        control_events
            .iter()
            .filter(|event| event.kind == EventKind::IoIngressClaimed)
            .count(),
        1
    );
    assert_eq!(
        control_events
            .iter()
            .filter(|event| event.kind == EventKind::ThreadSpawned)
            .count(),
        1
    );
    assert_eq!(submitted, 1);
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test]
async fn durable_ingress_witness_and_admission_are_single_under_racing_applies() {
    let fixture_root = test_root("durable-ingress-single-preclaim-facts");
    let (server, bridge, _rx) = test_bridge_at_root(&fixture_root).await;
    let session_store_path = server.session_store_path().to_path_buf();
    let envelope = telegram_queue_envelope("race the pre-claim facts");
    let target = bridge.resolve_target(&envelope).await.unwrap();
    let coordinates = start_thread_for_target(&bridge, &target).await;
    let competing_bridge = CooldisDaemonIoBridge::from_app_server(&server);

    let (first, second) = tokio::join!(
        bridge.record_ingress_received(&coordinates, &envelope, Some(&envelope.id)),
        competing_bridge.record_ingress_received(&coordinates, &envelope, Some(&envelope.id)),
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(
        first.id, second.id,
        "racing applies must share one ingress witness"
    );

    let decision = AdmissionDecision::queue(
        "turn-preclaim-race",
        IoTurnInput::from_envelope(&envelope, &target),
    );
    let (first, second) = tokio::join!(
        bridge.record_admission_decided(
            &coordinates,
            &envelope,
            &decision,
            "sha256:policy",
            vec![first.id],
            false,
            true,
        ),
        competing_bridge.record_admission_decided(
            &coordinates,
            &envelope,
            &decision,
            "sha256:policy",
            vec![first.id],
            false,
            true,
        ),
    );
    assert_eq!(
        first.unwrap().id,
        second.unwrap().id,
        "racing applies must share one admission decision"
    );

    let events = control_events_for(&session_store_path, &coordinates).await;
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::IoIngressReceived)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::AdmissionDecided)
            .count(),
        1
    );
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test]
async fn racing_initial_applies_share_the_durable_conversation_binding() {
    let fixture_root = test_root("durable-ingress-racing-initial-binding");
    let (server, bridge, _rx) = test_bridge_at_root(&fixture_root).await;
    let egress_db = fixture_root.join("io.sqlite");
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    let competing_bridge = CooldisDaemonIoBridge::from_app_server(&server);
    register_route_state(
        &competing_bridge,
        &route_with_egress(Vec::new(), None),
        &egress_db,
    )
    .await;
    let barrier = Arc::new(Barrier::new(2));
    *bridge.ingress_binding_barrier.lock().unwrap() = Some(Arc::clone(&barrier));
    *competing_bridge.ingress_binding_barrier.lock().unwrap() = Some(barrier);
    let envelope = telegram_queue_envelope("race the initial binding");

    let (first, second) = tokio::join!(
        bridge.submit_queued_envelope(envelope.clone(), 1),
        competing_bridge.submit_queued_envelope(envelope.clone(), 1),
    );
    first.unwrap();
    second.unwrap();

    let bindings = route_bindings(&bridge).await;
    assert_eq!(
        bindings.len(),
        1,
        "the durable scope must select one root thread"
    );
    let coordinates = &bindings[0].coordinates;
    let events = control_events_for(server.session_store_path(), coordinates).await;
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::IoIngressClaimed)
            .count(),
        1
    );
    let snapshot = bridge.supervisor.snapshot().await;
    let tenant = snapshot
        .tenants
        .iter()
        .find(|tenant| tenant.tenant_id == bridge.tenant_id)
        .unwrap();
    assert_eq!(
        tenant.runtime.threads.len(),
        1,
        "only the reserved root may become resident"
    );
    let mut candidates = bridge.initial_root_candidates.lock().unwrap().clone();
    candidates.extend(
        competing_bridge
            .initial_root_candidates
            .lock()
            .unwrap()
            .clone(),
    );
    candidates.sort_by_key(|candidate| candidate.thread_id.to_string());
    candidates.dedup_by_key(|candidate| candidate.thread_id);
    assert_eq!(
        candidates.len(),
        2,
        "both racers must preallocate a root id"
    );
    let loser = candidates
        .iter()
        .find(|candidate| candidate.thread_id != coordinates.thread_id)
        .expect("one candidate must lose the durable route reservation");
    assert!(
        thread_events_for(server.session_store_path(), loser)
            .await
            .is_empty(),
        "the losing root candidate must write zero durable start history"
    );
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test]
async fn coalesced_fork_first_attempt_loser_runs_no_recovery_effects() {
    let fixture_root = test_root("coalesced-fork-first-attempt-loser");
    let (server, bridge, _rx) = test_bridge_at_root(&fixture_root).await;
    let egress_db = fixture_root.join("io.sqlite");
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    bridge
        .submit_envelope(test_envelope("seed the coalesced fork parent"))
        .await
        .unwrap();
    let parent_coordinates = only_thread_coordinates(&bridge).await;
    let competing_bridge = CooldisDaemonIoBridge::from_app_server(&server);
    register_route_state(
        &competing_bridge,
        &route_with_egress(Vec::new(), None),
        &egress_db,
    )
    .await;
    let envelope = telegram_queue_envelope("coalesced fork contention")
        .with_metadata("cooldis_route_policy", "fork_on_new_dm");
    bridge
        .pause_after_ingress_claim
        .store(true, Ordering::SeqCst);
    let claim_paused = bridge.ingress_claim_paused.notified();
    let first_bridge = bridge.clone();
    let first_envelope = envelope.clone();
    let first = tokio::spawn(async move {
        first_bridge
            .submit_coalesced_queued_envelopes(
                first_envelope.clone(),
                std::slice::from_ref(&first_envelope),
                std::slice::from_ref(&first_envelope.id),
                1,
            )
            .await
    });
    claim_paused.await;

    competing_bridge
        .submit_coalesced_queued_envelopes(
            envelope.clone(),
            std::slice::from_ref(&envelope),
            std::slice::from_ref(&envelope.id),
            1,
        )
        .await
        .unwrap();
    assert!(
        bridge
            .supervisor
            .children_of_at(&parent_coordinates)
            .await
            .unwrap()
            .is_empty(),
        "a racing first-attempt loser must not recover the claim owner's fork"
    );
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test(start_paused = true)]
async fn settled_fork_redelivery_repeats_no_control_effects() {
    let fixture_root = test_root("fork-settled-redelivery");
    let (server, bridge, _rx) = test_bridge_at_root(&fixture_root).await;
    let session_store_path = server.session_store_path().to_path_buf();
    let egress_db = fixture_root.join("io.sqlite");
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    let envelope = telegram_queue_envelope("redeliver a settled fork")
        .with_metadata("cooldis_route_policy", "fork_on_new_dm");
    let queue = ScriptedIngressQueue::new(
        "message-fork-settled-redelivery",
        envelope,
        std::iter::empty::<&str>(),
    );
    let faulting_queue = Arc::new(FaultingIngressQueue::new(Arc::new(queue.clone())).fail_nth(
        "complete_ingress",
        1,
        "scripted complete failure",
    ));
    let worker = CooldisDaemonQueueWorker::new(
        faulting_queue,
        bridge.clone(),
        "worker-fork-settled-redelivery",
        30,
    );

    let err = worker.drain_once().await.unwrap_err();
    assert!(err.to_string().contains("scripted complete failure"));
    tokio::time::advance(Duration::from_secs(30)).await;
    assert_eq!(worker.drain_once().await.unwrap(), 1);

    let bindings = route_bindings(&bridge).await;
    assert_eq!(
        bindings.len(),
        2,
        "redelivery must not create a second child"
    );
    let mut control_events = Vec::new();
    let mut submitted = 0;
    for binding in &bindings {
        control_events.extend(control_events_for(&session_store_path, &binding.coordinates).await);
        submitted += thread_events_for(&session_store_path, &binding.coordinates)
            .await
            .iter()
            .filter(|event| event.kind == EventKind::TurnSubmitted)
            .count();
    }
    assert_eq!(
        control_events
            .iter()
            .filter(|event| event.kind == EventKind::IoIngressClaimed)
            .count(),
        1
    );
    assert_eq!(
        control_events
            .iter()
            .filter(|event| event.kind == EventKind::IoIngressSettled)
            .count(),
        1
    );
    assert_eq!(
        control_events
            .iter()
            .filter(|event| event.kind == EventKind::ThreadSpawned)
            .count(),
        1
    );
    assert_eq!(submitted, 1);
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test(start_paused = true)]
async fn fork_claim_before_fork_recovers_one_child_after_restart() {
    let fixture_root = test_root("fork-claim-before-fork-cut");
    let egress_db = fixture_root.join("io.sqlite");
    let (server, bridge, _rx) = test_bridge_at_root(&fixture_root).await;
    let session_store_path = server.session_store_path().to_path_buf();
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    let envelope = telegram_queue_envelope("recover fork after claim")
        .with_metadata("cooldis_route_policy", "fork_on_new_dm");
    let queue = Arc::new(ScriptedIngressQueue::new(
        "message-fork-claim-cut",
        envelope,
        std::iter::empty::<&str>(),
    ));
    bridge
        .pause_after_ingress_claim
        .store(true, Ordering::SeqCst);
    let claim_paused = bridge.ingress_claim_paused.notified();
    let worker = CooldisDaemonQueueWorker::new(
        queue.clone(),
        bridge.clone(),
        "worker-before-fork-claim-cut",
        30,
    );
    let drain = tokio::spawn(async move { worker.drain_once().await });
    claim_paused.await;

    let parent_coordinates = only_thread_coordinates(&bridge).await;
    let control_events = control_events_for(&session_store_path, &parent_coordinates).await;
    assert_eq!(
        control_events
            .iter()
            .filter(|event| event.kind == EventKind::IoIngressClaimed)
            .count(),
        1
    );
    assert!(
        !control_events
            .iter()
            .any(|event| event.kind == EventKind::ThreadSpawned)
    );
    assert!(
        !control_events
            .iter()
            .any(|event| event.kind == EventKind::IoIngressSettled)
    );
    assert!(
        bridge
            .supervisor
            .children_of_at(&parent_coordinates)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(route_bindings(&bridge).await.len(), 1);

    drain.abort();
    assert!(drain.await.unwrap_err().is_cancelled());
    drop(bridge);
    drop(server);
    tokio::time::advance(Duration::from_secs(30)).await;

    let (_server, restarted_bridge, _rx) = restarted_bridge_at_root(&fixture_root).await;
    register_route_state(
        &restarted_bridge,
        &route_with_egress(Vec::new(), None),
        &egress_db,
    )
    .await;
    let restarted_worker = CooldisDaemonQueueWorker::new(
        queue.clone(),
        restarted_bridge.clone(),
        "worker-after-fork-claim-cut",
        30,
    );
    assert_eq!(restarted_worker.drain_once().await.unwrap(), 1);

    let control_events = control_events_for(&session_store_path, &parent_coordinates).await;
    let claim = control_events
        .iter()
        .find(|event| event.kind == EventKind::IoIngressClaimed)
        .unwrap();
    let spawned = control_events
        .iter()
        .find(|event| event.kind == EventKind::ThreadSpawned)
        .expect("recovery should create and witness one child");
    let spawned_payload: ThreadSpawnedPayload =
        serde_json::from_value(spawned.payload.clone()).unwrap();
    assert_eq!(spawned_payload.fork.unwrap().claim_event_id, Some(claim.id));
    let settle = control_events
        .iter()
        .find(|event| event.kind == EventKind::IoIngressSettled)
        .expect("recovery should settle the fork claim");
    assert_eq!(settle.payload["settled_by"], "recovery");
    assert_eq!(
        settle.payload["evidence_event_id"].as_str(),
        Some(spawned.id.to_string().as_str())
    );
    assert_eq!(route_bindings(&restarted_bridge).await.len(), 2);
    let child_coordinates = ThreadCoordinates {
        tenant_id: parent_coordinates.tenant_id.clone(),
        user_id: parent_coordinates.user_id.clone(),
        session_id: parent_coordinates.session_id.clone(),
        thread_id: spawned_payload.child_thread_id,
    };
    assert_eq!(
        thread_events_for(&session_store_path, &child_coordinates)
            .await
            .iter()
            .filter(|event| event.kind == EventKind::TurnSubmitted)
            .count(),
        1
    );
    assert!(queue.completed().await);
    let _ = std::fs::remove_dir_all(fixture_root);
}

async fn append_raw_legacy_fork_claim(
    session_store_path: &Path,
    coordinates: &ThreadCoordinates,
    ingress_envelope_id: &str,
    settled: bool,
) -> EventRecord {
    let store = SqliteSessionStore::open(session_store_path).await.unwrap();
    let control_stream = control_stream_id(coordinates);
    let ingress_witness_event_id = EventRecordId::new();
    let admission_event_id = EventRecordId::new();
    let claim = NewEventRecord::discharged(
        coordinates.clone(),
        EventKind::IoIngressClaimed,
        serde_json::json!({
            "ingress_envelope_ids": [ingress_envelope_id],
            "ingress_witness_event_ids": [ingress_witness_event_id],
            "admission_event_id": admission_event_id,
            "intent": {
                "outcome": "fork",
                "child_key": "legacy-child-turn",
                "input_digest": "sha256:legacy-input"
            }
        }),
        ingress_claim_provenance(
            &control_stream,
            &[ingress_witness_event_id],
            admission_event_id,
        ),
    );
    let claim_id = claim.id;
    let mut records = vec![claim];
    if settled {
        records.push(NewEventRecord::discharged(
            coordinates.clone(),
            EventKind::IoIngressSettled,
            serde_json::json!({
                "claim_event_id": claim_id,
                "ingress_envelope_ids": [ingress_envelope_id],
                "settled_by": "recovery"
            }),
            ingress_settle_provenance(&control_stream, coordinates, claim_id, None),
        ));
    }
    store
        .append_events(&control_stream, records)
        .await
        .unwrap()
        .remove(0)
}

#[tokio::test(start_paused = true)]
async fn settled_legacy_fork_claim_does_not_poison_new_scope_envelopes() {
    let fixture_root = test_root("settled-legacy-fork-claim-scope");
    let egress_db = fixture_root.join("io.sqlite");
    let (server, bridge, _rx) = test_bridge_at_root(&fixture_root).await;
    let session_store_path = server.session_store_path().to_path_buf();
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    bridge
        .submit_envelope(telegram_queue_envelope_with_update(
            "seed legacy claim scope",
            "6100",
        ))
        .await
        .unwrap();
    let coordinates = only_thread_coordinates(&bridge).await;
    append_raw_legacy_fork_claim(
        &session_store_path,
        &coordinates,
        "rc6-settled-fork-envelope",
        true,
    )
    .await;

    let envelope = telegram_queue_envelope_with_update("new work after rc6 upgrade", "6101");
    let ingress_id = envelope.id.clone();
    let queue = Arc::new(ScriptedIngressQueue::new(
        "message-after-settled-legacy-fork",
        envelope,
        std::iter::empty::<&str>(),
    ));
    let worker = CooldisDaemonQueueWorker::new(
        queue.clone(),
        bridge.clone(),
        "worker-after-settled-legacy-fork",
        30,
    );
    assert_eq!(worker.drain_once().await.unwrap(), 1);
    assert!(queue.completed().await);

    let events = control_events_for(&session_store_path, &coordinates).await;
    let new_claim = events
        .iter()
        .find(|event| {
            event.kind == EventKind::IoIngressClaimed
                && event.payload["ingress_envelope_ids"]
                    .as_array()
                    .is_some_and(|ids| ids.iter().any(|id| id.as_str() == Some(&ingress_id)))
        })
        .expect("the new envelope should claim normally beside legacy history");
    assert!(events.iter().any(|event| {
        event.kind == EventKind::IoIngressSettled
            && event.payload["claim_event_id"].as_str() == Some(new_claim.id.to_string().as_str())
    }));
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test(start_paused = true)]
async fn unsettled_legacy_fork_claim_errors_only_its_own_envelope() {
    let fixture_root = test_root("unsettled-legacy-fork-claim-scope");
    let egress_db = fixture_root.join("io.sqlite");
    let (server, bridge, _rx) = test_bridge_at_root(&fixture_root).await;
    let session_store_path = server.session_store_path().to_path_buf();
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    bridge
        .submit_envelope(telegram_queue_envelope_with_update(
            "seed unsettled legacy scope",
            "6200",
        ))
        .await
        .unwrap();
    let coordinates = only_thread_coordinates(&bridge).await;

    let legacy_envelope = telegram_queue_envelope_with_update("legacy redelivery", "6201")
        .with_metadata("cooldis_route_policy", "fork_on_new_dm");
    append_raw_legacy_fork_claim(
        &session_store_path,
        &coordinates,
        &legacy_envelope.id,
        false,
    )
    .await;
    let legacy_queue = Arc::new(ScriptedIngressQueue::new(
        "message-unsettled-legacy-fork",
        legacy_envelope,
        std::iter::empty::<&str>(),
    ));
    legacy_queue.state.lock().await.attempt = 1;
    let legacy_worker = CooldisDaemonQueueWorker::new(
        legacy_queue.clone(),
        bridge.clone(),
        "worker-unsettled-legacy-fork",
        30,
    );
    let err = legacy_worker.drain_once().await.unwrap_err();
    assert!(
        err.to_string()
            .contains("predates reservation-before-creation and cannot be recovered"),
        "unexpected legacy recovery error: {err}"
    );
    assert!(!legacy_queue.completed().await);

    let fresh_envelope = telegram_queue_envelope_with_update("fresh work beside legacy", "6202");
    let fresh_ingress_id = fresh_envelope.id.clone();
    let fresh_queue = Arc::new(ScriptedIngressQueue::new(
        "message-beside-unsettled-legacy-fork",
        fresh_envelope,
        std::iter::empty::<&str>(),
    ));
    let fresh_worker = CooldisDaemonQueueWorker::new(
        fresh_queue.clone(),
        bridge.clone(),
        "worker-beside-unsettled-legacy-fork",
        30,
    );
    assert_eq!(fresh_worker.drain_once().await.unwrap(), 1);
    assert!(fresh_queue.completed().await);
    let events = control_events_for(&session_store_path, &coordinates).await;
    let fresh_claim = events
        .iter()
        .find(|event| {
            event.kind == EventKind::IoIngressClaimed
                && event.payload["ingress_envelope_ids"]
                    .as_array()
                    .is_some_and(|ids| ids.iter().any(|id| id.as_str() == Some(&fresh_ingress_id)))
        })
        .expect("a different envelope should not be poisoned by the legacy claim");
    assert!(events.iter().any(|event| {
        event.kind == EventKind::IoIngressSettled
            && event.payload["claim_event_id"].as_str() == Some(fresh_claim.id.to_string().as_str())
    }));
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test(start_paused = true)]
async fn fork_creation_before_spawn_recovers_the_reserved_child_after_restart() {
    let fixture_root = test_root("fork-creation-before-spawn-cut");
    let egress_db = fixture_root.join("io.sqlite");
    let (server, bridge, _rx) = test_bridge_at_root(&fixture_root).await;
    let session_store_path = server.session_store_path().to_path_buf();
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    let envelope = telegram_queue_envelope("recover reserved fork child")
        .with_metadata("cooldis_route_policy", "fork_on_new_dm");
    let queue = Arc::new(ScriptedIngressQueue::new(
        "message-fork-creation-cut",
        envelope,
        std::iter::empty::<&str>(),
    ));
    bridge
        .pause_after_fork_creation
        .store(true, Ordering::SeqCst);
    let creation_paused = bridge.fork_creation_paused.notified();
    let worker = CooldisDaemonQueueWorker::new(
        queue.clone(),
        bridge.clone(),
        "worker-before-fork-creation-cut",
        30,
    );
    let drain = tokio::spawn(async move { worker.drain_once().await });
    creation_paused.await;

    let parent_coordinates = route_bindings(&bridge).await[0].coordinates.clone();
    let control_events = control_events_for(&session_store_path, &parent_coordinates).await;
    let claim = control_events
        .iter()
        .find(|event| event.kind == EventKind::IoIngressClaimed)
        .expect("the reservation claim must precede creation");
    let claim_payload: IoIngressClaimedPayload =
        serde_json::from_value(claim.payload.clone()).unwrap();
    let reserved_child_thread_id = match claim_payload.intent {
        IngressOutcomeIntent::Fork {
            child_thread_id, ..
        } => child_thread_id.expect("new fork claims must reserve their child id"),
        other => panic!("unexpected claim intent: {other:?}"),
    };
    assert!(
        !control_events
            .iter()
            .any(|event| event.kind == EventKind::ThreadSpawned),
        "the cut must land before thread.spawned"
    );
    let child_coordinates = ThreadCoordinates {
        tenant_id: parent_coordinates.tenant_id.clone(),
        user_id: parent_coordinates.user_id.clone(),
        session_id: parent_coordinates.session_id.clone(),
        thread_id: reserved_child_thread_id,
    };
    let child_events = thread_events_for(&session_store_path, &child_coordinates).await;
    assert_eq!(
        child_events
            .iter()
            .filter(|event| {
                event.kind == EventKind::SessionEntryAppended
                    && event.payload["runtime_kind"].as_str() == Some("thread_started")
                    && event.payload["runtime_payload"]["metadata"]["forked_from_thread_id"]
                        .as_str()
                        .is_some_and(|id| id == parent_coordinates.thread_id.to_string())
            })
            .count(),
        1,
        "the child must exist durably inside the creation-before-spawn window"
    );

    drain.abort();
    assert!(drain.await.unwrap_err().is_cancelled());
    drop(bridge);
    drop(server);
    tokio::time::advance(Duration::from_secs(30)).await;

    let (_server, restarted_bridge, _rx) = restarted_bridge_at_root(&fixture_root).await;
    register_route_state(
        &restarted_bridge,
        &route_with_egress(Vec::new(), None),
        &egress_db,
    )
    .await;
    let restarted_worker = CooldisDaemonQueueWorker::new(
        queue.clone(),
        restarted_bridge.clone(),
        "worker-after-fork-creation-cut",
        30,
    );
    assert_eq!(restarted_worker.drain_once().await.unwrap(), 1);

    let control_events = control_events_for(&session_store_path, &parent_coordinates).await;
    let spawned = control_events
        .iter()
        .filter(|event| event.kind == EventKind::ThreadSpawned)
        .collect::<Vec<_>>();
    assert_eq!(spawned.len(), 1, "recovery must join the topology once");
    let spawned_payload: ThreadSpawnedPayload =
        serde_json::from_value(spawned[0].payload.clone()).unwrap();
    assert_eq!(spawned_payload.child_thread_id, reserved_child_thread_id);
    assert_eq!(spawned_payload.fork.unwrap().claim_event_id, Some(claim.id));
    assert_eq!(
        thread_events_for(&session_store_path, &child_coordinates)
            .await
            .iter()
            .filter(|event| {
                event.kind == EventKind::SessionEntryAppended
                    && event.payload["runtime_kind"].as_str() == Some("thread_started")
                    && event.payload["runtime_payload"]["metadata"]["forked_from_thread_id"]
                        .as_str()
                        .is_some_and(|id| id == parent_coordinates.thread_id.to_string())
            })
            .count(),
        1,
        "recovery must adopt rather than recreate the reserved child"
    );
    assert!(queue.completed().await);
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test(start_paused = true)]
async fn fork_spawn_before_settle_recovers_binding_and_submit_after_restart() {
    let fixture_root = test_root("fork-spawn-before-settle-cut");
    let egress_db = fixture_root.join("io.sqlite");
    let runtime_config = AgentLoopConfig::new(
        ProviderApi::Other(APP_SERVER_LOCAL_PROVIDER.to_string()),
        APP_SERVER_LOCAL_PROVIDER,
        APP_SERVER_LOCAL_MODEL,
    );
    let bridge = bridge_with_runtime_factory_at_root(
        &fixture_root,
        Arc::new(AgentLoopFactory::new(
            runtime_config.clone(),
            Arc::new(RecordingRouteProviderClient::default()),
        )),
    )
    .await;
    let session_store_path = bridge.session_store_path.clone().unwrap();
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    let envelope = telegram_queue_envelope("recover spawned fork")
        .with_metadata("cooldis_route_policy", "fork_on_new_dm");
    let queue = Arc::new(ScriptedIngressQueue::new(
        "message-fork-spawn-cut",
        envelope,
        std::iter::empty::<&str>(),
    ));
    bridge.pause_after_fork_spawn.store(true, Ordering::SeqCst);
    let spawn_paused = bridge.fork_spawn_paused.notified();
    let worker = CooldisDaemonQueueWorker::new(
        queue.clone(),
        bridge.clone(),
        "worker-before-fork-spawn-cut",
        30,
    );
    let drain = tokio::spawn(async move { worker.drain_once().await });
    spawn_paused.await;

    let parent_coordinates = only_thread_coordinates(&bridge).await;
    let control_events = control_events_for(&session_store_path, &parent_coordinates).await;
    let claim = control_events
        .iter()
        .find(|event| event.kind == EventKind::IoIngressClaimed)
        .expect("claim should exist before the fork effects");
    let spawned = control_events
        .iter()
        .find(|event| event.kind == EventKind::ThreadSpawned)
        .expect("the cut should land after thread.spawned");
    let spawned_payload: ThreadSpawnedPayload =
        serde_json::from_value(spawned.payload.clone()).unwrap();
    assert_eq!(
        spawned_payload.fork.as_ref().unwrap().claim_event_id,
        Some(claim.id)
    );
    assert!(
        !control_events
            .iter()
            .any(|event| event.kind == EventKind::IoIngressSettled)
    );
    let children = bridge
        .supervisor
        .children_of_at(&parent_coordinates)
        .await
        .unwrap();
    assert_eq!(children.len(), 1);
    let child_coordinates = children[0].context().coordinates.clone();
    assert_eq!(child_coordinates.thread_id, spawned_payload.child_thread_id);
    assert!(
        !thread_events_for(&session_store_path, &child_coordinates)
            .await
            .iter()
            .any(|event| event.kind == EventKind::TurnSubmitted)
    );
    assert_eq!(route_bindings(&bridge).await.len(), 1);

    drain.abort();
    assert!(drain.await.unwrap_err().is_cancelled());
    drop(bridge);
    tokio::time::advance(Duration::from_secs(30)).await;

    let restarted_bridge = bridge_with_runtime_factory_at_root(
        &fixture_root,
        Arc::new(AgentLoopFactory::new(
            runtime_config,
            Arc::new(RecordingRouteProviderClient::default()),
        )),
    )
    .await;
    register_route_state(
        &restarted_bridge,
        &route_with_egress(Vec::new(), None),
        &egress_db,
    )
    .await;
    let restarted_worker = CooldisDaemonQueueWorker::new(
        queue.clone(),
        restarted_bridge.clone(),
        "worker-after-fork-spawn-cut",
        30,
    );
    assert_eq!(restarted_worker.drain_once().await.unwrap(), 1);

    let control_events = control_events_for(&session_store_path, &parent_coordinates).await;
    assert_eq!(
        control_events
            .iter()
            .filter(|event| event.kind == EventKind::ThreadSpawned)
            .count(),
        1,
        "recovery must reuse the child named by thread.spawned"
    );
    let settle = control_events
        .iter()
        .find(|event| event.kind == EventKind::IoIngressSettled)
        .expect("recovery should settle the existing spawned child");
    assert_eq!(settle.payload["settled_by"], "recovery");
    assert_eq!(
        settle.payload["evidence_event_id"].as_str(),
        Some(spawned.id.to_string().as_str())
    );
    assert_eq!(route_bindings(&restarted_bridge).await.len(), 2);
    let resolved = restarted_bridge
        .resolve_target(&telegram_queue_envelope("binding probe"))
        .await
        .unwrap();
    assert_eq!(
        restarted_bridge
            .resolved_target_coordinates(&resolved)
            .await
            .unwrap()
            .unwrap()
            .thread_id,
        child_coordinates.thread_id
    );
    assert_eq!(
        thread_events_for(&session_store_path, &child_coordinates)
            .await
            .iter()
            .filter(|event| event.kind == EventKind::TurnSubmitted)
            .count(),
        1
    );
    assert!(queue.completed().await);
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test]
async fn queued_interrupt_claims_before_cancel_and_settles_replacement() {
    let fixture_root = test_root("queue-interrupt-outcome");
    let (server, bridge, _rx) = test_bridge_at_root(&fixture_root).await;
    let session_store_path = server.session_store_path().to_path_buf();
    let egress_db = fixture_root.join("io.sqlite");
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    let envelope = telegram_queue_envelope("interrupt with replacement")
        .with_metadata("cooldis_route_policy", "interrupt_on_new_dm");
    let ingress_id = envelope.id.clone();
    let queue = Arc::new(ScriptedIngressQueue::new(
        "message-interrupt-outcome",
        envelope,
        std::iter::empty::<&str>(),
    ));
    let worker = CooldisDaemonQueueWorker::new(
        queue.clone(),
        bridge.clone(),
        "worker-interrupt-outcome",
        30,
    );

    assert_eq!(worker.drain_once().await.unwrap(), 1);
    assert!(queue.completed().await);
    let coordinates = only_thread_coordinates(&bridge).await;
    let control_events = control_events_for(&session_store_path, &coordinates).await;
    let claim = control_events
        .iter()
        .find(|event| event.kind == EventKind::IoIngressClaimed)
        .unwrap();
    let claim_payload =
        serde_json::from_value::<IoIngressClaimedPayload>(claim.payload.clone()).unwrap();
    let replacement_turn_id = match claim_payload.intent {
        IngressOutcomeIntent::Interrupt {
            replacement_turn_id: Some(turn_id),
            ..
        } => turn_id,
        other => panic!("unexpected interrupt claim intent: {other:?}"),
    };
    assert_eq!(claim_payload.ingress_envelope_ids, vec![ingress_id]);
    let settle = control_events
        .iter()
        .find(|event| event.kind == EventKind::IoIngressSettled)
        .unwrap();
    let settle_payload =
        serde_json::from_value::<IoIngressSettledPayload>(settle.payload.clone()).unwrap();
    assert_eq!(settle_payload.claim_event_id, claim.id);
    assert!(settle_payload.evidence_event_id.is_some());
    assert!(
        thread_events_for(&session_store_path, &coordinates)
            .await
            .iter()
            .any(|event| {
                event.kind == EventKind::SessionEntryAppended
                    && event.payload["turn_id"].as_str() == Some(&replacement_turn_id)
            })
    );
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test(start_paused = true)]
async fn queue_worker_rejection_before_submission_does_not_mark_ingress_applied() {
    let fixture_root = test_root("queue-submit-rejection");
    let bridge = bridge_with_execution_policy(
        &fixture_root,
        RuntimeExecutionPolicy::default().with_max_pending_inputs(0),
    )
    .await;
    let session_store_path = bridge.session_store_path.clone().unwrap();
    let egress_db = fixture_root.join("io.sqlite");
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    let envelope = telegram_queue_envelope("reject before durable apply");
    let ingress_id = envelope.id.clone();
    let queue = Arc::new(ScriptedIngressQueue::new(
        "message-submit-rejection",
        envelope,
        std::iter::empty::<&str>(),
    ));
    let worker =
        CooldisDaemonQueueWorker::new(queue.clone(), bridge.clone(), "worker-submit-rejection", 30);

    let first = worker.drain_once().await.unwrap_err();
    assert!(first.to_string().contains("max pending input count is 0"));
    assert_eq!(queue.retry_calls().await, 1);
    let coordinates = only_thread_coordinates(&bridge).await;
    assert!(
        !control_events_for(&session_store_path, &coordinates)
            .await
            .iter()
            .any(|event| {
                event.kind == EventKind::IoIngressClaimed
                    && event.payload["ingress_envelope_ids"]
                        .as_array()
                        .is_some_and(|ids| ids.iter().any(|id| id.as_str() == Some(&ingress_id)))
            })
    );
    assert!(
        bridge.active_turns.lock().unwrap().is_empty(),
        "a rejected submission must not remain active in bridge state"
    );

    tokio::time::advance(Duration::from_secs(30)).await;
    let second = worker.drain_once().await.unwrap_err();
    assert!(second.to_string().contains("max pending input count is 0"));
    assert_eq!(queue.retry_calls().await, 2);
    assert!(!queue.completed().await);
    assert_eq!(queue.complete_calls().await, 0);
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test]
async fn fork_worker_rejection_keeps_one_claimed_child_for_recovery() {
    let fixture_root = test_root("fork-submit-rejection");
    let bridge = bridge_with_execution_policy(
        &fixture_root,
        RuntimeExecutionPolicy::default().with_max_pending_inputs(0),
    )
    .await;
    let session_store_path = bridge.session_store_path.clone().unwrap();
    let egress_db = fixture_root.join("io.sqlite");
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    let envelope = telegram_queue_envelope("fork rejection")
        .with_metadata("cooldis_route_policy", "fork_on_new_dm");
    let ingress_id = envelope.id.clone();
    let queue = Arc::new(ScriptedIngressQueue::new(
        "message-fork-rejection",
        envelope,
        std::iter::empty::<&str>(),
    ));
    let worker =
        CooldisDaemonQueueWorker::new(queue.clone(), bridge.clone(), "worker-fork-rejection", 30);

    let first = worker.drain_once().await.unwrap_err();
    assert!(first.to_string().contains("max pending input count is 0"));
    let child_coordinates = only_thread_coordinates(&bridge).await;
    assert!(
        thread_events_for(&session_store_path, &child_coordinates)
            .await
            .iter()
            .any(|event| {
                event.kind == EventKind::TurnSubmitted
                    && event.payload["ingress_envelope_id"].as_str() == Some(&ingress_id)
            })
    );
    assert_eq!(route_bindings(&bridge).await.len(), 2);
    let child = bridge
        .supervisor
        .get_thread_at(&child_coordinates)
        .await
        .unwrap();
    let parent_coordinates = ThreadCoordinates {
        tenant_id: child_coordinates.tenant_id.clone(),
        user_id: child_coordinates.user_id.clone(),
        session_id: child_coordinates.session_id.clone(),
        thread_id: child.context().parent_thread_id.unwrap(),
    };
    let control_events = control_events_for(&session_store_path, &parent_coordinates).await;
    assert_eq!(
        control_events
            .iter()
            .filter(|event| event.kind == EventKind::IoIngressClaimed)
            .count(),
        1
    );
    assert_eq!(
        control_events
            .iter()
            .filter(|event| event.kind == EventKind::ThreadSpawned)
            .count(),
        1
    );
    assert!(
        !control_events
            .iter()
            .any(|event| event.kind == EventKind::IoIngressSettled)
    );
    assert!(
        bridge.active_turns.lock().unwrap().is_empty(),
        "a rejected fork turn must not remain active in bridge state"
    );
    assert!(!queue.completed().await);
    assert_eq!(queue.complete_calls().await, 0);
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test]
async fn interrupt_cancel_wait_does_not_hold_active_turn_state_lock() {
    let fixture_root = test_root("interrupt-active-turn-lock");
    let (bridge, runtime) = bridge_with_unresponsive_runtime(&fixture_root).await;
    bridge
        .submit_envelope(test_envelope("active turn"))
        .await
        .unwrap();
    runtime.running.notified().await;

    let coordinates = only_thread_coordinates(&bridge).await;
    let handle = bridge.supervisor.get_thread_at(&coordinates).await.unwrap();
    let signal_before_interrupt = handle.lifecycle_record().await.latest_signal_id;
    let interrupt_bridge = bridge.clone();
    let interrupt = tokio::spawn(async move {
        interrupt_bridge
            .submit_envelope(
                test_envelope("replacement")
                    .with_metadata("cooldis_route_policy", "interrupt_on_new_dm"),
            )
            .await
    });

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if handle.lifecycle_record().await.latest_signal_id != signal_before_interrupt {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("interrupt should reach its cancellation grace wait");

    let target = bridge
        .resolve_target(&test_envelope("state probe"))
        .await
        .unwrap();
    let state_read =
        tokio::time::timeout(Duration::from_secs(1), bridge.ingress_state(&target)).await;
    assert!(
        state_read.is_ok(),
        "an interrupt waiting for cancellation grace must not block unrelated active-turn reads"
    );

    interrupt.abort();
    let _ = interrupt.await;
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[test]
fn ingress_outcome_fold_rejects_conflicting_claims() {
    let coordinates = ThreadCoordinates::new("tenant", "user", "session");
    let stream_id = control_stream_id(&coordinates);
    let admission_event_id = EventRecordId::new();
    let claim = |sequence: i64, turn_id: &str, envelope_ids: &[&str]| {
        EventRecord::from_new(
            stream_id.clone(),
            EventSequence::new(sequence),
            NewEventRecord::witnessed(
                coordinates.clone(),
                EventKind::IoIngressClaimed,
                serde_json::to_value(IoIngressClaimedPayload {
                    ingress_envelope_ids: envelope_ids.iter().map(|id| id.to_string()).collect(),
                    ingress_witness_event_ids: vec![EventRecordId::new()],
                    admission_event_id,
                    intent: IngressOutcomeIntent::Turn {
                        turn_id: turn_id.to_string(),
                        submission_mode: "queue".to_string(),
                        input_digest: "sha256:input".to_string(),
                    },
                })
                .unwrap(),
            ),
        )
    };
    let claims = vec![
        claim(1, "turn-first", &["duplicate", "message-a"]),
        claim(2, "turn-second", &["duplicate", "message-b"]),
    ];

    let err = ingress_outcome_fold(&claims, &["duplicate".to_string()]).unwrap_err();
    assert!(err.to_string().contains("more than one claim"));
    let err = ingress_outcome_fold(
        &claims[..1],
        &["message-a".to_string(), "message-missing".to_string()],
    )
    .unwrap_err();
    assert!(err.to_string().contains("partially overlaps"));
}

#[tokio::test]
async fn lone_effect_free_claims_fail_closed_during_recovery() {
    let root = test_root("effect-free-claim-corruption");
    let (server, bridge, _rx) = test_bridge_at_root(&root).await;
    let envelope = observe_only_envelope("seed effect-free recovery target");
    bridge.submit_envelope(envelope.clone()).await.unwrap();
    let target = bridge.resolve_target(&envelope).await.unwrap();
    let coordinates = only_thread_coordinates(&bridge).await;
    let stream_id = control_stream_id(&coordinates);

    for (index, intent) in [
        IngressOutcomeIntent::Observe {
            reason: "observe corruption".to_string(),
        },
        IngressOutcomeIntent::Reject {
            reason: "reject corruption".to_string(),
        },
    ]
    .into_iter()
    .enumerate()
    {
        let payload = IoIngressClaimedPayload {
            ingress_envelope_ids: vec![envelope.id.clone()],
            ingress_witness_event_ids: vec![EventRecordId::new()],
            admission_event_id: EventRecordId::new(),
            intent,
        };
        let claim = EventRecord::from_new(
            stream_id.clone(),
            EventSequence::new(index as i64 + 1),
            NewEventRecord::witnessed(
                coordinates.clone(),
                EventKind::IoIngressClaimed,
                serde_json::to_value(&payload).unwrap(),
            ),
        );
        let err = bridge
            .recover_ingress_outcome(
                &envelope,
                &target,
                IngressOutcomeState::Claimed { claim, payload },
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("effect-free ingress claim is missing its atomic settle")
        );
    }

    drop(server);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(start_paused = true)]
async fn non_fork_claim_owner_survives_fork_rebind_before_redelivery() {
    let fixture_root = test_root("non-fork-claim-owner-rebind");
    let egress_db = fixture_root.join("io.sqlite");
    let (server, bridge, _rx) = test_bridge_at_root(&fixture_root).await;
    let session_store_path = server.session_store_path().to_path_buf();
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    bridge
        .submit_envelope(test_envelope("seed the owning parent"))
        .await
        .unwrap();
    let parent = only_thread_coordinates(&bridge).await;

    let envelope = telegram_queue_envelope_with_update("recover on the owner", "40101");
    let ingress_id = envelope.id.clone();
    let queue = Arc::new(ScriptedIngressQueue::new(
        "message-owner-rebind",
        envelope,
        std::iter::empty::<&str>(),
    ));
    bridge
        .pause_after_ingress_claim
        .store(true, Ordering::SeqCst);
    let claim_paused = bridge.ingress_claim_paused.notified();
    let worker = CooldisDaemonQueueWorker::new(
        queue.clone(),
        bridge.clone(),
        "worker-before-owner-rebind",
        30,
    );
    let drain = tokio::spawn(async move { worker.drain_once().await });
    claim_paused.await;

    let parent_events = control_events_for(&session_store_path, &parent).await;
    let claim = parent_events
        .iter()
        .find(|event| {
            event.kind == EventKind::IoIngressClaimed
                && event.payload["ingress_envelope_ids"]
                    .as_array()
                    .is_some_and(|ids| ids.iter().any(|id| id.as_str() == Some(&ingress_id)))
        })
        .expect("the parent must own the claim before the process-death cut")
        .clone();
    assert!(!parent_events.iter().any(|event| {
        event.kind == EventKind::IoIngressSettled
            && event.payload["claim_event_id"].as_str() == Some(claim.id.to_string().as_str())
    }));

    drain.abort();
    assert!(drain.await.unwrap_err().is_cancelled());
    queue
        .retry_ingress("message-owner-rebind", "injected process death")
        .await
        .unwrap();
    drop(bridge);
    drop(server);

    let (_server, restarted, _rx) = restarted_bridge_at_root(&fixture_root).await;
    register_route_state(&restarted, &route_with_egress(Vec::new(), None), &egress_db).await;
    let fork = telegram_queue_envelope_with_update("rebind to a child", "40102")
        .with_metadata("cooldis_route_policy", "fork_on_new_dm");
    restarted.submit_queued_envelope(fork, 1).await.unwrap();
    let child = only_thread_coordinates(&restarted).await;
    assert_ne!(child.thread_id, parent.thread_id);

    let worker = CooldisDaemonQueueWorker::new(
        queue.clone(),
        restarted.clone(),
        "worker-after-owner-rebind",
        30,
    );
    assert_eq!(worker.drain_once().await.unwrap(), 1);

    let parent_events = control_events_for(&session_store_path, &parent).await;
    let settle = parent_events
        .iter()
        .find(|event| {
            event.kind == EventKind::IoIngressSettled
                && event.payload["claim_event_id"].as_str() == Some(claim.id.to_string().as_str())
        })
        .expect("redelivery must settle the claim on its owning parent stream");
    assert_eq!(settle.payload["settled_by"].as_str(), Some("recovery"));
    let child_control = control_events_for(&session_store_path, &child).await;
    assert!(!child_control.iter().any(|event| {
        event.kind == EventKind::IoIngressClaimed
            && event.payload["ingress_envelope_ids"]
                .as_array()
                .is_some_and(|ids| ids.iter().any(|id| id.as_str() == Some(&ingress_id)))
    }));
    assert!(
        !thread_events_for(&session_store_path, &child)
            .await
            .iter()
            .any(|event| {
                event.kind == EventKind::TurnSubmitted
                    && event.payload["ingress_envelope_id"].as_str() == Some(&ingress_id)
            })
    );
    assert!(queue.completed().await);
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test(start_paused = true)]
async fn ownership_tombstone_is_superseded_after_rebind_before_any_claim() {
    let fixture_root = test_root("ownership-tombstone-rebind");
    let egress_db = fixture_root.join("io.sqlite");
    let (server, bridge, _rx) = test_bridge_at_root(&fixture_root).await;
    let session_store_path = server.session_store_path().to_path_buf();
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    bridge
        .submit_envelope(test_envelope("seed the tombstone parent"))
        .await
        .unwrap();
    let parent = only_thread_coordinates(&bridge).await;

    let envelope = telegram_queue_envelope_with_update("survive the ownership cut", "40103");
    let ingress_id = envelope.id.clone();
    let dedupe_key = envelope.dedupe_key.as_ref().unwrap().stable_key();
    let queue = Arc::new(ScriptedIngressQueue::new(
        "message-ownership-cut",
        envelope,
        std::iter::empty::<&str>(),
    ));
    bridge
        .pause_after_ingress_ownership
        .store(true, Ordering::SeqCst);
    let ownership_paused = bridge.ingress_ownership_paused.notified();
    let worker = CooldisDaemonQueueWorker::new(
        queue.clone(),
        bridge.clone(),
        "worker-before-ownership-cut",
        30,
    );
    let drain = tokio::spawn(async move { worker.drain_once().await });
    ownership_paused.await;

    assert!(
        !control_events_for(&session_store_path, &parent)
            .await
            .iter()
            .any(|event| {
                event.kind == EventKind::IoIngressClaimed
                    && event.payload["ingress_envelope_ids"]
                        .as_array()
                        .is_some_and(|ids| ids.iter().any(|id| id.as_str() == Some(&ingress_id)))
            })
    );
    let connection = rusqlite::Connection::open(&egress_db).unwrap();
    let owner_stream: String = connection
        .query_row(
            "SELECT stream_id FROM cooldis_daemon_ingress_ownership WHERE dedupe_key = ?1",
            params![dedupe_key],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(owner_stream, control_stream_id(&parent).to_string());
    drop(connection);

    drain.abort();
    assert!(drain.await.unwrap_err().is_cancelled());
    queue
        .retry_ingress("message-ownership-cut", "injected process death")
        .await
        .unwrap();
    drop(bridge);
    drop(server);

    let (_server, restarted, _rx) = restarted_bridge_at_root(&fixture_root).await;
    register_route_state(&restarted, &route_with_egress(Vec::new(), None), &egress_db).await;
    let fork = telegram_queue_envelope_with_update("move past the tombstone", "40104")
        .with_metadata("cooldis_route_policy", "fork_on_new_dm");
    restarted.submit_queued_envelope(fork, 1).await.unwrap();
    let child = only_thread_coordinates(&restarted).await;
    assert_ne!(child.thread_id, parent.thread_id);

    let worker = CooldisDaemonQueueWorker::new(
        queue.clone(),
        restarted.clone(),
        "worker-after-ownership-cut",
        30,
    );
    assert_eq!(worker.drain_once().await.unwrap(), 1);

    let bindings = route_bindings(&restarted).await;
    let mut claims = Vec::new();
    for binding in &bindings {
        claims.extend(
            control_events_for(&session_store_path, &binding.coordinates)
                .await
                .into_iter()
                .filter(|event| {
                    event.kind == EventKind::IoIngressClaimed
                        && event.payload["ingress_envelope_ids"]
                            .as_array()
                            .is_some_and(|ids| {
                                ids.iter().any(|id| id.as_str() == Some(&ingress_id))
                            })
                }),
        );
    }
    assert_eq!(
        claims.len(),
        1,
        "the tombstone cut must produce exactly one later claim"
    );
    assert_eq!(claims[0].coordinates.thread_id, child.thread_id);
    let connection = rusqlite::Connection::open(&egress_db).unwrap();
    let mut statement = connection
        .prepare(
            "SELECT stream_id FROM cooldis_daemon_ingress_ownership
             WHERE dedupe_key = ?1 ORDER BY ownership_id",
        )
        .unwrap();
    let owners = statement
        .query_map(params![dedupe_key], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(owners, vec![control_stream_id(&child).to_string()]);
    assert!(queue.completed().await);
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test(start_paused = true)]
async fn claim_committed_before_submit_recovers_original_turn_once_after_restart() {
    let fixture_root = test_root("queue-claim-submit-crash-cut");
    let egress_db = fixture_root.join("io.sqlite");
    let (server, bridge, _rx) = test_bridge_at_root(&fixture_root).await;
    let session_store_path = server.session_store_path().to_path_buf();
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    let envelope = telegram_queue_envelope("recover claimed turn");
    let ingress_id = envelope.id.clone();
    let queue = Arc::new(ScriptedIngressQueue::new(
        "message-claim-cut",
        envelope,
        std::iter::empty::<&str>(),
    ));
    bridge
        .pause_after_ingress_claim
        .store(true, Ordering::SeqCst);
    let claim_paused = bridge.ingress_claim_paused.notified();
    let worker =
        CooldisDaemonQueueWorker::new(queue.clone(), bridge.clone(), "worker-before-claim-cut", 30);
    let drain = tokio::spawn(async move { worker.drain_once().await });
    claim_paused.await;

    let coordinates = only_thread_coordinates(&bridge).await;
    let control_events = control_events_for(&session_store_path, &coordinates).await;
    let claim = control_events
        .iter()
        .find(|event| event.kind == EventKind::IoIngressClaimed)
        .expect("claim should commit before the injected process-death cut");
    let claimed_turn_id = claim.payload["intent"]["turn_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        !control_events
            .iter()
            .any(|event| event.kind == EventKind::IoIngressSettled)
    );
    assert!(
        !thread_events_for(&session_store_path, &coordinates)
            .await
            .iter()
            .any(|event| event.kind == EventKind::TurnSubmitted)
    );

    drain.abort();
    assert!(drain.await.unwrap_err().is_cancelled());
    drop(bridge);
    drop(server);
    tokio::time::advance(Duration::from_secs(30)).await;

    let (_server, restarted_bridge, _rx) = restarted_bridge_at_root(&fixture_root).await;
    register_route_state(
        &restarted_bridge,
        &route_with_egress(Vec::new(), None),
        &egress_db,
    )
    .await;
    let restarted_worker = CooldisDaemonQueueWorker::new(
        queue.clone(),
        restarted_bridge.clone(),
        "worker-after-claim-cut",
        30,
    );
    assert_eq!(restarted_worker.drain_once().await.unwrap(), 1);

    assert!(queue.completed().await);
    let control_events = control_events_for(&session_store_path, &coordinates).await;
    assert_eq!(
        control_events
            .iter()
            .filter(|event| event.kind == EventKind::IoIngressClaimed)
            .count(),
        1
    );
    let settle = control_events
        .iter()
        .find(|event| event.kind == EventKind::IoIngressSettled)
        .expect("redelivery should settle the claim");
    assert_eq!(settle.payload["settled_by"].as_str(), Some("recovery"));
    let thread_events = thread_events_for(&session_store_path, &coordinates).await;
    let submitted = thread_events
        .iter()
        .filter(|event| event.kind == EventKind::TurnSubmitted)
        .collect::<Vec<_>>();
    assert_eq!(submitted.len(), 1);
    assert_eq!(
        submitted[0].payload["turn_id"].as_str(),
        Some(claimed_turn_id.as_str())
    );
    assert_eq!(
        thread_events
            .iter()
            .filter(|event| {
                event.kind == EventKind::SessionEntryAppended
                    && event.payload["turn_id"].as_str() == Some(claimed_turn_id.as_str())
            })
            .count(),
        1,
        "recovered execution must adopt the turn input entry"
    );
    assert_single_durable_ingress_turn(&session_store_path, &coordinates, &ingress_id).await;
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test]
async fn input_persisted_before_compile_recovery_resubmits_and_adopts_entry() {
    let fixture_root = test_root("queue-input-compile-crash-cut");
    let egress_db = fixture_root.join("io.sqlite");
    let state = Arc::new(PersistedInputCutState::default());
    let bridge = bridge_with_runtime_factory_at_root(
        &fixture_root,
        Arc::new(PersistedInputCutRuntimeFactory {
            state: Arc::clone(&state),
        }),
    )
    .await;
    let session_store_path = bridge.session_store_path.clone().unwrap();
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    let envelope = telegram_queue_envelope("recover after persisted input");
    let queue = Arc::new(ScriptedIngressQueue::new(
        "message-input-compile-cut",
        envelope,
        std::iter::empty::<&str>(),
    ));
    let input_persisted = state.input_persisted.notified();
    let worker = CooldisDaemonQueueWorker::new(
        queue.clone(),
        bridge.clone(),
        "worker-before-input-compile-cut",
        30,
    );
    let drain = tokio::spawn(async move { worker.drain_once().await });
    tokio::time::timeout(Duration::from_secs(3), input_persisted)
        .await
        .expect("executing side should reach the input-persisted cut");

    let coordinates = only_thread_coordinates(&bridge).await;
    let control_events = control_events_for(&session_store_path, &coordinates).await;
    let claim = control_events
        .iter()
        .find(|event| event.kind == EventKind::IoIngressClaimed)
        .expect("claim should precede input persistence");
    let claimed_turn_id = claim.payload["intent"]["turn_id"]
        .as_str()
        .unwrap()
        .to_string();
    let thread_events = thread_events_for(&session_store_path, &coordinates).await;
    let input_event = thread_events
        .iter()
        .find(|event| {
            event.kind == EventKind::SessionEntryAppended
                && event.payload["turn_id"].as_str() == Some(&claimed_turn_id)
        })
        .expect("executing side should persist the claimed input");
    let input_event_id = input_event.id;
    assert!(
        !thread_events
            .iter()
            .any(|event| event.kind == EventKind::ContextCompileCompleted)
    );
    assert!(
        !control_events
            .iter()
            .any(|event| event.kind == EventKind::IoIngressSettled)
    );

    drain.abort();
    assert!(drain.await.unwrap_err().is_cancelled());
    drop(bridge);
    queue
        .retry_ingress("message-input-compile-cut", "injected process death")
        .await
        .unwrap();

    let (_server, restarted_bridge, _rx) = restarted_bridge_at_root(&fixture_root).await;
    register_route_state(
        &restarted_bridge,
        &route_with_egress(Vec::new(), None),
        &egress_db,
    )
    .await;
    let restarted_worker = CooldisDaemonQueueWorker::new(
        queue.clone(),
        restarted_bridge,
        "worker-after-input-compile-cut",
        30,
    );
    assert_eq!(restarted_worker.drain_once().await.unwrap(), 1);

    let thread_events = thread_events_for(&session_store_path, &coordinates).await;
    assert_eq!(
        thread_events
            .iter()
            .filter(|event| {
                event.kind == EventKind::SessionEntryAppended
                    && event.payload["turn_id"].as_str() == Some(&claimed_turn_id)
            })
            .count(),
        1,
        "recovery must adopt the persisted turn input"
    );
    assert!(thread_events.iter().any(|event| {
        event.kind == EventKind::ContextCompileCompleted
            && event.payload["turn_id"].as_str() == Some(&claimed_turn_id)
    }));
    assert!(thread_events.iter().any(|event| {
        event.kind == EventKind::TurnCompleted
            && event.payload["turn_id"].as_str() == Some(&claimed_turn_id)
    }));
    let control_events = control_events_for(&session_store_path, &coordinates).await;
    let settle = control_events
        .iter()
        .find(|event| event.kind == EventKind::IoIngressSettled)
        .expect("recovery should settle after turn-trace evidence");
    assert_eq!(settle.payload["settled_by"].as_str(), Some("recovery"));
    let evidence_id = settle.payload["evidence_event_id"].as_str().unwrap();
    assert_ne!(evidence_id, input_event_id.to_string());
    assert!(thread_events.iter().any(|event| {
        event.id.to_string() == evidence_id && event.kind == EventKind::ContextCompileCompleted
    }));
    assert!(queue.completed().await);
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test]
async fn failed_runtime_replacement_sheds_turn_reservation_for_recovery() {
    let fixture_root = test_root("queue-runtime-failure-reservation");
    let egress_db = fixture_root.join("io.sqlite");
    let state = Arc::new(FailOnceRuntimeState::default());
    let runtime_config = AgentLoopConfig::new(
        ProviderApi::Other(APP_SERVER_LOCAL_PROVIDER.to_string()),
        APP_SERVER_LOCAL_PROVIDER,
        APP_SERVER_LOCAL_MODEL,
    );
    let provider_client: Arc<dyn ProviderClient> =
        Arc::new(RecordingRouteProviderClient::default());
    let bridge = bridge_with_runtime_factory_at_root(
        &fixture_root,
        Arc::new(FailOnceThenAgentLoopFactory {
            builds: AtomicUsize::new(0),
            state: Arc::clone(&state),
            provider: AgentLoopFactory::new(runtime_config, provider_client),
        }),
    )
    .await;
    let session_store_path = bridge.session_store_path.clone().unwrap();
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    let queue = Arc::new(ScriptedIngressQueue::new(
        "message-runtime-failure",
        telegram_queue_envelope("recover after runtime failure"),
        std::iter::empty::<&str>(),
    ));
    let failed = state.failed.notified();
    let worker = CooldisDaemonQueueWorker::new(
        queue.clone(),
        bridge.clone(),
        "worker-before-runtime-restart",
        30,
    );
    let drain = tokio::spawn(async move { worker.drain_once().await });
    tokio::time::timeout(Duration::from_secs(3), failed)
        .await
        .expect("runtime should reach the injected failure");

    let coordinates = only_thread_coordinates(&bridge).await;
    let control_events = control_events_for(&session_store_path, &coordinates).await;
    let claim = control_events
        .iter()
        .find(|event| event.kind == EventKind::IoIngressClaimed)
        .unwrap();
    let claimed_turn_id = claim.payload["intent"]["turn_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        !control_events
            .iter()
            .any(|event| event.kind == EventKind::IoIngressSettled)
    );
    drain.abort();
    assert!(drain.await.unwrap_err().is_cancelled());
    assert_eq!(
        bridge
            .supervisor
            .get_thread_at(&coordinates)
            .await
            .unwrap()
            .status(),
        ThreadStatus::Failed,
        "the failed runtime must still be resident at redelivery"
    );
    queue
        .retry_ingress("message-runtime-failure", "runtime redelivery")
        .await
        .unwrap();

    let restarted_worker = CooldisDaemonQueueWorker::new(
        queue.clone(),
        bridge.clone(),
        "worker-after-runtime-restart",
        30,
    );
    assert_eq!(restarted_worker.drain_once().await.unwrap(), 1);

    let control_events = control_events_for(&session_store_path, &coordinates).await;
    let settle = control_events
        .iter()
        .find(|event| event.kind == EventKind::IoIngressSettled)
        .expect("replacement runtime should settle the claim");
    assert_eq!(settle.payload["settled_by"].as_str(), Some("recovery"));
    assert!(
        thread_events_for(&session_store_path, &coordinates)
            .await
            .iter()
            .any(|event| {
                event.kind == EventKind::ContextCompileCompleted
                    && event.payload["turn_id"].as_str() == Some(&claimed_turn_id)
            })
    );
    assert!(queue.completed().await);
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test]
async fn concurrent_lazy_load_of_cyclic_topology_fails_closed_without_lock_deadlock() {
    let fixture_root = test_root("lazy-load-cyclic-topology");
    let bridge = bridge_with_runtime_factory_at_root(
        &fixture_root,
        Arc::new(AgentLoopFactory::new(
            AgentLoopConfig::new(
                ProviderApi::Other(APP_SERVER_LOCAL_PROVIDER.to_string()),
                APP_SERVER_LOCAL_PROVIDER,
                APP_SERVER_LOCAL_MODEL,
            ),
            Arc::new(RecordingRouteProviderClient::default()),
        )),
    )
    .await;
    let store = SqliteSessionStore::open(bridge.session_store_path.as_ref().unwrap())
        .await
        .unwrap();
    let first = ThreadCoordinates {
        tenant_id: bridge.tenant_id.clone(),
        user_id: bridge.user_id.clone(),
        session_id: "cyclic-session".to_string(),
        thread_id: ThreadId::new(),
    };
    let second = ThreadCoordinates {
        thread_id: ThreadId::new(),
        ..first.clone()
    };
    for (coordinates, parent_thread_id) in [(&first, second.thread_id), (&second, first.thread_id)]
    {
        store
            .append(
                coordinates,
                None,
                SessionEntryKind::Runtime {
                    kind: "thread_started".to_string(),
                    payload: json!({
                        "parent_thread_id": parent_thread_id,
                        "topology": ThreadTopology::branch_from(parent_thread_id, None),
                        "metadata": {},
                    }),
                },
            )
            .await
            .unwrap();
    }
    let barrier = Arc::new(Barrier::new(2));
    *bridge.thread_load_root_barrier.lock().unwrap() = Some(barrier);
    let first_bridge = bridge.clone();
    let first_coordinates = first.clone();
    let first_load = tokio::spawn(async move {
        first_bridge
            .get_or_load_thread_handle(&first_coordinates)
            .await
    });
    let second_bridge = bridge.clone();
    let second_coordinates = second.clone();
    let second_load = tokio::spawn(async move {
        second_bridge
            .get_or_load_thread_handle(&second_coordinates)
            .await
    });

    let (first_error, second_error) = tokio::time::timeout(Duration::from_secs(1), async {
        (
            match first_load.await.unwrap() {
                Ok(_) => panic!("first cyclic load unexpectedly succeeded"),
                Err(err) => err,
            },
            match second_load.await.unwrap() {
                Ok(_) => panic!("second cyclic load unexpectedly succeeded"),
                Err(err) => err,
            },
        )
    })
    .await
    .expect("cyclic concurrent loads must fail instead of deadlocking");
    for error in [first_error, second_error] {
        assert!(error.into_inner().to_string().contains("topology cycle"));
    }
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test(start_paused = true)]
async fn queue_worker_restart_after_apply_before_complete_does_not_duplicate_turn() {
    let fixture_root = test_root("queue-apply-crash-cut");
    let egress_db = fixture_root.join("io.sqlite");
    let (server, bridge, _rx) = test_bridge_at_root(&fixture_root).await;
    let session_store_path = server.session_store_path().to_path_buf();
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    let envelope = telegram_queue_envelope("survive apply ack crash cut");
    let ingress_id = envelope.id.clone();
    let queue = Arc::new(ScriptedIngressQueue::new(
        "message-crash-cut",
        envelope,
        std::iter::empty::<&str>(),
    ));
    queue.block_next_complete();
    let worker =
        CooldisDaemonQueueWorker::new(queue.clone(), bridge.clone(), "worker-before-crash", 30);
    let drain = tokio::spawn(async move { worker.drain_once().await });
    queue.wait_for_complete_started().await;
    let original_coordinates = only_thread_coordinates(&bridge).await;
    drain.abort();
    assert!(drain.await.unwrap_err().is_cancelled());
    drop(bridge);
    drop(server);

    tokio::time::advance(Duration::from_secs(30)).await;
    let (_restarted_server, restarted_bridge, _rx) = restarted_bridge_at_root(&fixture_root).await;
    register_route_state(
        &restarted_bridge,
        &route_with_egress(Vec::new(), None),
        &egress_db,
    )
    .await;
    let cold_bindings = restarted_bridge.threads.lock().await.clone();
    assert_eq!(cold_bindings.len(), 1);
    assert_eq!(
        cold_bindings.values().next(),
        Some(&original_coordinates),
        "restart must cold-seed the original durable binding"
    );
    assert!(
        matches!(
            restarted_bridge
                .supervisor
                .get_thread_at(&original_coordinates)
                .await,
            Err(CooldisError::ThreadNotFound(thread_id))
                if thread_id == original_coordinates.thread_id
        ),
        "cold-seeded binding must remain nonresident before redelivery"
    );
    let restarted_worker = CooldisDaemonQueueWorker::new(
        queue.clone(),
        restarted_bridge.clone(),
        "worker-after-crash",
        30,
    );
    assert_eq!(restarted_worker.drain_once().await.unwrap(), 1);

    assert!(queue.completed().await);
    assert_eq!(
        *restarted_bridge.threads.lock().await,
        cold_bindings,
        "dedupe lookup must preserve the cold durable binding"
    );
    assert!(
        matches!(
            restarted_bridge
                .supervisor
                .get_thread_at(&original_coordinates)
                .await,
            Err(CooldisError::ThreadNotFound(thread_id))
                if thread_id == original_coordinates.thread_id
        ),
        "dedupe lookup must complete redelivery without loading a runtime"
    );
    assert_single_durable_ingress_turn(&session_store_path, &original_coordinates, &ingress_id)
        .await;
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test(start_paused = true)]
async fn observe_settled_before_complete_redelivery_appends_nothing() {
    let fixture_root = test_root("observe-apply-complete-crash-cut");
    let egress_db = fixture_root.join("io.sqlite");
    let (server, bridge, _rx) = test_bridge_at_root(&fixture_root).await;
    let session_store_path = server.session_store_path().to_path_buf();
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    let envelope = observe_only_envelope("observe exactly once");
    let ingress_id = envelope.id.clone();
    let queue = Arc::new(ScriptedIngressQueue::new(
        "message-observe-cut",
        envelope,
        std::iter::empty::<&str>(),
    ));
    queue.block_next_complete();
    let worker = CooldisDaemonQueueWorker::new(
        queue.clone(),
        bridge.clone(),
        "worker-before-observe-cut",
        30,
    );
    let drain = tokio::spawn(async move { worker.drain_once().await });
    queue.wait_for_complete_started().await;

    let coordinates = only_thread_coordinates(&bridge).await;
    let before = control_events_for(&session_store_path, &coordinates).await;
    assert_eq!(
        before
            .iter()
            .filter(|event| event.kind == EventKind::IoIngressReceived)
            .count(),
        1,
        "the ingress witness must exist before the process-death cut"
    );
    assert_eq!(
        before
            .iter()
            .filter(|event| event.kind == EventKind::AdmissionDecided)
            .count(),
        1,
        "the observe decision must exist before the process-death cut"
    );
    let claim = before
        .iter()
        .find(|event| event.kind == EventKind::IoIngressClaimed)
        .expect("the observe claim must exist before the process-death cut");
    assert_eq!(claim.payload["intent"]["outcome"].as_str(), Some("observe"));
    let settle = before
        .iter()
        .find(|event| event.kind == EventKind::IoIngressSettled)
        .expect("the observe settle must exist before the process-death cut");
    assert_eq!(
        settle.payload["claim_event_id"].as_str(),
        Some(claim.id.to_string()).as_deref()
    );
    assert!(settle.payload["evidence_event_id"].is_null());
    assert_eq!(settle.payload["settled_by"].as_str(), Some("execution"));
    assert_eq!(
        thread_events_for(&session_store_path, &coordinates)
            .await
            .iter()
            .filter(|event| event.kind == EventKind::TurnSubmitted)
            .count(),
        0
    );
    assert!(!queue.completed().await);

    drain.abort();
    assert!(drain.await.unwrap_err().is_cancelled());
    drop(bridge);
    drop(server);
    tokio::time::advance(Duration::from_secs(30)).await;

    let (_server, restarted_bridge, _rx) = restarted_bridge_at_root(&fixture_root).await;
    register_route_state(
        &restarted_bridge,
        &route_with_egress(Vec::new(), None),
        &egress_db,
    )
    .await;
    let worker = CooldisDaemonQueueWorker::new(
        queue.clone(),
        restarted_bridge,
        "worker-after-observe-cut",
        30,
    );
    assert_eq!(worker.drain_once().await.unwrap(), 1);

    assert!(queue.completed().await);
    assert_eq!(queue.retry_calls().await, 0);
    let after = control_events_for(&session_store_path, &coordinates).await;
    assert_eq!(after, before, "redelivery must append no control event");
    assert!(matches!(
        ingress_outcome_fold(&after, &[ingress_id]).unwrap(),
        IngressOutcomeState::Settled { .. }
    ));
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test(start_paused = true)]
async fn reject_settled_before_complete_redelivery_dedupes_and_completes() {
    let fixture_root = test_root("reject-apply-complete-crash-cut");
    let egress_db = fixture_root.join("io.sqlite");
    let (server, bridge, _rx) = test_bridge_at_root(&fixture_root).await;
    let session_store_path = server.session_store_path().to_path_buf();
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    let envelope = telegram_queue_envelope("reject exactly once")
        .with_metadata("cooldis_route_policy", "reject");
    let ingress_id = envelope.id.clone();
    let queue = Arc::new(ScriptedIngressQueue::new(
        "message-reject-cut",
        envelope,
        std::iter::empty::<&str>(),
    ));
    queue.block_next_complete();
    let worker = CooldisDaemonQueueWorker::new(
        queue.clone(),
        bridge.clone(),
        "worker-before-reject-cut",
        30,
    );
    let drain = tokio::spawn(async move { worker.drain_once().await });
    queue.wait_for_complete_started().await;

    let coordinates = only_thread_coordinates(&bridge).await;
    let before = control_events_for(&session_store_path, &coordinates).await;
    assert_eq!(
        before
            .iter()
            .filter(|event| event.kind == EventKind::IoIngressReceived)
            .count(),
        1
    );
    assert_eq!(
        before
            .iter()
            .filter(|event| event.kind == EventKind::AdmissionDecided)
            .count(),
        1
    );
    let claim = before
        .iter()
        .find(|event| event.kind == EventKind::IoIngressClaimed)
        .expect("the reject claim must exist before the process-death cut");
    assert_eq!(claim.payload["intent"]["outcome"].as_str(), Some("reject"));
    let settle = before
        .iter()
        .find(|event| event.kind == EventKind::IoIngressSettled)
        .expect("the reject settle must exist before the process-death cut");
    assert!(settle.payload["evidence_event_id"].is_null());
    assert_eq!(settle.payload["settled_by"].as_str(), Some("execution"));
    assert!(!queue.completed().await);

    drain.abort();
    assert!(drain.await.unwrap_err().is_cancelled());
    drop(bridge);
    drop(server);
    tokio::time::advance(Duration::from_secs(30)).await;

    let (_server, restarted_bridge, _rx) = restarted_bridge_at_root(&fixture_root).await;
    register_route_state(
        &restarted_bridge,
        &route_with_egress(Vec::new(), None),
        &egress_db,
    )
    .await;
    let worker = CooldisDaemonQueueWorker::new(
        queue.clone(),
        restarted_bridge,
        "worker-after-reject-cut",
        30,
    );
    assert_eq!(worker.drain_once().await.unwrap(), 1);

    assert!(queue.completed().await);
    assert_eq!(queue.retry_calls().await, 0);
    let after = control_events_for(&session_store_path, &coordinates).await;
    assert_eq!(after, before, "redelivery must not re-decide a reject");
    assert!(matches!(
        ingress_outcome_fold(&after, &[ingress_id]).unwrap(),
        IngressOutcomeState::Settled { .. }
    ));
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test(start_paused = true)]
async fn queue_worker_fresh_envelope_marks_applied_and_completes_once() {
    let fixture_root = test_root("queue-fresh-applied");
    let (server, bridge, _rx) = test_bridge_at_root(&fixture_root).await;
    let session_store_path = server.session_store_path().to_path_buf();
    let egress_db = fixture_root.join("io.sqlite");
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    let envelope = telegram_queue_envelope("fresh durable ingress");
    let ingress_id = envelope.id.clone();
    let queue = Arc::new(ScriptedIngressQueue::new(
        "message-fresh",
        envelope,
        std::iter::empty::<&str>(),
    ));
    let worker = CooldisDaemonQueueWorker::new(queue.clone(), bridge.clone(), "worker-fresh", 30);

    assert_eq!(worker.drain_once().await.unwrap(), 1);

    assert!(queue.completed().await);
    assert_eq!(queue.complete_calls().await, 1);
    let coordinates = only_thread_coordinates(&bridge).await;
    assert_single_durable_ingress_turn(&session_store_path, &coordinates, &ingress_id).await;
    let control_events = control_events_for(&session_store_path, &coordinates).await;
    assert!(!control_events.iter().any(|event| {
        event.kind == EventKind::IoIngressReceived
            && event.payload["dedupe_seen"].as_bool() == Some(true)
    }));
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test]
async fn queue_worker_processes_sqlite_backed_envelope() {
    let (bridge, mut rx, _) = test_bridge().await;
    let egress_db = test_root("queue-egress").join("io.sqlite");
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    let db = std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(format!("queue-{}.sqlite", uuid::Uuid::now_v7()));
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "ingress"))
            .await
            .unwrap(),
    );
    queue.submit(test_envelope("hello queue")).await.unwrap();

    let worker = CooldisDaemonQueueWorker::new(queue, bridge, "worker-test", 30);
    assert_eq!(worker.drain_once().await.unwrap(), 1);
    drain_until_egress(&worker.bridge, "telegram.bot", "main", 1).await;

    let egress = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        egress.kind,
        EgressKind::AssistantMessage { ref text } if text.contains("hello queue")
    ));
    let _ = std::fs::remove_file(db);
}

#[tokio::test]
async fn content_policy_observe_only_event_does_not_start_turn() {
    let root = test_root("content-policy-observe");
    let (server, bridge, _rx) = test_bridge_at_root(&root).await;
    let session_store_path = server.session_store_path().to_path_buf();
    let mut route = route_with_egress(Vec::new(), None);
    route.policy = Some("queue_per_conversation".to_string());
    route.content_policies = Some(BTreeMap::from([(
        "external.event".to_string(),
        "observe_only".to_string(),
    )]));
    let sink = RouteIngressSink::new(bridge.direct_sink(), &route);

    let ack = sink.submit(event_envelope("external.event")).await.unwrap();

    assert!(ack.accepted);
    let coordinates = only_thread_coordinates(&bridge).await;
    let control_events = control_events_for(&session_store_path, &coordinates).await;
    let ingress = control_events
        .iter()
        .find(|event| event.kind == crate::EventKind::IoIngressReceived)
        .unwrap();
    assert_eq!(ingress.payload["external_message_id"].as_str(), Some("556"));
    let admission = control_events
        .iter()
        .find(|event| event.kind == crate::EventKind::AdmissionDecided)
        .unwrap();
    assert_eq!(admission.payload["decision"].as_str(), Some("observe"));
    let thread_events = thread_events_for(&session_store_path, &coordinates).await;
    assert_eq!(
        thread_events
            .iter()
            .filter(|event| event.kind == crate::EventKind::TurnSubmitted)
            .count(),
        0
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn content_policy_queue_starts_turn_with_event_payload() {
    let root = test_root("content-policy-queue");
    let (server, bridge, _rx) = test_bridge_at_root(&root).await;
    let session_store_path = server.session_store_path().to_path_buf();
    let mut route = route_with_egress(Vec::new(), None);
    route.policy = Some("observe_only".to_string());
    route.content_policies = Some(BTreeMap::from([(
        "external.event".to_string(),
        "queue_per_conversation".to_string(),
    )]));
    let sink = RouteIngressSink::new(bridge.direct_sink(), &route);

    let ack = sink.submit(event_envelope("external.event")).await.unwrap();

    assert!(ack.accepted);
    let coordinates = only_thread_coordinates(&bridge).await;
    let control_events = control_events_for(&session_store_path, &coordinates).await;
    let admission = control_events
        .iter()
        .find(|event| event.kind == crate::EventKind::AdmissionDecided)
        .unwrap();
    assert_eq!(admission.payload["decision"].as_str(), Some("queue"));
    wait_for_user_text_containing(&bridge, &coordinates, "external.event").await;
    wait_for_user_text_containing(&bridge, &coordinates, "\"message_id\":556").await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn content_policy_no_match_uses_route_default_for_event() {
    let envelopes = Arc::new(TokioMutex::new(Vec::new()));
    let capture = Arc::new(CaptureSink {
        envelopes: envelopes.clone(),
    });
    let mut route = route_with_egress(Vec::new(), None);
    route.policy = Some("queue_per_conversation".to_string());
    route.content_policies = Some(BTreeMap::from([(
        "other.event".to_string(),
        "observe_only".to_string(),
    )]));
    let sink = RouteIngressSink::new(capture, &route);

    sink.submit(event_envelope("external.event")).await.unwrap();

    let captured = envelopes.lock().await;
    assert_eq!(
        captured[0]
            .metadata
            .get("cooldis_route_policy")
            .map(String::as_str),
        Some("queue_per_conversation")
    );
}

#[tokio::test]
async fn content_policy_ignores_spoofed_kind_on_plain_message() {
    let envelopes = Arc::new(TokioMutex::new(Vec::new()));
    let capture = Arc::new(CaptureSink {
        envelopes: envelopes.clone(),
    });
    let mut route = route_with_egress(Vec::new(), None);
    route.policy = Some("queue_per_conversation".to_string());
    route.content_policies = Some(BTreeMap::from([(
        "external.event".to_string(),
        "observe_only".to_string(),
    )]));
    let sink = RouteIngressSink::new(capture, &route);

    sink.submit(
        telegram_queue_envelope("ordinary text mentioning external.event")
            .with_metadata("content_kind", "external.event"),
    )
    .await
    .unwrap();

    let captured = envelopes.lock().await;
    assert_eq!(
        captured[0]
            .metadata
            .get("cooldis_route_policy")
            .map(String::as_str),
        Some("queue_per_conversation")
    );
}

#[tokio::test]
async fn content_policy_observe_only_bypasses_route_coalesce_metadata() {
    let envelopes = Arc::new(TokioMutex::new(Vec::new()));
    let capture = Arc::new(CaptureSink {
        envelopes: envelopes.clone(),
    });
    let mut route = route_with_egress(Vec::new(), None);
    route.policy = Some("coalesce_bursts".to_string());
    route.content_policies = Some(BTreeMap::from([(
        "external.event".to_string(),
        "observe_only".to_string(),
    )]));
    route.coalesce_bursts = Some(CooldisCoalesceBurstsConfig {
        window_ms: 60_000,
        max_batch: 8,
    });
    let sink = RouteIngressSink::new(capture, &route);

    sink.submit(event_envelope("external.event")).await.unwrap();

    let captured = envelopes.lock().await;
    assert_eq!(
        captured[0]
            .metadata
            .get("cooldis_route_policy")
            .map(String::as_str),
        Some("observe_only")
    );
    assert!(!captured[0].metadata.contains_key("cooldis_coalesce_bursts"));
    assert!(
        !captured[0]
            .metadata
            .contains_key("cooldis_coalesce_window_ms")
    );
    assert!(
        !captured[0]
            .metadata
            .contains_key("cooldis_coalesce_max_batch")
    );
}

#[tokio::test]
async fn content_policy_coalesce_stamps_metadata_for_matching_event() {
    let envelopes = Arc::new(TokioMutex::new(Vec::new()));
    let capture = Arc::new(CaptureSink {
        envelopes: envelopes.clone(),
    });
    let mut route = route_with_egress(Vec::new(), None);
    route.policy = Some("observe_only".to_string());
    route.content_policies = Some(BTreeMap::from([(
        "external.event".to_string(),
        "coalesce_bursts".to_string(),
    )]));
    route.coalesce_bursts = Some(CooldisCoalesceBurstsConfig {
        window_ms: 60_000,
        max_batch: 8,
    });
    let sink = RouteIngressSink::new(capture, &route);

    sink.submit(event_envelope("external.event")).await.unwrap();

    let captured = envelopes.lock().await;
    assert_eq!(
        captured[0]
            .metadata
            .get("cooldis_route_policy")
            .map(String::as_str),
        Some("coalesce_bursts")
    );
    assert_eq!(
        captured[0]
            .metadata
            .get("cooldis_coalesce_bursts")
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(
        captured[0]
            .metadata
            .get("cooldis_coalesce_window_ms")
            .map(String::as_str),
        Some("60000")
    );
    assert_eq!(
        captured[0]
            .metadata
            .get("cooldis_coalesce_max_batch")
            .map(String::as_str),
        Some("8")
    );
}

#[tokio::test]
async fn content_policy_reject_rejects_matching_event() {
    let (bridge, _rx, _) = test_bridge().await;
    let mut route = route_with_egress(Vec::new(), None);
    route.policy = Some("queue_per_conversation".to_string());
    route.content_policies = Some(BTreeMap::from([(
        "external.event".to_string(),
        "reject".to_string(),
    )]));
    let sink = RouteIngressSink::new(bridge.direct_sink(), &route);

    let err = sink
        .submit(event_envelope("external.event"))
        .await
        .unwrap_err();

    assert!(matches!(err, IoError::PolicyRejected(reason) if reason == "route policy reject"));
}

#[tokio::test]
async fn content_policy_observe_only_bypasses_route_coalesce_in_queued_lane() {
    let root = test_root("content-policy-queue-observe-coalesce");
    let (server, bridge, _rx) = test_bridge_at_root(&root).await;
    let session_store_path = server.session_store_path().to_path_buf();
    let mut route = route_with_egress(Vec::new(), None);
    route.policy = Some("coalesce_bursts".to_string());
    route.content_policies = Some(BTreeMap::from([(
        "external.event".to_string(),
        "observe_only".to_string(),
    )]));
    route.coalesce_bursts = Some(CooldisCoalesceBurstsConfig {
        window_ms: 60_000,
        max_batch: 8,
    });
    let db = root.join("content-policy-queue.sqlite");
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "ingress"))
            .await
            .unwrap(),
    );
    let sink = RouteIngressSink::new(queue.clone(), &route);

    let ack = sink.submit(event_envelope("external.event")).await.unwrap();

    assert!(ack.accepted);
    let worker = CooldisDaemonQueueWorker::new(queue, bridge.clone(), "content-policy-worker", 30);
    assert_eq!(worker.drain_once().await.unwrap(), 1);

    let coordinates = only_thread_coordinates(&bridge).await;
    let control_events = control_events_for(&session_store_path, &coordinates).await;
    let ingress_index = control_events
        .iter()
        .position(|event| event.kind == crate::EventKind::IoIngressReceived)
        .unwrap();
    let admission_index = control_events
        .iter()
        .position(|event| event.kind == crate::EventKind::AdmissionDecided)
        .unwrap();
    assert!(ingress_index < admission_index);
    let admission = &control_events[admission_index];
    assert_eq!(admission.payload["decision"].as_str(), Some("observe"));
    assert!(
        admission.payload["admissible"]
            .as_array()
            .is_some_and(|admissible| !admissible
                .iter()
                .any(|decision| decision.as_str() == Some("coalesce")))
    );
    assert_eq!(
        admission.payload["source_ingress_event_ids"]
            .as_array()
            .map(Vec::len),
        Some(1)
    );
    let thread_events = thread_events_for(&session_store_path, &coordinates).await;
    assert_eq!(
        thread_events
            .iter()
            .filter(|event| event.kind == crate::EventKind::TurnSubmitted)
            .count(),
        0
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn queue_worker_releases_invalid_coalesce_metadata_for_retry() {
    let (bridge, _rx, _) = test_bridge().await;
    let db = std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(format!(
            "queue-coalesce-invalid-{}.sqlite",
            uuid::Uuid::now_v7()
        ));
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "ingress"))
            .await
            .unwrap(),
    );
    let mut envelope = coalesce_envelope("bad", "9401", 20, 10);
    envelope.metadata.insert(
        "cooldis_coalesce_max_batch".to_string(),
        "not-a-number".to_string(),
    );
    queue.submit(envelope).await.unwrap();

    let worker =
        CooldisDaemonQueueWorker::new(queue.clone(), bridge.clone(), "worker-coalesce-invalid", 30);
    let err = worker.drain_once().await.unwrap_err();
    assert!(
        err.to_string()
            .contains("invalid coalesce_bursts max_batch")
    );

    let leased = queue
        .lease_ingress("worker-coalesce-invalid-retry", 1, 30)
        .await
        .unwrap();
    assert_eq!(leased.len(), 1);
    assert!(leased[0].attempt > 1);
    queue.complete_ingress(&leased[0].message_id).await.unwrap();
    let _ = std::fs::remove_file(db);
}

#[tokio::test]
async fn queue_worker_coalesces_window_expired_batch_into_one_turn_and_source_list() {
    let (bridge, _rx, session_store_path) = test_bridge().await;
    let db = std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(format!("queue-coalesce-{}.sqlite", uuid::Uuid::now_v7()));
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "ingress"))
            .await
            .unwrap(),
    );
    queue
        .submit(expired_coalesce_envelope("one", "1001", 20, 10))
        .await
        .unwrap();
    queue
        .submit(expired_coalesce_envelope("two", "1002", 20, 10))
        .await
        .unwrap();
    queue
        .submit(expired_coalesce_envelope("three", "1003", 20, 10))
        .await
        .unwrap();

    let worker =
        CooldisDaemonQueueWorker::new(queue.clone(), bridge.clone(), "worker-coalesce", 30);
    assert_eq!(worker.drain_once().await.unwrap(), 3);

    let coordinates = only_thread_coordinates(&bridge).await;
    let control_events = control_events_for(&session_store_path, &coordinates).await;
    let ingress_events: Vec<_> = control_events
        .iter()
        .filter(|event| event.kind == crate::EventKind::IoIngressReceived)
        .collect();
    let admission_events: Vec<_> = control_events
        .iter()
        .filter(|event| event.kind == crate::EventKind::AdmissionDecided)
        .collect();
    assert_eq!(ingress_events.len(), 3);
    assert_eq!(admission_events.len(), 1);
    assert_eq!(
        admission_events[0].payload["decision"].as_str(),
        Some("coalesce")
    );
    assert_eq!(
        admission_source_ids(admission_events[0]),
        ingress_events
            .iter()
            .map(|event| event.id.to_string())
            .collect::<Vec<_>>()
    );

    let thread_events = thread_events_for(&session_store_path, &coordinates).await;
    assert_eq!(
        thread_events
            .iter()
            .filter(|event| event.kind == crate::EventKind::IoIngressReceived)
            .count(),
        0
    );
    assert_eq!(
        thread_events
            .iter()
            .filter(|event| event.kind == crate::EventKind::TurnSubmitted)
            .count(),
        1
    );
    assert!(
        user_texts_for(&bridge, &coordinates)
            .await
            .contains(&"one\ntwo\nthree".to_string())
    );
    let _ = std::fs::remove_file(db);
}

#[tokio::test]
async fn queue_worker_flushes_coalesce_batch_when_max_batch_is_reached() {
    let (bridge, _rx, session_store_path) = test_bridge().await;
    let db = std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(format!(
            "queue-coalesce-max-{}.sqlite",
            uuid::Uuid::now_v7()
        ));
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "ingress"))
            .await
            .unwrap(),
    );
    queue
        .submit(coalesce_envelope("first", "2001", 60_000, 2))
        .await
        .unwrap();
    queue
        .submit(coalesce_envelope("second", "2002", 60_000, 2))
        .await
        .unwrap();

    let worker =
        CooldisDaemonQueueWorker::new(queue.clone(), bridge.clone(), "worker-coalesce-max", 30);
    assert_eq!(worker.drain_once().await.unwrap(), 2);

    let coordinates = only_thread_coordinates(&bridge).await;
    let control_events = control_events_for(&session_store_path, &coordinates).await;
    let admission = control_events
        .iter()
        .find(|event| event.kind == crate::EventKind::AdmissionDecided)
        .unwrap();
    assert_eq!(admission.payload["decision"].as_str(), Some("coalesce"));
    assert_eq!(admission_source_ids(admission).len(), 2);
    assert!(
        user_texts_for(&bridge, &coordinates)
            .await
            .contains(&"first\nsecond".to_string())
    );
    let _ = std::fs::remove_file(db);
}

#[tokio::test]
async fn queue_worker_admits_cross_drain_burst_as_separate_recovery_batches() {
    let (bridge, _rx, session_store_path) = test_bridge().await;
    let db = std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(format!(
            "queue-coalesce-cross-drain-{}.sqlite",
            uuid::Uuid::now_v7()
        ));
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "ingress"))
            .await
            .unwrap(),
    );
    queue
        .submit(coalesce_envelope("first drain", "2501", 60_000, 2))
        .await
        .unwrap();

    let worker = CooldisDaemonQueueWorker::new(
        queue.clone(),
        bridge.clone(),
        "worker-coalesce-cross-drain-hold",
        30,
    )
    .with_max_messages(1);
    assert_eq!(worker.drain_once().await.unwrap(), 1);
    assert!(bridge.threads.lock().await.is_empty());

    queue
        .submit(coalesce_envelope("second drain", "2502", 60_000, 2))
        .await
        .unwrap();
    assert_eq!(worker.drain_once().await.unwrap(), 1);
    assert!(
        bridge.threads.lock().await.is_empty(),
        "a later drain must not silently complete the earlier held batch"
    );

    let connection = rusqlite::Connection::open(&db).unwrap();
    let released = connection
        .execute(
            "UPDATE pgqrs_messages
             SET vt = datetime('now', '-1 second')
             WHERE archived_at IS NULL",
            [],
        )
        .unwrap();
    assert_eq!(released, 2, "both held messages should become visible");
    drop(connection);
    drop(worker);
    drop(queue);

    let reopened = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "ingress"))
            .await
            .unwrap(),
    );
    let restarted_worker = CooldisDaemonQueueWorker::new(
        reopened,
        bridge.clone(),
        "worker-coalesce-cross-drain-restart",
        30,
    )
    .with_max_messages(1);
    assert_eq!(restarted_worker.drain_once().await.unwrap(), 1);
    assert_eq!(restarted_worker.drain_once().await.unwrap(), 1);

    let coordinates = only_thread_coordinates(&bridge).await;
    let control_events = control_events_for(&session_store_path, &coordinates).await;
    let admissions = control_events
        .iter()
        .filter(|event| event.kind == crate::EventKind::AdmissionDecided)
        .collect::<Vec<_>>();
    assert_eq!(admissions.len(), 2);
    assert!(admissions.iter().all(|admission| {
        admission.payload["decision"].as_str() == Some("coalesce")
            && admission_source_ids(admission).len() == 1
    }));

    let user_texts = user_texts_for(&bridge, &coordinates).await;
    assert!(user_texts.contains(&"first drain".to_string()));
    assert!(user_texts.contains(&"second drain".to_string()));
    assert!(!user_texts.contains(&"first drain\nsecond drain".to_string()));
    let _ = std::fs::remove_file(db);
}

#[tokio::test(flavor = "current_thread")]
async fn queue_worker_recovers_held_coalesce_batch_after_restart() {
    let fixture_root = test_root("queue-coalesce-restart");
    let db = fixture_root.join("queue.sqlite");
    let durable_queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "ingress"))
            .await
            .unwrap(),
    );
    durable_queue
        .submit(coalesce_envelope("before", "3001", 1_000, 10))
        .await
        .unwrap();
    durable_queue
        .submit(coalesce_envelope("restart", "3002", 1_000, 10))
        .await
        .unwrap();
    let (_server, bridge, _rx) = test_bridge_at_root(&fixture_root).await;
    let worker = CooldisDaemonQueueWorker::new(
        durable_queue.clone(),
        bridge.clone(),
        "worker-coalesce-hold",
        30,
    );
    assert_eq!(worker.drain_once().await.unwrap(), 2);
    assert!(
        bridge.threads.lock().await.is_empty(),
        "held coalesce batches should not submit before the window expires"
    );
    let connection = rusqlite::Connection::open(&db).unwrap();
    let released = connection
        .execute(
            "UPDATE pgqrs_messages
             SET vt = datetime('now', '-1 second')
             WHERE archived_at IS NULL",
            [],
        )
        .unwrap();
    assert_eq!(released, 2, "both held messages should become visible");
    drop(connection);
    drop(worker);
    drop(durable_queue);

    let (restarted_server, restarted_bridge, _rx) = restarted_bridge_at_root(&fixture_root).await;
    let reopened = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "ingress"))
            .await
            .unwrap(),
    );
    let worker = CooldisDaemonQueueWorker::new(
        reopened,
        restarted_bridge.clone(),
        "worker-coalesce-restart",
        30,
    );
    assert_eq!(worker.drain_once().await.unwrap(), 2);

    let coordinates = only_thread_coordinates(&restarted_bridge).await;
    let control_events =
        control_events_for(restarted_server.session_store_path(), &coordinates).await;
    let admission = control_events
        .iter()
        .find(|event| event.kind == crate::EventKind::AdmissionDecided)
        .unwrap();
    assert_eq!(admission.payload["decision"].as_str(), Some("coalesce"));
    assert_eq!(admission_source_ids(admission).len(), 2);
    let thread = restarted_bridge
        .supervisor
        .get_thread_at(&coordinates)
        .await
        .unwrap();
    restarted_bridge
        .supervisor
        .shutdown_thread_at(&coordinates)
        .await
        .unwrap();
    let user_texts = thread
        .session_context()
        .await
        .unwrap()
        .entries
        .iter()
        .filter_map(|entry| match &entry.kind {
            SessionEntryKind::Message {
                message: CanonicalMessage::User { content, .. },
            }
            | SessionEntryKind::CustomContextMessage {
                message: CanonicalMessage::User { content, .. },
            } => Some(text_from_canonical_content(content)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        user_texts.contains(&"before\nrestart".to_string()),
        "unexpected coalesced user texts: {user_texts:?}"
    );
    let _ = std::fs::remove_file(db);
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test]
async fn coalesce_composes_with_steer_when_active_as_one_merged_turn() {
    let (bridge, _rx, session_store_path) = test_bridge().await;
    let active = bridge
        .submit_envelope(telegram_queue_envelope_with_update("active", "4000"))
        .await
        .unwrap();
    let thread_id = active.thread_id.as_deref().unwrap();
    let db = std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(format!(
            "queue-coalesce-steer-{}.sqlite",
            uuid::Uuid::now_v7()
        ));
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "ingress"))
            .await
            .unwrap(),
    );
    queue
        .submit(steer_coalesce_envelope("steer one", "4001", 20, 10))
        .await
        .unwrap();
    queue
        .submit(steer_coalesce_envelope("steer two", "4002", 20, 10))
        .await
        .unwrap();

    let worker =
        CooldisDaemonQueueWorker::new(queue.clone(), bridge.clone(), "worker-coalesce-steer", 30);
    assert_eq!(worker.drain_once().await.unwrap(), 2);

    let coordinates = bridge
        .threads
        .lock()
        .await
        .values()
        .find(|coordinates| coordinates.thread_id.to_string() == thread_id)
        .cloned()
        .unwrap();
    let control_events = control_events_for(&session_store_path, &coordinates).await;
    let latest_admission = control_events
        .iter()
        .filter(|event| event.kind == crate::EventKind::AdmissionDecided)
        .next_back()
        .unwrap();
    assert_eq!(
        latest_admission.payload["decision"].as_str(),
        Some("coalesce")
    );
    assert!(
        latest_admission.payload["admissible"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value.as_str() == Some("steer"))
    );
    let thread_events = thread_events_for(&session_store_path, &coordinates).await;
    assert_eq!(
        thread_events
            .iter()
            .filter(|event| event.kind == crate::EventKind::TurnSubmitted)
            .count(),
        2
    );
    let latest_ingress_context = thread_events
        .iter()
        .filter(|event| event.kind == crate::EventKind::TurnSubmitted)
        .next_back()
        .unwrap();
    assert_eq!(
        latest_ingress_context.payload["ingress_metadata"]["cooldis_coalesced_batch_size"].as_str(),
        Some("2")
    );
    assert_eq!(admission_source_ids(latest_admission).len(), 2);
    let claim = control_events
        .iter()
        .filter(|event| event.kind == EventKind::IoIngressClaimed)
        .next_back()
        .unwrap();
    let claim_payload =
        serde_json::from_value::<IoIngressClaimedPayload>(claim.payload.clone()).unwrap();
    let steer_turn_id = match &claim_payload.intent {
        IngressOutcomeIntent::Turn {
            turn_id,
            submission_mode,
            ..
        } if submission_mode == "steer" => turn_id,
        other => panic!("unexpected steer claim intent: {other:?}"),
    };
    let settle_payload = control_events
        .iter()
        .filter(|event| event.kind == EventKind::IoIngressSettled)
        .next_back()
        .map(|event| {
            serde_json::from_value::<IoIngressSettledPayload>(event.payload.clone()).unwrap()
        })
        .unwrap();
    assert_eq!(settle_payload.claim_event_id, claim.id);
    let steer_input = thread_events
        .iter()
        .find(|event| {
            event.kind == EventKind::SessionEntryAppended
                && event.payload["turn_id"].as_str() == Some(steer_turn_id)
        })
        .expect("steer consumption should persist its input");
    assert_eq!(settle_payload.evidence_event_id, Some(steer_input.id));
    let _ = std::fs::remove_file(db);
}

#[tokio::test]
async fn active_steer_settles_on_persisted_input_evidence() {
    let fixture_root = test_root("active-steer-evidence");
    let egress_db = fixture_root.join("io.sqlite");
    let client = Arc::new(BlockingRouteProviderClient::default());
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let server = test_server_with_provider_at_root(&fixture_root, provider_client).await;
    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let session_store_path = server.session_store_path().to_path_buf();
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;

    bridge
        .submit_envelope(telegram_queue_envelope("active turn"))
        .await
        .unwrap();
    client.wait_for_requests(1).await;
    let coordinates = only_thread_coordinates(&bridge).await;
    let handle = bridge.supervisor.get_thread_at(&coordinates).await.unwrap();
    assert_eq!(handle.status(), ThreadStatus::Running);

    let queue = Arc::new(ScriptedIngressQueue::new(
        "message-active-steer",
        telegram_queue_envelope("steer accepted")
            .with_metadata("cooldis_route_policy", "steer_when_active"),
        std::iter::empty::<&str>(),
    ));
    let worker =
        CooldisDaemonQueueWorker::new(queue.clone(), bridge.clone(), "worker-active-steer", 30);
    assert_eq!(worker.drain_once().await.unwrap(), 1);

    let control_events = control_events_for(&session_store_path, &coordinates).await;
    let claim = control_events
        .iter()
        .find(|event| event.kind == EventKind::IoIngressClaimed)
        .unwrap();
    let steer_turn_id = claim.payload["intent"]["turn_id"].as_str().unwrap();
    assert_eq!(claim.payload["intent"]["submission_mode"], "steer");
    let thread_events = thread_events_for(&session_store_path, &coordinates).await;
    let steer_input = thread_events
        .iter()
        .find(|event| {
            event.kind == EventKind::SessionEntryAppended
                && event.payload["turn_id"].as_str() == Some(steer_turn_id)
        })
        .unwrap();
    let settle = control_events
        .iter()
        .find(|event| event.kind == EventKind::IoIngressSettled)
        .unwrap();
    assert_eq!(
        settle.payload["evidence_event_id"].as_str(),
        Some(steer_input.id.to_string().as_str())
    );
    assert!(queue.completed().await);

    client.release();
    bridge.supervisor.shutdown_all().await.unwrap();
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test]
async fn idle_rejected_steer_persists_input_and_settles_on_it() {
    let fixture_root = test_root("idle-steer-evidence");
    let egress_db = fixture_root.join("io.sqlite");
    let (server, bridge, _rx) = test_bridge_at_root(&fixture_root).await;
    let session_store_path = server.session_store_path().to_path_buf();
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;

    bridge
        .submit_envelope(telegram_queue_envelope("finished turn"))
        .await
        .unwrap();
    let coordinates = only_thread_coordinates(&bridge).await;
    let handle = bridge.supervisor.get_thread_at(&coordinates).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let events = thread_events_for(&session_store_path, &coordinates).await;
        if handle.status() == ThreadStatus::Idle
            && events
                .iter()
                .any(|event| event.kind == EventKind::TurnCompleted)
        {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut runtime_events = handle.subscribe_events();

    let queue = Arc::new(ScriptedIngressQueue::new(
        "message-idle-steer",
        telegram_queue_envelope("steer while idle")
            .with_metadata("cooldis_route_policy", "steer_when_active"),
        std::iter::empty::<&str>(),
    ));
    let worker =
        CooldisDaemonQueueWorker::new(queue.clone(), bridge.clone(), "worker-idle-steer", 30);
    assert_eq!(worker.drain_once().await.unwrap(), 1);

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(ThreadEvent::Runtime { event, .. }) = runtime_events.recv().await
                && matches!(
                    event.kind,
                    crate::RuntimeEventKind::PolicyRejected { ref code, .. }
                        if code == "no_active_turn"
                )
            {
                break;
            }
        }
    })
    .await
    .expect("idle steer should emit its policy rejection");

    let control_events = control_events_for(&session_store_path, &coordinates).await;
    let claim = control_events
        .iter()
        .find(|event| event.kind == EventKind::IoIngressClaimed)
        .unwrap();
    let steer_turn_id = claim.payload["intent"]["turn_id"].as_str().unwrap();
    assert_eq!(claim.payload["intent"]["submission_mode"], "steer");
    let thread_events = thread_events_for(&session_store_path, &coordinates).await;
    let steer_input = thread_events
        .iter()
        .find(|event| {
            event.kind == EventKind::SessionEntryAppended
                && event.payload["turn_id"].as_str() == Some(steer_turn_id)
        })
        .unwrap();
    let settle = control_events
        .iter()
        .find(|event| event.kind == EventKind::IoIngressSettled)
        .unwrap();
    assert_eq!(
        settle.payload["evidence_event_id"].as_str(),
        Some(steer_input.id.to_string().as_str())
    );
    assert!(
        user_texts_for(&bridge, &coordinates)
            .await
            .contains(&"steer while idle".to_string())
    );
    assert!(queue.completed().await);
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test]
async fn fork_on_new_dm_invokes_thread_fork_and_witnesses_spawn_lineage() {
    let (bridge, _rx, session_store_path) = test_bridge().await;
    let receipt = bridge
        .submit_envelope(
            telegram_queue_envelope_with_update("fork me", "5001")
                .with_metadata("cooldis_route_policy", "fork_on_new_dm"),
        )
        .await
        .unwrap();
    assert!(receipt.thread_id.is_some());

    let child_thread_id = receipt.thread_id.as_deref().unwrap();
    let child_coordinates = bridge
        .threads
        .lock()
        .await
        .values()
        .find(|coordinates| coordinates.thread_id.to_string() == child_thread_id)
        .cloned()
        .expect("fork admission should bind the route to the child thread");
    let child_thread_events = thread_events_for(&session_store_path, &child_coordinates).await;
    assert_eq!(
        child_thread_events
            .iter()
            .filter(|event| event.kind == crate::EventKind::TurnSubmitted)
            .count(),
        1
    );
    wait_for_user_text(&bridge, &child_coordinates, "fork me").await;

    let session_store = crate::SqliteSessionStore::open(&session_store_path)
        .await
        .unwrap();
    let child_handle = bridge
        .supervisor
        .get_thread_at(&child_coordinates)
        .await
        .unwrap();
    let parent_thread_id = child_handle
        .context()
        .parent_thread_id
        .expect("fork child should record parent thread id");
    let parent_coordinates = ThreadCoordinates {
        tenant_id: child_coordinates.tenant_id.clone(),
        user_id: child_coordinates.user_id.clone(),
        session_id: child_coordinates.session_id.clone(),
        thread_id: parent_thread_id,
    };
    let spawned_events = session_store
        .read_events(&control_stream_id(&parent_coordinates), None)
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.kind == crate::EventKind::ThreadSpawned)
        .collect::<Vec<_>>();
    assert_eq!(spawned_events.len(), 1);
    let spawned = &spawned_events[0];
    let spawned_payload: ThreadSpawnedPayload =
        serde_json::from_value(spawned.payload.clone()).unwrap();
    assert_eq!(
        spawned.payload["child_thread_id"].as_str(),
        Some(child_thread_id)
    );
    assert_eq!(
        spawned_payload.child_thread_id.to_string(),
        child_thread_id.to_string()
    );
    let fork = spawned_payload
        .fork
        .expect("thread.spawned fork provenance should be typed");
    assert_eq!(fork.mode, "clone");
    assert_eq!(fork.claim_event_id, None);
    assert_eq!(fork.source_cut.thread_id, spawned_payload.parent_thread_id);
    assert_eq!(spawned.payload["fork"]["mode"].as_str(), Some("clone"));
    assert_eq!(
        spawned.payload["fork"]["sourceCut"]["threadId"].as_str(),
        spawned.payload["parent_thread_id"].as_str()
    );
    assert!(
        spawned.payload["inputs_hash"]
            .as_str()
            .is_some_and(|hash| hash.starts_with("sha256:"))
    );
}

#[tokio::test]
async fn queue_worker_processes_envelope_after_queue_and_bridge_restart() {
    let db = std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(format!("queue-restart-{}.sqlite", uuid::Uuid::now_v7()));
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "ingress"))
            .await
            .unwrap(),
    );
    queue
        .submit(telegram_queue_envelope("hello after restart"))
        .await
        .unwrap();
    drop(queue);

    let (bridge, mut rx, session_store_path) = test_bridge().await;
    let egress_db = test_root("queue-restart-egress").join("io.sqlite");
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    let reopened = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "ingress"))
            .await
            .unwrap(),
    );

    let worker =
        CooldisDaemonQueueWorker::new(reopened.clone(), bridge.clone(), "worker-restart", 30);
    assert_eq!(worker.drain_once().await.unwrap(), 1);
    drain_until_egress(&worker.bridge, "telegram.bot", "main", 1).await;

    let egress = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        egress.kind,
        EgressKind::AssistantMessage { ref text } if text.contains("hello after restart")
    ));

    let coordinates = bridge
        .threads
        .lock()
        .await
        .values()
        .next()
        .cloned()
        .expect("queue admission should create a target thread");
    let session_store = crate::SqliteSessionStore::open(&session_store_path)
        .await
        .unwrap();
    let control_stream = crate::EventStreamId::new(format!("control:{}", coordinates.thread_id));
    let thread_stream = crate::EventStreamId::for_thread(&coordinates);
    let control_events = session_store
        .read_events(&control_stream, None)
        .await
        .unwrap();
    let ingress_pos = control_events
        .iter()
        .position(|event| event.kind == crate::EventKind::IoIngressReceived)
        .unwrap();
    let admission_pos = control_events
        .iter()
        .position(|event| event.kind == crate::EventKind::AdmissionDecided)
        .unwrap();
    assert!(ingress_pos < admission_pos);
    assert_eq!(
        control_events[ingress_pos].payload["route_id"].as_str(),
        Some("main")
    );
    assert_eq!(
        control_events[ingress_pos].payload["dedupe_key"].as_str(),
        Some("telegram.bot:main:update:999")
    );
    assert_eq!(
        control_events[ingress_pos].payload["external_conversation_id"].as_str(),
        Some("telegram:chat:123")
    );
    assert_eq!(
        control_events[ingress_pos].payload["external_actor_id"].as_str(),
        Some("telegram:user:42")
    );
    assert_eq!(
        control_events[ingress_pos].payload["external_message_id"].as_str(),
        Some("555")
    );
    assert!(
        control_events[ingress_pos].payload["envelope_digest"]
            .as_str()
            .is_some_and(|hash| hash.starts_with("sha256:"))
    );
    let policy_bound = control_events
        .iter()
        .find(|event| event.kind == crate::EventKind::PolicyBound)
        .unwrap();
    assert_eq!(
        control_events[admission_pos].payload["decision"].as_str(),
        Some("queue")
    );
    assert_eq!(
        control_events[admission_pos].payload["route_id"].as_str(),
        Some("main")
    );
    assert_eq!(
        control_events[admission_pos].payload["policy_hash"],
        policy_bound.payload["content_hash"]
    );
    assert!(
        control_events[admission_pos].payload["admissible"]
            .as_array()
            .is_some_and(|admissible| !admissible.is_empty())
    );
    assert_eq!(
        control_events[admission_pos].payload["source_ingress_event_ids"][0].as_str(),
        Some(control_events[ingress_pos].id.to_string().as_str())
    );
    let thread_events = session_store
        .read_events(&thread_stream, None)
        .await
        .unwrap();
    crate::kernel::admission::assert_admission_precedes_turn_records(
        &control_events,
        &thread_events,
    );
    let turn_submitted_count = thread_events
        .iter()
        .filter(|event| event.kind == crate::EventKind::TurnSubmitted)
        .count();
    assert_eq!(turn_submitted_count, 1);
    let submitted = thread_events
        .iter()
        .find(|event| event.kind == crate::EventKind::TurnSubmitted)
        .unwrap();
    assert_eq!(submitted.origin, crate::EventOrigin::Discharged);
    assert_eq!(
        submitted.provenance.source_streams,
        vec![control_stream.clone()]
    );
    assert_eq!(
        submitted.provenance.source_event_ids,
        vec![control_events[ingress_pos].id]
    );

    let observe_source = IoSource::new("telegram.bot", "main");
    reopened
        .submit(
            observe_only_envelope("observe after restart")
                .with_dedupe_key(IoDedupeKey::for_source(&observe_source, "update:1000")),
        )
        .await
        .unwrap();
    assert_eq!(worker.drain_once().await.unwrap(), 1);
    let control_events_after = session_store
        .read_events(&control_stream, None)
        .await
        .unwrap();
    let observe_admission = control_events_after
        .iter()
        .filter(|event| event.kind == crate::EventKind::AdmissionDecided)
        .next_back()
        .unwrap();
    assert_eq!(
        observe_admission.payload["decision"].as_str(),
        Some("observe")
    );
    assert!(
        observe_admission.payload["admissible"]
            .as_array()
            .is_some_and(|admissible| !admissible.is_empty())
    );
    let thread_events_after = session_store
        .read_events(&thread_stream, None)
        .await
        .unwrap();
    assert_eq!(
        thread_events_after
            .iter()
            .filter(|event| event.kind == crate::EventKind::TurnSubmitted)
            .count(),
        turn_submitted_count
    );
    let _ = std::fs::remove_file(db);
}

#[tokio::test]
async fn clock_route_restarts_after_due_coalesces_one_missed_tick() {
    let server = test_server().await;
    let (store, coordinates, mandate) =
        start_clock_thread_with_mandate(&server, MandateCatchUpPolicy::CoalesceMissed).await;
    let after_due = event_time(mandate.event.created_at_ms, 90_000);
    let clock = Arc::new(FakeClock::new(after_due));
    let db = std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(format!("clock-coalesce-{}.sqlite", uuid::Uuid::now_v7()));
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "clock"))
            .await
            .unwrap(),
    );
    let route =
        CooldisDaemonClockRoute::new("clock-main", store.clone(), queue.clone(), clock.clone());

    assert_eq!(route.enqueue_due_once().await.unwrap(), 1);
    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let worker = CooldisDaemonQueueWorker::new(queue.clone(), bridge, "clock-worker", 30);
    assert_eq!(worker.drain_once().await.unwrap(), 1);

    let fired = timer_payloads(&store, &coordinates).await;
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].mandate_event_id, mandate.event.id);
    assert_eq!(fired[0].occurrence_index, 0);
    assert!(fired[0].catch_up);
    assert_eq!(route.enqueue_due_once().await.unwrap(), 0);
    let _ = std::fs::remove_file(db);
}

#[tokio::test]
async fn clock_route_restarts_after_due_skips_missed_until_next_occurrence() {
    let server = test_server().await;
    let (store, coordinates, mandate) =
        start_clock_thread_with_mandate(&server, MandateCatchUpPolicy::SkipMissed).await;
    let after_first_due = event_time(mandate.event.created_at_ms, 90_000);
    let second_due = event_time(mandate.event.created_at_ms, 120_000);
    let clock = Arc::new(FakeClock::new(after_first_due));
    let db = std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(format!("clock-skip-{}.sqlite", uuid::Uuid::now_v7()));
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "clock"))
            .await
            .unwrap(),
    );
    let route =
        CooldisDaemonClockRoute::new("clock-main", store.clone(), queue.clone(), clock.clone());

    assert_eq!(route.enqueue_due_once().await.unwrap(), 0);
    assert!(timer_payloads(&store, &coordinates).await.is_empty());
    clock.set(second_due);
    assert_eq!(route.enqueue_due_once().await.unwrap(), 1);

    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let worker = CooldisDaemonQueueWorker::new(queue.clone(), bridge, "clock-worker", 30);
    assert_eq!(worker.drain_once().await.unwrap(), 1);
    let fired = timer_payloads(&store, &coordinates).await;
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].mandate_event_id, mandate.event.id);
    assert_eq!(fired[0].occurrence_index, 1);
    assert!(!fired[0].catch_up);
    let _ = std::fs::remove_file(db);
}

#[tokio::test]
async fn clock_route_duplicate_enqueue_before_ack_does_not_double_fire() {
    let server = test_server().await;
    let (store, coordinates, mandate) =
        start_clock_thread_with_mandate(&server, MandateCatchUpPolicy::CoalesceMissed).await;
    let after_due = event_time(mandate.event.created_at_ms, 90_000);
    let clock = Arc::new(FakeClock::new(after_due));
    let db = std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(format!("clock-dedupe-{}.sqlite", uuid::Uuid::now_v7()));
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "clock"))
            .await
            .unwrap(),
    );
    let route =
        CooldisDaemonClockRoute::new("clock-main", store.clone(), queue.clone(), clock.clone());
    assert_eq!(route.enqueue_due_once().await.unwrap(), 1);
    drop(route);
    drop(queue);

    let reopened = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "clock"))
            .await
            .unwrap(),
    );
    let restarted_route =
        CooldisDaemonClockRoute::new("clock-main", store.clone(), reopened.clone(), clock.clone());
    assert_eq!(restarted_route.enqueue_due_once().await.unwrap(), 0);

    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let worker = CooldisDaemonQueueWorker::new(reopened.clone(), bridge, "clock-worker", 30);
    assert_eq!(worker.drain_once().await.unwrap(), 1);
    assert_eq!(worker.drain_once().await.unwrap(), 0);

    let fired = timer_payloads(&store, &coordinates).await;
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].mandate_event_id, mandate.event.id);
    assert_eq!(fired[0].occurrence_index, 0);
    let _ = std::fs::remove_file(db);
}

#[tokio::test(start_paused = true)]
async fn clock_tick_apply_before_complete_redelivery_does_not_double_fire() {
    let root = test_root("clock-apply-complete-crash-cut");
    let server = test_server_at_root(&root).await;
    let (store, coordinates, mandate) =
        start_clock_thread_with_mandate(&server, MandateCatchUpPolicy::CoalesceMissed).await;
    let after_due = event_time(mandate.event.created_at_ms, 90_000);
    let clock = Arc::new(FakeClock::new(after_due));
    let placeholder = telegram_queue_envelope("clock placeholder");
    let queue = Arc::new(ScriptedIngressQueue::new(
        "message-clock-cut",
        placeholder,
        std::iter::empty::<&str>(),
    ));
    let route = CooldisDaemonClockRoute::new("clock-main", store.clone(), queue.clone(), clock);
    assert_eq!(route.enqueue_due_once().await.unwrap(), 1);

    queue.block_next_complete();
    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let worker =
        CooldisDaemonQueueWorker::new(queue.clone(), bridge.clone(), "clock-worker-before-cut", 30);
    let drain = tokio::spawn(async move { worker.drain_once().await });
    queue.wait_for_complete_started().await;
    let fired_before = timer_payloads(&store, &coordinates).await;
    assert_eq!(
        fired_before.len(),
        1,
        "timer.fired must exist before the cut"
    );
    assert_eq!(fired_before[0].mandate_event_id, mandate.event.id);
    assert_eq!(fired_before[0].occurrence_index, 0);
    assert!(!queue.completed().await);

    drain.abort();
    assert!(drain.await.unwrap_err().is_cancelled());
    drop(bridge);
    drop(server);
    tokio::time::advance(Duration::from_secs(30)).await;

    let (_server, restarted_bridge, _rx) = restarted_bridge_at_root(&root).await;
    let worker = CooldisDaemonQueueWorker::new(
        queue.clone(),
        restarted_bridge,
        "clock-worker-after-cut",
        30,
    );
    assert_eq!(worker.drain_once().await.unwrap(), 1);
    assert!(queue.completed().await);
    assert_eq!(queue.retry_calls().await, 0);
    assert_eq!(timer_payloads(&store, &coordinates).await, fired_before);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn clock_route_revoke_prevents_further_ticks() {
    let server = test_server().await;
    let (store, coordinates, mandate) =
        start_clock_thread_with_mandate(&server, MandateCatchUpPolicy::CoalesceMissed).await;
    revoke_mandate(&store, &coordinates, mandate.event.id)
        .await
        .unwrap();
    let after_due = event_time(mandate.event.created_at_ms, 90_000);
    let clock = Arc::new(FakeClock::new(after_due));
    let db = std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(format!("clock-revoke-{}.sqlite", uuid::Uuid::now_v7()));
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "clock"))
            .await
            .unwrap(),
    );
    let route =
        CooldisDaemonClockRoute::new("clock-main", store.clone(), queue.clone(), clock.clone());

    assert_eq!(route.enqueue_due_once().await.unwrap(), 0);
    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let worker = CooldisDaemonQueueWorker::new(queue.clone(), bridge, "clock-worker", 30);
    assert_eq!(worker.drain_once().await.unwrap(), 0);
    assert!(timer_payloads(&store, &coordinates).await.is_empty());
    let _ = std::fs::remove_file(db);
}

#[tokio::test]
async fn egress_projector_delivers_after_bridge_restart_from_persisted_cursor() {
    let root = test_root("egress-restart");
    let db = root.join("io.sqlite");
    let route = route_with_egress(Vec::new(), None);

    let (server, bridge, mut first_rx) = test_bridge_at_root(&root).await;
    register_route_state(&bridge, &route, &db).await;
    let (thread_id, _) = submit_and_wait_for_assistant_event(&bridge, "after restart").await;
    assert!(first_rx.try_recv().is_err());
    drop(bridge);
    drop(server);

    let (_restarted_server, restarted, mut rx) = restarted_bridge_at_root(&root).await;
    register_route_state(&restarted, &route, &db).await;

    let _ = restarted
        .drain_egress_once("telegram.bot", "main")
        .await
        .unwrap();
    let egress = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        egress.kind,
        EgressKind::AssistantMessage { ref text } if text == "local:after restart"
    ));
    assert_eq!(
        restarted
            .drain_egress_once("telegram.bot", "main")
            .await
            .unwrap(),
        0
    );

    let delivered = egress_receipts(&restarted, &thread_id, EventKind::IoEgressDelivered).await;
    assert_eq!(delivered.len(), 1);
    assert_eq!(
        delivered[0]
            .payload
            .get("external_message_id")
            .and_then(serde_json::Value::as_str),
        Some("capture")
    );
    assert!(egress_cursor(&restarted, &thread_id).await.is_some());
}

#[tokio::test]
async fn late_egress_completion_does_not_clear_newer_active_turn() {
    let root = test_root("egress-stale-active-turn");
    let db = root.join("io.sqlite");
    let route = route_with_egress(Vec::new(), None);
    let (_server, bridge, mut rx) = test_bridge_at_root(&root).await;
    register_route_state(&bridge, &route, &db).await;
    let (_thread_id, _) = submit_and_wait_for_assistant_event(&bridge, "older turn").await;
    let scope_key = bridge
        .resolve_target(&test_envelope("scope"))
        .await
        .unwrap()
        .address
        .scope_key();
    bridge
        .active_turns
        .lock()
        .unwrap()
        .insert(scope_key.clone(), "turn-newer".to_string());

    assert_eq!(
        bridge
            .drain_egress_once(&route.kind, &route.id)
            .await
            .unwrap(),
        1
    );
    tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        bridge.active_turns.lock().unwrap().get(&scope_key),
        Some(&"turn-newer".to_string()),
        "completion for an older ingress turn must not clear its replacement"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn per_conversation_binding_survives_true_runtime_restart() {
    let root = test_root("ingress-binding-restart");
    let db = root.join("io.sqlite");
    let route = route_with_egress(Vec::new(), None);

    let (server, bridge, _rx) = test_bridge_at_root(&root).await;
    register_route_state(&bridge, &route, &db).await;
    let (thread_id, _) = submit_and_wait_for_assistant_event(&bridge, "before restart").await;
    let (scope_key, coordinates) = bridge
        .threads
        .lock()
        .await
        .iter()
        .find(|(_, coordinates)| coordinates.thread_id.to_string() == thread_id)
        .map(|(scope_key, coordinates)| (scope_key.clone(), coordinates.clone()))
        .unwrap();
    assert_eq!(route_bindings(&bridge).await.len(), 1);
    drop(bridge);
    drop(server);

    let (_restarted_server, restarted, _rx) = restarted_bridge_at_root(&root).await;
    assert!(restarted.threads.lock().await.is_empty());
    assert!(matches!(
        restarted.supervisor.get_thread_at(&coordinates).await,
        Err(CooldisError::ThreadNotFound(_))
    ));
    register_route_state(&restarted, &route, &db).await;
    assert_eq!(
        restarted.threads.lock().await.get(&scope_key).cloned(),
        Some(coordinates.clone())
    );
    assert!(matches!(
        restarted.supervisor.get_thread_at(&coordinates).await,
        Err(CooldisError::ThreadNotFound(_))
    ));
    let (resumed_thread_id, _) =
        submit_and_wait_for_assistant_event(&restarted, "after restart").await;

    assert_eq!(resumed_thread_id, thread_id);
    assert_eq!(route_bindings(&restarted).await.len(), 1);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn new_thread_is_durably_bound_before_first_turn_submission() {
    let root = test_root("ingress-binding-order");
    let db = root.join("io.sqlite");
    let (server, bridge, _rx) = test_bridge_at_root(&root).await;
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &db).await;
    let envelope = test_envelope("not submitted");
    let target = bridge.resolve_target(&envelope).await.unwrap();

    let (coordinates, _) = bridge.ensure_thread(&target, &envelope).await.unwrap();

    let bindings = route_bindings(&bridge).await;
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].scope_key, target.address.scope_key());
    assert_eq!(bindings[0].coordinates, coordinates);
    let thread_events = thread_events_for(server.session_store_path(), &coordinates).await;
    assert!(
        thread_events
            .iter()
            .all(|event| event.kind != EventKind::TurnSubmitted)
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn configured_route_rejects_ingress_until_durable_state_is_seeded() {
    let root = test_root("ingress-binding-startup-race");
    let db = root.join("io.sqlite");
    let route = route_with_egress(Vec::new(), None);
    let (_server, bridge, _rx) = test_bridge_at_root(&root).await;
    let source = test_envelope("").source;
    bridge
        .register_egress_route_config(&source.protocol, &source.instance_id, &route)
        .await
        .unwrap();
    let envelope = test_envelope("during startup");
    let target = bridge.resolve_target(&envelope).await.unwrap();
    let existing = start_thread_for_target(&bridge, &target).await;
    let state = DaemonEgressState::connect(sqlite_dsn(&db)).unwrap();
    insert_route_binding(&state, &target.address.scope_key(), &existing, 1);
    drop(state);

    let err = bridge.submit_envelope(envelope.clone()).await.unwrap_err();

    assert!(matches!(
        err,
        IoError::Bridge(message) if message.contains("durable route state")
    ));
    assert!(bridge.threads.lock().await.is_empty());

    bridge
        .register_egress_state_sqlite_dsn(&source.protocol, &source.instance_id, sqlite_dsn(&db))
        .await
        .unwrap();
    let receipt = bridge.submit_envelope(envelope).await.unwrap();
    assert_eq!(
        receipt.thread_id.as_deref(),
        Some(existing.thread_id.to_string().as_str())
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn concurrent_ingress_thread_creation_has_one_durable_winner() {
    const CONCURRENCY: usize = 16;

    let root = test_root("ingress-binding-concurrent");
    let db = root.join("io.sqlite");
    let route = route_with_egress(Vec::new(), None);
    let (_server, bridge, _rx) = test_bridge_at_root(&root).await;
    register_route_state(&bridge, &route, &db).await;
    let envelope = test_envelope("concurrent");
    let target = bridge.resolve_target(&envelope).await.unwrap();
    let barrier = Arc::new(Barrier::new(CONCURRENCY));
    let mut tasks = Vec::new();
    for _ in 0..CONCURRENCY {
        let bridge = bridge.clone();
        let barrier = barrier.clone();
        let envelope = envelope.clone();
        let target = target.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            bridge.ensure_thread(&target, &envelope).await.unwrap().0
        }));
    }

    let mut thread_ids = HashSet::new();
    for task in tasks {
        thread_ids.insert(task.await.unwrap().thread_id);
    }

    assert_eq!(thread_ids.len(), 1);
    assert_eq!(route_bindings(&bridge).await.len(), 1);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn failed_durable_bind_does_not_publish_thread_in_memory() {
    let root = test_root("ingress-binding-write-failure");
    let db = root.join("io.sqlite");
    let route = route_with_egress(Vec::new(), None);
    let (_server, bridge, _rx) = test_bridge_at_root(&root).await;
    register_route_state(&bridge, &route, &db).await;
    let envelope = test_envelope("write fails");
    let target = bridge.resolve_target(&envelope).await.unwrap();
    let state = bridge
        .egress_states
        .read()
        .await
        .get(&envelope.source.stable_scope())
        .cloned()
        .unwrap();
    state
        .lock_connection()
        .unwrap()
        .execute("DROP TABLE cooldis_daemon_egress_threads", [])
        .unwrap();

    bridge
        .ensure_thread(&target, &envelope)
        .await
        .err()
        .expect("durable binding write should fail");

    assert!(
        !bridge
            .threads
            .lock()
            .await
            .contains_key(&target.address.scope_key())
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn reserved_root_start_failure_retries_the_same_durable_binding() {
    let root = test_root("ingress-binding-load-failure");
    let db = root.join("io.sqlite");
    let route = route_with_egress(Vec::new(), None);
    let (bridge, failure_probe) = bridge_with_runtime_build_failure(&root).await;
    let envelope = test_envelope("fresh thread");
    let target = bridge.resolve_target(&envelope).await.unwrap();
    let stale_coordinates = ThreadCoordinates {
        tenant_id: target.address.tenant_id.clone(),
        user_id: target.address.user_id.clone(),
        session_id: target.address.session_id.clone(),
        thread_id: ThreadId::new(),
    };
    failure_probe.reject_once(stale_coordinates.thread_id);
    let state = DaemonEgressState::connect(sqlite_dsn(&db)).unwrap();
    insert_route_binding(&state, &target.address.scope_key(), &stale_coordinates, 1);
    drop(state);

    register_route_state(&bridge, &route, &db).await;
    assert_eq!(
        bridge
            .threads
            .lock()
            .await
            .get(&target.address.scope_key())
            .cloned(),
        Some(stale_coordinates.clone())
    );
    let err = bridge.submit_envelope(envelope.clone()).await.unwrap_err();
    assert!(err.to_string().contains("test rejected lifecycle load"));
    assert_eq!(failure_probe.failures(), vec![stale_coordinates.thread_id]);
    assert_eq!(
        bridge
            .threads
            .lock()
            .await
            .get(&target.address.scope_key())
            .cloned(),
        Some(stale_coordinates.clone()),
        "a failed start must not discard the durable root reservation"
    );

    let receipt = bridge.submit_envelope(envelope).await.unwrap();
    let reserved_thread_id = stale_coordinates.thread_id.to_string();
    assert_eq!(
        receipt.thread_id.as_deref(),
        Some(reserved_thread_id.as_str())
    );
    let recovered_coordinates = bridge
        .threads
        .lock()
        .await
        .get(&target.address.scope_key())
        .cloned()
        .unwrap();
    wait_for_user_text(&bridge, &recovered_coordinates, "fresh thread").await;
    let bindings = route_bindings(&bridge).await;
    assert_eq!(bindings.len(), 1);
    assert_eq!(
        bindings[0].coordinates.thread_id,
        stale_coordinates.thread_id
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn seeded_binding_unloaded_before_ingress_is_reloaded() {
    let root = test_root("ingress-binding-unloaded-after-seed");
    let db = root.join("io.sqlite");
    let route = route_with_egress(Vec::new(), None);
    let (_server, bridge, _rx) = test_bridge_at_root(&root).await;
    let envelope = test_envelope("fresh thread");
    let target = bridge.resolve_target(&envelope).await.unwrap();
    let stale_coordinates = start_thread_for_target(&bridge, &target).await;
    let state = DaemonEgressState::connect(sqlite_dsn(&db)).unwrap();
    insert_route_binding(&state, &target.address.scope_key(), &stale_coordinates, 1);
    drop(state);
    register_route_state(&bridge, &route, &db).await;
    bridge
        .supervisor
        .shutdown_thread_at(&stale_coordinates)
        .await
        .unwrap();

    let receipt = bridge.submit_envelope(envelope).await.unwrap();

    assert_eq!(
        receipt.thread_id.as_deref(),
        Some(stale_coordinates.thread_id.to_string().as_str())
    );
    assert_eq!(route_bindings(&bridge).await.len(), 1);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn daemon_lazy_reload_recovers_unwitnessed_workspace_metadata_as_unbound() {
    let root = test_root("daemon-lazy-workspace-witness");
    let bridge = bridge_with_runtime_factory_at_root(
        &root,
        Arc::new(AgentLoopFactory::new(
            AgentLoopConfig::new(
                ProviderApi::Other(APP_SERVER_LOCAL_PROVIDER.to_string()),
                APP_SERVER_LOCAL_PROVIDER,
                APP_SERVER_LOCAL_MODEL,
            ),
            Arc::new(RecordingRouteProviderClient::default()),
        )),
    )
    .await;
    let coordinates = ThreadCoordinates {
        tenant_id: bridge.tenant_id.clone(),
        user_id: bridge.user_id.clone(),
        session_id: "workspace-reload".to_string(),
        thread_id: ThreadId::new(),
    };
    let store = SqliteSessionStore::open(bridge.session_store_path.as_ref().unwrap())
        .await
        .unwrap();
    store
        .append(
            &coordinates,
            None,
            SessionEntryKind::Runtime {
                kind: "thread_started".to_string(),
                payload: json!({
                    "parent_thread_id": null,
                    "topology": ThreadTopology::root(),
                    "metadata": {
                        "cooldis.agent.workspace": serde_json::to_string(&json!({
                            "guest_path": "/work",
                            "host_path": root.join("unwitnessed"),
                            "mode": "rw"
                        })).unwrap()
                    },
                }),
            },
        )
        .await
        .unwrap();

    let handle = bridge
        .get_or_load_thread_handle(&coordinates)
        .await
        .unwrap();

    assert!(
        !handle
            .context()
            .metadata
            .contains_key("cooldis.agent.workspace"),
        "daemon lazy reload must not mount lifecycle workspace metadata without a bind receipt"
    );
    bridge
        .supervisor
        .shutdown_thread_at(&coordinates)
        .await
        .unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn daemon_fork_copies_the_parent_workspace_bind_witness() {
    let root = test_root("daemon-fork-workspace-witness");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let bridge = bridge_with_runtime_factory_at_root(
        &root,
        Arc::new(AgentLoopFactory::new(
            AgentLoopConfig::new(
                ProviderApi::Other(APP_SERVER_LOCAL_PROVIDER.to_string()),
                APP_SERVER_LOCAL_PROVIDER,
                APP_SERVER_LOCAL_MODEL,
            ),
            Arc::new(RecordingRouteProviderClient::default()),
        )),
    )
    .await;
    let resolved_workspace = crate::AgentManifestResolvedWorkspaceMount {
        guest_path: PathBuf::from("/work"),
        host_path: std::fs::canonicalize(&workspace).unwrap(),
        mode: crate::AgentManifestWorkspaceMode::ReadWrite,
    };
    let mut metadata = BTreeMap::new();
    metadata.insert(
        "cooldis.agent.workspace".to_string(),
        serde_json::to_string(&resolved_workspace).unwrap(),
    );
    let parent = bridge
        .supervisor
        .start_thread(ThreadStartRequest {
            tenant_id: bridge.tenant_id.clone(),
            user_id: bridge.user_id.clone(),
            session_id: "workspace-fork".to_string(),
            topology: ThreadTopology::root(),
            metadata,
        })
        .await
        .unwrap();
    let compile_payload = json!({
        "ref_uri": "agent://workspace@0.1.0",
        "manifest_hash": "sha256:workspace",
        "source_hash": "sha256:source"
    });
    let bind_payload = serde_json::to_value(crate::AgentManifestBindReceipt {
        ref_uri: "agent://workspace@0.1.0".to_string(),
        manifest_hash: "sha256:workspace".to_string(),
        model_profile_id: "default".to_string(),
        model_profile_origin: None,
        provider_id: APP_SERVER_LOCAL_PROVIDER.to_string(),
        model_id: APP_SERVER_LOCAL_MODEL.to_string(),
        tool_ids: Vec::new(),
        operation_bindings: Vec::new(),
        skill_packages: Vec::new(),
        skill_discovery: None,
        static_context_segments: Vec::new(),
        tool_universes: Vec::new(),
        couplings: Vec::new(),
        granted: Vec::new(),
        grant_bindings: Vec::new(),
        effective_runtime: crate::AgentManifestRuntimeDefaults::default(),
        overridden_keys: Vec::new(),
        placement: Some(crate::AgentManifestPlacementBinding::default()),
        placement_origin: None,
        workspace: Some(resolved_workspace),
        workspace_origin: None,
    })
    .unwrap();
    parent
        .record_manifest_receipts(compile_payload, bind_payload)
        .await
        .unwrap();
    let inherited = bridge
        .inherited_workspace_manifest_receipts(&parent)
        .await
        .unwrap();
    let checkpoint = bridge
        .supervisor
        .create_checkpoint_at(
            &parent.context().coordinates,
            None,
            Some("workspace-fork".to_string()),
            parent.context().metadata.clone(),
        )
        .await
        .unwrap();
    let child = bridge
        .fork_thread_with_manifest_witness(checkpoint, ThreadId::new(), inherited)
        .await
        .unwrap();

    assert!(
        child
            .read_thread_events(None)
            .await
            .unwrap()
            .iter()
            .any(|event| event.kind == EventKind::ManifestBindCompleted),
        "daemon fork children must have a child-local workspace bind witness"
    );
    bridge.supervisor.shutdown_all().await.unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn cancelled_daemon_start_finishes_the_workspace_bind_witness() {
    let root = test_root("daemon-start-workspace-cancel");
    let (bridge, gate) = bridge_with_runtime_build_gate(&root).await;
    let thread_id = ThreadId::new();
    let coordinates = ThreadCoordinates {
        tenant_id: bridge.tenant_id.clone(),
        user_id: bridge.user_id.clone(),
        session_id: "workspace-cancel".to_string(),
        thread_id,
    };
    gate.block_first_build(coordinates.clone());
    let workspace = json!({
        "guest_path": "/work",
        "host_path": root.join("workspace"),
        "mode": "rw"
    });
    let mut metadata = BTreeMap::new();
    metadata.insert(
        "cooldis.agent.workspace".to_string(),
        serde_json::to_string(&workspace).unwrap(),
    );
    let binding = KernelThreadSpawnAgentBinding {
        metadata: metadata.clone(),
        compile_receipt: json!({
            "manifest_hash": "sha256:cancelled-daemon-start"
        }),
        bind_receipt: json!({
            "manifest_hash": "sha256:cancelled-daemon-start",
            "placement": {"target": "local"},
            "workspace": workspace
        }),
    };
    let request = ThreadStartRequest {
        tenant_id: coordinates.tenant_id.clone(),
        user_id: coordinates.user_id.clone(),
        session_id: coordinates.session_id.clone(),
        topology: ThreadTopology::root(),
        metadata,
    };
    let caller_bridge = bridge.clone();
    let caller = tokio::spawn(async move {
        caller_bridge
            .start_thread_with_manifest_witness(request, Some(thread_id), Some(binding))
            .await
    });

    gate.wait_for_builds(1).await;
    caller.abort();
    match caller.await {
        Err(err) => assert!(err.is_cancelled()),
        Ok(_) => panic!("daemon start caller was not cancelled"),
    }
    gate.release();

    let handle = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Ok(handle) = bridge.supervisor.get_thread_at(&coordinates).await
                && handle
                    .read_thread_events(None)
                    .await
                    .unwrap()
                    .iter()
                    .any(|event| event.kind == EventKind::ManifestBindCompleted)
            {
                break handle;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("daemon workspace bind witness did not finish after caller cancellation");
    assert!(
        handle
            .context()
            .metadata
            .contains_key("cooldis.agent.workspace")
    );
    bridge.supervisor.shutdown_all().await.unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn concurrent_lazy_loads_build_one_runtime_and_share_its_handle() {
    let root = test_root("ingress-binding-concurrent-load");
    let (bridge, gate) = bridge_with_runtime_build_gate(&root).await;
    let envelope = test_envelope("concurrent reload");
    let target = bridge.resolve_target(&envelope).await.unwrap();
    let coordinates = ThreadCoordinates {
        tenant_id: target.address.tenant_id,
        user_id: target.address.user_id,
        session_id: target.address.session_id,
        thread_id: ThreadId::new(),
    };
    gate.block_first_build(coordinates.clone());

    let first_bridge = bridge.clone();
    let first_coordinates = coordinates.clone();
    let first = tokio::spawn(async move {
        first_bridge
            .get_or_load_thread_handle(&first_coordinates)
            .await
    });
    gate.wait_for_builds(1).await;

    let second_bridge = bridge.clone();
    let second_coordinates = coordinates.clone();
    let second = tokio::spawn(async move {
        second_bridge
            .get_or_load_thread_handle(&second_coordinates)
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(250), gate.wait_for_builds(2))
            .await
            .is_err(),
        "a concurrent lazy load built a duplicate runtime"
    );

    gate.release();
    let first_handle = first.await.unwrap().unwrap();
    let second_handle = second.await.unwrap().unwrap();
    assert_eq!(first_handle.context().coordinates, coordinates);
    assert_eq!(second_handle.context().coordinates, coordinates);
    assert_eq!(gate.matching_builds(), 1);
    let store = bridge
        .supervisor
        .runtime_store(&coordinates.tenant_id)
        .await
        .unwrap();
    assert_eq!(
        store
            .read_events(&EventStreamId::for_thread(&coordinates), None)
            .await
            .unwrap()
            .iter()
            .filter(|event| event.kind == EventKind::ThreadReloadDegraded)
            .count(),
        1,
        "racing lazy loads must share one degraded-reload witness"
    );
    bridge
        .supervisor
        .shutdown_thread_at(&coordinates)
        .await
        .unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn thread_already_exists_retry_keeps_scope_mismatch_fail_closed() {
    let root = test_root("ingress-binding-load-scope-mismatch");
    let (bridge, gate) = bridge_with_runtime_build_gate(&root).await;
    let envelope = test_envelope("scope mismatch");
    let target = bridge.resolve_target(&envelope).await.unwrap();
    let thread_id = ThreadId::new();
    let requested = ThreadCoordinates {
        tenant_id: target.address.tenant_id.clone(),
        user_id: target.address.user_id,
        session_id: target.address.session_id,
        thread_id,
    };
    let conflicting = ThreadCoordinates {
        tenant_id: target.address.tenant_id,
        user_id: "other-user".to_string(),
        session_id: "other-session".to_string(),
        thread_id,
    };
    gate.block_first_build(conflicting.clone());

    let conflicting_supervisor = bridge.supervisor.clone();
    let conflicting_coordinates = conflicting.clone();
    let conflicting_load = tokio::spawn(async move {
        conflicting_supervisor
            .load_thread_from_lifecycle(ThreadLifecycleRecord {
                coordinates: conflicting_coordinates,
                parent_thread_id: None,
                topology: ThreadTopology::root(),
                status: ThreadLifecycleStatus::Idle,
                latest_signal_id: None,
                latest_checkpoint_id: None,
                created_at_ms: now_ms(),
                updated_at_ms: now_ms(),
                metadata: BTreeMap::new(),
            })
            .await
    });
    gate.wait_for_builds(1).await;

    let loading_bridge = bridge.clone();
    let loading_coordinates = requested.clone();
    let mut loading = tokio::spawn(async move {
        loading_bridge
            .get_or_load_thread_handle(&loading_coordinates)
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(250), &mut loading)
            .await
            .is_err(),
        "lazy load must wait for the conflicting start reservation to settle"
    );

    gate.release();
    conflicting_load.await.unwrap().unwrap();

    assert!(matches!(
        loading.await.unwrap(),
        Err(ThreadHandleResolutionError::Lookup(
            CooldisError::ThreadScopeMismatch { .. }
        ))
    ));
    bridge
        .supervisor
        .shutdown_thread_at(&conflicting)
        .await
        .unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn duplicate_ingress_bindings_seed_the_latest_thread() {
    let root = test_root("ingress-binding-duplicate");
    let db = root.join("io.sqlite");
    let route = route_with_egress(Vec::new(), None);
    let (_server, bridge, _rx) = test_bridge_at_root(&root).await;
    let envelope = test_envelope("latest thread");
    let target = bridge.resolve_target(&envelope).await.unwrap();
    let older = start_thread_for_target(&bridge, &target).await;
    let latest = start_thread_for_target(&bridge, &target).await;
    let state = DaemonEgressState::connect(sqlite_dsn(&db)).unwrap();
    insert_route_binding(&state, &target.address.scope_key(), &older, 1);
    insert_route_binding(&state, &target.address.scope_key(), &latest, 2);
    drop(state);

    register_route_state(&bridge, &route, &db).await;
    let receipt = bridge.submit_envelope(envelope).await.unwrap();

    assert_eq!(
        receipt.thread_id.as_deref(),
        Some(latest.thread_id.to_string().as_str())
    );
    assert_eq!(route_bindings(&bridge).await.len(), 2);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn ingress_ownership_migration_preserves_existing_dedupe_rows_deterministically() {
    let root = test_root("ingress-ownership-migration");
    let db = root.join("io.sqlite");
    std::fs::create_dir_all(&root).unwrap();
    let connection = rusqlite::Connection::open(&db).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE cooldis_ingress_dedupe (
                queue_name TEXT NOT NULL,
                dedupe_key TEXT NOT NULL,
                envelope_id TEXT NOT NULL,
                inserted_at_ms INTEGER NOT NULL,
                PRIMARY KEY (queue_name, dedupe_key)
             );
             INSERT INTO cooldis_ingress_dedupe
                (queue_name, dedupe_key, envelope_id, inserted_at_ms)
             VALUES
                ('ingress', 'telegram.bot:main:update:before', 'envelope-before', 11),
                ('ingress', 'telegram.bot:main:update:after', 'envelope-after', 12);",
        )
        .unwrap();
    drop(connection);

    drop(DaemonEgressState::connect(sqlite_dsn(&db)).unwrap());
    drop(DaemonEgressState::connect(sqlite_dsn(&db)).unwrap());

    let connection = rusqlite::Connection::open(&db).unwrap();
    let dedupe_rows = connection
        .prepare(
            "SELECT queue_name, dedupe_key, envelope_id, inserted_at_ms
             FROM cooldis_ingress_dedupe ORDER BY dedupe_key",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        dedupe_rows,
        vec![
            (
                "ingress".to_string(),
                "telegram.bot:main:update:after".to_string(),
                "envelope-after".to_string(),
                12,
            ),
            (
                "ingress".to_string(),
                "telegram.bot:main:update:before".to_string(),
                "envelope-before".to_string(),
                11,
            ),
        ]
    );
    let ownership_columns = connection
        .prepare("PRAGMA table_info(cooldis_daemon_ingress_ownership)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        ownership_columns,
        vec![
            "dedupe_key",
            "ownership_id",
            "ingress_envelope_id",
            "stream_id",
            "attempt",
            "created_at_ms",
        ]
    );
    let trigger_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'trigger' AND name = 'cooldis_ingress_dedupe_delete_ownership'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(trigger_count, 1);
    connection
        .execute(
            "INSERT INTO cooldis_daemon_ingress_ownership
                (dedupe_key, ownership_id, ingress_envelope_id, stream_id, attempt, created_at_ms)
             VALUES (?1, 'ownership-before', 'envelope-before', 'control:parent', 1, 13)",
            params!["telegram.bot:main:update:before"],
        )
        .unwrap();
    connection
        .execute(
            "DELETE FROM cooldis_ingress_dedupe
             WHERE queue_name = 'ingress' AND dedupe_key = ?1",
            params!["telegram.bot:main:update:before"],
        )
        .unwrap();
    let aged_ownership: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM cooldis_daemon_ingress_ownership
             WHERE dedupe_key = ?1",
            params!["telegram.bot:main:update:before"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(aged_ownership, 0, "ownership must age with its dedupe row");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn equal_timestamp_ingress_bindings_seed_the_last_committed_thread() {
    let root = test_root("ingress-binding-equal-timestamp");
    let db = root.join("io.sqlite");
    let route = route_with_egress(Vec::new(), None);
    let (_server, bridge, _rx) = test_bridge_at_root(&root).await;
    let envelope = test_envelope("latest equal-time binding");
    let target = bridge.resolve_target(&envelope).await.unwrap();
    let older = ThreadCoordinates {
        tenant_id: target.address.tenant_id.clone(),
        user_id: target.address.user_id.clone(),
        session_id: target.address.session_id.clone(),
        thread_id: ThreadId::parse_str("ffffffff-ffff-7fff-bfff-ffffffffffff").unwrap(),
    };
    let latest = ThreadCoordinates {
        thread_id: ThreadId::parse_str("00000000-0000-7000-8000-000000000001").unwrap(),
        ..older.clone()
    };
    let state = DaemonEgressState::connect(sqlite_dsn(&db)).unwrap();
    insert_route_binding(&state, &target.address.scope_key(), &older, 1);
    insert_route_binding(&state, &target.address.scope_key(), &latest, 1);
    drop(state);

    register_route_state(&bridge, &route, &db).await;
    assert_eq!(
        bridge.threads.lock().await.get(&target.address.scope_key()),
        Some(&latest),
        "row commit order must break equal-timestamp binding ties"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn egress_refresh_does_not_roll_back_the_active_ingress_rebind() {
    let root = test_root("egress-refresh-keeps-ingress-rebind");
    let db = root.join("io.sqlite");
    let state = DaemonEgressState::connect(sqlite_dsn(&db)).unwrap();
    let scope_key = "test.protocol:main:conversation:123";
    let parent = ThreadCoordinates::new("tenant", "user", "session");
    let child = ThreadCoordinates {
        thread_id: ThreadId::new(),
        ..parent.clone()
    };
    assert_eq!(
        state
            .claim_ingress_thread_binding("main", "test.protocol:main", scope_key, &parent)
            .unwrap(),
        parent
    );
    state
        .bind_thread("main", "test.protocol:main", scope_key, &child)
        .unwrap();
    state
        .rebind_ingress_thread("main", "test.protocol:main", scope_key, &child)
        .unwrap();
    state
        .bind_thread("main", "test.protocol:main", scope_key, &parent)
        .unwrap();

    let active = state.active_ingress_threads("main").unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].coordinates, child);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn egress_projector_delivers_requested_platform_action_after_bridge_restart() {
    let root = test_root("egress-requested-action-restart");
    let db = root.join("io.sqlite");
    let route = route_with_egress(Vec::new(), None);

    let (server, bridge, mut first_rx) = test_bridge_at_root(&root).await;
    register_route_state(&bridge, &route, &db).await;
    let receipt = bridge
        .submit_envelope(telegram_queue_envelope("please send action"))
        .await
        .unwrap();
    let thread_id = receipt.thread_id.expect("receipt should include thread id");
    wait_for_assistant_text(&bridge, &thread_id, "local:please send action").await;
    assert_eq!(
        bridge
            .drain_egress_once("telegram.bot", "main")
            .await
            .unwrap(),
        1
    );
    let assistant = tokio::time::timeout(Duration::from_secs(3), first_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        assistant.kind,
        EgressKind::AssistantMessage { ref text } if text == "local:please send action"
    ));
    let assistant_cursor = egress_cursor(&bridge, &thread_id)
        .await
        .expect("assistant delivery cursor");

    let parsed = ThreadId::parse_str(&thread_id).unwrap();
    let handle = bridge
        .supervisor
        .get_thread(&bridge.tenant_id, parsed)
        .await
        .unwrap();
    let ingress_event = handle
        .read_thread_events(None)
        .await
        .unwrap()
        .into_iter()
        .find(|event| {
            event.kind == EventKind::TurnSubmitted && event.payload["turn_id"].as_str().is_some()
        })
        .expect("ingress turn submission");
    let ingress_context = ingress_context_from_event(&ingress_event).unwrap();
    let mut target = ingress_context.target.clone();
    target.metadata = ingress_context.metadata.clone();
    let mut payload = serde_json::to_value(IoEgressRequestedPayload {
        egress_kind: serde_json::to_value(EgressKind::PlatformAction {
            action: "sticker".to_string(),
            payload: json!({
                "file_id": "file-555"
            }),
        })
        .unwrap(),
        resolved_target: Some(serde_json::to_value(target).unwrap()),
        requested_by_tool_call_id: "call_platform_action".to_string(),
        quote: Some("please send action".to_string()),
        match_event_id: Some(ingress_event.id),
    })
    .unwrap();
    payload.as_object_mut().unwrap().insert(
        "schema".to_string(),
        json!(EventKind::IoEgressRequested.payload_schema_id()),
    );
    let requested_event = handle
        .append_thread_event_record(NewEventRecord::discharged(
            handle.context().coordinates.clone(),
            EventKind::IoEgressRequested,
            payload,
            EventProvenance {
                source_streams: vec![EventStreamId::for_thread(&handle.context().coordinates)],
                source_event_ids: vec![ingress_event.id],
                discharged_by: Some("rpc:append_events".to_string()),
                function: Some("io_egress_requested/v1".to_string()),
                ..EventProvenance::default()
            },
        ))
        .await
        .unwrap();
    let requested_event_id = requested_event.id.to_string();
    assert_eq!(
        bridge
            .drain_egress_once("telegram.bot", "main")
            .await
            .unwrap(),
        1
    );
    let platform_action = tokio::time::timeout(Duration::from_secs(3), first_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        platform_action.kind,
        EgressKind::PlatformAction { ref action, ref payload }
            if action == "sticker"
                && payload["file_id"].as_str() == Some("file-555")
    ));
    let state = bridge
        .egress_states
        .read()
        .await
        .get(&source_scope("telegram.bot", "main"))
        .cloned()
        .unwrap();
    state
        .store_cursor("main", &thread_id, &assistant_cursor)
        .unwrap();
    drop(bridge);
    drop(server);

    let (_restarted_server, restarted, mut rx) = restarted_bridge_at_root(&root).await;
    register_route_state(&restarted, &route, &db).await;
    let _ = restarted
        .drain_egress_once("telegram.bot", "main")
        .await
        .unwrap();
    assert!(rx.try_recv().is_err());
    assert_eq!(
        restarted
            .drain_egress_once("telegram.bot", "main")
            .await
            .unwrap(),
        0
    );

    let delivered = egress_receipts(&restarted, &thread_id, EventKind::IoEgressDelivered).await;
    assert_eq!(
        delivered
            .iter()
            .filter(|event| {
                event.payload["source_event_id"].as_str() == Some(requested_event_id.as_str())
                    && event.payload["egress_kind"].as_str() == Some("platform_action:sticker")
            })
            .count(),
        1
    );
}

#[tokio::test]
async fn egress_projector_skips_invalid_requested_egress_and_continues() {
    let root = test_root("egress-requested-poison-skip");
    let db = root.join("io.sqlite");
    let route = route_with_egress(Vec::new(), None);

    let (_server, bridge, mut rx) = test_bridge_at_root(&root).await;
    register_route_state(&bridge, &route, &db).await;
    let receipt = bridge
        .submit_envelope(telegram_queue_envelope("poison skip"))
        .await
        .unwrap();
    let thread_id = receipt.thread_id.expect("receipt should include thread id");
    wait_for_assistant_text(&bridge, &thread_id, "local:poison skip").await;
    assert_eq!(
        bridge
            .drain_egress_once("telegram.bot", "main")
            .await
            .unwrap(),
        1
    );
    let assistant = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        assistant.kind,
        EgressKind::AssistantMessage { ref text } if text == "local:poison skip"
    ));

    let parsed = ThreadId::parse_str(&thread_id).unwrap();
    let handle = bridge
        .supervisor
        .get_thread(&bridge.tenant_id, parsed)
        .await
        .unwrap();
    let ingress_event = handle
        .read_thread_events(None)
        .await
        .unwrap()
        .into_iter()
        .find(|event| {
            event.kind == EventKind::TurnSubmitted && event.payload["turn_id"].as_str().is_some()
        })
        .expect("ingress turn submission");
    let ingress_context = ingress_context_from_event(&ingress_event).unwrap();
    let mut target = ingress_context.target.clone();
    target.metadata = ingress_context.metadata.clone();

    let mut invalid_payload = serde_json::to_value(IoEgressRequestedPayload {
        egress_kind: serde_json::to_value(EgressKind::PlatformAction {
            action: "sticker".to_string(),
            payload: json!({
                "file_id": "bad"
            }),
        })
        .unwrap(),
        resolved_target: Some(json!({})),
        requested_by_tool_call_id: "call_bad".to_string(),
        quote: Some("poison skip".to_string()),
        match_event_id: None,
    })
    .unwrap();
    invalid_payload.as_object_mut().unwrap().insert(
        "schema".to_string(),
        json!(EventKind::IoEgressRequested.payload_schema_id()),
    );
    handle
        .append_thread_event_record(NewEventRecord::discharged(
            handle.context().coordinates.clone(),
            EventKind::IoEgressRequested,
            invalid_payload,
            EventProvenance {
                source_streams: vec![EventStreamId::for_thread(&handle.context().coordinates)],
                source_event_ids: vec![ingress_event.id],
                discharged_by: Some("rpc:append_events".to_string()),
                function: Some("io_egress_requested/v1".to_string()),
                ..EventProvenance::default()
            },
        ))
        .await
        .unwrap();

    let mut valid_payload = serde_json::to_value(IoEgressRequestedPayload {
        egress_kind: serde_json::to_value(EgressKind::PlatformAction {
            action: "sticker".to_string(),
            payload: json!({
                "file_id": "file-777"
            }),
        })
        .unwrap(),
        resolved_target: Some(serde_json::to_value(target).unwrap()),
        requested_by_tool_call_id: "call_good".to_string(),
        quote: Some("poison skip".to_string()),
        match_event_id: Some(ingress_event.id),
    })
    .unwrap();
    valid_payload.as_object_mut().unwrap().insert(
        "schema".to_string(),
        json!(EventKind::IoEgressRequested.payload_schema_id()),
    );
    let valid_event = handle
        .append_thread_event_record(NewEventRecord::discharged(
            handle.context().coordinates.clone(),
            EventKind::IoEgressRequested,
            valid_payload,
            EventProvenance {
                source_streams: vec![EventStreamId::for_thread(&handle.context().coordinates)],
                source_event_ids: vec![ingress_event.id],
                discharged_by: Some("rpc:append_events".to_string()),
                function: Some("io_egress_requested/v1".to_string()),
                ..EventProvenance::default()
            },
        ))
        .await
        .unwrap();
    let valid_event_id = valid_event.id.to_string();

    assert_eq!(
        bridge
            .drain_egress_once("telegram.bot", "main")
            .await
            .unwrap(),
        1
    );
    let egress = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        egress.kind,
        EgressKind::PlatformAction { ref action, ref payload }
            if action == "sticker"
                && payload["file_id"].as_str() == Some("file-777")
    ));
    let delivered = egress_receipts(&bridge, &thread_id, EventKind::IoEgressDelivered).await;
    assert!(delivered.iter().any(|event| {
        event.payload["source_event_id"].as_str() == Some(valid_event_id.as_str())
            && event.payload["egress_kind"].as_str() == Some("platform_action:sticker")
    }));
}

#[tokio::test]
async fn egress_projector_recovers_missing_projection_after_partial_receipt_cursor() {
    let root = test_root("egress-partial-projection-cursor");
    let db = root.join("io.sqlite");
    let route = route_with_egress(
        Vec::new(),
        Some(crate::CooldisTypingSimulationConfig {
            chars_per_second: 0,
        }),
    );

    let (_server, bridge, mut rx) = test_bridge_at_root(&root).await;
    register_route_state(&bridge, &route, &db).await;
    let (thread_id, expected) =
        submit_and_wait_for_assistant_event(&bridge, "partial cursor").await;

    let parsed = ThreadId::parse_str(&thread_id).unwrap();
    let handle = bridge
        .supervisor
        .get_thread(&bridge.tenant_id, parsed)
        .await
        .unwrap();
    let context = handle.session_context().await.unwrap();
    let events = handle.read_thread_events(None).await.unwrap();
    let source_event = events
        .iter()
        .find(|event| {
            assistant_text_from_session_event(event, &context.entries).as_deref()
                == Some(expected.as_str())
        })
        .unwrap()
        .clone();
    let source_context = events.iter().find_map(ingress_context_from_event).unwrap();
    let mut source_envelope = EgressEnvelope::new(
        source_context.target,
        EgressKind::AssistantMessage {
            text: expected.clone(),
        },
        now_ms(),
    );
    source_envelope.source_ingress_id = source_context.source_ingress_id;
    source_envelope.metadata = source_context.metadata;
    let typing_envelope = sibling_egress(
        &source_envelope,
        EgressKind::PlatformAction {
            action: "typing".to_string(),
            payload: JsonValue::Object(JsonMap::new()),
        },
    );
    let binding = BoundEgressThread {
        route_id: "main".to_string(),
        scope_key: "test-scope".to_string(),
        coordinates: handle.context().coordinates.clone(),
    };
    let partial_dedupe_key = egress_dedupe_key(source_event.id, 0);
    let partial_receipt = append_egress_delivered_receipt(
        &handle,
        &binding,
        &source_event,
        0,
        &partial_dedupe_key,
        &typing_envelope,
        &DeliveryReceipt::delivered(&typing_envelope, "typing-before-crash"),
        1,
    )
    .await
    .unwrap();
    let state = bridge
        .egress_states
        .read()
        .await
        .get(&source_scope("telegram.bot", "main"))
        .cloned()
        .unwrap();
    state
        .store_cursor("main", &thread_id, &partial_receipt.cursor_v1())
        .unwrap();

    assert_eq!(
        bridge
            .drain_egress_once("telegram.bot", "main")
            .await
            .unwrap(),
        1
    );
    let egress = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        egress.kind,
        EgressKind::AssistantMessage { ref text } if text == &expected
    ));
    assert!(rx.try_recv().is_err());

    let delivered = egress_receipts(&bridge, &thread_id, EventKind::IoEgressDelivered).await;
    assert_eq!(delivered.len(), 2);
    let assistant_dedupe_key = egress_dedupe_key(source_event.id, 1);
    assert!(delivered.iter().any(|event| {
        event.payload["dedupe_key"].as_str() == Some(partial_dedupe_key.as_str())
    }));
    assert!(delivered.iter().any(|event| {
        event.payload["dedupe_key"].as_str() == Some(assistant_dedupe_key.as_str())
    }));
    let cursor = egress_cursor(&bridge, &thread_id).await.unwrap();
    assert!(cursor.sequence.get() > partial_receipt.sequence.get());
}

#[tokio::test]
async fn egress_projector_retries_transient_failures_and_records_attempts() {
    let root = test_root("egress-retry");
    let db = root.join("io.sqlite");
    let adapter = Arc::new(ScriptedEgress::new(
        ["telegram 500", "telegram 500"],
        &["telegram-message-3"],
    ));
    let (_server, bridge, _rx) = test_bridge_at_root(&root).await;
    bridge
        .register_egress_adapter("telegram.bot", "main", adapter.clone())
        .await;
    let route = route_with_egress_and_retry(
        Vec::new(),
        None,
        crate::CooldisEgressRetryConfig {
            max_attempts: 5,
            base_backoff_ms: 0,
        },
    );
    register_route_state(&bridge, &route, &db).await;
    let (thread_id, _) = submit_and_wait_for_assistant_event(&bridge, "retry me").await;

    assert_eq!(
        bridge
            .drain_egress_once("telegram.bot", "main")
            .await
            .unwrap(),
        1
    );

    assert_eq!(adapter.calls().await.len(), 3);
    let delivered = egress_receipts(&bridge, &thread_id, EventKind::IoEgressDelivered).await;
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].payload["attempts"].as_u64(), Some(3));
    assert_eq!(
        delivered[0].payload["external_message_id"].as_str(),
        Some("telegram-message-3")
    );
}

#[tokio::test]
async fn egress_projector_dead_letters_after_max_attempts() {
    let root = test_root("egress-dead-letter");
    let db = root.join("io.sqlite");
    let adapter = Arc::new(ScriptedEgress::new(
        ["telegram 500", "telegram 500", "telegram 500"],
        &[],
    ));
    let (_server, bridge, _rx) = test_bridge_at_root(&root).await;
    bridge
        .register_egress_adapter("telegram.bot", "main", adapter.clone())
        .await;
    let route = route_with_egress_and_retry(
        Vec::new(),
        None,
        crate::CooldisEgressRetryConfig {
            max_attempts: 3,
            base_backoff_ms: 0,
        },
    );
    register_route_state(&bridge, &route, &db).await;
    let (thread_id, _) = submit_and_wait_for_assistant_event(&bridge, "dead letter").await;

    assert_eq!(
        bridge
            .drain_egress_once("telegram.bot", "main")
            .await
            .unwrap(),
        1
    );

    assert_eq!(adapter.calls().await.len(), 3);
    let failed = egress_receipts(&bridge, &thread_id, EventKind::IoEgressFailed).await;
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].payload["attempts"].as_u64(), Some(3));
    assert_eq!(failed[0].payload["dead_lettered"].as_bool(), Some(true));
    assert_eq!(
        bridge
            .egress_dead_letter_count("telegram.bot", "main")
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn egress_projector_witnesses_silence_without_wire_call() {
    let root = test_root("egress-silence");
    let db = root.join("io.sqlite");
    let adapter = Arc::new(ScriptedEgress::new(std::iter::empty::<&str>(), &[]));
    let (_server, bridge, _rx) = test_bridge_at_root(&root).await;
    bridge
        .register_egress_adapter("telegram.bot", "main", adapter.clone())
        .await;
    let route = route_with_egress_and_retry(
        vec![crate::CooldisEgressProjectionRuleConfig {
            pattern: r"local:\[no_response\]".to_string(),
            action: "silence".to_string(),
        }],
        None,
        crate::CooldisEgressRetryConfig {
            max_attempts: 5,
            base_backoff_ms: 0,
        },
    );
    register_route_state(&bridge, &route, &db).await;
    let (thread_id, _) = submit_and_wait_for_assistant_event(&bridge, "[no_response]").await;

    assert_eq!(
        bridge
            .drain_egress_once("telegram.bot", "main")
            .await
            .unwrap(),
        1
    );

    assert!(adapter.calls().await.is_empty());
    let delivered = egress_receipts(&bridge, &thread_id, EventKind::IoEgressDelivered).await;
    assert_eq!(delivered.len(), 1);
    assert_eq!(
        delivered[0].payload["egress_kind"].as_str(),
        Some("silence")
    );
    assert_eq!(delivered[0].payload["attempts"].as_u64(), Some(1));
}

#[tokio::test]
async fn telegram_webhook_accepts_update_and_uses_sink() {
    let envelopes = Arc::new(TokioMutex::new(Vec::new()));
    let sink = Arc::new(CaptureSink {
        envelopes: envelopes.clone(),
    });
    let server = TelegramWebhookServer::bind(
        TelegramWebhookServerConfig {
            route_id: "main".to_string(),
            listen: "127.0.0.1:0".to_string(),
            path: "/telegram".to_string(),
            secret_token: "secret".to_string(),
        },
        sink,
    )
    .await
    .unwrap();
    let addr = server.local_addr().unwrap();
    tokio::spawn(server.serve());

    let response = post_json(
        addr,
        "/telegram",
        Some("secret"),
        json!({
            "update_id": 999,
            "message": {
                "message_id": 555,
                "chat": { "id": 123, "type": "private" },
                "date": 1777000000,
                "text": "hello webhook"
            }
        }),
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 200 OK"));
    let captured = envelopes.lock().await;
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].content.text_projection(), "hello webhook");
    let envelope = captured[0].clone();
    drop(captured);

    let (bridge, _rx, session_store_path) = test_bridge().await;
    let receipt = bridge.submit_envelope(envelope).await.unwrap();
    let thread_id = receipt.thread_id.unwrap();
    wait_for_assistant_text(&bridge, &thread_id, "local:hello webhook").await;
    let store = SqliteSessionStore::open(session_store_path).await.unwrap();
    let coordinates = only_thread_coordinates(&bridge).await;
    let control_events = store
        .read_events(&control_stream_id(&coordinates), None)
        .await
        .unwrap();
    let thread_events = store
        .read_events(&EventStreamId::for_thread(&coordinates), None)
        .await
        .unwrap();
    let admission = crate::kernel::admission::assert_admission_precedes_turn_records(
        &control_events,
        &thread_events,
    );
    assert_eq!(admission.payload["route_id"], "telegram.bot:main");
}

#[tokio::test]
async fn telegram_webhook_auth_is_uniform_and_precedes_routing_and_payload_parsing() {
    let envelopes = Arc::new(TokioMutex::new(Vec::new()));
    let sink = Arc::new(CaptureSink {
        envelopes: envelopes.clone(),
    });
    let server = TelegramWebhookServer::bind(
        TelegramWebhookServerConfig {
            route_id: "main".to_string(),
            listen: "127.0.0.1:0".to_string(),
            path: "/telegram".to_string(),
            secret_token: "secret".to_string(),
        },
        sink,
    )
    .await
    .unwrap();
    let addr = server.local_addr().unwrap();
    tokio::spawn(server.serve());

    let missing = post_raw_json(addr, "/telegram", None, "{").await;
    let wrong = post_raw_json(addr, "/telegram", Some("wrong"), "{}").await;
    let hidden_route = post_raw_json(addr, "/not-telegram", None, "{}").await;
    let oversized_head = send_raw_request(
        addr,
        format!(
            "POST /telegram HTTP/1.1\r\nHost: {addr}\r\nX-Telegram-Bot-Api-Secret-Token: secret\r\nX-Padding: {}\r\nContent-Length: 2\r\n\r\n{{}}",
            "a".repeat(MAX_HTTP_HEADER_BYTES)
        ),
    )
    .await;

    assert!(missing.starts_with("HTTP/1.1 401 Unauthorized"));
    assert_eq!(wrong, missing);
    assert_eq!(hidden_route, missing);
    assert_eq!(oversized_head, missing);
    assert!(envelopes.lock().await.is_empty());

    let accepted = post_json(
        addr,
        "/telegram",
        Some("secret"),
        json!({
            "update_id": 1001,
            "message": {
                "message_id": 557,
                "chat": { "id": 789, "type": "private" },
                "date": 1777000000,
                "text": "authenticated webhook"
            }
        }),
    )
    .await;

    assert!(accepted.starts_with("HTTP/1.1 200 OK"));
    assert_eq!(envelopes.lock().await.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn telegram_webhook_times_out_a_stalled_request_head() {
    let (mut reader, mut writer) = tokio::io::duplex(1024);
    writer
        .write_all(b"POST /telegram HTTP/1.1\r\nHost: localhost\r\n")
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(60), read_http_request_head(&mut reader))
        .await
        .expect("request-head deadline did not fire within the hang-detector bound");
    let Err(err) = result else {
        panic!("partial request head unexpectedly parsed");
    };
    assert!(err.to_string().contains("request head timed out"));
    drop(writer);
}

#[tokio::test]
async fn telegram_webhook_serve_cancellation_aborts_accepted_requests() {
    let sink = Arc::new(BlockingSink {
        entered: Notify::new(),
        release: Notify::new(),
    });
    let server = TelegramWebhookServer::bind(
        TelegramWebhookServerConfig {
            route_id: "main".to_string(),
            listen: "127.0.0.1:0".to_string(),
            path: "/telegram".to_string(),
            secret_token: "secret".to_string(),
        },
        sink.clone(),
    )
    .await
    .unwrap();
    let addr = server.local_addr().unwrap();
    let server_task = tokio::spawn(server.serve());
    let entered = sink.entered.notified();
    let response_task = tokio::spawn(post_json(
        addr,
        "/telegram",
        Some("secret"),
        json!({
            "update_id": 1002,
            "message": {
                "message_id": 558,
                "chat": { "id": 790, "type": "private" },
                "date": 1777000000,
                "text": "cancel this request"
            }
        }),
    ));

    tokio::time::timeout(Duration::from_secs(5), entered)
        .await
        .expect("request did not reach the sink");
    server_task.abort();
    server_task.await.unwrap_err();
    sink.release.notify_one();

    let response = tokio::time::timeout(Duration::from_secs(5), response_task)
        .await
        .expect("accepted connection did not close after server cancellation")
        .unwrap();
    assert!(response.is_empty());
}

#[tokio::test]
async fn telegram_webhook_queue_mode_writes_to_sqlite() {
    let db = std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(format!("telegram-{}.sqlite", uuid::Uuid::now_v7()));
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "telegram"))
            .await
            .unwrap(),
    );
    let server = TelegramWebhookServer::bind(
        TelegramWebhookServerConfig {
            route_id: "main".to_string(),
            listen: "127.0.0.1:0".to_string(),
            path: "/telegram".to_string(),
            secret_token: "secret".to_string(),
        },
        queue.clone(),
    )
    .await
    .unwrap();
    let addr = server.local_addr().unwrap();
    tokio::spawn(server.serve());

    let response = post_json(
        addr,
        "/telegram",
        Some("secret"),
        json!({
            "update_id": 1000,
            "message": {
                "message_id": 556,
                "chat": { "id": 456, "type": "private" },
                "date": 1777000000,
                "text": "queued webhook"
            }
        }),
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 200 OK"));
    let leased = queue.lease_default("test", 1).await.unwrap();
    assert_eq!(leased.len(), 1);
    assert_eq!(
        leased[0].envelope.content.text_projection(),
        "queued webhook"
    );
    queue.complete_ingress(&leased[0].message_id).await.unwrap();
    let _ = std::fs::remove_file(db);
}

async fn post_json(
    addr: SocketAddr,
    path: &str,
    secret: Option<&str>,
    body: serde_json::Value,
) -> String {
    let body = body.to_string();
    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
        body.len()
    );
    if let Some(secret) = secret {
        request.push_str(&format!("X-Telegram-Bot-Api-Secret-Token: {secret}\r\n"));
    }
    request.push_str("\r\n");
    request.push_str(&body);

    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    response
}

async fn post_raw_json(addr: SocketAddr, path: &str, secret: Option<&str>, body: &str) -> String {
    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
        body.len()
    );
    if let Some(secret) = secret {
        request.push_str(&format!("X-Telegram-Bot-Api-Secret-Token: {secret}\r\n"));
    }
    request.push_str("\r\n");
    request.push_str(body);

    send_raw_request(addr, request).await
}

async fn send_raw_request(addr: SocketAddr, request: String) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    if let Err(err) = stream.read_to_string(&mut response).await {
        assert_eq!(err.kind(), std::io::ErrorKind::ConnectionReset);
        assert!(
            !response.is_empty(),
            "connection reset before HTTP response"
        );
    }
    response
}
