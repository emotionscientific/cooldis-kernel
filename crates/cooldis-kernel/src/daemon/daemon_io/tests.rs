use super::*;
use crate::{
    AppServerListenAddr, CooldisAppServerConfig, CooldisDaemonClockRoute, DaemonClock, EventKind,
    EventStore, MandateCatchUpPolicy, MandateSchedulePayload, MandateStartRequest, StreamCursorV1,
    TimerFiredPayload, control_stream_id, revoke_mandate, start_mandate,
};
use chrono::{DateTime, TimeZone, Utc};
use cooldis_io_core::{
    ConversationKind, DeliveryReceipt, IngressContent, IoActor, IoConversation, IoDedupeKey,
    IoProtocolAdapter, IoProtocolCapabilities, IoSource, IoTarget,
};
use cooldis_io_pgqrs::{PgqrsIngressQueue, PgqrsQueueConfig, sqlite_dsn};
use serde_json::json;
use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Mutex as StdMutex;
use tokio::sync::{Mutex as TokioMutex, mpsc};

#[derive(Clone)]
struct CaptureSink {
    envelopes: Arc<TokioMutex<Vec<IngressEnvelope>>>,
}

#[async_trait]
impl IngressSink for CaptureSink {
    async fn submit(&self, envelope: IngressEnvelope) -> IoResult<IngressAck> {
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

fn test_envelope(text: &str) -> IngressEnvelope {
    IngressEnvelope::new(
        IoSource::new("telegram.bot", "main"),
        IoConversation::new("telegram:chat:123", ConversationKind::Direct),
        IngressContent::text(text),
        now_ms(),
    )
}

fn telegram_queue_envelope(text: &str) -> IngressEnvelope {
    let source = IoSource::new("telegram.bot", "main");
    IngressEnvelope::new(
        source.clone(),
        IoConversation::new("telegram:chat:123", ConversationKind::Direct),
        IngressContent::text(text),
        now_ms(),
    )
    .with_actor(IoActor::new("telegram:user:42"))
    .with_dedupe_key(IoDedupeKey::for_source(&source, "update:999"))
    .with_metadata("cooldis_route_id", "main")
    .with_metadata("cooldis_route_policy", "queue_per_conversation")
    .with_metadata("telegram_message_id", "555")
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
        threading: None,
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

async fn register_route_state(
    bridge: &CooldisDaemonIoBridge,
    route: &CooldisIoRouteConfig,
    db: &Path,
) {
    bridge
        .register_egress_route_config("telegram.bot", "main", route)
        .await
        .unwrap();
    bridge
        .register_egress_state_sqlite_dsn("telegram.bot", "main", sqlite_dsn(db))
        .await
        .unwrap();
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
async fn egress_projection_strips_reaction_tag_and_preserves_order() {
    let route = route_with_egress(
        vec![crate::CooldisEgressProjectionRuleConfig {
            pattern: r"\[reaction:(?P<emoji>[^\]]+)\]".to_string(),
            action: "reaction".to_string(),
        }],
        None,
    );
    let config = RouteEgressConfig::from_route(&route).unwrap();

    let projected = config.project(test_egress("hello[reaction:👍] friend"));

    assert_eq!(projected.len(), 2);
    assert!(matches!(
        projected[0].kind,
        EgressKind::AssistantMessage { ref text } if text == "hello friend"
    ));
    assert!(matches!(
        projected[1].kind,
        EgressKind::PlatformAction { ref action, ref payload }
            if action == "reaction"
                && payload["emoji"] == "👍"
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
            pattern: r"\[reaction:(?P<emoji>[^\]]+)\]".to_string(),
            action: "reaction".to_string(),
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
    let store = SqliteSessionStore::open(server.session_store_path()).unwrap();
    let receipt = start_mandate(
        &store,
        &coordinates,
        MandateStartRequest {
            schedule: MandateSchedulePayload::Interval { every_ms: 60_000 },
            max_occurrences: Some(3),
            catch_up: Some(catch_up),
            input_template: Some("wake".to_string()),
            snapshot_id: None,
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
    let session_store = crate::SqliteSessionStore::open(&session_store_path).unwrap();
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
    let turn_submitted_count = thread_events
        .iter()
        .filter(|event| event.kind == crate::EventKind::TurnSubmitted)
        .count();
    assert_eq!(turn_submitted_count, 1);

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
        .last()
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

    assert_eq!(
        restarted
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
            secret_token: Some("secret".to_string()),
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
            secret_token: None,
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
        None,
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
