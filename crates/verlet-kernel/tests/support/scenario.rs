#![allow(dead_code)]

//! Scenario engine surface (ADR 0004, Decision 2).
//!
//! A scenario is one seeded, bounded run: an operation sequence and a fault
//! plan derived from the same seed, executed with every declared invariant
//! checked after every step (lexicon: scenario). This file fixes the
//! operation alphabet, the invariant contract, the failure receipt, and the
//! corpus entry format implemented by the generator, runner, and minimizer.
//!
//! Vocabulary-v1 derivation uses two stable lanes from the same
//! version-salted root as [`FaultPlan::derive`]: `scenario-op-count-v1` fixes
//! the bounded sequence length and `scenario-ops-v1` fixes operation draws.
//! Changing either label or the construction below is a vocabulary change.

use verlet::EventStore as _;

const SCENARIO_ASYNC_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const SCENARIO_ASYNC_RECHECK_INTERVAL: std::time::Duration = std::time::Duration::from_millis(1);

struct ScenarioRunRoot {
    path: std::path::PathBuf,
}

impl ScenarioRunRoot {
    fn new(seed: u64) -> Self {
        Self {
            path: std::env::temp_dir()
                .join("verlet-scenario-engine")
                .join(format!("{seed:016x}-{}", uuid::Uuid::now_v7())),
        }
    }
}

impl Drop for ScenarioRunRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

struct DynRuntimeStore {
    inner: std::sync::Arc<dyn verlet::RuntimeStore>,
    canonical_timestamp_ms: i64,
    control: std::sync::Arc<ScenarioStoreControl>,
}

#[derive(Default)]
struct ScenarioStoreControl {
    pause_next_turn_input: std::sync::atomic::AtomicBool,
    turn_input_started: tokio::sync::Notify,
    release_turn_input: tokio::sync::Notify,
}

impl DynRuntimeStore {
    fn kind(&self, kind: verlet::SessionEntryKind) -> verlet::SessionEntryKind {
        let mut value = serde_json::to_value(&kind).expect("serialize scenario session entry");
        replace_timestamp_ms(&mut value, self.canonical_timestamp_ms);
        serde_json::from_value(value).expect("deserialize deterministic scenario session entry")
    }

    fn events(&self, mut records: Vec<verlet::NewEventRecord>) -> Vec<verlet::NewEventRecord> {
        for record in &mut records {
            record.created_at_ms = self.canonical_timestamp_ms;
        }
        records
    }
}

fn replace_timestamp_ms(value: &mut serde_json::Value, timestamp_ms: i64) {
    match value {
        serde_json::Value::Object(object) => {
            for (key, value) in object {
                if key == "timestamp_ms" {
                    *value = serde_json::json!(timestamp_ms);
                } else {
                    replace_timestamp_ms(value, timestamp_ms);
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                replace_timestamp_ms(value, timestamp_ms);
            }
        }
        _ => {}
    }
}

#[async_trait::async_trait]
impl verlet::SessionStore for DynRuntimeStore {
    async fn append(
        &self,
        coordinates: &verlet::ThreadCoordinates,
        parent_entry_id: Option<verlet::SessionEntryId>,
        kind: verlet::SessionEntryKind,
    ) -> verlet::HistoryResult<verlet::SessionEntry> {
        self.inner
            .append(coordinates, parent_entry_id, self.kind(kind))
            .await
    }

    async fn append_with_provenance(
        &self,
        coordinates: &verlet::ThreadCoordinates,
        parent_entry_id: Option<verlet::SessionEntryId>,
        kind: verlet::SessionEntryKind,
        provenance: verlet::EventProvenance,
    ) -> verlet::HistoryResult<verlet::SessionEntry> {
        self.inner
            .append_with_provenance(coordinates, parent_entry_id, self.kind(kind), provenance)
            .await
    }

    async fn append_turn_input(
        &self,
        coordinates: &verlet::ThreadCoordinates,
        turn_id: &str,
        kind: verlet::SessionEntryKind,
    ) -> verlet::HistoryResult<verlet::SessionEntry> {
        if self
            .control
            .pause_next_turn_input
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.control.turn_input_started.notify_one();
            self.control.release_turn_input.notified().await;
        }
        self.inner
            .append_turn_input(coordinates, turn_id, self.kind(kind))
            .await
    }

    async fn active_leaf(
        &self,
        coordinates: &verlet::ThreadCoordinates,
    ) -> verlet::HistoryResult<Option<verlet::SessionEntryId>> {
        self.inner.active_leaf(coordinates).await
    }

    async fn select_branch(
        &self,
        coordinates: &verlet::ThreadCoordinates,
        leaf_entry_id: Option<verlet::SessionEntryId>,
    ) -> verlet::HistoryResult<()> {
        self.inner.select_branch(coordinates, leaf_entry_id).await
    }

    async fn build_context(
        &self,
        coordinates: &verlet::ThreadCoordinates,
    ) -> verlet::HistoryResult<verlet::SessionContext> {
        self.inner.build_context(coordinates).await
    }

    async fn clone_branch(
        &self,
        source_coordinates: &verlet::ThreadCoordinates,
        source_leaf: Option<verlet::SessionEntryId>,
        target_coordinates: &verlet::ThreadCoordinates,
    ) -> verlet::HistoryResult<Option<verlet::SessionEntryId>> {
        self.inner
            .clone_branch(source_coordinates, source_leaf, target_coordinates)
            .await
    }

    async fn fork_by_reference(
        &self,
        source_coordinates: &verlet::ThreadCoordinates,
        target_coordinates: &verlet::ThreadCoordinates,
        base: verlet::ThreadBaseRef,
    ) -> verlet::HistoryResult<()> {
        self.inner
            .fork_by_reference(source_coordinates, target_coordinates, base)
            .await
    }
}

#[async_trait::async_trait]
impl verlet::EventStore for DynRuntimeStore {
    async fn append_events(
        &self,
        stream_id: &verlet::EventStreamId,
        records: Vec<verlet::NewEventRecord>,
    ) -> verlet::HistoryResult<Vec<verlet::EventRecord>> {
        self.inner
            .append_events(stream_id, self.events(records))
            .await
    }

    async fn append_events_fenced(
        &self,
        stream_id: &verlet::EventStreamId,
        expected_next_sequence: verlet::EventSequence,
        records: Vec<verlet::NewEventRecord>,
    ) -> verlet::HistoryResult<Vec<verlet::EventRecord>> {
        self.inner
            .append_events_fenced(stream_id, expected_next_sequence, self.events(records))
            .await
    }

    async fn read_events(
        &self,
        stream_id: &verlet::EventStreamId,
        from_sequence: Option<verlet::EventSequence>,
    ) -> verlet::HistoryResult<Vec<verlet::EventRecord>> {
        self.inner.read_events(stream_id, from_sequence).await
    }

    async fn read_events_after_cursor(
        &self,
        stream_id: &verlet::EventStreamId,
        cursor: &verlet::StreamCursorV1,
    ) -> verlet::HistoryResult<Vec<verlet::EventRecord>> {
        self.inner.read_events_after_cursor(stream_id, cursor).await
    }
}

#[async_trait::async_trait]
impl verlet::ObservationStore for DynRuntimeStore {
    async fn append_observation(
        &self,
        record: verlet::NewObservationRecord,
    ) -> verlet::HistoryResult<verlet::ObservationRecord> {
        self.inner.append_observation(record).await
    }

    async fn list_observations(
        &self,
        scope: &verlet::ThreadCoordinates,
        kind: Option<&str>,
    ) -> verlet::HistoryResult<Vec<verlet::ObservationRecord>> {
        self.inner.list_observations(scope, kind).await
    }
}

struct ScenarioProvider {
    inner: verlet::LocalOfflineProviderClient,
    pause_next_complete: std::sync::atomic::AtomicBool,
    complete_started: tokio::sync::Notify,
}

impl ScenarioProvider {
    fn new() -> Self {
        Self {
            inner: verlet::LocalOfflineProviderClient::new(
                verlet::APP_SERVER_LOCAL_PROVIDER,
                verlet::APP_SERVER_LOCAL_MODEL,
            ),
            pause_next_complete: std::sync::atomic::AtomicBool::new(false),
            complete_started: tokio::sync::Notify::new(),
        }
    }
}

#[async_trait::async_trait]
impl verlet::ProviderClient for ScenarioProvider {
    fn capabilities(&self) -> Option<verlet::ProviderCapabilityRecord> {
        self.inner.capabilities()
    }

    async fn complete(
        &self,
        request: &verlet::ProviderRequest,
    ) -> verlet::ProviderResult<verlet::ProviderResponse> {
        if self
            .pause_next_complete
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.complete_started.notify_one();
            std::future::pending::<()>().await;
        }
        self.inner.complete(request).await
    }
}

#[derive(Clone, Debug, serde::Serialize)]
struct QueueLeaseReceipt {
    message_id: String,
    attempt: u32,
    visible_until_tick: u64,
}

#[derive(Clone, Debug, serde::Serialize)]
struct QueueCompleteReceipt {
    message_id: String,
    attempt: u32,
    tick: u64,
}

enum QueueProbeReceipt {
    Lease(QueueLeaseReceipt),
    Complete(QueueCompleteReceipt),
}

#[derive(Default)]
struct QueueProbeLog {
    receipts: Vec<QueueProbeReceipt>,
}

struct ScenarioQueuedMessage {
    message_id: String,
    envelope: verlet_io_core::IngressEnvelope,
    attempt: u32,
    visible_at_tick: u64,
    completed: bool,
}

#[derive(Default)]
struct ScenarioQueue {
    messages: tokio::sync::Mutex<Vec<ScenarioQueuedMessage>>,
    tick: std::sync::Arc<std::sync::atomic::AtomicU64>,
    pause_before_next_complete: std::sync::atomic::AtomicBool,
    before_complete_started: tokio::sync::Notify,
    release_before_complete: tokio::sync::Notify,
    pause_next_complete: std::sync::atomic::AtomicBool,
    complete_started: tokio::sync::Notify,
}

impl ScenarioQueue {
    fn new(tick: std::sync::Arc<std::sync::atomic::AtomicU64>) -> Self {
        Self {
            messages: tokio::sync::Mutex::new(Vec::new()),
            tick,
            pause_before_next_complete: std::sync::atomic::AtomicBool::new(false),
            before_complete_started: tokio::sync::Notify::new(),
            release_before_complete: tokio::sync::Notify::new(),
            pause_next_complete: std::sync::atomic::AtomicBool::new(false),
            complete_started: tokio::sync::Notify::new(),
        }
    }

    async fn pending_count(&self) -> usize {
        self.messages
            .lock()
            .await
            .iter()
            .filter(|message| !message.completed)
            .count()
    }

    async fn lease_attempts(&self) -> Vec<u32> {
        self.messages
            .lock()
            .await
            .iter()
            .filter(|message| !message.completed)
            .map(|message| message.attempt)
            .collect()
    }
}

#[async_trait::async_trait]
impl verlet_io_core::IngressSink for ScenarioQueue {
    async fn submit(
        &self,
        envelope: verlet_io_core::IngressEnvelope,
    ) -> verlet_io_core::IoResult<verlet_io_core::IngressAck> {
        let ack = verlet_io_core::IngressAck::accepted(&envelope);
        let mut messages = self.messages.lock().await;
        if messages
            .iter()
            .any(|message| message.envelope.dedupe_key == envelope.dedupe_key && !message.completed)
        {
            return Ok(verlet_io_core::IngressAck::rejected(
                &envelope,
                "duplicate dedupe key",
            ));
        }
        messages.push(ScenarioQueuedMessage {
            message_id: envelope.id.clone(),
            envelope,
            attempt: 0,
            visible_at_tick: self.tick.load(std::sync::atomic::Ordering::SeqCst),
            completed: false,
        });
        Ok(ack)
    }
}

#[async_trait::async_trait]
impl verlet_io_core::IngressQueueStore for ScenarioQueue {
    async fn lease_ingress(
        &self,
        worker_id: &str,
        max_messages: usize,
        visibility_timeout_secs: u32,
    ) -> verlet_io_core::IoResult<Vec<verlet_io_core::LeasedIngressEnvelope>> {
        let now = self.tick.load(std::sync::atomic::Ordering::SeqCst);
        let mut messages = self.messages.lock().await;
        let mut leased = Vec::new();
        for message in messages
            .iter_mut()
            .filter(|message| !message.completed && message.visible_at_tick <= now)
        {
            if leased.len() == max_messages {
                break;
            }
            message.attempt += 1;
            message.visible_at_tick = now + u64::from(visibility_timeout_secs);
            let mut item = verlet_io_core::LeasedIngressEnvelope::new(
                message.message_id.clone(),
                message.envelope.clone(),
            );
            item.attempt = message.attempt;
            item.lease_owner = Some(worker_id.to_string());
            leased.push(item);
        }
        Ok(leased)
    }

    async fn complete_ingress(&self, message_id: &str) -> verlet_io_core::IoResult<()> {
        if self
            .pause_before_next_complete
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.before_complete_started.notify_one();
            self.release_before_complete.notified().await;
        }
        if self
            .pause_next_complete
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.complete_started.notify_one();
            std::future::pending::<()>().await;
        }
        if let Some(message) = self
            .messages
            .lock()
            .await
            .iter_mut()
            .find(|message| message.message_id == message_id)
        {
            message.completed = true;
        }
        Ok(())
    }

    async fn hold_ingress_until(
        &self,
        message_id: &str,
        visible_at_ms: u64,
    ) -> verlet_io_core::IoResult<()> {
        if let Some(message) = self
            .messages
            .lock()
            .await
            .iter_mut()
            .find(|message| message.message_id == message_id)
        {
            message.visible_at_tick = visible_at_ms.div_ceil(1_000);
        }
        Ok(())
    }

    async fn retry_ingress(&self, message_id: &str, _reason: &str) -> verlet_io_core::IoResult<()> {
        if let Some(message) = self
            .messages
            .lock()
            .await
            .iter_mut()
            .find(|message| message.message_id == message_id)
        {
            message.visible_at_tick = self.tick.load(std::sync::atomic::Ordering::SeqCst);
        }
        Ok(())
    }
}

struct ProbedIngressQueue<Q> {
    inner: std::sync::Arc<Q>,
    probes: std::sync::Arc<std::sync::Mutex<QueueProbeLog>>,
    tick: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl<Q> ProbedIngressQueue<Q> {
    fn new(
        inner: std::sync::Arc<Q>,
        probes: std::sync::Arc<std::sync::Mutex<QueueProbeLog>>,
        tick: std::sync::Arc<std::sync::atomic::AtomicU64>,
    ) -> Self {
        Self {
            inner,
            probes,
            tick,
        }
    }
}

#[async_trait::async_trait]
impl<Q: verlet_io_core::IngressQueueStore + 'static> verlet_io_core::IngressSink
    for ProbedIngressQueue<Q>
{
    async fn submit(
        &self,
        envelope: verlet_io_core::IngressEnvelope,
    ) -> verlet_io_core::IoResult<verlet_io_core::IngressAck> {
        self.inner.submit(envelope).await
    }
}

#[async_trait::async_trait]
impl<Q: verlet_io_core::IngressQueueStore + 'static> verlet_io_core::IngressQueueStore
    for ProbedIngressQueue<Q>
{
    async fn lease_ingress(
        &self,
        worker_id: &str,
        max_messages: usize,
        visibility_timeout_secs: u32,
    ) -> verlet_io_core::IoResult<Vec<verlet_io_core::LeasedIngressEnvelope>> {
        let leased = self
            .inner
            .lease_ingress(worker_id, max_messages, visibility_timeout_secs)
            .await?;
        let now = self.tick.load(std::sync::atomic::Ordering::SeqCst);
        let mut probes = self.probes.lock().unwrap();
        for message in &leased {
            probes
                .receipts
                .push(QueueProbeReceipt::Lease(QueueLeaseReceipt {
                    message_id: message.message_id.clone(),
                    attempt: message.attempt,
                    visible_until_tick: now + u64::from(visibility_timeout_secs),
                }));
        }
        Ok(leased)
    }

    async fn complete_ingress(&self, message_id: &str) -> verlet_io_core::IoResult<()> {
        self.inner.complete_ingress(message_id).await?;
        let tick = self.tick.load(std::sync::atomic::Ordering::SeqCst);
        let mut probes = self.probes.lock().unwrap();
        let attempt = probes
            .receipts
            .iter()
            .rev()
            .find_map(|receipt| match receipt {
                QueueProbeReceipt::Lease(lease) if lease.message_id == message_id => {
                    Some(lease.attempt)
                }
                _ => None,
            })
            .expect("accepted scenario queue completion must follow a lease");
        probes
            .receipts
            .push(QueueProbeReceipt::Complete(QueueCompleteReceipt {
                message_id: message_id.to_string(),
                attempt,
                tick,
            }));
        Ok(())
    }

    async fn hold_ingress_until(
        &self,
        message_id: &str,
        visible_at_ms: u64,
    ) -> verlet_io_core::IoResult<()> {
        self.inner
            .hold_ingress_until(message_id, visible_at_ms)
            .await
    }

    async fn retry_ingress(&self, message_id: &str, reason: &str) -> verlet_io_core::IoResult<()> {
        self.inner.retry_ingress(message_id, reason).await
    }
}

#[derive(Default)]
struct EmptyIngressQueue;

#[async_trait::async_trait]
impl verlet_io_core::IngressSink for EmptyIngressQueue {
    async fn submit(
        &self,
        envelope: verlet_io_core::IngressEnvelope,
    ) -> verlet_io_core::IoResult<verlet_io_core::IngressAck> {
        Ok(verlet_io_core::IngressAck::accepted(&envelope))
    }
}

#[async_trait::async_trait]
impl verlet_io_core::IngressQueueStore for EmptyIngressQueue {
    async fn lease_ingress(
        &self,
        _worker_id: &str,
        _max_messages: usize,
        _visibility_timeout_secs: u32,
    ) -> verlet_io_core::IoResult<Vec<verlet_io_core::LeasedIngressEnvelope>> {
        Ok(Vec::new())
    }

    async fn complete_ingress(&self, _message_id: &str) -> verlet_io_core::IoResult<()> {
        Ok(())
    }

    async fn hold_ingress_until(
        &self,
        _message_id: &str,
        _visible_at_ms: u64,
    ) -> verlet_io_core::IoResult<()> {
        Ok(())
    }

    async fn retry_ingress(
        &self,
        _message_id: &str,
        _reason: &str,
    ) -> verlet_io_core::IoResult<()> {
        Ok(())
    }
}

fn clone_plan(
    plan: &crate::support::fault_plan::FaultPlan,
) -> crate::support::fault_plan::FaultPlan {
    crate::support::fault_plan::FaultPlan {
        seed: plan.seed,
        vocabulary_version: plan.vocabulary_version,
        intensity: plan.intensity,
        directives: plan.directives.clone(),
    }
}

fn scenario_route() -> verlet::VerletIoRouteConfig {
    verlet::VerletIoRouteConfig {
        id: "scenario".to_string(),
        kind: "scenario".to_string(),
        enabled: true,
        policy: None,
        content_policies: None,
        threading: None,
        agent_ref: None,
        coalesce_bursts: None,
        ingress: None,
        egress_projection: Vec::new(),
        typing_simulation: None,
        egress_retry: verlet::VerletEgressRetryConfig::default(),
        telegram: None,
        metadata: std::collections::BTreeMap::new(),
    }
}

struct ScenarioHarness {
    root: std::path::PathBuf,
    route_db: std::path::PathBuf,
    server: verlet::VerletAppServer,
    bridge: verlet::VerletDaemonIoBridge,
    queue: std::sync::Arc<dyn verlet_io_core::IngressQueueStore>,
    queue_inner: std::sync::Arc<ScenarioQueue>,
    probes: std::sync::Arc<std::sync::Mutex<QueueProbeLog>>,
    probe_cursor: usize,
    tick: std::sync::Arc<std::sync::atomic::AtomicU64>,
    store_control: std::sync::Arc<ScenarioStoreControl>,
    runtime_store: std::sync::Arc<dyn verlet::RuntimeStore>,
    raw_store: verlet::SqliteSessionStore,
    provider: std::sync::Arc<ScenarioProvider>,
    projector_host: verlet::RuntimeHost,
    plan: crate::support::fault_plan::FaultPlan,
    transcript: crate::support::transcript::TypedTranscript,
    coordinates: Vec<verlet::ThreadCoordinates>,
    collected: std::collections::BTreeMap<String, i64>,
    current_root: usize,
    root_count: usize,
    envelope_index: usize,
    runtime_generation: usize,
    active_runtime_ids: std::collections::BTreeMap<String, String>,
    process_cut_index: usize,
    shut_down: bool,
}

impl ScenarioHarness {
    async fn build(
        root: std::path::PathBuf,
        plan: crate::support::fault_plan::FaultPlan,
        clean: bool,
        surviving_queue: Option<std::sync::Arc<ScenarioQueue>>,
    ) -> Self {
        if clean {
            let _ = std::fs::remove_dir_all(&root);
        }
        std::fs::create_dir_all(&root).expect("create scenario fixture root");
        let route_db = root.join("route.sqlite3");
        let probes = std::sync::Arc::new(std::sync::Mutex::new(QueueProbeLog::default()));
        let tick = surviving_queue
            .as_ref()
            .map(|queue| std::sync::Arc::clone(&queue.tick))
            .unwrap_or_else(|| std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)));
        let queue_inner = surviving_queue.unwrap_or_else(|| {
            std::sync::Arc::new(ScenarioQueue::new(std::sync::Arc::clone(&tick)))
        });
        let probed_queue = std::sync::Arc::new(ProbedIngressQueue::new(
            std::sync::Arc::clone(&queue_inner),
            std::sync::Arc::clone(&probes),
            std::sync::Arc::clone(&tick),
        ));
        let provider_control = std::sync::Arc::new(ScenarioProvider::new());
        let applied = plan.apply(
            std::sync::Arc::new(verlet::InMemorySessionStore::new()),
            probed_queue,
            std::sync::Arc::clone(&provider_control),
        );
        let queue: std::sync::Arc<dyn verlet_io_core::IngressQueueStore> =
            std::sync::Arc::new(applied.queue);
        let provider: std::sync::Arc<dyn verlet::ProviderClient> =
            std::sync::Arc::new(applied.provider);
        let runtime_config = verlet::AgentLoopConfig::new(
            verlet::ProviderApi::Other(verlet::APP_SERVER_LOCAL_PROVIDER.to_string()),
            verlet::APP_SERVER_LOCAL_PROVIDER,
            verlet::APP_SERVER_LOCAL_MODEL,
        );
        let runtime_factory: std::sync::Arc<dyn verlet::AgentRuntimeFactory> =
            std::sync::Arc::new(verlet::AgentLoopFactory::new(runtime_config, provider));

        let socket = root.join("app-server.sock");
        let listen = verlet::AppServerListenAddr::parse(&format!("unix://{}", socket.display()))
            .expect("scenario app-server listen address");
        let mut config = verlet::VerletAppServerConfig::local(listen, "/workspace");
        config.runtime_home = root.join("runtime");
        config.state_home = root.join("state");
        config.user_state_home = root.join("user-state");
        config.agent_registry_root = root.join("agent-registry");
        config.blob_registry_root = root.join("blob-registry");
        config.skill_registry_root = root.join("skill-registry");
        config.capsule_bindings.registry_root = None;
        config.tenant_id = format!("scenario-{:016x}", plan.seed);
        config.user_id = "scenario-user".to_string();
        config.console_principal =
            Some(verlet::daemon::identity::PrincipalId::new("scenario-user"));

        let decorated_slot = std::sync::Arc::new(std::sync::Mutex::new(
            None::<std::sync::Arc<dyn verlet::RuntimeStore>>,
        ));
        let decorated_capture = std::sync::Arc::clone(&decorated_slot);
        let store_control = std::sync::Arc::new(ScenarioStoreControl::default());
        let store_control_capture = std::sync::Arc::clone(&store_control);
        let store_plan = clone_plan(&plan);
        let server = crate::support::scenario_app_server(
            config,
            std::sync::Arc::clone(&runtime_factory),
            move |raw| {
                let canonical_timestamp_ms = store_plan.seed as i64;
                let applied = store_plan.apply(
                    std::sync::Arc::new(DynRuntimeStore {
                        inner: raw,
                        canonical_timestamp_ms,
                        control: store_control_capture,
                    }),
                    std::sync::Arc::new(EmptyIngressQueue),
                    std::sync::Arc::new(verlet::LocalOfflineProviderClient::new(
                        verlet::APP_SERVER_LOCAL_PROVIDER,
                        verlet::APP_SERVER_LOCAL_MODEL,
                    )),
                );
                let decorated: std::sync::Arc<dyn verlet::RuntimeStore> =
                    std::sync::Arc::new(applied.store);
                *decorated_capture.lock().unwrap() = Some(std::sync::Arc::clone(&decorated));
                decorated
            },
        )
        .await
        .expect("build decorated scenario app server");
        let runtime_store = decorated_slot
            .lock()
            .unwrap()
            .clone()
            .expect("session-store decorator should capture the installed store");
        let projector_host = verlet::RuntimeHost::with_session_store(
            std::sync::Arc::clone(&runtime_factory),
            std::sync::Arc::clone(&runtime_store),
        );
        let raw_store = verlet::SqliteSessionStore::open(server.session_store_path())
            .await
            .expect("open scenario store for durable probes");
        let bridge = verlet::VerletDaemonIoBridge::from_app_server(&server);
        let route = scenario_route();
        bridge
            .register_egress_route_config("scenario", "scenario", &route)
            .await
            .expect("register scenario route config");
        bridge
            .register_egress_state_sqlite_dsn(
                "scenario",
                "scenario",
                verlet_io_pgqrs::sqlite_dsn(&route_db),
            )
            .await
            .expect("register scenario route state");

        Self {
            root,
            route_db,
            server,
            bridge,
            queue,
            queue_inner,
            probes,
            probe_cursor: 0,
            tick,
            store_control,
            runtime_store,
            raw_store,
            provider: provider_control,
            projector_host,
            plan,
            transcript: crate::support::transcript::TypedTranscript::new(),
            coordinates: Vec::new(),
            collected: std::collections::BTreeMap::new(),
            current_root: 0,
            root_count: 0,
            envelope_index: 0,
            runtime_generation: 0,
            active_runtime_ids: std::collections::BTreeMap::new(),
            process_cut_index: 0,
            shut_down: false,
        }
    }

    fn deterministic_thread_id(&self, index: usize) -> verlet::ThreadId {
        let value = (u128::from(self.plan.seed) << 64) | 0x5343_454e_0000_0000u128 | index as u128;
        verlet::ThreadId::parse_str(&uuid::Uuid::from_u128(value).to_string()).unwrap()
    }

    fn source() -> verlet_io_core::IoSource {
        verlet_io_core::IoSource::new("scenario", "scenario")
    }

    fn conversation(index: usize) -> verlet_io_core::IoConversation {
        verlet_io_core::IoConversation::new(
            format!("scenario:conversation:{index}"),
            verlet_io_core::ConversationKind::Direct,
        )
    }

    fn session_id(index: usize) -> String {
        format!(
            "io:{}:{}",
            Self::source().stable_scope(),
            Self::conversation(index).stable_key()
        )
    }

    fn root_coordinates(&self, index: usize) -> verlet::ThreadCoordinates {
        verlet::ThreadCoordinates {
            tenant_id: self.server.tenant_id().to_string(),
            user_id: self.server.user_id().to_string(),
            session_id: Self::session_id(index),
            thread_id: self.deterministic_thread_id(index + 1),
        }
    }

    fn reserve_root(&mut self, index: usize) -> verlet::ThreadCoordinates {
        let coordinates = self.root_coordinates(index);
        let address = verlet_io_core::ThreadAddress::new(
            coordinates.tenant_id.clone(),
            coordinates.user_id.clone(),
            coordinates.session_id.clone(),
        );
        let connection = rusqlite::Connection::open(&self.route_db)
            .expect("open scenario route state for reservation");
        connection
            .execute(
                "INSERT OR IGNORE INTO cooldis_daemon_ingress_bindings (
                    route_id, source_scope, scope_key, tenant_id, user_id, session_id, thread_id, updated_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                rusqlite::params![
                    "scenario",
                    Self::source().stable_scope(),
                    address.scope_key(),
                    coordinates.tenant_id,
                    coordinates.user_id,
                    coordinates.session_id,
                    coordinates.thread_id.to_string(),
                    0i64,
                ],
            )
            .expect("commit scenario initial-route reservation");
        let observed: String = connection
            .query_row(
                "SELECT thread_id FROM cooldis_daemon_ingress_bindings
                 WHERE route_id = ?1 AND source_scope = ?2 AND scope_key = ?3",
                rusqlite::params![
                    "scenario",
                    Self::source().stable_scope(),
                    address.scope_key(),
                ],
                |row| row.get(0),
            )
            .expect("observe committed scenario route row");
        assert_eq!(observed, coordinates.thread_id.to_string());
        self.transcript.push_receipt(
            "thread.reservation",
            &serde_json::json!({
                "kind": "thread.reservation",
                "thread_id": observed,
                "reservation_kind": "initial_route",
            }),
        );
        coordinates
    }

    fn replace_root_reservation_after_cut(&mut self, index: usize) -> Option<String> {
        let coordinates = self.root_coordinates(index);
        let address = verlet_io_core::ThreadAddress::new(
            coordinates.tenant_id.clone(),
            coordinates.user_id.clone(),
            coordinates.session_id.clone(),
        );
        let connection = rusqlite::Connection::open(&self.route_db)
            .expect("open scenario route state after ingress-binding cut");
        let previous = connection
            .query_row(
                "SELECT thread_id FROM cooldis_daemon_ingress_bindings
                 WHERE route_id = ?1 AND source_scope = ?2 AND scope_key = ?3",
                rusqlite::params![
                    "scenario",
                    Self::source().stable_scope(),
                    address.scope_key(),
                ],
                |row| row.get::<_, String>(0),
            )
            .ok();
        connection
            .execute(
                "INSERT INTO cooldis_daemon_ingress_bindings (
                    route_id, source_scope, scope_key, tenant_id, user_id, session_id, thread_id, updated_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(route_id, source_scope, scope_key) DO UPDATE SET
                    tenant_id = excluded.tenant_id,
                    user_id = excluded.user_id,
                    session_id = excluded.session_id,
                    thread_id = excluded.thread_id,
                    updated_at_ms = excluded.updated_at_ms",
                rusqlite::params![
                    "scenario",
                    Self::source().stable_scope(),
                    address.scope_key(),
                    coordinates.tenant_id,
                    coordinates.user_id,
                    coordinates.session_id,
                    coordinates.thread_id.to_string(),
                    0i64,
                ],
            )
            .expect("commit deterministic route reservation after cut");
        self.transcript.push_receipt(
            "thread.reservation",
            &serde_json::json!({
                "kind": "thread.reservation",
                "thread_id": coordinates.thread_id,
                "reservation_kind": "initial_route",
            }),
        );
        previous
    }

    fn rebind_root_to_child(&self, index: usize, coordinates: &verlet::ThreadCoordinates) {
        let address = verlet_io_core::ThreadAddress::new(
            coordinates.tenant_id.clone(),
            coordinates.user_id.clone(),
            Self::session_id(index),
        );
        let mut connection = rusqlite::Connection::open(&self.route_db)
            .expect("open scenario route state for child rebind");
        let tx = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .expect("lock scenario route state for child rebind");
        let updated = tx
            .execute(
                "UPDATE cooldis_daemon_ingress_bindings
                 SET tenant_id = ?4, user_id = ?5, session_id = ?6, thread_id = ?7, updated_at_ms = ?8
                 WHERE route_id = ?1 AND source_scope = ?2 AND scope_key = ?3",
                rusqlite::params![
                    "scenario",
                    Self::source().stable_scope(),
                    address.scope_key(),
                    coordinates.tenant_id,
                    coordinates.user_id,
                    coordinates.session_id,
                    coordinates.thread_id.to_string(),
                    0i64,
                ],
            )
            .expect("rebind scenario ingress route to child");
        assert_eq!(
            updated, 1,
            "scenario root reservation must exist before fork"
        );
        tx.execute(
            "INSERT INTO cooldis_daemon_egress_threads (
                route_id, source_scope, scope_key, tenant_id, user_id, session_id, thread_id, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(route_id, thread_id) DO UPDATE SET
                source_scope = excluded.source_scope,
                scope_key = excluded.scope_key,
                tenant_id = excluded.tenant_id,
                user_id = excluded.user_id,
                session_id = excluded.session_id,
                updated_at_ms = excluded.updated_at_ms",
            rusqlite::params![
                "scenario",
                Self::source().stable_scope(),
                address.scope_key(),
                coordinates.tenant_id,
                coordinates.user_id,
                coordinates.session_id,
                coordinates.thread_id.to_string(),
                0i64,
            ],
        )
        .expect("project scenario child into egress route state");
        tx.commit().expect("commit scenario child route rebind");
    }

    fn envelope(
        &mut self,
        root_index: usize,
        policy: &str,
        text: &str,
    ) -> verlet_io_core::IngressEnvelope {
        self.envelope_index += 1;
        let source = Self::source();
        let delivery_id = format!("{}:{}", self.plan.seed, self.envelope_index);
        let mut envelope = verlet_io_core::IngressEnvelope::new(
            source.clone(),
            Self::conversation(root_index),
            verlet_io_core::IngressContent::text(text),
            self.tick.load(std::sync::atomic::Ordering::SeqCst) * 1_000,
        )
        .with_dedupe_key(verlet_io_core::IoDedupeKey::for_source(
            &source,
            delivery_id.clone(),
        ))
        .with_delivery(verlet_io_core::IoDelivery::new(delivery_id))
        .with_principal(verlet_io_core::IoPrincipal::new(
            self.server.tenant_id(),
            self.server.user_id(),
            "route:scenario",
        ))
        .with_metadata("cooldis_route_id", "scenario")
        .with_metadata("cooldis_route_policy", policy);
        envelope.id = format!(
            "scenario-ingress-{}-{}",
            self.plan.seed, self.envelope_index
        );
        envelope
    }

    fn bound_coordinates(&self, root_index: usize) -> Option<verlet::ThreadCoordinates> {
        let address = verlet_io_core::ThreadAddress::new(
            self.server.tenant_id(),
            self.server.user_id(),
            Self::session_id(root_index),
        );
        let connection = rusqlite::Connection::open(&self.route_db).ok()?;
        connection
            .query_row(
                "SELECT tenant_id, user_id, session_id, thread_id
                 FROM cooldis_daemon_ingress_bindings
                 WHERE route_id = ?1 AND source_scope = ?2 AND scope_key = ?3",
                rusqlite::params![
                    "scenario",
                    Self::source().stable_scope(),
                    address.scope_key(),
                ],
                |row| {
                    let thread_id: String = row.get(3)?;
                    Ok(verlet::ThreadCoordinates {
                        tenant_id: row.get(0)?,
                        user_id: row.get(1)?,
                        session_id: row.get(2)?,
                        thread_id: verlet::ThreadId::parse_str(&thread_id).map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                3,
                                rusqlite::types::Type::Text,
                                Box::new(error),
                            )
                        })?,
                    })
                },
            )
            .ok()
    }

    fn bound_root_index(&self, coordinates: &verlet::ThreadCoordinates) -> Option<usize> {
        (0..self.root_count).find(|index| {
            self.bound_coordinates(*index)
                .is_some_and(|bound| bound.thread_id == coordinates.thread_id)
        })
    }

    async fn wait_for_idle(&self, coordinates: &verlet::ThreadCoordinates) {
        let Ok(handle) = self.server.supervisor().get_thread_at(coordinates).await else {
            return;
        };
        let mut status = handle.subscribe_status();
        let started = std::time::Instant::now();
        loop {
            if matches!(
                *status.borrow(),
                verlet::ThreadStatus::Idle
                    | verlet::ThreadStatus::Stopped
                    | verlet::ThreadStatus::Failed
            ) && handle.queued_command_count() == 0
            {
                return;
            }
            assert!(
                started.elapsed() < SCENARIO_ASYNC_WAIT_TIMEOUT,
                "scenario thread {coordinates:?} did not become quiescent within {SCENARIO_ASYNC_WAIT_TIMEOUT:?}; status={:?}, queued_commands={}",
                *status.borrow(),
                handle.queued_command_count(),
            );
            tokio::select! {
                biased;
                changed = status.changed() => {
                    assert!(
                        changed.is_ok(),
                        "scenario thread {coordinates:?} closed its status channel before becoming quiescent"
                    );
                }
                _ = tokio::time::sleep(SCENARIO_ASYNC_RECHECK_INTERVAL) => {}
            }
        }
    }

    async fn wait_for_turn_input(
        &self,
        coordinates: &verlet::ThreadCoordinates,
        events: &mut tokio::sync::broadcast::Receiver<verlet::ThreadEvent>,
        turn_id: &str,
    ) {
        loop {
            match events.recv().await {
                Ok(verlet::ThreadEvent::CanonicalMirror { entry, .. })
                    if entry.turn_id.as_deref() == Some(turn_id) =>
                {
                    return;
                }
                Ok(verlet::ThreadEvent::Failed { .. }) => {
                    self.require_durable_turn_input(coordinates, turn_id, "failed")
                        .await;
                    return;
                }
                Ok(verlet::ThreadEvent::Stopped { .. }) => {
                    self.require_durable_turn_input(coordinates, turn_id, "stopped")
                        .await;
                    return;
                }
                Ok(verlet::ThreadEvent::Cancelled { .. }) => {
                    self.require_durable_turn_input(coordinates, turn_id, "cancelled")
                        .await;
                    return;
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    if self.turn_input_is_durable(coordinates, turn_id).await {
                        return;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    self.require_durable_turn_input(coordinates, turn_id, "event channel closed")
                        .await;
                    return;
                }
                Ok(_) => {}
            }
        }
    }

    async fn turn_input_is_durable(
        &self,
        coordinates: &verlet::ThreadCoordinates,
        turn_id: &str,
    ) -> bool {
        let stream_id = verlet::EventStreamId::for_thread(coordinates);
        self.raw_store
            .read_events(&stream_id, None)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "read durable scenario stream {stream_id} for thread {coordinates:?} and turn {turn_id:?}: {error}"
                )
            })
            .into_iter()
            .any(|event| {
                event.kind == verlet::EventKind::SessionEntryAppended
                    && event.payload.get("turn_id").and_then(serde_json::Value::as_str)
                        == Some(turn_id)
            })
    }

    async fn require_durable_turn_input(
        &self,
        coordinates: &verlet::ThreadCoordinates,
        turn_id: &str,
        terminal: &str,
    ) {
        assert!(
            self.turn_input_is_durable(coordinates, turn_id).await,
            "scenario thread {coordinates:?} {terminal} before steer input {turn_id:?} became durable"
        );
    }

    async fn append_placement(
        &mut self,
        coordinates: &verlet::ThreadCoordinates,
        state: &str,
        resident_state: Option<&str>,
    ) {
        let thread_id = coordinates.thread_id.to_string();
        let runtime_id = if state == "active" {
            let runtime_id = format!(
                "scenario-runtime-{}-{}",
                coordinates.thread_id, self.runtime_generation
            );
            self.active_runtime_ids
                .insert(thread_id.clone(), runtime_id.clone());
            runtime_id
        } else {
            self.active_runtime_ids
                .remove(&thread_id)
                .unwrap_or_else(|| {
                    format!(
                        "scenario-runtime-{}-{}",
                        coordinates.thread_id, self.runtime_generation
                    )
                })
        };
        let record = || {
            let reservation_key = format!("thread:{}", coordinates.thread_id);
            let mut payload = serde_json::json!({
                "runtime_id": runtime_id,
                "runtime_state": state,
                "reservation_progress": reservation_key,
            });
            if let Some(resident_state) = resident_state {
                let object = payload.as_object_mut().expect("placement payload object");
                object.insert(
                    "resident_state".to_string(),
                    serde_json::json!(resident_state),
                );
                object.insert(
                    "reservation_key".to_string(),
                    serde_json::json!(reservation_key),
                );
            }
            verlet::NewEventRecord::witnessed(
                coordinates.clone(),
                verlet::EventKind::PlacementDecision,
                payload,
            )
        };
        let stream_id = verlet::EventStreamId::for_thread(coordinates);
        for _ in 0..32 {
            if self
                .runtime_store
                .append_events(&stream_id, vec![record()])
                .await
                .is_ok()
            {
                return;
            }
        }
        panic!("fault plan prevented durable placement witness after bounded retries");
    }

    fn flush_queue_probes(&mut self) {
        let probes = self.probes.lock().unwrap();
        let observed_lease = probes
            .receipts
            .iter()
            .skip(self.probe_cursor)
            .any(|receipt| matches!(receipt, QueueProbeReceipt::Lease(_)));
        for receipt in probes.receipts.iter().skip(self.probe_cursor) {
            match receipt {
                QueueProbeReceipt::Lease(lease) => {
                    self.transcript.push_receipt("queue.lease", lease);
                    if lease.attempt > 1 {
                        self.transcript.push_receipt(
                            "queue.redelivery",
                            &serde_json::json!({
                                "message_id": lease.message_id,
                                "attempt": lease.attempt,
                            }),
                        );
                    }
                }
                QueueProbeReceipt::Complete(complete) => {
                    self.transcript.push_receipt("queue.complete", complete);
                }
            }
        }
        self.probe_cursor = probes.receipts.len();
        if observed_lease {
            self.transcript.push_receipt(
                "queue.clock",
                &serde_json::json!({
                    "tick": self.tick.load(std::sync::atomic::Ordering::SeqCst),
                }),
            );
        }
    }

    async fn drain_queue(&mut self) -> usize {
        let worker = verlet::VerletDaemonQueueWorker::new(
            std::sync::Arc::clone(&self.queue),
            self.bridge.clone(),
            "scenario-worker",
            30,
        )
        .with_max_messages(16);
        for _ in 0..16 {
            match worker.drain_once().await {
                Ok(0) => break,
                Ok(_) => {}
                Err(error) => {
                    self.transcript.push_receipt(
                        "scenario.operation.error",
                        &serde_json::json!({"operation": "drain_queue", "error": error.to_string()}),
                    );
                    self.tick.fetch_add(30, std::sync::atomic::Ordering::SeqCst);
                }
            }
        }
        self.flush_queue_probes();
        let remaining = self.queue_inner.pending_count().await;
        self.transcript.push_receipt(
            "queue.drain.completed",
            &serde_json::json!({"remaining": remaining}),
        );
        remaining
    }

    async fn collect_events(&mut self) {
        for coordinates in &self.coordinates {
            for stream_id in [
                verlet::control_stream_id(coordinates),
                verlet::EventStreamId::for_thread(coordinates),
            ] {
                self.transcript.preserve_id(stream_id.as_str());
                let from = self
                    .collected
                    .get(stream_id.as_str())
                    .map(|sequence| verlet::EventSequence::new(sequence + 1));
                let events = self
                    .raw_store
                    .read_events(&stream_id, from)
                    .await
                    .unwrap_or_default();
                for event in events {
                    self.collected
                        .insert(stream_id.as_str().to_string(), event.sequence.get());
                    self.transcript.push_event("durable", &event);
                }
            }
        }
    }

    async fn discover_bound_child(&mut self) {
        let Some(coordinates) = self.bound_coordinates(self.current_root) else {
            return;
        };
        if self
            .coordinates
            .iter()
            .any(|known| known.thread_id == coordinates.thread_id)
        {
            return;
        }
        self.wait_for_idle(&coordinates).await;
        self.runtime_generation += 1;
        self.append_placement(&coordinates, "active", None).await;
        self.coordinates.push(coordinates);
    }

    fn spawn_cut_worker(
        &self,
        worker_id: &'static str,
    ) -> tokio::task::JoinHandle<verlet_io_core::IoResult<usize>> {
        let worker = verlet::VerletDaemonQueueWorker::new(
            std::sync::Arc::clone(&self.queue),
            self.bridge.clone(),
            worker_id,
            30,
        );
        tokio::spawn(async move { worker.drain_once().await })
    }

    async fn remains_parked<T>(&self, task: &tokio::task::JoinHandle<T>) -> bool {
        // tight-timeout: this is an absence window for a task that must remain parked
        for _ in 0..128 {
            if task.is_finished() {
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        !task.is_finished()
    }

    /// Faulted harness-issued setup operations are receipts and failed retry
    /// attempts, never runner panics.
    async fn fresh_active_root(&mut self, label: &str) -> verlet::ThreadCoordinates {
        for retry in 0..32 {
            let root_index = self.root_count;
            self.root_count += 1;
            self.current_root = root_index;
            let coordinates = self.reserve_root(root_index);
            let envelope = self.envelope(
                root_index,
                "observe_only",
                &format!("{label}-setup-{retry}"),
            );
            if let Err(error) = self.queue.submit(envelope).await {
                self.transcript.push_receipt(
                    "scenario.operation.error",
                    &serde_json::json!({"operation": "crash_cut_setup", "error": error.to_string()}),
                );
                continue;
            }
            self.drain_queue().await;
            self.wait_for_idle(&coordinates).await;
            if self
                .server
                .supervisor()
                .get_thread_at(&coordinates)
                .await
                .is_ok_and(|handle| handle.status() == verlet::ThreadStatus::Idle)
            {
                self.runtime_generation += 1;
                self.append_placement(&coordinates, "active", None).await;
                self.coordinates.push(coordinates.clone());
                return coordinates;
            }
            self.runtime_generation += 1;
            self.append_placement(&coordinates, "terminal", Some("failed"))
                .await;
            self.coordinates.push(coordinates);
        }
        panic!("fault plan prevented a live crash-cut setup runtime after bounded retries");
    }

    /// Faulted harness-issued cut operations are receipt-bearing no-ops for
    /// the current scenario step, never runner panics.
    async fn queue_cut_envelope(
        &mut self,
        policy: &str,
        label: &str,
    ) -> Option<verlet::ThreadCoordinates> {
        let root_index = self.root_count;
        self.root_count += 1;
        self.current_root = root_index;
        let coordinates = self.reserve_root(root_index);
        let envelope = self.envelope(root_index, policy, label);
        if let Err(error) = self.queue.submit(envelope).await {
            self.transcript.push_receipt(
                "scenario.operation.error",
                &serde_json::json!({"operation": "crash_cut_submit", "error": error.to_string()}),
            );
            return None;
        }
        Some(coordinates)
    }

    async fn execute(&mut self, op: ScenarioOp) {
        self.tick.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        match op {
            ScenarioOp::StartThread => {
                let root_index = self.root_count;
                self.root_count += 1;
                self.current_root = root_index;
                let coordinates = self.reserve_root(root_index);
                let envelope = self.envelope(root_index, "observe_only", "start");
                match self.queue.submit(envelope).await {
                    Ok(_) => {
                        self.drain_queue().await;
                        self.wait_for_idle(&coordinates).await;
                        self.runtime_generation += 1;
                        self.append_placement(&coordinates, "active", None).await;
                        self.coordinates.push(coordinates);
                    }
                    Err(error) => self.transcript.push_receipt(
                        "scenario.operation.error",
                        &serde_json::json!({"operation": "start_thread", "error": error.to_string()}),
                    ),
                }
            }
            ScenarioOp::SubmitTurn => {
                if let Some(coordinates) = self.bound_coordinates(self.current_root) {
                    self.envelope_index += 1;
                    let turn_id =
                        format!("scenario-turn-{}-{}", self.plan.seed, self.envelope_index);
                    if let Err(error) = self
                        .server
                        .supervisor()
                        .submit_to(&coordinates, turn_id, "submit")
                        .await
                    {
                        self.transcript.push_receipt(
                            "scenario.operation.error",
                            &serde_json::json!({"operation": "submit_turn", "error": error.to_string()}),
                        );
                    } else {
                        self.wait_for_idle(&coordinates).await;
                    }
                }
            }
            ScenarioOp::Steer => {
                if let Some(coordinates) = self.bound_coordinates(self.current_root) {
                    let Ok(handle) = self.server.supervisor().get_thread_at(&coordinates).await
                    else {
                        return;
                    };
                    let mut events = handle.subscribe_events();
                    self.envelope_index += 1;
                    let turn_id =
                        format!("scenario-steer-{}-{}", self.plan.seed, self.envelope_index);
                    // Idle steer deliberately stays Idle while it persists the
                    // rejected input. The async store may yield after dequeuing,
                    // so status plus queue depth cannot witness quiescence here.
                    if self
                        .server
                        .supervisor()
                        .submit_to_with_mode(
                            &coordinates,
                            turn_id.clone(),
                            "steer",
                            verlet::TurnSubmissionMode::Steer,
                        )
                        .await
                        .is_ok()
                    {
                        self.wait_for_turn_input(&coordinates, &mut events, &turn_id)
                            .await;
                    }
                }
            }
            ScenarioOp::Cancel => {
                if let Some(coordinates) = self.bound_coordinates(self.current_root) {
                    let _ = self
                        .server
                        .supervisor()
                        .cancel_at(&coordinates, "scenario cancel")
                        .await;
                    self.wait_for_idle(&coordinates).await;
                }
            }
            ScenarioOp::Fork => {
                if let Some(parent) = self.bound_coordinates(self.current_root) {
                    self.envelope_index += 1;
                    let fork_envelope_id = format!("scenario-fork-{}", self.envelope_index);
                    let child_thread_id = self.deterministic_thread_id(
                        0x1000_0000usize.saturating_add(self.envelope_index),
                    );
                    let control_stream = verlet::control_stream_id(&parent);
                    let claim = self
                        .runtime_store
                        .append_events(
                            &control_stream,
                            vec![verlet::NewEventRecord::witnessed(
                                parent.clone(),
                                verlet::EventKind::IoIngressClaimed,
                                serde_json::json!({
                                    "ingress_envelope_ids": [&fork_envelope_id],
                                    "ingress_witness_event_ids": [],
                                    "admission_event_id": uuid::Uuid::from_u128(
                                        (u128::from(self.plan.seed) << 64) | self.envelope_index as u128
                                    ).to_string(),
                                    "intent": {
                                        "outcome": "fork",
                                        "child_key": &fork_envelope_id,
                                        "input_digest": format!("sha256:{:064x}", self.plan.seed),
                                        "child_thread_id": child_thread_id,
                                    }
                                }),
                            )],
                        )
                        .await;
                    if let Ok(mut claims) = claim
                        && let Some(claim) = claims.pop()
                        && let Ok(child) = crate::support::scenario_fork_with_id(
                            &self.server,
                            &parent,
                            child_thread_id,
                        )
                        .await
                    {
                        let _ = self
                            .runtime_store
                            .append_events(
                                &control_stream,
                                vec![
                                    verlet::NewEventRecord::witnessed(
                                        parent.clone(),
                                        verlet::EventKind::ThreadSpawned,
                                        serde_json::json!({
                                            "child_thread_id": child.thread_id,
                                            "fork": {"claim_event_id": claim.id},
                                        }),
                                    ),
                                    verlet::NewEventRecord::witnessed(
                                        parent,
                                        verlet::EventKind::IoIngressSettled,
                                        serde_json::json!({
                                            "claim_event_id": claim.id,
                                            "ingress_envelope_ids": [fork_envelope_id],
                                            "evidence_event_id": null,
                                            "settled_by": "execution",
                                        }),
                                    ),
                                ],
                            )
                            .await;
                        self.rebind_root_to_child(self.current_root, &child);
                        self.wait_for_idle(&child).await;
                        self.runtime_generation += 1;
                        self.append_placement(&child, "active", None).await;
                        self.coordinates.push(child);
                    }
                }
            }
            ScenarioOp::Restart => {
                unreachable!("restart is executed through CrashCutHost")
            }
            ScenarioOp::DrainQueue => {
                self.drain_queue().await;
            }
            ScenarioOp::ShutdownAll => {
                let _ = self.drain_queue().await;
                let _ = self.server.supervisor().shutdown_all().await;
                for coordinates in self.coordinates.clone() {
                    self.append_placement(&coordinates, "terminal", Some("completed"))
                        .await;
                }
                self.shut_down = true;
                self.transcript.push_receipt(
                    "shutdown_all.completed",
                    &serde_json::json!({"kind": "shutdown_all.completed"}),
                );
            }
        }
    }

    async fn finish_crash_cut(&mut self) {
        for coordinates in self.coordinates.clone() {
            self.append_placement(&coordinates, "terminal", Some("failed"))
                .await;
        }
        self.collect_events().await;
    }
}

/// Receipt for the storage-engine scenario lane introduced by EMO-415.
/// Unlike the runtime operation alphabet, this lane drives the stream store
/// directly so the cut occurs below `cancellation_safe`, in Turso IO.
#[derive(Debug)]
pub struct StreamIoCrashReceipt {
    pub io_transcript: Vec<crate::support::simulated_io::IoTranscriptEntry>,
    pub integrity_check: Vec<String>,
    pub event_ids: Vec<verlet::EventRecordId>,
    pub expected_event_ids: Vec<verlet::EventRecordId>,
    pub sequences: Vec<i64>,
}

/// Execute the fixed seeded IO crash scenario: commit a prefix, arm a crash
/// on the first engine write of an append burst, discard unsynced bytes,
/// reopen through a fresh `Db::open_with_io`, and replay the burst iff none of
/// it survived. A partial burst is an invariant violation, not repair input.
pub async fn run_stream_io_crash_scenario(seed: u64) -> Result<StreamIoCrashReceipt, String> {
    fn record(
        seed: u64,
        index: usize,
        coordinates: &verlet::ThreadCoordinates,
    ) -> verlet::NewEventRecord {
        verlet::NewEventRecord {
            id: verlet::EventRecordId::from_uuid(uuid::Uuid::from_u128(
                (u128::from(seed) << 64) | 0x4150_0000u128 | index as u128,
            )),
            coordinates: coordinates.clone(),
            created_at_ms: seed as i64 + index as i64,
            kind: verlet::EventKind::TurnSubmitted,
            origin: verlet::EventOrigin::Witnessed,
            provenance: verlet::EventProvenance::default(),
            payload: serde_json::json!({"turn_id": format!("io-crash-{index}")}),
        }
    }

    let path = std::path::PathBuf::from(format!("/simulated/emo-415-history-{seed:016x}.sqlite3"));
    let coordinates = verlet::ThreadCoordinates {
        tenant_id: "emo-415-tenant".to_string(),
        user_id: "emo-415-user".to_string(),
        session_id: "emo-415-session".to_string(),
        thread_id: verlet::ThreadId::parse_str(
            &uuid::Uuid::from_u128((u128::from(seed) << 64) | 0x415u128).to_string(),
        )
        .map_err(|error| error.to_string())?,
    };
    let stream_id = verlet::EventStreamId::for_thread(&coordinates);
    let prefix = (0..3)
        .map(|index| record(seed, index, &coordinates))
        .collect::<Vec<_>>();
    let burst = (3..11)
        .map(|index| record(seed, index, &coordinates))
        .collect::<Vec<_>>();
    let expected_event_ids = prefix
        .iter()
        .chain(&burst)
        .map(|record| record.id)
        .collect::<Vec<_>>();

    let simulated = std::sync::Arc::new(crate::support::simulated_io::SimulatedIo::new(seed));
    let injected: std::sync::Arc<dyn verlet_sqlite::io::IO> = simulated.clone();
    let db = verlet_sqlite::Db::open_with_io(&path, verlet_sqlite::DbConfig::default(), injected)
        .await
        .map_err(|error| error.to_string())?;
    let store = verlet_history_sqlite::SqliteSessionStore::from_db(db.clone())
        .await
        .map_err(|error| error.to_string())?;
    store
        .append_events(&stream_id, prefix.clone())
        .await
        .map_err(|error| error.to_string())?;
    let checkpoint_io_start = simulated.transcript().len();
    let checkpoint = db.connect().await.map_err(|error| error.to_string())?;
    let mut checkpoint_rows = checkpoint
        .query("PRAGMA wal_checkpoint(TRUNCATE)", ())
        .await
        .map_err(|error| error.to_string())?;
    let checkpoint_row = checkpoint_rows
        .next()
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "wal_checkpoint(TRUNCATE) returned no status row".to_string())?;
    let checkpoint_status = [
        checkpoint_row
            .get::<i64>(0)
            .map_err(|error| error.to_string())?,
        checkpoint_row
            .get::<i64>(1)
            .map_err(|error| error.to_string())?,
        checkpoint_row
            .get::<i64>(2)
            .map_err(|error| error.to_string())?,
    ];
    if checkpoint_rows
        .next()
        .await
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("wal_checkpoint(TRUNCATE) returned multiple status rows".to_string());
    }
    if checkpoint_status[0] != 0 || checkpoint_status[1] != checkpoint_status[2] {
        return Err(format!(
            "wal_checkpoint(TRUNCATE) did not fully backfill the prefix: busy/log/checkpointed={checkpoint_status:?}"
        ));
    }
    drop(checkpoint_rows);
    drop(checkpoint);
    let checkpoint_transcript = simulated.transcript();
    let main_path = path
        .to_str()
        .ok_or_else(|| "simulated database path is not UTF-8".to_string())?;
    if !checkpoint_transcript[checkpoint_io_start..]
        .iter()
        .any(|entry| {
            entry.operation == crate::support::simulated_io::IO_SYNC
                && entry.path == main_path
                && entry.outcome == "ok"
        })
    {
        return Err(
            "wal_checkpoint(TRUNCATE) did not truthfully sync the main database image".to_string(),
        );
    }

    simulated.arm(
        crate::support::simulated_io::IoFaultPlan::crash_after_write(
            seed,
            1,
            crate::support::simulated_io::CrashSurvival::DiscardUnsynced,
        ),
    )?;
    let armed_io_start = simulated.transcript().len();
    let crashed_append = store.append_events(&stream_id, burst.clone()).await;
    if crashed_append.is_ok() || !simulated.crashed() {
        return Err(format!(
            "seeded IO cut did not interrupt the append: {crashed_append:?}"
        ));
    }
    let crash_transcript = simulated.transcript();
    let first_armed_write = crash_transcript[armed_io_start..]
        .iter()
        .find(|entry| entry.operation == crate::support::simulated_io::IO_WRITE)
        .ok_or_else(|| "armed burst append issued no engine write".to_string())?;
    if !first_armed_write.outcome.starts_with("crash:") {
        return Err(format!(
            "the first armed write was not the crash cut: {first_armed_write:?}"
        ));
    }
    drop(store);
    drop(db);

    let recovered = simulated.recover()?;
    let injected: std::sync::Arc<dyn verlet_sqlite::io::IO> =
        std::sync::Arc::new(recovered.clone());
    let db = verlet_sqlite::Db::open_with_io(&path, verlet_sqlite::DbConfig::default(), injected)
        .await
        .map_err(|error| error.to_string())?;
    let store = verlet_history_sqlite::SqliteSessionStore::from_db(db.clone())
        .await
        .map_err(|error| error.to_string())?;
    let after_crash = store
        .read_events(&stream_id, None)
        .await
        .map_err(|error| error.to_string())?;
    let survived_prefix = after_crash
        .iter()
        .take(prefix.len())
        .map(|event| event.id)
        .collect::<Vec<_>>();
    let expected_prefix = prefix.iter().map(|record| record.id).collect::<Vec<_>>();
    if survived_prefix != expected_prefix {
        return Err(format!(
            "checkpointed prefix did not survive crash: expected {expected_prefix:?}, got {survived_prefix:?}"
        ));
    }
    let survived_burst = after_crash
        .iter()
        .filter(|event| burst.iter().any(|record| record.id == event.id))
        .count();
    match survived_burst {
        0 => {
            store
                .append_events(&stream_id, burst)
                .await
                .map_err(|error| error.to_string())?;
        }
        count if count == burst.len() => {}
        count => {
            return Err(format!(
                "atomic append burst was torn across crash: {count}/{} events survived",
                burst.len()
            ));
        }
    }

    let events = store
        .read_events(&stream_id, None)
        .await
        .map_err(|error| error.to_string())?;
    let connection = db.connect().await.map_err(|error| error.to_string())?;
    let mut rows = connection
        .query("PRAGMA integrity_check", ())
        .await
        .map_err(|error| error.to_string())?;
    let mut integrity_check = Vec::new();
    while let Some(row) = rows.next().await.map_err(|error| error.to_string())? {
        integrity_check.push(row.get::<String>(0).map_err(|error| error.to_string())?);
    }
    drop(rows);
    drop(connection);
    drop(store);
    drop(db);

    Ok(StreamIoCrashReceipt {
        io_transcript: recovered.transcript(),
        integrity_check,
        event_ids: events.iter().map(|event| event.id).collect(),
        expected_event_ids,
        sequences: events.iter().map(|event| event.sequence.get()).collect(),
    })
}

struct ScenarioStoreState {
    root: std::path::PathBuf,
    plan: crate::support::fault_plan::FaultPlan,
    transcript: crate::support::transcript::TypedTranscript,
    coordinates: Vec<verlet::ThreadCoordinates>,
    collected: std::collections::BTreeMap<String, i64>,
    current_root: usize,
    root_count: usize,
    envelope_index: usize,
    runtime_generation: usize,
    active_runtime_ids: std::collections::BTreeMap<String, String>,
    process_cut_index: usize,
    shut_down: bool,
    tick: u64,
    queue_inner: std::sync::Arc<ScenarioQueue>,
}

#[async_trait::async_trait]
impl crate::support::fault_plan::CrashCutHost for ScenarioHarness {
    type StoreState = ScenarioStoreState;

    async fn run_to_cut(&mut self, seam: crate::support::fault_plan::CrashCutSeam) {
        match seam {
            crate::support::fault_plan::CrashCutSeam::PauseAfterIngressClaim => {
                let Some(coordinates) = self
                    .queue_cut_envelope("queue_per_conversation", "claim-submit-cut")
                    .await
                else {
                    self.finish_crash_cut().await;
                    return;
                };
                let (pause, paused) =
                    crate::support::scenario_pause_after_ingress_claim(&self.bridge);
                loop {
                    pause.store(true, std::sync::atomic::Ordering::SeqCst);
                    let paused = std::sync::Arc::clone(&paused);
                    let mut reached = tokio::spawn(async move { paused.notified().await });
                    tokio::task::yield_now().await;
                    let mut drain = self.spawn_cut_worker("scenario-claim-cut");
                    tokio::select! {
                        _ = &mut reached => {
                            drain.abort();
                            let _ = drain.await;
                            self.runtime_generation += 1;
                            self.append_placement(&coordinates, "active", None).await;
                            self.coordinates.push(coordinates.clone());
                            break;
                        }
                        _ = &mut drain => {
                            reached.abort();
                            let _ = reached.await;
                            self.tick.fetch_add(30, std::sync::atomic::Ordering::SeqCst);
                        }
                    }
                }
            }
            crate::support::fault_plan::CrashCutSeam::PersistedInputRuntimeNotify => {
                let mut coordinates = self.fresh_active_root("provider-cut").await;
                self.provider
                    .pause_next_complete
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                let mut submit_errors = Vec::new();
                for attempt in 0..32 {
                    if let Err(error) = self
                        .server
                        .supervisor()
                        .submit_to(
                            &coordinates,
                            format!("scenario-provider-cut-{attempt}"),
                            "provider crash cut",
                        )
                        .await
                    {
                        submit_errors.push(error.to_string());
                        coordinates = self.fresh_active_root("provider-cut-retry").await;
                        continue;
                    }
                    let complete_started = self.provider.complete_started.notified();
                    tokio::pin!(complete_started);
                    tokio::select! {
                        biased;
                        _ = &mut complete_started => break,
                        _ = self.wait_for_idle(&coordinates) => {}
                    }
                }
                assert!(
                    !self
                        .provider
                        .pause_next_complete
                        .load(std::sync::atomic::Ordering::SeqCst),
                    "real runtime did not reach provider after persisting input; submit errors: {submit_errors:?}"
                );
            }
            crate::support::fault_plan::CrashCutSeam::QueueCompleteBarrier => {
                let Some(coordinates) = self
                    .queue_cut_envelope("observe_only", "queue-complete-cut")
                    .await
                else {
                    self.finish_crash_cut().await;
                    return;
                };
                let wait_started = std::time::Instant::now();
                'attempts: loop {
                    self.queue_inner
                        .pause_next_complete
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    let queue_inner = std::sync::Arc::clone(&self.queue_inner);
                    let complete_started = queue_inner.complete_started.notified();
                    tokio::pin!(complete_started);
                    let mut drain = self.spawn_cut_worker("scenario-complete-cut");
                    loop {
                        if wait_started.elapsed() >= SCENARIO_ASYNC_WAIT_TIMEOUT {
                            drain.abort();
                            let _ = drain.await;
                            let attempts = queue_inner.lease_attempts().await;
                            panic!(
                                "scenario queue completion cut did not reach its receipt or terminate within {SCENARIO_ASYNC_WAIT_TIMEOUT:?}; tick={}, attempts={attempts:?}",
                                self.tick.load(std::sync::atomic::Ordering::SeqCst),
                            );
                        }
                        tokio::select! {
                            _ = &mut complete_started => {
                                drain.abort();
                                let _ = drain.await;
                                self.runtime_generation += 1;
                                self.append_placement(&coordinates, "active", None).await;
                                self.coordinates.push(coordinates.clone());
                                break 'attempts;
                            }
                            result = &mut drain => {
                                match result {
                                    Ok(_) => {
                                        // A planned queue/store failure or an empty
                                        // drain may end an attempt before it reaches
                                        // the completion cut. Expire only that joined
                                        // attempt; real IO latency must not advance
                                        // queue time while the lease is live.
                                        self.tick.fetch_add(
                                            30,
                                            std::sync::atomic::Ordering::SeqCst,
                                        );
                                        break;
                                    }
                                    Err(error) => {
                                        panic!(
                                            "scenario queue completion worker terminated abnormally before its receipt: {error}"
                                        );
                                    }
                                }
                            }
                            _ = tokio::time::sleep(SCENARIO_ASYNC_RECHECK_INTERVAL) => {}
                        }
                    }
                }
            }
            crate::support::fault_plan::CrashCutSeam::IngressBindingBarrier => {
                let root_index = self.root_count;
                self.root_count += 1;
                self.current_root = root_index;
                let coordinates = self.root_coordinates(root_index);
                let envelope = self.envelope(root_index, "observe_only", "ingress-binding-cut");
                // A faulted harness-issued submit makes this cut a receipt-bearing
                // no-op for the current scenario step.
                if let Err(error) = self.queue.submit(envelope).await {
                    self.transcript.push_receipt(
                        "scenario.operation.error",
                        &serde_json::json!({
                            "operation": "ingress_binding_crash_cut_submit",
                            "error": error.to_string(),
                        }),
                    );
                    self.finish_crash_cut().await;
                    return;
                }
                let hook = crate::support::scenario_ingress_binding_barrier(&self.bridge);
                let mut reached = false;
                for _ in 0..32 {
                    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
                    *hook.lock().unwrap_or_else(|error| error.into_inner()) = Some(barrier);
                    let drain = self.spawn_cut_worker("scenario-binding-cut");
                    if self.remains_parked(&drain).await {
                        drain.abort();
                        let _ = drain.await;
                        let previous = self.replace_root_reservation_after_cut(root_index);
                        assert!(
                            previous.is_none(),
                            "ingress binding committed before the registered barrier cut"
                        );
                        reached = true;
                        break;
                    }
                    let _ = drain.await;
                    self.tick.fetch_add(30, std::sync::atomic::Ordering::SeqCst);
                }
                assert!(reached, "real bridge did not reach ingress-binding barrier");
                *hook.lock().unwrap_or_else(|error| error.into_inner()) = None;
                self.coordinates.push(coordinates);
            }
            crate::support::fault_plan::CrashCutSeam::ThreadLoadRootBarrier => {
                let coordinates = self.fresh_active_root("thread-load-cut-setup").await;
                // A faulted harness-issued shutdown makes this cut a receipt-bearing
                // no-op for the current scenario step.
                if let Err(error) = self
                    .server
                    .supervisor()
                    .shutdown_thread_at(&coordinates)
                    .await
                {
                    self.transcript.push_receipt(
                        "scenario.operation.error",
                        &serde_json::json!({
                            "operation": "cold_load_crash_cut_shutdown",
                            "error": error.to_string(),
                        }),
                    );
                    self.finish_crash_cut().await;
                    return;
                }
                let envelope = self.envelope(self.current_root, "observe_only", "thread-load-cut");
                // A faulted harness-issued submit makes this cut a receipt-bearing
                // no-op for the current scenario step.
                if let Err(error) = self.queue.submit(envelope).await {
                    self.transcript.push_receipt(
                        "scenario.operation.error",
                        &serde_json::json!({
                            "operation": "cold_load_crash_cut_submit",
                            "error": error.to_string(),
                        }),
                    );
                    self.finish_crash_cut().await;
                    return;
                }
                let hook = crate::support::scenario_thread_load_root_barrier(&self.bridge);
                loop {
                    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
                    *hook.lock().unwrap_or_else(|error| error.into_inner()) = Some(barrier);
                    let drain = self.spawn_cut_worker("scenario-thread-load-cut");
                    if self.remains_parked(&drain).await {
                        drain.abort();
                        let _ = drain.await;
                        break;
                    }
                    let _ = drain.await;
                    self.tick.fetch_add(30, std::sync::atomic::Ordering::SeqCst);
                }
                *hook.lock().unwrap_or_else(|error| error.into_inner()) = None;
            }
            crate::support::fault_plan::CrashCutSeam::SpawnSnapshotBarrier => {
                let coordinates = self.fresh_active_root("spawn-snapshot-cut-setup").await;
                loop {
                    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
                    let projection = tokio::spawn(crate::support::scenario_project_spawn_snapshot(
                        self.projector_host.clone(),
                        coordinates.clone(),
                        std::sync::Arc::clone(&barrier),
                    ));
                    if self.remains_parked(&projection).await {
                        projection.abort();
                        let _ = projection.await;
                        break;
                    }
                    let _ = projection.await;
                }
            }
            crate::support::fault_plan::CrashCutSeam::ThreadTerminalJoinCommit => {
                panic!(
                    "thread-terminal-join-commit uses the dedicated provider-backed EMO-426 host"
                );
            }
        }
        // This receipt is seam-coverage evidence; the no-op returns above
        // must not emit it when a harness-issued operation faults first.
        self.transcript.push_receipt(
            "scenario.crash_cut",
            &serde_json::json!({"seam": format!("{seam:?}")}),
        );
        self.finish_crash_cut().await;
    }

    fn tear_down(self) -> Self::StoreState {
        ScenarioStoreState {
            root: self.root,
            plan: self.plan,
            transcript: self.transcript,
            coordinates: self.coordinates,
            collected: self.collected,
            current_root: self.current_root,
            root_count: self.root_count,
            envelope_index: self.envelope_index,
            runtime_generation: self.runtime_generation,
            active_runtime_ids: self.active_runtime_ids,
            process_cut_index: self.process_cut_index,
            shut_down: self.shut_down,
            tick: self.tick.load(std::sync::atomic::Ordering::SeqCst),
            queue_inner: self.queue_inner,
        }
    }

    async fn rebuild(state: Self::StoreState) -> Self {
        let mut rebuilt = ScenarioHarness::build(
            state.root,
            clone_plan(&state.plan),
            false,
            Some(state.queue_inner),
        )
        .await;
        rebuilt.plan = state.plan;
        rebuilt.transcript = state.transcript;
        rebuilt.coordinates = state.coordinates;
        rebuilt.collected = state.collected;
        rebuilt.current_root = state.current_root;
        rebuilt.root_count = state.root_count;
        rebuilt.envelope_index = state.envelope_index;
        rebuilt.runtime_generation = state.runtime_generation + 1;
        rebuilt.active_runtime_ids = state.active_runtime_ids;
        rebuilt.process_cut_index = state.process_cut_index;
        rebuilt.shut_down = state.shut_down;
        rebuilt
            .tick
            .store(state.tick, std::sync::atomic::Ordering::SeqCst);
        rebuilt
    }

    async fn recover(&mut self) {
        self.tick.fetch_add(30, std::sync::atomic::Ordering::SeqCst);
        for coordinates in self.coordinates.clone() {
            self.transcript.push_receipt(
                "recovery.probe",
                &serde_json::json!({"reservation_key": format!("thread:{}", coordinates.thread_id)}),
            );
        }
        let _ = self.drain_queue().await;
        for coordinates in self.coordinates.clone() {
            if self
                .server
                .supervisor()
                .get_thread_at(&coordinates)
                .await
                .is_err()
                && let Some(root_index) = self.bound_root_index(&coordinates)
            {
                let envelope = self.envelope(root_index, "observe_only", "recovery-probe");
                let _ = self.queue.submit(envelope).await;
                let _ = self.drain_queue().await;
            }
        }
        // Loading a bound child recursively adopts its parent. Observe final
        // placement only after every bound-route recovery has had that chance;
        // a single pass would falsely leave an earlier parent terminal.
        for coordinates in self.coordinates.clone() {
            let state = if self
                .server
                .supervisor()
                .get_thread_at(&coordinates)
                .await
                .is_ok()
            {
                "active"
            } else {
                "terminal"
            };
            self.append_placement(
                &coordinates,
                state,
                (state == "terminal").then_some("failed"),
            )
            .await;
        }
    }
}

/// Operation alphabet v1 (ADR 0004). Deliberately small; growing it is a
/// versioned vocabulary change, never a silent addition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScenarioOp {
    StartThread,
    SubmitTurn,
    Steer,
    Cancel,
    Fork,
    /// Kill the simulated host at the next planned cut, rebuild it over the
    /// surviving store state, and run recovery.
    Restart,
    DrainQueue,
    ShutdownAll,
}

/// Generation bounds: short enough to minimize, long enough to interleave.
#[derive(Clone, Copy, Debug)]
pub struct ScenarioBounds {
    pub max_ops: usize,
    pub intensity: crate::support::fault_plan::Intensity,
}

/// One seeded scenario, reproducible from `(seed, harness version)` alone.
#[derive(Debug)]
pub struct Scenario {
    pub seed: u64,
    pub ops: Vec<ScenarioOp>,
    pub plan: crate::support::fault_plan::FaultPlan,
}

impl Scenario {
    /// Derive the operation sequence and fault plan from the same seed,
    /// through independent `SplitMix64` split lanes.
    pub fn derive(seed: u64, bounds: ScenarioBounds) -> Self {
        let plan = crate::support::fault_plan::FaultPlan::derive(seed, bounds.intensity);
        if bounds.max_ops == 0 {
            return Self {
                seed,
                ops: Vec::new(),
                plan,
            };
        }

        let version_salt = u64::from(crate::support::fault_plan::FAULT_VOCABULARY_VERSION)
            .wrapping_mul(0xD6E8_FEB8_6659_FD93);
        let lane = |label: &str| {
            let mut root = crate::support::fault_plan::SplitMix64::new(seed ^ version_salt);
            root.split(label)
        };
        let mut count_lane = lane("scenario-op-count-v1");
        let mut op_lane = lane("scenario-ops-v1");
        let target_len = 1 + count_lane.next_below(bounds.max_ops as u64) as usize;
        let mut remaining_cuts = plan
            .directives
            .iter()
            .filter(|directive| {
                directive.component == crate::support::fault_plan::FaultComponent::Process
            })
            .count();
        let mut ops = Vec::with_capacity(target_len);
        ops.push(ScenarioOp::StartThread);
        while ops.len() < target_len {
            let drawn = match op_lane.next_below(8) {
                0 => ScenarioOp::StartThread,
                1 => ScenarioOp::SubmitTurn,
                2 => ScenarioOp::Steer,
                3 => ScenarioOp::Cancel,
                4 => ScenarioOp::Fork,
                5 if remaining_cuts > 0 => {
                    remaining_cuts -= 1;
                    ScenarioOp::Restart
                }
                5 | 6 => ScenarioOp::DrainQueue,
                7 => ScenarioOp::ShutdownAll,
                _ => unreachable!("scenario operation draw is bounded by eight"),
            };
            ops.push(drawn);
            if drawn == ScenarioOp::ShutdownAll {
                break;
            }
        }
        Self { seed, ops, plan }
    }
}

/// What an invariant may look at after each step. Deliberately store-first:
/// invariants check durable truth plus the normalized transcript, never
/// in-process convenience state. Anything an invariant needs that is not
/// reachable from here is a missing witness in the design, not a reason to
/// widen this surface casually.
pub struct ScenarioWorld<'a> {
    pub store: &'a (dyn verlet::RuntimeStore + Send + Sync),
    /// The ingress queue under test, when the scenario exercises ingress;
    /// bounded-queue invariants pass when it is absent.
    pub queue: Option<&'a (dyn verlet_io_core::IngressQueueStore + Send + Sync)>,
    /// Normalized durable events and non-mutating witness receipts. Queue
    /// witnesses are `queue.lease`, `queue.redelivery`, `queue.complete`,
    /// `queue.clock`, and `queue.drain.completed`.
    pub transcript: &'a crate::support::transcript::NormalizedTranscript,
    /// Index into `Scenario::ops` of the operation just executed.
    pub step: usize,
    /// True once `ShutdownAll` has completed, for terminal invariants.
    pub shut_down: bool,
}

/// One violation, named for its invariant with enough detail to read the
/// failure without re-running.
#[derive(Clone, Debug)]
pub struct InvariantViolation {
    pub invariant: &'static str,
    pub detail: String,
}

/// A named invariant checked after every scenario step. The v1 normative
/// set is ADR 0004 "The invariant set, v1"; each entry lands here carrying
/// its number in its name (for example "inv2-unique-active-topology").
#[async_trait::async_trait]
pub trait ScenarioInvariant: Send + Sync {
    fn name(&self) -> &'static str;
    /// Return every violation found; empty when the invariant holds. Async
    /// because truth lives in the store.
    async fn check(&self, world: &ScenarioWorld<'_>) -> Vec<InvariantViolation>;
}

/// The receipt of a failing scenario: seed plus normalized transcript
/// (lexicon law), with the violations that fired and where.
#[derive(Debug)]
pub struct ScenarioFailure {
    pub seed: u64,
    pub vocabulary_version: u32,
    pub failing_step: usize,
    pub violations: Vec<InvariantViolation>,
    pub transcript: crate::support::transcript::NormalizedTranscript,
}

/// Runner surface: execute the scenario on the paused-time harness under
/// its fault plan, checking `invariants` after every operation. `Ok(())`
/// when everything held; the failure receipt otherwise. Minimization
/// (shrink ops first, then directives, keeping the failure) belongs to the
/// same ticket and runs on the receipt before it is reported.
pub async fn run_scenario(
    scenario: Scenario,
    invariants: &[Box<dyn ScenarioInvariant>],
) -> Result<(), ScenarioFailure> {
    let seed = scenario.seed;
    let original_ops = scenario.ops.clone();
    let original_plan = clone_plan(&scenario.plan);
    match run_scenario_once(&scenario, invariants).await {
        Ok(_) => Ok(()),
        Err(first) => {
            let minimized =
                minimize_scenario(seed, original_ops, original_plan, &first, invariants).await;
            eprintln!(
                "scenario seed={} vocabulary={} failed at step {}\n{}",
                minimized.seed,
                minimized.vocabulary_version,
                minimized.failing_step,
                minimized.transcript.render()
            );
            Err(minimized)
        }
    }
}

async fn run_scenario_once(
    scenario: &Scenario,
    invariants: &[Box<dyn ScenarioInvariant>],
) -> Result<crate::support::transcript::NormalizedTranscript, ScenarioFailure> {
    let run_root = ScenarioRunRoot::new(scenario.seed);
    let root = run_root.path.clone();
    let mut harness =
        ScenarioHarness::build(root.clone(), clone_plan(&scenario.plan), true, None).await;
    let mut normative_invariants = crate::support::invariants::invariant_set_v1();
    normative_invariants.push(Box::new(crate::support::invariant_claims::Inv6ClaimsSettle));
    normative_invariants.extend(crate::support::invariant_forks::fork_invariants_v1());

    for (step, op) in scenario.ops.iter().copied().enumerate() {
        if op == ScenarioOp::Restart {
            let process = harness
                .plan
                .directives
                .iter()
                .filter(|directive| {
                    directive.component == crate::support::fault_plan::FaultComponent::Process
                })
                .nth(harness.process_cut_index)
                .cloned();
            if let Some(process) = process {
                harness.process_cut_index += 1;
                harness =
                    crate::support::fault_plan::run_crash_cut(process.operation, harness).await;
            } else {
                harness.transcript.push_receipt(
                    "scenario.operation.error",
                    &serde_json::json!({"operation": "restart", "error": "no remaining process cut"}),
                );
            }
        } else {
            harness.execute(op).await;
        }
        harness.collect_events().await;
        let transcript = harness.transcript.normalize();
        let world = ScenarioWorld {
            store: &harness.raw_store,
            queue: Some(harness.queue.as_ref()),
            transcript: &transcript,
            step,
            shut_down: harness.shut_down,
        };
        let mut violations = Vec::new();
        for invariant in &normative_invariants {
            violations.extend(invariant.check(&world).await);
        }
        for invariant in invariants {
            violations.extend(invariant.check(&world).await);
        }
        if !violations.is_empty() {
            let failure = ScenarioFailure {
                seed: scenario.seed,
                vocabulary_version: scenario.plan.vocabulary_version,
                failing_step: step,
                violations,
                transcript,
            };
            let _ = harness.server.supervisor().shutdown_all().await;
            drop(harness);
            return Err(failure);
        }
    }

    let transcript = harness.transcript.normalize();
    let _ = harness.server.supervisor().shutdown_all().await;
    drop(harness);
    Ok(transcript)
}

async fn minimize_scenario(
    seed: u64,
    mut ops: Vec<ScenarioOp>,
    mut plan: crate::support::fault_plan::FaultPlan,
    first: &ScenarioFailure,
    invariants: &[Box<dyn ScenarioInvariant>],
) -> ScenarioFailure {
    let target = first
        .violations
        .iter()
        .map(|violation| (violation.invariant, violation.detail.clone()))
        .collect::<std::collections::BTreeSet<_>>();

    // Delta-debug contiguous subsequences first. A candidate is retained only
    // when at least one invariant from the original failure still fires.
    let mut granularity = 2usize;
    while !ops.is_empty() && granularity <= ops.len() {
        let chunk = ops.len().div_ceil(granularity);
        let mut reduced = false;
        let mut start = 0usize;
        while start < ops.len() {
            let end = (start + chunk).min(ops.len());
            let mut candidate_ops = ops.clone();
            candidate_ops.drain(start..end);
            let candidate = Scenario {
                seed,
                ops: candidate_ops.clone(),
                plan: clone_plan(&plan),
            };
            if failure_matches(&candidate, invariants, &target).await {
                ops = candidate_ops;
                granularity = 2;
                reduced = true;
                break;
            }
            start = end;
        }
        if !reduced {
            if granularity == ops.len() {
                break;
            }
            granularity = (granularity * 2).min(ops.len());
        }
    }

    // Then remove directives one at a time, restarting after every retained
    // removal so indexes stay stable.
    let mut directive_index = 0usize;
    while directive_index < plan.directives.len() {
        let mut directives = plan.directives.clone();
        directives.remove(directive_index);
        let candidate = Scenario {
            seed,
            ops: ops.clone(),
            plan: crate::support::fault_plan::FaultPlan {
                seed: plan.seed,
                vocabulary_version: plan.vocabulary_version,
                intensity: plan.intensity,
                directives: directives.clone(),
            },
        };
        if failure_matches(&candidate, invariants, &target).await {
            plan.directives = directives;
        } else {
            directive_index += 1;
        }
    }

    let minimized = Scenario {
        seed,
        ops: ops.clone(),
        plan,
    };
    let mut failure = run_scenario_once(&minimized, invariants)
        .await
        .expect_err("the minimized scenario must retain the original failure class");
    failure
        .transcript
        .items
        .push(crate::support::transcript::NormalizedTranscriptItem {
            kind: "receipt".to_string(),
            label: "scenario.minimized_reproduction".to_string(),
            value: serde_json::json!({
                "seed": seed,
                "vocabulary_version": failure.vocabulary_version,
                "ops": ops.iter().map(|op| op_name(*op)).collect::<Vec<_>>(),
                "directives": minimized.plan.directives,
            }),
        });
    failure
}

async fn failure_matches(
    scenario: &Scenario,
    invariants: &[Box<dyn ScenarioInvariant>],
    target: &std::collections::BTreeSet<(&'static str, String)>,
) -> bool {
    match run_scenario_once(scenario, invariants).await {
        Ok(_) => false,
        Err(failure) => failure
            .violations
            .iter()
            .any(|violation| target.contains(&(violation.invariant, violation.detail.clone()))),
    }
}

fn op_name(op: ScenarioOp) -> &'static str {
    match op {
        ScenarioOp::StartThread => "start_thread",
        ScenarioOp::SubmitTurn => "submit_turn",
        ScenarioOp::Steer => "steer",
        ScenarioOp::Cancel => "cancel",
        ScenarioOp::Fork => "fork",
        ScenarioOp::Restart => "restart",
        ScenarioOp::DrainQueue => "drain_queue",
        ScenarioOp::ShutdownAll => "shutdown_all",
    }
}

/// One fixed-corpus entry (`tests/fixtures/scenarios/corpus.json`): a seed,
/// the vocabulary version that gives it meaning, its generation bounds, and
/// the defect it pins.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct CorpusEntry {
    pub seed: u64,
    pub vocabulary_version: u32,
    pub max_ops: usize,
    /// "sparse" | "moderate" | "hostile".
    pub intensity: String,
    /// Provenance line naming the defect or gate finding this seed pins.
    pub pins: String,
}

fn corpus_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/scenarios/corpus.json")
}

fn corpus_intensity(
    entry: &CorpusEntry,
    index: usize,
) -> Result<crate::support::fault_plan::Intensity, String> {
    match entry.intensity.as_str() {
        "sparse" => Ok(crate::support::fault_plan::Intensity::Sparse),
        "moderate" => Ok(crate::support::fault_plan::Intensity::Moderate),
        "hostile" => Ok(crate::support::fault_plan::Intensity::Hostile),
        intensity => Err(format!(
            "corpus entry {index} (seed {}) has unknown intensity {intensity:?}",
            entry.seed
        )),
    }
}

fn load_corpus(
    path: &std::path::Path,
) -> Result<Vec<(CorpusEntry, crate::support::fault_plan::Intensity)>, String> {
    let bytes = std::fs::read(path).map_err(|error| {
        format!(
            "corpus entry source {} could not be read: {error}",
            path.display()
        )
    })?;
    let entries: Vec<CorpusEntry> = serde_json::from_slice(&bytes).map_err(|error| {
        format!(
            "corpus entry in {} could not be parsed: {error}",
            path.display()
        )
    })?;
    if entries.is_empty() {
        return Err(format!(
            "corpus entry list in {} must not be empty",
            path.display()
        ));
    }

    entries
        .into_iter()
        .enumerate()
        .map(|(index, entry)| {
            if entry.vocabulary_version != crate::support::fault_plan::FAULT_VOCABULARY_VERSION {
                return Err(format!(
                    "corpus entry {index} (seed {}) has vocabulary_version {}, expected {}",
                    entry.seed,
                    entry.vocabulary_version,
                    crate::support::fault_plan::FAULT_VOCABULARY_VERSION
                ));
            }
            let intensity = corpus_intensity(&entry, index)?;
            Ok((entry, intensity))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::support::fault_plan::CrashCutHost as _;
    use futures_util::FutureExt as _;
    use verlet_io_core::IngressQueueStore as _;
    use verlet_io_core::IngressSink as _;

    #[derive(Debug, serde::Serialize)]
    struct SweepFailure {
        seed: u64,
        kind: &'static str,
        detail: String,
    }

    #[derive(serde::Serialize)]
    struct SweepReceipt {
        base_seed: u64,
        count: usize,
        max_ops: usize,
        per_intensity_tallies: std::collections::BTreeMap<&'static str, usize>,
        failures: Vec<SweepFailure>,
        corpus_size: usize,
        commit_sha: String,
        status: &'static str,
    }

    fn parse_sweep_env(name: &str, default: Option<&str>) -> u64 {
        let value = verlet_runtime_contracts::env_compat::var(name)
            .ok()
            .or_else(|| default.map(str::to_owned))
            .unwrap_or_else(|| panic!("{name} is required for scenario_nightly_sweep"));
        value
            .parse::<u64>()
            .unwrap_or_else(|error| panic!("{name} must be a u64, got {value:?}: {error}"))
    }

    fn first_transcript_mismatch(
        first: &crate::support::transcript::NormalizedTranscript,
        second: &crate::support::transcript::NormalizedTranscript,
    ) -> Option<usize> {
        first
            .items
            .iter()
            .zip(&second.items)
            .position(|(left, right)| left != right)
            .or_else(|| {
                (first.items.len() != second.items.len())
                    .then_some(first.items.len().min(second.items.len()))
            })
    }

    fn outcome_transcript(
        outcome: &Result<
            crate::support::transcript::NormalizedTranscript,
            crate::support::scenario::ScenarioFailure,
        >,
    ) -> &crate::support::transcript::NormalizedTranscript {
        match outcome {
            Ok(transcript) => transcript,
            Err(failure) => &failure.transcript,
        }
    }

    fn panic_detail(payload: Box<dyn std::any::Any + Send>) -> String {
        if let Some(message) = payload.downcast_ref::<String>() {
            message.clone()
        } else if let Some(message) = payload.downcast_ref::<&str>() {
            (*message).to_string()
        } else {
            "non-string panic payload".to_string()
        }
    }

    async fn catch_scenario_once(
        scenario: &crate::support::scenario::Scenario,
    ) -> Result<
        Result<
            crate::support::transcript::NormalizedTranscript,
            crate::support::scenario::ScenarioFailure,
        >,
        String,
    > {
        std::panic::AssertUnwindSafe(crate::support::scenario::run_scenario_once(scenario, &[]))
            .catch_unwind()
            .await
            .map_err(panic_detail)
    }

    fn write_sweep_receipt(receipt: &SweepReceipt) {
        let json = serde_json::to_string_pretty(receipt).expect("serialize nightly sweep receipt");
        if let Ok(path) =
            verlet_runtime_contracts::env_compat::var("VERLET_SCENARIO_SWEEP_RECEIPT_PATH")
        {
            std::fs::write(&path, format!("{json}\n"))
                .unwrap_or_else(|error| panic!("write nightly sweep receipt to {path}: {error}"));
        } else {
            eprintln!("scenario nightly receipt:\n{json}");
        }
    }

    fn no_fault_scenario(
        seed: u64,
        ops: Vec<crate::support::scenario::ScenarioOp>,
    ) -> crate::support::scenario::Scenario {
        crate::support::scenario::Scenario {
            seed,
            ops,
            plan: crate::support::fault_plan::FaultPlan {
                seed,
                vocabulary_version: crate::support::fault_plan::FAULT_VOCABULARY_VERSION,
                intensity: crate::support::fault_plan::Intensity::Sparse,
                directives: Vec::new(),
            },
        }
    }

    #[test]
    fn derivation_is_repeatable_bounded_and_well_formed() {
        for intensity in [
            crate::support::fault_plan::Intensity::Sparse,
            crate::support::fault_plan::Intensity::Moderate,
            crate::support::fault_plan::Intensity::Hostile,
        ] {
            for seed in 0..256 {
                let first = crate::support::scenario::Scenario::derive(
                    seed,
                    crate::support::scenario::ScenarioBounds {
                        max_ops: 12,
                        intensity,
                    },
                );
                let second = crate::support::scenario::Scenario::derive(
                    seed,
                    crate::support::scenario::ScenarioBounds {
                        max_ops: 12,
                        intensity,
                    },
                );
                assert_eq!(first.ops, second.ops);
                assert_eq!(first.plan, second.plan);
                assert!(!first.ops.is_empty());
                assert!(first.ops.len() <= 12);
                assert_eq!(
                    first.ops[0],
                    crate::support::scenario::ScenarioOp::StartThread
                );
                if let Some(shutdown) = first
                    .ops
                    .iter()
                    .position(|op| *op == crate::support::scenario::ScenarioOp::ShutdownAll)
                {
                    assert_eq!(shutdown + 1, first.ops.len());
                }
                assert!(
                    first
                        .ops
                        .iter()
                        .filter(|op| **op == crate::support::scenario::ScenarioOp::Restart)
                        .count()
                        <= first
                            .plan
                            .directives
                            .iter()
                            .filter(|directive| {
                                directive.component
                                    == crate::support::fault_plan::FaultComponent::Process
                            })
                            .count()
                );
            }
        }
    }

    #[test]
    fn operation_lane_is_independent_of_fault_intensity() {
        let seed = 0x4050_0003;
        let ops = [
            crate::support::fault_plan::Intensity::Sparse,
            crate::support::fault_plan::Intensity::Moderate,
            crate::support::fault_plan::Intensity::Hostile,
        ]
        .map(|intensity| {
            crate::support::scenario::Scenario::derive(
                seed,
                crate::support::scenario::ScenarioBounds {
                    max_ops: 12,
                    intensity,
                },
            )
            .ops
        });
        assert_eq!(ops[0], ops[1]);
        assert_eq!(ops[1], ops[2]);
    }

    #[test]
    fn zero_bound_derives_an_empty_sequence() {
        let scenario = crate::support::scenario::Scenario::derive(
            7,
            crate::support::scenario::ScenarioBounds {
                max_ops: 0,
                intensity: crate::support::fault_plan::Intensity::Sparse,
            },
        );
        assert!(scenario.ops.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn seeded_runner_engages_inv2_inv3_inv5_inv6_inv7_and_inv8_witnesses() {
        if !crate::support::scenario_unit_harness() {
            return;
        }
        let scenario = no_fault_scenario(
            0x4030_0001,
            vec![
                crate::support::scenario::ScenarioOp::StartThread,
                crate::support::scenario::ScenarioOp::SubmitTurn,
                crate::support::scenario::ScenarioOp::Fork,
                crate::support::scenario::ScenarioOp::DrainQueue,
                crate::support::scenario::ScenarioOp::ShutdownAll,
            ],
        );
        let transcript = crate::support::scenario::run_scenario_once(&scenario, &[])
            .await
            .expect("normal seeded scenario should hold every invariant");

        assert!(transcript.items.iter().any(|item| {
            item.kind == "event" && item.value.pointer("/payload/runtime_state").is_some()
        }));
        assert!(
            transcript
                .items
                .iter()
                .any(|item| item.label == "queue.lease")
        );
        assert!(transcript.items.iter().any(|item| {
            item.kind == "event"
                && item.value.get("kind").and_then(serde_json::Value::as_str)
                    == Some("io.ingress.claimed")
        }));
        assert!(transcript.items.iter().any(|item| {
            item.value
                .pointer("/payload/intent/outcome")
                .and_then(serde_json::Value::as_str)
                == Some("fork")
        }));
        assert!(
            transcript
                .items
                .iter()
                .any(|item| item.label == "thread.reservation")
        );

        let recovery = crate::support::scenario::Scenario {
            seed: 0x4030_0005,
            ops: vec![
                crate::support::scenario::ScenarioOp::StartThread,
                crate::support::scenario::ScenarioOp::Restart,
            ],
            plan: crate::support::fault_plan::FaultPlan {
                seed: 0x4030_0005,
                vocabulary_version: crate::support::fault_plan::FAULT_VOCABULARY_VERSION,
                intensity: crate::support::fault_plan::Intensity::Sparse,
                directives: vec![crate::support::fault_plan::FaultDirective {
                    component: crate::support::fault_plan::FaultComponent::Process,
                    operation: "ingress-binding",
                    nth: 1,
                    timing: crate::support::fault_plan::FaultTiming::Before,
                    action: crate::support::fault_plan::PlannedAction::Fail,
                }],
            },
        };
        let recovery = crate::support::scenario::run_scenario_once(&recovery, &[])
            .await
            .expect("real crash-cut scenario should engage inv5");
        let terminal = recovery
            .items
            .iter()
            .position(|item| {
                item.kind == "event"
                    && item.value.pointer("/payload/resident_state").is_some()
                    && item.value.pointer("/payload/reservation_key").is_some()
            })
            .expect("inv5 requires a durable terminal-resident witness");
        let probe = recovery
            .items
            .iter()
            .position(|item| item.label == "recovery.probe")
            .expect("inv5 requires a completed recovery probe");
        let progress = recovery
            .items
            .iter()
            .enumerate()
            .skip(probe + 1)
            .find(|(_, item)| {
                item.value
                    .pointer("/payload/reservation_progress")
                    .is_some()
            })
            .map(|(index, _)| index)
            .expect("inv5 requires durable progress after recovery");
        assert!(terminal < probe && probe < progress);
    }

    #[tokio::test(start_paused = true)]
    async fn completed_lease_is_not_lost_when_later_fault_advances_clock_past_expiry() {
        if !crate::support::scenario_unit_harness() {
            return;
        }
        let seed = 4_286_450_925_398_396_449;
        let scenario = crate::support::scenario::Scenario {
            seed,
            ops: vec![
                crate::support::scenario::ScenarioOp::StartThread,
                crate::support::scenario::ScenarioOp::StartThread,
            ],
            plan: crate::support::fault_plan::FaultPlan {
                seed,
                vocabulary_version: crate::support::fault_plan::FAULT_VOCABULARY_VERSION,
                intensity: crate::support::fault_plan::Intensity::Sparse,
                directives: vec![crate::support::fault_plan::FaultDirective {
                    component: crate::support::fault_plan::FaultComponent::Queue,
                    operation: "lease_ingress",
                    nth: 2,
                    timing: crate::support::fault_plan::FaultTiming::Before,
                    action: crate::support::fault_plan::PlannedAction::Fail,
                }],
            },
        };

        crate::support::scenario::run_scenario_once(&scenario, &[])
            .await
            .expect("a completed first lease must not require redelivery");
    }

    #[tokio::test]
    async fn scenario_queue_accepts_message_id_completion_after_visibility_expiry() {
        let tick = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let queue = crate::support::scenario::ScenarioQueue::new(std::sync::Arc::clone(&tick));
        let source = crate::support::scenario::ScenarioHarness::source();
        let envelope = verlet_io_core::IngressEnvelope::new(
            source.clone(),
            crate::support::scenario::ScenarioHarness::conversation(0),
            verlet_io_core::IngressContent::text("accepted stale completion"),
            0,
        )
        .with_dedupe_key(verlet_io_core::IoDedupeKey::for_source(
            &source,
            "stale-completion",
        ))
        .with_delivery(verlet_io_core::IoDelivery::new("stale-completion"))
        .with_principal(verlet_io_core::IoPrincipal::new(
            "scenario-tenant",
            "scenario-user",
            "route:scenario",
        ));
        queue.submit(envelope).await.unwrap();
        let leased = queue.lease_ingress("worker-a", 1, 1).await.unwrap();
        assert_eq!(leased.len(), 1);
        tick.store(2, std::sync::atomic::Ordering::SeqCst);

        queue
            .complete_ingress(&leased[0].message_id)
            .await
            .expect("message-id queue contract accepts completion after expiry");

        assert!(
            queue
                .lease_ingress("worker-b", 1, 1)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(queue.pending_count().await, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn fixed_seed_smokes_cover_each_intensity() {
        if !crate::support::scenario_unit_harness() {
            return;
        }
        for (intensity, seeds) in [
            (
                crate::support::fault_plan::Intensity::Sparse,
                [0x4031_0001, 0x4031_0003, 0x4031_0005],
            ),
            (
                crate::support::fault_plan::Intensity::Moderate,
                [0x4032_0001, 0x4032_0003, 0x4032_0005],
            ),
            (
                crate::support::fault_plan::Intensity::Hostile,
                [0x4033_0001, 0x4033_0003, 0x4033_0005],
            ),
        ] {
            for seed in seeds {
                let scenario = crate::support::scenario::Scenario::derive(
                    seed,
                    crate::support::scenario::ScenarioBounds {
                        max_ops: 4,
                        intensity,
                    },
                );
                if let Err(failure) =
                    crate::support::scenario::run_scenario_once(&scenario, &[]).await
                {
                    panic!(
                        "fixed seed {seed:#x} ({intensity:?}) failed at step {}: {:?}",
                        failure.failing_step, failure.violations
                    );
                }
            }
        }
    }

    /// Runs every fail-closed fixed-corpus entry through the normal minimized
    /// scenario runner. Missing, empty, malformed, stale, or unknown-intensity
    /// entries are test failures and never skips.
    #[tokio::test(start_paused = true)]
    async fn scenario_corpus_holds() {
        if !crate::support::scenario_unit_harness() {
            return;
        }
        let started = std::time::Instant::now();
        let corpus =
            crate::support::scenario::load_corpus(&crate::support::scenario::corpus_path())
                .unwrap_or_else(|error| panic!("{error}"));
        let corpus_size = corpus.len();
        for (entry, intensity) in corpus {
            eprintln!(
                "scenario corpus: running seed {} intensity={} max_ops={} pin={}",
                entry.seed, entry.intensity, entry.max_ops, entry.pins
            );
            let scenario = crate::support::scenario::Scenario::derive(
                entry.seed,
                crate::support::scenario::ScenarioBounds {
                    max_ops: entry.max_ops,
                    intensity,
                },
            );
            if let Err(failure) = crate::support::scenario::run_scenario(scenario, &[]).await {
                panic!("corpus entry seed {} failed: {failure:?}", entry.seed);
            }
        }
        eprintln!(
            "scenario corpus: {corpus_size} entries passed in {:.3}s",
            started.elapsed().as_secs_f64()
        );
    }

    #[test]
    fn corpus_loader_fails_closed_when_path_is_wired_wrong() {
        let error = crate::support::scenario::load_corpus(std::path::Path::new(
            "definitely-missing-scenario-corpus.json",
        ))
        .unwrap_err();
        assert!(error.contains("definitely-missing-scenario-corpus.json"));
        assert!(error.contains("could not be read"));
    }

    #[test]
    fn corpus_loader_fails_closed_on_vocabulary_version_mismatch() {
        let path = std::env::temp_dir().join(format!(
            "verlet-scenario-corpus-version-mismatch-{}.json",
            std::process::id()
        ));
        std::fs::write(
            &path,
            r#"[{"seed":404,"vocabulary_version":999,"max_ops":4,"intensity":"sparse","pins":"test"}]"#,
        )
        .expect("write temporary corpus");
        let error = crate::support::scenario::load_corpus(&path).unwrap_err();
        let _ = std::fs::remove_file(path);
        assert!(error.contains("entry 0 (seed 404)"));
        assert!(error.contains("vocabulary_version 999"));
    }

    /// Runs the rotating nightly lane. `VERLET_SCENARIO_SWEEP_BASE_SEED` is
    /// required and must be a u64. `VERLET_SCENARIO_SWEEP_COUNT` defaults to
    /// 24 and `VERLET_SCENARIO_SWEEP_MAX_OPS` defaults to 8. The optional
    /// receipt path and commit SHA variables are workflow witnesses. The test
    /// is excluded from normal suites only by `#[ignore]`. Missing or invalid
    /// required env and an invalid fixed corpus fail closed.
    #[tokio::test(start_paused = true)]
    #[ignore = "rotating nightly scenario sweep"]
    async fn scenario_nightly_sweep() {
        if !crate::support::scenario_unit_harness() {
            return;
        }
        let base_seed = parse_sweep_env("VERLET_SCENARIO_SWEEP_BASE_SEED", None);
        let count = parse_sweep_env("VERLET_SCENARIO_SWEEP_COUNT", Some("24")) as usize;
        let max_ops = parse_sweep_env("VERLET_SCENARIO_SWEEP_MAX_OPS", Some("8")) as usize;
        let corpus_size =
            crate::support::scenario::load_corpus(&crate::support::scenario::corpus_path())
                .unwrap_or_else(|error| panic!("{error}"))
                .len();
        let commit_sha =
            verlet_runtime_contracts::env_compat::var("VERLET_SCENARIO_SWEEP_COMMIT_SHA")
                .unwrap_or_else(|_| "local".to_string());
        let mut root = crate::support::fault_plan::SplitMix64::new(base_seed);
        let mut lane = root.split("scenario-nightly-v1");
        let mut tallies =
            std::collections::BTreeMap::from([("sparse", 0), ("moderate", 0), ("hostile", 0)]);
        let mut failures = Vec::new();

        for index in 0..count {
            let seed = lane.next_u64();
            let (intensity_name, intensity) = match index % 3 {
                0 => ("sparse", crate::support::fault_plan::Intensity::Sparse),
                1 => ("moderate", crate::support::fault_plan::Intensity::Moderate),
                _ => ("hostile", crate::support::fault_plan::Intensity::Hostile),
            };
            *tallies.get_mut(intensity_name).unwrap() += 1;
            eprintln!("sweep progress: index {index} intensity {intensity_name} seed {seed}");
            let derive = || {
                crate::support::scenario::Scenario::derive(
                    seed,
                    crate::support::scenario::ScenarioBounds { max_ops, intensity },
                )
            };
            let first = catch_scenario_once(&derive()).await;
            let second = catch_scenario_once(&derive()).await;

            if let (Ok(first), Ok(second)) = (&first, &second) {
                let first_transcript = outcome_transcript(first);
                let second_transcript = outcome_transcript(second);
                if first_transcript != second_transcript {
                    let mismatch = first_transcript_mismatch(first_transcript, second_transcript);
                    failures.push(SweepFailure {
                        seed,
                        kind: "same-seed-drift",
                        detail: format!(
                            "same-seed transcript mismatch at {mismatch:?}: left={:?} right={:?}",
                            mismatch.and_then(|item| first_transcript.items.get(item)),
                            mismatch.and_then(|item| second_transcript.items.get(item)),
                        ),
                    });
                }
            }
            if let Err(detail) = &first {
                failures.push(SweepFailure {
                    seed,
                    kind: "runner-panic",
                    detail: format!("first same-seed run panicked: {detail}"),
                });
            }
            if let Err(detail) = &second {
                failures.push(SweepFailure {
                    seed,
                    kind: "runner-panic",
                    detail: format!("second same-seed run panicked: {detail}"),
                });
            }
            if let Ok(Err(failure)) = first {
                eprintln!("nightly scenario failure for seed {seed}: {failure:?}");
                match std::panic::AssertUnwindSafe(crate::support::scenario::run_scenario(
                    derive(),
                    &[],
                ))
                .catch_unwind()
                .await
                {
                    Ok(Err(minimized)) => failures.push(SweepFailure {
                        seed,
                        kind: "scenario-failure",
                        detail: format!(
                            "vocabulary {} failed at step {} with {:?}",
                            minimized.vocabulary_version,
                            minimized.failing_step,
                            minimized.violations
                        ),
                    }),
                    Ok(Ok(())) => failures.push(SweepFailure {
                        seed,
                        kind: "nondeterministic-failure",
                        detail: "first run failed but minimization did not reproduce it"
                            .to_string(),
                    }),
                    Err(payload) => failures.push(SweepFailure {
                        seed,
                        kind: "minimizer-panic",
                        detail: panic_detail(payload),
                    }),
                }
            }
        }

        let receipt = SweepReceipt {
            base_seed,
            count,
            max_ops,
            per_intensity_tallies: tallies,
            status: if failures.is_empty() {
                "passed"
            } else {
                "failed"
            },
            failures,
            corpus_size,
            commit_sha,
        };
        write_sweep_receipt(&receipt);
        assert!(
            receipt.failures.is_empty(),
            "nightly scenario sweep failures: {:?}",
            receipt.failures
        );
    }

    #[tokio::test(start_paused = true)]
    async fn same_seed_has_byte_identical_literal_stream_transcript() {
        if !crate::support::scenario_unit_harness() {
            return;
        }
        let scenario = || {
            let seed = 0x4030_0002;
            crate::support::scenario::Scenario {
                seed,
                ops: vec![
                    crate::support::scenario::ScenarioOp::StartThread,
                    crate::support::scenario::ScenarioOp::Restart,
                    crate::support::scenario::ScenarioOp::SubmitTurn,
                    crate::support::scenario::ScenarioOp::Fork,
                    crate::support::scenario::ScenarioOp::DrainQueue,
                    crate::support::scenario::ScenarioOp::ShutdownAll,
                ],
                plan: crate::support::fault_plan::FaultPlan {
                    seed,
                    vocabulary_version: crate::support::fault_plan::FAULT_VOCABULARY_VERSION,
                    intensity: crate::support::fault_plan::Intensity::Sparse,
                    directives: vec![crate::support::fault_plan::FaultDirective {
                        component: crate::support::fault_plan::FaultComponent::Process,
                        operation: "ingress-binding",
                        nth: 1,
                        timing: crate::support::fault_plan::FaultTiming::Before,
                        action: crate::support::fault_plan::PlannedAction::Fail,
                    }],
                },
            }
        };
        let first = crate::support::scenario::run_scenario_once(&scenario(), &[])
            .await
            .unwrap();
        let second = crate::support::scenario::run_scenario_once(&scenario(), &[])
            .await
            .unwrap();
        if first != second {
            let mismatch = first
                .items
                .iter()
                .zip(&second.items)
                .position(|(left, right)| left != right);
            panic!(
                "same-seed transcript mismatch at {mismatch:?}: left={:?} right={:?}",
                mismatch.and_then(|index| first.items.get(index)),
                mismatch.and_then(|index| second.items.get(index)),
            );
        }
        assert!(
            first
                .items
                .iter()
                .filter(|item| item.kind == "event")
                .all(|item| {
                    item.value
                        .get("stream_id")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|stream_id| !stream_id.starts_with('$'))
                })
        );
    }

    #[tokio::test(start_paused = true)]
    async fn emo_514_pin_records_submit_fault_without_claiming_cut_reached() {
        if !crate::support::scenario_unit_harness() {
            return;
        }
        let derive = || {
            crate::support::scenario::Scenario::derive(
                13970258769908900442,
                crate::support::scenario::ScenarioBounds {
                    max_ops: 8,
                    intensity: crate::support::fault_plan::Intensity::Moderate,
                },
            )
        };
        assert_eq!(
            derive().ops,
            vec![
                crate::support::scenario::ScenarioOp::StartThread,
                crate::support::scenario::ScenarioOp::Fork,
                crate::support::scenario::ScenarioOp::SubmitTurn,
                crate::support::scenario::ScenarioOp::Restart,
                crate::support::scenario::ScenarioOp::Steer,
                crate::support::scenario::ScenarioOp::Fork,
                crate::support::scenario::ScenarioOp::Fork,
                crate::support::scenario::ScenarioOp::StartThread,
            ]
        );
        let first = crate::support::scenario::run_scenario_once(&derive(), &[])
            .await
            .expect("EMO-514 pin should not panic or violate an invariant");
        let second = crate::support::scenario::run_scenario_once(&derive(), &[])
            .await
            .expect("EMO-514 pin should be repeatable");

        assert_eq!(first, second);
        assert!(first.items.iter().any(|item| {
            item.label == "scenario.operation.error"
                && item
                    .value
                    .get("operation")
                    .and_then(serde_json::Value::as_str)
                    == Some("crash_cut_submit")
                && item
                    .value
                    .get("error")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|error| error.contains("submit occurrence 2"))
        }));
        assert!(
            first
                .items
                .iter()
                .all(|item| item.label != "scenario.crash_cut"),
            "a faulted setup submit must not claim that its registered seam was reached"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn provider_cut_delay_does_not_hide_cut_notification_emo_524() {
        if !crate::support::scenario_unit_harness() {
            return;
        }
        let seed = 0x5240_0001;
        let scenario = crate::support::scenario::Scenario {
            seed,
            ops: vec![crate::support::scenario::ScenarioOp::Restart],
            plan: crate::support::fault_plan::FaultPlan {
                seed,
                vocabulary_version: crate::support::fault_plan::FAULT_VOCABULARY_VERSION,
                intensity: crate::support::fault_plan::Intensity::Sparse,
                directives: vec![
                    crate::support::fault_plan::FaultDirective {
                        component: crate::support::fault_plan::FaultComponent::Provider,
                        operation: "complete",
                        nth: 1,
                        timing: crate::support::fault_plan::FaultTiming::Before,
                        action: crate::support::fault_plan::PlannedAction::Delay(
                            std::time::Duration::from_millis(1),
                        ),
                    },
                    crate::support::fault_plan::FaultDirective {
                        component: crate::support::fault_plan::FaultComponent::Process,
                        operation: "queue-input-compile",
                        nth: 1,
                        timing: crate::support::fault_plan::FaultTiming::Before,
                        action: crate::support::fault_plan::PlannedAction::Fail,
                    },
                ],
            },
        };

        let transcript = crate::support::scenario::run_scenario_once(&scenario, &[])
            .await
            .expect("a delayed provider cut must still be observed before quiescence");
        assert!(transcript.items.iter().any(|item| {
            item.label == "scenario.crash_cut"
                && item.value.get("seam").and_then(serde_json::Value::as_str)
                    == Some("PersistedInputRuntimeNotify")
        }));
    }

    #[tokio::test(start_paused = true)]
    async fn fresh_sweep_output_hash_seeds_are_same_seed_deterministic() {
        if !crate::support::scenario_unit_harness() {
            return;
        }
        for seed in [12756048029454721330, 4861954629787943465] {
            let derive = || {
                crate::support::scenario::Scenario::derive(
                    seed,
                    crate::support::scenario::ScenarioBounds {
                        max_ops: 8,
                        intensity: crate::support::fault_plan::Intensity::Hostile,
                    },
                )
            };
            let first = crate::support::scenario::run_scenario_once(&derive(), &[])
                .await
                .unwrap();
            let second = crate::support::scenario::run_scenario_once(&derive(), &[])
                .await
                .unwrap();
            assert_eq!(
                first, second,
                "fresh sweep seed {seed} drifted between same-seed runs"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn scenario_steer_waits_for_persisted_input_when_idle_cannot_signal_completion() {
        if !crate::support::scenario_unit_harness() {
            return;
        }
        let seed = 0x4250_0001;
        let scenario = no_fault_scenario(
            seed,
            vec![crate::support::scenario::ScenarioOp::StartThread],
        );
        let run_root = crate::support::scenario::ScenarioRunRoot::new(seed);
        let mut harness = crate::support::scenario::ScenarioHarness::build(
            run_root.path.clone(),
            crate::support::scenario::clone_plan(&scenario.plan),
            true,
            None,
        )
        .await;
        harness
            .execute(crate::support::scenario::ScenarioOp::StartThread)
            .await;

        let control = std::sync::Arc::clone(&harness.store_control);
        control
            .pause_next_turn_input
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let steer = tokio::spawn(async move {
            harness
                .execute(crate::support::scenario::ScenarioOp::Steer)
                .await;
            harness
        });
        control.turn_input_started.notified().await;
        for _ in 0..256 {
            tokio::task::yield_now().await;
        }
        assert!(
            !steer.is_finished(),
            "scenario advanced while the idle steer input was not durable"
        );

        control.release_turn_input.notify_one();
        let harness = steer.await.expect("join forced steer interleaving");
        harness.server.supervisor().shutdown_all().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn queue_complete_cut_does_not_advance_clock_while_lease_processing_is_in_flight() {
        if !crate::support::scenario_unit_harness() {
            return;
        }
        let seed = 0x4250_0004;
        let scenario = no_fault_scenario(seed, Vec::new());
        let run_root = crate::support::scenario::ScenarioRunRoot::new(seed);
        let mut harness = crate::support::scenario::ScenarioHarness::build(
            run_root.path.clone(),
            crate::support::scenario::clone_plan(&scenario.plan),
            true,
            None,
        )
        .await;
        let queue = std::sync::Arc::clone(&harness.queue_inner);
        let tick = std::sync::Arc::clone(&harness.tick);
        let starting_tick = tick.load(std::sync::atomic::Ordering::SeqCst);
        queue
            .pause_before_next_complete
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let cut = tokio::spawn(async move {
            harness
                .run_to_cut(crate::support::fault_plan::CrashCutSeam::QueueCompleteBarrier)
                .await;
            harness
        });
        queue.before_complete_started.notified().await;
        for _ in 0..4_096 {
            if tick.load(std::sync::atomic::Ordering::SeqCst) != starting_tick {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            queue.lease_attempts().await,
            vec![1],
            "scenario redelivered a message whose first lease was still in store IO"
        );
        assert_eq!(
            tick.load(std::sync::atomic::Ordering::SeqCst),
            starting_tick,
            "scenario queue clock advanced while a leased message was still in store IO"
        );

        queue.release_before_complete.notify_one();
        let harness = cut.await.expect("join forced queue lease interleaving");
        harness.server.supervisor().shutdown_all().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn scenario_wait_for_idle_rechecks_queue_depth_without_status_edge() {
        if !crate::support::scenario_unit_harness() {
            return;
        }
        let seed = 0x4250_0002;
        let scenario = no_fault_scenario(
            seed,
            vec![crate::support::scenario::ScenarioOp::StartThread],
        );
        let run_root = crate::support::scenario::ScenarioRunRoot::new(seed);
        let mut harness = crate::support::scenario::ScenarioHarness::build(
            run_root.path.clone(),
            crate::support::scenario::clone_plan(&scenario.plan),
            true,
            None,
        )
        .await;
        harness
            .execute(crate::support::scenario::ScenarioOp::StartThread)
            .await;
        let coordinates = harness
            .bound_coordinates(harness.current_root)
            .expect("started scenario root remains bound");
        let handle = harness
            .server
            .supervisor()
            .get_thread_at(&coordinates)
            .await
            .expect("started scenario root remains resident");

        let control = std::sync::Arc::clone(&harness.store_control);
        control
            .pause_next_turn_input
            .store(true, std::sync::atomic::Ordering::SeqCst);
        harness
            .server
            .supervisor()
            .submit_to_with_mode(
                &coordinates,
                format!("scenario-steer-{seed}-1"),
                "steer",
                verlet::TurnSubmissionMode::Steer,
            )
            .await
            .expect("submit paused idle steer");
        control.turn_input_started.notified().await;
        handle
            .send(verlet::ThreadCommand::CancelTurn {
                watchdog_token_id: u64::MAX,
                reason: "scenario no-op cancel".to_string(),
            })
            .await
            .expect("queue no-op cancel behind paused steer");
        assert_eq!(handle.queued_command_count(), 1);

        let waiting = tokio::spawn(async move {
            harness.wait_for_idle(&coordinates).await;
            harness
        });
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert!(
            !waiting.is_finished(),
            "wait returned before the queue drained"
        );

        control.release_turn_input.notify_one();
        let started = std::time::Instant::now();
        while !waiting.is_finished() && started.elapsed() < std::time::Duration::from_secs(30) {
            tokio::time::sleep(crate::support::scenario::SCENARIO_ASYNC_RECHECK_INTERVAL).await;
        }
        assert!(
            waiting.is_finished(),
            "wait did not re-check after a queue drain without a status edge"
        );
        let harness = waiting.await.expect("join queue-depth wait regression");
        harness.server.supervisor().shutdown_all().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn scenario_turn_input_wait_recovers_from_lag_and_terminal_after_durability() {
        if !crate::support::scenario_unit_harness() {
            return;
        }
        let seed = 0x4250_0003;
        let scenario = no_fault_scenario(
            seed,
            vec![crate::support::scenario::ScenarioOp::StartThread],
        );
        let run_root = crate::support::scenario::ScenarioRunRoot::new(seed);
        let mut harness = crate::support::scenario::ScenarioHarness::build(
            run_root.path.clone(),
            crate::support::scenario::clone_plan(&scenario.plan),
            true,
            None,
        )
        .await;
        harness
            .execute(crate::support::scenario::ScenarioOp::StartThread)
            .await;
        harness
            .execute(crate::support::scenario::ScenarioOp::Steer)
            .await;
        let coordinates = harness
            .bound_coordinates(harness.current_root)
            .expect("started scenario root remains bound");
        let turn_id = format!("scenario-steer-{seed}-{}", harness.envelope_index);

        let (lagged_tx, mut lagged_events) = tokio::sync::broadcast::channel(1);
        for text in ["first", "second"] {
            lagged_tx
                .send(verlet::ThreadEvent::Output {
                    thread_id: coordinates.thread_id,
                    text: text.to_string(),
                })
                .expect("send synthetic lag event");
        }
        harness
            .wait_for_turn_input(&coordinates, &mut lagged_events, &turn_id)
            .await;

        let (terminal_tx, mut terminal_events) = tokio::sync::broadcast::channel(1);
        terminal_tx
            .send(verlet::ThreadEvent::Failed {
                thread_id: coordinates.thread_id,
                message: "synthetic terminal after durability".to_string(),
            })
            .expect("send synthetic terminal event");
        harness
            .wait_for_turn_input(&coordinates, &mut terminal_events, &turn_id)
            .await;

        harness.server.supervisor().shutdown_all().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn restart_rebuilds_the_real_daemon_over_the_surviving_store() {
        if !crate::support::scenario_unit_harness() {
            return;
        }
        let seed = 0x4030_0003;
        let scenario = crate::support::scenario::Scenario {
            seed,
            ops: vec![
                crate::support::scenario::ScenarioOp::StartThread,
                crate::support::scenario::ScenarioOp::Restart,
                crate::support::scenario::ScenarioOp::SubmitTurn,
                crate::support::scenario::ScenarioOp::ShutdownAll,
            ],
            plan: crate::support::fault_plan::FaultPlan {
                seed,
                vocabulary_version: crate::support::fault_plan::FAULT_VOCABULARY_VERSION,
                intensity: crate::support::fault_plan::Intensity::Sparse,
                directives: vec![crate::support::fault_plan::FaultDirective {
                    component: crate::support::fault_plan::FaultComponent::Process,
                    operation: "ingress-binding",
                    nth: 1,
                    timing: crate::support::fault_plan::FaultTiming::Before,
                    action: crate::support::fault_plan::PlannedAction::Fail,
                }],
            },
        };
        let transcript = crate::support::scenario::run_scenario_once(&scenario, &[])
            .await
            .expect("real daemon crash-cut scenario should recover");
        assert!(
            transcript
                .items
                .iter()
                .any(|item| item.label == "scenario.crash_cut")
        );
        assert!(
            transcript
                .items
                .iter()
                .any(|item| item.label == "recovery.probe")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn every_registered_cut_drives_a_real_component_and_recovers() {
        if !crate::support::scenario_unit_harness() {
            return;
        }
        for (index, operation) in crate::support::fault_plan::CUTS_V1
            .iter()
            .copied()
            .enumerate()
        {
            let seed = 0x4030_1000 + index as u64;
            let scenario = crate::support::scenario::Scenario {
                seed,
                ops: vec![
                    crate::support::scenario::ScenarioOp::StartThread,
                    crate::support::scenario::ScenarioOp::Restart,
                ],
                plan: crate::support::fault_plan::FaultPlan {
                    seed,
                    vocabulary_version: crate::support::fault_plan::FAULT_VOCABULARY_VERSION,
                    intensity: crate::support::fault_plan::Intensity::Sparse,
                    directives: vec![crate::support::fault_plan::FaultDirective {
                        component: crate::support::fault_plan::FaultComponent::Process,
                        operation,
                        nth: 1,
                        timing: crate::support::fault_plan::FaultTiming::Before,
                        action: crate::support::fault_plan::PlannedAction::Fail,
                    }],
                },
            };
            let transcript = crate::support::scenario::run_scenario_once(&scenario, &[])
                .await
                .unwrap_or_else(|failure| panic!("registered cut {operation} failed: {failure:?}"));
            assert!(transcript.items.iter().any(|item| {
                item.label == "scenario.crash_cut"
                    && item.value["seam"]
                        .as_str()
                        .is_some_and(|seam| !seam.is_empty())
            }));
            assert!(
                transcript
                    .items
                    .iter()
                    .any(|item| item.label == "recovery.probe"),
                "registered cut {operation} did not recover"
            );
        }
    }

    struct BrokenInvariant;

    #[async_trait::async_trait]
    impl crate::support::scenario::ScenarioInvariant for BrokenInvariant {
        fn name(&self) -> &'static str {
            "test-broken-invariant"
        }

        async fn check(
            &self,
            world: &crate::support::scenario::ScenarioWorld<'_>,
        ) -> Vec<crate::support::scenario::InvariantViolation> {
            (world.step >= 2)
                .then(|| crate::support::scenario::InvariantViolation {
                    invariant: self.name(),
                    detail: "deliberately broken after the third operation".to_string(),
                })
                .into_iter()
                .collect()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn broken_invariant_returns_a_minimized_readable_reproduction() {
        if !crate::support::scenario_unit_harness() {
            return;
        }
        let scenario = no_fault_scenario(
            0x4030_0004,
            vec![
                crate::support::scenario::ScenarioOp::StartThread,
                crate::support::scenario::ScenarioOp::SubmitTurn,
                crate::support::scenario::ScenarioOp::DrainQueue,
                crate::support::scenario::ScenarioOp::Cancel,
                crate::support::scenario::ScenarioOp::ShutdownAll,
            ],
        );
        let failure =
            crate::support::scenario::run_scenario(scenario, &[Box::new(BrokenInvariant)])
                .await
                .unwrap_err();
        let reproduction = failure
            .transcript
            .items
            .iter()
            .find(|item| item.label == "scenario.minimized_reproduction")
            .expect("failure should include its minimized reproduction");
        assert_eq!(
            reproduction.value["ops"].as_array().unwrap().len(),
            3,
            "the step-sensitive failure should minimize to three operations"
        );
        assert!(failure.violations[0].detail.contains("deliberately broken"));
    }

    struct ReservationCountInvariant;

    #[async_trait::async_trait]
    impl crate::support::scenario::ScenarioInvariant for ReservationCountInvariant {
        fn name(&self) -> &'static str {
            "test-reservation-count-invariant"
        }

        async fn check(
            &self,
            world: &crate::support::scenario::ScenarioWorld<'_>,
        ) -> Vec<crate::support::scenario::InvariantViolation> {
            (world.step >= 1)
                .then(|| {
                    let count = world
                        .transcript
                        .items
                        .iter()
                        .filter(|item| item.label == "thread.reservation")
                        .count();
                    crate::support::scenario::InvariantViolation {
                        invariant: self.name(),
                        detail: format!("reservation-count={count}"),
                    }
                })
                .into_iter()
                .collect()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn minimizer_preserves_the_original_violation_not_only_its_invariant_name() {
        if !crate::support::scenario_unit_harness() {
            return;
        }
        let scenario = no_fault_scenario(
            0x4050_0001,
            vec![
                crate::support::scenario::ScenarioOp::StartThread,
                crate::support::scenario::ScenarioOp::SubmitTurn,
                crate::support::scenario::ScenarioOp::DrainQueue,
                crate::support::scenario::ScenarioOp::Cancel,
            ],
        );
        let failure = crate::support::scenario::run_scenario(
            scenario,
            &[Box::new(ReservationCountInvariant)],
        )
        .await
        .unwrap_err();
        assert_eq!(failure.violations[0].detail, "reservation-count=1");
    }

    struct OverlapInvariant {
        rendezvous: std::sync::Arc<tokio::sync::Barrier>,
    }

    #[async_trait::async_trait]
    impl crate::support::scenario::ScenarioInvariant for OverlapInvariant {
        fn name(&self) -> &'static str {
            "test-overlapping-scenario-runs"
        }

        async fn check(
            &self,
            world: &crate::support::scenario::ScenarioWorld<'_>,
        ) -> Vec<crate::support::scenario::InvariantViolation> {
            if world.step == 0 {
                self.rendezvous.wait().await;
            }
            Vec::new()
        }
    }

    #[tokio::test]
    async fn same_seed_runs_do_not_require_process_global_serialization() {
        if !crate::support::scenario_unit_harness() {
            return;
        }
        let rendezvous = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let first_invariant: Vec<Box<dyn crate::support::scenario::ScenarioInvariant>> =
            vec![Box::new(OverlapInvariant {
                rendezvous: std::sync::Arc::clone(&rendezvous),
            })];
        let second_invariant: Vec<Box<dyn crate::support::scenario::ScenarioInvariant>> =
            vec![Box::new(OverlapInvariant { rendezvous })];
        let first = no_fault_scenario(
            0x4050_0002,
            vec![crate::support::scenario::ScenarioOp::StartThread],
        );
        let second = no_fault_scenario(
            0x4050_0002,
            vec![crate::support::scenario::ScenarioOp::StartThread],
        );
        let overlapping = tokio::time::timeout(std::time::Duration::from_secs(60), async {
            tokio::join!(
                crate::support::scenario::run_scenario_once(&first, &first_invariant),
                crate::support::scenario::run_scenario_once(&second, &second_invariant),
            )
        })
        .await
        .expect("scenario runs were serialized by process-global test state");
        assert_eq!(overlapping.0.unwrap(), overlapping.1.unwrap());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn simulated_io_same_seed_runs_have_identical_transcripts() {
        if !crate::support::scenario_unit_harness() {
            return;
        }
        const SEED: u64 = 0x4150_0005_D15E_A5E6;

        let first = crate::support::scenario::run_stream_io_crash_scenario(SEED)
            .await
            .expect("seeded IO crash scenario should recover");
        let second = crate::support::scenario::run_stream_io_crash_scenario(SEED)
            .await
            .expect("same seeded IO crash scenario should recover");

        assert_eq!(first.io_transcript, second.io_transcript);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn seeded_io_crash_cut_reopens_with_exactly_once_uncorrupted_stream() {
        if !crate::support::scenario_unit_harness() {
            return;
        }
        const SEED: u64 = 0x4150_0005_D15E_A5E5;

        let first = crate::support::scenario::run_stream_io_crash_scenario(SEED)
            .await
            .expect("seeded IO crash scenario should recover");

        assert_eq!(first.integrity_check, vec!["ok"]);
        assert_eq!(first.event_ids, first.expected_event_ids);
        assert_eq!(
            first.sequences,
            (1..=first.event_ids.len() as i64).collect::<Vec<_>>()
        );
    }
}
