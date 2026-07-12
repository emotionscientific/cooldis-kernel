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

use super::fault_plan::{
    CrashCutHost, CrashCutSeam, FAULT_VOCABULARY_VERSION, FaultComponent, FaultPlan, Intensity,
    SplitMix64,
};
use super::kernel_test::{
    APP_SERVER_LOCAL_MODEL, APP_SERVER_LOCAL_PROVIDER, AppServerListenAddr,
    CanonicalProviderRuntimeConfig, CanonicalProviderRuntimeFactory, CooldisAppServer,
    CooldisAppServerConfig, CooldisDaemonIoBridge, CooldisDaemonQueueWorker,
    CooldisEgressRetryConfig, CooldisIoRouteConfig, EventKind, EventProvenance, EventRecord,
    EventSequence, EventStore, EventStreamId, HistoryResult, LocalOfflineProviderClient,
    NewEventRecord, NewObservationRecord, ObservationRecord, ObservationStore, ProviderApi,
    ProviderCapabilityRecord, ProviderClient, ProviderRequest, ProviderResponse, ProviderResult,
    RuntimeHost, RuntimeStore, SessionContext, SessionEntry, SessionEntryId, SessionEntryKind,
    SessionStore, StreamCursorV1, ThreadBaseRef, ThreadCoordinates, ThreadId, ThreadStatus,
    TurnSubmissionMode,
};
use super::transcript::{NormalizedTranscript, NormalizedTranscriptItem, TypedTranscript};
use super::{Inv6ClaimsSettle, fork_invariants_v1, invariant_set_v1};
use async_trait::async_trait;
use cooldis_io_core::{
    ConversationKind, IngressAck, IngressContent, IngressEnvelope, IngressQueueStore, IngressSink,
    IoConversation, IoDedupeKey, IoResult, IoSource, LeasedIngressEnvelope, ThreadAddress,
};
use cooldis_io_pgqrs::sqlite_dsn;
use futures_util::FutureExt;
use serde::Serialize;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

struct ScenarioRunRoot {
    path: PathBuf,
}

impl ScenarioRunRoot {
    fn new(seed: u64) -> Self {
        Self {
            path: std::env::temp_dir()
                .join("cooldis-scenario-engine")
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
    inner: Arc<dyn RuntimeStore>,
    canonical_timestamp_ms: i64,
}

impl DynRuntimeStore {
    fn kind(&self, kind: SessionEntryKind) -> SessionEntryKind {
        let mut value = serde_json::to_value(&kind).expect("serialize scenario session entry");
        replace_timestamp_ms(&mut value, self.canonical_timestamp_ms);
        serde_json::from_value(value).expect("deserialize deterministic scenario session entry")
    }

    fn events(&self, mut records: Vec<NewEventRecord>) -> Vec<NewEventRecord> {
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
                    *value = json!(timestamp_ms);
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

#[async_trait]
impl SessionStore for DynRuntimeStore {
    async fn append(
        &self,
        coordinates: &ThreadCoordinates,
        parent_entry_id: Option<SessionEntryId>,
        kind: SessionEntryKind,
    ) -> HistoryResult<SessionEntry> {
        self.inner
            .append(coordinates, parent_entry_id, self.kind(kind))
            .await
    }

    async fn append_with_provenance(
        &self,
        coordinates: &ThreadCoordinates,
        parent_entry_id: Option<SessionEntryId>,
        kind: SessionEntryKind,
        provenance: EventProvenance,
    ) -> HistoryResult<SessionEntry> {
        self.inner
            .append_with_provenance(coordinates, parent_entry_id, self.kind(kind), provenance)
            .await
    }

    async fn append_turn_input(
        &self,
        coordinates: &ThreadCoordinates,
        turn_id: &str,
        kind: SessionEntryKind,
    ) -> HistoryResult<SessionEntry> {
        self.inner
            .append_turn_input(coordinates, turn_id, self.kind(kind))
            .await
    }

    async fn active_leaf(
        &self,
        coordinates: &ThreadCoordinates,
    ) -> HistoryResult<Option<SessionEntryId>> {
        self.inner.active_leaf(coordinates).await
    }

    async fn select_branch(
        &self,
        coordinates: &ThreadCoordinates,
        leaf_entry_id: Option<SessionEntryId>,
    ) -> HistoryResult<()> {
        self.inner.select_branch(coordinates, leaf_entry_id).await
    }

    async fn build_context(
        &self,
        coordinates: &ThreadCoordinates,
    ) -> HistoryResult<SessionContext> {
        self.inner.build_context(coordinates).await
    }

    async fn clone_branch(
        &self,
        source_coordinates: &ThreadCoordinates,
        source_leaf: Option<SessionEntryId>,
        target_coordinates: &ThreadCoordinates,
    ) -> HistoryResult<Option<SessionEntryId>> {
        self.inner
            .clone_branch(source_coordinates, source_leaf, target_coordinates)
            .await
    }

    async fn fork_by_reference(
        &self,
        source_coordinates: &ThreadCoordinates,
        target_coordinates: &ThreadCoordinates,
        base: ThreadBaseRef,
    ) -> HistoryResult<()> {
        self.inner
            .fork_by_reference(source_coordinates, target_coordinates, base)
            .await
    }
}

#[async_trait]
impl EventStore for DynRuntimeStore {
    async fn append_events(
        &self,
        stream_id: &EventStreamId,
        records: Vec<NewEventRecord>,
    ) -> HistoryResult<Vec<EventRecord>> {
        self.inner
            .append_events(stream_id, self.events(records))
            .await
    }

    async fn append_events_fenced(
        &self,
        stream_id: &EventStreamId,
        expected_next_sequence: EventSequence,
        records: Vec<NewEventRecord>,
    ) -> HistoryResult<Vec<EventRecord>> {
        self.inner
            .append_events_fenced(stream_id, expected_next_sequence, self.events(records))
            .await
    }

    async fn read_events(
        &self,
        stream_id: &EventStreamId,
        from_sequence: Option<EventSequence>,
    ) -> HistoryResult<Vec<EventRecord>> {
        self.inner.read_events(stream_id, from_sequence).await
    }

    async fn read_events_after_cursor(
        &self,
        stream_id: &EventStreamId,
        cursor: &StreamCursorV1,
    ) -> HistoryResult<Vec<EventRecord>> {
        self.inner.read_events_after_cursor(stream_id, cursor).await
    }
}

#[async_trait]
impl ObservationStore for DynRuntimeStore {
    async fn append_observation(
        &self,
        record: NewObservationRecord,
    ) -> HistoryResult<ObservationRecord> {
        self.inner.append_observation(record).await
    }

    async fn list_observations(
        &self,
        scope: &ThreadCoordinates,
        kind: Option<&str>,
    ) -> HistoryResult<Vec<ObservationRecord>> {
        self.inner.list_observations(scope, kind).await
    }
}

struct ScenarioProvider {
    inner: LocalOfflineProviderClient,
    pause_next_complete: std::sync::atomic::AtomicBool,
    complete_started: tokio::sync::Notify,
}

impl ScenarioProvider {
    fn new() -> Self {
        Self {
            inner: LocalOfflineProviderClient::new(
                APP_SERVER_LOCAL_PROVIDER,
                APP_SERVER_LOCAL_MODEL,
            ),
            pause_next_complete: std::sync::atomic::AtomicBool::new(false),
            complete_started: tokio::sync::Notify::new(),
        }
    }
}

#[async_trait]
impl ProviderClient for ScenarioProvider {
    fn capabilities(&self) -> Option<ProviderCapabilityRecord> {
        self.inner.capabilities()
    }

    async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        if self
            .pause_next_complete
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.complete_started.notify_waiters();
            std::future::pending::<()>().await;
        }
        self.inner.complete(request).await
    }
}

#[derive(Clone, Debug, Serialize)]
struct QueueLeaseReceipt {
    message_id: String,
    attempt: u32,
    visible_until_tick: u64,
}

#[derive(Clone, Debug, Serialize)]
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
    envelope: IngressEnvelope,
    attempt: u32,
    visible_at_tick: u64,
    completed: bool,
}

#[derive(Default)]
struct ScenarioQueue {
    messages: tokio::sync::Mutex<Vec<ScenarioQueuedMessage>>,
    tick: Arc<std::sync::atomic::AtomicU64>,
    pause_next_complete: std::sync::atomic::AtomicBool,
    complete_started: tokio::sync::Notify,
}

impl ScenarioQueue {
    fn new(tick: Arc<std::sync::atomic::AtomicU64>) -> Self {
        Self {
            messages: tokio::sync::Mutex::new(Vec::new()),
            tick,
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
}

#[async_trait]
impl IngressSink for ScenarioQueue {
    async fn submit(&self, envelope: IngressEnvelope) -> IoResult<IngressAck> {
        let ack = IngressAck::accepted(&envelope);
        let mut messages = self.messages.lock().await;
        if messages
            .iter()
            .any(|message| message.envelope.dedupe_key == envelope.dedupe_key && !message.completed)
        {
            return Ok(IngressAck::rejected(&envelope, "duplicate dedupe key"));
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

#[async_trait]
impl IngressQueueStore for ScenarioQueue {
    async fn lease_ingress(
        &self,
        worker_id: &str,
        max_messages: usize,
        visibility_timeout_secs: u32,
    ) -> IoResult<Vec<LeasedIngressEnvelope>> {
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
            let mut item =
                LeasedIngressEnvelope::new(message.message_id.clone(), message.envelope.clone());
            item.attempt = message.attempt;
            item.lease_owner = Some(worker_id.to_string());
            leased.push(item);
        }
        Ok(leased)
    }

    async fn complete_ingress(&self, message_id: &str) -> IoResult<()> {
        if self
            .pause_next_complete
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.complete_started.notify_waiters();
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

    async fn hold_ingress_until(&self, message_id: &str, visible_at_ms: u64) -> IoResult<()> {
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

    async fn retry_ingress(&self, message_id: &str, _reason: &str) -> IoResult<()> {
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
    inner: Arc<Q>,
    probes: Arc<Mutex<QueueProbeLog>>,
    tick: Arc<std::sync::atomic::AtomicU64>,
}

impl<Q> ProbedIngressQueue<Q> {
    fn new(
        inner: Arc<Q>,
        probes: Arc<Mutex<QueueProbeLog>>,
        tick: Arc<std::sync::atomic::AtomicU64>,
    ) -> Self {
        Self {
            inner,
            probes,
            tick,
        }
    }
}

#[async_trait]
impl<Q: IngressQueueStore + 'static> IngressSink for ProbedIngressQueue<Q> {
    async fn submit(&self, envelope: IngressEnvelope) -> IoResult<IngressAck> {
        self.inner.submit(envelope).await
    }
}

#[async_trait]
impl<Q: IngressQueueStore + 'static> IngressQueueStore for ProbedIngressQueue<Q> {
    async fn lease_ingress(
        &self,
        worker_id: &str,
        max_messages: usize,
        visibility_timeout_secs: u32,
    ) -> IoResult<Vec<LeasedIngressEnvelope>> {
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

    async fn complete_ingress(&self, message_id: &str) -> IoResult<()> {
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

    async fn hold_ingress_until(&self, message_id: &str, visible_at_ms: u64) -> IoResult<()> {
        self.inner
            .hold_ingress_until(message_id, visible_at_ms)
            .await
    }

    async fn retry_ingress(&self, message_id: &str, reason: &str) -> IoResult<()> {
        self.inner.retry_ingress(message_id, reason).await
    }
}

#[derive(Default)]
struct EmptyIngressQueue;

#[async_trait]
impl IngressSink for EmptyIngressQueue {
    async fn submit(&self, envelope: IngressEnvelope) -> IoResult<IngressAck> {
        Ok(IngressAck::accepted(&envelope))
    }
}

#[async_trait]
impl IngressQueueStore for EmptyIngressQueue {
    async fn lease_ingress(
        &self,
        _worker_id: &str,
        _max_messages: usize,
        _visibility_timeout_secs: u32,
    ) -> IoResult<Vec<LeasedIngressEnvelope>> {
        Ok(Vec::new())
    }

    async fn complete_ingress(&self, _message_id: &str) -> IoResult<()> {
        Ok(())
    }

    async fn hold_ingress_until(&self, _message_id: &str, _visible_at_ms: u64) -> IoResult<()> {
        Ok(())
    }

    async fn retry_ingress(&self, _message_id: &str, _reason: &str) -> IoResult<()> {
        Ok(())
    }
}

fn clone_plan(plan: &FaultPlan) -> FaultPlan {
    FaultPlan {
        seed: plan.seed,
        vocabulary_version: plan.vocabulary_version,
        intensity: plan.intensity,
        directives: plan.directives.clone(),
    }
}

fn scenario_route() -> CooldisIoRouteConfig {
    CooldisIoRouteConfig {
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
        egress_retry: CooldisEgressRetryConfig::default(),
        telegram: None,
        metadata: BTreeMap::new(),
    }
}

struct ScenarioHarness {
    root: PathBuf,
    route_db: PathBuf,
    server: CooldisAppServer,
    bridge: CooldisDaemonIoBridge,
    queue: Arc<dyn IngressQueueStore>,
    queue_inner: Arc<ScenarioQueue>,
    probes: Arc<Mutex<QueueProbeLog>>,
    probe_cursor: usize,
    tick: Arc<std::sync::atomic::AtomicU64>,
    runtime_store: Arc<dyn RuntimeStore>,
    raw_store: super::kernel_test::SqliteSessionStore,
    provider: Arc<ScenarioProvider>,
    projector_host: RuntimeHost,
    plan: FaultPlan,
    transcript: TypedTranscript,
    coordinates: Vec<ThreadCoordinates>,
    collected: BTreeMap<String, i64>,
    current_root: usize,
    root_count: usize,
    envelope_index: usize,
    runtime_generation: usize,
    active_runtime_ids: BTreeMap<String, String>,
    process_cut_index: usize,
    shut_down: bool,
}

impl ScenarioHarness {
    async fn build(
        root: PathBuf,
        plan: FaultPlan,
        clean: bool,
        surviving_queue: Option<Arc<ScenarioQueue>>,
    ) -> Self {
        if clean {
            let _ = std::fs::remove_dir_all(&root);
        }
        std::fs::create_dir_all(&root).expect("create scenario fixture root");
        let route_db = root.join("route.sqlite3");
        let probes = Arc::new(Mutex::new(QueueProbeLog::default()));
        let tick = surviving_queue
            .as_ref()
            .map(|queue| Arc::clone(&queue.tick))
            .unwrap_or_else(|| Arc::new(std::sync::atomic::AtomicU64::new(0)));
        let queue_inner =
            surviving_queue.unwrap_or_else(|| Arc::new(ScenarioQueue::new(Arc::clone(&tick))));
        let probed_queue = Arc::new(ProbedIngressQueue::new(
            Arc::clone(&queue_inner),
            Arc::clone(&probes),
            Arc::clone(&tick),
        ));
        let provider_control = Arc::new(ScenarioProvider::new());
        let applied = plan.apply(
            Arc::new(super::kernel_test::InMemorySessionStore::new()),
            probed_queue,
            Arc::clone(&provider_control),
        );
        let queue: Arc<dyn IngressQueueStore> = Arc::new(applied.queue);
        let provider: Arc<dyn ProviderClient> = Arc::new(applied.provider);
        let runtime_config = CanonicalProviderRuntimeConfig::new(
            ProviderApi::Other(APP_SERVER_LOCAL_PROVIDER.to_string()),
            APP_SERVER_LOCAL_PROVIDER,
            APP_SERVER_LOCAL_MODEL,
        );
        let runtime_factory: Arc<dyn super::kernel_test::AgentRuntimeFactory> = Arc::new(
            CanonicalProviderRuntimeFactory::new(runtime_config, provider),
        );

        let socket = root.join("app-server.sock");
        let listen = AppServerListenAddr::parse(&format!("unix://{}", socket.display()))
            .expect("scenario app-server listen address");
        let mut config = CooldisAppServerConfig::local(listen, "/workspace");
        config.runtime_home = root.join("runtime");
        config.state_home = root.join("state");
        config.user_state_home = root.join("user-state");
        config.agent_registry_root = root.join("agent-registry");
        config.blob_registry_root = root.join("blob-registry");
        config.skill_registry_root = root.join("skill-registry");
        config.capsule_bindings.registry_root = None;
        config.tenant_id = format!("scenario-{:016x}", plan.seed);
        config.user_id = "scenario-user".to_string();

        let decorated_slot = Arc::new(Mutex::new(None::<Arc<dyn RuntimeStore>>));
        let decorated_capture = Arc::clone(&decorated_slot);
        let store_plan = clone_plan(&plan);
        let server = super::scenario_app_server(config, Arc::clone(&runtime_factory), move |raw| {
            let canonical_timestamp_ms = store_plan.seed as i64;
            let applied = store_plan.apply(
                Arc::new(DynRuntimeStore {
                    inner: raw,
                    canonical_timestamp_ms,
                }),
                Arc::new(EmptyIngressQueue),
                Arc::new(LocalOfflineProviderClient::new(
                    APP_SERVER_LOCAL_PROVIDER,
                    APP_SERVER_LOCAL_MODEL,
                )),
            );
            let decorated: Arc<dyn RuntimeStore> = Arc::new(applied.store);
            *decorated_capture.lock().unwrap() = Some(Arc::clone(&decorated));
            decorated
        })
        .await
        .expect("build decorated scenario app server");
        let runtime_store = decorated_slot
            .lock()
            .unwrap()
            .clone()
            .expect("session-store decorator should capture the installed store");
        let projector_host = RuntimeHost::with_session_store(
            Arc::clone(&runtime_factory),
            Arc::clone(&runtime_store),
        );
        let raw_store = super::kernel_test::SqliteSessionStore::open(server.session_store_path())
            .await
            .expect("open scenario store for durable probes");
        let bridge = CooldisDaemonIoBridge::from_app_server(&server);
        let route = scenario_route();
        bridge
            .register_egress_route_config("scenario", "scenario", &route)
            .await
            .expect("register scenario route config");
        bridge
            .register_egress_state_sqlite_dsn("scenario", "scenario", sqlite_dsn(&route_db))
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
            runtime_store,
            raw_store,
            provider: provider_control,
            projector_host,
            plan,
            transcript: TypedTranscript::new(),
            coordinates: Vec::new(),
            collected: BTreeMap::new(),
            current_root: 0,
            root_count: 0,
            envelope_index: 0,
            runtime_generation: 0,
            active_runtime_ids: BTreeMap::new(),
            process_cut_index: 0,
            shut_down: false,
        }
    }

    fn deterministic_thread_id(&self, index: usize) -> ThreadId {
        let value = (u128::from(self.plan.seed) << 64) | 0x5343_454e_0000_0000u128 | index as u128;
        ThreadId::parse_str(&uuid::Uuid::from_u128(value).to_string()).unwrap()
    }

    fn source() -> IoSource {
        IoSource::new("scenario", "scenario")
    }

    fn conversation(index: usize) -> IoConversation {
        IoConversation::new(
            format!("scenario:conversation:{index}"),
            ConversationKind::Direct,
        )
    }

    fn session_id(index: usize) -> String {
        format!(
            "io:{}:{}",
            Self::source().stable_scope(),
            Self::conversation(index).stable_key()
        )
    }

    fn root_coordinates(&self, index: usize) -> ThreadCoordinates {
        ThreadCoordinates {
            tenant_id: self.server.tenant_id().to_string(),
            user_id: self.server.user_id().to_string(),
            session_id: Self::session_id(index),
            thread_id: self.deterministic_thread_id(index + 1),
        }
    }

    fn reserve_root(&mut self, index: usize) -> ThreadCoordinates {
        let coordinates = self.root_coordinates(index);
        let address = ThreadAddress::new(
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
            &json!({
                "kind": "thread.reservation",
                "thread_id": observed,
                "reservation_kind": "initial_route",
            }),
        );
        coordinates
    }

    fn replace_root_reservation_after_cut(&mut self, index: usize) -> Option<String> {
        let coordinates = self.root_coordinates(index);
        let address = ThreadAddress::new(
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
            &json!({
                "kind": "thread.reservation",
                "thread_id": coordinates.thread_id,
                "reservation_kind": "initial_route",
            }),
        );
        previous
    }

    fn rebind_root_to_child(&self, index: usize, coordinates: &ThreadCoordinates) {
        let address = ThreadAddress::new(
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

    fn envelope(&mut self, root_index: usize, policy: &str, text: &str) -> IngressEnvelope {
        self.envelope_index += 1;
        let source = Self::source();
        let mut envelope = IngressEnvelope::new(
            source.clone(),
            Self::conversation(root_index),
            IngressContent::text(text),
            self.tick.load(std::sync::atomic::Ordering::SeqCst) * 1_000,
        )
        .with_dedupe_key(IoDedupeKey::for_source(
            &source,
            format!("{}:{}", self.plan.seed, self.envelope_index),
        ))
        .with_metadata("cooldis_route_id", "scenario")
        .with_metadata("cooldis_route_policy", policy);
        envelope.id = format!(
            "scenario-ingress-{}-{}",
            self.plan.seed, self.envelope_index
        );
        envelope
    }

    fn bound_coordinates(&self, root_index: usize) -> Option<ThreadCoordinates> {
        let address = ThreadAddress::new(
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
                    Ok(ThreadCoordinates {
                        tenant_id: row.get(0)?,
                        user_id: row.get(1)?,
                        session_id: row.get(2)?,
                        thread_id: ThreadId::parse_str(&thread_id).map_err(|error| {
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

    fn bound_root_index(&self, coordinates: &ThreadCoordinates) -> Option<usize> {
        (0..self.root_count).find(|index| {
            self.bound_coordinates(*index)
                .is_some_and(|bound| bound.thread_id == coordinates.thread_id)
        })
    }

    async fn wait_for_idle(&self, coordinates: &ThreadCoordinates) {
        for _ in 0..128 {
            match self.server.supervisor().get_thread_at(coordinates).await {
                Ok(handle)
                    if matches!(handle.status(), ThreadStatus::Idle | ThreadStatus::Stopped)
                        && handle.queued_command_count() == 0 =>
                {
                    return;
                }
                _ => tokio::task::yield_now().await,
            }
        }
    }

    async fn append_placement(
        &mut self,
        coordinates: &ThreadCoordinates,
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
            let mut payload = json!({
                "runtime_id": runtime_id,
                "runtime_state": state,
                "reservation_progress": reservation_key,
            });
            if let Some(resident_state) = resident_state {
                let object = payload.as_object_mut().expect("placement payload object");
                object.insert("resident_state".to_string(), json!(resident_state));
                object.insert("reservation_key".to_string(), json!(reservation_key));
            }
            NewEventRecord::witnessed(coordinates.clone(), EventKind::PlacementDecision, payload)
        };
        let stream_id = EventStreamId::for_thread(coordinates);
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
                            &json!({
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
                &json!({
                    "tick": self.tick.load(std::sync::atomic::Ordering::SeqCst),
                }),
            );
        }
    }

    async fn drain_queue(&mut self) -> usize {
        let worker = CooldisDaemonQueueWorker::new(
            Arc::clone(&self.queue),
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
                        &json!({"operation": "drain_queue", "error": error.to_string()}),
                    );
                    self.tick.fetch_add(30, std::sync::atomic::Ordering::SeqCst);
                }
            }
        }
        self.flush_queue_probes();
        let remaining = self.queue_inner.pending_count().await;
        self.transcript
            .push_receipt("queue.drain.completed", &json!({"remaining": remaining}));
        remaining
    }

    async fn collect_events(&mut self) {
        for coordinates in &self.coordinates {
            for stream_id in [
                super::kernel_test::control_stream_id(coordinates),
                EventStreamId::for_thread(coordinates),
            ] {
                self.transcript.preserve_id(stream_id.as_str());
                let from = self
                    .collected
                    .get(stream_id.as_str())
                    .map(|sequence| EventSequence::new(sequence + 1));
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
    ) -> tokio::task::JoinHandle<IoResult<usize>> {
        let worker = CooldisDaemonQueueWorker::new(
            Arc::clone(&self.queue),
            self.bridge.clone(),
            worker_id,
            30,
        );
        tokio::spawn(async move { worker.drain_once().await })
    }

    async fn remains_parked<T>(&self, task: &tokio::task::JoinHandle<T>) -> bool {
        for _ in 0..128 {
            if task.is_finished() {
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        !task.is_finished()
    }

    async fn fresh_active_root(&mut self, label: &str) -> ThreadCoordinates {
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
            self.queue
                .submit(envelope)
                .await
                .expect("submit crash-cut setup envelope");
            self.drain_queue().await;
            self.wait_for_idle(&coordinates).await;
            if self
                .server
                .supervisor()
                .get_thread_at(&coordinates)
                .await
                .is_ok_and(|handle| handle.status() == ThreadStatus::Idle)
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

    async fn queue_cut_envelope(&mut self, policy: &str, label: &str) -> ThreadCoordinates {
        let root_index = self.root_count;
        self.root_count += 1;
        self.current_root = root_index;
        let coordinates = self.reserve_root(root_index);
        let envelope = self.envelope(root_index, policy, label);
        self.queue
            .submit(envelope)
            .await
            .expect("submit crash-cut envelope");
        coordinates
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
                        &json!({"operation": "start_thread", "error": error.to_string()}),
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
                            &json!({"operation": "submit_turn", "error": error.to_string()}),
                        );
                    } else {
                        self.wait_for_idle(&coordinates).await;
                    }
                }
            }
            ScenarioOp::Steer => {
                if let Some(coordinates) = self.bound_coordinates(self.current_root) {
                    self.envelope_index += 1;
                    let _ = self
                        .server
                        .supervisor()
                        .submit_to_with_mode(
                            &coordinates,
                            format!("scenario-steer-{}-{}", self.plan.seed, self.envelope_index),
                            "steer",
                            TurnSubmissionMode::Steer,
                        )
                        .await;
                    self.wait_for_idle(&coordinates).await;
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
                    let control_stream = super::kernel_test::control_stream_id(&parent);
                    let claim = self
                        .runtime_store
                        .append_events(
                            &control_stream,
                            vec![NewEventRecord::witnessed(
                                parent.clone(),
                                EventKind::IoIngressClaimed,
                                json!({
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
                        && let Ok(child) =
                            super::scenario_fork_with_id(&self.server, &parent, child_thread_id)
                                .await
                    {
                        let _ = self
                            .runtime_store
                            .append_events(
                                &control_stream,
                                vec![
                                    NewEventRecord::witnessed(
                                        parent.clone(),
                                        EventKind::ThreadSpawned,
                                        json!({
                                            "child_thread_id": child.thread_id,
                                            "fork": {"claim_event_id": claim.id},
                                        }),
                                    ),
                                    NewEventRecord::witnessed(
                                        parent,
                                        EventKind::IoIngressSettled,
                                        json!({
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
                    &json!({"kind": "shutdown_all.completed"}),
                );
            }
        }
    }
}

struct ScenarioStoreState {
    root: PathBuf,
    plan: FaultPlan,
    transcript: TypedTranscript,
    coordinates: Vec<ThreadCoordinates>,
    collected: BTreeMap<String, i64>,
    current_root: usize,
    root_count: usize,
    envelope_index: usize,
    runtime_generation: usize,
    active_runtime_ids: BTreeMap<String, String>,
    process_cut_index: usize,
    shut_down: bool,
    tick: u64,
    queue_inner: Arc<ScenarioQueue>,
}

#[async_trait]
impl CrashCutHost for ScenarioHarness {
    type StoreState = ScenarioStoreState;

    async fn run_to_cut(&mut self, seam: CrashCutSeam) {
        self.transcript
            .push_receipt("scenario.crash_cut", &json!({"seam": format!("{seam:?}")}));
        match seam {
            CrashCutSeam::PauseAfterIngressClaim => {
                let coordinates = self
                    .queue_cut_envelope("queue_per_conversation", "claim-submit-cut")
                    .await;
                let (pause, paused) = super::scenario_pause_after_ingress_claim(&self.bridge);
                loop {
                    pause.store(true, std::sync::atomic::Ordering::SeqCst);
                    let paused = Arc::clone(&paused);
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
            CrashCutSeam::PersistedInputRuntimeNotify => {
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
                    for _ in 0..512 {
                        if !self
                            .provider
                            .pause_next_complete
                            .load(std::sync::atomic::Ordering::SeqCst)
                        {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                    if !self
                        .provider
                        .pause_next_complete
                        .load(std::sync::atomic::Ordering::SeqCst)
                    {
                        break;
                    }
                    self.wait_for_idle(&coordinates).await;
                }
                assert!(
                    !self
                        .provider
                        .pause_next_complete
                        .load(std::sync::atomic::Ordering::SeqCst),
                    "real runtime did not reach provider after persisting input; submit errors: {submit_errors:?}"
                );
            }
            CrashCutSeam::QueueCompleteBarrier => {
                let coordinates = self
                    .queue_cut_envelope("observe_only", "queue-complete-cut")
                    .await;
                loop {
                    self.queue_inner
                        .pause_next_complete
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    let drain = self.spawn_cut_worker("scenario-complete-cut");
                    for _ in 0..512 {
                        if !self
                            .queue_inner
                            .pause_next_complete
                            .load(std::sync::atomic::Ordering::SeqCst)
                        {
                            drain.abort();
                            let _ = drain.await;
                            self.runtime_generation += 1;
                            self.append_placement(&coordinates, "active", None).await;
                            self.coordinates.push(coordinates.clone());
                            break;
                        }
                        if drain.is_finished() {
                            let _ = drain.await;
                            self.tick.fetch_add(30, std::sync::atomic::Ordering::SeqCst);
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                    if self
                        .coordinates
                        .iter()
                        .any(|known| known.thread_id == coordinates.thread_id)
                    {
                        break;
                    }
                }
            }
            CrashCutSeam::IngressBindingBarrier => {
                let root_index = self.root_count;
                self.root_count += 1;
                self.current_root = root_index;
                let coordinates = self.root_coordinates(root_index);
                let envelope = self.envelope(root_index, "observe_only", "ingress-binding-cut");
                self.queue
                    .submit(envelope)
                    .await
                    .expect("submit ingress-binding crash-cut envelope");
                let hook = super::scenario_ingress_binding_barrier(&self.bridge);
                let mut reached = false;
                for _ in 0..32 {
                    let barrier = Arc::new(tokio::sync::Barrier::new(2));
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
            CrashCutSeam::ThreadLoadRootBarrier => {
                let coordinates = self.fresh_active_root("thread-load-cut-setup").await;
                self.server
                    .supervisor()
                    .shutdown_thread_at(&coordinates)
                    .await
                    .expect("remove resident runtime before real cold load");
                let envelope = self.envelope(self.current_root, "observe_only", "thread-load-cut");
                self.queue
                    .submit(envelope)
                    .await
                    .expect("submit cold-load crash-cut envelope");
                let hook = super::scenario_thread_load_root_barrier(&self.bridge);
                loop {
                    let barrier = Arc::new(tokio::sync::Barrier::new(2));
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
            CrashCutSeam::SpawnSnapshotBarrier => {
                let coordinates = self.fresh_active_root("spawn-snapshot-cut-setup").await;
                loop {
                    let barrier = Arc::new(tokio::sync::Barrier::new(2));
                    let projection = tokio::spawn(super::scenario_project_spawn_snapshot(
                        self.projector_host.clone(),
                        coordinates.clone(),
                        Arc::clone(&barrier),
                    ));
                    if self.remains_parked(&projection).await {
                        projection.abort();
                        let _ = projection.await;
                        break;
                    }
                    let _ = projection.await;
                }
            }
        }
        for coordinates in self.coordinates.clone() {
            self.append_placement(&coordinates, "terminal", Some("failed"))
                .await;
        }
        self.collect_events().await;
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
                &json!({"reservation_key": format!("thread:{}", coordinates.thread_id)}),
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
    pub intensity: Intensity,
}

/// One seeded scenario, reproducible from `(seed, harness version)` alone.
#[derive(Debug)]
pub struct Scenario {
    pub seed: u64,
    pub ops: Vec<ScenarioOp>,
    pub plan: FaultPlan,
}

impl Scenario {
    /// Derive the operation sequence and fault plan from the same seed,
    /// through independent `SplitMix64` split lanes.
    pub fn derive(seed: u64, bounds: ScenarioBounds) -> Self {
        let plan = FaultPlan::derive(seed, bounds.intensity);
        if bounds.max_ops == 0 {
            return Self {
                seed,
                ops: Vec::new(),
                plan,
            };
        }

        let version_salt = u64::from(FAULT_VOCABULARY_VERSION).wrapping_mul(0xD6E8_FEB8_6659_FD93);
        let lane = |label: &str| {
            let mut root = SplitMix64::new(seed ^ version_salt);
            root.split(label)
        };
        let mut count_lane = lane("scenario-op-count-v1");
        let mut op_lane = lane("scenario-ops-v1");
        let target_len = 1 + count_lane.next_below(bounds.max_ops as u64) as usize;
        let mut remaining_cuts = plan
            .directives
            .iter()
            .filter(|directive| directive.component == FaultComponent::Process)
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
    pub store: &'a (dyn RuntimeStore + Send + Sync),
    /// The ingress queue under test, when the scenario exercises ingress;
    /// bounded-queue invariants pass when it is absent.
    pub queue: Option<&'a (dyn IngressQueueStore + Send + Sync)>,
    /// Normalized durable events and non-mutating witness receipts. Queue
    /// witnesses are `queue.lease`, `queue.redelivery`, `queue.complete`,
    /// `queue.clock`, and `queue.drain.completed`.
    pub transcript: &'a NormalizedTranscript,
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
#[async_trait]
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
    pub transcript: NormalizedTranscript,
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
) -> Result<NormalizedTranscript, ScenarioFailure> {
    let run_root = ScenarioRunRoot::new(scenario.seed);
    let root = run_root.path.clone();
    let mut harness =
        ScenarioHarness::build(root.clone(), clone_plan(&scenario.plan), true, None).await;
    let mut normative_invariants = invariant_set_v1();
    normative_invariants.push(Box::new(Inv6ClaimsSettle));
    normative_invariants.extend(fork_invariants_v1());

    for (step, op) in scenario.ops.iter().copied().enumerate() {
        if op == ScenarioOp::Restart {
            let process = harness
                .plan
                .directives
                .iter()
                .filter(|directive| directive.component == FaultComponent::Process)
                .nth(harness.process_cut_index)
                .cloned();
            if let Some(process) = process {
                harness.process_cut_index += 1;
                harness = super::fault_plan::run_crash_cut(process.operation, harness).await;
            } else {
                harness.transcript.push_receipt(
                    "scenario.operation.error",
                    &json!({"operation": "restart", "error": "no remaining process cut"}),
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
    mut plan: FaultPlan,
    first: &ScenarioFailure,
    invariants: &[Box<dyn ScenarioInvariant>],
) -> ScenarioFailure {
    let target = first
        .violations
        .iter()
        .map(|violation| (violation.invariant, violation.detail.clone()))
        .collect::<BTreeSet<_>>();

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
            plan: FaultPlan {
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
    failure.transcript.items.push(NormalizedTranscriptItem {
        kind: "receipt".to_string(),
        label: "scenario.minimized_reproduction".to_string(),
        value: json!({
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
    target: &BTreeSet<(&'static str, String)>,
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

fn corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/scenarios/corpus.json")
}

fn corpus_intensity(entry: &CorpusEntry, index: usize) -> Result<Intensity, String> {
    match entry.intensity.as_str() {
        "sparse" => Ok(Intensity::Sparse),
        "moderate" => Ok(Intensity::Moderate),
        "hostile" => Ok(Intensity::Hostile),
        intensity => Err(format!(
            "corpus entry {index} (seed {}) has unknown intensity {intensity:?}",
            entry.seed
        )),
    }
}

fn load_corpus(path: &Path) -> Result<Vec<(CorpusEntry, Intensity)>, String> {
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
            if entry.vocabulary_version != FAULT_VOCABULARY_VERSION {
                return Err(format!(
                    "corpus entry {index} (seed {}) has vocabulary_version {}, expected {}",
                    entry.seed, entry.vocabulary_version, FAULT_VOCABULARY_VERSION
                ));
            }
            let intensity = corpus_intensity(&entry, index)?;
            Ok((entry, intensity))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::fault_plan::{CUTS_V1, FaultDirective, FaultTiming, PlannedAction};
    use super::*;

    #[derive(Debug, Serialize)]
    struct SweepFailure {
        seed: u64,
        kind: &'static str,
        detail: String,
    }

    #[derive(Serialize)]
    struct SweepReceipt {
        base_seed: u64,
        count: usize,
        max_ops: usize,
        per_intensity_tallies: BTreeMap<&'static str, usize>,
        failures: Vec<SweepFailure>,
        corpus_size: usize,
        commit_sha: String,
        status: &'static str,
    }

    fn parse_sweep_env(name: &str, default: Option<&str>) -> u64 {
        let value = std::env::var(name)
            .ok()
            .or_else(|| default.map(str::to_owned))
            .unwrap_or_else(|| panic!("{name} is required for scenario_nightly_sweep"));
        value
            .parse::<u64>()
            .unwrap_or_else(|error| panic!("{name} must be a u64, got {value:?}: {error}"))
    }

    fn first_transcript_mismatch(
        first: &NormalizedTranscript,
        second: &NormalizedTranscript,
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
        outcome: &Result<NormalizedTranscript, ScenarioFailure>,
    ) -> &NormalizedTranscript {
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
        scenario: &Scenario,
    ) -> Result<Result<NormalizedTranscript, ScenarioFailure>, String> {
        std::panic::AssertUnwindSafe(run_scenario_once(scenario, &[]))
            .catch_unwind()
            .await
            .map_err(panic_detail)
    }

    fn write_sweep_receipt(receipt: &SweepReceipt) {
        let json = serde_json::to_string_pretty(receipt).expect("serialize nightly sweep receipt");
        if let Ok(path) = std::env::var("COOLDIS_SCENARIO_SWEEP_RECEIPT_PATH") {
            std::fs::write(&path, format!("{json}\n"))
                .unwrap_or_else(|error| panic!("write nightly sweep receipt to {path}: {error}"));
        } else {
            eprintln!("scenario nightly receipt:\n{json}");
        }
    }

    fn no_fault_scenario(seed: u64, ops: Vec<ScenarioOp>) -> Scenario {
        Scenario {
            seed,
            ops,
            plan: FaultPlan {
                seed,
                vocabulary_version: FAULT_VOCABULARY_VERSION,
                intensity: Intensity::Sparse,
                directives: Vec::new(),
            },
        }
    }

    #[test]
    fn derivation_is_repeatable_bounded_and_well_formed() {
        for intensity in [Intensity::Sparse, Intensity::Moderate, Intensity::Hostile] {
            for seed in 0..256 {
                let first = Scenario::derive(
                    seed,
                    ScenarioBounds {
                        max_ops: 12,
                        intensity,
                    },
                );
                let second = Scenario::derive(
                    seed,
                    ScenarioBounds {
                        max_ops: 12,
                        intensity,
                    },
                );
                assert_eq!(first.ops, second.ops);
                assert_eq!(first.plan, second.plan);
                assert!(!first.ops.is_empty());
                assert!(first.ops.len() <= 12);
                assert_eq!(first.ops[0], ScenarioOp::StartThread);
                if let Some(shutdown) = first
                    .ops
                    .iter()
                    .position(|op| *op == ScenarioOp::ShutdownAll)
                {
                    assert_eq!(shutdown + 1, first.ops.len());
                }
                assert!(
                    first
                        .ops
                        .iter()
                        .filter(|op| **op == ScenarioOp::Restart)
                        .count()
                        <= first
                            .plan
                            .directives
                            .iter()
                            .filter(|directive| { directive.component == FaultComponent::Process })
                            .count()
                );
            }
        }
    }

    #[test]
    fn operation_lane_is_independent_of_fault_intensity() {
        let seed = 0x4050_0003;
        let ops = [Intensity::Sparse, Intensity::Moderate, Intensity::Hostile].map(|intensity| {
            Scenario::derive(
                seed,
                ScenarioBounds {
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
        let scenario = Scenario::derive(
            7,
            ScenarioBounds {
                max_ops: 0,
                intensity: Intensity::Sparse,
            },
        );
        assert!(scenario.ops.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn seeded_runner_engages_inv2_inv3_inv5_inv6_inv7_and_inv8_witnesses() {
        if !super::super::scenario_unit_harness() {
            return;
        }
        let scenario = no_fault_scenario(
            0x4030_0001,
            vec![
                ScenarioOp::StartThread,
                ScenarioOp::SubmitTurn,
                ScenarioOp::Fork,
                ScenarioOp::DrainQueue,
                ScenarioOp::ShutdownAll,
            ],
        );
        let transcript = run_scenario_once(&scenario, &[])
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

        let recovery = Scenario {
            seed: 0x4030_0005,
            ops: vec![ScenarioOp::StartThread, ScenarioOp::Restart],
            plan: FaultPlan {
                seed: 0x4030_0005,
                vocabulary_version: FAULT_VOCABULARY_VERSION,
                intensity: Intensity::Sparse,
                directives: vec![FaultDirective {
                    component: FaultComponent::Process,
                    operation: "ingress-binding",
                    nth: 1,
                    timing: FaultTiming::Before,
                    action: PlannedAction::Fail,
                }],
            },
        };
        let recovery = run_scenario_once(&recovery, &[])
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
        if !super::super::scenario_unit_harness() {
            return;
        }
        let seed = 4_286_450_925_398_396_449;
        let scenario = Scenario {
            seed,
            ops: vec![ScenarioOp::StartThread, ScenarioOp::StartThread],
            plan: FaultPlan {
                seed,
                vocabulary_version: FAULT_VOCABULARY_VERSION,
                intensity: Intensity::Sparse,
                directives: vec![FaultDirective {
                    component: FaultComponent::Queue,
                    operation: "lease_ingress",
                    nth: 2,
                    timing: FaultTiming::Before,
                    action: PlannedAction::Fail,
                }],
            },
        };

        run_scenario_once(&scenario, &[])
            .await
            .expect("a completed first lease must not require redelivery");
    }

    #[tokio::test]
    async fn scenario_queue_accepts_message_id_completion_after_visibility_expiry() {
        let tick = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let queue = ScenarioQueue::new(Arc::clone(&tick));
        let source = ScenarioHarness::source();
        let envelope = IngressEnvelope::new(
            source.clone(),
            ScenarioHarness::conversation(0),
            IngressContent::text("accepted stale completion"),
            0,
        )
        .with_dedupe_key(IoDedupeKey::for_source(&source, "stale-completion"));
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
        if !super::super::scenario_unit_harness() {
            return;
        }
        for (intensity, seeds) in [
            (Intensity::Sparse, [0x4031_0001, 0x4031_0003, 0x4031_0005]),
            (Intensity::Moderate, [0x4032_0001, 0x4032_0003, 0x4032_0005]),
            (Intensity::Hostile, [0x4033_0001, 0x4033_0003, 0x4033_0005]),
        ] {
            for seed in seeds {
                let scenario = Scenario::derive(
                    seed,
                    ScenarioBounds {
                        max_ops: 4,
                        intensity,
                    },
                );
                if let Err(failure) = run_scenario_once(&scenario, &[]).await {
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
        if !super::super::scenario_unit_harness() {
            return;
        }
        let started = std::time::Instant::now();
        let corpus = load_corpus(&corpus_path()).unwrap_or_else(|error| panic!("{error}"));
        let corpus_size = corpus.len();
        for (entry, intensity) in corpus {
            eprintln!(
                "scenario corpus: running seed {} intensity={} max_ops={} pin={}",
                entry.seed, entry.intensity, entry.max_ops, entry.pins
            );
            let scenario = Scenario::derive(
                entry.seed,
                ScenarioBounds {
                    max_ops: entry.max_ops,
                    intensity,
                },
            );
            if let Err(failure) = run_scenario(scenario, &[]).await {
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
        let error = load_corpus(Path::new("definitely-missing-scenario-corpus.json")).unwrap_err();
        assert!(error.contains("definitely-missing-scenario-corpus.json"));
        assert!(error.contains("could not be read"));
    }

    #[test]
    fn corpus_loader_fails_closed_on_vocabulary_version_mismatch() {
        let path = std::env::temp_dir().join(format!(
            "cooldis-scenario-corpus-version-mismatch-{}.json",
            std::process::id()
        ));
        std::fs::write(
            &path,
            r#"[{"seed":404,"vocabulary_version":999,"max_ops":4,"intensity":"sparse","pins":"test"}]"#,
        )
        .expect("write temporary corpus");
        let error = load_corpus(&path).unwrap_err();
        let _ = std::fs::remove_file(path);
        assert!(error.contains("entry 0 (seed 404)"));
        assert!(error.contains("vocabulary_version 999"));
    }

    /// Runs the rotating nightly lane. `COOLDIS_SCENARIO_SWEEP_BASE_SEED` is
    /// required and must be a u64. `COOLDIS_SCENARIO_SWEEP_COUNT` defaults to
    /// 24 and `COOLDIS_SCENARIO_SWEEP_MAX_OPS` defaults to 8. The optional
    /// receipt path and commit SHA variables are workflow witnesses. The test
    /// is excluded from normal suites only by `#[ignore]`. Missing or invalid
    /// required env and an invalid fixed corpus fail closed.
    #[tokio::test(start_paused = true)]
    #[ignore = "rotating nightly scenario sweep"]
    async fn scenario_nightly_sweep() {
        if !super::super::scenario_unit_harness() {
            return;
        }
        let base_seed = parse_sweep_env("COOLDIS_SCENARIO_SWEEP_BASE_SEED", None);
        let count = parse_sweep_env("COOLDIS_SCENARIO_SWEEP_COUNT", Some("24")) as usize;
        let max_ops = parse_sweep_env("COOLDIS_SCENARIO_SWEEP_MAX_OPS", Some("8")) as usize;
        let corpus_size = load_corpus(&corpus_path())
            .unwrap_or_else(|error| panic!("{error}"))
            .len();
        let commit_sha = std::env::var("COOLDIS_SCENARIO_SWEEP_COMMIT_SHA")
            .unwrap_or_else(|_| "local".to_string());
        let mut root = SplitMix64::new(base_seed);
        let mut lane = root.split("scenario-nightly-v1");
        let mut tallies = BTreeMap::from([("sparse", 0), ("moderate", 0), ("hostile", 0)]);
        let mut failures = Vec::new();

        for index in 0..count {
            let seed = lane.next_u64();
            let (intensity_name, intensity) = match index % 3 {
                0 => ("sparse", Intensity::Sparse),
                1 => ("moderate", Intensity::Moderate),
                _ => ("hostile", Intensity::Hostile),
            };
            *tallies.get_mut(intensity_name).unwrap() += 1;
            let derive = || Scenario::derive(seed, ScenarioBounds { max_ops, intensity });
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
                match std::panic::AssertUnwindSafe(run_scenario(derive(), &[]))
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
        if !super::super::scenario_unit_harness() {
            return;
        }
        let scenario = || {
            let seed = 0x4030_0002;
            Scenario {
                seed,
                ops: vec![
                    ScenarioOp::StartThread,
                    ScenarioOp::Restart,
                    ScenarioOp::SubmitTurn,
                    ScenarioOp::Fork,
                    ScenarioOp::DrainQueue,
                    ScenarioOp::ShutdownAll,
                ],
                plan: FaultPlan {
                    seed,
                    vocabulary_version: FAULT_VOCABULARY_VERSION,
                    intensity: Intensity::Sparse,
                    directives: vec![FaultDirective {
                        component: FaultComponent::Process,
                        operation: "ingress-binding",
                        nth: 1,
                        timing: FaultTiming::Before,
                        action: PlannedAction::Fail,
                    }],
                },
            }
        };
        let first = run_scenario_once(&scenario(), &[]).await.unwrap();
        let second = run_scenario_once(&scenario(), &[]).await.unwrap();
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
    async fn fresh_sweep_output_hash_seeds_are_same_seed_deterministic() {
        if !super::super::scenario_unit_harness() {
            return;
        }
        for seed in [12756048029454721330, 4861954629787943465] {
            let derive = || {
                Scenario::derive(
                    seed,
                    ScenarioBounds {
                        max_ops: 8,
                        intensity: Intensity::Hostile,
                    },
                )
            };
            let first = run_scenario_once(&derive(), &[]).await.unwrap();
            let second = run_scenario_once(&derive(), &[]).await.unwrap();
            assert_eq!(
                first, second,
                "fresh sweep seed {seed} drifted between same-seed runs"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn restart_rebuilds_the_real_daemon_over_the_surviving_store() {
        if !super::super::scenario_unit_harness() {
            return;
        }
        let seed = 0x4030_0003;
        let scenario = Scenario {
            seed,
            ops: vec![
                ScenarioOp::StartThread,
                ScenarioOp::Restart,
                ScenarioOp::SubmitTurn,
                ScenarioOp::ShutdownAll,
            ],
            plan: FaultPlan {
                seed,
                vocabulary_version: FAULT_VOCABULARY_VERSION,
                intensity: Intensity::Sparse,
                directives: vec![FaultDirective {
                    component: FaultComponent::Process,
                    operation: "ingress-binding",
                    nth: 1,
                    timing: FaultTiming::Before,
                    action: PlannedAction::Fail,
                }],
            },
        };
        let transcript = run_scenario_once(&scenario, &[])
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
        if !super::super::scenario_unit_harness() {
            return;
        }
        for (index, operation) in CUTS_V1.iter().copied().enumerate() {
            let seed = 0x4030_1000 + index as u64;
            let scenario = Scenario {
                seed,
                ops: vec![ScenarioOp::StartThread, ScenarioOp::Restart],
                plan: FaultPlan {
                    seed,
                    vocabulary_version: FAULT_VOCABULARY_VERSION,
                    intensity: Intensity::Sparse,
                    directives: vec![FaultDirective {
                        component: FaultComponent::Process,
                        operation,
                        nth: 1,
                        timing: FaultTiming::Before,
                        action: PlannedAction::Fail,
                    }],
                },
            };
            let transcript = run_scenario_once(&scenario, &[])
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

    #[async_trait]
    impl ScenarioInvariant for BrokenInvariant {
        fn name(&self) -> &'static str {
            "test-broken-invariant"
        }

        async fn check(&self, world: &ScenarioWorld<'_>) -> Vec<InvariantViolation> {
            (world.step >= 2)
                .then(|| InvariantViolation {
                    invariant: self.name(),
                    detail: "deliberately broken after the third operation".to_string(),
                })
                .into_iter()
                .collect()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn broken_invariant_returns_a_minimized_readable_reproduction() {
        if !super::super::scenario_unit_harness() {
            return;
        }
        let scenario = no_fault_scenario(
            0x4030_0004,
            vec![
                ScenarioOp::StartThread,
                ScenarioOp::SubmitTurn,
                ScenarioOp::DrainQueue,
                ScenarioOp::Cancel,
                ScenarioOp::ShutdownAll,
            ],
        );
        let failure = run_scenario(scenario, &[Box::new(BrokenInvariant)])
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

    #[async_trait]
    impl ScenarioInvariant for ReservationCountInvariant {
        fn name(&self) -> &'static str {
            "test-reservation-count-invariant"
        }

        async fn check(&self, world: &ScenarioWorld<'_>) -> Vec<InvariantViolation> {
            (world.step >= 1)
                .then(|| {
                    let count = world
                        .transcript
                        .items
                        .iter()
                        .filter(|item| item.label == "thread.reservation")
                        .count();
                    InvariantViolation {
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
        if !super::super::scenario_unit_harness() {
            return;
        }
        let scenario = no_fault_scenario(
            0x4050_0001,
            vec![
                ScenarioOp::StartThread,
                ScenarioOp::SubmitTurn,
                ScenarioOp::DrainQueue,
                ScenarioOp::Cancel,
            ],
        );
        let failure = run_scenario(scenario, &[Box::new(ReservationCountInvariant)])
            .await
            .unwrap_err();
        assert_eq!(failure.violations[0].detail, "reservation-count=1");
    }

    struct OverlapInvariant {
        rendezvous: Arc<tokio::sync::Barrier>,
    }

    #[async_trait]
    impl ScenarioInvariant for OverlapInvariant {
        fn name(&self) -> &'static str {
            "test-overlapping-scenario-runs"
        }

        async fn check(&self, world: &ScenarioWorld<'_>) -> Vec<InvariantViolation> {
            if world.step == 0 {
                self.rendezvous.wait().await;
            }
            Vec::new()
        }
    }

    #[tokio::test]
    async fn same_seed_runs_do_not_require_process_global_serialization() {
        if !super::super::scenario_unit_harness() {
            return;
        }
        let rendezvous = Arc::new(tokio::sync::Barrier::new(2));
        let first_invariant: Vec<Box<dyn ScenarioInvariant>> = vec![Box::new(OverlapInvariant {
            rendezvous: Arc::clone(&rendezvous),
        })];
        let second_invariant: Vec<Box<dyn ScenarioInvariant>> =
            vec![Box::new(OverlapInvariant { rendezvous })];
        let first = no_fault_scenario(0x4050_0002, vec![ScenarioOp::StartThread]);
        let second = no_fault_scenario(0x4050_0002, vec![ScenarioOp::StartThread]);
        let overlapping = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::join!(
                run_scenario_once(&first, &first_invariant),
                run_scenario_once(&second, &second_invariant),
            )
        })
        .await
        .expect("scenario runs were serialized by process-global test state");
        assert_eq!(overlapping.0.unwrap(), overlapping.1.unwrap());
    }
}
