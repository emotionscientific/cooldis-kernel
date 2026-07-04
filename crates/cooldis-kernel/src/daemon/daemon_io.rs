use crate::agent::manifest_bind::canonical_json_hash;
use crate::{
    AdmissionDecidedPayload, AdmissionDecision as EventAdmissionDecision, CLOCK_TICK_ROUTE_KIND,
    CanonicalContent, CanonicalMessage, CooldisAppServer, CooldisEgressProjectionRuleConfig,
    CooldisEgressRetryConfig, CooldisError, CooldisIoRouteConfig, CooldisResult, CooldisSupervisor,
    CooldisTypingSimulationConfig, EventKind, EventProvenance, EventRecord, EventRecordId,
    EventStore, EventStreamId, IoEgressDeliveredPayload, IoEgressFailedPayload,
    IoIngressReceivedPayload, NewEventRecord, PolicyBoundPayload, PolicyKind, RuntimeThreadHandle,
    SessionEntry, SessionEntryKind, SqliteSessionStore, StreamCursorV1, TIMER_FIRED_ENVELOPE_KIND,
    ThreadCoordinates, ThreadId, ThreadLifecycleRecord, ThreadLifecycleStatus, ThreadStartRequest,
    ThreadTopology, TimerFiredPayload, TurnInput, TurnSubmissionMode, control_stream_id,
    list_active_mandates, parse_mandate_event_id,
};
use async_trait::async_trait;
use cooldis_io_core::{
    AdmissionDecision, DeliveryReceipt, EgressAdapter, EgressEnvelope, EgressKind, IngressAck,
    IngressContent, IngressEnvelope, IngressQueueStore, IngressSink, IngressState, IoError,
    IoResult, IoTarget, IoTurnInput, KernelIoBridge, KernelIoReceipt, ProviderPolicy,
    ResolvedIoTarget, ThreadAddress,
};
use cooldis_io_telegram::{TelegramUpdate, TelegramWebhookAdapter};
use regex::{Captures, Regex};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value, Value as JsonValue, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;

const DEFAULT_QUEUE_BATCH: usize = 16;
const DEFAULT_WORKER_POLL_MS: u64 = 250;
const DEFAULT_EGRESS_PROJECTOR_POLL_MS: u64 = 250;
const MAX_HTTP_HEADER_BYTES: usize = 16 * 1024;
const MAX_HTTP_BODY_BYTES: usize = 4 * 1024 * 1024;
const MAX_TYPING_SIMULATION_DELAY: Duration = Duration::from_secs(8);
const IO_EGRESS_PROJECTOR_DISCHARGED_BY: &str = "projector:io-egress";
const IO_EGRESS_PROJECTOR_FUNCTION: &str = "delivery/v1";

#[derive(Clone, Debug, Default)]
struct RouteEgressConfig {
    projection_rules: Vec<CompiledEgressProjectionRule>,
    typing_simulation: Option<CooldisTypingSimulationConfig>,
    retry: CooldisEgressRetryConfig,
}

impl RouteEgressConfig {
    fn from_route(route: &CooldisIoRouteConfig) -> CooldisResult<Self> {
        let mut projection_rules = Vec::new();
        for (index, rule) in route.egress_projection.iter().enumerate() {
            projection_rules.push(CompiledEgressProjectionRule::compile(
                &route.id, index, rule,
            )?);
        }
        Ok(Self {
            projection_rules,
            typing_simulation: route.typing_simulation.clone(),
            retry: route.egress_retry,
        })
    }

    fn project(&self, envelope: EgressEnvelope) -> Vec<EgressEnvelope> {
        if self.projection_rules.is_empty() {
            return vec![envelope];
        }

        let EgressKind::AssistantMessage { text } = &envelope.kind else {
            return vec![envelope];
        };
        let text = text.clone();
        let matches = self.projection_matches(&text);
        if matches.is_empty() {
            return vec![envelope];
        }

        let stripped_text = strip_projection_matches(&text, &matches);
        let has_silence = matches.iter().any(|matched| matched.action == "silence");
        let text_order = first_remaining_text_offset(&text, &matches);
        let mut projected = Vec::new();

        if !has_silence && !stripped_text.trim().is_empty() {
            let mut text_envelope = envelope.clone();
            text_envelope.kind = EgressKind::AssistantMessage {
                text: stripped_text,
            };
            projected.push(ProjectedEgress {
                order: text_order.unwrap_or(usize::MAX),
                tie_breaker: usize::MAX,
                envelope: text_envelope,
            });
        }

        for (index, matched) in matches.into_iter().enumerate() {
            let kind = if matched.action == "silence" {
                EgressKind::Silence {
                    reason: matched
                        .payload
                        .get("reason")
                        .and_then(JsonValue::as_str)
                        .map(ToOwned::to_owned),
                }
            } else {
                EgressKind::PlatformAction {
                    action: matched.action,
                    payload: matched.payload,
                }
            };
            projected.push(ProjectedEgress {
                order: matched.start,
                tie_breaker: index,
                envelope: sibling_egress(&envelope, kind),
            });
        }

        projected.sort_by_key(|projected| (projected.order, projected.tie_breaker));
        projected
            .into_iter()
            .map(|projected| projected.envelope)
            .collect()
    }

    fn projection_matches(&self, text: &str) -> Vec<ProjectionMatch> {
        let mut matches = Vec::new();
        for (rule_index, rule) in self.projection_rules.iter().enumerate() {
            for captures in rule.regex.captures_iter(text) {
                let Some(span) = captures.get(0) else {
                    continue;
                };
                matches.push(ProjectionMatch {
                    start: span.start(),
                    end: span.end(),
                    rule_index,
                    action: rule.action.clone(),
                    payload: projection_payload(rule, &captures),
                });
            }
        }

        matches.sort_by_key(|matched| (matched.start, matched.rule_index, matched.end));
        let mut accepted = Vec::new();
        let mut previous_end = 0;
        for matched in matches {
            if matched.start < previous_end {
                continue;
            }
            previous_end = matched.end;
            accepted.push(matched);
        }
        accepted
    }
}

#[derive(Clone, Debug)]
struct CompiledEgressProjectionRule {
    regex: Regex,
    action: String,
}

impl CompiledEgressProjectionRule {
    fn compile(
        route_id: &str,
        index: usize,
        rule: &CooldisEgressProjectionRuleConfig,
    ) -> CooldisResult<Self> {
        let regex = Regex::new(&rule.pattern).map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "io.routes.{route_id}.egress_projection[{index}].pattern invalid regex: {err}"
            ))
        })?;
        Ok(Self {
            regex,
            action: rule.action.trim().to_string(),
        })
    }
}

#[derive(Debug)]
struct ProjectionMatch {
    start: usize,
    end: usize,
    rule_index: usize,
    action: String,
    payload: JsonValue,
}

#[derive(Debug)]
struct ProjectedEgress {
    order: usize,
    tie_breaker: usize,
    envelope: EgressEnvelope,
}

#[derive(Clone, Debug)]
struct BoundEgressThread {
    route_id: String,
    scope_key: String,
    coordinates: ThreadCoordinates,
}

#[derive(Clone)]
struct DaemonEgressState {
    connection: Arc<StdMutex<rusqlite::Connection>>,
}

impl DaemonEgressState {
    fn connect(dsn: impl AsRef<str>) -> IoResult<Self> {
        let connection = open_egress_state_connection(dsn.as_ref())?;
        init_egress_state_schema(&connection)?;
        Ok(Self {
            connection: Arc::new(StdMutex::new(connection)),
        })
    }

    fn bind_thread(
        &self,
        route_id: &str,
        source_scope: &str,
        scope_key: &str,
        coordinates: &ThreadCoordinates,
    ) -> IoResult<()> {
        let connection = self.lock_connection()?;
        connection
            .execute(
                "INSERT INTO cooldis_daemon_egress_threads (
                    route_id, source_scope, scope_key, tenant_id, user_id, session_id, thread_id, updated_at_ms
                 )
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(route_id, thread_id) DO UPDATE SET
                    source_scope = excluded.source_scope,
                    scope_key = excluded.scope_key,
                    tenant_id = excluded.tenant_id,
                    user_id = excluded.user_id,
                    session_id = excluded.session_id,
                    updated_at_ms = excluded.updated_at_ms",
                params![
                    route_id,
                    source_scope,
                    scope_key,
                    coordinates.tenant_id,
                    coordinates.user_id,
                    coordinates.session_id,
                    coordinates.thread_id.to_string(),
                    now_ms() as i64
                ],
            )
            .map_err(egress_state_error)?;
        Ok(())
    }

    fn bound_threads(&self, route_id: &str) -> IoResult<Vec<BoundEgressThread>> {
        let connection = self.lock_connection()?;
        let mut statement = connection
            .prepare(
                "SELECT route_id, source_scope, scope_key, tenant_id, user_id, session_id, thread_id
                 FROM cooldis_daemon_egress_threads
                 WHERE route_id = ?1
                 ORDER BY updated_at_ms, thread_id",
            )
            .map_err(egress_state_error)?;
        let rows = statement
            .query_map(params![route_id], |row| {
                let thread_id: String = row.get(6)?;
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    thread_id,
                ))
            })
            .map_err(egress_state_error)?;

        let mut bindings = Vec::new();
        for row in rows {
            let (route_id, scope_key, tenant_id, user_id, session_id, thread_id) =
                row.map_err(egress_state_error)?;
            let thread_id = ThreadId::parse_str(&thread_id).map_err(|err| {
                IoError::Queue(format!("invalid egress thread id {thread_id:?}: {err}"))
            })?;
            bindings.push(BoundEgressThread {
                route_id,
                scope_key,
                coordinates: ThreadCoordinates {
                    tenant_id,
                    user_id,
                    session_id,
                    thread_id,
                },
            });
        }
        Ok(bindings)
    }

    fn cursor(&self, route_id: &str, thread_id: &str) -> IoResult<Option<StreamCursorV1>> {
        let connection = self.lock_connection()?;
        let cursor_json = connection
            .query_row(
                "SELECT cursor_json
                 FROM cooldis_daemon_egress_cursors
                 WHERE route_id = ?1 AND thread_id = ?2",
                params![route_id, thread_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(egress_state_error)?;
        cursor_json
            .map(|json| {
                serde_json::from_str(&json)
                    .map_err(|err| IoError::Queue(format!("decode egress cursor: {err}")))
            })
            .transpose()
    }

    fn store_cursor(
        &self,
        route_id: &str,
        thread_id: &str,
        cursor: &StreamCursorV1,
    ) -> IoResult<()> {
        let connection = self.lock_connection()?;
        let current_json = connection
            .query_row(
                "SELECT cursor_json
                 FROM cooldis_daemon_egress_cursors
                 WHERE route_id = ?1 AND thread_id = ?2",
                params![route_id, thread_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(egress_state_error)?;
        if let Some(current_json) = current_json {
            let current: StreamCursorV1 = serde_json::from_str(&current_json)
                .map_err(|err| IoError::Queue(format!("decode egress cursor: {err}")))?;
            if current.stream_id == cursor.stream_id
                && current.sequence.get() >= cursor.sequence.get()
            {
                return Ok(());
            }
        }
        let cursor_json = serde_json::to_string(cursor)
            .map_err(|err| IoError::Queue(format!("encode egress cursor: {err}")))?;
        connection
            .execute(
                "INSERT INTO cooldis_daemon_egress_cursors (
                    route_id, thread_id, cursor_json, updated_at_ms
                 )
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(route_id, thread_id) DO UPDATE SET
                    cursor_json = excluded.cursor_json,
                    updated_at_ms = excluded.updated_at_ms",
                params![route_id, thread_id, cursor_json, now_ms() as i64],
            )
            .map_err(egress_state_error)?;
        Ok(())
    }

    fn push_dead_letter(&self, dead_letter: &EgressDeadLetter) -> IoResult<()> {
        let envelope_json = serde_json::to_string(&dead_letter.envelope)
            .map_err(|err| IoError::Queue(format!("encode dead-letter envelope: {err}")))?;
        let connection = self.lock_connection()?;
        connection
            .execute(
                "INSERT INTO cooldis_daemon_egress_dead_letters (
                    id, route_id, thread_id, source_event_id, envelope_index, dedupe_key,
                    egress_kind, attempts, error, envelope_json, created_at_ms
                 )
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    format!("dead-{}", uuid::Uuid::now_v7()),
                    dead_letter.route_id,
                    dead_letter.thread_id,
                    dead_letter.source_event_id,
                    dead_letter.envelope_index as i64,
                    dead_letter.dedupe_key,
                    dead_letter.egress_kind,
                    dead_letter.attempts as i64,
                    dead_letter.error,
                    envelope_json,
                    now_ms() as i64,
                ],
            )
            .map_err(egress_state_error)?;
        Ok(())
    }

    fn dead_letter_count(&self, route_id: &str) -> IoResult<usize> {
        let connection = self.lock_connection()?;
        let count = connection
            .query_row(
                "SELECT COUNT(*)
                 FROM cooldis_daemon_egress_dead_letters
                 WHERE route_id = ?1",
                params![route_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(egress_state_error)?;
        Ok(count.max(0) as usize)
    }

    fn lock_connection(&self) -> IoResult<std::sync::MutexGuard<'_, rusqlite::Connection>> {
        self.connection
            .lock()
            .map_err(|err| IoError::Queue(format!("egress state lock poisoned: {err}")))
    }
}

#[derive(Clone, Debug)]
struct EgressDeadLetter {
    route_id: String,
    thread_id: String,
    source_event_id: String,
    envelope_index: usize,
    dedupe_key: String,
    egress_kind: String,
    attempts: u32,
    error: String,
    envelope: EgressEnvelope,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct IngressReceiptContext {
    target: IoTarget,
    metadata: BTreeMap<String, String>,
    source_ingress_id: Option<String>,
}

#[derive(Clone)]
pub struct CooldisDaemonIoBridge {
    supervisor: CooldisSupervisor,
    tenant_id: String,
    user_id: String,
    model: String,
    model_provider: String,
    cwd: PathBuf,
    session_store_path: Option<PathBuf>,
    threads: Arc<Mutex<HashMap<String, ThreadCoordinates>>>,
    active_turns: Arc<Mutex<HashMap<String, String>>>,
    egress_adapters: Arc<RwLock<HashMap<String, Arc<dyn EgressAdapter>>>>,
    egress_route_configs: Arc<RwLock<HashMap<String, RouteEgressConfig>>>,
    egress_states: Arc<RwLock<HashMap<String, Arc<DaemonEgressState>>>>,
}

impl CooldisDaemonIoBridge {
    pub fn new(
        supervisor: CooldisSupervisor,
        tenant_id: impl Into<String>,
        user_id: impl Into<String>,
        model_provider: impl Into<String>,
        model: impl Into<String>,
        cwd: impl Into<PathBuf>,
    ) -> Self {
        Self {
            supervisor,
            tenant_id: tenant_id.into(),
            user_id: user_id.into(),
            model: model.into(),
            model_provider: model_provider.into(),
            cwd: cwd.into(),
            session_store_path: None,
            threads: Arc::new(Mutex::new(HashMap::new())),
            active_turns: Arc::new(Mutex::new(HashMap::new())),
            egress_adapters: Arc::new(RwLock::new(HashMap::new())),
            egress_route_configs: Arc::new(RwLock::new(HashMap::new())),
            egress_states: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn from_app_server(server: &CooldisAppServer) -> Self {
        let mut bridge = Self::new(
            server.supervisor(),
            server.tenant_id().to_string(),
            server.user_id().to_string(),
            server.model_provider().to_string(),
            server.model().to_string(),
            server.cwd().to_path_buf(),
        );
        bridge.session_store_path = Some(server.session_store_path().to_path_buf());
        bridge
    }

    pub fn direct_sink(&self) -> Arc<dyn IngressSink> {
        Arc::new(DirectRuntimeIngressSink::new(self.clone()))
    }

    pub async fn register_egress_adapter(
        &self,
        protocol: impl Into<String>,
        instance_id: impl Into<String>,
        adapter: Arc<dyn EgressAdapter>,
    ) {
        let protocol = protocol.into();
        let instance_id = instance_id.into();
        self.egress_adapters
            .write()
            .await
            .insert(source_scope(&protocol, &instance_id), adapter);
    }

    pub async fn register_egress_route_config(
        &self,
        protocol: impl Into<String>,
        instance_id: impl Into<String>,
        route: &CooldisIoRouteConfig,
    ) -> CooldisResult<()> {
        let protocol = protocol.into();
        let instance_id = instance_id.into();
        let config = RouteEgressConfig::from_route(route)?;
        self.egress_route_configs
            .write()
            .await
            .insert(source_scope(&protocol, &instance_id), config);
        Ok(())
    }

    pub async fn register_egress_state_sqlite_dsn(
        &self,
        protocol: impl Into<String>,
        instance_id: impl Into<String>,
        dsn: impl AsRef<str>,
    ) -> IoResult<()> {
        let protocol = protocol.into();
        let instance_id = instance_id.into();
        let state = Arc::new(DaemonEgressState::connect(dsn)?);
        self.egress_states
            .write()
            .await
            .insert(source_scope(&protocol, &instance_id), state);
        Ok(())
    }

    pub async fn start_egress_projector_sqlite_dsn(
        &self,
        protocol: impl Into<String>,
        instance_id: impl Into<String>,
        dsn: impl AsRef<str>,
    ) -> IoResult<JoinHandle<()>> {
        let protocol = protocol.into();
        let instance_id = instance_id.into();
        self.register_egress_state_sqlite_dsn(&protocol, &instance_id, dsn)
            .await?;
        let bridge = self.clone();
        Ok(tokio::spawn(async move {
            bridge.run_egress_projector(protocol, instance_id).await;
        }))
    }

    pub async fn drain_egress_once(&self, protocol: &str, instance_id: &str) -> IoResult<usize> {
        let key = source_scope(protocol, instance_id);
        let state = self.egress_states.read().await.get(&key).cloned();
        let Some(state) = state else {
            return Ok(0);
        };
        let route_config = self
            .egress_route_configs
            .read()
            .await
            .get(&key)
            .cloned()
            .unwrap_or_default();
        let adapter = self.egress_adapters.read().await.get(&key).cloned();
        let mut delivered_sources = 0;
        for binding in state.bound_threads(instance_id)? {
            let handle = match self.bound_thread_handle(&binding).await {
                Ok(handle) => handle,
                Err(_) => continue,
            };
            delivered_sources += self
                .drain_thread_egress(&state, &binding, handle, adapter.as_deref(), &route_config)
                .await?;
        }
        Ok(delivered_sources)
    }

    async fn bound_thread_handle(
        &self,
        binding: &BoundEgressThread,
    ) -> CooldisResult<RuntimeThreadHandle> {
        match self.supervisor.get_thread_at(&binding.coordinates).await {
            Ok(handle) => Ok(handle),
            Err(CooldisError::ThreadNotFound(_)) => {
                self.supervisor
                    .load_thread_from_lifecycle(ThreadLifecycleRecord {
                        coordinates: binding.coordinates.clone(),
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
            }
            Err(err) => Err(err),
        }
    }

    pub async fn egress_cursor_for_thread(
        &self,
        protocol: &str,
        instance_id: &str,
        thread_id: &str,
    ) -> IoResult<Option<StreamCursorV1>> {
        let key = source_scope(protocol, instance_id);
        let state = self.egress_states.read().await.get(&key).cloned();
        let Some(state) = state else {
            return Ok(None);
        };
        state.cursor(instance_id, thread_id)
    }

    pub async fn egress_dead_letter_count(
        &self,
        protocol: &str,
        instance_id: &str,
    ) -> IoResult<usize> {
        let key = source_scope(protocol, instance_id);
        let state = self.egress_states.read().await.get(&key).cloned();
        let Some(state) = state else {
            return Ok(0);
        };
        state.dead_letter_count(instance_id)
    }

    pub async fn submit_envelope(&self, envelope: IngressEnvelope) -> IoResult<KernelIoReceipt> {
        if is_clock_tick_envelope(&envelope) {
            return self.submit_clock_tick_envelope(&envelope).await;
        }
        let mut target = self.resolve_target(&envelope).await?;
        let (coordinates, _) = self.ensure_thread(&target).await?;
        target.address.thread_id = Some(coordinates.thread_id.to_string());
        let state = self.ingress_state(&target).await;
        let policy_hash = self
            .ensure_route_policy_bound(&coordinates, &envelope)
            .await?;
        let ingress_event = self
            .record_ingress_received(&coordinates, &envelope)
            .await?;
        let decision = self.decide(&envelope, &target, &state).await?;
        self.record_admission_decided(
            &coordinates,
            &envelope,
            &decision,
            &policy_hash,
            ingress_event.id,
        )
        .await?;
        self.apply(&envelope, &target, &decision).await
    }

    async fn submit_clock_tick_envelope(
        &self,
        envelope: &IngressEnvelope,
    ) -> IoResult<KernelIoReceipt> {
        let store_path = self.session_store_path.as_ref().ok_or_else(|| {
            IoError::Bridge("clock.tick requires a daemon session store path".to_string())
        })?;
        let coordinates = clock_tick_coordinates(envelope)?;
        let target = ResolvedIoTarget::new(
            ThreadAddress::new(
                coordinates.tenant_id.clone(),
                coordinates.user_id.clone(),
                coordinates.session_id.clone(),
            )
            .with_thread_id(coordinates.thread_id.to_string()),
        );
        let decision = AdmissionDecision::ObserveOnly {
            reason: "clock tick admitted as timer.fired".to_string(),
        };
        let mut receipt = KernelIoReceipt::new(envelope, target, &decision);
        receipt.thread_id = Some(coordinates.thread_id.to_string());

        let timer = clock_tick_payload(envelope)?;
        let store = SqliteSessionStore::open(store_path).map_err(cooldis_history_error)?;
        let mandate_is_live = list_active_mandates(&store, &coordinates)
            .await
            .map_err(cooldis_bridge_error)?
            .iter()
            .any(|mandate| mandate.event.id == timer.mandate_event_id);
        if !mandate_is_live {
            return Ok(receipt);
        }
        let stream_id = control_stream_id(&coordinates);
        let events = store
            .read_events(&stream_id, None)
            .await
            .map_err(cooldis_history_error)?;
        for event in &events {
            if event.kind != EventKind::TimerFired {
                continue;
            }
            let payload = serde_json::from_value::<TimerFiredPayload>(event.payload.clone())
                .map_err(|err| IoError::Bridge(format!("invalid timer.fired payload: {err}")))?;
            if payload.mandate_event_id == timer.mandate_event_id
                && payload.occurrence_index == timer.occurrence_index
            {
                return Ok(receipt);
            }
        }

        let mandate_event_id = timer.mandate_event_id;
        let mut record = NewEventRecord::witnessed(
            coordinates,
            EventKind::TimerFired,
            serde_json::to_value(timer)
                .map_err(|err| IoError::Bridge(format!("encode timer.fired payload: {err}")))?,
        );
        record.provenance = EventProvenance {
            source_streams: vec![stream_id.clone()],
            source_event_ids: vec![mandate_event_id],
            ..EventProvenance::default()
        };
        store
            .append_events(&stream_id, vec![record])
            .await
            .map_err(cooldis_history_error)?;
        Ok(receipt)
    }

    async fn resolve_target(&self, envelope: &IngressEnvelope) -> IoResult<ResolvedIoTarget> {
        let threading = envelope
            .metadata
            .get("cooldis_route_threading")
            .map(String::as_str)
            .unwrap_or("per_conversation");
        let session_id = match threading {
            "single_thread" | "route_single_thread" => {
                format!("io:{}", envelope.source.stable_scope())
            }
            "per_actor" => format!(
                "io:{}:{}",
                envelope.source.stable_scope(),
                envelope
                    .actor
                    .as_ref()
                    .map(|actor| actor.external_actor_id.as_str())
                    .unwrap_or("anonymous")
            ),
            _ => format!(
                "io:{}:{}",
                envelope.source.stable_scope(),
                envelope.conversation.stable_key()
            ),
        };

        let mut target = ResolvedIoTarget::new(ThreadAddress::new(
            self.tenant_id.clone(),
            self.user_id.clone(),
            session_id,
        ))
        .with_provider_policy(ProviderPolicy::new(
            self.model_provider.clone(),
            self.model.clone(),
        ));
        target.metadata.insert(
            "cooldis_source_scope".to_string(),
            envelope.source.stable_scope(),
        );
        Ok(target)
    }

    async fn ingress_state(&self, target: &ResolvedIoTarget) -> IngressState {
        let active_turn_id = self
            .active_turns
            .lock()
            .await
            .get(&target.address.scope_key())
            .cloned();
        IngressState {
            active_turn_id,
            pending_count: 0,
            dedupe_seen: false,
            metadata: target.metadata.clone(),
        }
    }

    async fn decide(
        &self,
        envelope: &IngressEnvelope,
        target: &ResolvedIoTarget,
        state: &IngressState,
    ) -> IoResult<AdmissionDecision> {
        let input = IoTurnInput::from_envelope(envelope, target);
        let turn_id = format!("turn-{}", uuid::Uuid::now_v7());
        match envelope
            .metadata
            .get("cooldis_route_policy")
            .map(String::as_str)
            .unwrap_or("queue_per_conversation")
        {
            "observe_only" => Ok(AdmissionDecision::ObserveOnly {
                reason: "route policy observe_only".to_string(),
            }),
            "reject" => Ok(AdmissionDecision::reject("route policy reject")),
            "steer" | "steer_when_active" => {
                if let Some(active_turn_id) = &state.active_turn_id {
                    Ok(AdmissionDecision::steer(
                        turn_id,
                        Some(active_turn_id.clone()),
                        input,
                    ))
                } else {
                    Ok(AdmissionDecision::queue(turn_id, input))
                }
            }
            "interrupt" | "interrupt_on_new_dm" => Ok(AdmissionDecision::Interrupt {
                reason: "route policy interrupt".to_string(),
                replacement_turn_id: Some(turn_id),
                replacement: Some(input),
            }),
            _ => Ok(AdmissionDecision::queue(turn_id, input)),
        }
    }

    async fn ensure_route_policy_bound(
        &self,
        coordinates: &ThreadCoordinates,
        envelope: &IngressEnvelope,
    ) -> IoResult<String> {
        let policy_id = admission_route_policy_id(envelope);
        let content_hash = canonical_json_hash(&admission_route_policy_config(envelope))
            .map_err(cooldis_bridge_error)?;
        let handle = self
            .supervisor
            .get_thread_at(coordinates)
            .await
            .map_err(cooldis_bridge_error)?;
        let control_events = handle
            .read_control_events()
            .await
            .map_err(cooldis_bridge_error)?;
        let latest = control_events
            .iter()
            .filter(|event| event.kind == EventKind::PolicyBound)
            .filter(|event| {
                event.payload.get("policy_id").and_then(Value::as_str) == Some(policy_id.as_str())
            })
            .max_by_key(|event| event.sequence.get());
        if latest.and_then(|event| event.payload.get("content_hash").and_then(Value::as_str))
            == Some(content_hash.as_str())
        {
            return Ok(content_hash);
        }
        let payload = PolicyBoundPayload {
            policy_kind: PolicyKind::AdmissionRoute,
            policy_id,
            content_hash: content_hash.clone(),
            valid_from_note: "valid until next policy.bound of same policy_id".to_string(),
        };
        let mut value = serde_json::to_value(payload)
            .map_err(|err| IoError::Bridge(format!("policy.bound payload codec failed: {err}")))?;
        if let Some(object) = value.as_object_mut() {
            object.insert(
                "schema".to_string(),
                json!(EventKind::PolicyBound.payload_schema_id()),
            );
        }
        handle
            .append_control_event(NewEventRecord::witnessed(
                coordinates.clone(),
                EventKind::PolicyBound,
                value,
            ))
            .await
            .map_err(cooldis_bridge_error)?;
        Ok(content_hash)
    }

    async fn record_ingress_received(
        &self,
        coordinates: &ThreadCoordinates,
        envelope: &IngressEnvelope,
    ) -> IoResult<crate::EventRecord> {
        let handle = self
            .supervisor
            .get_thread_at(coordinates)
            .await
            .map_err(cooldis_bridge_error)?;
        let envelope_value = serde_json::to_value(envelope)
            .map_err(|err| IoError::Bridge(format!("ingress envelope codec failed: {err}")))?;
        let payload = IoIngressReceivedPayload {
            route_id: Some(route_id_for_envelope(envelope)),
            dedupe_key: envelope.dedupe_key.as_ref().map(|key| key.stable_key()),
            external_conversation_id: Some(envelope.conversation.external_conversation_id.clone()),
            external_actor_id: envelope
                .actor
                .as_ref()
                .map(|actor| actor.external_actor_id.clone()),
            external_message_id: external_message_id(envelope),
            envelope_digest: canonical_json_hash(&envelope_value).map_err(cooldis_bridge_error)?,
        };
        let mut value = serde_json::to_value(payload).map_err(|err| {
            IoError::Bridge(format!("io.ingress.received payload codec failed: {err}"))
        })?;
        if let Some(object) = value.as_object_mut() {
            object.insert(
                "schema".to_string(),
                json!(EventKind::IoIngressReceived.payload_schema_id()),
            );
        }
        handle
            .append_control_event(NewEventRecord::witnessed(
                coordinates.clone(),
                EventKind::IoIngressReceived,
                value,
            ))
            .await
            .map_err(cooldis_bridge_error)
    }

    async fn record_admission_decided(
        &self,
        coordinates: &ThreadCoordinates,
        envelope: &IngressEnvelope,
        decision: &AdmissionDecision,
        policy_hash: &str,
        ingress_event_id: crate::EventRecordId,
    ) -> IoResult<crate::EventRecord> {
        let handle = self
            .supervisor
            .get_thread_at(coordinates)
            .await
            .map_err(cooldis_bridge_error)?;
        let route_id = route_id_for_envelope(envelope);
        let payload = AdmissionDecidedPayload {
            route_id: route_id.clone(),
            policy_hash: policy_hash.to_string(),
            decision: event_admission_decision(decision),
            admissible: Some(admissible_decisions_for_envelope(envelope)),
            source_ingress_event_ids: vec![ingress_event_id],
        };
        let mut value = serde_json::to_value(payload).map_err(|err| {
            IoError::Bridge(format!("admission.decided payload codec failed: {err}"))
        })?;
        if let Some(object) = value.as_object_mut() {
            object.insert(
                "schema".to_string(),
                json!(EventKind::AdmissionDecided.payload_schema_id()),
            );
        }
        handle
            .append_control_event(NewEventRecord::discharged(
                coordinates.clone(),
                EventKind::AdmissionDecided,
                value,
                EventProvenance {
                    source_streams: vec![crate::EventStreamId::new(format!(
                        "control:{}",
                        coordinates.thread_id
                    ))],
                    source_event_ids: vec![ingress_event_id],
                    discharged_by: Some(format!("policy:admission_route:{route_id}")),
                    function: Some("admission_route/v1".to_string()),
                    config_hash: Some(policy_hash.to_string()),
                    ..EventProvenance::default()
                },
            ))
            .await
            .map_err(cooldis_bridge_error)
    }

    async fn ensure_thread(
        &self,
        target: &ResolvedIoTarget,
    ) -> IoResult<(ThreadCoordinates, RuntimeThreadHandle)> {
        if let Some(thread_id) = &target.address.thread_id {
            let thread_id = ThreadId::parse_str(thread_id)
                .map_err(|err| IoError::Bridge(format!("invalid target thread id: {err}")))?;
            let coordinates = ThreadCoordinates {
                tenant_id: target.address.tenant_id.clone(),
                user_id: target.address.user_id.clone(),
                session_id: target.address.session_id.clone(),
                thread_id,
            };
            let handle = self
                .supervisor
                .get_thread_at(&coordinates)
                .await
                .map_err(cooldis_bridge_error)?;
            return Ok((coordinates, handle));
        }

        let scope_key = target.address.scope_key();
        if let Some(coordinates) = self.threads.lock().await.get(&scope_key).cloned() {
            let handle = self
                .supervisor
                .get_thread_at(&coordinates)
                .await
                .map_err(cooldis_bridge_error)?;
            return Ok((coordinates, handle));
        }

        let topology = target
            .parent_thread_id
            .as_deref()
            .map(ThreadId::parse_str)
            .transpose()
            .map_err(|err| IoError::Bridge(format!("invalid parent thread id: {err}")))?
            .map(ThreadTopology::spawned_from)
            .unwrap_or_else(ThreadTopology::root);

        let handle = self
            .supervisor
            .start_thread(ThreadStartRequest {
                tenant_id: target.address.tenant_id.clone(),
                user_id: target.address.user_id.clone(),
                session_id: target.address.session_id.clone(),
                topology,
                metadata: Default::default(),
            })
            .await
            .map_err(cooldis_bridge_error)?;
        let coordinates = handle.context().coordinates.clone();
        self.threads
            .lock()
            .await
            .insert(scope_key, coordinates.clone());
        Ok((coordinates, handle))
    }

    fn runtime_input(&self, input: &IoTurnInput) -> TurnInput {
        let policy = input.provider_policy.clone().unwrap_or_else(|| {
            ProviderPolicy::new(self.model_provider.clone(), self.model.clone())
        });
        let mut turn = TurnInput::text(input.text.clone())
            .with_provider(policy.provider)
            .with_model(policy.model)
            .with_cwd(self.cwd.clone());
        for (key, value) in &input.metadata {
            turn = turn.with_metadata(key.clone(), value.clone());
        }
        for attachment in &input.attachments {
            turn = turn.with_metadata(
                format!("attachment:{}", attachment.id),
                attachment
                    .name
                    .clone()
                    .unwrap_or_else(|| attachment.media_type.clone()),
            );
        }
        turn
    }

    async fn bind_egress_thread(
        &self,
        envelope: &IngressEnvelope,
        target: &ResolvedIoTarget,
        coordinates: &ThreadCoordinates,
    ) -> IoResult<()> {
        let route_id = route_id_for_ingress(envelope);
        let key = source_scope(&envelope.source.protocol, &route_id);
        let state = self.egress_states.read().await.get(&key).cloned();
        if let Some(state) = state {
            state.bind_thread(&route_id, &key, &target.address.scope_key(), coordinates)?;
        }
        Ok(())
    }

    async fn append_ingress_received_event(
        &self,
        handle: &RuntimeThreadHandle,
        envelope: &IngressEnvelope,
        _target: &ResolvedIoTarget,
        turn_id: &str,
    ) -> IoResult<EventRecord> {
        let route_id = route_id_for_ingress(envelope);
        let mut payload = serde_json::to_value(IoIngressReceivedPayload {
            route_id: Some(route_id.clone()),
            dedupe_key: envelope.dedupe_key.as_ref().map(|key| key.stable_key()),
            external_conversation_id: Some(envelope.conversation.external_conversation_id.clone()),
            external_actor_id: envelope
                .actor
                .as_ref()
                .map(|actor| actor.external_actor_id.clone()),
            external_message_id: envelope.metadata.get("telegram_message_id").cloned(),
            envelope_digest: ingress_envelope_digest(envelope)?,
        })
        .map_err(|err| IoError::Bridge(format!("encode ingress receipt payload: {err}")))?;
        let object = payload.as_object_mut().ok_or_else(|| {
            IoError::Bridge("ingress receipt payload did not encode as object".to_string())
        })?;
        object.insert(
            "turn_id".to_string(),
            JsonValue::String(turn_id.to_string()),
        );
        object.insert(
            "source_scope".to_string(),
            JsonValue::String(envelope.source.stable_scope()),
        );
        object.insert(
            "ingress_envelope_id".to_string(),
            JsonValue::String(envelope.id.clone()),
        );
        object.insert(
            "target".to_string(),
            serde_json::to_value(IoTarget::reply_to(envelope))
                .map_err(|err| IoError::Bridge(format!("encode ingress target: {err}")))?,
        );
        object.insert(
            "ingress_metadata".to_string(),
            serde_json::to_value(&envelope.metadata)
                .map_err(|err| IoError::Bridge(format!("encode ingress metadata: {err}")))?,
        );

        handle
            .append_thread_event_record(NewEventRecord::witnessed(
                handle.context().coordinates.clone(),
                EventKind::IoIngressReceived,
                payload,
            ))
            .await
            .map_err(cooldis_bridge_error)
    }

    async fn run_egress_projector(self, protocol: String, instance_id: String) {
        let poll_interval = Duration::from_millis(DEFAULT_EGRESS_PROJECTOR_POLL_MS);
        loop {
            let _ = self.drain_egress_once(&protocol, &instance_id).await;
            tokio::time::sleep(poll_interval).await;
        }
    }

    #[cfg(test)]
    async fn deliver_egress(&self, envelope: EgressEnvelope) {
        let key = envelope.target.source.stable_scope();
        let adapter = self.egress_adapters.read().await.get(&key).cloned();
        let Some(adapter) = adapter else {
            return;
        };
        let route_config = self
            .egress_route_configs
            .read()
            .await
            .get(&key)
            .cloned()
            .unwrap_or_default();
        for envelope in route_config.project(envelope) {
            if let Some(typing) = &route_config.typing_simulation
                && let EgressKind::AssistantMessage { text } = &envelope.kind
                && !text.is_empty()
            {
                let typing_envelope = sibling_egress(
                    &envelope,
                    EgressKind::PlatformAction {
                        action: "typing".to_string(),
                        payload: JsonValue::Object(JsonMap::new()),
                    },
                );
                let _ = adapter.deliver(typing_envelope).await;
                let delay = typing_delay_for_text(text, typing.chars_per_second);
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
            }
            let _ = adapter.deliver(envelope).await;
        }
    }

    async fn drain_thread_egress(
        &self,
        state: &DaemonEgressState,
        binding: &BoundEgressThread,
        handle: RuntimeThreadHandle,
        adapter: Option<&dyn EgressAdapter>,
        route_config: &RouteEgressConfig,
    ) -> IoResult<usize> {
        let thread_id = binding.coordinates.thread_id.to_string();
        let cursor = state.cursor(&binding.route_id, &thread_id)?;
        let after_cursor_ids = match &cursor {
            Some(cursor) => handle
                .read_thread_events_after_cursor(cursor)
                .await
                .map_err(cooldis_bridge_error)?
                .into_iter()
                .map(|event| event.id)
                .collect::<HashSet<_>>(),
            None => HashSet::new(),
        };
        let all_events = handle
            .read_thread_events(None)
            .await
            .map_err(cooldis_bridge_error)?;
        let context = handle
            .session_context()
            .await
            .map_err(cooldis_bridge_error)?;
        let mut receipt_cursors = receipt_dedupe_cursors(&all_events);
        let mut pending_contexts = Vec::<IngressReceiptContext>::new();
        let mut active_context = None;
        let mut delivered_sources = 0;

        for event in &all_events {
            if let Some(context) = ingress_context_from_event(event) {
                pending_contexts.push(context);
            }
            if let Some(entry) = session_entry_for_event(event, &context.entries)
                && session_entry_is_user_authored(entry)
                && !pending_contexts.is_empty()
            {
                active_context = Some(pending_contexts.remove(0));
            }

            if matches!(
                event.kind,
                EventKind::IoEgressDelivered | EventKind::IoEgressFailed
            ) {
                let after_cursor = cursor.is_none() || after_cursor_ids.contains(&event.id);
                if !after_cursor {
                    continue;
                }
                state.store_cursor(&binding.route_id, &thread_id, &event.cursor_v1())?;
                continue;
            }

            let Some(text) = assistant_text_from_session_event(event, &context.entries) else {
                let after_cursor = cursor.is_none() || after_cursor_ids.contains(&event.id);
                if !after_cursor {
                    continue;
                }
                state.store_cursor(&binding.route_id, &thread_id, &event.cursor_v1())?;
                continue;
            };
            let Some(source_context) = active_context.clone() else {
                let after_cursor = cursor.is_none() || after_cursor_ids.contains(&event.id);
                if !after_cursor {
                    continue;
                }
                state.store_cursor(&binding.route_id, &thread_id, &event.cursor_v1())?;
                continue;
            };
            let after_cursor = cursor.is_none() || after_cursor_ids.contains(&event.id);
            if !after_cursor
                && !source_has_partial_projected_receipts(
                    route_config,
                    event,
                    &source_context,
                    &text,
                    &receipt_cursors,
                )
            {
                continue;
            }

            match self
                .deliver_assistant_source(
                    state,
                    binding,
                    &handle,
                    adapter,
                    route_config,
                    event,
                    source_context,
                    text,
                    &mut receipt_cursors,
                )
                .await?
            {
                SourceDeliveryOutcome::Completed => {
                    delivered_sources += 1;
                    self.active_turns.lock().await.remove(&binding.scope_key);
                }
                SourceDeliveryOutcome::Blocked => break,
            }
        }

        Ok(delivered_sources)
    }

    async fn deliver_assistant_source(
        &self,
        state: &DaemonEgressState,
        binding: &BoundEgressThread,
        handle: &RuntimeThreadHandle,
        adapter: Option<&dyn EgressAdapter>,
        route_config: &RouteEgressConfig,
        source_event: &EventRecord,
        source_context: IngressReceiptContext,
        text: String,
        receipt_cursors: &mut HashMap<String, ReceiptDedupeCursor>,
    ) -> IoResult<SourceDeliveryOutcome> {
        let mut envelope = EgressEnvelope::new(
            source_context.target,
            EgressKind::AssistantMessage { text },
            now_ms(),
        );
        envelope.source_ingress_id = source_context.source_ingress_id;
        envelope.metadata = source_context.metadata;

        let mut envelope_index = 0;
        let mut latest_receipt_cursor = None;
        for projected in route_config.project(envelope) {
            if let Some(typing) = &route_config.typing_simulation
                && let EgressKind::AssistantMessage { text } = &projected.kind
                && !text.is_empty()
            {
                let typing_envelope = sibling_egress(
                    &projected,
                    EgressKind::PlatformAction {
                        action: "typing".to_string(),
                        payload: JsonValue::Object(JsonMap::new()),
                    },
                );
                let outcome = self
                    .deliver_projected_envelope(
                        state,
                        binding,
                        handle,
                        adapter,
                        source_event,
                        envelope_index,
                        typing_envelope,
                        route_config.retry,
                        receipt_cursors,
                    )
                    .await?;
                match outcome {
                    EnvelopeDeliveryOutcome::Delivered(cursor) => {
                        retain_newest_cursor(&mut latest_receipt_cursor, cursor);
                    }
                    EnvelopeDeliveryOutcome::Blocked => return Ok(SourceDeliveryOutcome::Blocked),
                }
                envelope_index += 1;
                let delay = typing_delay_for_text(text, typing.chars_per_second);
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
            }

            let outcome = self
                .deliver_projected_envelope(
                    state,
                    binding,
                    handle,
                    adapter,
                    source_event,
                    envelope_index,
                    projected,
                    route_config.retry,
                    receipt_cursors,
                )
                .await?;
            match outcome {
                EnvelopeDeliveryOutcome::Delivered(cursor) => {
                    retain_newest_cursor(&mut latest_receipt_cursor, cursor);
                }
                EnvelopeDeliveryOutcome::Blocked => return Ok(SourceDeliveryOutcome::Blocked),
            }
            envelope_index += 1;
        }

        let cursor = latest_receipt_cursor.unwrap_or_else(|| source_event.cursor_v1());
        state.store_cursor(
            &binding.route_id,
            &binding.coordinates.thread_id.to_string(),
            &cursor,
        )?;
        Ok(SourceDeliveryOutcome::Completed)
    }

    async fn deliver_projected_envelope(
        &self,
        state: &DaemonEgressState,
        binding: &BoundEgressThread,
        handle: &RuntimeThreadHandle,
        adapter: Option<&dyn EgressAdapter>,
        source_event: &EventRecord,
        envelope_index: usize,
        envelope: EgressEnvelope,
        retry: CooldisEgressRetryConfig,
        receipt_cursors: &mut HashMap<String, ReceiptDedupeCursor>,
    ) -> IoResult<EnvelopeDeliveryOutcome> {
        let dedupe_key = egress_dedupe_key(source_event.id, envelope_index);
        if let Some(receipt) = matching_receipt_cursor(receipt_cursors, &dedupe_key, &envelope.kind)
        {
            return Ok(EnvelopeDeliveryOutcome::Delivered(receipt.cursor.clone()));
        }

        if matches!(envelope.kind, EgressKind::Silence { .. }) {
            let receipt = DeliveryReceipt {
                egress_id: envelope.id.clone(),
                delivered: true,
                external_message_id: None,
                error: None,
                metadata: BTreeMap::new(),
            };
            let event = append_egress_delivered_receipt(
                handle,
                binding,
                source_event,
                envelope_index,
                &dedupe_key,
                &envelope,
                &receipt,
                1,
            )
            .await?;
            let cursor = event.cursor_v1();
            receipt_cursors.insert(
                dedupe_key,
                ReceiptDedupeCursor {
                    cursor: cursor.clone(),
                    egress_kind: egress_kind_name(&envelope.kind),
                },
            );
            return Ok(EnvelopeDeliveryOutcome::Delivered(cursor));
        }

        let Some(adapter) = adapter else {
            return Ok(EnvelopeDeliveryOutcome::Blocked);
        };
        let max_attempts = retry.max_attempts.max(1);
        let mut last_error = String::new();
        for attempt in 1..=max_attempts {
            match adapter.deliver(envelope.clone()).await {
                Ok(receipt) => {
                    let event = append_egress_delivered_receipt(
                        handle,
                        binding,
                        source_event,
                        envelope_index,
                        &dedupe_key,
                        &envelope,
                        &receipt,
                        attempt,
                    )
                    .await?;
                    let cursor = event.cursor_v1();
                    receipt_cursors.insert(
                        dedupe_key,
                        ReceiptDedupeCursor {
                            cursor: cursor.clone(),
                            egress_kind: egress_kind_name(&envelope.kind),
                        },
                    );
                    return Ok(EnvelopeDeliveryOutcome::Delivered(cursor));
                }
                Err(err) => {
                    last_error = err.to_string();
                    if attempt < max_attempts {
                        let delay = egress_backoff_delay(retry.base_backoff_ms, attempt);
                        if !delay.is_zero() {
                            tokio::time::sleep(delay).await;
                        }
                    }
                }
            }
        }

        let event = append_egress_failed_receipt(
            handle,
            binding,
            source_event,
            envelope_index,
            &dedupe_key,
            &envelope,
            max_attempts,
            &last_error,
        )
        .await?;
        let egress_kind = egress_kind_name(&envelope.kind);
        state.push_dead_letter(&EgressDeadLetter {
            route_id: binding.route_id.clone(),
            thread_id: binding.coordinates.thread_id.to_string(),
            source_event_id: source_event.id.to_string(),
            envelope_index,
            dedupe_key: dedupe_key.clone(),
            egress_kind: egress_kind.clone(),
            attempts: max_attempts,
            error: last_error,
            envelope,
        })?;
        let cursor = event.cursor_v1();
        receipt_cursors.insert(
            dedupe_key,
            ReceiptDedupeCursor {
                cursor: cursor.clone(),
                egress_kind,
            },
        );
        Ok(EnvelopeDeliveryOutcome::Delivered(cursor))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceDeliveryOutcome {
    Completed,
    Blocked,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum EnvelopeDeliveryOutcome {
    Delivered(StreamCursorV1),
    Blocked,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReceiptDedupeCursor {
    cursor: StreamCursorV1,
    egress_kind: String,
}

#[async_trait]
impl KernelIoBridge for CooldisDaemonIoBridge {
    async fn apply(
        &self,
        envelope: &IngressEnvelope,
        target: &ResolvedIoTarget,
        decision: &AdmissionDecision,
    ) -> IoResult<KernelIoReceipt> {
        match decision {
            AdmissionDecision::Queue { turn_id, input } => {
                let (coordinates, handle) = self.ensure_thread(target).await?;
                self.bind_egress_thread(envelope, target, &coordinates)
                    .await?;
                self.append_ingress_received_event(&handle, envelope, target, turn_id)
                    .await?;
                self.active_turns
                    .lock()
                    .await
                    .insert(target.address.scope_key(), turn_id.clone());
                self.supervisor
                    .submit_turn_to_with_mode(
                        &coordinates,
                        turn_id.clone(),
                        self.runtime_input(input),
                        TurnSubmissionMode::Queue,
                    )
                    .await
                    .map_err(cooldis_bridge_error)?;
                let mut receipt = KernelIoReceipt::new(envelope, target.clone(), decision);
                receipt.thread_id = Some(coordinates.thread_id.to_string());
                Ok(receipt)
            }
            AdmissionDecision::Steer { turn_id, input, .. } => {
                let (coordinates, handle) = self.ensure_thread(target).await?;
                self.bind_egress_thread(envelope, target, &coordinates)
                    .await?;
                self.append_ingress_received_event(&handle, envelope, target, turn_id)
                    .await?;
                self.active_turns
                    .lock()
                    .await
                    .insert(target.address.scope_key(), turn_id.clone());
                self.supervisor
                    .submit_turn_to_with_mode(
                        &coordinates,
                        turn_id.clone(),
                        self.runtime_input(input),
                        TurnSubmissionMode::Steer,
                    )
                    .await
                    .map_err(cooldis_bridge_error)?;
                let mut receipt = KernelIoReceipt::new(envelope, target.clone(), decision);
                receipt.thread_id = Some(coordinates.thread_id.to_string());
                Ok(receipt)
            }
            AdmissionDecision::Interrupt {
                reason,
                replacement_turn_id,
                replacement,
            } => {
                let (coordinates, handle) = self.ensure_thread(target).await?;
                self.supervisor
                    .cancel_at(&coordinates, reason.clone())
                    .await
                    .map_err(cooldis_bridge_error)?;
                if let (Some(turn_id), Some(input)) = (replacement_turn_id, replacement) {
                    self.bind_egress_thread(envelope, target, &coordinates)
                        .await?;
                    self.append_ingress_received_event(&handle, envelope, target, turn_id)
                        .await?;
                    self.active_turns
                        .lock()
                        .await
                        .insert(target.address.scope_key(), turn_id.clone());
                    self.supervisor
                        .submit_turn_to_with_mode(
                            &coordinates,
                            turn_id.clone(),
                            self.runtime_input(input),
                            TurnSubmissionMode::Interrupt,
                        )
                        .await
                        .map_err(cooldis_bridge_error)?;
                }
                let mut receipt = KernelIoReceipt::new(envelope, target.clone(), decision);
                receipt.thread_id = Some(coordinates.thread_id.to_string());
                Ok(receipt)
            }
            AdmissionDecision::ObserveOnly { .. } => {
                Ok(KernelIoReceipt::new(envelope, target.clone(), decision))
            }
            AdmissionDecision::Reject { reason, .. } => {
                Err(IoError::PolicyRejected(reason.clone()))
            }
            AdmissionDecision::Fork { .. } => Err(IoError::Bridge(
                "fork admission is not wired into the daemon bridge yet".to_string(),
            )),
        }
    }
}

#[derive(Clone)]
pub struct DirectRuntimeIngressSink {
    bridge: CooldisDaemonIoBridge,
}

impl DirectRuntimeIngressSink {
    pub fn new(bridge: CooldisDaemonIoBridge) -> Self {
        Self { bridge }
    }
}

#[async_trait]
impl IngressSink for DirectRuntimeIngressSink {
    async fn submit(&self, envelope: IngressEnvelope) -> IoResult<IngressAck> {
        let ack = IngressAck::accepted(&envelope);
        self.bridge.submit_envelope(envelope).await?;
        Ok(ack)
    }
}

pub struct RouteIngressSink {
    inner: Arc<dyn IngressSink>,
    route_id: String,
    policy: Option<String>,
    threading: Option<String>,
}

impl RouteIngressSink {
    pub fn new(inner: Arc<dyn IngressSink>, route: &CooldisIoRouteConfig) -> Self {
        Self {
            inner,
            route_id: route.id.clone(),
            policy: route.policy.clone(),
            threading: route.threading.clone(),
        }
    }
}

#[async_trait]
impl IngressSink for RouteIngressSink {
    async fn submit(&self, mut envelope: IngressEnvelope) -> IoResult<IngressAck> {
        envelope
            .metadata
            .insert("cooldis_route_id".to_string(), self.route_id.clone());
        if let Some(policy) = &self.policy {
            envelope
                .metadata
                .insert("cooldis_route_policy".to_string(), policy.clone());
        }
        if let Some(threading) = &self.threading {
            envelope
                .metadata
                .insert("cooldis_route_threading".to_string(), threading.clone());
        }
        self.inner.submit(envelope).await
    }
}

pub struct CooldisDaemonQueueWorker {
    queue: Arc<dyn IngressQueueStore>,
    bridge: CooldisDaemonIoBridge,
    worker_id: String,
    max_messages: usize,
    poll_interval: Duration,
    visibility_timeout_secs: u32,
}

impl CooldisDaemonQueueWorker {
    pub fn new(
        queue: Arc<dyn IngressQueueStore>,
        bridge: CooldisDaemonIoBridge,
        worker_id: impl Into<String>,
        visibility_timeout_secs: u32,
    ) -> Self {
        Self {
            queue,
            bridge,
            worker_id: worker_id.into(),
            max_messages: DEFAULT_QUEUE_BATCH,
            poll_interval: Duration::from_millis(DEFAULT_WORKER_POLL_MS),
            visibility_timeout_secs,
        }
    }

    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    pub fn with_max_messages(mut self, max_messages: usize) -> Self {
        self.max_messages = max_messages;
        self
    }

    pub async fn drain_once(&self) -> IoResult<usize> {
        let leased = self
            .queue
            .lease_ingress(
                &self.worker_id,
                self.max_messages,
                self.visibility_timeout_secs,
            )
            .await?;
        let count = leased.len();
        for message in leased {
            match self.bridge.submit_envelope(message.envelope).await {
                Ok(_) => self.queue.complete_ingress(&message.message_id).await?,
                Err(err) => {
                    let reason = err.to_string();
                    self.queue
                        .retry_ingress(&message.message_id, &reason)
                        .await?;
                    return Err(err);
                }
            }
        }
        Ok(count)
    }

    pub async fn run(self) {
        loop {
            match self.drain_once().await {
                Ok(0) => tokio::time::sleep(self.poll_interval).await,
                Ok(_) => {}
                Err(err) => {
                    eprintln!("cooldis daemon ingress worker failed: {err}");
                    tokio::time::sleep(self.poll_interval).await;
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct TelegramWebhookServerConfig {
    pub route_id: String,
    pub listen: String,
    pub path: String,
    pub secret_token: Option<String>,
}

pub struct TelegramWebhookServer {
    config: TelegramWebhookServerConfig,
    listener: TcpListener,
    sink: Arc<dyn IngressSink>,
}

impl TelegramWebhookServer {
    pub async fn bind(
        config: TelegramWebhookServerConfig,
        sink: Arc<dyn IngressSink>,
    ) -> CooldisResult<Self> {
        let listener = TcpListener::bind(&config.listen).await.map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "failed to bind Telegram webhook route {} on {}: {err}",
                config.route_id, config.listen
            ))
        })?;
        Ok(Self {
            config,
            listener,
            sink,
        })
    }

    pub fn local_addr(&self) -> CooldisResult<SocketAddr> {
        self.listener.local_addr().map_err(|err| {
            CooldisError::RuntimeFactory(format!("failed to read Telegram webhook address: {err}"))
        })
    }

    pub async fn serve(self) -> CooldisResult<()> {
        let adapter = Arc::new(TelegramWebhookAdapter::new(self.config.route_id.clone()));
        loop {
            let (stream, _) = self.listener.accept().await.map_err(|err| {
                CooldisError::RuntimeFactory(format!(
                    "failed to accept Telegram webhook connection: {err}"
                ))
            })?;
            let config = self.config.clone();
            let sink = self.sink.clone();
            let adapter = adapter.clone();
            tokio::spawn(async move {
                if let Err(err) =
                    handle_telegram_webhook_connection(stream, config, adapter, sink).await
                {
                    eprintln!("cooldis Telegram webhook request failed: {err}");
                }
            });
        }
    }
}

async fn handle_telegram_webhook_connection(
    mut stream: TcpStream,
    config: TelegramWebhookServerConfig,
    adapter: Arc<TelegramWebhookAdapter>,
    sink: Arc<dyn IngressSink>,
) -> CooldisResult<()> {
    let request = match read_http_request(&mut stream).await {
        Ok(request) => request,
        Err(err) => {
            write_json_response(
                &mut stream,
                400,
                json!({ "ok": false, "error": err.to_string() }),
            )
            .await?;
            return Ok(());
        }
    };

    if request.method != "POST" {
        write_json_response(
            &mut stream,
            405,
            json!({ "ok": false, "error": "method_not_allowed" }),
        )
        .await?;
        return Ok(());
    }
    if request.path != config.path {
        write_json_response(
            &mut stream,
            404,
            json!({ "ok": false, "error": "not_found" }),
        )
        .await?;
        return Ok(());
    }
    if let Some(secret_token) = config.secret_token.as_deref() {
        let actual = request
            .headers
            .get("x-telegram-bot-api-secret-token")
            .map(String::as_str);
        if actual != Some(secret_token) {
            write_json_response(
                &mut stream,
                401,
                json!({ "ok": false, "error": "unauthorized" }),
            )
            .await?;
            return Ok(());
        }
    }

    let update: TelegramUpdate = serde_json::from_slice(&request.body).map_err(|err| {
        CooldisError::RuntimeFactory(format!("failed to decode Telegram update JSON: {err}"))
    })?;
    match adapter
        .submit_update(sink.as_ref(), &update, now_ms())
        .await
        .map_err(|err| CooldisError::RuntimeFactory(err.to_string()))?
    {
        Some(ack) => {
            write_json_response(
                &mut stream,
                200,
                json!({ "ok": true, "accepted": ack.accepted, "envelopeId": ack.envelope_id }),
            )
            .await?;
        }
        None => {
            write_json_response(
                &mut stream,
                200,
                json!({ "ok": true, "accepted": false, "reason": "unsupported_update" }),
            )
            .await?;
        }
    }
    Ok(())
}

#[derive(Debug)]
struct HttpRequest {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

async fn read_http_request(stream: &mut TcpStream) -> CooldisResult<HttpRequest> {
    let mut buffer = Vec::new();
    let header_end;
    loop {
        let mut chunk = [0_u8; 1024];
        let read = stream.read(&mut chunk).await.map_err(|err| {
            CooldisError::RuntimeFactory(format!("failed to read HTTP request: {err}"))
        })?;
        if read == 0 {
            return Err(CooldisError::RuntimeFactory(
                "connection closed before HTTP headers".to_string(),
            ));
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(index) = find_header_end(&buffer) {
            header_end = index;
            break;
        }
        if buffer.len() > MAX_HTTP_HEADER_BYTES {
            return Err(CooldisError::RuntimeFactory(
                "HTTP headers are too large".to_string(),
            ));
        }
    }

    let headers_text = std::str::from_utf8(&buffer[..header_end]).map_err(|err| {
        CooldisError::RuntimeFactory(format!("HTTP headers are not UTF-8: {err}"))
    })?;
    let mut lines = headers_text.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| CooldisError::RuntimeFactory("missing HTTP request line".to_string()))?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .ok_or_else(|| CooldisError::RuntimeFactory("missing HTTP method".to_string()))?
        .to_string();
    let path = request_parts
        .next()
        .ok_or_else(|| CooldisError::RuntimeFactory("missing HTTP path".to_string()))?
        .to_string();

    let mut headers = HashMap::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }

    let content_length = headers
        .get("content-length")
        .map(|value| {
            value.parse::<usize>().map_err(|err| {
                CooldisError::RuntimeFactory(format!("invalid content-length {value:?}: {err}"))
            })
        })
        .transpose()?
        .unwrap_or(0);
    if content_length > MAX_HTTP_BODY_BYTES {
        return Err(CooldisError::RuntimeFactory(
            "HTTP body is too large".to_string(),
        ));
    }

    let body_start = header_end + 4;
    let mut body = buffer[body_start..].to_vec();
    while body.len() < content_length {
        let mut chunk = vec![0_u8; content_length - body.len()];
        let read = stream.read(&mut chunk).await.map_err(|err| {
            CooldisError::RuntimeFactory(format!("failed to read HTTP body: {err}"))
        })?;
        if read == 0 {
            return Err(CooldisError::RuntimeFactory(
                "connection closed before HTTP body".to_string(),
            ));
        }
        body.extend_from_slice(&chunk[..read]);
    }
    body.truncate(content_length);

    Ok(HttpRequest {
        method,
        path,
        headers,
        body,
    })
}

async fn write_json_response(
    stream: &mut TcpStream,
    status: u16,
    body: serde_json::Value,
) -> CooldisResult<()> {
    let body = body.to_string();
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Internal Server Error",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\n\
Content-Type: application/json\r\n\
Content-Length: {}\r\n\
Connection: close\r\n\
\r\n\
{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await.map_err(|err| {
        CooldisError::RuntimeFactory(format!("failed to write HTTP response: {err}"))
    })?;
    Ok(())
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

fn projection_payload(rule: &CompiledEgressProjectionRule, captures: &Captures<'_>) -> JsonValue {
    let mut payload = JsonMap::new();
    for name in rule.regex.capture_names().flatten() {
        if let Some(value) = captures.name(name) {
            payload.insert(
                name.to_string(),
                JsonValue::String(value.as_str().to_string()),
            );
        }
    }
    JsonValue::Object(payload)
}

fn strip_projection_matches(text: &str, matches: &[ProjectionMatch]) -> String {
    let mut stripped = String::with_capacity(text.len());
    let mut cursor = 0;
    for matched in matches {
        if cursor < matched.start {
            stripped.push_str(&text[cursor..matched.start]);
        }
        cursor = matched.end;
    }
    if cursor < text.len() {
        stripped.push_str(&text[cursor..]);
    }
    stripped
}

fn first_remaining_text_offset(text: &str, matches: &[ProjectionMatch]) -> Option<usize> {
    let mut cursor = 0;
    for matched in matches {
        if cursor < matched.start {
            return Some(cursor);
        }
        cursor = matched.end;
    }
    (cursor < text.len()).then_some(cursor)
}

fn sibling_egress(source: &EgressEnvelope, kind: EgressKind) -> EgressEnvelope {
    let mut envelope = EgressEnvelope::new(source.target.clone(), kind, now_ms());
    envelope.source_ingress_id = source.source_ingress_id.clone();
    envelope.metadata = source.metadata.clone();
    envelope
}

fn source_has_partial_projected_receipts(
    route_config: &RouteEgressConfig,
    source_event: &EventRecord,
    source_context: &IngressReceiptContext,
    text: &str,
    receipt_cursors: &HashMap<String, ReceiptDedupeCursor>,
) -> bool {
    let mut envelope = EgressEnvelope::new(
        source_context.target.clone(),
        EgressKind::AssistantMessage {
            text: text.to_string(),
        },
        now_ms(),
    );
    envelope.source_ingress_id = source_context.source_ingress_id.clone();
    envelope.metadata = source_context.metadata.clone();

    let mut envelope_index = 0;
    let mut saw_receipt = false;
    let mut saw_missing = false;
    for projected in route_config.project(envelope) {
        if let Some(_typing) = &route_config.typing_simulation
            && let EgressKind::AssistantMessage { text } = &projected.kind
            && !text.is_empty()
        {
            let typing_envelope = sibling_egress(
                &projected,
                EgressKind::PlatformAction {
                    action: "typing".to_string(),
                    payload: JsonValue::Object(JsonMap::new()),
                },
            );
            note_projection_receipt_presence(
                source_event.id,
                envelope_index,
                &typing_envelope.kind,
                receipt_cursors,
                &mut saw_receipt,
                &mut saw_missing,
            );
            envelope_index += 1;
        }

        note_projection_receipt_presence(
            source_event.id,
            envelope_index,
            &projected.kind,
            receipt_cursors,
            &mut saw_receipt,
            &mut saw_missing,
        );
        envelope_index += 1;
    }
    saw_receipt && saw_missing
}

fn note_projection_receipt_presence(
    source_event_id: EventRecordId,
    envelope_index: usize,
    kind: &EgressKind,
    receipt_cursors: &HashMap<String, ReceiptDedupeCursor>,
    saw_receipt: &mut bool,
    saw_missing: &mut bool,
) {
    let dedupe_key = egress_dedupe_key(source_event_id, envelope_index);
    if matching_receipt_cursor(receipt_cursors, &dedupe_key, kind).is_some() {
        *saw_receipt = true;
    } else {
        *saw_missing = true;
    }
}

fn matching_receipt_cursor<'a>(
    receipt_cursors: &'a HashMap<String, ReceiptDedupeCursor>,
    dedupe_key: &str,
    kind: &EgressKind,
) -> Option<&'a ReceiptDedupeCursor> {
    let egress_kind = egress_kind_name(kind);
    receipt_cursors
        .get(dedupe_key)
        .filter(|receipt| receipt.egress_kind == egress_kind)
}

fn retain_newest_cursor(slot: &mut Option<StreamCursorV1>, candidate: StreamCursorV1) {
    if slot
        .as_ref()
        .is_none_or(|current| candidate.sequence.get() > current.sequence.get())
    {
        *slot = Some(candidate);
    }
}

async fn append_egress_delivered_receipt(
    handle: &RuntimeThreadHandle,
    binding: &BoundEgressThread,
    source_event: &EventRecord,
    envelope_index: usize,
    dedupe_key: &str,
    envelope: &EgressEnvelope,
    receipt: &DeliveryReceipt,
    attempts: u32,
) -> IoResult<EventRecord> {
    let payload = egress_delivered_payload(binding, envelope, receipt, attempts)?;
    append_egress_receipt_event(
        handle,
        source_event,
        EventKind::IoEgressDelivered,
        add_egress_receipt_metadata(
            payload,
            source_event.id,
            envelope_index,
            dedupe_key,
            &envelope.id,
        )?,
    )
    .await
}

async fn append_egress_failed_receipt(
    handle: &RuntimeThreadHandle,
    binding: &BoundEgressThread,
    source_event: &EventRecord,
    envelope_index: usize,
    dedupe_key: &str,
    envelope: &EgressEnvelope,
    attempts: u32,
    error: &str,
) -> IoResult<EventRecord> {
    let payload = egress_failed_payload(binding, envelope, attempts, error)?;
    append_egress_receipt_event(
        handle,
        source_event,
        EventKind::IoEgressFailed,
        add_egress_receipt_metadata(
            payload,
            source_event.id,
            envelope_index,
            dedupe_key,
            &envelope.id,
        )?,
    )
    .await
}

async fn append_egress_receipt_event(
    handle: &RuntimeThreadHandle,
    source_event: &EventRecord,
    kind: EventKind,
    payload: JsonValue,
) -> IoResult<EventRecord> {
    let stream_id = EventStreamId::for_thread(&handle.context().coordinates);
    handle
        .append_thread_event_record(NewEventRecord::discharged(
            handle.context().coordinates.clone(),
            kind,
            payload,
            EventProvenance {
                source_streams: vec![stream_id],
                source_event_ids: vec![source_event.id],
                discharged_by: Some(IO_EGRESS_PROJECTOR_DISCHARGED_BY.to_string()),
                function: Some(IO_EGRESS_PROJECTOR_FUNCTION.to_string()),
                ..EventProvenance::default()
            },
        ))
        .await
        .map_err(cooldis_bridge_error)
}

fn egress_delivered_payload(
    binding: &BoundEgressThread,
    envelope: &EgressEnvelope,
    receipt: &DeliveryReceipt,
    attempts: u32,
) -> IoResult<JsonValue> {
    serde_json::to_value(IoEgressDeliveredPayload {
        route_id: binding.route_id.clone(),
        egress_kind: egress_kind_name(&envelope.kind),
        external_message_id: receipt.external_message_id.clone(),
        attempts,
    })
    .map_err(|err| IoError::Bridge(format!("encode egress delivered payload: {err}")))
}

fn egress_failed_payload(
    binding: &BoundEgressThread,
    envelope: &EgressEnvelope,
    attempts: u32,
    error: &str,
) -> IoResult<JsonValue> {
    let mut payload = serde_json::to_value(IoEgressFailedPayload {
        route_id: binding.route_id.clone(),
        egress_kind: egress_kind_name(&envelope.kind),
        attempts,
        error_class: "delivery_failed".to_string(),
        dead_lettered: true,
    })
    .map_err(|err| IoError::Bridge(format!("encode egress failed payload: {err}")))?;
    payload_object_mut(&mut payload)?
        .insert("error".to_string(), JsonValue::String(error.to_string()));
    Ok(payload)
}

fn add_egress_receipt_metadata(
    mut payload: JsonValue,
    source_event_id: EventRecordId,
    envelope_index: usize,
    dedupe_key: &str,
    egress_id: &str,
) -> IoResult<JsonValue> {
    let object = payload_object_mut(&mut payload)?;
    object.insert(
        "source_event_id".to_string(),
        JsonValue::String(source_event_id.to_string()),
    );
    object.insert(
        "envelope_index".to_string(),
        JsonValue::Number(serde_json::Number::from(envelope_index as u64)),
    );
    object.insert(
        "dedupe_key".to_string(),
        JsonValue::String(dedupe_key.to_string()),
    );
    object.insert(
        "egress_id".to_string(),
        JsonValue::String(egress_id.to_string()),
    );
    Ok(payload)
}

fn payload_object_mut(payload: &mut JsonValue) -> IoResult<&mut JsonMap<String, JsonValue>> {
    payload
        .as_object_mut()
        .ok_or_else(|| IoError::Bridge("receipt payload did not encode as object".to_string()))
}

fn receipt_dedupe_cursors(events: &[EventRecord]) -> HashMap<String, ReceiptDedupeCursor> {
    events
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                EventKind::IoEgressDelivered | EventKind::IoEgressFailed
            )
        })
        .filter_map(|event| {
            let dedupe_key = event
                .payload
                .get("dedupe_key")
                .and_then(JsonValue::as_str)
                .map(ToOwned::to_owned)?;
            let egress_kind = event
                .payload
                .get("egress_kind")
                .and_then(JsonValue::as_str)
                .map(ToOwned::to_owned)?;
            Some((
                dedupe_key,
                ReceiptDedupeCursor {
                    cursor: event.cursor_v1(),
                    egress_kind,
                },
            ))
        })
        .collect()
}

fn ingress_context_from_event(event: &EventRecord) -> Option<IngressReceiptContext> {
    if event.kind != EventKind::IoIngressReceived {
        return None;
    }
    let target = serde_json::from_value(event.payload.get("target")?.clone()).ok()?;
    let metadata = event
        .payload
        .get("ingress_metadata")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .ok()
        .flatten()
        .unwrap_or_default();
    let source_ingress_id = event
        .payload
        .get("ingress_envelope_id")
        .and_then(JsonValue::as_str)
        .map(ToOwned::to_owned);
    Some(IngressReceiptContext {
        target,
        metadata,
        source_ingress_id,
    })
}

fn assistant_text_from_session_event(
    event: &EventRecord,
    entries: &[SessionEntry],
) -> Option<String> {
    let entry = session_entry_for_event(event, entries)?;
    assistant_text_from_entry(entry)
}

fn session_entry_for_event<'a>(
    event: &EventRecord,
    entries: &'a [SessionEntry],
) -> Option<&'a SessionEntry> {
    if event.kind != EventKind::SessionEntryAppended {
        return None;
    }
    let entry_id = event.payload.get("entry_id").and_then(JsonValue::as_str)?;
    entries
        .iter()
        .find(|entry| entry.entry_id.to_string() == entry_id)
}

fn session_entry_is_user_authored(entry: &SessionEntry) -> bool {
    matches!(
        entry.kind,
        SessionEntryKind::Message {
            message: CanonicalMessage::User { .. },
        } | SessionEntryKind::CustomContextMessage {
            message: CanonicalMessage::User { .. },
        }
    )
}

fn assistant_text_from_entry(entry: &SessionEntry) -> Option<String> {
    let (SessionEntryKind::Message {
        message: CanonicalMessage::Assistant { content, .. },
    }
    | SessionEntryKind::CustomContextMessage {
        message: CanonicalMessage::Assistant { content, .. },
    }) = &entry.kind
    else {
        return None;
    };
    let text = text_from_canonical_content(content);
    (!text.is_empty()).then_some(text)
}

fn text_from_canonical_content(content: &[CanonicalContent]) -> String {
    content
        .iter()
        .filter_map(|content| match content {
            CanonicalContent::Text { text, .. } => Some(text.as_str()),
            CanonicalContent::Image { .. }
            | CanonicalContent::Thinking { .. }
            | CanonicalContent::ToolCall { .. } => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

fn route_id_for_ingress(envelope: &IngressEnvelope) -> String {
    envelope
        .metadata
        .get("cooldis_route_id")
        .cloned()
        .unwrap_or_else(|| envelope.source.instance_id.clone())
}

fn ingress_envelope_digest(envelope: &IngressEnvelope) -> IoResult<String> {
    let bytes = serde_json::to_vec(envelope)
        .map_err(|err| IoError::Bridge(format!("encode ingress envelope digest: {err}")))?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn egress_dedupe_key(source_event_id: EventRecordId, envelope_index: usize) -> String {
    format!("{source_event_id}:{envelope_index}")
}

fn egress_kind_name(kind: &EgressKind) -> String {
    match kind {
        EgressKind::AssistantDelta { .. } => "assistant_delta".to_string(),
        EgressKind::AssistantMessage { .. } => "assistant_message".to_string(),
        EgressKind::Status { .. } => "status".to_string(),
        EgressKind::ToolStarted { .. } => "tool_started".to_string(),
        EgressKind::ToolCompleted { .. } => "tool_completed".to_string(),
        EgressKind::Error { .. } => "error".to_string(),
        EgressKind::PlatformAction { action, .. } => format!("platform_action:{action}"),
        EgressKind::Silence { .. } => "silence".to_string(),
    }
}

fn egress_backoff_delay(base_backoff_ms: u64, failed_attempt: u32) -> Duration {
    if base_backoff_ms == 0 {
        return Duration::ZERO;
    }
    let exponent = failed_attempt.saturating_sub(1).min(31);
    let factor = 1_u64.checked_shl(exponent).unwrap_or(u64::MAX);
    Duration::from_millis(base_backoff_ms.saturating_mul(factor))
}

fn typing_delay_for_text(text: &str, chars_per_second: u32) -> Duration {
    if chars_per_second == 0 {
        return Duration::ZERO;
    }
    let chars = text.chars().count();
    if chars == 0 {
        return Duration::ZERO;
    }
    let seconds =
        (chars as f64 / chars_per_second as f64).min(MAX_TYPING_SIMULATION_DELAY.as_secs_f64());
    Duration::from_secs_f64(seconds)
}

fn source_scope(protocol: &str, instance_id: &str) -> String {
    format!("{protocol}:{instance_id}")
}

fn route_id_for_envelope(envelope: &IngressEnvelope) -> String {
    envelope
        .metadata
        .get("cooldis_route_id")
        .cloned()
        .unwrap_or_else(|| envelope.source.stable_scope())
}

fn admission_route_policy_id(envelope: &IngressEnvelope) -> String {
    format!("admission_route:{}", route_id_for_envelope(envelope))
}

fn admission_route_policy_config(envelope: &IngressEnvelope) -> Value {
    json!({
        "route_id": route_id_for_envelope(envelope),
        "policy": envelope
            .metadata
            .get("cooldis_route_policy")
            .map(String::as_str)
            .unwrap_or("queue_per_conversation"),
        "threading": envelope
            .metadata
            .get("cooldis_route_threading")
            .map(String::as_str)
            .unwrap_or("per_conversation"),
    })
}

fn external_message_id(envelope: &IngressEnvelope) -> Option<String> {
    envelope
        .metadata
        .get("external_message_id")
        .or_else(|| envelope.metadata.get("telegram_message_id"))
        .cloned()
}

fn event_admission_decision(decision: &AdmissionDecision) -> EventAdmissionDecision {
    match decision {
        AdmissionDecision::Queue { .. } => EventAdmissionDecision::Queue,
        AdmissionDecision::Steer { .. } => EventAdmissionDecision::Steer,
        AdmissionDecision::Interrupt { .. } => EventAdmissionDecision::Interrupt,
        AdmissionDecision::Fork { .. } => EventAdmissionDecision::Fork,
        AdmissionDecision::ObserveOnly { .. } => EventAdmissionDecision::Observe,
        AdmissionDecision::Reject { .. } => EventAdmissionDecision::Reject,
    }
}

fn admissible_decisions_for_envelope(envelope: &IngressEnvelope) -> Vec<EventAdmissionDecision> {
    match envelope
        .metadata
        .get("cooldis_route_policy")
        .map(String::as_str)
        .unwrap_or("queue_per_conversation")
    {
        "observe_only" => vec![EventAdmissionDecision::Observe],
        "reject" => vec![EventAdmissionDecision::Reject],
        "steer" | "steer_when_active" => {
            vec![EventAdmissionDecision::Queue, EventAdmissionDecision::Steer]
        }
        "interrupt" | "interrupt_on_new_dm" => vec![EventAdmissionDecision::Interrupt],
        _ => vec![EventAdmissionDecision::Queue],
    }
}

fn is_clock_tick_envelope(envelope: &IngressEnvelope) -> bool {
    envelope.source.protocol == CLOCK_TICK_ROUTE_KIND
        && matches!(
            &envelope.content,
            IngressContent::Event { kind, .. } if kind == TIMER_FIRED_ENVELOPE_KIND
        )
}

fn clock_tick_coordinates(envelope: &IngressEnvelope) -> IoResult<ThreadCoordinates> {
    Ok(ThreadCoordinates {
        tenant_id: required_metadata(envelope, "cooldis_tenant_id")?.to_string(),
        user_id: required_metadata(envelope, "cooldis_user_id")?.to_string(),
        session_id: required_metadata(envelope, "cooldis_session_id")?.to_string(),
        thread_id: ThreadId::parse_str(required_metadata(envelope, "cooldis_thread_id")?)
            .map_err(|err| IoError::Bridge(format!("invalid clock.tick thread id: {err}")))?,
    })
}

fn clock_tick_payload(envelope: &IngressEnvelope) -> IoResult<TimerFiredPayload> {
    if let IngressContent::Event { kind, payload } = &envelope.content
        && kind == TIMER_FIRED_ENVELOPE_KIND
    {
        return serde_json::from_value::<TimerFiredPayload>(payload.clone())
            .map_err(|err| IoError::Bridge(format!("invalid clock.tick payload: {err}")));
    }

    Ok(TimerFiredPayload {
        mandate_event_id: parse_mandate_event_id(required_metadata(
            envelope,
            "cooldis_mandate_event_id",
        )?)
        .map_err(cooldis_bridge_error)?,
        scheduled_for: required_metadata(envelope, "cooldis_scheduled_for")?.to_string(),
        occurrence_index: required_metadata(envelope, "cooldis_occurrence_index")?
            .parse::<u64>()
            .map_err(|err| {
                IoError::Bridge(format!("invalid clock.tick occurrence index: {err}"))
            })?,
        catch_up: required_metadata(envelope, "cooldis_catch_up")?
            .parse::<bool>()
            .map_err(|err| IoError::Bridge(format!("invalid clock.tick catch_up flag: {err}")))?,
    })
}

fn required_metadata<'a>(envelope: &'a IngressEnvelope, key: &str) -> IoResult<&'a str> {
    envelope
        .metadata
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| IoError::Bridge(format!("clock.tick missing metadata {key:?}")))
}

fn open_egress_state_connection(dsn: &str) -> IoResult<rusqlite::Connection> {
    let path = sqlite_path_from_dsn(dsn)?;
    if path == Path::new(":memory:") {
        return rusqlite::Connection::open_in_memory().map_err(egress_state_error);
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|err| {
            IoError::Queue(format!(
                "create egress sqlite directory {}: {err}",
                parent.display()
            ))
        })?;
    }
    if !path.exists() {
        std::fs::File::create(&path).map_err(|err| {
            IoError::Queue(format!(
                "create egress sqlite file {}: {err}",
                path.display()
            ))
        })?;
    }
    let connection = rusqlite::Connection::open(path).map_err(egress_state_error)?;
    connection
        .busy_timeout(Duration::from_secs(5))
        .map_err(egress_state_error)?;
    Ok(connection)
}

fn sqlite_path_from_dsn(dsn: &str) -> IoResult<PathBuf> {
    let Some(path) = dsn.strip_prefix("sqlite://") else {
        return Err(IoError::Queue(format!(
            "egress projector requires a sqlite:// DSN, got {dsn:?}"
        )));
    };
    Ok(PathBuf::from(path))
}

fn init_egress_state_schema(connection: &rusqlite::Connection) -> IoResult<()> {
    connection
        .execute_batch(
            "
            CREATE TABLE IF NOT EXISTS cooldis_daemon_egress_threads (
                route_id TEXT NOT NULL,
                source_scope TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (route_id, thread_id)
            );
            CREATE INDEX IF NOT EXISTS idx_cooldis_daemon_egress_threads_route
                ON cooldis_daemon_egress_threads (route_id, updated_at_ms);
            CREATE TABLE IF NOT EXISTS cooldis_daemon_egress_cursors (
                route_id TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                cursor_json TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (route_id, thread_id)
            );
            CREATE TABLE IF NOT EXISTS cooldis_daemon_egress_dead_letters (
                id TEXT PRIMARY KEY,
                route_id TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                source_event_id TEXT NOT NULL,
                envelope_index INTEGER NOT NULL,
                dedupe_key TEXT NOT NULL,
                egress_kind TEXT NOT NULL,
                attempts INTEGER NOT NULL,
                error TEXT NOT NULL,
                envelope_json TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_cooldis_daemon_egress_dead_letters_route
                ON cooldis_daemon_egress_dead_letters (route_id, created_at_ms);
            ",
        )
        .map_err(egress_state_error)
}

fn cooldis_bridge_error(err: CooldisError) -> IoError {
    IoError::Bridge(err.to_string())
}

fn cooldis_history_error(err: impl std::fmt::Display) -> IoError {
    IoError::Bridge(err.to_string())
}

fn egress_state_error(err: rusqlite::Error) -> IoError {
    IoError::Queue(format!("egress state sqlite: {err}"))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests;
