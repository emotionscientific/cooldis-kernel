use crate::agent::manifest_bind::canonical_json_hash;
use crate::kernel::admission::{AdmissionGateContext, append_admission_decided};
use crate::kernel::runtime_host::ReservedTurnSubmission;
use crate::{
    AdmissionDecision as EventAdmissionDecision, CLOCK_TICK_ROUTE_KIND, CanonicalContent,
    CanonicalMessage, CooldisAppServer, CooldisCoalesceBurstsConfig,
    CooldisEgressProjectionRuleConfig, CooldisEgressRetryConfig, CooldisError,
    CooldisIoRouteConfig, CooldisResult, CooldisSupervisor, CooldisTypingSimulationConfig,
    EventKind, EventProvenance, EventRecord, EventRecordId, EventSequence, EventStore,
    EventStreamId, HistoryError, IngressOutcomeIntent, IngressSettledBy, IoEgressDeliveredPayload,
    IoEgressFailedPayload, IoEgressRequestedPayload, IoIngressClaimedPayload,
    IoIngressReceivedPayload, IoIngressSettledPayload, KernelThreadSpawnAgentBinding,
    NewEventRecord, PolicyBoundPayload, PolicyKind, RuntimeThreadHandle, SessionEntry,
    SessionEntryKind, SqliteSessionStore, StreamCursorV1, THREAD_AGENT_MANIFEST_HASH_METADATA,
    THREAD_SPAWN_GRANTED_METADATA, TIMER_FIRED_ENVELOPE_KIND, ThreadCheckpoint, ThreadCoordinates,
    ThreadId, ThreadLifecycleRecord, ThreadLifecycleStatus, ThreadReloadDegradedPayload,
    ThreadSpawnedForkPayload, ThreadSpawnedForkSourceCutPayload, ThreadSpawnedPayload,
    ThreadStartRequest, ThreadTopology, TimerFiredPayload, TurnInput, TurnSubmissionMode,
    control_stream_id, list_active_mandates, parse_mandate_event_id,
};
use async_trait::async_trait;
use cooldis_io_core::{
    AdmissionDecision, DeliveryReceipt, EgressAdapter, EgressEnvelope, EgressKind, IngressAck,
    IngressContent, IngressEnvelope, IngressQueueStore, IngressSink, IngressState, IoError,
    IoResult, IoTarget, IoTurnInput, KernelIoBridge, KernelIoReceipt, LeasedIngressEnvelope,
    ProviderPolicy, ResolvedIoTarget, ThreadAddress,
};
use cooldis_io_telegram::{TelegramUpdate, TelegramWebhookAdapter};
use futures_util::future::BoxFuture;
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
const ROUTE_AGENT_REF_METADATA: &str = "cooldis_route_agent_ref";
const INGRESS_MESSAGE_ID_FIELD: &str = "ingress_message_id";
const INGRESS_DEDUPE_SEEN_FIELD: &str = "dedupe_seen";

#[derive(Clone, Debug, Default)]
struct RouteEgressConfig {
    projection_rules: Vec<CompiledEgressProjectionRule>,
    typing_simulation: Option<CooldisTypingSimulationConfig>,
    retry: CooldisEgressRetryConfig,
    threading: Option<String>,
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
            threading: route.threading.clone(),
        })
    }

    fn restores_per_conversation_bindings(&self) -> bool {
        self.threading.as_deref().unwrap_or("per_conversation") == "per_conversation"
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

#[derive(Debug)]
enum ThreadHandleResolutionError {
    Lookup(CooldisError),
    LifecycleLoad(CooldisError),
}

impl ThreadHandleResolutionError {
    fn into_inner(self) -> CooldisError {
        match self {
            Self::Lookup(err) | Self::LifecycleLoad(err) => err,
        }
    }
}

/// Durable route state shared by ingress thread binding and egress projection.
///
/// `cooldis_daemon_egress_threads` keeps its historical name, but its bindings
/// serve both directions and are recovered by ingress during route startup.
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
    turn_id: Option<String>,
}

enum IngressClaimAppend {
    Appended(EventRecord),
    Existing(IngressOutcomeState),
}

#[derive(Clone, Debug)]
enum IngressOutcomeState {
    Missing,
    Claimed {
        claim: EventRecord,
        payload: IoIngressClaimedPayload,
    },
    Settled {
        claim_payload: IoIngressClaimedPayload,
        settle: EventRecord,
    },
}

#[derive(Clone)]
pub struct CooldisDaemonIoBridge {
    app_server: Option<CooldisAppServer>,
    supervisor: CooldisSupervisor,
    tenant_id: String,
    user_id: String,
    model: String,
    model_provider: String,
    cwd: PathBuf,
    session_store_path: Option<PathBuf>,
    threads: Arc<Mutex<HashMap<String, ThreadCoordinates>>>,
    thread_scope_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    thread_load_locks: Arc<Mutex<HashMap<ThreadId, Arc<Mutex<()>>>>>,
    active_turns: Arc<StdMutex<HashMap<String, String>>>,
    egress_adapters: Arc<RwLock<HashMap<String, Arc<dyn EgressAdapter>>>>,
    egress_route_configs: Arc<RwLock<HashMap<String, RouteEgressConfig>>>,
    egress_states: Arc<RwLock<HashMap<String, Arc<DaemonEgressState>>>>,
    #[cfg(test)]
    pause_after_ingress_claim: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(test)]
    ingress_claim_paused: Arc<tokio::sync::Notify>,
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
            app_server: None,
            supervisor,
            tenant_id: tenant_id.into(),
            user_id: user_id.into(),
            model: model.into(),
            model_provider: model_provider.into(),
            cwd: cwd.into(),
            session_store_path: None,
            threads: Arc::new(Mutex::new(HashMap::new())),
            thread_scope_locks: Arc::new(Mutex::new(HashMap::new())),
            thread_load_locks: Arc::new(Mutex::new(HashMap::new())),
            active_turns: Arc::new(StdMutex::new(HashMap::new())),
            egress_adapters: Arc::new(RwLock::new(HashMap::new())),
            egress_route_configs: Arc::new(RwLock::new(HashMap::new())),
            egress_states: Arc::new(RwLock::new(HashMap::new())),
            #[cfg(test)]
            pause_after_ingress_claim: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            ingress_claim_paused: Arc::new(tokio::sync::Notify::new()),
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
        bridge.app_server = Some(server.clone());
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

    pub async fn validate_route_agent_ref(
        &self,
        route: &CooldisIoRouteConfig,
    ) -> CooldisResult<()> {
        let Some(agent_ref) = route.agent_ref.as_deref() else {
            return Ok(());
        };
        let app_server = self.app_server.as_ref().ok_or_else(|| {
            CooldisError::RuntimeFactory(format!(
                "io.routes.{}.agent_ref requires daemon IO to be backed by an app-server",
                route.id
            ))
        })?;
        app_server
            .validate_daemon_route_agent_ref(agent_ref)
            .await
            .map_err(|err| {
                CooldisError::RuntimeFactory(format!(
                    "io.routes.{}.agent_ref {agent_ref:?} did not bind from agent registry root {}: {err}. Publish the agent with `cooldis agent publish --registry-root {}` before starting the daemon.",
                    route.id,
                    app_server.agent_registry_root().display(),
                    app_server.agent_registry_root().display()
                ))
            })
    }

    pub async fn register_egress_state_sqlite_dsn(
        &self,
        protocol: impl Into<String>,
        instance_id: impl Into<String>,
        dsn: impl AsRef<str>,
    ) -> IoResult<()> {
        let protocol = protocol.into();
        let instance_id = instance_id.into();
        let key = source_scope(&protocol, &instance_id);
        let state = Arc::new(DaemonEgressState::connect(dsn)?);
        let restores_per_conversation_bindings = self
            .egress_route_configs
            .read()
            .await
            .get(&key)
            .is_some_and(RouteEgressConfig::restores_per_conversation_bindings);
        let bindings = if restores_per_conversation_bindings {
            self.ingress_bindings(&state, &instance_id)?
        } else {
            Vec::new()
        };

        // Publish the recovered map and its backing state together. A configured
        // per-conversation route cannot create a thread until the state appears,
        // and state readers remain blocked until the map seed is visible.
        let mut states = self.egress_states.write().await;
        if restores_per_conversation_bindings {
            self.threads.lock().await.extend(bindings);
        }
        states.insert(key, state);
        Ok(())
    }

    /// Loads durable ingress bindings for the startup hot-path map seed.
    ///
    /// Rows are ordered oldest-first by `bound_threads`, so replacing by
    /// `scope_key` reduces duplicate crash residue to the latest binding.
    /// Runtime handles are loaded lazily on first ingress or egress use.
    fn ingress_bindings(
        &self,
        state: &DaemonEgressState,
        route_id: &str,
    ) -> IoResult<Vec<(String, ThreadCoordinates)>> {
        let mut latest_by_scope = HashMap::new();
        for binding in state.bound_threads(route_id)? {
            latest_by_scope.insert(binding.scope_key, binding.coordinates);
        }

        Ok(latest_by_scope.into_iter().collect())
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
        self.get_or_load_thread_handle(&binding.coordinates)
            .await
            .map_err(ThreadHandleResolutionError::into_inner)
    }

    /// Resolves a resident runtime thread or lazily rehydrates it from its
    /// durable coordinates and session history after a process restart.
    /// Concurrent ingress and egress callers serialize the load by thread id
    /// so only a fully initialized winner can be observed through this bridge.
    async fn get_or_load_thread_handle(
        &self,
        coordinates: &ThreadCoordinates,
    ) -> Result<RuntimeThreadHandle, ThreadHandleResolutionError> {
        self.get_or_load_thread_handle_inner(coordinates, HashSet::new())
            .await
    }

    fn get_or_load_thread_handle_inner<'a>(
        &'a self,
        coordinates: &'a ThreadCoordinates,
        mut loading: HashSet<ThreadId>,
    ) -> BoxFuture<'a, Result<RuntimeThreadHandle, ThreadHandleResolutionError>> {
        Box::pin(async move {
            if !loading.insert(coordinates.thread_id) {
                return Err(ThreadHandleResolutionError::LifecycleLoad(
                    CooldisError::History(format!(
                        "thread lifecycle topology cycle while lazily loading {}",
                        coordinates.thread_id
                    )),
                ));
            }
            let load_lock = self.thread_load_lock(coordinates.thread_id).await;
            let _load_guard = load_lock.lock().await;
            loop {
                match self.supervisor.get_thread_at(coordinates).await {
                    Ok(handle) => return Ok(handle),
                    Err(CooldisError::ThreadNotFound(_)) => {
                        let lifecycle = match self
                            .reconstruct_thread_lifecycle(coordinates)
                            .await
                            .map_err(ThreadHandleResolutionError::LifecycleLoad)?
                        {
                            Some(lifecycle) => lifecycle,
                            None => {
                                let payload = serde_json::to_value(ThreadReloadDegradedPayload {
                                    thread_id: coordinates.thread_id,
                                    missing: vec![
                                        "topology".to_string(),
                                        "parent_thread_id".to_string(),
                                        "metadata".to_string(),
                                    ],
                                    fallback: "fabricated_root".to_string(),
                                })
                                .map_err(|err| {
                                    ThreadHandleResolutionError::LifecycleLoad(
                                        CooldisError::History(format!(
                                            "thread.reload.degraded payload codec failed: {err}"
                                        )),
                                    )
                                })?;
                                let stream_id = EventStreamId::for_thread(coordinates);
                                self.supervisor
                                    .runtime_store(&coordinates.tenant_id)
                                    .await
                                    .map_err(ThreadHandleResolutionError::LifecycleLoad)?
                                    .append_events(
                                        &stream_id,
                                        vec![NewEventRecord::witnessed(
                                            coordinates.clone(),
                                            EventKind::ThreadReloadDegraded,
                                            payload,
                                        )],
                                    )
                                    .await
                                    .map_err(|err| {
                                        ThreadHandleResolutionError::LifecycleLoad(
                                            CooldisError::History(err.to_string()),
                                        )
                                    })?;
                                let now = now_ms();
                                ThreadLifecycleRecord {
                                    coordinates: coordinates.clone(),
                                    parent_thread_id: None,
                                    topology: ThreadTopology::root(),
                                    status: ThreadLifecycleStatus::Idle,
                                    latest_signal_id: None,
                                    latest_checkpoint_id: None,
                                    created_at_ms: now,
                                    updated_at_ms: now,
                                    metadata: BTreeMap::new(),
                                }
                            }
                        };
                        let mut seen_related = HashSet::new();
                        let related_thread_ids = lifecycle
                            .topology
                            .related_thread_ids()
                            .into_iter()
                            .filter(|thread_id| seen_related.insert(*thread_id));
                        for related_thread_id in related_thread_ids {
                            let related_coordinates = ThreadCoordinates {
                                tenant_id: coordinates.tenant_id.clone(),
                                user_id: coordinates.user_id.clone(),
                                session_id: coordinates.session_id.clone(),
                                thread_id: related_thread_id,
                            };
                            self.get_or_load_thread_handle_inner(
                                &related_coordinates,
                                loading.clone(),
                            )
                            .await?;
                        }
                        match self.supervisor.load_thread_from_lifecycle(lifecycle).await {
                            Ok(handle) => return Ok(handle),
                            Err(CooldisError::ThreadAlreadyExists(_)) => {
                                self.supervisor
                                    .wait_for_thread_start_reservation(
                                        &coordinates.tenant_id,
                                        coordinates.thread_id,
                                    )
                                    .await
                                    .map_err(ThreadHandleResolutionError::Lookup)?;
                            }
                            Err(err) => {
                                return Err(ThreadHandleResolutionError::LifecycleLoad(err));
                            }
                        }
                    }
                    Err(err) => return Err(ThreadHandleResolutionError::Lookup(err)),
                }
            }
        })
    }

    /// EMO-370 seam: reconstruct a lazily loaded thread's lifecycle record
    /// from its own journal: thread-start provenance (topology, parent,
    /// metadata) plus the manifest compile/bind receipts recorded at
    /// creation. The stream is the only durable truth; the binding table
    /// stays a coordinates-only read model, and identity is never
    /// re-resolved from the route's current agent alias (an `@latest` alias
    /// may have moved).
    ///
    /// Returns `Ok(None)` when the journal predates the identity payload
    /// and cannot supply full identity. The caller then applies the
    /// fabricated-root fallback, and the implementation must witness that
    /// fallback with a `thread.reload.degraded` event. Degradation is
    /// never silent.
    async fn reconstruct_thread_lifecycle(
        &self,
        coordinates: &ThreadCoordinates,
    ) -> CooldisResult<Option<ThreadLifecycleRecord>> {
        let store = self
            .supervisor
            .runtime_store(&coordinates.tenant_id)
            .await?;
        let stream_id = EventStreamId::for_thread(coordinates);
        let events = store
            .read_events(&stream_id, None)
            .await
            .map_err(|err| CooldisError::History(err.to_string()))?;
        let Some(start) = events.iter().rev().find(|event| {
            event.kind == EventKind::SessionEntryAppended
                && event.payload.get("entry_kind").and_then(Value::as_str) == Some("runtime")
                && event.payload.get("runtime_kind").and_then(Value::as_str)
                    == Some("thread_started")
        }) else {
            return Ok(None);
        };
        let Some(payload) = start
            .payload
            .get("runtime_payload")
            .and_then(Value::as_object)
        else {
            return Ok(None);
        };
        if !payload.contains_key("parent_thread_id")
            || !payload.contains_key("topology")
            || !payload.contains_key("metadata")
        {
            return Ok(None);
        }
        let parent_thread_id = serde_json::from_value(payload["parent_thread_id"].clone())
            .map_err(|err| {
                CooldisError::History(format!("thread-start parent codec failed: {err}"))
            })?;
        let topology: ThreadTopology = serde_json::from_value(payload["topology"].clone())
            .map_err(|err| {
                CooldisError::History(format!("thread-start topology codec failed: {err}"))
            })?;
        let metadata: BTreeMap<String, String> =
            serde_json::from_value(payload["metadata"].clone()).map_err(|err| {
                CooldisError::History(format!("thread-start metadata codec failed: {err}"))
            })?;
        if parent_thread_id != topology.compatibility_parent_thread_id() {
            return Err(CooldisError::History(format!(
                "thread-start parent does not match topology for {}",
                coordinates.thread_id
            )));
        }
        let created_at_ms = u64::try_from(start.created_at_ms).unwrap_or_default();
        Ok(Some(ThreadLifecycleRecord {
            coordinates: coordinates.clone(),
            parent_thread_id,
            topology,
            status: ThreadLifecycleStatus::Idle,
            latest_signal_id: None,
            latest_checkpoint_id: None,
            created_at_ms,
            updated_at_ms: created_at_ms,
            metadata,
        }))
    }

    async fn thread_load_lock(&self, thread_id: ThreadId) -> Arc<Mutex<()>> {
        let mut locks = self.thread_load_locks.lock().await;
        locks
            .entry(thread_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
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
        let source_envelopes = [envelope.clone()];
        self.submit_envelope_with_sources(envelope, &source_envelopes, &[], false)
            .await
    }

    async fn submit_queued_envelope(&self, envelope: IngressEnvelope) -> IoResult<KernelIoReceipt> {
        if is_clock_tick_envelope(&envelope) {
            return self.submit_clock_tick_envelope(&envelope).await;
        }
        let source_envelopes = [envelope.clone()];
        let ingress_message_ids = [envelope.id.clone()];
        self.submit_envelope_with_sources(envelope, &source_envelopes, &ingress_message_ids, false)
            .await
    }

    async fn queued_message_was_applied(&self, message: &LeasedIngressEnvelope) -> IoResult<bool> {
        let ingress_message_ids = [message.envelope.id.clone()];
        let target = self.resolve_target(&message.envelope).await?;
        Ok(matches!(
            self.ingress_outcome(&target, &ingress_message_ids).await?,
            IngressOutcomeState::Settled { .. }
        ))
    }

    pub async fn submit_coalesced_envelopes(
        &self,
        envelope: IngressEnvelope,
        source_envelopes: &[IngressEnvelope],
    ) -> IoResult<KernelIoReceipt> {
        if source_envelopes.is_empty() {
            return Err(IoError::Bridge(
                "coalesced ingress submit requires at least one source envelope".to_string(),
            ));
        }
        if is_clock_tick_envelope(&envelope) {
            return Err(IoError::Bridge(
                "clock.tick envelopes cannot be coalesced".to_string(),
            ));
        }
        self.submit_envelope_with_sources(envelope, source_envelopes, &[], true)
            .await
    }

    async fn submit_coalesced_queued_envelopes(
        &self,
        envelope: IngressEnvelope,
        source_envelopes: &[IngressEnvelope],
        ingress_message_ids: &[String],
    ) -> IoResult<KernelIoReceipt> {
        if source_envelopes.is_empty() || source_envelopes.len() != ingress_message_ids.len() {
            return Err(IoError::Bridge(
                "coalesced queued ingress requires one message id per source envelope".to_string(),
            ));
        }
        if is_clock_tick_envelope(&envelope) {
            return Err(IoError::Bridge(
                "clock.tick envelopes cannot be coalesced".to_string(),
            ));
        }
        self.submit_envelope_with_sources(envelope, source_envelopes, ingress_message_ids, true)
            .await
    }

    async fn submit_envelope_with_sources(
        &self,
        envelope: IngressEnvelope,
        source_envelopes: &[IngressEnvelope],
        ingress_message_ids: &[String],
        coalesced: bool,
    ) -> IoResult<KernelIoReceipt> {
        if !ingress_message_ids.is_empty() && source_envelopes.len() != ingress_message_ids.len() {
            return Err(IoError::Bridge(
                "durable ingress requires one message id per source envelope".to_string(),
            ));
        }
        let mut target = self.resolve_target(&envelope).await?;
        if !ingress_message_ids.is_empty() {
            match self.ingress_outcome(&target, ingress_message_ids).await? {
                IngressOutcomeState::Missing => {}
                state @ IngressOutcomeState::Claimed { .. } => {
                    return self
                        .recover_ingress_outcome(&envelope, &target, state)
                        .await;
                }
                state @ IngressOutcomeState::Settled { .. } => {
                    return Ok(deduplicated_ingress_receipt(&envelope, target, &state));
                }
            }
        }
        let (coordinates, _handle) = self.ensure_thread(&target, &envelope).await?;
        target.address.thread_id = Some(coordinates.thread_id.to_string());
        let state = self.ingress_state(&target).await?;
        let policy_hash = self
            .ensure_route_policy_bound(&coordinates, &envelope)
            .await?;
        let mut ingress_event_ids = Vec::new();
        for (index, source_envelope) in source_envelopes.iter().enumerate() {
            let ingress_event = self
                .record_ingress_received(
                    &coordinates,
                    source_envelope,
                    ingress_message_ids.get(index).map(String::as_str),
                )
                .await?;
            ingress_event_ids.push(ingress_event.id);
        }
        let decision = self.decide(&envelope, &target, &state).await?;
        let ingress_source_stream = control_stream_id(&coordinates);
        let admission_event = self
            .record_admission_decided(
                &coordinates,
                &envelope,
                &decision,
                &policy_hash,
                ingress_event_ids.clone(),
                coalesced,
            )
            .await?;
        let (receipt, _) = self
            .apply_with_ingress_outcomes(
                &envelope,
                &target,
                &decision,
                ingress_message_ids,
                Some(&ingress_source_stream),
                &ingress_event_ids,
                Some(admission_event.id),
            )
            .await?;
        Ok(receipt)
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
        if let Some(agent_ref) = envelope.metadata.get(ROUTE_AGENT_REF_METADATA) {
            target
                .metadata
                .insert(ROUTE_AGENT_REF_METADATA.to_string(), agent_ref.clone());
        }
        Ok(target)
    }

    fn ingress_event_store(&self) -> IoResult<SqliteSessionStore> {
        let store_path = self.session_store_path.as_ref().ok_or_else(|| {
            IoError::Bridge("durable ingress outcomes require a daemon session store".to_string())
        })?;
        SqliteSessionStore::open(store_path).map_err(cooldis_history_error)
    }

    async fn resolved_target_coordinates(
        &self,
        target: &ResolvedIoTarget,
    ) -> IoResult<Option<ThreadCoordinates>> {
        if let Some(thread_id) = target.address.thread_id.as_deref() {
            return Ok(Some(ThreadCoordinates {
                tenant_id: target.address.tenant_id.clone(),
                user_id: target.address.user_id.clone(),
                session_id: target.address.session_id.clone(),
                thread_id: ThreadId::parse_str(thread_id).map_err(|err| {
                    IoError::Bridge(format!("invalid resolved ingress thread id: {err}"))
                })?,
            }));
        }
        Ok(self
            .threads
            .lock()
            .await
            .get(&target.address.scope_key())
            .cloned())
    }

    async fn ingress_outcome(
        &self,
        target: &ResolvedIoTarget,
        ingress_envelope_ids: &[String],
    ) -> IoResult<IngressOutcomeState> {
        if ingress_envelope_ids.is_empty() {
            return Ok(IngressOutcomeState::Missing);
        }
        let Some(coordinates) = self.resolved_target_coordinates(target).await? else {
            return Ok(IngressOutcomeState::Missing);
        };
        let store = self.ingress_event_store()?;
        let events = store
            .read_events(&control_stream_id(&coordinates), None)
            .await
            .map_err(cooldis_history_error)?;
        ingress_outcome_fold(&events, ingress_envelope_ids)
    }

    async fn append_ingress_claim(
        &self,
        coordinates: &ThreadCoordinates,
        ingress_envelope_ids: &[String],
        ingress_witness_event_ids: &[EventRecordId],
        admission_event_id: EventRecordId,
        intent: IngressOutcomeIntent,
    ) -> IoResult<IngressClaimAppend> {
        let store = self.ingress_event_store()?;
        let stream_id = control_stream_id(coordinates);
        loop {
            let events = store
                .read_events(&stream_id, None)
                .await
                .map_err(cooldis_history_error)?;
            match ingress_outcome_fold(&events, ingress_envelope_ids)? {
                IngressOutcomeState::Missing => {}
                state => return Ok(IngressClaimAppend::Existing(state)),
            }
            let expected_next_sequence = events
                .last()
                .map(|event| EventSequence::new(event.sequence.get() + 1))
                .unwrap_or_else(|| EventSequence::new(1));
            let payload = IoIngressClaimedPayload {
                ingress_envelope_ids: ingress_envelope_ids.to_vec(),
                ingress_witness_event_ids: ingress_witness_event_ids.to_vec(),
                admission_event_id,
                intent: intent.clone(),
            };
            let claim = NewEventRecord::discharged(
                coordinates.clone(),
                EventKind::IoIngressClaimed,
                serde_json::to_value(payload).map_err(|err| {
                    IoError::Bridge(format!("encode io.ingress.claimed payload: {err}"))
                })?,
                ingress_claim_provenance(&stream_id, ingress_witness_event_ids, admission_event_id),
            );
            match store
                .append_events_fenced(&stream_id, expected_next_sequence, vec![claim])
                .await
            {
                Ok(mut appended) => {
                    let claim = appended.pop().ok_or_else(|| {
                        IoError::Bridge("ingress claim append returned no record".to_string())
                    })?;
                    #[cfg(test)]
                    if self
                        .pause_after_ingress_claim
                        .load(std::sync::atomic::Ordering::SeqCst)
                    {
                        self.ingress_claim_paused.notify_waiters();
                        std::future::pending::<()>().await;
                    }
                    return Ok(IngressClaimAppend::Appended(claim));
                }
                Err(HistoryError::AppendFenceConflict { .. }) => continue,
                Err(err) => return Err(cooldis_history_error(err)),
            }
        }
    }

    async fn append_ingress_settle(
        &self,
        coordinates: &ThreadCoordinates,
        claim: &EventRecord,
        claim_payload: &IoIngressClaimedPayload,
        evidence_event_id: Option<EventRecordId>,
        settled_by: IngressSettledBy,
    ) -> IoResult<EventRecord> {
        let store = self.ingress_event_store()?;
        let stream_id = control_stream_id(coordinates);
        loop {
            let events = store
                .read_events(&stream_id, None)
                .await
                .map_err(cooldis_history_error)?;
            match ingress_outcome_fold(&events, &claim_payload.ingress_envelope_ids)? {
                IngressOutcomeState::Settled { settle, .. } => return Ok(settle),
                IngressOutcomeState::Claimed {
                    claim: existing, ..
                } if existing.id == claim.id => {}
                IngressOutcomeState::Claimed { .. } | IngressOutcomeState::Missing => {
                    return Err(IoError::Bridge(
                        "ingress settle no longer matches the active claim".to_string(),
                    ));
                }
            }
            let expected_next_sequence = events
                .last()
                .map(|event| EventSequence::new(event.sequence.get() + 1))
                .unwrap_or_else(|| EventSequence::new(1));
            let payload = IoIngressSettledPayload {
                claim_event_id: claim.id,
                ingress_envelope_ids: claim_payload.ingress_envelope_ids.clone(),
                evidence_event_id,
                settled_by,
            };
            let settle = NewEventRecord::discharged(
                coordinates.clone(),
                EventKind::IoIngressSettled,
                serde_json::to_value(payload).map_err(|err| {
                    IoError::Bridge(format!("encode io.ingress.settled payload: {err}"))
                })?,
                ingress_settle_provenance(&stream_id, coordinates, claim.id, evidence_event_id),
            );
            match store
                .append_events_fenced(&stream_id, expected_next_sequence, vec![settle])
                .await
            {
                Ok(mut appended) => {
                    return appended.pop().ok_or_else(|| {
                        IoError::Bridge("ingress settle append returned no record".to_string())
                    });
                }
                Err(HistoryError::AppendFenceConflict { .. }) => continue,
                Err(err) => return Err(cooldis_history_error(err)),
            }
        }
    }

    async fn wait_for_turn_execution_evidence(
        &self,
        coordinates: &ThreadCoordinates,
        turn_id: &str,
        submission_mode: TurnSubmissionMode,
    ) -> IoResult<EventRecord> {
        let store = self.ingress_event_store()?;
        let stream_id = EventStreamId::for_thread(coordinates);
        tokio::time::timeout(Duration::from_secs(30), async {
            let mut next_sequence = EventSequence::new(1);
            loop {
                let events = store
                    .read_events(&stream_id, Some(next_sequence))
                    .await
                    .map_err(cooldis_history_error)?;
                if let Some(evidence) = turn_execution_evidence(&events, turn_id, submission_mode) {
                    return Ok(evidence);
                }
                if let Some(last) = events.last() {
                    next_sequence = EventSequence::new(last.sequence.get() + 1);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| {
            IoError::Bridge(format!(
                "timed out waiting for execution evidence for ingress turn {turn_id}"
            ))
        })?
    }

    async fn ingress_state(&self, target: &ResolvedIoTarget) -> IoResult<IngressState> {
        let active_turn_id = self
            .lock_active_turns()
            .get(&target.address.scope_key())
            .cloned();
        Ok(IngressState {
            active_turn_id,
            pending_count: 0,
            dedupe_seen: false,
            metadata: target.metadata.clone(),
        })
    }

    async fn decide(
        &self,
        envelope: &IngressEnvelope,
        target: &ResolvedIoTarget,
        state: &IngressState,
    ) -> IoResult<AdmissionDecision> {
        let input = IoTurnInput::from_envelope(envelope, target);
        let turn_id = format!("turn-{}", uuid::Uuid::now_v7());
        let policy = envelope
            .metadata
            .get("cooldis_route_policy")
            .map(String::as_str)
            .unwrap_or("queue_per_conversation");
        match policy {
            "queue_per_conversation" | "coalesce_bursts" => {
                Ok(AdmissionDecision::queue(turn_id, input))
            }
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
            "fork" | "fork_on_new_dm" => Ok(AdmissionDecision::Fork {
                child_key: turn_id,
                input,
            }),
            other => Err(IoError::Bridge(format!("unknown route policy {other:?}"))),
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
        ingress_message_id: Option<&str>,
    ) -> IoResult<crate::EventRecord> {
        let handle = self
            .supervisor
            .get_thread_at(coordinates)
            .await
            .map_err(cooldis_bridge_error)?;
        handle
            .append_control_event(ingress_received_control_record(
                coordinates,
                envelope,
                ingress_message_id,
            )?)
            .await
            .map_err(cooldis_bridge_error)
    }

    async fn record_admission_decided(
        &self,
        coordinates: &ThreadCoordinates,
        envelope: &IngressEnvelope,
        decision: &AdmissionDecision,
        policy_hash: &str,
        ingress_event_ids: Vec<crate::EventRecordId>,
        coalesced: bool,
    ) -> IoResult<crate::EventRecord> {
        let handle = self
            .supervisor
            .get_thread_at(coordinates)
            .await
            .map_err(cooldis_bridge_error)?;
        let route_id = route_id_for_envelope(envelope);
        let context = AdmissionGateContext::route_policy(
            route_id,
            policy_hash.to_string(),
            if coalesced {
                EventAdmissionDecision::Coalesce
            } else {
                event_admission_decision(decision)
            },
            admissible_decisions_for_envelope(envelope),
            ingress_event_ids,
        );
        append_admission_decided(&handle, context)
            .await
            .map_err(cooldis_bridge_error)
    }

    async fn ensure_thread(
        &self,
        target: &ResolvedIoTarget,
        envelope: &IngressEnvelope,
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
        let durable_binding = self.durable_ingress_binding(envelope).await?;
        let scope_lock = self.thread_scope_lock(&scope_key).await;
        let _scope_guard = scope_lock.lock().await;

        let existing_coordinates = {
            let threads = self.threads.lock().await;
            threads.get(&scope_key).cloned()
        };
        if let Some(coordinates) = existing_coordinates {
            match self.get_or_load_thread_handle(&coordinates).await {
                Ok(handle) => return Ok((coordinates, handle)),
                Err(ThreadHandleResolutionError::LifecycleLoad(_)) => {
                    let mut threads = self.threads.lock().await;
                    if threads.get(&scope_key) == Some(&coordinates) {
                        threads.remove(&scope_key);
                    }
                }
                Err(ThreadHandleResolutionError::Lookup(err)) => {
                    return Err(cooldis_bridge_error(err));
                }
            }
        }

        let topology = target
            .parent_thread_id
            .as_deref()
            .map(ThreadId::parse_str)
            .transpose()
            .map_err(|err| IoError::Bridge(format!("invalid parent thread id: {err}")))?
            .map(ThreadTopology::spawned_from)
            .unwrap_or_else(ThreadTopology::root);
        let agent_binding = self.route_agent_binding(target).await?;
        let metadata = agent_binding
            .as_ref()
            .map(|binding| binding.metadata.clone())
            .unwrap_or_default();

        let handle = self
            .supervisor
            .start_thread(ThreadStartRequest {
                tenant_id: target.address.tenant_id.clone(),
                user_id: target.address.user_id.clone(),
                session_id: target.address.session_id.clone(),
                topology,
                metadata,
            })
            .await
            .map_err(cooldis_bridge_error)?;
        if let Some(binding) = agent_binding
            && let Err(err) = handle
                .record_manifest_receipts(binding.compile_receipt, binding.bind_receipt)
                .await
        {
            let _ = self
                .supervisor
                .shutdown_thread_at(&handle.context().coordinates)
                .await;
            return Err(cooldis_bridge_error(err));
        }
        let coordinates = handle.context().coordinates.clone();
        if let Some((route_id, source_scope, state)) = durable_binding {
            if let Err(err) = state.bind_thread(
                &route_id,
                &source_scope,
                &target.address.scope_key(),
                &coordinates,
            ) {
                let _ = self.supervisor.shutdown_thread_at(&coordinates).await;
                return Err(err);
            }
            pause_after_ingress_binding_for_restart_smoke().await?;
        }
        self.threads
            .lock()
            .await
            .insert(scope_key, coordinates.clone());
        Ok((coordinates, handle))
    }

    async fn thread_scope_lock(&self, scope_key: &str) -> Arc<Mutex<()>> {
        let mut locks = self.thread_scope_locks.lock().await;
        locks
            .entry(scope_key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    fn lock_active_turns(&self) -> std::sync::MutexGuard<'_, HashMap<String, String>> {
        self.active_turns
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn clear_active_turn_if_matches(&self, scope_key: &str, completed_turn_id: &str) {
        let mut active_turns = self.lock_active_turns();
        if active_turns
            .get(scope_key)
            .is_some_and(|active_turn_id| active_turn_id == completed_turn_id)
        {
            active_turns.remove(scope_key);
        }
    }

    async fn durable_ingress_binding(
        &self,
        envelope: &IngressEnvelope,
    ) -> IoResult<Option<(String, String, Arc<DaemonEgressState>)>> {
        let threading = envelope
            .metadata
            .get("cooldis_route_threading")
            .map(String::as_str)
            .unwrap_or("per_conversation");
        if threading != "per_conversation" {
            return Ok(None);
        }

        let route_id = route_id_for_ingress(envelope);
        let source_scope = source_scope(&envelope.source.protocol, &route_id);
        let route_requires_state = self
            .egress_route_configs
            .read()
            .await
            .get(&source_scope)
            .is_some_and(RouteEgressConfig::restores_per_conversation_bindings);
        let state = self.egress_states.read().await.get(&source_scope).cloned();
        match state {
            Some(state) => Ok(Some((route_id, source_scope, state))),
            None if route_requires_state => Err(IoError::Bridge(format!(
                "durable route state for {source_scope:?} is not ready"
            ))),
            None => Ok(None),
        }
    }

    async fn route_agent_binding(
        &self,
        target: &ResolvedIoTarget,
    ) -> IoResult<Option<KernelThreadSpawnAgentBinding>> {
        let Some(agent_ref) = target
            .metadata
            .get(ROUTE_AGENT_REF_METADATA)
            .filter(|agent_ref| !agent_ref.trim().is_empty())
        else {
            return Ok(None);
        };
        let app_server = self.app_server.as_ref().ok_or_else(|| {
            IoError::Bridge(
                "daemon route agent_ref requires daemon IO to be backed by an app-server"
                    .to_string(),
            )
        })?;
        app_server
            .bind_daemon_route_agent(agent_ref)
            .await
            .map(Some)
            .map_err(cooldis_bridge_error)
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

    async fn apply_fork_admission(
        &self,
        envelope: &IngressEnvelope,
        target: &ResolvedIoTarget,
        child_key: &str,
        input: &IoTurnInput,
        _ingress_message_ids: &[String],
        ingress_source_stream: Option<&EventStreamId>,
        source_ingress_event_ids: &[EventRecordId],
    ) -> IoResult<(KernelIoReceipt, Option<String>)> {
        let (parent_coordinates, parent_handle) = self.ensure_thread(target, envelope).await?;
        let checkpoint = self
            .supervisor
            .create_checkpoint_at(
                &parent_coordinates,
                None,
                Some("daemon-io-fork".to_string()),
                parent_handle.context().metadata.clone(),
            )
            .await
            .map_err(cooldis_bridge_error)?;
        let source_cut = fork_source_cut_payload(&parent_coordinates, &checkpoint, None);
        let child_handle = self
            .supervisor
            .fork_thread_from_checkpoint_at(checkpoint)
            .await
            .map_err(cooldis_bridge_error)?;
        let child_coordinates = child_handle.context().coordinates.clone();
        self.append_fork_thread_spawned_event(
            &parent_handle,
            &parent_coordinates,
            &child_handle,
            &source_cut,
        )
        .await?;

        let scope_key = target.address.scope_key();
        let scope_lock = self.thread_scope_lock(&scope_key).await;
        let scope_guard = scope_lock.lock().await;
        if let Err(err) = self
            .bind_egress_thread(envelope, target, &child_coordinates)
            .await
        {
            drop(scope_guard);
            let _ = self.supervisor.shutdown_thread_at(&child_coordinates).await;
            return Err(err);
        }
        self.threads
            .lock()
            .await
            .insert(scope_key, child_coordinates.clone());
        drop(scope_guard);
        self.append_ingress_turn_submitted_event(
            &child_handle,
            envelope,
            target,
            child_key,
            ingress_source_stream,
            source_ingress_event_ids,
        )
        .await?;
        self.lock_active_turns()
            .insert(target.address.scope_key(), child_key.to_string());
        if let Err(err) = self
            .supervisor
            .submit_turn_to_with_admission(
                &child_coordinates,
                child_key.to_string(),
                self.runtime_input(input),
                TurnSubmissionMode::Queue,
                None,
            )
            .await
        {
            self.clear_active_turn_if_matches(&target.address.scope_key(), child_key);
            return Err(cooldis_bridge_error(err));
        }

        let mut receipt_target = target.clone();
        receipt_target.address.thread_id = Some(child_coordinates.thread_id.to_string());
        let mut receipt = KernelIoReceipt::new(
            envelope,
            receipt_target,
            &AdmissionDecision::Fork {
                child_key: child_key.to_string(),
                input: input.clone(),
            },
        );
        receipt.thread_id = Some(child_coordinates.thread_id.to_string());
        Ok((receipt, None))
    }

    async fn append_fork_thread_spawned_event(
        &self,
        parent_handle: &RuntimeThreadHandle,
        parent_coordinates: &ThreadCoordinates,
        child_handle: &RuntimeThreadHandle,
        source_cut: &ThreadSpawnedForkSourceCutPayload,
    ) -> IoResult<EventRecord> {
        let child_context = child_handle.context();
        let metadata = &child_context.metadata;
        let child_manifest_hash = metadata
            .get(THREAD_AGENT_MANIFEST_HASH_METADATA)
            .cloned()
            .unwrap_or_else(|| "unbound".to_string());
        let granted = metadata
            .get(THREAD_SPAWN_GRANTED_METADATA)
            .map(|raw| {
                serde_json::from_str::<Vec<String>>(raw).map_err(|err| {
                    IoError::Bridge(format!("thread.spawned granted metadata is invalid: {err}"))
                })
            })
            .transpose()?
            .unwrap_or_default();
        let fork = ThreadSpawnedForkPayload {
            mode: "clone".to_string(),
            source_cut: source_cut.clone(),
        };
        let inputs_context = json!({
            "operation": "thread/fork",
            "fork": &fork,
        });
        let inputs_hash = canonical_json_hash(&inputs_context).map_err(cooldis_bridge_error)?;
        let payload = ThreadSpawnedPayload {
            parent_thread_id: parent_coordinates.thread_id,
            parent_turn_id: None,
            child_thread_id: child_context.coordinates.thread_id,
            child_manifest_hash,
            child_policy_hash: None,
            granted,
            inputs_hash,
            fork: Some(fork),
        };
        let mut value = serde_json::to_value(payload).map_err(|err| {
            IoError::Bridge(format!("thread.spawned payload codec failed: {err}"))
        })?;
        let object = value.as_object_mut().ok_or_else(|| {
            IoError::Bridge("thread.spawned payload did not encode as object".to_string())
        })?;
        object.insert(
            "schema".to_string(),
            json!(EventKind::ThreadSpawned.payload_schema_id()),
        );
        parent_handle
            .append_control_event(NewEventRecord::witnessed(
                parent_coordinates.clone(),
                EventKind::ThreadSpawned,
                value,
            ))
            .await
            .map_err(cooldis_bridge_error)
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

    async fn append_ingress_turn_submitted_event(
        &self,
        handle: &RuntimeThreadHandle,
        envelope: &IngressEnvelope,
        _target: &ResolvedIoTarget,
        turn_id: &str,
        ingress_source_stream: Option<&EventStreamId>,
        source_ingress_event_ids: &[EventRecordId],
    ) -> IoResult<EventRecord> {
        if let Some(existing) = handle
            .read_thread_events(None)
            .await
            .map_err(cooldis_bridge_error)?
            .into_iter()
            .find(|event| {
                event.kind == EventKind::TurnSubmitted
                    && event.payload.get("turn_id").and_then(Value::as_str) == Some(turn_id)
                    && event
                        .payload
                        .get("ingress_envelope_id")
                        .and_then(Value::as_str)
                        == Some(envelope.id.as_str())
            })
        {
            return Ok(existing);
        }
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
            "schema".to_string(),
            json!(EventKind::TurnSubmitted.payload_schema_id()),
        );
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
        if !source_ingress_event_ids.is_empty() && ingress_source_stream.is_none() {
            return Err(IoError::Bridge(
                "derived ingress turn submission requires its control source stream".to_string(),
            ));
        }

        let record = || {
            if source_ingress_event_ids.is_empty() {
                return NewEventRecord::witnessed(
                    handle.context().coordinates.clone(),
                    EventKind::TurnSubmitted,
                    payload.clone(),
                );
            }
            NewEventRecord::discharged(
                handle.context().coordinates.clone(),
                EventKind::TurnSubmitted,
                payload.clone(),
                EventProvenance {
                    source_streams: ingress_source_stream.cloned().into_iter().collect(),
                    source_event_ids: source_ingress_event_ids.to_vec(),
                    discharged_by: Some("projector:io-ingress-apply".to_string()),
                    function: Some("ingress_turn_submit/v1".to_string()),
                    ..EventProvenance::default()
                },
            )
        };
        handle
            .append_thread_event_record(record())
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

            if event.kind == EventKind::IoEgressRequested {
                let after_cursor = cursor.is_none() || after_cursor_ids.contains(&event.id);
                if !after_cursor {
                    continue;
                }
                let envelope = match requested_egress_from_event(event, &all_events) {
                    Ok(Some(envelope)) => envelope,
                    Ok(None) => {
                        state.store_cursor(&binding.route_id, &thread_id, &event.cursor_v1())?;
                        continue;
                    }
                    Err(err) => {
                        eprintln!(
                            "cooldis egress projector skipped invalid io.egress.requested event {}: {err}",
                            event.id
                        );
                        state.store_cursor(&binding.route_id, &thread_id, &event.cursor_v1())?;
                        continue;
                    }
                };
                let outcome = self
                    .deliver_projected_envelope(
                        state,
                        binding,
                        &handle,
                        adapter,
                        event,
                        0,
                        envelope,
                        route_config.retry,
                        &mut receipt_cursors,
                    )
                    .await?;
                match outcome {
                    EnvelopeDeliveryOutcome::Delivered(cursor) => {
                        state.store_cursor(&binding.route_id, &thread_id, &cursor)?;
                        delivered_sources += 1;
                    }
                    EnvelopeDeliveryOutcome::Blocked => break,
                }
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
            let completed_turn_id = source_context.turn_id.clone();
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
                    if let Some(completed_turn_id) = completed_turn_id {
                        self.clear_active_turn_if_matches(&binding.scope_key, &completed_turn_id);
                    }
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

impl CooldisDaemonIoBridge {
    fn ingress_claim_intent(decision: &AdmissionDecision) -> IoResult<IngressOutcomeIntent> {
        let input_digest = |input: &IoTurnInput| {
            serde_json::to_value(input)
                .map_err(|err| IoError::Bridge(format!("encode ingress turn input: {err}")))
                .and_then(|value| canonical_json_hash(&value).map_err(cooldis_bridge_error))
        };
        match decision {
            AdmissionDecision::Queue { turn_id, input } => Ok(IngressOutcomeIntent::Turn {
                turn_id: turn_id.clone(),
                submission_mode: "queue".to_string(),
                input_digest: input_digest(input)?,
            }),
            AdmissionDecision::Steer { turn_id, input, .. } => Ok(IngressOutcomeIntent::Turn {
                turn_id: turn_id.clone(),
                submission_mode: "steer".to_string(),
                input_digest: input_digest(input)?,
            }),
            AdmissionDecision::Interrupt {
                reason,
                replacement_turn_id,
                replacement,
            } => Ok(IngressOutcomeIntent::Interrupt {
                replacement_turn_id: replacement_turn_id.clone(),
                cancel_reason: reason.clone(),
                input_digest: match replacement {
                    Some(input) => input_digest(input)?,
                    None => canonical_json_hash(&JsonValue::Null).map_err(cooldis_bridge_error)?,
                },
            }),
            AdmissionDecision::Fork { child_key, input } => Ok(IngressOutcomeIntent::Fork {
                child_key: child_key.clone(),
                input_digest: input_digest(input)?,
            }),
            AdmissionDecision::ObserveOnly { reason } => Ok(IngressOutcomeIntent::Observe {
                reason: reason.clone(),
            }),
            AdmissionDecision::Reject { reason, .. } => Ok(IngressOutcomeIntent::Reject {
                reason: reason.clone(),
            }),
        }
    }

    fn claimed_decision(
        envelope: &IngressEnvelope,
        target: &ResolvedIoTarget,
        intent: &IngressOutcomeIntent,
    ) -> AdmissionDecision {
        let input = || IoTurnInput::from_envelope(envelope, target);
        match intent {
            IngressOutcomeIntent::Turn {
                turn_id,
                submission_mode,
                ..
            } if submission_mode == "steer" => {
                AdmissionDecision::steer(turn_id.clone(), None, input())
            }
            IngressOutcomeIntent::Turn { turn_id, .. } => {
                AdmissionDecision::queue(turn_id.clone(), input())
            }
            IngressOutcomeIntent::Interrupt {
                replacement_turn_id,
                cancel_reason,
                ..
            } => AdmissionDecision::Interrupt {
                reason: cancel_reason.clone(),
                replacement_turn_id: replacement_turn_id.clone(),
                replacement: replacement_turn_id.as_ref().map(|_| input()),
            },
            IngressOutcomeIntent::Fork { child_key, .. } => AdmissionDecision::Fork {
                child_key: child_key.clone(),
                input: input(),
            },
            IngressOutcomeIntent::Observe { reason } => AdmissionDecision::ObserveOnly {
                reason: reason.clone(),
            },
            IngressOutcomeIntent::Reject { reason } => AdmissionDecision::reject(reason.clone()),
        }
    }

    async fn complete_claimed_turn(
        &self,
        envelope: &IngressEnvelope,
        target: &ResolvedIoTarget,
        coordinates: &ThreadCoordinates,
        handle: &RuntimeThreadHandle,
        decision: &AdmissionDecision,
        claim: &EventRecord,
        claim_payload: &IoIngressClaimedPayload,
        turn_id: &str,
        submission_mode: TurnSubmissionMode,
        reserved: ReservedTurnSubmission,
        ingress_source_stream: &EventStreamId,
        settled_by: IngressSettledBy,
    ) -> IoResult<KernelIoReceipt> {
        self.bind_egress_thread(envelope, target, coordinates)
            .await?;
        self.append_ingress_turn_submitted_event(
            handle,
            envelope,
            target,
            turn_id,
            Some(ingress_source_stream),
            &claim_payload.ingress_witness_event_ids,
        )
        .await?;
        self.lock_active_turns()
            .insert(target.address.scope_key(), turn_id.to_string());
        reserved.submit().await;
        let evidence = self
            .wait_for_turn_execution_evidence(coordinates, turn_id, submission_mode)
            .await?;
        self.append_ingress_settle(
            coordinates,
            claim,
            claim_payload,
            Some(evidence.id),
            settled_by,
        )
        .await?;
        let mut receipt = KernelIoReceipt::new(envelope, target.clone(), decision);
        receipt.thread_id = Some(coordinates.thread_id.to_string());
        Ok(receipt)
    }

    async fn recover_ingress_outcome(
        &self,
        envelope: &IngressEnvelope,
        target: &ResolvedIoTarget,
        state: IngressOutcomeState,
    ) -> IoResult<KernelIoReceipt> {
        let IngressOutcomeState::Claimed { claim, payload } = state else {
            return Ok(deduplicated_ingress_receipt(
                envelope,
                target.clone(),
                &state,
            ));
        };
        let coordinates = self
            .resolved_target_coordinates(target)
            .await?
            .ok_or_else(|| {
                IoError::Bridge(
                    "claimed ingress outcome has no resolved control stream".to_string(),
                )
            })?;
        let handle = self
            .get_or_load_thread_handle(&coordinates)
            .await
            .map_err(|err| cooldis_bridge_error(err.into_inner()))?;
        let decision = Self::claimed_decision(envelope, target, &payload.intent);
        let source_stream = control_stream_id(&coordinates);
        match &payload.intent {
            IngressOutcomeIntent::Turn {
                turn_id,
                submission_mode,
                input_digest,
            } => {
                let input = IoTurnInput::from_envelope(envelope, target);
                let actual_digest =
                    canonical_json_hash(&serde_json::to_value(&input).map_err(|err| {
                        IoError::Bridge(format!("encode recovered ingress input: {err}"))
                    })?)
                    .map_err(cooldis_bridge_error)?;
                if &actual_digest != input_digest {
                    return Err(IoError::Bridge(
                        "recovered ingress input does not match the claimed digest".to_string(),
                    ));
                }
                let mode = match submission_mode.as_str() {
                    "queue" => TurnSubmissionMode::Queue,
                    "steer" => TurnSubmissionMode::Steer,
                    other => {
                        return Err(IoError::Bridge(format!(
                            "claimed ingress turn has unknown submission mode {other:?}"
                        )));
                    }
                };
                let thread_events = handle
                    .read_thread_events(None)
                    .await
                    .map_err(cooldis_bridge_error)?;
                if let Some(evidence) = turn_execution_evidence(&thread_events, turn_id, mode) {
                    self.append_ingress_settle(
                        &coordinates,
                        &claim,
                        &payload,
                        Some(evidence.id),
                        IngressSettledBy::Recovery,
                    )
                    .await?;
                    let mut receipt = KernelIoReceipt::new(envelope, target.clone(), &decision);
                    receipt.thread_id = Some(coordinates.thread_id.to_string());
                    return Ok(receipt);
                }
                let reserved = self
                    .supervisor
                    .reserve_turn_to_with_admission(
                        &coordinates,
                        turn_id.clone(),
                        self.runtime_input(&input),
                        mode,
                        None,
                    )
                    .await
                    .map_err(cooldis_bridge_error)?;
                self.complete_claimed_turn(
                    envelope,
                    target,
                    &coordinates,
                    &handle,
                    &decision,
                    &claim,
                    &payload,
                    turn_id,
                    mode,
                    reserved,
                    &source_stream,
                    IngressSettledBy::Recovery,
                )
                .await
            }
            IngressOutcomeIntent::Interrupt {
                replacement_turn_id,
                cancel_reason,
                input_digest,
            } => {
                self.supervisor
                    .cancel_at(&coordinates, cancel_reason.clone())
                    .await
                    .map_err(cooldis_bridge_error)?;
                let Some(turn_id) = replacement_turn_id else {
                    self.append_ingress_settle(
                        &coordinates,
                        &claim,
                        &payload,
                        None,
                        IngressSettledBy::Recovery,
                    )
                    .await?;
                    let mut receipt = KernelIoReceipt::new(envelope, target.clone(), &decision);
                    receipt.thread_id = Some(coordinates.thread_id.to_string());
                    return Ok(receipt);
                };
                let input = IoTurnInput::from_envelope(envelope, target);
                let actual_digest =
                    canonical_json_hash(&serde_json::to_value(&input).map_err(|err| {
                        IoError::Bridge(format!("encode recovered interrupt input: {err}"))
                    })?)
                    .map_err(cooldis_bridge_error)?;
                if &actual_digest != input_digest {
                    return Err(IoError::Bridge(
                        "recovered interrupt input does not match the claimed digest".to_string(),
                    ));
                }
                let thread_events = handle
                    .read_thread_events(None)
                    .await
                    .map_err(cooldis_bridge_error)?;
                if let Some(evidence) =
                    turn_execution_evidence(&thread_events, turn_id, TurnSubmissionMode::Interrupt)
                {
                    self.append_ingress_settle(
                        &coordinates,
                        &claim,
                        &payload,
                        Some(evidence.id),
                        IngressSettledBy::Recovery,
                    )
                    .await?;
                    let mut receipt = KernelIoReceipt::new(envelope, target.clone(), &decision);
                    receipt.thread_id = Some(coordinates.thread_id.to_string());
                    return Ok(receipt);
                }
                let reserved = self
                    .supervisor
                    .reserve_turn_to_with_admission(
                        &coordinates,
                        turn_id.clone(),
                        self.runtime_input(&input),
                        TurnSubmissionMode::Interrupt,
                        None,
                    )
                    .await
                    .map_err(cooldis_bridge_error)?;
                self.complete_claimed_turn(
                    envelope,
                    target,
                    &coordinates,
                    &handle,
                    &decision,
                    &claim,
                    &payload,
                    turn_id,
                    TurnSubmissionMode::Interrupt,
                    reserved,
                    &source_stream,
                    IngressSettledBy::Recovery,
                )
                .await
            }
            IngressOutcomeIntent::Observe { .. } | IngressOutcomeIntent::Reject { .. } => {
                Err(IoError::Bridge(
                    "effect-free ingress claim is missing its atomic settle".to_string(),
                ))
            }
            IngressOutcomeIntent::Fork { .. } => Err(IoError::Bridge(
                "fork ingress claim recovery belongs to EMO-384".to_string(),
            )),
        }
    }

    async fn apply_with_ingress_outcomes(
        &self,
        envelope: &IngressEnvelope,
        target: &ResolvedIoTarget,
        decision: &AdmissionDecision,
        ingress_message_ids: &[String],
        ingress_source_stream: Option<&EventStreamId>,
        source_ingress_event_ids: &[EventRecordId],
        admission_event_id: Option<EventRecordId>,
    ) -> IoResult<(KernelIoReceipt, Option<String>)> {
        match decision {
            AdmissionDecision::Queue { turn_id, input } => {
                let (coordinates, handle) = self.ensure_thread(target, envelope).await?;
                let reserved = self
                    .supervisor
                    .reserve_turn_to_with_admission(
                        &coordinates,
                        turn_id.clone(),
                        self.runtime_input(input),
                        TurnSubmissionMode::Queue,
                        None,
                    )
                    .await
                    .map_err(cooldis_bridge_error)?;
                if ingress_message_ids.is_empty() {
                    self.bind_egress_thread(envelope, target, &coordinates)
                        .await?;
                    self.append_ingress_turn_submitted_event(
                        &handle,
                        envelope,
                        target,
                        turn_id,
                        ingress_source_stream,
                        source_ingress_event_ids,
                    )
                    .await?;
                    self.lock_active_turns()
                        .insert(target.address.scope_key(), turn_id.clone());
                    reserved.submit().await;
                    let mut receipt = KernelIoReceipt::new(envelope, target.clone(), decision);
                    receipt.thread_id = Some(coordinates.thread_id.to_string());
                    return Ok((receipt, None));
                }
                let admission_event_id = admission_event_id.ok_or_else(|| {
                    IoError::Bridge("durable ingress claim requires admission evidence".to_string())
                })?;
                let source_stream = ingress_source_stream.ok_or_else(|| {
                    IoError::Bridge("durable ingress claim requires its control stream".to_string())
                })?;
                let claim = self
                    .append_ingress_claim(
                        &coordinates,
                        ingress_message_ids,
                        source_ingress_event_ids,
                        admission_event_id,
                        Self::ingress_claim_intent(decision)?,
                    )
                    .await?;
                let IngressClaimAppend::Appended(claim) = claim else {
                    let IngressClaimAppend::Existing(state) = claim else {
                        unreachable!()
                    };
                    let settled_turn_id = ingress_outcome_turn_id(&state).map(ToOwned::to_owned);
                    let receipt = match state {
                        state @ IngressOutcomeState::Claimed { .. } => {
                            self.recover_ingress_outcome(envelope, target, state)
                                .await?
                        }
                        state => deduplicated_ingress_receipt(envelope, target.clone(), &state),
                    };
                    return Ok((receipt, settled_turn_id));
                };
                let claim_payload =
                    serde_json::from_value::<IoIngressClaimedPayload>(claim.payload.clone())
                        .map_err(|err| {
                            IoError::Bridge(format!("decode appended ingress claim: {err}"))
                        })?;
                let receipt = self
                    .complete_claimed_turn(
                        envelope,
                        target,
                        &coordinates,
                        &handle,
                        decision,
                        &claim,
                        &claim_payload,
                        turn_id,
                        TurnSubmissionMode::Queue,
                        reserved,
                        source_stream,
                        IngressSettledBy::Execution,
                    )
                    .await?;
                Ok((receipt, None))
            }
            AdmissionDecision::Steer { turn_id, input, .. } => {
                let (coordinates, handle) = self.ensure_thread(target, envelope).await?;
                let reserved = self
                    .supervisor
                    .reserve_turn_to_with_admission(
                        &coordinates,
                        turn_id.clone(),
                        self.runtime_input(input),
                        TurnSubmissionMode::Steer,
                        None,
                    )
                    .await
                    .map_err(cooldis_bridge_error)?;
                if ingress_message_ids.is_empty() {
                    self.bind_egress_thread(envelope, target, &coordinates)
                        .await?;
                    self.append_ingress_turn_submitted_event(
                        &handle,
                        envelope,
                        target,
                        turn_id,
                        ingress_source_stream,
                        source_ingress_event_ids,
                    )
                    .await?;
                    self.lock_active_turns()
                        .insert(target.address.scope_key(), turn_id.clone());
                    reserved.submit().await;
                    let mut receipt = KernelIoReceipt::new(envelope, target.clone(), decision);
                    receipt.thread_id = Some(coordinates.thread_id.to_string());
                    return Ok((receipt, None));
                }
                let admission_event_id = admission_event_id.ok_or_else(|| {
                    IoError::Bridge("durable ingress claim requires admission evidence".to_string())
                })?;
                let source_stream = ingress_source_stream.ok_or_else(|| {
                    IoError::Bridge("durable ingress claim requires its control stream".to_string())
                })?;
                let claim = self
                    .append_ingress_claim(
                        &coordinates,
                        ingress_message_ids,
                        source_ingress_event_ids,
                        admission_event_id,
                        Self::ingress_claim_intent(decision)?,
                    )
                    .await?;
                let IngressClaimAppend::Appended(claim) = claim else {
                    let IngressClaimAppend::Existing(state) = claim else {
                        unreachable!()
                    };
                    let settled_turn_id = ingress_outcome_turn_id(&state).map(ToOwned::to_owned);
                    let receipt = match state {
                        state @ IngressOutcomeState::Claimed { .. } => {
                            self.recover_ingress_outcome(envelope, target, state)
                                .await?
                        }
                        state => deduplicated_ingress_receipt(envelope, target.clone(), &state),
                    };
                    return Ok((receipt, settled_turn_id));
                };
                let claim_payload =
                    serde_json::from_value::<IoIngressClaimedPayload>(claim.payload.clone())
                        .map_err(|err| {
                            IoError::Bridge(format!("decode appended ingress claim: {err}"))
                        })?;
                let receipt = self
                    .complete_claimed_turn(
                        envelope,
                        target,
                        &coordinates,
                        &handle,
                        decision,
                        &claim,
                        &claim_payload,
                        turn_id,
                        TurnSubmissionMode::Steer,
                        reserved,
                        source_stream,
                        IngressSettledBy::Execution,
                    )
                    .await?;
                Ok((receipt, None))
            }
            AdmissionDecision::Interrupt {
                reason,
                replacement_turn_id,
                replacement,
            } => {
                let (coordinates, handle) = self.ensure_thread(target, envelope).await?;
                let reserved =
                    if let (Some(turn_id), Some(input)) = (replacement_turn_id, replacement) {
                        Some(
                            self.supervisor
                                .reserve_turn_to_with_admission(
                                    &coordinates,
                                    turn_id.clone(),
                                    self.runtime_input(input),
                                    TurnSubmissionMode::Interrupt,
                                    None,
                                )
                                .await
                                .map_err(cooldis_bridge_error)?,
                        )
                    } else {
                        None
                    };
                if ingress_message_ids.is_empty() {
                    self.supervisor
                        .cancel_at(&coordinates, reason.clone())
                        .await
                        .map_err(cooldis_bridge_error)?;
                    if let (Some(turn_id), Some(reserved)) = (replacement_turn_id, reserved) {
                        self.bind_egress_thread(envelope, target, &coordinates)
                            .await?;
                        self.append_ingress_turn_submitted_event(
                            &handle,
                            envelope,
                            target,
                            turn_id,
                            ingress_source_stream,
                            source_ingress_event_ids,
                        )
                        .await?;
                        self.lock_active_turns()
                            .insert(target.address.scope_key(), turn_id.clone());
                        reserved.submit().await;
                    }
                    let mut receipt = KernelIoReceipt::new(envelope, target.clone(), decision);
                    receipt.thread_id = Some(coordinates.thread_id.to_string());
                    return Ok((receipt, None));
                }
                let admission_event_id = admission_event_id.ok_or_else(|| {
                    IoError::Bridge("durable ingress claim requires admission evidence".to_string())
                })?;
                let source_stream = ingress_source_stream.ok_or_else(|| {
                    IoError::Bridge("durable ingress claim requires its control stream".to_string())
                })?;
                let claim = self
                    .append_ingress_claim(
                        &coordinates,
                        ingress_message_ids,
                        source_ingress_event_ids,
                        admission_event_id,
                        Self::ingress_claim_intent(decision)?,
                    )
                    .await?;
                let IngressClaimAppend::Appended(claim) = claim else {
                    let IngressClaimAppend::Existing(state) = claim else {
                        unreachable!()
                    };
                    let settled_turn_id = ingress_outcome_turn_id(&state).map(ToOwned::to_owned);
                    let receipt = match state {
                        state @ IngressOutcomeState::Claimed { .. } => {
                            self.recover_ingress_outcome(envelope, target, state)
                                .await?
                        }
                        state => deduplicated_ingress_receipt(envelope, target.clone(), &state),
                    };
                    return Ok((receipt, settled_turn_id));
                };
                let claim_payload =
                    serde_json::from_value::<IoIngressClaimedPayload>(claim.payload.clone())
                        .map_err(|err| {
                            IoError::Bridge(format!("decode appended ingress claim: {err}"))
                        })?;
                self.supervisor
                    .cancel_at(&coordinates, reason.clone())
                    .await
                    .map_err(cooldis_bridge_error)?;
                if let (Some(turn_id), Some(reserved)) = (replacement_turn_id, reserved) {
                    let receipt = self
                        .complete_claimed_turn(
                            envelope,
                            target,
                            &coordinates,
                            &handle,
                            decision,
                            &claim,
                            &claim_payload,
                            turn_id,
                            TurnSubmissionMode::Interrupt,
                            reserved,
                            source_stream,
                            IngressSettledBy::Execution,
                        )
                        .await?;
                    Ok((receipt, None))
                } else {
                    self.append_ingress_settle(
                        &coordinates,
                        &claim,
                        &claim_payload,
                        None,
                        IngressSettledBy::Execution,
                    )
                    .await?;
                    let mut receipt = KernelIoReceipt::new(envelope, target.clone(), decision);
                    receipt.thread_id = Some(coordinates.thread_id.to_string());
                    Ok((receipt, None))
                }
            }
            AdmissionDecision::ObserveOnly { .. } => Ok((
                KernelIoReceipt::new(envelope, target.clone(), decision),
                None,
            )),
            AdmissionDecision::Reject { reason, .. } => {
                Err(IoError::PolicyRejected(reason.clone()))
            }
            AdmissionDecision::Fork { child_key, input } => {
                self.apply_fork_admission(
                    envelope,
                    target,
                    child_key,
                    input,
                    ingress_message_ids,
                    ingress_source_stream,
                    source_ingress_event_ids,
                )
                .await
            }
        }
    }
}

#[async_trait]
impl KernelIoBridge for CooldisDaemonIoBridge {
    async fn apply(
        &self,
        envelope: &IngressEnvelope,
        target: &ResolvedIoTarget,
        decision: &AdmissionDecision,
    ) -> IoResult<KernelIoReceipt> {
        let (receipt, _) = self
            .apply_with_ingress_outcomes(envelope, target, decision, &[], None, &[], None)
            .await?;
        Ok(receipt)
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
    content_policies: Option<BTreeMap<String, String>>,
    threading: Option<String>,
    agent_ref: Option<String>,
    coalesce_bursts: Option<CooldisCoalesceBurstsConfig>,
}

impl RouteIngressSink {
    pub fn new(inner: Arc<dyn IngressSink>, route: &CooldisIoRouteConfig) -> Self {
        Self {
            inner,
            route_id: route.id.clone(),
            policy: route.policy.clone(),
            content_policies: route.content_policies.clone(),
            threading: route.threading.clone(),
            agent_ref: route.agent_ref.clone(),
            coalesce_bursts: route.coalesce_bursts,
        }
    }
}

#[async_trait]
impl IngressSink for RouteIngressSink {
    async fn submit(&self, mut envelope: IngressEnvelope) -> IoResult<IngressAck> {
        envelope
            .metadata
            .insert("cooldis_route_id".to_string(), self.route_id.clone());
        let content_policy = match &envelope.content {
            IngressContent::Event { kind, .. } => self
                .content_policies
                .as_ref()
                .and_then(|policies| policies.get(kind))
                .map(String::as_str),
            _ => None,
        };
        let effective_policy = content_policy.or(self.policy.as_deref());
        if let Some(policy) = effective_policy {
            envelope
                .metadata
                .insert("cooldis_route_policy".to_string(), policy.to_string());
        }
        if let Some(threading) = &self.threading {
            envelope
                .metadata
                .insert("cooldis_route_threading".to_string(), threading.clone());
        }
        if let Some(agent_ref) = &self.agent_ref {
            envelope
                .metadata
                .insert(ROUTE_AGENT_REF_METADATA.to_string(), agent_ref.clone());
        }
        if route_coalesce_applies(effective_policy)
            && let Some(coalesce) = self.coalesce_bursts
        {
            envelope
                .metadata
                .insert("cooldis_coalesce_bursts".to_string(), "true".to_string());
            envelope.metadata.insert(
                "cooldis_coalesce_window_ms".to_string(),
                coalesce.window_ms.to_string(),
            );
            envelope.metadata.insert(
                "cooldis_coalesce_max_batch".to_string(),
                coalesce.max_batch.to_string(),
            );
        }
        self.inner.submit(envelope).await
    }
}

fn route_coalesce_applies(policy: Option<&str>) -> bool {
    !matches!(policy, Some("observe_only" | "reject"))
}

pub struct CooldisDaemonQueueWorker {
    queue: Arc<dyn IngressQueueStore>,
    bridge: CooldisDaemonIoBridge,
    worker_id: String,
    max_messages: usize,
    poll_interval: Duration,
    visibility_timeout_secs: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct QueueDrainOutcome {
    count: usize,
    held_until_ms: Option<u64>,
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
        Ok(self.drain_once_inner().await?.count)
    }

    async fn drain_once_inner(&self) -> IoResult<QueueDrainOutcome> {
        let leased = self
            .queue
            .lease_ingress(
                &self.worker_id,
                self.max_messages,
                self.visibility_timeout_secs,
            )
            .await?;
        let count = leased.len();
        let mut held_until_ms: Option<u64> = None;
        let mut coalesce_groups: BTreeMap<CoalesceGroupKey, Vec<LeasedIngressEnvelope>> =
            BTreeMap::new();
        for message in leased {
            match coalesce_policy_for_envelope(&message.envelope) {
                Ok(Some(_)) => {
                    coalesce_groups
                        .entry(coalesce_group_key(&message.envelope))
                        .or_default()
                        .push(message);
                }
                Ok(None) => self.submit_leased_message(message).await?,
                Err(err) => {
                    let reason = err.to_string();
                    self.queue
                        .retry_ingress(&message.message_id, &reason)
                        .await?;
                    return Err(err);
                }
            }
        }
        for (_key, messages) in coalesce_groups {
            if let Some(visible_at_ms) = self.process_coalesce_group(messages).await? {
                held_until_ms = Some(
                    held_until_ms
                        .map(|existing| existing.min(visible_at_ms))
                        .unwrap_or(visible_at_ms),
                );
            }
        }
        Ok(QueueDrainOutcome {
            count,
            held_until_ms,
        })
    }

    async fn submit_leased_message(&self, message: LeasedIngressEnvelope) -> IoResult<()> {
        match self.bridge.submit_queued_envelope(message.envelope).await {
            Ok(_) => self.queue.complete_ingress(&message.message_id).await,
            Err(err) => {
                let reason = err.to_string();
                self.queue
                    .retry_ingress(&message.message_id, &reason)
                    .await?;
                Err(err)
            }
        }
    }

    async fn process_coalesce_group(
        &self,
        mut messages: Vec<LeasedIngressEnvelope>,
    ) -> IoResult<Option<u64>> {
        sort_coalesce_messages(&mut messages);
        while !messages.is_empty() {
            let policy = coalesce_policy_for_envelope(&messages[0].envelope)?.ok_or_else(|| {
                IoError::Queue("coalesce group is missing coalesce policy".to_string())
            })?;
            let batch_len = messages.len().min(policy.max_batch);
            let ready = coalesce_batch_is_ready(&messages[..batch_len], policy);
            if !ready {
                let visible_at_ms = coalesce_visible_at_ms(&messages[0].envelope, policy);
                for message in messages {
                    self.queue
                        .hold_ingress_until(&message.message_id, visible_at_ms)
                        .await?;
                }
                return Ok(Some(visible_at_ms));
            }

            let remainder = messages.split_off(batch_len);
            let batch = messages;
            let mut fresh_batch = Vec::with_capacity(batch.len());
            for message in batch {
                if self.bridge.queued_message_was_applied(&message).await? {
                    self.queue.complete_ingress(&message.message_id).await?;
                } else {
                    fresh_batch.push(message);
                }
            }
            if fresh_batch.len() < batch_len {
                fresh_batch.extend(remainder);
                messages = fresh_batch;
                sort_coalesce_messages(&mut messages);
                continue;
            }
            let batch = fresh_batch;
            let merged = merged_coalesce_envelope(&batch)?;
            let source_envelopes = batch
                .iter()
                .map(|message| message.envelope.clone())
                .collect::<Vec<_>>();
            let ingress_message_ids = batch
                .iter()
                .map(|message| message.envelope.id.clone())
                .collect::<Vec<_>>();
            match self
                .bridge
                .submit_coalesced_queued_envelopes(merged, &source_envelopes, &ingress_message_ids)
                .await
            {
                Ok(_) => {
                    for message in &batch {
                        self.queue.complete_ingress(&message.message_id).await?;
                    }
                }
                Err(err) => {
                    let reason = err.to_string();
                    for message in &batch {
                        self.queue
                            .retry_ingress(&message.message_id, &reason)
                            .await?;
                    }
                    return Err(err);
                }
            }
            messages = remainder;
        }
        Ok(None)
    }

    pub async fn run(self) {
        loop {
            match self.drain_once_inner().await {
                Ok(QueueDrainOutcome { count: 0, .. }) => {
                    tokio::time::sleep(self.poll_interval).await
                }
                Ok(QueueDrainOutcome {
                    held_until_ms: Some(held_until_ms),
                    ..
                }) => {
                    let delay_ms = held_until_ms.saturating_sub(now_ms());
                    if delay_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    }
                }
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
    if event.kind != EventKind::TurnSubmitted {
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
    let turn_id = event
        .payload
        .get("turn_id")
        .and_then(JsonValue::as_str)
        .map(ToOwned::to_owned);
    Some(IngressReceiptContext {
        target,
        metadata,
        source_ingress_id,
        turn_id,
    })
}

fn ingress_outcome_fold(
    events: &[EventRecord],
    ingress_envelope_ids: &[String],
) -> IoResult<IngressOutcomeState> {
    if ingress_envelope_ids.is_empty() {
        return Ok(IngressOutcomeState::Missing);
    }
    let requested = ingress_envelope_ids.iter().collect::<HashSet<_>>();
    let mut owner_by_envelope = HashMap::<String, (EventRecord, IoIngressClaimedPayload)>::new();
    let mut settles = HashMap::<EventRecordId, (EventRecord, IoIngressSettledPayload)>::new();
    for event in events {
        match event.kind {
            EventKind::IoIngressClaimed => {
                let payload =
                    serde_json::from_value::<IoIngressClaimedPayload>(event.payload.clone())
                        .map_err(|err| {
                            IoError::Bridge(format!("invalid io.ingress.claimed payload: {err}"))
                        })?;
                for envelope_id in &payload.ingress_envelope_ids {
                    if owner_by_envelope.contains_key(envelope_id) {
                        return Err(IoError::Bridge(format!(
                            "ingress envelope {envelope_id:?} has more than one claim"
                        )));
                    }
                    owner_by_envelope.insert(envelope_id.clone(), (event.clone(), payload.clone()));
                }
            }
            EventKind::IoIngressSettled => {
                let payload =
                    serde_json::from_value::<IoIngressSettledPayload>(event.payload.clone())
                        .map_err(|err| {
                            IoError::Bridge(format!("invalid io.ingress.settled payload: {err}"))
                        })?;
                if settles
                    .insert(payload.claim_event_id, (event.clone(), payload))
                    .is_some()
                {
                    return Err(IoError::Bridge(
                        "ingress claim has more than one settle".to_string(),
                    ));
                }
            }
            _ => {}
        }
    }
    let mut owners = requested
        .iter()
        .filter_map(|id| owner_by_envelope.get(id.as_str()))
        .collect::<Vec<_>>();
    if owners.is_empty() {
        return Ok(IngressOutcomeState::Missing);
    }
    if owners.len() != requested.len() {
        return Err(IoError::Bridge(
            "durable ingress batch partially overlaps claimed envelopes".to_string(),
        ));
    }
    let (claim, claim_payload) = owners.pop().expect("non-empty claim owner set");
    if owners.iter().any(|(event, _)| event.id != claim.id) {
        return Err(IoError::Bridge(
            "durable ingress batch maps to different claims".to_string(),
        ));
    }
    match settles.remove(&claim.id) {
        Some((settle, settle_payload)) => {
            if settle_payload.ingress_envelope_ids != claim_payload.ingress_envelope_ids {
                return Err(IoError::Bridge(
                    "ingress settle envelope set does not match its claim".to_string(),
                ));
            }
            Ok(IngressOutcomeState::Settled {
                claim_payload: claim_payload.clone(),
                settle,
            })
        }
        None => Ok(IngressOutcomeState::Claimed {
            claim: claim.clone(),
            payload: claim_payload.clone(),
        }),
    }
}

fn ingress_claim_provenance(
    control_stream: &EventStreamId,
    ingress_witness_event_ids: &[EventRecordId],
    admission_event_id: EventRecordId,
) -> EventProvenance {
    EventProvenance {
        source_streams: vec![control_stream.clone()],
        source_event_ids: ingress_witness_event_ids
            .iter()
            .copied()
            .chain(std::iter::once(admission_event_id))
            .collect(),
        discharged_by: Some("controller:ingress-outcome".to_string()),
        function: Some("claim/v1".to_string()),
        ..EventProvenance::default()
    }
}

fn ingress_settle_provenance(
    control_stream: &EventStreamId,
    coordinates: &ThreadCoordinates,
    claim_event_id: EventRecordId,
    evidence_event_id: Option<EventRecordId>,
) -> EventProvenance {
    let mut source_streams = vec![control_stream.clone()];
    if evidence_event_id.is_some() {
        source_streams.push(EventStreamId::for_thread(coordinates));
    }
    EventProvenance {
        source_streams,
        source_event_ids: std::iter::once(claim_event_id)
            .chain(evidence_event_id)
            .collect(),
        discharged_by: Some("controller:ingress-outcome".to_string()),
        function: Some("settle/v1".to_string()),
        ..EventProvenance::default()
    }
}

fn deduplicated_ingress_receipt(
    envelope: &IngressEnvelope,
    target: ResolvedIoTarget,
    state: &IngressOutcomeState,
) -> KernelIoReceipt {
    let turn_id = ingress_outcome_turn_id(state);
    let reason = match turn_id {
        Some(turn_id) => format!("durable ingress claim settled for turn {turn_id}"),
        None => "durable ingress claim already settled".to_string(),
    };
    eprintln!(
        "cooldis daemon ingress {} deduplicated: {reason}",
        envelope.id
    );
    let decision = AdmissionDecision::ObserveOnly { reason };
    let mut receipt = KernelIoReceipt::new(envelope, target.clone(), &decision);
    receipt.thread_id = target.address.thread_id;
    receipt
}

fn ingress_outcome_turn_id(state: &IngressOutcomeState) -> Option<&str> {
    let intent = match state {
        IngressOutcomeState::Missing => return None,
        IngressOutcomeState::Claimed { payload, .. } => &payload.intent,
        IngressOutcomeState::Settled { claim_payload, .. } => &claim_payload.intent,
    };
    match intent {
        IngressOutcomeIntent::Turn { turn_id, .. } => Some(turn_id),
        IngressOutcomeIntent::Interrupt {
            replacement_turn_id,
            ..
        } => replacement_turn_id.as_deref(),
        IngressOutcomeIntent::Fork { child_key, .. } => Some(child_key),
        IngressOutcomeIntent::Observe { .. } | IngressOutcomeIntent::Reject { .. } => None,
    }
}

fn turn_execution_evidence(
    events: &[EventRecord],
    turn_id: &str,
    submission_mode: TurnSubmissionMode,
) -> Option<EventRecord> {
    events
        .iter()
        .find(|event| {
            event.kind != EventKind::TurnSubmitted
                && (submission_mode == TurnSubmissionMode::Steer
                    || event.kind != EventKind::SessionEntryAppended)
                && (event.payload.get("turn_id").and_then(Value::as_str) == Some(turn_id)
                    || event
                        .payload
                        .get("subject")
                        .and_then(|subject| subject.get("turn_id"))
                        .and_then(Value::as_str)
                        == Some(turn_id))
        })
        .cloned()
}

fn requested_egress_from_event(
    event: &EventRecord,
    events: &[EventRecord],
) -> IoResult<Option<EgressEnvelope>> {
    let request = serde_json::from_value::<IoEgressRequestedPayload>(event.payload.clone())
        .map_err(|err| IoError::Bridge(format!("invalid io.egress.requested payload: {err}")))?;
    let kind = serde_json::from_value::<EgressKind>(request.egress_kind)
        .map_err(|err| IoError::Bridge(format!("invalid requested egress kind: {err}")))?;
    let matched_context = request.match_event_id.and_then(|match_event_id| {
        events
            .iter()
            .find(|candidate| candidate.id == match_event_id)
            .and_then(ingress_context_from_event)
    });
    let target = if let Some(context) = &matched_context {
        context.target.clone()
    } else if let Some(target) = request.resolved_target {
        serde_json::from_value::<IoTarget>(target)
            .map_err(|err| IoError::Bridge(format!("invalid requested egress target: {err}")))?
    } else {
        return Ok(None);
    };
    let mut envelope = EgressEnvelope::new(target, kind, now_ms());
    if let Some(context) = matched_context {
        envelope.source_ingress_id = context.source_ingress_id;
        envelope.metadata = context.metadata;
    } else {
        envelope.metadata = envelope.target.metadata.clone();
    }
    Ok(Some(envelope))
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CoalescePolicy {
    window_ms: u64,
    max_batch: usize,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CoalesceGroupKey {
    route_id: String,
    source_scope: String,
    conversation_key: String,
    threading: String,
    actor_key: Option<String>,
}

fn coalesce_policy_for_envelope(envelope: &IngressEnvelope) -> IoResult<Option<CoalescePolicy>> {
    let policy = envelope
        .metadata
        .get("cooldis_route_policy")
        .map(String::as_str)
        .unwrap_or("queue_per_conversation");
    let enabled = policy == "coalesce_bursts"
        || envelope
            .metadata
            .get("cooldis_coalesce_bursts")
            .is_some_and(|value| value == "true")
        || envelope.metadata.contains_key("cooldis_coalesce_window_ms")
        || envelope.metadata.contains_key("cooldis_coalesce_max_batch");
    if !enabled {
        return Ok(None);
    }
    let window_ms = envelope
        .metadata
        .get("cooldis_coalesce_window_ms")
        .ok_or_else(|| IoError::Queue("coalesce_bursts requires window_ms".to_string()))?
        .parse::<u64>()
        .map_err(|err| IoError::Queue(format!("invalid coalesce_bursts window_ms: {err}")))?;
    let max_batch = envelope
        .metadata
        .get("cooldis_coalesce_max_batch")
        .ok_or_else(|| IoError::Queue("coalesce_bursts requires max_batch".to_string()))?
        .parse::<usize>()
        .map_err(|err| IoError::Queue(format!("invalid coalesce_bursts max_batch: {err}")))?;
    if window_ms == 0 {
        return Err(IoError::Queue(
            "coalesce_bursts window_ms must be greater than zero".to_string(),
        ));
    }
    if max_batch == 0 {
        return Err(IoError::Queue(
            "coalesce_bursts max_batch must be greater than zero".to_string(),
        ));
    }
    Ok(Some(CoalescePolicy {
        window_ms,
        max_batch,
    }))
}

fn coalesce_group_key(envelope: &IngressEnvelope) -> CoalesceGroupKey {
    let threading = envelope
        .metadata
        .get("cooldis_route_threading")
        .map(String::as_str)
        .unwrap_or("per_conversation");
    let actor_key = if threading == "per_actor" {
        Some(
            envelope
                .actor
                .as_ref()
                .map(|actor| actor.external_actor_id.clone())
                .unwrap_or_else(|| "anonymous".to_string()),
        )
    } else {
        None
    };
    CoalesceGroupKey {
        route_id: route_id_for_envelope(envelope),
        source_scope: envelope.source.stable_scope(),
        conversation_key: envelope.conversation.stable_key(),
        threading: threading.to_string(),
        actor_key,
    }
}

fn sort_coalesce_messages(messages: &mut [LeasedIngressEnvelope]) {
    messages.sort_by(|left, right| {
        left.envelope
            .received_at_ms
            .cmp(&right.envelope.received_at_ms)
            .then_with(|| left.message_id.cmp(&right.message_id))
    });
}

fn coalesce_batch_is_ready(messages: &[LeasedIngressEnvelope], policy: CoalescePolicy) -> bool {
    messages.len() >= policy.max_batch
        || messages.iter().any(|message| message.attempt > 1)
        || messages
            .first()
            .is_some_and(|message| now_ms() >= coalesce_visible_at_ms(&message.envelope, policy))
}

fn coalesce_visible_at_ms(envelope: &IngressEnvelope, policy: CoalescePolicy) -> u64 {
    envelope.received_at_ms.saturating_add(policy.window_ms)
}

fn merged_coalesce_envelope(messages: &[LeasedIngressEnvelope]) -> IoResult<IngressEnvelope> {
    let first = messages
        .first()
        .ok_or_else(|| IoError::Queue("cannot coalesce an empty ingress batch".to_string()))?;
    let mut merged = first.envelope.clone();
    merged.content = IngressContent::text(
        messages
            .iter()
            .map(|message| message.envelope.content.text_projection())
            .collect::<Vec<_>>()
            .join("\n"),
    );
    merged.attachments = messages
        .iter()
        .flat_map(|message| message.envelope.attachments.clone())
        .collect();
    merged.dedupe_key = None;
    merged.received_at_ms = first.envelope.received_at_ms;
    merged
        .metadata
        .insert("cooldis_coalesced".to_string(), "true".to_string());
    merged.metadata.insert(
        "cooldis_coalesced_batch_size".to_string(),
        messages.len().to_string(),
    );
    merged.metadata.insert(
        "cooldis_coalesced_source_envelope_ids".to_string(),
        messages
            .iter()
            .map(|message| message.envelope.id.as_str())
            .collect::<Vec<_>>()
            .join(","),
    );
    Ok(merged)
}

fn fork_source_cut_payload(
    coordinates: &ThreadCoordinates,
    checkpoint: &ThreadCheckpoint,
    stream_to_sequence: Option<EventSequence>,
) -> ThreadSpawnedForkSourceCutPayload {
    let stream_id = EventStreamId::for_thread(coordinates);
    ThreadSpawnedForkSourceCutPayload {
        thread_id: coordinates.thread_id,
        checkpoint_id: checkpoint.id,
        leaf_entry_id: checkpoint.active_entry_id,
        stream_id,
        stream_to_sequence,
    }
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
    let mut config = json!({
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
    });
    if envelope_declares_coalesce(envelope)
        && let Some(object) = config.as_object_mut()
    {
        let window_ms = envelope
            .metadata
            .get("cooldis_coalesce_window_ms")
            .and_then(|value| value.parse::<u64>().ok());
        let max_batch = envelope
            .metadata
            .get("cooldis_coalesce_max_batch")
            .and_then(|value| value.parse::<usize>().ok());
        object.insert(
            "coalesce_bursts".to_string(),
            json!({
                "window_ms": window_ms,
                "max_batch": max_batch,
            }),
        );
    }
    config
}

fn external_message_id(envelope: &IngressEnvelope) -> Option<String> {
    envelope
        .metadata
        .get("external_message_id")
        .or_else(|| envelope.metadata.get("telegram_message_id"))
        .cloned()
}

fn ingress_received_control_record(
    coordinates: &ThreadCoordinates,
    envelope: &IngressEnvelope,
    ingress_message_id: Option<&str>,
) -> IoResult<NewEventRecord> {
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
    let object = value.as_object_mut().ok_or_else(|| {
        IoError::Bridge("io.ingress.received payload did not encode as object".to_string())
    })?;
    object.insert(
        "schema".to_string(),
        json!(EventKind::IoIngressReceived.payload_schema_id()),
    );
    if let Some(message_id) = ingress_message_id {
        object.insert(
            INGRESS_MESSAGE_ID_FIELD.to_string(),
            JsonValue::String(message_id.to_string()),
        );
        object.insert(
            INGRESS_DEDUPE_SEEN_FIELD.to_string(),
            JsonValue::Bool(false),
        );
    }
    Ok(NewEventRecord::witnessed(
        coordinates.clone(),
        EventKind::IoIngressReceived,
        value,
    ))
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
    let mut admissible = match envelope
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
        "fork" | "fork_on_new_dm" => vec![EventAdmissionDecision::Fork],
        "coalesce_bursts" => vec![
            EventAdmissionDecision::Queue,
            EventAdmissionDecision::Coalesce,
        ],
        _ => vec![EventAdmissionDecision::Queue],
    };
    if envelope_declares_coalesce(envelope)
        && !admissible.contains(&EventAdmissionDecision::Coalesce)
    {
        admissible.push(EventAdmissionDecision::Coalesce);
    }
    admissible
}

fn envelope_declares_coalesce(envelope: &IngressEnvelope) -> bool {
    envelope
        .metadata
        .get("cooldis_route_policy")
        .is_some_and(|policy| policy == "coalesce_bursts")
        || envelope
            .metadata
            .get("cooldis_coalesce_bursts")
            .is_some_and(|value| value == "true")
        || envelope.metadata.contains_key("cooldis_coalesce_window_ms")
        || envelope.metadata.contains_key("cooldis_coalesce_max_batch")
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

/// Deterministic crash cut for `cooldis-restart-smoke`.
///
/// When the test-only variable names a marker path, the daemon creates it
/// after the durable route binding commits and parks before publishing the
/// binding in memory or submitting the first turn. The smoke then SIGKILLs the
/// process. Normal daemon runs do not set the variable and return immediately.
async fn pause_after_ingress_binding_for_restart_smoke() -> IoResult<()> {
    let Some(marker) = std::env::var_os("COOLDIS_TEST_PAUSE_AFTER_INGRESS_BINDING") else {
        return Ok(());
    };
    let marker = std::path::PathBuf::from(marker);
    std::fs::write(&marker, b"binding persisted\n").map_err(|err| {
        IoError::Bridge(format!(
            "write restart smoke binding marker {}: {err}",
            marker.display()
        ))
    })?;
    std::future::pending::<()>().await;
    Ok(())
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
