use crate::agent::manifest_bind::canonical_json_hash;
use crate::{
    AdmissionDecidedPayload, AdmissionDecision as EventAdmissionDecision, CooldisAppServer,
    CooldisEgressProjectionRuleConfig, CooldisError, CooldisIoRouteConfig, CooldisResult,
    CooldisSupervisor, CooldisTypingSimulationConfig, EventKind, EventProvenance,
    IoIngressReceivedPayload, NewEventRecord, PolicyBoundPayload, PolicyKind, RuntimeEventKind,
    RuntimeTerminalState, RuntimeThreadHandle, ThreadCoordinates, ThreadEvent, ThreadId,
    ThreadStartRequest, ThreadTopology, TurnInput, TurnSubmissionMode,
};
use async_trait::async_trait;
use cooldis_io_core::{
    AdmissionDecision, EgressAdapter, EgressEnvelope, EgressKind, IngressAck, IngressEnvelope,
    IngressQueueStore, IngressSink, IngressState, IoError, IoResult, IoTurnInput, KernelIoBridge,
    KernelIoReceipt, ProviderPolicy, ResolvedIoTarget, ThreadAddress,
};
use cooldis_io_telegram::{TelegramUpdate, TelegramWebhookAdapter};
use regex::{Captures, Regex};
use serde_json::{Map as JsonMap, Value, Value as JsonValue, json};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock, broadcast};

const DEFAULT_QUEUE_BATCH: usize = 16;
const DEFAULT_WORKER_POLL_MS: u64 = 250;
const MAX_HTTP_HEADER_BYTES: usize = 16 * 1024;
const MAX_HTTP_BODY_BYTES: usize = 4 * 1024 * 1024;
const MAX_TYPING_SIMULATION_DELAY: Duration = Duration::from_secs(8);

#[derive(Clone, Debug, Default)]
struct RouteEgressConfig {
    projection_rules: Vec<CompiledEgressProjectionRule>,
    typing_simulation: Option<CooldisTypingSimulationConfig>,
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

#[derive(Clone)]
pub struct CooldisDaemonIoBridge {
    supervisor: CooldisSupervisor,
    tenant_id: String,
    user_id: String,
    model: String,
    model_provider: String,
    cwd: PathBuf,
    threads: Arc<Mutex<HashMap<String, ThreadCoordinates>>>,
    active_turns: Arc<Mutex<HashMap<String, String>>>,
    egress_adapters: Arc<RwLock<HashMap<String, Arc<dyn EgressAdapter>>>>,
    egress_route_configs: Arc<RwLock<HashMap<String, RouteEgressConfig>>>,
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
            threads: Arc::new(Mutex::new(HashMap::new())),
            active_turns: Arc::new(Mutex::new(HashMap::new())),
            egress_adapters: Arc::new(RwLock::new(HashMap::new())),
            egress_route_configs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn from_app_server(server: &CooldisAppServer) -> Self {
        Self::new(
            server.supervisor(),
            server.tenant_id().to_string(),
            server.user_id().to_string(),
            server.model_provider().to_string(),
            server.model().to_string(),
            server.cwd().to_path_buf(),
        )
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

    pub async fn submit_envelope(&self, envelope: IngressEnvelope) -> IoResult<KernelIoReceipt> {
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

    async fn watch_for_egress(
        self,
        envelope: IngressEnvelope,
        target: ResolvedIoTarget,
        turn_id: String,
        mut events: broadcast::Receiver<ThreadEvent>,
    ) {
        let mut assistant_text = String::new();
        let mut terminal = false;
        let timeout = tokio::time::sleep(Duration::from_secs(30));
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                _ = &mut timeout => break,
                event = events.recv() => {
                    let Ok(event) = event else {
                        break;
                    };
                    match event {
                        ThreadEvent::Runtime { event, .. } => match event.kind {
                            RuntimeEventKind::TextDelta { text } => assistant_text.push_str(&text),
                            RuntimeEventKind::Terminal { state } => {
                                terminal = matches!(
                                    state,
                                    RuntimeTerminalState::Completed
                                        | RuntimeTerminalState::Cancelled
                                        | RuntimeTerminalState::Stopped
                                        | RuntimeTerminalState::Failed
                                );
                                break;
                            }
                            RuntimeEventKind::Failed { message, .. } => {
                                self.deliver_egress(daemon_egress_for_ingress(
                                    &envelope,
                                    EgressKind::Error { message },
                                ))
                                .await;
                                break;
                            }
                            _ => {}
                        },
                        ThreadEvent::Failed { message, .. } => {
                            self.deliver_egress(daemon_egress_for_ingress(
                                &envelope,
                                EgressKind::Error { message },
                            ))
                            .await;
                            break;
                        }
                        ThreadEvent::Stopped { .. } | ThreadEvent::Cancelled { .. } => {
                            terminal = true;
                            break;
                        }
                        ThreadEvent::Output { text, .. } => {
                            if assistant_text.is_empty() {
                                assistant_text = text;
                            }
                            terminal = true;
                            break;
                        }
                        ThreadEvent::Started { .. }
                        | ThreadEvent::CanonicalMirror { .. }
                        | ThreadEvent::Signal { .. } => {}
                    }
                }
            }
        }

        self.active_turns
            .lock()
            .await
            .remove(&target.address.scope_key());

        if terminal && !assistant_text.is_empty() {
            self.deliver_egress(daemon_egress_for_ingress(
                &envelope,
                EgressKind::AssistantMessage {
                    text: assistant_text,
                },
            ))
            .await;
        } else if !terminal {
            eprintln!(
                "cooldis daemon IO turn {turn_id} egress watcher stopped before terminal event"
            );
        }
    }

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
            self.deliver_projected_egress(adapter.as_ref(), &route_config, envelope)
                .await;
        }
    }

    async fn deliver_projected_egress(
        &self,
        adapter: &dyn EgressAdapter,
        route_config: &RouteEgressConfig,
        envelope: EgressEnvelope,
    ) {
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
            if let Err(err) = adapter.deliver(typing_envelope).await {
                eprintln!("cooldis daemon IO egress typing action failed: {err}");
            }
            let delay = typing_delay_for_text(text, typing.chars_per_second);
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
        }

        if let Err(err) = adapter.deliver(envelope).await {
            eprintln!("cooldis daemon IO egress delivery failed: {err}");
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
        match decision {
            AdmissionDecision::Queue { turn_id, input } => {
                let (coordinates, handle) = self.ensure_thread(target).await?;
                let events = handle.subscribe_events();
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
                tokio::spawn(self.clone().watch_for_egress(
                    envelope.clone(),
                    target.clone(),
                    turn_id.clone(),
                    events,
                ));
                let mut receipt = KernelIoReceipt::new(envelope, target.clone(), decision);
                receipt.thread_id = Some(coordinates.thread_id.to_string());
                Ok(receipt)
            }
            AdmissionDecision::Steer { turn_id, input, .. } => {
                let (coordinates, handle) = self.ensure_thread(target).await?;
                let events = handle.subscribe_events();
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
                tokio::spawn(self.clone().watch_for_egress(
                    envelope.clone(),
                    target.clone(),
                    turn_id.clone(),
                    events,
                ));
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
                let events = handle.subscribe_events();
                self.supervisor
                    .cancel_at(&coordinates, reason.clone())
                    .await
                    .map_err(cooldis_bridge_error)?;
                if let (Some(turn_id), Some(input)) = (replacement_turn_id, replacement) {
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
                    tokio::spawn(self.clone().watch_for_egress(
                        envelope.clone(),
                        target.clone(),
                        turn_id.clone(),
                        events,
                    ));
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

fn daemon_egress_for_ingress(ingress: &IngressEnvelope, kind: EgressKind) -> EgressEnvelope {
    let mut envelope = EgressEnvelope::for_ingress(ingress, kind, now_ms());
    envelope.metadata = ingress.metadata.clone();
    envelope
}

fn sibling_egress(source: &EgressEnvelope, kind: EgressKind) -> EgressEnvelope {
    let mut envelope = EgressEnvelope::new(source.target.clone(), kind, now_ms());
    envelope.source_ingress_id = source.source_ingress_id.clone();
    envelope.metadata = source.metadata.clone();
    envelope
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

fn cooldis_bridge_error(err: CooldisError) -> IoError {
    IoError::Bridge(err.to_string())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests;
