macro_rules! turn_entry_surfaces {
    ($($constant:ident => $name:literal),+ $(,)?) => {
        $(pub(crate) const $constant: &str = $name;)+

        /// The complete registry of turn-entry surfaces guarded by admission.
        ///
        /// Every continuation the kernel executes was admitted; there is no side
        /// door around this boundary. Add a new surface to the declaration below,
        /// then add its end-to-end fixture to the admission coverage manifest. The
        /// coverage ratchet fails and names any registered surface without one.
        pub(crate) const TURN_ENTRY_SURFACES: &[&str] = &[$($constant),+];
    };
}

turn_entry_surfaces! {
    HOST_SUBMIT_SURFACE => "host-submit",
    APP_SERVER_RPC_SURFACE => "app-server-rpc",
    APP_SERVER_ENVELOPE_INGRESS_SURFACE => "app-server-envelope-ingress",
    MCP_ADAPTER_SURFACE => "mcp-adapter",
    ACP_ADAPTER_SURFACE => "acp-adapter",
    DEBUG_RPC_SURFACE => "debug-rpc",
    TELEGRAM_WEBHOOK_SURFACE => "telegram-webhook-ingress",
    PGQRS_QUEUE_SURFACE => "pgqrs-queue-ingress",
    REMOTE_SYNC_INGRESS_SURFACE => "remote-sync-ingress",
    KERNEL_THREAD_SUBMIT_SURFACE => "kernel-thread-submit",
}

pub(crate) fn app_server_surface(client_name: Option<&str>) -> &'static str {
    let surface = match client_name {
        Some("verlet-mcp-server") => MCP_ADAPTER_SURFACE,
        Some("verlet-acp-agent") => ACP_ADAPTER_SURFACE,
        Some("verlet-debug-rpc") => DEBUG_RPC_SURFACE,
        Some(name) if name == concat!("cool", "dis-mcp-server") => MCP_ADAPTER_SURFACE,
        Some(name) if name == concat!("cool", "dis-acp-agent") => ACP_ADAPTER_SURFACE,
        Some(name) if name == concat!("cool", "dis-debug-rpc") => DEBUG_RPC_SURFACE,
        _ => APP_SERVER_RPC_SURFACE,
    };
    debug_assert!(TURN_ENTRY_SURFACES.contains(&surface));
    surface
}

const SURFACE_ADMISSION_FUNCTION: &str = "surface_admission/v1";
const ADMISSION_ROUTE_FUNCTION: &str = "admission_route/v1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AdmissionGateContext {
    pub(crate) route_id: String,
    pub(crate) policy_hash: String,
    pub(crate) decision: crate::kernel::history::AdmissionDecision,
    pub(crate) admissible: Option<Vec<crate::kernel::history::AdmissionDecision>>,
    pub(crate) source_ingress_event_ids: Vec<crate::kernel::history::EventRecordId>,
    pub(crate) discharged_by: String,
    pub(crate) function: String,
}

impl AdmissionGateContext {
    pub(crate) fn route_policy(
        route_id: String,
        policy_hash: String,
        decision: crate::kernel::history::AdmissionDecision,
        admissible: Vec<crate::kernel::history::AdmissionDecision>,
        source_ingress_event_ids: Vec<crate::kernel::history::EventRecordId>,
    ) -> Self {
        Self {
            discharged_by: format!("policy:admission_route:{route_id}"),
            function: ADMISSION_ROUTE_FUNCTION.to_string(),
            route_id,
            policy_hash,
            decision,
            admissible: Some(admissible),
            source_ingress_event_ids,
        }
    }

    pub(crate) fn surface_default(
        surface_name: &str,
        source_ingress_event_ids: Vec<crate::kernel::history::EventRecordId>,
    ) -> crate::VerletResult<Self> {
        let route_id = format!("surface:{surface_name}");
        let policy_hash =
            crate::agent::manifest_bind::canonical_json_hash(&surface_default_policy(&route_id))?;
        Ok(Self {
            route_id,
            policy_hash,
            decision: crate::kernel::history::AdmissionDecision::Queue,
            admissible: Some(vec![crate::kernel::history::AdmissionDecision::Queue]),
            source_ingress_event_ids,
            discharged_by: format!("policy:admission_surface:{surface_name}"),
            function: SURFACE_ADMISSION_FUNCTION.to_string(),
        })
    }
}

/// Appends the single `admission.decided` record for a turn-acceptance boundary.
///
/// This is the admission-as-scheduling law: callers must await this append before
/// enqueueing the turn for runtime execution, and a failed append must abort
/// scheduling so no turn runs without an admission decision on the control
/// stream.
pub(crate) async fn append_admission_decided(
    handle: &crate::RuntimeThreadHandle,
    context: AdmissionGateContext,
) -> crate::VerletResult<crate::kernel::history::EventRecord> {
    let record = admission_decided_record(handle.context().coordinates.clone(), context)?;
    handle.append_control_event(record).await
}

pub(crate) async fn submit_turn(
    host: &crate::RuntimeHost,
    thread_id: crate::ThreadId,
    turn_id: impl Into<String>,
    input: crate::TurnInput,
    mode: crate::TurnSubmissionMode,
    admission: Option<AdmissionGateContext>,
) -> crate::VerletResult<()> {
    let reserved = reserve_turn(host, thread_id, turn_id, input, mode, admission).await?;
    submit_reserved(reserved).await;
    Ok(())
}

pub(crate) async fn reserve_turn(
    host: &crate::RuntimeHost,
    thread_id: crate::ThreadId,
    turn_id: impl Into<String>,
    input: crate::TurnInput,
    mode: crate::TurnSubmissionMode,
    admission: Option<AdmissionGateContext>,
) -> crate::VerletResult<crate::kernel::runtime_host::ReservedTurnSubmission> {
    host.reserve_turn_submission_at_choke_point(thread_id, turn_id, input, mode, admission)
        .await
}

pub(crate) async fn submit_reserved(
    reserved: crate::kernel::runtime_host::ReservedTurnSubmission,
) -> bool {
    reserved.submit_unchecked().await
}

pub(crate) fn admission_decided_record(
    coordinates: crate::ThreadCoordinates,
    context: AdmissionGateContext,
) -> crate::VerletResult<crate::kernel::history::NewEventRecord> {
    let kind = crate::kernel::history::EventKind::AdmissionDecided;
    let payload = crate::kernel::history::AdmissionDecidedPayload {
        route_id: context.route_id.clone(),
        policy_hash: context.policy_hash.clone(),
        decision: context.decision,
        admissible: context.admissible.clone(),
        source_ingress_event_ids: context.source_ingress_event_ids.clone(),
    };
    let mut value = serde_json::to_value(payload).map_err(|err| {
        crate::VerletError::History(format!("admission.decided payload codec failed: {err}"))
    })?;
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "schema".to_string(),
            serde_json::json!(kind.payload_schema_id()),
        );
    }
    Ok(crate::kernel::history::NewEventRecord::discharged(
        coordinates.clone(),
        kind,
        value,
        crate::kernel::history::EventProvenance {
            source_streams: vec![crate::kernel::history::EventStreamId::new(format!(
                "control:{}",
                coordinates.thread_id
            ))],
            source_event_ids: context.source_ingress_event_ids,
            discharged_by: Some(context.discharged_by),
            function: Some(context.function),
            config_hash: Some(context.policy_hash),
            ..crate::kernel::history::EventProvenance::default()
        },
    ))
}

fn surface_default_policy(route_id: &str) -> serde_json::Value {
    serde_json::json!({
        "schema": "cooldis.admission.surface_policy/1",
        "route_id": route_id,
        "decision": "queue",
        "admissible": ["queue"],
    })
}

#[cfg(test)]
pub(crate) fn assert_admission_precedes_turn_records<'a>(
    control_events: &'a [crate::kernel::history::EventRecord],
    thread_events: &[crate::kernel::history::EventRecord],
) -> &'a crate::kernel::history::EventRecord {
    let admission = control_events
        .iter()
        .find(|event| event.kind.as_str() == "admission.decided")
        .expect("control stream missing admission.decided");
    let turn_events = thread_events
        .iter()
        .filter(|event| {
            event.kind.as_str() == "session.entry.appended"
                && event
                    .payload
                    .get("runtime_kind")
                    .and_then(serde_json::Value::as_str)
                    != Some("thread_started")
        })
        .collect::<Vec<_>>();
    assert!(
        !turn_events.is_empty(),
        "thread stream missing executed turn session entry"
    );
    let admission_key = event_order_key(admission);
    for event in turn_events {
        let event_key = event_order_key(event);
        assert!(
            admission_key < event_key,
            "admission.decided {} at {} must precede executed turn event {} at {}",
            admission.id,
            admission.created_at_ms,
            event.id,
            event.created_at_ms,
        );
    }
    admission
}

#[cfg(test)]
pub(crate) fn assert_admission_precedes_turn_values<'a>(
    control_events: &'a [serde_json::Value],
    thread_events: &[serde_json::Value],
) -> &'a serde_json::Value {
    let admission = control_events
        .iter()
        .find(|event| {
            event.get("kind").and_then(serde_json::Value::as_str) == Some("admission.decided")
        })
        .expect("control stream missing admission.decided");
    let admission_ms = admission
        .get("atMs")
        .and_then(serde_json::Value::as_i64)
        .expect("admission.decided missing atMs");
    let admission_key = value_order_key(admission);
    let turn_events = thread_events
        .iter()
        .filter(|event| {
            event.get("kind").and_then(serde_json::Value::as_str) == Some("session.entry.appended")
                && event
                    .get("payload")
                    .and_then(|payload| payload.get("runtime_kind"))
                    .and_then(serde_json::Value::as_str)
                    != Some("thread_started")
        })
        .collect::<Vec<_>>();
    assert!(
        !turn_events.is_empty(),
        "thread stream missing executed turn session entry"
    );
    for event in turn_events {
        let event_ms = event
            .get("atMs")
            .and_then(serde_json::Value::as_i64)
            .expect("turn event missing atMs");
        let event_key = value_order_key(event);
        assert!(
            admission_key < event_key,
            "admission.decided at {admission_ms} must precede executed turn event at {event_ms}"
        );
    }
    admission
}

#[cfg(test)]
fn event_order_key(event: &crate::kernel::history::EventRecord) -> (i64, String, i64, String) {
    (
        event.created_at_ms,
        event.stream_id.to_string(),
        event.sequence.get(),
        event.id.to_string(),
    )
}

#[cfg(test)]
fn value_order_key(event: &serde_json::Value) -> (i64, String, i64, String) {
    let at_ms = event
        .get("atMs")
        .and_then(serde_json::Value::as_i64)
        .expect("event missing atMs");
    let stream_id = event
        .get("stream_id")
        .and_then(serde_json::Value::as_str)
        .expect("event missing stream_id")
        .to_string();
    let sequence = event
        .get("sequence")
        .and_then(serde_json::Value::as_i64)
        .expect("event missing sequence");
    let event_id = event
        .get("eventId")
        .and_then(serde_json::Value::as_str)
        .expect("event missing eventId")
        .to_string();
    (at_ms, stream_id, sequence, event_id)
}

#[cfg(test)]
mod tests {

    const COVERED_SURFACE_FIXTURES: &[(&str, &str)] = &[
        (
            crate::kernel::admission::HOST_SUBMIT_SURFACE,
            "runtime_host_submit_records_surface_admission_before_turn_execution",
        ),
        (
            crate::kernel::admission::APP_SERVER_RPC_SURFACE,
            "app_server_turn_start_records_surface_admission_before_execution",
        ),
        (
            crate::kernel::admission::APP_SERVER_ENVELOPE_INGRESS_SURFACE,
            "app_server_envelope_ingress_records_surface_admission_before_execution",
        ),
        (
            crate::kernel::admission::MCP_ADAPTER_SURFACE,
            "mcp_server_runs_prompt_and_command_through_daemon",
        ),
        (
            crate::kernel::admission::ACP_ADAPTER_SURFACE,
            "acp_agent_process_smoke_runs_binary_over_stdio",
        ),
        (
            crate::kernel::admission::DEBUG_RPC_SURFACE,
            "debug_rpc_cli_calls_and_streams_turns_over_websocket",
        ),
        (
            crate::kernel::admission::TELEGRAM_WEBHOOK_SURFACE,
            "telegram_webhook_accepts_update_and_uses_sink",
        ),
        (
            crate::kernel::admission::PGQRS_QUEUE_SURFACE,
            "queue_worker_processes_envelope_after_queue_and_bridge_restart",
        ),
        (
            crate::kernel::admission::REMOTE_SYNC_INGRESS_SURFACE,
            "remote_queue_redelivery_enters_child_ingress_once",
        ),
        (
            crate::kernel::admission::KERNEL_THREAD_SUBMIT_SURFACE,
            "cross_thread_prompt_and_result_events_do_not_rewrite_lineage",
        ),
    ];

    #[test]
    fn admission_order_key_matches_replay_merge_tie_breaks() {
        let first = crate::kernel::history::EventRecordId::from_uuid(uuid::Uuid::now_v7());
        let second = crate::kernel::history::EventRecordId::from_uuid(uuid::Uuid::now_v7());

        // UUIDv7's canonical hyphenated representation preserves UUID byte
        // order, and `Uuid::now_v7` is process-monotonic within a millisecond.
        assert!(first.to_string() < second.to_string());
        assert!(
            (
                1_772_650_000_000_i64,
                "control:thread".to_string(),
                1_i64,
                first.to_string(),
            ) < (
                1_772_650_000_000_i64,
                "thread:thread".to_string(),
                1_i64,
                second.to_string(),
            )
        );
    }

    #[test]
    fn raw_submit_guard_detects_non_call_symbol_references() {
        let source = r#"
let reserve = RuntimeHost::reserve_turn_submission_at_choke_point;
pub(super) use ReservedTurnSubmission::submit_unchecked;
reserved.submit_unchecked
    ();
"#;

        assert_eq!(
            identifier_occurrences(source, "reserve_turn_submission_at_choke_point")
                .into_iter()
                .map(|occurrence| occurrence.line)
                .collect::<Vec<_>>(),
            vec![2]
        );
        assert_eq!(
            identifier_occurrences(source, "submit_unchecked")
                .into_iter()
                .map(|occurrence| occurrence.line)
                .collect::<Vec<_>>(),
            vec![3, 4]
        );
    }

    #[test]
    fn admission_fixture_contract_rejects_ignored_or_assertionless_tests() {
        let ignored = r#"
#[tokio::test]
#[ignore = "not in the required lane"]
async fn fixture() {
    assert_admission_surface().await;
}
"#;
        let assertionless = r#"
#[test]
fn fixture() {
    // assert_admission_precedes_turn_records();
    assert!(true);
}
"#;
        let covered = r#"
#[tokio::test]
async fn fixture() {
    assert_admission_precedes_turn_records();
}
"#;

        assert!(admission_fixture_contract(ignored, "fixture").is_err());
        assert!(admission_fixture_contract(assertionless, "fixture").is_err());
        assert!(admission_fixture_contract(covered, "fixture").is_ok());
    }

    #[test]
    fn every_registered_surface_has_an_admission_fixture() {
        let covered = COVERED_SURFACE_FIXTURES
            .iter()
            .map(|(surface, _)| *surface)
            .collect::<std::collections::BTreeSet<_>>();
        let missing = crate::kernel::admission::TURN_ENTRY_SURFACES
            .iter()
            .copied()
            .filter(|surface| !covered.contains(surface))
            .collect::<Vec<_>>();

        assert!(
            missing.is_empty(),
            "registered turn-entry surface(s) missing admission coverage fixture: {}",
            missing.join(", ")
        );
        assert_eq!(
            covered.len(),
            COVERED_SURFACE_FIXTURES.len(),
            "admission coverage manifest contains duplicate surface entries"
        );

        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let mut test_sources = Vec::new();
        collect_rust_sources(&manifest_dir, &mut |path| {
            let source = std::fs::read_to_string(path)
                .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
            test_sources.push((path.to_path_buf(), source));
        });
        for (surface, fixture) in COVERED_SURFACE_FIXTURES {
            let declarations = test_sources
                .iter()
                .filter(|(_, source)| source.contains(&format!("fn {fixture}")))
                .collect::<Vec<_>>();
            assert_eq!(
                declarations.len(),
                1,
                "registered turn-entry surface {surface} must name exactly one admission fixture {fixture}, found {}",
                declarations.len()
            );
            let (path, source) = declarations[0];
            admission_fixture_contract(source, fixture).unwrap_or_else(|reason| {
                let relative = path.strip_prefix(&manifest_dir).unwrap_or(path);
                panic!(
                    "registered turn-entry surface {surface} has invalid admission fixture {fixture} in {}: {reason}",
                    relative.display()
                )
            });
        }
    }

    #[test]
    fn app_server_adapter_clients_resolve_to_registered_surfaces() {
        assert_eq!(
            crate::kernel::admission::app_server_surface(None),
            crate::kernel::admission::APP_SERVER_RPC_SURFACE
        );
        assert_eq!(
            crate::kernel::admission::app_server_surface(Some("verlet-mcp-server")),
            crate::kernel::admission::MCP_ADAPTER_SURFACE
        );
        assert_eq!(
            crate::kernel::admission::app_server_surface(Some("verlet-acp-agent")),
            crate::kernel::admission::ACP_ADAPTER_SURFACE
        );
        assert_eq!(
            crate::kernel::admission::app_server_surface(Some("verlet-debug-rpc")),
            crate::kernel::admission::DEBUG_RPC_SURFACE
        );
        assert_eq!(
            crate::kernel::admission::app_server_surface(Some(concat!("cool", "dis-mcp-server"))),
            crate::kernel::admission::MCP_ADAPTER_SURFACE
        );
        assert_eq!(
            crate::kernel::admission::app_server_surface(Some(concat!("cool", "dis-acp-agent"))),
            crate::kernel::admission::ACP_ADAPTER_SURFACE
        );
        assert_eq!(
            crate::kernel::admission::app_server_surface(Some(concat!("cool", "dis-debug-rpc"))),
            crate::kernel::admission::DEBUG_RPC_SURFACE
        );
    }

    #[test]
    fn raw_turn_submit_choke_point_has_no_callers_outside_admission() {
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let source_root = manifest_dir.join("src");
        let admission_path = source_root.join("kernel/admission.rs");
        let runtime_host_path = source_root.join("kernel/runtime_host.rs");
        let raw_symbols = ["reserve_turn_submission_at_choke_point", "submit_unchecked"];
        let mut definitions = std::collections::BTreeMap::new();
        let mut offenders = Vec::new();
        collect_rust_sources(&source_root, &mut |path| {
            if path == admission_path {
                return;
            }
            let source = std::fs::read_to_string(path)
                .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
            let compact = source
                .chars()
                .filter(|character| !character.is_whitespace())
                .collect::<String>();
            if path.starts_with(source_root.join("kernel"))
                && (compact.contains("#[path=") || compact.contains("include!("))
            {
                let relative = path.strip_prefix(&manifest_dir).unwrap_or(path);
                offenders.push(format!(
                    "{}: kernel module uses #[path] or include!, which can hide a raw choke-point reference from the source ratchet",
                    relative.display()
                ));
            }
            for symbol in raw_symbols {
                for occurrence in identifier_occurrences(&source, symbol) {
                    let line = source.lines().nth(occurrence.line - 1).unwrap_or_default();
                    if path == runtime_host_path
                        && previous_identifier(&source, occurrence.offset) == Some("fn")
                    {
                        *definitions.entry(symbol).or_insert(0_usize) += 1;
                        continue;
                    }
                    let relative = path.strip_prefix(&manifest_dir).unwrap_or(path);
                    offenders.push(format!(
                        "{}:{}: {}",
                        relative.display(),
                        occurrence.line,
                        line.trim()
                    ));
                }
            }
        });
        for symbol in raw_symbols {
            let count = definitions.get(symbol).copied().unwrap_or_default();
            if count != 1 {
                offenders.push(format!(
                    "src/kernel/runtime_host.rs: expected exactly one private definition of {symbol}, found {count}"
                ));
            }
        }

        assert!(
            offenders.is_empty(),
            "turn-submit choke point referenced outside kernel/admission.rs:\n{}",
            offenders.join("\n")
        );
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct IdentifierOccurrence {
        offset: usize,
        line: usize,
    }

    fn identifier_occurrences(source: &str, identifier: &str) -> Vec<IdentifierOccurrence> {
        source
            .match_indices(identifier)
            .filter_map(|(offset, _)| {
                let before = source[..offset].chars().next_back();
                let after = source[offset + identifier.len()..].chars().next();
                let is_boundary = |character: Option<char>| {
                    character
                        .is_none_or(|character| character != '_' && !character.is_alphanumeric())
                };
                (is_boundary(before) && is_boundary(after)).then(|| IdentifierOccurrence {
                    offset,
                    line: source[..offset]
                        .bytes()
                        .filter(|byte| *byte == b'\n')
                        .count()
                        + 1,
                })
            })
            .collect()
    }

    fn previous_identifier(source: &str, offset: usize) -> Option<&str> {
        source[..offset]
            .rsplit(|character: char| character != '_' && !character.is_alphanumeric())
            .find(|token| !token.is_empty())
    }

    fn admission_fixture_contract(source: &str, fixture: &str) -> Result<(), String> {
        let declaration = format!("fn {fixture}");
        let declaration_offset = source
            .find(&declaration)
            .ok_or_else(|| "function declaration is missing".to_string())?;
        if source[declaration_offset + declaration.len()..].contains(&declaration) {
            return Err("function declaration is duplicated".to_string());
        }
        let declaration_line_start = source[..declaration_offset]
            .rfind('\n')
            .map_or(0, |offset| offset + 1);
        let attribute_start = source[..declaration_line_start]
            .rfind("\n\n")
            .map_or(0, |offset| offset + 2);
        let attributes = &source[attribute_start..declaration_line_start];
        if !(attributes.contains("#[test]") || attributes.contains("#[tokio::test")) {
            return Err("fixture is not declared as a test".to_string());
        }
        if attributes.contains("ignore") {
            return Err(
                "fixture is ignored and does not run in the required full suite".to_string(),
            );
        }
        let body_start = source[declaration_offset..]
            .find('{')
            .map(|offset| declaration_offset + offset + 1)
            .ok_or_else(|| "fixture body is missing".to_string())?;
        let declaration_prefix = &source[declaration_line_start..declaration_offset];
        let indent_len = declaration_prefix
            .find(|character: char| !character.is_whitespace())
            .unwrap_or(declaration_prefix.len());
        let indent = &declaration_prefix[..indent_len];
        let body_end_marker = format!("\n{indent}}}");
        let body_end = source[body_start..]
            .find(&body_end_marker)
            .map(|offset| body_start + offset)
            .ok_or_else(|| "fixture body closing brace is missing".to_string())?;
        let body = &source[body_start..body_end];
        let calls_admission_assertion = body.lines().any(|line| {
            line.split("//")
                .next()
                .unwrap_or_default()
                .split(|character: char| character != '_' && !character.is_alphanumeric())
                .any(|token| token.starts_with("assert_admission_"))
        });
        if !calls_admission_assertion {
            return Err("fixture body no longer calls an admission assertion".to_string());
        }
        Ok(())
    }

    fn collect_rust_sources(root: &std::path::Path, visit: &mut impl FnMut(&std::path::Path)) {
        let entries = std::fs::read_dir(root)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", root.display()));
        for entry in entries {
            let path = entry.expect("source directory entry").path();
            if path.is_dir() {
                collect_rust_sources(&path, visit);
            } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
                visit(&path);
            }
        }
    }
}
