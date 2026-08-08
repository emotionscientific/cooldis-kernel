//! The `coupling` subcommand family.

pub(super) async fn run_coupling(
    mut args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_coupling_help();
        return Ok(());
    }
    let subcommand = args.remove(0);
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        match subcommand.to_string_lossy().as_ref() {
            "init" => print_coupling_init_help(),
            "run" => print_coupling_run_help(),
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unknown coupling subcommand {other:?}"
                )));
            }
        }
        return Ok(());
    }
    match subcommand.to_string_lossy().as_ref() {
        "init" => coupling_init(args).await,
        "run" => coupling_run(args).await,
        _ => Err(crate::cli::usage_error(format!(
            "unknown coupling subcommand {subcommand:?}"
        ))),
    }
}

pub(super) async fn coupling_init(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_coupling_init_args(args)?;
    if options.help {
        print_coupling_init_help();
        return Ok(());
    }
    let name = options
        .name
        .ok_or_else(|| crate::cli::usage_error("coupling init requires <name>"))?;
    let root = options
        .out_path
        .unwrap_or_else(|| std::path::PathBuf::from(&name));
    write_coupling_project(&name, &root, options.force)?;
    println!("{}", root.display());
    Ok(())
}

pub(super) async fn coupling_run(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_coupling_run_args(args)?;
    if options.help {
        print_coupling_run_help();
        return Ok(());
    }
    if !options.replay {
        return Err(crate::cli::usage_error(
            "coupling run currently requires --replay",
        ));
    }
    let artifact = options.artifact.as_deref().ok_or_else(|| {
        crate::cli::usage_error("coupling run --replay requires --artifact <path|op://ref>")
    })?;
    let mut coupling_set = load_replay_coupling_set(&options)?;
    if let Some(coupling_id) = &options.coupling_id {
        coupling_set = select_replay_coupling(&coupling_set, coupling_id)?;
    }
    let operation_registry_root =
        resolve_replay_artifact(artifact, options.registry_root.clone(), &mut coupling_set).await?;
    let events = load_replay_recorded_events(&options).await?;
    let replayed_event_count = events.len();
    let receipt = replay_coupling_events(&coupling_set, events, operation_registry_root).await?;
    let report = CouplingReplayReport::from_receipt(replayed_event_count, &coupling_set, receipt);
    if options.json {
        serde_json::to_writer_pretty(std::io::stdout(), &report).map_err(|err| {
            crate::cli::usage_error(format!("failed to encode coupling replay JSON: {err}"))
        })?;
        println!();
    } else {
        print_coupling_replay_report(&report);
    }
    Ok(())
}

pub(super) fn load_replay_coupling_set(
    options: &CouplingRunArgs,
) -> crate::kernel::runtime_host::VerletResult<crate::agent::manifest_bind::BoundCouplingSet> {
    if let Some(path) = &options.coupling_file {
        return load_replay_coupling_set_file(path);
    }
    if let Some(path) = &options.export_bundle {
        let value = read_json_file(path)?;
        if let Some(coupling_set) = coupling_set_from_export_bundle(&value)? {
            return Ok(coupling_set);
        }
    }
    Err(crate::cli::usage_error(
        "coupling run --replay requires --coupling-file unless the export bundle contains a bound coupling set",
    ))
}

pub(super) fn load_replay_coupling_set_file(
    path: &std::path::Path,
) -> crate::kernel::runtime_host::VerletResult<crate::agent::manifest_bind::BoundCouplingSet> {
    let raw = std::fs::read_to_string(path).map_err(|err| {
        crate::cli::usage_error(format!("failed to read {}: {err}", path.display()))
    })?;
    if let Ok(set) = serde_json::from_str::<crate::agent::manifest_bind::BoundCouplingSet>(&raw) {
        return Ok(set);
    }
    if let Ok(coupling) = serde_json::from_str::<crate::agent::manifest_bind::BoundCoupling>(&raw) {
        return Ok(crate::agent::manifest_bind::BoundCouplingSet::new(
            "replay-file",
            vec![coupling],
        ));
    }
    if let Ok(set) = toml::from_str::<crate::agent::manifest_bind::BoundCouplingSet>(&raw) {
        return Ok(set);
    }
    if let Ok(coupling) = toml::from_str::<crate::agent::manifest_bind::BoundCoupling>(&raw) {
        return Ok(crate::agent::manifest_bind::BoundCouplingSet::new(
            "replay-file",
            vec![coupling],
        ));
    }
    Err(crate::cli::usage_error(format!(
        "coupling file {} must be a serialized BoundCouplingSet or BoundCoupling",
        path.display()
    )))
}

pub(super) fn coupling_set_from_export_bundle(
    value: &serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<Option<crate::agent::manifest_bind::BoundCouplingSet>>
{
    for pointer in ["/boundCouplingSet", "/couplingSet"] {
        if let Some(candidate) = value.pointer(pointer)
            && !candidate.is_null()
        {
            return serde_json::from_value::<crate::agent::manifest_bind::BoundCouplingSet>(
                candidate.clone(),
            )
            .map(Some)
            .map_err(|err| {
                crate::cli::usage_error(format!("export bundle {pointer} is invalid: {err}"))
            });
        }
    }
    if let Some(raw) = value
        .pointer("/thread/metadata/cooldis.agent.bound_coupling_set")
        .and_then(serde_json::Value::as_str)
    {
        return serde_json::from_str::<crate::agent::manifest_bind::BoundCouplingSet>(raw)
            .map(Some)
            .map_err(|err| {
                crate::cli::usage_error(format!(
                    "export bundle bound coupling metadata is invalid: {err}"
                ))
            });
    }
    Ok(None)
}

pub(super) fn select_replay_coupling(
    coupling_set: &crate::agent::manifest_bind::BoundCouplingSet,
    coupling_id: &str,
) -> crate::kernel::runtime_host::VerletResult<crate::agent::manifest_bind::BoundCouplingSet> {
    let couplings = coupling_set
        .couplings
        .iter()
        .filter(|coupling| coupling.id == coupling_id)
        .cloned()
        .collect::<Vec<_>>();
    if couplings.is_empty() {
        return Err(crate::cli::usage_error(format!(
            "bound coupling id {coupling_id:?} was not found in replay coupling set"
        )));
    }
    let grant_expiries = coupling_set
        .grant_expiries
        .get(coupling_id)
        .cloned()
        .map(|expiries| std::collections::BTreeMap::from([(coupling_id.to_string(), expiries)]))
        .unwrap_or_default();
    Ok(
        crate::agent::manifest_bind::BoundCouplingSet::new_with_grant_expiries(
            coupling_set.snapshot_id.clone(),
            couplings,
            grant_expiries,
        ),
    )
}

#[cfg(test)]
mod expiry_selection_tests {

    #[test]
    fn selecting_one_replay_coupling_preserves_its_grant_expiries() {
        let selected = replay_coupling("selected");
        let other = replay_coupling("other");
        let set = crate::agent::manifest_bind::BoundCouplingSet::new_with_grant_expiries(
            "snapshot-a",
            vec![selected, other],
            std::collections::BTreeMap::from([
                (
                    "selected".to_string(),
                    vec![verlet_agent::manifest_schema::AgentManifestGrantExpiry {
                        capability: "stream.read:thread".to_string(),
                        expires_at: "2050-01-01T00:00:00Z".to_string(),
                    }],
                ),
                (
                    "other".to_string(),
                    vec![verlet_agent::manifest_schema::AgentManifestGrantExpiry {
                        capability: "stream.write:control".to_string(),
                        expires_at: "2060-01-01T00:00:00Z".to_string(),
                    }],
                ),
            ]),
        );

        let selected = crate::cli::coupling::select_replay_coupling(&set, "selected").unwrap();

        assert_eq!(selected.couplings.len(), 1);
        assert_eq!(selected.couplings[0].id, "selected");
        assert_eq!(
            selected.grant_expiries.keys().cloned().collect::<Vec<_>>(),
            vec!["selected"]
        );
    }

    fn replay_coupling(id: &str) -> crate::agent::manifest_bind::BoundCoupling {
        crate::agent::manifest_bind::BoundCoupling {
            id: id.to_string(),
            role: crate::agent::manifest_bind::CouplingRole::Controller,
            trigger_kind: verlet_history::EventKind::TurnCompleted,
            trigger_match: std::collections::BTreeMap::new(),
            trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
            source_selectors: vec![crate::agent::manifest_bind::BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![verlet_history::EventKind::TurnCompleted],
                scope: None,
                since: None,
            }],
            sink: crate::agent::manifest_bind::BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![verlet_history::EventKind::PlacementDecision],
            },
            function_ref: format!("op://{id}/run@sha256:{}", "a".repeat(64)),
            function: crate::agent::manifest_bind::BoundCouplingFunction {
                name: id.to_string(),
                artifact_hash: "a".repeat(64),
                operation_name: Some("run".to_string()),
            },
            grants: vec![
                "stream.read:thread".to_string(),
                "stream.write:control".to_string(),
            ],
            budget: Default::default(),
            config: serde_json::json!({}),
            config_hash: "sha256:test".to_string(),
        }
    }
}

pub(super) async fn resolve_replay_artifact(
    artifact: &str,
    registry_root: Option<std::path::PathBuf>,
    coupling_set: &mut crate::agent::manifest_bind::BoundCouplingSet,
) -> crate::kernel::runtime_host::VerletResult<std::path::PathBuf> {
    if artifact.starts_with("op://") {
        let parsed = parse_pinned_operation_ref(artifact)?;
        let root = registry_root.unwrap_or_else(crate::cli::tool::default_registry_root);
        let record = verlet_operations::operation_store::LocalOperationRegistry::new(&root)
            .load_version_record(&parsed.name, &parsed.artifact_hash)
            .map_err(|err| {
                crate::cli::usage_error(format!(
                    "replay artifact {artifact:?} was not found in operation registry {}: {err}",
                    root.display()
                ))
            })?;
        apply_replay_operation_record(coupling_set, &record, parsed.operation.as_deref())?;
        return Ok(root);
    }

    let artifact_path = std::path::PathBuf::from(artifact);
    let root = registry_root.unwrap_or_else(|| {
        std::env::temp_dir()
            .join(format!("verlet-coupling-replay-{}", uuid::Uuid::now_v7()))
            .join("operations")
    });
    let record = verlet_operations::operation_store::LocalOperationRegistry::new(&root)
        .publish_artifact(
            verlet_operations::operation_store::PublishOperationRequest {
                name: "replay-coupling".to_string(),
                artifact_path: artifact_path.clone(),
                source: verlet_operations::operation_store::PublishedOperationSource::Wasm {
                    bin_path: artifact_path,
                },
                interface: None,
                capability_grants: std::collections::BTreeSet::new(),
                metadata: std::collections::BTreeMap::from([(
                    "coupling.replay.local_artifact".to_string(),
                    serde_json::json!(true),
                )]),
            },
        )
        .await?;
    apply_replay_operation_record(coupling_set, &record, None)?;
    Ok(root)
}

pub(super) fn apply_replay_operation_record(
    coupling_set: &mut crate::agent::manifest_bind::BoundCouplingSet,
    record: &verlet_operations::operation_store::PublishedOperationRecord,
    selected_operation: Option<&str>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    for coupling in &mut coupling_set.couplings {
        let operation_name = select_replay_operation_name(
            &coupling.id,
            selected_operation.or(coupling.function.operation_name.as_deref()),
            &record.manifest,
        )?;
        validate_replay_coupling_operation(&coupling.id, &operation_name, &record.manifest)?;
        coupling.function_ref = format!(
            "op://{}/{operation_name}@sha256:{}",
            record.name, record.active_artifact_hash
        );
        coupling.function.name = record.name.clone();
        coupling.function.artifact_hash = record.active_artifact_hash.clone();
        coupling.function.operation_name = Some(operation_name);
    }
    Ok(())
}

pub(super) fn select_replay_operation_name(
    coupling_id: &str,
    selected_operation: Option<&str>,
    manifest: &verlet_abi::WasmOperationManifest,
) -> crate::kernel::runtime_host::VerletResult<String> {
    if let Some(operation_name) = selected_operation {
        if manifest.operation(operation_name).is_some() {
            return Ok(operation_name.to_string());
        }
        return Err(crate::cli::usage_error(format!(
            "replay artifact does not expose operation {operation_name:?} for coupling {coupling_id:?}"
        )));
    }
    if manifest.operations.len() == 1 {
        return Ok(manifest.operations[0].name.clone());
    }
    Err(crate::cli::usage_error(format!(
        "replay artifact for coupling {coupling_id:?} exposes multiple operations; use op://<record>/<operation>@sha256:<hash> or a bound coupling operation_name"
    )))
}

pub(super) fn validate_replay_coupling_operation(
    coupling_id: &str,
    operation_name: &str,
    manifest: &verlet_abi::WasmOperationManifest,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let operation = manifest.operation(operation_name).ok_or_else(|| {
        crate::cli::usage_error(format!(
            "replay artifact does not expose operation {operation_name:?} for coupling {coupling_id:?}"
        ))
    })?;
    if operation.input != verlet_abi::WasmOperationValueKind::Json {
        return Err(crate::cli::usage_error(format!(
            "coupling {coupling_id:?} operation {operation_name:?} must declare json input for {COUPLING_INVOCATION_ABI}",
            COUPLING_INVOCATION_ABI = verlet_abi::COUPLING_INVOCATION_ABI
        )));
    }
    if operation.output != verlet_abi::WasmOperationValueKind::Json {
        return Err(crate::cli::usage_error(format!(
            "coupling {coupling_id:?} operation {operation_name:?} must declare json output for {COUPLING_DISCHARGE_ABI}",
            COUPLING_DISCHARGE_ABI = verlet_abi::COUPLING_DISCHARGE_ABI
        )));
    }
    if !operation.required_capabilities.is_empty() {
        return Err(crate::cli::usage_error(format!(
            "coupling {coupling_id:?} operation {operation_name:?} declares effect capabilities; replay couplings must be pure compute"
        )));
    }
    Ok(())
}

pub(super) async fn load_replay_recorded_events(
    options: &CouplingRunArgs,
) -> crate::kernel::runtime_host::VerletResult<Vec<verlet_history::EventRecord>> {
    match (&options.journal, &options.thread_id, &options.export_bundle) {
        (Some(_), _, Some(_)) => Err(crate::cli::usage_error(
            "coupling run --replay accepts either --journal/--thread-id or --export, not both",
        )),
        (Some(journal), Some(thread_id), None) => {
            let store = verlet_history_sqlite::SqliteSessionStore::open_read_only(journal)
                .await
                .map_err(|err| {
                    let message = err.to_string();
                    if crate::cli::secret::turso_cross_process_lock_error(&message) {
                        crate::cli::secret::cross_process_database_guidance(
                            "stop the daemon and retry",
                        )
                    } else {
                        crate::cli::usage_error(format!("failed to open journal read-only: {err}"))
                    }
                })?;
            let events = store.list_thread_events(*thread_id).await.map_err(|err| {
                crate::cli::usage_error(format!(
                    "failed to read recorded events for thread {thread_id}: {err}"
                ))
            })?;
            if events.is_empty() {
                return Err(crate::cli::usage_error(format!(
                    "journal {} has no events for thread {thread_id}",
                    journal.display()
                )));
            }
            Ok(events)
        }
        (Some(_), None, None) => Err(crate::cli::usage_error(
            "coupling run --replay with --journal requires --thread-id",
        )),
        (None, _, Some(export)) => load_replay_events_from_export(export),
        (None, _, None) => Err(crate::cli::usage_error(
            "coupling run --replay requires --journal/--thread-id or --export",
        )),
    }
}

pub(super) fn load_replay_events_from_export(
    path: &std::path::Path,
) -> crate::kernel::runtime_host::VerletResult<Vec<verlet_history::EventRecord>> {
    let value = read_json_file(path)?;
    let streams = value
        .get("streams")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| crate::cli::usage_error("export bundle missing streams array"))?;
    let mut events = Vec::new();
    for stream in streams {
        let data = stream
            .get("data")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| crate::cli::usage_error("export bundle stream missing data array"))?;
        for event in data {
            events.push(event_record_from_export_value(event.clone())?);
        }
    }
    events.sort_by(|left, right| {
        (
            left.created_at_ms,
            left.stream_id.to_string(),
            left.sequence.get(),
            left.id.to_string(),
        )
            .cmp(&(
                right.created_at_ms,
                right.stream_id.to_string(),
                right.sequence.get(),
                right.id.to_string(),
            ))
    });
    if events.is_empty() {
        return Err(crate::cli::usage_error(
            "export bundle contains no replayable events",
        ));
    }
    Ok(events)
}

pub(super) fn event_record_from_export_value(
    value: serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<verlet_history::EventRecord> {
    let envelope = serde_json::from_value::<verlet_history::StreamRecordEnvelopeV1>(value)
        .map_err(|err| {
            crate::cli::usage_error(format!("export stream record is invalid: {err}"))
        })?;
    let kind = verlet_history::EventKind::try_from(envelope.kind).map_err(|err| {
        crate::cli::usage_error(format!("export stream record kind is invalid: {err}"))
    })?;
    Ok(verlet_history::EventRecord {
        id: envelope.event_id,
        stream_id: envelope.stream_id,
        sequence: envelope.sequence,
        coordinates: envelope.coordinates,
        created_at_ms: envelope.created_at_ms,
        kind,
        origin: envelope.origin,
        provenance: envelope.provenance,
        payload: envelope.payload,
    })
}

pub(super) async fn replay_coupling_events(
    coupling_set: &crate::agent::manifest_bind::BoundCouplingSet,
    recorded_events: Vec<verlet_history::EventRecord>,
    operation_registry_root: std::path::PathBuf,
) -> crate::kernel::runtime_host::VerletResult<
    crate::kernel::coupling_scheduler::CouplingSchedulerCycleReceipt,
> {
    let replay_store = CouplingReplayEventStore::default();
    let executor = crate::kernel::coupling_executor_registry::CouplingExecutorRegistry::new(Some(
        operation_registry_root,
    ));
    let scheduler =
        crate::kernel::coupling_scheduler::CouplingScheduler::new(&replay_store, &executor);
    let mut aggregate = crate::kernel::coupling_scheduler::CouplingSchedulerCycleReceipt {
        snapshot_id: coupling_set.snapshot_id.clone(),
        runs: Vec::new(),
        appended_events: Vec::new(),
    };
    for event in recorded_events {
        let appended = replay_store
            .append_recorded_event(event)
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
        if appended.is_empty() {
            continue;
        }
        let receipt = scheduler.run_batch(coupling_set, appended).await?;
        aggregate.runs.extend(receipt.runs);
        aggregate.appended_events.extend(receipt.appended_events);
    }
    Ok(aggregate)
}

pub(super) fn read_json_file(
    path: &std::path::Path,
) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
    let raw = std::fs::read_to_string(path).map_err(|err| {
        crate::cli::usage_error(format!("failed to read {}: {err}", path.display()))
    })?;
    serde_json::from_str(&raw).map_err(|err| {
        crate::cli::usage_error(format!("failed to parse {} as JSON: {err}", path.display()))
    })
}

pub(super) fn parse_pinned_operation_ref(
    reference: &str,
) -> crate::kernel::runtime_host::VerletResult<ParsedOperationRef> {
    let body = reference
        .strip_prefix("op://")
        .ok_or_else(|| crate::cli::usage_error("operation ref must start with op://"))?;
    let (name_part, artifact_hash) = body.split_once("@sha256:").ok_or_else(|| {
        crate::cli::usage_error(
            "operation ref must be pinned as op://<record>/<operation>@sha256:<hash>",
        )
    })?;
    if artifact_hash.len() != 64 || !artifact_hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(crate::cli::usage_error(
            "operation ref has an invalid sha256 artifact hash",
        ));
    }
    let segments = name_part.split('/').collect::<Vec<_>>();
    let (name, operation) = match segments.as_slice() {
        [name] if !name.is_empty() => ((*name).to_string(), None),
        [name, operation] if !name.is_empty() && !operation.is_empty() => {
            ((*name).to_string(), Some((*operation).to_string()))
        }
        _ => {
            return Err(crate::cli::usage_error(
                "operation ref must match op://<record>@sha256:<hash> or op://<record>/<operation>@sha256:<hash>",
            ));
        }
    };
    Ok(ParsedOperationRef {
        name,
        operation,
        artifact_hash: artifact_hash.to_string(),
    })
}

pub(super) fn print_coupling_replay_report(report: &CouplingReplayReport) {
    println!("DRY RUN ONLY: proposed coupling discharges; no stream was appended.");
    println!("mode {}", report.mode);
    println!("snapshot {}", report.snapshot_id);
    println!("replayed_events {}", report.replayed_event_count);
    println!("runs {}", report.runs.len());
    for run in &report.runs {
        if run.blocked {
            println!(
                "run {} BLOCKED {} trigger={} sequence={}",
                run.coupling_id,
                run.reason.as_deref().unwrap_or("blocked"),
                run.trigger_event_id,
                run.trigger_sequence
            );
        } else {
            println!(
                "run {} {} trigger={} sequence={} proposals={}",
                run.coupling_id,
                run.status,
                run.trigger_event_id,
                run.trigger_sequence,
                run.proposal_event_count
            );
        }
    }
    println!("proposal_events {}", report.proposal_events.len());
    for event in &report.proposal_events {
        println!(
            "proposal stream={} kind={} payload={}",
            event.stream,
            event.kind,
            crate::cli::tool::compact_json(&event.payload)
        );
    }
}

#[derive(Debug)]
pub(super) struct CouplingInitArgs {
    name: Option<String>,
    out_path: Option<std::path::PathBuf>,
    force: bool,
    help: bool,
}

#[derive(Debug)]
pub(super) struct CouplingRunArgs {
    replay: bool,
    artifact: Option<String>,
    coupling_file: Option<std::path::PathBuf>,
    coupling_id: Option<String>,
    journal: Option<std::path::PathBuf>,
    thread_id: Option<verlet_runtime_contracts::ThreadId>,
    export_bundle: Option<std::path::PathBuf>,
    registry_root: Option<std::path::PathBuf>,
    json: bool,
    help: bool,
}

#[derive(Debug)]
pub(super) struct ParsedOperationRef {
    name: String,
    operation: Option<String>,
    artifact_hash: String,
}

#[derive(Default)]
pub(super) struct CouplingReplayEventStore {
    streams: std::sync::Mutex<std::collections::BTreeMap<String, Vec<verlet_history::EventRecord>>>,
}

impl CouplingReplayEventStore {
    fn append_recorded_event(
        &self,
        event: verlet_history::EventRecord,
    ) -> verlet_history::HistoryResult<Vec<verlet_history::EventRecord>> {
        let mut streams = self.streams.lock().map_err(|err| {
            verlet_history::HistoryError::Storage(format!(
                "replay event store lock poisoned: {err}"
            ))
        })?;
        let stream = streams.entry(event.stream_id.to_string()).or_default();
        if stream.iter().any(|existing| existing.id == event.id) {
            return Ok(Vec::new());
        }
        stream.push(event.clone());
        stream.sort_by(|left, right| {
            (left.sequence.get(), left.id.to_string())
                .cmp(&(right.sequence.get(), right.id.to_string()))
        });
        Ok(vec![event])
    }
}

#[async_trait::async_trait]
impl verlet_history::EventStore for CouplingReplayEventStore {
    async fn append_events(
        &self,
        stream_id: &verlet_history::EventStreamId,
        records: Vec<verlet_history::NewEventRecord>,
    ) -> verlet_history::HistoryResult<Vec<verlet_history::EventRecord>> {
        let mut streams = self.streams.lock().map_err(|err| {
            verlet_history::HistoryError::Storage(format!(
                "replay event store lock poisoned: {err}"
            ))
        })?;
        let stream = streams.entry(stream_id.to_string()).or_default();
        let mut next_sequence = stream
            .iter()
            .map(|event| event.sequence.get())
            .max()
            .unwrap_or(0)
            + 1;
        let mut appended = Vec::with_capacity(records.len());
        for record in records {
            let event = verlet_history::EventRecord::from_new(
                stream_id.clone(),
                verlet_history::EventSequence::new(next_sequence),
                record,
            );
            next_sequence += 1;
            stream.push(event.clone());
            appended.push(event);
        }
        Ok(appended)
    }

    async fn read_events(
        &self,
        stream_id: &verlet_history::EventStreamId,
        from_sequence: Option<verlet_history::EventSequence>,
    ) -> verlet_history::HistoryResult<Vec<verlet_history::EventRecord>> {
        let streams = self.streams.lock().map_err(|err| {
            verlet_history::HistoryError::Storage(format!(
                "replay event store lock poisoned: {err}"
            ))
        })?;
        let events = streams
            .get(&stream_id.to_string())
            .cloned()
            .unwrap_or_default();
        Ok(match from_sequence {
            Some(sequence) => events
                .into_iter()
                .filter(|event| event.sequence.get() >= sequence.get())
                .collect(),
            None => events,
        })
    }
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CouplingReplayReport {
    schema: &'static str,
    mode: &'static str,
    dry_run: bool,
    snapshot_id: String,
    replayed_event_count: usize,
    runs: Vec<CouplingReplayRunReport>,
    proposal_events: Vec<CouplingReplayProposalEvent>,
}

impl CouplingReplayReport {
    fn from_receipt(
        replayed_event_count: usize,
        coupling_set: &crate::agent::manifest_bind::BoundCouplingSet,
        receipt: crate::kernel::coupling_scheduler::CouplingSchedulerCycleReceipt,
    ) -> Self {
        let mut proposal_ids = std::collections::BTreeSet::new();
        let mut proposal_streams = std::collections::BTreeMap::new();
        for run in &receipt.runs {
            let stream = coupling_set
                .couplings
                .iter()
                .find(|coupling| coupling.id == run.coupling_id)
                .map(|coupling| coupling.sink.stream.clone())
                .unwrap_or_else(|| run.trigger_stream_id.clone());
            for event_id in &run.discharged_event_ids {
                let id = event_id.to_string();
                proposal_ids.insert(id.clone());
                proposal_streams.insert(id, stream.clone());
            }
        }
        let proposal_events = receipt
            .appended_events
            .iter()
            .filter_map(|event| {
                let id = event.id.to_string();
                proposal_ids
                    .contains(&id)
                    .then(|| CouplingReplayProposalEvent {
                        stream: proposal_streams
                            .get(&id)
                            .cloned()
                            .unwrap_or_else(|| event.stream_id.to_string()),
                        stream_id: event.stream_id.to_string(),
                        kind: event.kind.to_string(),
                        payload: event.payload.clone(),
                        provenance: event.provenance.clone(),
                    })
            })
            .collect::<Vec<_>>();
        let runs = receipt
            .runs
            .into_iter()
            .map(CouplingReplayRunReport::from_run)
            .collect();
        Self {
            schema: "cooldis.coupling.replay/1",
            mode: "replay",
            dry_run: true,
            snapshot_id: receipt.snapshot_id,
            replayed_event_count,
            runs,
            proposal_events,
        }
    }
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CouplingReplayRunReport {
    coupling_id: String,
    status: String,
    scheduler_status: crate::kernel::coupling_scheduler::CouplingRunStatus,
    blocked: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    trigger_event_id: String,
    trigger_stream_id: String,
    trigger_sequence: i64,
    depth: u32,
    source_event_ids: Vec<String>,
    proposal_event_count: usize,
    budget_spent: crate::kernel::coupling_scheduler::CouplingBudgetSpent,
}

impl CouplingReplayRunReport {
    fn from_run(run: crate::kernel::coupling_scheduler::CouplingRunReceipt) -> Self {
        let blocked = replay_run_is_blocked(&run);
        Self {
            coupling_id: run.coupling_id,
            status: if blocked {
                "blocked".to_string()
            } else {
                run.status.to_string()
            },
            scheduler_status: run.status,
            blocked,
            reason: run.reason,
            trigger_event_id: run.trigger_event_id.to_string(),
            trigger_stream_id: run.trigger_stream_id,
            trigger_sequence: run.trigger_sequence,
            depth: run.depth,
            source_event_ids: run
                .source_event_ids
                .into_iter()
                .map(|event_id| event_id.to_string())
                .collect(),
            proposal_event_count: run.discharged_event_ids.len(),
            budget_spent: run.budget_spent,
        }
    }
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CouplingReplayProposalEvent {
    stream: String,
    stream_id: String,
    kind: String,
    payload: serde_json::Value,
    provenance: verlet_history::EventProvenance,
}

pub(super) fn replay_run_is_blocked(
    run: &crate::kernel::coupling_scheduler::CouplingRunReceipt,
) -> bool {
    run.reason.as_deref() == Some("quota_exhausted")
        || run
            .reason
            .as_deref()
            .is_some_and(|reason| reason.starts_with("budget:"))
}

pub(super) fn parse_coupling_init_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<CouplingInitArgs> {
    let mut name = None;
    let mut out_path = None;
    let mut force = false;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--out" => out_path = Some(crate::cli::tool::required_path_value(&mut iter, "--out")?),
            "--force" => force = true,
            other if other.starts_with('-') => {
                return Err(crate::cli::usage_error(format!(
                    "unknown coupling init argument {other:?}"
                )));
            }
            _ => {
                if name.is_some() {
                    return Err(crate::cli::usage_error(
                        "coupling init accepts exactly one <name>",
                    ));
                }
                name = Some(arg.to_string_lossy().to_string());
            }
        }
    }
    Ok(CouplingInitArgs {
        name,
        out_path,
        force,
        help,
    })
}

pub(super) fn parse_coupling_run_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<CouplingRunArgs> {
    let mut replay = false;
    let mut artifact = None;
    let mut coupling_file = None;
    let mut coupling_id = None;
    let mut journal = None;
    let mut thread_id = None;
    let mut export_bundle = None;
    let mut registry_root = None;
    let mut json = false;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--replay" => replay = true,
            "--artifact" => {
                artifact = Some(crate::cli::tool::required_string_value(
                    &mut iter,
                    "--artifact",
                )?)
            }
            "--coupling-file" => {
                coupling_file = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--coupling-file",
                )?)
            }
            "--coupling-id" => {
                coupling_id = Some(crate::cli::tool::required_string_value(
                    &mut iter,
                    "--coupling-id",
                )?)
            }
            "--journal" | "--db" => {
                journal = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--journal",
                )?)
            }
            "--thread-id" | "--thread" => {
                let value = crate::cli::tool::required_string_value(&mut iter, "--thread-id")?;
                thread_id = Some(
                    verlet_runtime_contracts::ThreadId::parse_str(&value).map_err(|err| {
                        crate::cli::usage_error(format!("invalid --thread-id: {err}"))
                    })?,
                );
            }
            "--export" => {
                export_bundle = Some(crate::cli::tool::required_path_value(
                    &mut iter, "--export",
                )?)
            }
            "--registry-root" => {
                registry_root = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--registry-root",
                )?)
            }
            "--json" => json = true,
            other if other.starts_with('-') => {
                return Err(crate::cli::usage_error(format!(
                    "unknown coupling run argument {other:?}"
                )));
            }
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unexpected coupling run positional argument {other:?}; use --artifact"
                )));
            }
        }
    }
    Ok(CouplingRunArgs {
        replay,
        artifact,
        coupling_file,
        coupling_id,
        journal,
        thread_id,
        export_bundle,
        registry_root,
        json,
        help,
    })
}

pub(super) fn write_coupling_project(
    name: &str,
    root: &std::path::Path,
    force: bool,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let package_name = verlet_operations::operation_store::validate_record_name(name)?;
    let operation_name = coupling_operation_name(&package_name)?;
    let cargo_toml_path = root.join("Cargo.toml");
    let lib_path = root.join("src/lib.rs");
    let package_path = root.join("verlet.tool.toml");
    let input_schema_path = root.join("schemas/coupling_invocation.input.json");
    let output_schema_path = root.join("schemas/coupling_discharge.output.json");
    let fixture_input_path = root.join("fixtures/invocation.json");
    let fixture_expect_path = root.join("fixtures/expect.discharge.json");
    let files = [
        cargo_toml_path.as_path(),
        lib_path.as_path(),
        package_path.as_path(),
        input_schema_path.as_path(),
        output_schema_path.as_path(),
        fixture_input_path.as_path(),
        fixture_expect_path.as_path(),
    ];
    if !force {
        for path in files {
            if path.exists() {
                return Err(crate::cli::usage_error(format!(
                    "coupling scaffold file {} already exists; pass --force to replace it",
                    path.display()
                )));
            }
        }
    }
    std::fs::create_dir_all(root.join("src")).map_err(crate::cli::io_error)?;
    std::fs::create_dir_all(root.join("schemas")).map_err(crate::cli::io_error)?;
    std::fs::create_dir_all(root.join("fixtures")).map_err(crate::cli::io_error)?;
    std::fs::write(&cargo_toml_path, render_coupling_cargo_toml(&package_name)?)
        .map_err(crate::cli::io_error)?;
    std::fs::write(&lib_path, render_coupling_lib_rs(&operation_name))
        .map_err(crate::cli::io_error)?;
    std::fs::write(
        &package_path,
        render_coupling_tool_manifest(&package_name, &operation_name),
    )
    .map_err(crate::cli::io_error)?;
    std::fs::write(&input_schema_path, COUPLING_INVOCATION_SCHEMA).map_err(crate::cli::io_error)?;
    std::fs::write(&output_schema_path, COUPLING_DISCHARGE_SCHEMA).map_err(crate::cli::io_error)?;
    std::fs::write(
        &fixture_input_path,
        render_coupling_fixture_input(&package_name),
    )
    .map_err(crate::cli::io_error)?;
    std::fs::write(
        &fixture_expect_path,
        render_coupling_fixture_expect(&package_name),
    )
    .map_err(crate::cli::io_error)
}

pub(super) fn render_coupling_cargo_toml(
    name: &str,
) -> crate::kernel::runtime_host::VerletResult<String> {
    let crate_name = coupling_crate_name(name)?;
    let sdk_path = guest_sdk_dependency_path();
    Ok(format!(
        "[package]\n\
name = {}\n\
version = \"0.1.0\"\n\
edition = \"2024\"\n\
publish = false\n\
\n\
[workspace]\n\
\n\
[lib]\n\
crate-type = [\"cdylib\"]\n\
\n\
[dependencies]\n\
verlet-guest-sdk = {{ path = {} }}\n\
serde = {{ version = \"1\", features = [\"derive\"] }}\n\
serde_json = \"1\"\n\
\n\
[profile.release]\n\
panic = \"abort\"\n",
        crate::cli::agent::toml_string(&crate_name),
        crate::cli::agent::toml_string(&sdk_path.display().to_string()),
    ))
}

pub(super) fn render_coupling_lib_rs(operation_name: &str) -> String {
    COUPLING_LIB_RS_TEMPLATE.replace("__OPERATION_NAME__", operation_name)
}

pub(super) fn render_coupling_tool_manifest(name: &str, operation_name: &str) -> String {
    format!(
        "kind = \"cooldis.tool\"\n\
schema_version = 0\n\
\n\
[identity]\n\
name = {}\n\
version = \"0.1.0\"\n\
description = \"Fold coupling invocation events into deterministic derived events.\"\n\
\n\
[runtime]\n\
kind = \"wasm32-unknown-unknown\"\n\
module_path = \".\"\n\
state = \"stateless\"\n\
release = true\n\
max_input_bytes = 262144\n\
max_output_bytes = 262144\n\
\n\
[[operations]]\n\
name = {}\n\
description = \"Emit one placement decision every configured number of selected events.\"\n\
input_schema = \"schemas/coupling_invocation.input.json\"\n\
output_schema = \"schemas/coupling_discharge.output.json\"\n\
required_capabilities = []\n\
\n\
[operations.command]\n\
name = {}\n\
stdin = \"json\"\n\
stdout = \"json\"\n\
\n\
[operations.mcp]\n\
tool_name = {}\n\
\n\
[[fixtures]]\n\
name = \"three_events\"\n\
operation = {}\n\
input = \"fixtures/invocation.json\"\n\
expect = \"fixtures/expect.discharge.json\"\n",
        crate::cli::agent::toml_string(name),
        crate::cli::agent::toml_string(operation_name),
        crate::cli::agent::toml_string(&format!("{name} {operation_name}")),
        crate::cli::agent::toml_string(operation_name),
        crate::cli::agent::toml_string(operation_name),
    )
}

pub(super) fn render_coupling_fixture_input(name: &str) -> String {
    format!(
        "{{\n\
  \"abi\": \"cooldis.coupling.invocation/0.1\",\n\
  \"trigger_event\": {{\n\
    \"id\": \"event-3\",\n\
    \"stream_id\": \"thread:session\",\n\
    \"sequence\": 3,\n\
    \"kind\": \"turn.completed\",\n\
    \"origin\": \"witnessed\",\n\
    \"payload\": {{}}\n\
  }},\n\
  \"selected_events\": [\n\
    {{\"id\": \"event-1\", \"stream_id\": \"thread:session\", \"sequence\": 1, \"kind\": \"turn.completed\", \"origin\": \"witnessed\", \"payload\": {{}}}},\n\
    {{\"id\": \"event-2\", \"stream_id\": \"thread:session\", \"sequence\": 2, \"kind\": \"turn.completed\", \"origin\": \"witnessed\", \"payload\": {{}}}},\n\
    {{\"id\": \"event-3\", \"stream_id\": \"thread:session\", \"sequence\": 3, \"kind\": \"turn.completed\", \"origin\": \"witnessed\", \"payload\": {{}}}}\n\
  ],\n\
  \"config\": {{\n\
    \"every\": 3,\n\
    \"sink_stream\": \"derived:counter\",\n\
    \"sink_kind\": \"placement.decision\"\n\
  }},\n\
  \"invocation_meta\": {{\n\
    \"coupling_id\": {},\n\
    \"thread_id\": \"session\",\n\
    \"depth\": 0\n\
  }}\n\
}}\n",
        crate::cli::agent::toml_string(name),
    )
}

pub(super) fn render_coupling_fixture_expect(name: &str) -> String {
    format!(
        "{{\n\
  \"abi\": \"cooldis.coupling.discharge/0.1\",\n\
  \"events\": [\n\
    {{\n\
      \"stream\": \"derived:counter\",\n\
      \"kind\": \"placement.decision\",\n\
      \"payload\": {{\n\
        \"schema\": \"cooldis.scaffold.counter_fold/1\",\n\
        \"count\": 3,\n\
        \"trigger_event_id\": \"event-3\",\n\
        \"coupling_id\": {}\n\
      }}\n\
    }}\n\
  ]\n\
}}\n",
        crate::cli::agent::toml_string(name),
    )
}

pub(super) fn coupling_operation_name(
    name: &str,
) -> crate::kernel::runtime_host::VerletResult<String> {
    let mut operation = String::new();
    for byte in name.bytes() {
        match byte {
            b'A'..=b'Z' => operation.push((byte as char).to_ascii_lowercase()),
            b'a'..=b'z' | b'0'..=b'9' | b'_' => operation.push(byte as char),
            b'-' | b'.' => operation.push('_'),
            _ => {}
        }
    }
    if operation
        .as_bytes()
        .first()
        .is_none_or(|byte| !byte.is_ascii_alphabetic() && *byte != b'_')
    {
        operation.insert_str(0, "coupling_");
    }
    verlet_operations::operation_store::validate_record_name(&operation)?;
    Ok(operation)
}

pub(super) fn coupling_crate_name(name: &str) -> crate::kernel::runtime_host::VerletResult<String> {
    let mut suffix = String::new();
    for byte in name.bytes() {
        match byte {
            b'A'..=b'Z' => suffix.push((byte as char).to_ascii_lowercase()),
            b'a'..=b'z' | b'0'..=b'9' => suffix.push(byte as char),
            b'-' | b'_' | b'.' => suffix.push('-'),
            _ => {}
        }
    }
    let suffix = suffix.trim_matches('-');
    if suffix.is_empty() {
        return Err(crate::cli::usage_error(
            "coupling name does not produce a Cargo package name",
        ));
    }
    Ok(format!("verlet-coupling-{suffix}"))
}

pub(super) fn guest_sdk_dependency_path() -> std::path::PathBuf {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../verlet-guest-sdk");
    path.canonicalize().unwrap_or(path)
}

pub(super) const COUPLING_LIB_RS_TEMPLATE: &str = r#"use verlet_guest_sdk::prelude::*;
use serde_json::json;

#[derive(Deserialize)]
struct CouplingConfig {
    #[serde(default = "default_every")]
    every: u64,
    #[serde(default = "default_sink_stream")]
    sink_stream: String,
    #[serde(default = "default_sink_kind")]
    sink_kind: String,
}

#[coupling]
pub fn __OPERATION_NAME__(ctx: CouplingContext) -> Result<Discharge, GuestError> {
    let config: CouplingConfig = ctx.config()?;
    let every = config.every.max(1);
    let count = ctx.sources().len() as u64;
    if count == 0 || count % every != 0 {
        return Ok(Discharge::empty());
    }
    Discharge::empty().event(
        config.sink_stream,
        config.sink_kind,
        json!({
            "schema": "cooldis.scaffold.counter_fold/1",
            "count": count,
            "trigger_event_id": ctx.trigger().id.clone(),
            "coupling_id": ctx.meta().coupling_id.clone(),
        }),
    )
}

fn default_every() -> u64 {
    3
}

fn default_sink_stream() -> String {
    "derived:counter".to_string()
}

fn default_sink_kind() -> String {
    "placement.decision".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use verlet_guest_sdk::testkit;

    #[test]
    fn fixture_runs_natively_without_wasm_host() {
        let invocation =
            testkit::invocation_from_fixture_file("fixtures/invocation.json").unwrap();
        let discharge = testkit::invoke_coupling(__OPERATION_NAME__, invocation).unwrap();
        assert_eq!(discharge.events.len(), 1);
        assert_eq!(discharge.events[0].stream, "derived:counter");
        assert_eq!(discharge.events[0].kind, "placement.decision");
        assert_eq!(discharge.events[0].payload["count"], 3);
    }
}
"#;

pub(super) const COUPLING_INVOCATION_SCHEMA: &str = r#"{
  "type": "object",
  "required": ["abi", "trigger_event", "invocation_meta"],
  "properties": {
    "abi": { "enum": ["cooldis.coupling.invocation/0.1"] },
    "trigger_event": { "type": "object", "additionalProperties": true },
    "selected_events": {
      "type": "array",
      "items": { "type": "object", "additionalProperties": true }
    },
    "config": { "type": "object", "additionalProperties": true },
    "invocation_meta": { "type": "object", "additionalProperties": true }
  },
  "additionalProperties": true
}
"#;

pub(super) const COUPLING_DISCHARGE_SCHEMA: &str = r#"{
  "type": "object",
  "required": ["abi", "events"],
  "properties": {
    "abi": { "enum": ["cooldis.coupling.discharge/0.1"] },
    "events": {
      "type": "array",
      "items": { "type": "object", "additionalProperties": true }
    }
  },
  "additionalProperties": true
}
"#;

pub(super) fn print_coupling_help() {
    println!(
        "verlet coupling\n\
\n\
Usage:\n\
  verlet coupling init <name> [--out <dir>] [--force]\n\
  verlet coupling run --replay --artifact <path|op://ref> --coupling-file <file> (--thread-id <id> --journal <db>|--export <bundle>) [--coupling-id <id>] [--registry-root .verlet/operations] [--json]\n\
\n\
Couplings are event-stream edges. `init` scaffolds a Rust Wasm coupling\n\
package that uses #[coupling] and validates through `verlet tool build\n\
--package`. `run --replay` runs a coupling artifact against a recorded\n\
thread in dry-run mode and prints proposed discharges without appending to\n\
the source journal.\n"
    );
}

pub(super) fn print_coupling_init_help() {
    println!(
        "verlet coupling init\n\
\n\
Usage:\n\
  verlet coupling init <name> [--out <dir>] [--force]\n\
\n\
Writes a macro-authored coupling crate with verlet.tool.toml, schemas,\n\
fixtures, and one native testkit test.\n"
    );
}

pub(super) fn print_coupling_run_help() {
    println!(
        "verlet coupling run\n\
\n\
Usage:\n\
  verlet coupling run --replay --artifact <path|op://ref> --coupling-file <file> (--thread-id <id> --journal <db>|--export <bundle>) [--coupling-id <id>] [--registry-root .verlet/operations] [--json]\n\
\n\
Replays recorded thread events through the bound coupling trigger, selector,\n\
quota, budget, and Wasm execution path. Output is proposals only; no source\n\
journal stream is appended. --json emits stable machine-readable replay\n\
receipts for tests and scripts.\n"
    );
}
