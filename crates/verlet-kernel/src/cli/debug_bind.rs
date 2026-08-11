//! Receipt-backed effective bind inspection.

use chrono::TimeZone as _;
#[cfg(test)]
mod tests;

#[derive(Debug)]
pub(super) struct DebugBindArgs {
    thread_id: String,
    json: bool,
    journal: Option<std::path::PathBuf>,
    endpoint: crate::cli::debug_rpc::DebugRpcEndpointArgs,
}

/// A projection of recorded compile and bind receipts. Under the receipt law
/// in `formalism/lexicon.md`, this explanation never recomputes resolution or
/// fills gaps from current configuration; absent receipt facts remain
/// unrecorded.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct BindExplanation {
    pub thread_id: String,
    pub manifest: BindManifestExplanation,
    pub model: BindModelExplanation,
    pub placement: Option<BindPlacementExplanation>,
    pub workspace: Option<BindWorkspaceExplanation>,
    pub runtime: Vec<BindRuntimeExplanation>,
    pub tools: Vec<BindToolExplanation>,
    pub universes: Vec<BindUniverseExplanation>,
    pub couplings: Vec<BindCouplingExplanation>,
    pub skills: Vec<BindSkillExplanation>,
    pub context: Vec<BindContextExplanation>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct BindManifestExplanation {
    pub ref_uri: String,
    pub manifest_hash: String,
    pub source_hash: String,
    pub alias: Option<BindAliasExplanation>,
    pub compile_event_id: String,
    pub bind_event_id: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct BindAliasExplanation {
    pub alias: String,
    pub version: String,
    pub resolved_at: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct BindModelExplanation {
    pub profile_id: String,
    pub provider_id: String,
    pub model_id: String,
    pub origin: Option<crate::agent::manifest_bind::AgentManifestModelProfileOrigin>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct BindPlacementExplanation {
    pub target: String,
    pub executor_ref: Option<String>,
    pub origin: Option<crate::agent::manifest_bind::AgentManifestBindingOrigin>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct BindWorkspaceExplanation {
    pub guest_path: std::path::PathBuf,
    pub host_path: std::path::PathBuf,
    pub mode: String,
    pub origin: Option<crate::agent::manifest_bind::AgentManifestBindingOrigin>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct BindRuntimeExplanation {
    pub key: String,
    pub value: serde_json::Value,
    pub overridden: bool,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct BindToolExplanation {
    pub tool_id: String,
    pub origin: Option<String>,
    pub row_id: Option<String>,
    pub pinned: bool,
    pub operation_name: Option<String>,
    pub artifact_hash: Option<String>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct BindUniverseExplanation {
    pub import_id: String,
    pub server_ref: String,
    pub discovery_hash: String,
    pub tool_count: usize,
    pub pinned_count: usize,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct BindCouplingExplanation {
    pub id: String,
    pub role: String,
    pub function_ref: String,
    pub artifact_hash: String,
    pub config_hash: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct BindSkillExplanation {
    pub package: String,
    pub artifact_hash: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct BindContextExplanation {
    pub ref_uri: String,
    pub content_sha256: String,
}

#[derive(Clone, Debug)]
struct RecordedReceiptEvent {
    event_id: String,
    sequence: i64,
    kind: String,
    source_event_ids: Vec<String>,
    payload: serde_json::Value,
}

pub(super) async fn run_debug_bind(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_debug_bind_help();
        return Ok(());
    }
    let options = parse_debug_bind_args(args)?;
    let events = match &options.journal {
        Some(journal) => load_debug_bind_journal_events(journal, &options.thread_id).await?,
        None => load_debug_bind_daemon_events(&options.endpoint, &options.thread_id).await?,
    };
    let (compile_event, bind_event) = active_receipt_events(&events)?;
    let compile: crate::agent::manifest_bind::AgentManifestCompileReceipt =
        serde_json::from_value(compile_event.payload.clone()).map_err(|err| {
            crate::cli::usage_error(format!(
                "manifest.compile.completed payload is invalid: {err}"
            ))
        })?;
    let bind: crate::agent::manifest_bind::AgentManifestBindReceipt =
        serde_json::from_value(bind_event.payload.clone()).map_err(|err| {
            crate::cli::usage_error(format!("manifest.bind.completed payload is invalid: {err}"))
        })?;
    let explanation = assemble_bind_explanation(
        &options.thread_id,
        &compile_event.event_id,
        &bind_event.event_id,
        &compile,
        &bind,
    )?;
    if options.json {
        serde_json::to_writer_pretty(std::io::stdout(), &explanation).map_err(|err| {
            crate::cli::usage_error(format!("failed to encode bind explanation: {err}"))
        })?;
        println!();
    } else {
        print!("{}", render_bind_explanation(&explanation));
    }
    Ok(())
}

pub(super) fn parse_debug_bind_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<DebugBindArgs> {
    let mut endpoint = crate::cli::debug_rpc::DebugRpcEndpointArgs {
        url: None,
        config: None,
    };
    let mut journal = None;
    let mut json = false;
    let mut positionals = Vec::new();
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--json" => json = true,
            "--url" => endpoint.url = Some(required_debug_bind_string(&mut iter, "--url")?),
            "--config" => endpoint.config = Some(required_debug_bind_path(&mut iter, "--config")?),
            "--journal" => journal = Some(required_debug_bind_path(&mut iter, "--journal")?),
            other if other.starts_with('-') => {
                return Err(debug_bind_usage_error(format!(
                    "unknown debug bind argument {other:?}"
                )));
            }
            _ => positionals.push(arg.to_string_lossy().to_string()),
        }
    }
    if positionals.len() != 1 {
        return Err(debug_bind_usage_error(
            "verlet debug bind requires exactly one <thread-id>",
        ));
    }
    let endpoint_count = usize::from(endpoint.url.is_some())
        + usize::from(endpoint.config.is_some())
        + usize::from(journal.is_some());
    if endpoint_count > 1 {
        return Err(debug_bind_usage_error(
            "verlet debug bind accepts --url, --config, or --journal, not more than one",
        ));
    }
    Ok(DebugBindArgs {
        thread_id: positionals.remove(0),
        json,
        journal,
        endpoint,
    })
}

fn required_debug_bind_string(
    iter: &mut impl Iterator<Item = std::ffi::OsString>,
    flag: &'static str,
) -> crate::kernel::runtime_host::VerletResult<String> {
    required_debug_bind_value(iter, flag).map(|value| value.to_string_lossy().to_string())
}

fn required_debug_bind_path(
    iter: &mut impl Iterator<Item = std::ffi::OsString>,
    flag: &'static str,
) -> crate::kernel::runtime_host::VerletResult<std::path::PathBuf> {
    required_debug_bind_value(iter, flag).map(std::path::PathBuf::from)
}

fn required_debug_bind_value(
    iter: &mut impl Iterator<Item = std::ffi::OsString>,
    flag: &'static str,
) -> crate::kernel::runtime_host::VerletResult<std::ffi::OsString> {
    let value = iter
        .next()
        .ok_or_else(|| crate::cli::usage_error(format!("{flag} requires a value")))?;
    if value.to_string_lossy().starts_with('-') {
        return Err(crate::cli::usage_error(format!("{flag} requires a value")));
    }
    Ok(value)
}

async fn load_debug_bind_daemon_events(
    endpoint: &crate::cli::debug_rpc::DebugRpcEndpointArgs,
    thread_id: &str,
) -> crate::kernel::runtime_host::VerletResult<Vec<RecordedReceiptEvent>> {
    let url = crate::cli::debug_rpc::resolve_debug_rpc_endpoint(endpoint)?;
    let mut client = crate::cli::debug_rpc::connect_debug_rpc_client(&url).await?;
    let mut cursor: Option<String> = None;
    let mut events = Vec::new();
    let compile_kind = verlet_history::EventKind::ManifestCompileCompleted;
    let bind_kind = verlet_history::EventKind::ManifestBindCompleted;
    let compile_kind_name: &str = compile_kind.as_ref();
    let bind_kind_name: &str = bind_kind.as_ref();
    loop {
        let mut params = serde_json::json!({
            "threadId": thread_id,
            "limit": 500,
            "kinds": [compile_kind_name, bind_kind_name],
        });
        if let Some(cursor) = cursor.as_ref() {
            params["cursor"] = serde_json::Value::String(cursor.clone());
        }
        let result = client.request("thread/events/list", params).await?;
        let page = result["data"].as_array().ok_or_else(|| {
            crate::cli::usage_error("thread/events/list response missing data array")
        })?;
        events.extend(
            page.iter()
                .map(recorded_receipt_event_from_rpc)
                .collect::<crate::kernel::runtime_host::VerletResult<Vec<_>>>()?,
        );
        cursor = result["cursor"].as_str().map(str::to_string);
        if cursor.is_none() {
            break;
        }
    }
    client.close().await?;
    Ok(events)
}

async fn load_debug_bind_journal_events(
    journal: &std::path::Path,
    thread_id: &str,
) -> crate::kernel::runtime_host::VerletResult<Vec<RecordedReceiptEvent>> {
    let thread_id = verlet_runtime_contracts::ThreadId::parse_str(thread_id)
        .map_err(|err| debug_bind_usage_error(format!("invalid thread id {thread_id:?}: {err}")))?;
    let store = verlet_history_sqlite::SqliteSessionStore::open_read_only(journal)
        .await
        .map_err(|err| {
            crate::cli::usage_error(format!("failed to open journal read-only: {err}"))
        })?;
    let events = store.list_thread_events(thread_id).await.map_err(|err| {
        crate::cli::usage_error(format!(
            "failed to read recorded events for thread {thread_id}: {err}"
        ))
    })?;
    Ok(events
        .into_iter()
        .filter(|event| {
            matches!(
                event.kind,
                verlet_history::EventKind::ManifestCompileCompleted
                    | verlet_history::EventKind::ManifestBindCompleted
            )
        })
        .map(|event| RecordedReceiptEvent {
            event_id: event.id.to_string(),
            sequence: event.sequence.get(),
            kind: event.kind.to_string(),
            source_event_ids: event
                .provenance
                .source_event_ids
                .iter()
                .map(ToString::to_string)
                .collect(),
            payload: event.payload,
        })
        .collect())
}

fn recorded_receipt_event_from_rpc(
    value: &serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<RecordedReceiptEvent> {
    let event_id = value["eventId"]
        .as_str()
        .ok_or_else(|| crate::cli::usage_error("thread/events/list event missing eventId"))?;
    let sequence = value["sequence"]
        .as_i64()
        .ok_or_else(|| crate::cli::usage_error("thread/events/list event missing sequence"))?;
    let kind = value["kind"]
        .as_str()
        .ok_or_else(|| crate::cli::usage_error("thread/events/list event missing kind"))?;
    let source_event_ids = value["provenance"]["source_event_ids"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_string)
        .collect();
    Ok(RecordedReceiptEvent {
        event_id: event_id.to_string(),
        sequence,
        kind: kind.to_string(),
        source_event_ids,
        payload: value["payload"].clone(),
    })
}

fn active_receipt_events(
    events: &[RecordedReceiptEvent],
) -> crate::kernel::runtime_host::VerletResult<(&RecordedReceiptEvent, &RecordedReceiptEvent)> {
    let compile_kind = verlet_history::EventKind::ManifestCompileCompleted;
    let bind_kind = verlet_history::EventKind::ManifestBindCompleted;
    let compile_kind_name: &str = compile_kind.as_ref();
    let bind_kind_name: &str = bind_kind.as_ref();
    let bind = events
        .iter()
        .filter(|event| event.kind == bind_kind_name)
        .max_by_key(|event| event.sequence)
        .ok_or_else(|| {
            crate::cli::usage_error("manifest.bind.completed receipt event was not found")
        })?;
    let compile_id = bind.source_event_ids.first().ok_or_else(|| {
        crate::cli::usage_error("active manifest bind receipt does not witness a compile receipt")
    })?;
    let compile = events
        .iter()
        .find(|event| event.event_id == *compile_id && event.kind == compile_kind_name)
        .ok_or_else(|| {
            crate::cli::usage_error(
                "active manifest bind receipt references an unavailable compile receipt",
            )
        })?;
    Ok((compile, bind))
}

pub fn assemble_bind_explanation(
    thread_id: &str,
    compile_event_id: &str,
    bind_event_id: &str,
    compile: &crate::agent::manifest_bind::AgentManifestCompileReceipt,
    bind: &crate::agent::manifest_bind::AgentManifestBindReceipt,
) -> crate::kernel::runtime_host::VerletResult<BindExplanation> {
    let alias = compile
        .alias
        .as_ref()
        .map(
            |alias| -> crate::kernel::runtime_host::VerletResult<BindAliasExplanation> {
                let resolved_at_ms = i64::try_from(alias.resolved_at_ms).map_err(|_| {
                    crate::cli::usage_error("alias receipt resolved_at_ms is out of range")
                })?;
                let resolved_at = chrono::Utc
                    .timestamp_millis_opt(resolved_at_ms)
                    .single()
                    .ok_or_else(|| {
                        crate::cli::usage_error("alias receipt resolved_at_ms is out of range")
                    })?
                    .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
                Ok(BindAliasExplanation {
                    alias: alias.alias.clone(),
                    version: alias.version.clone(),
                    resolved_at,
                })
            },
        )
        .transpose()?;
    let overridden = bind
        .overridden_keys
        .iter()
        .collect::<std::collections::BTreeSet<_>>();
    let runtime_values = [
        (
            "default_cwd",
            serde_json::json!(bind.effective_runtime.default_cwd),
        ),
        (
            "streaming",
            serde_json::json!(bind.effective_runtime.streaming),
        ),
        (
            "turn_timeout_ms",
            serde_json::json!(bind.effective_runtime.turn_timeout_ms),
        ),
        (
            "cancellation_grace_ms",
            serde_json::json!(bind.effective_runtime.cancellation_grace_ms),
        ),
        (
            "max_tool_rounds",
            serde_json::json!(bind.effective_runtime.max_tool_rounds),
        ),
        (
            "compaction.auto_at_text_bytes",
            serde_json::json!(bind.effective_runtime.compaction.auto_at_text_bytes),
        ),
    ];
    let runtime = runtime_values
        .into_iter()
        .map(|(key, value)| BindRuntimeExplanation {
            key: key.to_string(),
            value,
            overridden: overridden.contains(&key.to_string()),
        })
        .collect();
    let tools = assemble_tools(bind);
    Ok(BindExplanation {
        thread_id: thread_id.to_string(),
        manifest: BindManifestExplanation {
            ref_uri: compile.ref_uri.clone(),
            manifest_hash: compile.manifest_hash.clone(),
            source_hash: compile.source_hash.clone(),
            alias,
            compile_event_id: compile_event_id.to_string(),
            bind_event_id: bind_event_id.to_string(),
        },
        model: BindModelExplanation {
            profile_id: bind.model_profile_id.clone(),
            provider_id: bind.provider_id.clone(),
            model_id: bind.model_id.clone(),
            origin: bind.model_profile_origin,
        },
        placement: Some(BindPlacementExplanation {
            target: bind
                .placement
                .as_ref()
                .map(|placement| &placement.target)
                .map(|target| serde_json::to_value(target).expect("placement target serializes"))
                .unwrap_or_else(|| serde_json::json!("local"))
                .as_str()
                .expect("placement target is a string")
                .to_string(),
            executor_ref: bind
                .placement
                .as_ref()
                .and_then(|placement| placement.executor_ref.clone()),
            origin: bind.placement_origin,
        }),
        workspace: bind
            .workspace
            .as_ref()
            .map(|workspace| BindWorkspaceExplanation {
                guest_path: workspace.guest_path.clone(),
                host_path: workspace.host_path.clone(),
                mode: serde_json::to_value(workspace.mode)
                    .expect("workspace mode serializes")
                    .as_str()
                    .expect("workspace mode is a string")
                    .to_string(),
                origin: bind.workspace_origin,
            }),
        runtime,
        tools,
        universes: bind
            .tool_universes
            .iter()
            .map(|universe| BindUniverseExplanation {
                import_id: universe.import_id.clone(),
                server_ref: universe.server_ref.clone(),
                discovery_hash: universe.discovery_hash.clone(),
                tool_count: universe.tools.len(),
                pinned_count: universe.pinned.len(),
            })
            .collect(),
        couplings: bind
            .couplings
            .iter()
            .map(|coupling| BindCouplingExplanation {
                id: coupling.id.clone(),
                role: serde_json::to_value(coupling.role)
                    .expect("coupling role serializes")
                    .as_str()
                    .expect("coupling role is a string")
                    .to_string(),
                function_ref: coupling.function_ref.clone(),
                artifact_hash: coupling.artifact_hash.clone(),
                config_hash: coupling.config_hash.clone(),
            })
            .collect(),
        skills: bind
            .skill_packages
            .iter()
            .map(|skill| BindSkillExplanation {
                package: skill.package_name.clone(),
                artifact_hash: skill.artifact_hash.clone(),
            })
            .collect(),
        context: bind
            .static_context_segments
            .iter()
            .map(|segment| BindContextExplanation {
                ref_uri: segment.ref_uri.clone(),
                content_sha256: segment.content_sha256.clone(),
            })
            .collect(),
    })
}

fn assemble_tools(
    bind: &crate::agent::manifest_bind::AgentManifestBindReceipt,
) -> Vec<BindToolExplanation> {
    let mut tools = Vec::new();
    for tool_id in &bind.tool_ids {
        if let Some(universe) = bind
            .tool_universes
            .iter()
            .find(|universe| universe.import_id == *tool_id)
        {
            if let [pin] = universe.pinned.as_slice()
                && let Some((operation_name, artifact_hash)) = pinned_operation(pin)
            {
                tools.push(BindToolExplanation {
                    tool_id: tool_id.clone(),
                    origin: Some("universe".to_string()),
                    row_id: Some(universe.import_id.clone()),
                    pinned: true,
                    operation_name: Some(operation_name),
                    artifact_hash: Some(artifact_hash),
                });
            } else if !universe.pinned.is_empty() {
                tools.push(unrecorded_tool(tool_id));
            }
            continue;
        }
        tools.push(unrecorded_tool(tool_id));
    }
    tools
}

fn unrecorded_tool(tool_id: &str) -> BindToolExplanation {
    BindToolExplanation {
        tool_id: tool_id.to_string(),
        origin: None,
        row_id: None,
        pinned: false,
        operation_name: None,
        artifact_hash: None,
    }
}

fn pinned_operation(pin: &str) -> Option<(String, String)> {
    let body = pin.strip_prefix("mcptool://")?;
    let (path, hash) = body.rsplit_once("@sha256:")?;
    let name = path.rsplit('/').next()?.to_string();
    Some((name, format!("sha256:{hash}")))
}

pub fn render_bind_explanation(explanation: &BindExplanation) -> String {
    let mut out = String::new();
    out.push_str(&format!("thread {}\n", explanation.thread_id));
    out.push_str(&format!(
        "manifest {} (manifest {}, source {})\n",
        explanation.manifest.ref_uri,
        short_hash(&explanation.manifest.manifest_hash),
        short_hash(&explanation.manifest.source_hash),
    ));
    if let Some(alias) = &explanation.manifest.alias {
        out.push_str(&format!(
            "  alias {} -> {} (resolved {})\n",
            alias.alias, alias.version, alias.resolved_at
        ));
    }
    out.push_str(&format!(
        "  receipts compile {} bind {}\n",
        explanation.manifest.compile_event_id, explanation.manifest.bind_event_id
    ));
    out.push_str("\nmodel\n");
    out.push_str(&format!(
        "  {}: {} / {}   [{}]\n",
        explanation.model.profile_id,
        explanation.model.provider_id,
        explanation.model.model_id,
        model_origin_text(explanation.model.origin),
    ));
    if let Some(placement) = &explanation.placement {
        out.push_str("\nplacement\n");
        out.push_str(&format!(
            "  {} (executor {})   [{}]\n",
            placement.target,
            placement.executor_ref.as_deref().unwrap_or("-"),
            binding_origin_text(placement.origin),
        ));
    }
    if let Some(workspace) = &explanation.workspace {
        out.push_str("\nworkspace\n");
        out.push_str(&format!(
            "  {} -> {} ({})   [{}]\n",
            workspace.guest_path.display(),
            workspace.host_path.display(),
            workspace.mode,
            binding_origin_text(workspace.origin),
        ));
    }
    if !explanation.runtime.is_empty() {
        out.push_str("\nruntime\n");
        for runtime in &explanation.runtime {
            out.push_str(&format!(
                "  {} = {}   [{}]\n",
                runtime.key,
                runtime_value_text(&runtime.value),
                if runtime.overridden {
                    "override"
                } else {
                    "manifest"
                },
            ));
        }
    }
    if !explanation.tools.is_empty() {
        out.push_str("\ntools\n");
        for tool in &explanation.tools {
            match (
                tool.origin.as_deref(),
                tool.row_id.as_deref(),
                tool.operation_name.as_deref(),
                tool.artifact_hash.as_deref(),
            ) {
                (Some(origin), Some(row_id), Some(operation_name), Some(artifact_hash)) => {
                    let pinned = if tool.pinned { " pinned" } else { "" };
                    out.push_str(&format!(
                        "  {}   [{} {}{}]  operation {}@{}\n",
                        tool.tool_id,
                        origin,
                        row_id,
                        pinned,
                        operation_name,
                        short_hash(artifact_hash),
                    ));
                }
                _ => out.push_str(&format!("  {}   [unrecorded]\n", tool.tool_id)),
            }
        }
    }
    if !explanation.universes.is_empty() {
        out.push_str("\nuniverses\n");
        for universe in &explanation.universes {
            out.push_str(&format!(
                "  {} {} (discovery {})  tools {}  pinned {}\n",
                universe.import_id,
                universe.server_ref,
                short_hash(&universe.discovery_hash),
                universe.tool_count,
                universe.pinned_count,
            ));
        }
    }
    if !explanation.couplings.is_empty() {
        out.push_str("\ncouplings\n");
        for coupling in &explanation.couplings {
            let function_ref =
                coupling_function_ref_base(&coupling.function_ref, &coupling.artifact_hash);
            out.push_str(&format!(
                "  {} ({})  fn {}@{}  config {}\n",
                coupling.id,
                coupling.role,
                function_ref,
                short_hash(&coupling.artifact_hash),
                short_hash(&coupling.config_hash),
            ));
        }
    }
    if !explanation.skills.is_empty() {
        out.push_str("\nskills\n");
        for skill in &explanation.skills {
            out.push_str(&format!(
                "  {}  {}\n",
                skill.package,
                short_hash(&skill.artifact_hash),
            ));
        }
    }
    if !explanation.context.is_empty() {
        out.push_str("\ncontext\n");
        for context in &explanation.context {
            out.push_str(&format!(
                "  {}  {}\n",
                context.ref_uri,
                short_hash(&context.content_sha256),
            ));
        }
    }
    out
}

fn runtime_value_text(value: &serde_json::Value) -> String {
    value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string())
}

fn coupling_function_ref_base<'a>(function_ref: &'a str, artifact_hash: &str) -> &'a str {
    let hash = artifact_hash
        .strip_prefix("sha256:")
        .unwrap_or(artifact_hash);
    function_ref
        .strip_suffix(&format!("@sha256:{hash}"))
        .unwrap_or(function_ref)
}

fn short_hash(value: &str) -> String {
    let prefixed = value.strip_prefix("sha256:");
    let hash = prefixed.unwrap_or(value);
    let is_hex = !hash.is_empty() && hash.bytes().all(|byte| byte.is_ascii_hexdigit());
    if hash.len() > 12 && is_hex {
        format!("sha256:{}…", &hash[..12])
    } else if prefixed.is_some() {
        value.to_string()
    } else if is_hex {
        format!("sha256:{value}")
    } else {
        value.to_string()
    }
}

const UNRECORDED_ORIGIN: &str = "unrecorded";

fn model_origin_text(
    origin: Option<crate::agent::manifest_bind::AgentManifestModelProfileOrigin>,
) -> &'static str {
    match origin {
        Some(origin) => origin.into(),
        None => UNRECORDED_ORIGIN,
    }
}

fn binding_origin_text(
    origin: Option<crate::agent::manifest_bind::AgentManifestBindingOrigin>,
) -> &'static str {
    match origin {
        Some(origin) => origin.into(),
        None => UNRECORDED_ORIGIN,
    }
}

pub(super) fn print_debug_bind_help() {
    println!(
        "verlet debug bind\n\
\n\
Usage:\n\
  verlet debug bind <thread-id> [--json] [--url <ws-url> | --config <verlet.toml> | --journal <db>]\n\
\n\
Explain the effective runtime envelope from recorded manifest compile and bind\n\
receipts. Daemon mode uses thread/events/list; --journal reads SQLite offline.\n"
    );
}

fn debug_bind_usage_error(message: impl Into<String>) -> crate::kernel::runtime_host::VerletError {
    crate::cli::usage_error(format!(
        "{}\nUsage: verlet debug bind --help",
        message.into()
    ))
}
