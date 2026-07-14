//! The `agent` subcommand family.

use super::*;

pub(super) async fn run_agent(mut args: Vec<OsString>) -> CooldisResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_agent_help();
        return Ok(());
    }
    let subcommand = args.remove(0);
    match subcommand.to_string_lossy().as_ref() {
        "init" => agent_init(args).await,
        "plan" => agent_plan(args).await,
        "publish" => agent_publish(args).await,
        "list" => agent_list(args).await,
        "show" => agent_show(args).await,
        "run" => agent_run(args).await,
        other => Err(usage_error(format!("unknown agent subcommand {other:?}"))),
    }
}

pub(super) async fn agent_init(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_agent_init_args(args)?;
    if options.help {
        print_agent_init_help();
        return Ok(());
    }
    let name = options
        .name
        .ok_or_else(|| usage_error("agent init requires <name>"))?;
    let target = AgentInitTarget::from_options(&name, options.out_path);
    match target {
        AgentInitTarget::SingleFile(out_path) => {
            write_agent_manifest_file(&name, &out_path, options.force)?;
            println!("{}", out_path.display());
        }
        AgentInitTarget::ProjectDirectory(root) => {
            write_agent_project(&name, &root, options.force)?;
            println!("{}", root.display());
        }
    }
    Ok(())
}

pub(super) async fn agent_plan(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_agent_manifest_args(args, "agent plan")?;
    if options.help {
        print_agent_plan_help();
        return Ok(());
    }
    let manifest_path = options
        .manifest_path
        .ok_or_else(|| usage_error("agent plan requires <manifest>"))?;
    let registry = LocalAgentRegistry::new(agent_registry_root(options.registry_root));
    let mut plan = registry.plan_manifest_path(manifest_path)?;
    let operations_registry_root = agent_operations_registry_root(options.operations_registry_root);
    if operations_registry_root.exists() {
        plan.verify_operation_refs(&operations_registry_root)?;
    } else {
        plan.mark_operation_refs_unverified_offline();
    }
    println!("agent plan {}", plan.ref_uri);
    println!("name: {}", plan.name);
    println!("version: {}", plan.version);
    println!("source_hash: {}", plan.source_hash);
    println!("manifest_hash: {}", plan.manifest_hash);
    println!("models: {}", plan.model_profile_count);
    println!("tools: {}", plan.tool_count);
    println!("resources: {}", plan.resource_count);
    for resolved_ref in &plan.resolved_refs {
        match resolved_ref.status {
            AgentManifestRefStatus::Resolved => {
                let content_hash = resolved_ref.content_hash.as_deref().ok_or_else(|| {
                    CooldisError::RuntimeFactory(format!(
                        "resolved artifact ref {:?} is missing content_hash",
                        resolved_ref.declared
                    ))
                })?;
                let status = plan
                    .verification_status_for_ref(&resolved_ref.declared)
                    .map(|status| format!(" [{}]", status.as_str()))
                    .unwrap_or_default();
                println!(
                    "resolved_ref: {} -> {} ({}){}",
                    resolved_ref.declared,
                    resolved_ref
                        .resolved
                        .as_deref()
                        .unwrap_or(&resolved_ref.declared),
                    content_hash,
                    status
                );
            }
            AgentManifestRefStatus::UnresolvedOffline => {
                let status = plan
                    .verification_status_for_ref(&resolved_ref.declared)
                    .map(|status| format!(" [{}]", status.as_str()))
                    .unwrap_or_default();
                println!(
                    "unresolved-offline_ref: {}{}",
                    resolved_ref.declared, status
                );
            }
        }
    }
    for line in agent_context_source_lines(&plan.resolved_manifest) {
        println!("{line}");
    }
    println!("writes: agent record none");
    Ok(())
}

pub(super) async fn agent_publish(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_agent_manifest_args(args, "agent publish")?;
    if options.help {
        print_agent_publish_help();
        return Ok(());
    }
    let manifest_path = options
        .manifest_path
        .ok_or_else(|| usage_error("agent publish requires <manifest>"))?;
    let registry = LocalAgentRegistry::new(agent_registry_root(options.registry_root));
    let operations_registry_root = agent_operations_registry_root(options.operations_registry_root);
    if options.resolve_ops {
        let resolutions =
            resolve_manifest_operation_refs(&manifest_path, &operations_registry_root)?;
        for resolution in &resolutions {
            println!(
                "resolved operation_ref: {} -> {}",
                resolution.declared, resolution.resolved
            );
        }
    }
    let record = registry
        .publish_manifest_path_with_operation_registry(manifest_path, operations_registry_root)?;
    println!("published {}", record.ref_uri);
    println!("manifest_hash: {}", record.manifest_hash);
    for resolved_ref in &record.resolved_refs {
        let content_hash = resolved_ref.content_hash.as_deref().ok_or_else(|| {
            CooldisError::RuntimeFactory(format!(
                "resolved artifact ref {:?} is missing content_hash",
                resolved_ref.declared
            ))
        })?;
        println!(
            "resolved_ref: {} -> {} ({})",
            resolved_ref.declared,
            resolved_ref
                .resolved
                .as_deref()
                .unwrap_or(&resolved_ref.declared),
            content_hash
        );
    }
    for line in agent_context_source_lines(&record.resolved_manifest) {
        println!("{line}");
    }
    println!(
        "alias: {} -> {}",
        crate::agent_ref_uri(record.namespace.as_deref(), &record.name, "latest"),
        record.version
    );
    println!("record: {}", registry.record_path(&record.name)?.display());
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ManifestOperationRef {
    tool_id: String,
    reference: String,
    grants: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct OperationRefResolution {
    declared: String,
    resolved: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct UnpinnedOperationRef {
    record_name: String,
    operation_name: Option<String>,
}

pub(super) fn resolve_manifest_operation_refs(
    manifest_path: &Path,
    operation_registry_root: &Path,
) -> CooldisResult<Vec<OperationRefResolution>> {
    let source = fs::read_to_string(manifest_path).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "failed to read agent manifest {}: {err}",
            manifest_path.display()
        ))
    })?;
    let operation_refs = manifest_operation_refs_from_source(&source)?;
    let registry = LocalOperationRegistry::new(operation_registry_root);
    let mut resolutions = Vec::new();
    let mut replacements = BTreeMap::new();
    for operation_ref in operation_refs {
        let Some(parsed) = parse_resolvable_operation_ref(&operation_ref.reference)? else {
            continue;
        };
        let record = registry.load_record(&parsed.record_name).map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "tool {:?} operation_ref {:?} was not found in the local operation registry: {err}; seed the operation registry or fix the op:// record name",
                operation_ref.tool_id,
                operation_ref.reference
            ))
        })?;
        let resolved = format!(
            "op://{}{}@sha256:{}",
            parsed.record_name,
            parsed
                .operation_name
                .as_deref()
                .map(|operation| format!("/{operation}"))
                .unwrap_or_default(),
            record.active_artifact_hash
        );
        crate::agent::manifest_bind::verify_operation_ref(
            &operation_ref.tool_id,
            &resolved,
            &operation_ref.grants,
            operation_registry_root,
        )?;
        match replacements.get(&operation_ref.reference) {
            Some(existing) if existing != &resolved => {
                return Err(CooldisError::RuntimeFactory(format!(
                    "operation_ref {:?} resolved inconsistently to {:?} and {:?}",
                    operation_ref.reference, existing, resolved
                )));
            }
            Some(_) => {}
            None => {
                replacements.insert(operation_ref.reference.clone(), resolved.clone());
                resolutions.push(OperationRefResolution {
                    declared: operation_ref.reference,
                    resolved,
                });
            }
        }
    }
    if replacements.is_empty() {
        return Ok(resolutions);
    }
    let rewritten = rewrite_operation_ref_values(&source, &replacements, manifest_path)?;
    write_text_atomically(
        manifest_path,
        format!("agent manifest operation refs {}", manifest_path.display()),
        &rewritten,
    )?;
    Ok(resolutions)
}

pub(super) fn manifest_operation_refs_from_source(
    source: &str,
) -> CooldisResult<Vec<ManifestOperationRef>> {
    let value: toml::Value = toml::from_str(source)
        .map_err(|err| CooldisError::RuntimeFactory(format!("invalid agent manifest: {err}")))?;
    let Some(tools) = value.get("tools").and_then(toml::Value::as_array) else {
        return Ok(Vec::new());
    };
    let mut refs = Vec::new();
    for tool in tools {
        let Some(table) = tool.as_table() else {
            continue;
        };
        let Some(reference) = table.get("operation_ref").and_then(toml::Value::as_str) else {
            continue;
        };
        let tool_id = table
            .get("id")
            .and_then(toml::Value::as_str)
            .unwrap_or("<unknown>")
            .to_string();
        let grants = table
            .get("grants")
            .and_then(toml::Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(toml::Value::as_str)
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        refs.push(ManifestOperationRef {
            tool_id,
            reference: reference.to_string(),
            grants,
        });
    }
    Ok(refs)
}

pub(super) fn parse_resolvable_operation_ref(
    reference: &str,
) -> CooldisResult<Option<UnpinnedOperationRef>> {
    let Some(body) = reference.strip_prefix("op://") else {
        return Ok(None);
    };
    if reference.contains("@sha256:") {
        return Ok(None);
    }
    let body = body.strip_suffix("@latest").unwrap_or(body);
    if body.contains('@') {
        return Err(CooldisError::RuntimeFactory(format!(
            "operation_ref {reference:?} cannot be resolved by --resolve-ops; use op://<record>, op://<record>/<operation>, op://<record>@latest, or op://<record>/<operation>@latest"
        )));
    }
    let segments = body.split('/').collect::<Vec<_>>();
    match segments.as_slice() {
        [record_name] if !record_name.is_empty() => Ok(Some(UnpinnedOperationRef {
            record_name: (*record_name).to_string(),
            operation_name: None,
        })),
        [record_name, operation_name] if !record_name.is_empty() && !operation_name.is_empty() => {
            Ok(Some(UnpinnedOperationRef {
                record_name: (*record_name).to_string(),
                operation_name: Some((*operation_name).to_string()),
            }))
        }
        _ => Err(CooldisError::RuntimeFactory(format!(
            "operation_ref {reference:?} must match op://<record>, op://<record>/<operation>, op://<record>@latest, or op://<record>/<operation>@latest"
        ))),
    }
}

pub(super) fn rewrite_operation_ref_values(
    source: &str,
    replacements: &BTreeMap<String, String>,
    manifest_path: &Path,
) -> CooldisResult<String> {
    let mut touched = BTreeSet::new();
    let mut rewritten = String::with_capacity(source.len());
    for line in source.split_inclusive('\n') {
        rewritten.push_str(&rewrite_operation_ref_line(
            line,
            replacements,
            &mut touched,
        ));
    }
    let missing = replacements
        .keys()
        .filter(|reference| !touched.contains(*reference))
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(CooldisError::RuntimeFactory(format!(
            "failed to rewrite operation_ref value(s) {} in {}; --resolve-ops supports single-line operation_ref string values",
            missing.join(", "),
            manifest_path.display()
        )));
    }
    Ok(rewritten)
}

pub(super) fn rewrite_operation_ref_line(
    line: &str,
    replacements: &BTreeMap<String, String>,
    touched: &mut BTreeSet<String>,
) -> String {
    let mut index = 0;
    let bytes = line.as_bytes();
    while index < bytes.len() && matches!(bytes[index], b' ' | b'\t') {
        index += 1;
    }
    if !line[index..].starts_with("operation_ref") {
        return line.to_string();
    }
    let mut cursor = index + "operation_ref".len();
    if line[cursor..]
        .chars()
        .next()
        .is_some_and(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    {
        return line.to_string();
    }
    while cursor < bytes.len() && matches!(bytes[cursor], b' ' | b'\t') {
        cursor += 1;
    }
    if bytes.get(cursor) != Some(&b'=') {
        return line.to_string();
    }
    cursor += 1;
    while cursor < bytes.len() && matches!(bytes[cursor], b' ' | b'\t') {
        cursor += 1;
    }
    let Some(&quote) = bytes.get(cursor) else {
        return line.to_string();
    };
    if quote != b'"' && quote != b'\'' {
        return line.to_string();
    }
    let value_start = cursor + 1;
    let mut value_end = value_start;
    while value_end < bytes.len() {
        if bytes[value_end] == quote
            && (quote == b'\'' || !is_escaped_basic_string_quote(bytes, value_end))
        {
            break;
        }
        value_end += 1;
    }
    if value_end >= bytes.len() {
        return line.to_string();
    }
    let value = &line[value_start..value_end];
    let Some(replacement) = replacements.get(value) else {
        return line.to_string();
    };
    touched.insert(value.to_string());
    let mut rewritten = String::with_capacity(line.len() + replacement.len());
    rewritten.push_str(&line[..value_start]);
    rewritten.push_str(replacement);
    rewritten.push_str(&line[value_end..]);
    rewritten
}

pub(super) fn is_escaped_basic_string_quote(bytes: &[u8], quote_index: usize) -> bool {
    let mut backslashes = 0;
    let mut index = quote_index;
    while index > 0 {
        index -= 1;
        if bytes[index] == b'\\' {
            backslashes += 1;
        } else {
            break;
        }
    }
    backslashes % 2 == 1
}

pub(super) fn write_text_atomically(path: &Path, label: String, body: &str) -> CooldisResult<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "failed to create {label} directory {}: {err}",
            parent.display()
        ))
    })?;
    let tmp_path = parent.join(format!(".cooldis.tmp.{}", Uuid::now_v7()));
    {
        let mut file = fs::File::create(&tmp_path).map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "failed to create temp {label} {}: {err}",
                tmp_path.display()
            ))
        })?;
        file.write_all(body.as_bytes()).map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "failed to write temp {label} {}: {err}",
                tmp_path.display()
            ))
        })?;
        file.sync_all().map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "failed to sync temp {label} {}: {err}",
                tmp_path.display()
            ))
        })?;
    }
    fs::rename(&tmp_path, path).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "failed to atomically install {label} {}: {err}",
            path.display()
        ))
    })
}

pub(super) async fn agent_list(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_agent_registry_args(args, "agent list")?;
    if options.help {
        print_agent_list_help();
        return Ok(());
    }
    let registry = LocalAgentRegistry::new(agent_registry_root(options.registry_root));
    let records = registry.list_records()?;
    if records.is_empty() {
        println!("no published agents");
        return Ok(());
    }
    println!("{:<28} {:<16} REF", "NAME", "VERSION");
    for record in records {
        println!(
            "{:<28} {:<16} {}",
            record.name, record.version, record.ref_uri
        );
    }
    Ok(())
}

pub(super) async fn agent_show(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_agent_show_args(args)?;
    if options.help {
        print_agent_show_help();
        return Ok(());
    }
    let reference = options
        .reference
        .ok_or_else(|| usage_error("agent show requires <agent-ref-or-name>"))?;
    let registry = LocalAgentRegistry::new(agent_registry_root(options.registry_root));
    let record = registry.load_ref(&reference)?;
    print_agent_record_json(&record)
}

pub(super) async fn agent_run(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_agent_run_args(args)?;
    if options.help {
        print_agent_run_help();
        return Ok(());
    }
    let reference = options
        .reference
        .clone()
        .ok_or_else(|| usage_error("agent run requires <agent-ref>"))?;
    let input = options
        .input
        .clone()
        .ok_or_else(|| usage_error("agent run requires --input <text>"))?;
    let root = PathBuf::from("/tmp").join(format!("cdis-agent-{}", Uuid::now_v7().simple()));
    let cwd = std::env::current_dir()
        .map_err(|err| usage_error(format!("failed to read current directory: {err}")))?;
    let listen = AppServerListenAddr::WebSocket("127.0.0.1:0".parse().map_err(|err| {
        usage_error(format!(
            "failed to build local app-server listen address: {err}"
        ))
    })?);
    let mut config = CooldisAppServerConfig::local(listen, cwd);
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    let agent_registry_root = agent_registry_root(options.registry_root.clone());
    config.blob_registry_root =
        default_blob_registry_root_for_agent_registry_root(&agent_registry_root);
    config.agent_registry_root = agent_registry_root;
    let app = CooldisAppServer::new_local(config).await?;
    let thread_start = app
        .local_json_rpc_request(
            "thread/start",
            json!({
            "agentRef": reference,
            }),
        )
        .await?;
    let thread_id = thread_start["thread"]["id"]
        .as_str()
        .ok_or_else(|| usage_error("thread/start response missing thread id"))?
        .to_string();
    let receipt_ids = manifest_receipt_event_ids(&app, &thread_id).await?;
    let assistant_text = run_local_app_turn(&app, &thread_id, &input).await?;
    println!("{assistant_text}");
    println!("manifest.compile.completed: {}", receipt_ids.0);
    println!("manifest.bind.completed: {}", receipt_ids.1);
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}

#[derive(Debug)]
pub(super) struct AgentInitArgs {
    name: Option<String>,
    out_path: Option<PathBuf>,
    force: bool,
    help: bool,
}

#[derive(Debug)]
pub(super) enum AgentInitTarget {
    SingleFile(PathBuf),
    ProjectDirectory(PathBuf),
}

impl AgentInitTarget {
    fn from_options(name: &str, out_path: Option<PathBuf>) -> Self {
        match out_path {
            Some(path) if is_agent_manifest_file_path(&path) => Self::SingleFile(path),
            Some(path) => Self::ProjectDirectory(path),
            None => Self::ProjectDirectory(PathBuf::from(name)),
        }
    }
}

#[derive(Debug)]
pub(super) struct AgentManifestArgs {
    manifest_path: Option<PathBuf>,
    registry_root: Option<PathBuf>,
    operations_registry_root: Option<PathBuf>,
    resolve_ops: bool,
    help: bool,
}

#[derive(Debug)]
pub(super) struct AgentRegistryArgs {
    registry_root: Option<PathBuf>,
    help: bool,
}

#[derive(Debug)]
pub(super) struct AgentShowArgs {
    reference: Option<String>,
    registry_root: Option<PathBuf>,
    help: bool,
}

#[derive(Debug)]
pub(super) struct AgentRunArgs {
    reference: Option<String>,
    input: Option<String>,
    registry_root: Option<PathBuf>,
    help: bool,
}

pub(super) fn parse_agent_init_args(args: Vec<OsString>) -> CooldisResult<AgentInitArgs> {
    let mut name = None;
    let mut out_path = None;
    let mut force = false;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--out" => out_path = Some(required_path_value(&mut iter, "--out")?),
            "--force" => force = true,
            other if other.starts_with('-') => {
                return Err(usage_error(format!(
                    "unknown agent init argument {other:?}"
                )));
            }
            _ => {
                if name.is_some() {
                    return Err(usage_error("agent init accepts exactly one <name>"));
                }
                name = Some(arg.to_string_lossy().to_string());
            }
        }
    }
    Ok(AgentInitArgs {
        name,
        out_path,
        force,
        help,
    })
}

pub(super) fn parse_agent_manifest_args(
    args: Vec<OsString>,
    command: &str,
) -> CooldisResult<AgentManifestArgs> {
    let mut manifest_path = None;
    let mut registry_root = None;
    let mut operations_registry_root = None;
    let mut resolve_ops = false;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--resolve-ops" if command == "agent publish" => resolve_ops = true,
            "--registry-root" => {
                registry_root = Some(required_path_value(&mut iter, "--registry-root")?)
            }
            "--operations-registry-root" => {
                operations_registry_root = Some(required_path_value(
                    &mut iter,
                    "--operations-registry-root",
                )?)
            }
            other if other.starts_with('-') => {
                return Err(usage_error(format!("unknown {command} argument {other:?}")));
            }
            _ => {
                if manifest_path.is_some() {
                    return Err(usage_error(format!(
                        "{command} accepts exactly one <manifest>"
                    )));
                }
                manifest_path = Some(PathBuf::from(arg));
            }
        }
    }
    Ok(AgentManifestArgs {
        manifest_path,
        registry_root,
        operations_registry_root,
        resolve_ops,
        help,
    })
}

pub(super) fn parse_agent_registry_args(
    args: Vec<OsString>,
    command: &str,
) -> CooldisResult<AgentRegistryArgs> {
    let mut registry_root = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--registry-root" => {
                registry_root = Some(required_path_value(&mut iter, "--registry-root")?)
            }
            other => {
                return Err(usage_error(format!("unknown {command} argument {other:?}")));
            }
        }
    }
    Ok(AgentRegistryArgs {
        registry_root,
        help,
    })
}

pub(super) fn parse_agent_show_args(args: Vec<OsString>) -> CooldisResult<AgentShowArgs> {
    let mut reference = None;
    let mut registry_root = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--registry-root" => {
                registry_root = Some(required_path_value(&mut iter, "--registry-root")?)
            }
            other if other.starts_with('-') => {
                return Err(usage_error(format!(
                    "unknown agent show argument {other:?}"
                )));
            }
            _ => {
                if reference.is_some() {
                    return Err(usage_error(
                        "agent show accepts exactly one <agent-ref-or-name>",
                    ));
                }
                reference = Some(arg.to_string_lossy().to_string());
            }
        }
    }
    Ok(AgentShowArgs {
        reference,
        registry_root,
        help,
    })
}

pub(super) fn parse_agent_run_args(args: Vec<OsString>) -> CooldisResult<AgentRunArgs> {
    let mut reference = None;
    let mut input = None;
    let mut registry_root = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--input" => input = Some(required_string_value(&mut iter, "--input")?),
            "--registry-root" => {
                registry_root = Some(required_path_value(&mut iter, "--registry-root")?)
            }
            other if other.starts_with('-') => {
                return Err(usage_error(format!("unknown agent run argument {other:?}")));
            }
            _ => {
                if reference.is_some() {
                    return Err(usage_error("agent run accepts exactly one <agent-ref>"));
                }
                reference = Some(arg.to_string_lossy().to_string());
            }
        }
    }
    Ok(AgentRunArgs {
        reference,
        input,
        registry_root,
        help,
    })
}

pub(super) fn agent_registry_root(registry_root: Option<PathBuf>) -> PathBuf {
    registry_root.unwrap_or_else(|| PathBuf::from(".cooldis/agents"))
}

pub(super) fn agent_operations_registry_root(registry_root: Option<PathBuf>) -> PathBuf {
    registry_root.unwrap_or_else(default_operations_registry_root)
}

pub(super) fn is_agent_manifest_file_path(path: &Path) -> bool {
    path.extension().and_then(|extension| extension.to_str()) == Some("toml")
}

pub(super) fn write_agent_manifest_file(
    name: &str,
    out_path: &Path,
    force: bool,
) -> CooldisResult<()> {
    if out_path.exists() && !force {
        return Err(usage_error(format!(
            "agent manifest {} already exists; pass --force to replace it",
            out_path.display()
        )));
    }
    if let Some(parent) = out_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).map_err(io_error)?;
    }
    fs::write(out_path, render_agent_manifest_template(name)?).map_err(io_error)
}

pub(super) fn write_agent_project(name: &str, root: &Path, force: bool) -> CooldisResult<()> {
    let manifest_path = root.join("cooldis.agent.toml");
    let system_prompt_path = root.join("prompts/system.md");
    let operation_refs_path = root.join("components/operations.toml");
    let coupling_templates_path = root.join("components/couplings.toml");
    let operation_slot_path = root.join("operations/README.md");
    let files = [
        manifest_path.as_path(),
        system_prompt_path.as_path(),
        operation_refs_path.as_path(),
        coupling_templates_path.as_path(),
        operation_slot_path.as_path(),
    ];
    if !force {
        for path in files {
            if path.exists() {
                return Err(usage_error(format!(
                    "agent project file {} already exists; pass --force to replace it",
                    path.display()
                )));
            }
        }
    }
    fs::create_dir_all(root.join("prompts")).map_err(io_error)?;
    fs::create_dir_all(root.join("components")).map_err(io_error)?;
    fs::create_dir_all(root.join("operations")).map_err(io_error)?;
    fs::write(&manifest_path, render_agent_manifest_template(name)?).map_err(io_error)?;
    fs::write(
        &system_prompt_path,
        render_agent_system_prompt_template(name)?,
    )
    .map_err(io_error)?;
    fs::write(
        &operation_refs_path,
        render_agent_operation_refs_template(name)?,
    )
    .map_err(io_error)?;
    fs::write(
        &coupling_templates_path,
        render_agent_coupling_templates_template(name)?,
    )
    .map_err(io_error)?;
    fs::write(
        &operation_slot_path,
        render_agent_operation_slot_template(name)?,
    )
    .map_err(io_error)
}

pub(super) fn render_agent_manifest_template(name: &str) -> CooldisResult<String> {
    crate::validate_record_name(name)?;
    Ok(format!(
        "# Cooldis V1 folder-first agent manifest.\n\
# Prompt text lives in prompts/system.md. Add tools only after publishing\n\
# operation packages and replacing component refs with real op:// hashes.\n\
\n\
[agent]\n\
name = {name:?}\n\
version = \"0.1.0\"\n\
description = \"Describe what this agent is responsible for.\"\n\
kind = \"cooldis.agent-manifest\"\n\
schema_version = 1\n\
\n\
[[model_profiles]]\n\
id = \"default\"\n\
provider_ref = \"provider://local_offline\"\n\
model_ref = \"model://local_offline/echo\"\n\
\n\
[runtime]\n\
default_cwd = \".\"\n\
streaming = false\n\
"
    ))
}

pub(super) fn render_agent_system_prompt_template(name: &str) -> CooldisResult<String> {
    crate::validate_record_name(name)?;
    Ok(format!(
        "You are the {name} agent.\n\
\n\
Keep the user's goal explicit, call only declared operations, and surface the\n\
receipt or event evidence needed to resume or debug the run.\n"
    ))
}

pub(super) fn render_agent_operation_refs_template(name: &str) -> CooldisResult<String> {
    crate::validate_record_name(name)?;
    Ok(format!(
        "# Component refs for {name}.\n\
# V1 publication is component-first: publish operation packages, then publish\n\
# cooldis.agent.toml after replacing placeholder refs with real op:// hashes.\n\
\n\
[[operations]]\n\
name = \"example-tool\"\n\
source = \"../operations/example-tool\"\n\
operation_ref = \"op://example-tool@sha256:0000000000000000000000000000000000000000000000000000000000000000\"\n"
    ))
}

pub(super) fn render_agent_coupling_templates_template(name: &str) -> CooldisResult<String> {
    crate::validate_record_name(name)?;
    let mut out = format!(
        "# Coupling template catalog for {name}.\n\
# V1 couplings are declared as event-stream edges, not hidden callbacks.\n\
# Pick template ids here, then bind manifest coupling rows only after choosing\n\
# the published function_ref that implements the edge.\n"
    );
    for template in crate::coupling_template_catalog_v1().templates {
        out.push_str(&format!(
            "\n[[templates]]\n\
id = {:?}\n\
maturity = {:?}\n\
role = {:?}\n\
runtime_executable = {}\n\
must_have = {}\n\
channel_decision_required = {}\n\
summary = {:?}\n",
            template.id,
            coupling_template_maturity_toml_label(template.maturity),
            coupling_template_role_toml_label(template.role),
            template.runtime_executable,
            template.must_have,
            template.channel_decision_required,
            template.summary,
        ));
    }
    Ok(out)
}

pub(super) fn coupling_template_maturity_toml_label(
    maturity: crate::CouplingTemplateMaturity,
) -> &'static str {
    match maturity {
        crate::CouplingTemplateMaturity::KernelBacked => "kernel_backed",
        crate::CouplingTemplateMaturity::InterfaceOnly => "interface_only",
        crate::CouplingTemplateMaturity::ReferenceOnly => "reference_only",
    }
}

pub(super) fn coupling_template_role_toml_label(role: crate::CouplingRole) -> &'static str {
    match role {
        crate::CouplingRole::Projection => "projection",
        crate::CouplingRole::Controller => "controller",
    }
}

pub(super) fn render_agent_operation_slot_template(name: &str) -> CooldisResult<String> {
    crate::validate_record_name(name)?;
    Ok(format!(
        "# Local operations for {name}\n\
\n\
Put custom operation packages under this directory. Each package should own a\n\
cooldis.tool.toml, schemas, fixtures, and source artifact. Publish operations\n\
before publishing cooldis.agent.toml.\n"
    ))
}

pub(super) fn toml_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_string())
}

pub(super) fn print_agent_record_json(record: &PublishedAgentRecord) -> CooldisResult<()> {
    let json = serde_json::to_string_pretty(record)
        .map_err(|err| usage_error(format!("failed to encode agent record: {err}")))?;
    println!("{json}");
    Ok(())
}

pub(super) fn agent_context_source_lines(manifest: &Value) -> Vec<String> {
    let resources = manifest
        .get("resources")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|resource| {
            let name = resource.get("name").and_then(Value::as_str)?;
            let ref_uri = resource
                .get("ref")
                .or_else(|| resource.get("reference"))
                .and_then(Value::as_str)?;
            Some((name, ref_uri))
        })
        .collect::<BTreeMap<_, _>>();
    manifest
        .get("context")
        .and_then(|context| context.get("sources"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|source| {
            source.get("assembler").and_then(Value::as_str) == Some("kernel://assembler/static")
        })
        .filter_map(|source| {
            let id = source.get("id").and_then(Value::as_str)?;
            let input = source.get("input").and_then(Value::as_str)?;
            let ref_uri = resources.get(input).copied().unwrap_or(input);
            let content_hash = content_hash_label_from_ref(ref_uri)?;
            Some(format!(
                "context_source: {id} -> {ref_uri} ({content_hash})"
            ))
        })
        .collect()
}

pub(super) fn content_hash_label_from_ref(ref_uri: &str) -> Option<String> {
    if let Some(hash) = ref_uri.strip_prefix("resource://artifact/sha256:")
        && hash.len() == 64
    {
        return Some(format!("sha256:{hash}"));
    }
    let (_prefix, hash) = ref_uri.rsplit_once("@sha256:")?;
    (hash.len() == 64).then(|| format!("sha256:{hash}"))
}

pub(super) fn print_agent_help() {
    println!(
        "cooldis agent\n\
\n\
Usage:\n\
  cooldis agent init <name> [--out <dir|manifest.toml>]\n\
  cooldis agent plan <manifest> [--registry-root .cooldis/agents] [--operations-registry-root .cooldis/operations]\n\
  cooldis agent publish <manifest> [--registry-root .cooldis/agents] [--operations-registry-root .cooldis/operations]\n\
  cooldis agent list [--registry-root .cooldis/agents]\n\
  cooldis agent show <agent-ref-or-name> [--registry-root .cooldis/agents]\n\
  cooldis agent run <agent-ref> --input <text> [--registry-root .cooldis/agents]\n\
\n\
Agents are declarative runtime artifacts. `plan` resolves the manifest and\n\
writes nothing; `publish` reruns the plan and writes an immutable local record.\n"
    );
}

pub(super) fn print_agent_init_help() {
    println!(
        "cooldis agent init\n\
\n\
Usage:\n\
  cooldis init <name> [--out <dir|manifest.toml>] [--force]\n\
  cooldis agent init <name> [--out <dir|manifest.toml>] [--force]\n\
\n\
Writes a folder-first Cooldis agent project by default. Use --out path.toml for\n\
the legacy single-manifest file form.\n"
    );
}

pub(super) fn print_agent_plan_help() {
    println!(
        "cooldis agent plan\n\
\n\
Usage:\n\
  cooldis agent plan <manifest> [--registry-root .cooldis/agents] [--operations-registry-root .cooldis/operations]\n\
\n\
Validates and resolves an agent manifest, previews the publish record, and\n\
does not write an agent record. Folder-first prompts are lowered through the\n\
idempotent blob registry so the preview matches publish. When an operations\n\
registry is present, op:// refs are verified against it; otherwise they are\n\
reported unverified-offline.\n"
    );
}

pub(super) fn print_agent_publish_help() {
    println!(
        "cooldis agent publish\n\
\n\
Usage:\n\
  cooldis agent publish <manifest> [--resolve-ops] [--registry-root .cooldis/agents] [--operations-registry-root .cooldis/operations]\n\
\n\
Reruns the agent plan and writes an immutable published agent record. Every\n\
op:// tool ref must exist in the operations registry and its row grants must\n\
cover the selected operation requirements. --resolve-ops rewrites op://name\n\
or op://name@latest authoring refs in the manifest file to pinned\n\
op://name@sha256:<hash> refs before publish verification runs.\n"
    );
}

pub(super) fn print_agent_list_help() {
    println!(
        "cooldis agent list\n\
\n\
Usage:\n\
  cooldis agent list [--registry-root .cooldis/agents]\n\
\n\
Lists published agent records in the local registry.\n"
    );
}

pub(super) fn print_agent_show_help() {
    println!(
        "cooldis agent show\n\
\n\
Usage:\n\
  cooldis agent show <agent-ref-or-name> [--registry-root .cooldis/agents]\n\
\n\
Prints the published agent record as JSON.\n"
    );
}

pub(super) fn print_agent_run_help() {
    println!(
        "cooldis agent run\n\
\n\
Usage:\n\
  cooldis agent run <agent-ref> --input <text> [--registry-root .cooldis/agents]\n\
\n\
Starts a manifest-backed app-server thread, runs one turn, prints the assistant\n\
output, then prints the manifest compile and bind receipt event ids.\n"
    );
}
