//! The `agent` subcommand family.

use chrono::TimeZone as _;
use std::io::Write as _;

pub(super) async fn run_agent(mut args: Vec<std::ffi::OsString>) -> crate::VerletResult<()> {
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
        "versions" => agent_versions(args).await,
        "diff" => agent_diff(args).await,
        "show" => agent_show(args).await,
        "run" => agent_run(args).await,
        other => Err(crate::cli::usage_error(format!(
            "unknown agent subcommand {other:?}"
        ))),
    }
}

pub(super) async fn agent_init(args: Vec<std::ffi::OsString>) -> crate::VerletResult<()> {
    let options = parse_agent_init_args(args)?;
    if options.help {
        print_agent_init_help();
        return Ok(());
    }
    let name = options
        .name
        .ok_or_else(|| crate::cli::usage_error("agent init requires <name>"))?;
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

pub(super) async fn agent_plan(args: Vec<std::ffi::OsString>) -> crate::VerletResult<()> {
    let options = parse_agent_manifest_args(args, "agent plan")?;
    if options.help {
        print_agent_plan_help();
        return Ok(());
    }
    let manifest_path = options
        .manifest_path
        .ok_or_else(|| crate::cli::usage_error("agent plan requires <manifest>"))?;
    let registry = crate::LocalAgentRegistry::new(agent_registry_root(options.registry_root));
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
            crate::AgentManifestRefStatus::Resolved => {
                let content_hash = resolved_ref.content_hash.as_deref().ok_or_else(|| {
                    crate::VerletError::RuntimeFactory(format!(
                        "resolved artifact ref {:?} is missing content_hash",
                        resolved_ref.declared
                    ))
                })?;
                let status = plan
                    .verification_status_for_ref(&resolved_ref.declared)
                    .map(|status| format!(" [{status}]"))
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
            crate::AgentManifestRefStatus::UnresolvedOffline => {
                let status = plan
                    .verification_status_for_ref(&resolved_ref.declared)
                    .map(|status| format!(" [{status}]"))
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

pub(super) async fn agent_publish(args: Vec<std::ffi::OsString>) -> crate::VerletResult<()> {
    let options = parse_agent_manifest_args(args, "agent publish")?;
    if options.help {
        print_agent_publish_help();
        return Ok(());
    }
    let manifest_path = options
        .manifest_path
        .ok_or_else(|| crate::cli::usage_error("agent publish requires <manifest>"))?;
    let registry = crate::LocalAgentRegistry::new(agent_registry_root(options.registry_root));
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
            crate::VerletError::RuntimeFactory(format!(
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
    manifest_path: &std::path::Path,
    operation_registry_root: &std::path::Path,
) -> crate::VerletResult<Vec<OperationRefResolution>> {
    let source = std::fs::read_to_string(manifest_path).map_err(|err| {
        crate::VerletError::RuntimeFactory(format!(
            "failed to read agent manifest {}: {err}",
            manifest_path.display()
        ))
    })?;
    let operation_refs = manifest_operation_refs_from_source(&source)?;
    let registry = crate::LocalOperationRegistry::new(operation_registry_root);
    let mut resolutions = Vec::new();
    let mut replacements = std::collections::BTreeMap::new();
    for operation_ref in operation_refs {
        let Some(parsed) = parse_resolvable_operation_ref(&operation_ref.reference)? else {
            continue;
        };
        let record = registry.load_record(&parsed.record_name).map_err(|err| {
            crate::VerletError::RuntimeFactory(format!(
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
                return Err(crate::VerletError::RuntimeFactory(format!(
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
) -> crate::VerletResult<Vec<ManifestOperationRef>> {
    let value: toml::Value = toml::from_str(source).map_err(|err| {
        crate::VerletError::RuntimeFactory(format!("invalid agent manifest: {err}"))
    })?;
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
) -> crate::VerletResult<Option<UnpinnedOperationRef>> {
    let Some(body) = reference.strip_prefix("op://") else {
        return Ok(None);
    };
    if reference.contains("@sha256:") {
        return Ok(None);
    }
    let body = body.strip_suffix("@latest").unwrap_or(body);
    if body.contains('@') {
        return Err(crate::VerletError::RuntimeFactory(format!(
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
        _ => Err(crate::VerletError::RuntimeFactory(format!(
            "operation_ref {reference:?} must match op://<record>, op://<record>/<operation>, op://<record>@latest, or op://<record>/<operation>@latest"
        ))),
    }
}

pub(super) fn rewrite_operation_ref_values(
    source: &str,
    replacements: &std::collections::BTreeMap<String, String>,
    manifest_path: &std::path::Path,
) -> crate::VerletResult<String> {
    let mut touched = std::collections::BTreeSet::new();
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
        return Err(crate::VerletError::RuntimeFactory(format!(
            "failed to rewrite operation_ref value(s) {} in {}; --resolve-ops supports single-line operation_ref string values",
            missing.join(", "),
            manifest_path.display()
        )));
    }
    Ok(rewritten)
}

pub(super) fn rewrite_operation_ref_line(
    line: &str,
    replacements: &std::collections::BTreeMap<String, String>,
    touched: &mut std::collections::BTreeSet<String>,
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

pub(super) fn write_text_atomically(
    path: &std::path::Path,
    label: String,
    body: &str,
) -> crate::VerletResult<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    std::fs::create_dir_all(parent).map_err(|err| {
        crate::VerletError::RuntimeFactory(format!(
            "failed to create {label} directory {}: {err}",
            parent.display()
        ))
    })?;
    let tmp_path = parent.join(format!(".verlet.tmp.{}", uuid::Uuid::now_v7()));
    {
        let mut file = std::fs::File::create(&tmp_path).map_err(|err| {
            crate::VerletError::RuntimeFactory(format!(
                "failed to create temp {label} {}: {err}",
                tmp_path.display()
            ))
        })?;
        file.write_all(body.as_bytes()).map_err(|err| {
            crate::VerletError::RuntimeFactory(format!(
                "failed to write temp {label} {}: {err}",
                tmp_path.display()
            ))
        })?;
        file.sync_all().map_err(|err| {
            crate::VerletError::RuntimeFactory(format!(
                "failed to sync temp {label} {}: {err}",
                tmp_path.display()
            ))
        })?;
    }
    std::fs::rename(&tmp_path, path).map_err(|err| {
        crate::VerletError::RuntimeFactory(format!(
            "failed to atomically install {label} {}: {err}",
            path.display()
        ))
    })
}

pub(super) async fn agent_list(args: Vec<std::ffi::OsString>) -> crate::VerletResult<()> {
    let options = parse_agent_registry_args(args, "agent list")?;
    if options.help {
        print_agent_list_help();
        return Ok(());
    }
    let registry = crate::LocalAgentRegistry::new(agent_registry_root(options.registry_root));
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

pub(super) async fn agent_versions(args: Vec<std::ffi::OsString>) -> crate::VerletResult<()> {
    let options = parse_agent_versions_args(args)?;
    if options.help {
        print_agent_versions_help();
        return Ok(());
    }
    let name = options
        .name
        .ok_or_else(|| crate::cli::usage_error("agent versions requires <name>"))?;
    let registry = crate::LocalAgentRegistry::new(agent_registry_root(options.registry_root));
    let records = registry.list_version_records(&name)?;
    if options.json {
        let rows = records
            .iter()
            .map(|record| {
                serde_json::json!({
                    "version": record.version,
                    "source_hash": record.source_hash,
                    "manifest_hash": record.manifest_hash,
                    "published_at_ms": record.published_at_ms,
                    "authored_source_present": record.authored_source_present,
                })
            })
            .collect::<Vec<_>>();
        serde_json::to_writer_pretty(std::io::stdout(), &rows).map_err(|err| {
            crate::cli::usage_error(format!("failed to encode agent versions JSON: {err}"))
        })?;
        println!();
        return Ok(());
    }
    println!("{:<24}  {:<16}  MANIFEST HASH", "PUBLISHED AT", "VERSION");
    for record in records {
        let published_at = i64::try_from(record.published_at_ms)
            .ok()
            .and_then(|millis| chrono::Utc.timestamp_millis_opt(millis).single())
            .ok_or_else(|| {
                crate::VerletError::RuntimeFactory(format!(
                    "agent record {}@{} has invalid published_at_ms {}",
                    name, record.version, record.published_at_ms
                ))
            })?
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let authored = if !record.authored_source_present {
            "  [no-authored-source]"
        } else {
            ""
        };
        println!(
            "{published_at:<24}  {:<16}  {}{authored}",
            record.version, record.manifest_hash
        );
    }
    Ok(())
}

pub(super) async fn agent_diff(args: Vec<std::ffi::OsString>) -> crate::VerletResult<()> {
    let options = parse_agent_diff_args(args)?;
    if options.help {
        print_agent_diff_help();
        return Ok(());
    }
    let name = options
        .name
        .ok_or_else(|| crate::cli::usage_error("agent diff requires <name>"))?;
    let from = options.from.ok_or_else(|| {
        crate::cli::usage_error("agent diff requires --from <version>[:authored|:resolved]")
    })?;
    let to = options.to.ok_or_else(|| {
        crate::cli::usage_error("agent diff requires --to <version>[:authored|:resolved]")
    })?;
    let registry = crate::LocalAgentRegistry::new(agent_registry_root(options.registry_root));
    let from_record = registry.load_version_record(&name, &from.version)?;
    let to_record = registry.load_version_record(&name, &to.version)?;
    let before = manifest_diff_snapshot(&from_record, from.form)?;
    let after = manifest_diff_snapshot(&to_record, to.form)?;
    let changes = crate::diff_canonical_json(&before, &after);
    if options.json {
        serde_json::to_writer_pretty(std::io::stdout(), &changes).map_err(|err| {
            crate::cli::usage_error(format!("failed to encode agent diff JSON: {err}"))
        })?;
        println!();
        return Ok(());
    }
    println!(
        "manifest {name} {}:{} -> {}:{}",
        from.version, from.form, to.version, to.form
    );
    for change in changes {
        match change.kind {
            crate::AgentManifestDiffKind::Changed => println!(
                "~ {}: {} -> {}",
                change.path,
                render_manifest_diff_value(change.before.as_ref().expect("changed before value")),
                render_manifest_diff_value(change.after.as_ref().expect("changed after value"))
            ),
            crate::AgentManifestDiffKind::Added => println!(
                "+ {}: {}",
                change.path,
                render_manifest_diff_value(change.after.as_ref().expect("added after value"))
            ),
            crate::AgentManifestDiffKind::Removed => println!(
                "- {}: {}",
                change.path,
                render_manifest_diff_value(change.before.as_ref().expect("removed before value"))
            ),
        }
    }
    Ok(())
}

pub(super) fn manifest_diff_snapshot(
    record: &crate::PublishedAgentRecord,
    form: AgentManifestForm,
) -> crate::VerletResult<serde_json::Value> {
    match form {
        AgentManifestForm::Resolved => Ok(record.resolved_manifest.clone()),
        AgentManifestForm::Authored => {
            let source = record.authored_source.as_deref().ok_or_else(|| {
                crate::VerletError::RuntimeFactory(format!(
                    "agent record {}@{}: legacy record has no authored_source; authored-form diff is unavailable",
                    record.name, record.version
                ))
            })?;
            crate::agent::manifest::canonical_json_from_authored_source(source)
        }
    }
}

pub(super) fn render_manifest_diff_value(value: &serde_json::Value) -> String {
    let rendered = serde_json::to_string(value).unwrap_or_else(|_| "null".to_string());
    if rendered.chars().count() <= 120 {
        return rendered;
    }
    rendered.chars().take(117).collect::<String>() + "..."
}

pub(super) async fn agent_show(args: Vec<std::ffi::OsString>) -> crate::VerletResult<()> {
    let options = parse_agent_show_args(args)?;
    if options.help {
        print_agent_show_help();
        return Ok(());
    }
    let reference = options
        .reference
        .ok_or_else(|| crate::cli::usage_error("agent show requires <agent-ref-or-name>"))?;
    let registry = crate::LocalAgentRegistry::new(agent_registry_root(options.registry_root));
    let record = registry.load_ref(&reference)?;
    print_agent_record_json(&record)
}

pub(super) async fn agent_run(args: Vec<std::ffi::OsString>) -> crate::VerletResult<()> {
    let options = parse_agent_run_args(args)?;
    if options.help {
        print_agent_run_help();
        return Ok(());
    }
    let reference = options
        .reference
        .clone()
        .ok_or_else(|| crate::cli::usage_error("agent run requires <agent-ref>"))?;
    let input = options
        .input
        .clone()
        .ok_or_else(|| crate::cli::usage_error("agent run requires --input <text>"))?;
    let root = std::path::PathBuf::from("/tmp")
        .join(format!("cdis-agent-{}", uuid::Uuid::now_v7().simple()));
    let cwd = std::env::current_dir().map_err(|err| {
        crate::cli::usage_error(format!("failed to read current directory: {err}"))
    })?;
    let listen = crate::AppServerListenAddr::WebSocket("127.0.0.1:0".parse().map_err(|err| {
        crate::cli::usage_error(format!(
            "failed to build local app-server listen address: {err}"
        ))
    })?);
    let mut config = crate::VerletAppServerConfig::local(listen, cwd);
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    let agent_registry_root = agent_registry_root(options.registry_root.clone());
    config.blob_registry_root =
        crate::default_blob_registry_root_for_agent_registry_root(&agent_registry_root);
    config.agent_registry_root = agent_registry_root;
    let app = crate::VerletAppServer::new_local(config).await?;
    let thread_start = app
        .local_json_rpc_request(
            "thread/start",
            serde_json::json!({
            "agentRef": reference,
            }),
        )
        .await?;
    let thread_id = thread_start["thread"]["id"]
        .as_str()
        .ok_or_else(|| crate::cli::usage_error("thread/start response missing thread id"))?
        .to_string();
    let receipt_ids = crate::cli::console::manifest_receipt_event_ids(&app, &thread_id).await?;
    let assistant_text = crate::cli::console::run_local_app_turn(&app, &thread_id, &input).await?;
    println!("{assistant_text}");
    println!("manifest.compile.completed: {}", receipt_ids.0);
    println!("manifest.bind.completed: {}", receipt_ids.1);
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}

#[derive(Debug)]
pub(super) struct AgentInitArgs {
    name: Option<String>,
    out_path: Option<std::path::PathBuf>,
    force: bool,
    help: bool,
}

#[derive(Debug)]
pub(super) enum AgentInitTarget {
    SingleFile(std::path::PathBuf),
    ProjectDirectory(std::path::PathBuf),
}

impl AgentInitTarget {
    fn from_options(name: &str, out_path: Option<std::path::PathBuf>) -> Self {
        match out_path {
            Some(path) if is_agent_manifest_file_path(&path) => Self::SingleFile(path),
            Some(path) => Self::ProjectDirectory(path),
            None => Self::ProjectDirectory(std::path::PathBuf::from(name)),
        }
    }
}

#[derive(Debug)]
pub(super) struct AgentManifestArgs {
    manifest_path: Option<std::path::PathBuf>,
    registry_root: Option<std::path::PathBuf>,
    operations_registry_root: Option<std::path::PathBuf>,
    resolve_ops: bool,
    help: bool,
}

#[derive(Debug)]
pub(super) struct AgentRegistryArgs {
    registry_root: Option<std::path::PathBuf>,
    help: bool,
}

#[derive(Debug)]
pub(super) struct AgentVersionsArgs {
    name: Option<String>,
    registry_root: Option<std::path::PathBuf>,
    json: bool,
    help: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, strum::AsRefStr, strum::Display)]
#[strum(serialize_all = "snake_case")]
pub(super) enum AgentManifestForm {
    Authored,
    Resolved,
}

#[derive(Debug)]
pub(super) struct AgentDiffEndpoint {
    version: String,
    form: AgentManifestForm,
}

#[derive(Debug)]
pub(super) struct AgentDiffArgs {
    name: Option<String>,
    from: Option<AgentDiffEndpoint>,
    to: Option<AgentDiffEndpoint>,
    registry_root: Option<std::path::PathBuf>,
    json: bool,
    help: bool,
}

#[derive(Debug)]
pub(super) struct AgentShowArgs {
    reference: Option<String>,
    registry_root: Option<std::path::PathBuf>,
    help: bool,
}

#[derive(Debug)]
pub(super) struct AgentRunArgs {
    reference: Option<String>,
    input: Option<String>,
    registry_root: Option<std::path::PathBuf>,
    help: bool,
}

pub(super) fn parse_agent_init_args(
    args: Vec<std::ffi::OsString>,
) -> crate::VerletResult<AgentInitArgs> {
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
                    "unknown agent init argument {other:?}"
                )));
            }
            _ => {
                if name.is_some() {
                    return Err(crate::cli::usage_error(
                        "agent init accepts exactly one <name>",
                    ));
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
    args: Vec<std::ffi::OsString>,
    command: &str,
) -> crate::VerletResult<AgentManifestArgs> {
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
                registry_root = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--registry-root",
                )?)
            }
            "--operations-registry-root" => {
                operations_registry_root = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--operations-registry-root",
                )?)
            }
            other if other.starts_with('-') => {
                return Err(crate::cli::usage_error(format!(
                    "unknown {command} argument {other:?}"
                )));
            }
            _ => {
                if manifest_path.is_some() {
                    return Err(crate::cli::usage_error(format!(
                        "{command} accepts exactly one <manifest>"
                    )));
                }
                manifest_path = Some(std::path::PathBuf::from(arg));
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
    args: Vec<std::ffi::OsString>,
    command: &str,
) -> crate::VerletResult<AgentRegistryArgs> {
    let mut registry_root = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--registry-root" => {
                registry_root = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--registry-root",
                )?)
            }
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unknown {command} argument {other:?}"
                )));
            }
        }
    }
    Ok(AgentRegistryArgs {
        registry_root,
        help,
    })
}

pub(super) fn parse_agent_versions_args(
    args: Vec<std::ffi::OsString>,
) -> crate::VerletResult<AgentVersionsArgs> {
    let mut name = None;
    let mut registry_root = None;
    let mut json = false;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--json" => json = true,
            "--registry-root" => {
                registry_root = Some(std::path::PathBuf::from(required_agent_history_value(
                    &mut iter,
                    "--registry-root",
                )?))
            }
            other if other.starts_with('-') => {
                return Err(crate::cli::usage_error(format!(
                    "unknown agent versions argument {other:?}"
                )));
            }
            _ => {
                if name.is_some() {
                    return Err(crate::cli::usage_error(
                        "agent versions accepts exactly one <name>",
                    ));
                }
                name = Some(arg.to_string_lossy().to_string());
            }
        }
    }
    Ok(AgentVersionsArgs {
        name,
        registry_root,
        json,
        help,
    })
}

pub(super) fn parse_agent_diff_args(
    args: Vec<std::ffi::OsString>,
) -> crate::VerletResult<AgentDiffArgs> {
    let mut name = None;
    let mut from = None;
    let mut to = None;
    let mut registry_root = None;
    let mut json = false;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--json" => json = true,
            "--from" => {
                from = Some(parse_agent_diff_endpoint(
                    &required_agent_history_value(&mut iter, "--from")?.to_string_lossy(),
                )?)
            }
            "--to" => {
                to = Some(parse_agent_diff_endpoint(
                    &required_agent_history_value(&mut iter, "--to")?.to_string_lossy(),
                )?)
            }
            "--registry-root" => {
                registry_root = Some(std::path::PathBuf::from(required_agent_history_value(
                    &mut iter,
                    "--registry-root",
                )?))
            }
            other if other.starts_with('-') => {
                return Err(crate::cli::usage_error(format!(
                    "unknown agent diff argument {other:?}"
                )));
            }
            _ => {
                if name.is_some() {
                    return Err(crate::cli::usage_error(
                        "agent diff accepts exactly one <name>",
                    ));
                }
                name = Some(arg.to_string_lossy().to_string());
            }
        }
    }
    Ok(AgentDiffArgs {
        name,
        from,
        to,
        registry_root,
        json,
        help,
    })
}

pub(super) fn parse_agent_diff_endpoint(value: &str) -> crate::VerletResult<AgentDiffEndpoint> {
    let (version, form) = match value.rsplit_once(':') {
        Some((version, "authored")) => (version, AgentManifestForm::Authored),
        Some((version, "resolved")) => (version, AgentManifestForm::Resolved),
        _ => (value, AgentManifestForm::Resolved),
    };
    verlet_agent::validate_version(version)?;
    Ok(AgentDiffEndpoint {
        version: version.to_string(),
        form,
    })
}

fn required_agent_history_value(
    iter: &mut impl Iterator<Item = std::ffi::OsString>,
    flag: &'static str,
) -> crate::VerletResult<std::ffi::OsString> {
    let value = iter
        .next()
        .ok_or_else(|| crate::cli::usage_error(format!("{flag} requires a value")))?;
    if value.to_string_lossy().starts_with('-') {
        return Err(crate::cli::usage_error(format!("{flag} requires a value")));
    }
    Ok(value)
}

pub(super) fn parse_agent_show_args(
    args: Vec<std::ffi::OsString>,
) -> crate::VerletResult<AgentShowArgs> {
    let mut reference = None;
    let mut registry_root = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--registry-root" => {
                registry_root = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--registry-root",
                )?)
            }
            other if other.starts_with('-') => {
                return Err(crate::cli::usage_error(format!(
                    "unknown agent show argument {other:?}"
                )));
            }
            _ => {
                if reference.is_some() {
                    return Err(crate::cli::usage_error(
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

pub(super) fn parse_agent_run_args(
    args: Vec<std::ffi::OsString>,
) -> crate::VerletResult<AgentRunArgs> {
    let mut reference = None;
    let mut input = None;
    let mut registry_root = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--input" => {
                input = Some(crate::cli::tool::required_string_value(
                    &mut iter, "--input",
                )?)
            }
            "--registry-root" => {
                registry_root = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--registry-root",
                )?)
            }
            other if other.starts_with('-') => {
                return Err(crate::cli::usage_error(format!(
                    "unknown agent run argument {other:?}"
                )));
            }
            _ => {
                if reference.is_some() {
                    return Err(crate::cli::usage_error(
                        "agent run accepts exactly one <agent-ref>",
                    ));
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

pub(super) fn agent_registry_root(registry_root: Option<std::path::PathBuf>) -> std::path::PathBuf {
    registry_root.unwrap_or_else(|| {
        let legacy = std::path::PathBuf::from(concat!(".", "cool", "dis/agents"));
        if std::path::Path::new(".verlet").exists() || !legacy.exists() {
            std::path::PathBuf::from(".verlet/agents")
        } else {
            eprintln!(
                "warning: {} is deprecated; existing state will continue to be used in place through v0.3.0",
                legacy.display()
            );
            legacy
        }
    })
}

pub(super) fn agent_operations_registry_root(
    registry_root: Option<std::path::PathBuf>,
) -> std::path::PathBuf {
    registry_root.unwrap_or_else(crate::default_operations_registry_root)
}

pub(super) fn is_agent_manifest_file_path(path: &std::path::Path) -> bool {
    path.extension().and_then(|extension| extension.to_str()) == Some("toml")
}

pub(super) fn write_agent_manifest_file(
    name: &str,
    out_path: &std::path::Path,
    force: bool,
) -> crate::VerletResult<()> {
    if out_path.exists() && !force {
        return Err(crate::cli::usage_error(format!(
            "agent manifest {} already exists; pass --force to replace it",
            out_path.display()
        )));
    }
    if let Some(parent) = out_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent).map_err(crate::cli::io_error)?;
    }
    std::fs::write(out_path, render_agent_manifest_template(name)?).map_err(crate::cli::io_error)
}

pub(super) fn write_agent_project(
    name: &str,
    root: &std::path::Path,
    force: bool,
) -> crate::VerletResult<()> {
    let manifest_path = root.join("verlet.agent.toml");
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
                return Err(crate::cli::usage_error(format!(
                    "agent project file {} already exists; pass --force to replace it",
                    path.display()
                )));
            }
        }
    }
    std::fs::create_dir_all(root.join("prompts")).map_err(crate::cli::io_error)?;
    std::fs::create_dir_all(root.join("components")).map_err(crate::cli::io_error)?;
    std::fs::create_dir_all(root.join("operations")).map_err(crate::cli::io_error)?;
    std::fs::write(&manifest_path, render_agent_manifest_template(name)?)
        .map_err(crate::cli::io_error)?;
    std::fs::write(
        &system_prompt_path,
        render_agent_system_prompt_template(name)?,
    )
    .map_err(crate::cli::io_error)?;
    std::fs::write(
        &operation_refs_path,
        render_agent_operation_refs_template(name)?,
    )
    .map_err(crate::cli::io_error)?;
    std::fs::write(
        &coupling_templates_path,
        render_agent_coupling_templates_template(name)?,
    )
    .map_err(crate::cli::io_error)?;
    std::fs::write(
        &operation_slot_path,
        render_agent_operation_slot_template(name)?,
    )
    .map_err(crate::cli::io_error)
}

pub(super) fn render_agent_manifest_template(name: &str) -> crate::VerletResult<String> {
    crate::validate_record_name(name)?;
    Ok(format!(
        "# Verlet V1 folder-first agent manifest.\n\
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

pub(super) fn render_agent_system_prompt_template(name: &str) -> crate::VerletResult<String> {
    crate::validate_record_name(name)?;
    Ok(format!(
        "You are the {name} agent.\n\
\n\
Keep the user's goal explicit, call only declared operations, and surface the\n\
receipt or event evidence needed to resume or debug the run.\n"
    ))
}

pub(super) fn render_agent_operation_refs_template(name: &str) -> crate::VerletResult<String> {
    crate::validate_record_name(name)?;
    Ok(format!(
        "# Component refs for {name}.\n\
# V1 publication is component-first: publish operation packages, then publish\n\
# verlet.agent.toml after replacing placeholder refs with real op:// hashes.\n\
\n\
[[operations]]\n\
name = \"example-tool\"\n\
source = \"../operations/example-tool\"\n\
operation_ref = \"op://example-tool@sha256:0000000000000000000000000000000000000000000000000000000000000000\"\n"
    ))
}

pub(super) fn render_agent_coupling_templates_template(name: &str) -> crate::VerletResult<String> {
    crate::validate_record_name(name)?;
    let mut out = format!(
        "# Coupling template catalog for {name}.\n\
# V1 couplings are declared as event-stream edges, not hidden callbacks.\n\
# Pick template ids here, then bind manifest coupling rows only after choosing\n\
# the published function_ref that implements the edge.\n"
    );
    for template in crate::coupling_template_catalog_v1().templates {
        let maturity: &str = template.maturity.as_ref();
        let role: &str = template.role.as_ref();
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
            maturity,
            role,
            template.runtime_executable,
            template.must_have,
            template.channel_decision_required,
            template.summary,
        ));
    }
    Ok(out)
}

pub(super) fn render_agent_operation_slot_template(name: &str) -> crate::VerletResult<String> {
    crate::validate_record_name(name)?;
    Ok(format!(
        "# Local operations for {name}\n\
\n\
Put custom operation packages under this directory. Each package should own a\n\
verlet.tool.toml, schemas, fixtures, and source artifact. Publish operations\n\
before publishing verlet.agent.toml.\n"
    ))
}

pub(super) fn toml_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_string())
}

pub(super) fn print_agent_record_json(
    record: &crate::PublishedAgentRecord,
) -> crate::VerletResult<()> {
    let json = serde_json::to_string_pretty(record)
        .map_err(|err| crate::cli::usage_error(format!("failed to encode agent record: {err}")))?;
    println!("{json}");
    Ok(())
}

pub(super) fn agent_context_source_lines(manifest: &serde_json::Value) -> Vec<String> {
    let resources = manifest
        .get("resources")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|resource| {
            let name = resource.get("name").and_then(serde_json::Value::as_str)?;
            let ref_uri = resource
                .get("ref")
                .or_else(|| resource.get("reference"))
                .and_then(serde_json::Value::as_str)?;
            Some((name, ref_uri))
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    manifest
        .get("context")
        .and_then(|context| context.get("sources"))
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|source| {
            source.get("assembler").and_then(serde_json::Value::as_str)
                == Some("kernel://assembler/static")
        })
        .filter_map(|source| {
            let id = source.get("id").and_then(serde_json::Value::as_str)?;
            let input = source.get("input").and_then(serde_json::Value::as_str)?;
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
        "verlet agent\n\
\n\
Usage:\n\
  verlet agent init <name> [--out <dir|manifest.toml>]\n\
  verlet agent plan <manifest> [--registry-root .verlet/agents] [--operations-registry-root .verlet/operations]\n\
  verlet agent publish <manifest> [--registry-root .verlet/agents] [--operations-registry-root .verlet/operations]\n\
  verlet agent list [--registry-root .verlet/agents]\n\
  verlet agent versions <name> [--json] [--registry-root .verlet/agents]\n\
  verlet agent diff <name> --from <version>[:authored|:resolved] --to <version>[:authored|:resolved] [--json] [--registry-root .verlet/agents]\n\
  verlet agent show <agent-ref-or-name> [--registry-root .verlet/agents]\n\
  verlet agent run <agent-ref> --input <text> [--registry-root .verlet/agents]\n\
\n\
Agents are declarative runtime artifacts. `plan` resolves the manifest and\n\
writes nothing; `publish` reruns the plan and writes an immutable local record.\n"
    );
}

pub(super) fn print_agent_init_help() {
    println!(
        "verlet agent init\n\
\n\
Usage:\n\
  verlet init <name> [--out <dir|manifest.toml>] [--force]\n\
  verlet agent init <name> [--out <dir|manifest.toml>] [--force]\n\
\n\
Writes a folder-first Verlet agent project by default. Use --out path.toml for\n\
the legacy single-manifest file form.\n"
    );
}

pub(super) fn print_agent_plan_help() {
    println!(
        "verlet agent plan\n\
\n\
Usage:\n\
  verlet agent plan <manifest> [--registry-root .verlet/agents] [--operations-registry-root .verlet/operations]\n\
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
        "verlet agent publish\n\
\n\
Usage:\n\
  verlet agent publish <manifest> [--resolve-ops] [--registry-root .verlet/agents] [--operations-registry-root .verlet/operations]\n\
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
        "verlet agent list\n\
\n\
Usage:\n\
  verlet agent list [--registry-root .verlet/agents]\n\
\n\
Lists published agent records in the local registry.\n"
    );
}

pub(super) fn print_agent_versions_help() {
    println!(
        "verlet agent versions\n\
\n\
Usage:\n\
  verlet agent versions <name> [--json] [--registry-root .verlet/agents]\n\
\n\
Lists immutable agent versions in publication order.\n"
    );
}

pub(super) fn print_agent_diff_help() {
    println!(
        "verlet agent diff\n\
\n\
Usage:\n\
  verlet agent diff <name> --from <version>[:authored|:resolved] --to <version>[:authored|:resolved] [--json] [--registry-root .verlet/agents]\n\
\n\
Prints a structural diff between immutable authored or resolved manifest snapshots.\n"
    );
}

pub(super) fn print_agent_show_help() {
    println!(
        "verlet agent show\n\
\n\
Usage:\n\
  verlet agent show <agent-ref-or-name> [--registry-root .verlet/agents]\n\
\n\
Prints the published agent record as JSON.\n"
    );
}

pub(super) fn print_agent_run_help() {
    println!(
        "verlet agent run\n\
\n\
Usage:\n\
  verlet agent run <agent-ref> --input <text> [--registry-root .verlet/agents]\n\
\n\
Starts a manifest-backed app-server thread, runs one turn, prints the assistant\n\
output, then prints the manifest compile and bind receipt event ids.\n"
    );
}

#[cfg(test)]
mod tests {

    #[test]
    fn agent_diff_endpoint_preserves_colons_inside_versions() {
        let endpoint = crate::cli::agent::parse_agent_diff_endpoint("release:2026:07").unwrap();
        assert_eq!(endpoint.version, "release:2026:07");
        assert_eq!(
            endpoint.form,
            crate::cli::agent::AgentManifestForm::Resolved
        );

        let endpoint =
            crate::cli::agent::parse_agent_diff_endpoint("release:2026:07:authored").unwrap();
        assert_eq!(endpoint.version, "release:2026:07");
        assert_eq!(
            endpoint.form,
            crate::cli::agent::AgentManifestForm::Authored
        );
    }

    #[test]
    fn agent_history_parsers_reject_missing_flag_values_and_duplicate_names() {
        let error = crate::cli::agent::parse_agent_diff_args(
            ["auditor", "--from", "--to", "2.0.0"]
                .into_iter()
                .map(std::ffi::OsString::from)
                .collect(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("--from requires a value"));

        let error = crate::cli::agent::parse_agent_versions_args(
            ["auditor", "--registry-root", "--json"]
                .into_iter()
                .map(std::ffi::OsString::from)
                .collect(),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("--registry-root requires a value")
        );

        let error = crate::cli::agent::parse_agent_diff_args(
            ["auditor", "second", "--from", "1", "--to", "2"]
                .into_iter()
                .map(std::ffi::OsString::from)
                .collect(),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("agent diff accepts exactly one <name>")
        );
    }

    #[test]
    fn manifest_diff_values_are_compact_json_truncated_to_120_characters() {
        let value = serde_json::Value::String("x".repeat(200));
        let rendered = crate::cli::agent::render_manifest_diff_value(&value);

        assert_eq!(rendered.chars().count(), 120);
        assert!(rendered.ends_with("..."));
        assert!(rendered.starts_with('"'));
    }

    #[test]
    fn manifest_diff_value_truncation_counts_multibyte_characters_at_the_boundary() {
        let exactly_120 = crate::cli::agent::render_manifest_diff_value(
            &serde_json::Value::String("é".repeat(118)),
        );
        assert_eq!(exactly_120.chars().count(), 120);
        assert!(!exactly_120.ends_with("..."));

        let over_boundary = crate::cli::agent::render_manifest_diff_value(
            &serde_json::Value::String("é".repeat(119)),
        );
        assert_eq!(over_boundary.chars().count(), 120);
        assert!(over_boundary.ends_with("..."));
    }
}
