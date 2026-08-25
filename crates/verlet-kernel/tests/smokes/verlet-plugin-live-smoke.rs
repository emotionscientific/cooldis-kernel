const DEFAULT_ENV_FILE: &str = ".env";
const TOOL_NAME: &str = "tailcat_cat";
const EXPECTED_FILE_CONTENT: &str = "hello from live plugin llm smoke\n";
const FINAL_MARKER: &str = "VERLET_PLUGIN_TOOL_OK";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = SmokeConfig::load()?;
    let temp = std::env::temp_dir().join(format!("verlet-plugin-live-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&temp)?;
    let registry_root = temp.join("plugins");
    let workspace = temp.join("workspace");
    std::fs::create_dir_all(&workspace)?;
    std::fs::write(workspace.join("input.txt"), EXPECTED_FILE_CONTENT)?;

    let repo = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let module_path = repo.join("tests/fixtures/wasm-vfs-tools");
    let build = verlet::operations::operation_builder::build_rust_wasm_module(
        verlet::operations::operation_builder::RustWasmBuildOptions::new(&module_path),
    )?;
    verlet_operations::operation_store::LocalOperationRegistry::new(&registry_root)
        .publish_artifact(
            verlet_operations::operation_store::PublishOperationRequest {
                name: "tailcat".to_string(),
                artifact_path: build.artifact_path,
                source: verlet_operations::operation_store::PublishedOperationSource::Rust {
                    module_path,
                    release: true,
                },
                interface: None,
                capability_grants: std::collections::BTreeSet::from([
                    verlet_wasm::runner::FS_WRITE_CAPABILITY.to_string(),
                ]),
                metadata: std::collections::BTreeMap::new(),
            },
        )
        .await?;

    let catalog = verlet::operations::plugins::LocalPluginCatalog::load(
        verlet::operations::plugins::LocalPluginCatalogConfig::new(&registry_root).with_mount(
            verlet::operations::plugins::PluginMount::host_read_only("/workspace", &workspace),
        ),
    )
    .await?;

    let adapter: std::sync::Arc<dyn verlet_provider::ProviderWireAdapter> =
        std::sync::Arc::new(verlet_provider::OpenAIResponsesAdapter {
            include_encrypted_reasoning: false,
            reasoning_summary: verlet_provider::OpenAIReasoningSummary::Auto,
        });
    let client = std::sync::Arc::new(verlet_provider::ProviderHttpClient::new(
        verlet_provider::ProviderEndpoint::openai_responses(
            &config.base_url,
            config.api_key.clone(),
        ),
        adapter,
    )?);
    let mut runtime_config = verlet::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        config.model.clone(),
    );
    runtime_config.max_tokens = 256;
    runtime_config.system.push(verlet_provider::SystemBlock::text(format!(
        "You are testing a newly installed Verlet plugin. You must call the {TOOL_NAME} tool with input /workspace/input.txt before answering. After the tool result is visible, reply with exactly: {FINAL_MARKER}: <file content>. Do not invent the file content."
    )));

    let host = verlet::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(
        verlet::adapters::agent_loop::AgentLoopFactory::new(runtime_config, client)
            .with_operation_registry(catalog.operation_registry()),
    ));
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new(
                "smoke_tenant",
                "smoke_user",
                "plugin_live",
            ),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await?;
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-plugin-live",
        "Use the installed plugin to read /workspace/input.txt and confirm the file content.",
    )
    .await?;

    let trace = collect_live_plugin_trace(&mut events).await?;
    host.shutdown_thread(thread.context().coordinates.thread_id)
        .await?;

    println!(
        "verlet plugin live smoke ok model={} tool={} text={}",
        config.model,
        TOOL_NAME,
        compact(&trace.final_output)
    );
    Ok(())
}

struct SmokeConfig {
    base_url: String,
    api_key: String,
    model: String,
}

impl SmokeConfig {
    fn load() -> Result<Self, Box<dyn std::error::Error>> {
        let env_file = std::env::var("VERLET_PLUGIN_LIVE_ENV_FILE")
            .or_else(|_| std::env::var("VERLET_BIFROST_ENV_FILE"))
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::path::PathBuf::from(DEFAULT_ENV_FILE));
        let file_env = read_env_file_if_exists(&env_file)?;

        let base_url = env_or_file("VERLET_PLUGIN_LIVE_BASE_URL", &file_env)
            .or_else(|| env_or_file("VERLET_BIFROST_URL", &file_env))
            .or_else(|| env_or_file("LLM_PROXY_PUBLIC_URL", &file_env))
            .or_else(|| env_or_file("LLM_PROXY_URL", &file_env))
            .or_else(|| {
                env_or_file("OPENAI_API_KEY", &file_env)
                    .map(|_| "https://api.openai.com".to_string())
            })
            .ok_or("missing VERLET_PLUGIN_LIVE_BASE_URL, VERLET_BIFROST_URL, or OPENAI_API_KEY")?
            .trim_end_matches('/')
            .to_string();

        let api_key = env_or_file("VERLET_PLUGIN_LIVE_KEY", &file_env)
            .or_else(|| env_or_file("VERLET_BIFROST_KEY", &file_env))
            .or_else(|| env_or_file("BIFROST_SYSTEM_VIRTUAL_KEY", &file_env))
            .or_else(|| env_or_file("BIFROST_SYSTEM_KEY", &file_env))
            .or_else(|| env_or_file("OPENAI_API_KEY", &file_env))
            .ok_or("missing VERLET_PLUGIN_LIVE_KEY, VERLET_BIFROST_KEY, or OPENAI_API_KEY")?;

        let model = env_or_file("VERLET_PLUGIN_LIVE_MODEL", &file_env)
            .or_else(|| env_or_file("VERLET_BIFROST_OPENAI_MODEL", &file_env))
            .or_else(|| env_or_file("OPENAI_MODEL", &file_env))
            .unwrap_or_else(|| {
                if base_url.contains("api.openai.com") {
                    "gpt-4.1-mini".to_string()
                } else {
                    "openai/gpt-5.5".to_string()
                }
            });

        Ok(Self {
            base_url,
            api_key,
            model,
        })
    }
}

#[derive(Debug)]
struct LivePluginTrace {
    final_output: String,
}

async fn collect_live_plugin_trace(
    events: &mut tokio::sync::broadcast::Receiver<
        verlet::kernel::runtime_host::runtime_api::ThreadEvent,
    >,
) -> Result<LivePluginTrace, Box<dyn std::error::Error>> {
    let mut saw_tool_start = false;
    let mut saw_tool_result = false;
    let final_output = loop {
        let event =
            tokio::time::timeout(std::time::Duration::from_secs(120), events.recv()).await??;
        match event {
            verlet::kernel::runtime_host::runtime_api::ThreadEvent::Runtime { event, .. } => match event.kind {
                verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallStarted { name, input, .. }
                    if name == TOOL_NAME =>
                {
                    if input.get("input").and_then(|value| value.as_str())
                        != Some("/workspace/input.txt")
                    {
                        return Err(format!("{TOOL_NAME} was called with unexpected input").into());
                    }
                    saw_tool_start = true;
                }
                verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallResult {
                    output, success, ..
                } if success && output == EXPECTED_FILE_CONTENT => {
                    saw_tool_result = true;
                }
                verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallResult {
                    output, success, ..
                } if success => {
                    return Err(format!(
                        "{TOOL_NAME} returned unexpected output: {}",
                        compact(&output)
                    )
                    .into());
                }
                _ => {}
            },
            verlet::kernel::runtime_host::runtime_api::ThreadEvent::Output { text, .. } => {
                if text.contains(FINAL_MARKER) {
                    break text;
                }
            }
            verlet::kernel::runtime_host::runtime_api::ThreadEvent::Failed { message, .. } => return Err(message.into()),
            _ => {}
        }
    };

    if !saw_tool_start {
        return Err(format!("model answered without calling {TOOL_NAME}").into());
    }
    if !saw_tool_result {
        return Err(format!("{TOOL_NAME} did not return the expected file content").into());
    }
    if !final_output.contains(EXPECTED_FILE_CONTENT.trim_end()) {
        return Err(format!(
            "final answer did not confirm expected content; got {}",
            compact(&final_output)
        )
        .into());
    }

    Ok(LivePluginTrace { final_output })
}

fn read_env_file_if_exists(
    path: &std::path::Path,
) -> Result<std::collections::HashMap<String, String>, Box<dyn std::error::Error>> {
    if !path.exists() {
        return Ok(std::collections::HashMap::new());
    }
    let text = std::fs::read_to_string(path)?;
    let mut env = std::collections::HashMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        env.insert(key.trim().to_string(), unquote(value.trim()));
    }
    Ok(env)
}

fn env_or_file(key: &str, file_env: &std::collections::HashMap<String, String>) -> Option<String> {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| file_env.get(key).cloned())
        .filter(|value| !value.trim().is_empty())
}

fn unquote(value: &str) -> String {
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .or_else(|| {
            value
                .strip_prefix('\'')
                .and_then(|value| value.strip_suffix('\''))
        })
        .unwrap_or(value)
        .to_string()
}

fn compact(text: &str) -> String {
    let one_line = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if one_line.len() <= 240 {
        one_line
    } else {
        format!("{}...", &one_line[..240])
    }
}
