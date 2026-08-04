use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use uuid::Uuid;
use verlet::{
    AgentLoopConfig, AgentLoopFactory, LocalOperationRegistry, LocalPluginCatalog,
    LocalPluginCatalogConfig, OpenAIReasoningSummary, OpenAIResponsesAdapter, PluginMount,
    ProviderApi, ProviderEndpoint, ProviderHttpClient, ProviderWireAdapter,
    PublishOperationRequest, PublishedOperationSource, RuntimeEventKind, RuntimeHost,
    RustWasmBuildOptions, SystemBlock, ThreadCoordinates, ThreadEvent, ThreadTopology,
    build_rust_wasm_module,
};

const DEFAULT_ENV_FILE: &str = ".env";
const TOOL_NAME: &str = "tailcat_cat";
const EXPECTED_FILE_CONTENT: &str = "hello from live plugin llm smoke\n";
const FINAL_MARKER: &str = "VERLET_PLUGIN_TOOL_OK";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = SmokeConfig::load()?;
    let temp = std::env::temp_dir().join(format!("verlet-plugin-live-{}", Uuid::now_v7()));
    std::fs::create_dir_all(&temp)?;
    let registry_root = temp.join("plugins");
    let workspace = temp.join("workspace");
    std::fs::create_dir_all(&workspace)?;
    std::fs::write(workspace.join("input.txt"), EXPECTED_FILE_CONTENT)?;

    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let module_path = repo.join("tests/fixtures/wasm-vfs-tools");
    let build = build_rust_wasm_module(RustWasmBuildOptions::new(&module_path))?;
    LocalOperationRegistry::new(&registry_root)
        .publish_artifact(PublishOperationRequest {
            name: "tailcat".to_string(),
            artifact_path: build.artifact_path,
            source: PublishedOperationSource::Rust {
                module_path,
                release: true,
            },
            interface: None,
            capability_grants: BTreeSet::new(),
            metadata: BTreeMap::new(),
        })
        .await?;

    let catalog = LocalPluginCatalog::load(
        LocalPluginCatalogConfig::new(&registry_root)
            .with_mount(PluginMount::host_read_only("/workspace", &workspace)),
    )
    .await?;

    let adapter: Arc<dyn ProviderWireAdapter> = Arc::new(OpenAIResponsesAdapter {
        include_encrypted_reasoning: false,
        reasoning_summary: OpenAIReasoningSummary::Auto,
    });
    let client = Arc::new(ProviderHttpClient::new(
        ProviderEndpoint::openai_responses(&config.base_url, config.api_key.clone()),
        adapter,
    )?);
    let mut runtime_config =
        AgentLoopConfig::new(ProviderApi::OpenAIResponses, "openai", config.model.clone());
    runtime_config.max_tokens = 256;
    runtime_config.system.push(SystemBlock::text(format!(
        "You are testing a newly installed Verlet plugin. You must call the {TOOL_NAME} tool with input /workspace/input.txt before answering. After the tool result is visible, reply with exactly: {FINAL_MARKER}: <file content>. Do not invent the file content."
    )));

    let host = RuntimeHost::new(Arc::new(
        AgentLoopFactory::new(runtime_config, client)
            .with_operation_registry(catalog.operation_registry()),
    ));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("smoke_tenant", "smoke_user", "plugin_live"),
            ThreadTopology::root(),
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
        let env_file = verlet_runtime_contracts::env_compat::var("VERLET_PLUGIN_LIVE_ENV_FILE")
            .or_else(|_| verlet_runtime_contracts::env_compat::var("VERLET_BIFROST_ENV_FILE"))
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(DEFAULT_ENV_FILE));
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
    events: &mut tokio::sync::broadcast::Receiver<ThreadEvent>,
) -> Result<LivePluginTrace, Box<dyn std::error::Error>> {
    let mut saw_tool_start = false;
    let mut saw_tool_result = false;
    let final_output = loop {
        let event = timeout(Duration::from_secs(120), events.recv()).await??;
        match event {
            ThreadEvent::Runtime { event, .. } => match event.kind {
                RuntimeEventKind::ToolCallStarted { name, input, .. } if name == TOOL_NAME => {
                    if input.get("input").and_then(|value| value.as_str())
                        != Some("/workspace/input.txt")
                    {
                        return Err(format!("{TOOL_NAME} was called with unexpected input").into());
                    }
                    saw_tool_start = true;
                }
                RuntimeEventKind::ToolCallResult {
                    output, success, ..
                } if success && output == EXPECTED_FILE_CONTENT => {
                    saw_tool_result = true;
                }
                RuntimeEventKind::ToolCallResult {
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
            ThreadEvent::Output { text, .. } => {
                if text.contains(FINAL_MARKER) {
                    break text;
                }
            }
            ThreadEvent::Failed { message, .. } => return Err(message.into()),
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
    path: &Path,
) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let text = std::fs::read_to_string(path)?;
    let mut env = HashMap::new();
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

fn env_or_file(key: &str, file_env: &HashMap<String, String>) -> Option<String> {
    verlet_runtime_contracts::env_compat::var(key)
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
