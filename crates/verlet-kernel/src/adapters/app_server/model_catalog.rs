//! Catalog seam between `model/list` and model-metadata sources (EMO-558).
//!
//! `model/list` composes its entries from this seam so the built-in snapshot
//! and models.dev refresh (EMO-561) can plug in behind it without touching
//! the RPC layer again.

const BUILT_IN_SNAPSHOT_JSON: &str = include_str!("../../../data/model-catalog.json");
const DEFAULT_MODEL_CATALOG_URL: &str = "https://models.dev/api.json";
const MODEL_CATALOG_URL_ENV: &str = "VERLET_MODEL_CATALOG_URL";
const CATALOG_CACHE_DIR: &str = "model-catalog";
const CATALOG_CACHE_FILE: &str = "models.json";
const REFRESH_STATE_FILE: &str = "refresh.json";
const MAX_REMOTE_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const REFRESH_INTERVAL_SECS: u64 = 24 * 60 * 60;

pub(crate) const CATALOG_API_OPENAI_CHAT_COMPLETIONS: &str = "openai_chat_completions";
pub(crate) const CATALOG_API_ANTHROPIC_MESSAGES: &str = "anthropic_messages";
pub(crate) const CATALOG_API_OPENAI_RESPONSES: &str = "openai_responses";
pub(crate) const CATALOG_AUTH_KIND_API_KEY: &str = "api_key";
pub(crate) const CATALOG_AUTH_KIND_OAUTH: &str = "oauth";

/// Trust-bearing base-URL/family pins for majors whose models.dev entry has no
/// `api` URL (or a non-derivable SDK); each URL is verified against the
/// provider's public API docs. Overrides win over derivation.
const PROVIDER_OVERRIDES: &[(&str, &str, &str)] = &[
    (
        "anthropic",
        "https://api.anthropic.com",
        CATALOG_API_ANTHROPIC_MESSAGES,
    ),
    (
        "cerebras",
        "https://api.cerebras.ai/v1",
        CATALOG_API_OPENAI_CHAT_COMPLETIONS,
    ),
    (
        "deepinfra",
        "https://api.deepinfra.com/v1/openai",
        CATALOG_API_OPENAI_CHAT_COMPLETIONS,
    ),
    (
        "deepseek",
        "https://api.deepseek.com",
        CATALOG_API_OPENAI_CHAT_COMPLETIONS,
    ),
    (
        "groq",
        "https://api.groq.com/openai/v1",
        CATALOG_API_OPENAI_CHAT_COMPLETIONS,
    ),
    (
        "mistral",
        "https://api.mistral.ai/v1",
        CATALOG_API_OPENAI_CHAT_COMPLETIONS,
    ),
    (
        "openai",
        "https://api.openai.com/v1",
        CATALOG_API_OPENAI_CHAT_COMPLETIONS,
    ),
    (
        "openrouter",
        "https://openrouter.ai/api/v1",
        CATALOG_API_OPENAI_CHAT_COMPLETIONS,
    ),
    (
        "perplexity",
        "https://api.perplexity.ai",
        CATALOG_API_OPENAI_CHAT_COMPLETIONS,
    ),
    (
        "togetherai",
        "https://api.together.xyz/v1",
        CATALOG_API_OPENAI_CHAT_COMPLETIONS,
    ),
    (
        "xai",
        "https://api.x.ai/v1",
        CATALOG_API_OPENAI_CHAT_COMPLETIONS,
    ),
];

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ModelCatalogSnapshot {
    #[serde(rename = "_comment", default, skip_serializing_if = "Option::is_none")]
    comment: Option<String>,
    // Cache files written before EMO-574 have no provider section; defaulting
    // keeps their models usable while providers fall back to the built-in set.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    providers: Vec<ModelCatalogProvider>,
    models: Vec<ModelCatalogModel>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ModelCatalogProvider {
    pub(crate) provider_id: String,
    pub(crate) display_name: String,
    pub(crate) base_url: String,
    /// One of the `CATALOG_API_*` families verlet-provider can speak.
    pub(crate) api: String,
    /// `api_key` for every provider except the OAuth-only `openai-codex`.
    pub(crate) auth_kind: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) env_vars: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) doc_url: Option<String>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ModelCatalogModel {
    provider_id: String,
    model_id: String,
    display_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context_window: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_output_tokens: Option<u64>,
    /// USD per one million input tokens, as reported by models.dev.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    input_price: Option<f64>,
    /// USD per one million output tokens, as reported by models.dev.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    output_price: Option<f64>,
    #[serde(default)]
    reasoning: bool,
    #[serde(default)]
    deprecated: bool,
}

#[derive(serde::Deserialize)]
struct ModelsDevProvider {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    env: Vec<String>,
    #[serde(default)]
    npm: Option<String>,
    #[serde(default)]
    doc: Option<String>,
    #[serde(default)]
    api: Option<String>,
    #[serde(default)]
    models: std::collections::BTreeMap<String, serde_json::Value>,
}

#[derive(serde::Deserialize)]
struct ModelsDevModel {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    limit: ModelsDevLimit,
    #[serde(default)]
    cost: ModelsDevCost,
    #[serde(default)]
    reasoning: bool,
    #[serde(default)]
    status: Option<String>,
}

#[derive(Default, serde::Deserialize)]
struct ModelsDevLimit {
    context: Option<u64>,
    output: Option<u64>,
}

#[derive(Default, serde::Deserialize)]
struct ModelsDevCost {
    input: Option<f64>,
    output: Option<f64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
struct CatalogRefreshState {
    checked_at_unix_secs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_modified: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CatalogRefreshOutcome {
    Updated,
    NotModified,
    SkippedFresh,
}

#[derive(Debug, thiserror::Error)]
enum CatalogRefreshError {
    #[error("catalog cache I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("catalog JSON was invalid: {0}")]
    Json(#[from] serde_json::Error),
    #[error("catalog HTTP client could not be constructed")]
    Client,
    #[error("catalog HTTP request failed")]
    Request,
    #[error("catalog HTTP response returned {0}")]
    HttpStatus(reqwest::StatusCode),
    #[error("catalog HTTP response exceeded {MAX_REMOTE_BYTES} bytes")]
    ResponseTooLarge,
    #[error("catalog response contained no supported models")]
    EmptyCatalog,
}

struct CatalogRefreshOptions {
    state_home: std::path::PathBuf,
    url: String,
    now_unix_secs: u64,
    request_timeout: std::time::Duration,
    retry_delays: Vec<std::time::Duration>,
}

impl CatalogRefreshOptions {
    fn for_runtime_with_env(
        state_home: std::path::PathBuf,
        read_env: impl FnOnce(&str) -> Result<String, std::env::VarError>,
    ) -> Option<Self> {
        let url = match read_env(MODEL_CATALOG_URL_ENV) {
            Ok(value) if value.trim().is_empty() => return None,
            Ok(value) => value.trim().to_string(),
            Err(std::env::VarError::NotPresent) => DEFAULT_MODEL_CATALOG_URL.to_string(),
            // A configured but non-Unicode value must not unexpectedly fall
            // back to ambient network access.
            Err(std::env::VarError::NotUnicode(_)) => return None,
        };
        let now_unix_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Some(Self {
            state_home,
            url,
            now_unix_secs,
            request_timeout: std::time::Duration::from_secs(4),
            retry_delays: jittered_retry_delays(),
        })
    }

    #[cfg(test)]
    fn for_test(state_home: std::path::PathBuf, url: String, now_unix_secs: u64) -> Self {
        Self {
            state_home,
            url,
            now_unix_secs,
            request_timeout: std::time::Duration::from_secs(2),
            retry_delays: vec![
                std::time::Duration::from_millis(1),
                std::time::Duration::from_millis(1),
            ],
        }
    }
}

fn jittered_retry_delays() -> Vec<std::time::Duration> {
    let jitter = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_millis() as u64
        % 100;
    vec![
        std::time::Duration::from_millis(150 + jitter),
        std::time::Duration::from_millis(350 + ((jitter * 37) % 100)),
    ]
}

pub(crate) fn spawn_runtime_refresh(
    tasks: &std::sync::Arc<super::lifecycle::InstanceTaskSet>,
    user_state_home: std::path::PathBuf,
) -> bool {
    spawn_runtime_refresh_with_env(tasks, user_state_home, |name| std::env::var(name))
}

fn spawn_runtime_refresh_with_env(
    tasks: &std::sync::Arc<super::lifecycle::InstanceTaskSet>,
    user_state_home: std::path::PathBuf,
    read_env: impl FnOnce(&str) -> Result<String, std::env::VarError>,
) -> bool {
    let Some(options) = CatalogRefreshOptions::for_runtime_with_env(user_state_home, read_env)
    else {
        return false;
    };
    spawn_runtime_refresh_with_options(tasks, options)
}

fn spawn_runtime_refresh_with_options(
    tasks: &std::sync::Arc<super::lifecycle::InstanceTaskSet>,
    options: CatalogRefreshOptions,
) -> bool {
    // Abandonment is safe: awaits only cover network/sleep work, while every
    // cache mutation is a non-awaiting atomic_write transaction.
    tasks.spawn_cancellable(async move {
        if let Err(error) = refresh_catalog(&options).await {
            log::debug!("model catalog refresh failed; using cached or built-in data: {error}");
        }
    })
}

fn built_in_snapshot() -> &'static ModelCatalogSnapshot {
    static SNAPSHOT: std::sync::OnceLock<ModelCatalogSnapshot> = std::sync::OnceLock::new();
    SNAPSHOT.get_or_init(|| {
        let snapshot: ModelCatalogSnapshot = serde_json::from_str(BUILT_IN_SNAPSHOT_JSON)
            .expect("checked-in model catalog snapshot must be valid JSON");
        sanitize_snapshot(snapshot)
    })
}

fn normalize_models_dev_json(bytes: &[u8]) -> Result<ModelCatalogSnapshot, CatalogRefreshError> {
    let upstream: std::collections::BTreeMap<String, serde_json::Value> =
        serde_json::from_slice(bytes)?;
    let mut providers = Vec::new();
    let mut models = Vec::new();
    for (provider_id, value) in upstream {
        // The static OAuth entry below is authoritative for openai-codex.
        if provider_id == verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID {
            continue;
        }
        let Ok(provider) = serde_json::from_value::<ModelsDevProvider>(value) else {
            continue;
        };
        let Some((base_url, api)) = derive_provider_endpoint(&provider_id, &provider) else {
            continue;
        };
        providers.push(ModelCatalogProvider {
            display_name: provider
                .name
                .as_deref()
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .unwrap_or(&provider_id)
                .to_string(),
            base_url,
            api: api.to_string(),
            auth_kind: CATALOG_AUTH_KIND_API_KEY.to_string(),
            env_vars: provider.env.clone(),
            doc_url: provider.doc.clone(),
            provider_id: provider_id.clone(),
        });
        let provider_models = normalize_provider(&provider_id, &provider);
        if provider_id == "openai" {
            models.extend(
                provider_models
                    .iter()
                    .filter(|model| is_openai_codex_model(&model.model_id))
                    .cloned()
                    .map(|mut model| {
                        model.provider_id =
                            verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID.to_string();
                        model
                    }),
            );
        }
        models.extend(provider_models);
    }
    providers.push(openai_codex_catalog_provider());
    let snapshot = sanitize_snapshot(ModelCatalogSnapshot {
        comment: None,
        providers,
        models,
    });
    if snapshot.models.is_empty() {
        return Err(CatalogRefreshError::EmptyCatalog);
    }
    Ok(snapshot)
}

fn derive_provider_endpoint(
    provider_id: &str,
    provider: &ModelsDevProvider,
) -> Option<(String, &'static str)> {
    if let Some((_, base_url, api)) = PROVIDER_OVERRIDES
        .iter()
        .find(|(overridden, _, _)| *overridden == provider_id)
    {
        return Some(((*base_url).to_string(), api));
    }
    let base_url = provider
        .api
        .as_deref()
        .map(str::trim)
        .filter(|url| !url.is_empty())?;
    match provider.npm.as_deref() {
        Some("@ai-sdk/openai-compatible") => {
            Some((base_url.to_string(), CATALOG_API_OPENAI_CHAT_COMPLETIONS))
        }
        Some("@ai-sdk/anthropic") => Some((base_url.to_string(), CATALOG_API_ANTHROPIC_MESSAGES)),
        _ => None,
    }
}

fn openai_codex_catalog_provider() -> ModelCatalogProvider {
    ModelCatalogProvider {
        provider_id: verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID.to_string(),
        display_name: "OpenAI Codex (ChatGPT plan)".to_string(),
        base_url: verlet_metadata::provider_store::OPENAI_CODEX_RESPONSES_URL.to_string(),
        api: CATALOG_API_OPENAI_RESPONSES.to_string(),
        auth_kind: CATALOG_AUTH_KIND_OAUTH.to_string(),
        env_vars: Vec::new(),
        doc_url: None,
    }
}

fn normalize_provider(provider_id: &str, provider: &ModelsDevProvider) -> Vec<ModelCatalogModel> {
    provider
        .models
        .iter()
        .filter_map(|(key, value)| {
            let model: ModelsDevModel = serde_json::from_value(value.clone()).ok()?;
            let model_id = model
                .id
                .as_deref()
                .filter(|id| !id.trim().is_empty())
                .unwrap_or(key)
                .trim();
            if model_id.is_empty() {
                return None;
            }
            let display_name = model
                .name
                .as_deref()
                .filter(|name| !name.trim().is_empty())
                .unwrap_or(model_id)
                .trim();
            Some(ModelCatalogModel {
                provider_id: provider_id.to_string(),
                model_id: model_id.to_string(),
                display_name: display_name.to_string(),
                context_window: model.limit.context,
                max_output_tokens: model.limit.output,
                input_price: model.cost.input,
                output_price: model.cost.output,
                reasoning: model.reasoning,
                deprecated: model.status.as_deref() == Some("deprecated"),
            })
        })
        .collect()
}

fn is_openai_codex_model(model_id: &str) -> bool {
    let normalized = model_id.to_ascii_lowercase();
    normalized.contains("codex") || matches!(normalized.as_str(), "gpt-5.6-sol" | "gpt-5.6-terra")
}

/// Providers must publish an absolute `https` URL; plain `http` is accepted
/// only for loopback hosts (local inference servers), because a cleartext
/// remote endpoint would leak API keys, and `${...}` templates never resolve.
fn valid_catalog_base_url(base_url: &str) -> bool {
    if base_url.contains("${") {
        return false;
    }
    let Some((scheme, rest)) = base_url.split_once("://") else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let host = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    let host = match host.strip_prefix('[') {
        Some(bracketed) => bracketed
            .split_once(']')
            .map(|(host, _)| host)
            .unwrap_or_default(),
        None => host.split_once(':').map_or(host, |(host, _)| host),
    };
    if host.is_empty() {
        return false;
    }
    match scheme {
        "https" => true,
        "http" => matches!(
            host.to_ascii_lowercase().as_str(),
            "localhost" | "127.0.0.1" | "::1"
        ),
        _ => false,
    }
}

fn sanitize_snapshot(snapshot: ModelCatalogSnapshot) -> ModelCatalogSnapshot {
    let mut providers = snapshot
        .providers
        .into_iter()
        .filter_map(|mut provider| {
            provider.provider_id = provider.provider_id.trim().to_string();
            provider.display_name = provider.display_name.trim().to_string();
            if provider.display_name.is_empty() {
                provider.display_name.clone_from(&provider.provider_id);
            }
            provider.base_url = provider.base_url.trim().to_string();
            provider.env_vars = provider
                .env_vars
                .iter()
                .map(|name| name.trim().to_string())
                .filter(|name| !name.is_empty())
                .collect();
            provider.doc_url = provider
                .doc_url
                .map(|url| url.trim().to_string())
                .filter(|url| !url.is_empty());
            (!provider.provider_id.is_empty()
                && valid_catalog_base_url(&provider.base_url)
                && matches!(
                    provider.api.as_str(),
                    CATALOG_API_OPENAI_CHAT_COMPLETIONS
                        | CATALOG_API_ANTHROPIC_MESSAGES
                        | CATALOG_API_OPENAI_RESPONSES
                )
                && matches!(
                    provider.auth_kind.as_str(),
                    CATALOG_AUTH_KIND_API_KEY | CATALOG_AUTH_KIND_OAUTH
                ))
            .then_some(provider)
        })
        .collect::<Vec<_>>();
    providers.sort_by(|left, right| left.provider_id.cmp(&right.provider_id));
    providers.dedup_by(|left, right| left.provider_id == right.provider_id);
    let provider_ids = providers
        .iter()
        .map(|provider| provider.provider_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let mut models = snapshot
        .models
        .into_iter()
        .filter_map(|mut model| {
            model.provider_id = model.provider_id.trim().to_string();
            model.model_id = model.model_id.trim().to_string();
            model.display_name = model.display_name.trim().to_string();
            if model.display_name.is_empty() {
                model.display_name.clone_from(&model.model_id);
            }
            model.context_window = model.context_window.filter(|value| *value > 0);
            model.max_output_tokens = model.max_output_tokens.filter(|value| *value > 0);
            model.input_price = model
                .input_price
                .filter(|value| value.is_finite() && *value >= 0.0);
            model.output_price = model
                .output_price
                .filter(|value| value.is_finite() && *value >= 0.0);
            // A pre-providers snapshot carries no provider set to check against.
            (provider_ids.is_empty() || provider_ids.contains(&model.provider_id))
                .then_some(model)
                .filter(|model| {
                    !model.provider_id.is_empty()
                        && !model.model_id.is_empty()
                        && (model.provider_id
                            != verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID
                            || is_openai_codex_model(&model.model_id))
                })
        })
        .collect::<Vec<_>>();
    models.sort_by(|left, right| {
        (&left.provider_id, &left.model_id).cmp(&(&right.provider_id, &right.model_id))
    });
    models.dedup_by(|left, right| {
        left.provider_id == right.provider_id && left.model_id == right.model_id
    });
    ModelCatalogSnapshot {
        comment: snapshot.comment,
        providers,
        models,
    }
}

/// Built-in catalog overlaid by the last valid models.dev cache.
///
/// Cache reads happen when the seam is read, so a source retained by EMO-558
/// observes a background refresh without coupling the refresh task to RPC state.
pub(crate) struct MergedModelCatalog {
    state_home: std::path::PathBuf,
}

impl MergedModelCatalog {
    pub(crate) fn new(state_home: impl Into<std::path::PathBuf>) -> Self {
        Self {
            state_home: state_home.into(),
        }
    }

    fn full_entries(&self) -> Vec<ModelCatalogModel> {
        let mut merged = built_in_snapshot()
            .models
            .iter()
            .cloned()
            .map(|model| ((model.provider_id.clone(), model.model_id.clone()), model))
            .collect::<std::collections::BTreeMap<_, _>>();
        match read_catalog_cache(&self.state_home) {
            Ok(Some(cached)) => {
                for model in cached.models {
                    merged.insert((model.provider_id.clone(), model.model_id.clone()), model);
                }
            }
            Ok(None) => {}
            Err(error) => {
                log::debug!("model catalog cache could not be read; using built-in data: {error}");
            }
        }
        merged
            .into_values()
            .filter(|model| !model.deprecated)
            .collect()
    }

    pub(crate) fn providers(&self) -> Vec<ModelCatalogProvider> {
        let mut merged = built_in_snapshot()
            .providers
            .iter()
            .cloned()
            .map(|provider| (provider.provider_id.clone(), provider))
            .collect::<std::collections::BTreeMap<_, _>>();
        match read_catalog_cache(&self.state_home) {
            // A pre-providers cache keeps its models but contributes no
            // provider metadata; the built-in provider set stays authoritative.
            Ok(Some(cached)) => {
                for provider in cached.providers {
                    let Some(reviewed) = merged.get_mut(&provider.provider_id) else {
                        continue;
                    };
                    // Endpoint and auth semantics are trust-bearing and ship
                    // only in the reviewed snapshot. The refresh may update
                    // non-endpoint provider display metadata.
                    reviewed.display_name = provider.display_name;
                    reviewed.env_vars = provider.env_vars;
                    reviewed.doc_url = provider.doc_url;
                }
            }
            Ok(None) => {}
            Err(error) => {
                log::debug!("model catalog cache could not be read; using built-in data: {error}");
            }
        }
        merged.into_values().collect()
    }
}

impl ModelCatalogSource for MergedModelCatalog {
    fn entries(&self) -> Vec<ModelCatalogEntry> {
        self.full_entries()
            .into_iter()
            .map(|model| ModelCatalogEntry {
                provider_id: model.provider_id,
                model_id: model.model_id,
                display_name: model.display_name,
                context_window: model.context_window,
                max_output_tokens: model.max_output_tokens,
            })
            .collect()
    }
}

async fn refresh_catalog(
    options: &CatalogRefreshOptions,
) -> Result<CatalogRefreshOutcome, CatalogRefreshError> {
    let mut previous = match read_refresh_state(&options.state_home) {
        Ok(state) => state.unwrap_or_default(),
        Err(error) => {
            log::debug!("model catalog refresh state was invalid; refreshing anyway: {error}");
            CatalogRefreshState::default()
        }
    };
    // Validators only describe a usable cached representation. Sending them
    // after cache loss can produce a 304 that cannot restore that cache.
    if !matches!(read_catalog_cache(&options.state_home), Ok(Some(_))) {
        previous.etag = None;
        previous.last_modified = None;
    }
    if previous.checked_at_unix_secs > 0
        && (options.now_unix_secs < previous.checked_at_unix_secs
            || options
                .now_unix_secs
                .saturating_sub(previous.checked_at_unix_secs)
                < REFRESH_INTERVAL_SECS)
    {
        return Ok(CatalogRefreshOutcome::SkippedFresh);
    }

    let result = fetch_catalog(options, &previous).await;
    let mut next_state = previous;
    next_state.checked_at_unix_secs = options.now_unix_secs;
    match result {
        Ok(FetchedCatalog::Updated {
            snapshot,
            etag,
            last_modified,
        }) => {
            write_catalog_cache(&options.state_home, &snapshot)?;
            next_state.etag = etag;
            next_state.last_modified = last_modified;
            write_refresh_state(&options.state_home, &next_state)?;
            Ok(CatalogRefreshOutcome::Updated)
        }
        Ok(FetchedCatalog::NotModified {
            etag,
            last_modified,
        }) => {
            if etag.is_some() {
                next_state.etag = etag;
            }
            if last_modified.is_some() {
                next_state.last_modified = last_modified;
            }
            write_refresh_state(&options.state_home, &next_state)?;
            Ok(CatalogRefreshOutcome::NotModified)
        }
        Err(error) => {
            if let Err(state_error) = write_refresh_state(&options.state_home, &next_state) {
                log::debug!("model catalog failed attempt could not be recorded: {state_error}");
            }
            Err(error)
        }
    }
}

enum FetchedCatalog {
    Updated {
        snapshot: ModelCatalogSnapshot,
        etag: Option<String>,
        last_modified: Option<String>,
    },
    NotModified {
        etag: Option<String>,
        last_modified: Option<String>,
    },
}

async fn fetch_catalog(
    options: &CatalogRefreshOptions,
    previous: &CatalogRefreshState,
) -> Result<FetchedCatalog, CatalogRefreshError> {
    let client = reqwest::Client::builder()
        .timeout(options.request_timeout)
        .user_agent("verlet-model-catalog/0.1")
        .build()
        .map_err(|_| CatalogRefreshError::Client)?;

    for attempt in 0..=options.retry_delays.len() {
        let mut request = client.get(&options.url);
        if let Some(value) = previous.etag.as_deref().and_then(valid_header_value) {
            request = request.header(reqwest::header::IF_NONE_MATCH, value);
        }
        if let Some(value) = previous
            .last_modified
            .as_deref()
            .and_then(valid_header_value)
        {
            request = request.header(reqwest::header::IF_MODIFIED_SINCE, value);
        }
        match request.send().await {
            Ok(response) if response.status() == reqwest::StatusCode::NOT_MODIFIED => {
                return Ok(FetchedCatalog::NotModified {
                    etag: response_header(&response, reqwest::header::ETAG),
                    last_modified: response_header(&response, reqwest::header::LAST_MODIFIED),
                });
            }
            Ok(response) if response.status().is_success() => {
                let etag = response_header(&response, reqwest::header::ETAG);
                let last_modified = response_header(&response, reqwest::header::LAST_MODIFIED);
                let bytes = bounded_response_body(response).await?;
                let snapshot = normalize_models_dev_json(&bytes)?;
                return Ok(FetchedCatalog::Updated {
                    snapshot,
                    etag,
                    last_modified,
                });
            }
            Ok(response)
                if (response.status().is_server_error()
                    || response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS)
                    && attempt < options.retry_delays.len() => {}
            Ok(response) => return Err(CatalogRefreshError::HttpStatus(response.status())),
            Err(_) if attempt < options.retry_delays.len() => {}
            Err(_) => return Err(CatalogRefreshError::Request),
        }
        tokio::time::sleep(options.retry_delays[attempt]).await;
    }
    Err(CatalogRefreshError::Request)
}

fn valid_header_value(value: &str) -> Option<reqwest::header::HeaderValue> {
    reqwest::header::HeaderValue::from_str(value).ok()
}

fn response_header(
    response: &reqwest::Response,
    name: reqwest::header::HeaderName,
) -> Option<String> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

async fn bounded_response_body(
    response: reqwest::Response,
) -> Result<Vec<u8>, CatalogRefreshError> {
    use futures_util::StreamExt as _;

    if response
        .content_length()
        .is_some_and(|length| length > MAX_REMOTE_BYTES as u64)
    {
        return Err(CatalogRefreshError::ResponseTooLarge);
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| CatalogRefreshError::Request)?;
        if body.len().saturating_add(chunk.len()) > MAX_REMOTE_BYTES {
            return Err(CatalogRefreshError::ResponseTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn catalog_cache_path(state_home: &std::path::Path) -> std::path::PathBuf {
    state_home.join(CATALOG_CACHE_DIR).join(CATALOG_CACHE_FILE)
}

fn refresh_state_path(state_home: &std::path::Path) -> std::path::PathBuf {
    state_home.join(CATALOG_CACHE_DIR).join(REFRESH_STATE_FILE)
}

fn read_catalog_cache(
    state_home: &std::path::Path,
) -> Result<Option<ModelCatalogSnapshot>, CatalogRefreshError> {
    let bytes = match std::fs::read(catalog_cache_path(state_home)) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let snapshot = serde_json::from_slice(&bytes)?;
    Ok(Some(sanitize_snapshot(snapshot)))
}

fn write_catalog_cache(
    state_home: &std::path::Path,
    snapshot: &ModelCatalogSnapshot,
) -> Result<(), CatalogRefreshError> {
    atomic_write(
        &catalog_cache_path(state_home),
        &render_snapshot_json(snapshot)?,
    )?;
    Ok(())
}

/// Deterministic snapshot serialization shared by the cache writer and the
/// checked-in snapshot regeneration entry point.
fn render_snapshot_json(snapshot: &ModelCatalogSnapshot) -> Result<Vec<u8>, CatalogRefreshError> {
    let mut bytes = serde_json::to_vec_pretty(snapshot)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn read_refresh_state(
    state_home: &std::path::Path,
) -> Result<Option<CatalogRefreshState>, CatalogRefreshError> {
    let bytes = match std::fs::read(refresh_state_path(state_home)) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    Ok(Some(serde_json::from_slice(&bytes)?))
}

fn write_refresh_state(
    state_home: &std::path::Path,
    state: &CatalogRefreshState,
) -> Result<(), CatalogRefreshError> {
    let mut bytes = serde_json::to_vec_pretty(state)?;
    bytes.push(b'\n');
    atomic_write(&refresh_state_path(state_home), &bytes)?;
    Ok(())
}

fn atomic_write(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;

    let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
    std::fs::create_dir_all(parent)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
    }
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("model-catalog"),
        uuid::Uuid::now_v7()
    ));
    let result = (|| {
        let mut options = std::fs::OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        replace_file(&temporary, path)?;
        #[cfg(unix)]
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

#[cfg(not(windows))]
fn replace_file(temporary: &std::path::Path, path: &std::path::Path) -> std::io::Result<()> {
    std::fs::rename(temporary, path)
}

#[cfg(windows)]
fn replace_file(temporary: &std::path::Path, path: &std::path::Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt as _;

    let temporary = temporary
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let path = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let flags = windows_sys::Win32::Storage::FileSystem::MOVEFILE_REPLACE_EXISTING
        | windows_sys::Win32::Storage::FileSystem::MOVEFILE_WRITE_THROUGH;
    // SAFETY: both pointers reference NUL-terminated UTF-16 buffers that stay
    // alive for the duration of the call.
    let replaced = unsafe {
        windows_sys::Win32::Storage::FileSystem::MoveFileExW(
            temporary.as_ptr(),
            path.as_ptr(),
            flags,
        )
    };
    if replaced == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// One selectable model as surfaced by `model/list`.
///
/// Auth status and the active flag are not part of the entry: the RPC layer
/// annotates them per request from the provider store and the live
/// [`super::ActiveModelSelection`].
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ModelCatalogEntry {
    pub(crate) provider_id: String,
    pub(crate) model_id: String,
    pub(crate) display_name: String,
    /// Total context window in tokens, when known.
    pub(crate) context_window: Option<u64>,
    /// Maximum output tokens per response, when known.
    pub(crate) max_output_tokens: Option<u64>,
}

/// Source of catalog entries consulted by `model/list`.
///
/// EMO-558 implements `model/list` over this trait. App-server model listing
/// should use [`MergedModelCatalog`] so the checked-in snapshot remains the
/// offline floor and a valid models.dev cache overlays it.
pub(crate) trait ModelCatalogSource: Send + Sync {
    /// Every known model, in source order. The RPC layer owns ordering,
    /// dedup by (provider, model), and per-request annotation.
    fn entries(&self) -> Vec<ModelCatalogEntry>;
}

/// Fixed-entry source retained for callers that inject an explicit catalog.
pub(crate) struct StaticModelCatalog {
    entries: Vec<ModelCatalogEntry>,
}

impl StaticModelCatalog {
    pub(crate) fn new(entries: Vec<ModelCatalogEntry>) -> Self {
        Self { entries }
    }
}

impl ModelCatalogSource for StaticModelCatalog {
    fn entries(&self) -> Vec<ModelCatalogEntry> {
        self.entries.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::ModelCatalogSource as _;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    const FIRST_CHECK_SECS: u64 = 1_800_000_000;

    #[test]
    fn checked_in_snapshot_parses_and_carries_the_full_schema() {
        let snapshot = super::built_in_snapshot();

        assert!(!snapshot.models.is_empty());
        assert!(snapshot.providers.len() > 100);
        let provider_ids = snapshot
            .providers
            .iter()
            .map(|provider| provider.provider_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(
            snapshot
                .models
                .iter()
                .all(|model| provider_ids.contains(model.provider_id.as_str()))
        );
        let anthropic = snapshot
            .providers
            .iter()
            .find(|provider| provider.provider_id == "anthropic")
            .unwrap();
        assert_eq!(anthropic.base_url, "https://api.anthropic.com");
        assert_eq!(anthropic.api, super::CATALOG_API_ANTHROPIC_MESSAGES);
        assert_eq!(anthropic.auth_kind, super::CATALOG_AUTH_KIND_API_KEY);
        assert!(
            anthropic
                .env_vars
                .contains(&"ANTHROPIC_API_KEY".to_string())
        );
        let openai = snapshot
            .providers
            .iter()
            .find(|provider| provider.provider_id == "openai")
            .unwrap();
        assert_eq!(openai.base_url, "https://api.openai.com/v1");
        assert_eq!(openai.api, super::CATALOG_API_OPENAI_CHAT_COMPLETIONS);
        let codex = snapshot
            .providers
            .iter()
            .find(|provider| provider.provider_id == "openai-codex")
            .unwrap();
        assert_eq!(
            codex.base_url,
            verlet_metadata::provider_store::OPENAI_CODEX_RESPONSES_URL
        );
        assert_eq!(codex.api, super::CATALOG_API_OPENAI_RESPONSES);
        assert_eq!(codex.auth_kind, super::CATALOG_AUTH_KIND_OAUTH);
        assert!(
            snapshot
                .providers
                .iter()
                .all(|provider| super::valid_catalog_base_url(&provider.base_url)),
            "the checked-in snapshot must not carry unusable or cleartext base URLs"
        );
        assert!(snapshot.models.iter().any(|model| {
            model.provider_id == "anthropic"
                && model.model_id == "claude-sonnet-4-6"
                && model.context_window == Some(1_000_000)
                && model.max_output_tokens == Some(128_000)
                && model.input_price == Some(3.0)
                && model.output_price == Some(15.0)
                && model.reasoning
        }));
        assert!(snapshot.models.iter().any(|model| {
            model.provider_id == "openai-codex"
                && model.model_id.contains("codex")
                && !model.display_name.is_empty()
        }));

        let offline_home = std::env::temp_dir().join(format!(
            "verlet-model-catalog-offline-{}",
            uuid::Uuid::now_v7()
        ));
        let offline_entries = super::MergedModelCatalog::new(offline_home).entries();
        assert_eq!(
            offline_entries.len(),
            snapshot
                .models
                .iter()
                .filter(|model| !model.deprecated)
                .count()
        );
        assert!(!offline_entries.is_empty());
        assert!(offline_entries.iter().all(|entry| {
            snapshot.models.iter().any(|model| {
                model.provider_id == entry.provider_id
                    && model.model_id == entry.model_id
                    && !model.deprecated
            })
        }));
    }

    #[test]
    fn models_dev_normalization_drops_unknown_fields_and_curates_codex_provider() {
        let snapshot = super::normalize_models_dev_json(raw_models_dev_fixture().as_bytes())
            .expect("fixture must normalize");

        assert_eq!(snapshot.models.len(), 9);
        assert!(snapshot.models.iter().any(|model| {
            model.provider_id == "anthropic" && model.model_id == "claude-test" && model.deprecated
        }));
        assert!(snapshot.models.iter().any(|model| {
            model.provider_id == "openai-codex" && model.model_id == "gpt-test-codex"
        }));
        assert!(snapshot.models.iter().any(|model| {
            model.provider_id == "openai-codex" && model.model_id == "gpt-5.6-sol"
        }));
        assert!(
            snapshot
                .models
                .iter()
                .any(|model| model.provider_id == "compat-fixture"
                    && model.model_id == "compat-model"),
            "well-formed models must survive a malformed sibling entry"
        );
        assert!(
            !snapshot
                .models
                .iter()
                .any(|model| model.model_id == "bad-model" || model.model_id == "gemini-fixture")
        );
        assert!(
            !snapshot.models.iter().any(|model| {
                model.model_id == "template-model" || model.model_id == "cleartext-model"
            }),
            "models of providers with unusable base URLs must be dropped with them"
        );
        assert!(
            snapshot
                .models
                .iter()
                .any(|model| model.model_id == "loopback-model")
        );

        let normalized = serde_json::to_value(snapshot).unwrap();
        let first = normalized["models"].as_array().unwrap().first().unwrap();
        assert!(first.get("description").is_none());
        assert!(first.get("tool_call").is_none());
        assert!(first.get("unknown_upstream_field").is_none());
    }

    #[test]
    fn models_dev_normalization_derives_the_full_provider_set() {
        let snapshot = super::normalize_models_dev_json(raw_models_dev_fixture().as_bytes())
            .expect("fixture must normalize");
        let provider = |id: &str| {
            snapshot
                .providers
                .iter()
                .find(|provider| provider.provider_id == id)
        };

        let anthropic = provider("anthropic").expect("override row for anthropic");
        assert_eq!(anthropic.display_name, "Anthropic");
        assert_eq!(anthropic.base_url, "https://api.anthropic.com");
        assert_eq!(anthropic.api, super::CATALOG_API_ANTHROPIC_MESSAGES);
        assert_eq!(anthropic.auth_kind, super::CATALOG_AUTH_KIND_API_KEY);
        assert_eq!(anthropic.env_vars, vec!["ANTHROPIC_API_KEY".to_string()]);
        assert_eq!(
            anthropic.doc_url.as_deref(),
            Some("https://docs.anthropic.example/models")
        );
        let openai = provider("openai").expect("override row for openai");
        assert_eq!(openai.base_url, "https://api.openai.com/v1");
        assert_eq!(openai.api, super::CATALOG_API_OPENAI_CHAT_COMPLETIONS);
        let compat = provider("compat-fixture").expect("derived openai-compatible row");
        assert_eq!(compat.base_url, "https://compat.example.invalid/v1");
        assert_eq!(compat.api, super::CATALOG_API_OPENAI_CHAT_COMPLETIONS);
        let anthropic_compat =
            provider("anthropic-compat-fixture").expect("derived anthropic-sdk row");
        assert_eq!(
            anthropic_compat.base_url,
            "https://anthropic-compat.example.invalid/v1"
        );
        assert_eq!(anthropic_compat.api, super::CATALOG_API_ANTHROPIC_MESSAGES);
        assert_eq!(anthropic_compat.display_name, "anthropic-compat-fixture");
        let deepseek = provider("deepseek").expect("override row for deepseek");
        assert_eq!(
            deepseek.base_url, "https://api.deepseek.com",
            "the curated override must win over the upstream api URL"
        );
        let codex = provider("openai-codex").expect("static openai-codex row");
        assert_eq!(codex.auth_kind, super::CATALOG_AUTH_KIND_OAUTH);
        assert!(
            provider("google").is_none(),
            "unsupported API families must be skipped"
        );
        assert!(provider("compat-without-api").is_none());
        assert!(provider("malformed-provider").is_none());
        assert!(provider("ignored-provider").is_none());
        assert!(
            provider("template-url-fixture").is_none(),
            "${{...}} template base URLs never resolve and must be dropped"
        );
        assert!(
            provider("cleartext-fixture").is_none(),
            "non-loopback http base URLs would leak API keys and must be dropped"
        );
        let loopback = provider("loopback-fixture").expect("loopback http row");
        assert_eq!(loopback.base_url, "http://127.0.0.1:1234/v1");
        let ordered = snapshot
            .providers
            .iter()
            .map(|provider| provider.provider_id.as_str())
            .collect::<Vec<_>>();
        let mut sorted = ordered.clone();
        sorted.sort_unstable();
        assert_eq!(ordered, sorted, "provider ordering must be deterministic");
    }

    #[test]
    fn base_url_validation_requires_https_or_loopback_http() {
        assert!(super::valid_catalog_base_url("https://api.example.com/v1"));
        assert!(super::valid_catalog_base_url("http://localhost:1234/v1"));
        assert!(super::valid_catalog_base_url("http://127.0.0.1/v1"));
        assert!(super::valid_catalog_base_url("http://[::1]:8080/v1"));
        assert!(!super::valid_catalog_base_url(
            "http://cleartext.example.invalid/v1"
        ));
        assert!(!super::valid_catalog_base_url("${GATEWAY_BASE_URL}/v1"));
        assert!(!super::valid_catalog_base_url("https://${HOST}/v1"));
        assert!(!super::valid_catalog_base_url("ftp://example.com"));
        assert!(!super::valid_catalog_base_url("https://"));
        assert!(!super::valid_catalog_base_url("api.example.com/v1"));
    }

    #[test]
    fn snapshot_rendering_is_byte_stable() {
        // Regeneration determinism: the same upstream bytes must produce the
        // same snapshot bytes, and re-rendering a parsed snapshot must too.
        let first = super::render_snapshot_json(
            &super::normalize_models_dev_json(raw_models_dev_fixture().as_bytes()).unwrap(),
        )
        .unwrap();
        let second = super::render_snapshot_json(
            &super::normalize_models_dev_json(raw_models_dev_fixture().as_bytes()).unwrap(),
        )
        .unwrap();
        assert_eq!(first, second);
        let reparsed: super::ModelCatalogSnapshot = serde_json::from_slice(&first).unwrap();
        assert_eq!(
            super::render_snapshot_json(&super::sanitize_snapshot(reparsed)).unwrap(),
            first
        );
    }

    // Regeneration entry point for scripts/update-model-catalog.sh: the
    // checked-in snapshot flows through the same normalization as the runtime.
    #[test]
    #[ignore = "regeneration entry point; run via scripts/update-model-catalog.sh"]
    fn regenerate_built_in_snapshot_from_env() {
        let input = std::env::var("VERLET_MODEL_CATALOG_REGEN_INPUT")
            .expect("VERLET_MODEL_CATALOG_REGEN_INPUT must point at a models.dev api.json file");
        let output = std::env::var("VERLET_MODEL_CATALOG_REGEN_OUTPUT")
            .expect("VERLET_MODEL_CATALOG_REGEN_OUTPUT must point at the snapshot to write");
        let bytes = std::fs::read(&input).expect("regeneration input must be readable");
        let mut snapshot =
            super::normalize_models_dev_json(&bytes).expect("upstream payload must normalize");
        snapshot.comment = Some(
            "Generated by scripts/update-model-catalog.sh from models.dev; do not edit by hand."
                .to_string(),
        );
        std::fs::write(&output, super::render_snapshot_json(&snapshot).unwrap())
            .expect("regeneration output must be writable");
    }

    #[test]
    fn sanitization_normalizes_strings_limits_and_prices() {
        let snapshot = super::sanitize_snapshot(super::ModelCatalogSnapshot {
            comment: None,
            providers: Vec::new(),
            models: vec![
                super::ModelCatalogModel {
                    provider_id: " openai ".to_string(),
                    model_id: " gpt-test ".to_string(),
                    display_name: "   ".to_string(),
                    context_window: Some(0),
                    max_output_tokens: Some(42),
                    input_price: Some(-1.0),
                    output_price: Some(2.5),
                    reasoning: false,
                    deprecated: false,
                },
                super::ModelCatalogModel {
                    provider_id: "openai-codex".to_string(),
                    model_id: "gpt-uncurated".to_string(),
                    display_name: "Must be dropped".to_string(),
                    context_window: None,
                    max_output_tokens: None,
                    input_price: None,
                    output_price: None,
                    reasoning: false,
                    deprecated: false,
                },
            ],
        });

        assert_eq!(snapshot.models.len(), 1);
        let model = &snapshot.models[0];
        assert_eq!(model.provider_id, "openai");
        assert_eq!(model.model_id, "gpt-test");
        assert_eq!(model.display_name, "gpt-test");
        assert_eq!(model.context_window, None);
        assert_eq!(model.max_output_tokens, Some(42));
        assert_eq!(model.input_price, None);
        assert_eq!(model.output_price, Some(2.5));
    }

    #[test]
    fn empty_and_garbage_upstream_payloads_are_rejected() {
        assert!(matches!(
            super::normalize_models_dev_json(b"{}"),
            Err(super::CatalogRefreshError::EmptyCatalog)
        ));
        assert!(matches!(
            super::normalize_models_dev_json(b"not json"),
            Err(super::CatalogRefreshError::Json(_))
        ));
    }

    #[test]
    fn cached_remote_overlays_builtin_by_provider_and_model() {
        let state_home = test_state_home("overlay");
        let built_in = super::built_in_snapshot();
        let built_in_provider = built_in
            .providers
            .iter()
            .find(|provider| provider.provider_id == "anthropic")
            .unwrap();
        let target = built_in
            .models
            .iter()
            .find(|model| model.provider_id == "anthropic")
            .unwrap();
        let cached = super::ModelCatalogSnapshot {
            comment: None,
            providers: vec![
                super::ModelCatalogProvider {
                    provider_id: "anthropic".to_string(),
                    display_name: "Remote Anthropic".to_string(),
                    base_url: "https://credential-redirect.example.invalid".to_string(),
                    api: super::CATALOG_API_OPENAI_RESPONSES.to_string(),
                    auth_kind: super::CATALOG_AUTH_KIND_OAUTH.to_string(),
                    env_vars: vec!["REMOTE_ANTHROPIC_API_KEY".to_string()],
                    doc_url: Some("https://remote-docs.example.invalid".to_string()),
                },
                super::ModelCatalogProvider {
                    provider_id: "remote-only-provider".to_string(),
                    display_name: "Remote Only Provider".to_string(),
                    base_url: "https://remote-only.example.invalid/v1".to_string(),
                    api: super::CATALOG_API_OPENAI_CHAT_COMPLETIONS.to_string(),
                    auth_kind: super::CATALOG_AUTH_KIND_API_KEY.to_string(),
                    env_vars: vec!["REMOTE_ONLY_API_KEY".to_string()],
                    doc_url: None,
                },
                built_in
                    .providers
                    .iter()
                    .find(|provider| provider.provider_id == "openai")
                    .unwrap()
                    .clone(),
            ],
            models: vec![
                super::ModelCatalogModel {
                    provider_id: target.provider_id.clone(),
                    model_id: target.model_id.clone(),
                    display_name: "Remote replacement".to_string(),
                    context_window: Some(123),
                    max_output_tokens: Some(45),
                    input_price: Some(1.25),
                    output_price: Some(7.5),
                    reasoning: true,
                    deprecated: false,
                },
                super::ModelCatalogModel {
                    provider_id: "openai".to_string(),
                    model_id: "remote-only".to_string(),
                    display_name: "Remote only".to_string(),
                    context_window: Some(999),
                    max_output_tokens: Some(111),
                    input_price: None,
                    output_price: None,
                    reasoning: false,
                    deprecated: false,
                },
            ],
        };
        super::write_catalog_cache(&state_home, &cached).unwrap();

        let catalog = super::MergedModelCatalog::new(&state_home);
        let providers = catalog.providers();
        let provider = providers
            .iter()
            .find(|provider| provider.provider_id == "anthropic")
            .unwrap();
        assert_eq!(provider.display_name, "Remote Anthropic");
        assert_eq!(provider.base_url, built_in_provider.base_url);
        assert_eq!(provider.api, built_in_provider.api);
        assert_eq!(provider.auth_kind, built_in_provider.auth_kind);
        assert_eq!(provider.env_vars, vec!["REMOTE_ANTHROPIC_API_KEY"]);
        assert_eq!(
            provider.doc_url.as_deref(),
            Some("https://remote-docs.example.invalid")
        );
        assert!(
            providers
                .iter()
                .all(|provider| provider.provider_id != "remote-only-provider"),
            "providers absent from the reviewed snapshot must not enter the merged view"
        );
        let full = catalog.full_entries();
        let replaced = full
            .iter()
            .find(|model| {
                model.provider_id == target.provider_id && model.model_id == target.model_id
            })
            .unwrap();
        assert_eq!(replaced.display_name, "Remote replacement");
        assert!(
            full.iter()
                .any(|model| model.provider_id == "openai" && model.model_id == "remote-only")
        );

        let seam = catalog.entries();
        let projected = seam
            .iter()
            .find(|model| {
                model.provider_id == target.provider_id && model.model_id == target.model_id
            })
            .unwrap();
        assert_eq!(projected.display_name, "Remote replacement");
        assert_eq!(projected.context_window, Some(123));
        assert_eq!(projected.max_output_tokens, Some(45));

        remove_test_state_home(&state_home);
    }

    #[test]
    fn old_format_cache_keeps_models_and_falls_back_to_built_in_providers() {
        let state_home = test_state_home("old-format");
        let old_format = serde_json::json!({
            "models": [{
                "provider_id": "legacy-provider",
                "model_id": "legacy-model",
                "display_name": "Legacy Model",
                "reasoning": false,
                "deprecated": false
            }]
        });
        std::fs::create_dir_all(state_home.join(super::CATALOG_CACHE_DIR)).unwrap();
        std::fs::write(
            super::catalog_cache_path(&state_home),
            old_format.to_string(),
        )
        .unwrap();

        let catalog = super::MergedModelCatalog::new(&state_home);
        assert_eq!(catalog.providers(), super::built_in_snapshot().providers);
        assert!(catalog.entries().iter().any(
            |entry| entry.provider_id == "legacy-provider" && entry.model_id == "legacy-model"
        ));

        remove_test_state_home(&state_home);
    }

    #[tokio::test]
    async fn refresh_override_honors_validators_and_twenty_four_hour_cap() {
        let state_home = test_state_home("conditional");
        let server = FixtureServer::start(vec![
            FixtureResponse::ok(raw_models_dev_fixture())
                .with_header("ETag", "\"catalog-v1\"")
                .with_header("Last-Modified", "Sun, 09 Aug 2026 20:00:00 GMT"),
            FixtureResponse::not_modified(),
        ])
        .await;
        let expected_url = server.url.clone();
        let mut options =
            super::CatalogRefreshOptions::for_runtime_with_env(state_home.clone(), move |name| {
                assert_eq!(name, super::MODEL_CATALOG_URL_ENV);
                Ok(expected_url)
            })
            .expect("fixture URL enables refresh");
        options.now_unix_secs = FIRST_CHECK_SECS;
        options.retry_delays = vec![std::time::Duration::ZERO, std::time::Duration::ZERO];
        assert_eq!(options.url, server.url);

        assert_eq!(
            super::refresh_catalog(&options).await.unwrap(),
            super::CatalogRefreshOutcome::Updated
        );
        let original_cache = std::fs::read(super::catalog_cache_path(&state_home)).unwrap();
        let original_modified = std::fs::metadata(super::catalog_cache_path(&state_home))
            .unwrap()
            .modified()
            .unwrap();

        options.now_unix_secs = FIRST_CHECK_SECS + 60;
        assert_eq!(
            super::refresh_catalog(&options).await.unwrap(),
            super::CatalogRefreshOutcome::SkippedFresh
        );

        options.now_unix_secs = FIRST_CHECK_SECS + super::REFRESH_INTERVAL_SECS;
        assert_eq!(
            super::refresh_catalog(&options).await.unwrap(),
            super::CatalogRefreshOutcome::NotModified
        );
        assert_eq!(
            std::fs::read(super::catalog_cache_path(&state_home)).unwrap(),
            original_cache,
            "304 must not rewrite the catalog cache"
        );
        assert_eq!(
            std::fs::metadata(super::catalog_cache_path(&state_home))
                .unwrap()
                .modified()
                .unwrap(),
            original_modified,
            "304 must leave catalog cache metadata untouched"
        );

        let requests = server.finish().await;
        assert_eq!(requests.len(), 2, "fresh refresh state must skip the GET");
        assert!(requests[1].contains("if-none-match: \"catalog-v1\""));
        assert!(requests[1].contains("if-modified-since: sun, 09 aug 2026 20:00:00 gmt"));
        let state = super::read_refresh_state(&state_home).unwrap().unwrap();
        assert_eq!(state.checked_at_unix_secs, options.now_unix_secs);

        remove_test_state_home(&state_home);
    }

    #[tokio::test]
    async fn refresh_does_not_send_validators_without_a_usable_cache() {
        let state_home = test_state_home("validator-without-cache");
        super::write_refresh_state(
            &state_home,
            &super::CatalogRefreshState {
                checked_at_unix_secs: FIRST_CHECK_SECS - super::REFRESH_INTERVAL_SECS,
                etag: Some("\"orphaned-etag\"".to_string()),
                last_modified: Some("Sun, 09 Aug 2026 20:00:00 GMT".to_string()),
            },
        )
        .unwrap();
        let server =
            FixtureServer::start(vec![FixtureResponse::ok(raw_models_dev_fixture())]).await;
        let options = super::CatalogRefreshOptions::for_test(
            state_home.clone(),
            server.url.clone(),
            FIRST_CHECK_SECS,
        );

        assert_eq!(
            super::refresh_catalog(&options).await.unwrap(),
            super::CatalogRefreshOutcome::Updated
        );
        let requests = server.finish().await;
        assert_eq!(requests.len(), 1);
        assert!(!requests[0].contains("if-none-match:"));
        assert!(!requests[0].contains("if-modified-since:"));
        assert!(super::catalog_cache_path(&state_home).is_file());

        remove_test_state_home(&state_home);
    }

    #[tokio::test]
    async fn refresh_retries_server_failures_but_remains_bounded() {
        let state_home = test_state_home("retry");
        let server = FixtureServer::start(vec![
            FixtureResponse::status("500 Internal Server Error"),
            FixtureResponse::status("503 Service Unavailable"),
            FixtureResponse::ok(raw_models_dev_fixture()),
        ])
        .await;
        let mut options = super::CatalogRefreshOptions::for_test(
            state_home.clone(),
            server.url.clone(),
            FIRST_CHECK_SECS,
        );
        options.retry_delays = vec![std::time::Duration::ZERO, std::time::Duration::ZERO];

        assert_eq!(
            super::refresh_catalog(&options).await.unwrap(),
            super::CatalogRefreshOutcome::Updated
        );
        assert_eq!(server.finish().await.len(), 3);
        assert!(super::catalog_cache_path(&state_home).is_file());

        remove_test_state_home(&state_home);
    }

    #[tokio::test]
    async fn fetch_rejects_oversized_responses_before_reading_the_body() {
        let state_home = test_state_home("oversized");
        let server = FixtureServer::start(vec![
            FixtureResponse::ok(String::new()).with_declared_length(super::MAX_REMOTE_BYTES + 1),
        ])
        .await;
        let mut options = super::CatalogRefreshOptions::for_test(
            state_home.clone(),
            server.url.clone(),
            FIRST_CHECK_SECS,
        );
        options.retry_delays.clear();

        assert!(matches!(
            super::fetch_catalog(&options, &super::CatalogRefreshState::default()).await,
            Err(super::CatalogRefreshError::ResponseTooLarge)
        ));
        assert_eq!(server.finish().await.len(), 1);

        remove_test_state_home(&state_home);
    }

    #[tokio::test]
    async fn fetch_does_not_retry_non_retryable_http_statuses() {
        let state_home = test_state_home("http-status");
        let server = FixtureServer::start(vec![FixtureResponse::status("400 Bad Request")]).await;
        let options = super::CatalogRefreshOptions::for_test(
            state_home.clone(),
            server.url.clone(),
            FIRST_CHECK_SECS,
        );

        assert!(matches!(
            super::fetch_catalog(&options, &super::CatalogRefreshState::default()).await,
            Err(super::CatalogRefreshError::HttpStatus(
                reqwest::StatusCode::BAD_REQUEST
            ))
        ));
        assert_eq!(server.finish().await.len(), 1);

        remove_test_state_home(&state_home);
    }

    #[tokio::test]
    async fn fetch_request_timeout_is_bounded() {
        let state_home = test_state_home("request-timeout");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/api.json", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
        });
        let mut options =
            super::CatalogRefreshOptions::for_test(state_home.clone(), url, FIRST_CHECK_SECS);
        options.request_timeout = std::time::Duration::from_millis(50);
        options.retry_delays.clear();

        assert!(matches!(
            super::fetch_catalog(&options, &super::CatalogRefreshState::default()).await,
            Err(super::CatalogRefreshError::Request)
        ));

        server.abort();
        let _ = server.await;
        remove_test_state_home(&state_home);
    }

    #[tokio::test]
    async fn failed_refresh_keeps_cached_remote_available_and_records_the_attempt() {
        let state_home = test_state_home("fallback");
        let cached = super::ModelCatalogSnapshot {
            comment: None,
            providers: Vec::new(),
            models: vec![super::ModelCatalogModel {
                provider_id: "openai".to_string(),
                model_id: "cached-model".to_string(),
                display_name: "Cached model".to_string(),
                context_window: Some(1_024),
                max_output_tokens: Some(128),
                input_price: Some(1.0),
                output_price: Some(2.0),
                reasoning: false,
                deprecated: false,
            }],
        };
        super::write_catalog_cache(&state_home, &cached).unwrap();
        let cache_before = std::fs::read(super::catalog_cache_path(&state_home)).unwrap();
        let server = FixtureServer::start(vec![
            FixtureResponse::status("500 Internal Server Error"),
            FixtureResponse::status("500 Internal Server Error"),
            FixtureResponse::status("500 Internal Server Error"),
        ])
        .await;
        let mut options = super::CatalogRefreshOptions::for_test(
            state_home.clone(),
            server.url.clone(),
            FIRST_CHECK_SECS,
        );
        options.retry_delays = vec![std::time::Duration::ZERO, std::time::Duration::ZERO];

        assert!(super::refresh_catalog(&options).await.is_err());
        assert_eq!(server.finish().await.len(), 3);
        options.now_unix_secs = FIRST_CHECK_SECS + 60;
        assert_eq!(
            super::refresh_catalog(&options).await.unwrap(),
            super::CatalogRefreshOutcome::SkippedFresh,
            "a failed attempt must still enforce the 24-hour cap"
        );
        assert_eq!(
            std::fs::read(super::catalog_cache_path(&state_home)).unwrap(),
            cache_before
        );
        assert!(
            super::MergedModelCatalog::new(&state_home)
                .full_entries()
                .iter()
                .any(|model| model.model_id == "cached-model")
        );
        assert_eq!(
            super::read_refresh_state(&state_home)
                .unwrap()
                .unwrap()
                .checked_at_unix_secs,
            FIRST_CHECK_SECS
        );

        remove_test_state_home(&state_home);
    }

    #[test]
    fn empty_runtime_url_disables_refresh_scheduling() {
        let state_home = test_state_home("disabled");
        let tasks = std::sync::Arc::new(super::super::lifecycle::InstanceTaskSet::new());

        let scheduled = super::spawn_runtime_refresh_with_env(&tasks, state_home.clone(), |name| {
            assert_eq!(name, super::MODEL_CATALOG_URL_ENV);
            Ok(" \t ".to_string())
        });

        assert!(!scheduled);
        assert_eq!(tasks.task_count(), 0);
        assert!(!state_home.join(super::CATALOG_CACHE_DIR).exists());
        remove_test_state_home(&state_home);
    }

    #[test]
    fn non_unicode_runtime_url_disables_refresh_scheduling() {
        let state_home = test_state_home("non-unicode-url");
        let tasks = std::sync::Arc::new(super::super::lifecycle::InstanceTaskSet::new());

        let scheduled = super::spawn_runtime_refresh_with_env(&tasks, state_home.clone(), |_| {
            Err(std::env::VarError::NotUnicode(std::ffi::OsString::from(
                "configured-but-invalid",
            )))
        });

        assert!(!scheduled);
        assert_eq!(tasks.task_count(), 0);
        remove_test_state_home(&state_home);
    }

    #[tokio::test]
    async fn runtime_refresh_is_abandoned_at_instance_shutdown() {
        let state_home = test_state_home("cancellable");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/api.json", listener.local_addr().unwrap());
        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            let _ = accepted_tx.send(());
            std::future::pending::<()>().await;
        });
        let tasks = std::sync::Arc::new(super::super::lifecycle::InstanceTaskSet::new());
        let mut options =
            super::CatalogRefreshOptions::for_test(state_home.clone(), url, FIRST_CHECK_SECS);
        options.request_timeout = std::time::Duration::from_secs(30);

        assert!(super::spawn_runtime_refresh_with_options(&tasks, options));
        accepted_rx.await.unwrap();
        // tight-timeout: cancellation must not wait for the pending HTTP timeout
        tokio::time::timeout(std::time::Duration::from_secs(1), tasks.shutdown())
            .await
            .expect("instance shutdown waited for the catalog request timeout");

        server.abort();
        let _ = server.await;
        assert!(!state_home.join(super::CATALOG_CACHE_DIR).exists());
        remove_test_state_home(&state_home);
    }

    #[test]
    fn atomic_write_replaces_whole_files_and_cleans_failed_temporaries() {
        let state_home = test_state_home("atomic-write");
        let target = state_home.join("target.json");
        std::fs::write(&target, b"old contents").unwrap();

        super::atomic_write(&target, b"new contents").unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"new contents");

        let directory_target = state_home.join("directory-target");
        std::fs::create_dir(&directory_target).unwrap();
        assert!(super::atomic_write(&directory_target, b"cannot replace a directory").is_err());
        let leftovers = std::fs::read_dir(&state_home)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"))
            .collect::<Vec<_>>();
        assert!(leftovers.is_empty(), "failed write left temporary files");

        remove_test_state_home(&state_home);
    }

    fn raw_models_dev_fixture() -> String {
        serde_json::json!({
            "anthropic": {
                "id": "anthropic",
                "name": "Anthropic",
                "npm": "@ai-sdk/anthropic",
                "env": ["ANTHROPIC_API_KEY"],
                "doc": "https://docs.anthropic.example/models",
                "models": {
                    "claude-test": {
                        "id": "claude-test",
                        "name": "Claude Test",
                        "description": "must not survive normalization",
                        "reasoning": true,
                        "tool_call": true,
                        "status": "deprecated",
                        "limit": { "context": 200000, "output": 8192 },
                        "cost": { "input": 3.0, "output": 15.0 },
                        "unknown_upstream_field": "drop me"
                    }
                }
            },
            "openai": {
                "id": "openai",
                "name": "OpenAI",
                "npm": "@ai-sdk/openai",
                "env": ["OPENAI_API_KEY"],
                "models": {
                    "gpt-test-codex": {
                        "id": "gpt-test-codex",
                        "name": "GPT Test Codex",
                        "reasoning": true,
                        "limit": { "context": 400000, "output": 128000 },
                        "cost": { "input": 1.25, "output": 10.0 }
                    },
                    "gpt-5.6-sol": {
                        "id": "gpt-5.6-sol",
                        "name": "GPT-5.6 Sol",
                        "reasoning": true,
                        "limit": { "context": 1050000, "output": 128000 },
                        "cost": { "input": 5.0, "output": 30.0 }
                    }
                }
            },
            "compat-fixture": {
                "id": "compat-fixture",
                "name": "Compat Fixture",
                "npm": "@ai-sdk/openai-compatible",
                "api": " https://compat.example.invalid/v1 ",
                "env": ["COMPAT_FIXTURE_API_KEY"],
                "doc": "https://compat.example.invalid/docs",
                "models": {
                    "compat-model": { "id": "compat-model", "name": "Compat Model" },
                    "bad-model": "not an object"
                }
            },
            "anthropic-compat-fixture": {
                "id": "anthropic-compat-fixture",
                "npm": "@ai-sdk/anthropic",
                "api": "https://anthropic-compat.example.invalid/v1",
                "models": {
                    "anthropic-compat-model": { "id": "anthropic-compat-model" }
                }
            },
            "deepseek": {
                "id": "deepseek",
                "name": "DeepSeek",
                "npm": "@ai-sdk/openai-compatible",
                "api": "https://upstream-must-lose.example.invalid",
                "models": {
                    "deepseek-chat": { "id": "deepseek-chat", "name": "DeepSeek Chat" }
                }
            },
            "google": {
                "id": "google",
                "name": "Google",
                "npm": "@ai-sdk/google",
                "api": "https://google.example.invalid/v1beta",
                "models": {
                    "gemini-fixture": { "id": "gemini-fixture", "name": "Gemini Fixture" }
                }
            },
            "compat-without-api": {
                "id": "compat-without-api",
                "npm": "@ai-sdk/openai-compatible",
                "models": {
                    "unreachable-model": { "id": "unreachable-model" }
                }
            },
            "template-url-fixture": {
                "id": "template-url-fixture",
                "npm": "@ai-sdk/openai-compatible",
                "api": "${TEMPLATE_FIXTURE_BASE_URL}/v1",
                "models": {
                    "template-model": { "id": "template-model" }
                }
            },
            "cleartext-fixture": {
                "id": "cleartext-fixture",
                "npm": "@ai-sdk/openai-compatible",
                "api": "http://cleartext.example.invalid/v1",
                "models": {
                    "cleartext-model": { "id": "cleartext-model" }
                }
            },
            "loopback-fixture": {
                "id": "loopback-fixture",
                "npm": "@ai-sdk/openai-compatible",
                "api": "http://127.0.0.1:1234/v1",
                "models": {
                    "loopback-model": { "id": "loopback-model" }
                }
            },
            "malformed-provider": "not an object",
            "ignored-provider": {
                "id": "ignored-provider",
                "models": {
                    "ignored-model": { "id": "ignored-model", "name": "Ignored" }
                }
            }
        })
        .to_string()
    }

    fn test_state_home(label: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "verlet-model-catalog-{label}-{}",
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn remove_test_state_home(path: &std::path::Path) {
        std::fs::remove_dir_all(path).unwrap();
    }

    struct FixtureServer {
        url: String,
        requests: std::sync::Arc<tokio::sync::Mutex<Vec<String>>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl FixtureServer {
        async fn start(responses: Vec<FixtureResponse>) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/api.json", listener.local_addr().unwrap());
            let requests = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
            let captured = std::sync::Arc::clone(&requests);
            let task = tokio::spawn(async move {
                for response in responses {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let mut request = Vec::new();
                    let mut buffer = [0_u8; 1024];
                    loop {
                        let read = stream.read(&mut buffer).await.unwrap();
                        if read == 0 {
                            break;
                        }
                        request.extend_from_slice(&buffer[..read]);
                        if request.windows(4).any(|window| window == b"\r\n\r\n") {
                            break;
                        }
                    }
                    captured
                        .lock()
                        .await
                        .push(String::from_utf8(request).unwrap().to_ascii_lowercase());
                    stream
                        .write_all(response.render().as_bytes())
                        .await
                        .unwrap();
                    stream.shutdown().await.unwrap();
                }
            });
            Self {
                url,
                requests,
                task,
            }
        }

        async fn finish(self) -> Vec<String> {
            // tight-timeout: the loopback fixture has only in-memory responses
            tokio::time::timeout(std::time::Duration::from_secs(2), self.task)
                .await
                .expect("fixture server did not receive the expected requests")
                .unwrap();
            self.requests.lock().await.clone()
        }
    }

    struct FixtureResponse {
        status: &'static str,
        headers: Vec<(&'static str, &'static str)>,
        body: String,
        declared_content_length: Option<usize>,
    }

    impl FixtureResponse {
        fn ok(body: String) -> Self {
            Self {
                status: "200 OK",
                headers: Vec::new(),
                body,
                declared_content_length: None,
            }
        }

        fn not_modified() -> Self {
            Self::status("304 Not Modified")
        }

        fn status(status: &'static str) -> Self {
            Self {
                status,
                headers: Vec::new(),
                body: String::new(),
                declared_content_length: None,
            }
        }

        fn with_header(mut self, name: &'static str, value: &'static str) -> Self {
            self.headers.push((name, value));
            self
        }

        fn with_declared_length(mut self, length: usize) -> Self {
            self.declared_content_length = Some(length);
            self
        }

        fn render(&self) -> String {
            let mut response = format!(
                "HTTP/1.1 {}\r\nContent-Length: {}\r\nConnection: close\r\n",
                self.status,
                self.declared_content_length.unwrap_or(self.body.len())
            );
            for (name, value) in &self.headers {
                response.push_str(&format!("{name}: {value}\r\n"));
            }
            response.push_str("\r\n");
            response.push_str(&self.body);
            response
        }
    }
}
