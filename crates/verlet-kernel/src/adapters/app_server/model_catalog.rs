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

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ModelCatalogSnapshot {
    #[serde(rename = "_comment", default, skip_serializing_if = "Option::is_none")]
    comment: Option<String>,
    models: Vec<ModelCatalogModel>,
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
    models: std::collections::BTreeMap<String, ModelsDevModel>,
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
            Ok(value) => value,
            Err(_) => DEFAULT_MODEL_CATALOG_URL.to_string(),
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
    let mut upstream: std::collections::BTreeMap<String, ModelsDevProvider> =
        serde_json::from_slice(bytes)?;
    let anthropic = upstream.remove("anthropic").unwrap_or(ModelsDevProvider {
        models: std::collections::BTreeMap::new(),
    });
    let openai = upstream.remove("openai").unwrap_or(ModelsDevProvider {
        models: std::collections::BTreeMap::new(),
    });
    let mut models = normalize_provider("anthropic", &anthropic);
    let openai_models = normalize_provider("openai", &openai);
    models.extend(openai_models.iter().cloned());
    models.extend(
        openai_models
            .into_iter()
            .filter(|model| is_openai_codex_model(&model.model_id))
            .map(|mut model| {
                model.provider_id = "openai-codex".to_string();
                model
            }),
    );
    let snapshot = sanitize_snapshot(ModelCatalogSnapshot {
        comment: None,
        models,
    });
    if snapshot.models.is_empty() {
        return Err(CatalogRefreshError::EmptyCatalog);
    }
    Ok(snapshot)
}

fn normalize_provider(provider_id: &str, provider: &ModelsDevProvider) -> Vec<ModelCatalogModel> {
    provider
        .models
        .iter()
        .filter_map(|(key, model)| {
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

fn sanitize_snapshot(snapshot: ModelCatalogSnapshot) -> ModelCatalogSnapshot {
    let mut models = snapshot
        .models
        .into_iter()
        .filter(|model| {
            matches!(
                model.provider_id.as_str(),
                "anthropic" | "openai" | "openai-codex"
            ) && !model.model_id.trim().is_empty()
                && !model.display_name.trim().is_empty()
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
    let previous = match read_refresh_state(&options.state_home) {
        Ok(state) => state.unwrap_or_default(),
        Err(error) => {
            log::debug!("model catalog refresh state was invalid; refreshing anyway: {error}");
            CatalogRefreshState::default()
        }
    };
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
    let mut bytes = serde_json::to_vec_pretty(snapshot)?;
    bytes.push(b'\n');
    atomic_write(&catalog_cache_path(state_home), &bytes)?;
    Ok(())
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
        #[cfg(windows)]
        if path.exists() {
            std::fs::remove_file(path)?;
        }
        std::fs::rename(&temporary, path)?;
        #[cfg(unix)]
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
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
        assert!(snapshot.models.iter().all(|model| matches!(
            model.provider_id.as_str(),
            "anthropic" | "openai" | "openai-codex"
        )));
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
        assert_eq!(offline_entries.len(), snapshot.models.len());
        assert!(!offline_entries.is_empty());
    }

    #[test]
    fn models_dev_normalization_drops_unknown_fields_and_curates_codex_provider() {
        let snapshot = super::normalize_models_dev_json(raw_models_dev_fixture().as_bytes())
            .expect("fixture must normalize");

        assert_eq!(snapshot.models.len(), 5);
        assert!(snapshot.models.iter().any(|model| {
            model.provider_id == "anthropic" && model.model_id == "claude-test" && model.deprecated
        }));
        assert!(snapshot.models.iter().any(|model| {
            model.provider_id == "openai-codex" && model.model_id == "gpt-test-codex"
        }));
        assert!(snapshot.models.iter().any(|model| {
            model.provider_id == "openai-codex" && model.model_id == "gpt-5.6-sol"
        }));

        let normalized = serde_json::to_value(snapshot).unwrap();
        let first = normalized["models"].as_array().unwrap().first().unwrap();
        assert!(first.get("description").is_none());
        assert!(first.get("tool_call").is_none());
        assert!(first.get("unknown_upstream_field").is_none());
    }

    #[test]
    fn cached_remote_overlays_builtin_by_provider_and_model() {
        let state_home = test_state_home("overlay");
        let built_in = super::built_in_snapshot();
        let target = built_in
            .models
            .iter()
            .find(|model| model.provider_id == "anthropic")
            .unwrap();
        let cached = super::ModelCatalogSnapshot {
            comment: None,
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
    async fn failed_refresh_keeps_cached_remote_available_and_records_the_attempt() {
        let state_home = test_state_home("fallback");
        let cached = super::ModelCatalogSnapshot {
            comment: None,
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

    fn raw_models_dev_fixture() -> String {
        serde_json::json!({
            "anthropic": {
                "id": "anthropic",
                "name": "Anthropic",
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
    }

    impl FixtureResponse {
        fn ok(body: String) -> Self {
            Self {
                status: "200 OK",
                headers: Vec::new(),
                body,
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
            }
        }

        fn with_header(mut self, name: &'static str, value: &'static str) -> Self {
            self.headers.push((name, value));
            self
        }

        fn render(&self) -> String {
            let mut response = format!(
                "HTTP/1.1 {}\r\nContent-Length: {}\r\nConnection: close\r\n",
                self.status,
                self.body.len()
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
