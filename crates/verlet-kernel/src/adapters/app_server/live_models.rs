//! In-memory live model-list enrichment for configured provider records.
//!
//! Reads never await network work. Missing or stale provider entries schedule
//! one instance-owned refresh and immediately return the last cached models.

const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_MODELS: usize = 10_000;
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(4);
pub(crate) const REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60 * 60);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LiveModel {
    pub(crate) provider_id: String,
    pub(crate) model_id: String,
    pub(crate) display_name: String,
}

#[derive(Clone)]
pub(crate) enum RefreshCredential {
    ApiKey(String),
    None,
}

impl std::fmt::Debug for RefreshCredential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ApiKey(_) => formatter.write_str("ApiKey(<redacted>)"),
            Self::None => formatter.write_str("None"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProviderFingerprint {
    api: verlet_history::ProviderApi,
    base_url: String,
}

impl From<&verlet_metadata::provider_store::LlmProviderRecord> for ProviderFingerprint {
    fn from(provider: &verlet_metadata::provider_store::LlmProviderRecord) -> Self {
        Self {
            api: provider.api.clone(),
            base_url: provider.base_url.clone(),
        }
    }
}

struct CachedModels {
    fingerprint: ProviderFingerprint,
    models: Vec<LiveModel>,
    checked_at: tokio::time::Instant,
}

pub(crate) struct LiveModelCache {
    entries: std::sync::RwLock<std::collections::BTreeMap<String, CachedModels>>,
    refreshing: std::sync::Mutex<std::collections::BTreeSet<String>>,
    refresh_interval: std::time::Duration,
    request_timeout: std::time::Duration,
}

impl Default for LiveModelCache {
    fn default() -> Self {
        Self::new()
    }
}

impl LiveModelCache {
    pub(crate) fn new() -> Self {
        Self {
            entries: std::sync::RwLock::new(std::collections::BTreeMap::new()),
            refreshing: std::sync::Mutex::new(std::collections::BTreeSet::new()),
            refresh_interval: REFRESH_INTERVAL,
            request_timeout: REQUEST_TIMEOUT,
        }
    }

    pub(crate) fn entries_and_refresh(
        self: &std::sync::Arc<Self>,
        tasks: &std::sync::Arc<super::lifecycle::InstanceTaskSet>,
        provider: &verlet_metadata::provider_store::LlmProviderRecord,
        credential: RefreshCredential,
    ) -> Vec<LiveModel> {
        let fingerprint = ProviderFingerprint::from(provider);
        let now = tokio::time::Instant::now();
        let (models, stale) = {
            let entries = self
                .entries
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match entries.get(&provider.provider_id) {
                Some(entry) if entry.fingerprint == fingerprint => {
                    let stale = now
                        .checked_duration_since(entry.checked_at)
                        .is_none_or(|age| age >= self.refresh_interval);
                    (entry.models.clone(), stale)
                }
                _ => (Vec::new(), true),
            }
        };
        if stale && self.start_refresh(&provider.provider_id) {
            let provider = provider.clone();
            let refresh_id = provider.provider_id.clone();
            let provider_id = refresh_id.clone();
            let cache = std::sync::Arc::clone(self);
            let request_timeout = self.request_timeout;
            let accepted = tasks.spawn_cancellable(async move {
                match fetch_models(&provider, &credential, request_timeout).await {
                    Ok(models) => cache.finish_refresh(
                        provider_id,
                        fingerprint,
                        Some(models),
                        tokio::time::Instant::now(),
                    ),
                    Err(error) => {
                        log::debug!(
                            "live model refresh failed for provider {}: {}",
                            provider.provider_id,
                            error
                        );
                        cache.finish_refresh(
                            provider_id,
                            fingerprint,
                            None,
                            tokio::time::Instant::now(),
                        );
                    }
                }
            });
            if !accepted {
                self.clear_refresh(&refresh_id);
            }
        }
        models
    }

    fn start_refresh(&self, provider_id: &str) -> bool {
        self.refreshing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(provider_id.to_string())
    }

    fn clear_refresh(&self, provider_id: &str) {
        self.refreshing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(provider_id);
    }

    fn finish_refresh(
        &self,
        provider_id: String,
        fingerprint: ProviderFingerprint,
        models: Option<Vec<LiveModel>>,
        checked_at: tokio::time::Instant,
    ) {
        {
            let mut entries = self
                .entries
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match models {
                Some(models) => {
                    entries.insert(
                        provider_id.clone(),
                        CachedModels {
                            fingerprint,
                            models,
                            checked_at,
                        },
                    );
                }
                None => match entries.get_mut(&provider_id) {
                    Some(entry) if entry.fingerprint == fingerprint => {
                        entry.checked_at = checked_at;
                    }
                    _ => {
                        entries.insert(
                            provider_id.clone(),
                            CachedModels {
                                fingerprint,
                                models: Vec::new(),
                                checked_at,
                            },
                        );
                    }
                },
            }
        }
        self.clear_refresh(&provider_id);
    }

    #[cfg(test)]
    fn is_refreshing_for_test(&self, provider_id: &str) -> bool {
        self.refreshing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(provider_id)
    }

    #[cfg(test)]
    fn seed_for_test(
        &self,
        provider: &verlet_metadata::provider_store::LlmProviderRecord,
        models: Vec<LiveModel>,
        age: std::time::Duration,
    ) {
        self.entries
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                provider.provider_id.clone(),
                CachedModels {
                    fingerprint: ProviderFingerprint::from(provider),
                    models,
                    checked_at: tokio::time::Instant::now() - age,
                },
            );
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LiveModelAuthError {
    #[error("credential lookup failed")]
    CredentialLookup,
    #[error("credential resolution failed")]
    CredentialResolution,
}

pub(crate) async fn resolve_refresh_credential(
    auth_store: &dyn verlet_metadata::provider_store::LlmProviderAuthStore,
    provider: &verlet_metadata::provider_store::LlmProviderRecord,
    auth_context: &verlet_metadata::provider_store::LlmProviderAuthContext,
) -> Result<Option<RefreshCredential>, LiveModelAuthError> {
    if !matches!(
        provider.api,
        verlet_history::ProviderApi::OpenAIChatCompletions
            | verlet_history::ProviderApi::OpenAIResponses
            | verlet_history::ProviderApi::AnthropicMessages
    ) {
        return Ok(None);
    }
    let stored = auth_store
        .get_credential(&provider.provider_id)
        .await
        .map_err(|_| LiveModelAuthError::CredentialLookup)?;
    if matches!(
        stored,
        Some(verlet_metadata::provider_store::LlmProviderCredential::OAuth { .. })
    ) {
        return Ok(None);
    }
    if matches!(
        provider.auth,
        verlet_metadata::provider_store::LlmProviderAuthConfig::None
    ) {
        return Ok(Some(RefreshCredential::None));
    }
    let Some(resolved) = verlet_metadata::provider_store::resolve_llm_provider_auth(
        auth_store,
        provider,
        auth_context,
    )
    .await
    .map_err(|_| LiveModelAuthError::CredentialResolution)?
    else {
        return Ok(None);
    };
    let eligible = match resolved.source {
        verlet_metadata::provider_store::LlmProviderAuthSourceKind::Environment => true,
        verlet_metadata::provider_store::LlmProviderAuthSourceKind::Stored => matches!(
            stored,
            Some(verlet_metadata::provider_store::LlmProviderCredential::ApiKey { .. })
        ),
        verlet_metadata::provider_store::LlmProviderAuthSourceKind::Runtime
        | verlet_metadata::provider_store::LlmProviderAuthSourceKind::CatalogInline
        | verlet_metadata::provider_store::LlmProviderAuthSourceKind::CatalogCommand
        | verlet_metadata::provider_store::LlmProviderAuthSourceKind::None => false,
    };
    if !eligible {
        return Ok(None);
    }
    if provider.auth_header {
        Ok(Some(RefreshCredential::ApiKey(resolved.api_key)))
    } else {
        Ok(Some(RefreshCredential::None))
    }
}

#[derive(Debug, thiserror::Error)]
enum LiveModelRefreshError {
    #[error("HTTP client could not be constructed")]
    Client,
    #[error("HTTP request failed")]
    Request,
    #[error("HTTP status was not successful")]
    HttpStatus,
    #[error("HTTP response exceeded the size limit")]
    ResponseTooLarge,
    #[error("HTTP response was not a valid model list")]
    InvalidResponse,
    #[error("HTTP response exceeded the model-count limit")]
    TooManyModels,
}

#[derive(serde::Deserialize)]
struct ListModelsResponse {
    data: Vec<ListModelsItem>,
}

#[derive(serde::Deserialize)]
struct ListModelsItem {
    id: String,
    #[serde(default)]
    display_name: Option<String>,
}

async fn fetch_models(
    provider: &verlet_metadata::provider_store::LlmProviderRecord,
    credential: &RefreshCredential,
    request_timeout: std::time::Duration,
) -> Result<Vec<LiveModel>, LiveModelRefreshError> {
    let url = list_models_url(provider).ok_or(LiveModelRefreshError::Request)?;
    let client = reqwest::Client::builder()
        .timeout(request_timeout)
        .redirect(reqwest::redirect::Policy::none())
        .user_agent("verlet-live-models/0.1")
        .build()
        .map_err(|_| LiveModelRefreshError::Client)?;
    let mut request = client.get(url);
    match (&provider.api, credential) {
        (
            verlet_history::ProviderApi::OpenAIChatCompletions
            | verlet_history::ProviderApi::OpenAIResponses,
            RefreshCredential::ApiKey(key),
        ) => {
            request = request.bearer_auth(key);
        }
        (verlet_history::ProviderApi::AnthropicMessages, RefreshCredential::ApiKey(key)) => {
            request = request.header("x-api-key", key);
        }
        (_, RefreshCredential::None) => {}
        (verlet_history::ProviderApi::Other(_), RefreshCredential::ApiKey(_)) => {
            return Err(LiveModelRefreshError::Request);
        }
    }
    if matches!(provider.api, verlet_history::ProviderApi::AnthropicMessages) {
        request = request.header("anthropic-version", verlet_provider::ANTHROPIC_API_VERSION);
    }
    let response = request
        .send()
        .await
        .map_err(|_| LiveModelRefreshError::Request)?;
    if !response.status().is_success() {
        return Err(LiveModelRefreshError::HttpStatus);
    }
    let body = bounded_response_body(response).await?;
    let response: ListModelsResponse =
        serde_json::from_slice(&body).map_err(|_| LiveModelRefreshError::InvalidResponse)?;
    if response.data.len() > MAX_MODELS {
        return Err(LiveModelRefreshError::TooManyModels);
    }
    let mut models = std::collections::BTreeMap::new();
    for item in response.data {
        let model_id = item.id.trim();
        if model_id.is_empty() {
            continue;
        }
        let display_name = item
            .display_name
            .as_deref()
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .unwrap_or(model_id);
        models
            .entry(model_id.to_string())
            .or_insert_with(|| LiveModel {
                provider_id: provider.provider_id.clone(),
                model_id: model_id.to_string(),
                display_name: display_name.to_string(),
            });
    }
    Ok(models.into_values().collect())
}

fn list_models_url(
    provider: &verlet_metadata::provider_store::LlmProviderRecord,
) -> Option<String> {
    let base_url = provider.base_url.trim_end_matches('/');
    if base_url.is_empty() {
        return None;
    }
    match provider.api {
        verlet_history::ProviderApi::OpenAIChatCompletions
        | verlet_history::ProviderApi::OpenAIResponses => Some(format!("{base_url}/models")),
        verlet_history::ProviderApi::AnthropicMessages => {
            if base_url.ends_with("/v1") {
                Some(format!("{base_url}/models"))
            } else {
                Some(format!("{base_url}/v1/models"))
            }
        }
        verlet_history::ProviderApi::Other(_) => None,
    }
}

async fn bounded_response_body(
    response: reqwest::Response,
) -> Result<Vec<u8>, LiveModelRefreshError> {
    use futures_util::StreamExt as _;

    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
    {
        return Err(LiveModelRefreshError::ResponseTooLarge);
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| LiveModelRefreshError::Request)?;
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(LiveModelRefreshError::ResponseTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    struct HttpResponse {
        status: &'static str,
        body: String,
        release: Option<std::sync::Arc<tokio::sync::Semaphore>>,
    }

    impl HttpResponse {
        fn ok(body: impl Into<String>) -> Self {
            Self {
                status: "200 OK",
                body: body.into(),
                release: None,
            }
        }

        fn gated(mut self, release: std::sync::Arc<tokio::sync::Semaphore>) -> Self {
            self.release = Some(release);
            self
        }
    }

    struct RecordingHttpServer {
        base_url: String,
        requests: tokio::sync::mpsc::UnboundedReceiver<String>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for RecordingHttpServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn spawn_http_sequence(responses: Vec<HttpResponse>) -> RecordingHttpServer {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_tx, request_rx) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut handlers = tokio::task::JoinSet::new();
            for response in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request_tx = request_tx.clone();
                handlers.spawn(async move {
                    let mut request = Vec::new();
                    loop {
                        let mut chunk = [0_u8; 1024];
                        let read = stream.read(&mut chunk).await.unwrap();
                        assert!(read > 0, "request ended before its headers");
                        request.extend_from_slice(&chunk[..read]);
                        if request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                            break;
                        }
                    }
                    request_tx
                        .send(String::from_utf8(request).unwrap())
                        .unwrap();
                    if let Some(release) = response.release {
                        let permit = release.acquire().await.unwrap();
                        permit.forget();
                    }
                    let wire = format!(
                        "HTTP/1.1 {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                        response.status,
                        response.body.len(),
                        response.body,
                    );
                    stream.write_all(wire.as_bytes()).await.unwrap();
                });
            }
            while handlers.join_next().await.is_some() {}
        });
        RecordingHttpServer {
            base_url: format!("http://{address}"),
            requests: request_rx,
            task,
        }
    }

    async fn next_request(server: &mut RecordingHttpServer) -> String {
        tokio::time::timeout(std::time::Duration::from_secs(2), server.requests.recv())
            .await
            .expect("model-list request timed out")
            .expect("model-list request channel closed")
    }

    async fn wait_until_idle(cache: &super::LiveModelCache, provider_id: &str) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while cache.is_refreshing_for_test(provider_id) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("live-model refresh did not settle");
    }

    #[tokio::test]
    async fn anthropic_fetch_uses_models_path_version_header_and_display_name() {
        let mut server = spawn_http_sequence(vec![HttpResponse::ok(
            r#"{"data":[{"id":"claude-live","display_name":"Claude Live"}]}"#,
        )])
        .await;
        let provider = verlet_metadata::provider_store::LlmProviderRecord::new(
            "anthropic-fixture",
            verlet_history::ProviderApi::AnthropicMessages,
            &server.base_url,
        )
        .with_auth_header(true);

        let models = super::fetch_models(
            &provider,
            &super::RefreshCredential::ApiKey("anthropic-secret".to_string()),
            std::time::Duration::from_secs(2),
        )
        .await
        .unwrap();

        assert_eq!(models.len(), 1);
        assert_eq!(models[0].model_id, "claude-live");
        assert_eq!(models[0].display_name, "Claude Live");
        let request = next_request(&mut server).await;
        let request_lower = request.to_ascii_lowercase();
        assert!(request.starts_with("GET /v1/models HTTP/1.1"));
        assert!(request_lower.contains("x-api-key: anthropic-secret"));
        assert!(request_lower.contains("anthropic-version: 2023-06-01"));
        assert!(!request_lower.contains("authorization:"));
    }

    #[tokio::test]
    async fn refresh_errors_never_include_credentials_or_response_bodies() {
        let mut server = spawn_http_sequence(vec![HttpResponse {
            status: "500 Internal Server Error",
            body: "response-body-secret".to_string(),
            release: None,
        }])
        .await;
        let provider = verlet_metadata::provider_store::LlmProviderRecord::new(
            "redaction-fixture",
            verlet_history::ProviderApi::OpenAIResponses,
            format!("{}/v1", server.base_url),
        )
        .with_auth_header(true);

        let error = super::fetch_models(
            &provider,
            &super::RefreshCredential::ApiKey("credential-secret".to_string()),
            std::time::Duration::from_secs(2),
        )
        .await
        .unwrap_err();

        let message = error.to_string();
        assert!(!message.contains("credential-secret"));
        assert!(!message.contains("response-body-secret"));
        assert_eq!(message, "HTTP status was not successful");
        let request = next_request(&mut server).await;
        assert!(request.starts_with("GET /v1/models HTTP/1.1"));
    }

    #[tokio::test]
    async fn missing_cache_reads_are_immediate_and_single_flight() {
        let release = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
        let mut server = spawn_http_sequence(vec![
            HttpResponse::ok(r#"{"data":[{"id":"live-model"}]}"#)
                .gated(std::sync::Arc::clone(&release)),
            HttpResponse::ok(r#"{"data":[{"id":"unexpected-second-request"}]}"#),
        ])
        .await;
        let provider = verlet_metadata::provider_store::LlmProviderRecord::new(
            "single-flight-fixture",
            verlet_history::ProviderApi::OpenAIChatCompletions,
            format!("{}/v1", server.base_url),
        )
        .with_auth_header(true);
        let cache = std::sync::Arc::new(super::LiveModelCache::new());
        let tasks =
            std::sync::Arc::new(crate::adapters::app_server::lifecycle::InstanceTaskSet::new());
        let credential = super::RefreshCredential::ApiKey("fixture-key".to_string());

        for _ in 0..16 {
            assert!(
                cache
                    .entries_and_refresh(&tasks, &provider, credential.clone())
                    .is_empty()
            );
        }
        let request = next_request(&mut server).await;
        assert!(request.starts_with("GET /v1/models HTTP/1.1"));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), server.requests.recv(),)
                .await
                .is_err()
        );

        release.add_permits(1);
        wait_until_idle(&cache, "single-flight-fixture").await;
        let models = cache.entries_and_refresh(&tasks, &provider, credential);
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].model_id, "live-model");
        tasks.shutdown().await;
    }

    #[tokio::test]
    async fn expired_cache_entry_triggers_exactly_one_refresh() {
        let release = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
        let mut server = spawn_http_sequence(vec![
            HttpResponse::ok(r#"{"data":[{"id":"refreshed-model"}]}"#)
                .gated(std::sync::Arc::clone(&release)),
            HttpResponse::ok(r#"{"data":[{"id":"unexpected-second-request"}]}"#),
        ])
        .await;
        let provider = verlet_metadata::provider_store::LlmProviderRecord::new(
            "ttl-fixture",
            verlet_history::ProviderApi::OpenAIChatCompletions,
            format!("{}/v1", server.base_url),
        )
        .with_auth_header(true);
        let cache = std::sync::Arc::new(super::LiveModelCache::new());
        cache.seed_for_test(
            &provider,
            vec![super::LiveModel {
                provider_id: provider.provider_id.clone(),
                model_id: "stale-model".to_string(),
                display_name: "Stale Model".to_string(),
            }],
            super::REFRESH_INTERVAL + std::time::Duration::from_secs(1),
        );
        let tasks =
            std::sync::Arc::new(crate::adapters::app_server::lifecycle::InstanceTaskSet::new());
        let credential = super::RefreshCredential::ApiKey("fixture-key".to_string());

        for _ in 0..16 {
            let models = cache.entries_and_refresh(&tasks, &provider, credential.clone());
            assert_eq!(models[0].model_id, "stale-model");
        }
        let _request = next_request(&mut server).await;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), server.requests.recv(),)
                .await
                .is_err()
        );

        release.add_permits(1);
        wait_until_idle(&cache, "ttl-fixture").await;
        let models = cache.entries_and_refresh(&tasks, &provider, credential);
        assert_eq!(models[0].model_id, "refreshed-model");
        tasks.shutdown().await;
    }
}
