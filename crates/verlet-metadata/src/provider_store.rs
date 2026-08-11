pub const OPENAI_COMPATIBLE_PROVIDER_ID: &str = "openai_compatible";
pub const OPENAI_COMPATIBLE_BASE_URL: &str = "https://api.example.invalid/v1";
pub const OPENAI_COMPATIBLE_DEFAULT_MODEL: &str = "example-chat-model";
pub const OPENAI_COMPATIBLE_ALT_MODEL: &str = "example-chat-model-large";
pub const OPENAI_COMPATIBLE_EXAMPLE_HEADER: &str = "X-Example-Provider";
pub const OPENAI_CODEX_PROVIDER_ID: &str = "openai-codex";
pub const OPENAI_CODEX_RESPONSES_URL: &str = "https://chatgpt.com/backend-api/codex/responses";
pub const OPENAI_CODEX_DEFAULT_MODEL: &str = "gpt-5.6-sol";
pub const OPENAI_CODEX_MODELS: &[&str] =
    &[OPENAI_CODEX_DEFAULT_MODEL, "gpt-5.6-terra", "gpt-5.6-luna"];

pub type LlmProviderStoreResult<T> = Result<T, LlmProviderStoreError>;

pub type MetadataStoreResult<T> = Result<T, MetadataStoreError>;

async fn provider_cancellation_safe<T>(
    future: impl std::future::Future<Output = LlmProviderStoreResult<T>> + Send + 'static,
) -> LlmProviderStoreResult<T>
where
    T: Send + 'static,
{
    tokio::spawn(future).await.map_err(|error| {
        LlmProviderStoreError::Storage(format!("sqlite transaction task failed: {error}"))
    })?
}

async fn metadata_cancellation_safe<T>(
    future: impl std::future::Future<Output = MetadataStoreResult<T>> + Send + 'static,
) -> MetadataStoreResult<T>
where
    T: Send + 'static,
{
    tokio::spawn(future).await.map_err(|error| {
        MetadataStoreError::Storage(format!("sqlite transaction task failed: {error}"))
    })?
}

#[derive(Debug, thiserror::Error)]
pub enum LlmProviderStoreError {
    #[error("LLM provider id cannot be empty")]
    EmptyProviderId,
    #[error("LLM provider model id cannot be empty")]
    EmptyModelId,
    #[error("LLM provider storage failed: {0}")]
    Storage(String),
    #[error("LLM provider storage codec failed: {0}")]
    Codec(String),
    #[error("LLM provider credential for {provider_id} is expired")]
    ExpiredCredential { provider_id: String },
    #[error("LLM provider credential for {provider_id} requires OAuth refresh")]
    OAuthRefreshRequired { provider_id: String },
    #[error("LLM provider auth command resolution is not enabled for {provider_id}")]
    CommandAuthUnsupported { provider_id: String },
}

#[derive(Debug, thiserror::Error)]
pub enum MetadataStoreError {
    #[error("metadata store failed: {0}")]
    Storage(String),
    #[error("metadata store codec failed: {0}")]
    Codec(String),
}

impl From<LlmProviderStoreError> for MetadataStoreError {
    fn from(err: LlmProviderStoreError) -> Self {
        Self::Storage(err.to_string())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LlmProviderRecord {
    pub provider_id: String,
    pub api: verlet_history::ProviderApi,
    pub base_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default)]
    pub auth: LlmProviderAuthConfig,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub headers: std::collections::BTreeMap<String, LlmProviderConfigValue>,
    #[serde(default)]
    pub auth_header: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<LlmProviderModelRecord>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub metadata: std::collections::BTreeMap<String, String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl LlmProviderRecord {
    pub fn new(
        provider_id: impl Into<String>,
        api: verlet_history::ProviderApi,
        base_url: impl Into<String>,
    ) -> Self {
        let now = verlet_history::now_ms();
        Self {
            provider_id: provider_id.into(),
            api,
            base_url: base_url.into(),
            display_name: None,
            auth: LlmProviderAuthConfig::default(),
            headers: std::collections::BTreeMap::new(),
            auth_header: false,
            models: Vec::new(),
            metadata: std::collections::BTreeMap::new(),
            created_at_ms: now,
            updated_at_ms: now,
        }
    }

    pub fn with_display_name(mut self, display_name: impl Into<String>) -> Self {
        self.display_name = Some(display_name.into());
        self
    }

    pub fn with_auth(mut self, auth: LlmProviderAuthConfig) -> Self {
        self.auth = auth;
        self
    }

    pub fn with_auth_header(mut self, auth_header: bool) -> Self {
        self.auth_header = auth_header;
        self
    }

    pub fn with_header(mut self, name: impl Into<String>, value: LlmProviderConfigValue) -> Self {
        self.headers.insert(name.into(), value);
        self
    }

    pub fn with_model(mut self, model: LlmProviderModelRecord) -> Self {
        self.models.push(model);
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    fn validate(&self) -> LlmProviderStoreResult<()> {
        validate_provider_id(&self.provider_id)?;
        for model in &self.models {
            validate_model_id(&model.model_id)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LlmProviderModelRecord {
    pub model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api: Option<verlet_history::ProviderApi>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_modalities: Vec<LlmProviderInputModality>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub headers: std::collections::BTreeMap<String, LlmProviderConfigValue>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub metadata: std::collections::BTreeMap<String, String>,
}

impl LlmProviderModelRecord {
    pub fn new(model_id: impl Into<String>) -> Self {
        Self {
            model_id: model_id.into(),
            display_name: None,
            api: None,
            base_url: None,
            context_window_tokens: None,
            max_output_tokens: None,
            input_modalities: Vec::new(),
            headers: std::collections::BTreeMap::new(),
            metadata: std::collections::BTreeMap::new(),
        }
    }

    pub fn with_display_name(mut self, display_name: impl Into<String>) -> Self {
        self.display_name = Some(display_name.into());
        self
    }

    pub fn with_context_window_tokens(mut self, context_window_tokens: u64) -> Self {
        self.context_window_tokens = Some(context_window_tokens);
        self
    }

    pub fn with_max_output_tokens(mut self, max_output_tokens: u32) -> Self {
        self.max_output_tokens = Some(max_output_tokens);
        self
    }

    pub fn with_input_modality(mut self, modality: LlmProviderInputModality) -> Self {
        self.input_modalities.push(modality);
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LlmProviderInputModality {
    Text,
    Image,
    Audio,
    File,
    Other(String),
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LlmProviderConfigValue {
    Literal { value: String },
    Env { name: String },
    Command { command: String },
}

impl LlmProviderConfigValue {
    pub fn literal(value: impl Into<String>) -> Self {
        Self::Literal {
            value: value.into(),
        }
    }

    pub fn env(name: impl Into<String>) -> Self {
        Self::Env { name: name.into() }
    }

    pub fn command(command: impl Into<String>) -> Self {
        Self::Command {
            command: command.into(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LlmProviderAuthConfig {
    #[default]
    StoredOrEnvironment,
    None,
    Env {
        name: String,
    },
    InlineApiKey {
        key: String,
    },
    Command {
        command: String,
    },
}

#[derive(Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LlmProviderCredential {
    ApiKey {
        key: String,
    },
    OAuth {
        access: String,
        refresh: String,
        expires_at_ms: i64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        account_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        email: Option<String>,
    },
}

impl std::fmt::Debug for LlmProviderCredential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ApiKey { .. } => formatter
                .debug_struct("ApiKey")
                .field("key", &"[REDACTED]")
                .finish(),
            Self::OAuth { expires_at_ms, .. } => formatter
                .debug_struct("OAuth")
                .field("access", &"[REDACTED]")
                .field("refresh", &"[REDACTED]")
                .field("expires_at_ms", expires_at_ms)
                .field("account_id", &"[REDACTED]")
                .field("email", &"[REDACTED]")
                .finish(),
        }
    }
}

#[derive(Clone, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LlmProviderAuthContext {
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub runtime_api_keys: std::collections::BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub environment: std::collections::BTreeMap<String, String>,
}

impl std::fmt::Debug for LlmProviderAuthContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        struct RedactedValues<'a>(&'a std::collections::BTreeMap<String, String>);

        impl std::fmt::Debug for RedactedValues<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_map()
                    .entries(self.0.keys().map(|name| (name, "<redacted>")))
                    .finish()
            }
        }

        f.debug_struct("LlmProviderAuthContext")
            .field("runtime_api_keys", &RedactedValues(&self.runtime_api_keys))
            .field("environment", &RedactedValues(&self.environment))
            .finish()
    }
}

impl LlmProviderAuthContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_process_env() -> Self {
        Self::from_env_vars(std::env::vars_os())
    }

    /// `std::env::vars()` panics on non-unicode values, and a foreign
    /// non-UTF8 variable anywhere in the environment must not take down auth
    /// resolution; such entries can never match a provider variable, so they
    /// are skipped.
    fn from_env_vars(vars: impl Iterator<Item = (std::ffi::OsString, std::ffi::OsString)>) -> Self {
        Self {
            runtime_api_keys: std::collections::BTreeMap::new(),
            environment: vars
                .filter_map(|(name, value)| {
                    Some((name.into_string().ok()?, value.into_string().ok()?))
                })
                .collect(),
        }
    }

    pub fn with_runtime_api_key(
        mut self,
        provider_id: impl Into<String>,
        key: impl Into<String>,
    ) -> Self {
        self.runtime_api_keys.insert(provider_id.into(), key.into());
        self
    }

    pub fn with_env(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.environment.insert(name.into(), value.into());
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LlmProviderAuthSourceKind {
    Runtime,
    Stored,
    Environment,
    CatalogInline,
    CatalogCommand,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LlmProviderAuthStatus {
    pub configured: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<LlmProviderAuthSourceKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

impl LlmProviderAuthStatus {
    pub fn missing() -> Self {
        Self {
            configured: false,
            source: None,
            label: None,
        }
    }

    pub fn configured(source: LlmProviderAuthSourceKind, label: impl Into<String>) -> Self {
        Self {
            configured: true,
            source: Some(source),
            label: Some(label.into()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LlmProviderResolvedAuth {
    pub api_key: String,
    pub source: LlmProviderAuthSourceKind,
}

#[async_trait::async_trait]
pub trait LlmProviderCatalogStore: Send + Sync {
    async fn upsert_provider(&self, record: LlmProviderRecord) -> LlmProviderStoreResult<()>;
    async fn get_provider(
        &self,
        provider_id: &str,
    ) -> LlmProviderStoreResult<Option<LlmProviderRecord>>;
    async fn list_providers(&self) -> LlmProviderStoreResult<Vec<LlmProviderRecord>>;
    async fn delete_provider(&self, provider_id: &str) -> LlmProviderStoreResult<()>;
}

#[async_trait::async_trait]
pub trait LlmProviderAuthStore: Send + Sync {
    async fn set_credential(
        &self,
        provider_id: &str,
        credential: LlmProviderCredential,
    ) -> LlmProviderStoreResult<()>;
    async fn get_credential(
        &self,
        provider_id: &str,
    ) -> LlmProviderStoreResult<Option<LlmProviderCredential>>;
    async fn delete_credential(&self, provider_id: &str) -> LlmProviderStoreResult<()>;
}

#[async_trait::async_trait]
pub trait ThreadMetadataStore: Send + Sync {
    async fn upsert_thread_lifecycle(
        &self,
        record: verlet_runtime_contracts::ThreadLifecycleRecord,
    ) -> MetadataStoreResult<()>;
    async fn get_thread_lifecycle(
        &self,
        thread_id: verlet_runtime_contracts::ThreadId,
    ) -> MetadataStoreResult<Option<verlet_runtime_contracts::ThreadLifecycleRecord>>;
    async fn list_thread_lifecycle(
        &self,
        scope: &verlet_runtime_contracts::ThreadScope,
    ) -> MetadataStoreResult<Vec<verlet_runtime_contracts::ThreadLifecycleRecord>>;
    async fn list_thread_lifecycle_for_user(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> MetadataStoreResult<Vec<verlet_runtime_contracts::ThreadLifecycleRecord>>;
}

/// Test-support template for a generic OpenAI-compatible provider. It points
/// at the unusable `example.invalid` endpoint and is never seeded as a
/// product default (EMO-575); tests upsert it explicitly when they exercise
/// generic provider behavior.
pub fn example_openai_compatible_record() -> LlmProviderRecord {
    LlmProviderRecord::new(
        OPENAI_COMPATIBLE_PROVIDER_ID,
        verlet_history::ProviderApi::OpenAIChatCompletions,
        OPENAI_COMPATIBLE_BASE_URL,
    )
    .with_display_name("OpenAI Compatible")
    .with_auth_header(true)
    .with_header(
        OPENAI_COMPATIBLE_EXAMPLE_HEADER,
        LlmProviderConfigValue::literal("required"),
    )
    .with_model(
        LlmProviderModelRecord::new(OPENAI_COMPATIBLE_DEFAULT_MODEL)
            .with_display_name("Example Chat Model")
            .with_input_modality(LlmProviderInputModality::Text)
            .with_metadata("default", "true"),
    )
    .with_model(
        LlmProviderModelRecord::new(OPENAI_COMPATIBLE_ALT_MODEL)
            .with_display_name("Example Chat Model Large")
            .with_input_modality(LlmProviderInputModality::Text),
    )
    .with_metadata("api_family", "openai_chat_completions")
    .with_metadata(
        "auth_env",
        "OPENAI_COMPATIBLE_API_KEY,VERLET_OPENAI_COMPATIBLE_API_KEY",
    )
}

pub fn default_openai_codex_llm_provider_record() -> LlmProviderRecord {
    let mut provider = LlmProviderRecord::new(
        OPENAI_CODEX_PROVIDER_ID,
        verlet_history::ProviderApi::OpenAIResponses,
        OPENAI_CODEX_RESPONSES_URL,
    )
    .with_display_name("OpenAI Codex (ChatGPT plan)")
    .with_auth_header(true)
    .with_metadata("api_family", "openai_responses")
    .with_metadata("billing", "chatgpt_plan")
    .with_metadata("catalog", "static_emo_560");
    for model_id in OPENAI_CODEX_MODELS {
        let mut model = LlmProviderModelRecord::new(*model_id)
            .with_display_name(*model_id)
            .with_input_modality(LlmProviderInputModality::Text);
        if *model_id == OPENAI_CODEX_DEFAULT_MODEL {
            model = model.with_metadata("default", "true");
        }
        provider = provider.with_model(model);
    }
    provider
}

pub async fn seed_openai_codex_llm_provider(
    store: &dyn LlmProviderCatalogStore,
) -> LlmProviderStoreResult<()> {
    store
        .upsert_provider(default_openai_codex_llm_provider_record())
        .await
}

pub async fn seed_default_llm_providers<S>(store: &S) -> LlmProviderStoreResult<()>
where
    S: LlmProviderCatalogStore + LlmProviderAuthStore,
{
    remove_placeholder_openai_compatible_record(store, store).await?;
    seed_openai_codex_llm_provider(store).await
}

/// Migration (EMO-575): earlier releases seeded a placeholder
/// `openai_compatible` record pointing at `example.invalid`. Delete it IFF it
/// is pristine (base_url still the placeholder AND no credential stored in
/// this store) so a record the user modified or credentialed survives as a
/// custom provider. Idempotent: once removed, later opens see no record and
/// do nothing.
async fn remove_placeholder_openai_compatible_record(
    catalog_store: &dyn LlmProviderCatalogStore,
    auth_store: &dyn LlmProviderAuthStore,
) -> LlmProviderStoreResult<()> {
    let Some(record) = catalog_store
        .get_provider(OPENAI_COMPATIBLE_PROVIDER_ID)
        .await?
    else {
        return Ok(());
    };
    if record.base_url != OPENAI_COMPATIBLE_BASE_URL {
        return Ok(());
    }
    if auth_store
        .get_credential(OPENAI_COMPATIBLE_PROVIDER_ID)
        .await?
        .is_some()
    {
        return Ok(());
    }
    catalog_store
        .delete_provider(OPENAI_COMPATIBLE_PROVIDER_ID)
        .await
}

#[derive(Clone)]
pub struct SqliteLlmProviderStore {
    inner: verlet_sqlite::Db,
}

impl SqliteLlmProviderStore {
    pub async fn open(path: impl AsRef<std::path::Path>) -> LlmProviderStoreResult<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).map_err(storage_error)?;
                restrict_dir_permissions(parent)?;
            }
        }
        let inner = verlet_sqlite::Db::open(path, verlet_sqlite::DbConfig::default())
            .await
            .map_err(storage_error)?;
        restrict_file_permissions(path)?;
        Self::from_db(inner).await
    }

    pub async fn in_memory() -> LlmProviderStoreResult<Self> {
        let inner = verlet_sqlite::Db::in_memory(verlet_sqlite::DbConfig::default())
            .await
            .map_err(storage_error)?;
        Self::from_db(inner).await
    }

    async fn from_db(inner: verlet_sqlite::Db) -> LlmProviderStoreResult<Self> {
        provider_cancellation_safe(async move {
            let store = Self { inner };
            let connection = store.inner.connect().await.map_err(storage_error)?;
            init_provider_store_schema(&connection).await?;
            Ok(store)
        })
        .await
    }

    async fn connect(&self) -> LlmProviderStoreResult<verlet_sqlite::Connection> {
        self.inner.connect().await.map_err(storage_error)
    }
}

#[derive(Clone)]
pub struct SqliteMetadataStore {
    provider_store: SqliteLlmProviderStore,
}

impl SqliteMetadataStore {
    pub async fn open(path: impl AsRef<std::path::Path>) -> MetadataStoreResult<Self> {
        let provider_store = SqliteLlmProviderStore::open(path)
            .await
            .map_err(MetadataStoreError::from)?;
        metadata_cancellation_safe(async move {
            let store = Self { provider_store };
            store.init_metadata_schema().await?;
            Ok(store)
        })
        .await
    }

    pub async fn in_memory() -> MetadataStoreResult<Self> {
        let provider_store = SqliteLlmProviderStore::in_memory()
            .await
            .map_err(MetadataStoreError::from)?;
        metadata_cancellation_safe(async move {
            let store = Self { provider_store };
            store.init_metadata_schema().await?;
            Ok(store)
        })
        .await
    }

    pub fn llm_provider_store(&self) -> &SqliteLlmProviderStore {
        &self.provider_store
    }

    async fn init_metadata_schema(&self) -> MetadataStoreResult<()> {
        let connection = self
            .provider_store
            .connect()
            .await
            .map_err(MetadataStoreError::from)?;
        init_thread_metadata_schema(&connection).await
    }
}

#[async_trait::async_trait]
impl LlmProviderCatalogStore for SqliteMetadataStore {
    async fn upsert_provider(&self, record: LlmProviderRecord) -> LlmProviderStoreResult<()> {
        self.provider_store.upsert_provider(record).await
    }

    async fn get_provider(
        &self,
        provider_id: &str,
    ) -> LlmProviderStoreResult<Option<LlmProviderRecord>> {
        self.provider_store.get_provider(provider_id).await
    }

    async fn list_providers(&self) -> LlmProviderStoreResult<Vec<LlmProviderRecord>> {
        self.provider_store.list_providers().await
    }

    async fn delete_provider(&self, provider_id: &str) -> LlmProviderStoreResult<()> {
        self.provider_store.delete_provider(provider_id).await
    }
}

#[async_trait::async_trait]
impl LlmProviderAuthStore for SqliteMetadataStore {
    async fn set_credential(
        &self,
        provider_id: &str,
        credential: LlmProviderCredential,
    ) -> LlmProviderStoreResult<()> {
        self.provider_store
            .set_credential(provider_id, credential)
            .await
    }

    async fn get_credential(
        &self,
        provider_id: &str,
    ) -> LlmProviderStoreResult<Option<LlmProviderCredential>> {
        self.provider_store.get_credential(provider_id).await
    }

    async fn delete_credential(&self, provider_id: &str) -> LlmProviderStoreResult<()> {
        self.provider_store.delete_credential(provider_id).await
    }
}

#[async_trait::async_trait]
impl ThreadMetadataStore for SqliteMetadataStore {
    async fn upsert_thread_lifecycle(
        &self,
        mut record: verlet_runtime_contracts::ThreadLifecycleRecord,
    ) -> MetadataStoreResult<()> {
        let connection = self
            .provider_store
            .connect()
            .await
            .map_err(MetadataStoreError::from)?;
        let thread_id = record.coordinates.thread_id.to_string();
        record.updated_at_ms = now_ms_u64()?;
        let tenant_id = record.coordinates.tenant_id.clone();
        let user_id = record.coordinates.user_id.clone();
        let session_id = record.coordinates.session_id.clone();
        let parent_thread_id = record.parent_thread_id.map(|id| id.to_string());
        let status: &str = record.status.as_ref();
        let record_json = serde_json::to_string(&record).map_err(metadata_codec_error)?;
        connection
            .execute(
                "INSERT INTO thread_lifecycle_records (
                    thread_id, tenant_id, user_id, session_id, parent_thread_id,
                    status, record_json, created_at_ms, updated_at_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                ON CONFLICT(thread_id) DO UPDATE SET
                    tenant_id = excluded.tenant_id,
                    user_id = excluded.user_id,
                    session_id = excluded.session_id,
                    parent_thread_id = excluded.parent_thread_id,
                    status = excluded.status,
                    record_json = json_set(
                        excluded.record_json,
                        '$.created_at_ms',
                        thread_lifecycle_records.created_at_ms
                    ),
                    updated_at_ms = excluded.updated_at_ms",
                verlet_sqlite::params![
                    thread_id,
                    tenant_id,
                    user_id,
                    session_id,
                    parent_thread_id,
                    status,
                    record_json,
                    sqlite_timestamp(record.created_at_ms)?,
                    sqlite_timestamp(record.updated_at_ms)?,
                ],
            )
            .await
            .map_err(metadata_storage_error)?;
        Ok(())
    }

    async fn get_thread_lifecycle(
        &self,
        thread_id: verlet_runtime_contracts::ThreadId,
    ) -> MetadataStoreResult<Option<verlet_runtime_contracts::ThreadLifecycleRecord>> {
        let connection = self
            .provider_store
            .connect()
            .await
            .map_err(MetadataStoreError::from)?;
        sqlite_get_thread_lifecycle(&connection, thread_id).await
    }

    async fn list_thread_lifecycle(
        &self,
        scope: &verlet_runtime_contracts::ThreadScope,
    ) -> MetadataStoreResult<Vec<verlet_runtime_contracts::ThreadLifecycleRecord>> {
        let connection = self
            .provider_store
            .connect()
            .await
            .map_err(MetadataStoreError::from)?;
        sqlite_list_thread_lifecycle(&connection, scope).await
    }

    async fn list_thread_lifecycle_for_user(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> MetadataStoreResult<Vec<verlet_runtime_contracts::ThreadLifecycleRecord>> {
        let connection = self
            .provider_store
            .connect()
            .await
            .map_err(MetadataStoreError::from)?;
        sqlite_list_thread_lifecycle_for_user(&connection, tenant_id, user_id).await
    }
}

#[async_trait::async_trait]
impl LlmProviderCatalogStore for SqliteLlmProviderStore {
    async fn upsert_provider(&self, mut record: LlmProviderRecord) -> LlmProviderStoreResult<()> {
        record.validate()?;
        let connection = self.connect().await?;
        record.updated_at_ms = verlet_history::now_ms();
        let record_json = serde_json::to_string(&record).map_err(codec_error)?;
        connection
            .execute(
                "INSERT INTO llm_provider_records (
                    provider_id, record_json, created_at_ms, updated_at_ms
                ) VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(provider_id) DO UPDATE SET
                    record_json = json_set(
                        excluded.record_json,
                        '$.created_at_ms',
                        llm_provider_records.created_at_ms
                    ),
                    updated_at_ms = excluded.updated_at_ms",
                verlet_sqlite::params![
                    record.provider_id,
                    record_json,
                    record.created_at_ms,
                    record.updated_at_ms,
                ],
            )
            .await
            .map_err(storage_error)?;
        Ok(())
    }

    async fn get_provider(
        &self,
        provider_id: &str,
    ) -> LlmProviderStoreResult<Option<LlmProviderRecord>> {
        validate_provider_id(provider_id)?;
        let connection = self.connect().await?;
        sqlite_get_provider(&connection, provider_id).await
    }

    async fn list_providers(&self) -> LlmProviderStoreResult<Vec<LlmProviderRecord>> {
        let connection = self.connect().await?;
        let mut rows = connection
            .query(
                "SELECT record_json FROM llm_provider_records ORDER BY provider_id",
                (),
            )
            .await
            .map_err(storage_error)?;
        let mut records = Vec::new();
        while let Some(row) = rows.next().await.map_err(storage_error)? {
            records.push(decode_provider_record(
                &row.get::<String>(0).map_err(storage_error)?,
            )?);
        }
        Ok(records)
    }

    async fn delete_provider(&self, provider_id: &str) -> LlmProviderStoreResult<()> {
        validate_provider_id(provider_id)?;
        let connection = self.connect().await?;
        connection
            .execute(
                "DELETE FROM llm_provider_records WHERE provider_id = ?1",
                verlet_sqlite::params![provider_id],
            )
            .await
            .map_err(storage_error)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl LlmProviderAuthStore for SqliteLlmProviderStore {
    async fn set_credential(
        &self,
        provider_id: &str,
        credential: LlmProviderCredential,
    ) -> LlmProviderStoreResult<()> {
        validate_provider_id(provider_id)?;
        let credential_json = serde_json::to_string(&credential).map_err(codec_error)?;
        let now = verlet_history::now_ms();
        let connection = self.connect().await?;
        connection
            .execute(
                "INSERT INTO llm_provider_credentials (
                        provider_id, credential_json, updated_at_ms
                    ) VALUES (?1, ?2, ?3)
                    ON CONFLICT(provider_id) DO UPDATE SET
                        credential_json = excluded.credential_json,
                        updated_at_ms = excluded.updated_at_ms",
                verlet_sqlite::params![provider_id, credential_json, now],
            )
            .await
            .map_err(storage_error)?;
        Ok(())
    }

    async fn get_credential(
        &self,
        provider_id: &str,
    ) -> LlmProviderStoreResult<Option<LlmProviderCredential>> {
        validate_provider_id(provider_id)?;
        let connection = self.connect().await?;
        sqlite_get_credential(&connection, provider_id).await
    }

    async fn delete_credential(&self, provider_id: &str) -> LlmProviderStoreResult<()> {
        validate_provider_id(provider_id)?;
        let connection = self.connect().await?;
        connection
            .execute(
                "DELETE FROM llm_provider_credentials WHERE provider_id = ?1",
                verlet_sqlite::params![provider_id],
            )
            .await
            .map_err(storage_error)?;
        Ok(())
    }
}

pub async fn resolve_llm_provider_auth(
    auth_store: &dyn LlmProviderAuthStore,
    provider: &LlmProviderRecord,
    context: &LlmProviderAuthContext,
) -> LlmProviderStoreResult<Option<LlmProviderResolvedAuth>> {
    if let Some(key) = context.runtime_api_keys.get(&provider.provider_id) {
        return Ok(Some(LlmProviderResolvedAuth {
            api_key: key.clone(),
            source: LlmProviderAuthSourceKind::Runtime,
        }));
    }

    if let Some(credential) = auth_store.get_credential(&provider.provider_id).await? {
        return credential_to_resolved_auth(&provider.provider_id, credential)
            .map(|api_key| Some((api_key, LlmProviderAuthSourceKind::Stored).into()));
    }

    for env_name in provider_env_candidates(provider) {
        if let Some(key) = environment_value(context, &env_name)
            && !key.is_empty()
        {
            return Ok(Some(LlmProviderResolvedAuth {
                api_key: key,
                source: LlmProviderAuthSourceKind::Environment,
            }));
        }
    }

    match &provider.auth {
        LlmProviderAuthConfig::StoredOrEnvironment | LlmProviderAuthConfig::None => Ok(None),
        LlmProviderAuthConfig::Env { name } => Ok(environment_value(context, name)
            .filter(|key| !key.is_empty())
            .map(|key| LlmProviderResolvedAuth {
                api_key: key,
                source: LlmProviderAuthSourceKind::Environment,
            })),
        LlmProviderAuthConfig::InlineApiKey { key } => Ok(Some(LlmProviderResolvedAuth {
            api_key: key.clone(),
            source: LlmProviderAuthSourceKind::CatalogInline,
        })),
        LlmProviderAuthConfig::Command { .. } => {
            Err(LlmProviderStoreError::CommandAuthUnsupported {
                provider_id: provider.provider_id.clone(),
            })
        }
    }
}

pub async fn llm_provider_auth_status(
    auth_store: &dyn LlmProviderAuthStore,
    provider: &LlmProviderRecord,
    context: &LlmProviderAuthContext,
) -> LlmProviderStoreResult<LlmProviderAuthStatus> {
    if context.runtime_api_keys.contains_key(&provider.provider_id) {
        return Ok(LlmProviderAuthStatus::configured(
            LlmProviderAuthSourceKind::Runtime,
            "runtime override",
        ));
    }

    if auth_store
        .get_credential(&provider.provider_id)
        .await?
        .is_some()
    {
        return Ok(LlmProviderAuthStatus::configured(
            LlmProviderAuthSourceKind::Stored,
            "stored credential",
        ));
    }

    for env_name in provider_env_candidates(provider) {
        if environment_value(context, &env_name).is_some_and(|key| !key.is_empty()) {
            return Ok(LlmProviderAuthStatus::configured(
                LlmProviderAuthSourceKind::Environment,
                env_name,
            ));
        }
    }

    match &provider.auth {
        LlmProviderAuthConfig::StoredOrEnvironment | LlmProviderAuthConfig::None => {
            Ok(LlmProviderAuthStatus::missing())
        }
        LlmProviderAuthConfig::Env { name } => {
            if environment_value(context, name).is_some_and(|key| !key.is_empty()) {
                Ok(LlmProviderAuthStatus::configured(
                    LlmProviderAuthSourceKind::Environment,
                    name.clone(),
                ))
            } else {
                Ok(LlmProviderAuthStatus::missing())
            }
        }
        LlmProviderAuthConfig::InlineApiKey { .. } => Ok(LlmProviderAuthStatus::configured(
            LlmProviderAuthSourceKind::CatalogInline,
            "provider catalog",
        )),
        LlmProviderAuthConfig::Command { .. } => Ok(LlmProviderAuthStatus::configured(
            LlmProviderAuthSourceKind::CatalogCommand,
            "provider catalog command",
        )),
    }
}

fn environment_value(context: &LlmProviderAuthContext, canonical: &str) -> Option<String> {
    verlet_runtime_contracts::env_compat::string_with(canonical, |name| {
        context.environment.get(name).cloned()
    })
}

fn credential_to_resolved_auth(
    provider_id: &str,
    credential: LlmProviderCredential,
) -> LlmProviderStoreResult<String> {
    match credential {
        LlmProviderCredential::ApiKey { key } => Ok(key),
        LlmProviderCredential::OAuth {
            access,
            expires_at_ms,
            ..
        } => {
            if expires_at_ms <= verlet_history::now_ms() {
                Err(LlmProviderStoreError::OAuthRefreshRequired {
                    provider_id: provider_id.to_string(),
                })
            } else {
                Ok(access)
            }
        }
    }
}

impl From<(String, LlmProviderAuthSourceKind)> for LlmProviderResolvedAuth {
    fn from((api_key, source): (String, LlmProviderAuthSourceKind)) -> Self {
        Self { api_key, source }
    }
}

fn provider_env_candidates(provider: &LlmProviderRecord) -> Vec<String> {
    let mut candidates = Vec::new();
    push_env_candidate(
        &mut candidates,
        format!("{}_API_KEY", env_prefix(&provider.provider_id)),
    );
    match provider.provider_id.as_str() {
        "openai" | "openai_responses" | "openai_chat_completions" => {
            push_env_candidate(&mut candidates, "OPENAI_API_KEY".to_string());
        }
        "anthropic" | "anthropic_messages" => {
            push_env_candidate(&mut candidates, "ANTHROPIC_API_KEY".to_string());
        }
        "openai_compatible" => {
            push_env_candidate(&mut candidates, "OPENAI_COMPATIBLE_API_KEY".to_string());
            push_env_candidate(
                &mut candidates,
                "VERLET_OPENAI_COMPATIBLE_API_KEY".to_string(),
            );
        }
        _ => {}
    }
    candidates
}

fn push_env_candidate(candidates: &mut Vec<String>, candidate: String) {
    if !candidates.contains(&candidate) {
        candidates.push(candidate);
    }
}

fn env_prefix(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

async fn init_provider_store_schema(
    connection: &verlet_sqlite::Connection,
) -> LlmProviderStoreResult<()> {
    connection
        .execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS llm_provider_records (
                provider_id TEXT PRIMARY KEY NOT NULL,
                record_json TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS llm_provider_credentials (
                provider_id TEXT PRIMARY KEY NOT NULL,
                credential_json TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );
            "#,
        )
        .await
        .map_err(storage_error)
}

async fn init_thread_metadata_schema(
    connection: &verlet_sqlite::Connection,
) -> MetadataStoreResult<()> {
    connection
        .execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS thread_lifecycle_records (
                thread_id TEXT PRIMARY KEY NOT NULL,
                tenant_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                parent_thread_id TEXT,
                status TEXT NOT NULL,
                record_json TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS thread_lifecycle_records_scope_idx
                ON thread_lifecycle_records(tenant_id, user_id, session_id, created_at_ms);

            CREATE INDEX IF NOT EXISTS thread_lifecycle_records_parent_idx
                ON thread_lifecycle_records(parent_thread_id);
            "#,
        )
        .await
        .map_err(metadata_storage_error)
}

async fn sqlite_get_provider(
    connection: &verlet_sqlite::Connection,
    provider_id: &str,
) -> LlmProviderStoreResult<Option<LlmProviderRecord>> {
    let mut rows = connection
        .query(
            "SELECT record_json FROM llm_provider_records WHERE provider_id = ?1",
            verlet_sqlite::params![provider_id],
        )
        .await
        .map_err(storage_error)?;
    let record_json = rows
        .next()
        .await
        .map_err(storage_error)?
        .map(|row| row.get::<String>(0).map_err(storage_error))
        .transpose()?;
    record_json
        .map(|json| decode_provider_record(&json))
        .transpose()
}

async fn sqlite_get_credential(
    connection: &verlet_sqlite::Connection,
    provider_id: &str,
) -> LlmProviderStoreResult<Option<LlmProviderCredential>> {
    let mut rows = connection
        .query(
            "SELECT credential_json FROM llm_provider_credentials WHERE provider_id = ?1",
            verlet_sqlite::params![provider_id],
        )
        .await
        .map_err(storage_error)?;
    let credential_json = rows
        .next()
        .await
        .map_err(storage_error)?
        .map(|row| row.get::<String>(0).map_err(storage_error))
        .transpose()?;
    credential_json
        .map(|json| serde_json::from_str(&json).map_err(codec_error))
        .transpose()
}

async fn sqlite_get_thread_lifecycle(
    connection: &verlet_sqlite::Connection,
    thread_id: verlet_runtime_contracts::ThreadId,
) -> MetadataStoreResult<Option<verlet_runtime_contracts::ThreadLifecycleRecord>> {
    let mut rows = connection
        .query(
            "SELECT record_json FROM thread_lifecycle_records WHERE thread_id = ?1",
            verlet_sqlite::params![thread_id.to_string()],
        )
        .await
        .map_err(metadata_storage_error)?;
    let record_json = rows
        .next()
        .await
        .map_err(metadata_storage_error)?
        .map(|row| row.get::<String>(0).map_err(metadata_storage_error))
        .transpose()?;
    record_json
        .map(|json| decode_thread_lifecycle_record(&json))
        .transpose()
}

async fn sqlite_list_thread_lifecycle(
    connection: &verlet_sqlite::Connection,
    scope: &verlet_runtime_contracts::ThreadScope,
) -> MetadataStoreResult<Vec<verlet_runtime_contracts::ThreadLifecycleRecord>> {
    let mut rows = connection
        .query(
            "SELECT record_json FROM thread_lifecycle_records
            WHERE tenant_id = ?1 AND user_id = ?2 AND session_id = ?3
            ORDER BY created_at_ms, thread_id",
            verlet_sqlite::params![
                scope.tenant_id.as_str(),
                scope.user_id.as_str(),
                scope.session_id.as_str()
            ],
        )
        .await
        .map_err(metadata_storage_error)?;
    let mut records = Vec::new();
    while let Some(row) = rows.next().await.map_err(metadata_storage_error)? {
        let json = row.get::<String>(0).map_err(metadata_storage_error)?;
        records.push(decode_thread_lifecycle_record(&json)?);
    }
    Ok(records)
}

async fn sqlite_list_thread_lifecycle_for_user(
    connection: &verlet_sqlite::Connection,
    tenant_id: &str,
    user_id: &str,
) -> MetadataStoreResult<Vec<verlet_runtime_contracts::ThreadLifecycleRecord>> {
    let mut rows = connection
        .query(
            "SELECT record_json FROM thread_lifecycle_records
            WHERE tenant_id = ?1 AND user_id = ?2
            ORDER BY created_at_ms, session_id, thread_id",
            verlet_sqlite::params![tenant_id, user_id],
        )
        .await
        .map_err(metadata_storage_error)?;
    let mut records = Vec::new();
    while let Some(row) = rows.next().await.map_err(metadata_storage_error)? {
        let json = row.get::<String>(0).map_err(metadata_storage_error)?;
        records.push(decode_thread_lifecycle_record(&json)?);
    }
    Ok(records)
}

fn decode_provider_record(json: &str) -> LlmProviderStoreResult<LlmProviderRecord> {
    serde_json::from_str(json).map_err(codec_error)
}

fn decode_thread_lifecycle_record(
    json: &str,
) -> MetadataStoreResult<verlet_runtime_contracts::ThreadLifecycleRecord> {
    serde_json::from_str(json).map_err(metadata_codec_error)
}

fn now_ms_u64() -> MetadataStoreResult<u64> {
    u64::try_from(verlet_history::now_ms()).map_err(|err| {
        MetadataStoreError::Storage(format!("current timestamp cannot fit u64: {err}"))
    })
}

fn sqlite_timestamp(value: u64) -> MetadataStoreResult<i64> {
    i64::try_from(value).map_err(|err| {
        MetadataStoreError::Storage(format!(
            "timestamp {value} cannot fit sqlite integer: {err}"
        ))
    })
}

fn validate_provider_id(provider_id: &str) -> LlmProviderStoreResult<()> {
    if provider_id.is_empty() {
        Err(LlmProviderStoreError::EmptyProviderId)
    } else {
        Ok(())
    }
}

fn validate_model_id(model_id: &str) -> LlmProviderStoreResult<()> {
    if model_id.is_empty() {
        Err(LlmProviderStoreError::EmptyModelId)
    } else {
        Ok(())
    }
}

fn restrict_dir_permissions(path: &std::path::Path) -> LlmProviderStoreResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .map_err(storage_error)?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

fn restrict_file_permissions(path: &std::path::Path) -> LlmProviderStoreResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .map_err(storage_error)?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

fn storage_error(err: impl std::fmt::Display) -> LlmProviderStoreError {
    LlmProviderStoreError::Storage(err.to_string())
}

fn codec_error(err: impl std::fmt::Display) -> LlmProviderStoreError {
    LlmProviderStoreError::Codec(err.to_string())
}

fn metadata_storage_error(err: impl std::fmt::Display) -> MetadataStoreError {
    MetadataStoreError::Storage(err.to_string())
}

fn metadata_codec_error(err: impl std::fmt::Display) -> MetadataStoreError {
    MetadataStoreError::Codec(err.to_string())
}

#[cfg(test)]
mod tests;
