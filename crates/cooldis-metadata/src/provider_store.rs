use cooldis_history::{ProviderApi, now_ms};
use cooldis_runtime_contracts::{
    ThreadId, ThreadLifecycleRecord, ThreadLifecycleStatus, ThreadScope,
};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use thiserror::Error;

pub const OPENAI_COMPATIBLE_PROVIDER_ID: &str = "openai_compatible";
pub const OPENAI_COMPATIBLE_BASE_URL: &str = "https://api.example.invalid/v1";
pub const OPENAI_COMPATIBLE_DEFAULT_MODEL: &str = "example-chat-model";
pub const OPENAI_COMPATIBLE_ALT_MODEL: &str = "example-chat-model-large";
pub const OPENAI_COMPATIBLE_EXAMPLE_HEADER: &str = "X-Example-Provider";

pub type LlmProviderStoreResult<T> = Result<T, LlmProviderStoreError>;

pub type MetadataStoreResult<T> = Result<T, MetadataStoreError>;

#[derive(Debug, Error)]
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

#[derive(Debug, Error)]
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LlmProviderRecord {
    pub provider_id: String,
    pub api: ProviderApi,
    pub base_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default)]
    pub auth: LlmProviderAuthConfig,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, LlmProviderConfigValue>,
    #[serde(default)]
    pub auth_header: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<LlmProviderModelRecord>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl LlmProviderRecord {
    pub fn new(
        provider_id: impl Into<String>,
        api: ProviderApi,
        base_url: impl Into<String>,
    ) -> Self {
        let now = now_ms();
        Self {
            provider_id: provider_id.into(),
            api,
            base_url: base_url.into(),
            display_name: None,
            auth: LlmProviderAuthConfig::default(),
            headers: BTreeMap::new(),
            auth_header: false,
            models: Vec::new(),
            metadata: BTreeMap::new(),
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LlmProviderModelRecord {
    pub model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api: Option<ProviderApi>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_modalities: Vec<LlmProviderInputModality>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, LlmProviderConfigValue>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
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
            headers: BTreeMap::new(),
            metadata: BTreeMap::new(),
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LlmProviderInputModality {
    Text,
    Image,
    Audio,
    File,
    Other(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LlmProviderCredential {
    ApiKey {
        key: String,
    },
    OAuth {
        access: String,
        refresh: String,
        expires_at_ms: i64,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct LlmProviderAuthContext {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub runtime_api_keys: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub environment: BTreeMap<String, String>,
}

impl LlmProviderAuthContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_process_env() -> Self {
        Self {
            runtime_api_keys: BTreeMap::new(),
            environment: std::env::vars().collect(),
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LlmProviderAuthSourceKind {
    Runtime,
    Stored,
    Environment,
    CatalogInline,
    CatalogCommand,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LlmProviderResolvedAuth {
    pub api_key: String,
    pub source: LlmProviderAuthSourceKind,
}

pub trait LlmProviderCatalogStore: Send + Sync {
    fn upsert_provider(&self, record: LlmProviderRecord) -> LlmProviderStoreResult<()>;
    fn get_provider(&self, provider_id: &str) -> LlmProviderStoreResult<Option<LlmProviderRecord>>;
    fn list_providers(&self) -> LlmProviderStoreResult<Vec<LlmProviderRecord>>;
    fn delete_provider(&self, provider_id: &str) -> LlmProviderStoreResult<()>;
}

pub trait LlmProviderAuthStore: Send + Sync {
    fn set_credential(
        &self,
        provider_id: &str,
        credential: LlmProviderCredential,
    ) -> LlmProviderStoreResult<()>;
    fn get_credential(
        &self,
        provider_id: &str,
    ) -> LlmProviderStoreResult<Option<LlmProviderCredential>>;
    fn delete_credential(&self, provider_id: &str) -> LlmProviderStoreResult<()>;
}

pub trait ThreadMetadataStore: Send + Sync {
    fn upsert_thread_lifecycle(&self, record: ThreadLifecycleRecord) -> MetadataStoreResult<()>;
    fn get_thread_lifecycle(
        &self,
        thread_id: ThreadId,
    ) -> MetadataStoreResult<Option<ThreadLifecycleRecord>>;
    fn list_thread_lifecycle(
        &self,
        scope: &ThreadScope,
    ) -> MetadataStoreResult<Vec<ThreadLifecycleRecord>>;
    fn list_thread_lifecycle_for_user(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> MetadataStoreResult<Vec<ThreadLifecycleRecord>>;
}

pub fn default_openai_compatible_llm_provider_record() -> LlmProviderRecord {
    LlmProviderRecord::new(
        OPENAI_COMPATIBLE_PROVIDER_ID,
        ProviderApi::OpenAIChatCompletions,
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
        "OPENAI_COMPATIBLE_API_KEY,COOLDIS_OPENAI_COMPATIBLE_API_KEY",
    )
}

pub fn seed_openai_compatible_llm_provider(
    store: &dyn LlmProviderCatalogStore,
) -> LlmProviderStoreResult<()> {
    store.upsert_provider(default_openai_compatible_llm_provider_record())
}

pub fn seed_default_llm_providers(
    store: &dyn LlmProviderCatalogStore,
) -> LlmProviderStoreResult<()> {
    seed_openai_compatible_llm_provider(store)
}

#[derive(Clone)]
pub struct SqliteLlmProviderStore {
    inner: Arc<Mutex<rusqlite::Connection>>,
}

impl SqliteLlmProviderStore {
    pub fn open(path: impl AsRef<Path>) -> LlmProviderStoreResult<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).map_err(storage_error)?;
                restrict_dir_permissions(parent)?;
            }
        }
        let connection = rusqlite::Connection::open(path).map_err(storage_error)?;
        restrict_file_permissions(path)?;
        Self::from_connection(connection)
    }

    pub fn in_memory() -> LlmProviderStoreResult<Self> {
        let connection = rusqlite::Connection::open_in_memory().map_err(storage_error)?;
        Self::from_connection(connection)
    }

    fn from_connection(connection: rusqlite::Connection) -> LlmProviderStoreResult<Self> {
        init_provider_store_schema(&connection)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(connection)),
        })
    }

    fn lock_connection(
        &self,
    ) -> LlmProviderStoreResult<std::sync::MutexGuard<'_, rusqlite::Connection>> {
        self.inner.lock().map_err(|err| {
            LlmProviderStoreError::Storage(format!("sqlite connection lock poisoned: {err}"))
        })
    }
}

#[derive(Clone)]
pub struct SqliteMetadataStore {
    provider_store: SqliteLlmProviderStore,
}

impl SqliteMetadataStore {
    pub fn open(path: impl AsRef<Path>) -> MetadataStoreResult<Self> {
        let provider_store =
            SqliteLlmProviderStore::open(path).map_err(MetadataStoreError::from)?;
        let store = Self { provider_store };
        store.init_metadata_schema()?;
        Ok(store)
    }

    pub fn in_memory() -> MetadataStoreResult<Self> {
        let provider_store =
            SqliteLlmProviderStore::in_memory().map_err(MetadataStoreError::from)?;
        let store = Self { provider_store };
        store.init_metadata_schema()?;
        Ok(store)
    }

    pub fn llm_provider_store(&self) -> &SqliteLlmProviderStore {
        &self.provider_store
    }

    fn init_metadata_schema(&self) -> MetadataStoreResult<()> {
        let connection = self.lock_connection()?;
        init_thread_metadata_schema(&connection)
    }

    fn lock_connection(
        &self,
    ) -> MetadataStoreResult<std::sync::MutexGuard<'_, rusqlite::Connection>> {
        self.provider_store
            .lock_connection()
            .map_err(MetadataStoreError::from)
    }
}

impl LlmProviderCatalogStore for SqliteMetadataStore {
    fn upsert_provider(&self, record: LlmProviderRecord) -> LlmProviderStoreResult<()> {
        self.provider_store.upsert_provider(record)
    }

    fn get_provider(&self, provider_id: &str) -> LlmProviderStoreResult<Option<LlmProviderRecord>> {
        self.provider_store.get_provider(provider_id)
    }

    fn list_providers(&self) -> LlmProviderStoreResult<Vec<LlmProviderRecord>> {
        self.provider_store.list_providers()
    }

    fn delete_provider(&self, provider_id: &str) -> LlmProviderStoreResult<()> {
        self.provider_store.delete_provider(provider_id)
    }
}

impl LlmProviderAuthStore for SqliteMetadataStore {
    fn set_credential(
        &self,
        provider_id: &str,
        credential: LlmProviderCredential,
    ) -> LlmProviderStoreResult<()> {
        self.provider_store.set_credential(provider_id, credential)
    }

    fn get_credential(
        &self,
        provider_id: &str,
    ) -> LlmProviderStoreResult<Option<LlmProviderCredential>> {
        self.provider_store.get_credential(provider_id)
    }

    fn delete_credential(&self, provider_id: &str) -> LlmProviderStoreResult<()> {
        self.provider_store.delete_credential(provider_id)
    }
}

impl ThreadMetadataStore for SqliteMetadataStore {
    fn upsert_thread_lifecycle(
        &self,
        mut record: ThreadLifecycleRecord,
    ) -> MetadataStoreResult<()> {
        let mut connection = self.lock_connection()?;
        let tx = connection.transaction().map_err(metadata_storage_error)?;
        let thread_id = record.coordinates.thread_id.to_string();
        let existing = sqlite_get_thread_lifecycle(&tx, record.coordinates.thread_id)?;
        if let Some(existing) = existing {
            record.created_at_ms = existing.created_at_ms;
        }
        record.updated_at_ms = now_ms_u64()?;
        let tenant_id = record.coordinates.tenant_id.clone();
        let user_id = record.coordinates.user_id.clone();
        let session_id = record.coordinates.session_id.clone();
        let parent_thread_id = record.parent_thread_id.map(|id| id.to_string());
        let status = thread_lifecycle_status_string(record.status);
        let record_json = serde_json::to_string(&record).map_err(metadata_codec_error)?;
        tx.execute(
            "INSERT INTO thread_lifecycle_records (
                thread_id,
                tenant_id,
                user_id,
                session_id,
                parent_thread_id,
                status,
                record_json,
                created_at_ms,
                updated_at_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            ON CONFLICT(thread_id) DO UPDATE SET
                tenant_id = excluded.tenant_id,
                user_id = excluded.user_id,
                session_id = excluded.session_id,
                parent_thread_id = excluded.parent_thread_id,
                status = excluded.status,
                record_json = excluded.record_json,
                updated_at_ms = excluded.updated_at_ms",
            params![
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
        .map_err(metadata_storage_error)?;
        tx.commit().map_err(metadata_storage_error)?;
        Ok(())
    }

    fn get_thread_lifecycle(
        &self,
        thread_id: ThreadId,
    ) -> MetadataStoreResult<Option<ThreadLifecycleRecord>> {
        let connection = self.lock_connection()?;
        sqlite_get_thread_lifecycle(&connection, thread_id)
    }

    fn list_thread_lifecycle(
        &self,
        scope: &ThreadScope,
    ) -> MetadataStoreResult<Vec<ThreadLifecycleRecord>> {
        let connection = self.lock_connection()?;
        sqlite_list_thread_lifecycle(&connection, scope)
    }

    fn list_thread_lifecycle_for_user(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> MetadataStoreResult<Vec<ThreadLifecycleRecord>> {
        let connection = self.lock_connection()?;
        sqlite_list_thread_lifecycle_for_user(&connection, tenant_id, user_id)
    }
}

impl LlmProviderCatalogStore for SqliteLlmProviderStore {
    fn upsert_provider(&self, mut record: LlmProviderRecord) -> LlmProviderStoreResult<()> {
        record.validate()?;
        let mut connection = self.lock_connection()?;
        let tx = connection.transaction().map_err(storage_error)?;
        let existing = sqlite_get_provider(&tx, &record.provider_id)?;
        if let Some(existing) = existing {
            record.created_at_ms = existing.created_at_ms;
        }
        record.updated_at_ms = now_ms();
        let record_json = serde_json::to_string(&record).map_err(codec_error)?;
        tx.execute(
            "INSERT INTO llm_provider_records (
                provider_id,
                record_json,
                created_at_ms,
                updated_at_ms
            ) VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(provider_id) DO UPDATE SET
                record_json = excluded.record_json,
                updated_at_ms = excluded.updated_at_ms",
            params![
                record.provider_id,
                record_json,
                record.created_at_ms,
                record.updated_at_ms,
            ],
        )
        .map_err(storage_error)?;
        tx.commit().map_err(storage_error)?;
        Ok(())
    }

    fn get_provider(&self, provider_id: &str) -> LlmProviderStoreResult<Option<LlmProviderRecord>> {
        validate_provider_id(provider_id)?;
        let connection = self.lock_connection()?;
        sqlite_get_provider(&connection, provider_id)
    }

    fn list_providers(&self) -> LlmProviderStoreResult<Vec<LlmProviderRecord>> {
        let connection = self.lock_connection()?;
        let mut statement = connection
            .prepare("SELECT record_json FROM llm_provider_records ORDER BY provider_id")
            .map_err(storage_error)?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(storage_error)?;
        let mut records = Vec::new();
        for row in rows {
            let json = row.map_err(storage_error)?;
            records.push(decode_provider_record(&json)?);
        }
        Ok(records)
    }

    fn delete_provider(&self, provider_id: &str) -> LlmProviderStoreResult<()> {
        validate_provider_id(provider_id)?;
        let connection = self.lock_connection()?;
        connection
            .execute(
                "DELETE FROM llm_provider_records WHERE provider_id = ?1",
                params![provider_id],
            )
            .map_err(storage_error)?;
        Ok(())
    }
}

impl LlmProviderAuthStore for SqliteLlmProviderStore {
    fn set_credential(
        &self,
        provider_id: &str,
        credential: LlmProviderCredential,
    ) -> LlmProviderStoreResult<()> {
        validate_provider_id(provider_id)?;
        let connection = self.lock_connection()?;
        let credential_json = serde_json::to_string(&credential).map_err(codec_error)?;
        let now = now_ms();
        connection
            .execute(
                "INSERT INTO llm_provider_credentials (
                    provider_id,
                    credential_json,
                    updated_at_ms
                ) VALUES (?1, ?2, ?3)
                ON CONFLICT(provider_id) DO UPDATE SET
                    credential_json = excluded.credential_json,
                    updated_at_ms = excluded.updated_at_ms",
                params![provider_id, credential_json, now],
            )
            .map_err(storage_error)?;
        Ok(())
    }

    fn get_credential(
        &self,
        provider_id: &str,
    ) -> LlmProviderStoreResult<Option<LlmProviderCredential>> {
        validate_provider_id(provider_id)?;
        let connection = self.lock_connection()?;
        sqlite_get_credential(&connection, provider_id)
    }

    fn delete_credential(&self, provider_id: &str) -> LlmProviderStoreResult<()> {
        validate_provider_id(provider_id)?;
        let connection = self.lock_connection()?;
        connection
            .execute(
                "DELETE FROM llm_provider_credentials WHERE provider_id = ?1",
                params![provider_id],
            )
            .map_err(storage_error)?;
        Ok(())
    }
}

pub fn resolve_llm_provider_auth(
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

    if let Some(credential) = auth_store.get_credential(&provider.provider_id)? {
        return credential_to_resolved_auth(&provider.provider_id, credential)
            .map(|api_key| Some((api_key, LlmProviderAuthSourceKind::Stored).into()));
    }

    for env_name in provider_env_candidates(provider) {
        if let Some(key) = context.environment.get(&env_name)
            && !key.is_empty()
        {
            return Ok(Some(LlmProviderResolvedAuth {
                api_key: key.clone(),
                source: LlmProviderAuthSourceKind::Environment,
            }));
        }
    }

    match &provider.auth {
        LlmProviderAuthConfig::StoredOrEnvironment | LlmProviderAuthConfig::None => Ok(None),
        LlmProviderAuthConfig::Env { name } => Ok(context
            .environment
            .get(name)
            .filter(|key| !key.is_empty())
            .map(|key| LlmProviderResolvedAuth {
                api_key: key.clone(),
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

pub fn llm_provider_auth_status(
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

    if auth_store.get_credential(&provider.provider_id)?.is_some() {
        return Ok(LlmProviderAuthStatus::configured(
            LlmProviderAuthSourceKind::Stored,
            "stored credential",
        ));
    }

    for env_name in provider_env_candidates(provider) {
        if context
            .environment
            .get(&env_name)
            .is_some_and(|key| !key.is_empty())
        {
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
            if context
                .environment
                .get(name)
                .is_some_and(|key| !key.is_empty())
            {
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
            if expires_at_ms <= now_ms() {
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
                "COOLDIS_OPENAI_COMPATIBLE_API_KEY".to_string(),
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

fn init_provider_store_schema(connection: &rusqlite::Connection) -> LlmProviderStoreResult<()> {
    connection
        .execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;

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
        .map_err(storage_error)
}

fn init_thread_metadata_schema(connection: &rusqlite::Connection) -> MetadataStoreResult<()> {
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
        .map_err(metadata_storage_error)
}

fn sqlite_get_provider(
    connection: &rusqlite::Connection,
    provider_id: &str,
) -> LlmProviderStoreResult<Option<LlmProviderRecord>> {
    let record_json = connection
        .query_row(
            "SELECT record_json FROM llm_provider_records WHERE provider_id = ?1",
            params![provider_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(storage_error)?;
    record_json
        .map(|json| decode_provider_record(&json))
        .transpose()
}

fn sqlite_get_credential(
    connection: &rusqlite::Connection,
    provider_id: &str,
) -> LlmProviderStoreResult<Option<LlmProviderCredential>> {
    let credential_json = connection
        .query_row(
            "SELECT credential_json FROM llm_provider_credentials WHERE provider_id = ?1",
            params![provider_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(storage_error)?;
    credential_json
        .map(|json| serde_json::from_str(&json).map_err(codec_error))
        .transpose()
}

fn sqlite_get_thread_lifecycle(
    connection: &rusqlite::Connection,
    thread_id: ThreadId,
) -> MetadataStoreResult<Option<ThreadLifecycleRecord>> {
    let record_json = connection
        .query_row(
            "SELECT record_json FROM thread_lifecycle_records WHERE thread_id = ?1",
            params![thread_id.to_string()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(metadata_storage_error)?;
    record_json
        .map(|json| decode_thread_lifecycle_record(&json))
        .transpose()
}

fn sqlite_list_thread_lifecycle(
    connection: &rusqlite::Connection,
    scope: &ThreadScope,
) -> MetadataStoreResult<Vec<ThreadLifecycleRecord>> {
    let mut statement = connection
        .prepare(
            "SELECT record_json FROM thread_lifecycle_records
            WHERE tenant_id = ?1 AND user_id = ?2 AND session_id = ?3
            ORDER BY created_at_ms, thread_id",
        )
        .map_err(metadata_storage_error)?;
    let rows = statement
        .query_map(
            params![&scope.tenant_id, &scope.user_id, &scope.session_id],
            |row| row.get::<_, String>(0),
        )
        .map_err(metadata_storage_error)?;
    let mut records = Vec::new();
    for row in rows {
        let json = row.map_err(metadata_storage_error)?;
        records.push(decode_thread_lifecycle_record(&json)?);
    }
    Ok(records)
}

fn sqlite_list_thread_lifecycle_for_user(
    connection: &rusqlite::Connection,
    tenant_id: &str,
    user_id: &str,
) -> MetadataStoreResult<Vec<ThreadLifecycleRecord>> {
    let mut statement = connection
        .prepare(
            "SELECT record_json FROM thread_lifecycle_records
            WHERE tenant_id = ?1 AND user_id = ?2
            ORDER BY created_at_ms, session_id, thread_id",
        )
        .map_err(metadata_storage_error)?;
    let rows = statement
        .query_map(params![tenant_id, user_id], |row| row.get::<_, String>(0))
        .map_err(metadata_storage_error)?;
    let mut records = Vec::new();
    for row in rows {
        let json = row.map_err(metadata_storage_error)?;
        records.push(decode_thread_lifecycle_record(&json)?);
    }
    Ok(records)
}

fn decode_provider_record(json: &str) -> LlmProviderStoreResult<LlmProviderRecord> {
    serde_json::from_str(json).map_err(codec_error)
}

fn decode_thread_lifecycle_record(json: &str) -> MetadataStoreResult<ThreadLifecycleRecord> {
    serde_json::from_str(json).map_err(metadata_codec_error)
}

fn thread_lifecycle_status_string(status: ThreadLifecycleStatus) -> &'static str {
    match status {
        ThreadLifecycleStatus::Starting => "starting",
        ThreadLifecycleStatus::Idle => "idle",
        ThreadLifecycleStatus::Running => "running",
        ThreadLifecycleStatus::Cancelling => "cancelling",
        ThreadLifecycleStatus::Stopped => "stopped",
        ThreadLifecycleStatus::Failed => "failed",
    }
}

fn now_ms_u64() -> MetadataStoreResult<u64> {
    u64::try_from(now_ms()).map_err(|err| {
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

fn restrict_dir_permissions(path: &Path) -> LlmProviderStoreResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .map_err(storage_error)?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

fn restrict_file_permissions(path: &Path) -> LlmProviderStoreResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
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
