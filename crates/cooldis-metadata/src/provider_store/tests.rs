use super::*;
use crate::SecretResolver;
use cooldis_runtime_contracts::{
    ThreadContext, ThreadCoordinates, ThreadLifecycleRecord, ThreadLifecycleStatus, ThreadTopology,
};
use uuid::Uuid;

const RUSQLITE_METADATA_V1: &[u8] =
    include_bytes!("../../tests/fixtures/rusqlite-metadata-v1.sqlite3");

#[test]
fn synchronous_store_boundary_is_reentrant_from_futures_executor() {
    futures_executor::block_on(async {
        let store = SqliteLlmProviderStore::in_memory().unwrap();
        store
            .upsert_provider(LlmProviderRecord::new(
                "nested-executor",
                ProviderApi::OpenAIChatCompletions,
                "https://nested.example.invalid/v1",
            ))
            .unwrap();
        assert!(store.get_provider("nested-executor").unwrap().is_some());
    });
}

#[test]
fn upserts_restore_record_json_created_at_from_the_atomic_column() {
    let db_path = temp_db_path("cooldis-created-at-atomicity");
    remove_sqlite_files(&db_path);

    let store = SqliteMetadataStore::open(&db_path).unwrap();
    let mut provider = LlmProviderRecord::new(
        "atomic-provider",
        ProviderApi::OpenAIChatCompletions,
        "https://atomic.example.invalid/v1",
    );
    provider.created_at_ms = 1_700_000_000_100;
    store.upsert_provider(provider.clone()).unwrap();

    let coordinates = ThreadCoordinates::new("tenant-atomic", "user-atomic", "session-atomic");
    let context = ThreadContext::root(coordinates.clone());
    let mut lifecycle =
        ThreadLifecycleRecord::new(&context, ThreadLifecycleStatus::Idle, BTreeMap::new());
    lifecycle.created_at_ms = 1_700_000_000_200;
    store.upsert_thread_lifecycle(lifecycle.clone()).unwrap();
    drop(store);

    futures_executor::block_on(async {
        let db = cooldis_sqlite::Db::open(&db_path, cooldis_sqlite::DbConfig::default())
            .await
            .unwrap();
        let conn = db.connect().await.unwrap();
        conn.execute(
            "UPDATE llm_provider_records
             SET record_json = json_set(record_json, '$.created_at_ms', 1700000000998)
             WHERE provider_id = 'atomic-provider'",
            (),
        )
        .await
        .unwrap();
        conn.execute(
            "UPDATE thread_lifecycle_records
             SET record_json = json_set(record_json, '$.created_at_ms', 1700000000999)
             WHERE thread_id = ?1",
            cooldis_sqlite::params![coordinates.thread_id.to_string()],
        )
        .await
        .unwrap();
    });

    provider.created_at_ms = 1_700_000_000_300;
    lifecycle.created_at_ms = 1_700_000_000_400;
    let store = SqliteMetadataStore::open(&db_path).unwrap();
    store.upsert_provider(provider).unwrap();
    store.upsert_thread_lifecycle(lifecycle).unwrap();
    drop(store);

    futures_executor::block_on(async {
        let db = cooldis_sqlite::Db::open(&db_path, cooldis_sqlite::DbConfig::default())
            .await
            .unwrap();
        let conn = db.connect().await.unwrap();
        let mut provider_rows = conn
            .query(
                "SELECT created_at_ms, record_json FROM llm_provider_records
                 WHERE provider_id = 'atomic-provider'",
                (),
            )
            .await
            .unwrap();
        let provider_row = provider_rows.next().await.unwrap().unwrap();
        let provider_column = provider_row.get::<i64>(0).unwrap();
        let provider_json: serde_json::Value =
            serde_json::from_str(&provider_row.get::<String>(1).unwrap()).unwrap();
        assert_eq!(provider_column, 1_700_000_000_100);
        assert_eq!(
            provider_json["created_at_ms"].as_i64(),
            Some(provider_column)
        );

        let mut lifecycle_rows = conn
            .query(
                "SELECT created_at_ms, record_json FROM thread_lifecycle_records
                 WHERE thread_id = ?1",
                cooldis_sqlite::params![coordinates.thread_id.to_string()],
            )
            .await
            .unwrap();
        let lifecycle_row = lifecycle_rows.next().await.unwrap().unwrap();
        let lifecycle_column = lifecycle_row.get::<i64>(0).unwrap();
        let lifecycle_json: serde_json::Value =
            serde_json::from_str(&lifecycle_row.get::<String>(1).unwrap()).unwrap();
        assert_eq!(lifecycle_column, 1_700_000_000_200);
        assert_eq!(
            lifecycle_json["created_at_ms"].as_i64(),
            Some(lifecycle_column)
        );
    });

    remove_sqlite_files(&db_path);
}

#[test]
fn turso_decodes_rusqlite_created_metadata_v1_fixture() {
    let db_path = temp_db_path("cooldis-rusqlite-decode-compat");
    remove_sqlite_files(&db_path);
    std::fs::write(&db_path, RUSQLITE_METADATA_V1).unwrap();

    let provider_store = SqliteLlmProviderStore::open(&db_path).unwrap();
    let provider = provider_store
        .get_provider("legacy-provider")
        .unwrap()
        .expect("legacy provider record should decode");
    assert_eq!(provider.base_url, "https://legacy.example.invalid/v1");
    assert_eq!(
        provider_store.get_credential("legacy-provider").unwrap(),
        Some(LlmProviderCredential::ApiKey {
            key: "legacy-provider-key".to_string(),
        })
    );
    drop(provider_store);

    let secret_store = crate::SqliteSecretStore::open(&db_path).unwrap();
    let secret = secret_store
        .resolve_secret("LEGACY_SECRET")
        .unwrap()
        .expect("legacy secret record should decode");
    assert_eq!(secret.value, "legacy-secret-value");
    assert_eq!(secret.source_kind, crate::SecretSourceKind::Local);
    drop(secret_store);

    remove_sqlite_files(&db_path);
}

#[test]
fn provider_catalog_and_auth_persist_across_reopen() {
    let db_path = temp_db_path("cooldis-provider-store");
    remove_sqlite_files(&db_path);

    let store = SqliteLlmProviderStore::open(&db_path).unwrap();
    let provider = LlmProviderRecord::new(
        "openai_compatible",
        ProviderApi::OpenAIChatCompletions,
        "https://api.example.invalid/v1",
    )
    .with_display_name("OpenAI Compatible")
    .with_auth_header(true)
    .with_model(
        LlmProviderModelRecord::new("example-chat-model-large")
            .with_display_name("Example Chat Model Large")
            .with_context_window_tokens(128_000)
            .with_max_output_tokens(8192)
            .with_input_modality(LlmProviderInputModality::Text),
    );
    store.upsert_provider(provider).unwrap();
    store
        .set_credential(
            "openai_compatible",
            LlmProviderCredential::ApiKey {
                key: "stored-openai_compatible-key".to_string(),
            },
        )
        .unwrap();
    drop(store);

    let reopened = SqliteLlmProviderStore::open(&db_path).unwrap();
    let provider = reopened.get_provider("openai_compatible").unwrap().unwrap();
    assert_eq!(provider.provider_id, "openai_compatible");
    assert_eq!(provider.models[0].model_id, "example-chat-model-large");

    let resolved = resolve_llm_provider_auth(&reopened, &provider, &LlmProviderAuthContext::new())
        .unwrap()
        .unwrap();
    assert_eq!(resolved.api_key, "stored-openai_compatible-key");
    assert_eq!(resolved.source, LlmProviderAuthSourceKind::Stored);

    remove_sqlite_files(&db_path);
}

#[test]
fn metadata_store_persists_provider_auth_and_thread_topology_in_one_db() {
    let db_path = temp_db_path("cooldis-metadata-store");
    remove_sqlite_files(&db_path);

    let store = SqliteMetadataStore::open(&db_path).unwrap();
    seed_default_llm_providers(&store).unwrap();
    store
        .set_credential(
            OPENAI_COMPATIBLE_PROVIDER_ID,
            LlmProviderCredential::ApiKey {
                key: "stored-openai_compatible-key".to_string(),
            },
        )
        .unwrap();

    let parent_coordinates = ThreadCoordinates::new("tenant-a", "user-1", "session-1");
    let child_coordinates = ThreadCoordinates::new("tenant-a", "user-1", "session-1");
    let child_context = ThreadContext::with_topology(
        child_coordinates.clone(),
        ThreadTopology::spawned_from(parent_coordinates.thread_id),
    );
    let mut metadata = BTreeMap::new();
    metadata.insert("purpose".to_string(), "topology-smoke".to_string());
    let child_record =
        ThreadLifecycleRecord::new(&child_context, ThreadLifecycleStatus::Idle, metadata);
    store.upsert_thread_lifecycle(child_record.clone()).unwrap();
    let sibling_coordinates = ThreadCoordinates::new("tenant-a", "user-1", "session-2");
    let sibling_context = ThreadContext::root(sibling_coordinates.clone());
    let sibling_record = ThreadLifecycleRecord::new(
        &sibling_context,
        ThreadLifecycleStatus::Idle,
        BTreeMap::new(),
    );
    store.upsert_thread_lifecycle(sibling_record).unwrap();
    drop(store);

    let reopened = SqliteMetadataStore::open(&db_path).unwrap();
    let provider = reopened
        .get_provider(OPENAI_COMPATIBLE_PROVIDER_ID)
        .unwrap()
        .expect("provider catalog should share the metadata db");
    let resolved = resolve_llm_provider_auth(&reopened, &provider, &LlmProviderAuthContext::new())
        .unwrap()
        .unwrap();
    assert_eq!(resolved.api_key, "stored-openai_compatible-key");

    let stored_child = reopened
        .get_thread_lifecycle(child_coordinates.thread_id)
        .unwrap()
        .expect("thread topology should survive reopening the metadata db");
    assert_eq!(stored_child.coordinates, child_coordinates);
    assert_eq!(
        stored_child.parent_thread_id,
        Some(parent_coordinates.thread_id)
    );
    assert_eq!(stored_child.topology, child_record.topology);
    assert_eq!(
        reopened
            .list_thread_lifecycle(&child_coordinates.scope())
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        reopened
            .list_thread_lifecycle_for_user("tenant-a", "user-1")
            .unwrap()
            .len(),
        2
    );

    remove_sqlite_files(&db_path);
}

#[test]
fn default_openai_compatible_provider_record_matches_runtime_connection_shape() {
    let provider = default_openai_compatible_llm_provider_record();

    assert_eq!(provider.provider_id, OPENAI_COMPATIBLE_PROVIDER_ID);
    assert_eq!(provider.api, ProviderApi::OpenAIChatCompletions);
    assert_eq!(provider.base_url, OPENAI_COMPATIBLE_BASE_URL);
    assert!(provider.auth_header);
    assert_eq!(
        provider.headers.get(OPENAI_COMPATIBLE_EXAMPLE_HEADER),
        Some(&LlmProviderConfigValue::literal("required"))
    );
    assert_eq!(provider.models.len(), 2);
    assert_eq!(provider.models[0].model_id, OPENAI_COMPATIBLE_DEFAULT_MODEL);
    assert_eq!(
        provider.models[0]
            .metadata
            .get("default")
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(
        provider.models[0].input_modalities,
        vec![LlmProviderInputModality::Text]
    );
    assert_eq!(provider.models[1].model_id, OPENAI_COMPATIBLE_ALT_MODEL);
    assert_eq!(
        provider.models[1].input_modalities,
        vec![LlmProviderInputModality::Text]
    );
}

#[test]
fn seeding_openai_compatible_prepopulates_catalog_and_resolves_environment_auth() {
    let store = SqliteLlmProviderStore::in_memory().unwrap();
    seed_openai_compatible_llm_provider(&store).unwrap();

    let provider = store
        .get_provider(OPENAI_COMPATIBLE_PROVIDER_ID)
        .unwrap()
        .unwrap();
    let status = llm_provider_auth_status(
        &store,
        &provider,
        &LlmProviderAuthContext::new()
            .with_env("OPENAI_COMPATIBLE_API_KEY", "env-openai_compatible-key"),
    )
    .unwrap();
    assert_eq!(
        status,
        LlmProviderAuthStatus::configured(
            LlmProviderAuthSourceKind::Environment,
            "OPENAI_COMPATIBLE_API_KEY",
        )
    );

    let resolved = resolve_llm_provider_auth(
        &store,
        &provider,
        &LlmProviderAuthContext::new().with_env(
            "COOLDIS_OPENAI_COMPATIBLE_API_KEY",
            "cooldis-openai_compatible-key",
        ),
    )
    .unwrap()
    .unwrap();
    assert_eq!(resolved.api_key, "cooldis-openai_compatible-key");
    assert_eq!(resolved.source, LlmProviderAuthSourceKind::Environment);
}

#[test]
fn seeding_default_providers_does_not_touch_stored_openai_compatible_credential() {
    let store = SqliteLlmProviderStore::in_memory().unwrap();
    store
        .set_credential(
            OPENAI_COMPATIBLE_PROVIDER_ID,
            LlmProviderCredential::ApiKey {
                key: "stored-openai_compatible-key".to_string(),
            },
        )
        .unwrap();

    seed_default_llm_providers(&store).unwrap();
    let provider = store
        .get_provider(OPENAI_COMPATIBLE_PROVIDER_ID)
        .unwrap()
        .unwrap();
    let resolved = resolve_llm_provider_auth(
        &store,
        &provider,
        &LlmProviderAuthContext::new()
            .with_env("OPENAI_COMPATIBLE_API_KEY", "env-openai_compatible-key"),
    )
    .unwrap()
    .unwrap();

    assert_eq!(resolved.api_key, "stored-openai_compatible-key");
    assert_eq!(resolved.source, LlmProviderAuthSourceKind::Stored);
}

#[test]
fn runtime_override_wins_without_being_persisted() {
    let store = SqliteLlmProviderStore::in_memory().unwrap();
    let provider = LlmProviderRecord::new(
        "openai",
        ProviderApi::OpenAIResponses,
        "https://api.openai.com/v1",
    );
    store.upsert_provider(provider.clone()).unwrap();
    store
        .set_credential(
            "openai",
            LlmProviderCredential::ApiKey {
                key: "stored-key".to_string(),
            },
        )
        .unwrap();

    let resolved = resolve_llm_provider_auth(
        &store,
        &provider,
        &LlmProviderAuthContext::new().with_runtime_api_key("openai", "runtime-key"),
    )
    .unwrap()
    .unwrap();
    assert_eq!(resolved.api_key, "runtime-key");
    assert_eq!(resolved.source, LlmProviderAuthSourceKind::Runtime);

    let resolved = resolve_llm_provider_auth(&store, &provider, &LlmProviderAuthContext::new())
        .unwrap()
        .unwrap();
    assert_eq!(resolved.api_key, "stored-key");
    assert_eq!(resolved.source, LlmProviderAuthSourceKind::Stored);
}

#[test]
fn environment_and_catalog_auth_are_fallbacks_after_stored_auth() {
    let store = SqliteLlmProviderStore::in_memory().unwrap();
    let provider = LlmProviderRecord::new(
        "custom-proxy",
        ProviderApi::OpenAIChatCompletions,
        "https://proxy.example/v1",
    )
    .with_auth(LlmProviderAuthConfig::Env {
        name: "CUSTOM_PROXY_KEY".to_string(),
    });

    let resolved = resolve_llm_provider_auth(
        &store,
        &provider,
        &LlmProviderAuthContext::new().with_env("CUSTOM_PROXY_KEY", "env-key"),
    )
    .unwrap()
    .unwrap();
    assert_eq!(resolved.api_key, "env-key");
    assert_eq!(resolved.source, LlmProviderAuthSourceKind::Environment);

    store
        .set_credential(
            "custom-proxy",
            LlmProviderCredential::ApiKey {
                key: "stored-key".to_string(),
            },
        )
        .unwrap();
    let resolved = resolve_llm_provider_auth(
        &store,
        &provider,
        &LlmProviderAuthContext::new().with_env("CUSTOM_PROXY_KEY", "env-key"),
    )
    .unwrap()
    .unwrap();
    assert_eq!(resolved.api_key, "stored-key");
    assert_eq!(resolved.source, LlmProviderAuthSourceKind::Stored);
}

#[test]
fn auth_status_redacts_values() {
    let store = SqliteLlmProviderStore::in_memory().unwrap();
    let provider = LlmProviderRecord::new(
        "anthropic",
        ProviderApi::AnthropicMessages,
        "https://api.anthropic.com/v1",
    );
    let context = LlmProviderAuthContext::new().with_env("ANTHROPIC_API_KEY", "secret");
    let status = llm_provider_auth_status(&store, &provider, &context).unwrap();
    assert_eq!(
        status,
        LlmProviderAuthStatus::configured(
            LlmProviderAuthSourceKind::Environment,
            "ANTHROPIC_API_KEY",
        )
    );
    let json = serde_json::to_string(&status).unwrap();
    assert!(!json.contains("secret"));
}

#[test]
fn command_auth_is_visible_but_not_executed_by_default() {
    let store = SqliteLlmProviderStore::in_memory().unwrap();
    let provider = LlmProviderRecord::new(
        "onepassword-backed",
        ProviderApi::OpenAIChatCompletions,
        "https://proxy.example/v1",
    )
    .with_auth(LlmProviderAuthConfig::Command {
        command: "op read op://vault/item/key".to_string(),
    });
    let status =
        llm_provider_auth_status(&store, &provider, &LlmProviderAuthContext::new()).unwrap();
    assert_eq!(
        status.source,
        Some(LlmProviderAuthSourceKind::CatalogCommand)
    );
    assert!(matches!(
        resolve_llm_provider_auth(&store, &provider, &LlmProviderAuthContext::new()),
        Err(LlmProviderStoreError::CommandAuthUnsupported { .. })
    ));
}

fn temp_db_path(prefix: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("{prefix}-{}.sqlite3", Uuid::now_v7().simple()))
}

fn remove_sqlite_files(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
}
