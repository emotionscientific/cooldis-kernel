use super::*;
use crate::SecretResolver;
use cooldis_runtime_contracts::{
    ThreadContext, ThreadCoordinates, ThreadLifecycleRecord, ThreadLifecycleStatus, ThreadTopology,
};
use uuid::Uuid;

const RUSQLITE_METADATA_V1: &[u8] =
    include_bytes!("../../tests/fixtures/rusqlite-metadata-v1.sqlite3");

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_schema_task_finishes_and_releases_the_write_lock() {
    let db_path = temp_db_path("cooldis-metadata-cancelled-schema-task");
    remove_sqlite_files(&db_path);
    let db = cooldis_sqlite::Db::open(&db_path, cooldis_sqlite::DbConfig::default())
        .await
        .unwrap();
    db.connect()
        .await
        .unwrap()
        .execute("CREATE TABLE cancellation_probe (value TEXT NOT NULL)", ())
        .await
        .unwrap();

    let (transaction_started_tx, transaction_started_rx) = tokio::sync::oneshot::channel();
    let (finish_transaction_tx, finish_transaction_rx) = tokio::sync::oneshot::channel();
    let task_db = db.clone();
    let schema_task = tokio::spawn(async move {
        provider_cancellation_safe(async move {
            let mut connection = task_db.connect().await.map_err(storage_error)?;
            let transaction = connection
                .transaction_with_behavior(cooldis_sqlite::TransactionBehavior::Immediate)
                .await
                .map_err(storage_error)?;
            transaction
                .execute(
                    "INSERT INTO cancellation_probe (value) VALUES ('committed')",
                    (),
                )
                .await
                .map_err(storage_error)?;
            transaction_started_tx.send(()).unwrap();
            finish_transaction_rx.await.unwrap();
            transaction.commit().await.map_err(storage_error)?;
            Ok(())
        })
        .await
    });

    transaction_started_rx.await.unwrap();
    schema_task.abort();
    assert!(schema_task.await.unwrap_err().is_cancelled());
    finish_transaction_tx.send(()).unwrap();

    let probe_db = cooldis_sqlite::Db::open(
        &db_path,
        cooldis_sqlite::DbConfig {
            busy_timeout: std::time::Duration::ZERO,
            ..cooldis_sqlite::DbConfig::default()
        },
    )
    .await
    .unwrap();
    let mut released = false;
    for _ in 0..10_000 {
        let mut connection = probe_db.connect().await.unwrap();
        if let Ok(transaction) = connection
            .transaction_with_behavior(cooldis_sqlite::TransactionBehavior::Immediate)
            .await
        {
            transaction.rollback().await.unwrap();
            released = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(released, "cancelled schema task kept the write lock wedged");

    let connection = db.connect().await.unwrap();
    let mut rows = connection
        .query("SELECT value FROM cancellation_probe", ())
        .await
        .unwrap();
    assert_eq!(
        rows.next()
            .await
            .unwrap()
            .unwrap()
            .get::<String>(0)
            .unwrap(),
        "committed"
    );

    drop(rows);
    drop(connection);
    drop(probe_db);
    drop(db);
    remove_sqlite_files(&db_path);
}

#[tokio::test]
async fn async_store_boundary_runs_inside_an_executor() {
    let store = SqliteLlmProviderStore::in_memory().await.unwrap();
    store
        .upsert_provider(LlmProviderRecord::new(
            "nested-executor",
            ProviderApi::OpenAIChatCompletions,
            "https://nested.example.invalid/v1",
        ))
        .await
        .unwrap();
    assert!(
        store
            .get_provider("nested-executor")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn upserts_restore_record_json_created_at_from_the_atomic_column() {
    let db_path = temp_db_path("cooldis-created-at-atomicity");
    remove_sqlite_files(&db_path);

    let store = SqliteMetadataStore::open(&db_path).await.unwrap();
    let mut provider = LlmProviderRecord::new(
        "atomic-provider",
        ProviderApi::OpenAIChatCompletions,
        "https://atomic.example.invalid/v1",
    );
    provider.created_at_ms = 1_700_000_000_100;
    store.upsert_provider(provider.clone()).await.unwrap();

    let coordinates = ThreadCoordinates::new("tenant-atomic", "user-atomic", "session-atomic");
    let context = ThreadContext::root(coordinates.clone());
    let mut lifecycle =
        ThreadLifecycleRecord::new(&context, ThreadLifecycleStatus::Idle, BTreeMap::new());
    lifecycle.created_at_ms = 1_700_000_000_200;
    store
        .upsert_thread_lifecycle(lifecycle.clone())
        .await
        .unwrap();
    drop(store);

    {
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
    }

    provider.created_at_ms = 1_700_000_000_300;
    lifecycle.created_at_ms = 1_700_000_000_400;
    let store = SqliteMetadataStore::open(&db_path).await.unwrap();
    store.upsert_provider(provider).await.unwrap();
    store.upsert_thread_lifecycle(lifecycle).await.unwrap();
    drop(store);

    {
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
    }

    remove_sqlite_files(&db_path);
}

#[tokio::test]
async fn turso_decodes_rusqlite_created_metadata_v1_fixture() {
    let db_path = temp_db_path("cooldis-rusqlite-decode-compat");
    remove_sqlite_files(&db_path);
    std::fs::write(&db_path, RUSQLITE_METADATA_V1).unwrap();

    let provider_store = SqliteLlmProviderStore::open(&db_path).await.unwrap();
    let provider = provider_store
        .get_provider("legacy-provider")
        .await
        .unwrap()
        .expect("legacy provider record should decode");
    assert_eq!(provider.base_url, "https://legacy.example.invalid/v1");
    assert_eq!(
        provider_store
            .get_credential("legacy-provider")
            .await
            .unwrap(),
        Some(LlmProviderCredential::ApiKey {
            key: "legacy-provider-key".to_string(),
        })
    );
    drop(provider_store);

    let secret_store = crate::SqliteSecretStore::open(&db_path).await.unwrap();
    let secret = secret_store
        .resolve_secret("LEGACY_SECRET")
        .await
        .unwrap()
        .expect("legacy secret record should decode");
    assert_eq!(secret.value, "legacy-secret-value");
    assert_eq!(secret.source_kind, crate::SecretSourceKind::Local);
    drop(secret_store);

    remove_sqlite_files(&db_path);
}

#[tokio::test]
async fn provider_catalog_and_auth_persist_across_reopen() {
    let db_path = temp_db_path("cooldis-provider-store");
    remove_sqlite_files(&db_path);

    let store = SqliteLlmProviderStore::open(&db_path).await.unwrap();
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
    store.upsert_provider(provider).await.unwrap();
    store
        .set_credential(
            "openai_compatible",
            LlmProviderCredential::ApiKey {
                key: "stored-openai_compatible-key".to_string(),
            },
        )
        .await
        .unwrap();
    drop(store);

    let reopened = SqliteLlmProviderStore::open(&db_path).await.unwrap();
    let provider = reopened
        .get_provider("openai_compatible")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(provider.provider_id, "openai_compatible");
    assert_eq!(provider.models[0].model_id, "example-chat-model-large");

    let resolved = resolve_llm_provider_auth(&reopened, &provider, &LlmProviderAuthContext::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resolved.api_key, "stored-openai_compatible-key");
    assert_eq!(resolved.source, LlmProviderAuthSourceKind::Stored);

    remove_sqlite_files(&db_path);
}

#[tokio::test]
async fn metadata_store_persists_provider_auth_and_thread_topology_in_one_db() {
    let db_path = temp_db_path("cooldis-metadata-store");
    remove_sqlite_files(&db_path);

    let store = SqliteMetadataStore::open(&db_path).await.unwrap();
    seed_default_llm_providers(&store).await.unwrap();
    store
        .set_credential(
            OPENAI_COMPATIBLE_PROVIDER_ID,
            LlmProviderCredential::ApiKey {
                key: "stored-openai_compatible-key".to_string(),
            },
        )
        .await
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
    store
        .upsert_thread_lifecycle(child_record.clone())
        .await
        .unwrap();
    let sibling_coordinates = ThreadCoordinates::new("tenant-a", "user-1", "session-2");
    let sibling_context = ThreadContext::root(sibling_coordinates.clone());
    let sibling_record = ThreadLifecycleRecord::new(
        &sibling_context,
        ThreadLifecycleStatus::Idle,
        BTreeMap::new(),
    );
    store.upsert_thread_lifecycle(sibling_record).await.unwrap();
    drop(store);

    let reopened = SqliteMetadataStore::open(&db_path).await.unwrap();
    let provider = reopened
        .get_provider(OPENAI_COMPATIBLE_PROVIDER_ID)
        .await
        .unwrap()
        .expect("provider catalog should share the metadata db");
    let resolved = resolve_llm_provider_auth(&reopened, &provider, &LlmProviderAuthContext::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resolved.api_key, "stored-openai_compatible-key");

    let stored_child = reopened
        .get_thread_lifecycle(child_coordinates.thread_id)
        .await
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
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        reopened
            .list_thread_lifecycle_for_user("tenant-a", "user-1")
            .await
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

#[tokio::test]
async fn seeding_openai_compatible_prepopulates_catalog_and_resolves_environment_auth() {
    let store = SqliteLlmProviderStore::in_memory().await.unwrap();
    seed_openai_compatible_llm_provider(&store).await.unwrap();

    let provider = store
        .get_provider(OPENAI_COMPATIBLE_PROVIDER_ID)
        .await
        .unwrap()
        .unwrap();
    let status = llm_provider_auth_status(
        &store,
        &provider,
        &LlmProviderAuthContext::new()
            .with_env("OPENAI_COMPATIBLE_API_KEY", "env-openai_compatible-key"),
    )
    .await
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
    .await
    .unwrap()
    .unwrap();
    assert_eq!(resolved.api_key, "cooldis-openai_compatible-key");
    assert_eq!(resolved.source, LlmProviderAuthSourceKind::Environment);
}

#[tokio::test]
async fn seeding_default_providers_does_not_touch_stored_openai_compatible_credential() {
    let store = SqliteLlmProviderStore::in_memory().await.unwrap();
    store
        .set_credential(
            OPENAI_COMPATIBLE_PROVIDER_ID,
            LlmProviderCredential::ApiKey {
                key: "stored-openai_compatible-key".to_string(),
            },
        )
        .await
        .unwrap();

    seed_default_llm_providers(&store).await.unwrap();
    let provider = store
        .get_provider(OPENAI_COMPATIBLE_PROVIDER_ID)
        .await
        .unwrap()
        .unwrap();
    let resolved = resolve_llm_provider_auth(
        &store,
        &provider,
        &LlmProviderAuthContext::new()
            .with_env("OPENAI_COMPATIBLE_API_KEY", "env-openai_compatible-key"),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(resolved.api_key, "stored-openai_compatible-key");
    assert_eq!(resolved.source, LlmProviderAuthSourceKind::Stored);
}

#[tokio::test]
async fn runtime_override_wins_without_being_persisted() {
    let store = SqliteLlmProviderStore::in_memory().await.unwrap();
    let provider = LlmProviderRecord::new(
        "openai",
        ProviderApi::OpenAIResponses,
        "https://api.openai.com/v1",
    );
    store.upsert_provider(provider.clone()).await.unwrap();
    store
        .set_credential(
            "openai",
            LlmProviderCredential::ApiKey {
                key: "stored-key".to_string(),
            },
        )
        .await
        .unwrap();

    let resolved = resolve_llm_provider_auth(
        &store,
        &provider,
        &LlmProviderAuthContext::new().with_runtime_api_key("openai", "runtime-key"),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(resolved.api_key, "runtime-key");
    assert_eq!(resolved.source, LlmProviderAuthSourceKind::Runtime);

    let resolved = resolve_llm_provider_auth(&store, &provider, &LlmProviderAuthContext::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resolved.api_key, "stored-key");
    assert_eq!(resolved.source, LlmProviderAuthSourceKind::Stored);
}

#[tokio::test]
async fn environment_and_catalog_auth_are_fallbacks_after_stored_auth() {
    let store = SqliteLlmProviderStore::in_memory().await.unwrap();
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
    .await
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
        .await
        .unwrap();
    let resolved = resolve_llm_provider_auth(
        &store,
        &provider,
        &LlmProviderAuthContext::new().with_env("CUSTOM_PROXY_KEY", "env-key"),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(resolved.api_key, "stored-key");
    assert_eq!(resolved.source, LlmProviderAuthSourceKind::Stored);
}

#[tokio::test]
async fn auth_status_redacts_values() {
    let store = SqliteLlmProviderStore::in_memory().await.unwrap();
    let provider = LlmProviderRecord::new(
        "anthropic",
        ProviderApi::AnthropicMessages,
        "https://api.anthropic.com/v1",
    );
    let context = LlmProviderAuthContext::new().with_env("ANTHROPIC_API_KEY", "secret");
    let status = llm_provider_auth_status(&store, &provider, &context)
        .await
        .unwrap();
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

#[tokio::test]
async fn command_auth_is_visible_but_not_executed_by_default() {
    let store = SqliteLlmProviderStore::in_memory().await.unwrap();
    let provider = LlmProviderRecord::new(
        "onepassword-backed",
        ProviderApi::OpenAIChatCompletions,
        "https://proxy.example/v1",
    )
    .with_auth(LlmProviderAuthConfig::Command {
        command: "op read op://vault/item/key".to_string(),
    });
    let status = llm_provider_auth_status(&store, &provider, &LlmProviderAuthContext::new())
        .await
        .unwrap();
    assert_eq!(
        status.source,
        Some(LlmProviderAuthSourceKind::CatalogCommand)
    );
    assert!(matches!(
        resolve_llm_provider_auth(&store, &provider, &LlmProviderAuthContext::new()).await,
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
