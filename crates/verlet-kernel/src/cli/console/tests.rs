#[cfg(unix)]
#[tokio::test]
async fn checked_browser_open_reports_a_launcher_failure() {
    let mut command = std::process::Command::new("sh");
    command.args(["-c", "exit 7"]);

    let error = super::wait_for_browser_open_command(command)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("browser opener exited"));
}

#[test]
fn parse_chat_args_collects_prompt_and_homes() {
    let args = vec![
        "--cwd",
        "/tmp/work",
        "--config",
        "/tmp/verlet-chat.json",
        "--runtime-home",
        "/tmp/runtime",
        "--state-home",
        "/tmp/state",
        "--provider",
        "bifrost_openai",
        "--model",
        "openai/gpt-5.5",
        "hello",
        "agent",
    ]
    .into_iter()
    .map(std::ffi::OsString::from)
    .collect();
    let parsed = crate::cli::console::parse_chat_args(args).unwrap();
    assert_eq!(parsed.cwd, std::path::PathBuf::from("/tmp/work"));
    assert_eq!(
        parsed.config_path,
        Some(std::path::PathBuf::from("/tmp/verlet-chat.json"))
    );
    assert_eq!(
        parsed.runtime_home,
        Some(std::path::PathBuf::from("/tmp/runtime"))
    );
    assert_eq!(
        parsed.state_home,
        Some(std::path::PathBuf::from("/tmp/state"))
    );
    assert_eq!(parsed.provider.as_deref(), Some("bifrost_openai"));
    assert_eq!(parsed.model.as_deref(), Some("openai/gpt-5.5"));
    assert_eq!(parsed.attach, None);
    assert_eq!(parsed.prompt.as_deref(), Some("hello agent"));
}

#[test]
fn parse_chat_args_collects_attach_endpoint() {
    let args = vec!["--attach", "unix:///tmp/verlet.sock", "hello"]
        .into_iter()
        .map(std::ffi::OsString::from)
        .collect();

    let parsed = crate::cli::console::parse_chat_args(args).unwrap();

    assert_eq!(parsed.attach.as_deref(), Some("unix:///tmp/verlet.sock"));
    assert_eq!(parsed.prompt.as_deref(), Some("hello"));
}

#[test]
fn parse_console_args_defaults_to_loopback_and_open() {
    let parsed = crate::cli::console::parse_console_args(Vec::new()).unwrap();

    assert_eq!(
        parsed.listen.ip(),
        "127.0.0.1".parse::<std::net::IpAddr>().unwrap()
    );
    assert_eq!(parsed.listen.port(), 0);
    assert!(parsed.open);
    assert_eq!(parsed.config_path, None);
    assert!(!parsed.cwd_explicit);
}

#[test]
fn parse_console_args_collects_browser_and_runtime_options() {
    let args = vec![
        "--no-open",
        "--cwd",
        "/tmp/work",
        "--config",
        "/tmp/verlet.toml",
        "--port",
        "4321",
    ]
    .into_iter()
    .map(std::ffi::OsString::from)
    .collect();

    let parsed = crate::cli::console::parse_console_args(args).unwrap();

    assert_eq!(parsed.listen, "127.0.0.1:4321".parse().unwrap());
    assert!(!parsed.open);
    assert_eq!(parsed.cwd, std::path::PathBuf::from("/tmp/work"));
    assert!(parsed.cwd_explicit);
    assert_eq!(
        parsed.config_path,
        Some(std::path::PathBuf::from("/tmp/verlet.toml"))
    );
}

#[test]
fn console_app_server_config_from_toml_preserves_config_cwd_unless_overridden() {
    let root = std::env::temp_dir().join(format!("verlet-console-config-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&root).unwrap();
    let config_path = root.join("verlet.toml");
    std::fs::write(
        &config_path,
        r#"
[daemon.runtime]
cwd = "configured-work"

[daemon.app_server]
listen = "unix:///tmp/ignored-console-config.sock"
"#,
    )
    .unwrap();
    let listen =
        crate::adapters::app_server::AppServerListenAddr::WebSocket("127.0.0.1:0".parse().unwrap());

    let parsed = crate::cli::console::parse_console_args(
        vec!["--config", config_path.to_str().unwrap()]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect(),
    )
    .unwrap();
    let config = crate::cli::console::console_app_server_config(&parsed, listen.clone()).unwrap();
    assert_eq!(config.listen, listen);
    assert_eq!(config.cwd, root.join("configured-work"));

    let parsed = crate::cli::console::parse_console_args(
        vec![
            "--config",
            config_path.to_str().unwrap(),
            "--cwd",
            "/tmp/override-work",
        ]
        .into_iter()
        .map(std::ffi::OsString::from)
        .collect(),
    )
    .unwrap();
    let config = crate::cli::console::console_app_server_config(&parsed, listen).unwrap();
    assert_eq!(config.cwd, std::path::PathBuf::from("/tmp/override-work"));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn console_app_server_config_defaults_to_project_local_roots_and_user_state() {
    let root =
        std::env::temp_dir().join(format!("verlet-console-project-{}", uuid::Uuid::now_v7()));
    let nested = root.join("work/nested");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::create_dir_all(root.join("work/.verlet")).unwrap();
    let parsed = crate::cli::console::parse_console_args(
        vec!["--cwd", nested.to_str().unwrap()]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect(),
    )
    .unwrap();
    let listen =
        crate::adapters::app_server::AppServerListenAddr::WebSocket("127.0.0.1:0".parse().unwrap());
    let config = crate::cli::console::console_app_server_config(&parsed, listen).unwrap();

    let project = root.join("work");
    assert_eq!(config.runtime_home, project.join(".verlet/runtime"));
    assert_eq!(config.state_home, project.join(".verlet/state"));
    assert_eq!(config.agent_registry_root, project.join(".verlet/agents"));
    assert_eq!(
        config.capsule_bindings.registry_root,
        Some(project.join(".verlet/operations"))
    );
    assert_eq!(
        config.user_metadata_store_path(),
        crate::cli::console::default_user_verlet_home()
            .unwrap()
            .join("state/metadata.sqlite3")
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn console_project_storage_root_does_not_reuse_user_home() {
    let root = std::env::temp_dir().join(format!("verlet-console-home-{}", uuid::Uuid::now_v7()));
    let project_root = root.join("home");
    let user_home = project_root.join(".verlet");

    assert_eq!(
        crate::cli::console::console_project_storage_root(&project_root, &user_home),
        user_home.join("projects/home")
    );
    assert_eq!(
        crate::cli::console::console_project_storage_root(&root.join("work"), &user_home),
        root.join("work/.verlet")
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn console_project_storage_ignores_existing_legacy_state_directory() {
    let root = std::env::temp_dir().join(format!("verlet-console-state-{}", uuid::Uuid::now_v7()));
    let project_root = root.join("project");
    let user_home = root.join("user");
    let legacy = project_root.join(concat!(".", "cool", "dis"));
    std::fs::create_dir_all(&legacy).unwrap();

    assert_eq!(
        crate::cli::console::console_project_storage_root(&project_root, &user_home),
        project_root.join(".verlet")
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn console_project_storage_prefers_new_state_directory() {
    let root = std::env::temp_dir().join(format!("verlet-console-state-{}", uuid::Uuid::now_v7()));
    let project_root = root.join("project");
    let user_home = root.join("user");
    let canonical = project_root.join(".verlet");
    let legacy = project_root.join(concat!(".", "cool", "dis"));
    std::fs::create_dir_all(&legacy).unwrap();
    std::fs::create_dir_all(&canonical).unwrap();

    assert_eq!(
        crate::cli::console::console_project_storage_root(&project_root, &user_home),
        canonical
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn prepare_console_project_storage_creates_operation_registry_root() {
    let root = std::env::temp_dir().join(format!("verlet-console-roots-{}", uuid::Uuid::now_v7()));
    let workspace = root.join("workspace");
    let mut config = crate::adapters::app_server::VerletAppServerConfig::local(
        crate::adapters::app_server::AppServerListenAddr::WebSocket("127.0.0.1:0".parse().unwrap()),
        &workspace,
    );
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.user_state_home = root.join("user-state");
    config.agent_registry_root = root.join("agents");
    config.capsule_bindings.registry_root = Some(root.join("operations"));

    crate::cli::console::prepare_console_project_storage(&config).unwrap();

    assert!(config.runtime_home.is_dir());
    assert!(config.state_home.is_dir());
    assert!(config.user_state_home.is_dir());
    assert!(config.agent_registry_root.is_dir());
    assert!(
        config
            .capsule_bindings
            .registry_root
            .as_ref()
            .is_some_and(|path| path.is_dir())
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn load_chat_provider_config_reads_bifrost_json() {
    let dir = std::env::temp_dir().join(format!("verlet-chat-config-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let config_path = dir.join("verlet.json");
    std::fs::write(
        &config_path,
        serde_json::to_vec(&serde_json::json!({
            "chat": {
                "provider": "bifrost_openai",
                "base_url": "https://bifrost.example.test",
                "api_key": "test-key",
                "model": "openai/gpt-5.5",
                "max_tokens": 2048,
                "stream": false
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let args = crate::cli::console::parse_chat_args(
        vec!["--config", config_path.to_str().unwrap()]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect(),
    )
    .unwrap();
    match crate::cli::console::load_chat_provider_config(&args).unwrap() {
        crate::cli::console::ChatProviderConfig::BifrostOpenAI {
            base_url,
            api_key,
            model,
            max_tokens,
            stream,
        } => {
            assert_eq!(base_url, "https://bifrost.example.test");
            assert_eq!(api_key, "test-key");
            assert_eq!(model, "openai/gpt-5.5");
            assert_eq!(max_tokens, 2048);
            assert!(!stream);
        }
        crate::cli::console::ChatProviderConfig::Local
        | crate::cli::console::ChatProviderConfig::OpenAICodex { .. } => {
            panic!("expected bifrost config")
        }
        crate::cli::console::ChatProviderConfig::OpenAIChatCompletions { .. } => {
            panic!("expected bifrost responses config")
        }
        crate::cli::console::ChatProviderConfig::AnthropicMessages { .. } => {
            panic!("expected bifrost responses config")
        }
        crate::cli::console::ChatProviderConfig::AnthropicBedrock { .. } => {
            panic!("expected bifrost responses config")
        }
        crate::cli::console::ChatProviderConfig::CatalogOpenAIChatCompletions { .. } => {
            panic!("expected bifrost responses config")
        }
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn load_chat_provider_config_reads_anthropic_json() {
    let dir =
        std::env::temp_dir().join(format!("verlet-anthropic-config-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let config_path = dir.join("verlet.json");
    std::fs::write(
        &config_path,
        serde_json::to_vec(&serde_json::json!({
            "chat": {
                "provider": "anthropic",
                "base_url": "https://api.anthropic.com",
                "api_key": "test-anthropic-key",
                "model": "claude-sonnet-4-5-20250929",
                "max_tokens": 1024,
                "stream": false
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let args = crate::cli::console::parse_chat_args(
        vec!["--config", config_path.to_str().unwrap()]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect(),
    )
    .unwrap();
    match crate::cli::console::load_chat_provider_config(&args).unwrap() {
        crate::cli::console::ChatProviderConfig::AnthropicMessages {
            base_url,
            api_key,
            model,
            max_tokens,
            stream,
        } => {
            assert_eq!(base_url, "https://api.anthropic.com");
            assert_eq!(api_key, "test-anthropic-key");
            assert_eq!(model, "claude-sonnet-4-5-20250929");
            assert_eq!(max_tokens, 1024);
            assert!(!stream);
        }
        _ => panic!("expected Anthropic Messages config"),
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn load_chat_provider_config_selects_openai_codex_without_an_api_key() {
    let args = crate::cli::console::parse_chat_args(
        [
            "--provider",
            "openai-codex",
            "--model",
            "gpt-5.6-terra",
            "--max-tokens",
            "2048",
            "--no-stream",
        ]
        .into_iter()
        .map(std::ffi::OsString::from)
        .collect(),
    )
    .unwrap();

    match crate::cli::console::load_chat_provider_config(&args).unwrap() {
        crate::cli::console::ChatProviderConfig::OpenAICodex {
            model,
            max_tokens,
            stream,
        } => {
            assert_eq!(model, "gpt-5.6-terra");
            assert_eq!(max_tokens, 2048);
            assert!(!stream);
        }
        _ => panic!("expected OpenAI Codex config"),
    }
}

#[test]
fn load_chat_provider_config_reads_anthropic_bedrock_env_file() {
    let dir = std::env::temp_dir().join(format!("verlet-bedrock-config-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let config_path = dir.join("verlet.json");
    let env_path = dir.join("bedrock.env");
    std::fs::write(
        &env_path,
        "\
AWS_ACCESS_KEY_ID=AKIA_TEST
AWS_SECRET_ACCESS_KEY=test-secret
AWS_SESSION_TOKEN=test-session
AWS_BEDROCK_REGION=us-west-2
VERLET_ANTHROPIC_BEDROCK_MODEL=us.anthropic.claude-sonnet-4-5-20250929-v1:0
",
    )
    .unwrap();
    std::fs::write(
        &config_path,
        serde_json::to_vec(&serde_json::json!({
            "chat": {
                "provider": "anthropic_bedrock",
                "base_url": "https://bedrock-runtime.us-west-2.amazonaws.com/",
                "env_file": "bedrock.env",
                "max_tokens": 2048,
                "stream": false
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let args = crate::cli::console::parse_chat_args(
        vec!["--config", config_path.to_str().unwrap()]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect(),
    )
    .unwrap();
    match crate::cli::console::load_chat_provider_config(&args).unwrap() {
        crate::cli::console::ChatProviderConfig::AnthropicBedrock {
            region,
            base_url,
            access_key_id,
            secret_access_key,
            session_token,
            model,
            max_tokens,
            stream,
        } => {
            assert_eq!(region, "us-west-2");
            assert_eq!(
                base_url.as_deref(),
                Some("https://bedrock-runtime.us-west-2.amazonaws.com")
            );
            assert_eq!(access_key_id, "AKIA_TEST");
            assert_eq!(secret_access_key, "test-secret");
            assert_eq!(session_token.as_deref(), Some("test-session"));
            assert_eq!(model, "us.anthropic.claude-sonnet-4-5-20250929-v1:0");
            assert_eq!(max_tokens, 2048);
            assert!(!stream);
        }
        _ => panic!("expected Anthropic Bedrock config"),
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn load_chat_provider_config_reads_openai_compatible_json() {
    let dir = std::env::temp_dir().join(format!(
        "verlet-openai_compatible-config-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let config_path = dir.join("verlet.json");
    std::fs::write(
        &config_path,
        serde_json::to_vec(&serde_json::json!({
            "chat": {
                "provider": "openai_compatible",
                "api_key": "test-openai_compatible-key",
                "stream": false
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let args = crate::cli::console::parse_chat_args(
        vec!["--config", config_path.to_str().unwrap()]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect(),
    )
    .unwrap();
    match crate::cli::console::load_chat_provider_config(&args).unwrap() {
        crate::cli::console::ChatProviderConfig::OpenAIChatCompletions {
            provider,
            base_url,
            api_key,
            model,
            max_tokens,
            stream,
            headers,
        } => {
            assert_eq!(provider, "openai_compatible");
            assert_eq!(base_url, "https://api.example.invalid/v1");
            assert_eq!(api_key, "test-openai_compatible-key");
            assert_eq!(
                model,
                crate::adapters::app_server::APP_SERVER_OPENAI_COMPATIBLE_MODEL
            );
            assert_eq!(max_tokens, 4096);
            assert!(!stream);
            assert_eq!(
                headers,
                vec![("X-Example-Provider".to_string(), "required".to_string())]
            );
        }
        crate::cli::console::ChatProviderConfig::Local
        | crate::cli::console::ChatProviderConfig::OpenAICodex { .. }
        | crate::cli::console::ChatProviderConfig::BifrostOpenAI { .. }
        | crate::cli::console::ChatProviderConfig::AnthropicMessages { .. }
        | crate::cli::console::ChatProviderConfig::AnthropicBedrock { .. } => {
            panic!("expected openai_compatible chat completions config")
        }
        crate::cli::console::ChatProviderConfig::CatalogOpenAIChatCompletions { .. } => {
            panic!("expected direct openai_compatible chat completions config")
        }
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn load_chat_provider_config_uses_catalog_for_plain_openai_compatible_without_key() {
    let dir = std::env::temp_dir().join(format!(
        "verlet-openai_compatible-catalog-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let config_path = dir.join("verlet.json");
    let env_path = dir.join("empty.env");
    std::fs::write(&env_path, "").unwrap();
    std::fs::write(
        &config_path,
        serde_json::to_vec(&serde_json::json!({
            "chat": {
                "provider": "openai_compatible",
                "model": "example-chat-model-large",
                "stream": false,
                "env_file": "empty.env"
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let args = crate::cli::console::parse_chat_args(
        vec!["--config", config_path.to_str().unwrap()]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect(),
    )
    .unwrap();
    match crate::cli::console::load_chat_provider_config(&args).unwrap() {
        crate::cli::console::ChatProviderConfig::CatalogOpenAIChatCompletions {
            provider_id,
            model,
            max_tokens,
            stream,
        } => {
            assert_eq!(provider_id, "openai_compatible");
            assert_eq!(model.as_deref(), Some("example-chat-model-large"));
            assert_eq!(max_tokens, 4096);
            assert!(!stream);
        }
        _ => panic!("expected catalog-backed openai_compatible config"),
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn load_daemon_provider_config_uses_catalog_for_plain_openai_compatible_without_key() {
    let dir = std::env::temp_dir().join(format!(
        "verlet-openai_compatible-daemon-catalog-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let env_path = dir.join("empty.env");
    std::fs::write(&env_path, "").unwrap();
    let config = crate::daemon::daemon_config::VerletProviderConfig {
        provider: Some("openai_compatible".to_string()),
        model: Some("example-chat-model-large".to_string()),
        stream: Some(false),
        env_file: Some(env_path),
        ..crate::daemon::daemon_config::VerletProviderConfig::default()
    };

    match crate::cli::daemon::load_daemon_provider_config(&config).unwrap() {
        crate::cli::console::ChatProviderConfig::CatalogOpenAIChatCompletions {
            provider_id,
            model,
            max_tokens,
            stream,
        } => {
            assert_eq!(provider_id, "openai_compatible");
            assert_eq!(model.as_deref(), Some("example-chat-model-large"));
            assert_eq!(max_tokens, 4096);
            assert!(!stream);
        }
        _ => panic!("expected catalog-backed openai_compatible daemon config"),
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn load_daemon_provider_config_selects_openai_codex_without_an_api_key() {
    let config = crate::daemon::daemon_config::VerletProviderConfig {
        provider: Some("openai-codex".to_string()),
        model: Some("gpt-5.6-luna".to_string()),
        max_tokens: Some(3072),
        stream: Some(false),
        ..crate::daemon::daemon_config::VerletProviderConfig::default()
    };

    match crate::cli::daemon::load_daemon_provider_config(&config).unwrap() {
        crate::cli::console::ChatProviderConfig::OpenAICodex {
            model,
            max_tokens,
            stream,
        } => {
            assert_eq!(model, "gpt-5.6-luna");
            assert_eq!(max_tokens, 3072);
            assert!(!stream);
        }
        _ => panic!("expected OpenAI Codex daemon config"),
    }
}

#[test]
fn load_daemon_provider_config_reads_anthropic_bedrock_env_file() {
    let dir = std::env::temp_dir().join(format!(
        "verlet-bedrock-daemon-config-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let env_path = dir.join("bedrock.env");
    std::fs::write(
        &env_path,
        "\
AWS_ACCESS_KEY_ID=AKIA_DAEMON_TEST
AWS_SECRET_ACCESS_KEY=daemon-secret
AWS_BEDROCK_REGION=us-east-1
AWS_BEDROCK_MODEL=anthropic.claude-sonnet-4-5-20250929-v1:0
",
    )
    .unwrap();
    let config = crate::daemon::daemon_config::VerletProviderConfig {
        provider: Some("anthropic_bedrock".to_string()),
        env_file: Some(env_path),
        stream: Some(false),
        ..crate::daemon::daemon_config::VerletProviderConfig::default()
    };

    match crate::cli::daemon::load_daemon_provider_config(&config).unwrap() {
        crate::cli::console::ChatProviderConfig::AnthropicBedrock {
            region,
            base_url,
            access_key_id,
            secret_access_key,
            session_token,
            model,
            max_tokens,
            stream,
        } => {
            assert_eq!(region, "us-east-1");
            assert_eq!(base_url, None);
            assert_eq!(access_key_id, "AKIA_DAEMON_TEST");
            assert_eq!(secret_access_key, "daemon-secret");
            assert_eq!(session_token, None);
            assert_eq!(model, "anthropic.claude-sonnet-4-5-20250929-v1:0");
            assert_eq!(max_tokens, 4096);
            assert!(!stream);
        }
        _ => panic!("expected Anthropic Bedrock daemon config"),
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn load_chat_operation_bindings_config_resolves_registry_root() {
    let dir =
        std::env::temp_dir().join(format!("verlet-operation-config-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let config_path = dir.join("verlet.json");
    std::fs::write(
        &config_path,
        serde_json::to_vec(&serde_json::json!({
            "chat": {
                // lexicon-allow: capsule - existing chat config field name
                "capsule_bindings": {
                    "registry_root": "operations",
                    "global_operation_names": ["search"],
                    "load_all_active_when_unbound": true
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let args = crate::cli::console::parse_chat_args(
        vec!["--config", config_path.to_str().unwrap()]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect(),
    )
    .unwrap();
    // lexicon-allow: capsule - existing chat config function name
    let bindings = crate::cli::console::load_chat_capsule_bindings_config(&args).unwrap();
    assert_eq!(bindings.registry_root, Some(dir.join("operations")));
    assert_eq!(bindings.global_operation_names, vec!["search"]);
    assert!(bindings.load_all_active_when_unbound);
    let _ = std::fs::remove_dir_all(dir);
}
