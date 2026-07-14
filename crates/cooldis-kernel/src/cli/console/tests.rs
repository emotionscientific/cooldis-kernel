use super::*;
#[test]
fn parse_chat_args_collects_prompt_and_homes() {
    let args = vec![
        "--cwd",
        "/tmp/work",
        "--config",
        "/tmp/cooldis-chat.json",
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
    .map(OsString::from)
    .collect();
    let parsed = parse_chat_args(args).unwrap();
    assert_eq!(parsed.cwd, PathBuf::from("/tmp/work"));
    assert_eq!(
        parsed.config_path,
        Some(PathBuf::from("/tmp/cooldis-chat.json"))
    );
    assert_eq!(parsed.runtime_home, Some(PathBuf::from("/tmp/runtime")));
    assert_eq!(parsed.state_home, Some(PathBuf::from("/tmp/state")));
    assert_eq!(parsed.provider.as_deref(), Some("bifrost_openai"));
    assert_eq!(parsed.model.as_deref(), Some("openai/gpt-5.5"));
    assert_eq!(parsed.attach, None);
    assert_eq!(parsed.prompt.as_deref(), Some("hello agent"));
}

#[test]
fn parse_chat_args_collects_attach_endpoint() {
    let args = vec!["--attach", "unix:///tmp/cooldis.sock", "hello"]
        .into_iter()
        .map(OsString::from)
        .collect();

    let parsed = parse_chat_args(args).unwrap();

    assert_eq!(parsed.attach.as_deref(), Some("unix:///tmp/cooldis.sock"));
    assert_eq!(parsed.prompt.as_deref(), Some("hello"));
}

#[test]
fn parse_console_args_defaults_to_loopback_and_open() {
    let parsed = parse_console_args(Vec::new()).unwrap();

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
        "/tmp/cooldis.toml",
        "--port",
        "4321",
    ]
    .into_iter()
    .map(OsString::from)
    .collect();

    let parsed = parse_console_args(args).unwrap();

    assert_eq!(parsed.listen, "127.0.0.1:4321".parse().unwrap());
    assert!(!parsed.open);
    assert_eq!(parsed.cwd, PathBuf::from("/tmp/work"));
    assert!(parsed.cwd_explicit);
    assert_eq!(parsed.config_path, Some(PathBuf::from("/tmp/cooldis.toml")));
}

#[test]
fn console_app_server_config_from_toml_preserves_config_cwd_unless_overridden() {
    let root = std::env::temp_dir().join(format!("cooldis-console-config-{}", Uuid::now_v7()));
    std::fs::create_dir_all(&root).unwrap();
    let config_path = root.join("cooldis.toml");
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
    let listen = AppServerListenAddr::WebSocket("127.0.0.1:0".parse().unwrap());

    let parsed = parse_console_args(
        vec!["--config", config_path.to_str().unwrap()]
            .into_iter()
            .map(OsString::from)
            .collect(),
    )
    .unwrap();
    let config = console_app_server_config(&parsed, listen.clone()).unwrap();
    assert_eq!(config.listen, listen);
    assert_eq!(config.cwd, root.join("configured-work"));

    let parsed = parse_console_args(
        vec![
            "--config",
            config_path.to_str().unwrap(),
            "--cwd",
            "/tmp/override-work",
        ]
        .into_iter()
        .map(OsString::from)
        .collect(),
    )
    .unwrap();
    let config = console_app_server_config(&parsed, listen).unwrap();
    assert_eq!(config.cwd, PathBuf::from("/tmp/override-work"));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn console_app_server_config_defaults_to_project_local_roots_and_user_state() {
    let root = std::env::temp_dir().join(format!("cooldis-console-project-{}", Uuid::now_v7()));
    let nested = root.join("work/nested");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::create_dir_all(root.join("work/.cooldis")).unwrap();
    let parsed = parse_console_args(
        vec!["--cwd", nested.to_str().unwrap()]
            .into_iter()
            .map(OsString::from)
            .collect(),
    )
    .unwrap();
    let listen = AppServerListenAddr::WebSocket("127.0.0.1:0".parse().unwrap());
    let config = console_app_server_config(&parsed, listen).unwrap();

    let project = root.join("work");
    assert_eq!(config.runtime_home, project.join(".cooldis/runtime"));
    assert_eq!(config.state_home, project.join(".cooldis/state"));
    assert_eq!(config.agent_registry_root, project.join(".cooldis/agents"));
    assert_eq!(
        config.capsule_bindings.registry_root,
        Some(project.join(".cooldis/operations"))
    );
    assert_eq!(
        config.user_metadata_store_path(),
        default_user_cooldis_home()
            .unwrap()
            .join("state/metadata.sqlite3")
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn console_project_storage_root_does_not_reuse_user_home() {
    let root = std::env::temp_dir().join(format!("cooldis-console-home-{}", Uuid::now_v7()));
    let project_root = root.join("home");
    let user_home = project_root.join(".cooldis");

    assert_eq!(
        console_project_storage_root(&project_root, &user_home),
        user_home.join("projects/home")
    );
    assert_eq!(
        console_project_storage_root(&root.join("work"), &user_home),
        root.join("work/.cooldis")
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn prepare_console_project_storage_creates_operation_registry_root() {
    let root = std::env::temp_dir().join(format!("cooldis-console-roots-{}", Uuid::now_v7()));
    let workspace = root.join("workspace");
    let mut config = CooldisAppServerConfig::local(
        AppServerListenAddr::WebSocket("127.0.0.1:0".parse().unwrap()),
        &workspace,
    );
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.user_state_home = root.join("user-state");
    config.agent_registry_root = root.join("agents");
    config.capsule_bindings.registry_root = Some(root.join("operations"));

    prepare_console_project_storage(&config).unwrap();

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
    let dir = std::env::temp_dir().join(format!("cooldis-chat-config-{}", Uuid::now_v7()));
    fs::create_dir_all(&dir).unwrap();
    let config_path = dir.join("cooldis.json");
    fs::write(
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
    let args = parse_chat_args(
        vec!["--config", config_path.to_str().unwrap()]
            .into_iter()
            .map(OsString::from)
            .collect(),
    )
    .unwrap();
    match load_chat_provider_config(&args).unwrap() {
        ChatProviderConfig::BifrostOpenAI {
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
        ChatProviderConfig::Local => panic!("expected bifrost config"),
        ChatProviderConfig::OpenAIChatCompletions { .. } => {
            panic!("expected bifrost responses config")
        }
        ChatProviderConfig::AnthropicMessages { .. } => {
            panic!("expected bifrost responses config")
        }
        ChatProviderConfig::AnthropicBedrock { .. } => {
            panic!("expected bifrost responses config")
        }
        ChatProviderConfig::CatalogOpenAIChatCompletions { .. } => {
            panic!("expected bifrost responses config")
        }
    }
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn load_chat_provider_config_reads_anthropic_json() {
    let dir = std::env::temp_dir().join(format!("cooldis-anthropic-config-{}", Uuid::now_v7()));
    fs::create_dir_all(&dir).unwrap();
    let config_path = dir.join("cooldis.json");
    fs::write(
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
    let args = parse_chat_args(
        vec!["--config", config_path.to_str().unwrap()]
            .into_iter()
            .map(OsString::from)
            .collect(),
    )
    .unwrap();
    match load_chat_provider_config(&args).unwrap() {
        ChatProviderConfig::AnthropicMessages {
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
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn load_chat_provider_config_reads_anthropic_bedrock_env_file() {
    let dir = std::env::temp_dir().join(format!("cooldis-bedrock-config-{}", Uuid::now_v7()));
    fs::create_dir_all(&dir).unwrap();
    let config_path = dir.join("cooldis.json");
    let env_path = dir.join("bedrock.env");
    fs::write(
        &env_path,
        "\
AWS_ACCESS_KEY_ID=AKIA_TEST
AWS_SECRET_ACCESS_KEY=test-secret
AWS_SESSION_TOKEN=test-session
AWS_BEDROCK_REGION=us-west-2
COOLDIS_ANTHROPIC_BEDROCK_MODEL=us.anthropic.claude-sonnet-4-5-20250929-v1:0
",
    )
    .unwrap();
    fs::write(
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
    let args = parse_chat_args(
        vec!["--config", config_path.to_str().unwrap()]
            .into_iter()
            .map(OsString::from)
            .collect(),
    )
    .unwrap();
    match load_chat_provider_config(&args).unwrap() {
        ChatProviderConfig::AnthropicBedrock {
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
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn load_chat_provider_config_reads_openai_compatible_json() {
    let dir = std::env::temp_dir().join(format!(
        "cooldis-openai_compatible-config-{}",
        Uuid::now_v7()
    ));
    fs::create_dir_all(&dir).unwrap();
    let config_path = dir.join("cooldis.json");
    fs::write(
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
    let args = parse_chat_args(
        vec!["--config", config_path.to_str().unwrap()]
            .into_iter()
            .map(OsString::from)
            .collect(),
    )
    .unwrap();
    match load_chat_provider_config(&args).unwrap() {
        ChatProviderConfig::OpenAIChatCompletions {
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
            assert_eq!(model, APP_SERVER_OPENAI_COMPATIBLE_MODEL);
            assert_eq!(max_tokens, 4096);
            assert!(!stream);
            assert_eq!(
                headers,
                vec![("X-Example-Provider".to_string(), "required".to_string())]
            );
        }
        ChatProviderConfig::Local
        | ChatProviderConfig::BifrostOpenAI { .. }
        | ChatProviderConfig::AnthropicMessages { .. }
        | ChatProviderConfig::AnthropicBedrock { .. } => {
            panic!("expected openai_compatible chat completions config")
        }
        ChatProviderConfig::CatalogOpenAIChatCompletions { .. } => {
            panic!("expected direct openai_compatible chat completions config")
        }
    }
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn load_chat_provider_config_uses_catalog_for_plain_openai_compatible_without_key() {
    let dir = std::env::temp_dir().join(format!(
        "cooldis-openai_compatible-catalog-{}",
        Uuid::now_v7()
    ));
    fs::create_dir_all(&dir).unwrap();
    let config_path = dir.join("cooldis.json");
    let env_path = dir.join("empty.env");
    fs::write(&env_path, "").unwrap();
    fs::write(
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
    let args = parse_chat_args(
        vec!["--config", config_path.to_str().unwrap()]
            .into_iter()
            .map(OsString::from)
            .collect(),
    )
    .unwrap();
    match load_chat_provider_config(&args).unwrap() {
        ChatProviderConfig::CatalogOpenAIChatCompletions {
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
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn load_daemon_provider_config_uses_catalog_for_plain_openai_compatible_without_key() {
    let dir = std::env::temp_dir().join(format!(
        "cooldis-openai_compatible-daemon-catalog-{}",
        Uuid::now_v7()
    ));
    fs::create_dir_all(&dir).unwrap();
    let env_path = dir.join("empty.env");
    fs::write(&env_path, "").unwrap();
    let config = CooldisProviderConfig {
        provider: Some("openai_compatible".to_string()),
        model: Some("example-chat-model-large".to_string()),
        stream: Some(false),
        env_file: Some(env_path),
        ..CooldisProviderConfig::default()
    };

    match load_daemon_provider_config(&config).unwrap() {
        ChatProviderConfig::CatalogOpenAIChatCompletions {
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
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn load_daemon_provider_config_reads_anthropic_bedrock_env_file() {
    let dir =
        std::env::temp_dir().join(format!("cooldis-bedrock-daemon-config-{}", Uuid::now_v7()));
    fs::create_dir_all(&dir).unwrap();
    let env_path = dir.join("bedrock.env");
    fs::write(
        &env_path,
        "\
AWS_ACCESS_KEY_ID=AKIA_DAEMON_TEST
AWS_SECRET_ACCESS_KEY=daemon-secret
AWS_BEDROCK_REGION=us-east-1
AWS_BEDROCK_MODEL=anthropic.claude-sonnet-4-5-20250929-v1:0
",
    )
    .unwrap();
    let config = CooldisProviderConfig {
        provider: Some("anthropic_bedrock".to_string()),
        env_file: Some(env_path),
        stream: Some(false),
        ..CooldisProviderConfig::default()
    };

    match load_daemon_provider_config(&config).unwrap() {
        ChatProviderConfig::AnthropicBedrock {
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
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn load_chat_operation_bindings_config_resolves_registry_root() {
    let dir = std::env::temp_dir().join(format!("cooldis-operation-config-{}", Uuid::now_v7()));
    fs::create_dir_all(&dir).unwrap();
    let config_path = dir.join("cooldis.json");
    fs::write(
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
    let args = parse_chat_args(
        vec!["--config", config_path.to_str().unwrap()]
            .into_iter()
            .map(OsString::from)
            .collect(),
    )
    .unwrap();
    // lexicon-allow: capsule - existing chat config function name
    let bindings = load_chat_capsule_bindings_config(&args).unwrap();
    assert_eq!(bindings.registry_root, Some(dir.join("operations")));
    assert_eq!(bindings.global_operation_names, vec!["search"]);
    assert!(bindings.load_all_active_when_unbound);
    let _ = fs::remove_dir_all(dir);
}
