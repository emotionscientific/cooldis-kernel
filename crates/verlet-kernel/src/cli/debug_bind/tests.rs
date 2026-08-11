#[test]
fn parse_debug_bind_requires_thread_id_and_rejects_conflicting_endpoints() {
    let missing = crate::cli::debug_bind::parse_debug_bind_args(Vec::new())
        .unwrap_err()
        .to_string();
    assert!(missing.contains("requires exactly one <thread-id>"));

    let conflicting = vec![
        "thread-1",
        "--url",
        "ws://127.0.0.1:49200/rpc",
        "--journal",
        "/tmp/history.sqlite3",
    ]
    .into_iter()
    .map(std::ffi::OsString::from)
    .collect();
    let err = crate::cli::debug_bind::parse_debug_bind_args(conflicting)
        .unwrap_err()
        .to_string();
    assert!(err.contains("--url, --config, or --journal"));

    for flag in ["--url", "--config", "--journal"] {
        let missing_value = vec!["thread-1", flag]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect();
        let err = crate::cli::debug_bind::parse_debug_bind_args(missing_value)
            .unwrap_err()
            .to_string();
        assert!(err.contains(&format!("{flag} requires a value")));

        let next_flag_is_not_a_value = vec!["thread-1", flag, "--json"]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect();
        let err = crate::cli::debug_bind::parse_debug_bind_args(next_flag_is_not_a_value)
            .unwrap_err()
            .to_string();
        assert!(err.contains(&format!("{flag} requires a value")));
    }
}

#[test]
fn legacy_receipts_assemble_with_unrecorded_origins() {
    let compile: crate::agent::manifest_bind::AgentManifestCompileReceipt =
        serde_json::from_value(serde_json::json!({
            "ref_uri": "agent://legacy@1.0.0",
            "manifest_hash": format!("sha256:{}", "1".repeat(64)),
            "source_hash": format!("sha256:{}", "2".repeat(64)),
        }))
        .unwrap();
    let bind: crate::agent::manifest_bind::AgentManifestBindReceipt =
        serde_json::from_value(serde_json::json!({
            "ref_uri": "agent://legacy@1.0.0",
            "manifest_hash": format!("sha256:{}", "1".repeat(64)),
            "model_profile_id": "default",
            "provider_id": "local_offline",
            "model_id": "echo",
            "tool_ids": [],
            "operation_bindings": [],
            "effective_runtime": {},
            "overridden_keys": [],
        }))
        .unwrap();

    let explanation = crate::cli::debug_bind::assemble_bind_explanation(
        "thread-1",
        "compile-1",
        "bind-1",
        &compile,
        &bind,
    )
    .unwrap();
    let rendered = crate::cli::debug_bind::render_bind_explanation(&explanation);

    assert_eq!(explanation.model.origin, None);
    let placement = explanation.placement.unwrap();
    assert_eq!(placement.target, "local");
    assert_eq!(placement.origin, None);
    assert!(rendered.contains("[unrecorded]"));
}

#[test]
fn active_receipts_follow_the_highest_bind_sequence_and_its_provenance() {
    let events = vec![
        crate::cli::debug_bind::RecordedReceiptEvent {
            event_id: "compile-1".to_string(),
            sequence: 1,
            kind: verlet_history::EventKind::ManifestCompileCompleted.to_string(),
            source_event_ids: Vec::new(),
            payload: serde_json::json!({"generation": 1}),
        },
        crate::cli::debug_bind::RecordedReceiptEvent {
            event_id: "bind-1".to_string(),
            sequence: 2,
            kind: verlet_history::EventKind::ManifestBindCompleted.to_string(),
            source_event_ids: vec!["compile-1".to_string()],
            payload: serde_json::json!({"generation": 1}),
        },
        crate::cli::debug_bind::RecordedReceiptEvent {
            event_id: "compile-2".to_string(),
            sequence: 3,
            kind: verlet_history::EventKind::ManifestCompileCompleted.to_string(),
            source_event_ids: Vec::new(),
            payload: serde_json::json!({"generation": 2}),
        },
        crate::cli::debug_bind::RecordedReceiptEvent {
            event_id: "bind-2".to_string(),
            sequence: 4,
            kind: verlet_history::EventKind::ManifestBindCompleted.to_string(),
            source_event_ids: vec!["compile-2".to_string()],
            payload: serde_json::json!({"generation": 2}),
        },
    ];

    let (compile, bind) = crate::cli::debug_bind::active_receipt_events(&events).unwrap();

    assert_eq!(compile.event_id, "compile-2");
    assert_eq!(bind.event_id, "bind-2");
}

#[test]
fn operation_tool_origins_are_not_guessed_from_receipt_order() {
    let bind: crate::agent::manifest_bind::AgentManifestBindReceipt =
        serde_json::from_value(serde_json::json!({
            "ref_uri": "agent://tools@1.0.0",
            "manifest_hash": format!("sha256:{}", "1".repeat(64)),
            "model_profile_id": "default",
            "provider_id": "local_offline",
            "model_id": "echo",
            "tool_ids": ["manifest-tool-id"],
            "operation_bindings": [{
                "name": "operation-record",
                "artifact_hash": "a".repeat(64),
                "operations": ["operation-name"]
            }],
            "effective_runtime": {},
            "overridden_keys": []
        }))
        .unwrap();

    let tools = crate::cli::debug_bind::assemble_tools(&bind);

    assert_eq!(
        tools,
        vec![crate::cli::debug_bind::BindToolExplanation {
            tool_id: "manifest-tool-id".to_string(),
            origin: None,
            row_id: None,
            pinned: false,
            operation_name: None,
            artifact_hash: None,
        }]
    );
    let explanation = crate::cli::debug_bind::BindExplanation {
        thread_id: "thread-1".to_string(),
        manifest: crate::cli::debug_bind::BindManifestExplanation {
            ref_uri: "agent://tools@1.0.0".to_string(),
            manifest_hash: format!("sha256:{}", "1".repeat(64)),
            source_hash: format!("sha256:{}", "2".repeat(64)),
            alias: None,
            compile_event_id: "compile-1".to_string(),
            bind_event_id: "bind-1".to_string(),
        },
        model: crate::cli::debug_bind::BindModelExplanation {
            profile_id: "default".to_string(),
            provider_id: "local_offline".to_string(),
            model_id: "echo".to_string(),
            origin: None,
        },
        placement: None,
        workspace: None,
        runtime: Vec::new(),
        tools,
        universes: Vec::new(),
        couplings: Vec::new(),
        skills: Vec::new(),
        context: Vec::new(),
    };
    assert!(
        crate::cli::debug_bind::render_bind_explanation(&explanation)
            .contains("tools\n  manifest-tool-id   [unrecorded]\n")
    );
}

#[test]
fn alias_timestamp_overflow_is_rejected() {
    let compile: crate::agent::manifest_bind::AgentManifestCompileReceipt =
        serde_json::from_value(serde_json::json!({
            "ref_uri": "agent://alias@1.0.0",
            "manifest_hash": format!("sha256:{}", "1".repeat(64)),
            "source_hash": format!("sha256:{}", "2".repeat(64)),
            "alias": {
                "ref_uri": "agent://alias@latest",
                "alias": "latest",
                "version": "1.0.0",
                "manifest_hash": format!("sha256:{}", "1".repeat(64)),
                "resolved_at_ms": u64::MAX
            }
        }))
        .unwrap();
    let bind: crate::agent::manifest_bind::AgentManifestBindReceipt =
        serde_json::from_value(serde_json::json!({
            "ref_uri": "agent://alias@1.0.0",
            "manifest_hash": format!("sha256:{}", "1".repeat(64)),
            "model_profile_id": "default",
            "provider_id": "local_offline",
            "model_id": "echo",
            "tool_ids": [],
            "operation_bindings": [],
            "effective_runtime": {},
            "overridden_keys": []
        }))
        .unwrap();

    let err = crate::cli::debug_bind::assemble_bind_explanation(
        "thread-1",
        "compile-1",
        "bind-1",
        &compile,
        &bind,
    )
    .unwrap_err()
    .to_string();

    assert!(err.contains("resolved_at_ms is out of range"));
}

#[test]
fn short_hash_preserves_non_hash_values() {
    assert_eq!(
        crate::cli::debug_bind::short_hash("not-a-hash"),
        "not-a-hash"
    );
    assert_eq!(
        crate::cli::debug_bind::short_hash("sha256:not-a-hash"),
        "sha256:not-a-hash"
    );
    assert_eq!(
        crate::cli::debug_bind::short_hash(&"a".repeat(64)),
        "sha256:aaaaaaaaaaaa…"
    );
}

#[test]
fn render_bind_explanation_matches_pinned_format() {
    let hash_a = format!("sha256:{}", "a".repeat(64));
    let hash_b = format!("sha256:{}", "b".repeat(64));
    let hash_c = format!("sha256:{}", "c".repeat(64));
    let explanation = crate::cli::debug_bind::BindExplanation {
        thread_id: "0190-thread".to_string(),
        manifest: crate::cli::debug_bind::BindManifestExplanation {
            ref_uri: "agent://analyst@1.2.3".to_string(),
            manifest_hash: hash_a.clone(),
            source_hash: hash_b.clone(),
            alias: Some(crate::cli::debug_bind::BindAliasExplanation {
                alias: "latest".to_string(),
                version: "1.2.3".to_string(),
                resolved_at: "2026-07-16T20:00:00.000Z".to_string(),
            }),
            compile_event_id: "compile-event".to_string(),
            bind_event_id: "bind-event".to_string(),
        },
        model: crate::cli::debug_bind::BindModelExplanation {
            profile_id: "fast".to_string(),
            provider_id: "openai".to_string(),
            model_id: "gpt-5".to_string(),
            origin: Some(
                crate::agent::manifest_bind::AgentManifestModelProfileOrigin::SelectedAtStart,
            ),
        },
        placement: Some(crate::cli::debug_bind::BindPlacementExplanation {
            target: "remote".to_string(),
            executor_ref: Some("executor://pool/main".to_string()),
            origin: Some(crate::agent::manifest_bind::AgentManifestBindingOrigin::BindOverride),
        }),
        workspace: Some(crate::cli::debug_bind::BindWorkspaceExplanation {
            guest_path: std::path::PathBuf::from("/workspace"),
            host_path: std::path::PathBuf::from("/srv/repo"),
            mode: "rw".to_string(),
            origin: Some(crate::agent::manifest_bind::AgentManifestBindingOrigin::DaemonDefault),
        }),
        runtime: vec![
            crate::cli::debug_bind::BindRuntimeExplanation {
                key: "streaming".to_string(),
                value: serde_json::json!(true),
                overridden: false,
            },
            crate::cli::debug_bind::BindRuntimeExplanation {
                key: "turn_timeout_ms".to_string(),
                value: serde_json::json!(30000),
                overridden: true,
            },
        ],
        tools: vec![crate::cli::debug_bind::BindToolExplanation {
            tool_id: "search".to_string(),
            origin: Some("direct".to_string()),
            row_id: Some("search".to_string()),
            pinned: false,
            operation_name: Some("search-op".to_string()),
            artifact_hash: Some(hash_c.clone()),
        }],
        universes: vec![crate::cli::debug_bind::BindUniverseExplanation {
            import_id: "docs".to_string(),
            server_ref: "mcp://docs".to_string(),
            discovery_hash: hash_b.clone(),
            tool_count: 8,
            pinned_count: 1,
        }],
        couplings: vec![crate::cli::debug_bind::BindCouplingExplanation {
            id: "audit".to_string(),
            role: "projection".to_string(),
            function_ref: format!("op://audit/project@{hash_c}"),
            artifact_hash: hash_c.clone(),
            config_hash: hash_a.clone(),
        }],
        skills: vec![crate::cli::debug_bind::BindSkillExplanation {
            package: "release-checks".to_string(),
            artifact_hash: hash_b.clone(),
        }],
        context: vec![crate::cli::debug_bind::BindContextExplanation {
            ref_uri: "resource://system".to_string(),
            content_sha256: hash_c,
        }],
    };

    assert_eq!(
        crate::cli::debug_bind::render_bind_explanation(&explanation),
        concat!(
            "thread 0190-thread\n",
            "manifest agent://analyst@1.2.3 (manifest sha256:aaaaaaaaaaaa…, source sha256:bbbbbbbbbbbb…)\n",
            "  alias latest -> 1.2.3 (resolved 2026-07-16T20:00:00.000Z)\n",
            "  receipts compile compile-event bind bind-event\n",
            "\n",
            "model\n",
            "  fast: openai / gpt-5   [selected-at-start]\n",
            "\n",
            "placement\n",
            "  remote (executor executor://pool/main)   [bind-override]\n",
            "\n",
            "workspace\n",
            "  /workspace -> /srv/repo (rw)   [daemon-default]\n",
            "\n",
            "runtime\n",
            "  streaming = true   [manifest]\n",
            "  turn_timeout_ms = 30000   [override]\n",
            "\n",
            "tools\n",
            "  search   [direct search]  operation search-op@sha256:cccccccccccc…\n",
            "\n",
            "universes\n",
            "  docs mcp://docs (discovery sha256:bbbbbbbbbbbb…)  tools 8  pinned 1\n",
            "\n",
            "couplings\n",
            "  audit (projection)  fn op://audit/project@sha256:cccccccccccc…  config sha256:aaaaaaaaaaaa…\n",
            "\n",
            "skills\n",
            "  release-checks  sha256:bbbbbbbbbbbb…\n",
            "\n",
            "context\n",
            "  resource://system  sha256:cccccccccccc…\n",
        )
    );
}
