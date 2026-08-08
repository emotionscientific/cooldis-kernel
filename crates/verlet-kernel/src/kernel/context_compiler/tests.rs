#[test]
fn context_compilation_is_deterministic_and_reports_drops() {
    let input = compile_input(
        vec![
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::user_text("old"),
            },
            verlet_history::SessionEntryKind::CustomContextMessage {
                message: verlet_history::CanonicalMessage::user_text("hook"),
            },
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::assistant(
                    "openai",
                    verlet_history::ProviderApi::OpenAIResponses,
                    "gpt-test",
                    vec![verlet_history::CanonicalContent::text("abcdef")],
                    verlet_history::CanonicalStopReason::EndTurn,
                ),
            },
        ],
        crate::kernel::context_compiler::AgentContextCompilePolicy {
            max_messages: Some(2),
            max_text_bytes: Some(5),
        },
    );

    let first = crate::kernel::context_compiler::AgentContextCompiler::compile(input.clone());
    let second = crate::kernel::context_compiler::AgentContextCompiler::compile(input);

    assert_eq!(first, second);
    assert_eq!(first.diagnostics.dropped_entries.len(), 1);
    assert_eq!(first.diagnostics.retained_text_bytes, 5);
    assert_eq!(first.diagnostics.truncated_text_bytes, 5);
    assert_eq!(message_texts(&first.messages), vec!["", "bcdef"]);
}

#[test]
fn synthetic_context_rebuild_preserves_persisted_timestamps() {
    let mut input = compile_input(
        vec![
            verlet_history::SessionEntryKind::Message {
                message: user_message_at("old", 100),
            },
            verlet_history::SessionEntryKind::Compaction {
                summary: "old facts".to_string(),
            },
            verlet_history::SessionEntryKind::CustomContextMessage {
                message: user_message_at("persisted hook", 300),
            },
        ],
        crate::kernel::context_compiler::AgentContextCompilePolicy::unbounded(),
    );
    input.session_entries[0].created_at_ms = 100;
    input.session_entries[1].created_at_ms = 200;
    input.session_entries[2].created_at_ms = 300;
    input.turn_anchor_timestamp_ms = 400;
    input.environment_contexts = vec!["environment".to_string()];
    input.hook_contexts = vec!["active hook".to_string()];
    input.turn_context.model_visible_context = vec!["turn context".to_string()];
    input
        .attachments
        .push(crate::kernel::context_compiler::AgentContextAttachment {
            path: std::path::PathBuf::from("notes.md"),
            mime_type: None,
            size_bytes: None,
            sha256: None,
            metadata: std::collections::BTreeMap::new(),
        });

    let first = crate::kernel::context_compiler::AgentContextCompiler::compile(input.clone());
    let reopened = crate::kernel::context_compiler::AgentContextCompiler::compile(input);

    assert_eq!(first, reopened);
    assert_eq!(
        message_texts(&first.messages),
        vec![
            "Compacted conversation summary:\nold facts",
            "persisted hook",
            "active hook",
            "turn context",
            "<attachments>\npath=notes.md\n</attachments>",
        ]
    );
    assert_eq!(
        message_timestamps(&first.messages),
        vec![200, 300, 400, 400, 400]
    );
}

#[test]
fn compaction_entry_replaces_prior_model_visible_context() {
    let input = compile_input(
        vec![
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::user_text("old"),
            },
            verlet_history::SessionEntryKind::Compaction {
                summary: "old facts".to_string(),
            },
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::user_text("new"),
            },
        ],
        crate::kernel::context_compiler::AgentContextCompilePolicy::unbounded(),
    );

    let compiled = crate::kernel::context_compiler::AgentContextCompiler::compile(input);

    assert_eq!(
        message_texts(&compiled.messages),
        vec!["Compacted conversation summary:\nold facts", "new"]
    );
    assert_eq!(compiled.diagnostics.dropped_entries.len(), 1);
    assert_eq!(
        compiled.diagnostics.dropped_entries[0].reason,
        "cleared_by_compaction"
    );
}

#[test]
fn environment_and_attachment_inputs_are_explicit_context() {
    let mut input = compile_input(
        Vec::new(),
        crate::kernel::context_compiler::AgentContextCompilePolicy::unbounded(),
    );
    input.turn_context.cwd = Some(std::path::PathBuf::from("/tmp/work"));
    input
        .attachments
        .push(crate::kernel::context_compiler::AgentContextAttachment {
            path: std::path::PathBuf::from("notes.md"),
            mime_type: Some("text/markdown".to_string()),
            size_bytes: Some(42),
            sha256: None,
            metadata: std::collections::BTreeMap::new(),
        });

    let compiled = crate::kernel::context_compiler::AgentContextCompiler::compile(input);

    assert_eq!(compiled.diagnostics.attachment_count, 1);
    assert_eq!(
        message_texts(&compiled.messages),
        vec![
            "<environment_context>\ncwd=/tmp/work\n</environment_context>",
            "<attachments>\npath=notes.md mime_type=text/markdown size_bytes=42\n</attachments>"
        ]
    );
    assert_eq!(message_timestamps(&compiled.messages), vec![123, 123]);
}

#[test]
fn static_sources_prepend_system_blocks_in_pipeline_order() {
    let mut input = compile_input(
        Vec::new(),
        crate::kernel::context_compiler::AgentContextCompilePolicy::unbounded(),
    );
    input.static_system_sources = vec![
        static_source("identity", "Prompt identity."),
        static_source("persona", "Second static source."),
    ];

    let compiled = crate::kernel::context_compiler::AgentContextCompiler::compile(input);

    let system = compiled
        .system
        .iter()
        .map(|block| block.text.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        system,
        vec!["Prompt identity.", "Second static source.", "system"]
    );
    assert_eq!(compiled.diagnostics.system_block_count, 3);
}

#[test]
fn budget_shared_static_sources_consume_text_budget() {
    let mut input = compile_input(
        vec![verlet_history::SessionEntryKind::Message {
            message: verlet_history::CanonicalMessage::user_text("abcdef"),
        }],
        crate::kernel::context_compiler::AgentContextCompilePolicy {
            max_messages: None,
            max_text_bytes: Some(10),
        },
    );
    input.static_system_sources = vec![
        static_source("identity", "Pinned prompt."),
        budgeted_static_source("playbook", "123456"),
    ];

    let compiled = crate::kernel::context_compiler::AgentContextCompiler::compile(input);

    assert_eq!(
        compiled
            .system
            .iter()
            .map(|block| block.text.as_str())
            .collect::<Vec<_>>(),
        vec!["Pinned prompt.", "123456", "system"]
    );
    assert_eq!(message_texts(&compiled.messages), vec!["cdef"]);
    assert_eq!(compiled.diagnostics.retained_text_bytes, 10);
    assert_eq!(compiled.diagnostics.truncated_text_bytes, 2);
}

#[test]
fn fractional_static_sources_are_capped_before_history_budget() {
    let mut input = compile_input(
        vec![verlet_history::SessionEntryKind::Message {
            message: verlet_history::CanonicalMessage::user_text("abcdef"),
        }],
        crate::kernel::context_compiler::AgentContextCompilePolicy {
            max_messages: None,
            max_text_bytes: Some(10),
        },
    );
    input.static_system_sources =
        vec![budgeted_static_source_with_share("playbook", "123456", 0.3)];

    let compiled = crate::kernel::context_compiler::AgentContextCompiler::compile(input);

    assert_eq!(
        compiled
            .system
            .iter()
            .map(|block| block.text.as_str())
            .collect::<Vec<_>>(),
        vec!["123", "system"]
    );
    assert_eq!(message_texts(&compiled.messages), vec!["abcdef"]);
    assert_eq!(compiled.diagnostics.retained_text_bytes, 9);
    assert_eq!(compiled.diagnostics.truncated_text_bytes, 3);
}

#[test]
fn tiny_static_source_budget_truncates_on_utf8_boundary_without_false_retention() {
    let mut input = compile_input(
        vec![verlet_history::SessionEntryKind::Message {
            message: verlet_history::CanonicalMessage::user_text("abcdef"),
        }],
        crate::kernel::context_compiler::AgentContextCompilePolicy {
            max_messages: None,
            max_text_bytes: Some(1),
        },
    );
    input.static_system_sources = vec![budgeted_static_source("identity", "éabc")];

    let compiled = crate::kernel::context_compiler::AgentContextCompiler::compile(input);

    assert_eq!(
        compiled
            .system
            .iter()
            .map(|block| block.text.as_str())
            .collect::<Vec<_>>(),
        vec!["system"]
    );
    assert_eq!(message_texts(&compiled.messages), vec!["f"]);
    assert_eq!(compiled.diagnostics.retained_text_bytes, 1);
    assert_eq!(compiled.diagnostics.truncated_text_bytes, "éabc".len() + 5);
}

#[test]
fn static_source_prefix_that_trims_empty_does_not_consume_message_budget() {
    let mut input = compile_input(
        vec![verlet_history::SessionEntryKind::Message {
            message: verlet_history::CanonicalMessage::user_text("abcdef"),
        }],
        crate::kernel::context_compiler::AgentContextCompilePolicy {
            max_messages: None,
            max_text_bytes: Some(2),
        },
    );
    input.static_system_sources = vec![budgeted_static_source("identity", "  abc")];

    let compiled = crate::kernel::context_compiler::AgentContextCompiler::compile(input);

    assert_eq!(
        compiled
            .system
            .iter()
            .map(|block| block.text.as_str())
            .collect::<Vec<_>>(),
        vec!["system"]
    );
    assert_eq!(message_texts(&compiled.messages), vec!["ef"]);
    assert_eq!(compiled.diagnostics.retained_text_bytes, 2);
    assert_eq!(compiled.diagnostics.truncated_text_bytes, "  abc".len() + 4);
}

fn compile_input(
    kinds: Vec<verlet_history::SessionEntryKind>,
    policy: crate::kernel::context_compiler::AgentContextCompilePolicy,
) -> crate::kernel::context_compiler::AgentContextCompileInput {
    let coordinates = verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
    let entries = kinds
        .into_iter()
        .map(|kind| verlet_history::SessionEntry::new(coordinates.clone(), None, kind))
        .collect::<Vec<_>>();
    let thread = verlet_runtime_contracts::ThreadContext::root(coordinates);
    crate::kernel::context_compiler::AgentContextCompileInput {
        system: vec![verlet_provider::SystemBlock::text("system")],
        static_system_sources: Vec::new(),
        session_entries: entries,
        turn_anchor_timestamp_ms: 123,
        turn_context: crate::kernel::runtime_host::turn::TurnContextSnapshot {
            turn_id: "turn".to_string(),
            trace_id: "trace".to_string(),
            coordinates: thread.coordinates,
            parent_thread_id: thread.parent_thread_id,
            topology: thread.topology,
            cwd: None,
            workspace_roots: Vec::new(),
            model: Some("gpt-test".to_string()),
            provider: Some("openai".to_string()),
            thinking: None,
            permission_profile: None,
            provider_metadata: std::collections::BTreeMap::new(),
            metadata: std::collections::BTreeMap::new(),
            environment: std::collections::BTreeMap::new(),
            model_visible_context: Vec::new(),
            budget: verlet_runtime_contracts::TurnBudget::default(),
            cancellation_requested: false,
        },
        hook_contexts: Vec::new(),
        environment_contexts: Vec::new(),
        attachments: Vec::new(),
        tools: vec![verlet_provider::ToolDefinition::new(
            "tool",
            "tool",
            serde_json::json!({"type":"object"}),
        )],
        policy,
    }
}

fn user_message_at(text: &str, timestamp_ms: i64) -> verlet_history::CanonicalMessage {
    verlet_history::CanonicalMessage::User {
        content: vec![verlet_history::CanonicalContent::text(text)],
        timestamp_ms,
    }
}

fn message_timestamps(messages: &[verlet_history::CanonicalMessage]) -> Vec<i64> {
    messages
        .iter()
        .map(|message| match message {
            verlet_history::CanonicalMessage::User { timestamp_ms, .. }
            | verlet_history::CanonicalMessage::Assistant { timestamp_ms, .. }
            | verlet_history::CanonicalMessage::ToolResult { timestamp_ms, .. } => *timestamp_ms,
        })
        .collect()
}

fn static_source(
    id: &str,
    content: &str,
) -> crate::agent::manifest_bind::AgentManifestStaticContextSegment {
    crate::agent::manifest_bind::AgentManifestStaticContextSegment {
        id: id.to_string(),
        assembler: "kernel://assembler/static".to_string(),
        input: id.to_string(),
        pinned: true,
        budget_share: None,
        ref_uri: format!("resource://artifact/sha256:{}", "a".repeat(64)),
        content_sha256: verlet_agent::contracts::sha256_hex(content.as_bytes()),
        content: content.to_string(),
    }
}

fn budgeted_static_source(
    id: &str,
    content: &str,
) -> crate::agent::manifest_bind::AgentManifestStaticContextSegment {
    budgeted_static_source_with_share(id, content, 1.0)
}

fn budgeted_static_source_with_share(
    id: &str,
    content: &str,
    budget_share: f64,
) -> crate::agent::manifest_bind::AgentManifestStaticContextSegment {
    crate::agent::manifest_bind::AgentManifestStaticContextSegment {
        pinned: false,
        budget_share: Some(budget_share),
        ..static_source(id, content)
    }
}

fn message_texts(messages: &[verlet_history::CanonicalMessage]) -> Vec<String> {
    messages
        .iter()
        .map(|message| match message {
            verlet_history::CanonicalMessage::User { content, .. }
            | verlet_history::CanonicalMessage::Assistant { content, .. }
            | verlet_history::CanonicalMessage::ToolResult { content, .. } => content
                .iter()
                .filter_map(|content| match content {
                    verlet_history::CanonicalContent::Text { text, .. } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n"),
        })
        .collect()
}
