use super::*;

fn source() -> ThreadContractSource {
    ThreadContractSource::markdown(
        r#"---
name: release-verifier
kind: thread
version: 0
---

### Requires

- `branch`: git branch or ref to inspect
- `checks`: required verification commands as JSON array

### Ensures

- `verdict`: ship, hold, or needs-review
- `report`: concise evidence summary

### Tools

- `cli`: git
- `cli`: cargo
- `verlet`: spawn_thread

### Effects

- `artifact.report`: host-allocated text artifact

### Runtime

- `model`: reasoning
- `propagator`: llm
- `budget`: bounded

### Delegates

- `test-runner`: run verification commands

### Instructions

Prefer source-grounded findings.
"#,
    )
}

#[test]
fn compiler_lowers_markdown_thread_contract_to_stable_runtime_shape() {
    let contract = ThreadContractCompiler::compile(&source()).unwrap();

    assert_eq!(contract.kind, THREAD_CONTRACT_KIND);
    assert_eq!(contract.version, THREAD_CONTRACT_VERSION);
    assert_eq!(contract.name, "release-verifier");
    assert!(contract.source_hash.starts_with("sha256:"));
    assert_eq!(contract.requires.len(), 2);
    assert_eq!(contract.requires[1].value, ThreadContractValueKind::Json);
    assert_eq!(contract.ensures.len(), 2);
    assert_eq!(
        contract.capabilities[0],
        ThreadCapabilityRequirement {
            kind: "cli".to_string(),
            name: "git".to_string()
        }
    );
    assert_eq!(contract.effects[0].kind, "artifact.write");
    assert_eq!(contract.effects[0].binding, "host_allocated");
    let value = serde_json::to_value(&contract).unwrap();
    assert_eq!(value["requires"][0]["kind"], "text");
    assert!(value["requires"][0].get("value").is_none());
    assert_eq!(
        contract.runtime.get("model").map(String::as_str),
        Some("reasoning")
    );
    assert_eq!(
        contract.instructions.as_deref(),
        Some("Prefer source-grounded findings.")
    );
    contract.validate().unwrap();
    contract.contract_hash().unwrap();
}

#[test]
fn compiled_thread_contract_projects_to_abi_ports_and_capabilities() {
    let contract = ThreadContractCompiler::compile(&source()).unwrap();
    let projection = contract.abi_projection().unwrap();

    assert_eq!(projection.registered_name, "release-verifier");
    assert_eq!(projection.operation_name, "run_thread");
    assert_eq!(projection.source_ports.len(), 2);
    assert_eq!(projection.source_ports[1].value, AbiPortValue::Json);
    assert_eq!(projection.sink_ports.len(), 2);
    assert_eq!(projection.effect_ports.len(), 1);
    assert_eq!(
        projection.required_capabilities,
        vec![
            "cli:git".to_string(),
            "cli:cargo".to_string(),
            "verlet:spawn_thread".to_string(),
        ]
    );
}

#[test]
fn thread_declaration_requires_one_contract_source() {
    let declaration = ThreadDeclaration::new(
        ThreadContractReference::inline_markdown(source().source),
        ThreadInitialTurn::user("verify release"),
    );

    declaration.validate().unwrap();

    let invalid = ThreadDeclaration::new(
        ThreadContractReference {
            ref_path: Some("a".to_string()),
            inline: Some("b".to_string()),
            format: None,
            compiled: None,
        },
        ThreadInitialTurn::user("verify release"),
    );
    assert!(invalid.validate().is_err());
}

#[test]
fn thread_contracts_accept_legacy_agent_compatibility_shape() {
    let legacy_source = ThreadContractSource {
        format: ThreadContractSourceFormat::MarkdownV0,
        source: source().source.replace("kind: thread", "kind: agent"),
    };
    let mut contract = ThreadContractCompiler::compile(&legacy_source).unwrap();
    assert_eq!(contract.kind, THREAD_CONTRACT_KIND);

    contract.kind = LEGACY_AGENT_CONTRACT_KIND.to_string();
    contract.validate().unwrap();

    let mut declaration = ThreadDeclaration::new(
        ThreadContractReference {
            ref_path: None,
            inline: Some(legacy_source.source),
            format: Some(LEGACY_AGENT_CONTRACT_SOURCE_FORMAT.to_string()),
            compiled: None,
        },
        ThreadInitialTurn::user("verify release"),
    );
    declaration.kind = LEGACY_AGENT_THREAD_DECLARATION_KIND.to_string();
    declaration.validate().unwrap();
}
