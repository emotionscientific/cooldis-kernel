use sha2::Digest as _;
use std::io::Read as _;
use std::io::Write as _;

#[path = "support/model_catalog.rs"]
mod model_catalog_test_support;

const TEST_OPERATION_HASH: &str =
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

#[test]
fn verlet_cli_tool_help_is_canonical() {
    let help = run_verlet(["tool", "--help"]);
    assert!(help.contains("verlet tool"));
    assert!(help.contains("verlet tool list"));
    assert!(help.contains("verlet tool publish"));
    assert!(help.contains("verlet tool manual"));

    let list_help = run_verlet(["tool", "list", "--help"]);
    assert!(list_help.contains("verlet tool list"));
    assert!(list_help.contains("--registry-root"));

    let publish_help = run_verlet(["tool", "publish", "--help"]);
    assert!(publish_help.contains("verlet tool publish --package"));
    assert!(!publish_help.contains("--module-path"));
    assert!(!publish_help.contains("--bin-path"));

    let run_help = run_verlet(["tool", "run", "--help"]);
    assert!(run_help.contains("verlet tool run"));

    let manual_help = run_verlet(["tool", "manual", "--help"]);
    assert!(manual_help.contains("verlet tool manual <published-name>"));

    let unknown = run_verlet_failed(["hello"]);
    assert!(stderr(&unknown).contains("unknown command"));
}

#[test]
fn verlet_cli_tool_list_and_legacy_bare_lookup_use_canonical_kernel_package_names() {
    let registry_root = temp_dir("canonical-kernel-package-list");
    verlet::operations::kernel_packages::ensure_verlet_threads_published(Some(&registry_root))
        .unwrap();
    verlet::operations::kernel_packages::ensure_verlet_schedule_published(Some(&registry_root))
        .unwrap();
    verlet::operations::kernel_packages::ensure_verlet_process_published(Some(&registry_root))
        .unwrap();
    verlet::operations::kernel_packages::ensure_verlet_notify_published(Some(&registry_root))
        .unwrap();

    let list = run_verlet([
        "tool",
        "list",
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    for canonical in [
        "verlet-threads",
        "verlet-schedule",
        "verlet-process",
        "verlet-notify",
    ] {
        assert!(list.contains(canonical), "tool list:\n{list}");
    }
    for legacy in [
        concat!("cool", "dis-threads"),
        concat!("cool", "dis-schedule"),
        concat!("cool", "dis-process"),
        concat!("cool", "dis-notify"),
    ] {
        assert!(!list.contains(legacy), "tool list:\n{list}");
    }

    let manual = run_verlet([
        "tool",
        "manual",
        concat!("cool", "dis-threads"),
        "thread_spawn",
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    assert!(manual.contains("verlet-threads thread_spawn"), "{manual}");
}

#[test]
fn verlet_cli_import_help_is_canonical() {
    let help = run_verlet(["import", "--help"]);
    assert!(help.contains("verlet import build"));
    assert!(help.contains("verlet import publish"));

    let build = run_verlet(["import", "build", "--help"]);
    assert!(build.contains("verlet import build --package"));

    let publish = run_verlet(["import", "publish", "--help"]);
    assert!(publish.contains("verlet import publish --package"));
    assert!(publish.contains("--registry-root"));

    let unused_registry = run_verlet_failed([
        "import",
        "build",
        "--registry-root",
        "/tmp/unused-import-registry",
    ]);
    assert!(
        stderr(&unused_registry).contains("does not accept --registry-root"),
        "{}",
        stderr(&unused_registry)
    );
}

#[test]
fn openapi_import_build_is_deterministic_and_publish_records_provenance() {
    let root = temp_dir("openapi-import-determinism");
    let package = write_openapi_import_package(&root, "https://api.example.com", "", "200");
    let first_source =
        verlet_operations::import_package::ImportPackageSource::load(&package).unwrap();
    let first_plan =
        verlet_operations::openapi_plan::OperationImportPlan::from_package(&first_source).unwrap();
    let first_artifact =
        verlet::operations::openapi_import::render_openapi_import_artifact(&first_plan).unwrap();
    let second_source =
        verlet_operations::import_package::ImportPackageSource::load(&package).unwrap();
    let second_plan =
        verlet_operations::openapi_plan::OperationImportPlan::from_package(&second_source).unwrap();
    let second_artifact =
        verlet::operations::openapi_import::render_openapi_import_artifact(&second_plan).unwrap();
    assert_eq!(first_artifact, second_artifact);

    let build = run_verlet(["import", "build", "--package", package.to_str().unwrap()]);
    assert!(build.contains("receipt import_build_v0"));
    assert!(build.contains(&format!("spec_sha256 {}", first_source.spec_sha256)));

    let registry_root = root.join("operations");
    let publish = run_verlet([
        "import",
        "publish",
        "--package",
        package.to_str().unwrap(),
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    assert!(publish.contains("published catalog"));
    let record = verlet_operations::operation_store::LocalOperationRegistry::new(&registry_root)
        .load_record("catalog")
        .unwrap();
    assert_eq!(
        record.active_artifact_hash,
        format!("{:x}", sha2::Sha256::digest(first_artifact))
    );
    assert!(matches!(
        record.source,
        verlet_operations::operation_store::PublishedOperationSource::Import {
            spec_sha256,
            ..
        } if spec_sha256 == first_source.spec_sha256
    ));
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn openapi_import_publish_gate_rejects_missing_network_and_secret_grants() {
    let root = temp_dir("openapi-import-grants");
    let package = write_openapi_import_package(
        &root,
        "https://api.example.com",
        "[auth]\nscheme = \"apiKey\"\nheader = \"x-api-key\"\nsecret = \"SEARCH_API_KEY\"\n",
        "200",
    );
    let source = verlet_operations::import_package::ImportPackageSource::load(&package).unwrap();
    let plan = verlet_operations::openapi_plan::OperationImportPlan::from_package(&source).unwrap();
    let artifact_path = root.join("catalog.wasm");
    std::fs::write(
        &artifact_path,
        verlet::operations::openapi_import::render_openapi_import_artifact(&plan).unwrap(),
    )
    .unwrap();
    let registry =
        verlet_operations::operation_store::LocalOperationRegistry::new(root.join("operations"));
    let request = |capability_grants| verlet_operations::operation_store::PublishOperationRequest {
        name: "catalog".to_string(),
        artifact_path: artifact_path.clone(),
        source: verlet_operations::operation_store::PublishedOperationSource::Import {
            manifest_path: package.clone(),
            spec_sha256: source.spec_sha256.clone(),
        },
        interface: None,
        capability_grants,
        metadata: std::collections::BTreeMap::new(),
    };

    let missing_network = registry
        .publish_artifact(request(std::collections::BTreeSet::new()))
        .await
        .unwrap_err()
        .to_string();
    assert!(missing_network.contains("net.http:POST:https://api.example.com"));

    let missing_secret = registry
        .publish_artifact(request(std::collections::BTreeSet::from([
            "net.http:POST:https://api.example.com".to_string(),
        ])))
        .await
        .unwrap_err()
        .to_string();
    assert!(missing_secret.contains("secret:SEARCH_API_KEY"));
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn openapi_import_publishes_and_runs_through_the_existing_cli_path() {
    let (base_url, server) = spawn_openapi_import_server(
        200,
        r#"{"results":[{"title":"Verlet runtime"}]}"#,
        Some("x-api-key: fixture-secret"),
    );
    let root = temp_dir("openapi-import-run");
    let package = write_openapi_import_package(
        &root,
        &base_url,
        "[auth]\nscheme = \"apiKey\"\nheader = \"x-api-key\"\nsecret = \"SEARCH_API_KEY\"\n",
        "200",
    );
    let registry_root = root.join("operations");
    let state_home = root.join("state");

    run_verlet([
        "import",
        "publish",
        "--package",
        package.to_str().unwrap(),
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    run_verlet_with_stdin(
        [
            "secret",
            "set",
            "SEARCH_API_KEY",
            "--value-stdin",
            "--state-home",
            state_home.to_str().unwrap(),
        ],
        "fixture-secret",
    );
    let output = run_verlet([
        "tool",
        "run",
        "catalog",
        "search",
        "--input",
        r#"{"query":"verlet"}"#,
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--state-home",
        state_home.to_str().unwrap(),
    ]);

    assert!(output.contains(r#""status":200"#), "{output}");
    assert!(output.contains("Verlet runtime"), "{output}");
    assert!(output.contains(r#""truncated":false"#), "{output}");
    server.join().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn openapi_import_returns_http_500_as_operation_output() {
    let (base_url, server) = spawn_openapi_import_server(500, r#"{"error":"upstream"}"#, None);
    let root = temp_dir("openapi-import-500");
    let package = write_openapi_import_package(&root, &base_url, "", "500");
    let registry_root = root.join("operations");

    run_verlet([
        "import",
        "publish",
        "--package",
        package.to_str().unwrap(),
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    let output = run_verlet([
        "tool",
        "run",
        "catalog",
        "search",
        "--input",
        r#"{"query":"verlet"}"#,
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);

    assert!(output.contains(r#""status":500"#), "{output}");
    assert!(output.contains("upstream"), "{output}");
    server.join().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn verlet_cli_uses_clean_public_entrypoints() {
    let root = run_verlet([]);
    assert!(root.contains("verlet init"));
    assert!(root.contains("verlet console"));
    assert!(root.contains("verlet chat"));
    assert!(root.contains("verlet commands"));
    assert!(root.contains("verlet help"));
    assert!(root.contains("man verlet"));
    assert_no_command(&root, &["dev"]);
    assert_no_command(&root, &["operator"]);
    assert!(!root.contains("verlet [PROMPT]"));

    let commands = run_verlet(["commands"]);
    assert!(commands.contains("verlet agent"));
    assert!(commands.contains("verlet tool"));
    assert!(commands.contains("verlet skill"));
    assert!(commands.contains("verlet auth"));
    assert!(commands.contains("verlet secret"));
    assert!(commands.contains("verlet rpc"));
    assert!(commands.contains("verlet console"));
    assert!(commands.contains("verlet chat [PROMPT]"));
    assert!(commands.contains("verlet debug rpc call"));
    assert!(commands.contains("verlet debug bind"));
    assert!(commands.contains("verlet tool manual"));
    assert!(commands.contains("verlet skill publish"));
    assert!(commands.contains("verlet skill import"));
    assert!(commands.contains("verlet auth set"));
    assert!(commands.contains("verlet agent versions <name> [--json]"));
    assert!(commands.contains("verlet agent diff <name> --from"));
    assert_no_command(&commands, &["dev"]);
    assert_no_command(&commands, &["operator"]);

    let chat_help = run_verlet(["chat", "--help"]);
    assert!(chat_help.contains("verlet chat"));
    assert!(chat_help.contains("bundled local terminal console"));

    let help_chat = run_verlet(["help", "chat"]);
    assert!(help_chat.contains("verlet chat"));

    let versions_help = run_verlet(["help", "agent", "versions"]);
    assert!(versions_help.contains("verlet agent versions <name> [--json]"));
    let diff_help = run_verlet(["help", "agent", "diff"]);
    assert!(diff_help.contains("--from <version>[:authored|:resolved]"));

    let help_auth = run_verlet(["help", "auth"]);
    assert!(help_auth.contains("verlet auth"));

    let help_tool_manual = run_verlet(["help", "tool", "manual"]);
    assert!(help_tool_manual.contains("verlet tool manual"));

    let skill_help = run_verlet(["skill", "--help"]);
    assert!(skill_help.contains("verlet skill publish"));

    let skill_publish_help = run_verlet(["skill", "publish", "--help"]);
    assert!(skill_publish_help.contains("verlet skill publish <dir>"));
    assert!(skill_publish_help.contains("--registry-root"));
    assert!(skill_publish_help.contains("floating package-name ref"));

    let skill_import_help = run_verlet(["skill", "import", "--help"]);
    assert!(skill_import_help.contains("verlet skill import <dir>"));
    assert!(skill_import_help.contains("--blob-registry-root"));
    assert!(skill_import_help.contains("--dry-run"));
    assert!(skill_import_help.contains("model-visible index"));
    assert!(skill_import_help.contains("skipped and reported"));

    let help_debug_rpc = run_verlet(["help", "debug", "rpc"]);
    assert!(help_debug_rpc.contains("verlet debug rpc"));

    let help_debug_bind = run_verlet(["help", "debug", "bind"]);
    assert!(help_debug_bind.contains("verlet debug bind"));

    let rpc = run_verlet(["rpc", "--help"]);
    assert!(rpc.contains("verlet rpc"));
    assert!(rpc.contains("--listen"));
    assert!(rpc.contains("--state-home"));
    assert!(rpc.contains("--runtime-home"));
    assert!(rpc.contains("--cwd"));
    assert!(rpc.contains("fresh temporary state home"));

    let console = run_verlet(["console", "--help"]);
    assert!(console.contains("local browser console"));
    assert!(console.contains("--no-open"));
    assert!(console.contains("--port"));

    let auth = run_verlet(["auth", "--help"]);
    assert!(auth.contains("verlet auth set"));
    assert!(auth.contains("verlet auth status"));

    let debug_rpc = run_verlet(["debug", "rpc", "--help"]);
    assert!(debug_rpc.contains("Protocol-level debug client"));

    let debug_bind = run_verlet(["debug", "bind", "--help"]);
    assert!(debug_bind.contains("recorded manifest compile and bind"));

    let old_tool_plan = run_verlet_failed(["tool", "plan", "--help"]);
    assert!(stderr(&old_tool_plan).contains("unknown tool subcommand"));
}

#[test]
fn verlet_cli_init_creates_folder_first_agent_project() {
    let workspace = temp_dir("agent-init-folder");
    let project = workspace.join("release-verifier");

    let output = run_verlet([
        "init",
        "release-verifier",
        "--out",
        project.to_str().unwrap(),
    ]);
    assert!(output.contains(project.to_str().unwrap()));

    let manifest_path = project.join("verlet.agent.toml");
    let prompt_path = project.join("prompts/system.md");
    let refs_path = project.join("components/operations.toml");
    let coupling_refs_path = project.join("components/couplings.toml");
    let operation_slot_path = project.join("operations/README.md");
    assert!(manifest_path.exists());
    assert!(prompt_path.exists());
    assert!(refs_path.exists());
    assert!(coupling_refs_path.exists());
    assert!(operation_slot_path.exists());

    let manifest = std::fs::read_to_string(&manifest_path).unwrap();
    assert!(manifest.contains("name = \"release-verifier\""));
    assert!(manifest.contains("kind = \"verlet.agent-manifest\""));
    assert!(manifest.contains("provider_ref = \"provider://local_offline\""));
    assert!(!manifest.contains("0000000000000000000000000000000000000000000000000000000000000000"));
    let prompt = std::fs::read_to_string(&prompt_path).unwrap();
    assert!(prompt.contains("You are the release-verifier agent."));
    let refs = std::fs::read_to_string(&refs_path).unwrap();
    assert!(refs.contains("component-first"));
    let coupling_refs = std::fs::read_to_string(&coupling_refs_path).unwrap();
    assert!(coupling_refs.contains("std::queue.task"));
    assert!(coupling_refs.contains("std::context.spill"));
    assert!(coupling_refs.contains("channel_decision_required = true"));

    let registry_root = temp_dir("agent-init-registry");
    let missing_operation_registry_root = workspace.join("missing-operations");
    let plan = run_verlet([
        "agent",
        "plan",
        manifest_path.to_str().unwrap(),
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--operations-registry-root",
        missing_operation_registry_root.to_str().unwrap(),
    ]);
    assert!(plan.contains("agent://release-verifier@0.1.0"));
    assert!(plan.contains("context_source: identity -> resource://artifact/sha256:"));
    assert!(plan.contains("resources: 1"));

    let publish = run_verlet([
        "agent",
        "publish",
        manifest_path.to_str().unwrap(),
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--operations-registry-root",
        missing_operation_registry_root.to_str().unwrap(),
    ]);
    assert!(publish.contains("published agent://release-verifier@0.1.0"));
    assert!(publish.contains("context_source: identity -> resource://artifact/sha256:"));
    let record = agent_record(&registry_root, "release-verifier");
    assert_eq!(record.resource_count, 1);
    assert!(
        record
            .resolved_refs
            .iter()
            .any(|resolved| resolved.declared.starts_with("resource://artifact/sha256:"))
    );

    let duplicate = run_verlet_failed([
        "init",
        "release-verifier",
        "--out",
        project.to_str().unwrap(),
    ]);
    assert!(stderr(&duplicate).contains("already exists"));
}

#[test]
fn verlet_cli_agent_plan_publish_accepts_explicit_folder_first_context() {
    let workspace = temp_dir("agent-explicit-folder-context");
    let project = workspace.join("explicit-runner");
    std::fs::create_dir_all(project.join("prompts")).unwrap();
    let prompt_text = "You are the explicit folder-first runner.\n";
    std::fs::write(project.join("prompts/system.md"), prompt_text).unwrap();
    let manifest_path = project.join("verlet.agent.toml");
    std::fs::write(
        &manifest_path,
        r#"
[agent]
name = "explicit-runner"
version = "0.1.0"
description = "Uses an explicit folder-first context pipeline."
kind = "verlet.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[runtime]
default_cwd = "."
streaming = false

[context]
[[context.pipelines]]
id = "default"

[[context.pipelines.sources]]
id = "identity"
assembler = "kernel://assembler/static"
pinned = true

[[context.pipelines.sources]]
id = "history"
assembler = "kernel://assembler/anchored-window"
select = { stream = "thread", since = "anchor|start" }
budget_share = 0.75
"#,
    )
    .unwrap();
    let registry_root = workspace.join("agents");
    let operation_registry_root = workspace.join("operations");

    let plan = run_verlet([
        "agent",
        "plan",
        manifest_path.to_str().unwrap(),
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--operations-registry-root",
        operation_registry_root.to_str().unwrap(),
    ]);
    let plan_context_line = plan
        .lines()
        .find(|line| line.starts_with("context_source: identity -> "))
        .unwrap_or_else(|| panic!("plan output did not include identity context source:\n{plan}"))
        .to_string();
    assert!(plan.contains("resources: 1"));

    let publish = run_verlet([
        "agent",
        "publish",
        manifest_path.to_str().unwrap(),
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--operations-registry-root",
        operation_registry_root.to_str().unwrap(),
    ]);
    assert!(publish.contains("published agent://explicit-runner@0.1.0"));
    assert!(publish.contains(&plan_context_line));

    let record = agent_record(&registry_root, "explicit-runner");
    assert_eq!(record.resource_count, 1);
    let prompt_ref = record.resolved_manifest["resources"][0]["ref"]
        .as_str()
        .unwrap();
    assert!(plan_context_line.contains(prompt_ref));
    assert_eq!(
        record.resolved_manifest["context"]["sources"][1]["budget_share"].as_f64(),
        Some(0.75)
    );
    let (_prompt_record, published_prompt) =
        verlet_operations::blob_store::LocalBlobRegistry::new(workspace.join("blobs"))
            .load_text_ref(prompt_ref)
            .unwrap();
    assert_eq!(published_prompt, prompt_text);
    let _ = std::fs::remove_dir_all(workspace);
}

#[test]
fn verlet_cli_blob_publish_is_idempotent() {
    let root = temp_dir("blob-publish-cli");
    let file = root.join("system.md");
    std::fs::write(&file, "Prompt text for the model.\n").unwrap();
    let registry_root = root.join("blobs");

    let first = run_verlet([
        "blob",
        "publish",
        file.to_str().unwrap(),
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--name",
        "identity",
    ]);
    let second = run_verlet([
        "blob",
        "publish",
        file.to_str().unwrap(),
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--name",
        "identity",
    ]);

    let first_ref = output_line_suffix(&first, "ref ");
    let second_ref = output_line_suffix(&second, "ref ");
    assert_eq!(first_ref, second_ref);
    assert!(first_ref.starts_with("resource://artifact/sha256:"));
    let artifact = output_line_suffix(&first, "artifact ");
    assert!(
        registry_root
            .join("records/artifact")
            .join(format!("sha256-{artifact}.json"))
            .exists()
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn verlet_cli_agent_run_uses_registry_relative_blob_root() {
    let root = temp_dir("agent-run-registry-relative-blob");
    let project = root.join("proj");
    let agent_registry_root = root.join("ags");
    let operation_registry_root = root.join("ops");

    run_verlet([
        "agent",
        "init",
        "smokey",
        "--out",
        project.to_str().unwrap(),
    ]);
    let manifest_path = project.join("verlet.agent.toml");
    let prompt_text = std::fs::read_to_string(project.join("prompts/system.md")).unwrap();
    assert!(prompt_text.contains("You are the smokey agent."));

    let publish = run_verlet([
        "agent",
        "publish",
        manifest_path.to_str().unwrap(),
        "--registry-root",
        agent_registry_root.to_str().unwrap(),
        "--operations-registry-root",
        operation_registry_root.to_str().unwrap(),
    ]);
    assert!(publish.contains("published agent://smokey@0.1.0"));
    assert!(publish.contains("context_source: identity -> resource://artifact/sha256:"));

    let record = agent_record(&agent_registry_root, "smokey");
    let prompt_ref = record
        .resolved_refs
        .iter()
        .find(|resolved| resolved.declared.starts_with("resource://artifact/sha256:"))
        .expect("folder-first prompt should publish a blob resource")
        .declared
        .clone();
    let (_prompt_record, published_prompt) =
        verlet_operations::blob_store::LocalBlobRegistry::new(agent_registry_root.join("blobs"))
            .load_text_ref(&prompt_ref)
            .unwrap();
    assert_eq!(published_prompt, prompt_text);

    let run = run_verlet([
        "agent",
        "run",
        "agent://smokey@latest",
        "--input",
        "who are you",
        "--registry-root",
        agent_registry_root.to_str().unwrap(),
    ]);
    assert!(run.contains("local:who are you"));
    assert!(run.contains("manifest.compile.completed:"));
    assert!(run.contains("manifest.bind.completed:"));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn verlet_cli_agent_init_out_toml_keeps_single_file_compatibility() {
    let workspace = temp_dir("agent-init-single-file");
    let manifest_path = workspace.join("single-agent.verlet.agent.toml");

    let output = run_verlet([
        "agent",
        "init",
        "single-agent",
        "--out",
        manifest_path.to_str().unwrap(),
    ]);
    assert!(output.contains(manifest_path.to_str().unwrap()));
    assert!(manifest_path.exists());
    assert!(!workspace.join("prompts").exists());

    let manifest = std::fs::read_to_string(&manifest_path).unwrap();
    assert!(manifest.contains("name = \"single-agent\""));
    assert!(manifest.contains("verlet.agent-manifest"));
}

#[test]
fn verlet_cli_secret_import_list_and_status_redact_values() {
    let state_home = temp_dir("secret-state");

    let import = run_verlet_with_env(
        [
            "secret",
            "import",
            "EXAMPLE_API_KEY",
            "--from-env",
            "VERLET_TEST_EXAMPLE_API_KEY",
            "--state-home",
            state_home.to_str().unwrap(),
        ],
        &[("VERLET_TEST_EXAMPLE_API_KEY", "fixture-secret")],
    );
    assert!(import.contains("imported secret EXAMPLE_API_KEY"));
    assert!(!import.contains("fixture-secret"));

    let list = run_verlet([
        "secret",
        "list",
        "--state-home",
        state_home.to_str().unwrap(),
    ]);
    assert!(list.contains("EXAMPLE_API_KEY"));
    assert!(list.contains("env:VERLET_TEST_EXAMPLE_API_KEY"));
    assert!(!list.contains("fixture-secret"));

    let status = run_verlet([
        "secret",
        "status",
        "EXAMPLE_API_KEY",
        "--state-home",
        state_home.to_str().unwrap(),
    ]);
    assert!(status.contains(r#""name": "EXAMPLE_API_KEY""#));
    assert!(status.contains(r#""redacted": true"#));
    assert!(!status.contains("fixture-secret"));
}

#[test]
fn verlet_cli_skill_publish_writes_deterministic_package() {
    let root = temp_dir("skill-publish-cli");
    let package_dir = root.join("karl-skills");
    write_skill_fixture(
        &package_dir,
        "frontmatter",
        r#"---
name: frontmatter-skill
description: Uses declared metadata.
trigger_hint: when metadata matters
---
# Frontmatter Skill

Body with metadata.
"#,
    );
    write_skill_fixture(
        &package_dir,
        "plain",
        r#"# Plain Skill

First plain description line.

More body.
"#,
    );
    write_skill_fixture(
        &package_dir,
        "設計",
        r#"# 設計

Unicode description line.
"#,
    );
    let registry_root = root.join("skills-registry");

    let first = run_verlet([
        "skill",
        "publish",
        package_dir.to_str().unwrap(),
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    let second = run_verlet([
        "skill",
        "publish",
        package_dir.to_str().unwrap(),
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    let first_hash = skill_artifact_hash(&first);
    let second_hash = skill_artifact_hash(&second);

    assert_eq!(first_hash, second_hash);
    assert!(first.contains("published karl-skills"));
    assert!(first.contains("skill frontmatter-skill"));
    assert!(first.contains("skill plain"));
    assert!(first.contains("skill 設計"));
    assert!(first.contains(&format!("ref skill://karl-skills@sha256:{first_hash}")));
    assert!(first.contains("floating skill://karl-skills"));

    let record = verlet_operations::skill_package::LocalSkillRegistry::new(&registry_root)
        .load_record("karl-skills")
        .unwrap();
    assert_eq!(record.active_artifact_hash, first_hash);
    assert_eq!(
        record.package.render_index(),
        "frontmatter-skill — Uses declared metadata.\nplain — First plain description line.\n設計 — Unicode description line.\n"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn verlet_cli_skill_import_dry_run_and_publish_are_deterministic() {
    let root = temp_dir("skill-import-cli");
    let skill_dir = root.join("fixture-skill");
    std::fs::create_dir_all(skill_dir.join("references")).unwrap();
    std::fs::create_dir_all(skill_dir.join("assets")).unwrap();
    std::fs::create_dir_all(skill_dir.join("scripts")).unwrap();
    std::fs::write(
        skill_dir.join("SKILL.md"),
        "# Fixture Skill\n\nFixture description.\n",
    )
    .unwrap();
    std::fs::write(skill_dir.join("references/guide.md"), "# Guide\n").unwrap();
    std::fs::write(skill_dir.join("assets/icon.bin"), [0_u8, 1, 2, 3]).unwrap();
    std::fs::write(skill_dir.join("scripts/check.py"), "print('check')\n").unwrap();
    std::fs::write(skill_dir.join("hooks.json"), r#"{"hooks": []}"#).unwrap();
    std::fs::write(skill_dir.join("README.md"), "not imported\n").unwrap();
    let skill_registry = root.join("skills");
    let blob_registry = root.join("blobs");

    let dry_run = run_verlet([
        "skill",
        "import",
        skill_dir.to_str().unwrap(),
        "--registry-root",
        skill_registry.to_str().unwrap(),
        "--blob-registry-root",
        blob_registry.to_str().unwrap(),
        "--dry-run",
    ]);
    assert!(dry_run.contains("dry-run fixture-skill"));
    assert!(dry_run.contains("omitted script scripts/check.py"));
    assert!(dry_run.contains("ignored hook hooks.json"));
    assert!(dry_run.contains("skipped file README.md"));
    assert!(dry_run.contains("blob assets/icon.bin resource://artifact/sha256:"));
    assert!(dry_run.contains("[[resources]]"));
    assert!(dry_run.contains("kind = \"skill\""));
    assert!(dry_run.contains("kind = \"blob\""));
    assert!(!skill_registry.exists());
    assert!(!blob_registry.exists());

    let invalid_skill_dir = root.join("invalid-skill");
    std::fs::create_dir_all(&invalid_skill_dir).unwrap();
    std::fs::write(invalid_skill_dir.join("SKILL.md"), "").unwrap();
    let invalid_skill_registry = root.join("invalid-skills");
    let invalid_blob_registry = root.join("invalid-blobs");
    let failed_dry_run = run_verlet_failed([
        "skill",
        "import",
        invalid_skill_dir.to_str().unwrap(),
        "--registry-root",
        invalid_skill_registry.to_str().unwrap(),
        "--blob-registry-root",
        invalid_blob_registry.to_str().unwrap(),
        "--dry-run",
    ]);
    assert!(stderr(&failed_dry_run).contains("is empty"));
    assert!(!invalid_skill_registry.exists());
    assert!(!invalid_blob_registry.exists());

    let first = run_verlet([
        "skill",
        "import",
        skill_dir.to_str().unwrap(),
        "--registry-root",
        skill_registry.to_str().unwrap(),
        "--blob-registry-root",
        blob_registry.to_str().unwrap(),
    ]);
    let second = run_verlet([
        "skill",
        "import",
        skill_dir.to_str().unwrap(),
        "--registry-root",
        skill_registry.to_str().unwrap(),
        "--blob-registry-root",
        blob_registry.to_str().unwrap(),
    ]);
    let first_hash = skill_artifact_hash(&first);
    let second_hash = skill_artifact_hash(&second);
    assert_eq!(first, second);
    assert_eq!(first_hash, second_hash);
    assert!(first.contains("published fixture-skill"));
    assert!(first.contains(&format!("ref skill://fixture-skill@sha256:{first_hash}")));
    assert!(first.contains(&format!(
        "record {}",
        skill_registry.join("records/fixture-skill.json").display()
    )));
    assert_eq!(
        std::fs::read_dir(skill_registry.join("versions/fixture-skill"))
            .unwrap()
            .count(),
        1
    );
    assert_eq!(
        std::fs::read_dir(blob_registry.join("records/artifact"))
            .unwrap()
            .count(),
        1
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn verlet_cli_tool_run_reports_missing_registered_operation_secret_refs() {
    let registry_root = temp_dir("tool-run-missing-secret-registry");
    let state_home = temp_dir("tool-run-missing-secret-state");
    seed_operation_record(
        &registry_root,
        "secret-search",
        TEST_OPERATION_HASH,
        &[("search", &["secret:EXAMPLE_API_KEY"])],
    );

    let failed = run_verlet_failed([
        "tool",
        "run",
        "secret-search",
        "search",
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--state-home",
        state_home.to_str().unwrap(),
        "--input",
        r#"{"query":"verlet"}"#,
    ]);
    let err = stderr(&failed);

    assert!(err.contains("missing required operation secrets: EXAMPLE_API_KEY"));
    assert!(err.contains("verlet secret import"));
    assert!(!err.contains("fixture-secret"));
}

#[tokio::test]
async fn verlet_cli_auth_set_status_and_delete_redact_values() {
    let state_home = temp_dir("auth-state");

    // The store no longer ships an api-key provider by default (EMO-575), so
    // pre-create the record the CLI credential commands operate on.
    {
        use verlet_metadata::provider_store::LlmProviderCatalogStore as _;

        let store = verlet_metadata::provider_store::SqliteMetadataStore::open(
            state_home.join("metadata.sqlite3"),
        )
        .await
        .unwrap();
        let mut provider = verlet_metadata::provider_store::example_openai_compatible_record();
        provider.base_url = "https://llm.internal.example/v1".to_string();
        store.upsert_provider(provider).await.unwrap();
    }

    let set = run_verlet_with_stdin(
        [
            "auth",
            "set",
            "openai_compatible",
            "--api-key-stdin",
            "--state-home",
            state_home.to_str().unwrap(),
        ],
        "fixture-provider-key\n",
    );
    assert!(set.contains("stored provider credential openai_compatible"));
    assert!(!set.contains("fixture-provider-key"));

    let status = run_verlet([
        "auth",
        "status",
        "openai_compatible",
        "--state-home",
        state_home.to_str().unwrap(),
    ]);
    assert!(status.contains(r#""provider_id": "openai_compatible""#));
    assert!(status.contains(r#""configured": true"#));
    assert!(status.contains(r#""source": "stored""#));
    assert!(!status.contains("fixture-provider-key"));

    let delete = run_verlet([
        "auth",
        "delete",
        "openai_compatible",
        "--state-home",
        state_home.to_str().unwrap(),
    ]);
    assert!(delete.contains("deleted provider credential openai_compatible"));

    let status = run_verlet([
        "auth",
        "status",
        "openai_compatible",
        "--state-home",
        state_home.to_str().unwrap(),
    ]);
    assert!(status.contains(r#""configured": false"#));
}

#[test]
fn verlet_cli_tool_source_add_list_show_and_remove_redacts_remote_mcp_auth() {
    let state_home = temp_dir("tool-source-state");

    let add = run_verlet([
        "tool",
        "source",
        "add",
        "arcade",
        "--kind",
        "mcp-http",
        "--url",
        "https://mcp.example.test/arcade",
        "--bearer-secret",
        "arcade.api_key",
        "--header",
        "x-tenant=demo",
        "--include-tool",
        "gmail_search",
        "--state-home",
        state_home.to_str().unwrap(),
    ]);
    assert!(add.contains("stored tool source arcade"));

    let list = run_verlet([
        "tool",
        "source",
        "list",
        "--json",
        "--state-home",
        state_home.to_str().unwrap(),
    ]);
    assert!(list.contains(r#""name": "arcade""#));
    assert!(list.contains(r#""secret": "arcade.api_key""#));
    assert!(list.contains(r#""type": "bearer_secret""#));
    assert!(!list.contains("demo"));

    let sources: Vec<serde_json::Value> = serde_json::from_str(&list).unwrap();
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0]["name"], "arcade");
    assert_eq!(sources[0]["transport"], "streamable_http");
    assert_eq!(sources[0]["include_tools"][0], "gmail_search");
    assert_eq!(sources[0]["auth"]["secret"], "arcade.api_key");
    assert_eq!(sources[0]["auth"]["value"]["redacted"], true);
    assert_eq!(sources[0]["headers"][0]["name"], "x-tenant");
    assert_eq!(sources[0]["headers"][0]["value"]["redacted"], true);

    let show = run_verlet([
        "tool",
        "source",
        "show",
        "arcade",
        "--json",
        "--state-home",
        state_home.to_str().unwrap(),
    ]);
    let source: serde_json::Value = serde_json::from_str(&show).unwrap();
    assert_eq!(source["name"], "arcade");
    assert_eq!(source["auth"]["secret"], "arcade.api_key");
    assert_eq!(source["auth"]["value"]["redacted"], true);
    assert_eq!(source["headers"][0]["name"], "x-tenant");
    assert_eq!(source["headers"][0]["value"]["redacted"], true);

    let remove = run_verlet([
        "tool",
        "source",
        "remove",
        "arcade",
        "--state-home",
        state_home.to_str().unwrap(),
    ]);
    assert!(remove.contains("removed tool source arcade"));

    let empty = run_verlet([
        "tool",
        "source",
        "list",
        "--json",
        "--state-home",
        state_home.to_str().unwrap(),
    ]);
    assert_eq!(empty.trim(), "[]");
}

#[test]
fn verlet_cli_tool_build_gates_stateless_conversion() {
    let repo = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let module_path = repo.join("tests/fixtures/wasm-csv-profile");

    let build = run_verlet([
        "tool",
        "build",
        "--module-path",
        module_path.to_str().unwrap(),
        "--name",
        "data",
        "--upstream-url",
        "https://github.com/emotionscientific/verlet",
        "--upstream-rev",
        "fixture",
        "--upstream-crate",
        "verlet-wasm-csv-profile",
    ]);
    assert!(build.contains("tool build data"));
    assert!(build.contains("conversion stateless_wasm"));
    assert!(build.contains("policy accepted"));
    assert!(build.contains("operation csv_profile json -> json"));

    let registry_root = temp_dir("strict-conversion-registry");
    let publish = run_verlet_failed([
        "tool",
        "publish",
        "--module-path",
        module_path.to_str().unwrap(),
        "--name",
        "data",
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--strict-conversion",
        "--upstream-url",
        "https://github.com/emotionscientific/verlet",
        "--upstream-rev",
        "fixture",
        "--upstream-crate",
        "verlet-wasm-csv-profile",
    ]);
    let publish_stderr = stderr(&publish);
    assert!(publish_stderr.contains("package proof gate"));
    assert!(publish_stderr.contains("--package"));
    assert!(
        !verlet_operations::operation_store::LocalOperationRegistry::new(&registry_root)
            .record_path("data")
            .unwrap()
            .exists()
    );

    let bad = temp_dir("stateful-conversion");
    std::fs::write(
        bad.join("Cargo.toml"),
        r#"
[package]
name = "bad-stateful-tool"
version = "0.1.0"
edition = "2024"

[lib]
crate-type = ["cdylib"]

[dependencies]
notify = "6"
"#,
    )
    .unwrap();
    std::fs::create_dir_all(bad.join("src")).unwrap();
    std::fs::write(
        bad.join("src/lib.rs"),
        "#[unsafe(no_mangle)] pub extern \"C\" fn noop() {}\n",
    )
    .unwrap();

    let rejected = run_verlet_failed([
        "tool",
        "build",
        "--module-path",
        bad.to_str().unwrap(),
        "--name",
        "bad",
        "--upstream-url",
        "https://github.com/example/bad",
        "--upstream-rev",
        "deadbeef",
        "--upstream-crate",
        "bad-stateful-tool",
    ]);
    assert!(stdout(&rejected).contains("policy rejected"));
    assert!(stdout(&rejected).contains("notify"));
    assert!(stderr(&rejected).contains("strict stateless conversion rejected"));
}

#[test]
fn verlet_cli_tool_package_build_publish_and_persist_interface() {
    let repo = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let module_path = repo.join("tests/fixtures/wasm-csv-profile");
    let package = temp_dir("tool-package-csv");
    write_json_file(
        &package.join("schemas/csv_profile.input.json"),
        r#"{
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "csv": { "type": "string" },
    "has_header": { "type": "boolean" }
  },
  "required": ["csv"]
}"#,
    );
    write_json_file(
        &package.join("schemas/csv_profile.output.json"),
        r#"{
  "type": "object",
  "properties": {
    "rows": { "type": "integer" },
    "columns": {
      "type": "array",
      "items": {
        "type": "object",
        "additionalProperties": true
      }
    }
  },
  "required": ["rows", "columns"]
}"#,
    );
    write_json_file(
        &package.join("fixtures/basic.input.json"),
        r#"{"csv":"name,score\nAda,10\nLinus,8\n","has_header":true}"#,
    );
    write_json_file(
        &package.join("fixtures/basic.expect.json"),
        r#"{
  "rows": 2,
  "columns": [
    {"name":"name","non_empty":2,"empty":0,"numeric_count":0},
    {"name":"score","non_empty":2,"empty":0,"numeric_count":2,"min":8.0,"max":10.0,"mean":9.0}
  ]
}"#,
    );
    std::fs::write(
        package.join("verlet.tool.toml"),
        format!(
            r#"
kind = "cooldis.tool"
schema_version = 0

[identity]
name = "data"
version = "0.1.0"
description = "Profile tabular text."

[runtime]
kind = "wasm32-unknown-unknown"
module_path = "{}"
state = "stateless"

[[operations]]
name = "csv_profile"
description = "Profile a CSV string."
input_schema = "schemas/csv_profile.input.json"
output_schema = "schemas/csv_profile.output.json"
required_capabilities = []

[operations.command]
name = "data profile"
stdin = "none"
stdout = "json"

[operations.mcp]
tool_name = "data_csv_profile"

[[fixtures]]
name = "basic"
operation = "csv_profile"
input = "fixtures/basic.input.json"
expect = "fixtures/basic.expect.json"
"#,
            module_path.display()
        ),
    )
    .unwrap();

    let manifest_path = package.join("verlet.tool.toml");
    let build = run_verlet([
        "tool",
        "build",
        "--package",
        manifest_path.to_str().unwrap(),
    ]);
    assert!(build.contains("tool package data"));
    assert!(build.contains("receipt tool_build_v0"));
    assert!(build.contains("runtime wasm32-unknown-unknown"));
    assert!(build.contains("operation csv_profile json -> json"));
    assert!(build.contains("fixture basic passed"));
    assert!(build.contains("command data profile"));
    assert!(build.contains("mcp data_csv_profile"));

    let registry_root = temp_dir("tool-package-registry");
    let publish = run_verlet([
        "tool",
        "publish",
        "--package",
        manifest_path.to_str().unwrap(),
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    assert!(publish.contains("published data"));
    let record = registry_record(&registry_root, "data");
    let active_hash = record.active_artifact_hash.clone();
    let list = run_verlet([
        "tool",
        "list",
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    assert!(list.contains("data"));
    assert!(list.contains("0.1.0"));
    assert!(list.contains("csv_profile"));
    assert!(list.contains(&active_hash));
    let interface = record
        .interface
        .expect("published record should store interface");
    assert_eq!(interface.identity.name, "data");
    assert_eq!(interface.runtime.kind, "wasm32-unknown-unknown");
    assert_eq!(interface.operations[0].name, "csv_profile");
    assert_eq!(
        interface.operations[0].command.as_ref().unwrap().name,
        "data profile"
    );
    assert_eq!(
        interface.operations[0].mcp.as_ref().unwrap().tool_name,
        "data_csv_profile"
    );
    let manual = interface.operations[0]
        .manual
        .as_ref()
        .expect("published interface should store an operation manual");
    assert_eq!(manual.operation_name, "csv_profile");
    assert_eq!(manual.summary, "Profile a CSV string.");
    assert!(!manual.generated);

    let manual_text = run_verlet([
        "tool",
        "manual",
        "data",
        "csv_profile",
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    assert!(manual_text.contains("NAME"));
    assert!(manual_text.contains("data csv_profile - Profile a CSV string."));
    assert!(manual_text.contains("USAGE"));
    assert!(manual_text.contains("verlet tool run data csv_profile"));
    assert!(manual_text.contains("EXIT STATUS"));

    let manual_json = run_verlet([
        "tool",
        "manual",
        "data",
        "csv_profile",
        "--json",
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    let manuals: Vec<serde_json::Value> = serde_json::from_str(&manual_json).unwrap();
    assert_eq!(manuals[0]["tool_name"], "data");
    assert_eq!(manuals[0]["operation_name"], "csv_profile");
    assert_eq!(manuals[0]["generated"], false);

    let agent_manifest_path = package.join("vendor-risk-agent.verlet.agent.toml");
    std::fs::write(
        &agent_manifest_path,
        format!(
            r#"
[agent]
name = "vendor-risk-agent"
version = "0.1.0"
description = "Uses published tools to inspect vendor risk."

[[model_profiles]]
id = "default"
provider_ref = "provider://openai_compatible"
model_ref = "model://example-chat-model"

[[tools]]
type = "direct_tool"
id = "csv_profile"
tool_name = "csv_profile"
operation_ref = "op://data/csv_profile@sha256:{active_hash}"
grants = []
"#,
        ),
    )
    .unwrap();
    let agent_registry_root = temp_dir("agent-tool-ref-registry");
    run_verlet([
        "agent",
        "publish",
        agent_manifest_path.to_str().unwrap(),
        "--registry-root",
        agent_registry_root.to_str().unwrap(),
        "--operations-registry-root",
        registry_root.to_str().unwrap(),
    ]);
    let agent = agent_record(&agent_registry_root, "vendor-risk-agent");
    assert_eq!(agent.tool_refs.len(), 1);
    assert_eq!(agent.tool_refs[0].name, "csv_profile");
    assert_eq!(
        agent.tool_refs[0].reference,
        format!("op://data/csv_profile@sha256:{active_hash}")
    );
    assert_eq!(agent.tool_refs[0].operation.as_deref(), Some("csv_profile"));
}

#[test]
fn verlet_cli_rejects_user_authored_kernel_tool_packages() {
    let package = temp_dir("tool-package-kernel-rejected");
    write_json_file(
        &package.join("schemas/thread_spawn.input.json"),
        r#"{"type":"object","properties":{"message":{"type":"string"}},"required":["message"]}"#,
    );
    write_json_file(
        &package.join("schemas/thread_spawn.output.json"),
        r#"{"type":"object","properties":{"thread_id":{"type":"string"}}}"#,
    );
    std::fs::write(
        package.join("verlet.tool.toml"),
        r#"
kind = "cooldis.tool"
schema_version = 0

[identity]
name = "fake-kernel"
version = "0.1.0"
description = "Attempts to publish kernel dispatch."

[runtime]
kind = "kernel"
state = "stateless"

[[operations]]
name = "thread_spawn"
description = "Start a child thread."
input_schema = "schemas/thread_spawn.input.json"
output_schema = "schemas/thread_spawn.output.json"
required_capabilities = ["threads.spawn"]

[operations.command]
name = "thread_spawn"
stdin = "json"
stdout = "json"
"#,
    )
    .unwrap();
    let manifest_path = package.join("verlet.tool.toml");

    let build = run_verlet_failed([
        "tool",
        "build",
        "--package",
        manifest_path.to_str().unwrap(),
    ]);
    let build_stderr = stderr(&build);
    assert!(build_stderr.contains("runtime.kind = \"kernel\""));
    assert!(build_stderr.contains("synthesized by Verlet startup"));
    assert!(build_stderr.contains("tool build/publish cannot author or publish"));

    let registry_root = temp_dir("tool-package-kernel-registry");
    let publish = run_verlet_failed([
        "tool",
        "publish",
        "--package",
        manifest_path.to_str().unwrap(),
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    let publish_stderr = stderr(&publish);
    assert!(publish_stderr.contains("runtime.kind = \"kernel\""));
    assert!(publish_stderr.contains("synthesized by Verlet startup"));
    assert!(publish_stderr.contains("tool build/publish cannot author or publish"));
}

#[test]
fn verlet_cli_tool_package_build_warns_on_generated_manuals() {
    let repo = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let module_path = repo.join("tests/fixtures/wasm-csv-profile");
    let package = temp_dir("tool-package-generated-manual");
    write_json_file(
        &package.join("schemas/csv_profile.input.json"),
        r#"{"type":"object","properties":{"csv":{"type":"string"}},"required":["csv"]}"#,
    );
    write_json_file(
        &package.join("schemas/csv_profile.output.json"),
        r#"{"type":"object","properties":{"rows":{"type":"integer"}}}"#,
    );
    std::fs::write(
        package.join("verlet.tool.toml"),
        format!(
            r#"
kind = "cooldis.tool"
schema_version = 0

[identity]
name = "data"
version = "0.1.0"
description = "Profile tabular text."

[runtime]
kind = "wasm32-unknown-unknown"
module_path = "{}"
state = "stateless"

[[operations]]
name = "csv_profile"
input_schema = "schemas/csv_profile.input.json"
output_schema = "schemas/csv_profile.output.json"
required_capabilities = []

[operations.command]
name = "data profile"
stdin = "json"
stdout = "json"
"#,
            module_path.display()
        ),
    )
    .unwrap();

    let manifest_path = package.join("verlet.tool.toml");
    let build = run_verlet([
        "tool",
        "build",
        "--package",
        manifest_path.to_str().unwrap(),
    ]);
    assert!(build.contains("warning operation csv_profile has no description"));
    assert!(build.contains("warning operation csv_profile has no fixtures"));

    let registry_root = temp_dir("tool-generated-manual-registry");
    run_verlet([
        "tool",
        "publish",
        "--package",
        manifest_path.to_str().unwrap(),
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    let record = registry_record(&registry_root, "data");
    let manual = record.interface.unwrap().operations[0]
        .manual
        .clone()
        .expect("generated manual should be persisted");
    assert!(manual.generated);
    assert!(manual.summary.contains("Run csv_profile"));
    assert!(!manual.warnings.is_empty());
}

#[test]
fn verlet_cli_tool_package_build_runs_internal_http_fixture() {
    let (base_url, server) = spawn_employee_server();
    let repo = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let module_path = repo.join("tests/fixtures/wasm-employee-lookup");
    let package = temp_dir("tool-package-employee");
    write_json_file(
        &package.join("schemas/lookup.input.json"),
        r#"{
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "base_url": { "type": "string" },
    "employee_id": { "type": "string" }
  },
  "required": ["base_url", "employee_id"]
}"#,
    );
    write_json_file(
        &package.join("schemas/lookup.output.json"),
        r#"{
  "type": "object",
  "properties": {
    "employee_id": { "type": "string" },
    "name": { "type": "string" },
    "department": { "type": "string" },
    "source_status": { "type": "integer" }
  },
  "required": ["employee_id", "name", "department", "source_status"]
}"#,
    );
    write_json_file(
        &package.join("fixtures/ada.input.json"),
        &format!(r#"{{"base_url":"{base_url}","employee_id":"ada"}}"#),
    );
    write_json_file(
        &package.join("fixtures/ada.expect.json"),
        r#"{"employee_id":"ada","name":"Ada Lovelace","department":"Research","source_status":200}"#,
    );
    let origin = base_url;
    std::fs::write(
        package.join("verlet.tool.toml"),
        format!(
            r#"
kind = "cooldis.tool"
schema_version = 0

[identity]
name = "employee"
version = "0.1.0"
description = "Look up employee metadata through an internal service."

[runtime]
kind = "wasm32-unknown-unknown"
module_path = "{}"
state = "stateless"

[[operations]]
name = "lookup"
description = "Look up an employee by ID."
input_schema = "schemas/lookup.input.json"
output_schema = "schemas/lookup.output.json"
required_capabilities = [
  "net.http.private",
  "net.http.private:GET:{}"
]

[operations.command]
name = "employee lookup"
stdin = "none"
stdout = "json"

[operations.mcp]
tool_name = "employee_lookup"

[[fixtures]]
name = "ada"
operation = "lookup"
input = "fixtures/ada.input.json"
expect = "fixtures/ada.expect.json"
"#,
            module_path.display(),
            origin
        ),
    )
    .unwrap();

    let manifest_path = package.join("verlet.tool.toml");
    let build = run_verlet([
        "tool",
        "build",
        "--package",
        manifest_path.to_str().unwrap(),
    ]);
    assert!(build.contains("tool package employee"));
    assert!(build.contains("capability net.http.private"));
    assert!(build.contains("fixture ada passed"));
    assert!(build.contains("command employee lookup"));
    server.join().unwrap();
}

#[test]
fn verlet_cli_plans_publishes_lists_and_shows_agent_manifest() {
    let workspace = temp_dir("agent-manifest");
    let manifest_path = workspace.join("release-verifier.verlet.agent.toml");
    std::fs::write(
        &manifest_path,
        format!(
            r#"
[agent]
name = "release-verifier"
version = "0.1.0"
description = "Checks a release branch."

[[model_profiles]]
id = "default"
provider_ref = "provider://openai_compatible"
model_ref = "model://example-chat-model"

[[tools]]
type = "bash_tool"
id = "tailcat"
command = "tailcat"
operation_ref = "op://tailcat@sha256:{TEST_OPERATION_HASH}"
"#,
        ),
    )
    .unwrap();
    let registry_root = temp_dir("agent-registry");
    let operation_registry_root = temp_dir("agent-operation-registry");
    seed_operation_record(
        &operation_registry_root,
        "tailcat",
        TEST_OPERATION_HASH,
        &[("cat", &[])],
    );
    let missing_operation_registry_root = workspace.join("missing-operations");

    let offline_plan = run_verlet([
        "agent",
        "plan",
        manifest_path.to_str().unwrap(),
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--operations-registry-root",
        missing_operation_registry_root.to_str().unwrap(),
    ]);
    assert!(offline_plan.contains("[unverified-offline]"));

    let plan = run_verlet([
        "agent",
        "plan",
        manifest_path.to_str().unwrap(),
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--operations-registry-root",
        operation_registry_root.to_str().unwrap(),
    ]);
    assert!(plan.contains("agent plan"));
    assert!(plan.contains("agent://release-verifier@0.1.0"));
    assert!(plan.contains("models: 1"));
    assert!(plan.contains("tools: 1"));
    assert!(plan.contains("[verified]"));
    assert!(
        verlet::agent::manifest::LocalAgentRegistry::new(&registry_root)
            .list_records()
            .unwrap()
            .is_empty()
    );

    let publish = run_verlet([
        "agent",
        "publish",
        manifest_path.to_str().unwrap(),
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--operations-registry-root",
        operation_registry_root.to_str().unwrap(),
    ]);
    assert!(publish.contains("published agent://release-verifier@0.1.0"));
    let record = agent_record(&registry_root, "release-verifier");
    assert_eq!(record.name, "release-verifier");
    assert_eq!(record.version, "0.1.0");
    assert_eq!(record.model_profile_count, 1);
    assert_eq!(record.tool_count, 1);

    let list = run_verlet([
        "agent",
        "list",
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    assert!(list.contains("release-verifier"));
    assert!(list.contains("0.1.0"));
    assert!(list.contains("agent://release-verifier@0.1.0"));

    let show_by_name = run_verlet([
        "agent",
        "show",
        "release-verifier",
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    assert!(show_by_name.contains("\"name\": \"release-verifier\""));
    assert!(show_by_name.contains("\"source_hash\""));

    let show_by_ref = run_verlet([
        "agent",
        "show",
        "agent://release-verifier@0.1.0",
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    assert!(show_by_ref.contains("\"version\": \"0.1.0\""));
}

#[test]
fn verlet_cli_lists_and_diffs_immutable_agent_version_snapshots() {
    let workspace = temp_dir("agent-version-snapshots");
    let project = workspace.join("auditor");
    std::fs::create_dir_all(project.join("prompts")).unwrap();
    let manifest_path = project.join("verlet.agent.toml");
    let source = |version: &str, description: &str| {
        format!(
            r#"[agent]
name = "auditor"
version = "{version}"
description = "{description}"

[[model_profiles]]
id = "default"
provider_ref = "provider://openai_compatible"
model_ref = "model://example-chat-model"
"#,
        )
    };
    std::fs::write(project.join("prompts/system.md"), "Audit every release.\n").unwrap();
    std::fs::write(&manifest_path, source("1.0.0", "First snapshot.")).unwrap();
    let registry_root = workspace.join("agents");
    let operation_registry_root = workspace.join("operations");

    run_verlet([
        "agent",
        "publish",
        manifest_path.to_str().unwrap(),
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--operations-registry-root",
        operation_registry_root.to_str().unwrap(),
    ]);
    std::fs::write(&manifest_path, source("2.0.0", "Second snapshot.")).unwrap();
    run_verlet([
        "agent",
        "publish",
        manifest_path.to_str().unwrap(),
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--operations-registry-root",
        operation_registry_root.to_str().unwrap(),
    ]);

    for (version, published_at_ms) in [("1.0.0", 0_u64), ("2.0.0", 1_000_u64)] {
        let path = verlet::agent::manifest::LocalAgentRegistry::new(&registry_root)
            .version_record_path("auditor", version)
            .unwrap();
        let mut record: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        record["published_at_ms"] = serde_json::Value::from(published_at_ms);
        std::fs::write(&path, serde_json::to_vec_pretty(&record).unwrap()).unwrap();
    }

    let versions = run_verlet([
        "agent",
        "versions",
        "auditor",
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    assert!(versions.contains("PUBLISHED AT"));
    assert!(versions.contains("VERSION"));
    assert!(versions.contains("MANIFEST HASH"));
    assert!(versions.contains("1970-01-01T00:00:00.000Z"));
    assert!(versions.contains("1970-01-01T00:00:01.000Z"));
    assert!(versions.find("1.0.0").unwrap() < versions.find("2.0.0").unwrap());
    assert!(!versions.contains("[no-authored-source]"));

    let versions_json = run_verlet([
        "agent",
        "versions",
        "auditor",
        "--json",
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    let versions: Vec<serde_json::Value> = serde_json::from_str(&versions_json).unwrap();
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0]["version"], "1.0.0");
    assert_eq!(versions[1]["version"], "2.0.0");
    assert!(
        versions[0]["source_hash"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert!(
        versions[0]["manifest_hash"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert_eq!(versions[0]["authored_source_present"], true);

    let diff = run_verlet([
        "agent",
        "diff",
        "auditor",
        "--from",
        "1.0.0",
        "--to",
        "2.0.0",
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    assert!(diff.contains("manifest auditor 1.0.0:resolved -> 2.0.0:resolved"));
    assert!(diff.contains("~ /identity/description: \"First snapshot.\" -> \"Second snapshot.\""));

    let diff_json = run_verlet([
        "agent",
        "diff",
        "auditor",
        "--from",
        "1.0.0",
        "--to",
        "2.0.0",
        "--json",
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    let changes: Vec<serde_json::Value> = serde_json::from_str(&diff_json).unwrap();
    assert!(changes.iter().any(|change| {
        change["path"] == "/identity/description" && change["kind"] == "changed"
    }));

    let cross_form = run_verlet([
        "agent",
        "diff",
        "auditor",
        "--from",
        "1.0.0:authored",
        "--to",
        "1.0.0:resolved",
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    assert!(cross_form.contains("manifest auditor 1.0.0:authored -> 1.0.0:resolved"));
    assert!(cross_form.contains("~ /context:"));
    assert!(cross_form.contains("+ /resources/0:"));

    let legacy_path = verlet::agent::manifest::LocalAgentRegistry::new(&registry_root)
        .version_record_path("auditor", "1.0.0")
        .unwrap();
    let mut legacy: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&legacy_path).unwrap()).unwrap();
    legacy.as_object_mut().unwrap().remove("authored_source");
    std::fs::write(&legacy_path, serde_json::to_vec_pretty(&legacy).unwrap()).unwrap();
    let failed = run_verlet_failed([
        "agent",
        "diff",
        "auditor",
        "--from",
        "1.0.0:authored",
        "--to",
        "2.0.0:resolved",
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    let error = stderr(&failed);
    assert!(error.contains("auditor@1.0.0"));
    assert!(error.contains("legacy record has no authored_source"));

    let missing_value = run_verlet_failed([
        "agent",
        "diff",
        "auditor",
        "--from",
        "--to",
        "2.0.0",
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    assert!(stderr(&missing_value).contains("--from requires a value"));

    let invalid_timestamp_path = verlet::agent::manifest::LocalAgentRegistry::new(&registry_root)
        .version_record_path("auditor", "2.0.0")
        .unwrap();
    let mut invalid_timestamp: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&invalid_timestamp_path).unwrap()).unwrap();
    invalid_timestamp["published_at_ms"] = serde_json::Value::from(u64::MAX);
    std::fs::write(
        &invalid_timestamp_path,
        serde_json::to_vec_pretty(&invalid_timestamp).unwrap(),
    )
    .unwrap();
    let invalid_timestamp = run_verlet_failed([
        "agent",
        "versions",
        "auditor",
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    let error = stderr(&invalid_timestamp);
    assert!(error.contains("auditor@2.0.0"));
    assert!(error.contains("invalid published_at_ms"));
}

#[test]
fn verlet_cli_agent_publish_resolve_ops_pins_unpinned_manifest_refs() {
    let workspace = temp_dir("agent-resolve-ops");
    let manifest_path = workspace.join("resolve-ops.verlet.agent.toml");
    std::fs::write(
        &manifest_path,
        r#"
# keep this comment across value rewrites
[agent]
name = "resolve-ops"
version = "0.1.0"
description = "Pins operation refs during publish."

[[model_profiles]]
id = "default"
provider_ref = "provider://openai_compatible"
model_ref = "model://example-chat-model"

[[tools]]
type = "bash_tool"
id = "tailcat"
command = "cat"
operation_ref = "op://tailcat"

[[tools]]
type = "direct_tool"
id = "exporter"
tool_name = "export"
operation_ref = "op://analytics/export@latest"
"#,
    )
    .unwrap();
    let registry_root = temp_dir("agent-resolve-ops-registry");
    let operation_registry_root = temp_dir("agent-resolve-ops-operation-registry");
    seed_operation_record(
        &operation_registry_root,
        "tailcat",
        TEST_OPERATION_HASH,
        &[("cat", &[])],
    );
    seed_operation_record(
        &operation_registry_root,
        "analytics",
        &"f".repeat(64),
        &[("export", &[])],
    );

    let publish = run_verlet([
        "agent",
        "publish",
        manifest_path.to_str().unwrap(),
        "--resolve-ops",
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--operations-registry-root",
        operation_registry_root.to_str().unwrap(),
    ]);
    let tailcat_ref = format!("op://tailcat@sha256:{TEST_OPERATION_HASH}");
    let analytics_ref = format!("op://analytics/export@sha256:{}", "f".repeat(64));
    assert!(publish.contains(&format!(
        "resolved operation_ref: op://tailcat -> {tailcat_ref}"
    )));
    assert!(publish.contains(&format!(
        "resolved operation_ref: op://analytics/export@latest -> {analytics_ref}"
    )));
    assert!(publish.contains("published agent://resolve-ops@0.1.0"));

    let rewritten = std::fs::read_to_string(&manifest_path).unwrap();
    assert!(rewritten.contains("# keep this comment across value rewrites"));
    assert!(rewritten.contains(&format!("operation_ref = \"{tailcat_ref}\"")));
    assert!(rewritten.contains(&format!("operation_ref = \"{analytics_ref}\"")));
    assert!(!rewritten.contains("operation_ref = \"op://tailcat\""));
    assert!(!rewritten.contains("operation_ref = \"op://analytics/export@latest\""));

    let record = agent_record(&registry_root, "resolve-ops");
    assert_eq!(record.tool_refs[0].reference, tailcat_ref);
    assert_eq!(record.tool_refs[1].reference, analytics_ref);
}

#[test]
fn verlet_cli_agent_publish_resolve_ops_canonicalizes_legacy_kernel_package_alias() {
    let workspace = temp_dir("agent-resolve-legacy-kernel-package");
    let manifest_path = workspace.join("resolve-legacy.verlet.agent.toml");
    let legacy_name = concat!("cool", "dis-threads");
    std::fs::write(
        &manifest_path,
        format!(
            r#"
[agent]
name = "resolve-legacy"
version = "0.1.0"
kind = "verlet.agent-manifest"

[[model_profiles]]
id = "default"
provider_ref = "provider://openai_compatible"
model_ref = "model://example-chat-model"

[[tools]]
type = "direct_tool"
id = "spawn"
tool_name = "thread_spawn"
operation_ref = "op://{legacy_name}/thread_spawn"
grants = ["threads.spawn"]
"#
        ),
    )
    .unwrap();
    let registry_root = temp_dir("agent-resolve-legacy-kernel-package-agents");
    let operation_registry_root = temp_dir("agent-resolve-legacy-kernel-package-operations");
    let record = verlet::operations::kernel_packages::ensure_verlet_threads_published(Some(
        &operation_registry_root,
    ))
    .unwrap()
    .unwrap();

    let publish = run_verlet([
        "agent",
        "publish",
        manifest_path.to_str().unwrap(),
        "--resolve-ops",
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--operations-registry-root",
        operation_registry_root.to_str().unwrap(),
    ]);
    let canonical_ref = format!(
        "op://verlet-threads/thread_spawn@sha256:{}",
        record.active_artifact_hash
    );

    assert!(publish.contains(&format!(
        "resolved operation_ref: op://{legacy_name}/thread_spawn -> {canonical_ref}"
    )));
    assert!(
        std::fs::read_to_string(&manifest_path)
            .unwrap()
            .contains(&format!("operation_ref = \"{canonical_ref}\""))
    );
    assert_eq!(
        agent_record(&registry_root, "resolve-legacy").tool_refs[0].reference,
        canonical_ref
    );
}

#[test]
fn verlet_cli_agent_publish_unpinned_ops_without_resolve_hint_fails() {
    let workspace = temp_dir("agent-unpinned-op-hint");
    let manifest_path = workspace.join("unresolved.verlet.agent.toml");
    std::fs::write(
        &manifest_path,
        r#"
[agent]
name = "unresolved"
version = "0.1.0"
description = "Fails without resolve-ops."

[[model_profiles]]
id = "default"
provider_ref = "provider://openai_compatible"
model_ref = "model://example-chat-model"

[[tools]]
type = "bash_tool"
id = "tailcat"
command = "cat"
operation_ref = "op://tailcat"
"#,
    )
    .unwrap();
    let registry_root = temp_dir("agent-unpinned-op-hint-registry");
    let operation_registry_root = temp_dir("agent-unpinned-op-hint-operation-registry");
    seed_operation_record(
        &operation_registry_root,
        "tailcat",
        TEST_OPERATION_HASH,
        &[("cat", &[])],
    );

    let publish = run_verlet_failed([
        "agent",
        "publish",
        manifest_path.to_str().unwrap(),
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--operations-registry-root",
        operation_registry_root.to_str().unwrap(),
    ]);
    let err = stderr(&publish);
    assert!(err.contains("unresolved artifact ref"));
    assert!(err.contains("--resolve-ops"));
}

#[test]
fn verlet_cli_agent_publish_resolve_ops_unknown_name_writes_nothing() {
    let workspace = temp_dir("agent-resolve-ops-unknown");
    let manifest_path = workspace.join("unknown.verlet.agent.toml");
    let source = r#"
[agent]
name = "unknown"
version = "0.1.0"
description = "Does not rewrite when resolution is ambiguous."

[[model_profiles]]
id = "default"
provider_ref = "provider://openai_compatible"
model_ref = "model://example-chat-model"

[[tools]]
type = "bash_tool"
id = "missing"
command = "cat"
operation_ref = "op://missing"
"#;
    std::fs::write(&manifest_path, source).unwrap();
    let registry_root = temp_dir("agent-resolve-ops-unknown-registry");
    let operation_registry_root = temp_dir("agent-resolve-ops-unknown-operation-registry");
    seed_operation_record(
        &operation_registry_root,
        "tailcat",
        TEST_OPERATION_HASH,
        &[("cat", &[])],
    );

    let publish = run_verlet_failed([
        "agent",
        "publish",
        manifest_path.to_str().unwrap(),
        "--resolve-ops",
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--operations-registry-root",
        operation_registry_root.to_str().unwrap(),
    ]);
    let err = stderr(&publish);
    assert!(err.contains("was not found in the local operation registry"));
    assert_eq!(std::fs::read_to_string(&manifest_path).unwrap(), source);
    assert!(
        verlet::agent::manifest::LocalAgentRegistry::new(&registry_root)
            .list_records()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn verlet_cli_tool_list_prints_records_and_empty_absent_root() {
    let registry_root = temp_dir("tool-list-registry");
    let record = seed_operation_record(
        &registry_root,
        "tailcat",
        TEST_OPERATION_HASH,
        &[("cat", &[]), ("tail", &[])],
    );

    let list = run_verlet([
        "tool",
        "list",
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    assert!(list.contains("NAME"));
    assert!(list.contains("VERSION"));
    assert!(list.contains("OPERATIONS"));
    assert!(list.contains("ACTIVE HASH"));
    assert!(list.contains("tailcat"));
    assert!(list.contains(" - "));
    assert!(list.contains("cat,tail"));
    assert!(list.contains(&record.active_artifact_hash));
    assert_eq!(list.matches(&record.active_artifact_hash).count(), 1);

    let absent_root = temp_dir("tool-list-absent").join("missing");
    let empty = run_verlet([
        "tool",
        "list",
        "--registry-root",
        absent_root.to_str().unwrap(),
    ]);
    assert!(empty.contains("NAME"));
    assert!(empty.contains("ACTIVE HASH"));
    assert!(!empty.contains("tailcat"));
    assert_eq!(empty.lines().count(), 1);
}

#[test]
fn verlet_cli_flat_source_publish_is_refused_without_writing_record() {
    let repo = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let module_path = repo.join("tests/fixtures/wasm-vfs-tools");
    let registry_root = temp_dir("publish-source-refused");

    let refused = run_verlet_failed([
        "tool",
        "publish",
        "--module-path",
        module_path.to_str().unwrap(),
        "--name",
        "tailcat",
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--metadata",
        "kind=\"vfs-tool\"",
    ]);
    let err = stderr(&refused);
    assert!(err.contains("package proof gate"));
    assert!(err.contains("--package"));
    assert!(
        !verlet_operations::operation_store::LocalOperationRegistry::new(&registry_root)
            .record_path("tailcat")
            .unwrap()
            .exists()
    );
}

#[test]
fn verlet_cli_flat_bin_publish_is_refused_without_writing_record() {
    let repo = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let module_path = repo.join("tests/fixtures/wasm-vfs-tools");
    let registry_root = temp_dir("publish-bin-refused");
    let build_output = run_verlet([
        "tool",
        "build",
        "--module-path",
        module_path.to_str().unwrap(),
    ]);
    let artifact_path = build_output
        .lines()
        .find_map(|line| line.strip_prefix("artifact "))
        .expect("tool build should print an artifact path");

    let refused = run_verlet_failed([
        "tool",
        "publish",
        "--bin-path",
        artifact_path,
        "--name",
        "tailcat-bin",
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    let err = stderr(&refused);
    assert!(err.contains("package proof gate"));
    assert!(err.contains("--package"));
    assert!(
        !verlet_operations::operation_store::LocalOperationRegistry::new(&registry_root)
            .record_path("tailcat-bin")
            .unwrap()
            .exists()
    );
}

#[test]
fn verlet_cli_flat_publish_refusal_preserves_previous_active_record() {
    let registry_root = temp_dir("atomic-republish");
    seed_operation_record(
        &registry_root,
        "tailcat",
        TEST_OPERATION_HASH,
        &[("cat", &[]), ("tail", &[])],
    );
    let before = registry_record(&registry_root, "tailcat");
    let invalid_path = registry_root.join("invalid.wasm");
    std::fs::write(&invalid_path, b"\0asm\x01\0\0\0\xff").unwrap();

    let failed = run_verlet_failed([
        "tool",
        "publish",
        "--bin-path",
        invalid_path.to_str().unwrap(),
        "--name",
        "tailcat",
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    assert!(stderr(&failed).contains("package proof gate"));
    assert!(stderr(&failed).contains("--package"));

    let after = registry_record(&registry_root, "tailcat");
    assert_eq!(after.active_artifact_hash, before.active_artifact_hash);
}

#[test]
fn verlet_cli_registry_run_rejects_corrupt_or_mismatched_artifacts() {
    let repo = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let module_path = repo.join("tests/fixtures/wasm-vfs-tools");
    let registry_root = temp_dir("corrupt-blob");
    let mount = fixture_mount(&module_path);

    publish_vfs_tool_package(&registry_root, "tailcat");

    let registry = verlet_operations::operation_store::LocalOperationRegistry::new(&registry_root);
    let record = registry.load_record("tailcat").unwrap();
    let blob_path = registry
        .blobs()
        .artifact_path(&record.active_artifact_hash)
        .unwrap();
    std::fs::remove_file(&blob_path).unwrap();
    let missing = run_verlet_failed([
        "tool",
        "run",
        "tailcat",
        "tail",
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--input",
        "/workspace/tail.txt",
        "--mount",
        &mount,
    ]);
    assert!(stderr(&missing).contains("was not found"));

    publish_vfs_tool_package(&registry_root, "tailcat");
    let record = registry.load_record("tailcat").unwrap();
    let blob_path = registry
        .blobs()
        .artifact_path(&record.active_artifact_hash)
        .unwrap();
    std::fs::write(&blob_path, b"corrupt").unwrap();
    let corrupt = run_verlet_failed([
        "tool",
        "run",
        "tailcat",
        "tail",
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--input",
        "/workspace/tail.txt",
        "--mount",
        &mount,
    ]);
    assert!(stderr(&corrupt).contains("hash mismatch"));

    publish_vfs_tool_package(&registry_root, "tailcat");
    let mut record = registry.load_record("tailcat").unwrap();
    record.manifest.operations[0].name = "other".to_string();
    record.interface = None;
    let registered = verlet_operations::RegisteredOperation {
        name: record.name.clone(),
        manifest: record.manifest.clone(),
        capability_grants: record.capability_grants.clone(),
        metadata: std::collections::BTreeMap::new(),
    };
    record.projections = registered.projections();
    std::fs::write(
        registry.record_path("tailcat").unwrap(),
        serde_json::to_vec_pretty(&record).unwrap(),
    )
    .unwrap();

    let mismatch = run_verlet_failed([
        "tool",
        "run",
        "tailcat",
        "tail",
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--input",
        "/workspace/tail.txt",
        "--mount",
        &mount,
    ]);
    assert!(stderr(&mismatch).contains("manifest mismatch"));
}

#[test]
fn verlet_cli_registry_run_rejects_missing_record() {
    let registry_root = temp_dir("missing-record");

    let missing = run_verlet_failed([
        "tool",
        "run",
        "missing",
        "tail",
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--input",
        "/workspace/tail.txt",
    ]);

    assert!(stderr(&missing).contains("failed to read operation record"));
}

#[test]
fn verlet_cli_flat_publish_config_file_is_refused() {
    let repo = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let module_path = repo.join("tests/fixtures/wasm-vfs-tools");
    let temp = temp_dir("config");
    let registry_root = temp.join("registry");
    let config_path = temp.join("verlet.json");
    std::fs::write(
        &config_path,
        serde_json::json!({
            "name": "tailcat-config",
            "module_path": module_path,
            "registry_root": registry_root,
            "release": true,
            "metadata": {
                "fixture": "config"
            }
        })
        .to_string(),
    )
    .unwrap();

    let failed = run_verlet_failed(["tool", "publish", "--config", config_path.to_str().unwrap()]);
    assert!(stderr(&failed).contains("package proof gate"));
    assert!(stderr(&failed).contains("--package"));
    assert!(
        !verlet_operations::operation_store::LocalOperationRegistry::new(&registry_root)
            .record_path("tailcat-config")
            .unwrap()
            .exists()
    );
}

#[test]
fn verlet_cli_flat_publish_discovered_config_file_is_refused() {
    let repo = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let module_path = repo.join("tests/fixtures/wasm-vfs-tools");
    let temp = temp_dir("config-discovery");
    let registry_root = temp.join("registry");
    let config_path = temp.join("verlet.json");
    std::fs::write(
        &config_path,
        serde_json::json!({
            "name": "tailcat-discovered",
            "module_path": module_path,
            "registry_root": registry_root,
            "release": true
        })
        .to_string(),
    )
    .unwrap();

    let failed = run_verlet_failed_in_dir(&temp, ["tool", "publish"]);
    assert!(stderr(&failed).contains("package proof gate"));
    assert!(stderr(&failed).contains("--package"));
    assert!(
        !verlet_operations::operation_store::LocalOperationRegistry::new(&registry_root)
            .record_path("tailcat-discovered")
            .unwrap()
            .exists()
    );
}

fn publish_vfs_tool_package(
    registry_root: &std::path::Path,
    name: &str,
) -> verlet_operations::operation_store::PublishedOperationRecord {
    let repo = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let module_path = repo.join("tests/fixtures/wasm-vfs-tools");
    let package = temp_dir("vfs-tool-package");
    write_json_file(
        &package.join("schemas/path.input.json"),
        r#"{"type":"string"}"#,
    );
    write_json_file(
        &package.join("schemas/bytes.output.json"),
        r#"{"type":"string"}"#,
    );
    std::fs::write(
        package.join("verlet.tool.toml"),
        format!(
            r#"
kind = "cooldis.tool"
schema_version = 0

[identity]
name = "{name}"
version = "0.1.0"
description = "Read files from a mounted VFS."

[runtime]
kind = "wasm32-unknown-unknown"
module_path = "{}"
state = "stateless"

[[operations]]
name = "cat"
description = "Read a full file."
input_schema = "schemas/path.input.json"
output_schema = "schemas/bytes.output.json"
required_capabilities = []

[operations.command]
name = "cat"
stdin = "text"
stdout = "bytes"

[[operations]]
name = "tail"
description = "Read the last lines from a file."
input_schema = "schemas/path.input.json"
output_schema = "schemas/bytes.output.json"
required_capabilities = []

[operations.command]
name = "tail"
stdin = "text"
stdout = "bytes"
"#,
            module_path.display()
        ),
    )
    .unwrap();

    run_verlet([
        "tool",
        "publish",
        "--package",
        package.join("verlet.tool.toml").to_str().unwrap(),
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    registry_record(registry_root, name)
}

fn temp_dir(label: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("verlet-cli-{label}-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn write_json_file(path: &std::path::Path, body: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, body).unwrap();
}

fn spawn_employee_server() -> (String, std::thread::JoinHandle<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");
    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0u8; 2048];
        let read = stream.read(&mut request).unwrap();
        let request = String::from_utf8_lossy(&request[..read]);
        assert!(
            request.contains("GET /employee/ada HTTP/1.1"),
            "unexpected employee lookup request: {request}"
        );
        let body = r#"{"employee_id":"ada","name":"Ada Lovelace","department":"Research"}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).unwrap();
    });
    (base_url, handle)
}

fn spawn_openapi_import_server(
    status: u16,
    response_body: &'static str,
    expected_header: Option<&'static str>,
) -> (String, std::thread::JoinHandle<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        loop {
            let read = stream.read(&mut buffer).unwrap();
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..read]);
            let text = String::from_utf8_lossy(&request);
            let Some(header_end) = text.find("\r\n\r\n") else {
                continue;
            };
            let content_length = text
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or_default();
            if request.len() >= header_end + 4 + content_length {
                break;
            }
        }
        let request = String::from_utf8_lossy(&request);
        assert!(request.contains("POST /search HTTP/1.1"), "{request}");
        assert!(request.contains(r#"{"query":"verlet"}"#), "{request}");
        if let Some(expected_header) = expected_header {
            assert!(request.contains(expected_header), "{request}");
        }
        let reason = if status == 200 {
            "OK"
        } else {
            "Internal Server Error"
        };
        let response = format!(
            "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{response_body}",
            response_body.len()
        );
        stream.write_all(response.as_bytes()).unwrap();
    });
    (base_url, handle)
}

fn write_openapi_import_package(
    root: &std::path::Path,
    base_url: &str,
    auth: &str,
    response_status: &str,
) -> std::path::PathBuf {
    std::fs::create_dir_all(root).unwrap();
    let spec = serde_json::json!({
        "openapi": "3.0.3",
        "info": {"title": "Catalog", "version": "1"},
        "servers": [{"url": base_url}],
        "paths": {
            "/search": {
                "post": {
                    "operationId": "search",
                    "description": "Search the catalog.",
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": ["query"],
                                    "properties": {"query": {"type": "string"}},
                                    "additionalProperties": false
                                }
                            }
                        }
                    },
                    "responses": std::collections::BTreeMap::from([(
                        response_status,
                        serde_json::json!({"description": "response"})
                    )])
                }
            }
        }
    });
    let spec_bytes = serde_json::to_vec_pretty(&spec).unwrap();
    std::fs::write(root.join("openapi.json"), &spec_bytes).unwrap();
    let spec_sha256 = format!("{:x}", sha2::Sha256::digest(&spec_bytes));
    let package = root.join("catalog.import.toml");
    std::fs::write(
        &package,
        format!(
            "[import]\nname = \"catalog\"\nversion = \"1.0.0\"\ndescription = \"Catalog API\"\n\n[spec]\npath = \"openapi.json\"\nsha256 = {spec_sha256:?}\n\n{auth}\n[[operations]]\noperation_id = \"search\"\n"
        ),
    )
    .unwrap();
    package
}

fn fixture_mount(module_path: &std::path::Path) -> String {
    format!(
        "/workspace={}",
        module_path.join("testdata").to_string_lossy()
    )
}

fn write_skill_fixture(package_dir: &std::path::Path, name: &str, body: &str) {
    let dir = package_dir.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("SKILL.md"), body).unwrap();
}

fn skill_artifact_hash(output: &str) -> String {
    output_line_suffix(output, "artifact ")
}

fn output_line_suffix(output: &str, prefix: &str) -> String {
    output
        .lines()
        .find_map(|line| line.strip_prefix(prefix))
        .unwrap_or_else(|| panic!("output did not contain line prefix {prefix:?}:\n{output}"))
        .to_string()
}

fn registry_record(
    root: &std::path::Path,
    name: &str,
) -> verlet_operations::operation_store::PublishedOperationRecord {
    verlet_operations::operation_store::LocalOperationRegistry::new(root)
        .load_record(name)
        .unwrap()
}

fn seed_operation_record(
    root: &std::path::Path,
    name: &str,
    artifact_hash: &str,
    operations: &[(&str, &[&str])],
) -> verlet_operations::operation_store::PublishedOperationRecord {
    let registry = verlet_operations::operation_store::LocalOperationRegistry::new(root);
    let manifest = verlet_abi::WasmOperationManifest {
        abi: "cooldis.operation/0.1".to_string(),
        operations: operations
            .iter()
            .enumerate()
            .map(|(index, (operation_name, required_capabilities))| {
                verlet_abi::WasmOperationDefinition {
                    id: (index + 1) as u32,
                    name: (*operation_name).to_string(),
                    input: verlet_abi::WasmOperationValueKind::Text,
                    output: verlet_abi::WasmOperationValueKind::Text,
                    events: Default::default(),
                    mode: Default::default(),
                    required_capabilities: required_capabilities
                        .iter()
                        .map(|capability| (*capability).to_string())
                        .collect(),
                }
            })
            .collect(),
    };
    let capability_grants = operations
        .iter()
        .flat_map(|(_, required_capabilities)| {
            required_capabilities
                .iter()
                .map(|capability| (*capability).to_string())
        })
        .collect::<std::collections::BTreeSet<_>>();
    let registered = verlet_operations::RegisteredOperation {
        name: name.to_string(),
        manifest: manifest.clone(),
        capability_grants: capability_grants.clone(),
        metadata: std::collections::BTreeMap::new(),
    };
    let record = verlet_operations::operation_store::PublishedOperationRecord {
        schema_version: 1,
        name: name.to_string(),
        active_artifact_hash: artifact_hash.to_string(),
        manifest,
        projections: registered.projections(),
        interface: None,
        capability_grants,
        metadata: std::collections::BTreeMap::new(),
        source: verlet_operations::operation_store::PublishedOperationSource::Kernel {
            package: "test".to_string(),
        },
        build: verlet_operations::operation_store::PublishedOperationBuild {
            artifact_path: std::path::PathBuf::from("<test>"),
            published_at_ms: 0,
        },
    };
    record.validate().unwrap();
    let record_path = registry.record_path(name).unwrap();
    std::fs::create_dir_all(record_path.parent().unwrap()).unwrap();
    std::fs::write(&record_path, serde_json::to_vec_pretty(&record).unwrap()).unwrap();
    let version_path = registry.version_record_path(name, artifact_hash).unwrap();
    std::fs::create_dir_all(version_path.parent().unwrap()).unwrap();
    std::fs::write(&version_path, serde_json::to_vec_pretty(&record).unwrap()).unwrap();
    record
}

fn agent_record(
    root: &std::path::Path,
    name: &str,
) -> verlet::agent::manifest::PublishedAgentRecord {
    verlet::agent::manifest::LocalAgentRegistry::new(root)
        .load_record(name)
        .unwrap()
}

fn assert_no_command(output: &str, path: &[&str]) {
    let needle = format!("verlet {}", path.join(" "));
    assert!(
        !output
            .lines()
            .any(|line| line.trim_start().starts_with(&needle)),
        "unexpected legacy command in output: {needle}\n{output}"
    );
}

fn run_verlet<const N: usize>(args: [&str; N]) -> String {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_verlet"));
    model_catalog_test_support::disable_for_std_command(&mut command);
    run_verlet_command(command.args(args))
}

fn run_verlet_with_env<const N: usize>(args: [&str; N], envs: &[(&str, &str)]) -> String {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_verlet"));
    model_catalog_test_support::disable_for_std_command(&mut command);
    command.args(args);
    for (key, value) in envs {
        command.env(key, value);
    }
    run_verlet_command(&mut command)
}

fn run_verlet_with_stdin<const N: usize>(args: [&str; N], stdin: &str) -> String {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_verlet"));
    model_catalog_test_support::disable_for_std_command(&mut command);
    let mut child = command
        .args(args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("failed to spawn verlet cli");
    child
        .stdin
        .as_mut()
        .expect("verlet stdin should be piped")
        .write_all(stdin.as_bytes())
        .expect("failed to write verlet stdin");
    let output = child.wait_with_output().expect("failed to run verlet cli");
    assert!(
        output.status.success(),
        "verlet cli failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("verlet output should be utf8")
}

fn run_verlet_failed_in_dir<const N: usize>(
    dir: &std::path::Path,
    args: [&str; N],
) -> std::process::Output {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_verlet"));
    model_catalog_test_support::disable_for_std_command(&mut command);
    let output = command
        .current_dir(dir)
        .args(args)
        .output()
        .expect("failed to run verlet cli");
    assert!(
        !output.status.success(),
        "verlet cli unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn run_verlet_command(command: &mut std::process::Command) -> String {
    let output = command.output().expect("failed to run verlet cli");
    assert!(
        output.status.success(),
        "verlet cli failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("verlet output should be utf8")
}

fn run_verlet_failed<const N: usize>(args: [&str; N]) -> std::process::Output {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_verlet"));
    model_catalog_test_support::disable_for_std_command(&mut command);
    let output = command
        .args(args)
        .output()
        .expect("failed to run verlet cli");
    assert!(
        !output.status.success(),
        "verlet cli unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn stderr(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

fn stdout(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}
