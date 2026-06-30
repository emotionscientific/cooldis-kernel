use cooldis::{
    LocalAgentRegistry, LocalOperationRegistry, PublishedAgentRecord, PublishedOperationBuild,
    PublishedOperationRecord, PublishedOperationSource, RegisteredOperation,
    WasmOperationDefinition, WasmOperationManifest, WasmOperationValueKind,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use uuid::Uuid;

const TEST_OPERATION_HASH: &str =
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

#[test]
fn cooldis_cli_tool_help_is_canonical_and_op_is_removed() {
    let help = run_cooldis(["tool", "--help"]);
    assert!(help.contains("cooldis tool"));
    assert!(help.contains("cooldis tool list"));
    assert!(help.contains("cooldis tool publish"));

    let list_help = run_cooldis(["tool", "list", "--help"]);
    assert!(list_help.contains("cooldis tool list"));
    assert!(list_help.contains("--registry-root"));

    let publish_help = run_cooldis(["tool", "publish", "--help"]);
    assert!(publish_help.contains("cooldis tool publish --package"));
    assert!(!publish_help.contains("--module-path"));
    assert!(!publish_help.contains("--bin-path"));

    let run_help = run_cooldis(["tool", "run", "--help"]);
    assert!(run_help.contains("cooldis tool run"));

    let old = run_cooldis_failed(["op", "--help"]);
    assert!(stderr(&old).contains("cooldis op has been removed"));
}

#[test]
fn cooldis_cli_uses_console_rpc_and_dev_entrypoints() {
    let root = run_cooldis([]);
    assert!(root.contains("cooldis init"));
    assert!(root.contains("cooldis agent"));
    assert!(root.contains("cooldis tool"));
    assert!(root.contains("cooldis secret"));
    assert!(root.contains("cooldis rpc"));
    assert!(root.contains("cooldis console"));
    assert!(root.contains("cooldis dev chat"));
    assert!(!root.contains("cooldis [PROMPT]"));

    let rpc = run_cooldis(["rpc", "--help"]);
    assert!(rpc.contains("cooldis rpc"));
    assert!(rpc.contains("--listen"));

    let console = run_cooldis(["console", "--help"]);
    assert!(console.contains("local browser console"));
    assert!(console.contains("--no-open"));
    assert!(console.contains("--port"));

    let dev = run_cooldis(["dev", "--help"]);
    assert!(dev.contains("cooldis dev chat"));
    assert!(dev.contains("cooldis dev tui"));

    let chat = run_cooldis(["dev", "chat", "--help"]);
    assert!(chat.contains("cooldis dev chat"));

    let operator = run_cooldis(["operator", "--help"]);
    assert!(operator.contains("bundled local terminal console"));

    let naked_prompt = run_cooldis_failed(["hello"]);
    assert!(stderr(&naked_prompt).contains("unknown command"));

    let old_chat = run_cooldis_failed(["chat", "--help"]);
    assert!(stderr(&old_chat).contains("cooldis chat has moved"));

    let old_tui = run_cooldis_failed(["tui", "--help"]);
    assert!(stderr(&old_tui).contains("cooldis tui has moved"));

    let old_app_server = run_cooldis_failed(["app-server", "--help"]);
    assert!(stderr(&old_app_server).contains("cooldis app-server has been removed"));

    let old_tool_plan = run_cooldis_failed(["tool", "plan", "--help"]);
    assert!(stderr(&old_tool_plan).contains("unknown tool subcommand"));

    let thread_subcommand = run_cooldis_failed(["thread", "start"]);
    assert!(stderr(&thread_subcommand).contains("use rpc thread/* methods"));
}

#[test]
fn cooldis_cli_init_creates_folder_first_agent_project() {
    let workspace = temp_dir("agent-init-folder");
    let project = workspace.join("release-verifier");

    let output = run_cooldis([
        "init",
        "release-verifier",
        "--out",
        project.to_str().unwrap(),
    ]);
    assert!(output.contains(project.to_str().unwrap()));

    let manifest_path = project.join("cooldis.agent.toml");
    let prompt_path = project.join("prompts/system.md");
    let refs_path = project.join("components/operations.toml");
    let coupling_refs_path = project.join("components/couplings.toml");
    let operation_slot_path = project.join("operations/README.md");
    assert!(manifest_path.exists());
    assert!(prompt_path.exists());
    assert!(refs_path.exists());
    assert!(coupling_refs_path.exists());
    assert!(operation_slot_path.exists());

    let manifest = fs::read_to_string(&manifest_path).unwrap();
    assert!(manifest.contains("name = \"release-verifier\""));
    assert!(manifest.contains("provider_ref = \"provider://local_offline\""));
    assert!(manifest.contains("operation_ref = \"op://example-tool@sha256:"));
    let prompt = fs::read_to_string(&prompt_path).unwrap();
    assert!(prompt.contains("You are the release-verifier agent."));
    let refs = fs::read_to_string(&refs_path).unwrap();
    assert!(refs.contains("component-first"));
    let coupling_refs = fs::read_to_string(&coupling_refs_path).unwrap();
    assert!(coupling_refs.contains("std::queue.task"));
    assert!(coupling_refs.contains("std::context.spill"));
    assert!(coupling_refs.contains("channel_decision_required = true"));

    let registry_root = temp_dir("agent-init-registry");
    let missing_operation_registry_root = workspace.join("missing-operations");
    let plan = run_cooldis([
        "agent",
        "plan",
        manifest_path.to_str().unwrap(),
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--operations-registry-root",
        missing_operation_registry_root.to_str().unwrap(),
    ]);
    assert!(plan.contains("agent://release-verifier@0.1.0"));
    assert!(plan.contains("[unverified-offline]"));

    let duplicate = run_cooldis_failed([
        "init",
        "release-verifier",
        "--out",
        project.to_str().unwrap(),
    ]);
    assert!(stderr(&duplicate).contains("already exists"));
}

#[test]
fn cooldis_cli_agent_init_out_toml_keeps_single_file_compatibility() {
    let workspace = temp_dir("agent-init-single-file");
    let manifest_path = workspace.join("single-agent.cooldis.agent.toml");

    let output = run_cooldis([
        "agent",
        "init",
        "single-agent",
        "--out",
        manifest_path.to_str().unwrap(),
    ]);
    assert!(output.contains(manifest_path.to_str().unwrap()));
    assert!(manifest_path.exists());
    assert!(!workspace.join("prompts").exists());

    let manifest = fs::read_to_string(&manifest_path).unwrap();
    assert!(manifest.contains("name = \"single-agent\""));
    assert!(manifest.contains("cooldis.agent-manifest"));
}

#[test]
fn cooldis_cli_secret_import_list_and_status_redact_values() {
    let state_home = temp_dir("secret-state");

    let import = run_cooldis_with_env(
        [
            "secret",
            "import",
            "EXAMPLE_API_KEY",
            "--from-env",
            "COOLDIS_TEST_EXAMPLE_API_KEY",
            "--state-home",
            state_home.to_str().unwrap(),
        ],
        &[("COOLDIS_TEST_EXAMPLE_API_KEY", "fixture-secret")],
    );
    assert!(import.contains("imported secret EXAMPLE_API_KEY"));
    assert!(!import.contains("fixture-secret"));

    let list = run_cooldis([
        "secret",
        "list",
        "--state-home",
        state_home.to_str().unwrap(),
    ]);
    assert!(list.contains("EXAMPLE_API_KEY"));
    assert!(list.contains("env:COOLDIS_TEST_EXAMPLE_API_KEY"));
    assert!(!list.contains("fixture-secret"));

    let status = run_cooldis([
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
fn cooldis_cli_tool_run_reports_missing_registered_operation_secret_refs() {
    let registry_root = temp_dir("tool-run-missing-secret-registry");
    let state_home = temp_dir("tool-run-missing-secret-state");
    seed_operation_record(
        &registry_root,
        "secret-search",
        TEST_OPERATION_HASH,
        &[("search", &["secret:EXAMPLE_API_KEY"])],
    );

    let failed = run_cooldis_failed([
        "tool",
        "run",
        "secret-search",
        "search",
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--state-home",
        state_home.to_str().unwrap(),
        "--input",
        r#"{"query":"cooldis"}"#,
    ]);
    let err = stderr(&failed);

    assert!(err.contains("missing required operation secrets: EXAMPLE_API_KEY"));
    assert!(err.contains("cooldis secret import"));
    assert!(!err.contains("fixture-secret"));
}

#[test]
fn cooldis_cli_provider_auth_set_status_and_delete_redact_values() {
    let state_home = temp_dir("provider-auth-state");

    let set = run_cooldis_with_stdin(
        [
            "provider",
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

    let status = run_cooldis([
        "provider",
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

    let delete = run_cooldis([
        "provider",
        "auth",
        "delete",
        "openai_compatible",
        "--state-home",
        state_home.to_str().unwrap(),
    ]);
    assert!(delete.contains("deleted provider credential openai_compatible"));

    let status = run_cooldis([
        "provider",
        "auth",
        "status",
        "openai_compatible",
        "--state-home",
        state_home.to_str().unwrap(),
    ]);
    assert!(status.contains(r#""configured": false"#));
}

#[test]
fn cooldis_cli_tool_source_add_list_show_and_remove_redacts_remote_mcp_auth() {
    let state_home = temp_dir("tool-source-state");

    let add = run_cooldis([
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

    let list = run_cooldis([
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

    let sources: Vec<Value> = serde_json::from_str(&list).unwrap();
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0]["name"], "arcade");
    assert_eq!(sources[0]["transport"], "streamable_http");
    assert_eq!(sources[0]["include_tools"][0], "gmail_search");
    assert_eq!(sources[0]["auth"]["secret"], "arcade.api_key");
    assert_eq!(sources[0]["auth"]["value"]["redacted"], true);
    assert_eq!(sources[0]["headers"][0]["name"], "x-tenant");
    assert_eq!(sources[0]["headers"][0]["value"]["redacted"], true);

    let show = run_cooldis([
        "tool",
        "source",
        "show",
        "arcade",
        "--json",
        "--state-home",
        state_home.to_str().unwrap(),
    ]);
    let source: Value = serde_json::from_str(&show).unwrap();
    assert_eq!(source["name"], "arcade");
    assert_eq!(source["auth"]["secret"], "arcade.api_key");
    assert_eq!(source["auth"]["value"]["redacted"], true);
    assert_eq!(source["headers"][0]["name"], "x-tenant");
    assert_eq!(source["headers"][0]["value"]["redacted"], true);

    let remove = run_cooldis([
        "tool",
        "source",
        "remove",
        "arcade",
        "--state-home",
        state_home.to_str().unwrap(),
    ]);
    assert!(remove.contains("removed tool source arcade"));

    let empty = run_cooldis([
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
fn cooldis_cli_tool_build_gates_stateless_conversion() {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let module_path = repo.join("tests/fixtures/wasm-csv-profile");

    let build = run_cooldis([
        "tool",
        "build",
        "--module-path",
        module_path.to_str().unwrap(),
        "--name",
        "data",
        "--upstream-url",
        "https://github.com/emotionscientific/cooldis",
        "--upstream-rev",
        "fixture",
        "--upstream-crate",
        "cooldis-wasm-csv-profile",
    ]);
    assert!(build.contains("tool build data"));
    assert!(build.contains("conversion stateless_wasm"));
    assert!(build.contains("policy accepted"));
    assert!(build.contains("operation csv_profile json -> json"));

    let registry_root = temp_dir("strict-conversion-registry");
    let publish = run_cooldis_failed([
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
        "https://github.com/emotionscientific/cooldis",
        "--upstream-rev",
        "fixture",
        "--upstream-crate",
        "cooldis-wasm-csv-profile",
    ]);
    let publish_stderr = stderr(&publish);
    assert!(publish_stderr.contains("package proof gate"));
    assert!(publish_stderr.contains("--package"));
    assert!(
        !LocalOperationRegistry::new(&registry_root)
            .record_path("data")
            .unwrap()
            .exists()
    );

    let bad = temp_dir("stateful-conversion");
    fs::write(
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
    fs::create_dir_all(bad.join("src")).unwrap();
    fs::write(
        bad.join("src/lib.rs"),
        "#[unsafe(no_mangle)] pub extern \"C\" fn noop() {}\n",
    )
    .unwrap();

    let rejected = run_cooldis_failed([
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
fn cooldis_cli_tool_package_build_publish_and_persist_interface() {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
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
    fs::write(
        package.join("cooldis.tool.toml"),
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

    let manifest_path = package.join("cooldis.tool.toml");
    let build = run_cooldis([
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
    let publish = run_cooldis([
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
    let list = run_cooldis([
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

    let man = run_cooldis([
        "man",
        "data",
        "csv_profile",
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    assert!(man.contains("NAME"));
    assert!(man.contains("data csv_profile - Profile a CSV string."));
    assert!(man.contains("USAGE"));
    assert!(man.contains("cooldis tool run data csv_profile"));
    assert!(man.contains("EXIT STATUS"));

    let man_json = run_cooldis([
        "man",
        "data",
        "csv_profile",
        "--json",
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    let manuals: Vec<Value> = serde_json::from_str(&man_json).unwrap();
    assert_eq!(manuals[0]["tool_name"], "data");
    assert_eq!(manuals[0]["operation_name"], "csv_profile");
    assert_eq!(manuals[0]["generated"], false);

    let agent_manifest_path = package.join("vendor-risk-agent.cooldis.agent.toml");
    fs::write(
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
    run_cooldis([
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
fn cooldis_cli_rejects_user_authored_kernel_tool_packages() {
    let package = temp_dir("tool-package-kernel-rejected");
    write_json_file(
        &package.join("schemas/thread_spawn.input.json"),
        r#"{"type":"object","properties":{"message":{"type":"string"}},"required":["message"]}"#,
    );
    write_json_file(
        &package.join("schemas/thread_spawn.output.json"),
        r#"{"type":"object","properties":{"thread_id":{"type":"string"}}}"#,
    );
    fs::write(
        package.join("cooldis.tool.toml"),
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
    let manifest_path = package.join("cooldis.tool.toml");

    let build = run_cooldis_failed([
        "tool",
        "build",
        "--package",
        manifest_path.to_str().unwrap(),
    ]);
    let build_stderr = stderr(&build);
    assert!(build_stderr.contains("runtime.kind = \"kernel\""));
    assert!(build_stderr.contains("synthesized by Cooldis startup"));
    assert!(build_stderr.contains("tool build/publish cannot author or publish"));

    let registry_root = temp_dir("tool-package-kernel-registry");
    let publish = run_cooldis_failed([
        "tool",
        "publish",
        "--package",
        manifest_path.to_str().unwrap(),
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    let publish_stderr = stderr(&publish);
    assert!(publish_stderr.contains("runtime.kind = \"kernel\""));
    assert!(publish_stderr.contains("synthesized by Cooldis startup"));
    assert!(publish_stderr.contains("tool build/publish cannot author or publish"));
}

#[test]
fn cooldis_cli_tool_package_build_warns_on_generated_manuals() {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
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
    fs::write(
        package.join("cooldis.tool.toml"),
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

    let manifest_path = package.join("cooldis.tool.toml");
    let build = run_cooldis([
        "tool",
        "build",
        "--package",
        manifest_path.to_str().unwrap(),
    ]);
    assert!(build.contains("warning operation csv_profile has no description"));
    assert!(build.contains("warning operation csv_profile has no fixtures"));

    let registry_root = temp_dir("tool-generated-manual-registry");
    run_cooldis([
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
fn cooldis_cli_tool_package_build_runs_internal_http_fixture() {
    let (base_url, server) = spawn_employee_server();
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
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
    fs::write(
        package.join("cooldis.tool.toml"),
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

    let manifest_path = package.join("cooldis.tool.toml");
    let build = run_cooldis([
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
fn cooldis_cli_plans_publishes_lists_and_shows_agent_manifest() {
    let workspace = temp_dir("agent-manifest");
    let manifest_path = workspace.join("release-verifier.cooldis.agent.toml");
    fs::write(
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

    let offline_plan = run_cooldis([
        "agent",
        "plan",
        manifest_path.to_str().unwrap(),
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--operations-registry-root",
        missing_operation_registry_root.to_str().unwrap(),
    ]);
    assert!(offline_plan.contains("[unverified-offline]"));

    let plan = run_cooldis([
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
        LocalAgentRegistry::new(&registry_root)
            .list_records()
            .unwrap()
            .is_empty()
    );

    let publish = run_cooldis([
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

    let list = run_cooldis([
        "agent",
        "list",
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    assert!(list.contains("release-verifier"));
    assert!(list.contains("0.1.0"));
    assert!(list.contains("agent://release-verifier@0.1.0"));

    let show_by_name = run_cooldis([
        "agent",
        "show",
        "release-verifier",
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    assert!(show_by_name.contains("\"name\": \"release-verifier\""));
    assert!(show_by_name.contains("\"source_hash\""));

    let show_by_ref = run_cooldis([
        "agent",
        "show",
        "agent://release-verifier@0.1.0",
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    assert!(show_by_ref.contains("\"version\": \"0.1.0\""));
}

#[test]
fn cooldis_cli_tool_list_prints_records_and_empty_absent_root() {
    let registry_root = temp_dir("tool-list-registry");
    let record = seed_operation_record(
        &registry_root,
        "tailcat",
        TEST_OPERATION_HASH,
        &[("cat", &[]), ("tail", &[])],
    );

    let list = run_cooldis([
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
    let empty = run_cooldis([
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
fn cooldis_cli_flat_source_publish_is_refused_without_writing_record() {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let module_path = repo.join("tests/fixtures/wasm-vfs-tools");
    let registry_root = temp_dir("publish-source-refused");

    let refused = run_cooldis_failed([
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
        !LocalOperationRegistry::new(&registry_root)
            .record_path("tailcat")
            .unwrap()
            .exists()
    );
}

#[test]
fn cooldis_cli_flat_bin_publish_is_refused_without_writing_record() {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let module_path = repo.join("tests/fixtures/wasm-vfs-tools");
    let registry_root = temp_dir("publish-bin-refused");
    let build_output = run_cooldis([
        "tool",
        "build",
        "--module-path",
        module_path.to_str().unwrap(),
    ]);
    let artifact_path = build_output
        .lines()
        .find_map(|line| line.strip_prefix("artifact "))
        .expect("tool build should print an artifact path");

    let refused = run_cooldis_failed([
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
        !LocalOperationRegistry::new(&registry_root)
            .record_path("tailcat-bin")
            .unwrap()
            .exists()
    );
}

#[test]
fn cooldis_cli_flat_publish_refusal_preserves_previous_active_record() {
    let registry_root = temp_dir("atomic-republish");
    seed_operation_record(
        &registry_root,
        "tailcat",
        TEST_OPERATION_HASH,
        &[("cat", &[]), ("tail", &[])],
    );
    let before = registry_record(&registry_root, "tailcat");
    let invalid_path = registry_root.join("invalid.wasm");
    fs::write(&invalid_path, b"\0asm\x01\0\0\0\xff").unwrap();

    let failed = run_cooldis_failed([
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
fn cooldis_cli_registry_run_rejects_corrupt_or_mismatched_artifacts() {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let module_path = repo.join("tests/fixtures/wasm-vfs-tools");
    let registry_root = temp_dir("corrupt-blob");
    let mount = fixture_mount(&module_path);

    publish_vfs_tool_package(&registry_root, "tailcat");

    let registry = LocalOperationRegistry::new(&registry_root);
    let record = registry.load_record("tailcat").unwrap();
    let blob_path = registry
        .blobs()
        .artifact_path(&record.active_artifact_hash)
        .unwrap();
    fs::remove_file(&blob_path).unwrap();
    let missing = run_cooldis_failed([
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
    fs::write(&blob_path, b"corrupt").unwrap();
    let corrupt = run_cooldis_failed([
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
    let registered = RegisteredOperation {
        name: record.name.clone(),
        manifest: record.manifest.clone(),
        capability_grants: record.capability_grants.clone(),
        metadata: BTreeMap::new(),
    };
    record.projections = registered.projections();
    fs::write(
        registry.record_path("tailcat").unwrap(),
        serde_json::to_vec_pretty(&record).unwrap(),
    )
    .unwrap();

    let mismatch = run_cooldis_failed([
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
fn cooldis_cli_registry_run_rejects_missing_record() {
    let registry_root = temp_dir("missing-record");

    let missing = run_cooldis_failed([
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
fn cooldis_cli_flat_publish_config_file_is_refused() {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let module_path = repo.join("tests/fixtures/wasm-vfs-tools");
    let temp = temp_dir("config");
    let registry_root = temp.join("registry");
    let config_path = temp.join("cooldis.json");
    fs::write(
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

    let failed = run_cooldis_failed(["tool", "publish", "--config", config_path.to_str().unwrap()]);
    assert!(stderr(&failed).contains("package proof gate"));
    assert!(stderr(&failed).contains("--package"));
    assert!(
        !LocalOperationRegistry::new(&registry_root)
            .record_path("tailcat-config")
            .unwrap()
            .exists()
    );
}

#[test]
fn cooldis_cli_flat_publish_discovered_config_file_is_refused() {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let module_path = repo.join("tests/fixtures/wasm-vfs-tools");
    let temp = temp_dir("config-discovery");
    let registry_root = temp.join("registry");
    let config_path = temp.join("cooldis.json");
    fs::write(
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

    let failed = run_cooldis_failed_in_dir(&temp, ["tool", "publish"]);
    assert!(stderr(&failed).contains("package proof gate"));
    assert!(stderr(&failed).contains("--package"));
    assert!(
        !LocalOperationRegistry::new(&registry_root)
            .record_path("tailcat-discovered")
            .unwrap()
            .exists()
    );
}

fn publish_vfs_tool_package(registry_root: &Path, name: &str) -> PublishedOperationRecord {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
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
    fs::write(
        package.join("cooldis.tool.toml"),
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

    run_cooldis([
        "tool",
        "publish",
        "--package",
        package.join("cooldis.tool.toml").to_str().unwrap(),
        "--registry-root",
        registry_root.to_str().unwrap(),
    ]);
    registry_record(registry_root, name)
}

fn temp_dir(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("cooldis-cli-{label}-{}", Uuid::now_v7()));
    fs::create_dir_all(&path).unwrap();
    path
}

fn write_json_file(path: &Path, body: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, body).unwrap();
}

fn spawn_employee_server() -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");
    let handle = thread::spawn(move || {
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

fn fixture_mount(module_path: &Path) -> String {
    format!(
        "/workspace={}",
        module_path.join("testdata").to_string_lossy()
    )
}

fn registry_record(root: &Path, name: &str) -> PublishedOperationRecord {
    LocalOperationRegistry::new(root).load_record(name).unwrap()
}

fn seed_operation_record(
    root: &Path,
    name: &str,
    artifact_hash: &str,
    operations: &[(&str, &[&str])],
) -> PublishedOperationRecord {
    let registry = LocalOperationRegistry::new(root);
    let manifest = WasmOperationManifest {
        abi: "cooldis.operation/0.1".to_string(),
        operations: operations
            .iter()
            .enumerate()
            .map(
                |(index, (operation_name, required_capabilities))| WasmOperationDefinition {
                    id: (index + 1) as u32,
                    name: (*operation_name).to_string(),
                    input: WasmOperationValueKind::Text,
                    output: WasmOperationValueKind::Text,
                    events: Default::default(),
                    mode: Default::default(),
                    required_capabilities: required_capabilities
                        .iter()
                        .map(|capability| (*capability).to_string())
                        .collect(),
                },
            )
            .collect(),
    };
    let capability_grants = operations
        .iter()
        .flat_map(|(_, required_capabilities)| {
            required_capabilities
                .iter()
                .map(|capability| (*capability).to_string())
        })
        .collect::<BTreeSet<_>>();
    let registered = RegisteredOperation {
        name: name.to_string(),
        manifest: manifest.clone(),
        capability_grants: capability_grants.clone(),
        metadata: BTreeMap::new(),
    };
    let record = PublishedOperationRecord {
        schema_version: 1,
        name: name.to_string(),
        active_artifact_hash: artifact_hash.to_string(),
        manifest,
        projections: registered.projections(),
        interface: None,
        capability_grants,
        metadata: BTreeMap::new(),
        source: PublishedOperationSource::Kernel {
            package: "test".to_string(),
        },
        build: PublishedOperationBuild {
            artifact_path: PathBuf::from("<test>"),
            published_at_ms: 0,
        },
    };
    record.validate().unwrap();
    let record_path = registry.record_path(name).unwrap();
    fs::create_dir_all(record_path.parent().unwrap()).unwrap();
    fs::write(&record_path, serde_json::to_vec_pretty(&record).unwrap()).unwrap();
    let version_path = registry.version_record_path(name, artifact_hash).unwrap();
    fs::create_dir_all(version_path.parent().unwrap()).unwrap();
    fs::write(&version_path, serde_json::to_vec_pretty(&record).unwrap()).unwrap();
    record
}

fn agent_record(root: &Path, name: &str) -> PublishedAgentRecord {
    LocalAgentRegistry::new(root).load_record(name).unwrap()
}

fn run_cooldis<const N: usize>(args: [&str; N]) -> String {
    run_cooldis_command(Command::new(env!("CARGO_BIN_EXE_cooldis")).args(args))
}

fn run_cooldis_with_env<const N: usize>(args: [&str; N], envs: &[(&str, &str)]) -> String {
    let mut command = Command::new(env!("CARGO_BIN_EXE_cooldis"));
    command.args(args);
    for (key, value) in envs {
        command.env(key, value);
    }
    run_cooldis_command(&mut command)
}

fn run_cooldis_with_stdin<const N: usize>(args: [&str; N], stdin: &str) -> String {
    let mut child = Command::new(env!("CARGO_BIN_EXE_cooldis"))
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn cooldis cli");
    child
        .stdin
        .as_mut()
        .expect("cooldis stdin should be piped")
        .write_all(stdin.as_bytes())
        .expect("failed to write cooldis stdin");
    let output = child.wait_with_output().expect("failed to run cooldis cli");
    assert!(
        output.status.success(),
        "cooldis cli failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("cooldis output should be utf8")
}

fn run_cooldis_failed_in_dir<const N: usize>(dir: &Path, args: [&str; N]) -> Output {
    let output = Command::new(env!("CARGO_BIN_EXE_cooldis"))
        .current_dir(dir)
        .args(args)
        .output()
        .expect("failed to run cooldis cli");
    assert!(
        !output.status.success(),
        "cooldis cli unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn run_cooldis_command(command: &mut Command) -> String {
    let output = command.output().expect("failed to run cooldis cli");
    assert!(
        output.status.success(),
        "cooldis cli failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("cooldis output should be utf8")
}

fn run_cooldis_failed<const N: usize>(args: [&str; N]) -> Output {
    let output = Command::new(env!("CARGO_BIN_EXE_cooldis"))
        .args(args)
        .output()
        .expect("failed to run cooldis cli");
    assert!(
        !output.status.success(),
        "cooldis cli unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}
