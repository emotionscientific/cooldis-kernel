use super::*;
use crate::{
    AsyncExecutionManager, AsyncProcessStartRequest, CanonicalContent, CanonicalMessage,
    CooldisProcessBackend, CooldisProcessEventKind, CooldisProcessHandle,
    CooldisProcessTerminalState, CooldisVfsBackend, LiveProcessBackend, LiveProcessSpawn,
    OperationRegistration, ProcessSnapshotStatus, ReadOnlyFileSystem, RuntimeHost,
    ThreadCoordinates, ThreadTopology, VfsMutationKind, WasmRuntimeArtifact,
};
use object_store::memory::InMemory as InMemoryObjectStore;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

async fn expect_output(events: &mut broadcast::Receiver<ThreadEvent>) -> String {
    loop {
        match events.recv().await.unwrap() {
            ThreadEvent::Output { text, .. } => return text,
            ThreadEvent::Failed { message, .. } => panic!("thread failed: {message}"),
            _ => {}
        }
    }
}

fn wat_bytes(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| match byte {
            b'\n' => "\\0a".to_string(),
            b'\r' => "\\0d".to_string(),
            b'\t' => "\\09".to_string(),
            b'"' => "\\22".to_string(),
            b'\\' => "\\5c".to_string(),
            0x20..=0x7e => (*byte as char).to_string(),
            _ => format!("\\{byte:02x}"),
        })
        .collect()
}

fn wat_guest(wat: impl AsRef<str>) -> Vec<u8> {
    wat::parse_str(wat.as_ref()).expect("test WAT fixture should compile to wasm")
}

fn process_test_provider() -> (BashToolProvider, TurnContextSnapshot) {
    let store: Arc<dyn crate::RuntimeStore> = Arc::new(crate::InMemorySessionStore::new());
    let coordinates = ThreadCoordinates::new("tenant", "user", "process-tool-session");
    let dispatcher = crate::kernel::process_handle_dispatch::test_process_dispatcher(
        Arc::clone(&store),
        coordinates.clone(),
    );
    let context = crate::TurnContext::new(
        crate::ThreadContext::root(coordinates),
        "process-tool-turn",
        &crate::TurnInput::text("process tool test"),
        tokio_util::sync::CancellationToken::new(),
    )
    .snapshot();
    (
        BashToolProvider::new(VirtualBashRuntimeConfig::default())
            .with_process_dispatcher(dispatcher),
        context,
    )
}

fn tool_result_json(message: CanonicalMessage) -> (serde_json::Value, bool) {
    let CanonicalMessage::ToolResult {
        content, is_error, ..
    } = message
    else {
        panic!("expected tool result");
    };
    let text = content
        .iter()
        .filter_map(|content| match content {
            CanonicalContent::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("");
    (serde_json::from_str(&text).unwrap(), is_error)
}

fn tool_result_text(message: CanonicalMessage) -> (String, bool) {
    let CanonicalMessage::ToolResult {
        content, is_error, ..
    } = message
    else {
        panic!("expected tool result");
    };
    let text = content
        .iter()
        .filter_map(|content| match content {
            CanonicalContent::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("");
    (text, is_error)
}

async fn invoke_bash(
    provider: &BashToolProvider,
    call_id: &str,
    command: &str,
) -> CanonicalMessage {
    provider
        .invoke_tool_call(AgentKernelToolCall {
            call_id: call_id.to_string(),
            tool_name: BASH_TOOL.to_string(),
            arguments: serde_json::json!({ "command": command }),
            turn_context: None,
        })
        .await
        .unwrap()
        .unwrap()
}

fn echo_operation_guest() -> String {
    let manifest = serde_json::json!({
        "abi": "cooldis.operation/0.1",
        "operations": [{
            "id": 1,
            "name": "echo",
            "input": "bytes",
            "output": "bytes",
            "events": "jsonl",
            "mode": "sync",
            "required_capabilities": []
        }]
    })
    .to_string();
    let event = br#"{"type":"cooldis_run","operation":"echo"}
"#;
    format!(
        r#"
            (module
              (import "cooldis_0.1" "source_read" (func $source_read (param i32 i32 i32) (result i32)))
              (import "cooldis_0.1" "sink_write" (func $sink_write (param i32 i32 i32) (result i32)))
              (import "cooldis_0.1" "event_emit" (func $event_emit (param i32 i32 i32) (result i32)))
              (memory (export "memory") 1)
              (data (i32.const 4096) "{manifest}")
              (data (i32.const 8192) "op:")
              (data (i32.const 8200) "{event}")
              (func (export "__cooldis_describe_module__") (param $sink i32) (result i32)
                i32.const 0
                i32.const {manifest_len}
                i32.store
                local.get $sink
                i32.const 4096
                i32.const 0
                call $sink_write)
              (func (export "__cooldis_call_operation__")
                (param $op i32)
                (param $invocation i32)
                (param $source i32)
                (param $output i32)
                (param $events i32)
                (result i32)
                (local $n i32)
                local.get $op
                i32.const 1
                i32.ne
                if
                  i32.const 2
                  return
                end
                i32.const 0
                i32.const 1024
                i32.store
                local.get $source
                i32.const 1024
                i32.const 0
                call $source_read
                drop
                i32.const 0
                i32.load
                local.set $n
                i32.const 0
                i32.const {event_len}
                i32.store
                local.get $invocation
                i32.const 8200
                i32.const 0
                call $event_emit
                drop
                i32.const 0
                i32.const 3
                i32.store
                local.get $output
                i32.const 8192
                i32.const 0
                call $sink_write
                drop
                i32.const 0
                local.get $n
                i32.store
                local.get $output
                i32.const 1024
                i32.const 0
                call $sink_write
                drop
                i32.const 0))
            "#,
        manifest = wat_bytes(manifest.as_bytes()),
        manifest_len = manifest.len(),
        event = wat_bytes(event),
        event_len = event.len(),
    )
}

async fn echo_operation_registry() -> Arc<OperationRegistry> {
    let registry = Arc::new(OperationRegistry::new());
    registry
        .register(OperationRegistration::new(
            "echoer",
            WasmRuntimeArtifact::bytes(wat_guest(echo_operation_guest())),
        ))
        .await
        .unwrap();
    registry
}

fn named_echo_operation_guest(operation_name: &str) -> String {
    named_echo_operation_guest_with_required(operation_name, Vec::new())
}

fn named_echo_operation_guest_with_required(
    operation_name: &str,
    required_capabilities: Vec<&str>,
) -> String {
    let manifest = serde_json::json!({
        "abi": "cooldis.operation/0.1",
        "operations": [{
            "id": 1,
            "name": operation_name,
            "input": "bytes",
            "output": "bytes",
            "events": "none",
            "mode": "sync",
            "required_capabilities": required_capabilities
        }]
    })
    .to_string();
    format!(
        r#"
            (module
              (import "cooldis_0.1" "source_read" (func $source_read (param i32 i32 i32) (result i32)))
              (import "cooldis_0.1" "sink_write" (func $sink_write (param i32 i32 i32) (result i32)))
              (memory (export "memory") 1)
              (data (i32.const 4096) "{manifest}")
              (data (i32.const 8192) "op:")
              (func (export "__cooldis_describe_module__") (param $sink i32) (result i32)
                i32.const 0
                i32.const {manifest_len}
                i32.store
                local.get $sink
                i32.const 4096
                i32.const 0
                call $sink_write)
              (func (export "__cooldis_call_operation__")
                (param $op i32)
                (param $invocation i32)
                (param $source i32)
                (param $output i32)
                (param $events i32)
                (result i32)
                (local $n i32)
                local.get $op
                i32.const 1
                i32.ne
                if
                  i32.const 2
                  return
                end
                i32.const 0
                i32.const 1024
                i32.store
                local.get $source
                i32.const 1024
                i32.const 0
                call $source_read
                drop
                i32.const 0
                i32.load
                local.set $n
                i32.const 0
                i32.const 3
                i32.store
                local.get $output
                i32.const 8192
                i32.const 0
                call $sink_write
                drop
                i32.const 0
                local.get $n
                i32.store
                local.get $output
                i32.const 1024
                i32.const 0
                call $sink_write
                drop
                i32.const 0))
            "#,
        manifest = wat_bytes(manifest.as_bytes()),
        manifest_len = manifest.len(),
    )
}

async fn named_echo_operation_registry(
    registered_name: &str,
    operation_name: &str,
) -> Arc<OperationRegistry> {
    let registry = Arc::new(OperationRegistry::new());
    registry
        .register(OperationRegistration::new(
            registered_name,
            WasmRuntimeArtifact::bytes(wat_guest(named_echo_operation_guest(operation_name))),
        ))
        .await
        .unwrap();
    registry
}

async fn named_echo_operation_registry_with_required(
    registered_name: &str,
    operation_name: &str,
    required_capabilities: Vec<&str>,
) -> Arc<OperationRegistry> {
    let registry = Arc::new(OperationRegistry::new());
    let mut registration = OperationRegistration::new(
        registered_name,
        WasmRuntimeArtifact::bytes(wat_guest(named_echo_operation_guest_with_required(
            operation_name,
            required_capabilities.clone(),
        ))),
    );
    registration =
        registration.with_capability_grants(required_capabilities.into_iter().map(String::from));
    registry.register(registration).await.unwrap();
    registry
}

#[tokio::test]
async fn harness_runs_virtual_file_commands_pipes_and_patch() {
    let mut harness = BashkitExecutionHarness::new(VirtualBashRuntimeConfig::default())
        .await
        .unwrap();

    let output = harness
        .execute(
            "pwd && mkdir -p dir && touch dir/touched.txt \
                 && echo alpha > dir/a.txt && echo beta >> dir/a.txt \
                 && cp /skills/README.md dir/skill.txt \
                 && head -n 1 dir/a.txt && tail -n 1 dir/a.txt && wc -l < dir/a.txt \
                 && stat dir/a.txt >/dev/null \
                 && cp dir/a.txt dir/b.txt && mv dir/b.txt dir/c.txt \
                 && cat dir/c.txt | grep beta && rm dir/c.txt",
        )
        .await
        .unwrap();
    assert!(output.success(), "{output:?}");
    assert!(output.stdout.contains("/workspace"));
    assert!(output.stdout.contains("alpha"));
    assert!(output.stdout.contains("beta"));
    assert!(output.stdout.contains("2"));
    let copied_skill = harness.read_file("/workspace/dir/skill.txt").await.unwrap();
    assert!(String::from_utf8(copied_skill).unwrap().contains("Cooldis"));

    let output = harness.execute("cd /tmp && pwd").await.unwrap();
    assert_eq!(output.stdout.trim(), "/tmp");
    let output = harness.execute("pwd").await.unwrap();
    assert_eq!(output.stdout.trim(), "/tmp");
    let output = harness.execute("cd /workspace").await.unwrap();
    assert!(output.success(), "{output:?}");

    let patch = r#"apply_patch <<'PATCH'
*** Begin Patch
*** Update File: dir/a.txt
@@
-alpha
+gamma
*** End Patch
PATCH"#;
    let output = harness.execute(patch).await.unwrap();
    assert!(output.success(), "{output:?}");
    let content = harness.read_file("/workspace/dir/a.txt").await.unwrap();
    assert_eq!(String::from_utf8(content).unwrap(), "gamma\nbeta\n");

    let patch = r#"apply_patch <<'PATCH'
*** Begin Patch
*** Add File: dir/new.txt
+fresh
*** Update File: dir/new.txt
*** Move to: dir/moved.txt
@@
-fresh
+moved
*** Delete File: dir/touched.txt
*** Update File: dir/a.txt
@@
 beta
+omega
*** End of File
*** End Patch
PATCH"#;
    let output = harness.execute(patch).await.unwrap();
    assert!(output.success(), "{output:?}");
    assert!(output.stdout.contains("A dir/new.txt"));
    assert!(output.stdout.contains("M dir/moved.txt"));
    assert!(output.stdout.contains("D dir/touched.txt"));
    let moved = harness.read_file("/workspace/dir/moved.txt").await.unwrap();
    assert_eq!(String::from_utf8(moved).unwrap(), "moved\n");
    let content = harness.read_file("/workspace/dir/a.txt").await.unwrap();
    assert_eq!(String::from_utf8(content).unwrap(), "gamma\nbeta\nomega\n");
    assert!(
        harness
            .read_file("/workspace/dir/touched.txt")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn virtual_bash_cooldis_run_invokes_registered_operation_from_pipe() {
    let config = VirtualBashRuntimeConfig::default()
        .with_operation_registry(echo_operation_registry().await);
    let mut harness = BashkitExecutionHarness::new(config).await.unwrap();

    let output = harness
        .execute("echo hello | cooldis run echoer echo")
        .await
        .unwrap();

    assert!(output.success(), "{output:?}");
    assert_eq!(output.stdout, "op:hello\n");
    assert!(output.stderr.contains(r#""operation":"echo""#));
}

#[tokio::test]
async fn virtual_bash_projects_registry_operations_as_host_builtins() {
    let config = VirtualBashRuntimeConfig::default()
        .with_operation_registry(named_echo_operation_registry("search", "search").await);
    let mut harness = BashkitExecutionHarness::new(config).await.unwrap();

    let output = harness
        .execute("command -v search && command -V search && printf cooldis | search")
        .await
        .unwrap();

    assert!(output.success(), "{output:?}");
    assert!(output.stdout.starts_with("search\n"), "{output:?}");
    assert!(output.stdout.contains("search is a shell builtin"));
    assert!(output.stdout.contains("op:cooldis"));
}

#[tokio::test]
async fn virtual_bash_man_describes_projected_operation_command() {
    let config = VirtualBashRuntimeConfig::default()
        .with_operation_registry(named_echo_operation_registry("search", "search").await);
    let mut harness = BashkitExecutionHarness::new(config).await.unwrap();

    let output = harness.execute("man search").await.unwrap();

    assert!(output.success(), "{output:?}");
    assert!(output.stdout.contains("NAME"));
    assert!(output.stdout.contains("search - search from search"));
    assert!(output.stdout.contains("cooldis run search search"));
    assert!(output.stdout.contains("STDIN"));
    assert!(output.stdout.contains("STDOUT"));
    assert!(output.stdout.contains("EXIT STATUS"));
}

#[tokio::test]
async fn virtual_bash_host_builtins_reflect_registry_add_and_remove_without_rebuild() {
    let registry = Arc::new(OperationRegistry::new());
    let config = VirtualBashRuntimeConfig::default().with_operation_registry(registry.clone());
    let mut harness = BashkitExecutionHarness::new(config).await.unwrap();

    let before = harness.execute("command -v search").await.unwrap();
    assert_ne!(before.exit_code, 0, "{before:?}");

    registry
        .register(OperationRegistration::new(
            "search",
            WasmRuntimeArtifact::bytes(wat_guest(named_echo_operation_guest("search"))),
        ))
        .await
        .unwrap();
    let after_register = harness
        .execute("command -v search && printf cooldis | search")
        .await
        .unwrap();
    assert!(after_register.success(), "{after_register:?}");
    assert!(after_register.stdout.contains("search\n"));
    assert!(after_register.stdout.contains("op:cooldis"));

    registry.unregister("search").await.unwrap();
    let after_remove = harness.execute("search cooldis").await.unwrap();
    assert_ne!(after_remove.exit_code, 0, "{after_remove:?}");
    assert!(
        after_remove.stderr.contains("not found") || after_remove.stderr.contains("command"),
        "{after_remove:?}"
    );
}

#[tokio::test]
async fn virtual_bash_reserved_operation_names_are_not_projected_as_shell_commands() {
    let registry = named_echo_operation_registry("capsule", "type").await;
    let registry_adapter = KernelVbashOperationRegistry::new(Arc::clone(&registry));
    let shell_commands =
        operation_shell_command_names(&registry_adapter, &reserved_operation_shell_commands())
            .await;
    assert!(!shell_commands.contains("type"));

    let config = VirtualBashRuntimeConfig::default().with_operation_registry(registry);
    let mut harness = BashkitExecutionHarness::new(config).await.unwrap();

    let output = harness.execute("printf cooldis | type").await.unwrap();

    assert!(!output.stdout.contains("op:cooldis"), "{output:?}");
    assert!(!output.stderr.contains("op:cooldis"), "{output:?}");
}

#[tokio::test]
async fn virtual_bash_operation_shell_commands_enforce_capability_grants() {
    let registry = named_echo_operation_registry_with_required(
        "secret-search",
        "search",
        vec!["secret:EXAMPLE_API_KEY"],
    )
    .await;
    let config = VirtualBashRuntimeConfig::default().with_operation_registry(registry.clone());
    let mut denied = BashkitExecutionHarness::new(config).await.unwrap();

    let output = denied.execute("printf cooldis | search").await.unwrap();
    assert_eq!(output.exit_code, 126, "{output:?}");
    assert!(
        output
            .stderr
            .contains("missing capability grants: secret:EXAMPLE_API_KEY"),
        "{output:?}"
    );

    let config = VirtualBashRuntimeConfig::default()
        .with_operation_registry(registry)
        .with_capability_grant("secret:EXAMPLE_API_KEY");
    let mut granted = BashkitExecutionHarness::new(config).await.unwrap();
    let output = granted.execute("printf cooldis | search").await.unwrap();

    assert!(output.success(), "{output:?}");
    assert!(output.stdout.contains("op:cooldis"));
}

#[tokio::test]
async fn virtual_bash_cooldis_run_works_with_vfs_redirection() {
    let config = VirtualBashRuntimeConfig::default()
        .with_operation_registry(echo_operation_registry().await)
        .with_writable_mount("/work");
    let mut harness = BashkitExecutionHarness::new(config).await.unwrap();
    harness
        .execute("printf '{\"query\":\"cooldis\"}' > /work/input.json")
        .await
        .unwrap();

    let output = harness
        .execute(
            "cooldis run echoer echo < /work/input.json > /work/output.json 2> /work/events.jsonl",
        )
        .await
        .unwrap();

    assert!(output.success(), "{output:?}");
    assert_eq!(
        String::from_utf8(harness.read_file("/work/output.json").await.unwrap()).unwrap(),
        "op:{\"query\":\"cooldis\"}"
    );
    assert!(
        String::from_utf8(harness.read_file("/work/events.jsonl").await.unwrap())
            .unwrap()
            .contains(r#""type":"cooldis_run""#)
    );
}

#[tokio::test]
async fn virtual_bash_execute_process_runs_cooldis_operation_with_stdin() {
    let config = VirtualBashRuntimeConfig::default()
        .with_operation_registry(echo_operation_registry().await);
    let mut harness = BashkitExecutionHarness::new(config).await.unwrap();

    let process = harness
        .execute_process("echo hello | cooldis run echoer echo")
        .await
        .unwrap();
    let output = process.output();

    assert_eq!(process.backend(), &CooldisProcessBackend::VirtualBash);
    assert_eq!(output.stdout_text_lossy(), "op:hello\n");
    assert!(output.stderr_text_lossy().contains(r#""operation":"echo""#));
    assert_eq!(output.exit_code(), Some(0));
    assert!(output.success());
}

#[tokio::test]
async fn harness_execute_process_replays_virtual_output_and_exit() {
    let mut harness = BashkitExecutionHarness::new(VirtualBashRuntimeConfig::default())
        .await
        .unwrap();

    let process = harness
        .execute_process("echo hi && ls /missing")
        .await
        .unwrap();
    let output = process.output();
    let replay = VirtualCommandOutput::from(&output);

    assert_eq!(process.backend(), &CooldisProcessBackend::VirtualBash);
    assert!(output.stdout_text_lossy().contains("hi"));
    assert!(output.stderr_text_lossy().contains("missing"));
    assert_ne!(output.exit_code(), Some(0));
    assert_eq!(replay.stdout, output.stdout_text_lossy());
    assert_eq!(replay.stderr, output.stderr_text_lossy());
    assert_eq!(replay.exit_code, output.exit_code().unwrap());
}

#[tokio::test]
async fn harness_rejects_readonly_mount_and_native_command() {
    let mut harness = BashkitExecutionHarness::new(VirtualBashRuntimeConfig::default())
        .await
        .unwrap();

    let output = harness
        .execute("cat /skills/README.md && echo nope > /skills/README.md")
        .await
        .unwrap();
    assert_ne!(output.exit_code, 0);
    assert!(output.stdout.contains("Cooldis virtual bash"));
    assert!(output.stderr.contains("read-only") || output.stderr.contains("denied"));

    let output = harness.execute("cargo test").await.unwrap();
    assert_ne!(output.exit_code, 0);
    assert!(
        output.stderr.contains("not found")
            || output.stderr.contains("command")
            || output.stderr.contains("cargo")
    );

    let output = harness.execute("ls /missing").await.unwrap();
    assert_ne!(output.exit_code, 0);
    assert!(output.stderr.contains("not found") || output.stderr.contains("missing"));
}

#[derive(Default)]
struct RecordingExternalExecutor {
    requests: Mutex<Vec<ExternalCommandRequest>>,
}

#[async_trait]
impl ExternalCommandExecutor for RecordingExternalExecutor {
    async fn exec(
        &self,
        request: ExternalCommandRequest,
    ) -> CooldisProcessResult<ExternalCommandResult> {
        self.requests.lock().await.push(request.clone());
        match &request.invocation {
            ExternalCommandInvocation::Argv { command, args } => {
                Ok(ExternalCommandResult::new(VirtualCommandOutput {
                    stdout: format!(
                        "{command} args={} stdin={}",
                        args.join(" "),
                        request.stdin.unwrap_or_default()
                    ),
                    stderr: String::new(),
                    exit_code: 0,
                    stdout_truncated: false,
                    stderr_truncated: false,
                }))
            }
            ExternalCommandInvocation::Script(_) => {
                let prefix = match request.executor {
                    ExternalExecutorKind::HostBash => "host",
                    ExternalExecutorKind::RemoteLinux => "remote",
                };
                Ok(ExternalCommandResult::new(VirtualCommandOutput {
                    stdout: format!("{prefix} stdout\n"),
                    stderr: format!("{prefix} stderr\n"),
                    exit_code: 7,
                    stdout_truncated: false,
                    stderr_truncated: false,
                })
                .with_file_write(ExternalFileWrite::new(
                    "/workspace/generated.txt",
                    format!("from {prefix}\n"),
                )))
            }
        }
    }
}

#[tokio::test]
async fn host_always_bypasses_bashkit_and_runs_in_host_cwd() {
    let host_root =
        std::env::temp_dir().join(format!("cooldis-host-bash-test-{}", uuid::Uuid::now_v7()));
    tokio::fs::create_dir_all(&host_root).await.unwrap();
    let canonical_host_root = tokio::fs::canonicalize(&host_root).await.unwrap();
    tokio::fs::write(host_root.join("host.txt"), "host file\n")
        .await
        .unwrap();
    let config = VirtualBashRuntimeConfig::default()
        .with_execution_policy(BashExecutionPolicy::host_always())
        .with_host_bash_executor(&host_root);
    let mut harness = BashkitExecutionHarness::new(config).await.unwrap();

    let process = harness
        .execute_process("pwd; cat host.txt; echo host err >&2; exit 3")
        .await
        .unwrap();
    let output = process.output();

    assert_eq!(process.backend(), &CooldisProcessBackend::HostBash);
    assert!(
        output
            .stdout_text_lossy()
            .contains(&canonical_host_root.display().to_string())
    );
    assert!(output.stdout_text_lossy().contains("host file"));
    assert!(output.stderr_text_lossy().contains("host err"));
    assert_eq!(output.exit_code(), Some(3));
    tokio::fs::remove_dir_all(host_root).await.unwrap();
}

#[tokio::test]
async fn host_always_can_list_real_repo_bin_dir() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let config = VirtualBashRuntimeConfig::default()
        .with_execution_policy(BashExecutionPolicy::host_always())
        .with_host_bash_executor(&repo_root);
    let mut harness = BashkitExecutionHarness::new(config).await.unwrap();

    let process = harness.execute_process("ls src/bin").await.unwrap();
    let output = process.output();

    assert_eq!(process.backend(), &CooldisProcessBackend::HostBash);
    assert!(output.success(), "{output:?}");
    assert!(output.stdout_text_lossy().contains("cooldis.rs"));
    assert!(
        !output
            .stdout_text_lossy()
            .contains("cooldis-vbash-smoke.rs")
    );
}

#[tokio::test]
async fn selective_proxy_routes_named_command_through_executor() {
    let executor = Arc::new(RecordingExternalExecutor::default());
    let policy = BashExecutionPolicy::selective([("cargo", CommandRoute::RemoteLinux)]);
    let mut harness = BashkitExecutionHarness::new(
        VirtualBashRuntimeConfig::default()
            .with_execution_policy(policy)
            .with_external_executor(executor.clone()),
    )
    .await
    .unwrap();

    let output = harness
        .execute("echo hi | cargo test > /workspace/out.txt")
        .await
        .unwrap();

    assert!(output.success(), "{output:?}");
    assert_eq!(
        String::from_utf8(harness.read_file("/workspace/out.txt").await.unwrap()).unwrap(),
        "cargo args=test stdin=hi\n"
    );
    let requests = executor.requests.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].executor, ExternalExecutorKind::RemoteLinux);
    assert_eq!(requests[0].cwd, PathBuf::from("/workspace"));
    assert_eq!(requests[0].stdin.as_deref(), Some("hi\n"));
    assert_eq!(requests[0].max_output_bytes, SPILL_RETENTION_MAX_BYTES);
    assert_eq!(
        requests[0].invocation,
        ExternalCommandInvocation::Argv {
            command: "cargo".to_string(),
            args: vec!["test".to_string()]
        }
    );
}

#[tokio::test]
async fn selective_proxy_sub_cap_pipeline_matches_the_legacy_result() {
    let executor = Arc::new(RecordingExternalExecutor::default());
    let policy = BashExecutionPolicy::selective([("cargo", CommandRoute::RemoteLinux)]);
    let mut harness = BashkitExecutionHarness::new(
        VirtualBashRuntimeConfig::default()
            .with_execution_policy(policy)
            .with_external_executor(executor),
    )
    .await
    .unwrap();

    let output = harness
        .execute("printf input | cargo test | sed 's/cargo/CARGO/'")
        .await
        .unwrap();

    assert_eq!(output.stdout, "CARGO args=test stdin=input\n");
    assert!(output.stderr.is_empty());
    assert_eq!(output.exit_code, 0);
    assert!(!output.stdout_truncated);
    assert!(!output.stderr_truncated);
}

#[tokio::test]
async fn selective_proxy_deny_does_not_invoke_executor() {
    let executor = Arc::new(RecordingExternalExecutor::default());
    let policy = BashExecutionPolicy::selective([("cargo", CommandRoute::Deny)]);
    let mut harness = BashkitExecutionHarness::new(
        VirtualBashRuntimeConfig::default()
            .with_execution_policy(policy)
            .with_external_executor(executor.clone()),
    )
    .await
    .unwrap();

    let output = harness.execute("cargo test").await.unwrap();

    assert_eq!(output.exit_code, 126);
    assert!(output.stderr.contains("denied by routing policy"));
    assert!(executor.requests.lock().await.is_empty());
}

#[tokio::test]
async fn harness_execute_process_records_host_external_result_and_file_deltas() {
    let executor = Arc::new(RecordingExternalExecutor::default());
    let mut harness = BashkitExecutionHarness::new(
        VirtualBashRuntimeConfig::default()
            .with_execution_policy(BashExecutionPolicy::host_always())
            .with_external_executor(executor),
    )
    .await
    .unwrap();

    let process = harness.execute_process("cargo test --lib").await.unwrap();
    let output = process.output();

    assert_eq!(process.backend(), &CooldisProcessBackend::HostBash);
    assert_eq!(output.stdout_text_lossy(), "host stdout\n");
    assert_eq!(output.stderr_text_lossy(), "host stderr\n");
    assert_eq!(output.exit_code(), Some(7));
    assert_eq!(output.file_deltas.len(), 1);
    assert_eq!(
        output.file_deltas[0].path,
        PathBuf::from("/workspace/generated.txt")
    );
    assert_eq!(
        String::from_utf8(harness.read_file("/workspace/generated.txt").await.unwrap()).unwrap(),
        "from host\n"
    );
}

#[tokio::test]
async fn harness_execute_process_records_remote_linux_backend() {
    let executor = Arc::new(RecordingExternalExecutor::default());
    let mut harness = BashkitExecutionHarness::new(
        VirtualBashRuntimeConfig::default()
            .with_execution_policy(BashExecutionPolicy::remote_always())
            .with_external_executor(executor.clone()),
    )
    .await
    .unwrap();

    let process = harness.execute_process("uname -a").await.unwrap();
    let output = process.output();

    assert_eq!(process.backend(), &CooldisProcessBackend::RemoteLinux);
    assert_eq!(output.stdout_text_lossy(), "remote stdout\n");
    let requests = executor.requests.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].executor, ExternalExecutorKind::RemoteLinux);
    assert_eq!(requests[0].max_output_bytes, SPILL_RETENTION_MAX_BYTES);
    assert_eq!(
        requests[0].invocation,
        ExternalCommandInvocation::Script("uname -a".to_string())
    );
}

struct SlowDeadlineExecutor;

#[async_trait]
impl ExternalCommandExecutor for SlowDeadlineExecutor {
    async fn exec(
        &self,
        request: ExternalCommandRequest,
    ) -> CooldisProcessResult<ExternalCommandResult> {
        let slow = tokio::time::sleep(Duration::from_secs(60));
        match tokio::time::timeout(request.deadline.remaining(), slow).await {
            Ok(_) => unreachable!("slow executor should outlive the deadline"),
            Err(_) => Ok(ExternalCommandResult::new(VirtualCommandOutput {
                stdout: String::new(),
                stderr: "host bash exec timed out\n".to_string(),
                exit_code: 124,
                stdout_truncated: false,
                stderr_truncated: false,
            })),
        }
    }
}

struct SerialNonObservingExternalExecutor {
    started: AtomicUsize,
    started_notify: tokio::sync::Notify,
    first_released: AtomicBool,
    first_release: tokio::sync::Notify,
}

impl SerialNonObservingExternalExecutor {
    async fn wait_for_started(&self, count: usize) {
        while self.started.load(Ordering::SeqCst) < count {
            self.started_notify.notified().await;
        }
    }

    fn release_first(&self) {
        self.first_released.store(true, Ordering::SeqCst);
        self.first_release.notify_waiters();
    }
}

#[async_trait]
impl ExternalCommandExecutor for SerialNonObservingExternalExecutor {
    async fn exec(
        &self,
        request: ExternalCommandRequest,
    ) -> CooldisProcessResult<ExternalCommandResult> {
        let order = self.started.fetch_add(1, Ordering::SeqCst);
        self.started_notify.notify_waiters();
        if order == 0 {
            while !self.first_released.load(Ordering::SeqCst) {
                self.first_release.notified().await;
            }
        }
        let label = request.label();
        Ok(ExternalCommandResult::new(VirtualCommandOutput {
            stdout: format!("{label}\n").repeat(4_000),
            stderr: String::new(),
            exit_code: 0,
            stdout_truncated: false,
            stderr_truncated: false,
        }))
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn abandoned_bash_invocation_keeps_the_harness_mutex_and_serializes_the_next_call() {
    let executor = Arc::new(SerialNonObservingExternalExecutor {
        started: AtomicUsize::new(0),
        started_notify: tokio::sync::Notify::new(),
        first_released: AtomicBool::new(false),
        first_release: tokio::sync::Notify::new(),
    });
    let provider = Arc::new(BashToolProvider::new(
        VirtualBashRuntimeConfig {
            max_output_bytes: 64,
            ..VirtualBashRuntimeConfig::default()
        }
        .with_execution_policy(BashExecutionPolicy::host_always())
        .with_external_executor(executor.clone()),
    ));
    let cancellation = CancellationToken::new();
    let first = tokio::spawn({
        let provider = Arc::clone(&provider);
        let cancellation = cancellation.clone();
        async move {
            provider
                .invoke_tool_call_cancellable(
                    AgentKernelToolCall {
                        call_id: "call-abandoned".to_string(),
                        tool_name: BASH_TOOL.to_string(),
                        arguments: serde_json::json!({"command": "first"}),
                        turn_context: None,
                    },
                    ToolInvocationCancellation::new(cancellation, Duration::from_millis(10)),
                )
                .await
        }
    });
    executor.wait_for_started(1).await;
    cancellation.cancel();
    tokio::time::advance(Duration::from_millis(10)).await;
    tokio::task::yield_now().await;

    let mut second = std::pin::pin!(provider.invoke_tool_call(AgentKernelToolCall {
        call_id: "call-abandoned".to_string(),
        tool_name: BASH_TOOL.to_string(),
        arguments: serde_json::json!({"command": "second"}),
        turn_context: None,
    }));
    assert!(matches!(
        futures_util::poll!(&mut second),
        std::task::Poll::Pending
    ));
    assert_eq!(
        executor.started.load(Ordering::SeqCst),
        1,
        "the next call must serialize behind the abandoned call's harness mutex"
    );

    executor.release_first();
    first.await.unwrap().unwrap();
    second.await.unwrap();
    assert_eq!(executor.started.load(Ordering::SeqCst), 2);
    let harness = provider.harness.lock().await;
    let stored = harness
        .as_ref()
        .unwrap()
        .read_file("/spill/call-abandoned.stdout.txt")
        .await
        .unwrap();
    assert_eq!(stored, b"first\n".repeat(4_000));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn harness_execute_process_marks_external_timeout_from_shared_deadline() {
    let mut config = VirtualBashRuntimeConfig::default()
        .with_execution_policy(BashExecutionPolicy::host_always())
        .with_external_executor(Arc::new(SlowDeadlineExecutor));
    config.execution_timeout = Duration::from_millis(10);
    let mut harness = BashkitExecutionHarness::new(config).await.unwrap();

    let process = harness.execute_process("sleep 60").await.unwrap();
    let output = process.output();

    assert_eq!(process.backend(), &CooldisProcessBackend::HostBash);
    assert_eq!(output.exit_code(), Some(124));
    assert!(matches!(
        output.terminal,
        Some(CooldisProcessTerminalState::TimedOut { .. })
    ));
}

#[tokio::test]
async fn async_manager_wraps_bashkit_backend_without_stdin_sink() {
    let manager = AsyncExecutionManager::default();
    let backend: Arc<dyn LiveProcessBackend> =
        Arc::new(BashkitLiveBackend::new(VirtualBashRuntimeConfig::default()));
    let request = AsyncProcessStartRequest::virtual_bash_script("sleep 0.05; echo done")
        .with_deadline(ExecutionDeadline::from_now(Duration::from_secs(1)))
        .with_yield_time(Duration::from_millis(5))
        .with_output_cap_bytes(1024);

    let started = manager.start(backend, request).await.unwrap();
    assert_eq!(started.snapshot.status, ProcessSnapshotStatus::Running);
    let process_id = started.snapshot.process_id.unwrap();

    let write = manager
        .write(
            process_id,
            b"hello\n".to_vec(),
            Duration::from_millis(10),
            1024,
        )
        .await
        .unwrap_err();
    assert!(write.to_string().contains("stdin"));

    let completed = manager
        .poll(process_id, Duration::from_secs(1), 1024)
        .await
        .unwrap();
    assert_eq!(completed.snapshot.status, ProcessSnapshotStatus::Completed);
    assert!(String::from_utf8_lossy(&completed.snapshot.stdout).contains("done"));
}

#[tokio::test]
async fn bash_tool_provider_exposes_process_handle_tools() {
    let provider = BashToolProvider::new(VirtualBashRuntimeConfig::default());

    let names = provider
        .tool_definitions()
        .await
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();

    assert!(names.contains(&BASH_TOOL.to_string()));
    assert!(names.contains(&PROCESS_EXEC_TOOL.to_string()));
    assert!(names.contains(&WRITE_STDIN_TOOL.to_string()));
}

#[tokio::test]
async fn bash_tool_inline_result_keeps_the_legacy_wire_bytes() {
    let provider = BashToolProvider::new(VirtualBashRuntimeConfig::default());

    let (text, is_error) =
        tool_result_text(invoke_bash(&provider, "call_inline", "echo exact").await);

    assert!(!is_error);
    assert_eq!(
        text,
        r#"{"exit_code":0,"stderr":"","stderr_truncated":false,"stdout":"exact\n","stdout_truncated":false}"#
    );
}

#[tokio::test]
async fn inline_stream_keeps_the_legacy_lossy_utf8_conversion() {
    let harness = BashkitExecutionHarness::new(VirtualBashRuntimeConfig::default())
        .await
        .unwrap();

    let (text, receipt, spilled) = present_output_stream(
        &harness,
        b"before\xffafter",
        1024,
        false,
        "/spill/lossy.stdout.txt",
    )
    .await;

    assert_eq!(text, "before\u{fffd}after");
    assert!(receipt.is_none());
    assert!(!spilled);
}

#[tokio::test]
async fn bash_tool_spills_complete_stdout_and_cat_round_trips_it() {
    let provider = BashToolProvider::new(VirtualBashRuntimeConfig {
        max_output_bytes: 64,
        ..VirtualBashRuntimeConfig::default()
    });
    let expected = b"x\n".repeat(10_000);

    let (result, is_error) =
        tool_result_json(invoke_bash(&provider, "call_round_trip", "yes x | head -c 20000").await);

    assert!(!is_error, "{result}");
    assert_eq!(result["stdout_truncated"], true);
    assert_eq!(result["stderr_truncated"], false);
    assert_eq!(
        result["spill"]["stdout"]["path"],
        "/spill/call_round_trip.stdout.txt"
    );
    assert_eq!(result["spill"]["stdout"]["total_bytes"], 20_000);
    assert_eq!(result["spill"]["stdout"]["preview_bytes"], 16_384);
    assert!(result["spill"].get("stderr").is_none());
    assert!(
        result["stdout"]
            .as_str()
            .unwrap()
            .contains("Tip: cat /spill/call_round_trip.stdout.txt")
    );

    let copied = invoke_bash(
        &provider,
        "call_copy_spill",
        "cat /spill/call_round_trip.stdout.txt > /workspace/retrieved.txt",
    )
    .await;
    let (_, copy_is_error) = tool_result_json(copied);
    assert!(!copy_is_error);
    let harness = provider.harness.lock().await;
    let retrieved = harness
        .as_ref()
        .unwrap()
        .read_file("/workspace/retrieved.txt")
        .await
        .unwrap();
    assert_eq!(retrieved, expected);
}

#[tokio::test]
async fn bash_tool_checks_manifest_grant_expiry_before_execution() {
    let provider = BashToolProvider::new(
        VirtualBashRuntimeConfig::default().with_capability_grant_expiries([
            crate::AgentManifestGrantExpiry {
                capability: "fs.read:/workspace".to_string(),
                expires_at: "1970-01-01T00:00:01Z".to_string(),
            },
        ]),
    );

    let err = provider
        .invoke_tool_call_at(
            AgentKernelToolCall {
                call_id: "call_expired".to_string(),
                tool_name: BASH_TOOL.to_string(),
                arguments: serde_json::json!({"command": "echo should-not-run"}),
                turn_context: None,
            },
            1_001,
        )
        .await
        .unwrap_err();

    assert!(
        err.to_string()
            .contains("missing capability grants: fs.read:/workspace")
    );
    assert!(err.to_string().contains("1970-01-01T00:00:01Z"));
}

#[tokio::test]
async fn bash_tool_spills_stderr_independently() {
    let provider = BashToolProvider::new(VirtualBashRuntimeConfig {
        max_output_bytes: 64,
        ..VirtualBashRuntimeConfig::default()
    });

    let (result, is_error) = tool_result_json(
        invoke_bash(
            &provider,
            "call_stderr",
            "printf ok; yes e | head -c 20000 >&2",
        )
        .await,
    );

    assert!(!is_error, "{result}");
    assert_eq!(result["stdout"], "ok");
    assert!(result["spill"].get("stdout").is_none());
    assert_eq!(
        result["spill"]["stderr"]["path"],
        "/spill/call_stderr.stderr.txt"
    );
    assert_eq!(result["spill"]["stderr"]["total_bytes"], 20_000);
}

#[tokio::test]
async fn bash_tool_spill_failure_returns_emergency_stub_without_failing_call() {
    let root: Arc<dyn CooldisVfsBackend> = Arc::new(ReadOnlyFileSystem::new(Arc::new(
        bashkit::InMemoryFs::new(),
    )));
    let workspace_vfs = Arc::new(CooldisVfs::new(root));
    let provider = BashToolProvider::new(VirtualBashRuntimeConfig {
        max_output_bytes: 64,
        workspace_vfs: Some(workspace_vfs),
        ..VirtualBashRuntimeConfig::default()
    });

    let (result, is_error) = tool_result_json(
        invoke_bash(&provider, "call_spill_failure", "yes f | head -c 20000").await,
    );

    assert!(!is_error, "{result}");
    assert_eq!(result["stdout_truncated"], true);
    assert!(result.get("spill").is_none());
    let stdout = result["stdout"].as_str().unwrap();
    assert!(stdout.contains("[CONTENT_OVERFLOW - spill path unavailable]"));
    assert!(stdout.contains("Length: 20000 bytes"));
    assert!(stdout.contains("Head bytes: 500"));
    assert!(stdout.contains("Tail bytes: 500"));
}

#[tokio::test]
async fn concurrent_bash_spills_are_isolated_by_call_id() {
    let provider = Arc::new(BashToolProvider::new(VirtualBashRuntimeConfig {
        max_output_bytes: 64,
        ..VirtualBashRuntimeConfig::default()
    }));
    let left = Arc::clone(&provider);
    let right = Arc::clone(&provider);

    let (left, right) = tokio::join!(
        async move { invoke_bash(&left, "call_left", "yes l | head -c 20000").await },
        async move { invoke_bash(&right, "call_right", "yes r | head -c 20000").await },
    );
    let (left, _) = tool_result_json(left);
    let (right, _) = tool_result_json(right);

    assert_eq!(
        left["spill"]["stdout"]["path"],
        "/spill/call_left.stdout.txt"
    );
    assert_eq!(
        right["spill"]["stdout"]["path"],
        "/spill/call_right.stdout.txt"
    );
    let harness = provider.harness.lock().await;
    let harness = harness.as_ref().unwrap();
    assert_eq!(
        harness
            .read_file("/spill/call_left.stdout.txt")
            .await
            .unwrap(),
        b"l\n".repeat(10_000)
    );
    assert_eq!(
        harness
            .read_file("/spill/call_right.stdout.txt")
            .await
            .unwrap(),
        b"r\n".repeat(10_000)
    );
}

#[tokio::test]
async fn repeated_call_id_does_not_overwrite_an_earlier_spill() {
    let provider = BashToolProvider::new(VirtualBashRuntimeConfig {
        max_output_bytes: 64,
        ..VirtualBashRuntimeConfig::default()
    });
    let first = b"a\n".repeat(10_000);

    let _ = invoke_bash(&provider, "call_repeat", "yes a | head -c 20000").await;
    let (second, is_error) =
        tool_result_json(invoke_bash(&provider, "call_repeat", "yes b | head -c 20000").await);

    assert!(!is_error, "{second}");
    assert!(second.get("spill").is_none());
    assert!(
        second["stdout"]
            .as_str()
            .unwrap()
            .contains("spill path unavailable")
    );
    let harness = provider.harness.lock().await;
    assert_eq!(
        harness
            .as_ref()
            .unwrap()
            .read_file("/spill/call_repeat.stdout.txt")
            .await
            .unwrap(),
        first
    );
}

#[tokio::test]
async fn concurrent_same_call_id_spills_keep_the_first_complete_stream() {
    let provider = Arc::new(BashToolProvider::new(VirtualBashRuntimeConfig {
        max_output_bytes: 64,
        ..VirtualBashRuntimeConfig::default()
    }));
    let left = Arc::clone(&provider);
    let right = Arc::clone(&provider);

    let (left, right) = tokio::join!(
        async move { invoke_bash(&left, "call_same", "yes a | head -c 20000").await },
        async move { invoke_bash(&right, "call_same", "yes b | head -c 20000").await },
    );
    let (left, _) = tool_result_json(left);
    let (right, _) = tool_result_json(right);

    assert_eq!(
        usize::from(left.get("spill").is_some()) + usize::from(right.get("spill").is_some()),
        1
    );
    assert_eq!(
        usize::from(
            left["stdout"]
                .as_str()
                .unwrap()
                .contains("spill path unavailable")
        ) + usize::from(
            right["stdout"]
                .as_str()
                .unwrap()
                .contains("spill path unavailable")
        ),
        1
    );
    let harness = provider.harness.lock().await;
    let stored = harness
        .as_ref()
        .unwrap()
        .read_file("/spill/call_same.stdout.txt")
        .await
        .unwrap();
    assert!(stored == b"a\n".repeat(10_000) || stored == b"b\n".repeat(10_000));
}

#[test]
fn spill_paths_are_single_component_collision_safe_and_bounded() {
    let hostile = spill_path("../call/_2f/💥", "stdout");
    assert!(hostile.starts_with("/spill/"));
    assert_eq!(hostile.matches('/').count(), 2);
    assert!(!hostile.contains(".."));
    assert_ne!(spill_path("/", "stdout"), spill_path("_2f", "stdout"));

    let overlong = spill_path(&"x".repeat(10_000), "stderr");
    assert!(
        overlong.len() <= 240,
        "overlong spill path: {}",
        overlong.len()
    );
    assert_eq!(overlong.matches('/').count(), 2);
}

struct RetentionCeilingExternalExecutor {
    requested_cap: AtomicUsize,
}

#[async_trait]
impl ExternalCommandExecutor for RetentionCeilingExternalExecutor {
    async fn exec(
        &self,
        request: ExternalCommandRequest,
    ) -> CooldisProcessResult<ExternalCommandResult> {
        self.requested_cap
            .store(request.max_output_bytes, Ordering::SeqCst);
        Ok(ExternalCommandResult::new(VirtualCommandOutput {
            stdout: "x".repeat(request.max_output_bytes),
            stderr: String::new(),
            exit_code: 0,
            stdout_truncated: true,
            stderr_truncated: false,
        }))
    }
}

#[tokio::test]
async fn retention_ceiling_truncation_still_spills_and_succeeds() {
    let executor = Arc::new(RetentionCeilingExternalExecutor {
        requested_cap: AtomicUsize::new(0),
    });
    let provider = BashToolProvider::new(
        VirtualBashRuntimeConfig {
            max_output_bytes: usize::MAX,
            ..VirtualBashRuntimeConfig::default()
        }
        .with_execution_policy(BashExecutionPolicy::host_always())
        .with_external_executor(executor.clone()),
    );

    let (result, is_error) =
        tool_result_json(invoke_bash(&provider, "call_retention_ceiling", "runaway-output").await);

    assert!(!is_error, "{result}");
    assert_eq!(
        executor.requested_cap.load(Ordering::SeqCst),
        SPILL_RETENTION_MAX_BYTES
    );
    assert_eq!(result["stdout_truncated"], true);
    assert_eq!(
        result["spill"]["stdout"]["total_bytes"],
        SPILL_RETENTION_MAX_BYTES
    );
    assert_eq!(result["spill"]["stdout"]["retention_truncated"], true);
    assert!(
        result["stdout"]
            .as_str()
            .unwrap()
            .contains("exceeded the 67108864-byte retention ceiling")
    );
    let harness = provider.harness.lock().await;
    assert_eq!(
        harness
            .as_ref()
            .unwrap()
            .read_file("/spill/call_retention_ceiling.stdout.txt")
            .await
            .unwrap()
            .len(),
        SPILL_RETENTION_MAX_BYTES
    );
}

#[test]
fn spill_payload_decodes_in_old_and_new_reader_shapes() {
    let old = r#"{"stdout":"ok","stderr":"","exit_code":0,"stdout_truncated":false,"stderr_truncated":false}"#;
    let decoded: BashToolResultPayload = serde_json::from_str(old).unwrap();
    assert!(decoded.spill.is_empty());

    let new = r#"{"stdout":"preview","stderr":"","exit_code":0,"stdout_truncated":true,"stderr_truncated":false,"spill":{"stdout":{"path":"/spill/c.stdout.txt","total_bytes":20000,"preview_bytes":16384}}}"#;
    let decoded: BashToolResultPayload = serde_json::from_str(new).unwrap();
    let receipt = decoded.spill.stdout.unwrap();
    assert_eq!(receipt.path, "/spill/c.stdout.txt");
    assert!(!receipt.retention_truncated);

    let retention_truncated = r#"{"stdout":"preview","stderr":"","exit_code":0,"stdout_truncated":true,"stderr_truncated":false,"spill":{"stdout":{"path":"/spill/c.stdout.txt","total_bytes":67108864,"preview_bytes":16384,"retention_truncated":true}}}"#;
    let decoded: BashToolResultPayload = serde_json::from_str(retention_truncated).unwrap();
    assert!(decoded.spill.stdout.unwrap().retention_truncated);

    #[derive(Deserialize)]
    struct LegacyBashToolResult {
        stdout: String,
        stderr: String,
        exit_code: i32,
        stdout_truncated: bool,
        stderr_truncated: bool,
    }
    let legacy: LegacyBashToolResult = serde_json::from_str(new).unwrap();
    assert_eq!(legacy.stdout, "preview");
    assert_eq!(legacy.stderr, "");
    assert_eq!(legacy.exit_code, 0);
    assert!(legacy.stdout_truncated);
    assert!(!legacy.stderr_truncated);

    let legacy: LegacyBashToolResult = serde_json::from_str(retention_truncated).unwrap();
    assert_eq!(legacy.stdout, "preview");
}

#[tokio::test]
async fn process_exec_tool_starts_and_polls_virtual_bash_handle() {
    let (provider, turn_context) = process_test_provider();

    let started = provider
        .invoke_tool_call(AgentKernelToolCall {
            call_id: "call_process_start".to_string(),
            tool_name: PROCESS_EXEC_TOOL.to_string(),
            arguments: serde_json::json!({
                "command": "sleep 0.05; echo done",
                "yield_time_ms": 1,
                "timeout_ms": 1000,
                "output_bytes_cap": 1024
            }),
            turn_context: Some(turn_context),
        })
        .await
        .unwrap()
        .unwrap();
    let (started, is_error) = tool_result_json(started);
    assert!(!is_error, "{started}");
    assert_eq!(started["status"].as_str(), Some("running"));
    let process_id = started["process_id"].as_str().unwrap().to_string();

    let completed = provider
        .invoke_tool_call(AgentKernelToolCall {
            call_id: "call_process_poll".to_string(),
            tool_name: PROCESS_EXEC_TOOL.to_string(),
            arguments: serde_json::json!({
                "process_id": process_id.clone(),
                "yield_time_ms": 1000,
                "output_bytes_cap": 1024
            }),
            turn_context: None,
        })
        .await
        .unwrap()
        .unwrap();
    let (completed, is_error) = tool_result_json(completed);
    assert!(!is_error, "{completed}");
    assert_eq!(completed["status"].as_str(), Some("completed"));
    assert!(completed["stdout"].as_str().unwrap().contains("done"));
    assert_eq!(completed["process_id"].as_str(), Some(process_id.as_str()));
}

#[cfg(unix)]
#[tokio::test]
async fn cancelling_a_process_poll_terminates_the_live_handle_and_returns_its_snapshot() {
    let store: Arc<dyn crate::RuntimeStore> = Arc::new(crate::InMemorySessionStore::new());
    let coordinates = ThreadCoordinates::new("tenant", "user", "process-cancel");
    let dispatcher = crate::kernel::process_handle_dispatch::test_process_dispatcher(
        Arc::clone(&store),
        coordinates.clone(),
    );
    let turn_context = crate::TurnContext::new(
        crate::ThreadContext::root(coordinates),
        "process-cancel-turn",
        &crate::TurnInput::text("process cancellation test"),
        CancellationToken::new(),
    )
    .snapshot();
    let provider = Arc::new(
        BashToolProvider::new(
            VirtualBashRuntimeConfig::default()
                .with_execution_policy(BashExecutionPolicy::host_always())
                .with_host_bash_executor("/"),
        )
        .with_process_dispatcher(dispatcher),
    );
    let started = provider
        .invoke_tool_call(AgentKernelToolCall {
            call_id: "call_process_start_for_cancel".to_string(),
            tool_name: PROCESS_EXEC_TOOL.to_string(),
            arguments: serde_json::json!({
                "command": "sleep 60",
                "yield_time_ms": 1,
                "timeout_ms": 60_000,
                "output_bytes_cap": 1024
            }),
            turn_context: Some(turn_context),
        })
        .await
        .unwrap()
        .unwrap();
    let (started, _) = tool_result_json(started);
    let process_id = started["process_id"].as_str().unwrap().to_string();
    let cancellation = CancellationToken::new();
    let poll = tokio::spawn({
        let provider = Arc::clone(&provider);
        let process_id = process_id.clone();
        let cancellation = cancellation.clone();
        async move {
            provider
                .invoke_tool_call_cancellable(
                    AgentKernelToolCall {
                        call_id: "call_process_cancel_poll".to_string(),
                        tool_name: PROCESS_EXEC_TOOL.to_string(),
                        arguments: serde_json::json!({
                            "process_id": process_id,
                            "yield_time_ms": 10_000,
                            "output_bytes_cap": 1024
                        }),
                        turn_context: None,
                    },
                    ToolInvocationCancellation::new(cancellation, Duration::from_secs(1)),
                )
                .await
        }
    });
    tokio::task::yield_now().await;
    cancellation.cancel();

    let AgentKernelToolOutcome::Completed(Some(cancelled)) =
        tokio::time::timeout(Duration::from_secs(2), poll)
            .await
            .expect("cancelled process poll did not return promptly")
            .unwrap()
            .unwrap()
    else {
        panic!("process poll did not return a completed tool result");
    };
    let (cancelled, is_error) = tool_result_json(cancelled);
    assert!(is_error, "{cancelled}");
    assert_eq!(cancelled["status"], "cancelled");
    assert_eq!(cancelled["process_id"], process_id);
}

#[tokio::test]
async fn process_exec_tool_spills_a_completed_initial_snapshot() {
    let (provider, turn_context) = process_test_provider();

    let completed = provider
        .invoke_tool_call(AgentKernelToolCall {
            call_id: "call_process_exec_spill".to_string(),
            tool_name: PROCESS_EXEC_TOOL.to_string(),
            arguments: serde_json::json!({
                "command": "yes q | head -c 20000; yes e | head -c 20000 >&2",
                "yield_time_ms": 1000,
                "timeout_ms": 1000,
                "output_bytes_cap": 64
            }),
            turn_context: Some(turn_context),
        })
        .await
        .unwrap()
        .unwrap();
    let (completed, is_error) = tool_result_json(completed);

    assert!(!is_error, "{completed}");
    assert_eq!(completed["status"], "completed");
    assert_eq!(completed["stdout_truncated"], true);
    assert_eq!(
        completed["spill"]["stdout"]["path"],
        "/spill/call_process_exec_spill.stdout.txt"
    );
    assert_eq!(completed["spill"]["stdout"]["total_bytes"], 20_000);
    assert_eq!(
        completed["spill"]["stderr"]["path"],
        "/spill/call_process_exec_spill.stderr.txt"
    );
    assert_eq!(completed["spill"]["stderr"]["total_bytes"], 20_000);
    assert!(
        completed["stdout"]
            .as_str()
            .unwrap()
            .contains("Tip: cat /spill/call_process_exec_spill.stdout.txt")
    );
}

#[tokio::test]
async fn process_poll_tool_spills_the_complete_later_snapshot() {
    let (provider, turn_context) = process_test_provider();
    let started = provider
        .invoke_tool_call(AgentKernelToolCall {
            call_id: "call_process_start_for_spill".to_string(),
            tool_name: PROCESS_EXEC_TOOL.to_string(),
            arguments: serde_json::json!({
                "command": "sleep 0.05; yes p | head -c 20000",
                "yield_time_ms": 1,
                "timeout_ms": 1000,
                "output_bytes_cap": 64
            }),
            turn_context: Some(turn_context),
        })
        .await
        .unwrap()
        .unwrap();
    let (started, is_error) = tool_result_json(started);
    assert!(!is_error, "{started}");
    assert_eq!(started["status"], "running");
    let process_id = started["process_id"].as_str().unwrap().to_string();

    let completed = provider
        .invoke_tool_call(AgentKernelToolCall {
            call_id: "call_process_poll_spill".to_string(),
            tool_name: PROCESS_EXEC_TOOL.to_string(),
            arguments: serde_json::json!({
                "process_id": process_id,
                "yield_time_ms": 1000,
                "output_bytes_cap": 64
            }),
            turn_context: None,
        })
        .await
        .unwrap()
        .unwrap();
    let (completed, is_error) = tool_result_json(completed);

    assert!(!is_error, "{completed}");
    assert_eq!(completed["status"], "completed");
    assert_eq!(
        completed["spill"]["stdout"]["path"],
        "/spill/call_process_poll_spill.stdout.txt"
    );
    assert_eq!(completed["spill"]["stdout"]["total_bytes"], 20_000);
    let harness = provider.harness.lock().await;
    assert_eq!(
        harness
            .as_ref()
            .unwrap()
            .read_file("/spill/call_process_poll_spill.stdout.txt")
            .await
            .unwrap(),
        b"p\n".repeat(10_000)
    );
}

#[tokio::test]
async fn write_stdin_tool_reports_unsupported_for_virtual_bash_handle() {
    let (provider, turn_context) = process_test_provider();
    let started = provider
        .invoke_tool_call(AgentKernelToolCall {
            call_id: "call_process_start".to_string(),
            tool_name: PROCESS_EXEC_TOOL.to_string(),
            arguments: serde_json::json!({
                "command": "sleep 0.05; echo done",
                "yield_time_ms": 1,
                "timeout_ms": 1000,
                "output_bytes_cap": 1024
            }),
            turn_context: Some(turn_context),
        })
        .await
        .unwrap()
        .unwrap();
    let (started, _) = tool_result_json(started);
    let process_id = started["process_id"].as_str().unwrap().to_string();

    let unsupported = provider
        .invoke_tool_call(AgentKernelToolCall {
            call_id: "call_stdin".to_string(),
            tool_name: WRITE_STDIN_TOOL.to_string(),
            arguments: serde_json::json!({
                "process_id": process_id,
                "delta_base64": "aGkK",
                "yield_time_ms": 1,
                "output_bytes_cap": 1024
            }),
            turn_context: None,
        })
        .await
        .unwrap()
        .unwrap();
    let (unsupported, is_error) = tool_result_json(unsupported);
    assert!(is_error, "{unsupported}");
    assert_eq!(unsupported["status"].as_str(), Some("unsupported"));
    assert!(unsupported["error"].as_str().unwrap().contains("stdin"));
}

struct StdinOutputBackend {
    requested_cap: Arc<AtomicUsize>,
}

#[async_trait]
impl LiveProcessBackend for StdinOutputBackend {
    fn backend_kind(&self) -> CooldisProcessBackend {
        CooldisProcessBackend::VirtualBash
    }

    async fn start(
        &self,
        request: crate::LiveProcessStartRequest,
        process: CooldisProcessHandle,
        cancellation: CancellationToken,
    ) -> CooldisProcessResult<LiveProcessSpawn> {
        self.requested_cap
            .store(request.output_cap_bytes, Ordering::SeqCst);
        process.record(CooldisProcessEventKind::Started {
            command: Some("stdin-output-test".to_string()),
        });
        let (stdin, mut input) = tokio::sync::mpsc::channel::<Vec<u8>>(1);
        let join = tokio::spawn(async move {
            tokio::select! {
                delta = input.recv() => {
                    if delta.is_some() {
                        process.record(CooldisProcessEventKind::Stdout {
                            bytes: vec![b'w'; 20_000],
                        });
                        process.record(CooldisProcessEventKind::Completed {
                            status: crate::CooldisProcessExitStatus::exited(0),
                        });
                    }
                }
                _ = cancellation.cancelled() => {
                    process.record(CooldisProcessEventKind::Cancelled {
                        reason: "cancelled".to_string(),
                    });
                }
            }
            Ok(())
        });
        Ok(LiveProcessSpawn {
            stdin: Some(stdin),
            join,
        })
    }
}

#[tokio::test]
async fn write_stdin_snapshot_uses_the_shared_spill_and_retention_bounds() {
    let (mut provider, turn_context) = process_test_provider();
    let requested_cap = Arc::new(AtomicUsize::new(0));
    provider.live_backend = Arc::new(StdinOutputBackend {
        requested_cap: Arc::clone(&requested_cap),
    });

    let started = provider
        .invoke_tool_call(AgentKernelToolCall {
            call_id: "call_stdin_start".to_string(),
            tool_name: PROCESS_EXEC_TOOL.to_string(),
            arguments: serde_json::json!({
                "command": "wait-for-stdin",
                "yield_time_ms": 1,
                "timeout_ms": 1000,
                "output_bytes_cap": 64
            }),
            turn_context: Some(turn_context),
        })
        .await
        .unwrap()
        .unwrap();
    let (started, is_error) = tool_result_json(started);
    assert!(!is_error, "{started}");
    let process_id = started["process_id"].as_str().unwrap();

    let written = provider
        .invoke_tool_call(AgentKernelToolCall {
            call_id: "call_stdin_spill".to_string(),
            tool_name: WRITE_STDIN_TOOL.to_string(),
            arguments: serde_json::json!({
                "process_id": process_id,
                "delta_base64": "aGkK",
                "yield_time_ms": 1000,
                "output_bytes_cap": 64
            }),
            turn_context: None,
        })
        .await
        .unwrap()
        .unwrap();
    let (written, is_error) = tool_result_json(written);

    assert!(!is_error, "{written}");
    assert_eq!(
        requested_cap.load(Ordering::SeqCst),
        SPILL_RETENTION_MAX_BYTES
    );
    assert_eq!(written["status"], "completed");
    assert_eq!(
        written["spill"]["stdout"]["path"],
        "/spill/call_stdin_spill.stdout.txt"
    );
    assert_eq!(written["spill"]["stdout"]["total_bytes"], 20_000);
    assert!(
        written["spill"]["stdout"]
            .get("retention_truncated")
            .is_none()
    );
}

struct EscapingExternalExecutor;

#[async_trait]
impl ExternalCommandExecutor for EscapingExternalExecutor {
    async fn exec(
        &self,
        _request: ExternalCommandRequest,
    ) -> CooldisProcessResult<ExternalCommandResult> {
        Ok(ExternalCommandResult::new(VirtualCommandOutput {
            stdout: String::new(),
            stderr: String::new(),
            exit_code: 0,
            stdout_truncated: false,
            stderr_truncated: false,
        })
        .with_file_write(ExternalFileWrite::new("/workspace/../escape.txt", "bad\n")))
    }
}

#[tokio::test]
async fn harness_rejects_external_file_writes_outside_normalized_vfs_paths() {
    let mut harness = BashkitExecutionHarness::new(
        VirtualBashRuntimeConfig::default()
            .with_execution_policy(BashExecutionPolicy::host_always())
            .with_external_executor(Arc::new(EscapingExternalExecutor)),
    )
    .await
    .unwrap();

    let err = harness.execute("cargo test --lib").await.unwrap_err();

    assert!(err.to_string().contains("must be normalized"));
}

#[tokio::test]
async fn harness_uses_configured_mounts_instead_of_hardcoded_mounts() {
    let config = VirtualBashRuntimeConfig {
        cwd: PathBuf::from("/work"),
        mounts: vec![
            VirtualMount::writable("/work").with_file("seed.txt", "seed\n"),
            VirtualMount::readonly("/docs", vec![VirtualFile::new("guide.txt", "read me\n")]),
        ],
        ..VirtualBashRuntimeConfig::default()
    };
    let mut harness = BashkitExecutionHarness::new(config).await.unwrap();

    let output = harness
        .execute(
            "cat /work/seed.txt && echo changed > /work/new.txt \
                 && cat /docs/guide.txt && test ! -e /workspace",
        )
        .await
        .unwrap();
    assert!(output.success(), "{output:?}");
    assert!(output.stdout.contains("seed"));
    assert!(output.stdout.contains("read me"));
    assert_eq!(
        String::from_utf8(harness.read_file("/work/new.txt").await.unwrap()).unwrap(),
        "changed\n"
    );

    let output = harness
        .execute("echo nope > /docs/guide.txt")
        .await
        .unwrap();
    assert_ne!(output.exit_code, 0);
    assert!(output.stderr.contains("read-only") || output.stderr.contains("denied"));
}

#[tokio::test]
async fn object_store_mount_has_read_your_writes_and_persists_final_state() {
    let store = Arc::new(InMemoryObjectStore::new()) as Arc<dyn ObjectStore>;
    let prefix = "tenant-a/session-a";
    let config = VirtualBashRuntimeConfig {
        cwd: PathBuf::from("/s3"),
        mounts: vec![VirtualMount::object_store(
            "/s3",
            ObjectStoreMountConfig::shared(store.clone(), prefix),
        )],
        ..VirtualBashRuntimeConfig::default()
    };
    let mut harness = BashkitExecutionHarness::new(config).await.unwrap();

    let output = harness
        .execute(
            "mkdir -p docs \
                 && echo alpha > docs/a.txt \
                 && echo beta >> docs/a.txt \
                 && cat docs/a.txt \
                 && cp docs/a.txt docs/b.txt \
                 && mv docs/b.txt docs/c.txt \
                 && rm docs/c.txt \
                 && test ! -e docs/c.txt \
                 && ls docs",
        )
        .await
        .unwrap();
    assert!(output.success(), "{output:?}");
    assert!(output.stdout.contains("alpha"));
    assert!(output.stdout.contains("beta"));
    assert!(output.stdout.contains("a.txt"));
    assert!(!output.stdout.contains("c.txt"));
    assert!(harness.mutations().iter().any(|mutation| {
        mutation.kind == VfsMutationKind::Write && mutation.path == Path::new("/s3/docs/a.txt")
    }));

    let stored = store
        .get(&ObjectPath::from("tenant-a/session-a/docs/a.txt"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(stored.as_ref(), b"alpha\nbeta\n");
    assert!(matches!(
        store
            .head(&ObjectPath::from("tenant-a/session-a/docs/c.txt"))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));

    let reload_config = VirtualBashRuntimeConfig {
        cwd: PathBuf::from("/s3"),
        mounts: vec![VirtualMount::object_store(
            "/s3",
            ObjectStoreMountConfig::shared(store.clone(), prefix),
        )],
        ..VirtualBashRuntimeConfig::default()
    };
    let mut reloaded = BashkitExecutionHarness::new(reload_config).await.unwrap();
    let output = reloaded
        .execute("cat docs/a.txt && test ! -e docs/c.txt")
        .await
        .unwrap();
    assert!(output.success(), "{output:?}");
    assert_eq!(output.stdout, "alpha\nbeta\n");
}

#[tokio::test]
async fn object_store_mounts_are_prefix_isolated_and_can_be_readonly() {
    let store = Arc::new(InMemoryObjectStore::new()) as Arc<dyn ObjectStore>;
    store
        .put(
            &ObjectPath::from("readonly/guide.txt"),
            Vec::from("read only\n").into(),
        )
        .await
        .unwrap();

    let config = VirtualBashRuntimeConfig {
        cwd: PathBuf::from("/a"),
        mounts: vec![
            VirtualMount::object_store(
                "/a",
                ObjectStoreMountConfig::shared(store.clone(), "tenant-a"),
            ),
            VirtualMount::object_store(
                "/b",
                ObjectStoreMountConfig::shared(store.clone(), "tenant-b"),
            ),
            VirtualMount::readonly_object_store(
                "/docs",
                ObjectStoreMountConfig::shared(store.clone(), "readonly"),
            ),
        ],
        ..VirtualBashRuntimeConfig::default()
    };
    let mut harness = BashkitExecutionHarness::new(config).await.unwrap();

    let output = harness
        .execute("echo alpha > /a/file.txt && test ! -e /b/file.txt && cat /docs/guide.txt")
        .await
        .unwrap();
    assert!(output.success(), "{output:?}");
    assert!(output.stdout.contains("read only"));
    assert!(
        store
            .head(&ObjectPath::from("tenant-a/file.txt"))
            .await
            .is_ok()
    );
    assert!(matches!(
        store.head(&ObjectPath::from("tenant-b/file.txt")).await,
        Err(object_store::Error::NotFound { .. })
    ));

    let output = harness.execute("echo nope > /docs/new.txt").await.unwrap();
    assert_ne!(output.exit_code, 0);
    assert!(output.stderr.contains("read-only") || output.stderr.contains("denied"));
    assert!(matches!(
        store.head(&ObjectPath::from("readonly/new.txt")).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn harness_rejects_bad_mount_config() {
    let duplicate = VirtualBashRuntimeConfig {
        mounts: vec![
            VirtualMount::writable("/work"),
            VirtualMount::readonly("/work", Vec::new()),
        ],
        ..VirtualBashRuntimeConfig::default()
    };
    let err = match BashkitExecutionHarness::new(duplicate).await {
        Ok(_) => panic!("duplicate mount config should fail"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("duplicate virtual mount path"));

    let relative = VirtualBashRuntimeConfig {
        mounts: vec![VirtualMount::writable("relative")],
        ..VirtualBashRuntimeConfig::default()
    };
    let err = match BashkitExecutionHarness::new(relative).await {
        Ok(_) => panic!("relative mount config should fail"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("must be absolute"));
}

#[tokio::test]
async fn runtime_isolates_virtual_filesystems_by_thread() {
    let host = RuntimeHost::new(Arc::new(VirtualBashRuntimeFactory::default()));
    let first = host
        .start_thread(
            ThreadCoordinates::new("tenant-a", "user", "session"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let second = host
        .start_thread(
            ThreadCoordinates::new("tenant-b", "user", "session"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut first_events = first.subscribe_events();
    let mut second_events = second.subscribe_events();

    first
        .send(ThreadCommand::Submit {
            turn_id: "one".to_string(),
            input: crate::TurnInput::text("echo tenant-a > marker && cat marker"),
            mode: TurnSubmissionMode::Queue,
        })
        .await
        .unwrap();
    second
        .send(ThreadCommand::Submit {
            turn_id: "two".to_string(),
            input: crate::TurnInput::text("test ! -e marker && echo isolated"),
            mode: TurnSubmissionMode::Queue,
        })
        .await
        .unwrap();

    assert!(expect_output(&mut first_events).await.contains("tenant-a"));
    assert!(expect_output(&mut second_events).await.contains("isolated"));
}

#[tokio::test]
async fn runtime_cancels_busy_virtual_bash_without_poisoning_thread() {
    let config = VirtualBashRuntimeConfig {
        execution_timeout: Duration::from_secs(30),
        max_output_bytes: 1024,
        ..VirtualBashRuntimeConfig::default()
    };
    let host = RuntimeHost::new(Arc::new(VirtualBashRuntimeFactory::new(config)));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant", "user", "session"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    thread
        .send(ThreadCommand::Submit {
            turn_id: "loop".to_string(),
            input: crate::TurnInput::text("while true; do :; done"),
            mode: TurnSubmissionMode::Queue,
        })
        .await
        .unwrap();
    thread.cancel("stop loop").await.unwrap();

    let cancelled = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let ThreadEvent::Cancelled { reason, .. } = events.recv().await.unwrap() {
                break reason;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(cancelled, "stop loop");

    thread
        .send(ThreadCommand::Submit {
            turn_id: "after".to_string(),
            input: crate::TurnInput::text("echo ok > after.txt && cat after.txt"),
            mode: TurnSubmissionMode::Queue,
        })
        .await
        .unwrap();
    assert!(expect_output(&mut events).await.contains("ok"));
}

#[tokio::test]
async fn runtime_cancels_busy_virtual_bash_with_operation_registry_and_recovers() {
    let config = VirtualBashRuntimeConfig {
        execution_timeout: Duration::from_secs(30),
        max_output_bytes: 1024,
        ..VirtualBashRuntimeConfig::default()
            .with_operation_registry(echo_operation_registry().await)
    };
    let host = RuntimeHost::new(Arc::new(VirtualBashRuntimeFactory::new(config)));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant", "user", "session"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    thread
        .send(ThreadCommand::Submit {
            turn_id: "loop-after-op".to_string(),
            input: crate::TurnInput::text(
                "echo before | cooldis run echoer echo && while true; do :; done",
            ),
            mode: TurnSubmissionMode::Queue,
        })
        .await
        .unwrap();
    thread.cancel("stop projected process").await.unwrap();

    let cancelled = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let ThreadEvent::Cancelled { reason, .. } = events.recv().await.unwrap() {
                break reason;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(cancelled, "stop projected process");

    thread
        .send(ThreadCommand::Submit {
            turn_id: "after-projected-cancel".to_string(),
            input: crate::TurnInput::text("echo after | cooldis run echoer echo"),
            mode: TurnSubmissionMode::Queue,
        })
        .await
        .unwrap();
    assert!(expect_output(&mut events).await.contains("op:after"));
}

#[tokio::test]
async fn runtime_does_not_project_agent_process_as_virtual_bash_processes() {
    let config = VirtualBashRuntimeConfig {
        execution_timeout: Duration::from_secs(5),
        max_output_bytes: 4096,
        ..VirtualBashRuntimeConfig::default()
    };
    let host = RuntimeHost::new(Arc::new(VirtualBashRuntimeFactory::new(config)));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant", "user", "session"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "agent-process",
        r#"
agent ps 2>&1 || echo agent-missing
checkpoint create --label parent-save 2>&1 || echo checkpoint-missing
"#,
    )
    .await
    .unwrap();

    let output = expect_output(&mut events).await;
    assert!(output.contains("agent-missing"), "output:\n{output}");
    assert!(output.contains("checkpoint-missing"), "output:\n{output}");
    assert!(!output.contains("cooldis.wait_thread"), "output:\n{output}");
    assert!(
        !output.contains("cooldis.create_checkpoint"),
        "output:\n{output}"
    );

    host.shutdown_all().await.unwrap();
}
