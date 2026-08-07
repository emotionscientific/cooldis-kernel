mod apply_patch;
mod harness;

pub use apply_patch::apply_patch_to_bashkit;
pub use harness::{
    BashkitExecutionConfig, BashkitExecutionHarness, BashkitLiveBackend, VbashOperationRegistry,
    operation_shell_command_names,
};

pub type VerletVirtualBashResult<T> = Result<T, VerletVirtualBashError>;

#[derive(Debug, thiserror::Error)]
pub enum VerletVirtualBashError {
    #[error("virtual bash factory failed: {0}")]
    RuntimeFactory(String),
    #[error("virtual bash execution failed: {0}")]
    RuntimeExecution(String),
}

pub const BASH_TOOL: &str = "bash";

#[derive(Clone, Debug)]
pub struct VirtualMount {
    pub path: std::path::PathBuf,
    pub mode: VirtualMountMode,
    pub backend: VirtualMountBackend,
    pub files: Vec<VirtualFile>,
}

impl VirtualMount {
    pub fn writable(path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            path: path.into(),
            mode: VirtualMountMode::ReadWrite,
            backend: VirtualMountBackend::Memory,
            files: Vec::new(),
        }
    }

    pub fn readonly(path: impl Into<std::path::PathBuf>, files: Vec<VirtualFile>) -> Self {
        Self {
            path: path.into(),
            mode: VirtualMountMode::ReadOnly,
            backend: VirtualMountBackend::Memory,
            files,
        }
    }

    pub fn object_store(
        path: impl Into<std::path::PathBuf>,
        config: verlet_vfs::ObjectStoreMountConfig,
    ) -> Self {
        Self {
            path: path.into(),
            mode: VirtualMountMode::ReadWrite,
            backend: VirtualMountBackend::ObjectStore(config),
            files: Vec::new(),
        }
    }

    pub fn readonly_object_store(
        path: impl Into<std::path::PathBuf>,
        config: verlet_vfs::ObjectStoreMountConfig,
    ) -> Self {
        Self {
            path: path.into(),
            mode: VirtualMountMode::ReadOnly,
            backend: VirtualMountBackend::ObjectStore(config),
            files: Vec::new(),
        }
    }

    pub fn with_file(
        mut self,
        path: impl Into<std::path::PathBuf>,
        content: impl Into<Vec<u8>>,
    ) -> Self {
        self.files.push(VirtualFile::new(path, content));
        self
    }
}

#[derive(Clone, Debug)]
pub enum VirtualMountBackend {
    Memory,
    ObjectStore(verlet_vfs::ObjectStoreMountConfig),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VirtualMountMode {
    ReadWrite,
    ReadOnly,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VirtualFile {
    pub path: std::path::PathBuf,
    pub content: Vec<u8>,
}

impl VirtualFile {
    pub fn new(path: impl Into<std::path::PathBuf>, content: impl Into<Vec<u8>>) -> Self {
        Self {
            path: path.into(),
            content: content.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandRoute {
    VirtualBash,
    HostBash,
    RemoteLinux,
    Deny,
}

impl CommandRoute {
    pub fn executor_kind(self) -> Option<verlet_process::ExternalExecutorKind> {
        match self {
            Self::HostBash => Some(verlet_process::ExternalExecutorKind::HostBash),
            Self::RemoteLinux => Some(verlet_process::ExternalExecutorKind::RemoteLinux),
            Self::VirtualBash | Self::Deny => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandRoutingPolicy {
    pub default_route: CommandRoute,
    pub named_proxy_routes: std::collections::BTreeMap<String, CommandRoute>,
}

impl CommandRoutingPolicy {
    pub fn virtual_only() -> Self {
        Self {
            default_route: CommandRoute::VirtualBash,
            named_proxy_routes: std::collections::BTreeMap::new(),
        }
    }

    pub fn host_always() -> Self {
        Self {
            default_route: CommandRoute::HostBash,
            named_proxy_routes: std::collections::BTreeMap::new(),
        }
    }

    pub fn remote_always() -> Self {
        Self {
            default_route: CommandRoute::RemoteLinux,
            named_proxy_routes: std::collections::BTreeMap::new(),
        }
    }

    pub fn selective<I, N>(named_proxy_routes: I) -> Self
    where
        I: IntoIterator<Item = (N, CommandRoute)>,
        N: Into<String>,
    {
        let mut policy = Self::virtual_only();
        for (name, route) in named_proxy_routes {
            policy = policy.with_named_proxy(name, route);
        }
        policy
    }

    pub fn with_named_proxy(mut self, name: impl Into<String>, route: CommandRoute) -> Self {
        self.named_proxy_routes.insert(name.into(), route);
        self
    }

    pub fn route_for_proxy(&self, name: &str) -> Option<CommandRoute> {
        self.named_proxy_routes.get(name).copied()
    }
}

impl Default for CommandRoutingPolicy {
    fn default() -> Self {
        Self::virtual_only()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BashExecutionPolicy {
    pub routing: CommandRoutingPolicy,
}

impl BashExecutionPolicy {
    pub fn virtual_only() -> Self {
        Self {
            routing: CommandRoutingPolicy::virtual_only(),
        }
    }

    pub fn host_always() -> Self {
        Self {
            routing: CommandRoutingPolicy::host_always(),
        }
    }

    pub fn remote_always() -> Self {
        Self {
            routing: CommandRoutingPolicy::remote_always(),
        }
    }

    pub fn selective<I, N>(named_proxy_routes: I) -> Self
    where
        I: IntoIterator<Item = (N, CommandRoute)>,
        N: Into<String>,
    {
        Self {
            routing: CommandRoutingPolicy::selective(named_proxy_routes),
        }
    }

    pub fn with_named_proxy(mut self, name: impl Into<String>, route: CommandRoute) -> Self {
        self.routing = self.routing.with_named_proxy(name, route);
        self
    }
}

impl Default for BashExecutionPolicy {
    fn default() -> Self {
        Self::virtual_only()
    }
}

pub fn absolute_mount_path(path: std::path::PathBuf) -> std::path::PathBuf {
    if path.is_absolute() {
        bashkit::normalize_path(&path)
    } else {
        bashkit::normalize_path(&std::path::PathBuf::from("/").join(path))
    }
}

pub fn default_virtual_mounts() -> Vec<VirtualMount> {
    vec![
        VirtualMount::writable("/workspace"),
        VirtualMount::writable("/tmp"),
        VirtualMount::writable("/files"),
        VirtualMount::writable("/my"),
        VirtualMount::writable("/livingapp"),
        VirtualMount::readonly(
            "/skills",
            vec![VirtualFile::new(
                "/README.md",
                "Verlet virtual bash skills mount\n",
            )],
        ),
    ]
}

pub fn validate_mounts(mounts: &[VirtualMount]) -> VerletVirtualBashResult<()> {
    let mut seen = std::collections::BTreeSet::new();
    for mount in mounts {
        if !mount.path.is_absolute() {
            return Err(VerletVirtualBashError::RuntimeFactory(format!(
                "virtual mount path must be absolute: {}",
                mount.path.display()
            )));
        }
        let normalized = bashkit::normalize_path(&mount.path);
        if normalized == std::path::Path::new("/") {
            return Err(VerletVirtualBashError::RuntimeFactory(
                "virtual mount path must not be /".to_string(),
            ));
        }
        if normalized.starts_with(std::path::Path::new("/spill")) {
            return Err(VerletVirtualBashError::RuntimeFactory(
                "virtual mount path /spill and its descendants are reserved for tool output spill"
                    .to_string(),
            ));
        }
        if !seen.insert(normalized.clone()) {
            return Err(VerletVirtualBashError::RuntimeFactory(format!(
                "duplicate virtual mount path: {}",
                normalized.display()
            )));
        }
    }
    Ok(())
}

pub async fn apply_external_file_writes(
    fs: &dyn bashkit::FileSystem,
    result: &verlet_process::ExternalCommandResult,
) -> VerletVirtualBashResult<()> {
    for write in &result.file_writes {
        validate_external_file_write(&write.path)?;
        fs.write_file(&write.path, &write.content)
            .await
            .map_err(virtual_bash_execution_error)?;
    }
    Ok(())
}

pub fn validate_external_file_write(path: &std::path::Path) -> VerletVirtualBashResult<()> {
    let normalized = absolute_mount_path(path.to_path_buf());
    if normalized != path {
        return Err(VerletVirtualBashError::RuntimeExecution(format!(
            "external file write path must be normalized: {}",
            path.display()
        )));
    }
    if normalized == std::path::Path::new("/") {
        return Err(VerletVirtualBashError::RuntimeExecution(
            "external file write path must not be /".to_string(),
        ));
    }
    Ok(())
}

pub fn deny_output(label: &str) -> verlet_process::VirtualCommandOutput {
    verlet_process::VirtualCommandOutput {
        stdout: String::new(),
        stderr: format!("verlet: command denied by routing policy: {label}\n"),
        exit_code: 126,
        stdout_truncated: false,
        stderr_truncated: false,
    }
}

pub fn virtual_command_output_from_exec_result(
    result: bashkit::ExecResult,
) -> verlet_process::VirtualCommandOutput {
    verlet_process::VirtualCommandOutput {
        stdout: result.stdout,
        stderr: result.stderr,
        exit_code: result.exit_code,
        stdout_truncated: result.stdout_truncated,
        stderr_truncated: result.stderr_truncated,
    }
}

pub fn exec_result_from_virtual_output(
    output: verlet_process::VirtualCommandOutput,
) -> bashkit::ExecResult {
    bashkit::ExecResult {
        stdout: output.stdout,
        stderr: output.stderr,
        exit_code: output.exit_code,
        stdout_truncated: output.stdout_truncated,
        stderr_truncated: output.stderr_truncated,
        ..Default::default()
    }
}

pub fn enforce_output_limit(
    mut output: verlet_process::VirtualCommandOutput,
    max_output_bytes: usize,
) -> verlet_process::VirtualCommandOutput {
    let capped_stdout = truncate_text_to_byte_limit(&mut output.stdout, max_output_bytes);
    let capped_stderr = truncate_text_to_byte_limit(&mut output.stderr, max_output_bytes);
    output.stdout_truncated |= capped_stdout;
    output.stderr_truncated |= capped_stderr;
    output
}

fn truncate_text_to_byte_limit(text: &mut String, max_output_bytes: usize) -> bool {
    if text.len() <= max_output_bytes {
        return false;
    }
    let mut end = max_output_bytes;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    true
}

pub const DEFAULT_SPILL_PREVIEW_BYTES: usize = 16 * 1024;
pub const EMERGENCY_SPILL_PREVIEW_BYTES: usize = 500;
pub const SPILL_RETENTION_MAX_BYTES: usize = 64 * 1024 * 1024;
pub const SPILL_VFS_MAX_BYTES: usize = 2 * SPILL_RETENTION_MAX_BYTES;

/// A stream that remains inline because it does not exceed the configured ceiling.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InlinePlan {
    pub content: String,
}

/// A stream whose retained raw bytes must be written to `path` before presenting `preview`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpillPlan<'a> {
    pub raw: &'a [u8],
    pub preview: String,
    pub path: String,
    pub total_bytes: usize,
    pub preview_bytes: usize,
    pub retention_truncated: bool,
}

/// Pure per-stream output plan selected before any VFS write is attempted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OverflowPlan<'a> {
    Inline(InlinePlan),
    Spill(SpillPlan<'a>),
}

pub fn plan_output_overflow<'a>(
    raw: &'a [u8],
    max_output_bytes: usize,
    retention_truncated: bool,
    spill_path: impl Into<String>,
) -> OverflowPlan<'a> {
    if !retention_truncated && raw.len() <= max_output_bytes {
        return OverflowPlan::Inline(InlinePlan {
            content: String::from_utf8_lossy(raw).into_owned(),
        });
    }

    let preview = utf8_safe_prefix(raw, DEFAULT_SPILL_PREVIEW_BYTES);
    OverflowPlan::Spill(SpillPlan {
        raw,
        preview: String::from_utf8_lossy(preview).into_owned(),
        path: spill_path.into(),
        total_bytes: raw.len(),
        preview_bytes: preview.len(),
        retention_truncated,
    })
}

pub fn format_spill_stub(plan: &SpillPlan<'_>) -> String {
    let retention = retention_truncation_notice(plan.retention_truncated);
    format!(
        "[CONTENT_SPILL: {} bytes]\n- Path: {}\n- Total bytes: {}\n- Preview bytes: {}{}\n\nPreview:\n---\n{}\n---\n\nTip: cat {}",
        plan.total_bytes,
        plan.path,
        plan.total_bytes,
        plan.preview_bytes,
        retention,
        plan.preview,
        plan.path,
    )
}

pub fn build_emergency_spill_stub(
    raw: &[u8],
    spill_path: &str,
    retention_truncated: bool,
) -> String {
    let head = utf8_safe_prefix(raw, EMERGENCY_SPILL_PREVIEW_BYTES);
    let tail = utf8_safe_suffix(raw, EMERGENCY_SPILL_PREVIEW_BYTES);
    let retention = retention_truncation_notice(retention_truncated);
    format!(
        "[CONTENT_OVERFLOW - spill path unavailable]\nPath: {spill_path}\nLength: {} bytes{}\nHead bytes: {}\nHead:\n{}\n...\nTail bytes: {}\nTail:\n{}",
        raw.len(),
        retention,
        head.len(),
        String::from_utf8_lossy(head),
        tail.len(),
        String::from_utf8_lossy(tail),
    )
}

fn retention_truncation_notice(retention_truncated: bool) -> String {
    if retention_truncated {
        format!(
            "\n- Retention: source stream exceeded the {SPILL_RETENTION_MAX_BYTES}-byte retention ceiling"
        )
    } else {
        String::new()
    }
}

fn utf8_safe_prefix(bytes: &[u8], max_bytes: usize) -> &[u8] {
    let mut end = bytes.len().min(max_bytes);
    while end > 0 && end < bytes.len() && is_utf8_continuation(bytes[end]) {
        end -= 1;
    }
    &bytes[..end]
}

fn utf8_safe_suffix(bytes: &[u8], max_bytes: usize) -> &[u8] {
    let mut start = bytes.len().saturating_sub(max_bytes);
    while start < bytes.len() && is_utf8_continuation(bytes[start]) {
        start += 1;
    }
    &bytes[start..]
}

fn is_utf8_continuation(byte: u8) -> bool {
    byte & 0b1100_0000 == 0b1000_0000
}

pub fn bytes_to_capped_text(bytes: &[u8], max_output_bytes: usize) -> (String, bool) {
    if bytes.len() <= max_output_bytes {
        return (String::from_utf8_lossy(bytes).to_string(), false);
    }
    (
        String::from_utf8_lossy(&bytes[..max_output_bytes]).to_string(),
        true,
    )
}

pub fn verlet_usage() -> String {
    "usage: verlet run <registered> <operation>\n".to_string()
}

pub fn reserved_operation_shell_commands() -> std::collections::BTreeSet<String> {
    [
        ".",
        ":",
        "[",
        "agent",
        "alias",
        "apply_patch",
        "assert",
        "awk",
        "base64",
        "basename",
        "bash",
        "bc",
        "break",
        "caller",
        "cat",
        "cd",
        "checkpoint",
        "chmod",
        "chown",
        "clear",
        "column",
        "command",
        "comm",
        "compgen",
        "continue",
        "verlet",
        "cp",
        "csv",
        "curl",
        "cut",
        "date",
        "declare",
        "df",
        "diff",
        "dirname",
        "dirs",
        "dotenv",
        "du",
        "echo",
        "env",
        "envsubst",
        "eval",
        "exec",
        "exit",
        "expand",
        "expr",
        "false",
        "fc",
        "file",
        "find",
        "fold",
        "git",
        "glob",
        "grep",
        "gunzip",
        "gzip",
        "hash",
        "head",
        "help",
        "hexdump",
        "history",
        "hostname",
        "http",
        "iconv",
        "id",
        "join",
        "jq",
        "json",
        "kill",
        "less",
        "let",
        "ln",
        "local",
        "log",
        "ls",
        "man",
        "mapfile",
        "md5sum",
        "mkdir",
        "mkfifo",
        "mktemp",
        "mv",
        "nl",
        "numfmt",
        "od",
        "parallel",
        "paste",
        "patch",
        "popd",
        "printf",
        "printenv",
        "python",
        "python3",
        "pushd",
        "pwd",
        "read",
        "readarray",
        "readlink",
        "readonly",
        "realpath",
        "return",
        "rev",
        "retry",
        "rg",
        "rm",
        "rmdir",
        "scp",
        "sed",
        "semver",
        "seq",
        "set",
        "sha1sum",
        "sha256sum",
        "sh",
        "shopt",
        "shift",
        "shuf",
        "sleep",
        "sort",
        "source",
        "split",
        "ssh",
        "sftp",
        "sqlite",
        "sqlite3",
        "stat",
        "strings",
        "tac",
        "tail",
        "tar",
        "tee",
        "template",
        "test",
        "times",
        "timeout",
        "tomlq",
        "touch",
        "tr",
        "trap",
        "tree",
        "true",
        "truncate",
        "ts",
        "type",
        "typeset",
        "typescript",
        "unalias",
        "uname",
        "unexpand",
        "uniq",
        "unset",
        "unzip",
        "verify",
        "wait",
        "watch",
        "wc",
        "wget",
        "which",
        "whoami",
        "xargs",
        "xxd",
        "yaml",
        "yes",
        "zip",
    ]
    .into_iter()
    .map(String::from)
    .collect()
}

pub fn operation_shell_reserved_commands(
    execution_policy: &BashExecutionPolicy,
) -> std::collections::BTreeSet<String> {
    let mut reserved = reserved_operation_shell_commands();
    reserved.extend(execution_policy.routing.named_proxy_routes.keys().cloned());
    reserved
}

pub fn summarize_operation_shell_commands(commands: &std::collections::BTreeSet<String>) -> String {
    const MAX_COMMANDS: usize = 12;
    let names = commands
        .iter()
        .take(MAX_COMMANDS)
        .cloned()
        .collect::<Vec<_>>();
    if commands.len() > MAX_COMMANDS {
        format!(
            "{} and {} more",
            names.join(", "),
            commands.len() - MAX_COMMANDS
        )
    } else {
        names.join(", ")
    }
}

pub fn operation_shell_manual(
    command: &str,
    projection: &verlet_operations::OperationProjection,
) -> String {
    let mut manual = String::new();
    manual.push_str("NAME\n");
    manual.push_str(&format!(
        "  {command} - {} from {}\n",
        projection.operation_name, projection.registered_name
    ));
    manual.push_str("USAGE\n");
    manual.push_str(&format!("  {command} [input]\n"));
    manual.push_str(&format!(
        "  verlet run {} {}\n",
        projection.registered_name, projection.operation_name
    ));
    manual.push_str("STDIN\n");
    manual.push_str(&format!("  {:?}\n", projection.input).to_ascii_lowercase());
    manual.push_str("STDOUT\n");
    manual.push_str(&format!("  {:?}\n", projection.output).to_ascii_lowercase());
    manual.push_str("CAPABILITIES\n");
    if projection.abi.required_capabilities.is_empty() {
        manual.push_str("  none\n");
    } else {
        for capability in &projection.abi.required_capabilities {
            manual.push_str(&format!("  {capability}\n"));
        }
    }
    manual.push_str("EXIT STATUS\n");
    manual.push_str("  0 operation succeeded\n");
    manual.push_str("  1 operation failed at runtime\n");
    manual.push_str("  2 caller supplied invalid input or arguments\n");
    manual.push_str("  126 capability or policy denied execution\n");
    manual.push_str("  127 tool or operation was not found\n");
    manual
}

pub fn operation_shell_command_name(projection: &verlet_operations::OperationProjection) -> String {
    normalize_operation_shell_command_name(&projection.operation_name)
}

pub fn normalize_operation_shell_command_name(raw: &str) -> String {
    let mut name = String::with_capacity(raw.len());
    let mut last_was_separator = false;
    for ch in raw.chars() {
        let normalized = if ch.is_ascii_alphanumeric() {
            last_was_separator = false;
            ch.to_ascii_lowercase()
        } else {
            if last_was_separator {
                continue;
            }
            last_was_separator = true;
            '_'
        };
        name.push(normalized);
    }
    name.trim_matches('_').to_string()
}

pub fn operation_shell_input(
    projection: &verlet_operations::OperationProjection,
    args: &[String],
    stdin: Option<&str>,
) -> VerletVirtualBashResult<Vec<u8>> {
    if let Some(stdin) = stdin.filter(|value| !value.is_empty()) {
        return Ok(stdin.as_bytes().to_vec());
    }

    match projection.input {
        verlet_abi::WasmOperationValueKind::Bytes | verlet_abi::WasmOperationValueKind::Text => {
            Ok(args.join(" ").into())
        }
        verlet_abi::WasmOperationValueKind::Json => {
            if args.is_empty() {
                return Ok(b"{}".to_vec());
            }
            if args.len() == 1 && looks_like_json(&args[0]) {
                serde_json::from_str::<serde_json::Value>(&args[0]).map_err(|err| {
                    VerletVirtualBashError::RuntimeExecution(format!(
                        "operation command {} received invalid JSON argument: {err}",
                        operation_shell_command_name(projection)
                    ))
                })?;
                return Ok(args[0].as_bytes().to_vec());
            }
            serde_json::to_vec(&serde_json::json!({ "query": args.join(" ") })).map_err(|err| {
                VerletVirtualBashError::RuntimeExecution(format!(
                    "operation command {} could not encode JSON input: {err}",
                    operation_shell_command_name(projection)
                ))
            })
        }
    }
}

pub fn looks_like_json(value: &str) -> bool {
    let value = value.trim_start();
    value.starts_with('{') || value.starts_with('[')
}

pub fn missing_operation_capability_grants(
    projection: &verlet_operations::OperationProjection,
    capability_grants: &std::collections::BTreeSet<String>,
) -> Vec<String> {
    projection
        .abi
        .required_capabilities
        .iter()
        .filter(|capability| !capability_grants.contains(capability.as_str()))
        .cloned()
        .collect()
}

pub fn virtual_bash_execution_error(err: impl std::fmt::Display) -> VerletVirtualBashError {
    VerletVirtualBashError::RuntimeExecution(err.to_string())
}

#[cfg(test)]
mod tests {

    #[test]
    fn routing_policy_defaults_to_virtual_bash() {
        assert_eq!(
            crate::BashExecutionPolicy::default().routing.default_route,
            crate::CommandRoute::VirtualBash
        );
        assert_eq!(
            crate::BashExecutionPolicy::selective([("cargo", crate::CommandRoute::RemoteLinux)])
                .routing
                .route_for_proxy("cargo"),
            Some(crate::CommandRoute::RemoteLinux)
        );
    }

    #[test]
    fn mount_validation_rejects_relative_duplicate_and_root_mounts() {
        assert!(crate::validate_mounts(&[crate::VirtualMount::writable("relative")]).is_err());
        assert!(crate::validate_mounts(&[crate::VirtualMount::writable("/")]).is_err());
        assert!(crate::validate_mounts(&[crate::VirtualMount::writable("/spill")]).is_err());
        assert!(crate::validate_mounts(&[crate::VirtualMount::writable("/spill/nested")]).is_err());
        assert!(
            crate::validate_mounts(&[
                crate::VirtualMount::writable("/workspace"),
                crate::VirtualMount::readonly("/workspace/../workspace", Vec::new()),
            ])
            .is_err()
        );
    }

    #[test]
    fn output_limit_marks_capped_streams() {
        let output = crate::enforce_output_limit(
            verlet_process::VirtualCommandOutput {
                stdout: "abcdef".to_string(),
                stderr: "xyz".to_string(),
                exit_code: 0,
                stdout_truncated: false,
                stderr_truncated: false,
            },
            3,
        );
        assert_eq!(output.stdout, "abc");
        assert!(output.stdout_truncated);
        assert!(!output.stderr_truncated);
    }

    #[test]
    fn output_limit_never_expands_past_the_byte_ceiling() {
        let output = crate::enforce_output_limit(
            verlet_process::VirtualCommandOutput {
                stdout: "💥💥".to_string(),
                stderr: String::new(),
                exit_code: 0,
                stdout_truncated: false,
                stderr_truncated: false,
            },
            5,
        );

        assert_eq!(output.stdout, "💥");
        assert_eq!(output.stdout.len(), 4);
        assert!(output.stdout_truncated);
    }

    #[test]
    fn overflow_plan_preserves_inline_bytes_and_spills_with_utf8_safe_preview() {
        let inline =
            crate::plan_output_overflow(b"exact output\n", 13, false, "/spill/call.stdout.txt");
        assert_eq!(
            inline,
            crate::OverflowPlan::Inline(crate::InlinePlan {
                content: "exact output\n".to_string(),
            })
        );

        let mut raw = vec![b'a'; crate::DEFAULT_SPILL_PREVIEW_BYTES - 1];
        raw.extend_from_slice("💥".as_bytes());
        raw.extend_from_slice(b"tail");
        let crate::OverflowPlan::Spill(plan) = crate::plan_output_overflow(
            &raw,
            crate::DEFAULT_SPILL_PREVIEW_BYTES,
            false,
            "/spill/call.stdout.txt",
        ) else {
            panic!("oversized output should spill");
        };

        assert_eq!(plan.raw, raw);
        assert_eq!(plan.raw.as_ptr(), raw.as_ptr());
        assert_eq!(plan.path, "/spill/call.stdout.txt");
        assert_eq!(plan.total_bytes, crate::DEFAULT_SPILL_PREVIEW_BYTES + 7);
        assert_eq!(plan.preview_bytes, crate::DEFAULT_SPILL_PREVIEW_BYTES - 1);
        assert_eq!(
            plan.preview.as_bytes(),
            &raw[..crate::DEFAULT_SPILL_PREVIEW_BYTES - 1]
        );
        assert!(!plan.preview.contains('\u{fffd}'));

        let stub = crate::format_spill_stub(&plan);
        assert!(stub.contains("[CONTENT_SPILL: 16391 bytes]"));
        assert!(stub.contains("- Path: /spill/call.stdout.txt"));
        assert!(stub.contains("- Preview bytes: 16383"));
        assert!(stub.contains("Tip: cat /spill/call.stdout.txt"));

        let crate::OverflowPlan::Spill(retained) = crate::plan_output_overflow(
            b"retained",
            usize::MAX,
            true,
            "/spill/retained.stdout.txt",
        ) else {
            panic!("retention-truncated output must spill regardless of presentation cap");
        };
        assert!(retained.retention_truncated);
        assert!(
            crate::format_spill_stub(&retained)
                .contains("exceeded the 67108864-byte retention ceiling")
        );
        assert!(
            crate::build_emergency_spill_stub(retained.raw, &retained.path, true)
                .contains("exceeded the 67108864-byte retention ceiling")
        );
    }

    #[test]
    fn emergency_spill_stub_has_utf8_safe_head_and_tail() {
        let mut raw = vec![b'h'; crate::EMERGENCY_SPILL_PREVIEW_BYTES - 1];
        raw.extend_from_slice("💥".as_bytes());
        raw.extend_from_slice(&vec![b't'; crate::EMERGENCY_SPILL_PREVIEW_BYTES]);

        let stub = crate::build_emergency_spill_stub(&raw, "/spill/call.stderr.txt", false);

        assert!(stub.contains("[CONTENT_OVERFLOW - spill path unavailable]"));
        assert!(stub.contains("Path: /spill/call.stderr.txt"));
        assert!(stub.contains("Length: 1003 bytes"));
        assert!(stub.contains("Head bytes: 499"));
        assert!(stub.contains("Tail bytes: 500"));
        assert!(!stub.contains('\u{fffd}'));
        assert!(!stub.contains('—'));
    }

    #[test]
    fn spill_retention_ceiling_is_sixty_four_mibibytes() {
        assert_eq!(crate::SPILL_RETENTION_MAX_BYTES, 64 * 1024 * 1024);
        assert_eq!(
            crate::SPILL_VFS_MAX_BYTES,
            2 * crate::SPILL_RETENTION_MAX_BYTES
        );
    }

    #[test]
    fn reserved_commands_include_builtin_surfaces() {
        let reserved = crate::operation_shell_reserved_commands(
            &crate::BashExecutionPolicy::selective([("cargo", crate::CommandRoute::HostBash)]),
        );
        assert!(reserved.contains("verlet"));
        assert!(reserved.contains("apply_patch"));
        assert!(reserved.contains("cargo"));
    }
}
