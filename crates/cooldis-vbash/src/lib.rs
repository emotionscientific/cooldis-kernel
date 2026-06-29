mod apply_patch;
mod harness;

pub use apply_patch::apply_patch_to_bashkit;
use bashkit::{ExecResult, FileSystem};
use cooldis_abi::WasmOperationValueKind;
use cooldis_operations::OperationProjection;
use cooldis_process::{ExternalCommandResult, ExternalExecutorKind, VirtualCommandOutput};
use cooldis_vfs::ObjectStoreMountConfig;
pub use harness::{
    BashkitExecutionConfig, BashkitExecutionHarness, BashkitLiveBackend, VbashOperationRegistry,
    operation_shell_command_names,
};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

pub type CooldisVirtualBashResult<T> = Result<T, CooldisVirtualBashError>;

#[derive(Debug, thiserror::Error)]
pub enum CooldisVirtualBashError {
    #[error("virtual bash factory failed: {0}")]
    RuntimeFactory(String),
    #[error("virtual bash execution failed: {0}")]
    RuntimeExecution(String),
}

pub const BASH_TOOL: &str = "bash";

#[derive(Clone, Debug)]
pub struct VirtualMount {
    pub path: PathBuf,
    pub mode: VirtualMountMode,
    pub backend: VirtualMountBackend,
    pub files: Vec<VirtualFile>,
}

impl VirtualMount {
    pub fn writable(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            mode: VirtualMountMode::ReadWrite,
            backend: VirtualMountBackend::Memory,
            files: Vec::new(),
        }
    }

    pub fn readonly(path: impl Into<PathBuf>, files: Vec<VirtualFile>) -> Self {
        Self {
            path: path.into(),
            mode: VirtualMountMode::ReadOnly,
            backend: VirtualMountBackend::Memory,
            files,
        }
    }

    pub fn object_store(path: impl Into<PathBuf>, config: ObjectStoreMountConfig) -> Self {
        Self {
            path: path.into(),
            mode: VirtualMountMode::ReadWrite,
            backend: VirtualMountBackend::ObjectStore(config),
            files: Vec::new(),
        }
    }

    pub fn readonly_object_store(path: impl Into<PathBuf>, config: ObjectStoreMountConfig) -> Self {
        Self {
            path: path.into(),
            mode: VirtualMountMode::ReadOnly,
            backend: VirtualMountBackend::ObjectStore(config),
            files: Vec::new(),
        }
    }

    pub fn with_file(mut self, path: impl Into<PathBuf>, content: impl Into<Vec<u8>>) -> Self {
        self.files.push(VirtualFile::new(path, content));
        self
    }
}

#[derive(Clone, Debug)]
pub enum VirtualMountBackend {
    Memory,
    ObjectStore(ObjectStoreMountConfig),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VirtualMountMode {
    ReadWrite,
    ReadOnly,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VirtualFile {
    pub path: PathBuf,
    pub content: Vec<u8>,
}

impl VirtualFile {
    pub fn new(path: impl Into<PathBuf>, content: impl Into<Vec<u8>>) -> Self {
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
    pub fn executor_kind(self) -> Option<ExternalExecutorKind> {
        match self {
            Self::HostBash => Some(ExternalExecutorKind::HostBash),
            Self::RemoteLinux => Some(ExternalExecutorKind::RemoteLinux),
            Self::VirtualBash | Self::Deny => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandRoutingPolicy {
    pub default_route: CommandRoute,
    pub named_proxy_routes: BTreeMap<String, CommandRoute>,
}

impl CommandRoutingPolicy {
    pub fn virtual_only() -> Self {
        Self {
            default_route: CommandRoute::VirtualBash,
            named_proxy_routes: BTreeMap::new(),
        }
    }

    pub fn host_always() -> Self {
        Self {
            default_route: CommandRoute::HostBash,
            named_proxy_routes: BTreeMap::new(),
        }
    }

    pub fn remote_always() -> Self {
        Self {
            default_route: CommandRoute::RemoteLinux,
            named_proxy_routes: BTreeMap::new(),
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

pub fn absolute_mount_path(path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        bashkit::normalize_path(&path)
    } else {
        bashkit::normalize_path(&PathBuf::from("/").join(path))
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
                "Cooldis virtual bash skills mount\n",
            )],
        ),
    ]
}

pub fn validate_mounts(mounts: &[VirtualMount]) -> CooldisVirtualBashResult<()> {
    let mut seen = BTreeSet::new();
    for mount in mounts {
        if !mount.path.is_absolute() {
            return Err(CooldisVirtualBashError::RuntimeFactory(format!(
                "virtual mount path must be absolute: {}",
                mount.path.display()
            )));
        }
        let normalized = bashkit::normalize_path(&mount.path);
        if normalized == Path::new("/") {
            return Err(CooldisVirtualBashError::RuntimeFactory(
                "virtual mount path must not be /".to_string(),
            ));
        }
        if !seen.insert(normalized.clone()) {
            return Err(CooldisVirtualBashError::RuntimeFactory(format!(
                "duplicate virtual mount path: {}",
                normalized.display()
            )));
        }
    }
    Ok(())
}

pub async fn apply_external_file_writes(
    fs: &dyn FileSystem,
    result: &ExternalCommandResult,
) -> CooldisVirtualBashResult<()> {
    for write in &result.file_writes {
        validate_external_file_write(&write.path)?;
        fs.write_file(&write.path, &write.content)
            .await
            .map_err(virtual_bash_execution_error)?;
    }
    Ok(())
}

pub fn validate_external_file_write(path: &Path) -> CooldisVirtualBashResult<()> {
    let normalized = absolute_mount_path(path.to_path_buf());
    if normalized != path {
        return Err(CooldisVirtualBashError::RuntimeExecution(format!(
            "external file write path must be normalized: {}",
            path.display()
        )));
    }
    if normalized == Path::new("/") {
        return Err(CooldisVirtualBashError::RuntimeExecution(
            "external file write path must not be /".to_string(),
        ));
    }
    Ok(())
}

pub fn deny_output(label: &str) -> VirtualCommandOutput {
    VirtualCommandOutput {
        stdout: String::new(),
        stderr: format!("cooldis: command denied by routing policy: {label}\n"),
        exit_code: 126,
        stdout_truncated: false,
        stderr_truncated: false,
    }
}

pub fn virtual_command_output_from_exec_result(result: ExecResult) -> VirtualCommandOutput {
    VirtualCommandOutput {
        stdout: result.stdout,
        stderr: result.stderr,
        exit_code: result.exit_code,
        stdout_truncated: result.stdout_truncated,
        stderr_truncated: result.stderr_truncated,
    }
}

pub fn exec_result_from_virtual_output(output: VirtualCommandOutput) -> ExecResult {
    ExecResult {
        stdout: output.stdout,
        stderr: output.stderr,
        exit_code: output.exit_code,
        stdout_truncated: output.stdout_truncated,
        stderr_truncated: output.stderr_truncated,
        ..Default::default()
    }
}

pub fn enforce_output_limit(
    output: VirtualCommandOutput,
    max_output_bytes: usize,
) -> VirtualCommandOutput {
    let (stdout, capped_stdout) = bytes_to_capped_text(output.stdout.as_bytes(), max_output_bytes);
    let (stderr, capped_stderr) = bytes_to_capped_text(output.stderr.as_bytes(), max_output_bytes);
    VirtualCommandOutput {
        stdout,
        stderr,
        exit_code: output.exit_code,
        stdout_truncated: output.stdout_truncated || capped_stdout,
        stderr_truncated: output.stderr_truncated || capped_stderr,
    }
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

pub fn cooldis_usage() -> String {
    "usage: cooldis run <registered> <operation>\n".to_string()
}

pub fn reserved_operation_shell_commands() -> BTreeSet<String> {
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
        "cooldis",
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
) -> BTreeSet<String> {
    let mut reserved = reserved_operation_shell_commands();
    reserved.extend(execution_policy.routing.named_proxy_routes.keys().cloned());
    reserved
}

pub fn summarize_operation_shell_commands(commands: &BTreeSet<String>) -> String {
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

pub fn operation_shell_manual(command: &str, projection: &OperationProjection) -> String {
    let mut manual = String::new();
    manual.push_str("NAME\n");
    manual.push_str(&format!(
        "  {command} - {} from {}\n",
        projection.operation_name, projection.registered_name
    ));
    manual.push_str("USAGE\n");
    manual.push_str(&format!("  {command} [input]\n"));
    manual.push_str(&format!(
        "  cooldis run {} {}\n",
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

pub fn operation_shell_command_name(projection: &OperationProjection) -> String {
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
    projection: &OperationProjection,
    args: &[String],
    stdin: Option<&str>,
) -> CooldisVirtualBashResult<Vec<u8>> {
    if let Some(stdin) = stdin.filter(|value| !value.is_empty()) {
        return Ok(stdin.as_bytes().to_vec());
    }

    match projection.input {
        WasmOperationValueKind::Bytes | WasmOperationValueKind::Text => Ok(args.join(" ").into()),
        WasmOperationValueKind::Json => {
            if args.is_empty() {
                return Ok(b"{}".to_vec());
            }
            if args.len() == 1 && looks_like_json(&args[0]) {
                serde_json::from_str::<serde_json::Value>(&args[0]).map_err(|err| {
                    CooldisVirtualBashError::RuntimeExecution(format!(
                        "operation command {} received invalid JSON argument: {err}",
                        operation_shell_command_name(projection)
                    ))
                })?;
                return Ok(args[0].as_bytes().to_vec());
            }
            serde_json::to_vec(&json!({ "query": args.join(" ") })).map_err(|err| {
                CooldisVirtualBashError::RuntimeExecution(format!(
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
    projection: &OperationProjection,
    capability_grants: &BTreeSet<String>,
) -> Vec<String> {
    projection
        .abi
        .required_capabilities
        .iter()
        .filter(|capability| !capability_grants.contains(capability.as_str()))
        .cloned()
        .collect()
}

pub fn virtual_bash_execution_error(err: impl std::fmt::Display) -> CooldisVirtualBashError {
    CooldisVirtualBashError::RuntimeExecution(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_policy_defaults_to_virtual_bash() {
        assert_eq!(
            BashExecutionPolicy::default().routing.default_route,
            CommandRoute::VirtualBash
        );
        assert_eq!(
            BashExecutionPolicy::selective([("cargo", CommandRoute::RemoteLinux)])
                .routing
                .route_for_proxy("cargo"),
            Some(CommandRoute::RemoteLinux)
        );
    }

    #[test]
    fn mount_validation_rejects_relative_duplicate_and_root_mounts() {
        assert!(validate_mounts(&[VirtualMount::writable("relative")]).is_err());
        assert!(validate_mounts(&[VirtualMount::writable("/")]).is_err());
        assert!(
            validate_mounts(&[
                VirtualMount::writable("/workspace"),
                VirtualMount::readonly("/workspace/../workspace", Vec::new()),
            ])
            .is_err()
        );
    }

    #[test]
    fn output_limit_marks_capped_streams() {
        let output = enforce_output_limit(
            VirtualCommandOutput {
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
    fn reserved_commands_include_builtin_surfaces() {
        let reserved = operation_shell_reserved_commands(&BashExecutionPolicy::selective([(
            "cargo",
            CommandRoute::HostBash,
        )]));
        assert!(reserved.contains("cooldis"));
        assert!(reserved.contains("apply_patch"));
        assert!(reserved.contains("cargo"));
    }
}
