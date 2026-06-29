use crate::CooldisProcessResult;
use crate::process_error;
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, SystemTime};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VirtualCommandOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

impl VirtualCommandOutput {
    pub fn success(&self) -> bool {
        self.exit_code == 0
    }

    pub fn event_text(&self) -> String {
        let mut text = String::new();
        text.push_str(&self.stdout);
        text.push_str(&self.stderr);
        if self.exit_code != 0 {
            if !text.ends_with('\n') && !text.is_empty() {
                text.push('\n');
            }
            text.push_str(&format!("[exit_code={}]\n", self.exit_code));
        }
        text
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalExecutorKind {
    HostBash,
    RemoteLinux,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExternalCommandInvocation {
    Script(String),
    Argv { command: String, args: Vec<String> },
}

impl ExternalCommandInvocation {
    pub fn label(&self) -> String {
        match self {
            Self::Script(script) => script.clone(),
            Self::Argv { command, args } if args.is_empty() => command.clone(),
            Self::Argv { command, args } => format!("{command} {}", args.join(" ")),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionDeadline {
    pub started_at: SystemTime,
    pub deadline_at: SystemTime,
    pub timeout: Duration,
}

impl ExecutionDeadline {
    pub fn from_now(timeout: Duration) -> Self {
        let started_at = SystemTime::now();
        let deadline_at = started_at.checked_add(timeout).unwrap_or(started_at);
        Self {
            started_at,
            deadline_at,
            timeout,
        }
    }

    pub fn remaining(&self) -> Duration {
        self.deadline_at
            .duration_since(SystemTime::now())
            .unwrap_or_default()
    }

    pub fn timeout_ms(&self) -> u64 {
        u64::try_from(self.timeout.as_millis()).unwrap_or(u64::MAX)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalCommandRequest {
    pub invocation: ExternalCommandInvocation,
    pub executor: ExternalExecutorKind,
    pub cwd: PathBuf,
    pub stdin: Option<String>,
    pub deadline: ExecutionDeadline,
    pub max_output_bytes: usize,
}

impl ExternalCommandRequest {
    pub fn label(&self) -> String {
        self.invocation.label()
    }
}

#[async_trait]
pub trait ExternalCommandExecutor: Send + Sync + 'static {
    async fn exec(
        &self,
        request: ExternalCommandRequest,
    ) -> CooldisProcessResult<ExternalCommandResult>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalFileWrite {
    pub path: PathBuf,
    pub content: Vec<u8>,
}

impl ExternalFileWrite {
    pub fn new(path: impl Into<PathBuf>, content: impl Into<Vec<u8>>) -> Self {
        Self {
            path: path.into(),
            content: content.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalCommandResult {
    pub output: VirtualCommandOutput,
    pub file_writes: Vec<ExternalFileWrite>,
}

impl ExternalCommandResult {
    pub fn new(output: VirtualCommandOutput) -> Self {
        Self {
            output,
            file_writes: Vec::new(),
        }
    }

    pub fn with_file_write(mut self, write: ExternalFileWrite) -> Self {
        self.file_writes.push(write);
        self
    }
}

#[derive(Clone, Debug, Default)]
pub struct RejectingExternalCommandExecutor;

#[async_trait]
impl ExternalCommandExecutor for RejectingExternalCommandExecutor {
    async fn exec(
        &self,
        request: ExternalCommandRequest,
    ) -> CooldisProcessResult<ExternalCommandResult> {
        Ok(ExternalCommandResult::new(VirtualCommandOutput {
            stdout: String::new(),
            stderr: format!(
                "external execution disabled for virtual bash command: {}\n",
                request.label()
            ),
            exit_code: 127,
            stdout_truncated: false,
            stderr_truncated: false,
        }))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostBashExecutorConfig {
    pub shell: PathBuf,
    pub workspace_root: PathBuf,
}

impl HostBashExecutorConfig {
    pub fn new(workspace_root: impl Into<PathBuf>) -> Self {
        Self {
            shell: PathBuf::from("/bin/bash"),
            workspace_root: workspace_root.into(),
        }
    }

    pub fn with_shell(mut self, shell: impl Into<PathBuf>) -> Self {
        self.shell = shell.into();
        self
    }
}

#[derive(Clone, Debug)]
pub struct HostBashExecutor {
    config: HostBashExecutorConfig,
}

impl HostBashExecutor {
    pub fn new(workspace_root: impl Into<PathBuf>) -> Self {
        Self {
            config: HostBashExecutorConfig::new(workspace_root),
        }
    }

    pub fn with_config(config: HostBashExecutorConfig) -> Self {
        Self { config }
    }

    fn host_cwd(&self, cwd: &Path) -> PathBuf {
        let workspace = Path::new("/workspace");
        if cwd == workspace {
            return self.config.workspace_root.clone();
        }
        if let Ok(relative) = cwd.strip_prefix(workspace) {
            return self.config.workspace_root.join(relative);
        }
        if cwd.is_relative() {
            return self.config.workspace_root.join(cwd);
        }
        self.config.workspace_root.clone()
    }
}

#[async_trait]
impl ExternalCommandExecutor for HostBashExecutor {
    async fn exec(
        &self,
        request: ExternalCommandRequest,
    ) -> CooldisProcessResult<ExternalCommandResult> {
        if request.executor != ExternalExecutorKind::HostBash {
            return Ok(ExternalCommandResult::new(VirtualCommandOutput {
                stdout: String::new(),
                stderr: format!(
                    "host bash executor cannot run {:?} command: {}\n",
                    request.executor,
                    request.label()
                ),
                exit_code: 127,
                stdout_truncated: false,
                stderr_truncated: false,
            }));
        }
        if request.deadline.remaining().is_zero() {
            return Ok(external_timeout_result("host bash exec timed out\n"));
        }

        let mut command = match &request.invocation {
            ExternalCommandInvocation::Script(script) => {
                let mut command = Command::new(&self.config.shell);
                command.arg("-lc").arg(script);
                command
            }
            ExternalCommandInvocation::Argv { command, args } => {
                let mut process = Command::new(command);
                process.args(args);
                process
            }
        };
        command
            .current_dir(self.host_cwd(&request.cwd))
            .stdin(if request.stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let stdin = request.stdin.clone();
        let wait_for_output = async move {
            let mut child = command.spawn().map_err(process_error)?;
            if let Some(stdin) = stdin
                && let Some(mut child_stdin) = child.stdin.take()
            {
                child_stdin
                    .write_all(stdin.as_bytes())
                    .await
                    .map_err(process_error)?;
            }
            child.wait_with_output().await.map_err(process_error)
        };

        let output = match tokio::time::timeout(request.deadline.remaining(), wait_for_output).await
        {
            Ok(output) => output?,
            Err(_) => return Ok(external_timeout_result("host bash exec timed out\n")),
        };
        Ok(ExternalCommandResult::new(output_from_host_process(
            output,
            request.max_output_bytes,
        )))
    }
}

fn external_timeout_result(message: impl Into<String>) -> ExternalCommandResult {
    let mut stderr = message.into();
    if !stderr.ends_with('\n') {
        stderr.push('\n');
    }
    ExternalCommandResult::new(VirtualCommandOutput {
        stdout: String::new(),
        stderr,
        exit_code: 124,
        stdout_truncated: false,
        stderr_truncated: false,
    })
}

fn output_from_host_process(
    output: std::process::Output,
    max_output_bytes: usize,
) -> VirtualCommandOutput {
    let (stdout, stdout_truncated) = bytes_to_capped_text(&output.stdout, max_output_bytes);
    let (stderr, stderr_truncated) = bytes_to_capped_text(&output.stderr, max_output_bytes);
    VirtualCommandOutput {
        stdout,
        stderr,
        exit_code: output.status.code().unwrap_or(1),
        stdout_truncated,
        stderr_truncated,
    }
}

fn bytes_to_capped_text(bytes: &[u8], max_output_bytes: usize) -> (String, bool) {
    if bytes.len() <= max_output_bytes {
        return (String::from_utf8_lossy(bytes).to_string(), false);
    }
    let capped = &bytes[..max_output_bytes];
    (String::from_utf8_lossy(capped).to_string(), true)
}
