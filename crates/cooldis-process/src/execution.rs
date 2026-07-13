use crate::CooldisProcessResult;
use crate::process_error;
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
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

        let mut child = command.spawn().map_err(process_error)?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| process_error("host bash stdout pipe was not available"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| process_error("host bash stderr pipe was not available"))?;
        let stdout_task = tokio::spawn(read_capped_output(stdout, request.max_output_bytes));
        let stderr_task = tokio::spawn(read_capped_output(stderr, request.max_output_bytes));
        if let Some(stdin) = request.stdin
            && let Some(mut child_stdin) = child.stdin.take()
        {
            child_stdin
                .write_all(stdin.as_bytes())
                .await
                .map_err(process_error)?;
        }

        let status = match tokio::time::timeout(request.deadline.remaining(), child.wait()).await {
            Ok(status) => status.map_err(process_error)?,
            Err(_) => {
                let _ = child.kill().await;
                stdout_task.abort();
                stderr_task.abort();
                return Ok(external_timeout_result("host bash exec timed out\n"));
            }
        };
        let (stdout, stdout_truncated) = stdout_task.await.map_err(process_error)??;
        let (stderr, stderr_truncated) = stderr_task.await.map_err(process_error)??;
        Ok(ExternalCommandResult::new(VirtualCommandOutput {
            stdout,
            stderr,
            exit_code: status.code().unwrap_or(1),
            stdout_truncated,
            stderr_truncated,
        }))
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

async fn read_capped_output<R>(
    mut reader: R,
    max_output_bytes: usize,
) -> CooldisProcessResult<(String, bool)>
where
    R: AsyncRead + Unpin,
{
    let mut retained = String::with_capacity(max_output_bytes.min(8192));
    let mut truncated = false;
    let mut buffer = [0_u8; 8192 + 3];
    let mut pending = 0;
    loop {
        let read = reader
            .read(&mut buffer[pending..pending + 8192])
            .await
            .map_err(process_error)?;
        if read == 0 {
            if pending > 0
                && !truncated
                && matches!(
                    append_lossy_chunk(&mut retained, &buffer[..pending], max_output_bytes, true,),
                    LossyChunk::Truncated
                )
            {
                truncated = true;
            }
            break;
        }
        if truncated {
            pending = 0;
            continue;
        }
        let available = pending + read;
        match append_lossy_chunk(&mut retained, &buffer[..available], max_output_bytes, false) {
            LossyChunk::Complete => pending = 0,
            LossyChunk::Pending(start) => {
                buffer.copy_within(start..available, 0);
                pending = available - start;
            }
            LossyChunk::Truncated => {
                truncated = true;
                pending = 0;
            }
        }
    }
    Ok((retained, truncated))
}

enum LossyChunk {
    Complete,
    Pending(usize),
    Truncated,
}

fn append_lossy_chunk(
    output: &mut String,
    input: &[u8],
    max_output_bytes: usize,
    eof: bool,
) -> LossyChunk {
    let mut offset = 0;
    loop {
        match std::str::from_utf8(&input[offset..]) {
            Ok(valid) => {
                return if append_valid_prefix(output, valid, max_output_bytes) {
                    LossyChunk::Complete
                } else {
                    LossyChunk::Truncated
                };
            }
            Err(error) => {
                let valid_end = offset + error.valid_up_to();
                let valid = std::str::from_utf8(&input[offset..valid_end])
                    .expect("Utf8Error::valid_up_to must delimit valid UTF-8");
                if !append_valid_prefix(output, valid, max_output_bytes) {
                    return LossyChunk::Truncated;
                }
                let Some(invalid_bytes) = error.error_len() else {
                    if !eof {
                        return LossyChunk::Pending(valid_end);
                    }
                    return if append_replacement(output, max_output_bytes) {
                        LossyChunk::Complete
                    } else {
                        LossyChunk::Truncated
                    };
                };
                if !append_replacement(output, max_output_bytes) {
                    return LossyChunk::Truncated;
                }
                offset = valid_end + invalid_bytes;
            }
        }
    }
}

fn append_valid_prefix(output: &mut String, valid: &str, max_output_bytes: usize) -> bool {
    let available = max_output_bytes.saturating_sub(output.len());
    let mut end = available.min(valid.len());
    while !valid.is_char_boundary(end) {
        end -= 1;
    }
    output.push_str(&valid[..end]);
    end == valid.len()
}

fn append_replacement(output: &mut String, max_output_bytes: usize) -> bool {
    if output.len().saturating_add('\u{fffd}'.len_utf8()) > max_output_bytes {
        return false;
    }
    output.push('\u{fffd}');
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn capped_reader_drains_but_never_retains_more_than_its_limit() {
        let (mut writer, reader) = tokio::io::duplex(64);
        let write = tokio::spawn(async move {
            writer.write_all(&vec![b'x'; 1025]).await.unwrap();
        });

        let (retained, truncated) = read_capped_output(reader, 1024).await.unwrap();
        write.await.unwrap();

        assert_eq!(retained.len(), 1024);
        assert!(truncated);
    }

    #[tokio::test]
    async fn lossy_conversion_is_bounded_and_keeps_small_legacy_output() {
        let legacy = b"before\xffafter";
        let (text, truncated) = read_capped_output(&legacy[..], 64).await.unwrap();
        assert_eq!(text, String::from_utf8_lossy(legacy));
        assert!(!truncated);

        let invalid = vec![0xff; 1024];
        let (text, truncated) = read_capped_output(&invalid[..], 1024).await.unwrap();
        assert!(text.len() <= 1024);
        assert!(truncated);
    }

    #[tokio::test]
    async fn lossy_conversion_preserves_utf8_split_at_the_reader_boundary() {
        let mut input = vec![b'a'; 8191];
        input.extend_from_slice("💥tail".as_bytes());

        let (text, truncated) = read_capped_output(&input[..], input.len()).await.unwrap();

        assert_eq!(text.as_bytes(), input);
        assert!(!truncated);
    }
}
