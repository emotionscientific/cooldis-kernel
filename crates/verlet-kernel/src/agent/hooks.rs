use tokio::io::AsyncWriteExt as _;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookEventName {
    SessionStart,
    UserPromptSubmit,
    PreToolUse,
    PostToolUse,
    PreCompact,
    PostCompact,
    Stop,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookRunStatus {
    Completed,
    Blocked,
    Stopped,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct HookHandlerSpec {
    pub id: String,
    pub event_name: HookEventName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matcher: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct HookRunRecord {
    pub hook_id: String,
    pub event_name: HookEventName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matcher: Option<String>,
    pub status: HookRunStatus,
    pub started_at_ms: u64,
    pub completed_at_ms: u64,
    pub duration_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct HookValueDigest {
    pub before_sha256: String,
    pub after_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct HookMutationWitness {
    pub hook_id: String,
    #[serde(rename = "hook_event_name")]
    pub event_name: HookEventName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matcher: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_sha256: Option<String>,
    pub mutated_fields: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_input: Option<HookValueDigest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_output: Option<HookValueDigest>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SessionStartHookRequest {
    pub coordinates: verlet_runtime_contracts::ThreadCoordinates,
    pub parent_thread_id: Option<verlet_runtime_contracts::ThreadId>,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<std::path::PathBuf>,
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_profile: Option<String>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UserPromptSubmitHookRequest {
    pub turn_context: crate::kernel::runtime_host::turn::TurnContextSnapshot,
    pub prompt: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PreToolUseHookRequest {
    pub turn_context: crate::kernel::runtime_host::turn::TurnContextSnapshot,
    pub call_id: String,
    pub tool_name: String,
    pub arguments: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PostToolUseHookRequest {
    pub turn_context: crate::kernel::runtime_host::turn::TurnContextSnapshot,
    pub call_id: String,
    pub tool_name: String,
    pub arguments: serde_json::Value,
    pub output: String,
    pub success: bool,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PreCompactHookRequest {
    pub turn_context: crate::kernel::runtime_host::turn::TurnContextSnapshot,
    pub trigger: crate::kernel::compaction::CompactionTrigger,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_summary: Option<String>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PostCompactHookRequest {
    pub turn_context: crate::kernel::runtime_host::turn::TurnContextSnapshot,
    pub trigger: crate::kernel::compaction::CompactionTrigger,
    pub summary: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StopHookRequest {
    pub turn_context: crate::kernel::runtime_host::turn::TurnContextSnapshot,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_assistant_message: Option<String>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
// lexicon-allow: hook - faithful external hook protocol tag.
#[serde(tag = "hook_event_name", rename_all = "snake_case")]
pub enum HookRequest {
    SessionStart(SessionStartHookRequest),
    UserPromptSubmit(UserPromptSubmitHookRequest),
    PreToolUse(PreToolUseHookRequest),
    PostToolUse(PostToolUseHookRequest),
    PreCompact(PreCompactHookRequest),
    PostCompact(PostCompactHookRequest),
    Stop(StopHookRequest),
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct HookHandlerOutput {
    #[serde(default)]
    pub should_block: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_input: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub additional_contexts: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub additional_context: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feedback: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replacement_output: Option<String>,
    #[serde(default)]
    pub should_stop: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SessionStartHookOutcome {
    pub records: Vec<HookRunRecord>,
    pub mutation_witnesses: Vec<HookMutationWitness>,
    pub should_stop: bool,
    pub stop_reason: Option<String>,
    pub additional_contexts: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct UserPromptSubmitHookOutcome {
    pub records: Vec<HookRunRecord>,
    pub mutation_witnesses: Vec<HookMutationWitness>,
    pub should_stop: bool,
    pub stop_reason: Option<String>,
    pub additional_contexts: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PreToolUseHookOutcome {
    pub records: Vec<HookRunRecord>,
    pub mutation_witnesses: Vec<HookMutationWitness>,
    pub should_block: bool,
    pub block_reason: Option<String>,
    pub updated_input: Option<serde_json::Value>,
    pub additional_contexts: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PostToolUseHookOutcome {
    pub records: Vec<HookRunRecord>,
    pub mutation_witnesses: Vec<HookMutationWitness>,
    pub should_stop: bool,
    pub stop_reason: Option<String>,
    pub additional_contexts: Vec<String>,
    pub feedback: Option<String>,
    pub replacement_output: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PreCompactHookOutcome {
    pub records: Vec<HookRunRecord>,
    pub mutation_witnesses: Vec<HookMutationWitness>,
    pub should_stop: bool,
    pub stop_reason: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PostCompactHookOutcome {
    pub records: Vec<HookRunRecord>,
    pub mutation_witnesses: Vec<HookMutationWitness>,
    pub should_stop: bool,
    pub stop_reason: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
// lexicon-allow: hook - existing host debug hook outcome API name retained for compatibility.
pub struct StopHookOutcome {
    pub records: Vec<HookRunRecord>,
    pub mutation_witnesses: Vec<HookMutationWitness>,
    pub should_stop: bool,
    pub stop_reason: Option<String>,
    pub should_block: bool,
    pub block_reason: Option<String>,
    pub additional_contexts: Vec<String>,
}

#[async_trait::async_trait]
// lexicon-allow: hook - existing host debug hook trait name retained for compatibility.
pub trait HookHandler: Send + Sync + 'static {
    fn spec(&self) -> HookHandlerSpec;

    fn command_sha256(&self) -> Option<String> {
        None
    }

    async fn run(
        &self,
        request: HookRequest,
    ) -> crate::kernel::runtime_host::VerletResult<HookHandlerOutput>;

    async fn run_with_shell(
        &self,
        request: HookRequest,
        _shell: Option<&str>,
    ) -> crate::kernel::runtime_host::VerletResult<HookHandlerOutput> {
        self.run(request).await
    }
}

#[derive(Clone, Default)]
// lexicon-allow: hook - existing host debug hook pipeline API name retained for compatibility.
pub struct HookPipeline {
    handlers: Vec<std::sync::Arc<dyn HookHandler>>,
    shell: Option<String>,
}

impl HookPipeline {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_handler(mut self, handler: std::sync::Arc<dyn HookHandler>) -> Self {
        self.handlers.push(handler);
        self
    }

    pub(crate) fn with_shell(mut self, shell: Option<String>) -> Self {
        self.shell = shell;
        self
    }

    pub fn with_command_handler(self, handler: CommandHookHandler) -> Self {
        self.with_handler(std::sync::Arc::new(handler))
    }

    pub async fn run_session_start(
        &self,
        request: SessionStartHookRequest,
        on_started: impl FnMut(&HookHandlerSpec),
    ) -> SessionStartHookOutcome {
        let source = request.source.clone();
        let records = self
            .run_matching(
                HookEventName::SessionStart,
                Some(source.as_str()),
                HookRequest::SessionStart(request),
                on_started,
            )
            .await;
        let outputs = records.outputs;
        SessionStartHookOutcome {
            records: records.records,
            mutation_witnesses: records.mutation_witnesses,
            should_stop: outputs.iter().any(|output| output.should_stop),
            stop_reason: outputs.iter().find_map(|output| output.stop_reason.clone()),
            additional_contexts: collect_additional_contexts(outputs.iter()),
        }
    }

    pub async fn run_user_prompt_submit(
        &self,
        request: UserPromptSubmitHookRequest,
        on_started: impl FnMut(&HookHandlerSpec),
    ) -> UserPromptSubmitHookOutcome {
        let records = self
            .run_matching(
                HookEventName::UserPromptSubmit,
                None,
                HookRequest::UserPromptSubmit(request),
                on_started,
            )
            .await;
        let outputs = records.outputs;
        UserPromptSubmitHookOutcome {
            records: records.records,
            mutation_witnesses: records.mutation_witnesses,
            should_stop: outputs.iter().any(|output| output.should_stop),
            stop_reason: outputs.iter().find_map(|output| output.stop_reason.clone()),
            additional_contexts: collect_additional_contexts(outputs.iter()),
        }
    }

    pub async fn run_pre_tool_use(
        &self,
        request: PreToolUseHookRequest,
        on_started: impl FnMut(&HookHandlerSpec),
    ) -> PreToolUseHookOutcome {
        let matcher = request.tool_name.clone();
        let records = self
            .run_matching(
                HookEventName::PreToolUse,
                Some(matcher.as_str()),
                HookRequest::PreToolUse(request),
                on_started,
            )
            .await;
        let outputs = records.outputs;
        let should_block = outputs.iter().any(|output| output.should_block);
        PreToolUseHookOutcome {
            records: records.records,
            mutation_witnesses: records.mutation_witnesses,
            should_block,
            block_reason: outputs
                .iter()
                .find_map(|output| output.block_reason.clone()),
            updated_input: if should_block {
                None
            } else {
                outputs
                    .iter()
                    .rev()
                    .find_map(|output| output.updated_input.clone())
            },
            additional_contexts: collect_additional_contexts(outputs.iter()),
        }
    }

    pub async fn run_post_tool_use(
        &self,
        request: PostToolUseHookRequest,
        on_started: impl FnMut(&HookHandlerSpec),
    ) -> PostToolUseHookOutcome {
        let matcher = request.tool_name.clone();
        let records = self
            .run_matching(
                HookEventName::PostToolUse,
                Some(matcher.as_str()),
                HookRequest::PostToolUse(request),
                on_started,
            )
            .await;
        let outputs = records.outputs;
        PostToolUseHookOutcome {
            records: records.records,
            mutation_witnesses: records.mutation_witnesses,
            should_stop: outputs.iter().any(|output| output.should_stop),
            stop_reason: outputs.iter().find_map(|output| output.stop_reason.clone()),
            additional_contexts: collect_additional_contexts(outputs.iter()),
            feedback: join_optional_text(
                outputs.iter().filter_map(|output| output.feedback.clone()),
            ),
            replacement_output: outputs
                .iter()
                .rev()
                .find_map(|output| output.replacement_output.clone()),
        }
    }

    pub async fn run_pre_compact(
        &self,
        request: PreCompactHookRequest,
        on_started: impl FnMut(&HookHandlerSpec),
    ) -> PreCompactHookOutcome {
        let matcher = request.trigger.to_string();
        let records = self
            .run_matching(
                HookEventName::PreCompact,
                Some(matcher.as_str()),
                HookRequest::PreCompact(request),
                on_started,
            )
            .await;
        let outputs = records.outputs;
        PreCompactHookOutcome {
            records: records.records,
            mutation_witnesses: records.mutation_witnesses,
            should_stop: outputs.iter().any(|output| output.should_stop),
            stop_reason: outputs.iter().find_map(|output| output.stop_reason.clone()),
        }
    }

    pub async fn run_post_compact(
        &self,
        request: PostCompactHookRequest,
        on_started: impl FnMut(&HookHandlerSpec),
    ) -> PostCompactHookOutcome {
        let matcher = request.trigger.to_string();
        let records = self
            .run_matching(
                HookEventName::PostCompact,
                Some(matcher.as_str()),
                HookRequest::PostCompact(request),
                on_started,
            )
            .await;
        let outputs = records.outputs;
        PostCompactHookOutcome {
            records: records.records,
            mutation_witnesses: records.mutation_witnesses,
            should_stop: outputs.iter().any(|output| output.should_stop),
            stop_reason: outputs.iter().find_map(|output| output.stop_reason.clone()),
        }
    }

    pub async fn run_stop(
        &self,
        request: StopHookRequest,
        on_started: impl FnMut(&HookHandlerSpec),
    ) -> StopHookOutcome {
        let records = self
            .run_matching(
                HookEventName::Stop,
                None,
                HookRequest::Stop(request),
                on_started,
            )
            .await;
        let outputs = records.outputs;
        StopHookOutcome {
            records: records.records,
            mutation_witnesses: records.mutation_witnesses,
            should_stop: outputs.iter().any(|output| output.should_stop),
            stop_reason: outputs.iter().find_map(|output| output.stop_reason.clone()),
            should_block: outputs.iter().any(|output| output.should_block),
            block_reason: outputs
                .iter()
                .find_map(|output| output.block_reason.clone()),
            additional_contexts: collect_additional_contexts(outputs.iter()),
        }
    }

    async fn run_matching(
        &self,
        event_name: HookEventName,
        matcher_input: Option<&str>,
        request: HookRequest,
        mut on_started: impl FnMut(&HookHandlerSpec),
    ) -> HookExecutionBatch {
        let mut records = Vec::new();
        let mut outputs = Vec::new();
        let mut mutation_witnesses = Vec::new();
        for handler in self.handlers.iter() {
            let spec = handler.spec();
            let command_sha256 = handler.command_sha256();
            if spec.event_name != event_name
                || !matches_hook(spec.matcher.as_deref(), matcher_input)
            {
                continue;
            }
            on_started(&spec);
            let started_at_ms = unix_timestamp_ms();
            let started = std::time::Instant::now();
            match handler
                .run_with_shell(request.clone(), self.shell.as_deref())
                .await
            {
                Ok(output) => {
                    let status = status_for_output(&output);
                    let message = message_for_output(&output);
                    if let Some(witness) =
                        mutation_witness_for_output(&spec, command_sha256, &request, &output)
                    {
                        mutation_witnesses.push(witness);
                    }
                    records.push(HookRunRecord {
                        hook_id: spec.id,
                        event_name,
                        matcher: spec.matcher,
                        status,
                        started_at_ms,
                        completed_at_ms: unix_timestamp_ms(),
                        duration_ms: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
                        message,
                    });
                    outputs.push(output);
                }
                Err(err) => {
                    records.push(HookRunRecord {
                        hook_id: spec.id,
                        event_name,
                        matcher: spec.matcher,
                        status: HookRunStatus::Failed,
                        started_at_ms,
                        completed_at_ms: unix_timestamp_ms(),
                        duration_ms: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
                        message: Some(err.to_string()),
                    });
                }
            }
        }
        HookExecutionBatch {
            records,
            outputs,
            mutation_witnesses,
        }
    }
}

#[derive(Clone, Debug)]
// lexicon-allow: hook - existing command hook adapter API name retained for compatibility.
pub struct CommandHookHandler {
    spec: HookHandlerSpec,
    command: String,
    timeout_ms: u64,
    env: std::collections::BTreeMap<String, String>,
}

impl CommandHookHandler {
    pub fn new(
        id: impl Into<String>,
        event_name: HookEventName,
        command: impl Into<String>,
    ) -> Self {
        Self {
            spec: HookHandlerSpec {
                id: id.into(),
                event_name,
                matcher: None,
            },
            command: command.into(),
            timeout_ms: 5_000,
            env: std::collections::BTreeMap::new(),
        }
    }

    pub fn with_matcher(mut self, matcher: impl Into<String>) -> Self {
        self.spec.matcher = Some(matcher.into());
        self
    }

    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    pub fn with_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }

    fn cwd_for_request(request: &HookRequest) -> Option<std::path::PathBuf> {
        match request {
            HookRequest::SessionStart(request) => request.cwd.clone(),
            HookRequest::UserPromptSubmit(request) => request.turn_context.cwd.clone(),
            HookRequest::PreToolUse(request) => request.turn_context.cwd.clone(),
            HookRequest::PostToolUse(request) => request.turn_context.cwd.clone(),
            HookRequest::PreCompact(request) => request.turn_context.cwd.clone(),
            HookRequest::PostCompact(request) => request.turn_context.cwd.clone(),
            HookRequest::Stop(request) => request.turn_context.cwd.clone(),
        }
    }

    async fn run_with_shell_override(
        &self,
        request: HookRequest,
        shell: Option<&str>,
    ) -> crate::kernel::runtime_host::VerletResult<HookHandlerOutput> {
        let input = serde_json::to_string(&request).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeExecution(err.to_string())
        })?;
        let mut command = default_shell_command(shell);
        command.arg(&self.command);
        command
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        command.envs(&self.env);
        if let Some(cwd) = Self::cwd_for_request(&request) {
            command.current_dir(cwd);
        }
        let mut child = command.spawn().map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                "hook spawn failed: {err}"
            ))
        })?;
        if let Some(mut stdin) = child.stdin.take() {
            // A hook may exit without reading stdin; the resulting broken pipe
            // is not a failure. The exit status and stdout decide the outcome.
            match stdin.write_all(input.as_bytes()).await {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => {}
                Err(err) => {
                    return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                        format!("failed to write hook stdin: {err}"),
                    ));
                }
            }
        }
        let output = tokio::time::timeout(
            std::time::Duration::from_millis(self.timeout_ms),
            child.wait_with_output(),
        )
        .await
        .map_err(|_| {
            crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                "hook timed out after {}ms",
                self.timeout_ms
            ))
        })?
        .map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeExecution(err.to_string())
        })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                if stderr.is_empty() {
                    format!("hook exited with status {}", output.status)
                } else {
                    stderr
                },
            ));
        }
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if stdout.is_empty() {
            return Ok(HookHandlerOutput::default());
        }
        serde_json::from_str(&stdout).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                "failed to parse hook stdout: {err}"
            ))
        })
    }
}

#[async_trait::async_trait]
impl HookHandler for CommandHookHandler {
    fn spec(&self) -> HookHandlerSpec {
        self.spec.clone()
    }

    fn command_sha256(&self) -> Option<String> {
        Some(verlet_agent::contracts::sha256_hex(self.command.as_bytes()))
    }

    async fn run(
        &self,
        request: HookRequest,
    ) -> crate::kernel::runtime_host::VerletResult<HookHandlerOutput> {
        self.run_with_shell_override(request, None).await
    }

    async fn run_with_shell(
        &self,
        request: HookRequest,
        shell: Option<&str>,
    ) -> crate::kernel::runtime_host::VerletResult<HookHandlerOutput> {
        self.run_with_shell_override(request, shell).await
    }
}

struct HookExecutionBatch {
    records: Vec<HookRunRecord>,
    outputs: Vec<HookHandlerOutput>,
    mutation_witnesses: Vec<HookMutationWitness>,
}

fn mutation_witness_for_output(
    spec: &HookHandlerSpec,
    command_sha256: Option<String>,
    request: &HookRequest,
    output: &HookHandlerOutput,
) -> Option<HookMutationWitness> {
    let mut mutated_fields = Vec::new();
    let mut tool_input = None;
    let mut tool_output = None;
    match request {
        HookRequest::SessionStart(_) | HookRequest::UserPromptSubmit(_) | HookRequest::Stop(_) => {
            push_additional_contexts_field(&mut mutated_fields, output);
            if output.should_block {
                mutated_fields.push("should_block".to_string());
            }
            if output.should_stop {
                mutated_fields.push("should_stop".to_string());
            }
        }
        HookRequest::PreToolUse(request) => {
            push_additional_contexts_field(&mut mutated_fields, output);
            if output.should_block {
                mutated_fields.push("should_block".to_string());
            }
            if !output.should_block
                && let Some(updated_input) = &output.updated_input
                && updated_input != &request.arguments
            {
                mutated_fields.push("updated_input".to_string());
                tool_input = Some(json_value_digest(&request.arguments, updated_input));
            }
        }
        HookRequest::PostToolUse(request) => {
            push_additional_contexts_field(&mut mutated_fields, output);
            if output
                .feedback
                .as_ref()
                .is_some_and(|feedback| !feedback.trim().is_empty())
            {
                mutated_fields.push("feedback".to_string());
            }
            if output.should_stop {
                mutated_fields.push("should_stop".to_string());
            }
            if let Some(replacement_output) = &output.replacement_output
                && replacement_output != &request.output
            {
                mutated_fields.push("replacement_output".to_string());
                tool_output = Some(text_digest(&request.output, replacement_output));
            }
        }
        HookRequest::PreCompact(_) | HookRequest::PostCompact(_) => {
            if output.should_stop {
                mutated_fields.push("should_stop".to_string());
            }
        }
    }
    if mutated_fields.is_empty() {
        return None;
    }
    Some(HookMutationWitness {
        hook_id: spec.id.clone(),
        event_name: spec.event_name,
        matcher: spec.matcher.clone(),
        command_sha256,
        mutated_fields,
        tool_input,
        tool_output,
    })
}

fn push_additional_contexts_field(mutated_fields: &mut Vec<String>, output: &HookHandlerOutput) {
    let has_context = output
        .additional_context
        .as_ref()
        .is_some_and(|context| !context.trim().is_empty())
        || output
            .additional_contexts
            .iter()
            .any(|context| !context.trim().is_empty());
    if has_context {
        mutated_fields.push("additional_contexts".to_string());
    }
}

fn json_value_digest(before: &serde_json::Value, after: &serde_json::Value) -> HookValueDigest {
    let before = serde_json::to_vec(before).unwrap_or_else(|_| before.to_string().into_bytes());
    let after = serde_json::to_vec(after).unwrap_or_else(|_| after.to_string().into_bytes());
    HookValueDigest {
        before_sha256: verlet_agent::contracts::sha256_hex(&before),
        after_sha256: verlet_agent::contracts::sha256_hex(&after),
    }
}

fn text_digest(before: &str, after: &str) -> HookValueDigest {
    HookValueDigest {
        before_sha256: verlet_agent::contracts::sha256_hex(before.as_bytes()),
        after_sha256: verlet_agent::contracts::sha256_hex(after.as_bytes()),
    }
}

fn matches_hook(matcher: Option<&str>, input: Option<&str>) -> bool {
    match matcher {
        None => true,
        Some("*") => true,
        Some(matcher) => input.is_some_and(|input| matcher == input),
    }
}

fn status_for_output(output: &HookHandlerOutput) -> HookRunStatus {
    if output.should_block {
        HookRunStatus::Blocked
    } else if output.should_stop {
        HookRunStatus::Stopped
    } else {
        HookRunStatus::Completed
    }
}

fn message_for_output(output: &HookHandlerOutput) -> Option<String> {
    output
        .block_reason
        .clone()
        .or_else(|| output.stop_reason.clone())
        .or_else(|| output.feedback.clone())
}

fn collect_additional_contexts<'a>(
    outputs: impl Iterator<Item = &'a HookHandlerOutput>,
) -> Vec<String> {
    outputs
        .flat_map(|output| {
            output
                .additional_context
                .clone()
                .into_iter()
                .chain(output.additional_contexts.clone())
        })
        .collect()
}

fn join_optional_text(chunks: impl Iterator<Item = String>) -> Option<String> {
    let chunks = chunks
        .filter(|chunk| !chunk.trim().is_empty())
        .collect::<Vec<_>>();
    if chunks.is_empty() {
        None
    } else {
        Some(chunks.join("\n"))
    }
}

fn unix_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn default_shell_command(shell: Option<&str>) -> tokio::process::Command {
    #[cfg(windows)]
    {
        let comspec = shell
            .map(str::to_string)
            .unwrap_or_else(|| std::env::var("COMSPEC").unwrap_or_else(|_| "cmd.exe".to_string()));
        let mut command = tokio::process::Command::new(comspec);
        command.arg("/C");
        command
    }

    #[cfg(not(windows))]
    {
        let shell = shell
            .map(str::to_string)
            .unwrap_or_else(|| std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string()));
        let mut command = tokio::process::Command::new(shell);
        command.arg("-lc");
        command
    }
}

#[cfg(test)]
mod tests;
