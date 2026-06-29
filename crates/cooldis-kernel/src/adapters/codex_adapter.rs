use crate::{
    AgentRuntime, AgentRuntimeFactory, CanonicalContent, CanonicalMessage, CanonicalStopReason,
    CooldisError, CooldisResult, ProviderApi, RuntimeEventKind, RuntimeServices,
    RuntimeTerminalState, SessionEntryKind, ThreadCommand, ThreadContext, ThreadCoordinates,
    ThreadEvent, ThreadSignal, ThreadStatus, TurnSubmissionMode, emit_runtime_event,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::process::Stdio;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

/// Tenant-scoped configuration needed to host Codex behind Cooldis.
///
/// This process-backed adapter is intentionally a v1 bridge. It proves Cooldis
/// can route tenant-scoped turns into a local `codex exec` process without
/// vendoring Codex or depending on its in-process APIs.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CodexRuntimeConfig {
    pub codex_bin: PathBuf,
    pub codex_home: PathBuf,
    pub sqlite_home: PathBuf,
    pub cwd: PathBuf,
    pub model: Option<String>,
    pub sandbox: Option<String>,
    pub extra_args: Vec<String>,
}

impl CodexRuntimeConfig {
    pub fn local(
        codex_home: impl Into<PathBuf>,
        sqlite_home: impl Into<PathBuf>,
        cwd: impl Into<PathBuf>,
    ) -> Self {
        Self {
            codex_bin: PathBuf::from("codex"),
            codex_home: codex_home.into(),
            sqlite_home: sqlite_home.into(),
            cwd: cwd.into(),
            model: None,
            sandbox: None,
            extra_args: Vec::new(),
        }
    }

    pub fn with_codex_bin(mut self, codex_bin: impl Into<PathBuf>) -> Self {
        self.codex_bin = codex_bin.into();
        self
    }

    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    pub fn with_sandbox(mut self, sandbox: impl Into<String>) -> Self {
        self.sandbox = Some(sandbox.into());
        self
    }

    pub fn with_extra_arg(mut self, arg: impl Into<String>) -> Self {
        self.extra_args.push(arg.into());
        self
    }
}

#[derive(Clone, Debug)]
pub struct CodexCliRuntimeFactory {
    pub config: CodexRuntimeConfig,
}

impl CodexCliRuntimeFactory {
    pub fn new(config: CodexRuntimeConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl AgentRuntimeFactory for CodexCliRuntimeFactory {
    async fn build(&self, _context: &ThreadContext) -> CooldisResult<Box<dyn AgentRuntime>> {
        Ok(Box::new(CodexCliRuntime {
            config: self.config.clone(),
        }))
    }
}

struct CodexCliRuntime {
    config: CodexRuntimeConfig,
}

#[async_trait]
impl AgentRuntime for CodexCliRuntime {
    async fn run(
        self: Box<Self>,
        context: ThreadContext,
        services: RuntimeServices,
        mut commands: mpsc::Receiver<ThreadCommand>,
        events: broadcast::Sender<ThreadEvent>,
        status: watch::Sender<ThreadStatus>,
        cancellation: CancellationToken,
    ) {
        let thread_id = context.coordinates.thread_id;
        let coordinates = context.coordinates.clone();
        emit_runtime_event(
            &events,
            &coordinates,
            RuntimeEventKind::ThreadStarted {
                parent_thread_id: context.parent_thread_id,
                topology: context.topology.clone(),
            },
        );
        let _ = events.send(ThreadEvent::Started { context });
        let _ = status.send(ThreadStatus::Idle);
        let mut pending_submits = VecDeque::new();

        loop {
            if let Some(ThreadCommand::Submit { input, .. }) = pending_submits.pop_front() {
                let _ = status.send(ThreadStatus::Running);
                match services.append_user_turn_input(&coordinates, &input).await {
                    Ok(entry) => {
                        let _ = events.send(ThreadEvent::CanonicalMirror { thread_id, entry });
                    }
                    Err(err) => {
                        let _ = status.send(ThreadStatus::Failed);
                        let _ = events.send(ThreadEvent::Failed {
                            thread_id,
                            message: err.to_string(),
                        });
                        break;
                    }
                }
                if run_codex_cli_turn(
                    &self.config,
                    &services,
                    &coordinates,
                    thread_id,
                    input.text_projection(),
                    &events,
                    &status,
                    &mut commands,
                    &cancellation,
                    &mut pending_submits,
                )
                .await
                {
                    break;
                }
                continue;
            }

            tokio::select! {
                _ = cancellation.cancelled() => {
                    emit_runtime_event(
                        &events,
                        &coordinates,
                        RuntimeEventKind::Terminal {
                            state: RuntimeTerminalState::Stopped,
                        },
                    );
                    let _ = status.send(ThreadStatus::Stopped);
                    let _ = events.send(ThreadEvent::Stopped { thread_id });
                    break;
                }
                command = commands.recv() => {
                    match command {
                        Some(ThreadCommand::Submit { input, mode, .. }) => {
                            if mode == TurnSubmissionMode::Steer {
                                emit_runtime_event(
                                    &events,
                                    &coordinates,
                                    RuntimeEventKind::PolicyRejected {
                                        code: "no_active_turn".to_string(),
                                        message: "steer input requires an active Codex CLI turn".to_string(),
                                    },
                                );
                                continue;
                            }
                            let _ = status.send(ThreadStatus::Running);
                            match services.append_user_turn_input(&coordinates, &input).await {
                                Ok(entry) => {
                                    let _ = events.send(ThreadEvent::CanonicalMirror { thread_id, entry });
                                }
                                Err(err) => {
                                    let _ = status.send(ThreadStatus::Failed);
                                    let _ = events.send(ThreadEvent::Failed {
                                        thread_id,
                                        message: err.to_string(),
                                    });
                                    break;
                                }
                            }
                            if run_codex_cli_turn(
                                &self.config,
                                &services,
                                &coordinates,
                                thread_id,
                                input.text_projection(),
                                &events,
                                &status,
                                &mut commands,
                                &cancellation,
                                &mut pending_submits,
                            )
                            .await
                            {
                                break;
                            }
                        }
                        Some(ThreadCommand::Cancel { reason }) => {
                            let _ = status.send(ThreadStatus::Cancelling);
                            let _ = events.send(ThreadEvent::Signal {
                                thread_id,
                                signal: ThreadSignal::interrupt_cancel(&coordinates, reason.clone()),
                            });
                            let _ = events.send(ThreadEvent::Cancelled { thread_id, reason });
                            let _ = status.send(ThreadStatus::Idle);
                        }
                        Some(ThreadCommand::Compact { .. }) => {
                            emit_runtime_event(
                                &events,
                                &coordinates,
                                RuntimeEventKind::PolicyRejected {
                                    code: "compact_unsupported".to_string(),
                                    message: "Codex CLI runtime does not support Cooldis compaction commands".to_string(),
                                },
                            );
                            let _ = status.send(ThreadStatus::Idle);
                        }
                        Some(ThreadCommand::ResumeToolCall { .. }) => {
                            emit_runtime_event(
                                &events,
                                &coordinates,
                                RuntimeEventKind::PolicyRejected {
                                    code: "tool_resume_unsupported".to_string(),
                                    message: "Codex CLI runtime does not support provider tool-call resume".to_string(),
                                },
                            );
                            let _ = status.send(ThreadStatus::Idle);
                        }
                        Some(ThreadCommand::Shutdown) | None => {
                            let _ = events.send(ThreadEvent::Signal {
                                thread_id,
                                signal: ThreadSignal::shutdown(&coordinates),
                            });
                            emit_runtime_event(
                                &events,
                                &coordinates,
                                RuntimeEventKind::Terminal {
                                    state: RuntimeTerminalState::Stopped,
                                },
                            );
                            let _ = status.send(ThreadStatus::Stopped);
                            let _ = events.send(ThreadEvent::Stopped { thread_id });
                            break;
                        }
                    }
                }
            }
        }
    }
}

async fn run_codex_cli_turn(
    config: &CodexRuntimeConfig,
    services: &RuntimeServices,
    coordinates: &ThreadCoordinates,
    thread_id: crate::ThreadId,
    input: String,
    events: &broadcast::Sender<ThreadEvent>,
    status: &watch::Sender<ThreadStatus>,
    commands: &mut mpsc::Receiver<ThreadCommand>,
    runtime_cancellation: &CancellationToken,
    pending_submits: &mut VecDeque<ThreadCommand>,
) -> bool {
    let turn_cancellation = CancellationToken::new();
    let mut cancelled_reason = None;
    let mut shutdown_after_turn = false;
    let mut failed = false;
    let execute = run_codex_exec(config, input, &turn_cancellation);
    tokio::pin!(execute);

    let result = loop {
        tokio::select! {
            result = &mut execute => break result,
            _ = runtime_cancellation.cancelled() => {
                turn_cancellation.cancel();
                cancelled_reason = Some("runtime cancellation requested".to_string());
            }
            command = commands.recv() => {
                match command {
                    Some(ThreadCommand::Cancel { reason }) => {
                        let _ = status.send(ThreadStatus::Cancelling);
                        let _ = events.send(ThreadEvent::Signal {
                            thread_id,
                            signal: ThreadSignal::interrupt_cancel(coordinates, reason.clone()),
                        });
                        emit_runtime_event(
                            events,
                            coordinates,
                            RuntimeEventKind::Cancelled {
                                reason: reason.clone(),
                            },
                        );
                        turn_cancellation.cancel();
                        cancelled_reason = Some(reason);
                    }
                    Some(ThreadCommand::Shutdown) | None => {
                        let _ = events.send(ThreadEvent::Signal {
                            thread_id,
                            signal: ThreadSignal::shutdown(coordinates),
                        });
                        emit_runtime_event(
                            events,
                            coordinates,
                            RuntimeEventKind::Terminal {
                                state: RuntimeTerminalState::Stopped,
                            },
                        );
                        turn_cancellation.cancel();
                        shutdown_after_turn = true;
                    }
                    Some(ThreadCommand::Submit { turn_id, input, mode }) => {
                        match mode {
                            TurnSubmissionMode::Queue => {
                                let _ = events.send(ThreadEvent::Signal {
                                    thread_id,
                                    signal: ThreadSignal::user_queue(coordinates, turn_id.clone()),
                                });
                                pending_submits.push_back(ThreadCommand::Submit {
                                    turn_id,
                                    input,
                                    mode,
                                });
                            }
                            TurnSubmissionMode::Steer => {
                                emit_runtime_event(
                                    events,
                                    coordinates,
                                    RuntimeEventKind::PolicyRejected {
                                        code: "active_turn_not_steerable".to_string(),
                                        message: "Codex CLI runtime does not support same-turn steering".to_string(),
                                    },
                                );
                            }
                            TurnSubmissionMode::Interrupt => {
                                let reason = format!("interrupted by turn {turn_id}");
                                let _ = status.send(ThreadStatus::Cancelling);
                                let _ = events.send(ThreadEvent::Signal {
                                    thread_id,
                                    signal: ThreadSignal::user_interrupt(coordinates, turn_id.clone()),
                                });
                                emit_runtime_event(
                                    events,
                                    coordinates,
                                    RuntimeEventKind::Cancelled {
                                        reason: reason.clone(),
                                    },
                                );
                                turn_cancellation.cancel();
                                cancelled_reason = Some(reason);
                                pending_submits.push_front(ThreadCommand::Submit {
                                    turn_id,
                                    input,
                                    mode: TurnSubmissionMode::Queue,
                                });
                            }
                        }
                    }
                    Some(ThreadCommand::Compact { .. }) => {
                        emit_runtime_event(
                            events,
                            coordinates,
                            RuntimeEventKind::PolicyRejected {
                                code: "compact_unsupported".to_string(),
                                message: "Codex CLI runtime does not support Cooldis compaction commands".to_string(),
                            },
                        );
                    }
                    Some(ThreadCommand::ResumeToolCall { .. }) => {
                        emit_runtime_event(
                            events,
                            coordinates,
                            RuntimeEventKind::PolicyRejected {
                                code: "tool_resume_unsupported".to_string(),
                                message: "Codex CLI runtime does not support provider tool-call resume".to_string(),
                            },
                        );
                    }
                }
            }
        }
    };

    if let Some(reason) = cancelled_reason {
        let _ = status.send(ThreadStatus::Idle);
        emit_runtime_event(
            events,
            coordinates,
            RuntimeEventKind::Terminal {
                state: RuntimeTerminalState::Cancelled,
            },
        );
        let _ = events.send(ThreadEvent::Cancelled { thread_id, reason });
    } else {
        match result {
            Ok(text) => {
                if let Ok(entry) = mirror_codex_output(
                    services,
                    coordinates,
                    text.clone(),
                    config.model.as_deref(),
                )
                .await
                {
                    let _ = events.send(ThreadEvent::CanonicalMirror { thread_id, entry });
                }
                emit_runtime_event(
                    events,
                    coordinates,
                    RuntimeEventKind::TextDelta { text: text.clone() },
                );
                let _ = events.send(ThreadEvent::Output { thread_id, text });
            }
            Err(err) => {
                let _ = status.send(ThreadStatus::Failed);
                let _ = events.send(ThreadEvent::Signal {
                    thread_id,
                    signal: ThreadSignal::failed(coordinates, err.to_string()),
                });
                emit_runtime_event(
                    events,
                    coordinates,
                    RuntimeEventKind::Failed {
                        code: "runtime_execution".to_string(),
                        message: err.to_string(),
                    },
                );
                let _ = events.send(ThreadEvent::Failed {
                    thread_id,
                    message: err.to_string(),
                });
                failed = true;
            }
        }
    }

    if shutdown_after_turn {
        emit_runtime_event(
            events,
            coordinates,
            RuntimeEventKind::Terminal {
                state: RuntimeTerminalState::Stopped,
            },
        );
        let _ = status.send(ThreadStatus::Stopped);
        let _ = events.send(ThreadEvent::Stopped { thread_id });
    } else if !failed {
        let _ = status.send(ThreadStatus::Idle);
    }
    shutdown_after_turn || failed
}

async fn run_codex_exec(
    config: &CodexRuntimeConfig,
    input: String,
    cancellation: &CancellationToken,
) -> CooldisResult<String> {
    let mut command = Command::new(&config.codex_bin);
    command
        .arg("exec")
        .arg("--json")
        .arg("--skip-git-repo-check")
        .arg("-C")
        .arg(&config.cwd)
        .env("CODEX_HOME", &config.codex_home)
        .env("CODEX_SQLITE_HOME", &config.sqlite_home)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command.kill_on_drop(true);

    if let Some(model) = config.model.as_deref() {
        command.arg("--model").arg(model);
    }
    if let Some(sandbox) = config.sandbox.as_deref() {
        command.arg("--sandbox").arg(sandbox);
    }
    for arg in &config.extra_args {
        command.arg(arg);
    }
    command.arg("-");

    let mut child = command.spawn().map_err(|err| {
        CooldisError::RuntimeExecution(format!(
            "failed to spawn {}: {err}",
            config.codex_bin.display()
        ))
    })?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(input.as_bytes()).await.map_err(|err| {
            CooldisError::RuntimeExecution(format!("failed to write stdin: {err}"))
        })?;
    }

    let output = tokio::select! {
        output = child.wait_with_output() => output.map_err(|err| {
            CooldisError::RuntimeExecution(format!("failed to wait for codex exec: {err}"))
        })?,
        _ = cancellation.cancelled() => {
            return Err(CooldisError::RuntimeExecution("codex exec cancelled".to_string()));
        }
    };

    if output.status.success() {
        return String::from_utf8(output.stdout).map_err(|err| {
            CooldisError::RuntimeExecution(format!("codex exec stdout was not utf8: {err}"))
        });
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    Err(CooldisError::RuntimeExecution(format!(
        "codex exec exited with status {}: {}",
        output.status,
        stderr.trim()
    )))
}

async fn mirror_codex_output(
    services: &RuntimeServices,
    coordinates: &ThreadCoordinates,
    text: String,
    model: Option<&str>,
) -> CooldisResult<crate::SessionEntry> {
    services
        .append_session_entry(
            coordinates,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::assistant(
                    "codex",
                    ProviderApi::Other("codex_native".to_string()),
                    model.unwrap_or("codex-native"),
                    vec![CanonicalContent::text(text)],
                    CanonicalStopReason::EndTurn,
                ),
            },
        )
        .await
}

#[cfg(test)]
mod tests;
