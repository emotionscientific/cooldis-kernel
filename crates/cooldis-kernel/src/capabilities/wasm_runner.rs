use crate::kernel::history::{
    CanonicalContent, CanonicalMessage, CanonicalStopReason, ProviderApi,
};
use crate::{
    AgentRuntime, AgentRuntimeFactory, CooldisProcessHandle, CooldisResult, RuntimeEventKind,
    RuntimeServices, RuntimeTerminalState, SessionEntryKind, ThreadCommand, ThreadContext,
    ThreadEvent, ThreadSignal, ThreadStatus, TurnSubmissionMode, emit_runtime_event,
};
use async_trait::async_trait;
use cooldis_abi::WasmOperationManifest;
use cooldis_process::WasmOperationOutput;
use cooldis_wasm::{WasmModuleRuntime, WasmRuntimeFactory as CoreWasmRuntimeFactory};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

pub use cooldis_wasm::{
    DEFAULT_ENTRYPOINT, DEFAULT_FUEL, DEFAULT_FUEL_YIELD_INTERVAL, DEFAULT_MAX_INPUT_BYTES,
    DEFAULT_MAX_OUTPUT_BYTES, DEFAULT_MEMORY_LIMIT_BYTES, DEFAULT_OPERATION_NAME, WasmHttpRequest,
    WasmHttpResponse, WasmRuntimeArtifact, WasmRuntimeConfig,
};

#[cfg(test)]
pub(crate) use crate::{CooldisVfs, InvocationContext};
#[cfg(test)]
pub(crate) use bashkit::FileSystem;
#[cfg(test)]
pub(crate) use cooldis_wasm::{
    FS_MODE_READ, HTTP_ABI, OPERATION_ABI, STATUS_CAPABILITY_DENIED, STATUS_EOF,
    STATUS_INVALID_ARGUMENT, STATUS_NOT_FOUND, ensure_http_capability, execute_http_request,
    http_origin,
};
#[cfg(test)]
pub(crate) use std::collections::{BTreeMap, BTreeSet};

pub struct WasmRuntimeFactory {
    core: Arc<CoreWasmRuntimeFactory>,
}

impl WasmRuntimeFactory {
    pub fn new(config: WasmRuntimeConfig) -> CooldisResult<Self> {
        Ok(Self {
            core: Arc::new(CoreWasmRuntimeFactory::new(config)?),
        })
    }

    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> CooldisResult<Self> {
        Ok(Self {
            core: Arc::new(CoreWasmRuntimeFactory::from_bytes(bytes)?),
        })
    }

    pub async fn describe(&self) -> CooldisResult<Option<WasmOperationManifest>> {
        Ok(self.core.describe().await?)
    }

    pub async fn validate_operation_artifact(&self) -> CooldisResult<WasmOperationManifest> {
        Ok(self.core.validate_operation_artifact().await?)
    }

    pub async fn invoke_operation_bytes(
        &self,
        operation_name: &str,
        input: impl Into<Vec<u8>>,
    ) -> CooldisResult<WasmOperationOutput> {
        Ok(self
            .core
            .invoke_operation_bytes(operation_name, input)
            .await?)
    }

    pub async fn invoke_operation_process(
        &self,
        operation_name: &str,
        input: impl Into<Vec<u8>>,
    ) -> CooldisResult<CooldisProcessHandle> {
        Ok(self
            .core
            .invoke_operation_process(operation_name, input)
            .await?)
    }
}

#[async_trait]
impl AgentRuntimeFactory for WasmRuntimeFactory {
    async fn build(&self, _context: &ThreadContext) -> CooldisResult<Box<dyn AgentRuntime>> {
        Ok(Box::new(WasmRuntime {
            runtime: self.core.build_runtime().await?,
        }))
    }
}

struct WasmRuntime {
    runtime: WasmModuleRuntime,
}

impl WasmRuntime {
    async fn execute_turn(&self, input: String) -> CooldisResult<String> {
        Ok(self.runtime.execute_turn(input).await?)
    }
}

#[async_trait]
impl AgentRuntime for WasmRuntime {
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
                if run_wasm_turn(
                    &self,
                    &services,
                    &coordinates,
                    thread_id,
                    input,
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
                    break;
                }
                command = commands.recv() => {
                    let Some(command) = command else {
                        break;
                    };
                    match command {
                        ThreadCommand::Submit { input, mode, .. } => {
                            if mode == TurnSubmissionMode::Steer {
                                emit_runtime_event(
                                    &events,
                                    &coordinates,
                                    RuntimeEventKind::PolicyRejected {
                                        code: "no_active_turn".to_string(),
                                        message: "steer input requires an active Wasm turn".to_string(),
                                    },
                                );
                                continue;
                            }
                            if run_wasm_turn(
                                &self,
                                &services,
                                &coordinates,
                                thread_id,
                                input,
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
                        ThreadCommand::Cancel { reason } => {
                            let _ = status.send(ThreadStatus::Cancelling);
                            let _ = events.send(ThreadEvent::Signal {
                                thread_id,
                                signal: ThreadSignal::interrupt_cancel(&coordinates, reason.clone()),
                            });
                            emit_runtime_event(
                                &events,
                                &coordinates,
                                RuntimeEventKind::Cancelled {
                                    reason: reason.clone(),
                                },
                            );
                            let _ = events.send(ThreadEvent::Cancelled { thread_id, reason });
                            let _ = status.send(ThreadStatus::Idle);
                        }
                        ThreadCommand::Compact { .. } => {
                            emit_runtime_event(
                                &events,
                                &coordinates,
                                RuntimeEventKind::PolicyRejected {
                                    code: "compact_unsupported".to_string(),
                                    message: "Wasm runtime does not support Cooldis compaction commands".to_string(),
                                },
                            );
                            let _ = status.send(ThreadStatus::Idle);
                        }
                        ThreadCommand::ResumeToolCall { .. } => {
                            emit_runtime_event(
                                &events,
                                &coordinates,
                                RuntimeEventKind::PolicyRejected {
                                    code: "tool_resume_unsupported".to_string(),
                                    message: "Wasm runtime does not support provider tool-call resume".to_string(),
                                },
                            );
                            let _ = status.send(ThreadStatus::Idle);
                        }
                        ThreadCommand::Shutdown => {
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
                            break;
                        }
                    }
                }
            }
        }

        emit_runtime_event(
            &events,
            &coordinates,
            RuntimeEventKind::Terminal {
                state: RuntimeTerminalState::Stopped,
            },
        );
        let _ = status.send(ThreadStatus::Stopped);
        let _ = events.send(ThreadEvent::Stopped { thread_id });
    }
}

async fn run_wasm_turn(
    runtime: &WasmRuntime,
    services: &RuntimeServices,
    coordinates: &crate::ThreadCoordinates,
    thread_id: crate::ThreadId,
    input: crate::TurnInput,
    events: &broadcast::Sender<ThreadEvent>,
    status: &watch::Sender<ThreadStatus>,
    commands: &mut mpsc::Receiver<ThreadCommand>,
    cancellation: &CancellationToken,
    pending_submits: &mut VecDeque<ThreadCommand>,
) -> bool {
    let _ = status.send(ThreadStatus::Running);
    match services.append_user_turn_input(coordinates, &input).await {
        Ok(entry) => {
            let _ = events.send(ThreadEvent::CanonicalMirror { thread_id, entry });
        }
        Err(err) => {
            let _ = status.send(ThreadStatus::Failed);
            let _ = events.send(ThreadEvent::Failed {
                thread_id,
                message: err.to_string(),
            });
            return true;
        }
    }

    let execute = runtime.execute_turn(input.text_projection());
    tokio::pin!(execute);
    let mut cancelled_reason = None;
    let mut shutdown_after_turn = false;

    let result = loop {
        tokio::select! {
            result = &mut execute => break Some(result),
            _ = cancellation.cancelled() => {
                cancelled_reason = Some("runtime cancellation requested".to_string());
                break None;
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
                        cancelled_reason = Some(reason);
                        break None;
                    }
                    Some(ThreadCommand::Shutdown) => {
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
                        shutdown_after_turn = true;
                        break None;
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
                                        message: "Wasm runtime does not support same-turn steering".to_string(),
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
                                cancelled_reason = Some(reason);
                                pending_submits.push_front(ThreadCommand::Submit {
                                    turn_id,
                                    input,
                                    mode: TurnSubmissionMode::Queue,
                                });
                                break None;
                            }
                        }
                    }
                    Some(ThreadCommand::Compact { .. }) => {
                        emit_runtime_event(
                            events,
                            coordinates,
                            RuntimeEventKind::PolicyRejected {
                                code: "compact_unsupported".to_string(),
                                message: "Wasm runtime does not support Cooldis compaction commands".to_string(),
                            },
                        );
                    }
                    Some(ThreadCommand::ResumeToolCall { .. }) => {
                        emit_runtime_event(
                            events,
                            coordinates,
                            RuntimeEventKind::PolicyRejected {
                                code: "tool_resume_unsupported".to_string(),
                                message: "Wasm runtime does not support provider tool-call resume".to_string(),
                            },
                        );
                    }
                    None => {
                        shutdown_after_turn = true;
                        break None;
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
        return shutdown_after_turn;
    }

    if let Some(result) = result {
        match result {
            Ok(output) => {
                if !output.is_empty() {
                    emit_runtime_event(
                        events,
                        coordinates,
                        RuntimeEventKind::TextDelta {
                            text: output.clone(),
                        },
                    );
                    let _ = events.send(ThreadEvent::Output {
                        thread_id,
                        text: output.clone(),
                    });
                    mirror_wasm_output(services, coordinates, thread_id, output, events).await;
                }
                emit_runtime_event(
                    events,
                    coordinates,
                    RuntimeEventKind::Terminal {
                        state: RuntimeTerminalState::Completed,
                    },
                );
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
                        code: "wasm_runtime".to_string(),
                        message: err.to_string(),
                    },
                );
                let _ = events.send(ThreadEvent::Failed {
                    thread_id,
                    message: err.to_string(),
                });
                return true;
            }
        }
    }

    if shutdown_after_turn {
        true
    } else {
        let _ = status.send(ThreadStatus::Idle);
        false
    }
}

async fn mirror_wasm_output(
    services: &RuntimeServices,
    coordinates: &crate::ThreadCoordinates,
    thread_id: crate::ThreadId,
    text: String,
    events: &broadcast::Sender<ThreadEvent>,
) {
    if let Ok(entry) = services
        .append_session_entry(
            coordinates,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::assistant(
                    "cooldis",
                    ProviderApi::Other("wasm_runner".to_string()),
                    "wasmtime",
                    vec![CanonicalContent::text(text)],
                    CanonicalStopReason::EndTurn,
                ),
            },
        )
        .await
    {
        let _ = events.send(ThreadEvent::CanonicalMirror { thread_id, entry });
    }
}

#[cfg(test)]
mod tests;
