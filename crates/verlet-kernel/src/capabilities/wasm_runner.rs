pub use verlet_wasm::{
    DEFAULT_ENTRYPOINT, DEFAULT_FUEL, DEFAULT_FUEL_YIELD_INTERVAL, DEFAULT_MAX_INPUT_BYTES,
    DEFAULT_MAX_OUTPUT_BYTES, DEFAULT_MEMORY_LIMIT_BYTES, DEFAULT_OPERATION_NAME, WasmHttpRequest,
    WasmHttpResponse, WasmRuntimeArtifact, WasmRuntimeConfig,
};

#[cfg(test)]
pub(crate) use crate::{InvocationContext, VerletVfs};
#[cfg(test)]
pub(crate) use bashkit::FileSystem;
#[cfg(test)]
pub(crate) use std::collections::{BTreeMap, BTreeSet};
#[cfg(test)]
pub(crate) use verlet_wasm::{
    FS_MODE_READ, HTTP_ABI, OPERATION_ABI, STATUS_CAPABILITY_DENIED, STATUS_EOF,
    STATUS_INVALID_ARGUMENT, STATUS_NOT_FOUND, ensure_http_capability, execute_http_request,
    http_origin,
};

pub struct WasmRuntimeFactory {
    core: std::sync::Arc<verlet_wasm::WasmRuntimeFactory>,
}

impl WasmRuntimeFactory {
    pub fn new(config: WasmRuntimeConfig) -> crate::VerletResult<Self> {
        Ok(Self {
            core: std::sync::Arc::new(verlet_wasm::WasmRuntimeFactory::new(config)?),
        })
    }

    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> crate::VerletResult<Self> {
        Ok(Self {
            core: std::sync::Arc::new(verlet_wasm::WasmRuntimeFactory::from_bytes(bytes)?),
        })
    }

    pub async fn describe(&self) -> crate::VerletResult<Option<verlet_abi::WasmOperationManifest>> {
        Ok(self.core.describe().await?)
    }

    pub async fn validate_operation_artifact(
        &self,
    ) -> crate::VerletResult<verlet_abi::WasmOperationManifest> {
        Ok(self.core.validate_operation_artifact().await?)
    }

    pub async fn invoke_operation_bytes(
        &self,
        operation_name: &str,
        input: impl Into<Vec<u8>>,
    ) -> crate::VerletResult<verlet_process::WasmOperationOutput> {
        Ok(self
            .core
            .invoke_operation_bytes(operation_name, input)
            .await?)
    }

    pub async fn invoke_operation_process(
        &self,
        operation_name: &str,
        input: impl Into<Vec<u8>>,
    ) -> crate::VerletResult<crate::VerletProcessHandle> {
        Ok(self
            .core
            .invoke_operation_process(operation_name, input)
            .await?)
    }
}

#[async_trait::async_trait]
impl crate::AgentRuntimeFactory for WasmRuntimeFactory {
    async fn build(
        &self,
        _context: &crate::ThreadContext,
    ) -> crate::VerletResult<Box<dyn crate::AgentRuntime>> {
        Ok(Box::new(WasmRuntime {
            runtime: self.core.build_runtime().await?,
        }))
    }
}

struct WasmRuntime {
    runtime: verlet_wasm::WasmModuleRuntime,
}

impl WasmRuntime {
    async fn execute_turn(&self, input: String) -> crate::VerletResult<String> {
        Ok(self.runtime.execute_turn(input).await?)
    }
}

#[async_trait::async_trait]
impl crate::AgentRuntime for WasmRuntime {
    async fn run(
        self: Box<Self>,
        context: crate::ThreadContext,
        services: crate::RuntimeServices,
        mut commands: tokio::sync::mpsc::Receiver<crate::ThreadCommand>,
        events: tokio::sync::broadcast::Sender<crate::ThreadEvent>,
        status: tokio::sync::watch::Sender<crate::ThreadStatus>,
        cancellation: tokio_util::sync::CancellationToken,
    ) {
        let thread_id = context.coordinates.thread_id;
        let coordinates = context.coordinates.clone();
        crate::emit_runtime_event(
            &events,
            &coordinates,
            crate::RuntimeEventKind::ThreadStarted {
                parent_thread_id: context.parent_thread_id,
                topology: context.topology.clone(),
                metadata: context.metadata.clone(),
            },
        );
        let _ = events.send(crate::ThreadEvent::Started { context });
        let _ = status.send(crate::ThreadStatus::Idle);
        let mut pending_submits = std::collections::VecDeque::new();

        loop {
            if let Some(crate::ThreadCommand::Submit { turn_id, input, .. }) =
                pending_submits.pop_front()
            {
                if run_wasm_turn(
                    &self,
                    &services,
                    &coordinates,
                    thread_id,
                    turn_id,
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
                        crate::ThreadCommand::Submit { turn_id, input, mode } => {
                            if mode == crate::TurnSubmissionMode::Steer {
                                crate::emit_runtime_event(
                                    &events,
                                    &coordinates,
                                    crate::RuntimeEventKind::PolicyRejected {
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
                                turn_id,
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
                        crate::ThreadCommand::Cancel { reason } => {
                            let _ = status.send(crate::ThreadStatus::Cancelling);
                            let _ = events.send(crate::ThreadEvent::Signal {
                                thread_id,
                                signal: crate::ThreadSignal::interrupt_cancel(&coordinates, reason.clone()),
                            });
                            crate::emit_runtime_event(
                                &events,
                                &coordinates,
                                crate::RuntimeEventKind::Cancelled {
                                    reason: reason.clone(),
                                },
                            );
                            let _ = events.send(crate::ThreadEvent::Cancelled { thread_id, reason });
                            let _ = status.send(crate::ThreadStatus::Idle);
                        }
                        crate::ThreadCommand::CancelTurn { .. } => {}
                        crate::ThreadCommand::Compact { .. } => {
                            crate::emit_runtime_event(
                                &events,
                                &coordinates,
                                crate::RuntimeEventKind::PolicyRejected {
                                    code: "compact_unsupported".to_string(),
                                    message: "Wasm runtime does not support Verlet compaction commands".to_string(),
                                },
                            );
                            let _ = status.send(crate::ThreadStatus::Idle);
                        }
                        crate::ThreadCommand::ResumeToolCall { .. } => {
                            crate::emit_runtime_event(
                                &events,
                                &coordinates,
                                crate::RuntimeEventKind::PolicyRejected {
                                    code: "tool_resume_unsupported".to_string(),
                                    message: "Wasm runtime does not support provider tool-call resume".to_string(),
                                },
                            );
                            let _ = status.send(crate::ThreadStatus::Idle);
                        }
                        crate::ThreadCommand::Shutdown => {
                            let _ = events.send(crate::ThreadEvent::Signal {
                                thread_id,
                                signal: crate::ThreadSignal::shutdown(&coordinates),
                            });
                            crate::emit_runtime_event(
                                &events,
                                &coordinates,
                                crate::RuntimeEventKind::Terminal {
                                    state: crate::RuntimeTerminalState::Stopped,
                                },
                            );
                            break;
                        }
                    }
                }
            }
        }

        crate::emit_runtime_event(
            &events,
            &coordinates,
            crate::RuntimeEventKind::Terminal {
                state: crate::RuntimeTerminalState::Stopped,
            },
        );
        let _ = status.send(crate::ThreadStatus::Stopped);
        let _ = events.send(crate::ThreadEvent::Stopped { thread_id });
    }
}

async fn run_wasm_turn(
    runtime: &WasmRuntime,
    services: &crate::RuntimeServices,
    coordinates: &crate::ThreadCoordinates,
    thread_id: crate::ThreadId,
    turn_id: String,
    input: crate::TurnInput,
    events: &tokio::sync::broadcast::Sender<crate::ThreadEvent>,
    status: &tokio::sync::watch::Sender<crate::ThreadStatus>,
    commands: &mut tokio::sync::mpsc::Receiver<crate::ThreadCommand>,
    cancellation: &tokio_util::sync::CancellationToken,
    pending_submits: &mut std::collections::VecDeque<crate::ThreadCommand>,
) -> bool {
    let _ = status.send(crate::ThreadStatus::Running);
    match services
        .append_user_turn_input(coordinates, &turn_id, &input)
        .await
    {
        Ok(entry) => {
            let _ = events.send(crate::ThreadEvent::CanonicalMirror { thread_id, entry });
        }
        Err(err) => {
            let _ = status.send(crate::ThreadStatus::Failed);
            let _ = events.send(crate::ThreadEvent::Failed {
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
                    Some(crate::ThreadCommand::Cancel { reason }) => {
                        let _ = status.send(crate::ThreadStatus::Cancelling);
                        let _ = events.send(crate::ThreadEvent::Signal {
                            thread_id,
                            signal: crate::ThreadSignal::interrupt_cancel(coordinates, reason.clone()),
                        });
                        crate::emit_runtime_event(
                            events,
                            coordinates,
                            crate::RuntimeEventKind::Cancelled {
                                reason: reason.clone(),
                            },
                        );
                        cancelled_reason = Some(reason);
                        break None;
                    }
                    Some(crate::ThreadCommand::CancelTurn {
                        watchdog_token_id,
                        reason,
                    }) => {
                        if input.turn_watchdog_id() != Some(watchdog_token_id) {
                            continue;
                        }
                        let _ = status.send(crate::ThreadStatus::Cancelling);
                        let _ = events.send(crate::ThreadEvent::Signal {
                            thread_id,
                            signal: crate::ThreadSignal::interrupt_cancel(coordinates, reason.clone()),
                        });
                        crate::emit_runtime_event(
                            events,
                            coordinates,
                            crate::RuntimeEventKind::Cancelled {
                                reason: reason.clone(),
                            },
                        );
                        cancelled_reason = Some(reason);
                        break None;
                    }
                    Some(crate::ThreadCommand::Shutdown) => {
                        let _ = events.send(crate::ThreadEvent::Signal {
                            thread_id,
                            signal: crate::ThreadSignal::shutdown(coordinates),
                        });
                        crate::emit_runtime_event(
                            events,
                            coordinates,
                            crate::RuntimeEventKind::Terminal {
                                state: crate::RuntimeTerminalState::Stopped,
                            },
                        );
                        shutdown_after_turn = true;
                        break None;
                    }
                    Some(crate::ThreadCommand::Submit { turn_id, input, mode }) => {
                        match mode {
                            crate::TurnSubmissionMode::Queue => {
                                let _ = events.send(crate::ThreadEvent::Signal {
                                    thread_id,
                                    signal: crate::ThreadSignal::user_queue(coordinates, turn_id.clone()),
                                });
                                pending_submits.push_back(crate::ThreadCommand::Submit {
                                    turn_id,
                                    input,
                                    mode,
                                });
                            }
                            crate::TurnSubmissionMode::Steer => {
                                crate::emit_runtime_event(
                                    events,
                                    coordinates,
                                    crate::RuntimeEventKind::PolicyRejected {
                                        code: "active_turn_not_steerable".to_string(),
                                        message: "Wasm runtime does not support same-turn steering".to_string(),
                                    },
                                );
                            }
                            crate::TurnSubmissionMode::Interrupt => {
                                let reason = format!("interrupted by turn {turn_id}");
                                let _ = status.send(crate::ThreadStatus::Cancelling);
                                let _ = events.send(crate::ThreadEvent::Signal {
                                    thread_id,
                                    signal: crate::ThreadSignal::user_interrupt(coordinates, turn_id.clone()),
                                });
                                crate::emit_runtime_event(
                                    events,
                                    coordinates,
                                    crate::RuntimeEventKind::Cancelled {
                                        reason: reason.clone(),
                                    },
                                );
                                cancelled_reason = Some(reason);
                                pending_submits.push_front(crate::ThreadCommand::Submit {
                                    turn_id,
                                    input,
                                    mode: crate::TurnSubmissionMode::Queue,
                                });
                                break None;
                            }
                        }
                    }
                    Some(crate::ThreadCommand::Compact { .. }) => {
                        crate::emit_runtime_event(
                            events,
                            coordinates,
                            crate::RuntimeEventKind::PolicyRejected {
                                code: "compact_unsupported".to_string(),
                                message: "Wasm runtime does not support Verlet compaction commands".to_string(),
                            },
                        );
                    }
                    Some(crate::ThreadCommand::ResumeToolCall { .. }) => {
                        crate::emit_runtime_event(
                            events,
                            coordinates,
                            crate::RuntimeEventKind::PolicyRejected {
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
        let _ = status.send(crate::ThreadStatus::Idle);
        crate::emit_runtime_event(
            events,
            coordinates,
            crate::RuntimeEventKind::Terminal {
                state: crate::RuntimeTerminalState::Cancelled,
            },
        );
        let _ = events.send(crate::ThreadEvent::Cancelled { thread_id, reason });
        return shutdown_after_turn;
    }

    if let Some(result) = result {
        match result {
            Ok(output) => {
                if !output.is_empty() {
                    crate::emit_runtime_event(
                        events,
                        coordinates,
                        crate::RuntimeEventKind::TextDelta {
                            text: output.clone(),
                        },
                    );
                    let _ = events.send(crate::ThreadEvent::Output {
                        thread_id,
                        text: output.clone(),
                    });
                    mirror_wasm_output(services, coordinates, thread_id, output, events).await;
                }
                crate::emit_runtime_event(
                    events,
                    coordinates,
                    crate::RuntimeEventKind::Terminal {
                        state: crate::RuntimeTerminalState::Completed,
                    },
                );
            }
            Err(err) => {
                let _ = status.send(crate::ThreadStatus::Failed);
                let _ = events.send(crate::ThreadEvent::Signal {
                    thread_id,
                    signal: crate::ThreadSignal::failed(coordinates, err.to_string()),
                });
                crate::emit_runtime_event(
                    events,
                    coordinates,
                    crate::RuntimeEventKind::Failed {
                        code: "wasm_runtime".to_string(),
                        message: err.to_string(),
                    },
                );
                let _ = events.send(crate::ThreadEvent::Failed {
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
        let _ = status.send(crate::ThreadStatus::Idle);
        false
    }
}

async fn mirror_wasm_output(
    services: &crate::RuntimeServices,
    coordinates: &crate::ThreadCoordinates,
    thread_id: crate::ThreadId,
    text: String,
    events: &tokio::sync::broadcast::Sender<crate::ThreadEvent>,
) {
    if let Ok(entry) = services
        .append_session_entry(
            coordinates,
            None,
            crate::SessionEntryKind::Message {
                message: crate::kernel::history::CanonicalMessage::assistant(
                    "verlet",
                    crate::kernel::history::ProviderApi::Other("wasm_runner".to_string()),
                    "wasmtime",
                    vec![crate::kernel::history::CanonicalContent::text(text)],
                    crate::kernel::history::CanonicalStopReason::EndTurn,
                ),
            },
        )
        .await
    {
        let _ = events.send(crate::ThreadEvent::CanonicalMirror { thread_id, entry });
    }
}

#[cfg(test)]
mod tests;
