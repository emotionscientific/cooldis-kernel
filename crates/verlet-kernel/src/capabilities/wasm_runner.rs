pub struct WasmRuntimeFactory {
    core: std::sync::Arc<verlet_wasm::runner::WasmRuntimeFactory>,
}

pub(crate) fn attachment_config_from_legacy_grants(
    grants: &std::collections::BTreeSet<String>,
) -> verlet_wasm::WasmAttachmentConfig {
    let mut config = verlet_wasm::WasmAttachmentConfig::default();
    for grant in grants {
        if let Some(secret_name) = grant.strip_prefix("secret:") {
            config.allowed_secrets.insert(secret_name.to_string());
            continue;
        }
        let Some(rule) = grant.strip_prefix("net.http.private:") else {
            continue;
        };
        let (method, origin) = if rule == "*" {
            ("*", "*")
        } else if let Some(origin) = rule.strip_prefix("*:") {
            ("*", origin)
        } else if let Some((method, origin)) = rule.split_once(':') {
            if origin.starts_with("//") {
                ("*", rule)
            } else {
                (method, origin)
            }
        } else {
            ("*", rule)
        };
        config
            .allowed_private_network
            .entry(origin.to_string())
            .or_default()
            .insert(method.to_string());
    }
    config
}

impl WasmRuntimeFactory {
    pub fn new(
        config: verlet_wasm::WasmRuntimeConfig,
    ) -> crate::kernel::runtime_host::VerletResult<Self> {
        Ok(Self {
            core: std::sync::Arc::new(verlet_wasm::runner::WasmRuntimeFactory::new(config)?),
        })
    }

    pub fn from_bytes(
        bytes: impl Into<Vec<u8>>,
    ) -> crate::kernel::runtime_host::VerletResult<Self> {
        Ok(Self {
            core: std::sync::Arc::new(verlet_wasm::runner::WasmRuntimeFactory::from_bytes(bytes)?),
        })
    }

    pub async fn describe(
        &self,
    ) -> crate::kernel::runtime_host::VerletResult<Option<verlet_abi::WasmOperationManifest>> {
        Ok(self.core.describe().await?)
    }

    pub async fn validate_operation_artifact(
        &self,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_abi::WasmOperationManifest> {
        Ok(self.core.validate_operation_artifact().await?)
    }

    pub async fn invoke_operation_bytes(
        &self,
        operation_name: &str,
        input: impl Into<Vec<u8>>,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_process::process::WasmOperationOutput>
    {
        Ok(self
            .core
            .invoke_operation_bytes(operation_name, input)
            .await?)
    }

    pub async fn invoke_operation_process(
        &self,
        operation_name: &str,
        input: impl Into<Vec<u8>>,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_process::process::VerletProcessHandle>
    {
        Ok(self
            .core
            .invoke_operation_process(operation_name, input)
            .await?)
    }
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::runtime_api::AgentRuntimeFactory for WasmRuntimeFactory {
    async fn build(
        &self,
        _context: &verlet_runtime_contracts::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<
        Box<dyn crate::kernel::runtime_host::runtime_api::AgentRuntime>,
    > {
        Ok(Box::new(WasmRuntime {
            runtime: self.core.build_runtime().await?,
        }))
    }
}

struct WasmRuntime {
    runtime: verlet_wasm::runner::WasmModuleRuntime,
}

impl WasmRuntime {
    async fn execute_turn(
        &self,
        input: String,
    ) -> crate::kernel::runtime_host::VerletResult<String> {
        Ok(self.runtime.execute_turn(input).await?)
    }
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::runtime_api::AgentRuntime for WasmRuntime {
    async fn run(
        self: Box<Self>,
        context: verlet_runtime_contracts::ThreadContext,
        services: crate::kernel::runtime_host::runtime_services::RuntimeServices,
        mut commands: tokio::sync::mpsc::Receiver<
            crate::kernel::runtime_host::runtime_api::ThreadCommand,
        >,
        events: tokio::sync::broadcast::Sender<
            crate::kernel::runtime_host::runtime_api::ThreadEvent,
        >,
        status: tokio::sync::watch::Sender<verlet_runtime_contracts::ThreadStatus>,
        cancellation: tokio_util::sync::CancellationToken,
    ) {
        let thread_id = context.coordinates.thread_id;
        let coordinates = context.coordinates.clone();
        crate::kernel::runtime_host::runtime_events::emit_runtime_event(
            &events,
            &coordinates,
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ThreadStarted {
                parent_thread_id: context.parent_thread_id,
                topology: context.topology.clone(),
                metadata: context.metadata.clone(),
            },
        );
        let _ =
            events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Started { context });
        let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
        let mut pending_submits = std::collections::VecDeque::new();

        loop {
            if let Some(crate::kernel::runtime_host::runtime_api::ThreadCommand::Submit {
                turn_id,
                input,
                ..
            }) = pending_submits.pop_front()
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
                        crate::kernel::runtime_host::runtime_api::ThreadCommand::Submit { turn_id, input, mode } => {
                            if mode == verlet_runtime_contracts::TurnSubmissionMode::Steer {
                                crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                                    &events,
                                    &coordinates,
                                    crate::kernel::runtime_host::runtime_events::RuntimeEventKind::PolicyRejected {
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
                        crate::kernel::runtime_host::runtime_api::ThreadCommand::Cancel { reason } => {
                            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Cancelling);
                            let _ = events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Signal {
                                thread_id,
                                signal: verlet_runtime_contracts::ThreadSignal::interrupt_cancel(&coordinates, reason.clone()),
                            });
                            crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                                &events,
                                &coordinates,
                                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Cancelled {
                                    reason: reason.clone(),
                                },
                            );
                            let _ = events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Cancelled { thread_id, reason });
                            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
                        }
                        crate::kernel::runtime_host::runtime_api::ThreadCommand::CancelTurn { .. } => {}
                        crate::kernel::runtime_host::runtime_api::ThreadCommand::Compact { .. } => {
                            crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                                &events,
                                &coordinates,
                                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::PolicyRejected {
                                    code: "compact_unsupported".to_string(),
                                    message: "Wasm runtime does not support Verlet compaction commands".to_string(),
                                },
                            );
                            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
                        }
                        crate::kernel::runtime_host::runtime_api::ThreadCommand::ResumeToolCall { .. } => {
                            crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                                &events,
                                &coordinates,
                                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::PolicyRejected {
                                    code: "tool_resume_unsupported".to_string(),
                                    message: "Wasm runtime does not support provider tool-call resume".to_string(),
                                },
                            );
                            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
                        }
                        crate::kernel::runtime_host::runtime_api::ThreadCommand::Shutdown => {
                            let _ = events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Signal {
                                thread_id,
                                signal: verlet_runtime_contracts::ThreadSignal::shutdown(&coordinates),
                            });
                            crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                                &events,
                                &coordinates,
                                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
                                    state: verlet_runtime_contracts::RuntimeTerminalState::Stopped,
                                },
                            );
                            break;
                        }
                    }
                }
            }
        }

        crate::kernel::runtime_host::runtime_events::emit_runtime_event(
            &events,
            &coordinates,
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
                state: verlet_runtime_contracts::RuntimeTerminalState::Stopped,
            },
        );
        let _ = status.send(verlet_runtime_contracts::ThreadStatus::Stopped);
        let _ = events
            .send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Stopped { thread_id });
    }
}

async fn run_wasm_turn(
    runtime: &WasmRuntime,
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    thread_id: verlet_runtime_contracts::ThreadId,
    turn_id: String,
    input: crate::kernel::runtime_host::turn::TurnInput,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    status: &tokio::sync::watch::Sender<verlet_runtime_contracts::ThreadStatus>,
    commands: &mut tokio::sync::mpsc::Receiver<
        crate::kernel::runtime_host::runtime_api::ThreadCommand,
    >,
    cancellation: &tokio_util::sync::CancellationToken,
    pending_submits: &mut std::collections::VecDeque<
        crate::kernel::runtime_host::runtime_api::ThreadCommand,
    >,
) -> bool {
    let _ = status.send(verlet_runtime_contracts::ThreadStatus::Running);
    match services
        .append_user_turn_input(coordinates, &turn_id, &input)
        .await
    {
        Ok(entry) => {
            let _ = events.send(
                crate::kernel::runtime_host::runtime_api::ThreadEvent::CanonicalMirror {
                    thread_id,
                    entry,
                },
            );
        }
        Err(err) => {
            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Failed);
            let _ = events.send(
                crate::kernel::runtime_host::runtime_api::ThreadEvent::Failed {
                    thread_id,
                    message: err.to_string(),
                },
            );
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
                    Some(crate::kernel::runtime_host::runtime_api::ThreadCommand::Cancel { reason }) => {
                        let _ = status.send(verlet_runtime_contracts::ThreadStatus::Cancelling);
                        let _ = events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Signal {
                            thread_id,
                            signal: verlet_runtime_contracts::ThreadSignal::interrupt_cancel(coordinates, reason.clone()),
                        });
                        crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                            events,
                            coordinates,
                            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Cancelled {
                                reason: reason.clone(),
                            },
                        );
                        cancelled_reason = Some(reason);
                        break None;
                    }
                    Some(crate::kernel::runtime_host::runtime_api::ThreadCommand::CancelTurn {
                        watchdog_token_id,
                        reason,
                    }) => {
                        if input.turn_watchdog_id() != Some(watchdog_token_id) {
                            continue;
                        }
                        let _ = status.send(verlet_runtime_contracts::ThreadStatus::Cancelling);
                        let _ = events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Signal {
                            thread_id,
                            signal: verlet_runtime_contracts::ThreadSignal::interrupt_cancel(coordinates, reason.clone()),
                        });
                        crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                            events,
                            coordinates,
                            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Cancelled {
                                reason: reason.clone(),
                            },
                        );
                        cancelled_reason = Some(reason);
                        break None;
                    }
                    Some(crate::kernel::runtime_host::runtime_api::ThreadCommand::Shutdown) => {
                        let _ = events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Signal {
                            thread_id,
                            signal: verlet_runtime_contracts::ThreadSignal::shutdown(coordinates),
                        });
                        crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                            events,
                            coordinates,
                            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
                                state: verlet_runtime_contracts::RuntimeTerminalState::Stopped,
                            },
                        );
                        shutdown_after_turn = true;
                        break None;
                    }
                    Some(crate::kernel::runtime_host::runtime_api::ThreadCommand::Submit { turn_id, input, mode }) => {
                        match mode {
                            verlet_runtime_contracts::TurnSubmissionMode::Queue => {
                                let _ = events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Signal {
                                    thread_id,
                                    signal: verlet_runtime_contracts::ThreadSignal::user_queue(coordinates, turn_id.clone()),
                                });
                                pending_submits.push_back(crate::kernel::runtime_host::runtime_api::ThreadCommand::Submit {
                                    turn_id,
                                    input,
                                    mode,
                                });
                            }
                            verlet_runtime_contracts::TurnSubmissionMode::Steer => {
                                crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                                    events,
                                    coordinates,
                                    crate::kernel::runtime_host::runtime_events::RuntimeEventKind::PolicyRejected {
                                        code: "active_turn_not_steerable".to_string(),
                                        message: "Wasm runtime does not support same-turn steering".to_string(),
                                    },
                                );
                            }
                            verlet_runtime_contracts::TurnSubmissionMode::Interrupt => {
                                let reason = format!("interrupted by turn {turn_id}");
                                let _ = status.send(verlet_runtime_contracts::ThreadStatus::Cancelling);
                                let _ = events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Signal {
                                    thread_id,
                                    signal: verlet_runtime_contracts::ThreadSignal::user_interrupt(coordinates, turn_id.clone()),
                                });
                                crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                                    events,
                                    coordinates,
                                    crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Cancelled {
                                        reason: reason.clone(),
                                    },
                                );
                                cancelled_reason = Some(reason);
                                pending_submits.push_front(crate::kernel::runtime_host::runtime_api::ThreadCommand::Submit {
                                    turn_id,
                                    input,
                                    mode: verlet_runtime_contracts::TurnSubmissionMode::Queue,
                                });
                                break None;
                            }
                        }
                    }
                    Some(crate::kernel::runtime_host::runtime_api::ThreadCommand::Compact { .. }) => {
                        crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                            events,
                            coordinates,
                            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::PolicyRejected {
                                code: "compact_unsupported".to_string(),
                                message: "Wasm runtime does not support Verlet compaction commands".to_string(),
                            },
                        );
                    }
                    Some(crate::kernel::runtime_host::runtime_api::ThreadCommand::ResumeToolCall { .. }) => {
                        crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                            events,
                            coordinates,
                            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::PolicyRejected {
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
        let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
        crate::kernel::runtime_host::runtime_events::emit_runtime_event(
            events,
            coordinates,
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
                state: verlet_runtime_contracts::RuntimeTerminalState::Cancelled,
            },
        );
        let _ = events.send(
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Cancelled { thread_id, reason },
        );
        return shutdown_after_turn;
    }

    if let Some(result) = result {
        match result {
            Ok(output) => {
                if !output.is_empty() {
                    crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                        events,
                        coordinates,
                        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::TextDelta {
                            text: output.clone(),
                        },
                    );
                    let _ = events.send(
                        crate::kernel::runtime_host::runtime_api::ThreadEvent::Output {
                            thread_id,
                            text: output.clone(),
                        },
                    );
                    mirror_wasm_output(services, coordinates, thread_id, output, events).await;
                }
                crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                    events,
                    coordinates,
                    crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
                        state: verlet_runtime_contracts::RuntimeTerminalState::Completed,
                    },
                );
            }
            Err(err) => {
                let _ = status.send(verlet_runtime_contracts::ThreadStatus::Failed);
                let _ = events.send(
                    crate::kernel::runtime_host::runtime_api::ThreadEvent::Signal {
                        thread_id,
                        signal: verlet_runtime_contracts::ThreadSignal::failed(
                            coordinates,
                            err.to_string(),
                        ),
                    },
                );
                crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                    events,
                    coordinates,
                    crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Failed {
                        code: "wasm_runtime".to_string(),
                        message: err.to_string(),
                    },
                );
                let _ = events.send(
                    crate::kernel::runtime_host::runtime_api::ThreadEvent::Failed {
                        thread_id,
                        message: err.to_string(),
                    },
                );
                return true;
            }
        }
    }

    if shutdown_after_turn {
        true
    } else {
        let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
        false
    }
}

async fn mirror_wasm_output(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    thread_id: verlet_runtime_contracts::ThreadId,
    text: String,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
) {
    if let Ok(entry) = services
        .append_session_entry(
            coordinates,
            None,
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::assistant(
                    "verlet",
                    verlet_history::ProviderApi::Other("wasm_runner".to_string()),
                    "wasmtime",
                    vec![verlet_history::CanonicalContent::text(text)],
                    verlet_history::CanonicalStopReason::EndTurn,
                ),
            },
        )
        .await
    {
        let _ = events.send(
            crate::kernel::runtime_host::runtime_api::ThreadEvent::CanonicalMirror {
                thread_id,
                entry,
            },
        );
    }
}

#[cfg(test)]
mod tests;
