#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegisteredCouplingExecutorKind {
    Stdlib,
    Wasm,
}

pub(crate) fn registered_coupling_executor_for_id(
    id: &str,
) -> Option<RegisteredCouplingExecutorKind> {
    if crate::kernel::stdlib_couplings::StdlibCouplingExecutor::supports_template(id) {
        Some(RegisteredCouplingExecutorKind::Stdlib)
    } else if crate::kernel::wasm_couplings::WasmCouplingExecutor::supports_coupling_id(id) {
        Some(RegisteredCouplingExecutorKind::Wasm)
    } else {
        None
    }
}

pub(crate) fn registered_coupling_executor_supports_template(id: &str) -> bool {
    registered_coupling_executor_for_id(id).is_some()
}

#[derive(Clone, Debug, Default)]
pub(crate) struct CouplingExecutorRegistry {
    wasm: Option<crate::kernel::wasm_couplings::WasmCouplingExecutor>,
}

impl CouplingExecutorRegistry {
    pub(crate) fn new(operation_registry_root: Option<std::path::PathBuf>) -> Self {
        Self {
            wasm: operation_registry_root
                .map(crate::kernel::wasm_couplings::WasmCouplingExecutor::new),
        }
    }
}

#[async_trait::async_trait]
impl crate::kernel::coupling_scheduler::CouplingExecutor for CouplingExecutorRegistry {
    async fn invoke(
        &self,
        request: crate::kernel::coupling_scheduler::CouplingInvocation,
    ) -> crate::kernel::runtime_host::VerletResult<
        crate::kernel::coupling_scheduler::CouplingExecutionResult,
    > {
        match registered_coupling_executor_for_id(&request.coupling.id) {
            Some(RegisteredCouplingExecutorKind::Stdlib) => {
                crate::kernel::stdlib_couplings::StdlibCouplingExecutor
                    .invoke(request)
                    .await
            }
            Some(RegisteredCouplingExecutorKind::Wasm) => {
                let Some(executor) = &self.wasm else {
                    return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                        format!(
                            "wasm coupling {:?} requires an operation registry root",
                            request.coupling.id
                        ),
                    ));
                };
                executor.invoke(request).await
            }
            None => Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "no registered executor for coupling id {:?}",
                    request.coupling.id
                ),
            )),
        }
    }
}
