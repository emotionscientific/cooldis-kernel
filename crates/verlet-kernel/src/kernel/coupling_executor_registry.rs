use super::stdlib_couplings::StdlibCouplingExecutor;
use super::wasm_couplings::WasmCouplingExecutor;
use crate::{
    CouplingExecutionResult, CouplingExecutor, CouplingInvocation, VerletError, VerletResult,
};
use async_trait::async_trait;
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegisteredCouplingExecutorKind {
    Stdlib,
    Wasm,
}

pub(crate) fn registered_coupling_executor_for_id(
    id: &str,
) -> Option<RegisteredCouplingExecutorKind> {
    if StdlibCouplingExecutor::supports_template(id) {
        Some(RegisteredCouplingExecutorKind::Stdlib)
    } else if WasmCouplingExecutor::supports_coupling_id(id) {
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
    wasm: Option<WasmCouplingExecutor>,
}

impl CouplingExecutorRegistry {
    pub(crate) fn new(operation_registry_root: Option<PathBuf>) -> Self {
        Self {
            wasm: operation_registry_root.map(WasmCouplingExecutor::new),
        }
    }
}

#[async_trait]
impl CouplingExecutor for CouplingExecutorRegistry {
    async fn invoke(&self, request: CouplingInvocation) -> VerletResult<CouplingExecutionResult> {
        match registered_coupling_executor_for_id(&request.coupling.id) {
            Some(RegisteredCouplingExecutorKind::Stdlib) => {
                StdlibCouplingExecutor.invoke(request).await
            }
            Some(RegisteredCouplingExecutorKind::Wasm) => {
                let Some(executor) = &self.wasm else {
                    return Err(VerletError::RuntimeFactory(format!(
                        "wasm coupling {:?} requires an operation registry root",
                        request.coupling.id
                    )));
                };
                executor.invoke(request).await
            }
            None => Err(VerletError::RuntimeFactory(format!(
                "no registered executor for coupling id {:?}",
                request.coupling.id
            ))),
        }
    }
}
