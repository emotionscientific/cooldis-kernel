pub mod blob_store;
pub mod import_package;
pub mod openapi_plan;
pub mod operation_registry;
pub mod operation_store;
pub mod skill_package;
pub mod tool_package;

pub use blob_store::*;
pub use import_package::*;
pub use openapi_plan::*;
pub use operation_registry::*;
pub use operation_store::*;
pub use skill_package::*;
pub use tool_package::*;

use cooldis_abi::{
    AbiOperationContract, WasmOperationDefinition, WasmOperationEventKind, WasmOperationManifest,
    WasmOperationMode, WasmOperationValueKind,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub type CooldisResult<T> = Result<T, CooldisOperationsError>;

#[derive(Debug, thiserror::Error)]
pub enum CooldisOperationsError {
    #[error("runtime factory failed: {0}")]
    RuntimeFactory(String),
    #[error("runtime execution failed: {0}")]
    RuntimeExecution(String),
}

impl From<cooldis_wasm::CooldisWasmError> for CooldisOperationsError {
    fn from(error: cooldis_wasm::CooldisWasmError) -> Self {
        match error {
            cooldis_wasm::CooldisWasmError::RuntimeFactory(message) => {
                Self::RuntimeFactory(message)
            }
            cooldis_wasm::CooldisWasmError::RuntimeExecution(message) => {
                Self::RuntimeExecution(message)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RegisteredOperation {
    pub name: String,
    pub manifest: WasmOperationManifest,
    pub capability_grants: BTreeSet<String>,
    pub metadata: BTreeMap<String, Value>,
}

impl RegisteredOperation {
    pub fn projections(&self) -> OperationProjectionSet {
        OperationProjectionSet::from_registered(self)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OperationProjectionSet {
    pub registered_name: String,
    pub operations: Vec<OperationProjection>,
}

impl OperationProjectionSet {
    pub fn from_registered(record: &RegisteredOperation) -> Self {
        Self {
            registered_name: record.name.clone(),
            operations: record
                .manifest
                .operations
                .iter()
                .map(|operation| OperationProjection::from_operation(&record.name, operation))
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OperationProjection {
    pub registered_name: String,
    pub operation_name: String,
    pub operation_id: u32,
    pub input: WasmOperationValueKind,
    pub output: WasmOperationValueKind,
    pub events: WasmOperationEventKind,
    pub mode: WasmOperationMode,
    pub cli: OperationCliProjection,
    pub process: OperationProcessProjection,
    pub http: OperationHttpProjection,
    pub llm_tool: OperationLlmToolProjection,
    pub mcp: OperationMcpProjection,
    pub abi: AbiOperationContract,
}

impl OperationProjection {
    fn from_operation(registered_name: &str, operation: &WasmOperationDefinition) -> Self {
        let tool_name = projection_tool_name(registered_name, &operation.name);
        Self {
            registered_name: registered_name.to_string(),
            operation_name: operation.name.clone(),
            operation_id: operation.id,
            input: operation.input.clone(),
            output: operation.output.clone(),
            events: operation.events.clone(),
            mode: operation.mode.clone(),
            cli: OperationCliProjection {
                command: format!("cooldis tool run {registered_name} {}", operation.name),
                stdin: operation.input.clone(),
                stdout: operation.output.clone(),
            },
            process: OperationProcessProjection {
                command: format!("cooldis run {registered_name} {}", operation.name),
                stdin: operation.input.clone(),
                stdout: operation.output.clone(),
                stderr: operation.events.clone(),
            },
            http: OperationHttpProjection {
                method: "POST".to_string(),
                path: format!("/operations/{registered_name}/{}", operation.name),
                request_body: operation.input.clone(),
                response_body: operation.output.clone(),
                event_stream: operation.events.clone(),
            },
            llm_tool: OperationLlmToolProjection {
                name: tool_name.clone(),
                input: operation.input.clone(),
                output: operation.output.clone(),
            },
            mcp: OperationMcpProjection {
                tool_name,
                input: operation.input.clone(),
                output: operation.output.clone(),
            },
            abi: AbiOperationContract::from_operation(registered_name, operation),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationCliProjection {
    pub command: String,
    pub stdin: WasmOperationValueKind,
    pub stdout: WasmOperationValueKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationProcessProjection {
    pub command: String,
    pub stdin: WasmOperationValueKind,
    pub stdout: WasmOperationValueKind,
    pub stderr: WasmOperationEventKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationHttpProjection {
    pub method: String,
    pub path: String,
    pub request_body: WasmOperationValueKind,
    pub response_body: WasmOperationValueKind,
    pub event_stream: WasmOperationEventKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationLlmToolProjection {
    pub name: String,
    pub input: WasmOperationValueKind,
    pub output: WasmOperationValueKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationMcpProjection {
    pub tool_name: String,
    pub input: WasmOperationValueKind,
    pub output: WasmOperationValueKind,
}

pub fn projection_tool_name(registered_name: &str, operation_name: &str) -> String {
    let registered = projection_tool_name_part(registered_name);
    let operation = projection_tool_name_part(operation_name);
    if registered == operation {
        operation
    } else {
        format!("{registered}_{operation}")
    }
}

fn projection_tool_name_part(raw: &str) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registered_operation_derives_all_projection_surfaces() {
        let operation = RegisteredOperation {
            name: "Example Search".to_string(),
            manifest: WasmOperationManifest {
                abi: "cooldis.operation/0.1".to_string(),
                operations: vec![WasmOperationDefinition {
                    id: 1,
                    name: "search".to_string(),
                    input: WasmOperationValueKind::Bytes,
                    output: WasmOperationValueKind::Bytes,
                    events: WasmOperationEventKind::None,
                    mode: WasmOperationMode::Sync,
                    required_capabilities: Vec::new(),
                }],
            },
            capability_grants: BTreeSet::new(),
            metadata: BTreeMap::new(),
        };

        let projections = operation.projections();

        assert_eq!(projections.registered_name, "Example Search");
        assert_eq!(projections.operations.len(), 1);
        let projection = &projections.operations[0];
        assert_eq!(
            projection.cli.command,
            "cooldis tool run Example Search search"
        );
        assert_eq!(
            projection.process.command,
            "cooldis run Example Search search"
        );
        assert_eq!(projection.process.stderr, WasmOperationEventKind::None);
        assert_eq!(projection.http.method, "POST");
        assert_eq!(projection.http.path, "/operations/Example Search/search");
        assert_eq!(projection.llm_tool.name, "example_search_search");
        assert_eq!(projection.mcp.tool_name, "example_search_search");
        assert_eq!(
            projection_tool_name("http-fetch", "http_fetch"),
            "http_fetch"
        );
        assert_eq!(
            projection_tool_name("document", "extract_text"),
            "document_extract_text"
        );
        assert_eq!(projection.input, WasmOperationValueKind::Bytes);
        assert_eq!(projection.output, WasmOperationValueKind::Bytes);
        assert_eq!(projection.abi.registered_name, "Example Search");
        assert_eq!(projection.abi.operation_name, "search");
        assert_eq!(projection.abi.source_ports[0].name, "input");
        assert_eq!(projection.abi.sink_ports[0].name, "output");
        assert!(!projection.abi.has_hidden_durable_sink());
    }
}
