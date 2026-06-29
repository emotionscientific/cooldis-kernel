use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct WasmOperationManifest {
    pub abi: String,
    pub operations: Vec<WasmOperationDefinition>,
}

impl WasmOperationManifest {
    pub fn operation(&self, name: &str) -> Option<&WasmOperationDefinition> {
        self.operations
            .iter()
            .find(|operation| operation.name == name)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct WasmOperationDefinition {
    pub id: u32,
    pub name: String,
    #[serde(default)]
    pub input: WasmOperationValueKind,
    #[serde(default)]
    pub output: WasmOperationValueKind,
    #[serde(default)]
    pub events: WasmOperationEventKind,
    #[serde(default)]
    pub mode: WasmOperationMode,
    #[serde(default)]
    pub required_capabilities: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WasmOperationValueKind {
    #[default]
    Bytes,
    Text,
    Json,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WasmOperationEventKind {
    #[default]
    None,
    Jsonl,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WasmOperationMode {
    #[default]
    Sync,
    Async,
    Streaming,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Principal {
    pub kind: PrincipalKind,
    pub id: String,
}

impl Principal {
    pub fn new(kind: PrincipalKind, id: impl Into<String>) -> Self {
        Self {
            kind,
            id: id.into(),
        }
    }

    pub fn user(id: impl Into<String>) -> Self {
        Self::new(PrincipalKind::User, id)
    }

    pub fn agent(id: impl Into<String>) -> Self {
        Self::new(PrincipalKind::Agent, id)
    }

    pub fn product_api(id: impl Into<String>) -> Self {
        Self::new(PrincipalKind::ProductApi, id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    User,
    Agent,
    Scheduler,
    ProductApi,
    System,
    Provisioner,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ExecutionPrincipal {
    Anonymous,
    Caller,
    Principal { principal: Principal },
    System { name: String },
    Provisioner { name: String },
}

impl Default for ExecutionPrincipal {
    fn default() -> Self {
        Self::Anonymous
    }
}

impl ExecutionPrincipal {
    pub fn principal(principal: Principal) -> Self {
        Self::Principal { principal }
    }

    pub fn system(name: impl Into<String>) -> Self {
        Self::System { name: name.into() }
    }

    pub fn provisioner(name: impl Into<String>) -> Self {
        Self::Provisioner { name: name.into() }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AbiCapabilityGrant {
    pub capability: String,
}

impl AbiCapabilityGrant {
    pub fn new(capability: impl Into<String>) -> Self {
        Self {
            capability: capability.into(),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.capability
    }
}

impl From<&str> for AbiCapabilityGrant {
    fn from(capability: &str) -> Self {
        Self::new(capability)
    }
}

impl From<String> for AbiCapabilityGrant {
    fn from(capability: String) -> Self {
        Self::new(capability)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum AttachmentIdentity {
    Secret { name: String },
    ServiceAccount { name: String },
    ScopedResource { name: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachmentBinding {
    pub handle: String,
    pub capability: AbiCapabilityGrant,
    pub identity: AttachmentIdentity,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

impl AttachmentBinding {
    pub fn new(
        handle: impl Into<String>,
        capability: impl Into<AbiCapabilityGrant>,
        identity: AttachmentIdentity,
    ) -> Self {
        Self {
            handle: handle.into(),
            capability: capability.into(),
            identity,
            metadata: BTreeMap::new(),
        }
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvocationContext {
    pub caller: Option<Principal>,
    pub execution: ExecutionPrincipal,
    #[serde(default)]
    pub grants: Vec<AbiCapabilityGrant>,
    #[serde(default)]
    pub attachment_bindings: Vec<AttachmentBinding>,
    #[serde(default)]
    pub audit_metadata: BTreeMap<String, String>,
}

impl InvocationContext {
    pub fn anonymous() -> Self {
        Self::default()
    }

    pub fn new(execution: ExecutionPrincipal) -> Self {
        Self {
            execution,
            ..Self::default()
        }
    }

    pub fn with_caller(mut self, caller: Principal) -> Self {
        self.caller = Some(caller);
        self
    }

    pub fn with_grant(mut self, grant: impl Into<AbiCapabilityGrant>) -> Self {
        self.grants.push(grant.into());
        self
    }

    pub fn with_attachment_binding(mut self, binding: AttachmentBinding) -> Self {
        self.attachment_bindings.push(binding);
        self
    }

    pub fn with_audit_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.audit_metadata.insert(key.into(), value.into());
        self
    }

    pub fn grant_set(&self) -> BTreeSet<String> {
        self.grants
            .iter()
            .map(|grant| grant.capability.clone())
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbiOperationContract {
    pub registered_name: String,
    pub operation_name: String,
    pub source_ports: Vec<AbiSourcePort>,
    pub sink_ports: Vec<AbiSinkPort>,
    pub effect_ports: Vec<AbiEffectPort>,
    pub event_ports: Vec<AbiEventPort>,
    pub required_capabilities: Vec<String>,
}

impl AbiOperationContract {
    pub fn from_operation(
        registered_name: impl Into<String>,
        operation: &WasmOperationDefinition,
    ) -> Self {
        let registered_name = registered_name.into();
        let operation_name = operation.name.clone();
        let source_ports = vec![AbiSourcePort {
            name: "input".to_string(),
            value: AbiPortValue::from(operation.input.clone()),
            binding: AbiSourceBinding::InvocationInput,
            required: true,
        }];
        let sink_ports = vec![AbiSinkPort {
            name: "output".to_string(),
            value: AbiPortValue::from(operation.output.clone()),
            binding: AbiSinkBinding::InvocationOutput,
            required: true,
        }];
        let event_ports = match operation.events {
            WasmOperationEventKind::None => Vec::new(),
            WasmOperationEventKind::Jsonl => vec![AbiEventPort {
                name: "events".to_string(),
                value: AbiEventValue::Jsonl,
                binding: AbiEventBinding::InvocationEvents,
            }],
        };
        Self {
            registered_name,
            operation_name,
            source_ports,
            sink_ports,
            effect_ports: Vec::new(),
            event_ports,
            required_capabilities: operation.required_capabilities.clone(),
        }
    }

    pub fn main_output(&self) -> Option<&AbiSinkPort> {
        self.sink_ports.iter().find(|port| port.name == "output")
    }

    pub fn main_input(&self) -> Option<&AbiSourcePort> {
        self.source_ports.iter().find(|port| port.name == "input")
    }

    pub fn output_can_feed(&self, consumer: &AbiOperationContract) -> bool {
        match (self.main_output(), consumer.main_input()) {
            (Some(output), Some(input)) => output.is_compatible_with(input),
            _ => false,
        }
    }

    pub fn has_hidden_durable_sink(&self) -> bool {
        self.sink_ports
            .iter()
            .any(|port| matches!(port.binding, AbiSinkBinding::DurableArtifact { .. }))
            || self
                .event_ports
                .iter()
                .any(|port| matches!(port.binding, AbiEventBinding::DurableLog { .. }))
    }

    pub fn allows_effect_claim(&self, claim: &AbiEffectClaim) -> bool {
        self.effect_ports.iter().any(|port| {
            port.name == claim.effect_port
                && port.kind == claim.kind
                && port.binding.allows_claim(&claim.binding)
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbiSourcePort {
    pub name: String,
    pub value: AbiPortValue,
    pub binding: AbiSourceBinding,
    pub required: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbiSinkPort {
    pub name: String,
    pub value: AbiPortValue,
    pub binding: AbiSinkBinding,
    pub required: bool,
}

impl AbiSinkPort {
    pub fn is_compatible_with(&self, source: &AbiSourcePort) -> bool {
        self.value == source.value
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbiEffectPort {
    pub name: String,
    pub kind: AbiEffectKind,
    pub binding: AbiEffectBinding,
    pub required: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbiEventPort {
    pub name: String,
    pub value: AbiEventValue,
    pub binding: AbiEventBinding,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum AbiPortValue {
    Bytes,
    Text,
    Json,
    VfsFile { media_type: Option<String> },
    VfsDirectory,
    Artifact { media_type: Option<String> },
}

impl From<WasmOperationValueKind> for AbiPortValue {
    fn from(value: WasmOperationValueKind) -> Self {
        match value {
            WasmOperationValueKind::Bytes => Self::Bytes,
            WasmOperationValueKind::Text => Self::Text,
            WasmOperationValueKind::Json => Self::Json,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum AbiSourceBinding {
    InvocationInput,
    VfsRead { path: Option<String> },
    HostAllocatedArtifact,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum AbiSinkBinding {
    InvocationOutput,
    DurableArtifact { path: Option<String> },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum AbiEffectKind {
    VfsWrite { mode: AbiVfsWriteMode },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AbiVfsWriteMode {
    WriteNew,
    Replace,
    Append,
    Scratch,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum AbiEffectBinding {
    CallerBoundPath { path: Option<String> },
    HostAllocatedPath,
    OperationSelectedPath { scope: String },
}

impl AbiEffectBinding {
    fn allows_claim(&self, claim: &AbiEffectBinding) -> bool {
        match (self, claim) {
            (
                AbiEffectBinding::CallerBoundPath { path: expected },
                AbiEffectBinding::CallerBoundPath { path: actual },
            ) => match (expected, actual) {
                (Some(expected), Some(actual)) => expected == actual,
                (Some(_), None) => false,
                (None, Some(_)) => true,
                (None, None) => false,
            },
            (AbiEffectBinding::HostAllocatedPath, AbiEffectBinding::HostAllocatedPath) => true,
            (
                AbiEffectBinding::OperationSelectedPath { scope: expected },
                AbiEffectBinding::OperationSelectedPath { scope: actual },
            ) => expected == actual,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AbiEventValue {
    Jsonl,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum AbiEventBinding {
    InvocationEvents,
    DurableLog { path: Option<String> },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbiEffectReceipt {
    pub effect_port: String,
    pub kind: AbiEffectReceiptKind,
    pub invocation_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbiEffectClaim {
    pub effect_port: String,
    pub kind: AbiEffectKind,
    pub binding: AbiEffectBinding,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum AbiEffectReceiptKind {
    VfsWrite {
        path: String,
        bytes: Option<u64>,
        sha256: Option<String>,
        media_type: Option<String>,
    },
}

#[cfg(test)]
mod tests;
