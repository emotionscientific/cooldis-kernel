//! Kernel-native operation packages synthesized by the daemon at startup.
//!
//! These records are published into the same operation registry as Wasm tools,
//! but their artifact bytes are the canonical serialized interface contract and
//! execution is dispatched back into kernel code by record and operation name.

use super::operation_store::PublishInterfaceOperationRequest;
use super::tool_package::TOOL_MANUAL_SCHEMA_VERSION;
use crate::{
    CooldisError, CooldisResult, LocalOperationRegistry, PublishedOperationRecord,
    PublishedOperationSource, ToolCommandContract, ToolInterfaceContract, ToolManualExitStatus,
    ToolOperationInterface, ToolOperationManual, ToolPackageIdentity, ToolRuntimeContract,
    WasmOperationDefinition, WasmOperationEventKind, WasmOperationManifest, WasmOperationMode,
    WasmOperationValueKind, wasm_sha256,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

pub const COOLDIS_THREADS_PACKAGE: &str = "cooldis-threads";
pub const COOLDIS_SCHEDULE_PACKAGE: &str = "cooldis-schedule";
pub const COOLDIS_PROCESS_PACKAGE: &str = "cooldis-process";
pub const COOLDIS_NOTIFY_PACKAGE: &str = "cooldis-notify";
pub const THREAD_SPAWN_OPERATION: &str = "thread_spawn";
pub const THREAD_SUBMIT_OPERATION: &str = "thread_submit";
pub const THREAD_WAIT_OPERATION: &str = "thread_wait";
pub const THREAD_STATUS_OPERATION: &str = "thread_status";
pub const THREAD_CANCEL_OPERATION: &str = "thread_cancel";
pub const MANDATE_START_OPERATION: &str = "mandate_start";
pub const MANDATE_REVOKE_OPERATION: &str = "mandate_revoke";
pub const MANDATE_LIST_OPERATION: &str = "mandate_list";
pub const PROCESS_EXEC_OPERATION: &str = "process_exec";
pub const PROCESS_POLL_OPERATION: &str = "process_poll";
pub const PROCESS_WRITE_OPERATION: &str = "process_write";
pub const PROCESS_TERMINATE_OPERATION: &str = "process_terminate";
pub const NOTIFY_PREVIEW_OPERATION: &str = "notify_preview";
pub const CHANNEL_EMIT_OPERATION: &str = "channel_emit";
pub const THREADS_SPAWN_CAPABILITY: &str = "threads.spawn";
pub const THREADS_CONTROL_CAPABILITY: &str = "threads.control";
pub const THREADS_READ_CAPABILITY: &str = "threads.read";
pub const SCHEDULE_MANAGE_CAPABILITY: &str = "schedule.manage";
pub const SCHEDULE_READ_CAPABILITY: &str = "schedule.read";
pub const PROCESS_SPAWN_CAPABILITY: &str = "process.spawn";
pub const PROCESS_READ_CAPABILITY: &str = "process.read";
pub const PROCESS_WRITE_CAPABILITY: &str = "process.write";
pub const PROCESS_CONTROL_CAPABILITY: &str = "process.control";
pub const NOTIFY_PREVIEW_CAPABILITY: &str = "notify.preview";
pub const CHANNEL_EMIT_CAPABILITY: &str = "channel.emit";
pub const KERNEL_RUNTIME_KIND: &str = "kernel";
pub const OPERATION_METADATA_RUNTIME_KIND: &str = "cooldis.runtime.kind";

pub fn ensure_cooldis_threads_published(
    registry_root: Option<&Path>,
) -> CooldisResult<Option<PublishedOperationRecord>> {
    let Some(registry_root) = registry_root else {
        eprintln!(
            "cooldis app-server: no operation registry root configured; skipping cooldis-threads kernel package"
        );
        return Ok(None);
    };
    let registry = LocalOperationRegistry::new(registry_root);
    let package = cooldis_threads_kernel_package();
    let expected_hash = package.interface_hash()?;
    if let Ok(existing) = registry.load_record(COOLDIS_THREADS_PACKAGE)
        && existing.active_artifact_hash == expected_hash
    {
        return Ok(Some(existing));
    }
    Ok(registry
        .publish_interface_record(PublishInterfaceOperationRequest {
            name: COOLDIS_THREADS_PACKAGE.to_string(),
            source: PublishedOperationSource::Kernel {
                package: COOLDIS_THREADS_PACKAGE.to_string(),
            },
            manifest: package.manifest,
            interface: package.interface,
            capability_grants: package.capability_grants,
            metadata: BTreeMap::from([(
                OPERATION_METADATA_RUNTIME_KIND.to_string(),
                Value::String(KERNEL_RUNTIME_KIND.to_string()),
            )]),
        })
        .map(Some)?)
}

pub fn ensure_cooldis_schedule_published(
    registry_root: Option<&Path>,
) -> CooldisResult<Option<PublishedOperationRecord>> {
    let Some(registry_root) = registry_root else {
        eprintln!(
            "cooldis app-server: no operation registry root configured; skipping cooldis-schedule kernel package"
        );
        return Ok(None);
    };
    let registry = LocalOperationRegistry::new(registry_root);
    let package = cooldis_schedule_kernel_package();
    let expected_hash = package.interface_hash()?;
    if let Ok(existing) = registry.load_record(COOLDIS_SCHEDULE_PACKAGE)
        && existing.active_artifact_hash == expected_hash
    {
        return Ok(Some(existing));
    }
    Ok(registry
        .publish_interface_record(PublishInterfaceOperationRequest {
            name: COOLDIS_SCHEDULE_PACKAGE.to_string(),
            source: PublishedOperationSource::Kernel {
                package: COOLDIS_SCHEDULE_PACKAGE.to_string(),
            },
            manifest: package.manifest,
            interface: package.interface,
            capability_grants: package.capability_grants,
            metadata: BTreeMap::from([(
                OPERATION_METADATA_RUNTIME_KIND.to_string(),
                Value::String(KERNEL_RUNTIME_KIND.to_string()),
            )]),
        })
        .map(Some)?)
}

pub fn ensure_cooldis_process_published(
    registry_root: Option<&Path>,
) -> CooldisResult<Option<PublishedOperationRecord>> {
    let Some(registry_root) = registry_root else {
        eprintln!(
            "cooldis app-server: no operation registry root configured; skipping cooldis-process kernel package"
        );
        return Ok(None);
    };
    let registry = LocalOperationRegistry::new(registry_root);
    let package = cooldis_process_kernel_package();
    let expected_hash = package.interface_hash()?;
    if let Ok(existing) = registry.load_record(COOLDIS_PROCESS_PACKAGE)
        && existing.active_artifact_hash == expected_hash
    {
        return Ok(Some(existing));
    }
    Ok(registry
        .publish_interface_record(PublishInterfaceOperationRequest {
            name: COOLDIS_PROCESS_PACKAGE.to_string(),
            source: PublishedOperationSource::Kernel {
                package: COOLDIS_PROCESS_PACKAGE.to_string(),
            },
            manifest: package.manifest,
            interface: package.interface,
            capability_grants: package.capability_grants,
            metadata: BTreeMap::from([(
                OPERATION_METADATA_RUNTIME_KIND.to_string(),
                Value::String(KERNEL_RUNTIME_KIND.to_string()),
            )]),
        })
        .map(Some)?)
}

pub fn ensure_cooldis_notify_published(
    registry_root: Option<&Path>,
) -> CooldisResult<Option<PublishedOperationRecord>> {
    let Some(registry_root) = registry_root else {
        eprintln!(
            "cooldis app-server: no operation registry root configured; skipping cooldis-notify kernel package"
        );
        return Ok(None);
    };
    let registry = LocalOperationRegistry::new(registry_root);
    let package = cooldis_notify_kernel_package();
    let expected_hash = package.interface_hash()?;
    if let Ok(existing) = registry.load_record(COOLDIS_NOTIFY_PACKAGE)
        && existing.active_artifact_hash == expected_hash
    {
        return Ok(Some(existing));
    }
    Ok(registry
        .publish_interface_record(PublishInterfaceOperationRequest {
            name: COOLDIS_NOTIFY_PACKAGE.to_string(),
            source: PublishedOperationSource::Kernel {
                package: COOLDIS_NOTIFY_PACKAGE.to_string(),
            },
            manifest: package.manifest,
            interface: package.interface,
            capability_grants: package.capability_grants,
            metadata: BTreeMap::from([(
                OPERATION_METADATA_RUNTIME_KIND.to_string(),
                Value::String(KERNEL_RUNTIME_KIND.to_string()),
            )]),
        })
        .map(Some)?)
}

pub struct KernelPackageDefinition {
    pub manifest: WasmOperationManifest,
    pub interface: ToolInterfaceContract,
    pub capability_grants: BTreeSet<String>,
}

impl KernelPackageDefinition {
    fn interface_hash(&self) -> CooldisResult<String> {
        let bytes = serde_json::to_vec(&self.interface).map_err(|err| {
            CooldisError::RuntimeFactory(format!("failed to encode kernel interface: {err}"))
        })?;
        Ok(wasm_sha256(&bytes))
    }
}

pub fn cooldis_threads_kernel_package() -> KernelPackageDefinition {
    let specs = thread_operation_specs();
    let manifest = WasmOperationManifest {
        abi: "cooldis.operation/0.1".to_string(),
        operations: specs
            .iter()
            .enumerate()
            .map(|(index, spec)| WasmOperationDefinition {
                id: (index + 1) as u32,
                name: spec.name.to_string(),
                input: WasmOperationValueKind::Json,
                output: WasmOperationValueKind::Json,
                events: WasmOperationEventKind::None,
                mode: WasmOperationMode::Sync,
                required_capabilities: spec
                    .capabilities
                    .iter()
                    .map(|capability| (*capability).to_string())
                    .collect(),
            })
            .collect(),
    };
    let identity = ToolPackageIdentity {
        name: COOLDIS_THREADS_PACKAGE.to_string(),
        version: Some("1.0.0".to_string()),
        description: Some(
            "Thread control operations implemented by the Cooldis kernel.".to_string(),
        ),
        owner: Some("cooldis".to_string()),
    };
    let runtime = ToolRuntimeContract {
        kind: KERNEL_RUNTIME_KIND.to_string(),
        state: None,
        module_path: None,
        bin_path: None,
        release: None,
        timeout_ms: None,
        max_input_bytes: None,
        max_output_bytes: None,
    };
    let operations = specs
        .iter()
        .map(|spec| {
            let required_capabilities = spec
                .capabilities
                .iter()
                .map(|capability| (*capability).to_string())
                .collect::<BTreeSet<_>>();
            ToolOperationInterface {
                name: spec.name.to_string(),
                description: Some(spec.summary.to_string()),
                input_schema: (spec.input_schema)(),
                output_schema: (spec.output_schema)(),
                required_capabilities: required_capabilities.clone(),
                command: Some(ToolCommandContract {
                    name: spec.name.to_string(),
                    stdin: Some("json".to_string()),
                    stdout: Some("json".to_string()),
                }),
                mcp: None,
                manual: Some(ToolOperationManual {
                    schema_version: TOOL_MANUAL_SCHEMA_VERSION,
                    tool_name: COOLDIS_THREADS_PACKAGE.to_string(),
                    operation_name: spec.name.to_string(),
                    summary: spec.summary.to_string(),
                    usage: vec![spec.name.to_string()],
                    input_schema: (spec.input_schema)(),
                    output_schema: (spec.output_schema)(),
                    required_capabilities,
                    examples: Vec::new(),
                    exit_status: manual_exit_status(),
                    generated: false,
                    warnings: Vec::new(),
                }),
            }
        })
        .collect::<Vec<_>>();
    let capability_grants = operations
        .iter()
        .flat_map(|operation| operation.required_capabilities.iter().cloned())
        .collect();
    KernelPackageDefinition {
        manifest,
        interface: ToolInterfaceContract {
            schema_version: crate::TOOL_PACKAGE_SCHEMA_VERSION,
            identity,
            runtime,
            operations,
            fixtures: Vec::new(),
        },
        capability_grants,
    }
}

pub fn cooldis_schedule_kernel_package() -> KernelPackageDefinition {
    let specs = schedule_operation_specs();
    let manifest = WasmOperationManifest {
        abi: "cooldis.operation/0.1".to_string(),
        operations: specs
            .iter()
            .enumerate()
            .map(|(index, spec)| WasmOperationDefinition {
                id: (index + 1) as u32,
                name: spec.name.to_string(),
                input: WasmOperationValueKind::Json,
                output: WasmOperationValueKind::Json,
                events: WasmOperationEventKind::None,
                mode: WasmOperationMode::Sync,
                required_capabilities: spec
                    .capabilities
                    .iter()
                    .map(|capability| (*capability).to_string())
                    .collect(),
            })
            .collect(),
    };
    let identity = ToolPackageIdentity {
        name: COOLDIS_SCHEDULE_PACKAGE.to_string(),
        version: Some("1.0.0".to_string()),
        description: Some(
            "Mandate lifecycle operations implemented by the Cooldis kernel.".to_string(),
        ),
        owner: Some("cooldis".to_string()),
    };
    let runtime = ToolRuntimeContract {
        kind: KERNEL_RUNTIME_KIND.to_string(),
        state: None,
        module_path: None,
        bin_path: None,
        release: None,
        timeout_ms: None,
        max_input_bytes: None,
        max_output_bytes: None,
    };
    let operations = specs
        .iter()
        .map(|spec| {
            let required_capabilities = spec
                .capabilities
                .iter()
                .map(|capability| (*capability).to_string())
                .collect::<BTreeSet<_>>();
            ToolOperationInterface {
                name: spec.name.to_string(),
                description: Some(spec.summary.to_string()),
                input_schema: (spec.input_schema)(),
                output_schema: (spec.output_schema)(),
                required_capabilities: required_capabilities.clone(),
                command: Some(ToolCommandContract {
                    name: spec.name.to_string(),
                    stdin: Some("json".to_string()),
                    stdout: Some("json".to_string()),
                }),
                mcp: None,
                manual: Some(ToolOperationManual {
                    schema_version: TOOL_MANUAL_SCHEMA_VERSION,
                    tool_name: COOLDIS_SCHEDULE_PACKAGE.to_string(),
                    operation_name: spec.name.to_string(),
                    summary: spec.summary.to_string(),
                    usage: vec![spec.name.to_string()],
                    input_schema: (spec.input_schema)(),
                    output_schema: (spec.output_schema)(),
                    required_capabilities,
                    examples: Vec::new(),
                    exit_status: manual_exit_status(),
                    generated: false,
                    warnings: Vec::new(),
                }),
            }
        })
        .collect::<Vec<_>>();
    let capability_grants = operations
        .iter()
        .flat_map(|operation| operation.required_capabilities.iter().cloned())
        .collect();
    KernelPackageDefinition {
        manifest,
        interface: ToolInterfaceContract {
            schema_version: crate::TOOL_PACKAGE_SCHEMA_VERSION,
            identity,
            runtime,
            operations,
            fixtures: Vec::new(),
        },
        capability_grants,
    }
}

pub fn cooldis_process_kernel_package() -> KernelPackageDefinition {
    let specs = process_operation_specs();
    let manifest = WasmOperationManifest {
        abi: "cooldis.operation/0.1".to_string(),
        operations: specs
            .iter()
            .enumerate()
            .map(|(index, spec)| WasmOperationDefinition {
                id: (index + 1) as u32,
                name: spec.name.to_string(),
                input: WasmOperationValueKind::Json,
                output: WasmOperationValueKind::Json,
                events: WasmOperationEventKind::None,
                mode: WasmOperationMode::Sync,
                required_capabilities: spec
                    .capabilities
                    .iter()
                    .map(|capability| (*capability).to_string())
                    .collect(),
            })
            .collect(),
    };
    let identity = ToolPackageIdentity {
        name: COOLDIS_PROCESS_PACKAGE.to_string(),
        version: Some("1.0.0".to_string()),
        description: Some(
            "Process handle operations implemented by the Cooldis kernel.".to_string(),
        ),
        owner: Some("cooldis".to_string()),
    };
    let runtime = ToolRuntimeContract {
        kind: KERNEL_RUNTIME_KIND.to_string(),
        state: None,
        module_path: None,
        bin_path: None,
        release: None,
        timeout_ms: None,
        max_input_bytes: None,
        max_output_bytes: None,
    };
    let operations = specs
        .iter()
        .map(|spec| {
            let required_capabilities = spec
                .capabilities
                .iter()
                .map(|capability| (*capability).to_string())
                .collect::<BTreeSet<_>>();
            ToolOperationInterface {
                name: spec.name.to_string(),
                description: Some(spec.summary.to_string()),
                input_schema: (spec.input_schema)(),
                output_schema: process_snapshot_output_schema(spec.receipt_operation),
                required_capabilities: required_capabilities.clone(),
                command: Some(ToolCommandContract {
                    name: spec.name.to_string(),
                    stdin: Some("json".to_string()),
                    stdout: Some("json".to_string()),
                }),
                mcp: None,
                manual: Some(ToolOperationManual {
                    schema_version: TOOL_MANUAL_SCHEMA_VERSION,
                    tool_name: COOLDIS_PROCESS_PACKAGE.to_string(),
                    operation_name: spec.name.to_string(),
                    summary: spec.summary.to_string(),
                    usage: vec![spec.name.to_string()],
                    input_schema: (spec.input_schema)(),
                    output_schema: process_snapshot_output_schema(spec.receipt_operation),
                    required_capabilities,
                    examples: Vec::new(),
                    exit_status: manual_exit_status(),
                    generated: false,
                    warnings: vec![
                        "Host process execution must be explicitly granted; it is not included in the default agent manifest.".to_string(),
                    ],
                }),
            }
        })
        .collect::<Vec<_>>();
    let capability_grants = operations
        .iter()
        .flat_map(|operation| operation.required_capabilities.iter().cloned())
        .collect();
    KernelPackageDefinition {
        manifest,
        interface: ToolInterfaceContract {
            schema_version: crate::TOOL_PACKAGE_SCHEMA_VERSION,
            identity,
            runtime,
            operations,
            fixtures: Vec::new(),
        },
        capability_grants,
    }
}

pub fn cooldis_notify_kernel_package() -> KernelPackageDefinition {
    let specs = notify_operation_specs();
    let manifest = WasmOperationManifest {
        abi: "cooldis.operation/0.1".to_string(),
        operations: specs
            .iter()
            .enumerate()
            .map(|(index, spec)| WasmOperationDefinition {
                id: (index + 1) as u32,
                name: spec.name.to_string(),
                input: WasmOperationValueKind::Json,
                output: WasmOperationValueKind::Json,
                events: WasmOperationEventKind::None,
                mode: WasmOperationMode::Sync,
                required_capabilities: spec
                    .capabilities
                    .iter()
                    .map(|capability| (*capability).to_string())
                    .collect(),
            })
            .collect(),
    };
    let identity = ToolPackageIdentity {
        name: COOLDIS_NOTIFY_PACKAGE.to_string(),
        version: Some("1.0.0".to_string()),
        description: Some(
            "Notification and channel intent operations implemented by the Cooldis kernel."
                .to_string(),
        ),
        owner: Some("cooldis".to_string()),
    };
    let runtime = ToolRuntimeContract {
        kind: KERNEL_RUNTIME_KIND.to_string(),
        state: None,
        module_path: None,
        bin_path: None,
        release: None,
        timeout_ms: None,
        max_input_bytes: None,
        max_output_bytes: None,
    };
    let operations = specs
        .iter()
        .map(|spec| {
            let required_capabilities = spec
                .capabilities
                .iter()
                .map(|capability| (*capability).to_string())
                .collect::<BTreeSet<_>>();
            ToolOperationInterface {
                name: spec.name.to_string(),
                description: Some(spec.summary.to_string()),
                input_schema: (spec.input_schema)(),
                output_schema: notify_receipt_output_schema(spec.receipt_operation),
                required_capabilities: required_capabilities.clone(),
                command: Some(ToolCommandContract {
                    name: spec.name.to_string(),
                    stdin: Some("json".to_string()),
                    stdout: Some("json".to_string()),
                }),
                mcp: None,
                manual: Some(ToolOperationManual {
                    schema_version: TOOL_MANUAL_SCHEMA_VERSION,
                    tool_name: COOLDIS_NOTIFY_PACKAGE.to_string(),
                    operation_name: spec.name.to_string(),
                    summary: spec.summary.to_string(),
                    usage: vec![spec.name.to_string()],
                    input_schema: (spec.input_schema)(),
                    output_schema: notify_receipt_output_schema(spec.receipt_operation),
                    required_capabilities,
                    examples: Vec::new(),
                    exit_status: manual_exit_status(),
                    generated: false,
                    warnings: vec![
                        "This V1 reference package records channel intent and does not deliver to Slack, Telegram, email, or HITL channels.".to_string(),
                    ],
                }),
            }
        })
        .collect::<Vec<_>>();
    let capability_grants = operations
        .iter()
        .flat_map(|operation| operation.required_capabilities.iter().cloned())
        .collect();
    KernelPackageDefinition {
        manifest,
        interface: ToolInterfaceContract {
            schema_version: crate::TOOL_PACKAGE_SCHEMA_VERSION,
            identity,
            runtime,
            operations,
            fixtures: Vec::new(),
        },
        capability_grants,
    }
}

struct ThreadOperationSpec {
    name: &'static str,
    summary: &'static str,
    capabilities: &'static [&'static str],
    input_schema: fn() -> Value,
    output_schema: fn() -> Value,
}

struct ScheduleOperationSpec {
    name: &'static str,
    summary: &'static str,
    capabilities: &'static [&'static str],
    input_schema: fn() -> Value,
    output_schema: fn() -> Value,
}

struct ProcessOperationSpec {
    name: &'static str,
    summary: &'static str,
    capabilities: &'static [&'static str],
    input_schema: fn() -> Value,
    receipt_operation: &'static str,
}

struct NotifyOperationSpec {
    name: &'static str,
    summary: &'static str,
    capabilities: &'static [&'static str],
    input_schema: fn() -> Value,
    receipt_operation: &'static str,
}

fn thread_operation_specs() -> Vec<ThreadOperationSpec> {
    vec![
        ThreadOperationSpec {
            name: THREAD_SPAWN_OPERATION,
            summary: "Start a supervised child thread and submit its first message.",
            capabilities: &[THREADS_SPAWN_CAPABILITY],
            input_schema: thread_spawn_input_schema,
            output_schema: spawn_output_schema,
        },
        ThreadOperationSpec {
            name: THREAD_SUBMIT_OPERATION,
            summary: "Submit a user message to a scoped thread.",
            capabilities: &[THREADS_CONTROL_CAPABILITY],
            input_schema: target_message_input_schema,
            output_schema: submit_output_schema,
        },
        ThreadOperationSpec {
            name: THREAD_WAIT_OPERATION,
            summary: "Wait for a child addressed by task name to settle.",
            capabilities: &[THREADS_READ_CAPABILITY],
            input_schema: wait_input_schema,
            output_schema: wait_output_schema,
        },
        ThreadOperationSpec {
            name: THREAD_STATUS_OPERATION,
            summary: "Report status for a child addressed by task name.",
            capabilities: &[THREADS_READ_CAPABILITY],
            input_schema: required_task_target_input_schema,
            output_schema: status_output_schema,
        },
        ThreadOperationSpec {
            name: THREAD_CANCEL_OPERATION,
            summary: "Cancel a scoped thread.",
            capabilities: &[THREADS_CONTROL_CAPABILITY],
            input_schema: cancel_input_schema,
            output_schema: lifecycle_output_schema,
        },
    ]
}

fn schedule_operation_specs() -> Vec<ScheduleOperationSpec> {
    vec![
        ScheduleOperationSpec {
            name: MANDATE_START_OPERATION,
            summary: "Witness a scheduled continuation mandate on a thread control stream.",
            capabilities: &[SCHEDULE_MANAGE_CAPABILITY],
            input_schema: mandate_start_input_schema,
            output_schema: mandate_start_output_schema,
        },
        ScheduleOperationSpec {
            name: MANDATE_REVOKE_OPERATION,
            summary: "Witness mandate revocation for a started mandate event.",
            capabilities: &[SCHEDULE_MANAGE_CAPABILITY],
            input_schema: mandate_revoke_input_schema,
            output_schema: mandate_revoke_output_schema,
        },
        ScheduleOperationSpec {
            name: MANDATE_LIST_OPERATION,
            summary: "List active scheduled mandates for a thread.",
            capabilities: &[SCHEDULE_READ_CAPABILITY],
            input_schema: mandate_list_input_schema,
            output_schema: mandate_list_output_schema,
        },
    ]
}

fn process_operation_specs() -> Vec<ProcessOperationSpec> {
    vec![
        ProcessOperationSpec {
            name: PROCESS_EXEC_OPERATION,
            summary: "Start a host command process and return its first process snapshot.",
            capabilities: &[PROCESS_SPAWN_CAPABILITY],
            input_schema: process_exec_input_schema,
            receipt_operation: "cooldis.process_exec",
        },
        ProcessOperationSpec {
            name: PROCESS_POLL_OPERATION,
            summary: "Poll an existing process handle and return its latest process snapshot.",
            capabilities: &[PROCESS_READ_CAPABILITY],
            input_schema: process_handle_input_schema,
            receipt_operation: "cooldis.process_poll",
        },
        ProcessOperationSpec {
            name: PROCESS_WRITE_OPERATION,
            summary: "Write base64 stdin bytes to a process handle and return its latest snapshot.",
            capabilities: &[PROCESS_WRITE_CAPABILITY],
            input_schema: process_write_input_schema,
            receipt_operation: "cooldis.process_write",
        },
        ProcessOperationSpec {
            name: PROCESS_TERMINATE_OPERATION,
            summary: "Terminate a process handle and return its terminal process snapshot.",
            capabilities: &[PROCESS_CONTROL_CAPABILITY],
            input_schema: process_terminate_input_schema,
            receipt_operation: "cooldis.process_terminate",
        },
    ]
}

fn thread_spawn_input_schema() -> Value {
    object_schema(
        json!({
            "task_name": string_schema("Stable task name for the child thread."),
            "message": string_schema("Initial user message submitted to the child thread."),
            "agent_ref": string_schema("Optional published agent reference for the child thread.")
        }),
        &["task_name", "message"],
    )
}

fn mandate_start_input_schema() -> Value {
    object_schema(
        json!({
            "thread_id": string_schema("Optional target Cooldis thread id; omitted means the calling thread."),
            "schedule": schedule_schema(),
            "max_occurrences": {
                "type": "integer",
                "description": "Optional maximum number of occurrences."
            },
            "catch_up": catch_up_schema(),
            "input_template": string_schema("Optional continuation turn input template. Rendered as a plain string; only {scheduled_for} is substituted.")
        }),
        &["schedule"],
    )
}

fn mandate_revoke_input_schema() -> Value {
    object_schema(
        json!({
            "thread_id": string_schema("Optional target Cooldis thread id; omitted means the calling thread."),
            "mandate_event_id": string_schema("Event id returned by mandate_start.")
        }),
        &["mandate_event_id"],
    )
}

fn mandate_list_input_schema() -> Value {
    object_schema(
        json!({
            "thread_id": string_schema("Optional target Cooldis thread id; omitted means the calling thread.")
        }),
        &[],
    )
}

fn notify_operation_specs() -> Vec<NotifyOperationSpec> {
    vec![
        NotifyOperationSpec {
            name: NOTIFY_PREVIEW_OPERATION,
            summary: "Normalize a notification intent without delivering it to a channel.",
            capabilities: &[NOTIFY_PREVIEW_CAPABILITY],
            input_schema: notify_preview_input_schema,
            receipt_operation: "cooldis.notify_preview",
        },
        NotifyOperationSpec {
            name: CHANNEL_EMIT_OPERATION,
            summary: "Record channel egress intent for an explicit external delivery adapter.",
            capabilities: &[CHANNEL_EMIT_CAPABILITY],
            input_schema: channel_emit_input_schema,
            receipt_operation: "cooldis.channel_emit",
        },
    ]
}

fn process_exec_input_schema() -> Value {
    object_schema(
        json!({
            "command": {
                "type": "array",
                "description": "Command argv to execute. The first item is the executable.",
                "items": string_schema("Command argument.")
            },
            "cwd": string_schema("Optional working directory."),
            "env": {
                "type": "object",
                "description": "Optional environment variable overrides.",
                "additionalProperties": string_schema("Environment variable value.")
            },
            "stream_stdin": { "type": "boolean" },
            "timeout_ms": {
                "type": "integer",
                "description": "Optional hard execution deadline in milliseconds."
            },
            "yield_time_ms": {
                "type": "integer",
                "description": "How long to wait for output or terminal state before returning."
            },
            "output_bytes_cap": {
                "type": "integer",
                "description": "Maximum stdout/stderr bytes retained for this snapshot."
            },
            "dispatch_id": string_schema("Optional idempotency identity; generated when absent.")
        }),
        &["command"],
    )
}

fn notify_preview_input_schema() -> Value {
    object_schema(
        json!({
            "channel": string_schema("Logical channel family or adapter name."),
            "subject": string_schema("Short notification subject."),
            "body": string_schema("Notification body."),
            "severity": string_schema("Optional severity such as info, warning, or critical.")
        }),
        &["channel", "body"],
    )
}

fn channel_emit_input_schema() -> Value {
    object_schema(
        json!({
            "channel": string_schema("Logical channel family or adapter name."),
            "message": string_schema("Channel message body."),
            "thread_id": string_schema("Optional Cooldis thread id associated with the egress intent.")
        }),
        &["channel", "message"],
    )
}

fn process_handle_input_schema() -> Value {
    object_schema(
        json!({
            "process_id": string_schema("Cooldis process id."),
            "yield_time_ms": {
                "type": "integer",
                "description": "How long to wait for output or terminal state before returning."
            },
            "output_bytes_cap": {
                "type": "integer",
                "description": "Maximum stdout/stderr bytes retained for this snapshot."
            }
        }),
        &["process_id"],
    )
}

fn process_write_input_schema() -> Value {
    object_schema(
        json!({
            "process_id": string_schema("Cooldis process id."),
            "delta_base64": string_schema("Base64 encoded stdin bytes."),
            "yield_time_ms": {
                "type": "integer",
                "description": "How long to wait for output or terminal state before returning."
            },
            "output_bytes_cap": {
                "type": "integer",
                "description": "Maximum stdout/stderr bytes retained for this snapshot."
            }
        }),
        &["process_id", "delta_base64"],
    )
}

fn process_terminate_input_schema() -> Value {
    object_schema(
        json!({
            "process_id": string_schema("Cooldis process id."),
            "reason": string_schema("Human-readable termination reason."),
            "yield_time_ms": {
                "type": "integer",
                "description": "How long to wait for terminal state before returning."
            }
        }),
        &["process_id"],
    )
}

fn target_message_input_schema() -> Value {
    object_schema(
        json!({
            "task_name": string_schema("Parent-scoped task name of the child thread."),
            "message": string_schema("User message to submit.")
        }),
        &["task_name", "message"],
    )
}

fn wait_input_schema() -> Value {
    object_schema(
        json!({
            "task_name": string_schema("Parent-scoped task name of the child thread."),
            "timeout_ms": {
                "type": "integer",
                "description": "Optional timeout in milliseconds."
            }
        }),
        &["task_name"],
    )
}

fn required_task_target_input_schema() -> Value {
    object_schema(
        json!({
            "task_name": string_schema("Parent-scoped task name of the child thread.")
        }),
        &["task_name"],
    )
}

fn cancel_input_schema() -> Value {
    object_schema(
        json!({
            "task_name": string_schema("Parent-scoped task name of the child thread.")
        }),
        &["task_name"],
    )
}

fn process_snapshot_output_schema(operation: &str) -> Value {
    object_schema(
        json!({
            "operation": {
                "type": "string",
                "enum": [operation],
                "description": "Receipt operation name."
            },
            "process_id": string_schema("Stable process id retained through terminal acknowledgement."),
            "dispatch_id": string_schema("Dispatch identity, present on process_exec receipts."),
            "status": process_status_schema("Current process status."),
            "backend": string_schema("Process backend kind."),
            "label": string_schema("Human-readable process label."),
            "exit_code": {
                "type": "integer",
                "description": "Process exit code, present after an exit code is known."
            },
            "stdout": string_schema("Visible stdout bytes decoded as UTF-8 lossily."),
            "stderr": string_schema("Visible stderr bytes decoded as UTF-8 lossily."),
            "truncated": { "type": "boolean" },
            "stdout_truncated": { "type": "boolean" },
            "stderr_truncated": { "type": "boolean" },
            "event_count": {
                "type": "integer",
                "description": "Number of process events represented by this snapshot."
            }
        }),
        &[
            "operation",
            "status",
            "backend",
            "label",
            "stdout",
            "stderr",
            "truncated",
            "stdout_truncated",
            "stderr_truncated",
            "event_count",
        ],
    )
}

fn mandate_start_output_schema() -> Value {
    object_schema(
        json!({
            "operation": {
                "type": "string",
                "enum": ["cooldis.mandate_start"],
                "description": "Receipt operation name."
            },
            "status": {
                "type": "string",
                "enum": ["started"]
            },
            "thread_id": string_schema("Target thread id."),
            "mandate_event_id": string_schema("Started mandate event id."),
            "stream_id": string_schema("Control stream id."),
            "sequence": {
                "type": "integer",
                "description": "Event sequence in the control stream."
            }
        }),
        &[
            "operation",
            "status",
            "thread_id",
            "mandate_event_id",
            "stream_id",
            "sequence",
        ],
    )
}

fn mandate_revoke_output_schema() -> Value {
    object_schema(
        json!({
            "operation": {
                "type": "string",
                "enum": ["cooldis.mandate_revoke"],
                "description": "Receipt operation name."
            },
            "status": {
                "type": "string",
                "enum": ["revoked", "already_revoked"]
            },
            "thread_id": string_schema("Target thread id."),
            "mandate_event_id": string_schema("Started mandate event id."),
            "revoked_event_id": string_schema("Revocation event id.")
        }),
        &[
            "operation",
            "status",
            "thread_id",
            "mandate_event_id",
            "revoked_event_id",
        ],
    )
}

fn mandate_list_output_schema() -> Value {
    object_schema(
        json!({
            "operation": {
                "type": "string",
                "enum": ["cooldis.mandate_list"],
                "description": "Receipt operation name."
            },
            "thread_id": string_schema("Target thread id."),
            "mandates": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": true
                }
            }
        }),
        &["operation", "thread_id", "mandates"],
    )
}

fn notify_receipt_output_schema(operation: &str) -> Value {
    object_schema(
        json!({
            "operation": {
                "type": "string",
                "enum": [operation],
                "description": "Receipt operation name."
            },
            "status": {
                "type": "string",
                "enum": ["recorded"],
                "description": "The channel intent was normalized as a receipt."
            },
            "delivery": {
                "type": "string",
                "enum": ["not_sent"],
                "description": "V1 reference operations do not deliver to external channels."
            },
            "channel": string_schema("Logical channel family or adapter name."),
            "subject": string_schema("Notification subject, when provided."),
            "body": string_schema("Notification body, for notify_preview."),
            "message": string_schema("Channel message body, for channel_emit."),
            "severity": string_schema("Notification severity, when provided."),
            "thread_id": string_schema("Associated thread id, when provided."),
            "channel_decision_required": { "type": "boolean" },
            "reason": string_schema("Why this reference operation did not deliver externally.")
        }),
        &[
            "operation",
            "status",
            "delivery",
            "channel",
            "channel_decision_required",
            "reason",
        ],
    )
}

fn spawn_output_schema() -> Value {
    task_handle_output_schema("Current child thread status.")
}

fn submit_output_schema() -> Value {
    task_handle_output_schema("Current target thread status.")
}

fn wait_output_schema() -> Value {
    task_handle_output_schema("Current target thread status after waiting.")
}

fn status_output_schema() -> Value {
    task_handle_output_schema("Current target thread status.")
}

fn lifecycle_output_schema() -> Value {
    task_handle_output_schema("Current target thread lifecycle status.")
}

fn task_handle_output_schema(status_description: &str) -> Value {
    object_schema(
        json!({
            "operation": string_schema("Receipt operation name."),
            "task_name": string_schema("Parent-scoped task name of the child thread."),
            "status": thread_status_schema(status_description)
        }),
        &["operation", "task_name", "status"],
    )
}

fn schedule_schema() -> Value {
    object_schema(
        json!({
            "cron": object_schema(
                json!({
                    "expr": string_schema("Cron expression."),
                    "tz": string_schema("IANA timezone name.")
                }),
                &["expr", "tz"]
            ),
            "interval": object_schema(
                json!({
                    "every_ms": {
                        "type": "integer",
                        "description": "Interval in milliseconds; minimum is 60000."
                    }
                }),
                &["every_ms"]
            ),
            "at": object_schema(
                json!({
                    "when": string_schema("RFC3339 timestamp.")
                }),
                &["when"]
            )
        }),
        &[],
    )
}

fn catch_up_schema() -> Value {
    json!({
        "type": "string",
        "enum": ["coalesce_missed", "skip_missed"],
        "description": "How missed occurrences are handled."
    })
}

fn object_schema(properties: Value, required: &[&str]) -> Value {
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false
    })
}

fn string_schema(description: &str) -> Value {
    json!({
        "type": "string",
        "description": description
    })
}

fn thread_status_schema(description: &str) -> Value {
    json!({
        "type": "string",
        "enum": [
            "starting",
            "idle",
            "running",
            "cancelling",
            "stopped",
            "failed"
        ],
        "description": description
    })
}

fn process_status_schema(description: &str) -> Value {
    json!({
        "type": "string",
        "enum": [
            "running",
            "completed",
            "failed",
            "timed_out",
            "cancelled"
        ],
        "description": description
    })
}

fn manual_exit_status() -> Vec<ToolManualExitStatus> {
    vec![
        ToolManualExitStatus {
            code: 0,
            meaning: "operation succeeded".to_string(),
        },
        ToolManualExitStatus {
            code: 1,
            meaning: "operation failed at runtime".to_string(),
        },
        ToolManualExitStatus {
            code: 2,
            meaning: "caller supplied invalid input or arguments".to_string(),
        },
        ToolManualExitStatus {
            code: 126,
            meaning: "capability or policy denied execution".to_string(),
        },
        ToolManualExitStatus {
            code: 127,
            meaning: "tool or operation was not found".to_string(),
        },
    ]
}

#[cfg(test)]
mod tests;
