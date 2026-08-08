#[derive(Clone, Debug)]
pub struct OperationRegistration {
    pub name: String,
    pub config: verlet_wasm::WasmRuntimeConfig,
    pub metadata: std::collections::BTreeMap<String, serde_json::Value>,
    pub operation_names: std::collections::BTreeSet<String>,
}

impl OperationRegistration {
    pub fn new(name: impl Into<String>, artifact: verlet_wasm::WasmRuntimeArtifact) -> Self {
        Self::from_config(name, verlet_wasm::WasmRuntimeConfig::new(artifact))
    }

    pub fn from_config(name: impl Into<String>, config: verlet_wasm::WasmRuntimeConfig) -> Self {
        Self {
            name: name.into(),
            config,
            metadata: std::collections::BTreeMap::new(),
            operation_names: std::collections::BTreeSet::new(),
        }
    }

    pub fn with_capability_grant(mut self, grant: impl Into<String>) -> Self {
        self.config.capability_grants.insert(grant.into());
        self
    }

    pub fn with_capability_grants(mut self, grants: impl IntoIterator<Item = String>) -> Self {
        self.config.capability_grants.extend(grants);
        self
    }

    pub fn with_operation_names<I, S>(mut self, operation_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.operation_names
            .extend(operation_names.into_iter().map(Into::into));
        self
    }

    pub fn with_invocation_context(mut self, context: verlet_abi::InvocationContext) -> Self {
        self.config.invocation_context = context;
        self
    }

    pub fn with_secret(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.secrets.insert(name.into(), value.into());
        self
    }

    pub fn with_metadata(
        mut self,
        key: impl Into<String>,
        value: impl Into<serde_json::Value>,
    ) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

#[derive(Clone)]
pub struct KernelOperationRegistration {
    pub name: String,
    pub manifest: verlet_abi::WasmOperationManifest,
    pub capability_grants: std::collections::BTreeSet<String>,
    pub metadata: std::collections::BTreeMap<String, serde_json::Value>,
    pub operation_names: std::collections::BTreeSet<String>,
    pub dispatcher: Option<std::sync::Arc<dyn KernelOperationDispatcher>>,
}

impl KernelOperationRegistration {
    pub fn new(name: impl Into<String>, manifest: verlet_abi::WasmOperationManifest) -> Self {
        Self {
            name: name.into(),
            manifest,
            capability_grants: std::collections::BTreeSet::new(),
            metadata: std::collections::BTreeMap::new(),
            operation_names: std::collections::BTreeSet::new(),
            dispatcher: None,
        }
    }

    pub fn with_capability_grants(mut self, grants: impl IntoIterator<Item = String>) -> Self {
        self.capability_grants.extend(grants);
        self
    }

    pub fn with_operation_names<I, S>(mut self, operation_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.operation_names
            .extend(operation_names.into_iter().map(Into::into));
        self
    }

    pub fn with_dispatcher(
        mut self,
        dispatcher: std::sync::Arc<dyn KernelOperationDispatcher>,
    ) -> Self {
        self.dispatcher = Some(dispatcher);
        self
    }
}

#[async_trait::async_trait]
pub trait KernelOperationDispatcher: Send + Sync + 'static {
    async fn invoke_kernel_operation(
        &self,
        operation_name: &str,
        input: Vec<u8>,
    ) -> crate::VerletResult<Vec<u8>>;

    async fn invoke_kernel_operation_with_metadata(
        &self,
        operation_name: &str,
        input: Vec<u8>,
        _metadata: std::collections::BTreeMap<String, serde_json::Value>,
    ) -> crate::VerletResult<Vec<u8>> {
        self.invoke_kernel_operation(operation_name, input).await
    }
}

#[derive(Default)]
pub struct OperationRegistry {
    entries: tokio::sync::RwLock<
        std::collections::BTreeMap<String, std::sync::Arc<OperationRegistryEntry>>,
    >,
}

struct OperationRegistryEntry {
    record: crate::RegisteredOperation,
    runtime: OperationRegistryEntryRuntime,
}

enum OperationRegistryEntryRuntime {
    Wasm {
        factory: std::sync::Arc<verlet_wasm::runner::WasmRuntimeFactory>,
    },
    Kernel {
        dispatcher: tokio::sync::RwLock<Option<std::sync::Arc<dyn KernelOperationDispatcher>>>,
    },
}

impl OperationRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(
        &self,
        registration: OperationRegistration,
    ) -> crate::VerletResult<crate::RegisteredOperation> {
        self.register_wasm(registration, None).await
    }

    #[doc(hidden)]
    pub async fn register_prevalidated(
        &self,
        registration: OperationRegistration,
        manifest: verlet_abi::WasmOperationManifest,
    ) -> crate::VerletResult<crate::RegisteredOperation> {
        self.register_wasm(registration, Some(manifest)).await
    }

    async fn register_wasm(
        &self,
        registration: OperationRegistration,
        manifest: Option<verlet_abi::WasmOperationManifest>,
    ) -> crate::VerletResult<crate::RegisteredOperation> {
        let name = normalize_registration_name(&registration.name)?;
        let config = registration.config.clone();
        let factory = std::sync::Arc::new(verlet_wasm::runner::WasmRuntimeFactory::new(
            config.clone(),
        )?);
        let mut manifest = match manifest {
            Some(manifest) => manifest,
            None => factory.describe().await?.ok_or_else(|| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "registered operation {name:?} does not export a Verlet operation manifest"
                ))
            })?,
        };
        if !registration.operation_names.is_empty() {
            manifest = filter_manifest_operations(&name, manifest, &registration.operation_names)?;
        }
        validate_required_grants(&name, &manifest, &config.effective_capability_grants())?;

        let record = crate::RegisteredOperation {
            name,
            manifest,
            capability_grants: config.effective_capability_grants(),
            metadata: registration.metadata,
        };
        let entry = std::sync::Arc::new(OperationRegistryEntry {
            record: record.clone(),
            runtime: OperationRegistryEntryRuntime::Wasm { factory },
        });
        self.entries
            .write()
            .await
            .insert(record.name.clone(), entry);
        Ok(record)
    }

    pub async fn register_kernel(
        &self,
        registration: KernelOperationRegistration,
    ) -> crate::VerletResult<crate::RegisteredOperation> {
        let name = normalize_registration_name(&registration.name)?;
        let mut manifest = registration.manifest;
        if !registration.operation_names.is_empty() {
            manifest = filter_manifest_operations(&name, manifest, &registration.operation_names)?;
        }
        validate_required_grants(&name, &manifest, &registration.capability_grants)?;
        let record = crate::RegisteredOperation {
            name,
            manifest,
            capability_grants: registration.capability_grants,
            metadata: registration.metadata,
        };
        let entry = std::sync::Arc::new(OperationRegistryEntry {
            record: record.clone(),
            runtime: OperationRegistryEntryRuntime::Kernel {
                dispatcher: tokio::sync::RwLock::new(registration.dispatcher),
            },
        });
        self.entries
            .write()
            .await
            .insert(record.name.clone(), entry);
        Ok(record)
    }

    pub async fn set_kernel_dispatcher(
        &self,
        name: &str,
        dispatcher: std::sync::Arc<dyn KernelOperationDispatcher>,
    ) -> crate::VerletResult<bool> {
        let name = normalize_registration_name(name)?;
        let entry = {
            let entries = self.entries.read().await;
            entries.get(&name).cloned()
        };
        let Some(entry) = entry else {
            return Ok(false);
        };
        match &entry.runtime {
            OperationRegistryEntryRuntime::Kernel { dispatcher: slot } => {
                *slot.write().await = Some(dispatcher);
                Ok(true)
            }
            OperationRegistryEntryRuntime::Wasm { .. } => {
                Err(crate::VerletOperationsError::RuntimeFactory(format!(
                    "registered operation {name:?} is not kernel-native"
                )))
            }
        }
    }

    pub async fn describe(&self, name: &str) -> Option<crate::RegisteredOperation> {
        self.entries
            .read()
            .await
            .get(name)
            .map(|entry| entry.record.clone())
    }

    pub async fn list(&self) -> Vec<crate::RegisteredOperation> {
        self.entries
            .read()
            .await
            .values()
            .map(|entry| entry.record.clone())
            .collect()
    }

    pub async fn unregister(&self, name: &str) -> Option<crate::RegisteredOperation> {
        self.entries
            .write()
            .await
            .remove(name)
            .map(|entry| entry.record.clone())
    }

    pub async fn invoke_bytes(
        &self,
        registered_name: &str,
        operation_name: &str,
        input: impl Into<Vec<u8>>,
    ) -> crate::VerletResult<verlet_process::process::WasmOperationOutput> {
        self.invoke_bytes_with_kernel_metadata(
            registered_name,
            operation_name,
            input,
            std::collections::BTreeMap::new(),
        )
        .await
    }

    pub async fn invoke_bytes_with_kernel_metadata(
        &self,
        registered_name: &str,
        operation_name: &str,
        input: impl Into<Vec<u8>>,
        kernel_metadata: std::collections::BTreeMap<String, serde_json::Value>,
    ) -> crate::VerletResult<verlet_process::process::WasmOperationOutput> {
        let entry = self
            .entries
            .read()
            .await
            .get(registered_name)
            .cloned()
            .ok_or_else(|| {
                crate::VerletOperationsError::RuntimeExecution(format!(
                    "registered operation {registered_name:?} was not found"
                ))
            })?;
        if entry.record.manifest.operation(operation_name).is_none() {
            return Err(crate::VerletOperationsError::RuntimeExecution(format!(
                "registered operation {registered_name:?} does not expose operation {operation_name:?}"
            )));
        }
        match &entry.runtime {
            OperationRegistryEntryRuntime::Wasm { factory } => Ok(factory
                .invoke_operation_bytes(operation_name, input.into())
                .await?),
            OperationRegistryEntryRuntime::Kernel { dispatcher } => {
                let dispatcher = dispatcher.read().await.clone().ok_or_else(|| {
                    crate::VerletOperationsError::RuntimeExecution(format!(
                        "kernel operation {registered_name:?}/{operation_name:?} has no dispatcher in this runtime"
                    ))
                })?;
                let output = dispatcher
                    .invoke_kernel_operation_with_metadata(
                        operation_name,
                        input.into(),
                        kernel_metadata,
                    )
                    .await?;
                let operation = entry
                    .record
                    .manifest
                    .operation(operation_name)
                    .expect("operation existence checked before dispatch")
                    .clone();
                Ok(verlet_process::process::WasmOperationOutput {
                    manifest: entry.record.manifest.clone(),
                    operation,
                    output,
                    events: Vec::new(),
                    invocation_context: verlet_abi::InvocationContext::default(),
                })
            }
        }
    }

    pub async fn invoke_process(
        &self,
        registered_name: &str,
        operation_name: &str,
        input: impl Into<Vec<u8>>,
    ) -> crate::VerletResult<verlet_process::process::VerletProcessHandle> {
        let output = self
            .invoke_bytes(registered_name, operation_name, input)
            .await?;
        Ok(
            verlet_process::process::VerletProcessHandle::from_wasm_operation_output(
                Some(registered_name.to_string()),
                output,
            ),
        )
    }

    pub async fn invoke_process_with_kernel_metadata(
        &self,
        registered_name: &str,
        operation_name: &str,
        input: impl Into<Vec<u8>>,
        kernel_metadata: std::collections::BTreeMap<String, serde_json::Value>,
    ) -> crate::VerletResult<verlet_process::process::VerletProcessHandle> {
        let output = self
            .invoke_bytes_with_kernel_metadata(
                registered_name,
                operation_name,
                input,
                kernel_metadata,
            )
            .await?;
        Ok(
            verlet_process::process::VerletProcessHandle::from_wasm_operation_output(
                Some(registered_name.to_string()),
                output,
            ),
        )
    }
}

fn normalize_registration_name(name: &str) -> crate::VerletResult<String> {
    let name = name.trim();
    if name.is_empty() {
        return Err(crate::VerletOperationsError::RuntimeFactory(
            "operation registration name cannot be empty".to_string(),
        ));
    }
    Ok(name.to_string())
}

fn validate_required_grants(
    registration_name: &str,
    manifest: &verlet_abi::WasmOperationManifest,
    grants: &std::collections::BTreeSet<String>,
) -> crate::VerletResult<()> {
    let missing: Vec<_> = manifest
        .operations
        .iter()
        .flat_map(|operation| {
            operation
                .required_capabilities
                .iter()
                .filter(|capability| !grants.contains(capability.as_str()))
                .map(|capability| format!("{}:{capability}", operation.name))
        })
        .collect();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(crate::VerletOperationsError::RuntimeFactory(format!(
            "operation registration {registration_name:?} requires ungranted capabilities: {}",
            missing.join(", ")
        )))
    }
}

#[doc(hidden)]
pub fn filter_manifest_operations(
    registration_name: &str,
    mut manifest: verlet_abi::WasmOperationManifest,
    operation_names: &std::collections::BTreeSet<String>,
) -> crate::VerletResult<verlet_abi::WasmOperationManifest> {
    let available = manifest
        .operations
        .iter()
        .map(|operation| operation.name.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let missing = operation_names
        .difference(&available)
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        let available = available.into_iter().collect::<Vec<_>>().join(", ");
        return Err(crate::VerletOperationsError::RuntimeFactory(format!(
            "operation registration {registration_name:?} selected unknown operation(s) {}; available operations: {}",
            missing.join(", "),
            if available.is_empty() {
                "<none>"
            } else {
                &available
            }
        )));
    }
    manifest
        .operations
        .retain(|operation| operation_names.contains(&operation.name));
    Ok(manifest)
}
