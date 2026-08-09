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

    /// Set an immutable fallback dispatcher shared by every caller. Dispatchers
    /// that capture a thread or caller belong in [`KernelDispatchOverlay`].
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
        dispatcher: Option<std::sync::Arc<dyn KernelOperationDispatcher>>,
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
                dispatcher: registration.dispatcher,
            },
        });
        self.entries
            .write()
            .await
            .insert(record.name.clone(), entry);
        Ok(record)
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
        self.invoke_entry_bytes_with_kernel_metadata(
            registered_name,
            operation_name,
            input.into(),
            kernel_metadata,
            None,
        )
        .await
    }

    async fn invoke_entry_bytes_with_kernel_metadata(
        &self,
        registered_name: &str,
        operation_name: &str,
        input: Vec<u8>,
        kernel_metadata: std::collections::BTreeMap<String, serde_json::Value>,
        overlay_dispatcher: Option<std::sync::Arc<dyn KernelOperationDispatcher>>,
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
                .invoke_operation_bytes(operation_name, input)
                .await?),
            OperationRegistryEntryRuntime::Kernel { dispatcher } => {
                let dispatcher = overlay_dispatcher
                    .or_else(|| dispatcher.clone())
                    .ok_or_else(|| {
                        crate::VerletOperationsError::RuntimeExecution(format!(
                            "kernel operation {registered_name:?}/{operation_name:?} has no dispatcher in this runtime"
                        ))
                    })?;
                let output = dispatcher
                    .invoke_kernel_operation_with_metadata(operation_name, input, kernel_metadata)
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

/// Per-caller overlay of kernel-operation dispatchers (EMO-550).
///
/// A dynamic kernel dispatcher captures one thread's control and context.
/// Registries are shared — across threads today, across instances on the
/// multi-tenant host — so such a dispatcher must never be written into a
/// registry slot after registration: the last writer silently redirects
/// every other thread's kernel operations. The overlay rides with the
/// invoking side instead (agent tool router, bash execution config) and is
/// consulted per invocation, before the dispatcher optionally supplied at
/// registration time (immutable after registration and required to be safe to
/// share across callers).
#[derive(Clone, Default)]
pub struct KernelDispatchOverlay {
    dispatchers: std::collections::BTreeMap<String, std::sync::Arc<dyn KernelOperationDispatcher>>,
}

impl KernelDispatchOverlay {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the dispatcher for an exact registered kernel package name; replaces
    /// any previous overlay entry.
    pub fn with_dispatcher(
        mut self,
        package: impl Into<String>,
        dispatcher: std::sync::Arc<dyn KernelOperationDispatcher>,
    ) -> Self {
        self.dispatchers.insert(package.into(), dispatcher);
        self
    }

    pub fn dispatcher(
        &self,
        package: &str,
    ) -> Option<std::sync::Arc<dyn KernelOperationDispatcher>> {
        self.dispatchers.get(package).cloned()
    }

    pub fn is_empty(&self) -> bool {
        self.dispatchers.is_empty()
    }
}

/// An operation-registry view scoped to one invoking context (EMO-550):
/// the shared registration state plus this caller's dispatch overlay.
///
/// Caller-scoped kernel-operation invocation goes through this view. Kernel
/// dispatch resolves from the overlay first, then from the dispatcher supplied
/// at registration, and fails with the existing "has no dispatcher in this
/// runtime" error when neither exists. Wasm entries are unaffected by the
/// overlay. Registration, listing, and describe stay on the underlying
/// [`OperationRegistry`].
#[derive(Clone)]
pub struct ScopedOperationRegistry {
    registry: std::sync::Arc<OperationRegistry>,
    overlay: KernelDispatchOverlay,
}

impl ScopedOperationRegistry {
    pub fn new(
        registry: std::sync::Arc<OperationRegistry>,
        overlay: KernelDispatchOverlay,
    ) -> Self {
        Self { registry, overlay }
    }

    /// The shared registration state (register/list/describe surfaces).
    pub fn registry(&self) -> &std::sync::Arc<OperationRegistry> {
        &self.registry
    }

    pub fn overlay(&self) -> &KernelDispatchOverlay {
        &self.overlay
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
        self.registry
            .invoke_entry_bytes_with_kernel_metadata(
                registered_name,
                operation_name,
                input.into(),
                kernel_metadata,
                self.overlay.dispatcher(registered_name),
            )
            .await
    }

    pub async fn invoke_process(
        &self,
        registered_name: &str,
        operation_name: &str,
        input: impl Into<Vec<u8>>,
    ) -> crate::VerletResult<verlet_process::process::VerletProcessHandle> {
        self.invoke_process_with_kernel_metadata(
            registered_name,
            operation_name,
            input,
            std::collections::BTreeMap::new(),
        )
        .await
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

#[cfg(test)]
mod tests {
    struct ThreadDispatcher {
        thread_id: &'static str,
        barrier: std::sync::Arc<std::sync::Barrier>,
    }

    #[async_trait::async_trait]
    impl super::KernelOperationDispatcher for ThreadDispatcher {
        async fn invoke_kernel_operation(
            &self,
            _operation_name: &str,
            _input: Vec<u8>,
        ) -> crate::VerletResult<Vec<u8>> {
            self.barrier.wait();
            Ok(self.thread_id.as_bytes().to_vec())
        }
    }

    #[test]
    fn scoped_registries_isolate_concurrent_kernel_dispatch() {
        let registry = std::sync::Arc::new(super::OperationRegistry::new());
        block_on(
            registry.register_kernel(super::KernelOperationRegistration::new(
                "kernel-package",
                kernel_manifest(),
            )),
        )
        .unwrap();

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let thread_a = super::ScopedOperationRegistry::new(
            std::sync::Arc::clone(&registry),
            super::KernelDispatchOverlay::new().with_dispatcher(
                "kernel-package",
                std::sync::Arc::new(ThreadDispatcher {
                    thread_id: "thread-a",
                    barrier: std::sync::Arc::clone(&barrier),
                }),
            ),
        );
        let thread_b = super::ScopedOperationRegistry::new(
            registry,
            super::KernelDispatchOverlay::new().with_dispatcher(
                "kernel-package",
                std::sync::Arc::new(ThreadDispatcher {
                    thread_id: "thread-b",
                    barrier,
                }),
            ),
        );

        let thread_a = std::thread::spawn(move || {
            block_on(thread_a.invoke_bytes("kernel-package", "identify-thread", Vec::new()))
                .unwrap()
                .output
        });
        let thread_b = std::thread::spawn(move || {
            block_on(thread_b.invoke_bytes("kernel-package", "identify-thread", Vec::new()))
                .unwrap()
                .output
        });

        assert_eq!(thread_a.join().unwrap(), b"thread-a");
        assert_eq!(thread_b.join().unwrap(), b"thread-b");
    }

    #[test]
    fn scoped_registry_prefers_overlay_then_falls_back_to_registration_dispatcher() {
        let registry = std::sync::Arc::new(super::OperationRegistry::new());
        block_on(
            registry.register_kernel(
                super::KernelOperationRegistration::new("kernel-package", kernel_manifest())
                    .with_dispatcher(std::sync::Arc::new(ThreadDispatcher {
                        thread_id: "registration",
                        barrier: std::sync::Arc::new(std::sync::Barrier::new(1)),
                    })),
            ),
        )
        .unwrap();
        let scoped = super::ScopedOperationRegistry::new(
            std::sync::Arc::clone(&registry),
            super::KernelDispatchOverlay::new().with_dispatcher(
                "kernel-package",
                std::sync::Arc::new(ThreadDispatcher {
                    thread_id: "overlay",
                    barrier: std::sync::Arc::new(std::sync::Barrier::new(1)),
                }),
            ),
        );

        let overlay_output =
            block_on(scoped.invoke_bytes("kernel-package", "identify-thread", Vec::new())).unwrap();
        let registration_output =
            block_on(registry.invoke_bytes("kernel-package", "identify-thread", Vec::new()))
                .unwrap();

        assert_eq!(overlay_output.output, b"overlay");
        assert_eq!(registration_output.output, b"registration");
    }

    fn kernel_manifest() -> verlet_abi::WasmOperationManifest {
        verlet_abi::WasmOperationManifest {
            abi: verlet_wasm::runner::OPERATION_ABI.to_string(),
            operations: vec![verlet_abi::WasmOperationDefinition {
                id: 1,
                name: "identify-thread".to_string(),
                input: verlet_abi::WasmOperationValueKind::Bytes,
                output: verlet_abi::WasmOperationValueKind::Bytes,
                events: verlet_abi::WasmOperationEventKind::None,
                mode: verlet_abi::WasmOperationMode::Sync,
                required_capabilities: Vec::new(),
            }],
        }
    }

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        struct ThreadWaker(std::thread::Thread);

        impl std::task::Wake for ThreadWaker {
            fn wake(self: std::sync::Arc<Self>) {
                self.0.unpark();
            }

            fn wake_by_ref(self: &std::sync::Arc<Self>) {
                self.0.unpark();
            }
        }

        let waker =
            std::task::Waker::from(std::sync::Arc::new(ThreadWaker(std::thread::current())));
        let mut context = std::task::Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);
        loop {
            match future.as_mut().poll(&mut context) {
                std::task::Poll::Ready(output) => return output,
                std::task::Poll::Pending => std::thread::park(),
            }
        }
    }
}
