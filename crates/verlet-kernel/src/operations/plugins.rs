#[derive(Clone, Debug)]
pub struct PluginMount {
    pub guest_path: std::path::PathBuf,
    pub host_path: std::path::PathBuf,
    pub mode: crate::HostFileSystemMode,
    expected_host_root: Option<std::path::PathBuf>,
}

impl PluginMount {
    pub fn host_read_only(
        guest_path: impl Into<std::path::PathBuf>,
        host_path: impl Into<std::path::PathBuf>,
    ) -> Self {
        Self {
            guest_path: guest_path.into(),
            host_path: host_path.into(),
            mode: crate::HostFileSystemMode::ReadOnly,
            expected_host_root: None,
        }
    }

    pub fn host_read_write(
        guest_path: impl Into<std::path::PathBuf>,
        host_path: impl Into<std::path::PathBuf>,
    ) -> Self {
        Self {
            guest_path: guest_path.into(),
            host_path: host_path.into(),
            mode: crate::HostFileSystemMode::ReadWrite,
            expected_host_root: None,
        }
    }

    pub(crate) fn pinned_host_read_only(
        guest_path: impl Into<std::path::PathBuf>,
        canonical_host_path: impl Into<std::path::PathBuf>,
    ) -> Self {
        let host_path = canonical_host_path.into();
        Self {
            guest_path: guest_path.into(),
            host_path: host_path.clone(),
            mode: crate::HostFileSystemMode::ReadOnly,
            expected_host_root: Some(host_path),
        }
    }

    pub(crate) fn pinned_host_read_write(
        guest_path: impl Into<std::path::PathBuf>,
        canonical_host_path: impl Into<std::path::PathBuf>,
    ) -> Self {
        let host_path = canonical_host_path.into();
        Self {
            guest_path: guest_path.into(),
            host_path: host_path.clone(),
            mode: crate::HostFileSystemMode::ReadWrite,
            expected_host_root: Some(host_path),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LocalPluginCatalogConfig {
    pub registry_root: std::path::PathBuf,
    pub operation_names: Vec<String>,
    pub mounts: Vec<PluginMount>,
}

impl LocalPluginCatalogConfig {
    pub fn new(registry_root: impl Into<std::path::PathBuf>) -> Self {
        Self {
            registry_root: registry_root.into(),
            operation_names: Vec::new(),
            mounts: Vec::new(),
        }
    }

    pub fn with_operation_name(mut self, name: impl Into<String>) -> Self {
        self.operation_names.push(name.into());
        self
    }

    pub fn with_mount(mut self, mount: PluginMount) -> Self {
        self.mounts.push(mount);
        self
    }
}

#[derive(Clone, Debug)]
pub struct LocalPluginCatalogRecord {
    pub record: crate::PublishedOperationRecord,
    pub operation_names: std::collections::BTreeSet<String>,
}

impl LocalPluginCatalogRecord {
    pub fn whole_record(record: crate::PublishedOperationRecord) -> Self {
        Self {
            record,
            operation_names: std::collections::BTreeSet::new(),
        }
    }

    pub fn selected_operations<I, S>(
        record: crate::PublishedOperationRecord,
        operation_names: I,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            record,
            operation_names: operation_names.into_iter().map(Into::into).collect(),
        }
    }
}

pub struct LocalPluginCatalog {
    operation_registry: std::sync::Arc<crate::OperationRegistry>,
    vfs: std::sync::Arc<crate::VerletVfs>,
    operations: Vec<crate::RegisteredOperation>,
}

impl LocalPluginCatalog {
    pub async fn load(config: LocalPluginCatalogConfig) -> crate::VerletResult<Self> {
        let registry_root = config.registry_root;
        let local_registry = crate::LocalOperationRegistry::new(registry_root.clone());

        let records = if config.operation_names.is_empty() {
            local_registry.list_records()?
        } else {
            let mut records = Vec::with_capacity(config.operation_names.len());
            for name in config.operation_names {
                records.push(local_registry.load_record(&name)?);
            }
            records
        };

        let records = records
            .into_iter()
            .map(LocalPluginCatalogRecord::whole_record)
            .collect();
        Self::load_catalog_records(registry_root, records, config.mounts, None).await
    }

    pub async fn load_records(
        registry_root: impl Into<std::path::PathBuf>,
        records: Vec<crate::PublishedOperationRecord>,
        mounts: Vec<PluginMount>,
    ) -> crate::VerletResult<Self> {
        let records = records
            .into_iter()
            .map(LocalPluginCatalogRecord::whole_record)
            .collect();
        Self::load_catalog_records(registry_root, records, mounts, None).await
    }

    pub async fn load_records_with_secret_resolver(
        registry_root: impl Into<std::path::PathBuf>,
        records: Vec<crate::PublishedOperationRecord>,
        mounts: Vec<PluginMount>,
        secret_resolver: std::sync::Arc<dyn crate::SecretResolver>,
    ) -> crate::VerletResult<Self> {
        let records = records
            .into_iter()
            .map(LocalPluginCatalogRecord::whole_record)
            .collect();
        Self::load_catalog_records(registry_root, records, mounts, Some(secret_resolver)).await
    }

    pub async fn load_selected_records(
        registry_root: impl Into<std::path::PathBuf>,
        records: Vec<LocalPluginCatalogRecord>,
        mounts: Vec<PluginMount>,
    ) -> crate::VerletResult<Self> {
        Self::load_catalog_records(registry_root, records, mounts, None).await
    }

    pub async fn load_selected_records_with_secret_resolver(
        registry_root: impl Into<std::path::PathBuf>,
        records: Vec<LocalPluginCatalogRecord>,
        mounts: Vec<PluginMount>,
        secret_resolver: std::sync::Arc<dyn crate::SecretResolver>,
    ) -> crate::VerletResult<Self> {
        Self::load_catalog_records(registry_root, records, mounts, Some(secret_resolver)).await
    }

    async fn load_catalog_records(
        registry_root: impl Into<std::path::PathBuf>,
        records: Vec<LocalPluginCatalogRecord>,
        mounts: Vec<PluginMount>,
        secret_resolver: Option<std::sync::Arc<dyn crate::SecretResolver>>,
    ) -> crate::VerletResult<Self> {
        let local_registry = crate::LocalOperationRegistry::new(registry_root);
        let limits = bashkit::FsLimits::default()
            .max_file_size(verlet_vbash::SPILL_RETENTION_MAX_BYTES as u64)
            .max_total_bytes(verlet_vbash::SPILL_VFS_MAX_BYTES as u64);
        let vfs = std::sync::Arc::new(crate::VerletVfs::new(std::sync::Arc::new(
            bashkit::InMemoryFs::with_limits(limits),
        )));
        mount_plugin_filesystems(&vfs, mounts)?;
        Self::from_records(local_registry, vfs, records, secret_resolver).await
    }

    async fn from_records(
        local_registry: crate::LocalOperationRegistry,
        vfs: std::sync::Arc<crate::VerletVfs>,
        records: Vec<LocalPluginCatalogRecord>,
        secret_resolver: Option<std::sync::Arc<dyn crate::SecretResolver>>,
    ) -> crate::VerletResult<Self> {
        let operation_registry = std::sync::Arc::new(crate::OperationRegistry::new());
        let mut operations = Vec::with_capacity(records.len());
        for selected_record in records {
            let LocalPluginCatalogRecord {
                record,
                operation_names,
            } = selected_record;
            if matches!(
                &record.source,
                crate::PublishedOperationSource::Kernel { .. }
            ) {
                let mut registration = crate::KernelOperationRegistration::new(
                    record.name.clone(),
                    record.manifest.clone(),
                )
                .with_capability_grants(record.capability_grants.clone())
                .with_operation_names(operation_names);
                registration.metadata = record.metadata;
                operations.push(operation_registry.register_kernel(registration).await?);
                continue;
            }
            let selected_manifest = if operation_names.is_empty() {
                record.manifest.clone()
            } else {
                crate::operations::operation_registry::filter_manifest_operations(
                    &record.name,
                    record.manifest.clone(),
                    &operation_names,
                )?
            };
            let mut runtime_config =
                local_registry.load_runtime_config_for_published_record(&record)?;
            if let Some(secret_resolver) = &secret_resolver {
                let resolution = crate::resolve_manifest_secret_resolution(
                    secret_resolver.as_ref(),
                    &selected_manifest,
                )
                .await
                .map_err(|err| {
                    crate::VerletError::RuntimeFactory(format!("secret store failed: {err}"))
                })?;
                if !resolution.is_ready() {
                    return Err(crate::VerletError::RuntimeFactory(format!(
                        "missing required operation secrets: {}; import with `verlet secret import <name> --from-env <ENV>` or `verlet secret set <name> --value-stdin`",
                        resolution
                            .missing
                            .iter()
                            .cloned()
                            .collect::<Vec<_>>()
                            .join(", ")
                    )));
                }
                runtime_config = runtime_config.with_secrets(resolution.values);
            };
            runtime_config = runtime_config.with_vfs(std::sync::Arc::clone(&vfs));
            let mut registration =
                crate::OperationRegistration::from_config(record.name.clone(), runtime_config);
            registration.metadata = record.metadata;
            operations.push(
                operation_registry
                    .register_prevalidated(registration, selected_manifest)
                    .await?,
            );
        }

        Ok(Self {
            operation_registry,
            vfs,
            operations,
        })
    }

    pub fn operation_registry(&self) -> std::sync::Arc<crate::OperationRegistry> {
        std::sync::Arc::clone(&self.operation_registry)
    }

    pub fn vfs(&self) -> std::sync::Arc<crate::VerletVfs> {
        std::sync::Arc::clone(&self.vfs)
    }

    pub fn operations(&self) -> &[crate::RegisteredOperation] {
        &self.operations
    }
}

fn mount_plugin_filesystems(
    vfs: &crate::VerletVfs,
    mounts: Vec<PluginMount>,
) -> crate::VerletResult<()> {
    for mount in mounts {
        if !mount.guest_path.has_root() {
            return Err(crate::VerletError::RuntimeFactory(format!(
                "plugin mount guest path must be absolute: {}",
                mount.guest_path.display()
            )));
        }
        let normalized_guest_path = bashkit::normalize_path(&mount.guest_path);
        if normalized_guest_path.starts_with(std::path::Path::new("/spill")) {
            return Err(crate::VerletError::RuntimeFactory(
                "plugin mount guest path /spill and its descendants are reserved for tool output spill"
                    .to_string(),
            ));
        }
        let fs = crate::HostFileSystem::new(&mount.host_path, mount.mode).map_err(|err| {
            crate::VerletError::RuntimeFactory(format!(
                "failed to open host plugin mount {}: {err}",
                mount.host_path.display()
            ))
        })?;
        if let Some(expected) = &mount.expected_host_root
            && fs.root() != expected
        {
            return Err(crate::VerletError::RuntimeFactory(format!(
                "host plugin mount {} resolved to {}, not its witnessed canonical root",
                mount.host_path.display(),
                fs.root().display()
            )));
        }
        let fs = std::sync::Arc::new(fs);
        vfs.mount(&mount.guest_path, fs).map_err(|err| {
            crate::VerletError::RuntimeFactory(format!(
                "failed to mount host path {} at {}: {err}",
                mount.host_path.display(),
                mount.guest_path.display()
            ))
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
