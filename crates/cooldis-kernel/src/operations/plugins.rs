use crate::{
    CooldisError, CooldisResult, CooldisVfs, HostFileSystem, HostFileSystemMode,
    KernelOperationRegistration, LocalOperationRegistry, OperationRegistration, OperationRegistry,
    PublishedOperationRecord, PublishedOperationSource, RegisteredOperation, SecretResolver,
    resolve_manifest_secret_resolution,
};
use bashkit::InMemoryFs;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use super::operation_registry::filter_manifest_operations;

#[derive(Clone, Debug)]
pub struct PluginMount {
    pub guest_path: PathBuf,
    pub host_path: PathBuf,
    pub mode: HostFileSystemMode,
}

impl PluginMount {
    pub fn host_read_only(guest_path: impl Into<PathBuf>, host_path: impl Into<PathBuf>) -> Self {
        Self {
            guest_path: guest_path.into(),
            host_path: host_path.into(),
            mode: HostFileSystemMode::ReadOnly,
        }
    }

    pub fn host_read_write(guest_path: impl Into<PathBuf>, host_path: impl Into<PathBuf>) -> Self {
        Self {
            guest_path: guest_path.into(),
            host_path: host_path.into(),
            mode: HostFileSystemMode::ReadWrite,
        }
    }
}

#[derive(Clone, Debug)]
pub struct LocalPluginCatalogConfig {
    pub registry_root: PathBuf,
    pub operation_names: Vec<String>,
    pub mounts: Vec<PluginMount>,
}

impl LocalPluginCatalogConfig {
    pub fn new(registry_root: impl Into<PathBuf>) -> Self {
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
    pub record: PublishedOperationRecord,
    pub operation_names: BTreeSet<String>,
}

impl LocalPluginCatalogRecord {
    pub fn whole_record(record: PublishedOperationRecord) -> Self {
        Self {
            record,
            operation_names: BTreeSet::new(),
        }
    }

    pub fn selected_operations<I, S>(record: PublishedOperationRecord, operation_names: I) -> Self
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
    operation_registry: Arc<OperationRegistry>,
    vfs: Arc<CooldisVfs>,
    operations: Vec<RegisteredOperation>,
}

impl LocalPluginCatalog {
    pub async fn load(config: LocalPluginCatalogConfig) -> CooldisResult<Self> {
        let registry_root = config.registry_root;
        let local_registry = LocalOperationRegistry::new(registry_root.clone());

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
        registry_root: impl Into<PathBuf>,
        records: Vec<PublishedOperationRecord>,
        mounts: Vec<PluginMount>,
    ) -> CooldisResult<Self> {
        let records = records
            .into_iter()
            .map(LocalPluginCatalogRecord::whole_record)
            .collect();
        Self::load_catalog_records(registry_root, records, mounts, None).await
    }

    pub async fn load_records_with_secret_resolver(
        registry_root: impl Into<PathBuf>,
        records: Vec<PublishedOperationRecord>,
        mounts: Vec<PluginMount>,
        secret_resolver: Arc<dyn SecretResolver>,
    ) -> CooldisResult<Self> {
        let records = records
            .into_iter()
            .map(LocalPluginCatalogRecord::whole_record)
            .collect();
        Self::load_catalog_records(registry_root, records, mounts, Some(secret_resolver)).await
    }

    pub async fn load_selected_records(
        registry_root: impl Into<PathBuf>,
        records: Vec<LocalPluginCatalogRecord>,
        mounts: Vec<PluginMount>,
    ) -> CooldisResult<Self> {
        Self::load_catalog_records(registry_root, records, mounts, None).await
    }

    pub async fn load_selected_records_with_secret_resolver(
        registry_root: impl Into<PathBuf>,
        records: Vec<LocalPluginCatalogRecord>,
        mounts: Vec<PluginMount>,
        secret_resolver: Arc<dyn SecretResolver>,
    ) -> CooldisResult<Self> {
        Self::load_catalog_records(registry_root, records, mounts, Some(secret_resolver)).await
    }

    async fn load_catalog_records(
        registry_root: impl Into<PathBuf>,
        records: Vec<LocalPluginCatalogRecord>,
        mounts: Vec<PluginMount>,
        secret_resolver: Option<Arc<dyn SecretResolver>>,
    ) -> CooldisResult<Self> {
        let local_registry = LocalOperationRegistry::new(registry_root);
        let vfs = Arc::new(CooldisVfs::new(Arc::new(InMemoryFs::new())));
        mount_plugin_filesystems(&vfs, mounts)?;
        Self::from_records(local_registry, vfs, records, secret_resolver).await
    }

    async fn from_records(
        local_registry: LocalOperationRegistry,
        vfs: Arc<CooldisVfs>,
        records: Vec<LocalPluginCatalogRecord>,
        secret_resolver: Option<Arc<dyn SecretResolver>>,
    ) -> CooldisResult<Self> {
        let operation_registry = Arc::new(OperationRegistry::new());
        let mut operations = Vec::with_capacity(records.len());
        for selected_record in records {
            let LocalPluginCatalogRecord {
                record,
                operation_names,
            } = selected_record;
            if matches!(&record.source, PublishedOperationSource::Kernel { .. }) {
                let mut registration =
                    KernelOperationRegistration::new(record.name.clone(), record.manifest.clone())
                        .with_capability_grants(record.capability_grants.clone())
                        .with_operation_names(operation_names);
                registration.metadata = record.metadata;
                operations.push(operation_registry.register_kernel(registration).await?);
                continue;
            }
            let selected_manifest = if operation_names.is_empty() {
                record.manifest.clone()
            } else {
                filter_manifest_operations(&record.name, record.manifest.clone(), &operation_names)?
            };
            let mut runtime_config =
                local_registry.load_runtime_config_for_published_record(&record)?;
            if let Some(secret_resolver) = &secret_resolver {
                let resolution = resolve_manifest_secret_resolution(
                    secret_resolver.as_ref(),
                    &selected_manifest,
                )
                .await
                .map_err(|err| {
                    CooldisError::RuntimeFactory(format!("secret store failed: {err}"))
                })?;
                if !resolution.is_ready() {
                    return Err(CooldisError::RuntimeFactory(format!(
                        "missing required operation secrets: {}; import with `cooldis secret import <name> --from-env <ENV>` or `cooldis secret set <name> --value-stdin`",
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
            runtime_config = runtime_config.with_vfs(Arc::clone(&vfs));
            let mut registration =
                OperationRegistration::from_config(record.name.clone(), runtime_config);
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

    pub fn operation_registry(&self) -> Arc<OperationRegistry> {
        Arc::clone(&self.operation_registry)
    }

    pub fn vfs(&self) -> Arc<CooldisVfs> {
        Arc::clone(&self.vfs)
    }

    pub fn operations(&self) -> &[RegisteredOperation] {
        &self.operations
    }
}

fn mount_plugin_filesystems(vfs: &CooldisVfs, mounts: Vec<PluginMount>) -> CooldisResult<()> {
    for mount in mounts {
        if !mount.guest_path.has_root() {
            return Err(CooldisError::RuntimeFactory(format!(
                "plugin mount guest path must be absolute: {}",
                mount.guest_path.display()
            )));
        }
        let fs = Arc::new(
            HostFileSystem::new(&mount.host_path, mount.mode).map_err(|err| {
                CooldisError::RuntimeFactory(format!(
                    "failed to open host plugin mount {}: {err}",
                    mount.host_path.display()
                ))
            })?,
        );
        vfs.mount(&mount.guest_path, fs).map_err(|err| {
            CooldisError::RuntimeFactory(format!(
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
