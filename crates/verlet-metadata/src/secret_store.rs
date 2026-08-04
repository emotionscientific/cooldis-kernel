use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use thiserror::Error;
use verlet_abi::WasmOperationManifest;
use verlet_history::now_ms;
use verlet_sqlite::{Connection, Db, DbConfig, params};

pub type SecretStoreResult<T> = Result<T, SecretStoreError>;

#[derive(Debug, Error)]
pub enum SecretStoreError {
    #[error("secret name cannot be empty")]
    EmptyName,
    #[error("secret name {0:?} is not valid")]
    InvalidName(String),
    #[error("secret value for {0} cannot be empty")]
    EmptyValue(String),
    #[error("environment variable {env_name} for secret {secret_name} is not configured")]
    MissingEnv {
        secret_name: String,
        env_name: String,
    },
    #[error("secret store failed: {0}")]
    Storage(String),
    #[error("secret store codec failed: {0}")]
    Codec(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretSourceKind {
    Env,
    Stdin,
    Local,
}

impl SecretSourceKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Env => "env",
            Self::Stdin => "stdin",
            Self::Local => "local",
        }
    }

    fn from_str(value: &str) -> SecretStoreResult<Self> {
        match value {
            "env" => Ok(Self::Env),
            "stdin" => Ok(Self::Stdin),
            "local" => Ok(Self::Local),
            other => Err(SecretStoreError::Codec(format!(
                "unknown secret source kind {other:?}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SecretStatus {
    pub name: String,
    pub source_kind: SecretSourceKind,
    pub source_label: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub value: RedactedSecretValue,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RedactedSecretValue {
    pub redacted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedSecret {
    pub name: String,
    pub value: String,
    pub source_kind: SecretSourceKind,
    pub source_label: Option<String>,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ManifestSecretResolution {
    pub values: BTreeMap<String, String>,
    pub missing: std::collections::BTreeSet<String>,
}

impl ManifestSecretResolution {
    pub fn is_ready(&self) -> bool {
        self.missing.is_empty()
    }
}

#[async_trait]
pub trait SecretResolver: Send + Sync + 'static {
    async fn resolve_secret(&self, name: &str) -> SecretStoreResult<Option<ResolvedSecret>>;
}

#[derive(Clone)]
pub struct SqliteSecretStore {
    inner: Db,
}

impl SqliteSecretStore {
    pub async fn open(path: impl AsRef<Path>) -> SecretStoreResult<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent()
            && !parent.exists()
        {
            std::fs::create_dir_all(parent).map_err(storage_error)?;
            restrict_dir_permissions(parent)?;
        }
        let inner = Db::open(path, DbConfig::default())
            .await
            .map_err(storage_error)?;
        restrict_file_permissions(path)?;
        Self::from_db(inner).await
    }

    pub async fn in_memory() -> SecretStoreResult<Self> {
        let inner = Db::in_memory(DbConfig::default())
            .await
            .map_err(storage_error)?;
        Self::from_db(inner).await
    }

    async fn from_db(inner: Db) -> SecretStoreResult<Self> {
        let store = Self { inner };
        let connection = store.inner.connect().await.map_err(storage_error)?;
        init_secret_store_schema(&connection).await?;
        Ok(store)
    }

    pub async fn set_secret(
        &self,
        name: impl AsRef<str>,
        value: impl Into<String>,
        source_kind: SecretSourceKind,
        source_label: Option<String>,
    ) -> SecretStoreResult<SecretStatus> {
        let name = validate_secret_name(name.as_ref())?;
        let value = value.into();
        if value.is_empty() {
            return Err(SecretStoreError::EmptyValue(name));
        }
        let now = now_ms();
        let connection = self.inner.connect().await.map_err(storage_error)?;
        connection
            .execute(
                r#"
                    INSERT INTO cooldis_secret_records (
                        name, value, source_kind, source_label, created_at_ms, updated_at_ms
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                    ON CONFLICT(name) DO UPDATE SET
                        value = excluded.value,
                        source_kind = excluded.source_kind,
                        source_label = excluded.source_label,
                        updated_at_ms = excluded.updated_at_ms
                    "#,
                params![
                    name.as_str(),
                    value,
                    source_kind.as_str(),
                    source_label,
                    now,
                    now
                ],
            )
            .await
            .map_err(storage_error)?;
        sqlite_secret_status_by_name(&connection, &name)
            .await?
            .ok_or_else(|| SecretStoreError::Storage(format!("secret {name:?} was not stored")))
    }

    pub async fn import_secret_from_env(
        &self,
        name: impl AsRef<str>,
        env_name: impl AsRef<str>,
    ) -> SecretStoreResult<SecretStatus> {
        let name = validate_secret_name(name.as_ref())?;
        let env_name = validate_secret_name(env_name.as_ref())?;
        let value = verlet_runtime_contracts::env_compat::var(&env_name).map_err(|_| {
            SecretStoreError::MissingEnv {
                secret_name: name.clone(),
                env_name: env_name.clone(),
            }
        })?;
        if value.is_empty() {
            return Err(SecretStoreError::EmptyValue(name));
        }
        self.set_secret(name, value, SecretSourceKind::Env, Some(env_name))
            .await
    }

    pub async fn status(&self, name: impl AsRef<str>) -> SecretStoreResult<Option<SecretStatus>> {
        let name = validate_secret_name(name.as_ref())?;
        let connection = self.inner.connect().await.map_err(storage_error)?;
        sqlite_secret_status_by_name(&connection, &name).await
    }

    pub async fn list(&self) -> SecretStoreResult<Vec<SecretStatus>> {
        let connection = self.inner.connect().await.map_err(storage_error)?;
        let mut rows = connection
            .query(
                r#"
                SELECT name, source_kind, source_label, created_at_ms, updated_at_ms
                FROM cooldis_secret_records
                ORDER BY name
                "#,
                (),
            )
            .await
            .map_err(storage_error)?;
        let mut statuses = Vec::new();
        while let Some(row) = rows.next().await.map_err(storage_error)? {
            statuses.push(sqlite_secret_status_from_row(&row)?);
        }
        Ok(statuses)
    }

    pub async fn delete_secret(&self, name: impl AsRef<str>) -> SecretStoreResult<bool> {
        let name = validate_secret_name(name.as_ref())?;
        let connection = self.inner.connect().await.map_err(storage_error)?;
        let deleted = connection
            .execute(
                "DELETE FROM cooldis_secret_records WHERE name = ?1",
                params![name],
            )
            .await
            .map_err(storage_error)?;
        Ok(deleted > 0)
    }
}

#[async_trait]
impl SecretResolver for SqliteSecretStore {
    async fn resolve_secret(&self, name: &str) -> SecretStoreResult<Option<ResolvedSecret>> {
        let name = validate_secret_name(name)?;
        let connection = self.inner.connect().await.map_err(storage_error)?;
        let mut rows = connection
            .query(
                r#"
                SELECT name, value, source_kind, source_label, updated_at_ms
                FROM cooldis_secret_records
                WHERE name = ?1
                "#,
                params![name],
            )
            .await
            .map_err(storage_error)?;
        rows.next()
            .await
            .map_err(storage_error)?
            .map(|row| {
                let source_kind = row.get::<String>(2).map_err(storage_error)?;
                Ok(ResolvedSecret {
                    name: row.get(0).map_err(storage_error)?,
                    value: row.get(1).map_err(storage_error)?,
                    source_kind: SecretSourceKind::from_str(&source_kind)?,
                    source_label: row.get(3).map_err(storage_error)?,
                    updated_at_ms: row.get(4).map_err(storage_error)?,
                })
            })
            .transpose()
    }
}

pub fn required_secret_names(
    manifest: &WasmOperationManifest,
) -> SecretStoreResult<std::collections::BTreeSet<String>> {
    let mut names = std::collections::BTreeSet::new();
    for operation in &manifest.operations {
        for capability in &operation.required_capabilities {
            if let Some(name) = capability.strip_prefix("secret:") {
                names.insert(validate_secret_name(name)?);
            }
        }
    }
    Ok(names)
}

pub async fn resolve_manifest_secrets(
    resolver: &dyn SecretResolver,
    manifest: &WasmOperationManifest,
) -> SecretStoreResult<BTreeMap<String, String>> {
    Ok(resolve_manifest_secret_resolution(resolver, manifest)
        .await?
        .values)
}

pub async fn resolve_manifest_secret_resolution(
    resolver: &dyn SecretResolver,
    manifest: &WasmOperationManifest,
) -> SecretStoreResult<ManifestSecretResolution> {
    let mut secrets = BTreeMap::new();
    let mut missing = std::collections::BTreeSet::new();
    for name in required_secret_names(manifest)? {
        if let Some(secret) = resolver.resolve_secret(&name).await? {
            secrets.insert(secret.name, secret.value);
        } else {
            missing.insert(name);
        }
    }
    Ok(ManifestSecretResolution {
        values: secrets,
        missing,
    })
}

pub fn validate_secret_name(name: &str) -> SecretStoreResult<String> {
    let name = name.trim();
    if name.is_empty() {
        return Err(SecretStoreError::EmptyName);
    }
    if name == "." || name == ".." || name.len() > 128 {
        return Err(SecretStoreError::InvalidName(name.to_string()));
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(SecretStoreError::InvalidName(name.to_string()));
    }
    Ok(name.to_string())
}

async fn init_secret_store_schema(connection: &Connection) -> SecretStoreResult<()> {
    connection
        .execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS cooldis_secret_records (
                name TEXT PRIMARY KEY NOT NULL,
                value TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                source_label TEXT,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );
            "#,
        )
        .await
        .map_err(storage_error)
}

async fn sqlite_secret_status_by_name(
    connection: &Connection,
    name: &str,
) -> SecretStoreResult<Option<SecretStatus>> {
    let mut rows = connection
        .query(
            r#"
            SELECT name, source_kind, source_label, created_at_ms, updated_at_ms
            FROM cooldis_secret_records
            WHERE name = ?1
            "#,
            params![name],
        )
        .await
        .map_err(storage_error)?;
    rows.next()
        .await
        .map_err(storage_error)?
        .map(|row| sqlite_secret_status_from_row(&row))
        .transpose()
}

fn sqlite_secret_status_from_row(row: &verlet_sqlite::Row) -> SecretStoreResult<SecretStatus> {
    let source_kind: String = row.get(1).map_err(storage_error)?;
    Ok(SecretStatus {
        name: row.get(0).map_err(storage_error)?,
        source_kind: SecretSourceKind::from_str(&source_kind)?,
        source_label: row.get(2).map_err(storage_error)?,
        created_at_ms: row.get(3).map_err(storage_error)?,
        updated_at_ms: row.get(4).map_err(storage_error)?,
        value: RedactedSecretValue { redacted: true },
    })
}

fn storage_error(err: impl std::fmt::Display) -> SecretStoreError {
    SecretStoreError::Storage(err.to_string())
}

#[cfg(unix)]
fn restrict_dir_permissions(path: &Path) -> SecretStoreResult<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).map_err(storage_error)
}

#[cfg(not(unix))]
fn restrict_dir_permissions(_path: &Path) -> SecretStoreResult<()> {
    Ok(())
}

#[cfg(unix)]
fn restrict_file_permissions(path: &Path) -> SecretStoreResult<()> {
    if path.exists() {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .map_err(storage_error)?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn restrict_file_permissions(_path: &Path) -> SecretStoreResult<()> {
    Ok(())
}

#[cfg(test)]
mod tests;
