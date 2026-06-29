use cooldis_abi::WasmOperationManifest;
use cooldis_history::now_ms;
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use thiserror::Error;

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

pub trait SecretResolver: Send + Sync + 'static {
    fn resolve_secret(&self, name: &str) -> SecretStoreResult<Option<ResolvedSecret>>;
}

#[derive(Clone)]
pub struct SqliteSecretStore {
    inner: Arc<Mutex<rusqlite::Connection>>,
}

impl SqliteSecretStore {
    pub fn open(path: impl AsRef<Path>) -> SecretStoreResult<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent()
            && !parent.exists()
        {
            std::fs::create_dir_all(parent).map_err(storage_error)?;
            restrict_dir_permissions(parent)?;
        }
        let connection = rusqlite::Connection::open(path).map_err(storage_error)?;
        restrict_file_permissions(path)?;
        Self::from_connection(connection)
    }

    pub fn in_memory() -> SecretStoreResult<Self> {
        let connection = rusqlite::Connection::open_in_memory().map_err(storage_error)?;
        Self::from_connection(connection)
    }

    fn from_connection(connection: rusqlite::Connection) -> SecretStoreResult<Self> {
        init_secret_store_schema(&connection)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(connection)),
        })
    }

    pub fn set_secret(
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
        let connection = self.lock_connection()?;
        let existing_created_at_ms = connection
            .query_row(
                "SELECT created_at_ms FROM cooldis_secret_records WHERE name = ?1",
                params![name],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(storage_error)?;
        let created_at_ms = existing_created_at_ms.unwrap_or(now);
        connection
            .execute(
                r#"
                INSERT INTO cooldis_secret_records (
                    name,
                    value,
                    source_kind,
                    source_label,
                    created_at_ms,
                    updated_at_ms
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                ON CONFLICT(name) DO UPDATE SET
                    value = excluded.value,
                    source_kind = excluded.source_kind,
                    source_label = excluded.source_label,
                    updated_at_ms = excluded.updated_at_ms
                "#,
                params![
                    name,
                    value,
                    source_kind.as_str(),
                    source_label,
                    created_at_ms,
                    now
                ],
            )
            .map_err(storage_error)?;
        sqlite_secret_status_by_name(&connection, &name)?
            .ok_or_else(|| SecretStoreError::Storage(format!("secret {name:?} was not stored")))
    }

    pub fn import_secret_from_env(
        &self,
        name: impl AsRef<str>,
        env_name: impl AsRef<str>,
    ) -> SecretStoreResult<SecretStatus> {
        let name = validate_secret_name(name.as_ref())?;
        let env_name = validate_secret_name(env_name.as_ref())?;
        let value = std::env::var(&env_name).map_err(|_| SecretStoreError::MissingEnv {
            secret_name: name.clone(),
            env_name: env_name.clone(),
        })?;
        if value.is_empty() {
            return Err(SecretStoreError::EmptyValue(name));
        }
        self.set_secret(name, value, SecretSourceKind::Env, Some(env_name))
    }

    pub fn status(&self, name: impl AsRef<str>) -> SecretStoreResult<Option<SecretStatus>> {
        let name = validate_secret_name(name.as_ref())?;
        let connection = self.lock_connection()?;
        sqlite_secret_status_by_name(&connection, &name)
    }

    pub fn list(&self) -> SecretStoreResult<Vec<SecretStatus>> {
        let connection = self.lock_connection()?;
        let mut statement = connection
            .prepare(
                r#"
                SELECT name, source_kind, source_label, created_at_ms, updated_at_ms
                FROM cooldis_secret_records
                ORDER BY name
                "#,
            )
            .map_err(storage_error)?;
        let rows = statement
            .query_map([], sqlite_secret_status_from_row)
            .map_err(storage_error)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(storage_error)
    }

    pub fn delete_secret(&self, name: impl AsRef<str>) -> SecretStoreResult<bool> {
        let name = validate_secret_name(name.as_ref())?;
        let connection = self.lock_connection()?;
        let deleted = connection
            .execute(
                "DELETE FROM cooldis_secret_records WHERE name = ?1",
                params![name],
            )
            .map_err(storage_error)?;
        Ok(deleted > 0)
    }

    fn lock_connection(
        &self,
    ) -> SecretStoreResult<std::sync::MutexGuard<'_, rusqlite::Connection>> {
        self.inner.lock().map_err(|err| {
            SecretStoreError::Storage(format!("sqlite connection lock poisoned: {err}"))
        })
    }
}

impl SecretResolver for SqliteSecretStore {
    fn resolve_secret(&self, name: &str) -> SecretStoreResult<Option<ResolvedSecret>> {
        let name = validate_secret_name(name)?;
        let connection = self.lock_connection()?;
        connection
            .query_row(
                r#"
                SELECT name, value, source_kind, source_label, updated_at_ms
                FROM cooldis_secret_records
                WHERE name = ?1
                "#,
                params![name],
                |row| {
                    let source_kind: String = row.get(2)?;
                    Ok(ResolvedSecret {
                        name: row.get(0)?,
                        value: row.get(1)?,
                        source_kind: SecretSourceKind::from_str(&source_kind).map_err(|err| {
                            rusqlite::Error::FromSqlConversionFailure(
                                2,
                                rusqlite::types::Type::Text,
                                Box::new(err),
                            )
                        })?,
                        source_label: row.get(3)?,
                        updated_at_ms: row.get(4)?,
                    })
                },
            )
            .optional()
            .map_err(storage_error)
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

pub fn resolve_manifest_secrets(
    resolver: &dyn SecretResolver,
    manifest: &WasmOperationManifest,
) -> SecretStoreResult<BTreeMap<String, String>> {
    Ok(resolve_manifest_secret_resolution(resolver, manifest)?.values)
}

pub fn resolve_manifest_secret_resolution(
    resolver: &dyn SecretResolver,
    manifest: &WasmOperationManifest,
) -> SecretStoreResult<ManifestSecretResolution> {
    let mut secrets = BTreeMap::new();
    let mut missing = std::collections::BTreeSet::new();
    for name in required_secret_names(manifest)? {
        if let Some(secret) = resolver.resolve_secret(&name)? {
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

fn init_secret_store_schema(connection: &rusqlite::Connection) -> SecretStoreResult<()> {
    connection
        .execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;

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
        .map_err(storage_error)
}

fn sqlite_secret_status_by_name(
    connection: &rusqlite::Connection,
    name: &str,
) -> SecretStoreResult<Option<SecretStatus>> {
    connection
        .query_row(
            r#"
            SELECT name, source_kind, source_label, created_at_ms, updated_at_ms
            FROM cooldis_secret_records
            WHERE name = ?1
            "#,
            params![name],
            sqlite_secret_status_from_row,
        )
        .optional()
        .map_err(storage_error)
}

fn sqlite_secret_status_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SecretStatus> {
    let source_kind: String = row.get(1)?;
    Ok(SecretStatus {
        name: row.get(0)?,
        source_kind: SecretSourceKind::from_str(&source_kind).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(err))
        })?,
        source_label: row.get(2)?,
        created_at_ms: row.get(3)?,
        updated_at_ms: row.get(4)?,
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
