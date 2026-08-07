use sha2::Digest as _;

pub const IMPORT_PACKAGE_FILE_NAME: &str = "verlet.import.toml";
pub const IMPORT_BUILD_RECEIPT_KIND: &str = "cooldis.import-build-receipt";
pub const IMPORT_BUILD_RECEIPT_SCHEMA_VERSION: u32 = 0;

/// Authoring manifest for one witnessed OpenAPI import batch.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportPackageManifest {
    pub import: ImportPackageIdentity,
    pub spec: ImportSpecDeclaration,
    #[serde(default)]
    pub auth: Option<ImportAuthDeclaration>,
    #[serde(default)]
    pub operations: Vec<ImportOperationDeclaration>,
}

/// Identity assigned to the published multi-operation record.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportPackageIdentity {
    pub name: String,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

/// Vendored OpenAPI document witness declared by an import package.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportSpecDeclaration {
    pub path: std::path::PathBuf,
    pub sha256: String,
}

/// Host-resolved credential wiring for imported HTTP operations.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportAuthDeclaration {
    pub scheme: String,
    pub header: String,
    pub secret: String,
}

/// Selection and optional projection overrides for one OpenAPI operation.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportOperationDeclaration {
    pub operation_id: String,
    #[serde(default)]
    pub alias: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

/// Loaded import manifest and its verified vendored specification bytes.
#[derive(Clone, Debug, PartialEq)]
pub struct ImportPackageSource {
    pub manifest_path: std::path::PathBuf,
    pub package_root: std::path::PathBuf,
    pub source_hash: String,
    pub spec_path: std::path::PathBuf,
    pub spec_sha256: String,
    pub spec_bytes: Vec<u8>,
    pub manifest: ImportPackageManifest,
}

impl ImportPackageSource {
    pub fn load(
        path: impl AsRef<std::path::Path>,
    ) -> Result<Self, crate::openapi_plan::OpenApiImportError> {
        let manifest_path = resolve_import_package_path(path.as_ref())?;
        let package_root = manifest_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .to_path_buf();
        let source = std::fs::read_to_string(&manifest_path).map_err(|source| {
            crate::openapi_plan::OpenApiImportError::ReadPackage {
                path: manifest_path.clone(),
                source,
            }
        })?;
        let mut manifest: ImportPackageManifest = toml::from_str(&source).map_err(|source| {
            crate::openapi_plan::OpenApiImportError::InvalidPackage {
                path: manifest_path.clone(),
                source,
            }
        })?;
        validate_manifest(&manifest)?;
        let spec_path = if manifest.spec.path.is_absolute() {
            manifest.spec.path.clone()
        } else {
            package_root.join(&manifest.spec.path)
        };
        manifest.spec.path = spec_path.clone();
        let expected = normalize_sha256(&manifest.spec.sha256)?;
        let spec_bytes = std::fs::read(&spec_path).map_err(|source| {
            crate::openapi_plan::OpenApiImportError::ReadSpec {
                path: spec_path.clone(),
                source,
            }
        })?;
        let actual = format!("{:x}", sha2::Sha256::digest(&spec_bytes));
        if actual != expected {
            return Err(crate::openapi_plan::OpenApiImportError::SpecHashMismatch {
                expected,
                actual,
            });
        }
        manifest.spec.sha256 = actual.clone();
        Ok(Self {
            manifest_path,
            package_root,
            source_hash: format!("sha256:{:x}", sha2::Sha256::digest(source.as_bytes())),
            spec_path,
            spec_sha256: actual,
            spec_bytes,
            manifest,
        })
    }
}

/// Deterministic build receipt emitted before an import batch is published.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ImportBuildReceipt {
    pub kind: String,
    pub schema_version: u32,
    pub name: String,
    pub source_hash: String,
    pub spec_sha256: String,
    pub artifact_hash: String,
    pub operations: Vec<ImportOperationBuild>,
    #[serde(default)]
    pub capabilities: std::collections::BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_path: Option<std::path::PathBuf>,
}

/// One operation row included in an import build receipt.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ImportOperationBuild {
    pub name: String,
    pub input_schema: serde_json::Value,
    pub output_schema: serde_json::Value,
}

fn resolve_import_package_path(
    path: &std::path::Path,
) -> Result<std::path::PathBuf, crate::openapi_plan::OpenApiImportError> {
    let path = if path.is_dir() {
        path.join(IMPORT_PACKAGE_FILE_NAME)
    } else {
        path.to_path_buf()
    };
    if path.exists() {
        Ok(path)
    } else {
        Err(crate::openapi_plan::OpenApiImportError::PackageNotFound { path })
    }
}

fn validate_manifest(
    manifest: &ImportPackageManifest,
) -> Result<(), crate::openapi_plan::OpenApiImportError> {
    let package_name = crate::validate_record_name(&manifest.import.name).map_err(|error| {
        crate::openapi_plan::OpenApiImportError::InvalidPackageIdentity {
            message: error.to_string(),
        }
    })?;
    if package_name != manifest.import.name {
        return Err(
            crate::openapi_plan::OpenApiImportError::InvalidPackageIdentity {
                message: "import package name must not contain surrounding whitespace".to_string(),
            },
        );
    }
    if manifest.operations.is_empty() {
        return Err(crate::openapi_plan::OpenApiImportError::EmptyOperationSelection);
    }
    for operation in &manifest.operations {
        if operation.operation_id.trim().is_empty() {
            return Err(crate::openapi_plan::OpenApiImportError::EmptyOperationId);
        }
        if let Some(alias) = &operation.alias {
            let normalized = crate::validate_record_name(alias).map_err(|error| {
                crate::openapi_plan::OpenApiImportError::InvalidOperationName {
                    name: alias.clone(),
                    message: error.to_string(),
                }
            })?;
            if normalized != *alias {
                return Err(
                    crate::openapi_plan::OpenApiImportError::InvalidOperationName {
                        name: alias.clone(),
                        message: "operation alias must not contain surrounding whitespace"
                            .to_string(),
                    },
                );
            }
        }
    }
    if let Some(auth) = &manifest.auth {
        if auth.scheme.trim().is_empty()
            || auth.header.trim().is_empty()
            || auth.secret.trim().is_empty()
        {
            return Err(
                crate::openapi_plan::OpenApiImportError::InvalidAuthentication {
                    message: "auth scheme, header, and secret must be non-empty".to_string(),
                },
            );
        }
        if !crate::openapi_plan::valid_http_header_name(&auth.header)
            || crate::openapi_plan::protected_http_header(&auth.header)
        {
            return Err(
                crate::openapi_plan::OpenApiImportError::InvalidAuthentication {
                    message: format!(
                        "auth header {:?} is invalid or reserved for HTTP routing/framing",
                        auth.header
                    ),
                },
            );
        }
        let secret = crate::validate_record_name(&auth.secret).map_err(|error| {
            crate::openapi_plan::OpenApiImportError::InvalidAuthentication {
                message: format!("invalid secret name {:?}: {error}", auth.secret),
            }
        })?;
        if secret != auth.secret {
            return Err(
                crate::openapi_plan::OpenApiImportError::InvalidAuthentication {
                    message: "auth secret name must not contain surrounding whitespace".to_string(),
                },
            );
        }
    }
    Ok(())
}

fn normalize_sha256(value: &str) -> Result<String, crate::openapi_plan::OpenApiImportError> {
    let value = value.strip_prefix("sha256:").unwrap_or(value);
    if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(value.to_ascii_lowercase())
    } else {
        Err(crate::openapi_plan::OpenApiImportError::InvalidSpecHash {
            value: value.to_string(),
        })
    }
}
