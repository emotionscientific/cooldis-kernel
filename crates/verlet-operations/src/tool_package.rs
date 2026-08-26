use sha2::Digest as _;

pub const TOOL_PACKAGE_KIND: &str = "cooldis.tool";
pub const TOOL_PACKAGE_SCHEMA_VERSION: u32 = 0;
pub const TOOL_BUILD_RECEIPT_KIND: &str = "cooldis.tool-build-receipt";
pub const TOOL_BUILD_RECEIPT_SCHEMA_VERSION: u32 = 0;
pub const TOOL_MANUAL_SCHEMA_VERSION: u32 = 0;
const TOOL_PACKAGE_FILE_NAME: &str = "verlet.tool.toml";

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ToolPackageManifest {
    pub kind: String,
    pub schema_version: u32,
    pub identity: ToolPackageIdentity,
    pub runtime: ToolRuntimeContract,
    #[serde(default)]
    pub operations: Vec<ToolOperationDeclaration>,
    #[serde(default)]
    pub fixtures: Vec<ToolFixtureDeclaration>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ToolPackageIdentity {
    pub name: String,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub owner: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ToolRuntimeContract {
    pub kind: String,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub module_path: Option<std::path::PathBuf>,
    #[serde(default)]
    pub bin_path: Option<std::path::PathBuf>,
    #[serde(default)]
    pub release: Option<bool>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub max_input_bytes: Option<u64>,
    #[serde(default)]
    pub max_output_bytes: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ToolOperationDeclaration {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub input_schema: std::path::PathBuf,
    pub output_schema: std::path::PathBuf,
    #[serde(default)]
    pub required_capabilities: std::collections::BTreeSet<String>,
    #[serde(default)]
    pub command: Option<ToolCommandContract>,
    #[serde(default)]
    pub mcp: Option<ToolMcpContract>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ToolCommandContract {
    pub name: String,
    #[serde(default)]
    pub stdin: Option<String>,
    #[serde(default)]
    pub stdout: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ToolMcpContract {
    pub tool_name: String,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ToolFixtureDeclaration {
    pub name: String,
    pub operation: String,
    pub input: std::path::PathBuf,
    pub expect: std::path::PathBuf,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ToolPackageSource {
    pub manifest_path: std::path::PathBuf,
    pub package_root: std::path::PathBuf,
    pub source_hash: String,
    pub manifest: ToolPackageManifest,
}

impl ToolPackageSource {
    pub fn load(path: impl AsRef<std::path::Path>) -> crate::VerletResult<Self> {
        let manifest_path = resolve_tool_package_path(path.as_ref())?;
        let package_root = manifest_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .to_path_buf();
        let source = std::fs::read_to_string(&manifest_path).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to read tool package {}: {err}",
                manifest_path.display()
            ))
        })?;
        let source_hash = text_sha256(source.as_bytes());
        let mut manifest: ToolPackageManifest = toml::from_str(&source).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "invalid tool package {}: {err}",
                manifest_path.display()
            ))
        })?;
        manifest.relativize_paths(&package_root);
        manifest.validate()?;
        Ok(Self {
            manifest_path,
            package_root,
            source_hash,
            manifest,
        })
    }
}

impl ToolPackageManifest {
    pub fn validate(&self) -> crate::VerletResult<()> {
        if self.kind != TOOL_PACKAGE_KIND {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "tool package kind must be {TOOL_PACKAGE_KIND:?}, got {:?}",
                self.kind
            )));
        }
        if self.schema_version != TOOL_PACKAGE_SCHEMA_VERSION {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "tool package schema_version {} is not supported",
                self.schema_version
            )));
        }
        crate::operation_store::validate_record_name(&self.identity.name)?;
        validate_runtime_contract(&self.runtime)?;
        if self.operations.is_empty() {
            return Err(crate::VerletOperationsError::RuntimeFactory(
                "tool package must declare at least one operation".to_string(),
            ));
        }
        let mut operation_names = std::collections::BTreeSet::new();
        for operation in &self.operations {
            crate::operation_store::validate_record_name(&operation.name)?;
            if !operation_names.insert(operation.name.clone()) {
                return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                    "tool package operation {:?} is duplicated",
                    operation.name
                )));
            }
            if operation.command.is_none() && operation.mcp.is_none() {
                return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                    "tool package operation {:?} must declare at least one command or MCP surface",
                    operation.name
                )));
            }
        }
        for fixture in &self.fixtures {
            crate::operation_store::validate_record_name(&fixture.name)?;
            if !operation_names.contains(&fixture.operation) {
                return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                    "tool package fixture {:?} references unknown operation {:?}",
                    fixture.name, fixture.operation
                )));
            }
        }
        Ok(())
    }

    fn relativize_paths(&mut self, base: &std::path::Path) {
        if let Some(path) = self.runtime.module_path.take() {
            self.runtime.module_path = Some(resolve_relative_path(base, path));
        }
        if let Some(path) = self.runtime.bin_path.take() {
            self.runtime.bin_path = Some(resolve_relative_path(base, path));
        }
        for operation in &mut self.operations {
            operation.input_schema = resolve_relative_path(base, operation.input_schema.clone());
            operation.output_schema = resolve_relative_path(base, operation.output_schema.clone());
        }
        for fixture in &mut self.fixtures {
            fixture.input = resolve_relative_path(base, fixture.input.clone());
            fixture.expect = resolve_relative_path(base, fixture.expect.clone());
        }
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ToolInterfaceContract {
    pub schema_version: u32,
    pub identity: ToolPackageIdentity,
    pub runtime: ToolRuntimeContract,
    pub operations: Vec<ToolOperationInterface>,
    pub fixtures: Vec<ToolFixtureContract>,
}

impl ToolInterfaceContract {
    pub fn from_package(
        package: &ToolPackageSource,
        manifest: &verlet_abi::WasmOperationManifest,
        projections: &crate::OperationProjectionSet,
    ) -> crate::VerletResult<Self> {
        let mut operations = Vec::with_capacity(package.manifest.operations.len());
        for operation in &package.manifest.operations {
            let wasm_operation = manifest.operation(&operation.name).ok_or_else(|| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "tool package operation {:?} is not exported by Wasm manifest",
                    operation.name
                ))
            })?;
            projections
                .operations
                .iter()
                .find(|projection| projection.operation_name == operation.name)
                .ok_or_else(|| {
                    crate::VerletOperationsError::RuntimeFactory(format!(
                        "tool package operation {:?} has no generated projection",
                        operation.name
                    ))
                })?;
            let mut missing_manifest_capabilities = Vec::new();
            for capability in &wasm_operation.required_capabilities {
                if !operation.required_capabilities.contains(capability) {
                    missing_manifest_capabilities.push(capability.clone());
                }
            }
            if !missing_manifest_capabilities.is_empty() {
                return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                    "tool package operation {:?} is missing required capabilities declared by Wasm manifest: {}",
                    operation.name,
                    missing_manifest_capabilities.join(", ")
                )));
            }
            let input_schema = read_json_schema(
                &operation.input_schema,
                &format!("tool package operation {:?} input_schema", operation.name),
            )?;
            let output_schema = read_json_schema(
                &operation.output_schema,
                &format!("tool package operation {:?} output_schema", operation.name),
            )?;
            validate_operation_json_fixtures(
                operation,
                &wasm_operation.input,
                &wasm_operation.output,
                &input_schema,
                &output_schema,
                package.manifest.fixtures.iter(),
            )?;
            let manual = ToolOperationManual::from_operation(
                &package.manifest.identity,
                operation,
                &input_schema,
                &output_schema,
                package.manifest.fixtures.iter(),
            );
            operations.push(ToolOperationInterface {
                name: operation.name.clone(),
                description: operation.description.clone(),
                input_schema,
                output_schema,
                required_capabilities: operation.required_capabilities.clone(),
                command: operation.command.clone(),
                mcp: operation.mcp.clone(),
                manual: Some(manual),
            });
        }
        Ok(Self {
            schema_version: TOOL_PACKAGE_SCHEMA_VERSION,
            identity: package.manifest.identity.clone(),
            runtime: package.manifest.runtime.clone(),
            operations,
            fixtures: package
                .manifest
                .fixtures
                .iter()
                .map(|fixture| ToolFixtureContract {
                    name: fixture.name.clone(),
                    operation: fixture.operation.clone(),
                    input: fixture.input.clone(),
                    expect: fixture.expect.clone(),
                })
                .collect(),
        })
    }

    pub fn capability_requests(&self) -> std::collections::BTreeSet<String> {
        self.operations
            .iter()
            .flat_map(|operation| operation.required_capabilities.iter().cloned())
            .collect()
    }

    pub fn validate_against_operation_record(
        &self,
        record_name: &str,
        manifest: &verlet_abi::WasmOperationManifest,
        projections: &crate::OperationProjectionSet,
    ) -> crate::VerletResult<()> {
        if self.identity.name != record_name {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "tool interface identity {:?} does not match operation record {:?}",
                self.identity.name, record_name
            )));
        }
        for operation in &self.operations {
            let wasm_operation = manifest.operation(&operation.name).ok_or_else(|| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "tool interface operation {:?} is not exported by Wasm manifest",
                    operation.name
                ))
            })?;
            for capability in &wasm_operation.required_capabilities {
                if !operation.required_capabilities.contains(capability) {
                    return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                        "tool interface operation {:?} is missing Wasm capability {capability:?}",
                        operation.name
                    )));
                }
            }
            if let Some(mcp) = &operation.mcp {
                let projection = projections
                    .operations
                    .iter()
                    .find(|projection| projection.operation_name == operation.name)
                    .ok_or_else(|| {
                        crate::VerletOperationsError::RuntimeFactory(format!(
                            "tool interface operation {:?} has no projection",
                            operation.name
                        ))
                    })?;
                if projection.mcp.tool_name != mcp.tool_name {
                    return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                        "tool interface operation {:?} MCP tool {:?} does not match stored projection {:?}",
                        operation.name, mcp.tool_name, projection.mcp.tool_name
                    )));
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ToolOperationInterface {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub input_schema: serde_json::Value,
    pub output_schema: serde_json::Value,
    #[serde(default)]
    pub required_capabilities: std::collections::BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<ToolCommandContract>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp: Option<ToolMcpContract>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manual: Option<ToolOperationManual>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ToolOperationManual {
    pub schema_version: u32,
    pub tool_name: String,
    pub operation_name: String,
    pub summary: String,
    pub usage: Vec<String>,
    pub input_schema: serde_json::Value,
    pub output_schema: serde_json::Value,
    #[serde(default)]
    pub required_capabilities: std::collections::BTreeSet<String>,
    #[serde(default)]
    pub examples: Vec<ToolManualExample>,
    #[serde(default)]
    pub exit_status: Vec<ToolManualExitStatus>,
    pub generated: bool,
    #[serde(default)]
    pub warnings: Vec<String>,
}

impl ToolOperationManual {
    fn from_operation<'a>(
        identity: &ToolPackageIdentity,
        operation: &ToolOperationDeclaration,
        input_schema: &serde_json::Value,
        output_schema: &serde_json::Value,
        fixtures: impl Iterator<Item = &'a ToolFixtureDeclaration>,
    ) -> Self {
        let mut warnings = Vec::new();
        let generated = operation.description.is_none();
        let summary = operation.description.clone().unwrap_or_else(|| {
            warnings.push(format!(
                "operation {} has no description; generated fallback manual summary",
                operation.name
            ));
            format!(
                "Run {} from {}.",
                operation.name,
                identity
                    .description
                    .as_deref()
                    .unwrap_or("this Verlet tool package")
            )
        });
        let mut usage = Vec::new();
        if let Some(command) = &operation.command {
            usage.push(match command.stdin.as_deref() {
                Some("json") | Some("text") | Some("bytes") => {
                    format!("printf '<input>' | {}", command.name)
                }
                _ => command.name.clone(),
            });
        }
        usage.push(format!(
            "verlet tool run {} {} --input '<input>'",
            identity.name, operation.name
        ));

        let examples = fixtures
            .filter(|fixture| fixture.operation == operation.name)
            .map(|fixture| ToolManualExample {
                name: fixture.name.clone(),
                command: operation
                    .command
                    .as_ref()
                    .map(|command| format!("{} < {}", command.name, fixture.input.display())),
            })
            .collect::<Vec<_>>();
        if examples.is_empty() {
            warnings.push(format!(
                "operation {} has no fixtures; manual has no examples",
                operation.name
            ));
        }

        Self {
            schema_version: TOOL_MANUAL_SCHEMA_VERSION,
            tool_name: identity.name.clone(),
            operation_name: operation.name.clone(),
            summary,
            usage,
            input_schema: input_schema.clone(),
            output_schema: output_schema.clone(),
            required_capabilities: operation.required_capabilities.clone(),
            examples,
            exit_status: default_manual_exit_status(),
            generated,
            warnings,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ToolManualExample {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ToolManualExitStatus {
    pub code: u8,
    pub meaning: String,
}

fn default_manual_exit_status() -> Vec<ToolManualExitStatus> {
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

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ToolFixtureContract {
    pub name: String,
    pub operation: String,
    pub input: std::path::PathBuf,
    pub expect: std::path::PathBuf,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ToolBuildReceipt {
    pub kind: String,
    pub schema_version: u32,
    pub name: String,
    pub source_hash: String,
    pub interface_hash: String,
    pub runtime: ToolRuntimeContract,
    pub operations: Vec<ToolOperationBuild>,
    #[serde(default)]
    pub capabilities: std::collections::BTreeSet<String>,
    #[serde(default)]
    pub fixtures: Vec<ToolFixtureRun>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_path: Option<std::path::PathBuf>,
}

impl ToolBuildReceipt {
    pub fn new(
        source: &ToolPackageSource,
        interface: &ToolInterfaceContract,
        projections: &crate::OperationProjectionSet,
        fixtures: Vec<ToolFixtureRun>,
        artifact_path: Option<std::path::PathBuf>,
    ) -> crate::VerletResult<Self> {
        let interface_hash = value_sha256(interface)?;
        let operations = interface
            .operations
            .iter()
            .map(|operation| {
                let projection = projections
                    .operations
                    .iter()
                    .find(|projection| projection.operation_name == operation.name)
                    .ok_or_else(|| {
                        crate::VerletOperationsError::RuntimeFactory(format!(
                            "tool build operation {:?} has no projection",
                            operation.name
                        ))
                    })?;
                Ok(ToolOperationBuild {
                    name: operation.name.clone(),
                    input: serde_json::to_value(&projection.input)
                        .unwrap_or(serde_json::Value::Null),
                    output: serde_json::to_value(&projection.output)
                        .unwrap_or(serde_json::Value::Null),
                    command: operation.command.clone(),
                    mcp: operation.mcp.clone(),
                })
            })
            .collect::<crate::VerletResult<Vec<_>>>()?;
        Ok(Self {
            kind: TOOL_BUILD_RECEIPT_KIND.to_string(),
            schema_version: TOOL_BUILD_RECEIPT_SCHEMA_VERSION,
            name: source.manifest.identity.name.clone(),
            source_hash: source.source_hash.clone(),
            interface_hash,
            runtime: interface.runtime.clone(),
            operations,
            capabilities: interface.capability_requests(),
            fixtures,
            warnings: interface
                .operations
                .iter()
                .flat_map(|operation| {
                    operation
                        .manual
                        .iter()
                        .flat_map(|manual| manual.warnings.iter().cloned())
                })
                .collect(),
            artifact_path,
        })
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ToolOperationBuild {
    pub name: String,
    pub input: serde_json::Value,
    pub output: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<ToolCommandContract>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp: Option<ToolMcpContract>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ToolFixtureRun {
    pub name: String,
    pub operation: String,
    pub status: String,
}

fn resolve_tool_package_path(path: &std::path::Path) -> crate::VerletResult<std::path::PathBuf> {
    let path = if path.is_dir() {
        path.join(TOOL_PACKAGE_FILE_NAME)
    } else {
        path.to_path_buf()
    };
    if !path.exists() {
        return Err(crate::VerletOperationsError::RuntimeFactory(format!(
            "tool package manifest not found at {}",
            path.display()
        )));
    }
    Ok(path)
}

fn resolve_relative_path(base: &std::path::Path, path: std::path::PathBuf) -> std::path::PathBuf {
    if path.is_absolute() {
        path
    } else {
        base.join(path)
    }
}

fn validate_runtime_contract(runtime: &ToolRuntimeContract) -> crate::VerletResult<()> {
    match runtime.kind.as_str() {
        "wasm32-unknown-unknown" | "wasm32_unknown_unknown" => {
            if runtime.module_path.is_none() && runtime.bin_path.is_none() {
                return Err(crate::VerletOperationsError::RuntimeFactory(
                    "tool package runtime requires module_path or bin_path".to_string(),
                ));
            }
            Ok(())
        }
        "kernel" => {
            if runtime.module_path.is_some() || runtime.bin_path.is_some() {
                return Err(crate::VerletOperationsError::RuntimeFactory(
                    "kernel tool packages are synthesized by Verlet startup and must not declare module_path or bin_path".to_string(),
                ));
            }
            Ok(())
        }
        other => Err(crate::VerletOperationsError::RuntimeFactory(format!(
            "tool package runtime kind {other:?} is not supported in V0"
        ))),
    }
}

fn read_json_schema(path: &std::path::Path, label: &str) -> crate::VerletResult<serde_json::Value> {
    let bytes = std::fs::read(path).map_err(|err| {
        crate::VerletOperationsError::RuntimeFactory(format!(
            "failed to read JSON schema {}: {err}",
            path.display()
        ))
    })?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(|err| {
        crate::VerletOperationsError::RuntimeFactory(format!(
            "failed to decode JSON schema {}: {err}",
            path.display()
        ))
    })?;
    verlet_runtime_contracts::schema::validate_json_schema_subset(&value, label).map_err(
        |err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "JSON schema {} is not in the supported Verlet schema subset: {err}",
                path.display()
            ))
        },
    )?;
    Ok(value)
}

fn validate_operation_json_fixtures<'a>(
    operation: &ToolOperationDeclaration,
    input_kind: &verlet_abi::WasmOperationValueKind,
    output_kind: &verlet_abi::WasmOperationValueKind,
    input_schema: &serde_json::Value,
    output_schema: &serde_json::Value,
    fixtures: impl Iterator<Item = &'a ToolFixtureDeclaration>,
) -> crate::VerletResult<()> {
    for fixture in fixtures.filter(|fixture| fixture.operation == operation.name) {
        if matches!(input_kind, verlet_abi::WasmOperationValueKind::Json) {
            validate_fixture_json(
                &fixture.input,
                input_schema,
                &format!(
                    "tool package fixture {:?} input for operation {:?}",
                    fixture.name, operation.name
                ),
            )?;
        }
        if matches!(output_kind, verlet_abi::WasmOperationValueKind::Json) {
            validate_fixture_json(
                &fixture.expect,
                output_schema,
                &format!(
                    "tool package fixture {:?} expectation for operation {:?}",
                    fixture.name, operation.name
                ),
            )?;
        }
    }
    Ok(())
}

fn validate_fixture_json(
    path: &std::path::Path,
    schema: &serde_json::Value,
    label: &str,
) -> crate::VerletResult<()> {
    let bytes = std::fs::read(path).map_err(|err| {
        crate::VerletOperationsError::RuntimeFactory(format!(
            "failed to read JSON fixture {}: {err}",
            path.display()
        ))
    })?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(|err| {
        crate::VerletOperationsError::RuntimeFactory(format!(
            "failed to decode JSON fixture {}: {err}",
            path.display()
        ))
    })?;
    verlet_runtime_contracts::schema::validate_json_value_against_schema(schema, &value, label)
        .map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "JSON fixture {} failed schema validation: {err}",
                path.display()
            ))
        })
}

fn text_sha256(bytes: &[u8]) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    format!("sha256:{digest:x}")
}

fn value_sha256(value: &impl serde::Serialize) -> crate::VerletResult<String> {
    let bytes = serde_json::to_vec(value).map_err(|err| {
        crate::VerletOperationsError::RuntimeFactory(format!(
            "failed to encode tool interface: {err}"
        ))
    })?;
    Ok(text_sha256(&bytes))
}

#[cfg(test)]
mod tests {

    #[test]
    fn tool_interface_rejects_unsupported_schema_keywords() {
        let root = temp_package_root("tool-interface-unsupported-schema");
        write_json(
            &root.join("input.json"),
            r#"{"type":"object","oneOf":[{"type":"object"}]}"#,
        );
        write_json(&root.join("output.json"), r#"{"type":"object"}"#);
        let package = package_source(&root, Vec::new());
        let manifest = wasm_manifest(
            verlet_abi::WasmOperationValueKind::Json,
            verlet_abi::WasmOperationValueKind::Json,
        );
        let projections = operation_projections(&manifest);

        let err = crate::tool_package::ToolInterfaceContract::from_package(
            &package,
            &manifest,
            &projections,
        )
        .unwrap_err();

        assert!(err.to_string().contains("input_schema"));
        assert!(err.to_string().contains("unsupported schema keyword"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn tool_interface_rejects_json_fixture_that_violates_declared_schema() {
        let root = temp_package_root("tool-interface-invalid-fixture");
        write_json(
            &root.join("input.json"),
            r#"{
  "type": "object",
  "required": ["message"],
  "properties": {"message": {"type": "string"}},
  "additionalProperties": false
}"#,
        );
        write_json(
            &root.join("output.json"),
            r#"{
  "type": "object",
  "required": ["ok"],
  "properties": {"ok": {"type": "boolean"}},
  "additionalProperties": false
}"#,
        );
        write_json(&root.join("bad.input.json"), r#"{}"#);
        write_json(&root.join("bad.expect.json"), r#"{"ok":true}"#);
        let package = package_source(
            &root,
            vec![crate::tool_package::ToolFixtureDeclaration {
                name: "bad".to_string(),
                operation: "profile".to_string(),
                input: root.join("bad.input.json"),
                expect: root.join("bad.expect.json"),
            }],
        );
        let manifest = wasm_manifest(
            verlet_abi::WasmOperationValueKind::Json,
            verlet_abi::WasmOperationValueKind::Json,
        );
        let projections = operation_projections(&manifest);

        let err = crate::tool_package::ToolInterfaceContract::from_package(
            &package,
            &manifest,
            &projections,
        )
        .unwrap_err();

        assert!(err.to_string().contains("JSON fixture"));
        assert!(err.to_string().contains("missing required"));
        let _ = std::fs::remove_dir_all(root);
    }

    fn temp_package_root(prefix: &str) -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    fn write_json(path: &std::path::Path, body: &str) {
        std::fs::write(path, body).unwrap();
    }

    fn package_source(
        root: &std::path::Path,
        fixtures: Vec<crate::tool_package::ToolFixtureDeclaration>,
    ) -> crate::tool_package::ToolPackageSource {
        crate::tool_package::ToolPackageSource {
            manifest_path: root.join("verlet.tool.toml"),
            package_root: root.to_path_buf(),
            source_hash: "sha256:test".to_string(),
            manifest: crate::tool_package::ToolPackageManifest {
                kind: crate::tool_package::TOOL_PACKAGE_KIND.to_string(),
                schema_version: crate::tool_package::TOOL_PACKAGE_SCHEMA_VERSION,
                identity: crate::tool_package::ToolPackageIdentity {
                    name: "profile".to_string(),
                    version: None,
                    description: None,
                    owner: None,
                },
                runtime: crate::tool_package::ToolRuntimeContract {
                    kind: "wasm32-unknown-unknown".to_string(),
                    state: Some("stateless".to_string()),
                    module_path: Some(root.join("module.wasm")),
                    bin_path: None,
                    release: None,
                    timeout_ms: None,
                    max_input_bytes: None,
                    max_output_bytes: None,
                },
                operations: vec![crate::tool_package::ToolOperationDeclaration {
                    name: "profile".to_string(),
                    description: Some("Profile input.".to_string()),
                    input_schema: root.join("input.json"),
                    output_schema: root.join("output.json"),
                    required_capabilities: std::collections::BTreeSet::new(),
                    command: Some(crate::tool_package::ToolCommandContract {
                        name: "profile run".to_string(),
                        stdin: Some("none".to_string()),
                        stdout: Some("json".to_string()),
                    }),
                    mcp: None,
                }],
                fixtures,
            },
        }
    }

    fn wasm_manifest(
        input: verlet_abi::WasmOperationValueKind,
        output: verlet_abi::WasmOperationValueKind,
    ) -> verlet_abi::WasmOperationManifest {
        verlet_abi::WasmOperationManifest {
            abi: "cooldis_0.1".to_string(),
            operations: vec![verlet_abi::WasmOperationDefinition {
                id: 1,
                name: "profile".to_string(),
                input,
                output,
                events: verlet_abi::WasmOperationEventKind::None,
                mode: verlet_abi::WasmOperationMode::Sync,
                required_capabilities: Vec::new(),
            }],
        }
    }

    fn operation_projections(
        manifest: &verlet_abi::WasmOperationManifest,
    ) -> crate::OperationProjectionSet {
        crate::RegisteredOperation {
            name: "profile".to_string(),
            manifest: manifest.clone(),
            capability_grants: std::collections::BTreeSet::new(),
            metadata: std::collections::BTreeMap::from([(
                "fixture".to_string(),
                serde_json::json!(true),
            )]),
        }
        .projections()
    }
}
