use crate::{ImportPackageSource, validate_record_name};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::PathBuf;
use verlet_runtime_contracts::validate_json_schema_subset;
use verlet_wasm::normalize_http_url;

/// Typed failures produced while loading or normalizing an OpenAPI import.
#[derive(Debug, thiserror::Error)]
pub enum OpenApiImportError {
    #[error("import package manifest not found at {path}")]
    PackageNotFound { path: PathBuf },
    #[error("failed to read import package {path}: {source}")]
    ReadPackage { path: PathBuf, source: io::Error },
    #[error("invalid import package {path}: {source}")]
    InvalidPackage {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("invalid import package identity: {message}")]
    InvalidPackageIdentity { message: String },
    #[error("import package must select at least one operation")]
    EmptyOperationSelection,
    #[error("import package operation_id must be non-empty")]
    EmptyOperationId,
    #[error("invalid imported operation name {name:?}: {message}")]
    InvalidOperationName { name: String, message: String },
    #[error("invalid import authentication: {message}")]
    InvalidAuthentication { message: String },
    #[error("invalid spec sha256 {value:?}")]
    InvalidSpecHash { value: String },
    #[error("failed to read vendored OpenAPI spec {path}: {source}")]
    ReadSpec { path: PathBuf, source: io::Error },
    #[error("vendored OpenAPI spec sha256 mismatch: expected {expected}, got {actual}")]
    SpecHashMismatch { expected: String, actual: String },
    #[error("invalid OpenAPI JSON: {message}")]
    InvalidDocument { message: String },
    #[error("OpenAPI version {version:?} is unsupported; V1 accepts OpenAPI 3.0 and 3.1")]
    UnsupportedVersion { version: String },
    #[error("OpenAPI webhooks are unsupported in V1")]
    WebhooksUnsupported,
    #[error("OpenAPI import requires exactly one server, got {count}")]
    ServerCount { count: usize },
    #[error("invalid OpenAPI server URL {url:?}: {message}")]
    InvalidServerUrl { url: String, message: String },
    #[error("selected OpenAPI operation {operation_id:?} was not found")]
    OperationNotFound { operation_id: String },
    #[error("projected operation name {name:?} is duplicated; supply aliases")]
    DuplicateProjectedName { name: String },
    #[error("OpenAPI operation {operation_id:?} declares callbacks, which are unsupported in V1")]
    CallbacksUnsupported { operation_id: String },
    #[error(
        "OpenAPI operation {operation_id:?} uses multipart content, which is unsupported in V1"
    )]
    MultipartUnsupported { operation_id: String },
    #[error(
        "OpenAPI operation {operation_id:?} request body media type {media_type:?} is unsupported"
    )]
    UnsupportedRequestMediaType {
        operation_id: String,
        media_type: String,
    },
    #[error("OpenAPI operation {operation_id:?} contains an unsupported schema: {message}")]
    UnsupportedSchema {
        operation_id: String,
        message: String,
    },
    #[error(
        "OpenAPI operation {operation_id:?} parameter {parameter:?} uses unsupported location {location:?}"
    )]
    UnsupportedParameterLocation {
        operation_id: String,
        parameter: String,
        location: String,
    },
    #[error(
        "OpenAPI operation {operation_id:?} parameter {parameter:?} uses unsupported serialization"
    )]
    UnsupportedParameterSerialization {
        operation_id: String,
        parameter: String,
    },
    #[error("OpenAPI operation {operation_id:?} path parameter {parameter:?} must be required")]
    OptionalPathParameter {
        operation_id: String,
        parameter: String,
    },
    #[error(
        "OpenAPI operation {operation_id:?} parameter {parameter:?} must use a primitive schema"
    )]
    UnsupportedParameterSchema {
        operation_id: String,
        parameter: String,
    },
    #[error(
        "OpenAPI operation {operation_id:?} path template does not contain parameter {parameter:?}"
    )]
    MissingPathPlaceholder {
        operation_id: String,
        parameter: String,
    },
    #[error(
        "OpenAPI operation {operation_id:?} path placeholder {parameter:?} has no parameter declaration"
    )]
    MissingPathParameter {
        operation_id: String,
        parameter: String,
    },
    #[error("OpenAPI operation {operation_id:?} input field {name:?} is duplicated")]
    DuplicateInputField { operation_id: String, name: String },
    #[error("invalid OpenAPI path template {path:?}: {message}")]
    InvalidPathTemplate { path: String, message: String },
    #[error(
        "OpenAPI operation {operation_id:?} header parameter {parameter:?} is unsupported: {message}"
    )]
    UnsupportedHeaderParameter {
        operation_id: String,
        parameter: String,
        message: String,
    },
    #[error(
        "OpenAPI operation {operation_id:?} header parameter {header:?} conflicts with credential injection"
    )]
    CredentialHeaderCollision {
        operation_id: String,
        header: String,
    },
    #[error("authentication scheme {scheme:?} is unsupported in V1")]
    UnsupportedAuthentication { scheme: String },
}

/// Normalized, deterministic plan for one import package and published record.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OperationImportPlan {
    pub name: String,
    pub version: Option<String>,
    pub description: Option<String>,
    pub spec_sha256: String,
    pub operations: Vec<ImportedOperationPlan>,
}

impl OperationImportPlan {
    pub fn from_package(package: &ImportPackageSource) -> Result<Self, OpenApiImportError> {
        let document: OpenApiDocument =
            serde_json::from_slice(&package.spec_bytes).map_err(|error| {
                OpenApiImportError::InvalidDocument {
                    message: error.to_string(),
                }
            })?;
        normalize_document(package, document)
    }

    pub fn capability_requests(&self) -> BTreeSet<String> {
        self.operations
            .iter()
            .flat_map(|operation| operation.required_capabilities.iter().cloned())
            .collect()
    }
}

/// Request-construction and ABI contract for one selected OpenAPI operation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ImportedOperationPlan {
    pub id: u32,
    pub name: String,
    pub source_operation_id: String,
    pub description: Option<String>,
    pub method: String,
    pub server_url: String,
    pub origin: String,
    pub path_template: String,
    pub parameters: Vec<OperationParameterPlan>,
    pub request_body: Option<OperationRequestBodyPlan>,
    pub secret_headers: Vec<OperationSecretHeaderPlan>,
    pub input_schema: Value,
    pub output_schema: Value,
    pub required_capabilities: BTreeSet<String>,
}

/// Supported OpenAPI parameter locations used by the HTTP request renderer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationParameterLocation {
    Path,
    Query,
    Header,
}

/// Normalized input mapping for one path, query, or header parameter.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OperationParameterPlan {
    pub name: String,
    pub input_property: String,
    pub location: OperationParameterLocation,
    pub required: bool,
    pub schema: Value,
}

/// JSON request-body mapping for one imported operation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OperationRequestBodyPlan {
    pub required: bool,
    pub input_property: Option<String>,
    pub schema: Value,
}

/// Secret-backed HTTP header mapping pinned into an imported artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationSecretHeaderPlan {
    pub name: String,
    pub secret: String,
    pub prefix: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenApiDocument {
    openapi: String,
    info: OpenApiInfo,
    #[serde(default)]
    servers: Vec<OpenApiServer>,
    paths: BTreeMap<String, OpenApiPathItem>,
    #[serde(default)]
    webhooks: Option<Value>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenApiInfo {
    title: String,
    version: String,
    #[serde(default)]
    description: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenApiServer {
    url: String,
    #[serde(default)]
    description: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenApiPathItem {
    #[serde(default)]
    summary: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    parameters: Vec<OpenApiParameter>,
    #[serde(default)]
    get: Option<OpenApiOperation>,
    #[serde(default)]
    put: Option<OpenApiOperation>,
    #[serde(default)]
    post: Option<OpenApiOperation>,
    #[serde(default)]
    delete: Option<OpenApiOperation>,
    #[serde(default)]
    options: Option<OpenApiOperation>,
    #[serde(default)]
    head: Option<OpenApiOperation>,
    #[serde(default)]
    patch: Option<OpenApiOperation>,
    #[serde(default)]
    trace: Option<OpenApiOperation>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenApiOperation {
    #[serde(default, rename = "operationId")]
    operation_id: Option<String>,
    #[serde(default)]
    summary: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    deprecated: Option<bool>,
    #[serde(default)]
    parameters: Vec<OpenApiParameter>,
    #[serde(default, rename = "requestBody")]
    request_body: Option<OpenApiRequestBody>,
    responses: BTreeMap<String, OpenApiResponse>,
    #[serde(default)]
    callbacks: Option<Value>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenApiParameter {
    name: String,
    #[serde(rename = "in")]
    location: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    required: bool,
    #[serde(default)]
    deprecated: Option<bool>,
    #[serde(default)]
    style: Option<String>,
    #[serde(default)]
    explode: Option<bool>,
    #[serde(default, rename = "allowEmptyValue")]
    allow_empty_value: Option<bool>,
    schema: Value,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenApiRequestBody {
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    required: bool,
    content: BTreeMap<String, OpenApiMediaType>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenApiResponse {
    description: String,
    #[serde(default)]
    content: BTreeMap<String, OpenApiMediaType>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenApiMediaType {
    #[serde(default)]
    schema: Option<Value>,
}

#[derive(Clone)]
struct CandidateOperation {
    source_name: String,
    method: String,
    path: String,
    path_parameters: Vec<OpenApiParameter>,
    operation: OpenApiOperation,
}

fn normalize_document(
    package: &ImportPackageSource,
    document: OpenApiDocument,
) -> Result<OperationImportPlan, OpenApiImportError> {
    if !supported_openapi_version(&document.openapi) {
        return Err(OpenApiImportError::UnsupportedVersion {
            version: document.openapi,
        });
    }
    if document.webhooks.is_some() {
        return Err(OpenApiImportError::WebhooksUnsupported);
    }
    if document.servers.len() != 1 {
        return Err(OpenApiImportError::ServerCount {
            count: document.servers.len(),
        });
    }
    let declared_server_url = &document.servers[0].url;
    if declared_server_url.contains(['{', '}']) {
        return Err(OpenApiImportError::InvalidServerUrl {
            url: declared_server_url.clone(),
            message: "server variables are unsupported in V1".to_string(),
        });
    }
    let target = normalize_http_url(declared_server_url).map_err(|message| {
        OpenApiImportError::InvalidServerUrl {
            url: declared_server_url.clone(),
            message,
        }
    })?;
    if target.has_credentials || target.has_query || target.has_fragment {
        return Err(OpenApiImportError::InvalidServerUrl {
            url: declared_server_url.clone(),
            message: "URL must contain no credentials, query, or fragment".to_string(),
        });
    }
    let server_url = target.url.trim_end_matches('/').to_string();
    let origin = target.origin;
    let _ = (
        &document.info.title,
        &document.info.version,
        &document.info.description,
        &document.servers[0].description,
    );
    let mut candidates = collect_candidates(document.paths)?;
    let mut operations = Vec::with_capacity(package.manifest.operations.len());
    let mut projected_names = BTreeSet::new();
    let mut projected_surface_names = BTreeSet::new();
    for (index, selection) in package.manifest.operations.iter().enumerate() {
        let position = candidates
            .iter()
            .position(|candidate| candidate.source_name == selection.operation_id)
            .ok_or_else(|| OpenApiImportError::OperationNotFound {
                operation_id: selection.operation_id.clone(),
            })?;
        let candidate = candidates.remove(position);
        let name = selection
            .alias
            .clone()
            .unwrap_or_else(|| candidate.source_name.clone());
        let normalized_name = validate_record_name(&name).map_err(|error| {
            OpenApiImportError::InvalidOperationName {
                name: name.clone(),
                message: error.to_string(),
            }
        })?;
        if normalized_name != name {
            return Err(OpenApiImportError::InvalidOperationName {
                name,
                message: "operation name must not contain surrounding whitespace".to_string(),
            });
        }
        if !projected_names.insert(name.clone()) {
            return Err(OpenApiImportError::DuplicateProjectedName { name });
        }
        let surface_name = crate::projection_tool_name(&package.manifest.import.name, &name);
        if !projected_surface_names.insert(surface_name.clone()) {
            return Err(OpenApiImportError::DuplicateProjectedName { name: surface_name });
        }
        let id = index
            .checked_add(1)
            .and_then(|id| u32::try_from(id).ok())
            .ok_or_else(|| OpenApiImportError::InvalidDocument {
                message: "selected operation count exceeds the ABI operation-id range".to_string(),
            })?;
        operations.push(normalize_operation(
            id,
            name,
            selection.description.clone(),
            &server_url,
            &origin,
            target.private_destination,
            candidate,
            package.manifest.auth.as_ref(),
        )?);
    }
    Ok(OperationImportPlan {
        name: package.manifest.import.name.clone(),
        version: package.manifest.import.version.clone(),
        description: package.manifest.import.description.clone(),
        spec_sha256: package.spec_sha256.clone(),
        operations,
    })
}

fn collect_candidates(
    paths: BTreeMap<String, OpenApiPathItem>,
) -> Result<Vec<CandidateOperation>, OpenApiImportError> {
    let mut candidates = Vec::new();
    for (path, item) in paths {
        validate_path_template(&path)?;
        let _ = (&item.summary, &item.description);
        for (method, operation) in [
            ("GET", item.get),
            ("PUT", item.put),
            ("POST", item.post),
            ("DELETE", item.delete),
            ("OPTIONS", item.options),
            ("HEAD", item.head),
            ("PATCH", item.patch),
            ("TRACE", item.trace),
        ] {
            if let Some(operation) = operation {
                candidates.push(CandidateOperation {
                    source_name: operation
                        .operation_id
                        .clone()
                        .unwrap_or_else(|| fallback_operation_name(method, &path)),
                    method: method.to_string(),
                    path: path.clone(),
                    path_parameters: item.parameters.clone(),
                    operation,
                });
            }
        }
    }
    Ok(candidates)
}

fn normalize_operation(
    id: u32,
    name: String,
    description_override: Option<String>,
    server_url: &str,
    origin: &str,
    private_destination: bool,
    candidate: CandidateOperation,
    auth: Option<&crate::ImportAuthDeclaration>,
) -> Result<ImportedOperationPlan, OpenApiImportError> {
    if candidate.operation.callbacks.is_some() {
        return Err(OpenApiImportError::CallbacksUnsupported {
            operation_id: candidate.source_name,
        });
    }
    let secret_headers = normalize_auth(auth)?;
    let mut parameter_rows = candidate.path_parameters;
    for parameter in candidate.operation.parameters.clone() {
        if let Some(position) = parameter_rows
            .iter()
            .position(|existing| same_parameter_identity(existing, &parameter))
        {
            parameter_rows[position] = parameter;
        } else {
            parameter_rows.push(parameter);
        }
    }
    let mut parameters = Vec::with_capacity(parameter_rows.len());
    let mut input_names = BTreeSet::new();
    let mut header_names = BTreeSet::new();
    for parameter in parameter_rows {
        let normalized = normalize_parameter(&candidate.source_name, &candidate.path, parameter)?;
        if matches!(normalized.location, OperationParameterLocation::Header) {
            let canonical_header = normalized.name.to_ascii_lowercase();
            if !header_names.insert(canonical_header) {
                return Err(OpenApiImportError::DuplicateInputField {
                    operation_id: candidate.source_name.clone(),
                    name: normalized.name,
                });
            }
            if secret_headers
                .iter()
                .any(|secret| secret.name.eq_ignore_ascii_case(&normalized.name))
            {
                return Err(OpenApiImportError::CredentialHeaderCollision {
                    operation_id: candidate.source_name.clone(),
                    header: normalized.name,
                });
            }
        }
        if !input_names.insert(normalized.input_property.clone()) {
            return Err(OpenApiImportError::DuplicateInputField {
                operation_id: candidate.source_name.clone(),
                name: normalized.input_property,
            });
        }
        parameters.push(normalized);
    }
    validate_path_placeholders(&candidate.source_name, &candidate.path, &parameters)?;
    let mut request_body = normalize_request_body(
        &candidate.source_name,
        candidate.operation.request_body.as_ref(),
    )?;
    if request_body
        .as_ref()
        .is_some_and(|request_body| !request_body.required || !parameters.is_empty())
    {
        if !input_names.insert("body".to_string()) {
            return Err(OpenApiImportError::DuplicateInputField {
                operation_id: candidate.source_name.clone(),
                name: "body".to_string(),
            });
        }
        request_body.as_mut().unwrap().input_property = Some("body".to_string());
    }
    validate_response_schemas(&candidate.source_name, &candidate.operation.responses)?;
    let input_schema = operation_input_schema(&parameters, request_body.as_ref());
    validate_json_schema_subset(&input_schema, &format!("import operation {name} input")).map_err(
        |error| OpenApiImportError::UnsupportedSchema {
            operation_id: candidate.source_name.clone(),
            message: error.to_string(),
        },
    )?;
    let output_schema = operation_output_schema();
    let mut required_capabilities = BTreeSet::from([format!(
        "{}:{}:{}",
        if private_destination {
            "net.http.private"
        } else {
            "net.http"
        },
        candidate.method,
        origin
    )]);
    for secret in &secret_headers {
        required_capabilities.insert(format!("secret:{}", secret.secret));
    }
    let description = description_override
        .or(candidate.operation.description)
        .or(candidate.operation.summary);
    let _ = candidate.operation.deprecated;
    Ok(ImportedOperationPlan {
        id,
        name,
        source_operation_id: candidate.source_name,
        description,
        method: candidate.method,
        server_url: server_url.to_string(),
        origin: origin.to_string(),
        path_template: candidate.path,
        parameters,
        request_body,
        secret_headers,
        input_schema,
        output_schema,
        required_capabilities,
    })
}

fn normalize_parameter(
    operation_id: &str,
    path: &str,
    parameter: OpenApiParameter,
) -> Result<OperationParameterPlan, OpenApiImportError> {
    if parameter.name.is_empty() {
        return Err(OpenApiImportError::InvalidDocument {
            message: format!("operation {operation_id:?} contains an empty parameter name"),
        });
    }
    let location = match parameter.location.as_str() {
        "path" => OperationParameterLocation::Path,
        "query" => OperationParameterLocation::Query,
        "header" => OperationParameterLocation::Header,
        other => {
            return Err(OpenApiImportError::UnsupportedParameterLocation {
                operation_id: operation_id.to_string(),
                parameter: parameter.name,
                location: other.to_string(),
            });
        }
    };
    if matches!(location, OperationParameterLocation::Header) {
        validate_import_header_parameter(operation_id, &parameter.name)?;
    }
    let expected_style = match location {
        OperationParameterLocation::Query => "form",
        OperationParameterLocation::Path | OperationParameterLocation::Header => "simple",
    };
    let expected_explode = matches!(location, OperationParameterLocation::Query);
    if parameter
        .style
        .as_deref()
        .is_some_and(|style| style != expected_style)
        || parameter
            .explode
            .is_some_and(|explode| explode != expected_explode)
        || parameter.allow_empty_value == Some(true)
    {
        return Err(OpenApiImportError::UnsupportedParameterSerialization {
            operation_id: operation_id.to_string(),
            parameter: parameter.name,
        });
    }
    if matches!(location, OperationParameterLocation::Path) && !parameter.required {
        return Err(OpenApiImportError::OptionalPathParameter {
            operation_id: operation_id.to_string(),
            parameter: parameter.name,
        });
    }
    validate_json_schema_subset(
        &parameter.schema,
        &format!(
            "import operation {operation_id} parameter {}",
            parameter.name
        ),
    )
    .map_err(|error| OpenApiImportError::UnsupportedSchema {
        operation_id: operation_id.to_string(),
        message: error.to_string(),
    })?;
    if !parameter_schema_is_primitive(&parameter.schema) {
        return Err(OpenApiImportError::UnsupportedParameterSchema {
            operation_id: operation_id.to_string(),
            parameter: parameter.name,
        });
    }
    if matches!(location, OperationParameterLocation::Path)
        && !path.contains(&format!("{{{}}}", parameter.name))
    {
        return Err(OpenApiImportError::MissingPathPlaceholder {
            operation_id: operation_id.to_string(),
            parameter: parameter.name,
        });
    }
    let _ = (&parameter.description, parameter.deprecated);
    Ok(OperationParameterPlan {
        input_property: parameter.name.clone(),
        name: parameter.name,
        location,
        required: parameter.required,
        schema: parameter.schema,
    })
}

fn normalize_request_body(
    operation_id: &str,
    request_body: Option<&OpenApiRequestBody>,
) -> Result<Option<OperationRequestBodyPlan>, OpenApiImportError> {
    let Some(request_body) = request_body else {
        return Ok(None);
    };
    let _ = &request_body.description;
    if request_body.content.contains_key("multipart/form-data") {
        return Err(OpenApiImportError::MultipartUnsupported {
            operation_id: operation_id.to_string(),
        });
    }
    if request_body.content.len() != 1 || !request_body.content.contains_key("application/json") {
        let media_type = request_body
            .content
            .keys()
            .next()
            .cloned()
            .unwrap_or_default();
        return Err(OpenApiImportError::UnsupportedRequestMediaType {
            operation_id: operation_id.to_string(),
            media_type,
        });
    }
    let schema = request_body.content["application/json"]
        .schema
        .clone()
        .unwrap_or_else(|| json!({"type": "object", "additionalProperties": true}));
    validate_json_schema_subset(
        &schema,
        &format!("import operation {operation_id} request body"),
    )
    .map_err(|error| OpenApiImportError::UnsupportedSchema {
        operation_id: operation_id.to_string(),
        message: error.to_string(),
    })?;
    Ok(Some(OperationRequestBodyPlan {
        required: request_body.required,
        input_property: None,
        schema,
    }))
}

fn validate_response_schemas(
    operation_id: &str,
    responses: &BTreeMap<String, OpenApiResponse>,
) -> Result<(), OpenApiImportError> {
    for response in responses.values() {
        let _ = &response.description;
        for media in response.content.values() {
            if let Some(schema) = &media.schema {
                validate_json_schema_subset(
                    schema,
                    &format!("import operation {operation_id} response"),
                )
                .map_err(|error| OpenApiImportError::UnsupportedSchema {
                    operation_id: operation_id.to_string(),
                    message: error.to_string(),
                })?;
            }
        }
    }
    Ok(())
}

fn normalize_auth(
    auth: Option<&crate::ImportAuthDeclaration>,
) -> Result<Vec<OperationSecretHeaderPlan>, OpenApiImportError> {
    let Some(auth) = auth else {
        return Ok(Vec::new());
    };
    match auth.scheme.as_str() {
        "apiKey" | "api_key" => Ok(vec![OperationSecretHeaderPlan {
            name: auth.header.clone(),
            secret: auth.secret.clone(),
            prefix: String::new(),
        }]),
        "bearer" => {
            if !auth.header.eq_ignore_ascii_case("authorization") {
                return Err(OpenApiImportError::InvalidAuthentication {
                    message: "bearer auth header must be Authorization".to_string(),
                });
            }
            Ok(vec![OperationSecretHeaderPlan {
                name: "Authorization".to_string(),
                secret: auth.secret.clone(),
                prefix: "Bearer ".to_string(),
            }])
        }
        scheme => Err(OpenApiImportError::UnsupportedAuthentication {
            scheme: scheme.to_string(),
        }),
    }
}

fn operation_input_schema(
    parameters: &[OperationParameterPlan],
    request_body: Option<&OperationRequestBodyPlan>,
) -> Value {
    if parameters.is_empty()
        && let Some(request_body) = request_body
        && request_body.input_property.is_none()
    {
        return request_body.schema.clone();
    }
    let mut properties = Map::new();
    let mut required = Vec::new();
    for parameter in parameters {
        properties.insert(parameter.input_property.clone(), parameter.schema.clone());
        if parameter.required {
            required.push(Value::String(parameter.input_property.clone()));
        }
    }
    if let Some(request_body) = request_body {
        let name = request_body
            .input_property
            .as_deref()
            .unwrap_or("body")
            .to_string();
        properties.insert(name.clone(), request_body.schema.clone());
        if request_body.required {
            required.push(Value::String(name));
        }
    }
    json!({
        "type": "object",
        "required": required,
        "properties": properties,
        "additionalProperties": false
    })
}

fn operation_output_schema() -> Value {
    json!({
        "type": "object",
        "required": ["status", "headers", "body", "truncated"],
        "properties": {
            "status": {"type": "integer"},
            "headers": {
                "type": "object",
                "additionalProperties": {"type": "string"}
            },
            "body": {
                "type": ["object", "array", "string", "number", "integer", "boolean", "null"]
            },
            "truncated": {"type": "boolean"}
        },
        "additionalProperties": false
    })
}

fn validate_path_placeholders(
    operation_id: &str,
    path: &str,
    parameters: &[OperationParameterPlan],
) -> Result<(), OpenApiImportError> {
    let declared = parameters
        .iter()
        .filter(|parameter| matches!(parameter.location, OperationParameterLocation::Path))
        .map(|parameter| parameter.name.as_str())
        .collect::<BTreeSet<_>>();
    let mut rest = path;
    while let Some(start) = rest.find('{') {
        let after = &rest[start + 1..];
        let Some(end) = after.find('}') else {
            return Err(OpenApiImportError::InvalidDocument {
                message: format!("path template {path:?} has an unterminated placeholder"),
            });
        };
        let parameter = &after[..end];
        if !declared.contains(parameter) {
            return Err(OpenApiImportError::MissingPathParameter {
                operation_id: operation_id.to_string(),
                parameter: parameter.to_string(),
            });
        }
        rest = &after[end + 1..];
    }
    Ok(())
}

fn parameter_schema_is_primitive(schema: &Value) -> bool {
    match schema.get("type") {
        Some(Value::String(value)) => {
            matches!(value.as_str(), "string" | "number" | "integer" | "boolean")
        }
        Some(Value::Array(values)) => values.iter().all(|value| {
            value.as_str().is_some_and(|value| {
                matches!(value, "string" | "number" | "integer" | "boolean" | "null")
            })
        }),
        _ => false,
    }
}

fn fallback_operation_name(method: &str, path: &str) -> String {
    let mut name = method.to_ascii_lowercase();
    for part in path.trim_matches('/').split('/') {
        if part.is_empty() {
            continue;
        }
        name.push('_');
        let part = part.trim_start_matches('{').trim_end_matches('}');
        let mut separator = false;
        for ch in part.chars() {
            if ch.is_ascii_alphanumeric() {
                name.push(ch.to_ascii_lowercase());
                separator = false;
            } else if !separator {
                name.push('_');
                separator = true;
            }
        }
        while name.ends_with('_') {
            name.pop();
        }
    }
    name
}

fn supported_openapi_version(version: &str) -> bool {
    let parts = version.split('.').collect::<Vec<_>>();
    parts.len() == 3
        && parts[0] == "3"
        && matches!(parts[1], "0" | "1")
        && !parts[2].is_empty()
        && parts[2].bytes().all(|byte| byte.is_ascii_digit())
}

fn validate_path_template(path: &str) -> Result<(), OpenApiImportError> {
    if !path.starts_with('/') {
        return Err(OpenApiImportError::InvalidPathTemplate {
            path: path.to_string(),
            message: "path must start with '/'".to_string(),
        });
    }
    if path.contains(['?', '#']) {
        return Err(OpenApiImportError::InvalidPathTemplate {
            path: path.to_string(),
            message: "path must not contain a query or fragment".to_string(),
        });
    }
    let mut open = false;
    let mut placeholder_len = 0;
    for ch in path.chars() {
        match ch {
            '{' if open => {
                return Err(OpenApiImportError::InvalidPathTemplate {
                    path: path.to_string(),
                    message: "path contains nested placeholders".to_string(),
                });
            }
            '{' => {
                open = true;
                placeholder_len = 0;
            }
            '}' if !open => {
                return Err(OpenApiImportError::InvalidPathTemplate {
                    path: path.to_string(),
                    message: "path contains an unmatched closing brace".to_string(),
                });
            }
            '}' if placeholder_len == 0 => {
                return Err(OpenApiImportError::InvalidPathTemplate {
                    path: path.to_string(),
                    message: "path contains an empty placeholder".to_string(),
                });
            }
            '}' => open = false,
            _ if open => placeholder_len += 1,
            _ => {}
        }
    }
    if open {
        return Err(OpenApiImportError::InvalidPathTemplate {
            path: path.to_string(),
            message: "path contains an unterminated placeholder".to_string(),
        });
    }
    Ok(())
}

fn same_parameter_identity(left: &OpenApiParameter, right: &OpenApiParameter) -> bool {
    left.location == right.location
        && if left.location == "header" {
            left.name.eq_ignore_ascii_case(&right.name)
        } else {
            left.name == right.name
        }
}

fn validate_import_header_parameter(
    operation_id: &str,
    name: &str,
) -> Result<(), OpenApiImportError> {
    let lower = name.to_ascii_lowercase();
    let reserved = matches!(
        lower.as_str(),
        "accept"
            | "authorization"
            | "connection"
            | "content-length"
            | "content-type"
            | "cookie"
            | "host"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "set-cookie"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    );
    if !valid_http_header_name(name) || reserved {
        return Err(OpenApiImportError::UnsupportedHeaderParameter {
            operation_id: operation_id.to_string(),
            parameter: name.to_string(),
            message: if reserved {
                "header is reserved for HTTP framing, content negotiation, or credentials"
                    .to_string()
            } else {
                "header name is not a valid HTTP token".to_string()
            },
        });
    }
    Ok(())
}

pub(crate) fn valid_http_header_name(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || byte == 0x60
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'|'
                        | b'~'
                )
        })
}

pub(crate) fn protected_http_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "content-length"
            | "host"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}
