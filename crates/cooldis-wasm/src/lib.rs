mod runner;

use cooldis_abi::InvocationContext;
use cooldis_vfs::CooldisVfs;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

pub const DEFAULT_ENTRYPOINT: &str = "handle_turn";
pub const DEFAULT_OPERATION_NAME: &str = "handle_turn";
pub const DEFAULT_MAX_INPUT_BYTES: usize = 1_048_576;
pub const DEFAULT_MAX_OUTPUT_BYTES: usize = 1_048_576;
pub const DEFAULT_MEMORY_LIMIT_BYTES: usize = 64 * 1_048_576;
pub const DEFAULT_FUEL: u64 = 10_000_000;
pub const DEFAULT_FUEL_YIELD_INTERVAL: u64 = 10_000;

pub type CooldisWasmResult<T> = Result<T, CooldisWasmError>;

#[derive(Debug, thiserror::Error)]
pub enum CooldisWasmError {
    #[error("runtime factory failed: {0}")]
    RuntimeFactory(String),
    #[error("runtime execution failed: {0}")]
    RuntimeExecution(String),
}

#[doc(hidden)]
pub use runner::{
    FS_MODE_READ, HTTP_ABI, OPERATION_ABI, STATUS_CAPABILITY_DENIED, STATUS_EOF, STATUS_NOT_FOUND,
};
pub use runner::{WasmModuleRuntime, WasmRuntimeFactory};
#[doc(hidden)]
pub use runner::{ensure_http_capability, execute_http_request, http_origin};

#[derive(Clone, Debug)]
pub enum WasmRuntimeArtifact {
    Bytes(Arc<[u8]>),
    Path(PathBuf),
}

impl WasmRuntimeArtifact {
    pub fn bytes(bytes: impl Into<Vec<u8>>) -> Self {
        Self::Bytes(Arc::from(bytes.into()))
    }

    pub fn path(path: impl Into<PathBuf>) -> Self {
        Self::Path(path.into())
    }

    pub async fn load_bytes(&self) -> CooldisWasmResult<Vec<u8>> {
        match self {
            Self::Bytes(bytes) => Ok(bytes.to_vec()),
            Self::Path(path) => tokio::fs::read(path).await.map_err(|err| {
                CooldisWasmError::RuntimeFactory(format!(
                    "failed to read wasm artifact {}: {err}",
                    path.display()
                ))
            }),
        }
    }
}

#[derive(Clone)]
pub struct WasmRuntimeConfig {
    pub artifact: WasmRuntimeArtifact,
    pub entrypoint: String,
    pub operation_name: String,
    pub max_input_bytes: usize,
    pub max_output_bytes: usize,
    pub memory_limit_bytes: Option<usize>,
    pub fuel: Option<u64>,
    pub fuel_yield_interval: Option<u64>,
    pub capability_grants: BTreeSet<String>,
    pub invocation_context: InvocationContext,
    pub secrets: BTreeMap<String, String>,
    pub vfs: Option<Arc<CooldisVfs>>,
}

impl WasmRuntimeConfig {
    pub fn new(artifact: WasmRuntimeArtifact) -> Self {
        Self {
            artifact,
            entrypoint: DEFAULT_ENTRYPOINT.to_string(),
            operation_name: DEFAULT_OPERATION_NAME.to_string(),
            max_input_bytes: DEFAULT_MAX_INPUT_BYTES,
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
            memory_limit_bytes: Some(DEFAULT_MEMORY_LIMIT_BYTES),
            fuel: Some(DEFAULT_FUEL),
            fuel_yield_interval: Some(DEFAULT_FUEL_YIELD_INTERVAL),
            capability_grants: BTreeSet::new(),
            invocation_context: InvocationContext::anonymous(),
            secrets: BTreeMap::new(),
            vfs: None,
        }
    }

    pub fn with_entrypoint(mut self, entrypoint: impl Into<String>) -> Self {
        self.entrypoint = entrypoint.into();
        self
    }

    pub fn with_operation_name(mut self, operation_name: impl Into<String>) -> Self {
        self.operation_name = operation_name.into();
        self
    }

    pub fn with_max_input_bytes(mut self, max_input_bytes: usize) -> Self {
        self.max_input_bytes = max_input_bytes;
        self
    }

    pub fn with_max_output_bytes(mut self, max_output_bytes: usize) -> Self {
        self.max_output_bytes = max_output_bytes;
        self
    }

    pub fn with_memory_limit_bytes(mut self, memory_limit_bytes: Option<usize>) -> Self {
        self.memory_limit_bytes = memory_limit_bytes;
        self
    }

    pub fn with_fuel(mut self, fuel: Option<u64>) -> Self {
        self.fuel = fuel;
        self
    }

    pub fn with_fuel_yield_interval(mut self, fuel_yield_interval: Option<u64>) -> Self {
        self.fuel_yield_interval = fuel_yield_interval;
        self
    }

    pub fn with_capability_grant(mut self, grant: impl Into<String>) -> Self {
        self.capability_grants.insert(grant.into());
        self
    }

    pub fn with_capability_grants(mut self, grants: impl IntoIterator<Item = String>) -> Self {
        self.capability_grants.extend(grants);
        self
    }

    pub fn with_invocation_context(mut self, context: InvocationContext) -> Self {
        self.invocation_context = context;
        self
    }

    pub fn effective_capability_grants(&self) -> BTreeSet<String> {
        self.capability_grants
            .iter()
            .cloned()
            .chain(self.invocation_context.grant_set())
            .collect()
    }

    pub fn with_secret(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.secrets.insert(name.into(), value.into());
        self
    }

    pub fn with_secrets(mut self, secrets: impl IntoIterator<Item = (String, String)>) -> Self {
        self.secrets.extend(secrets);
        self
    }

    pub fn with_vfs(mut self, vfs: Arc<CooldisVfs>) -> Self {
        self.vfs = Some(vfs);
        self
    }
}

impl fmt::Debug for WasmRuntimeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WasmRuntimeConfig")
            .field("artifact", &self.artifact)
            .field("entrypoint", &self.entrypoint)
            .field("operation_name", &self.operation_name)
            .field("max_input_bytes", &self.max_input_bytes)
            .field("max_output_bytes", &self.max_output_bytes)
            .field("memory_limit_bytes", &self.memory_limit_bytes)
            .field("fuel", &self.fuel)
            .field("fuel_yield_interval", &self.fuel_yield_interval)
            .field("capability_grants", &self.capability_grants)
            .field("invocation_context", &self.invocation_context)
            .field("secrets", &"<redacted>")
            .field("vfs", &self.vfs.as_ref().map(|_| "<CooldisVfs>"))
            .finish()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct WasmHttpRequest {
    pub abi: String,
    pub method: String,
    pub url: String,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    #[serde(default)]
    pub secret_headers: Vec<(String, String)>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub max_response_bytes: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct WasmHttpResponse {
    pub abi: String,
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub truncated: bool,
    pub elapsed_ms: u64,
}
