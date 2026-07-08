use crate::{
    CooldisWasmError, CooldisWasmResult, WasmHostImportPolicy, WasmHttpRequest, WasmHttpResponse,
    WasmRuntimeArtifact, WasmRuntimeConfig,
};
use bashkit::{Error as BashkitError, FileSystem};
use cooldis_abi::{InvocationContext, WasmOperationManifest};
use cooldis_process::{CooldisProcessHandle, WasmOperationOutput};
use cooldis_vfs::CooldisVfs;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use wasmtime::{
    Caller, Config, Engine, ExternType, Linker, Memory, Module, Store, StoreLimitsBuilder,
};

#[doc(hidden)]
pub const OPERATION_ABI: &str = "cooldis.operation/0.1";
const DESCRIBE_EXPORT: &str = "__cooldis_describe_module__";
const CALL_OPERATION_EXPORT: &str = "__cooldis_call_operation__";
const INPUT_SOURCE: u32 = 1;
const OUTPUT_SINK: u32 = 1;
const EVENT_SINK: u32 = 2;
const INVOCATION_HANDLE: u32 = 1;
pub const STATUS_OK: i32 = 0;
pub const STATUS_INVALID_ARGUMENT: i32 = 1;
pub const STATUS_NOT_FOUND: i32 = 2;
#[doc(hidden)]
pub const STATUS_CAPABILITY_DENIED: i32 = 3;
pub const STATUS_TRANSPORT_ERROR: i32 = 4;
pub const STATUS_TIMEOUT: i32 = 5;
pub const STATUS_CANCELLED: i32 = 6;
pub const STATUS_EOF: i32 = 7;
#[doc(hidden)]
pub const HTTP_ABI: &str = "cooldis.net.http/0.1";
const HTTP_DEFAULT_TIMEOUT_MS: u64 = 30_000;
const HTTP_MAX_TIMEOUT_MS: u64 = 180_000;
const HTTP_DEFAULT_MAX_RESPONSE_BYTES: usize = 1_048_576;
const FIRST_DYNAMIC_SOURCE: u32 = 10;
const FIRST_DYNAMIC_FILE: u32 = 1_000;
#[doc(hidden)]
pub const FS_MODE_READ: u32 = 0;
const BLOCKED_HTTP_ADDRESS_ERROR: &str =
    "refusing to connect to private or special-purpose address";

pub struct WasmRuntimeFactory {
    config: WasmRuntimeConfig,
    engine: Arc<Engine>,
}

impl WasmRuntimeFactory {
    pub fn new(config: WasmRuntimeConfig) -> CooldisWasmResult<Self> {
        let mut engine_config = Config::new();
        engine_config.consume_fuel(config.fuel.is_some());
        configure_wasmtime_engine(&mut engine_config);
        let engine = Engine::new(&engine_config)
            .map_err(|err| CooldisWasmError::RuntimeFactory(err.to_string()))?;
        Ok(Self {
            config,
            engine: Arc::new(engine),
        })
    }

    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> CooldisWasmResult<Self> {
        Self::new(WasmRuntimeConfig::new(WasmRuntimeArtifact::bytes(bytes)))
    }

    pub async fn describe(&self) -> CooldisWasmResult<Option<WasmOperationManifest>> {
        let module = self.load_module().await?;
        load_operation_manifest(&self.engine, &module, &self.config).await
    }

    pub async fn validate_operation_artifact(&self) -> CooldisWasmResult<WasmOperationManifest> {
        let module = self.load_module().await?;
        validate_operation_module(&self.engine, &module, &self.config).await
    }

    pub async fn build_validated_operation_runtime(&self) -> CooldisWasmResult<WasmModuleRuntime> {
        let module = self.load_module().await?;
        let manifest = validate_operation_module(&self.engine, &module, &self.config).await?;
        Ok(WasmModuleRuntime {
            config: self.config.clone(),
            engine: Arc::clone(&self.engine),
            module,
            manifest: Some(manifest),
        })
    }

    pub async fn invoke_operation_bytes(
        &self,
        operation_name: &str,
        input: impl Into<Vec<u8>>,
    ) -> CooldisWasmResult<WasmOperationOutput> {
        let module = self.load_module().await?;
        let manifest = load_operation_manifest(&self.engine, &module, &self.config)
            .await?
            .ok_or_else(|| {
                CooldisWasmError::RuntimeExecution(format!(
                    "wasm module does not export {DESCRIBE_EXPORT}"
                ))
            })?;
        execute_operation(
            &self.engine,
            &module,
            &self.config,
            &manifest,
            operation_name,
            input.into(),
        )
        .await
    }

    pub async fn invoke_operation_process(
        &self,
        operation_name: &str,
        input: impl Into<Vec<u8>>,
    ) -> CooldisWasmResult<CooldisProcessHandle> {
        let output = self.invoke_operation_bytes(operation_name, input).await?;
        Ok(CooldisProcessHandle::from_wasm_operation_output(
            None, output,
        ))
    }

    pub async fn build_runtime(&self) -> CooldisWasmResult<WasmModuleRuntime> {
        let module = self.load_module().await?;
        let manifest = load_operation_manifest(&self.engine, &module, &self.config).await?;
        Ok(WasmModuleRuntime {
            config: self.config.clone(),
            engine: Arc::clone(&self.engine),
            module,
            manifest,
        })
    }

    async fn load_module(&self) -> CooldisWasmResult<Arc<Module>> {
        let bytes = self.config.artifact.load_bytes().await?;
        reject_textual_wat_artifact(&bytes)?;
        let module = Module::new(&self.engine, bytes)
            .map_err(|err| CooldisWasmError::RuntimeFactory(err.to_string()))?;
        Ok(Arc::new(module))
    }
}

async fn validate_operation_module(
    engine: &Engine,
    module: &Module,
    config: &WasmRuntimeConfig,
) -> CooldisWasmResult<WasmOperationManifest> {
    validate_module_imports(module, config.host_import_policy)?;
    let manifest = load_operation_manifest(engine, module, config)
        .await?
        .ok_or_else(|| {
            CooldisWasmError::RuntimeFactory(format!(
                "wasm operation artifact must export {DESCRIBE_EXPORT}; legacy handle_turn modules cannot be published as operations"
            ))
        })?;
    if !module
        .exports()
        .any(|export| export.name() == CALL_OPERATION_EXPORT)
    {
        return Err(CooldisWasmError::RuntimeFactory(format!(
            "wasm operation artifact must export {CALL_OPERATION_EXPORT}"
        )));
    }
    Ok(manifest)
}

fn configure_wasmtime_engine(engine_config: &mut Config) {
    // Wasmtime's default macOS Mach-port trap handler owns a process-global
    // helper thread. Under the parallel lib test harness that thread has
    // intermittently aborted in `mach_msg` with no Rust panic, so Cooldis uses
    // the documented signal-handler trap path instead. This also matches the
    // runtime's fork/process-heavy embedding better than a Mach exception port.
    #[cfg(target_os = "macos")]
    engine_config.macos_use_mach_ports(false);
}

fn reject_textual_wat_artifact(bytes: &[u8]) -> CooldisWasmResult<()> {
    if bytes
        .iter()
        .copied()
        .skip_while(u8::is_ascii_whitespace)
        .take(7)
        .eq(b"(module".iter().copied())
    {
        return Err(CooldisWasmError::RuntimeFactory(
            "wasm runtime artifacts must be compiled .wasm bytes; textual WAT is only allowed in tests/fixtures before compilation"
                .to_string(),
        ));
    }
    Ok(())
}

pub struct WasmModuleRuntime {
    config: WasmRuntimeConfig,
    engine: Arc<Engine>,
    module: Arc<Module>,
    manifest: Option<WasmOperationManifest>,
}

impl WasmModuleRuntime {
    pub async fn invoke_operation_bytes(
        &self,
        operation_name: &str,
        input: impl Into<Vec<u8>>,
    ) -> CooldisWasmResult<WasmOperationOutput> {
        let manifest = self.manifest.as_ref().ok_or_else(|| {
            CooldisWasmError::RuntimeExecution(format!(
                "wasm module does not export {DESCRIBE_EXPORT}"
            ))
        })?;
        execute_operation(
            &self.engine,
            &self.module,
            &self.config,
            manifest,
            operation_name,
            input.into(),
        )
        .await
    }

    pub async fn execute_turn(&self, input: String) -> CooldisWasmResult<String> {
        if let Some(manifest) = &self.manifest {
            let output = execute_operation(
                &self.engine,
                &self.module,
                &self.config,
                manifest,
                &self.config.operation_name,
                input.into_bytes(),
            )
            .await?;
            let mut text = String::from_utf8_lossy(&output.output).to_string();
            if !output.events.is_empty() {
                if !text.is_empty() && !text.ends_with('\n') {
                    text.push('\n');
                }
                text.push_str(&String::from_utf8_lossy(&output.events));
            }
            return Ok(text);
        }

        let input = truncate_bytes(input.into_bytes(), self.config.max_input_bytes);
        let mut linker = Linker::new(&self.engine);
        add_cooldis_imports(&mut linker)
            .map_err(|err| CooldisWasmError::RuntimeExecution(err.to_string()))?;

        let mut store = Store::new(&self.engine, WasmTurnState::new(input, &self.config));
        configure_store(&mut store, &self.config)?;

        let instance = linker
            .instantiate_async(&mut store, &self.module)
            .await
            .map_err(|err| CooldisWasmError::RuntimeExecution(err.to_string()))?;
        let entry = instance
            .get_typed_func::<(), i32>(&mut store, &self.config.entrypoint)
            .map_err(|err| CooldisWasmError::RuntimeExecution(err.to_string()))?;
        let exit_code = entry
            .call_async(&mut store, ())
            .await
            .map_err(|err| CooldisWasmError::RuntimeExecution(err.to_string()))?;
        if exit_code != 0 {
            return Err(CooldisWasmError::RuntimeExecution(format!(
                "wasm entrypoint {} returned exit code {exit_code}",
                self.config.entrypoint
            )));
        }
        let state = store.into_data();
        let mut output = String::from_utf8_lossy(
            state
                .sinks
                .get(&OUTPUT_SINK)
                .map(Vec::as_slice)
                .unwrap_or_default(),
        )
        .to_string();
        if state.output_truncated {
            output.push_str("\n[output truncated]\n");
        }
        Ok(output)
    }
}

async fn load_operation_manifest(
    engine: &Engine,
    module: &Module,
    config: &WasmRuntimeConfig,
) -> CooldisWasmResult<Option<WasmOperationManifest>> {
    if !module
        .exports()
        .any(|export| export.name() == DESCRIBE_EXPORT)
    {
        return Ok(None);
    }

    let mut linker = Linker::new(engine);
    add_cooldis_imports(&mut linker)
        .map_err(|err| CooldisWasmError::RuntimeExecution(err.to_string()))?;
    let mut store = Store::new(engine, WasmTurnState::new(Vec::new(), config));
    configure_store(&mut store, config)?;
    let instance = linker
        .instantiate_async(&mut store, module)
        .await
        .map_err(|err| CooldisWasmError::RuntimeExecution(err.to_string()))?;
    let Some(export) = instance.get_export(&mut store, DESCRIBE_EXPORT) else {
        return Ok(None);
    };
    let Some(func) = export.into_func() else {
        return Err(CooldisWasmError::RuntimeExecution(format!(
            "{DESCRIBE_EXPORT} export is not a function"
        )));
    };
    let describe = func
        .typed::<u32, i32>(&store)
        .map_err(|err| CooldisWasmError::RuntimeExecution(err.to_string()))?;
    let status = describe
        .call_async(&mut store, OUTPUT_SINK)
        .await
        .map_err(|err| CooldisWasmError::RuntimeExecution(err.to_string()))?;
    if status != STATUS_OK {
        return Err(CooldisWasmError::RuntimeExecution(format!(
            "{DESCRIBE_EXPORT} returned status {status}"
        )));
    }
    let bytes = store.into_data().take_sink(OUTPUT_SINK);
    let manifest: WasmOperationManifest = serde_json::from_slice(&bytes).map_err(|err| {
        CooldisWasmError::RuntimeExecution(format!(
            "failed to decode wasm operation manifest: {err}"
        ))
    })?;
    validate_manifest(&manifest)?;
    Ok(Some(manifest))
}

async fn execute_operation(
    engine: &Engine,
    module: &Module,
    config: &WasmRuntimeConfig,
    manifest: &WasmOperationManifest,
    operation_name: &str,
    input: Vec<u8>,
) -> CooldisWasmResult<WasmOperationOutput> {
    let operation = manifest
        .operation(operation_name)
        .ok_or_else(|| {
            CooldisWasmError::RuntimeExecution(format!(
                "wasm operation {operation_name:?} is not in manifest"
            ))
        })?
        .clone();
    let effective_grants = config.effective_capability_grants();
    let missing_capabilities: Vec<_> = operation
        .required_capabilities
        .iter()
        .filter(|capability| !effective_grants.contains(capability.as_str()))
        .cloned()
        .collect();
    if !missing_capabilities.is_empty() {
        return Err(CooldisWasmError::RuntimeExecution(format!(
            "wasm operation {:?} requires ungranted capabilities: {}",
            operation.name,
            missing_capabilities.join(", ")
        )));
    }

    let mut linker = Linker::new(engine);
    add_cooldis_imports(&mut linker)
        .map_err(|err| CooldisWasmError::RuntimeExecution(err.to_string()))?;
    let input = truncate_bytes(input, config.max_input_bytes);
    let mut store = Store::new(engine, WasmTurnState::new(input, config));
    configure_store(&mut store, config)?;
    let instance = linker
        .instantiate_async(&mut store, module)
        .await
        .map_err(|err| CooldisWasmError::RuntimeExecution(err.to_string()))?;
    let call = instance
        .get_typed_func::<(u32, u32, u32, u32, u32), i32>(&mut store, CALL_OPERATION_EXPORT)
        .map_err(|err| CooldisWasmError::RuntimeExecution(err.to_string()))?;
    let status = call
        .call_async(
            &mut store,
            (
                operation.id,
                INVOCATION_HANDLE,
                INPUT_SOURCE,
                OUTPUT_SINK,
                EVENT_SINK,
            ),
        )
        .await
        .map_err(|err| CooldisWasmError::RuntimeExecution(err.to_string()))?;
    if status != STATUS_OK {
        return Err(CooldisWasmError::RuntimeExecution(format!(
            "{CALL_OPERATION_EXPORT} for {:?} returned status {status}",
            operation.name
        )));
    }
    let mut state = store.into_data();
    let invocation_context = state.invocation_context.clone();
    Ok(WasmOperationOutput {
        manifest: manifest.clone(),
        operation,
        output: state.take_sink(OUTPUT_SINK),
        events: state.take_sink(EVENT_SINK),
        invocation_context,
    })
}

fn validate_manifest(manifest: &WasmOperationManifest) -> CooldisWasmResult<()> {
    if manifest.abi != OPERATION_ABI {
        return Err(CooldisWasmError::RuntimeExecution(format!(
            "unsupported wasm operation abi {:?}",
            manifest.abi
        )));
    }
    if manifest.operations.is_empty() {
        return Err(CooldisWasmError::RuntimeExecution(
            "wasm operation manifest has no operations".to_string(),
        ));
    }
    let mut ids = BTreeSet::new();
    let mut names = BTreeSet::new();
    for operation in &manifest.operations {
        if operation.id == 0 {
            return Err(CooldisWasmError::RuntimeExecution(
                "wasm operation id 0 is reserved".to_string(),
            ));
        }
        if operation.name.trim().is_empty() {
            return Err(CooldisWasmError::RuntimeExecution(
                "wasm operation name cannot be empty".to_string(),
            ));
        }
        if !ids.insert(operation.id) {
            return Err(CooldisWasmError::RuntimeExecution(format!(
                "duplicate wasm operation id {}",
                operation.id
            )));
        }
        if !names.insert(operation.name.as_str()) {
            return Err(CooldisWasmError::RuntimeExecution(format!(
                "duplicate wasm operation name {:?}",
                operation.name
            )));
        }
    }
    Ok(())
}

fn validate_module_imports(module: &Module, policy: WasmHostImportPolicy) -> CooldisWasmResult<()> {
    let mut unsupported = Vec::new();
    for import in module.imports() {
        let module_name = import.module();
        let name = import.name();
        if allowed_import(module_name, name, import.ty(), policy) {
            continue;
        }
        let diagnostic = import_diagnostic(module_name, name);
        unsupported.push(format!("{module_name}::{name} ({diagnostic})"));
    }
    if unsupported.is_empty() {
        Ok(())
    } else {
        Err(CooldisWasmError::RuntimeFactory(format!(
            "wasm operation artifact imports unsupported host functions: {}",
            unsupported.join(", ")
        )))
    }
}

fn allowed_import(
    module_name: &str,
    name: &str,
    ty: ExternType,
    policy: WasmHostImportPolicy,
) -> bool {
    let is_function = matches!(ty, ExternType::Func(_));
    is_function
        && match policy {
            WasmHostImportPolicy::Operation => matches!(
                (module_name, name),
                ("cooldis", "input_len")
                    | ("cooldis", "input_read")
                    | ("cooldis", "output_write")
                    | ("cooldis", "log")
                    | ("cooldis_0.1", "source_read")
                    | ("cooldis_0.1", "sink_write")
                    | ("cooldis_0.1", "event_emit")
                    | ("cooldis_0.1", "http_request")
                    | ("cooldis_0.1", "check_cancelled")
                    | ("cooldis_0.1", "fs_open")
                    | ("cooldis_0.1", "fs_read")
                    | ("cooldis_0.1", "fs_close")
                    | ("cooldis_0.1", "log")
            ),
            WasmHostImportPolicy::PureCompute => matches!(
                (module_name, name),
                ("cooldis", "input_len")
                    | ("cooldis", "input_read")
                    | ("cooldis", "output_write")
                    | ("cooldis", "log")
                    | ("cooldis_0.1", "source_read")
                    | ("cooldis_0.1", "sink_write")
                    | ("cooldis_0.1", "event_emit")
                    | ("cooldis_0.1", "check_cancelled")
                    | ("cooldis_0.1", "log")
            ),
        }
}

fn import_diagnostic(module_name: &str, name: &str) -> &'static str {
    if module_name.contains("wbindgen") || name.contains("wbindgen") {
        "wasm-bindgen/browser imports are not supported by the Cooldis operation ABI; compile for wasm32-unknown-unknown with the guest SDK exports"
    } else if module_name == "wasi_snapshot_preview1" {
        if name == "random_get" {
            "WASI random_get is not available; deterministic operations must request host capabilities explicitly"
        } else {
            "WASI imports are not available in this runner yet; use Cooldis ABI imports instead"
        }
    } else if module_name.contains("getrandom")
        || name.contains("getrandom")
        || name.contains("random")
    {
        "ambient randomness is not available; add an explicit host capability before using random bytes"
    } else {
        "only cooldis_0.1 operation imports are available"
    }
}

fn configure_store(
    store: &mut Store<WasmTurnState>,
    config: &WasmRuntimeConfig,
) -> CooldisWasmResult<()> {
    store.limiter(|state| &mut state.limits);
    if let Some(fuel) = config.fuel {
        store
            .set_fuel(fuel)
            .map_err(|err| CooldisWasmError::RuntimeExecution(err.to_string()))?;
    }
    if let Some(interval) = config.fuel_yield_interval {
        store
            .fuel_async_yield_interval(Some(interval))
            .map_err(|err| CooldisWasmError::RuntimeExecution(err.to_string()))?;
    }
    Ok(())
}

struct WasmTurnState {
    input: Vec<u8>,
    input_offset: usize,
    sources: HashMap<u32, WasmSourceState>,
    next_source: u32,
    files: HashMap<u32, WasmFileState>,
    next_file: u32,
    sinks: HashMap<u32, Vec<u8>>,
    output_truncated: bool,
    max_output_bytes: usize,
    capability_grants: BTreeSet<String>,
    invocation_context: InvocationContext,
    secrets: BTreeMap<String, String>,
    vfs: Option<Arc<CooldisVfs>>,
    limits: wasmtime::StoreLimits,
}

struct WasmSourceState {
    bytes: Vec<u8>,
    offset: usize,
}

struct WasmFileState {
    bytes: Vec<u8>,
    offset: usize,
}

impl WasmTurnState {
    fn new(input: Vec<u8>, config: &WasmRuntimeConfig) -> Self {
        let mut limits = StoreLimitsBuilder::new();
        if let Some(memory_limit_bytes) = config.memory_limit_bytes {
            limits = limits.memory_size(memory_limit_bytes);
        }
        Self {
            input,
            input_offset: 0,
            sources: HashMap::new(),
            next_source: FIRST_DYNAMIC_SOURCE,
            files: HashMap::new(),
            next_file: FIRST_DYNAMIC_FILE,
            sinks: HashMap::new(),
            output_truncated: false,
            max_output_bytes: config.max_output_bytes,
            capability_grants: config.effective_capability_grants(),
            invocation_context: config.invocation_context.clone(),
            secrets: config.secrets.clone(),
            vfs: config.vfs.clone(),
            limits: limits.build(),
        }
    }

    fn take_sink(&mut self, sink: u32) -> Vec<u8> {
        self.sinks.remove(&sink).unwrap_or_default()
    }

    fn insert_source(&mut self, bytes: Vec<u8>) -> u32 {
        let handle = self.next_source;
        self.next_source = self.next_source.saturating_add(1);
        self.sources
            .insert(handle, WasmSourceState { bytes, offset: 0 });
        handle
    }

    fn read_source_chunk(&mut self, source: u32, capacity: usize) -> Option<(Vec<u8>, bool)> {
        if source == INPUT_SOURCE {
            let remaining = self.input.len().saturating_sub(self.input_offset);
            let copied = remaining.min(capacity);
            let bytes = self.input[self.input_offset..self.input_offset + copied].to_vec();
            self.input_offset += copied;
            let exhausted = self.input_offset >= self.input.len();
            return Some((bytes, exhausted));
        }

        let source = self.sources.get_mut(&source)?;
        let remaining = source.bytes.len().saturating_sub(source.offset);
        let copied = remaining.min(capacity);
        let bytes = source.bytes[source.offset..source.offset + copied].to_vec();
        source.offset += copied;
        let exhausted = source.offset >= source.bytes.len();
        Some((bytes, exhausted))
    }

    fn insert_file(&mut self, bytes: Vec<u8>) -> u32 {
        let handle = self.next_file;
        self.next_file = self.next_file.saturating_add(1);
        self.files
            .insert(handle, WasmFileState { bytes, offset: 0 });
        handle
    }

    fn read_file_chunk(&mut self, handle: u32, capacity: usize) -> Option<(Vec<u8>, bool)> {
        let file = self.files.get_mut(&handle)?;
        let remaining = file.bytes.len().saturating_sub(file.offset);
        let copied = remaining.min(capacity);
        let bytes = file.bytes[file.offset..file.offset + copied].to_vec();
        file.offset += copied;
        let exhausted = file.offset >= file.bytes.len();
        Some((bytes, exhausted))
    }

    fn close_file(&mut self, handle: u32) -> bool {
        self.files.remove(&handle).is_some()
    }
}

fn add_cooldis_imports(linker: &mut Linker<WasmTurnState>) -> wasmtime::Result<()> {
    linker.func_wrap(
        "cooldis",
        "input_len",
        |caller: Caller<'_, WasmTurnState>| -> i32 { saturating_i32(caller.data().input.len()) },
    )?;

    linker.func_wrap(
        "cooldis",
        "input_read",
        |mut caller: Caller<'_, WasmTurnState>, ptr: i32, max_len: i32| -> i32 {
            let ptr = nonnegative_usize(ptr);
            let max_len = nonnegative_usize(max_len);
            let Some(ptr) = ptr else {
                return -1;
            };
            let Some(max_len) = max_len else {
                return -1;
            };
            let input = caller.data().input.clone();
            let copied = input.len().min(max_len);
            let Some(memory) = exported_memory(&mut caller) else {
                return -1;
            };
            let data = memory.data_mut(&mut caller);
            let Some(end) = ptr.checked_add(copied) else {
                return -1;
            };
            if end > data.len() {
                return -1;
            }
            data[ptr..end].copy_from_slice(&input[..copied]);
            saturating_i32(copied)
        },
    )?;

    linker.func_wrap(
        "cooldis",
        "output_write",
        |mut caller: Caller<'_, WasmTurnState>, ptr: i32, len: i32| {
            let Some(bytes) = read_guest_memory(&mut caller, ptr, len) else {
                caller.data_mut().output_truncated = true;
                return;
            };
            append_output(caller.data_mut(), &bytes);
        },
    )?;

    linker.func_wrap(
        "cooldis",
        "log",
        |mut caller: Caller<'_, WasmTurnState>, ptr: i32, len: i32| {
            let Some(bytes) = read_guest_memory(&mut caller, ptr, len) else {
                return;
            };
            let mut line = b"[wasm log] ".to_vec();
            line.extend_from_slice(&bytes);
            if !line.ends_with(b"\n") {
                line.push(b'\n');
            }
            append_output(caller.data_mut(), &line);
        },
    )?;

    linker.func_wrap(
        "cooldis_0.1",
        "source_read",
        |mut caller: Caller<'_, WasmTurnState>, source: i32, ptr: i32, len_ptr: i32| -> i32 {
            let Some(source) = nonnegative_u32(source) else {
                return STATUS_INVALID_ARGUMENT;
            };
            let Some(ptr) = nonnegative_usize(ptr) else {
                return STATUS_INVALID_ARGUMENT;
            };
            let Some(len_ptr) = nonnegative_usize(len_ptr) else {
                return STATUS_INVALID_ARGUMENT;
            };
            let Some(capacity) = read_guest_u32_at(&mut caller, len_ptr) else {
                return STATUS_INVALID_ARGUMENT;
            };
            let capacity = capacity as usize;
            let Some((bytes, exhausted)) = caller.data_mut().read_source_chunk(source, capacity)
            else {
                return STATUS_NOT_FOUND;
            };
            let copied = bytes.len();
            let Some(memory) = exported_memory(&mut caller) else {
                return STATUS_INVALID_ARGUMENT;
            };
            let data = memory.data_mut(&mut caller);
            let Some(end) = ptr.checked_add(copied) else {
                return STATUS_INVALID_ARGUMENT;
            };
            if end > data.len() {
                return STATUS_INVALID_ARGUMENT;
            }
            data[ptr..end].copy_from_slice(&bytes);
            if !write_guest_u32_at(&mut caller, len_ptr, copied as u32) {
                return STATUS_INVALID_ARGUMENT;
            }
            if exhausted { -1 } else { STATUS_OK }
        },
    )?;

    linker.func_wrap(
        "cooldis_0.1",
        "sink_write",
        |mut caller: Caller<'_, WasmTurnState>, sink: i32, ptr: i32, len_ptr: i32| -> i32 {
            let Some(sink) = nonnegative_u32(sink) else {
                return STATUS_INVALID_ARGUMENT;
            };
            if sink != OUTPUT_SINK && sink != EVENT_SINK {
                return STATUS_NOT_FOUND;
            }
            let Some(len_ptr) = nonnegative_usize(len_ptr) else {
                return STATUS_INVALID_ARGUMENT;
            };
            let Some(len) = read_guest_u32_at(&mut caller, len_ptr) else {
                return STATUS_INVALID_ARGUMENT;
            };
            let Some(bytes) = read_guest_memory(&mut caller, ptr, len as i32) else {
                caller.data_mut().output_truncated = true;
                return STATUS_INVALID_ARGUMENT;
            };
            append_sink(caller.data_mut(), sink, &bytes);
            if !write_guest_u32_at(&mut caller, len_ptr, bytes.len() as u32) {
                return STATUS_INVALID_ARGUMENT;
            }
            STATUS_OK
        },
    )?;

    linker.func_wrap(
        "cooldis_0.1",
        "event_emit",
        |mut caller: Caller<'_, WasmTurnState>, _invocation: i32, ptr: i32, len_ptr: i32| -> i32 {
            let Some(len_ptr) = nonnegative_usize(len_ptr) else {
                return STATUS_INVALID_ARGUMENT;
            };
            let Some(len) = read_guest_u32_at(&mut caller, len_ptr) else {
                return STATUS_INVALID_ARGUMENT;
            };
            let Some(bytes) = read_guest_memory(&mut caller, ptr, len as i32) else {
                caller.data_mut().output_truncated = true;
                return STATUS_INVALID_ARGUMENT;
            };
            append_sink(caller.data_mut(), EVENT_SINK, &bytes);
            if !write_guest_u32_at(&mut caller, len_ptr, bytes.len() as u32) {
                return STATUS_INVALID_ARGUMENT;
            }
            STATUS_OK
        },
    )?;

    linker.func_wrap_async(
        "cooldis_0.1",
        "http_request",
        |mut caller: Caller<'_, WasmTurnState>,
         (invocation, request_ptr, request_len, body_ptr, body_len, out_ptr, event_sink): (
            i32,
            i32,
            i32,
            i32,
            i32,
            i32,
            i32,
        )| {
            Box::new(async move {
                if nonnegative_u32(invocation).is_none() {
                    return STATUS_INVALID_ARGUMENT;
                }
                let Some(out_ptr) = nonnegative_usize(out_ptr) else {
                    return STATUS_INVALID_ARGUMENT;
                };
                let Some(request_bytes) = read_guest_memory(&mut caller, request_ptr, request_len)
                else {
                    return STATUS_INVALID_ARGUMENT;
                };
                let body_bytes = if body_len == 0 {
                    Vec::new()
                } else if let Some(bytes) = read_guest_memory(&mut caller, body_ptr, body_len) {
                    bytes
                } else {
                    return STATUS_INVALID_ARGUMENT;
                };
                let Some(event_sink) = nonnegative_u32(event_sink) else {
                    return STATUS_INVALID_ARGUMENT;
                };
                if event_sink != EVENT_SINK {
                    return STATUS_NOT_FOUND;
                }
                let grants = caller.data().capability_grants.clone();
                let secrets = caller.data().secrets.clone();

                match execute_http_request(request_bytes, body_bytes, grants, secrets).await {
                    Ok(exchange) => {
                        let Some(body_source_ptr) = out_ptr.checked_add(4) else {
                            return STATUS_INVALID_ARGUMENT;
                        };
                        let response_meta = match serde_json::to_vec(&exchange.response) {
                            Ok(bytes) => bytes,
                            Err(_) => return STATUS_INVALID_ARGUMENT,
                        };
                        let meta_source = caller.data_mut().insert_source(response_meta);
                        let body_source = caller.data_mut().insert_source(exchange.body);
                        if !write_guest_u32_at(&mut caller, out_ptr, meta_source)
                            || !write_guest_u32_at(&mut caller, body_source_ptr, body_source)
                        {
                            return STATUS_INVALID_ARGUMENT;
                        }
                        append_http_event(caller.data_mut(), event_sink, "http_response", None);
                        STATUS_OK
                    }
                    Err(err) => {
                        append_http_event(
                            caller.data_mut(),
                            event_sink,
                            err.event_code(),
                            Some(&err.message),
                        );
                        err.status
                    }
                }
            })
        },
    )?;

    linker.func_wrap_async(
        "cooldis_0.1",
        "fs_open",
        |mut caller: Caller<'_, WasmTurnState>,
         (path_ptr, path_len, mode, out_handle_ptr): (i32, i32, i32, i32)| {
            Box::new(async move {
                let Some(mode) = nonnegative_u32(mode) else {
                    return STATUS_INVALID_ARGUMENT;
                };
                if mode != FS_MODE_READ {
                    return STATUS_INVALID_ARGUMENT;
                }
                let Some(out_handle_ptr) = nonnegative_usize(out_handle_ptr) else {
                    return STATUS_INVALID_ARGUMENT;
                };
                let Some(path_bytes) = read_guest_memory(&mut caller, path_ptr, path_len) else {
                    return STATUS_INVALID_ARGUMENT;
                };
                let Ok(path) = String::from_utf8(path_bytes) else {
                    return STATUS_INVALID_ARGUMENT;
                };
                let path = PathBuf::from(path);
                if !path.is_absolute() {
                    return STATUS_INVALID_ARGUMENT;
                }
                let Some(vfs) = caller.data().vfs.clone() else {
                    return STATUS_CAPABILITY_DENIED;
                };
                let bytes = match vfs.read_file(&path).await {
                    Ok(bytes) => bytes,
                    Err(err) => return vfs_error_status(err),
                };
                let handle = caller.data_mut().insert_file(bytes);
                if !write_guest_u32_at(&mut caller, out_handle_ptr, handle) {
                    return STATUS_INVALID_ARGUMENT;
                }
                STATUS_OK
            })
        },
    )?;

    linker.func_wrap(
        "cooldis_0.1",
        "fs_read",
        |mut caller: Caller<'_, WasmTurnState>, handle: i32, ptr: i32, len_ptr: i32| -> i32 {
            let Some(handle) = nonnegative_u32(handle) else {
                return STATUS_INVALID_ARGUMENT;
            };
            let Some(ptr) = nonnegative_usize(ptr) else {
                return STATUS_INVALID_ARGUMENT;
            };
            let Some(len_ptr) = nonnegative_usize(len_ptr) else {
                return STATUS_INVALID_ARGUMENT;
            };
            let Some(capacity) = read_guest_u32_at(&mut caller, len_ptr) else {
                return STATUS_INVALID_ARGUMENT;
            };
            let capacity = capacity as usize;
            let Some((bytes, exhausted)) = caller.data_mut().read_file_chunk(handle, capacity)
            else {
                return STATUS_NOT_FOUND;
            };
            let copied = bytes.len();
            let Some(memory) = exported_memory(&mut caller) else {
                return STATUS_INVALID_ARGUMENT;
            };
            let data = memory.data_mut(&mut caller);
            let Some(end) = ptr.checked_add(copied) else {
                return STATUS_INVALID_ARGUMENT;
            };
            if end > data.len() {
                return STATUS_INVALID_ARGUMENT;
            }
            data[ptr..end].copy_from_slice(&bytes);
            if !write_guest_u32_at(&mut caller, len_ptr, copied as u32) {
                return STATUS_INVALID_ARGUMENT;
            }
            if copied == 0 && exhausted {
                STATUS_EOF
            } else {
                STATUS_OK
            }
        },
    )?;

    linker.func_wrap(
        "cooldis_0.1",
        "fs_close",
        |mut caller: Caller<'_, WasmTurnState>, handle: i32| -> i32 {
            let Some(handle) = nonnegative_u32(handle) else {
                return STATUS_INVALID_ARGUMENT;
            };
            if caller.data_mut().close_file(handle) {
                STATUS_OK
            } else {
                STATUS_NOT_FOUND
            }
        },
    )?;

    linker.func_wrap(
        "cooldis_0.1",
        "log",
        |mut caller: Caller<'_, WasmTurnState>, _level: i32, ptr: i32, len: i32| -> i32 {
            let Some(bytes) = read_guest_memory(&mut caller, ptr, len) else {
                return STATUS_INVALID_ARGUMENT;
            };
            let mut line = b"[wasm log] ".to_vec();
            line.extend_from_slice(&bytes);
            if !line.ends_with(b"\n") {
                line.push(b'\n');
            }
            append_sink(caller.data_mut(), EVENT_SINK, &line);
            STATUS_OK
        },
    )?;

    linker.func_wrap(
        "cooldis_0.1",
        "check_cancelled",
        |_caller: Caller<'_, WasmTurnState>, invocation: i32| -> i32 {
            if nonnegative_u32(invocation).is_some() {
                STATUS_OK
            } else {
                STATUS_INVALID_ARGUMENT
            }
        },
    )?;

    Ok(())
}

#[doc(hidden)]
#[derive(Debug)]
pub struct WasmHttpExchange {
    pub response: WasmHttpResponse,
    pub body: Vec<u8>,
}

#[doc(hidden)]
#[derive(Debug)]
pub struct WasmHttpError {
    pub status: i32,
    pub message: String,
}

impl WasmHttpError {
    fn invalid_argument(message: impl Into<String>) -> Self {
        Self {
            status: STATUS_INVALID_ARGUMENT,
            message: message.into(),
        }
    }

    fn capability_denied(message: impl Into<String>) -> Self {
        Self {
            status: STATUS_CAPABILITY_DENIED,
            message: message.into(),
        }
    }

    fn transport(message: impl Into<String>) -> Self {
        Self {
            status: STATUS_TRANSPORT_ERROR,
            message: message.into(),
        }
    }

    fn timeout(message: impl Into<String>) -> Self {
        Self {
            status: STATUS_TIMEOUT,
            message: message.into(),
        }
    }

    fn event_code(&self) -> &'static str {
        match self.status {
            STATUS_INVALID_ARGUMENT => "http_invalid_argument",
            STATUS_CAPABILITY_DENIED => "http_capability_denied",
            STATUS_TRANSPORT_ERROR => "http_transport_error",
            STATUS_TIMEOUT => "http_timeout",
            STATUS_CANCELLED => "http_cancelled",
            _ => "http_error",
        }
    }
}

#[doc(hidden)]
pub async fn execute_http_request(
    request_bytes: Vec<u8>,
    body: Vec<u8>,
    grants: BTreeSet<String>,
    secrets: BTreeMap<String, String>,
) -> Result<WasmHttpExchange, WasmHttpError> {
    let request: WasmHttpRequest = serde_json::from_slice(&request_bytes)
        .map_err(|err| WasmHttpError::invalid_argument(format!("invalid HTTP request: {err}")))?;
    if request.abi != HTTP_ABI {
        return Err(WasmHttpError::invalid_argument(format!(
            "unsupported HTTP ABI {:?}",
            request.abi
        )));
    }

    let method = reqwest::Method::from_bytes(request.method.as_bytes())
        .map_err(|_| WasmHttpError::invalid_argument("invalid HTTP method"))?;
    let url = reqwest::Url::parse(&request.url)
        .map_err(|_| WasmHttpError::invalid_argument("invalid HTTP url"))?;
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(WasmHttpError::invalid_argument(
            "HTTP url scheme must be http or https",
        ));
    }

    let origin = http_origin(&url)
        .ok_or_else(|| WasmHttpError::invalid_argument("HTTP url must include a host"))?;
    let private_destination = is_private_or_special_url(&url);
    ensure_http_capability(&grants, &method, &origin, private_destination)?;

    let mut builder = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .dns_resolver(Arc::new(FilteredDnsResolver));
    let timeout_ms = request
        .timeout_ms
        .unwrap_or(HTTP_DEFAULT_TIMEOUT_MS)
        .min(HTTP_MAX_TIMEOUT_MS);
    builder = builder.timeout(Duration::from_millis(timeout_ms));
    let client = builder
        .build()
        .map_err(|err| WasmHttpError::transport(sanitize_http_error(err)))?;

    let mut http = client.request(method.clone(), url).body(body);
    for (name, value) in request.headers {
        let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| WasmHttpError::invalid_argument("invalid HTTP header name"))?;
        let value = reqwest::header::HeaderValue::from_str(&value)
            .map_err(|_| WasmHttpError::invalid_argument("invalid HTTP header value"))?;
        http = http.header(name, value);
    }
    for (name, secret_name) in request.secret_headers {
        let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| WasmHttpError::invalid_argument("invalid HTTP secret header name"))?;
        let capability = format!("secret:{secret_name}");
        if !grants.contains(&capability) {
            return Err(WasmHttpError::capability_denied(
                "missing required secret capability",
            ));
        }
        let Some(value) = secrets.get(&secret_name) else {
            return Err(WasmHttpError::capability_denied(
                "required secret is not available",
            ));
        };
        let value = reqwest::header::HeaderValue::from_str(value).map_err(|_| {
            WasmHttpError::invalid_argument("secret value is not valid for HTTP header")
        })?;
        http = http.header(name, value);
    }

    let started_at = Instant::now();
    let response = http.send().await.map_err(|err| {
        if err.is_timeout() {
            WasmHttpError::timeout("HTTP request timed out")
        } else {
            WasmHttpError::transport(sanitize_http_error(err))
        }
    })?;
    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                String::from_utf8_lossy(value.as_bytes()).to_string(),
            )
        })
        .collect();
    let max_response_bytes = request
        .max_response_bytes
        .unwrap_or(HTTP_DEFAULT_MAX_RESPONSE_BYTES)
        .min(HTTP_DEFAULT_MAX_RESPONSE_BYTES);
    let mut body = response.bytes().await.map_err(|err| {
        if err.is_timeout() {
            WasmHttpError::timeout("HTTP response timed out")
        } else {
            WasmHttpError::transport(sanitize_http_error(err))
        }
    })?;
    let truncated = body.len() > max_response_bytes;
    if truncated {
        body.truncate(max_response_bytes);
    }

    Ok(WasmHttpExchange {
        response: WasmHttpResponse {
            abi: HTTP_ABI.to_string(),
            status,
            headers,
            truncated,
            elapsed_ms: started_at
                .elapsed()
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
        },
        body: body.to_vec(),
    })
}

#[doc(hidden)]
pub fn ensure_http_capability(
    grants: &BTreeSet<String>,
    method: &reqwest::Method,
    origin: &str,
    private_destination: bool,
) -> Result<(), WasmHttpError> {
    let namespace = if private_destination {
        "net.http.private"
    } else {
        "net.http"
    };
    let method_grant = format!("{namespace}:{}:{origin}", method.as_str());
    if grants
        .iter()
        .any(|grant| http_capability_matches(grant, namespace, method.as_str(), origin))
    {
        Ok(())
    } else {
        Err(WasmHttpError::capability_denied(format!(
            "missing required capability {method_grant}"
        )))
    }
}

fn http_capability_matches(grant: &str, namespace: &str, method: &str, origin: &str) -> bool {
    let Some(rest) = grant
        .strip_prefix(namespace)
        .and_then(|tail| tail.strip_prefix(':'))
    else {
        return false;
    };
    if rest == "*" {
        return true;
    }
    if let Some(origin_pattern) = rest.strip_prefix("*:") {
        return wildcard_match(origin_pattern, origin);
    }
    if let Some(origin_pattern) = rest
        .strip_prefix(method)
        .and_then(|tail| tail.strip_prefix(':'))
    {
        return wildcard_match(origin_pattern, origin);
    }
    wildcard_match(rest, origin)
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    let pattern = pattern.as_bytes();
    let value = value.as_bytes();
    let (mut pattern_index, mut value_index) = (0, 0);
    let mut star_index = None;
    let mut star_value_index = 0;

    while value_index < value.len() {
        if pattern_index < pattern.len() && pattern[pattern_index] == value[value_index] {
            pattern_index += 1;
            value_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
            star_index = Some(pattern_index);
            star_value_index = value_index;
            pattern_index += 1;
        } else if let Some(index) = star_index {
            pattern_index = index + 1;
            star_value_index += 1;
            value_index = star_value_index;
        } else {
            return false;
        }
    }

    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}

fn vfs_error_status(err: BashkitError) -> i32 {
    match err {
        BashkitError::Io(err) => match err.kind() {
            std::io::ErrorKind::NotFound => STATUS_NOT_FOUND,
            std::io::ErrorKind::PermissionDenied => STATUS_CAPABILITY_DENIED,
            std::io::ErrorKind::InvalidInput => STATUS_INVALID_ARGUMENT,
            _ => STATUS_TRANSPORT_ERROR,
        },
        BashkitError::Cancelled => STATUS_CANCELLED,
        BashkitError::ResourceLimit(_) => STATUS_TRANSPORT_ERROR,
        BashkitError::Parse { .. }
        | BashkitError::Execution(_)
        | BashkitError::Network(_)
        | BashkitError::Regex(_)
        | BashkitError::Internal(_) => STATUS_TRANSPORT_ERROR,
    }
}

#[doc(hidden)]
pub fn http_origin(url: &reqwest::Url) -> Option<String> {
    let host = url.host_str()?;
    let host = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    };
    let default_port = match url.scheme() {
        "http" => Some(80),
        "https" => Some(443),
        _ => None,
    };
    let port = url.port().filter(|port| Some(*port) != default_port);
    Some(match port {
        Some(port) => format!("{}://{}:{}", url.scheme(), host, port),
        None => format!("{}://{}", url.scheme(), host),
    })
}

fn sanitize_http_error(mut err: reqwest::Error) -> String {
    if let Some(url) = err.url_mut() {
        url.set_query(None);
    }
    err.to_string()
}

fn append_http_event(
    state: &mut WasmTurnState,
    event_sink: u32,
    code: &str,
    message: Option<&str>,
) {
    let event = serde_json::json!({
        "type": "http",
        "code": code,
        "message": message.unwrap_or_default(),
    });
    append_sink(state, event_sink, format!("{event}\n").as_bytes());
}

#[derive(Debug)]
struct FilteredDnsResolver;

impl reqwest::dns::Resolve for FilteredDnsResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_owned();
        Box::pin(async move {
            let addrs = tokio::net::lookup_host((host.as_str(), 0)).await?;
            let filtered_addrs: Vec<SocketAddr> = addrs
                .filter(|addr| !is_private_or_special_ip(addr.ip()))
                .collect();

            if filtered_addrs.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    BLOCKED_HTTP_ADDRESS_ERROR,
                )
                .into());
            }

            Ok(Box::new(filtered_addrs.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

fn is_private_or_special_url(url: &reqwest::Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .map(is_private_or_special_ip)
            .unwrap_or(false)
}

fn is_private_or_special_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_private_or_special_ipv4(ip),
        IpAddr::V6(ip) => is_private_or_special_ipv6(ip),
    }
}

fn is_private_or_special_ipv4(ip: Ipv4Addr) -> bool {
    ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_unspecified()
        || ip.octets()[0] == 0
        || ip.octets()[0] >= 224
        || (ip.octets()[0] == 100 && (ip.octets()[1] & 0b1100_0000) == 0b0100_0000)
        || (ip.octets()[0] == 198 && (ip.octets()[1] & 0b1111_1110) == 18)
}

fn is_private_or_special_ipv6(ip: Ipv6Addr) -> bool {
    ip.is_loopback()
        || ip.is_unspecified()
        || (ip.segments()[0] & 0xfe00) == 0xfc00
        || (ip.segments()[0] & 0xffc0) == 0xfe80
        || ip.segments()[0] == 0x2001 && ip.segments()[1] == 0x0db8
        || ip.segments()[0] == 0x5f00
}

fn exported_memory(caller: &mut Caller<'_, WasmTurnState>) -> Option<Memory> {
    caller
        .get_export("memory")
        .and_then(|external| external.into_memory())
}

fn read_guest_memory(
    caller: &mut Caller<'_, WasmTurnState>,
    ptr: i32,
    len: i32,
) -> Option<Vec<u8>> {
    let ptr = nonnegative_usize(ptr)?;
    let len = nonnegative_usize(len)?;
    let memory = exported_memory(caller)?;
    let data = memory.data(caller);
    let end = ptr.checked_add(len)?;
    if end > data.len() {
        return None;
    }
    Some(data[ptr..end].to_vec())
}

fn append_output(state: &mut WasmTurnState, bytes: &[u8]) {
    append_sink(state, OUTPUT_SINK, bytes);
}

fn append_sink(state: &mut WasmTurnState, sink: u32, bytes: &[u8]) {
    let current_len = state.sinks.get(&sink).map(Vec::len).unwrap_or_default();
    let remaining = state.max_output_bytes.saturating_sub(current_len);
    if bytes.len() > remaining {
        state
            .sinks
            .entry(sink)
            .or_default()
            .extend_from_slice(&bytes[..remaining]);
        state.output_truncated = true;
    } else {
        state
            .sinks
            .entry(sink)
            .or_default()
            .extend_from_slice(bytes);
    }
}

fn nonnegative_usize(value: i32) -> Option<usize> {
    if value < 0 {
        None
    } else {
        Some(value as usize)
    }
}

fn nonnegative_u32(value: i32) -> Option<u32> {
    if value < 0 { None } else { Some(value as u32) }
}

fn saturating_i32(value: usize) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

fn read_guest_u32_at(caller: &mut Caller<'_, WasmTurnState>, ptr: usize) -> Option<u32> {
    let memory = exported_memory(caller)?;
    let data = memory.data(caller);
    let end = ptr.checked_add(4)?;
    if end > data.len() {
        return None;
    }
    Some(u32::from_le_bytes(data[ptr..end].try_into().ok()?))
}

fn write_guest_u32_at(caller: &mut Caller<'_, WasmTurnState>, ptr: usize, value: u32) -> bool {
    let Some(memory) = exported_memory(caller) else {
        return false;
    };
    let data = memory.data_mut(caller);
    let Some(end) = ptr.checked_add(4) else {
        return false;
    };
    if end > data.len() {
        return false;
    }
    data[ptr..end].copy_from_slice(&value.to_le_bytes());
    true
}

fn truncate_bytes(mut bytes: Vec<u8>, max_len: usize) -> Vec<u8> {
    if bytes.len() > max_len {
        bytes.truncate(max_len);
    }
    bytes
}
