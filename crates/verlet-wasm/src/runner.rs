use bashkit::FileSystem as _;

#[doc(hidden)]
pub const OPERATION_ABI: &str = "cooldis.operation/0.1";
const DESCRIBE_EXPORT: &str = "__verlet_describe_module__";
const CALL_OPERATION_EXPORT: &str = "__verlet_call_operation__";
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
#[doc(hidden)]
pub const FS_MODE_WRITE: u32 = 1;
/// Capability grant required for guest-initiated VFS mutation (`fs_open` in
/// `FS_MODE_WRITE` and `fs_mkdir`). Read-side fs imports (`fs_open` in
/// `FS_MODE_READ`, `fs_read`, `fs_stat`, `fs_list`) are gated only by whether
/// a VFS is attached to the turn.
#[doc(hidden)]
pub const FS_WRITE_CAPABILITY: &str = "fs.write";
const BLOCKED_HTTP_ADDRESS_ERROR: &str =
    "refusing to connect to private or special-purpose address";

pub struct WasmRuntimeFactory {
    config: crate::WasmRuntimeConfig,
    engine: std::sync::Arc<wasmtime::Engine>,
}

impl WasmRuntimeFactory {
    pub fn new(config: crate::WasmRuntimeConfig) -> crate::VerletWasmResult<Self> {
        let mut engine_config = wasmtime::Config::new();
        engine_config.consume_fuel(config.fuel.is_some());
        configure_wasmtime_engine(&mut engine_config);
        let engine = wasmtime::Engine::new(&engine_config)
            .map_err(|err| crate::VerletWasmError::RuntimeFactory(err.to_string()))?;
        Ok(Self {
            config,
            engine: std::sync::Arc::new(engine),
        })
    }

    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> crate::VerletWasmResult<Self> {
        Self::new(crate::WasmRuntimeConfig::new(
            crate::WasmRuntimeArtifact::bytes(bytes),
        ))
    }

    pub async fn describe(
        &self,
    ) -> crate::VerletWasmResult<Option<verlet_abi::WasmOperationManifest>> {
        let module = self.load_module().await?;
        load_operation_manifest(&self.engine, &module, &self.config).await
    }

    pub async fn validate_operation_artifact(
        &self,
    ) -> crate::VerletWasmResult<verlet_abi::WasmOperationManifest> {
        let module = self.load_module().await?;
        validate_operation_module(&self.engine, &module, &self.config).await
    }

    pub async fn build_validated_operation_runtime(
        &self,
    ) -> crate::VerletWasmResult<WasmModuleRuntime> {
        let module = self.load_module().await?;
        let manifest = validate_operation_module(&self.engine, &module, &self.config).await?;
        Ok(WasmModuleRuntime {
            config: self.config.clone(),
            engine: std::sync::Arc::clone(&self.engine),
            module,
            manifest: Some(manifest),
        })
    }

    pub async fn invoke_operation_bytes(
        &self,
        operation_name: &str,
        input: impl Into<Vec<u8>>,
    ) -> crate::VerletWasmResult<verlet_process::process::WasmOperationOutput> {
        let module = self.load_module().await?;
        let manifest = load_operation_manifest(&self.engine, &module, &self.config)
            .await?
            .ok_or_else(|| {
                crate::VerletWasmError::RuntimeExecution(format!(
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
    ) -> crate::VerletWasmResult<verlet_process::process::VerletProcessHandle> {
        let output = self.invoke_operation_bytes(operation_name, input).await?;
        Ok(verlet_process::process::VerletProcessHandle::from_wasm_operation_output(None, output))
    }

    pub async fn build_runtime(&self) -> crate::VerletWasmResult<WasmModuleRuntime> {
        let module = self.load_module().await?;
        let manifest = load_operation_manifest(&self.engine, &module, &self.config).await?;
        Ok(WasmModuleRuntime {
            config: self.config.clone(),
            engine: std::sync::Arc::clone(&self.engine),
            module,
            manifest,
        })
    }

    async fn load_module(&self) -> crate::VerletWasmResult<std::sync::Arc<wasmtime::Module>> {
        let bytes = self.config.artifact.load_bytes().await?;
        reject_textual_wat_artifact(&bytes)?;
        let module = wasmtime::Module::new(&self.engine, bytes)
            .map_err(|err| crate::VerletWasmError::RuntimeFactory(err.to_string()))?;
        Ok(std::sync::Arc::new(module))
    }
}

async fn validate_operation_module(
    engine: &wasmtime::Engine,
    module: &wasmtime::Module,
    config: &crate::WasmRuntimeConfig,
) -> crate::VerletWasmResult<verlet_abi::WasmOperationManifest> {
    validate_module_imports(module, config.host_import_policy)?;
    let manifest = load_operation_manifest(engine, module, config)
        .await?
        .ok_or_else(|| {
            crate::VerletWasmError::RuntimeFactory(format!(
                "wasm operation artifact must export {DESCRIBE_EXPORT}; legacy handle_turn modules cannot be published as operations"
            ))
        })?;
    if !module
        .exports()
        .any(|export| export.name() == CALL_OPERATION_EXPORT)
    {
        return Err(crate::VerletWasmError::RuntimeFactory(format!(
            "wasm operation artifact must export {CALL_OPERATION_EXPORT}"
        )));
    }
    Ok(manifest)
}

fn configure_wasmtime_engine(engine_config: &mut wasmtime::Config) {
    // Wasmtime's default macOS Mach-port trap handler owns a process-global
    // helper thread. Under the parallel lib test harness that thread has
    // intermittently aborted in `mach_msg` with no Rust panic, so Verlet uses
    // the documented signal-handler trap path instead. This also matches the
    // runtime's fork/process-heavy embedding better than a Mach exception port.
    #[cfg(target_os = "macos")]
    engine_config.macos_use_mach_ports(false);
}

fn reject_textual_wat_artifact(bytes: &[u8]) -> crate::VerletWasmResult<()> {
    if bytes
        .iter()
        .copied()
        .skip_while(u8::is_ascii_whitespace)
        .take(7)
        .eq(b"(module".iter().copied())
    {
        return Err(crate::VerletWasmError::RuntimeFactory(
            "wasm runtime artifacts must be compiled .wasm bytes; textual WAT is only allowed in tests/fixtures before compilation"
                .to_string(),
        ));
    }
    Ok(())
}

pub struct WasmModuleRuntime {
    config: crate::WasmRuntimeConfig,
    engine: std::sync::Arc<wasmtime::Engine>,
    module: std::sync::Arc<wasmtime::Module>,
    manifest: Option<verlet_abi::WasmOperationManifest>,
}

impl WasmModuleRuntime {
    pub async fn invoke_operation_bytes(
        &self,
        operation_name: &str,
        input: impl Into<Vec<u8>>,
    ) -> crate::VerletWasmResult<verlet_process::process::WasmOperationOutput> {
        let manifest = self.manifest.as_ref().ok_or_else(|| {
            crate::VerletWasmError::RuntimeExecution(format!(
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

    pub async fn execute_turn(&self, input: String) -> crate::VerletWasmResult<String> {
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
        let mut linker = wasmtime::Linker::new(&self.engine);
        add_verlet_imports(&mut linker)
            .map_err(|err| crate::VerletWasmError::RuntimeExecution(err.to_string()))?;

        let mut store = wasmtime::Store::new(&self.engine, WasmTurnState::new(input, &self.config));
        configure_store(&mut store, &self.config)?;

        let instance = linker
            .instantiate_async(&mut store, &self.module)
            .await
            .map_err(|err| crate::VerletWasmError::RuntimeExecution(err.to_string()))?;
        let entry = instance
            .get_typed_func::<(), i32>(&mut store, &self.config.entrypoint)
            .map_err(|err| crate::VerletWasmError::RuntimeExecution(err.to_string()))?;
        let exit_code = entry
            .call_async(&mut store, ())
            .await
            .map_err(|err| crate::VerletWasmError::RuntimeExecution(err.to_string()))?;
        if exit_code != 0 {
            return Err(crate::VerletWasmError::RuntimeExecution(format!(
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
    engine: &wasmtime::Engine,
    module: &wasmtime::Module,
    config: &crate::WasmRuntimeConfig,
) -> crate::VerletWasmResult<Option<verlet_abi::WasmOperationManifest>> {
    if !module
        .exports()
        .any(|export| export.name() == DESCRIBE_EXPORT)
    {
        return Ok(None);
    }

    let mut linker = wasmtime::Linker::new(engine);
    add_verlet_imports(&mut linker)
        .map_err(|err| crate::VerletWasmError::RuntimeExecution(err.to_string()))?;
    let mut store = wasmtime::Store::new(engine, WasmTurnState::new(Vec::new(), config));
    configure_store(&mut store, config)?;
    let instance = linker
        .instantiate_async(&mut store, module)
        .await
        .map_err(|err| crate::VerletWasmError::RuntimeExecution(err.to_string()))?;
    let Some(export) = instance.get_export(&mut store, DESCRIBE_EXPORT) else {
        return Ok(None);
    };
    let Some(func) = export.into_func() else {
        return Err(crate::VerletWasmError::RuntimeExecution(format!(
            "{DESCRIBE_EXPORT} export is not a function"
        )));
    };
    let describe = func
        .typed::<u32, i32>(&store)
        .map_err(|err| crate::VerletWasmError::RuntimeExecution(err.to_string()))?;
    let status = describe
        .call_async(&mut store, OUTPUT_SINK)
        .await
        .map_err(|err| crate::VerletWasmError::RuntimeExecution(err.to_string()))?;
    if status != STATUS_OK {
        return Err(crate::VerletWasmError::RuntimeExecution(format!(
            "{DESCRIBE_EXPORT} returned status {status}"
        )));
    }
    let bytes = store.into_data().take_sink(OUTPUT_SINK);
    let manifest: verlet_abi::WasmOperationManifest =
        serde_json::from_slice(&bytes).map_err(|err| {
            crate::VerletWasmError::RuntimeExecution(format!(
                "failed to decode wasm operation manifest: {err}"
            ))
        })?;
    validate_manifest(&manifest)?;
    Ok(Some(manifest))
}

async fn execute_operation(
    engine: &wasmtime::Engine,
    module: &wasmtime::Module,
    config: &crate::WasmRuntimeConfig,
    manifest: &verlet_abi::WasmOperationManifest,
    operation_name: &str,
    input: Vec<u8>,
) -> crate::VerletWasmResult<verlet_process::process::WasmOperationOutput> {
    let operation = manifest
        .operation(operation_name)
        .ok_or_else(|| {
            crate::VerletWasmError::RuntimeExecution(format!(
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
        return Err(crate::VerletWasmError::RuntimeExecution(format!(
            "wasm operation {:?} requires ungranted capabilities: {}",
            operation.name,
            missing_capabilities.join(", ")
        )));
    }

    let mut linker = wasmtime::Linker::new(engine);
    add_verlet_imports(&mut linker)
        .map_err(|err| crate::VerletWasmError::RuntimeExecution(err.to_string()))?;
    let input = truncate_bytes(input, config.max_input_bytes);
    let mut store = wasmtime::Store::new(engine, WasmTurnState::new(input, config));
    configure_store(&mut store, config)?;
    let instance = linker
        .instantiate_async(&mut store, module)
        .await
        .map_err(|err| crate::VerletWasmError::RuntimeExecution(err.to_string()))?;
    let call = instance
        .get_typed_func::<(u32, u32, u32, u32, u32), i32>(&mut store, CALL_OPERATION_EXPORT)
        .map_err(|err| crate::VerletWasmError::RuntimeExecution(err.to_string()))?;
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
        .map_err(|err| crate::VerletWasmError::RuntimeExecution(err.to_string()))?;
    if status != STATUS_OK {
        return Err(crate::VerletWasmError::RuntimeExecution(format!(
            "{CALL_OPERATION_EXPORT} for {:?} returned status {status}",
            operation.name
        )));
    }
    let mut state = store.into_data();
    let invocation_context = state.invocation_context.clone();
    Ok(verlet_process::process::WasmOperationOutput {
        manifest: manifest.clone(),
        operation,
        output: state.take_sink(OUTPUT_SINK),
        events: state.take_sink(EVENT_SINK),
        invocation_context,
    })
}

fn validate_manifest(manifest: &verlet_abi::WasmOperationManifest) -> crate::VerletWasmResult<()> {
    if manifest.abi != OPERATION_ABI {
        return Err(crate::VerletWasmError::RuntimeExecution(format!(
            "unsupported wasm operation abi {:?}",
            manifest.abi
        )));
    }
    if manifest.operations.is_empty() {
        return Err(crate::VerletWasmError::RuntimeExecution(
            "wasm operation manifest has no operations".to_string(),
        ));
    }
    let mut ids = std::collections::BTreeSet::new();
    let mut names = std::collections::BTreeSet::new();
    for operation in &manifest.operations {
        if operation.id == 0 {
            return Err(crate::VerletWasmError::RuntimeExecution(
                "wasm operation id 0 is reserved".to_string(),
            ));
        }
        if operation.name.trim().is_empty() {
            return Err(crate::VerletWasmError::RuntimeExecution(
                "wasm operation name cannot be empty".to_string(),
            ));
        }
        if !ids.insert(operation.id) {
            return Err(crate::VerletWasmError::RuntimeExecution(format!(
                "duplicate wasm operation id {}",
                operation.id
            )));
        }
        if !names.insert(operation.name.as_str()) {
            return Err(crate::VerletWasmError::RuntimeExecution(format!(
                "duplicate wasm operation name {:?}",
                operation.name
            )));
        }
    }
    Ok(())
}

fn validate_module_imports(
    module: &wasmtime::Module,
    policy: crate::WasmHostImportPolicy,
) -> crate::VerletWasmResult<()> {
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
        Err(crate::VerletWasmError::RuntimeFactory(format!(
            "wasm operation artifact imports unsupported host functions: {}",
            unsupported.join(", ")
        )))
    }
}

fn allowed_import(
    module_name: &str,
    name: &str,
    ty: wasmtime::ExternType,
    policy: crate::WasmHostImportPolicy,
) -> bool {
    let is_function = matches!(ty, wasmtime::ExternType::Func(_));
    is_function
        && match policy {
            crate::WasmHostImportPolicy::Operation => matches!(
                (module_name, name),
                ("verlet", "input_len")
                    | ("verlet", "input_read")
                    | ("verlet", "output_write")
                    | ("verlet", "log")
                    | ("cooldis_0.1", "source_read")
                    | ("cooldis_0.1", "sink_write")
                    | ("cooldis_0.1", "event_emit")
                    | ("cooldis_0.1", "http_request")
                    | ("cooldis_0.1", "check_cancelled")
                    | ("cooldis_0.1", "fs_open")
                    | ("cooldis_0.1", "fs_read")
                    | ("cooldis_0.1", "fs_close")
                    | ("cooldis_0.1", "fs_write")
                    | ("cooldis_0.1", "fs_stat")
                    | ("cooldis_0.1", "fs_list")
                    | ("cooldis_0.1", "fs_mkdir")
                    | ("cooldis_0.1", "log")
            ),
            crate::WasmHostImportPolicy::PureCompute => matches!(
                (module_name, name),
                ("verlet", "input_len")
                    | ("verlet", "input_read")
                    | ("verlet", "output_write")
                    | ("verlet", "log")
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
        "wasm-bindgen/browser imports are not supported by the Verlet operation ABI; compile for wasm32-unknown-unknown with the guest SDK exports"
    } else if module_name == "wasi_snapshot_preview1" {
        if name == "random_get" {
            "WASI random_get is not available; deterministic operations must request host capabilities explicitly"
        } else {
            "WASI imports are not available in this runner yet; use Verlet ABI imports instead"
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
    store: &mut wasmtime::Store<WasmTurnState>,
    config: &crate::WasmRuntimeConfig,
) -> crate::VerletWasmResult<()> {
    store.limiter(|state| &mut state.limits);
    if let Some(fuel) = config.fuel {
        store
            .set_fuel(fuel)
            .map_err(|err| crate::VerletWasmError::RuntimeExecution(err.to_string()))?;
    }
    if let Some(interval) = config.fuel_yield_interval {
        store
            .fuel_async_yield_interval(Some(interval))
            .map_err(|err| crate::VerletWasmError::RuntimeExecution(err.to_string()))?;
    }
    Ok(())
}

struct WasmTurnState {
    input: Vec<u8>,
    input_offset: usize,
    sources: std::collections::HashMap<u32, WasmSourceState>,
    next_source: u32,
    files: std::collections::HashMap<u32, WasmFileState>,
    next_file: u32,
    sinks: std::collections::HashMap<u32, Vec<u8>>,
    output_truncated: bool,
    max_output_bytes: usize,
    capability_grants: std::collections::BTreeSet<String>,
    attachment_config: crate::WasmAttachmentConfig,
    invocation_context: verlet_abi::InvocationContext,
    secrets: std::collections::BTreeMap<String, String>,
    vfs: Option<std::sync::Arc<verlet_vfs::VerletVfs>>,
    limits: wasmtime::StoreLimits,
}

struct WasmSourceState {
    bytes: Vec<u8>,
    offset: usize,
}

enum WasmFileState {
    /// Read handle: the whole file is buffered host-side at `fs_open` time
    /// and drained by `fs_read`.
    Read { bytes: Vec<u8>, offset: usize },
    /// Write handle: `fs_write` appends into the host-side buffer; nothing
    /// touches the VFS until `fs_close` commits the buffer as one whole-file
    /// replace of `path`. A handle dropped without `fs_close` is discarded.
    Write {
        path: std::path::PathBuf,
        buffer: Vec<u8>,
    },
}

impl WasmTurnState {
    fn new(input: Vec<u8>, config: &crate::WasmRuntimeConfig) -> Self {
        let mut limits = wasmtime::StoreLimitsBuilder::new();
        if let Some(memory_limit_bytes) = config.memory_limit_bytes {
            limits = limits.memory_size(memory_limit_bytes);
        }
        Self {
            input,
            input_offset: 0,
            sources: std::collections::HashMap::new(),
            next_source: FIRST_DYNAMIC_SOURCE,
            files: std::collections::HashMap::new(),
            next_file: FIRST_DYNAMIC_FILE,
            sinks: std::collections::HashMap::new(),
            output_truncated: false,
            max_output_bytes: config.max_output_bytes,
            capability_grants: config.effective_capability_grants(),
            attachment_config: config.attachment_config.clone(),
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

    fn insert_read_file(&mut self, bytes: Vec<u8>) -> u32 {
        self.insert_file(WasmFileState::Read { bytes, offset: 0 })
    }

    /// Open a pending write buffer for `path`. The buffer lives host-side
    /// and is committed to the VFS only by `fs_close`.
    fn insert_write_file(&mut self, path: std::path::PathBuf) -> u32 {
        self.insert_file(WasmFileState::Write {
            path,
            buffer: Vec::new(),
        })
    }

    fn insert_file(&mut self, state: WasmFileState) -> u32 {
        let handle = self.next_file;
        self.next_file = self.next_file.saturating_add(1);
        self.files.insert(handle, state);
        handle
    }

    fn read_file_chunk(&mut self, handle: u32, capacity: usize) -> Result<(Vec<u8>, bool), i32> {
        let Some(state) = self.files.get_mut(&handle) else {
            return Err(STATUS_NOT_FOUND);
        };
        let WasmFileState::Read { bytes, offset } = state else {
            return Err(STATUS_INVALID_ARGUMENT);
        };
        let remaining = bytes.len().saturating_sub(*offset);
        let copied = remaining.min(capacity);
        let chunk = bytes[*offset..*offset + copied].to_vec();
        *offset += copied;
        let exhausted = *offset >= bytes.len();
        Ok((chunk, exhausted))
    }

    fn take_file(&mut self, handle: u32) -> Option<WasmFileState> {
        self.files.remove(&handle)
    }
}

fn add_verlet_imports(linker: &mut wasmtime::Linker<WasmTurnState>) -> wasmtime::Result<()> {
    linker.func_wrap(
        "verlet",
        "input_len",
        |caller: wasmtime::Caller<'_, WasmTurnState>| -> i32 {
            saturating_i32(caller.data().input.len())
        },
    )?;

    linker.func_wrap(
        "verlet",
        "input_read",
        |mut caller: wasmtime::Caller<'_, WasmTurnState>, ptr: i32, max_len: i32| -> i32 {
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
        "verlet",
        "output_write",
        |mut caller: wasmtime::Caller<'_, WasmTurnState>, ptr: i32, len: i32| {
            let Some(bytes) = read_guest_memory(&mut caller, ptr, len) else {
                caller.data_mut().output_truncated = true;
                return;
            };
            append_output(caller.data_mut(), &bytes);
        },
    )?;

    linker.func_wrap(
        "verlet",
        "log",
        |mut caller: wasmtime::Caller<'_, WasmTurnState>, ptr: i32, len: i32| {
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
        |mut caller: wasmtime::Caller<'_, WasmTurnState>,
         source: i32,
         ptr: i32,
         len_ptr: i32|
         -> i32 {
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
        |mut caller: wasmtime::Caller<'_, WasmTurnState>,
         sink: i32,
         ptr: i32,
         len_ptr: i32|
         -> i32 {
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
        |mut caller: wasmtime::Caller<'_, WasmTurnState>,
         _invocation: i32,
         ptr: i32,
         len_ptr: i32|
         -> i32 {
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
        |mut caller: wasmtime::Caller<'_, WasmTurnState>,
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
                let attachment_config = caller.data().attachment_config.clone();
                let secrets = caller.data().secrets.clone();

                match execute_http_request(
                    request_bytes,
                    body_bytes,
                    grants,
                    attachment_config,
                    secrets,
                )
                .await
                {
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
        |mut caller: wasmtime::Caller<'_, WasmTurnState>,
         (path_ptr, path_len, mode, out_handle_ptr): (i32, i32, i32, i32)| {
            Box::new(async move {
                let Some(mode) = nonnegative_u32(mode) else {
                    return STATUS_INVALID_ARGUMENT;
                };
                if mode != FS_MODE_READ && mode != FS_MODE_WRITE {
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
                let path = std::path::PathBuf::from(path);
                if !path.is_absolute() {
                    return STATUS_INVALID_ARGUMENT;
                }
                let Some(vfs) = caller.data().vfs.clone() else {
                    return STATUS_CAPABILITY_DENIED;
                };
                if mode == FS_MODE_WRITE {
                    return fs_open_write_impl(&mut caller, path, out_handle_ptr);
                }
                let bytes = match vfs.read_file(&path).await {
                    Ok(bytes) => bytes,
                    Err(err) => return vfs_error_status(err),
                };
                let handle = caller.data_mut().insert_read_file(bytes);
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
        |mut caller: wasmtime::Caller<'_, WasmTurnState>,
         handle: i32,
         ptr: i32,
         len_ptr: i32|
         -> i32 {
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
            let (bytes, exhausted) = match caller.data_mut().read_file_chunk(handle, capacity) {
                Ok(chunk) => chunk,
                Err(status) => return status,
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

    linker.func_wrap_async(
        "cooldis_0.1",
        "fs_close",
        |mut caller: wasmtime::Caller<'_, WasmTurnState>, (handle,): (i32,)| {
            Box::new(async move {
                let Some(handle) = nonnegative_u32(handle) else {
                    return STATUS_INVALID_ARGUMENT;
                };
                match caller.data_mut().take_file(handle) {
                    None => STATUS_NOT_FOUND,
                    Some(WasmFileState::Read { .. }) => STATUS_OK,
                    Some(WasmFileState::Write { path, buffer }) => {
                        fs_close_commit_impl(&mut caller, path, buffer).await
                    }
                }
            })
        },
    )?;

    linker.func_wrap(
        "cooldis_0.1",
        "fs_write",
        |mut caller: wasmtime::Caller<'_, WasmTurnState>, handle: i32, ptr: i32, len: i32| -> i32 {
            fs_write_impl(&mut caller, handle, ptr, len)
        },
    )?;

    linker.func_wrap_async(
        "cooldis_0.1",
        "fs_stat",
        |mut caller: wasmtime::Caller<'_, WasmTurnState>,
         (path_ptr, path_len, out_ptr): (i32, i32, i32)| {
            Box::new(async move { fs_stat_impl(&mut caller, path_ptr, path_len, out_ptr).await })
        },
    )?;

    linker.func_wrap_async(
        "cooldis_0.1",
        "fs_list",
        |mut caller: wasmtime::Caller<'_, WasmTurnState>,
         (path_ptr, path_len, out_source_ptr): (i32, i32, i32)| {
            Box::new(
                async move { fs_list_impl(&mut caller, path_ptr, path_len, out_source_ptr).await },
            )
        },
    )?;

    linker.func_wrap_async(
        "cooldis_0.1",
        "fs_mkdir",
        |mut caller: wasmtime::Caller<'_, WasmTurnState>,
         (path_ptr, path_len, recursive): (i32, i32, i32)| {
            Box::new(async move { fs_mkdir_impl(&mut caller, path_ptr, path_len, recursive).await })
        },
    )?;

    linker.func_wrap(
        "cooldis_0.1",
        "log",
        |mut caller: wasmtime::Caller<'_, WasmTurnState>, _level: i32, ptr: i32, len: i32| -> i32 {
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
        |_caller: wasmtime::Caller<'_, WasmTurnState>, invocation: i32| -> i32 {
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
    pub response: crate::WasmHttpResponse,
    pub body: Vec<u8>,
}

#[doc(hidden)]
#[derive(Debug)]
pub struct WasmHttpError {
    pub status: i32,
    pub message: String,
}

/// Canonical URL facts shared by import planning and the HTTP host gate.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedHttpUrl {
    pub url: String,
    pub origin: String,
    pub private_destination: bool,
    pub has_credentials: bool,
    pub has_query: bool,
    pub has_fragment: bool,
}

/// Parse and canonicalize one HTTP URL using the same rules as host execution.
#[doc(hidden)]
pub fn normalize_http_url(value: &str) -> Result<NormalizedHttpUrl, String> {
    let url = reqwest::Url::parse(value).map_err(|_| "invalid HTTP URL".to_string())?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("HTTP URL scheme must be http or https".to_string());
    }
    let origin = http_origin(&url).ok_or_else(|| "HTTP URL must include a host".to_string())?;
    Ok(NormalizedHttpUrl {
        url: url.to_string(),
        origin,
        private_destination: is_private_or_special_url(&url),
        has_credentials: !url.username().is_empty() || url.password().is_some(),
        has_query: url.query().is_some(),
        has_fragment: url.fragment().is_some(),
    })
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
    grants: std::collections::BTreeSet<String>,
    attachment_config: crate::WasmAttachmentConfig,
    secrets: std::collections::BTreeMap<String, String>,
) -> Result<WasmHttpExchange, WasmHttpError> {
    let mut request: crate::WasmHttpRequest = serde_json::from_slice(&request_bytes)
        .map_err(|err| WasmHttpError::invalid_argument(format!("invalid HTTP request: {err}")))?;
    if request.abi != HTTP_ABI {
        return Err(WasmHttpError::invalid_argument(format!(
            "unsupported HTTP ABI {:?}",
            request.abi
        )));
    }

    let body = apply_http_input_mapping(&mut request, body)?;
    let response_envelope = request.response_envelope;
    let method = reqwest::Method::from_bytes(request.method.as_bytes())
        .map_err(|_| WasmHttpError::invalid_argument("invalid HTTP method"))?;
    let target = normalize_http_url(&request.url).map_err(WasmHttpError::invalid_argument)?;
    if target.has_credentials {
        return Err(WasmHttpError::invalid_argument(
            "HTTP URL must not contain credentials",
        ));
    }
    let url = reqwest::Url::parse(&target.url)
        .map_err(|_| WasmHttpError::invalid_argument("invalid canonical HTTP URL"))?;
    ensure_http_capability(
        &grants,
        &attachment_config,
        &method,
        &target.origin,
        target.private_destination,
    )?;

    let mut builder = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .dns_resolver(std::sync::Arc::new(FilteredDnsResolver));
    let timeout_ms = request
        .timeout_ms
        .unwrap_or(HTTP_DEFAULT_TIMEOUT_MS)
        .min(HTTP_MAX_TIMEOUT_MS);
    builder = builder.timeout(std::time::Duration::from_millis(timeout_ms));
    let client = builder
        .build()
        .map_err(|err| WasmHttpError::transport(sanitize_http_error(err)))?;

    let mut headers = reqwest::header::HeaderMap::new();
    for (name, value) in request.headers {
        let name = outbound_header_name(&name, "invalid HTTP header name")?;
        let value = reqwest::header::HeaderValue::from_str(&value)
            .map_err(|_| WasmHttpError::invalid_argument("invalid HTTP header value"))?;
        headers.insert(name, value);
    }
    for (name, secret_name) in request.secret_headers {
        let name = outbound_header_name(&name, "invalid HTTP secret header name")?;
        if !attachment_config.allowed_secrets.contains(&secret_name) {
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
        headers.insert(name, value);
    }
    for (name, secret_name, prefix) in request.secret_header_prefixes {
        let name = outbound_header_name(&name, "invalid HTTP prefixed secret header name")?;
        if !attachment_config.allowed_secrets.contains(&secret_name) {
            return Err(WasmHttpError::capability_denied(
                "missing required secret capability",
            ));
        }
        let Some(value) = secrets.get(&secret_name) else {
            return Err(WasmHttpError::capability_denied(
                "required secret is not available",
            ));
        };
        let value =
            reqwest::header::HeaderValue::from_str(&format!("{prefix}{value}")).map_err(|_| {
                WasmHttpError::invalid_argument("secret value is not valid for HTTP header")
            })?;
        headers.insert(name, value);
    }
    let http = client
        .request(method.clone(), url)
        .headers(headers)
        .body(body);

    let started_at = std::time::Instant::now();
    let mut response = http.send().await.map_err(|err| {
        if err.is_timeout() {
            WasmHttpError::timeout("HTTP request timed out")
        } else {
            WasmHttpError::transport(sanitize_http_error(err))
        }
    })?;
    let status = response.status().as_u16();
    let headers: Vec<(String, String)> = response
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
    let mut body = Vec::with_capacity(max_response_bytes.min(64 * 1024));
    let mut truncated = false;
    while let Some(chunk) = response.chunk().await.map_err(|err| {
        if err.is_timeout() {
            WasmHttpError::timeout("HTTP response timed out")
        } else {
            WasmHttpError::transport(sanitize_http_error(err))
        }
    })? {
        let remaining = max_response_bytes.saturating_sub(body.len());
        if chunk.len() > remaining {
            body.extend_from_slice(&chunk[..remaining]);
            truncated = true;
            break;
        }
        body.extend_from_slice(&chunk);
        if body.len() == max_response_bytes {
            if response
                .chunk()
                .await
                .map_err(|err| {
                    if err.is_timeout() {
                        WasmHttpError::timeout("HTTP response timed out")
                    } else {
                        WasmHttpError::transport(sanitize_http_error(err))
                    }
                })?
                .is_some()
            {
                truncated = true;
            }
            break;
        }
    }

    let (body, truncated) = if response_envelope {
        encode_http_response_envelope(status, &headers, &body, truncated, max_response_bytes)?
    } else {
        (body, truncated)
    };
    Ok(WasmHttpExchange {
        response: crate::WasmHttpResponse {
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
        body,
    })
}

#[derive(serde::Deserialize)]
struct WasmHttpInputMapping {
    #[serde(default)]
    input_schema: Option<serde_json::Value>,
    #[serde(default)]
    parameters: Vec<WasmHttpParameterMapping>,
    #[serde(default)]
    request_body: Option<WasmHttpRequestBodyMapping>,
}

#[derive(serde::Deserialize)]
struct WasmHttpParameterMapping {
    name: String,
    input_property: String,
    location: String,
    required: bool,
    schema: serde_json::Value,
}

#[derive(serde::Deserialize)]
struct WasmHttpRequestBodyMapping {
    required: bool,
    #[serde(default)]
    input_property: Option<String>,
    schema: serde_json::Value,
}

fn apply_http_input_mapping(
    request: &mut crate::WasmHttpRequest,
    input_bytes: Vec<u8>,
) -> Result<Vec<u8>, WasmHttpError> {
    let Some(mapping) = request.input_mapping.take() else {
        return Ok(input_bytes);
    };
    let mapping: WasmHttpInputMapping = serde_json::from_value(mapping).map_err(|err| {
        WasmHttpError::invalid_argument(format!("invalid HTTP input mapping: {err}"))
    })?;
    let input: serde_json::Value = serde_json::from_slice(&input_bytes)
        .map_err(|err| WasmHttpError::invalid_argument(format!("invalid JSON input: {err}")))?;
    if let Some(input_schema) = &mapping.input_schema {
        verlet_runtime_contracts::schema::validate_json_value_against_schema(
            input_schema,
            &input,
            "HTTP mapped input",
        )
        .map_err(|err| {
            WasmHttpError::invalid_argument(format!("HTTP input violates its pinned schema: {err}"))
        })?;
    }
    let object = input.as_object();
    let mut url = request.url.clone();
    let mut query = Vec::new();
    for parameter in mapping.parameters {
        let value = object.and_then(|object| object.get(&parameter.input_property));
        let Some(value) = value.filter(|value| !value.is_null()) else {
            if parameter.required {
                return Err(WasmHttpError::invalid_argument(format!(
                    "missing required HTTP input property {:?}",
                    parameter.input_property
                )));
            }
            continue;
        };
        verlet_runtime_contracts::schema::validate_json_value_against_schema(
            &parameter.schema,
            value,
            &format!("HTTP input property {:?}", parameter.input_property),
        )
        .map_err(|err| {
            WasmHttpError::invalid_argument(format!(
                "HTTP input property {:?} violates its pinned schema: {err}",
                parameter.input_property
            ))
        })?;
        let value = http_parameter_value(value)?;
        match parameter.location.as_str() {
            "path" => {
                let placeholder = format!("{{{}}}", parameter.name);
                if !url.contains(&placeholder) {
                    return Err(WasmHttpError::invalid_argument(format!(
                        "HTTP path placeholder {placeholder:?} was not found"
                    )));
                }
                url = url.replace(&placeholder, &percent_encode_path_segment(&value));
            }
            "query" => query.push((parameter.name, value)),
            "header" => request.headers.push((parameter.name, value)),
            _ => {
                return Err(WasmHttpError::invalid_argument(
                    "unsupported HTTP input parameter location",
                ));
            }
        }
    }
    let mut parsed_url = reqwest::Url::parse(&url)
        .map_err(|_| WasmHttpError::invalid_argument("invalid mapped HTTP url"))?;
    if !query.is_empty() {
        let mut pairs = parsed_url.query_pairs_mut();
        for (name, value) in query {
            pairs.append_pair(&name, &value);
        }
    }
    request.url = parsed_url.to_string();
    let Some(request_body) = mapping.request_body else {
        return Ok(Vec::new());
    };
    let body = match request_body.input_property {
        Some(property) => match object.and_then(|object| object.get(&property)) {
            Some(body) => body,
            None if !request_body.required => return Ok(Vec::new()),
            None => {
                return Err(WasmHttpError::invalid_argument(format!(
                    "missing required HTTP request body property {property:?}"
                )));
            }
        },
        None => &input,
    };
    if body.is_null() && request_body.required {
        return Err(WasmHttpError::invalid_argument(
            "required HTTP request body must not be null",
        ));
    }
    if body.is_null() {
        Ok(Vec::new())
    } else {
        verlet_runtime_contracts::schema::validate_json_value_against_schema(
            &request_body.schema,
            body,
            "HTTP request body",
        )
        .map_err(|err| {
            WasmHttpError::invalid_argument(format!(
                "HTTP request body violates its pinned schema: {err}"
            ))
        })?;
        serde_json::to_vec(body).map_err(|err| {
            WasmHttpError::invalid_argument(format!("failed to encode HTTP request body: {err}"))
        })
    }
}

fn outbound_header_name(
    name: &str,
    invalid_message: &'static str,
) -> Result<reqwest::header::HeaderName, WasmHttpError> {
    let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
        .map_err(|_| WasmHttpError::invalid_argument(invalid_message))?;
    if forbidden_outbound_header(&name) {
        return Err(WasmHttpError::invalid_argument(
            "forbidden HTTP header controls routing or message framing",
        ));
    }
    Ok(name)
}

fn forbidden_outbound_header(name: &reqwest::header::HeaderName) -> bool {
    matches!(
        name.as_str(),
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

fn encode_http_response_envelope(
    status: u16,
    headers: &[(String, String)],
    body: &[u8],
    truncated: bool,
    max_bytes: usize,
) -> Result<(Vec<u8>, bool), WasmHttpError> {
    let headers = headers
        .iter()
        .cloned()
        .collect::<std::collections::BTreeMap<String, String>>();
    let encode = |body: &[u8], truncated| {
        let body = serde_json::from_slice::<serde_json::Value>(body)
            .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(body).into()));
        serde_json::to_vec(&serde_json::json!({
            "status": status,
            "headers": headers,
            "body": body,
            "truncated": truncated
        }))
        .map_err(|err| {
            WasmHttpError::invalid_argument(format!("failed to encode HTTP response: {err}"))
        })
    };

    let encoded = encode(body, truncated)?;
    if encoded.len() <= max_bytes {
        return Ok((encoded, truncated));
    }

    let empty = encode(&[], true)?;
    if empty.len() > max_bytes {
        return Err(WasmHttpError::invalid_argument(
            "HTTP response headers exceed the configured response size limit",
        ));
    }
    let body_budget = max_bytes.saturating_sub(empty.len());
    let mut prefix_len = (body_budget / 6).min(body.len());
    loop {
        let encoded = encode(&body[..prefix_len], true)?;
        if encoded.len() <= max_bytes {
            return Ok((encoded, true));
        }
        if prefix_len == 0 {
            return Err(WasmHttpError::invalid_argument(
                "HTTP response envelope exceeds the configured response size limit",
            ));
        }
        prefix_len /= 2;
    }
}

fn http_parameter_value(value: &serde_json::Value) -> Result<String, WasmHttpError> {
    match value {
        serde_json::Value::String(value) => Ok(value.clone()),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        serde_json::Value::Bool(value) => Ok(value.to_string()),
        _ => Err(WasmHttpError::invalid_argument(
            "HTTP parameters must be strings, numbers, or booleans",
        )),
    }
}

fn percent_encode_path_segment(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

#[doc(hidden)]
pub fn ensure_http_capability(
    grants: &std::collections::BTreeSet<String>,
    attachment_config: &crate::WasmAttachmentConfig,
    method: &reqwest::Method,
    origin: &str,
    private_destination: bool,
) -> Result<(), WasmHttpError> {
    if private_destination {
        let allowed =
            attachment_config
                .allowed_private_network
                .iter()
                .any(|(origin_pattern, methods)| {
                    (methods.contains("*") || methods.contains(method.as_str()))
                        && wildcard_match(origin_pattern, origin)
                });
        if allowed {
            return Ok(());
        }
        return Err(WasmHttpError::capability_denied(format!(
            "missing required capability net.http.private:{}:{origin}",
            method.as_str()
        )));
    }

    let namespace = "net.http";
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

/// `fs_open` in `FS_MODE_WRITE`: open a pending write buffer for `path`.
///
/// Contract (fs write leg, guest ABI `cooldis_0.1`): the caller has already
/// validated the mode, decoded an absolute UTF-8 `path`, and confirmed a VFS
/// is attached to the turn. Mutation additionally requires the
/// [`FS_WRITE_CAPABILITY`] grant in `capability_grants`; return
/// `STATUS_CAPABILITY_DENIED` without it. On success, insert a write handle
/// via `insert_write_file` and write it to guest memory at `out_handle_ptr`
/// (`STATUS_INVALID_ARGUMENT` if that write fails). The VFS is not touched
/// here: `fs_write` appends into the host-side buffer and `fs_close` commits
/// it. Parent directories are NOT created on commit; guests create them
/// explicitly with `fs_mkdir`.
fn fs_open_write_impl(
    caller: &mut wasmtime::Caller<'_, WasmTurnState>,
    path: std::path::PathBuf,
    out_handle_ptr: usize,
) -> i32 {
    if !caller
        .data()
        .capability_grants
        .contains(FS_WRITE_CAPABILITY)
    {
        return STATUS_CAPABILITY_DENIED;
    }
    let handle = caller.data_mut().insert_write_file(path);
    if !write_guest_u32_at(caller, out_handle_ptr, handle) {
        return STATUS_INVALID_ARGUMENT;
    }
    STATUS_OK
}

/// `fs_close` on a write handle: commit the pending buffer to the VFS.
///
/// Contract: one whole-file replace of `path` via `VerletVfs::write_file`
/// (create if missing, truncate-replace if present; parents must already
/// exist). Failures map through `vfs_error_status`. The handle was already
/// removed from the file table by the caller: it is consumed whether or not
/// the commit succeeds, and there is no retry.
async fn fs_close_commit_impl(
    caller: &mut wasmtime::Caller<'_, WasmTurnState>,
    path: std::path::PathBuf,
    buffer: Vec<u8>,
) -> i32 {
    let Some(vfs) = caller.data().vfs.clone() else {
        return STATUS_CAPABILITY_DENIED;
    };
    match vfs.write_file(&path, &buffer).await {
        Ok(()) => STATUS_OK,
        Err(err) => vfs_error_status(err),
    }
}

/// `fs_write`: append `len` bytes from guest memory at `ptr` to the pending
/// buffer of a write handle.
///
/// Contract: all-or-nothing: on `STATUS_OK` the whole slice was appended.
/// Unknown handle -> `STATUS_NOT_FOUND`; a read handle or bad guest pointers
/// -> `STATUS_INVALID_ARGUMENT`. No size cap beyond host memory: the read leg
/// buffers whole files host-side with the same asymmetry.
fn fs_write_impl(
    caller: &mut wasmtime::Caller<'_, WasmTurnState>,
    handle: i32,
    ptr: i32,
    len: i32,
) -> i32 {
    let Some(handle) = nonnegative_u32(handle) else {
        return STATUS_INVALID_ARGUMENT;
    };
    let Some(state) = caller.data().files.get(&handle) else {
        return STATUS_NOT_FOUND;
    };
    if !matches!(state, WasmFileState::Write { .. }) {
        return STATUS_INVALID_ARGUMENT;
    }
    let Some(bytes) = read_guest_memory(caller, ptr, len) else {
        return STATUS_INVALID_ARGUMENT;
    };
    let Some(WasmFileState::Write { buffer, .. }) = caller.data_mut().files.get_mut(&handle) else {
        return STATUS_INVALID_ARGUMENT;
    };
    buffer.extend_from_slice(&bytes);
    STATUS_OK
}

/// `fs_stat`: stat an absolute UTF-8 VFS path into a 16-byte record.
///
/// Contract: decode `path_ptr`/`path_len` like `fs_open` (absolute UTF-8,
/// else `STATUS_INVALID_ARGUMENT`); no VFS attached ->
/// `STATUS_CAPABILITY_DENIED`; missing path -> `STATUS_NOT_FOUND`, which
/// doubles as the existence check (there is no separate exists import).
/// Read-side: no `fs.write` grant required. On success write exactly 16
/// little-endian bytes at `out_ptr`:
///   bytes 0..4   kind (u32): 0 = file, 1 = directory, 2 = other
///   bytes 4..8   reserved (u32): always zero
///   bytes 8..16  size (u64): size in bytes for files, 0 for directories
async fn fs_stat_impl(
    caller: &mut wasmtime::Caller<'_, WasmTurnState>,
    path_ptr: i32,
    path_len: i32,
    out_ptr: i32,
) -> i32 {
    let Some(out_ptr) = nonnegative_usize(out_ptr) else {
        return STATUS_INVALID_ARGUMENT;
    };
    let Some(path_bytes) = read_guest_memory(caller, path_ptr, path_len) else {
        return STATUS_INVALID_ARGUMENT;
    };
    let Ok(path) = String::from_utf8(path_bytes) else {
        return STATUS_INVALID_ARGUMENT;
    };
    let path = std::path::PathBuf::from(path);
    if !path.is_absolute() {
        return STATUS_INVALID_ARGUMENT;
    }
    let Some(vfs) = caller.data().vfs.clone() else {
        return STATUS_CAPABILITY_DENIED;
    };
    let metadata = match vfs.stat(&path).await {
        Ok(metadata) => metadata,
        Err(err) => return vfs_error_status(err),
    };
    let kind = match metadata.file_type {
        bashkit::FileType::File => 0u32,
        bashkit::FileType::Directory => 1u32,
        bashkit::FileType::Symlink | bashkit::FileType::Fifo => 2u32,
    };
    let size = if metadata.file_type == bashkit::FileType::Directory {
        0
    } else {
        metadata.size
    };
    let mut record = [0u8; 16];
    record[0..4].copy_from_slice(&kind.to_le_bytes());
    record[8..16].copy_from_slice(&size.to_le_bytes());
    let Some(memory) = exported_memory(caller) else {
        return STATUS_INVALID_ARGUMENT;
    };
    let data = memory.data_mut(caller);
    let Some(end) = out_ptr.checked_add(record.len()) else {
        return STATUS_INVALID_ARGUMENT;
    };
    if end > data.len() {
        return STATUS_INVALID_ARGUMENT;
    }
    data[out_ptr..end].copy_from_slice(&record);
    STATUS_OK
}

/// `fs_list`: list an absolute UTF-8 VFS directory as a readable source.
///
/// Contract: path decoding, VFS-attachment, and not-found behavior match
/// `fs_stat`; read-side, no `fs.write` grant required. On success,
/// materialize the listing host-side as one UTF-8 JSON array
/// `[{"name": <entry name>, "is_dir": <bool>}, ...]`, sorted by `name` in
/// byte order (deterministic, like the agent-tools walker), insert it as a
/// dynamic source, and write the source handle to guest memory at
/// `out_source_ptr`. The guest drains it with `source_read`. Names are entry
/// names only, never full paths.
async fn fs_list_impl(
    caller: &mut wasmtime::Caller<'_, WasmTurnState>,
    path_ptr: i32,
    path_len: i32,
    out_source_ptr: i32,
) -> i32 {
    #[derive(serde::Serialize)]
    struct ListingEntry {
        name: String,
        is_dir: bool,
    }

    let Some(out_source_ptr) = nonnegative_usize(out_source_ptr) else {
        return STATUS_INVALID_ARGUMENT;
    };
    let Some(path_bytes) = read_guest_memory(caller, path_ptr, path_len) else {
        return STATUS_INVALID_ARGUMENT;
    };
    let Ok(path) = String::from_utf8(path_bytes) else {
        return STATUS_INVALID_ARGUMENT;
    };
    let path = std::path::PathBuf::from(path);
    if !path.is_absolute() {
        return STATUS_INVALID_ARGUMENT;
    }
    let Some(vfs) = caller.data().vfs.clone() else {
        return STATUS_CAPABILITY_DENIED;
    };
    let mut entries = match vfs.read_dir(&path).await {
        Ok(entries) => entries,
        Err(err) => return vfs_error_status(err),
    };
    entries.sort_by(|left, right| left.name.as_bytes().cmp(right.name.as_bytes()));
    let listing: Vec<_> = entries
        .into_iter()
        .map(|entry| ListingEntry {
            name: entry.name,
            is_dir: entry.metadata.file_type == bashkit::FileType::Directory,
        })
        .collect();
    let bytes = match serde_json::to_vec(&listing) {
        Ok(bytes) => bytes,
        Err(_) => return STATUS_TRANSPORT_ERROR,
    };
    let source = caller.data_mut().insert_source(bytes);
    if !write_guest_u32_at(caller, out_source_ptr, source) {
        return STATUS_INVALID_ARGUMENT;
    }
    STATUS_OK
}

/// `fs_mkdir`: create a directory at an absolute UTF-8 VFS path.
///
/// Contract: path decoding and VFS-attachment behavior match `fs_stat`.
/// Mutation: requires the [`FS_WRITE_CAPABILITY`] grant ->
/// `STATUS_CAPABILITY_DENIED` without it. `recursive` must be 0 or 1 (any
/// other value -> `STATUS_INVALID_ARGUMENT`) and maps to the backend's
/// recursive flag: recursive creation of an existing directory succeeds;
/// non-recursive creation with a missing parent or an existing target maps
/// the backend error through `vfs_error_status`.
async fn fs_mkdir_impl(
    caller: &mut wasmtime::Caller<'_, WasmTurnState>,
    path_ptr: i32,
    path_len: i32,
    recursive: i32,
) -> i32 {
    let recursive = match recursive {
        0 => false,
        1 => true,
        _ => return STATUS_INVALID_ARGUMENT,
    };
    let Some(path_bytes) = read_guest_memory(caller, path_ptr, path_len) else {
        return STATUS_INVALID_ARGUMENT;
    };
    let Ok(path) = String::from_utf8(path_bytes) else {
        return STATUS_INVALID_ARGUMENT;
    };
    let path = std::path::PathBuf::from(path);
    if !path.is_absolute() {
        return STATUS_INVALID_ARGUMENT;
    }
    let Some(vfs) = caller.data().vfs.clone() else {
        return STATUS_CAPABILITY_DENIED;
    };
    if !caller
        .data()
        .capability_grants
        .contains(FS_WRITE_CAPABILITY)
    {
        return STATUS_CAPABILITY_DENIED;
    }
    match vfs.mkdir(&path, recursive).await {
        Ok(()) => STATUS_OK,
        Err(err) => vfs_error_status(err),
    }
}

fn vfs_error_status(err: bashkit::Error) -> i32 {
    match err {
        bashkit::Error::Io(err) => match err.kind() {
            std::io::ErrorKind::NotFound => STATUS_NOT_FOUND,
            std::io::ErrorKind::PermissionDenied => STATUS_CAPABILITY_DENIED,
            std::io::ErrorKind::InvalidInput => STATUS_INVALID_ARGUMENT,
            _ => STATUS_TRANSPORT_ERROR,
        },
        bashkit::Error::Cancelled => STATUS_CANCELLED,
        bashkit::Error::ResourceLimit(_) => STATUS_TRANSPORT_ERROR,
        bashkit::Error::Parse { .. }
        | bashkit::Error::Execution(_)
        | bashkit::Error::CommandFailure(_)
        | bashkit::Error::Network(_)
        | bashkit::Error::Regex(_)
        | bashkit::Error::Internal(_)
        | bashkit::Error::SnapshotTooNew { .. }
        | bashkit::Error::SnapshotCapabilityMismatch(_) => STATUS_TRANSPORT_ERROR,
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
            let filtered_addrs: Vec<std::net::SocketAddr> = addrs
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
            .parse::<std::net::IpAddr>()
            .map(is_private_or_special_ip)
            .unwrap_or(false)
}

fn is_private_or_special_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(ip) => is_private_or_special_ipv4(ip),
        std::net::IpAddr::V6(ip) => is_private_or_special_ipv6(ip),
    }
}

fn is_private_or_special_ipv4(ip: std::net::Ipv4Addr) -> bool {
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

fn is_private_or_special_ipv6(ip: std::net::Ipv6Addr) -> bool {
    ip.is_loopback()
        || ip.is_unspecified()
        || (ip.segments()[0] & 0xfe00) == 0xfc00
        || (ip.segments()[0] & 0xffc0) == 0xfe80
        || ip.segments()[0] == 0x2001 && ip.segments()[1] == 0x0db8
        || ip.segments()[0] == 0x5f00
}

fn exported_memory(caller: &mut wasmtime::Caller<'_, WasmTurnState>) -> Option<wasmtime::Memory> {
    caller
        .get_export("memory")
        .and_then(|external| external.into_memory())
}

fn read_guest_memory(
    caller: &mut wasmtime::Caller<'_, WasmTurnState>,
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

fn read_guest_u32_at(caller: &mut wasmtime::Caller<'_, WasmTurnState>, ptr: usize) -> Option<u32> {
    let memory = exported_memory(caller)?;
    let data = memory.data(caller);
    let end = ptr.checked_add(4)?;
    if end > data.len() {
        return None;
    }
    Some(u32::from_le_bytes(data[ptr..end].try_into().ok()?))
}

fn write_guest_u32_at(
    caller: &mut wasmtime::Caller<'_, WasmTurnState>,
    ptr: usize,
    value: u32,
) -> bool {
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
