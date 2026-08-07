//! Tool universes: mutable external tool sources mounted through the search
//! surface, never through tool rows.
//!
//! The lexicon law governs this module: nothing mutable backs a tool row. A
//! `protocol_tool_import` declares a UNIVERSE (an MCP server today; OpenAPI
//! later) whose per-tool contracts are discovered at runtime and can change
//! without Verlet's consent — attachment tier 3. The kernel therefore never
//! projects a live universe into the model's static tool set. Instead it
//! mounts three reserved meta-operations — the lexicon's `tool.search`,
//! `tool.describe`, and `tool.call` — and discovered contracts arrive in
//! context as witnessed, schema-hash-addressed CONTENT (man-page style),
//! never as rows. `tool.call` validates arguments just-in-time against the
//! witnessed schema and fails closed.
//!
//! The only passage from universe to row is a PIN: the acceptance of a
//! witnessed contract as a published, content-addressed record (a pin is a
//! publish). A pinned import projects as a direct row; live drift from the
//! pin is a fail-closed call error plus a witnessed event, never a silent
//! update. `expose = ["direct_tool"]` without a pin fails manifest compile.
//!
//! Within a turn, search/describe/call resolve against the same witnessed
//! discovery (the binding law: a running turn keeps its snapshot). Every
//! discovery and call is witnessed on the thread's event stream
//! (`tool.universe.discovery.completed`, `tool.universe.call.completed`).

pub use verlet_agent::PinnedToolRef;

/// Model-facing surface names for the reserved meta-operations. These are
/// the provider-charset projections of the lexicon's `tool.search` /
/// `tool.describe` / `tool.call` (provider tool names cannot contain `.`),
/// the same way `op://echo` projects as `echo_search`.
pub const TOOL_SEARCH_TOOL: &str = "tool_search";
pub const TOOL_DESCRIBE_TOOL: &str = "tool_describe";
pub const TOOL_CALL_TOOL: &str = "tool_call";

/// `discharged_by` coupling name for search-surface receipts, mirroring
/// `binder:manifest` on bind receipts.
pub const TOOL_UNIVERSE_SURFACE_DISCHARGED_BY: &str = "surface:tool-universe";
#[cfg(test)]
const MAX_SCHEMA_VALIDATION_DEPTH: usize = verlet_runtime_contracts::MAX_JSON_SCHEMA_SUBSET_DEPTH;

/// One tool contract as witnessed from a live universe at discovery time.
///
/// The schema hash is the contract's content address: `sha256_hex` over the
/// canonical JSON encoding of the input schema (serde_json object keys are
/// BTreeMap-ordered in this workspace — `preserve_order` must stay off, or
/// every schema hash in every receipt changes meaning).
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WitnessedToolContract {
    /// Tool name as the universe reports it (e.g. `GoogleSearch.search`).
    pub tool_name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
    /// `sha256:<hex>` over the canonical JSON encoding of `input_schema`.
    pub schema_hash: String,
}

impl WitnessedToolContract {
    /// Witness one tool definition from a live universe, stamping its
    /// schema hash.
    pub fn witness(definition: &crate::ToolDefinition) -> crate::VerletResult<Self> {
        let schema_hash = schema_hash_of(&definition.input_schema)?;
        Ok(Self {
            tool_name: definition.name.clone(),
            description: definition.description.clone(),
            input_schema: definition.input_schema.clone(),
            schema_hash,
        })
    }

    /// Whether this witnessed contract satisfies a pin: same tool name,
    /// same schema hash. Anything else is drift and fails closed.
    pub fn matches_pin(&self, pin: &PinnedToolRef) -> bool {
        self.tool_name == pin.tool_name && self.schema_hash == pin.schema_hash
    }
}

/// Canonical content address of a JSON schema value.
pub fn schema_hash_of(schema: &serde_json::Value) -> crate::VerletResult<String> {
    let bytes = serde_json::to_vec(schema).map_err(|err| {
        crate::VerletError::RuntimeFactory(format!(
            "failed to encode tool schema for hashing: {err}"
        ))
    })?;
    Ok(crate::agent::contracts::sha256_hex(&bytes))
}

pub(crate) fn args_fingerprint(
    tool_name: &str,
    arguments: &serde_json::Value,
) -> crate::VerletResult<String> {
    let invocation = std::collections::BTreeMap::from([
        ("arguments", arguments.clone()),
        (
            "tool_name",
            serde_json::Value::String(tool_name.to_string()),
        ),
    ]);
    let bytes = serde_json::to_vec(&invocation).map_err(|err| {
        crate::VerletError::RuntimeFactory(format!(
            "failed to encode tool invocation arguments for hashing: {err}"
        ))
    })?;
    Ok(crate::agent::contracts::sha256_hex(&bytes))
}

/// One witnessed discovery of a universe's tool contracts. This is the
/// snapshot a turn's search/describe/call resolve against.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolUniverseDiscovery {
    /// `mcp://<source-name>` reference of the configured source record.
    pub server_ref: String,
    pub tools: Vec<WitnessedToolContract>,
    /// `sha256:<hex>` over the canonical JSON encoding of `tools` — the
    /// content address of the discovery as a whole.
    pub discovery_hash: String,
    pub discovered_at_ms: i64,
}

impl ToolUniverseDiscovery {
    /// Assemble a witnessed discovery from the contracts a universe
    /// reported, stamping the discovery hash.
    pub fn witness(
        server_ref: impl Into<String>,
        tools: Vec<WitnessedToolContract>,
        discovered_at_ms: i64,
    ) -> crate::VerletResult<Self> {
        let encoded = serde_json::to_vec(&tools).map_err(|err| {
            crate::VerletError::RuntimeFactory(format!(
                "failed to encode tool universe discovery for hashing: {err}"
            ))
        })?;
        Ok(Self {
            server_ref: server_ref.into(),
            tools,
            discovery_hash: crate::agent::contracts::sha256_hex(&encoded),
            discovered_at_ms,
        })
    }

    /// The discovery restricted to a manifest-level `include_tools` filter
    /// (intersection; the source record's own filter has already applied at
    /// the client). Re-stamps the discovery hash for the filtered set.
    pub fn filtered(
        &self,
        include_tools: &std::collections::BTreeSet<String>,
    ) -> crate::VerletResult<Self> {
        let tools = self
            .tools
            .iter()
            .filter(|tool| include_tools.contains(&tool.tool_name))
            .cloned()
            .collect();
        Self::witness(self.server_ref.clone(), tools, self.discovered_at_ms)
    }

    pub fn contract(&self, tool_name: &str) -> Option<&WitnessedToolContract> {
        self.tools.iter().find(|tool| tool.tool_name == tool_name)
    }
}

/// One bound universe on a manifest-backed thread: the import declaration
/// resolved against a witnessed discovery, plus the pin if the import
/// exposes a direct row. Serialized into thread metadata
/// (`cooldis.agent.tool_universes`) so the runtime factory can remount the
/// search surface on restore, exactly like operation bindings.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolUniverseBinding {
    /// Manifest tool id of the `protocol_tool_import`.
    pub import_id: String,
    pub server_ref: String,
    #[serde(default, skip_serializing_if = "crate::EffectClass::is_at_most_once")]
    pub effect_class: crate::EffectClass,
    /// Manifest-level filter, already applied to `discovery`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_tools: Option<std::collections::BTreeSet<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pin: Option<PinnedToolRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grant_expiries: Vec<crate::AgentManifestGrantExpiry>,
    pub discovery: ToolUniverseDiscovery,
}

impl ToolUniverseBinding {
    pub fn validate(&self) -> crate::VerletResult<()> {
        if self.discovery.server_ref != self.server_ref {
            return Err(crate::VerletError::RuntimeFactory(format!(
                "tool universe binding {:?} discovery returned server_ref {:?}, expected {:?}; fail closed",
                self.import_id, self.discovery.server_ref, self.server_ref
            )));
        }
        if let Some(pin) = &self.pin {
            let witnessed = self.discovery.contract(&pin.tool_name);
            let witnessed_hash = witnessed
                .map(|contract| contract.schema_hash.as_str())
                .unwrap_or("<missing>");
            if !witnessed.is_some_and(|contract| contract.matches_pin(pin)) {
                return Err(crate::VerletError::RuntimeFactory(format!(
                    "tool universe binding {:?} pin drift for {:?}: expected schema hash {}, witnessed {}; fail closed",
                    self.import_id, pin.tool_name, pin.schema_hash, witnessed_hash
                )));
            }
        }
        Ok(())
    }
}

/// Bind-receipt entry for one universe: what an audit needs to answer
/// "which mutable surface could this thread reach, under which witnessed
/// contracts, and which contracts were pinned to rows".
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolUniverseBindReceipt {
    pub import_id: String,
    pub server_ref: String,
    pub discovery_hash: String,
    /// Name plus schema hash for every witnessed contract in scope.
    pub tools: Vec<ToolUniverseToolReceipt>,
    /// Pin references resolved to rows, empty when the import is
    /// search-surface only.
    pub pinned: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grant_expiries: Vec<crate::AgentManifestGrantExpiry>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolUniverseToolReceipt {
    pub tool_name: String,
    pub schema_hash: String,
    #[serde(default, skip_serializing_if = "crate::EffectClass::is_at_most_once")]
    pub effect_class: crate::EffectClass,
}

impl ToolUniverseBindReceipt {
    pub fn from_binding(binding: &ToolUniverseBinding) -> Self {
        Self {
            import_id: binding.import_id.clone(),
            server_ref: binding.server_ref.clone(),
            discovery_hash: binding.discovery.discovery_hash.clone(),
            tools: binding
                .discovery
                .tools
                .iter()
                .map(|tool| ToolUniverseToolReceipt {
                    tool_name: tool.tool_name.clone(),
                    schema_hash: tool.schema_hash.clone(),
                    effect_class: binding
                        .pin
                        .as_ref()
                        .filter(|pin| pin.tool_name == tool.tool_name)
                        .map(|_| binding.effect_class)
                        .unwrap_or_default(),
                })
                .collect(),
            pinned: binding
                .pin
                .iter()
                .map(|pin| {
                    format!(
                        "mcptool://{}/{}@{}",
                        pin.server, pin.tool_name, pin.schema_hash
                    )
                })
                .collect(),
            grant_expiries: binding.grant_expiries.clone(),
        }
    }
}

/// Payload of the witnessed `tool.universe.discovery.completed` event: a
/// discovery happened, addressed by content. Full schemas live in the
/// thread's binding snapshot and arrive in context through `tool.describe`;
/// the event carries names and hashes only.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolUniverseDiscoveryReceipt {
    pub server_ref: String,
    pub discovery_hash: String,
    pub tools: Vec<ToolUniverseToolReceipt>,
}

impl ToolUniverseDiscoveryReceipt {
    pub fn from_discovery(discovery: &ToolUniverseDiscovery) -> Self {
        Self {
            server_ref: discovery.server_ref.clone(),
            discovery_hash: discovery.discovery_hash.clone(),
            tools: discovery
                .tools
                .iter()
                .map(|tool| ToolUniverseToolReceipt {
                    tool_name: tool.tool_name.clone(),
                    schema_hash: tool.schema_hash.clone(),
                    effect_class: crate::EffectClass::AtMostOnce,
                })
                .collect(),
        }
    }
}

/// Payload of the witnessed `tool.universe.call.completed` event: one
/// `tool.call` against a live universe, with the contract it validated
/// against and the content address of what came back.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolUniverseCallReceipt {
    pub server_ref: String,
    pub tool_name: String,
    /// Schema hash the arguments were JIT-validated against.
    pub schema_hash: String,
    /// `sha256:<hex>` over the returned content bytes.
    pub output_hash: String,
    pub is_error: bool,
}

/// What a universe returns from one tool invocation.
#[derive(Clone, Debug, PartialEq)]
pub struct ToolUniverseCallOutput {
    pub content: String,
    pub is_error: bool,
}

/// Discovers a universe's tool contracts at bind time. Implemented by the
/// app server over the MCP client machinery (`McpRemoteToolProvider` /
/// `SqliteMcpSourceRegistry`); the bind layer depends only on this trait so
/// the dependency direction stays agent ← adapters.
#[async_trait::async_trait]
pub trait ToolUniverseDiscoverer: Send + Sync {
    /// Connect to the configured source behind `server_ref`, list its
    /// tools (the source record's own `include_tools` filter applies at the
    /// client), and witness the result. Fails closed on any transport or
    /// protocol error: a thread does not start against a universe it could
    /// not witness.
    async fn discover(&self, server_ref: &str) -> crate::VerletResult<ToolUniverseDiscovery>;
}

/// Invokes one tool on a live universe at turn time. Implemented over the
/// same MCP client a discovery used, so a turn's calls land on the universe
/// its snapshot witnessed.
#[async_trait::async_trait]
pub trait ToolUniverseCaller: Send + Sync {
    async fn call_tool(
        &self,
        tool_name: &str,
        arguments: serde_json::Value,
    ) -> crate::VerletResult<ToolUniverseCallOutput>;
}

/// One universe mounted on a thread's search surface: the bound snapshot
/// plus a live caller for `tool.call`.
pub struct MountedToolUniverse {
    pub binding: ToolUniverseBinding,
    pub caller: std::sync::Arc<dyn ToolUniverseCaller>,
}

/// The search surface: an `AgentKernelToolProvider` exposing the three
/// reserved meta-operations over every universe mounted on the thread.
///
/// Resolution rules (fixed here, implemented by the bodies below):
/// - `tool_search` takes an optional `query` and optional `universe`
///   (`mcp://...` ref) and returns matching tool names with one-line
///   descriptions and schema hashes — names only, never schemas as rows.
/// - `tool_describe` takes a `tool` name (optionally a `universe` to
///   disambiguate) and returns the full witnessed contract as man-page
///   style content: description, input schema, schema hash, server ref.
/// - `tool_call` takes `tool`, optional `universe`, and `arguments`;
///   arguments are JIT-validated against the witnessed schema before the
///   universe is touched, and the result rides back as a tool result plus
///   a witnessed `tool.universe.call.completed` event.
/// - An unqualified tool name resolves only when exactly one mounted
///   universe carries it; ambiguity is an error listing the qualified
///   candidates, never a silent pick.
pub struct ToolUniverseSearchSurface {
    universes: Vec<MountedToolUniverse>,
    event_store: Option<std::sync::Arc<dyn crate::RuntimeStore>>,
    live_discoverer: Option<std::sync::Arc<dyn ToolUniverseDiscoverer>>,
}

impl ToolUniverseSearchSurface {
    pub fn new(universes: Vec<MountedToolUniverse>) -> Self {
        Self {
            universes,
            event_store: None,
            live_discoverer: None,
        }
    }

    pub fn new_with_runtime(
        universes: Vec<MountedToolUniverse>,
        event_store: std::sync::Arc<dyn crate::RuntimeStore>,
        live_discoverer: std::sync::Arc<dyn ToolUniverseDiscoverer>,
    ) -> Self {
        Self {
            universes,
            event_store: Some(event_store),
            live_discoverer: Some(live_discoverer),
        }
    }

    pub fn universes(&self) -> &[MountedToolUniverse] {
        &self.universes
    }

    fn ensure_mounted_grants_live(
        &self,
        mounted: &MountedToolUniverse,
        now_ms: i64,
    ) -> crate::VerletResult<()> {
        crate::agent::manifest_bind::ensure_grant_expiries_live(
            &mounted.binding.grant_expiries,
            now_ms,
        )
    }

    fn resolve_contract<'a>(
        &'a self,
        tool_name: &str,
        universe: Option<&str>,
    ) -> crate::VerletResult<ResolvedUniverseTool<'a>> {
        let matches = self
            .universes
            .iter()
            .filter(|mounted| {
                universe
                    .map(|expected| mounted.binding.server_ref == expected)
                    .unwrap_or(true)
            })
            .filter_map(|mounted| {
                mounted
                    .binding
                    .discovery
                    .contract(tool_name)
                    .map(|contract| ResolvedUniverseTool { mounted, contract })
            })
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [resolved] => Ok(ResolvedUniverseTool {
                mounted: resolved.mounted,
                contract: resolved.contract,
            }),
            [] => {
                if let Some(universe) = universe {
                    Err(crate::VerletError::RuntimeExecution(format!(
                        "tool universe {universe:?} does not expose tool {tool_name:?}"
                    )))
                } else {
                    Err(crate::VerletError::RuntimeExecution(format!(
                        "tool {tool_name:?} is not mounted on this thread"
                    )))
                }
            }
            _ => {
                let candidates = matches
                    .iter()
                    .map(|resolved| {
                        format!(
                            "{}::{}",
                            resolved.mounted.binding.server_ref, resolved.contract.tool_name
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                Err(crate::VerletError::RuntimeExecution(format!(
                    "tool {tool_name:?} is ambiguous; qualify with universe. candidates: {candidates}"
                )))
            }
        }
    }

    fn resolve_pinned_contract<'a>(
        &'a self,
        tool_name: &str,
    ) -> crate::VerletResult<Option<ResolvedUniverseTool<'a>>> {
        let mut matches = Vec::new();
        let mut drift_errors = Vec::new();
        for mounted in &self.universes {
            let Some(pin) = mounted.binding.pin.as_ref() else {
                continue;
            };
            if pin.tool_name != tool_name {
                continue;
            }
            match mounted.binding.discovery.contract(&pin.tool_name) {
                Some(contract) if contract.matches_pin(pin) => {
                    matches.push(ResolvedUniverseTool { mounted, contract });
                }
                Some(contract) => {
                    drift_errors.push(format!(
                        "{} expected {}, witnessed {}",
                        mounted.binding.server_ref, pin.schema_hash, contract.schema_hash
                    ));
                }
                None => {
                    drift_errors.push(format!(
                        "{} expected {}, witnessed <missing>",
                        mounted.binding.server_ref, pin.schema_hash
                    ));
                }
            }
        }
        if !drift_errors.is_empty() {
            return Err(crate::VerletError::RuntimeExecution(format!(
                "pinned tool row {tool_name:?} binding drift: {}; fail closed",
                drift_errors.join("; ")
            )));
        }
        match matches.as_slice() {
            [resolved] => Ok(Some(ResolvedUniverseTool {
                mounted: resolved.mounted,
                contract: resolved.contract,
            })),
            [] => Ok(None),
            _ => {
                let candidates = matches
                    .iter()
                    .map(|resolved| {
                        format!(
                            "{}::{}",
                            resolved.mounted.binding.server_ref, resolved.contract.tool_name
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                Err(crate::VerletError::RuntimeExecution(format!(
                    "pinned tool row {tool_name:?} is ambiguous; candidates: {candidates}"
                )))
            }
        }
    }

    async fn record_call_receipt(
        &self,
        call: &crate::agent::agent_tool_router::AgentKernelToolCall,
        receipt: ToolUniverseCallReceipt,
    ) -> crate::VerletResult<()> {
        let (Some(event_store), Some(turn_context)) = (&self.event_store, &call.turn_context)
        else {
            return Ok(());
        };
        let stream_id = crate::EventStreamId::for_thread(&turn_context.coordinates);
        let payload = serde_json::to_value(&receipt).map_err(|err| {
            crate::VerletError::RuntimeExecution(format!(
                "failed to encode tool universe call receipt: {err}"
            ))
        })?;
        let appended = event_store
            .append_events(
                &stream_id,
                vec![crate::NewEventRecord::witnessed(
                    turn_context.coordinates.clone(),
                    crate::EventKind::ToolUniverseCallCompleted,
                    payload,
                )],
            )
            .await
            .map_err(|err| crate::VerletError::History(err.to_string()))?;
        if appended.len() != 1 {
            return Err(crate::VerletError::History(format!(
                "tool universe call receipt append returned {} record(s)",
                appended.len()
            )));
        }
        Ok(())
    }

    async fn finish_universe_call(
        &self,
        call: crate::agent::agent_tool_router::AgentKernelToolCall,
        resolved: ResolvedUniverseTool<'_>,
        output: ToolUniverseCallOutput,
    ) -> crate::VerletResult<crate::CanonicalMessage> {
        let schema_hash = resolved.contract.schema_hash.clone();
        self.finish_universe_call_with_schema_hash(call, resolved, schema_hash, output)
            .await
    }

    async fn finish_universe_call_with_schema_hash(
        &self,
        call: crate::agent::agent_tool_router::AgentKernelToolCall,
        resolved: ResolvedUniverseTool<'_>,
        schema_hash: String,
        output: ToolUniverseCallOutput,
    ) -> crate::VerletResult<crate::CanonicalMessage> {
        self.record_call_receipt(
            &call,
            ToolUniverseCallReceipt {
                server_ref: resolved.mounted.binding.server_ref.clone(),
                tool_name: resolved.contract.tool_name.clone(),
                schema_hash,
                output_hash: crate::agent::contracts::sha256_hex(output.content.as_bytes()),
                is_error: output.is_error,
            },
        )
        .await?;
        Ok(crate::CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            output.content,
            output.is_error,
        ))
    }

    async fn finish_pinned_error(
        &self,
        call: crate::agent::agent_tool_router::AgentKernelToolCall,
        resolved: ResolvedUniverseTool<'_>,
        content: String,
    ) -> crate::VerletResult<crate::CanonicalMessage> {
        let schema_hash = resolved
            .mounted
            .binding
            .pin
            .as_ref()
            .map(|pin| pin.schema_hash.clone())
            .unwrap_or_else(|| resolved.contract.schema_hash.clone());
        self.finish_universe_call_with_schema_hash(
            call,
            resolved,
            schema_hash,
            ToolUniverseCallOutput {
                content,
                is_error: true,
            },
        )
        .await
    }

    async fn invoke_universe_call(
        &self,
        call: crate::agent::agent_tool_router::AgentKernelToolCall,
        resolved: ResolvedUniverseTool<'_>,
        arguments: serde_json::Value,
    ) -> crate::VerletResult<crate::CanonicalMessage> {
        if let Err(err) = validate_tool_arguments(resolved.contract, &arguments) {
            return self
                .finish_universe_call(
                    call,
                    resolved,
                    ToolUniverseCallOutput {
                        content: err.to_string(),
                        is_error: true,
                    },
                )
                .await;
        }
        let output = match resolved
            .mounted
            .caller
            .call_tool(&resolved.contract.tool_name, arguments)
            .await
        {
            Ok(output) => output,
            Err(err) => ToolUniverseCallOutput {
                content: err.to_string(),
                is_error: true,
            },
        };
        self.finish_universe_call(call, resolved, output).await
    }

    async fn invoke_pinned_direct_call(
        &self,
        call: crate::agent::agent_tool_router::AgentKernelToolCall,
        now_ms: i64,
    ) -> crate::VerletResult<Option<crate::CanonicalMessage>> {
        let resolved = match self.resolve_pinned_contract(&call.tool_name)? {
            Some(resolved) => resolved,
            None => return Ok(None),
        };
        self.ensure_mounted_grants_live(resolved.mounted, now_ms)?;
        let pin = resolved.mounted.binding.pin.as_ref().ok_or_else(|| {
            crate::VerletError::RuntimeExecution(format!(
                "pinned tool row {:?} is missing its pin",
                call.tool_name
            ))
        })?;
        let live_discoverer = match self.live_discoverer.as_ref() {
            Some(live_discoverer) => live_discoverer,
            None => {
                let content = format!(
                    "pinned tool row {:?} requires a live discovery path; fail closed",
                    call.tool_name
                );
                return self
                    .finish_pinned_error(call, resolved, content)
                    .await
                    .map(Some);
            }
        };
        let live = match live_discoverer
            .discover(&resolved.mounted.binding.server_ref)
            .await
        {
            Ok(live) => live,
            Err(err) => {
                return self
                    .finish_pinned_error(call, resolved, err.to_string())
                    .await
                    .map(Some);
            }
        };
        let live_hash = live
            .contract(&pin.tool_name)
            .map(|contract| contract.schema_hash.as_str())
            .unwrap_or("<missing>");
        if live_hash != pin.schema_hash {
            let content = format!(
                "pinned tool {:?} drifted for server_ref {:?}: expected schema hash {}, witnessed {}; fail closed",
                pin.tool_name, resolved.mounted.binding.server_ref, pin.schema_hash, live_hash
            );
            return self
                .finish_pinned_error(call, resolved, content)
                .await
                .map(Some);
        }
        let arguments = call.arguments.clone();
        self.invoke_universe_call(call, resolved, arguments)
            .await
            .map(Some)
    }
}

struct ResolvedUniverseTool<'a> {
    mounted: &'a MountedToolUniverse,
    contract: &'a WitnessedToolContract,
}

#[async_trait::async_trait]
impl crate::agent::agent_tool_router::AgentKernelToolProvider for ToolUniverseSearchSurface {
    async fn tool_definitions(&self) -> Vec<crate::ToolDefinition> {
        if self.universes.is_empty() {
            return Vec::new();
        }
        let mut definitions = vec![
            crate::ToolDefinition::new(
                TOOL_SEARCH_TOOL,
                "Search the tool universes mounted on this thread. Returns matching tool names \
                 with one-line descriptions; use tool_describe for the full contract.",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Substring or keyword filter over tool names and descriptions. Omit to list everything."
                        },
                        "universe": {
                            "type": "string",
                            "description": "Restrict to one universe by its mcp:// reference."
                        }
                    },
                    "additionalProperties": false
                }),
            ),
            crate::ToolDefinition::new(
                TOOL_DESCRIBE_TOOL,
                "Show the witnessed contract of one tool (description, input schema, schema \
                 hash) as reference content — the man page for a universe tool.",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "tool": {
                            "type": "string",
                            "description": "Tool name as reported by tool_search."
                        },
                        "universe": {
                            "type": "string",
                            "description": "mcp:// reference, required only when the name is mounted by more than one universe."
                        }
                    },
                    "required": ["tool"],
                    "additionalProperties": false
                }),
            ),
            crate::ToolDefinition::new(
                TOOL_CALL_TOOL,
                "Invoke one universe tool. Arguments are validated against the witnessed \
                 schema before the call; mismatches fail closed without touching the universe.",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "tool": {
                            "type": "string",
                            "description": "Tool name as reported by tool_search."
                        },
                        "universe": {
                            "type": "string",
                            "description": "mcp:// reference, required only when the name is mounted by more than one universe."
                        },
                        "arguments": {
                            "type": "object",
                            "description": "Arguments matching the tool's witnessed input schema.",
                            "additionalProperties": true
                        }
                    },
                    "required": ["tool", "arguments"],
                    "additionalProperties": false
                }),
            ),
        ];
        for mounted in &self.universes {
            let Some(pin) = &mounted.binding.pin else {
                continue;
            };
            if let Some(contract) = mounted.binding.discovery.contract(&pin.tool_name) {
                if !contract.matches_pin(pin) {
                    continue;
                }
                definitions.push(crate::ToolDefinition::new(
                    contract.tool_name.clone(),
                    contract.description.clone(),
                    contract.input_schema.clone(),
                ));
            }
        }
        definitions
    }

    async fn invoke_tool_call(
        &self,
        call: crate::agent::agent_tool_router::AgentKernelToolCall,
    ) -> crate::VerletResult<Option<crate::CanonicalMessage>> {
        self.invoke_tool_call_at(call, crate::kernel::history::now_ms())
            .await
    }

    async fn invoke_tool_call_at(
        &self,
        call: crate::agent::agent_tool_router::AgentKernelToolCall,
        now_ms: i64,
    ) -> crate::VerletResult<Option<crate::CanonicalMessage>> {
        match call.tool_name.as_str() {
            TOOL_SEARCH_TOOL => {
                let query = optional_string_arg(&call.arguments, "query")?;
                let universe = optional_string_arg(&call.arguments, "universe")?;
                let query = query.map(|value| value.to_lowercase());
                for mounted in self.universes.iter().filter(|mounted| {
                    universe
                        .as_deref()
                        .map(|expected| mounted.binding.server_ref == expected)
                        .unwrap_or(true)
                }) {
                    self.ensure_mounted_grants_live(mounted, now_ms)?;
                }
                let tools = self
                    .universes
                    .iter()
                    .filter(|mounted| {
                        universe
                            .as_deref()
                            .map(|expected| mounted.binding.server_ref == expected)
                            .unwrap_or(true)
                    })
                    .flat_map(|mounted| {
                        mounted
                            .binding
                            .discovery
                            .tools
                            .iter()
                            .filter_map(|contract| {
                                if let Some(query) = &query {
                                    let haystack =
                                        format!("{}\n{}", contract.tool_name, contract.description)
                                            .to_lowercase();
                                    if !haystack.contains(query) {
                                        return None;
                                    }
                                }
                                Some(serde_json::json!({
                                    "server_ref": mounted.binding.server_ref,
                                    "tool_name": contract.tool_name,
                                    "description": first_line(&contract.description),
                                    "schema_hash": contract.schema_hash,
                                }))
                            })
                    })
                    .collect::<Vec<_>>();
                let content = serde_json::to_string_pretty(&serde_json::json!({
                    "tools": tools
                }))
                .map_err(|err| {
                    crate::VerletError::RuntimeExecution(format!(
                        "failed to encode tool_search result: {err}"
                    ))
                })?;
                Ok(Some(crate::CanonicalMessage::tool_result(
                    call.call_id,
                    call.tool_name,
                    content,
                    false,
                )))
            }
            TOOL_DESCRIBE_TOOL => {
                let tool_name = required_string_arg(&call.arguments, "tool")?;
                let universe = optional_string_arg(&call.arguments, "universe")?;
                let resolved = self.resolve_contract(&tool_name, universe.as_deref())?;
                self.ensure_mounted_grants_live(resolved.mounted, now_ms)?;
                let schema = serde_json::to_string_pretty(&resolved.contract.input_schema)
                    .map_err(|err| {
                        crate::VerletError::RuntimeExecution(format!(
                            "failed to encode tool schema: {err}"
                        ))
                    })?;
                let content = format!(
                    "NAME\n    {tool}\n\nSERVER\n    {server}\n\nSCHEMA HASH\n    {hash}\n\nDESCRIPTION\n    {description}\n\nINPUT SCHEMA\n{schema}",
                    tool = resolved.contract.tool_name,
                    server = resolved.mounted.binding.server_ref,
                    hash = resolved.contract.schema_hash,
                    description = resolved.contract.description,
                );
                Ok(Some(crate::CanonicalMessage::tool_result(
                    call.call_id,
                    call.tool_name,
                    content,
                    false,
                )))
            }
            TOOL_CALL_TOOL => {
                let tool_name = required_string_arg(&call.arguments, "tool")?;
                let universe = optional_string_arg(&call.arguments, "universe")?;
                let arguments = call.arguments.get("arguments").cloned().ok_or_else(|| {
                    crate::VerletError::RuntimeExecution(
                        "tool_call requires an arguments object".to_string(),
                    )
                })?;
                let resolved = self.resolve_contract(&tool_name, universe.as_deref())?;
                self.ensure_mounted_grants_live(resolved.mounted, now_ms)?;
                self.invoke_universe_call(call, resolved, arguments)
                    .await
                    .map(Some)
            }
            _ => self.invoke_pinned_direct_call(call, now_ms).await,
        }
    }

    async fn invoke_tool_call_cancellable_at(
        &self,
        call: crate::agent::agent_tool_router::AgentKernelToolCall,
        _cancellation: crate::ToolInvocationCancellation,
        now_ms: i64,
    ) -> crate::VerletResult<crate::agent::agent_tool_router::AgentKernelToolOutcome> {
        self.invoke_tool_call_at(call, now_ms)
            .await
            .map(crate::agent::agent_tool_router::AgentKernelToolOutcome::Completed)
    }
}

/// JIT validation of `tool.call` arguments against a witnessed contract.
/// Fails closed: a schema the validator cannot interpret rejects the call,
/// it never waves it through.
///
/// Scope: the JSON-schema subset MCP servers emit in practice — `type`,
/// `required`, `properties`, `enum`, `items`, `additionalProperties` —
/// recursing through objects and arrays.
pub fn validate_tool_arguments(
    contract: &WitnessedToolContract,
    arguments: &serde_json::Value,
) -> crate::VerletResult<()> {
    verlet_runtime_contracts::validate_json_schema_subset(
        &contract.input_schema,
        &contract.tool_name,
    )
    .map_err(|err| validation_error(&contract.tool_name, err))?;
    verlet_runtime_contracts::validate_json_value_against_schema(
        &contract.input_schema,
        arguments,
        &contract.tool_name,
    )
    .map_err(|err| validation_error(&contract.tool_name, err))
}

fn optional_string_arg(
    arguments: &serde_json::Value,
    key: &str,
) -> crate::VerletResult<Option<String>> {
    match arguments.get(key) {
        Some(serde_json::Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(crate::VerletError::RuntimeExecution(format!(
            "{key:?} must be a string"
        ))),
        None => Ok(None),
    }
}

fn required_string_arg(arguments: &serde_json::Value, key: &str) -> crate::VerletResult<String> {
    optional_string_arg(arguments, key)?.ok_or_else(|| {
        crate::VerletError::RuntimeExecution(format!("tool surface call requires {key:?}"))
    })
}

fn first_line(value: &str) -> &str {
    value.lines().next().unwrap_or(value)
}

fn validation_error(
    tool_name: &str,
    err: verlet_runtime_contracts::JsonSchemaValidationError,
) -> crate::VerletError {
    crate::VerletError::RuntimeExecution(format!(
        "tool {tool_name:?} arguments failed schema validation at {}: {}",
        err.path(),
        err.message()
    ))
}

#[cfg(test)]
mod tests;
