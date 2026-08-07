use sha2::Digest as _;

pub const THREAD_CONTRACT_KIND: &str = "cooldis.thread-contract";
pub const THREAD_CONTRACT_VERSION: u32 = 0;
pub const THREAD_CONTRACT_SOURCE_FORMAT: &str = "cooldis.thread.markdown.v0";
pub const THREAD_DECLARATION_KIND: &str = "cooldis.thread-declaration";
pub const THREAD_HANDLE_KIND: &str = "cooldis.thread-handle";
pub const DEFAULT_THREAD_PROPAGATOR_KIND: &str = "llm";
pub const LEGACY_AGENT_CONTRACT_KIND: &str = "cooldis.agent-contract";
pub const LEGACY_AGENT_CONTRACT_SOURCE_FORMAT: &str = "cooldis.agent.markdown.v0";
pub const LEGACY_AGENT_THREAD_DECLARATION_KIND: &str = "cooldis.agent-thread-declaration";
pub const LEGACY_AGENT_THREAD_HANDLE_KIND: &str = "cooldis.agent-thread-handle";

pub const AGENT_CONTRACT_KIND: &str = LEGACY_AGENT_CONTRACT_KIND;
pub const AGENT_CONTRACT_VERSION: u32 = THREAD_CONTRACT_VERSION;
pub const AGENT_CONTRACT_SOURCE_FORMAT: &str = LEGACY_AGENT_CONTRACT_SOURCE_FORMAT;
pub const AGENT_THREAD_DECLARATION_KIND: &str = LEGACY_AGENT_THREAD_DECLARATION_KIND;
pub const AGENT_THREAD_HANDLE_KIND: &str = LEGACY_AGENT_THREAD_HANDLE_KIND;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ThreadContractSource {
    pub format: ThreadContractSourceFormat,
    pub source: String,
}

impl ThreadContractSource {
    pub fn markdown(source: impl Into<String>) -> Self {
        Self {
            format: ThreadContractSourceFormat::MarkdownV0,
            source: source.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadContractSourceFormat {
    #[default]
    MarkdownV0,
}

impl ThreadContractSourceFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MarkdownV0 => THREAD_CONTRACT_SOURCE_FORMAT,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CompiledThreadContract {
    pub kind: String,
    pub version: u32,
    pub name: String,
    pub source_hash: String,
    #[serde(default)]
    pub requires: Vec<ThreadContractField>,
    #[serde(default)]
    pub ensures: Vec<ThreadContractField>,
    #[serde(default)]
    pub capabilities: Vec<ThreadCapabilityRequirement>,
    #[serde(default)]
    pub effects: Vec<ThreadEffectRequirement>,
    #[serde(default)]
    pub delegates: Vec<ThreadDelegateRequirement>,
    #[serde(default)]
    pub runtime: std::collections::BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
}

impl CompiledThreadContract {
    pub fn validate(&self) -> crate::VerletResult<()> {
        if !is_supported_contract_kind(&self.kind) {
            return Err(crate::VerletAgentError::RuntimeExecution(format!(
                "thread contract kind must be {THREAD_CONTRACT_KIND}"
            )));
        }
        if self.version != THREAD_CONTRACT_VERSION {
            return Err(crate::VerletAgentError::RuntimeExecution(format!(
                "thread contract version must be {THREAD_CONTRACT_VERSION}"
            )));
        }
        validate_name("thread contract name", &self.name)?;
        validate_hash("thread contract source_hash", &self.source_hash)?;
        validate_unique_fields(
            "requires",
            self.requires.iter().map(|field| field.name.as_str()),
        )?;
        validate_unique_fields(
            "ensures",
            self.ensures.iter().map(|field| field.name.as_str()),
        )?;
        for capability in &self.capabilities {
            validate_name("capability kind", &capability.kind)?;
            validate_name("capability name", &capability.name)?;
        }
        for effect in &self.effects {
            validate_name("effect name", &effect.name)?;
            validate_name("effect kind", &effect.kind)?;
            validate_name("effect binding", &effect.binding)?;
        }
        for delegate in &self.delegates {
            validate_name("delegate name", &delegate.name)?;
        }
        Ok(())
    }

    pub fn contract_hash(&self) -> crate::VerletResult<String> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(json_error)?;
        Ok(sha256_hex(&bytes))
    }

    pub fn abi_projection(&self) -> crate::VerletResult<verlet_abi::AbiOperationContract> {
        self.validate()?;
        Ok(verlet_abi::AbiOperationContract {
            registered_name: self.name.clone(),
            operation_name: "run_thread".to_string(),
            source_ports: self
                .requires
                .iter()
                .map(|field| verlet_abi::AbiSourcePort {
                    name: field.name.clone(),
                    value: field.value.clone().into(),
                    binding: verlet_abi::AbiSourceBinding::InvocationInput,
                    required: true,
                })
                .collect(),
            sink_ports: self
                .ensures
                .iter()
                .map(|field| verlet_abi::AbiSinkPort {
                    name: field.name.clone(),
                    value: field.value.clone().into(),
                    binding: verlet_abi::AbiSinkBinding::InvocationOutput,
                    required: true,
                })
                .collect(),
            effect_ports: self
                .effects
                .iter()
                .map(|effect| verlet_abi::AbiEffectPort {
                    name: effect.name.clone(),
                    kind: verlet_abi::AbiEffectKind::VfsWrite {
                        mode: verlet_abi::AbiVfsWriteMode::WriteNew,
                    },
                    binding: match effect.binding.as_str() {
                        "caller_bound" => {
                            verlet_abi::AbiEffectBinding::CallerBoundPath { path: None }
                        }
                        "operation_selected" => {
                            verlet_abi::AbiEffectBinding::OperationSelectedPath {
                                scope: effect.name.clone(),
                            }
                        }
                        _ => verlet_abi::AbiEffectBinding::HostAllocatedPath,
                    },
                    required: false,
                })
                .collect(),
            event_ports: vec![verlet_abi::AbiEventPort {
                name: "events".to_string(),
                value: verlet_abi::AbiEventValue::Jsonl,
                binding: verlet_abi::AbiEventBinding::InvocationEvents,
            }],
            required_capabilities: self
                .capabilities
                .iter()
                .map(|capability| format!("{}:{}", capability.kind, capability.name))
                .collect(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ThreadContractField {
    pub name: String,
    #[serde(rename = "kind", alias = "value")]
    pub value: ThreadContractValueKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadContractValueKind {
    Bytes,
    Text,
    Json,
}

impl From<ThreadContractValueKind> for verlet_abi::AbiPortValue {
    fn from(value: ThreadContractValueKind) -> Self {
        match value {
            ThreadContractValueKind::Bytes => Self::Bytes,
            ThreadContractValueKind::Text => Self::Text,
            ThreadContractValueKind::Json => Self::Json,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ThreadCapabilityRequirement {
    pub kind: String,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ThreadEffectRequirement {
    pub name: String,
    pub kind: String,
    pub binding: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ThreadDelegateRequirement {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ThreadDeclaration {
    #[serde(default = "default_thread_declaration_kind")]
    pub kind: String,
    #[serde(default)]
    pub version: u32,
    pub contract: ThreadContractReference,
    #[serde(default)]
    pub inputs: serde_json::Value,
    pub initial_turn: ThreadInitialTurn,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub propagator: Option<ThreadPropagatorSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topology: Option<ThreadTopologyDeclaration>,
    #[serde(default)]
    pub metadata: std::collections::BTreeMap<String, String>,
}

impl ThreadDeclaration {
    pub fn new(contract: ThreadContractReference, initial_turn: ThreadInitialTurn) -> Self {
        Self {
            kind: THREAD_DECLARATION_KIND.to_string(),
            version: THREAD_CONTRACT_VERSION,
            contract,
            inputs: serde_json::Value::Object(Default::default()),
            initial_turn,
            propagator: None,
            topology: None,
            metadata: std::collections::BTreeMap::new(),
        }
    }

    pub fn validate(&self) -> crate::VerletResult<()> {
        if !is_supported_declaration_kind(&self.kind) {
            return Err(crate::VerletAgentError::RuntimeExecution(format!(
                "thread declaration kind must be {THREAD_DECLARATION_KIND}"
            )));
        }
        if self.version != THREAD_CONTRACT_VERSION {
            return Err(crate::VerletAgentError::RuntimeExecution(format!(
                "thread declaration version must be {THREAD_CONTRACT_VERSION}"
            )));
        }
        self.contract.validate()?;
        if let Some(propagator) = &self.propagator {
            propagator.validate()?;
        }
        if self.initial_turn.content.trim().is_empty() {
            return Err(crate::VerletAgentError::RuntimeExecution(
                "thread declaration initial_turn.content cannot be empty".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ThreadContractReference {
    #[serde(
        default,
        rename = "ref",
        alias = "ref_path",
        skip_serializing_if = "Option::is_none"
    )]
    pub ref_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inline: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compiled: Option<CompiledThreadContract>,
}

impl ThreadContractReference {
    pub fn inline_markdown(source: impl Into<String>) -> Self {
        Self {
            ref_path: None,
            inline: Some(source.into()),
            format: Some(THREAD_CONTRACT_SOURCE_FORMAT.to_string()),
            compiled: None,
        }
    }

    pub fn compiled(contract: CompiledThreadContract) -> Self {
        Self {
            ref_path: None,
            inline: None,
            format: None,
            compiled: Some(contract),
        }
    }

    pub fn file_ref(path: impl Into<String>) -> Self {
        Self {
            ref_path: Some(path.into()),
            inline: None,
            format: None,
            compiled: None,
        }
    }

    pub fn validate(&self) -> crate::VerletResult<()> {
        let populated = self.ref_path.is_some() as u8
            + self.inline.is_some() as u8
            + self.compiled.is_some() as u8;
        if populated != 1 {
            return Err(crate::VerletAgentError::RuntimeExecution(
                "thread contract reference must set exactly one of ref_path, inline, or compiled"
                    .to_string(),
            ));
        }
        if let Some(format) = &self.format
            && !is_supported_source_format(format)
        {
            return Err(crate::VerletAgentError::RuntimeExecution(format!(
                "unsupported thread contract source format {format}"
            )));
        }
        if let Some(compiled) = &self.compiled {
            compiled.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ThreadInitialTurn {
    #[serde(default = "default_initial_turn_role")]
    pub role: String,
    pub content: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ThreadPropagatorSelection {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl ThreadPropagatorSelection {
    pub fn llm() -> Self {
        Self {
            kind: DEFAULT_THREAD_PROPAGATOR_KIND.to_string(),
            name: None,
        }
    }

    pub fn named(kind: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            name: Some(name.into()),
        }
    }

    pub fn from_runtime_hint(value: Option<&String>) -> Self {
        match value {
            Some(value) if !value.trim().is_empty() => Self {
                kind: value.trim().to_string(),
                name: None,
            },
            _ => Self::llm(),
        }
    }

    pub fn validate(&self) -> crate::VerletResult<()> {
        validate_name("thread propagator kind", &self.kind)?;
        if let Some(name) = &self.name {
            validate_name("thread propagator name", name)?;
        }
        Ok(())
    }
}

impl ThreadInitialTurn {
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: default_initial_turn_role(),
            content: content.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ThreadTopologyDeclaration {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spawned_from: Option<verlet_runtime_contracts::ThreadId>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ThreadReceiptSet {
    pub compile: String,
    pub spawn: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ThreadHandle {
    pub kind: String,
    pub version: u32,
    pub thread_id: verlet_runtime_contracts::ThreadId,
    pub status: verlet_runtime_contracts::ThreadStatus,
    pub propagator: ThreadPropagatorSelection,
    pub contract_hash: String,
    pub submitted_turn_id: String,
    pub receipts: ThreadReceiptSet,
}

pub struct ThreadContractCompiler;

impl ThreadContractCompiler {
    pub fn compile(source: &ThreadContractSource) -> crate::VerletResult<CompiledThreadContract> {
        match source.format {
            ThreadContractSourceFormat::MarkdownV0 => compile_markdown_v0(&source.source),
        }
    }
}

fn compile_markdown_v0(source: &str) -> crate::VerletResult<CompiledThreadContract> {
    let (frontmatter, body) = parse_frontmatter(source)?;
    let name = frontmatter.get("name").cloned().ok_or_else(|| {
        crate::VerletAgentError::RuntimeExecution("thread contract missing name".to_string())
    })?;
    let kind = frontmatter
        .get("kind")
        .map(String::as_str)
        .unwrap_or("thread");
    if !matches!(kind, "thread" | "agent") {
        return Err(crate::VerletAgentError::RuntimeExecution(format!(
            "thread contract frontmatter kind must be thread, got {kind}"
        )));
    }
    let version = frontmatter
        .get("version")
        .map(|value| value.parse::<u32>())
        .transpose()
        .map_err(|err| {
            crate::VerletAgentError::RuntimeExecution(format!("invalid thread version: {err}"))
        })?
        .unwrap_or(THREAD_CONTRACT_VERSION);
    if version != THREAD_CONTRACT_VERSION {
        return Err(crate::VerletAgentError::RuntimeExecution(format!(
            "thread contract version must be {THREAD_CONTRACT_VERSION}"
        )));
    }

    let sections = parse_sections(body);
    let contract = CompiledThreadContract {
        kind: THREAD_CONTRACT_KIND.to_string(),
        version,
        name,
        source_hash: sha256_hex(source.as_bytes()),
        requires: parse_fields(sections.get("requires").map(String::as_str).unwrap_or("")),
        ensures: parse_fields(sections.get("ensures").map(String::as_str).unwrap_or("")),
        capabilities: parse_capabilities(sections.get("tools").map(String::as_str).unwrap_or("")),
        effects: parse_effects(sections.get("effects").map(String::as_str).unwrap_or("")),
        delegates: parse_delegates(sections.get("delegates").map(String::as_str).unwrap_or("")),
        runtime: parse_runtime(sections.get("runtime").map(String::as_str).unwrap_or("")),
        instructions: sections
            .get("instructions")
            .map(|instructions| instructions.trim().to_string())
            .filter(|instructions| !instructions.is_empty()),
    };
    contract.validate()?;
    Ok(contract)
}

fn parse_frontmatter(
    source: &str,
) -> crate::VerletResult<(std::collections::BTreeMap<String, String>, &str)> {
    let trimmed = source.strip_prefix("---\n").ok_or_else(|| {
        crate::VerletAgentError::RuntimeExecution(
            "agent contract missing YAML-like frontmatter".to_string(),
        )
    })?;
    let Some(end) = trimmed.find("\n---") else {
        return Err(crate::VerletAgentError::RuntimeExecution(
            "agent contract frontmatter is not closed".to_string(),
        ));
    };
    let frontmatter_text = &trimmed[..end];
    let body_start = end + "\n---".len();
    let body = trimmed[body_start..]
        .strip_prefix('\n')
        .unwrap_or(&trimmed[body_start..]);
    let mut frontmatter = std::collections::BTreeMap::new();
    for line in frontmatter_text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some((key, value)) = line.split_once(':') else {
            return Err(crate::VerletAgentError::RuntimeExecution(format!(
                "invalid frontmatter line {line:?}"
            )));
        };
        frontmatter.insert(key.trim().to_string(), value.trim().to_string());
    }
    Ok((frontmatter, body))
}

fn parse_sections(body: &str) -> std::collections::BTreeMap<String, String> {
    let mut sections = std::collections::BTreeMap::new();
    let mut current: Option<String> = None;
    let mut buffer = String::new();

    for line in body.lines() {
        if let Some(header) = line.strip_prefix("### ") {
            if let Some(name) = current.take() {
                sections.insert(name, buffer.trim().to_string());
                buffer.clear();
            }
            current = Some(normalize_section_name(header));
            continue;
        }
        if current.is_some() {
            buffer.push_str(line);
            buffer.push('\n');
        }
    }

    if let Some(name) = current {
        sections.insert(name, buffer.trim().to_string());
    }
    sections
}

fn normalize_section_name(name: &str) -> String {
    name.trim().to_ascii_lowercase().replace(' ', "-")
}

fn parse_fields(section: &str) -> Vec<ThreadContractField> {
    bullet_items(section)
        .into_iter()
        .filter_map(|item| {
            let (name, description) = parse_named_description(&item)?;
            Some(ThreadContractField {
                name,
                value: infer_value_kind(description.as_deref().unwrap_or("")),
                description,
            })
        })
        .collect()
}

fn parse_capabilities(section: &str) -> Vec<ThreadCapabilityRequirement> {
    bullet_items(section)
        .into_iter()
        .filter_map(|item| {
            let (kind, name) = parse_pair(&item)?;
            Some(ThreadCapabilityRequirement { kind, name })
        })
        .collect()
}

fn parse_effects(section: &str) -> Vec<ThreadEffectRequirement> {
    bullet_items(section)
        .into_iter()
        .filter_map(|item| {
            let (name, description) = parse_named_description(&item)?;
            let description_text = description.as_deref().unwrap_or("");
            Some(ThreadEffectRequirement {
                name,
                kind: infer_effect_kind(description_text),
                binding: infer_effect_binding(description_text),
                description,
            })
        })
        .collect()
}

fn parse_delegates(section: &str) -> Vec<ThreadDelegateRequirement> {
    bullet_items(section)
        .into_iter()
        .filter_map(|item| {
            let (name, description) = parse_named_description(&item)?;
            Some(ThreadDelegateRequirement { name, description })
        })
        .collect()
}

fn parse_runtime(section: &str) -> std::collections::BTreeMap<String, String> {
    bullet_items(section)
        .into_iter()
        .filter_map(|item| parse_pair(&item))
        .collect()
}

fn bullet_items(section: &str) -> Vec<String> {
    section
        .lines()
        .filter_map(|line| line.trim().strip_prefix("- "))
        .map(|line| line.trim().to_string())
        .filter(|line| !line.is_empty())
        .collect()
}

fn parse_named_description(item: &str) -> Option<(String, Option<String>)> {
    let (name, description) = if let Some((name, description)) = item.split_once(':') {
        (
            strip_code_span(name.trim()).to_string(),
            Some(description.trim().to_string()),
        )
    } else {
        (strip_code_span(item.trim()).to_string(), None)
    };
    if name.is_empty() {
        None
    } else {
        Some((name, description.filter(|value| !value.is_empty())))
    }
}

fn parse_pair(item: &str) -> Option<(String, String)> {
    let (left, right) = item.split_once(':')?;
    let left = strip_code_span(left.trim()).to_string();
    let right = strip_code_span(right.trim()).to_string();
    if left.is_empty() || right.is_empty() {
        None
    } else {
        Some((left, right))
    }
}

fn strip_code_span(value: &str) -> &str {
    value
        .strip_prefix('`')
        .and_then(|value| value.strip_suffix('`'))
        .unwrap_or(value)
}

fn infer_value_kind(description: &str) -> ThreadContractValueKind {
    let lower = description.to_ascii_lowercase();
    if lower.contains("json") || lower.contains("object") || lower.contains("array") {
        ThreadContractValueKind::Json
    } else if lower.contains("bytes") || lower.contains("binary") {
        ThreadContractValueKind::Bytes
    } else {
        ThreadContractValueKind::Text
    }
}

fn infer_effect_kind(description: &str) -> String {
    let lower = description.to_ascii_lowercase();
    if lower.contains("vfs") || lower.contains("file") || lower.contains("artifact") {
        "artifact.write".to_string()
    } else {
        "effect".to_string()
    }
}

fn infer_effect_binding(description: &str) -> String {
    let lower = description.to_ascii_lowercase();
    if lower.contains("caller") {
        "caller_bound".to_string()
    } else if lower.contains("operation-selected") || lower.contains("operation selected") {
        "operation_selected".to_string()
    } else {
        "host_allocated".to_string()
    }
}

fn validate_name(label: &str, value: &str) -> crate::VerletResult<()> {
    let valid = !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | ':'));
    if valid {
        Ok(())
    } else {
        Err(crate::VerletAgentError::RuntimeExecution(format!(
            "{label} contains invalid characters: {value:?}"
        )))
    }
}

fn validate_unique_fields<'a>(
    label: &str,
    names: impl Iterator<Item = &'a str>,
) -> crate::VerletResult<()> {
    let mut seen = std::collections::BTreeSet::new();
    for name in names {
        validate_name(label, name)?;
        if !seen.insert(name.to_string()) {
            return Err(crate::VerletAgentError::RuntimeExecution(format!(
                "duplicate {label} field {name:?}"
            )));
        }
    }
    Ok(())
}

fn validate_hash(label: &str, value: &str) -> crate::VerletResult<()> {
    let Some(hash) = value.strip_prefix("sha256:") else {
        return Err(crate::VerletAgentError::RuntimeExecution(format!(
            "{label} must start with sha256:"
        )));
    };
    let valid = hash.len() == 64
        && hash
            .chars()
            .all(|ch| ch.is_ascii_digit() || matches!(ch, 'a'..='f'));
    if valid {
        Ok(())
    } else {
        Err(crate::VerletAgentError::RuntimeExecution(format!(
            "{label} must be a lowercase sha256 hash"
        )))
    }
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = sha2::Sha256::digest(bytes);
    format!("sha256:{digest:x}")
}

fn json_error(err: serde_json::Error) -> crate::VerletAgentError {
    crate::VerletAgentError::RuntimeExecution(format!("thread contract JSON error: {err}"))
}

fn default_initial_turn_role() -> String {
    "user".to_string()
}

fn default_thread_declaration_kind() -> String {
    THREAD_DECLARATION_KIND.to_string()
}

fn is_supported_contract_kind(kind: &str) -> bool {
    matches!(kind, THREAD_CONTRACT_KIND | LEGACY_AGENT_CONTRACT_KIND)
}

fn is_supported_declaration_kind(kind: &str) -> bool {
    matches!(
        kind,
        THREAD_DECLARATION_KIND | LEGACY_AGENT_THREAD_DECLARATION_KIND
    )
}

fn is_supported_source_format(format: &str) -> bool {
    matches!(
        format,
        THREAD_CONTRACT_SOURCE_FORMAT | LEGACY_AGENT_CONTRACT_SOURCE_FORMAT
    )
}

pub type AgentContractSource = ThreadContractSource;
pub type AgentContractSourceFormat = ThreadContractSourceFormat;
pub type CompiledAgentContract = CompiledThreadContract;
pub type AgentContractField = ThreadContractField;
pub type AgentContractValueKind = ThreadContractValueKind;
pub type AgentCapabilityRequirement = ThreadCapabilityRequirement;
pub type AgentEffectRequirement = ThreadEffectRequirement;
pub type AgentDelegateRequirement = ThreadDelegateRequirement;
pub type AgentThreadDeclaration = ThreadDeclaration;
pub type AgentContractReference = ThreadContractReference;
pub type AgentInitialTurn = ThreadInitialTurn;
pub type AgentThreadTopologyDeclaration = ThreadTopologyDeclaration;
pub type AgentThreadReceiptSet = ThreadReceiptSet;
pub type AgentThreadHandle = ThreadHandle;
pub type AgentContractCompiler = ThreadContractCompiler;

#[cfg(test)]
mod tests;
