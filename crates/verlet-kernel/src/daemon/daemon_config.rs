const DEFAULT_SQLITE_QUEUE_PATH: &str = ".verlet/queue/ingress.sqlite";
const DEFAULT_SERVICE_LABEL: &str = "com.verlet.daemon";
const DEFAULT_TELEGRAM_WEBHOOK_PATH: &str = "/telegram";
const ROUTE_POLICY_VALUES: &[&str] = &[
    "queue_per_conversation",
    "observe_only",
    "reject",
    "steer",
    "steer_when_active",
    "interrupt",
    "interrupt_on_new_dm",
    "fork",
    "fork_on_new_dm",
    "coalesce_bursts",
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedVerletDaemonConfig {
    pub config: VerletDaemonConfig,
    pub path: Option<std::path::PathBuf>,
    pub base_dir: std::path::PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerletProjectDiscovery {
    pub root: std::path::PathBuf,
    pub config_path: Option<std::path::PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerletDaemonConfig {
    #[serde(default)]
    pub identity: crate::daemon::identity::VerletDaemonIdentityConfig,
    #[serde(default)]
    pub runtime: VerletRuntimeConfig,
    #[serde(default)]
    pub app_server: VerletDaemonAppServerConfig,
    #[serde(default)]
    pub registries: VerletDaemonRegistriesConfig,
    #[serde(default)]
    pub operations: VerletDaemonOperationsConfig,
    #[serde(default)]
    pub provider: VerletProviderConfig,
    #[serde(default)]
    pub io: VerletIoConfig,
    #[serde(default)]
    pub sync: crate::daemon::remote_store::endpoint::VerletDaemonSyncConfig,
}

impl Default for VerletDaemonConfig {
    fn default() -> Self {
        Self {
            identity: synthesized_local_daemon_identity_config(),
            runtime: VerletRuntimeConfig::default(),
            app_server: VerletDaemonAppServerConfig::default(),
            registries: VerletDaemonRegistriesConfig::default(),
            operations: VerletDaemonOperationsConfig::default(),
            provider: VerletProviderConfig::default(),
            io: VerletIoConfig::default(),
            sync: crate::daemon::remote_store::endpoint::VerletDaemonSyncConfig::default(),
        }
    }
}

impl VerletDaemonConfig {
    pub fn validate(&self) -> crate::kernel::runtime_host::VerletResult<()> {
        let errors = self.validation_errors();
        if errors.is_empty() {
            return Ok(());
        }

        Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!("invalid Verlet daemon config:\n- {}", errors.join("\n- ")),
        ))
    }

    pub fn validation_errors(&self) -> Vec<String> {
        let mut errors = Vec::new();

        if let Err(err) = self.identity.validate() {
            errors.push(format!("identity: {err}"));
        }

        if let Err(err) = self.app_server.listen_addr() {
            errors.push(format!("app_server.listen: {err}"));
        }

        if let Some(provider) = &self.provider.provider {
            if !matches!(
                provider.as_str(),
                "local"
                    | "local_offline"
                    | "offline"
                    | "bifrost"
                    | "bifrost_openai"
                    | "bifrost_openai_chat"
                    | "bifrost_chat"
                    | "openai_chat"
                    | "openai_chat_completions"
                    | "anthropic"
                    | "anthropic_messages"
                    | "anthropic_bedrock"
                    | "bedrock"
                    | "bedrock_anthropic"
                    | "openai_compatible"
                    | "openai_compatible_openai"
                    | "openai_compatible_chat"
                    | "openai_compatible_serverless"
            ) {
                errors.push(format!(
                    "provider.provider must be local, bifrost_openai, openai_chat_completions, anthropic, anthropic_bedrock, or openai_compatible, got {provider:?}"
                ));
            }
        }

        self.io.validate(&mut errors);
        if let Err(err) = self.sync.validate() {
            errors.push(format!("sync: {err}"));
        }
        errors
    }

    fn resolve_paths(&mut self, base: &std::path::Path) {
        self.app_server.resolve_paths(base);
        if let Some(listen) = self.sync.listen.as_deref()
            && let Ok(crate::adapters::app_server::AppServerListenAddr::Unix(path)) =
                crate::adapters::app_server::AppServerListenAddr::parse(listen)
            && path.is_relative()
        {
            self.sync.listen = Some(unix_listen_url(resolve_config_path(base, path)));
        }
        self.registries.resolve_paths(base);
        if let Some(path) = self.runtime.cwd.take() {
            self.runtime.cwd = Some(resolve_config_path(base, path));
        }
        if let Some(path) = self.runtime.runtime_home.take() {
            self.runtime.runtime_home = Some(resolve_config_path(base, path));
        }
        if let Some(path) = self.runtime.state_home.take() {
            self.runtime.state_home = Some(resolve_config_path(base, path));
        }
        if let Some(workspace) = &mut self.runtime.workspace {
            workspace.host_path = resolve_config_path(base, workspace.host_path.clone());
        }
        if let Some(path) = self.provider.env_file.take() {
            self.provider.env_file = Some(resolve_config_path(base, path));
        }
        self.io.resolve_paths(base);
    }
}

pub(crate) fn synthesized_local_daemon_identity_config()
-> crate::daemon::identity::VerletDaemonIdentityConfig {
    crate::daemon::identity::VerletDaemonIdentityConfig {
        mode: crate::daemon::identity::IdentityMode::Local,
        tenant_id: Some("cooldis_app_server".to_string()),
        console_principal: Some(crate::daemon::identity::PrincipalId::new("local_user")),
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerletRuntimeConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<std::path::PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_home: Option<std::path::PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_home: Option<std::path::PathBuf>,
    /// Default placement applied to manifest binds unless an operator bind
    /// surface supplies an override. Absent means local.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement: Option<crate::agent::manifest_bind::AgentManifestPlacementBinding>,
    /// Default host workspace binding applied to a manifest that declares a
    /// workspace requirement. Bind-time RPC input may override it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<crate::agent::manifest_bind::AgentManifestWorkspaceBinding>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerletDaemonRegistriesConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operations: Option<std::path::PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agents: Option<std::path::PathBuf>,
}

impl VerletDaemonRegistriesConfig {
    fn resolve_paths(&mut self, base: &std::path::Path) {
        if let Some(path) = self.operations.take() {
            self.operations = Some(resolve_config_path(base, path));
        }
        if let Some(path) = self.agents.take() {
            self.agents = Some(resolve_config_path(base, path));
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerletDaemonOperationsConfig {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub global_operation_names: Vec<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub load_all_active_when_unbound: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerletDaemonAppServerConfig {
    #[serde(default = "default_app_server_listen")]
    pub listen: String,
}

impl Default for VerletDaemonAppServerConfig {
    fn default() -> Self {
        Self {
            listen: default_app_server_listen(),
        }
    }
}

impl VerletDaemonAppServerConfig {
    pub fn listen_addr(
        &self,
    ) -> crate::kernel::runtime_host::VerletResult<crate::adapters::app_server::AppServerListenAddr>
    {
        crate::adapters::app_server::AppServerListenAddr::parse(&self.listen)
    }

    fn resolve_paths(&mut self, base: &std::path::Path) {
        let Ok(crate::adapters::app_server::AppServerListenAddr::Unix(path)) = self.listen_addr()
        else {
            return;
        };
        if path.is_relative() {
            self.listen = unix_listen_url(resolve_config_path(base, path));
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerletProviderConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_env: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aws_access_key_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aws_secret_access_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aws_session_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env_file: Option<std::path::PathBuf>,
}

impl VerletProviderConfig {
    pub fn provider_name(&self) -> &str {
        self.provider.as_deref().unwrap_or_else(|| {
            if self.aws_access_key_id.is_some() || self.aws_secret_access_key.is_some() {
                "anthropic_bedrock"
            } else if self.base_url.is_some() || self.model.is_some() || self.api_key.is_some() {
                "bifrost_openai"
            } else {
                "local"
            }
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerletIoConfig {
    #[serde(default)]
    pub ingress: VerletIngressConfig,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub routes: Vec<VerletIoRouteConfig>,
}

impl Default for VerletIoConfig {
    fn default() -> Self {
        Self {
            ingress: VerletIngressConfig::default(),
            routes: Vec::new(),
        }
    }
}

impl VerletIoConfig {
    fn validate(&self, errors: &mut Vec<String>) {
        self.ingress.validate("io.ingress", errors);

        let mut route_ids = std::collections::BTreeSet::new();
        let mut clock_route_count = 0;
        for route in &self.routes {
            route.validate(errors);
            if !route.id.trim().is_empty() && !route_ids.insert(route.id.clone()) {
                errors.push(format!("io.routes id {:?} is duplicated", route.id));
            }
            if route.kind == "clock.tick" {
                clock_route_count += 1;
            }
        }
        if clock_route_count > 1 {
            errors.push("io.routes supports at most one clock.tick route".to_string());
        }
    }

    fn resolve_paths(&mut self, base: &std::path::Path) {
        self.ingress.resolve_paths(base);
        for route in &mut self.routes {
            route.resolve_paths(base);
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerletIngressConfig {
    #[serde(default)]
    pub persistence: verlet_io_core::IngressPersistenceConfig,
    #[serde(default)]
    pub queue: VerletQueueConfig,
}

impl Default for VerletIngressConfig {
    fn default() -> Self {
        Self {
            persistence: verlet_io_core::IngressPersistenceConfig::default(),
            queue: VerletQueueConfig::default(),
        }
    }
}

impl VerletIngressConfig {
    fn validate(&self, scope: &str, errors: &mut Vec<String>) {
        validate_persistence(scope, &self.persistence, errors);
        self.queue.validate(scope, errors);
    }

    fn resolve_paths(&mut self, base: &std::path::Path) {
        self.queue.resolve_paths(base);
        if self.queue.dsn.is_none() && self.queue.sqlite_path.is_none() {
            self.queue.sqlite_path = Some(default_sqlite_queue_path_for_base(base));
        }
    }

    pub fn effective_queue_dsn(&self) -> String {
        if let Some(dsn) = &self.queue.dsn {
            return dsn.clone();
        }

        let sqlite_path = self
            .queue
            .sqlite_path
            .clone()
            .unwrap_or_else(default_sqlite_queue_path);
        format!("sqlite://{}", sqlite_path.display())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerletQueueConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dsn: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sqlite_path: Option<std::path::PathBuf>,
}

impl Default for VerletQueueConfig {
    fn default() -> Self {
        Self {
            dsn: None,
            sqlite_path: None,
        }
    }
}

impl VerletQueueConfig {
    fn validate(&self, scope: &str, errors: &mut Vec<String>) {
        if self.dsn.is_some() && self.sqlite_path.is_some() {
            errors.push(format!("{scope}.queue cannot set both dsn and sqlite_path"));
        }
        if self.dsn.as_ref().is_some_and(|dsn| dsn.trim().is_empty()) {
            errors.push(format!("{scope}.queue.dsn cannot be empty"));
        }
    }

    fn resolve_paths(&mut self, base: &std::path::Path) {
        if let Some(path) = self.sqlite_path.take() {
            self.sqlite_path = Some(resolve_config_path(base, path));
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerletIoRouteConfig {
    pub id: String,
    pub kind: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_policies: Option<std::collections::BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub threading: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coalesce_bursts: Option<VerletCoalesceBurstsConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingress: Option<VerletIngressConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub egress_projection: Vec<VerletEgressProjectionRuleConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typing_simulation: Option<VerletTypingSimulationConfig>,
    #[serde(default, skip_serializing_if = "VerletEgressRetryConfig::is_default")]
    pub egress_retry: VerletEgressRetryConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telegram: Option<VerletTelegramRouteConfig>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub metadata: std::collections::BTreeMap<String, String>,
}

impl VerletIoRouteConfig {
    fn validate(&self, errors: &mut Vec<String>) {
        if self.id.trim().is_empty() {
            errors.push("io.routes id cannot be empty".to_string());
        }
        if self.kind.trim().is_empty() {
            errors.push(format!("io.routes {:?} kind cannot be empty", self.id));
        }
        if let Some(agent_ref) = &self.agent_ref {
            if agent_ref.trim().is_empty() {
                errors.push(format!("io.routes.{}.agent_ref cannot be empty", self.id));
            } else if !agent_ref.starts_with("agent://") {
                errors.push(format!(
                    "io.routes.{}.agent_ref must be an agent:// ref",
                    self.id
                ));
            } else if let Err(err) = crate::agent::manifest::AgentRecordRef::parse(agent_ref) {
                errors.push(format!(
                    "io.routes.{}.agent_ref must be an agent:// ref: {err}",
                    self.id
                ));
            }
        }
        if let Some(ingress) = &self.ingress {
            ingress.validate(&format!("io.routes.{}", self.id), errors);
        }
        if let Some(policy) = &self.policy {
            validate_route_policy(&format!("io.routes.{}", self.id), "policy", policy, errors);
        }
        if let Some(content_policies) = &self.content_policies {
            for (kind, policy) in content_policies {
                validate_route_policy(
                    &format!("io.routes.{}", self.id),
                    &format!("content_policies.{kind}"),
                    policy,
                    errors,
                );
            }
        }
        if self.policy.as_deref() == Some("coalesce_bursts") && self.coalesce_bursts.is_none() {
            errors.push(format!(
                "io.routes.{}.policy coalesce_bursts requires coalesce_bursts config",
                self.id
            ));
        }
        if let Some(content_policies) = &self.content_policies {
            for (kind, policy) in content_policies {
                if policy == "coalesce_bursts" && self.coalesce_bursts.is_none() {
                    errors.push(format!(
                        "io.routes.{}.content_policies.{kind} coalesce_bursts requires coalesce_bursts config",
                        self.id
                    ));
                }
            }
        }
        if let Some(coalesce) = &self.coalesce_bursts {
            coalesce.validate(&format!("io.routes.{}.coalesce_bursts", self.id), errors);
        }
        for (index, rule) in self.egress_projection.iter().enumerate() {
            let scope = format!("io.routes.{}.egress_projection[{index}]", self.id);
            if rule.pattern.trim().is_empty() {
                errors.push(format!("{scope}.pattern cannot be empty"));
            } else if let Err(err) = regex::Regex::new(&rule.pattern) {
                errors.push(format!("{scope}.pattern invalid regex: {err}"));
            }
            if rule.action.trim().is_empty() {
                errors.push(format!("{scope}.action cannot be empty"));
            }
        }
        if let Some(typing) = &self.typing_simulation
            && typing.chars_per_second == 0
        {
            errors.push(format!(
                "io.routes.{}.typing_simulation.chars_per_second must be greater than zero",
                self.id
            ));
        }
        if self.egress_retry.max_attempts == 0 {
            errors.push(format!(
                "io.routes.{}.egress_retry.max_attempts must be greater than zero",
                self.id
            ));
        }
        if self.kind == "telegram.bot" {
            match &self.telegram {
                Some(telegram) => telegram.validate(
                    &format!("io.routes.{}.telegram", self.id),
                    self.enabled,
                    errors,
                ),
                None if self.enabled => errors.push(format!(
                    "io.routes {:?} kind telegram.bot requires a telegram webhook config",
                    self.id
                )),
                None => {}
            }
        }
        if self.kind == "clock.tick" && self.telegram.is_some() {
            errors.push(format!(
                "io.routes {:?} kind clock.tick does not accept telegram config",
                self.id
            ));
        }
    }

    fn resolve_paths(&mut self, base: &std::path::Path) {
        if let Some(ingress) = &mut self.ingress {
            ingress.resolve_paths(base);
        }
    }
}

fn validate_route_policy(scope: &str, field: &str, policy: &str, errors: &mut Vec<String>) {
    if ROUTE_POLICY_VALUES.contains(&policy) {
        return;
    }
    errors.push(format!(
        "{scope}.{field} must be one of {}, got {policy:?}",
        ROUTE_POLICY_VALUES.join(", ")
    ));
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerletCoalesceBurstsConfig {
    pub window_ms: u64,
    pub max_batch: usize,
}

impl VerletCoalesceBurstsConfig {
    fn validate(&self, scope: &str, errors: &mut Vec<String>) {
        if self.window_ms == 0 {
            errors.push(format!("{scope}.window_ms must be greater than zero"));
        }
        if self.max_batch == 0 {
            errors.push(format!("{scope}.max_batch must be greater than zero"));
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerletEgressProjectionRuleConfig {
    pub pattern: String,
    pub action: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerletTypingSimulationConfig {
    pub chars_per_second: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerletEgressRetryConfig {
    #[serde(default = "default_egress_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_egress_base_backoff_ms")]
    pub base_backoff_ms: u64,
}

impl VerletEgressRetryConfig {
    fn is_default(value: &Self) -> bool {
        *value == Self::default()
    }
}

impl Default for VerletEgressRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: default_egress_max_attempts(),
            base_backoff_ms: default_egress_base_backoff_ms(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerletTelegramRouteConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub listen: Option<String>,
    #[serde(default = "default_telegram_webhook_path")]
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_token_env: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bot_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bot_token_env: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_base: Option<String>,
}

impl Default for VerletTelegramRouteConfig {
    fn default() -> Self {
        Self {
            listen: None,
            path: default_telegram_webhook_path(),
            secret_token: None,
            secret_token_env: None,
            bot_token: None,
            bot_token_env: None,
            api_base: None,
        }
    }
}

impl VerletTelegramRouteConfig {
    fn validate(&self, scope: &str, enabled: bool, errors: &mut Vec<String>) {
        if self
            .listen
            .as_deref()
            .is_some_and(|listen| listen.trim().is_empty())
        {
            errors.push(format!("{scope}.listen cannot be empty"));
        }
        if self.listen.is_none() {
            errors.push(format!("{scope}.listen is required"));
        }
        if !self.path.starts_with('/') {
            errors.push(format!("{scope}.path must start with /"));
        }
        if self
            .secret_token
            .as_ref()
            .is_some_and(|value| value.trim().is_empty())
        {
            errors.push(format!("{scope}.secret_token cannot be empty"));
        }
        if self
            .secret_token_env
            .as_ref()
            .is_some_and(|value| value.trim().is_empty())
        {
            errors.push(format!("{scope}.secret_token_env cannot be empty"));
        }
        if enabled && self.secret_token.is_none() && self.secret_token_env.is_none() {
            errors.push(format!(
                "{scope}.secret_token or secret_token_env is required when the route is enabled"
            ));
        }
        if self
            .bot_token
            .as_ref()
            .is_some_and(|value| value.trim().is_empty())
        {
            errors.push(format!("{scope}.bot_token cannot be empty"));
        }
        if self
            .bot_token_env
            .as_ref()
            .is_some_and(|value| value.trim().is_empty())
        {
            errors.push(format!("{scope}.bot_token_env cannot be empty"));
        }
        if self
            .api_base
            .as_ref()
            .is_some_and(|value| value.trim().is_empty())
        {
            errors.push(format!("{scope}.api_base cannot be empty"));
        }
    }

    pub fn secret_token_value(&self) -> crate::kernel::runtime_host::VerletResult<Option<String>> {
        resolve_optional_secret(
            "telegram secret_token",
            &self.secret_token,
            &self.secret_token_env,
        )
    }

    pub fn bot_token_value(&self) -> crate::kernel::runtime_host::VerletResult<Option<String>> {
        resolve_optional_secret("telegram bot_token", &self.bot_token, &self.bot_token_env)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, strum::EnumString)]
pub enum VerletDaemonServiceTarget {
    #[strum(serialize = "launchd", serialize = "macos", serialize = "darwin")]
    Launchd,
    #[strum(serialize = "systemd", serialize = "linux")]
    Systemd,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerletDaemonServiceSpec {
    pub label: String,
    pub executable: std::path::PathBuf,
    pub config_path: std::path::PathBuf,
    pub working_directory: Option<std::path::PathBuf>,
}

impl VerletDaemonServiceSpec {
    pub fn new(executable: std::path::PathBuf, config_path: std::path::PathBuf) -> Self {
        Self {
            label: DEFAULT_SERVICE_LABEL.to_string(),
            executable,
            config_path,
            working_directory: None,
        }
    }

    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = label.into();
        self
    }

    pub fn with_working_directory(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.working_directory = Some(path.into());
        self
    }
}

pub fn load_verlet_daemon_config(
    path: Option<&std::path::Path>,
) -> crate::kernel::runtime_host::VerletResult<LoadedVerletDaemonConfig> {
    match path {
        Some(path) => load_verlet_daemon_config_layers(
            &[path.to_path_buf()],
            path.parent()
                .unwrap_or_else(|| std::path::Path::new("."))
                .to_path_buf(),
        ),
        None => {
            let cwd = std::env::current_dir().map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "failed to read current working directory: {err}"
                ))
            })?;
            let project = discover_verlet_project(&cwd)?;
            match project.config_path {
                Some(path) => load_verlet_daemon_config_layers(&[path], project.root),
                None => Ok(LoadedVerletDaemonConfig {
                    config: VerletDaemonConfig::default(),
                    path: None,
                    base_dir: project.root,
                }),
            }
        }
    }
}

pub fn load_verlet_daemon_config_layers(
    paths: &[std::path::PathBuf],
    fallback_base_dir: std::path::PathBuf,
) -> crate::kernel::runtime_host::VerletResult<LoadedVerletDaemonConfig> {
    let mut config = VerletDaemonConfig::default();
    let mut loaded_path = None;
    let mut loaded_base_dir = fallback_base_dir;

    for path in paths {
        validate_config_extension(Some(path.as_path()))?;
        let text = read_config_text(path)?;
        let base_dir = path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .to_path_buf();
        let presence = daemon_config_presence(&text)?;
        let mut layer = decode_daemon_config(&text)?;
        layer.resolve_paths(&base_dir);
        merge_daemon_config_layer(&mut config, layer, presence);
        loaded_path = Some(path.clone());
        loaded_base_dir = base_dir;
    }

    config.validate()?;
    Ok(LoadedVerletDaemonConfig {
        config,
        path: loaded_path,
        base_dir: loaded_base_dir,
    })
}

pub fn default_verlet_daemon_socket_path() -> std::path::PathBuf {
    default_daemon_socket_path_from_env(|key| verlet_runtime_contracts::env_compat::var_os(key))
}

pub fn discover_verlet_daemon_config_path()
-> crate::kernel::runtime_host::VerletResult<Option<std::path::PathBuf>> {
    let cwd = std::env::current_dir().map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to read current working directory: {err}"
        ))
    })?;
    discover_verlet_project(&cwd).map(|project| project.config_path)
}

pub fn discover_verlet_project(
    start: &std::path::Path,
) -> crate::kernel::runtime_host::VerletResult<VerletProjectDiscovery> {
    discover_verlet_project_with_warning(start, |warning| eprintln!("{warning}"))
}

fn discover_verlet_project_with_warning(
    start: &std::path::Path,
    mut warn: impl FnMut(&str),
) -> crate::kernel::runtime_host::VerletResult<VerletProjectDiscovery> {
    let mut start = if start.is_absolute() {
        start.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "failed to read current working directory: {err}"
                ))
            })?
            .join(start)
    };
    if start.is_file() {
        start = start
            .parent()
            .unwrap_or_else(|| std::path::Path::new("/"))
            .to_path_buf();
    }

    for dir in start.ancestors() {
        let candidate = dir.join("verlet.toml");
        if candidate.is_file() {
            return Ok(VerletProjectDiscovery {
                root: dir.to_path_buf(),
                config_path: Some(candidate),
            });
        }
        let candidate = dir.join(concat!("cool", "dis.toml"));
        if candidate.is_file() {
            let warning = format!(
                "warning: {} is deprecated; use {} (compatibility will be removed in v0.4.0)",
                candidate.display(),
                dir.join("verlet.toml").display()
            );
            warn(&warning);
            return Ok(VerletProjectDiscovery {
                root: dir.to_path_buf(),
                config_path: Some(candidate),
            });
        }
    }

    for dir in start.ancestors() {
        if dir.join(".verlet").is_dir() {
            return Ok(VerletProjectDiscovery {
                root: dir.to_path_buf(),
                config_path: None,
            });
        }
        let legacy = dir.join(concat!(".", "cool", "dis"));
        if legacy.is_dir() {
            let warning = format!(
                "warning: {} is deprecated; keep using it for v0.3.0 or create {} for new state",
                legacy.display(),
                dir.join(".verlet").display()
            );
            warn(&warning);
            return Ok(VerletProjectDiscovery {
                root: dir.to_path_buf(),
                config_path: None,
            });
        }
    }

    Ok(VerletProjectDiscovery {
        root: start,
        config_path: None,
    })
}

#[derive(Default)]
struct DaemonConfigPresence {
    identity: bool,
    runtime: RuntimePresence,
    app_server: AppServerPresence,
    registries: RegistriesPresence,
    operations: OperationsPresence,
    provider: ProviderPresence,
    io: bool,
    sync: SyncPresence,
}

#[derive(Default)]
struct SyncPresence {
    listen: bool,
    lease_ttl_secs: bool,
}

#[derive(Default)]
struct RuntimePresence {
    cwd: bool,
    runtime_home: bool,
    state_home: bool,
    placement: bool,
    workspace: bool,
}

#[derive(Default)]
struct AppServerPresence {
    listen: bool,
}

#[derive(Default)]
struct RegistriesPresence {
    operations: bool,
    agents: bool,
}

#[derive(Default)]
struct OperationsPresence {
    global_operation_names: bool,
    load_all_active_when_unbound: bool,
}

#[derive(Default)]
struct ProviderPresence {
    provider: bool,
    base_url: bool,
    api_key: bool,
    api_key_env: bool,
    region: bool,
    aws_access_key_id: bool,
    aws_secret_access_key: bool,
    aws_session_token: bool,
    model: bool,
    max_tokens: bool,
    stream: bool,
    env_file: bool,
}

fn daemon_config_presence(
    text: &str,
) -> crate::kernel::runtime_host::VerletResult<DaemonConfigPresence> {
    let root: toml::Table = toml::from_str(text).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to parse Verlet daemon config: {err}"
        ))
    })?;
    let table = root
        .get("daemon")
        .and_then(toml::Value::as_table)
        .unwrap_or(&root);

    Ok(DaemonConfigPresence {
        identity: table.contains_key("identity"),
        runtime: RuntimePresence {
            cwd: section_has_key(table, "runtime", "cwd"),
            runtime_home: section_has_key(table, "runtime", "runtime_home"),
            state_home: section_has_key(table, "runtime", "state_home"),
            placement: section_has_key(table, "runtime", "placement"),
            workspace: section_has_key(table, "runtime", "workspace"),
        },
        app_server: AppServerPresence {
            listen: section_has_key(table, "app_server", "listen"),
        },
        registries: RegistriesPresence {
            operations: section_has_key(table, "registries", "operations"),
            agents: section_has_key(table, "registries", "agents"),
        },
        operations: OperationsPresence {
            global_operation_names: section_has_key(table, "operations", "global_operation_names"),
            load_all_active_when_unbound: section_has_key(
                table,
                "operations",
                "load_all_active_when_unbound",
            ),
        },
        provider: ProviderPresence {
            provider: section_has_key(table, "provider", "provider"),
            base_url: section_has_key(table, "provider", "base_url"),
            api_key: section_has_key(table, "provider", "api_key"),
            api_key_env: section_has_key(table, "provider", "api_key_env"),
            region: section_has_key(table, "provider", "region"),
            aws_access_key_id: section_has_key(table, "provider", "aws_access_key_id"),
            aws_secret_access_key: section_has_key(table, "provider", "aws_secret_access_key"),
            aws_session_token: section_has_key(table, "provider", "aws_session_token"),
            model: section_has_key(table, "provider", "model"),
            max_tokens: section_has_key(table, "provider", "max_tokens"),
            stream: section_has_key(table, "provider", "stream"),
            env_file: section_has_key(table, "provider", "env_file"),
        },
        io: table.contains_key("io"),
        sync: SyncPresence {
            listen: section_has_key(table, "sync", "listen"),
            lease_ttl_secs: section_has_key(table, "sync", "lease_ttl_secs"),
        },
    })
}

fn section_has_key(table: &toml::Table, section: &str, key: &str) -> bool {
    table
        .get(section)
        .and_then(toml::Value::as_table)
        .is_some_and(|section| section.contains_key(key))
}

fn merge_daemon_config_layer(
    config: &mut VerletDaemonConfig,
    mut layer: VerletDaemonConfig,
    presence: DaemonConfigPresence,
) {
    if presence.identity {
        config.identity = layer.identity;
        if config.identity.mode == crate::daemon::identity::IdentityMode::Local {
            let defaults = synthesized_local_daemon_identity_config();
            if config.identity.tenant_id.is_none() {
                config.identity.tenant_id = defaults.tenant_id;
            }
            if config.identity.console_principal.is_none() {
                config.identity.console_principal = defaults.console_principal;
            }
        }
    }
    if presence.runtime.cwd {
        config.runtime.cwd = layer.runtime.cwd.take();
    }
    if presence.runtime.runtime_home {
        config.runtime.runtime_home = layer.runtime.runtime_home.take();
    }
    if presence.runtime.state_home {
        config.runtime.state_home = layer.runtime.state_home.take();
    }
    if presence.runtime.placement {
        config.runtime.placement = layer.runtime.placement.take();
    }
    if presence.runtime.workspace {
        config.runtime.workspace = layer.runtime.workspace.take();
    }
    if presence.app_server.listen {
        config.app_server.listen = layer.app_server.listen;
    }
    if presence.registries.operations {
        config.registries.operations = layer.registries.operations.take();
    }
    if presence.registries.agents {
        config.registries.agents = layer.registries.agents.take();
    }
    if presence.operations.global_operation_names {
        config.operations.global_operation_names = layer.operations.global_operation_names;
    }
    if presence.operations.load_all_active_when_unbound {
        config.operations.load_all_active_when_unbound =
            layer.operations.load_all_active_when_unbound;
    }
    if presence.provider.provider {
        config.provider.provider = layer.provider.provider.take();
    }
    if presence.provider.base_url {
        config.provider.base_url = layer.provider.base_url.take();
    }
    if presence.provider.api_key {
        config.provider.api_key = layer.provider.api_key.take();
    }
    if presence.provider.api_key_env {
        config.provider.api_key_env = layer.provider.api_key_env.take();
    }
    if presence.provider.region {
        config.provider.region = layer.provider.region.take();
    }
    if presence.provider.aws_access_key_id {
        config.provider.aws_access_key_id = layer.provider.aws_access_key_id.take();
    }
    if presence.provider.aws_secret_access_key {
        config.provider.aws_secret_access_key = layer.provider.aws_secret_access_key.take();
    }
    if presence.provider.aws_session_token {
        config.provider.aws_session_token = layer.provider.aws_session_token.take();
    }
    if presence.provider.model {
        config.provider.model = layer.provider.model.take();
    }
    if presence.provider.max_tokens {
        config.provider.max_tokens = layer.provider.max_tokens.take();
    }
    if presence.provider.stream {
        config.provider.stream = layer.provider.stream;
    }
    if presence.provider.env_file {
        config.provider.env_file = layer.provider.env_file.take();
    }
    if presence.io {
        config.io = layer.io;
    }
    if presence.sync.listen {
        config.sync.listen = layer.sync.listen.take();
    }
    if presence.sync.lease_ttl_secs {
        config.sync.lease_ttl_secs = layer.sync.lease_ttl_secs;
    }
}

pub fn render_verlet_daemon_service(
    target: VerletDaemonServiceTarget,
    spec: &VerletDaemonServiceSpec,
) -> String {
    match target {
        VerletDaemonServiceTarget::Launchd => render_launchd_service(spec),
        VerletDaemonServiceTarget::Systemd => render_systemd_service(spec),
    }
}

pub fn verlet_daemon_service_file_name(
    target: VerletDaemonServiceTarget,
    label: &str,
) -> crate::kernel::runtime_host::VerletResult<String> {
    validate_service_label(label)?;
    Ok(match target {
        VerletDaemonServiceTarget::Launchd => format!("{label}.plist"),
        VerletDaemonServiceTarget::Systemd => format!("{label}.service"),
    })
}

pub fn verlet_daemon_service_install_path(
    target: VerletDaemonServiceTarget,
    label: &str,
) -> crate::kernel::runtime_host::VerletResult<std::path::PathBuf> {
    let home = verlet_runtime_contracts::env_compat::var_os("HOME")
        .map(std::path::PathBuf::from)
        .ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory("HOME is not set".to_string())
        })?;
    verlet_daemon_service_install_path_for_home(target, label, &home)
}

pub fn verlet_daemon_service_install_path_for_home(
    target: VerletDaemonServiceTarget,
    label: &str,
    home: &std::path::Path,
) -> crate::kernel::runtime_host::VerletResult<std::path::PathBuf> {
    let file_name = verlet_daemon_service_file_name(target, label)?;
    let dir = match target {
        VerletDaemonServiceTarget::Launchd => home.join("Library/LaunchAgents"),
        VerletDaemonServiceTarget::Systemd => {
            verlet_runtime_contracts::env_compat::var_os("XDG_CONFIG_HOME")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| home.join(".config"))
                .join("systemd/user")
        }
    };
    Ok(dir.join(file_name))
}

pub fn install_verlet_daemon_service(
    target: VerletDaemonServiceTarget,
    spec: &VerletDaemonServiceSpec,
) -> crate::kernel::runtime_host::VerletResult<std::path::PathBuf> {
    let path = verlet_daemon_service_install_path(target, &spec.label)?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to create service directory {}: {err}",
                parent.display()
            ))
        })?;
    }
    std::fs::write(&path, render_verlet_daemon_service(target, spec)).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to write service file {}: {err}",
            path.display()
        ))
    })?;
    Ok(path)
}

pub fn uninstall_verlet_daemon_service(
    target: VerletDaemonServiceTarget,
    label: &str,
) -> crate::kernel::runtime_host::VerletResult<Option<std::path::PathBuf>> {
    let path = verlet_daemon_service_install_path(target, label)?;
    if !path.exists() {
        return Ok(None);
    }
    std::fs::remove_file(&path).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to remove service file {}: {err}",
            path.display()
        ))
    })?;
    Ok(Some(path))
}

fn decode_daemon_config(
    text: &str,
) -> crate::kernel::runtime_host::VerletResult<VerletDaemonConfig> {
    #[derive(serde::Deserialize)]
    struct RootConfig {
        daemon: Option<VerletDaemonConfig>,
    }

    let root = decode_config::<RootConfig>(text)?;
    if let Some(daemon) = root.daemon {
        return Ok(daemon);
    }

    decode_config::<VerletDaemonConfig>(text)
}

fn decode_config<T: serde::de::DeserializeOwned>(
    text: &str,
) -> crate::kernel::runtime_host::VerletResult<T> {
    toml::from_str(text).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "invalid TOML config: {err}"
        ))
    })
}

fn read_config_text(path: &std::path::Path) -> crate::kernel::runtime_host::VerletResult<String> {
    std::fs::read_to_string(path).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to read Verlet config {}: {err}",
            path.display()
        ))
    })
}

fn validate_config_extension(
    path: Option<&std::path::Path>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    match path
        .and_then(std::path::Path::extension)
        .and_then(|extension| extension.to_str())
    {
        Some("toml") | None => Ok(()),
        Some(other) => Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!("unsupported Verlet config extension {other:?}; expected .toml"),
        )),
    }
}

fn validate_persistence(
    scope: &str,
    persistence: &verlet_io_core::IngressPersistenceConfig,
    errors: &mut Vec<String>,
) {
    if persistence.visibility_timeout_secs == 0 {
        errors.push(format!(
            "{scope}.persistence.visibility_timeout_secs must be greater than zero"
        ));
    }
    if persistence.mode == verlet_io_core::IngressPersistenceMode::DurableQueue {
        if persistence
            .queue_name
            .as_deref()
            .is_some_and(|name| name.trim().is_empty())
        {
            errors.push(format!("{scope}.persistence.queue_name cannot be empty"));
        }
    }
}

fn resolve_optional_secret(
    label: &str,
    literal: &Option<String>,
    env_name: &Option<String>,
) -> crate::kernel::runtime_host::VerletResult<Option<String>> {
    if let Some(value) = literal {
        return Ok(Some(value.clone()));
    }
    let Some(env_name) = env_name else {
        return Ok(None);
    };
    verlet_runtime_contracts::env_compat::var(env_name)
        .map(Some)
        .map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to read {label} from env {env_name}: {err}"
            ))
        })
}

fn render_launchd_service(spec: &VerletDaemonServiceSpec) -> String {
    let working_directory = spec.working_directory.as_ref().map(|path| {
        format!(
            "    <key>WorkingDirectory</key>\n    <string>{}</string>\n",
            xml_escape(&path.display().to_string())
        )
    });

    format!(
        concat!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
            "<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" ",
            "\"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n",
            "<plist version=\"1.0\">\n",
            "<dict>\n",
            "    <key>Label</key>\n",
            "    <string>{label}</string>\n",
            "    <key>ProgramArguments</key>\n",
            "    <array>\n",
            "        <string>{exe}</string>\n",
            "        <string>daemon</string>\n",
            "        <string>run</string>\n",
            "        <string>--config</string>\n",
            "        <string>{config}</string>\n",
            "    </array>\n",
            "{working_directory}",
            "    <key>RunAtLoad</key>\n",
            "    <true/>\n",
            "    <key>KeepAlive</key>\n",
            "    <true/>\n",
            "</dict>\n",
            "</plist>\n",
        ),
        label = xml_escape(&spec.label),
        exe = xml_escape(&spec.executable.display().to_string()),
        config = xml_escape(&spec.config_path.display().to_string()),
        working_directory = working_directory.unwrap_or_default(),
    )
}

fn render_systemd_service(spec: &VerletDaemonServiceSpec) -> String {
    let working_directory = spec
        .working_directory
        .as_ref()
        .map(|path| {
            format!(
                "WorkingDirectory={}\n",
                quote_systemd(&path.display().to_string())
            )
        })
        .unwrap_or_default();

    format!(
        "[Unit]\n\
Description=Verlet daemon\n\
After=network-online.target\n\
\n\
[Service]\n\
Type=simple\n\
ExecStart={} daemon run --config {}\n\
{}Restart=always\n\
RestartSec=2\n\
\n\
[Install]\n\
WantedBy=default.target\n",
        quote_systemd(&spec.executable.display().to_string()),
        quote_systemd(&spec.config_path.display().to_string()),
        working_directory,
    )
}

fn default_app_server_listen() -> String {
    unix_listen_url(default_verlet_daemon_socket_path())
}

fn default_true() -> bool {
    true
}

fn default_egress_max_attempts() -> u32 {
    5
}

fn default_egress_base_backoff_ms() -> u64 {
    500
}

fn default_telegram_webhook_path() -> String {
    DEFAULT_TELEGRAM_WEBHOOK_PATH.to_string()
}

fn default_sqlite_queue_path() -> std::path::PathBuf {
    default_sqlite_queue_path_for_base(std::path::Path::new("."))
}

fn default_sqlite_queue_path_for_base(base: &std::path::Path) -> std::path::PathBuf {
    let canonical = resolve_config_path(base, std::path::PathBuf::from(DEFAULT_SQLITE_QUEUE_PATH));
    let legacy_root = base.join(concat!(".", "cool", "dis"));
    if base.join(".verlet").exists() || !legacy_root.exists() {
        canonical
    } else {
        let legacy = legacy_root.join("queue/ingress.sqlite");
        eprintln!(
            "warning: {} is deprecated; existing queue state will continue to be used in place through v0.3.0",
            legacy.display()
        );
        legacy
    }
}

fn unix_listen_url(path: impl AsRef<std::path::Path>) -> String {
    format!("unix://{}", path.as_ref().display())
}

fn default_daemon_socket_path_from_env(
    get_env: impl Fn(&str) -> Option<std::ffi::OsString>,
) -> std::path::PathBuf {
    if let Some(path) =
        verlet_runtime_contracts::env_compat::var_os_with("VERLET_DAEMON_SOCKET", |name| {
            get_env(name)
        })
        .filter(|value| !value.is_empty())
        .map(std::path::PathBuf::from)
    {
        return path;
    }

    if let Some(dir) = get_env("XDG_RUNTIME_DIR")
        .filter(|value| !value.is_empty())
        .map(std::path::PathBuf::from)
    {
        return existing_daemon_socket_path(
            dir.join("verlet/verlet.sock"),
            dir.join(concat!("cool", "dis/cool", "dis.sock")),
        );
    }

    if cfg!(target_os = "macos")
        && let Some(home) = get_env("HOME")
            .filter(|value| !value.is_empty())
            .map(std::path::PathBuf::from)
    {
        return existing_daemon_socket_path(
            home.join("Library/Application Support/verlet/run/verlet.sock"),
            home.join(concat!(
                "Library/Application Support/",
                "cool",
                "dis/run/cool",
                "dis.sock"
            )),
        );
    }

    if let Some(dir) = get_env("XDG_STATE_HOME")
        .filter(|value| !value.is_empty())
        .map(std::path::PathBuf::from)
    {
        return existing_daemon_socket_path(
            dir.join("verlet/run/verlet.sock"),
            dir.join(concat!("cool", "dis/run/cool", "dis.sock")),
        );
    }

    if let Some(home) = get_env("HOME")
        .filter(|value| !value.is_empty())
        .map(std::path::PathBuf::from)
    {
        return existing_daemon_socket_path(
            home.join(".local/state/verlet/run/verlet.sock"),
            home.join(concat!(".local/state/", "cool", "dis/run/cool", "dis.sock")),
        );
    }

    let user = get_env("USER")
        .and_then(|value| value.into_string().ok())
        .filter(|value| !value.trim().is_empty())
        .map(|value| sanitize_socket_path_component(&value))
        .unwrap_or_else(|| "user".to_string());
    let temp_dir = if cfg!(unix) {
        std::path::PathBuf::from("/tmp")
    } else {
        std::env::temp_dir()
    };
    existing_daemon_socket_path(
        temp_dir.join(format!("verlet-{user}/verlet.sock")),
        temp_dir.join(format!(
            "{}-{user}/{}.sock",
            concat!("cool", "dis"),
            concat!("cool", "dis")
        )),
    )
}

fn existing_daemon_socket_path(
    canonical: std::path::PathBuf,
    legacy: std::path::PathBuf,
) -> std::path::PathBuf {
    let canonical_root_exists = canonical.parent().is_some_and(std::path::Path::exists);
    let legacy_root_exists = legacy.parent().is_some_and(std::path::Path::exists);
    if canonical_root_exists || !legacy_root_exists {
        canonical
    } else {
        eprintln!(
            "warning: {} is deprecated; existing daemon state will continue to be used in place through v0.3.0",
            legacy.display()
        );
        legacy
    }
}

fn sanitize_socket_path_component(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' => byte as char,
            _ => '_',
        })
        .collect()
}

fn resolve_config_path(base: &std::path::Path, path: std::path::PathBuf) -> std::path::PathBuf {
    if path.is_absolute() {
        path
    } else {
        base.join(path)
    }
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn quote_systemd(value: &str) -> String {
    if value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || b"/._:-".contains(&byte))
    {
        return value.to_string();
    }

    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

fn validate_service_label(label: &str) -> crate::kernel::runtime_host::VerletResult<()> {
    if label.trim().is_empty() {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            "daemon service label cannot be empty".to_string(),
        ));
    }
    if !label
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "daemon service label {label:?} may only contain ASCII letters, numbers, '.', '_', and '-'"
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
