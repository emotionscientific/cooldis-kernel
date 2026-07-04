use crate::{AppServerListenAddr, CooldisError, CooldisResult};
use cooldis_io_core::{IngressPersistenceConfig, IngressPersistenceMode};
use regex::Regex;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};

const DEFAULT_SQLITE_QUEUE_PATH: &str = ".cooldis/queue/ingress.sqlite";
const DEFAULT_SERVICE_LABEL: &str = "com.cooldis.daemon";
const DEFAULT_TELEGRAM_WEBHOOK_PATH: &str = "/telegram";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedCooldisDaemonConfig {
    pub config: CooldisDaemonConfig,
    pub path: Option<PathBuf>,
    pub base_dir: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CooldisProjectDiscovery {
    pub root: PathBuf,
    pub config_path: Option<PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CooldisDaemonConfig {
    #[serde(default)]
    pub runtime: CooldisRuntimeConfig,
    #[serde(default)]
    pub app_server: CooldisDaemonAppServerConfig,
    #[serde(default)]
    pub registries: CooldisDaemonRegistriesConfig,
    #[serde(default)]
    pub operations: CooldisDaemonOperationsConfig,
    #[serde(default)]
    pub provider: CooldisProviderConfig,
    #[serde(default)]
    pub io: CooldisIoConfig,
}

impl Default for CooldisDaemonConfig {
    fn default() -> Self {
        Self {
            runtime: CooldisRuntimeConfig::default(),
            app_server: CooldisDaemonAppServerConfig::default(),
            registries: CooldisDaemonRegistriesConfig::default(),
            operations: CooldisDaemonOperationsConfig::default(),
            provider: CooldisProviderConfig::default(),
            io: CooldisIoConfig::default(),
        }
    }
}

impl CooldisDaemonConfig {
    pub fn validate(&self) -> CooldisResult<()> {
        let errors = self.validation_errors();
        if errors.is_empty() {
            return Ok(());
        }

        Err(CooldisError::RuntimeFactory(format!(
            "invalid Cooldis daemon config:\n- {}",
            errors.join("\n- ")
        )))
    }

    pub fn validation_errors(&self) -> Vec<String> {
        let mut errors = Vec::new();

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
        errors
    }

    fn resolve_paths(&mut self, base: &Path) {
        self.app_server.resolve_paths(base);
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
        if let Some(path) = self.provider.env_file.take() {
            self.provider.env_file = Some(resolve_config_path(base, path));
        }
        self.io.resolve_paths(base);
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CooldisRuntimeConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_home: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_home: Option<PathBuf>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CooldisDaemonRegistriesConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operations: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agents: Option<PathBuf>,
}

impl CooldisDaemonRegistriesConfig {
    fn resolve_paths(&mut self, base: &Path) {
        if let Some(path) = self.operations.take() {
            self.operations = Some(resolve_config_path(base, path));
        }
        if let Some(path) = self.agents.take() {
            self.agents = Some(resolve_config_path(base, path));
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CooldisDaemonOperationsConfig {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub global_operation_names: Vec<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub load_all_active_when_unbound: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CooldisDaemonAppServerConfig {
    #[serde(default = "default_app_server_listen")]
    pub listen: String,
}

impl Default for CooldisDaemonAppServerConfig {
    fn default() -> Self {
        Self {
            listen: default_app_server_listen(),
        }
    }
}

impl CooldisDaemonAppServerConfig {
    pub fn listen_addr(&self) -> CooldisResult<AppServerListenAddr> {
        AppServerListenAddr::parse(&self.listen)
    }

    fn resolve_paths(&mut self, base: &Path) {
        let Ok(AppServerListenAddr::Unix(path)) = self.listen_addr() else {
            return;
        };
        if path.is_relative() {
            self.listen = unix_listen_url(resolve_config_path(base, path));
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CooldisProviderConfig {
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
    pub env_file: Option<PathBuf>,
}

impl CooldisProviderConfig {
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CooldisIoConfig {
    #[serde(default)]
    pub ingress: CooldisIngressConfig,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub routes: Vec<CooldisIoRouteConfig>,
}

impl Default for CooldisIoConfig {
    fn default() -> Self {
        Self {
            ingress: CooldisIngressConfig::default(),
            routes: Vec::new(),
        }
    }
}

impl CooldisIoConfig {
    fn validate(&self, errors: &mut Vec<String>) {
        self.ingress.validate("io.ingress", errors);

        let mut route_ids = BTreeSet::new();
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

    fn resolve_paths(&mut self, base: &Path) {
        self.ingress.resolve_paths(base);
        for route in &mut self.routes {
            route.resolve_paths(base);
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CooldisIngressConfig {
    #[serde(default)]
    pub persistence: IngressPersistenceConfig,
    #[serde(default)]
    pub queue: CooldisQueueConfig,
}

impl Default for CooldisIngressConfig {
    fn default() -> Self {
        Self {
            persistence: IngressPersistenceConfig::default(),
            queue: CooldisQueueConfig::default(),
        }
    }
}

impl CooldisIngressConfig {
    fn validate(&self, scope: &str, errors: &mut Vec<String>) {
        validate_persistence(scope, &self.persistence, errors);
        self.queue.validate(scope, errors);
    }

    fn resolve_paths(&mut self, base: &Path) {
        self.queue.resolve_paths(base);
        if self.queue.dsn.is_none() && self.queue.sqlite_path.is_none() {
            self.queue.sqlite_path = Some(resolve_config_path(base, default_sqlite_queue_path()));
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CooldisQueueConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dsn: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sqlite_path: Option<PathBuf>,
}

impl Default for CooldisQueueConfig {
    fn default() -> Self {
        Self {
            dsn: None,
            sqlite_path: None,
        }
    }
}

impl CooldisQueueConfig {
    fn validate(&self, scope: &str, errors: &mut Vec<String>) {
        if self.dsn.is_some() && self.sqlite_path.is_some() {
            errors.push(format!("{scope}.queue cannot set both dsn and sqlite_path"));
        }
        if self.dsn.as_ref().is_some_and(|dsn| dsn.trim().is_empty()) {
            errors.push(format!("{scope}.queue.dsn cannot be empty"));
        }
    }

    fn resolve_paths(&mut self, base: &Path) {
        if let Some(path) = self.sqlite_path.take() {
            self.sqlite_path = Some(resolve_config_path(base, path));
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CooldisIoRouteConfig {
    pub id: String,
    pub kind: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub threading: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingress: Option<CooldisIngressConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub egress_projection: Vec<CooldisEgressProjectionRuleConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typing_simulation: Option<CooldisTypingSimulationConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telegram: Option<CooldisTelegramRouteConfig>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

impl CooldisIoRouteConfig {
    fn validate(&self, errors: &mut Vec<String>) {
        if self.id.trim().is_empty() {
            errors.push("io.routes id cannot be empty".to_string());
        }
        if self.kind.trim().is_empty() {
            errors.push(format!("io.routes {:?} kind cannot be empty", self.id));
        }
        if let Some(ingress) = &self.ingress {
            ingress.validate(&format!("io.routes.{}", self.id), errors);
        }
        for (index, rule) in self.egress_projection.iter().enumerate() {
            let scope = format!("io.routes.{}.egress_projection[{index}]", self.id);
            if rule.pattern.trim().is_empty() {
                errors.push(format!("{scope}.pattern cannot be empty"));
            } else if let Err(err) = Regex::new(&rule.pattern) {
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
        if self.kind == "telegram.bot" {
            match &self.telegram {
                Some(telegram) => {
                    telegram.validate(&format!("io.routes.{}.telegram", self.id), errors)
                }
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

    fn resolve_paths(&mut self, base: &Path) {
        if let Some(ingress) = &mut self.ingress {
            ingress.resolve_paths(base);
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CooldisEgressProjectionRuleConfig {
    pub pattern: String,
    pub action: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CooldisTypingSimulationConfig {
    pub chars_per_second: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CooldisTelegramRouteConfig {
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

impl Default for CooldisTelegramRouteConfig {
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

impl CooldisTelegramRouteConfig {
    fn validate(&self, scope: &str, errors: &mut Vec<String>) {
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

    pub fn secret_token_value(&self) -> CooldisResult<Option<String>> {
        resolve_optional_secret(
            "telegram secret_token",
            &self.secret_token,
            &self.secret_token_env,
        )
    }

    pub fn bot_token_value(&self) -> CooldisResult<Option<String>> {
        resolve_optional_secret("telegram bot_token", &self.bot_token, &self.bot_token_env)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CooldisDaemonServiceTarget {
    Launchd,
    Systemd,
}

impl CooldisDaemonServiceTarget {
    pub fn parse(value: &str) -> CooldisResult<Self> {
        match value {
            "launchd" | "macos" | "darwin" => Ok(Self::Launchd),
            "systemd" | "linux" => Ok(Self::Systemd),
            other => Err(CooldisError::RuntimeFactory(format!(
                "unknown daemon service target {other:?}; expected launchd or systemd"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CooldisDaemonServiceSpec {
    pub label: String,
    pub executable: PathBuf,
    pub config_path: PathBuf,
    pub working_directory: Option<PathBuf>,
}

impl CooldisDaemonServiceSpec {
    pub fn new(executable: PathBuf, config_path: PathBuf) -> Self {
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

    pub fn with_working_directory(mut self, path: impl Into<PathBuf>) -> Self {
        self.working_directory = Some(path.into());
        self
    }
}

pub fn load_cooldis_daemon_config(path: Option<&Path>) -> CooldisResult<LoadedCooldisDaemonConfig> {
    match path {
        Some(path) => load_cooldis_daemon_config_layers(
            &[path.to_path_buf()],
            path.parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf(),
        ),
        None => {
            let cwd = std::env::current_dir().map_err(|err| {
                CooldisError::RuntimeFactory(format!(
                    "failed to read current working directory: {err}"
                ))
            })?;
            let project = discover_cooldis_project(&cwd)?;
            match project.config_path {
                Some(path) => load_cooldis_daemon_config_layers(&[path], project.root),
                None => Ok(LoadedCooldisDaemonConfig {
                    config: CooldisDaemonConfig::default(),
                    path: None,
                    base_dir: project.root,
                }),
            }
        }
    }
}

pub fn load_cooldis_daemon_config_layers(
    paths: &[PathBuf],
    fallback_base_dir: PathBuf,
) -> CooldisResult<LoadedCooldisDaemonConfig> {
    let mut config = CooldisDaemonConfig::default();
    let mut loaded_path = None;
    let mut loaded_base_dir = fallback_base_dir;

    for path in paths {
        validate_config_extension(Some(path.as_path()))?;
        let text = read_config_text(path)?;
        let base_dir = path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        let presence = daemon_config_presence(&text)?;
        let mut layer = decode_daemon_config(&text)?;
        layer.resolve_paths(&base_dir);
        merge_daemon_config_layer(&mut config, layer, presence);
        loaded_path = Some(path.clone());
        loaded_base_dir = base_dir;
    }

    config.validate()?;
    Ok(LoadedCooldisDaemonConfig {
        config,
        path: loaded_path,
        base_dir: loaded_base_dir,
    })
}

pub fn default_cooldis_daemon_socket_path() -> PathBuf {
    default_daemon_socket_path_from_env(|key| std::env::var_os(key))
}

pub fn discover_cooldis_daemon_config_path() -> CooldisResult<Option<PathBuf>> {
    let cwd = std::env::current_dir().map_err(|err| {
        CooldisError::RuntimeFactory(format!("failed to read current working directory: {err}"))
    })?;
    discover_cooldis_project(&cwd).map(|project| project.config_path)
}

pub fn discover_cooldis_project(start: &Path) -> CooldisResult<CooldisProjectDiscovery> {
    let mut start = if start.is_absolute() {
        start.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|err| {
                CooldisError::RuntimeFactory(format!(
                    "failed to read current working directory: {err}"
                ))
            })?
            .join(start)
    };
    if start.is_file() {
        start = start
            .parent()
            .unwrap_or_else(|| Path::new("/"))
            .to_path_buf();
    }

    for dir in start.ancestors() {
        let candidate = dir.join("cooldis.toml");
        if candidate.is_file() {
            return Ok(CooldisProjectDiscovery {
                root: dir.to_path_buf(),
                config_path: Some(candidate),
            });
        }
    }

    for dir in start.ancestors() {
        if dir.join(".cooldis").is_dir() {
            return Ok(CooldisProjectDiscovery {
                root: dir.to_path_buf(),
                config_path: None,
            });
        }
    }

    Ok(CooldisProjectDiscovery {
        root: start,
        config_path: None,
    })
}

#[derive(Default)]
struct DaemonConfigPresence {
    runtime: RuntimePresence,
    app_server: AppServerPresence,
    registries: RegistriesPresence,
    operations: OperationsPresence,
    provider: ProviderPresence,
    io: bool,
}

#[derive(Default)]
struct RuntimePresence {
    cwd: bool,
    runtime_home: bool,
    state_home: bool,
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

fn daemon_config_presence(text: &str) -> CooldisResult<DaemonConfigPresence> {
    let root: toml::Table = toml::from_str(text).map_err(|err| {
        CooldisError::RuntimeFactory(format!("failed to parse Cooldis daemon config: {err}"))
    })?;
    let table = root
        .get("daemon")
        .and_then(toml::Value::as_table)
        .unwrap_or(&root);

    Ok(DaemonConfigPresence {
        runtime: RuntimePresence {
            cwd: section_has_key(table, "runtime", "cwd"),
            runtime_home: section_has_key(table, "runtime", "runtime_home"),
            state_home: section_has_key(table, "runtime", "state_home"),
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
    })
}

fn section_has_key(table: &toml::Table, section: &str, key: &str) -> bool {
    table
        .get(section)
        .and_then(toml::Value::as_table)
        .is_some_and(|section| section.contains_key(key))
}

fn merge_daemon_config_layer(
    config: &mut CooldisDaemonConfig,
    mut layer: CooldisDaemonConfig,
    presence: DaemonConfigPresence,
) {
    if presence.runtime.cwd {
        config.runtime.cwd = layer.runtime.cwd.take();
    }
    if presence.runtime.runtime_home {
        config.runtime.runtime_home = layer.runtime.runtime_home.take();
    }
    if presence.runtime.state_home {
        config.runtime.state_home = layer.runtime.state_home.take();
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
}

pub fn render_cooldis_daemon_service(
    target: CooldisDaemonServiceTarget,
    spec: &CooldisDaemonServiceSpec,
) -> String {
    match target {
        CooldisDaemonServiceTarget::Launchd => render_launchd_service(spec),
        CooldisDaemonServiceTarget::Systemd => render_systemd_service(spec),
    }
}

pub fn cooldis_daemon_service_file_name(
    target: CooldisDaemonServiceTarget,
    label: &str,
) -> CooldisResult<String> {
    validate_service_label(label)?;
    Ok(match target {
        CooldisDaemonServiceTarget::Launchd => format!("{label}.plist"),
        CooldisDaemonServiceTarget::Systemd => format!("{label}.service"),
    })
}

pub fn cooldis_daemon_service_install_path(
    target: CooldisDaemonServiceTarget,
    label: &str,
) -> CooldisResult<PathBuf> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| CooldisError::RuntimeFactory("HOME is not set".to_string()))?;
    cooldis_daemon_service_install_path_for_home(target, label, &home)
}

pub fn cooldis_daemon_service_install_path_for_home(
    target: CooldisDaemonServiceTarget,
    label: &str,
    home: &Path,
) -> CooldisResult<PathBuf> {
    let file_name = cooldis_daemon_service_file_name(target, label)?;
    let dir = match target {
        CooldisDaemonServiceTarget::Launchd => home.join("Library/LaunchAgents"),
        CooldisDaemonServiceTarget::Systemd => std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".config"))
            .join("systemd/user"),
    };
    Ok(dir.join(file_name))
}

pub fn install_cooldis_daemon_service(
    target: CooldisDaemonServiceTarget,
    spec: &CooldisDaemonServiceSpec,
) -> CooldisResult<PathBuf> {
    let path = cooldis_daemon_service_install_path(target, &spec.label)?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "failed to create service directory {}: {err}",
                parent.display()
            ))
        })?;
    }
    std::fs::write(&path, render_cooldis_daemon_service(target, spec)).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "failed to write service file {}: {err}",
            path.display()
        ))
    })?;
    Ok(path)
}

pub fn uninstall_cooldis_daemon_service(
    target: CooldisDaemonServiceTarget,
    label: &str,
) -> CooldisResult<Option<PathBuf>> {
    let path = cooldis_daemon_service_install_path(target, label)?;
    if !path.exists() {
        return Ok(None);
    }
    std::fs::remove_file(&path).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "failed to remove service file {}: {err}",
            path.display()
        ))
    })?;
    Ok(Some(path))
}

fn decode_daemon_config(text: &str) -> CooldisResult<CooldisDaemonConfig> {
    #[derive(Deserialize)]
    struct RootConfig {
        daemon: Option<CooldisDaemonConfig>,
    }

    let root = decode_config::<RootConfig>(text)?;
    if let Some(daemon) = root.daemon {
        return Ok(daemon);
    }

    decode_config::<CooldisDaemonConfig>(text)
}

fn decode_config<T: DeserializeOwned>(text: &str) -> CooldisResult<T> {
    toml::from_str(text)
        .map_err(|err| CooldisError::RuntimeFactory(format!("invalid TOML config: {err}")))
}

fn read_config_text(path: &Path) -> CooldisResult<String> {
    std::fs::read_to_string(path).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "failed to read Cooldis config {}: {err}",
            path.display()
        ))
    })
}

fn validate_config_extension(path: Option<&Path>) -> CooldisResult<()> {
    match path
        .and_then(Path::extension)
        .and_then(|extension| extension.to_str())
    {
        Some("toml") | None => Ok(()),
        Some(other) => Err(CooldisError::RuntimeFactory(format!(
            "unsupported Cooldis config extension {other:?}; expected .toml"
        ))),
    }
}

fn validate_persistence(
    scope: &str,
    persistence: &IngressPersistenceConfig,
    errors: &mut Vec<String>,
) {
    if persistence.visibility_timeout_secs == 0 {
        errors.push(format!(
            "{scope}.persistence.visibility_timeout_secs must be greater than zero"
        ));
    }
    if persistence.mode == IngressPersistenceMode::DurableQueue {
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
) -> CooldisResult<Option<String>> {
    if let Some(value) = literal {
        return Ok(Some(value.clone()));
    }
    let Some(env_name) = env_name else {
        return Ok(None);
    };
    std::env::var(env_name).map(Some).map_err(|err| {
        CooldisError::RuntimeFactory(format!("failed to read {label} from env {env_name}: {err}"))
    })
}

fn render_launchd_service(spec: &CooldisDaemonServiceSpec) -> String {
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

fn render_systemd_service(spec: &CooldisDaemonServiceSpec) -> String {
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
Description=Cooldis daemon\n\
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
    unix_listen_url(default_cooldis_daemon_socket_path())
}

fn default_true() -> bool {
    true
}

fn default_telegram_webhook_path() -> String {
    DEFAULT_TELEGRAM_WEBHOOK_PATH.to_string()
}

fn default_sqlite_queue_path() -> PathBuf {
    PathBuf::from(DEFAULT_SQLITE_QUEUE_PATH)
}

fn unix_listen_url(path: impl AsRef<Path>) -> String {
    format!("unix://{}", path.as_ref().display())
}

fn default_daemon_socket_path_from_env(get_env: impl Fn(&str) -> Option<OsString>) -> PathBuf {
    if let Some(path) = get_env("COOLDIS_DAEMON_SOCKET")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
    {
        return path;
    }

    if let Some(dir) = get_env("XDG_RUNTIME_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
    {
        return dir.join("cooldis/cooldis.sock");
    }

    if cfg!(target_os = "macos")
        && let Some(home) = get_env("HOME")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
    {
        return home.join("Library/Application Support/cooldis/run/cooldis.sock");
    }

    if let Some(dir) = get_env("XDG_STATE_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
    {
        return dir.join("cooldis/run/cooldis.sock");
    }

    if let Some(home) = get_env("HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
    {
        return home.join(".local/state/cooldis/run/cooldis.sock");
    }

    let user = get_env("USER")
        .and_then(|value| value.into_string().ok())
        .filter(|value| !value.trim().is_empty())
        .map(|value| sanitize_socket_path_component(&value))
        .unwrap_or_else(|| "user".to_string());
    let temp_dir = if cfg!(unix) {
        PathBuf::from("/tmp")
    } else {
        std::env::temp_dir()
    };
    temp_dir.join(format!("cooldis-{user}/cooldis.sock"))
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

fn resolve_config_path(base: &Path, path: PathBuf) -> PathBuf {
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

fn validate_service_label(label: &str) -> CooldisResult<()> {
    if label.trim().is_empty() {
        return Err(CooldisError::RuntimeFactory(
            "daemon service label cannot be empty".to_string(),
        ));
    }
    if !label
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(CooldisError::RuntimeFactory(format!(
            "daemon service label {label:?} may only contain ASCII letters, numbers, '.', '_', and '-'"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
