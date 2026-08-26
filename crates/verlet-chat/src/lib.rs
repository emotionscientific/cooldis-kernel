//! The `verlet chat` terminal UI.
//!
//! This crate is presentation only. It never talks to the app-server: the host
//! (the `verlet` CLI) drives the JSON-RPC client and translates its
//! notifications into [`ChatEvent`]s; the UI translates keystrokes into
//! [`Action`]s for the host to execute. Everything in between — transcript
//! cells, composer, slash popup, layout — is a synchronous state machine
//! ([`app::App`]) that tests can drive without a terminal or a runtime.
//!
//! Built on [tuika](https://github.com/everruns/tuika), pinned to an exact
//! version in the workspace manifest: tuika is pre-1.0 and minor releases may
//! break API, so upgrades are deliberate changes, never incidental ones.
//! The cell and layout code started as a port of tuika's `codex` example and
//! deliberately stays close to it, so upstream improvements remain easy to
//! diff against.

pub mod app;
pub mod cells;
pub mod runner;
#[cfg(test)]
mod tests;
pub mod theme;
mod ui;

/// What the host must execute on the UI's behalf. Emitted by [`app::App::handle`],
/// drained by the host loop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// A non-slash prompt was submitted. The host decides whether this starts
    /// a turn or steers the active one (it owns the turn lifecycle).
    Submit(String),
    /// Interrupt the active turn, if any.
    Interrupt,
    /// `/new` — start a fresh thread.
    NewThread,
    /// `/sessions` — fetch the thread list for display.
    ListSessions,
    /// `/resume <id>`.
    Resume(String),
    /// `/rename <name>` — rename the current thread.
    Rename(String),
    /// `/fork` — fork the current thread.
    Fork,
    /// `/compact` — request compaction of the current thread.
    Compact,
    /// `/models` — fetch the model list; the host answers with
    /// [`ChatEvent::Models`], which opens the picker.
    ListModels,
    /// A picker row was chosen: switch the app-server's active model.
    SelectModel { provider_id: String, model: String },
    /// `/setup` (or the first-run gate): fetch the merged provider catalog;
    /// the host answers with [`ChatEvent::ProviderCatalog`], which opens (or
    /// refreshes) the setup window.
    FetchProviderCatalog,
    /// Create or update a custom provider from the setup window's form; the
    /// host answers with [`ChatEvent::CustomProviderResult`]. The API key is
    /// NOT part of the spec: the window follows up with [`Action::SetProviderKey`]
    /// once the record exists.
    UpsertCustomProvider { spec: CustomProviderSpec },
    /// Delete a custom provider record; the host answers with
    /// [`ChatEvent::CustomProviderResult`].
    DeleteCustomProvider { provider_id: String },
    /// A pasted API key to store for the provider; the host answers with
    /// [`ChatEvent::CredentialResult`].
    SetProviderKey {
        provider_id: String,
        api_key: String,
    },
    /// Start an OAuth login for the provider. The host runs the flow in the
    /// background and reports [`ChatEvent::LoginDeviceCode`] (device method)
    /// and [`ChatEvent::CredentialResult`].
    StartLogin {
        provider_id: String,
        method: LoginMethod,
    },
    /// Abort the in-flight OAuth login, if any.
    CancelLogin,
    /// Delete the provider's stored credential; the host answers with
    /// [`ChatEvent::CredentialCleared`] (or an error notice).
    ClearCredential { provider_id: String },
    /// Read the installed-kit records and probe the recommended kit
    /// sources; the host answers with [`ChatEvent::KitStatus`].
    FetchKitStatus { intent: KitStatusIntent },
    /// Install a kit from a local directory through the `verlet kit
    /// install` pipeline; the host answers with
    /// [`ChatEvent::KitInstallResult`].
    InstallKit { name: String, source: String },
}

/// How an OAuth-capable provider signs in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoginMethod {
    /// PKCE flow through the local browser.
    Browser,
    /// Device-code flow: the UI shows a URL and code to enter elsewhere.
    Device,
}

/// One provider of the setup window: models.dev catalog metadata merged with
/// provider-store and auth state, as reported by `modelProvider/catalog`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogProviderRow {
    pub provider_id: String,
    pub display_name: String,
    pub base_url: String,
    /// API family: `openai_chat_completions` | `openai_responses` |
    /// `anthropic_messages`.
    pub api: String,
    /// `api_key` | `oauth`.
    pub auth_kind: String,
    /// Environment variables that can satisfy this provider's auth.
    pub env_vars: Vec<String>,
    /// Whether a usable credential exists (stored, env, or OAuth tokens).
    pub configured: bool,
    /// The server's human status label ("stored credential",
    /// "OPENAI_API_KEY detected", ...). Empty when unconfigured.
    pub auth_label: String,
    /// Store-only provider with no catalog entry (user-created).
    pub custom: bool,
    /// Whether this provider owns the active model selection.
    pub active: bool,
    /// Models this provider would offer (store record or catalog).
    pub model_count: usize,
    /// The model auto-selected after a successful first credential.
    pub default_model: Option<String>,
}

/// Why kit status was fetched, so [`ChatEvent::KitStatus`] knows whether to
/// open the setup window's kit step, refresh it in place, or offer it only
/// when the recommended kit is missing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KitStatusIntent {
    /// Open the kit step after the user selects the Home action.
    Open,
    /// Refresh an already-open kit step without reopening it from Home.
    Refresh,
    /// First-run offer: open only if a recommended kit is not installed.
    OfferIfMissing,
}

/// One installed kit, as read from the host's installed-kit records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstalledKitRow {
    pub name: String,
    pub version: Option<String>,
    /// Model-facing tool names, in record order.
    pub tools: Vec<String>,
}

/// One kit the host recommends installing. The recommendation list is
/// hardcoded host-side; the UI only renders it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecommendedKitRow {
    pub name: String,
    /// One-line description shown next to the install row.
    pub blurb: String,
    /// A kit directory the host found for this kit (first existing
    /// candidate). None means the source is not on this machine and the
    /// step shows manual install guidance instead of an Install row.
    pub source: Option<String>,
}

/// A custom provider definition submitted from the setup window's form. The
/// API key is carried separately through [`Action::SetProviderKey`] so it
/// never rides a value that gets logged or echoed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CustomProviderSpec {
    pub provider_id: String,
    pub display_name: String,
    /// API family: `openai_chat_completions` | `openai_responses` |
    /// `anthropic_messages`.
    pub api: String,
    pub base_url: String,
    /// Optional extra header sent with every request.
    pub header: Option<(String, String)>,
    /// Model ids, first one is the provider's default.
    pub models: Vec<String>,
    /// True when the form was submitted without an API key: the provider
    /// needs no auth (a local server), and the record should say so instead
    /// of waiting for a credential.
    pub keyless: bool,
}

/// One row of the `/models` picker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelRow {
    pub provider_id: String,
    pub model: String,
    pub display_name: String,
    /// `configured` | `env` | `missing`, as reported by `model/list`.
    pub auth_status: String,
    /// Whether this is the app-server's active selection.
    pub active: bool,
}

/// One row of `/sessions` output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionRow {
    pub id: String,
    pub name: String,
    pub status: String,
    pub preview: String,
    pub current: bool,
}

/// What the host reports back into the UI.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChatEvent {
    /// Streamed assistant answer text (markdown).
    AnswerDelta(String),
    /// Streamed assistant thinking text.
    ThinkingDelta(String),
    /// A tool call began. `title` is already human-shaped ("cargo test",
    /// "web_search ...").
    ToolStarted { id: String, title: String },
    /// Streamed tool/command output.
    ToolOutputDelta { id: String, delta: String },
    /// A tool call finished. `output` replaces any streamed output when
    /// non-empty (the completed item carries the authoritative aggregate).
    ToolCompleted {
        id: String,
        success: bool,
        output: String,
    },
    /// A turn started (in response to a submit, or steered server-side).
    TurnStarted { turn_id: String },
    /// The active turn finished. `error` carries a failure message if any.
    TurnCompleted { error: Option<String> },
    /// A submit landed as mid-turn steering rather than a new turn.
    TurnSteered,
    /// Token usage reported for one model request in the active turn. The UI
    /// accumulates successive hints until the next turn starts.
    Usage { total_tokens: u64 },
    /// The UI now shows this thread (start, resume, fork all land here).
    ThreadSwitched {
        thread_id: String,
        name: Option<String>,
        cwd: Option<String>,
        reason: String,
    },
    /// `/rename` succeeded.
    ThreadRenamed { name: String },
    /// `/sessions` result.
    Sessions(Vec<SessionRow>),
    /// `/models` result: opens the model picker over these rows.
    Models(Vec<ModelRow>),
    /// `model/select` succeeded; the active model changed for later turns.
    ModelSelected { provider_id: String, model: String },
    /// The merged provider catalog: opens (or refreshes) the setup window.
    ProviderCatalog { providers: Vec<CatalogProviderRow> },
    /// A custom provider upsert or delete finished.
    CustomProviderResult {
        provider_id: String,
        error: Option<String>,
    },
    /// Bootstrap found zero configured providers: the setup window opens on
    /// the catalog list, and the footer nags until a provider is configured.
    NoConfiguredProviders,
    /// A device-code login started: show the URL and code to the user.
    LoginDeviceCode {
        verification_uri: String,
        user_code: String,
    },
    /// A credential attempt (pasted key or OAuth login) finished.
    CredentialResult {
        provider_id: String,
        error: Option<String>,
    },
    /// The provider's stored credential was deleted.
    CredentialCleared { provider_id: String },
    /// Installed kits plus the host's recommendations: opens (or refreshes)
    /// the setup window's kit step per the intent.
    KitStatus {
        intent: KitStatusIntent,
        installed: Vec<InstalledKitRow>,
        recommended: Vec<RecommendedKitRow>,
    },
    /// A kit install finished. `receipt` carries the same lines `verlet kit
    /// install` prints; shown as a transcript notice.
    KitInstallResult {
        name: String,
        error: Option<String>,
        receipt: Vec<String>,
    },
    /// The thread's runtime status changed ("idle", "running", ...).
    ThreadStatus(String),
    /// An informational notice for the transcript.
    Info { title: String, body: Vec<String> },
    /// An error notice for the transcript.
    Error { title: String, body: Vec<String> },
    /// The transcript can no longer be trusted incrementally (broadcast lag);
    /// the server will follow with a rebuilt snapshot. Shown as a notice.
    ResyncStarted,
}

/// Connection and current-thread facts the UI shows in the banner and footer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionMeta {
    /// "local/private" or "attach ws://...".
    pub connection_label: String,
    pub cwd: String,
    /// "provider/model".
    pub model_label: String,
    pub thread_id: String,
    pub thread_name: Option<String>,
    /// CLI version string for the banner.
    pub version: String,
}
