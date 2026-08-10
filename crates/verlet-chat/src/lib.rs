//! The `verlet chat` terminal UI.
//!
//! This crate is presentation only. It never talks to the app-server: the host
//! (the `verlet` CLI) drives the JSON-RPC client and translates its
//! notifications into [`ChatEvent`]s; the UI translates keystrokes into
//! [`Action`]s for the host to execute. Everything in between — transcript
//! cells, composer, slash popup, layout — is a synchronous state machine
//! ([`App`]) that tests can drive without a terminal or a runtime.
//!
//! Built on [tuika](https://github.com/everruns/tuika), pinned to an exact
//! version in the workspace manifest: tuika is pre-1.0 and minor releases may
//! break API, so upgrades are deliberate changes, never incidental ones.
//! The cell and layout code started as a port of tuika's `codex` example and
//! deliberately stays close to it, so upstream improvements remain easy to
//! diff against.

mod app;
mod cells;
mod runner;
#[cfg(test)]
mod tests;
mod theme;
mod ui;

pub use app::{App, COMMANDS, SlashCommand, parse_slash_command};
pub use cells::{Cell, ExecStatus, Tone};
pub use runner::{RunnerError, run_ui};
pub use theme::chat_theme;

/// What the host must execute on the UI's behalf. Emitted by [`App::handle`],
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
    /// `/setup` — fetch the provider auth catalog; the host answers with
    /// [`ChatEvent::Providers`], which opens the setup wizard.
    ListProviders,
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
}

/// How an OAuth-capable provider signs in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoginMethod {
    /// PKCE flow through the local browser.
    Browser,
    /// Device-code flow: the UI shows a URL and code to enter elsewhere.
    Device,
}

/// One row of the setup wizard's provider step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderRow {
    pub provider_id: String,
    pub display_name: String,
    /// `configured` | `env` | `missing`, as reported by
    /// `modelProvider/auth/status`.
    pub auth_status: String,
    /// The server's human status label ("stored credential",
    /// "OPENAI_API_KEY detected", ...). Shown as the row hint.
    pub label: String,
    /// OAuth-shaped provider (openai-codex): sign-in options instead of a
    /// pasted API key.
    pub oauth: bool,
    /// Whether this provider owns the active model selection.
    pub active: bool,
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
    /// Provider auth catalog: opens (or refreshes) the setup wizard. Sent
    /// unsolicited at bootstrap when the active model has no credentials.
    Providers(Vec<ProviderRow>),
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
