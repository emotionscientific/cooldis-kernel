//! Setup wizard state machine.
//!
//! Ported from yolop's `/setup` overlay (`yolop/src/app/setup.rs`, MIT) and
//! rewired for the presentation-only discipline: the `SetupStep` enum is the
//! wizard's state, these `impl App` methods are its transitions, and every
//! side effect is an [`Action`] for the host to execute. The wizard owns the
//! whole input surface while open — provider rows, credential options, key
//! entry, and login-wait screens never echo through the composer. Rendering
//! lives in `ui.rs`; this module is state and transitions only.

use tuika::prelude::*;

use super::App;
use crate::cells::Tone;
use crate::{Action, LoginMethod, ProviderRow};

/// Where the wizard is. Steps carry everything they need to render and to
/// return to the previous step, so transitions never re-fetch.
pub(crate) enum SetupStep {
    /// Pick a provider. Connected rows go straight to models; the rest go
    /// through credentials.
    Provider {
        rows: Vec<ProviderRow>,
        state: SelectState,
    },
    /// Pick how to authenticate the provider.
    Credential {
        rows: Vec<ProviderRow>,
        provider: ProviderRow,
        state: SelectState,
        error: Option<String>,
    },
    /// Masked API-key entry. `busy` is set between submitting the key and
    /// the host's [`crate::ChatEvent::CredentialResult`].
    KeyInput {
        rows: Vec<ProviderRow>,
        provider: ProviderRow,
        value: String,
        busy: bool,
        error: Option<String>,
    },
    /// An OAuth login is running in the host. Device logins fill
    /// `device_code` with `(verification_uri, user_code)` when it arrives.
    LoginWait {
        rows: Vec<ProviderRow>,
        provider: ProviderRow,
        method: LoginMethod,
        device_code: Option<(String, String)>,
    },
    /// A model list was requested for the chosen provider; the wizard closes
    /// into the model picker when [`crate::ChatEvent::Models`] arrives.
    AwaitModels { provider_id: String },
    /// The provider catalog was requested to route a "needs login" model row
    /// into its credential step.
    AwaitProviders { provider_id: String },
}

/// One selectable row of the credential step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CredentialOption {
    pub action: CredentialAction,
    pub label: &'static str,
    pub hint: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CredentialAction {
    BrowserLogin,
    DeviceLogin,
    PasteKey,
    ClearSaved,
    Skip,
}

/// The credential choices for a provider: sign-in flows for OAuth-shaped
/// providers, key entry for the rest.
pub(crate) fn credential_options(provider: &ProviderRow) -> Vec<CredentialOption> {
    if provider.oauth {
        vec![
            CredentialOption {
                action: CredentialAction::BrowserLogin,
                label: "Sign in with browser",
                hint: "opens on this machine",
            },
            CredentialOption {
                action: CredentialAction::DeviceLogin,
                label: "Sign in with device code",
                hint: "works on headless terminals",
            },
            CredentialOption {
                action: CredentialAction::ClearSaved,
                label: "Clear saved login",
                hint: "remove the stored tokens",
            },
            CredentialOption {
                action: CredentialAction::Skip,
                label: "Skip for now",
                hint: "leave setup unchanged",
            },
        ]
    } else {
        vec![
            CredentialOption {
                action: CredentialAction::PasteKey,
                label: "Paste API key",
                hint: "stored by the server, never shown",
            },
            CredentialOption {
                action: CredentialAction::ClearSaved,
                label: "Clear saved key",
                hint: "remove this provider's key",
            },
            CredentialOption {
                action: CredentialAction::Skip,
                label: "Skip for now",
                hint: "leave setup unchanged",
            },
        ]
    }
}

/// The status suffix for a provider row, yolop-style.
pub(crate) fn provider_status(row: &ProviderRow) -> String {
    match row.auth_status.as_str() {
        "configured" | "env" => format!("✓ {}", row.label),
        _ if row.oauth => "needs sign-in".to_string(),
        _ => "needs API key".to_string(),
    }
}

pub(crate) fn provider_connected(row: &ProviderRow) -> bool {
    matches!(row.auth_status.as_str(), "configured" | "env")
}

impl App {
    /// Open the wizard's provider step over a fresh catalog. If the wizard
    /// was waiting to route a "needs login" model row, jump straight to that
    /// provider's credential step.
    pub(crate) fn open_setup(&mut self, rows: Vec<ProviderRow>) {
        let previous = self.setup.take();
        match previous {
            step @ Some(
                SetupStep::Credential { .. }
                | SetupStep::KeyInput { .. }
                | SetupStep::LoginWait { .. }
                | SetupStep::AwaitModels { .. },
            ) => {
                self.setup = step;
                return;
            }
            Some(SetupStep::AwaitProviders { provider_id }) => {
                if let Some(provider) = rows
                    .iter()
                    .find(|row| row.provider_id == provider_id)
                    .cloned()
                {
                    self.popup = None;
                    self.picker = None;
                    self.setup = Some(SetupStep::Credential {
                        rows,
                        provider,
                        state: SelectState::new(),
                        error: None,
                    });
                    return;
                }
                self.pending_selection = None;
                self.notice(
                    Tone::Error,
                    format!("provider {provider_id} is no longer available"),
                    Vec::new(),
                );
            }
            None | Some(SetupStep::Provider { .. }) => {
                self.pending_selection = None;
            }
        }
        self.popup = None;
        self.picker = None;
        if rows.is_empty() {
            self.setup = None;
            self.pending_selection = None;
            self.notice(
                Tone::Error,
                "no providers available".to_string(),
                Vec::new(),
            );
            return;
        }
        let mut state = SelectState::new();
        state.select(rows.iter().position(|row| row.active).or(Some(0)));
        self.setup = Some(SetupStep::Provider { rows, state });
    }

    /// Route a chosen "needs login" model row into the wizard: remember the
    /// selection, fetch the catalog, and land on the provider's credential
    /// step when it arrives.
    pub(crate) fn setup_for_model(&mut self, provider_id: String, model: String) {
        self.pending_selection = Some((provider_id.clone(), model));
        self.setup = Some(SetupStep::AwaitProviders { provider_id });
        self.actions.push(Action::ListProviders);
    }

    /// The wizard is modal while it has a visible step; every event lands
    /// here. Await states are invisible (a fetch is in flight) and do not
    /// capture input.
    pub(crate) fn setup_visible(&self) -> bool {
        !matches!(
            self.setup,
            None | Some(SetupStep::AwaitModels { .. }) | Some(SetupStep::AwaitProviders { .. })
        )
    }

    pub(crate) fn handle_setup(&mut self, event: &Event) {
        let Some(step) = self.setup.take() else {
            return;
        };
        match step {
            SetupStep::Provider { rows, state } => self.handle_provider_step(event, rows, state),
            SetupStep::Credential {
                rows,
                provider,
                state,
                error,
            } => self.handle_credential_step(event, rows, provider, state, error),
            SetupStep::KeyInput {
                rows,
                provider,
                value,
                busy,
                error,
            } => self.handle_key_input(event, rows, provider, value, busy, error),
            SetupStep::LoginWait {
                rows,
                provider,
                method,
                device_code,
            } => self.handle_login_wait(event, rows, provider, method, device_code),
            step @ (SetupStep::AwaitModels { .. } | SetupStep::AwaitProviders { .. }) => {
                self.setup = Some(step);
            }
        }
    }

    fn handle_provider_step(
        &mut self,
        event: &Event,
        rows: Vec<ProviderRow>,
        mut state: SelectState,
    ) {
        // `c` forces the credential step even when connected (rotate or
        // clear a key), mirroring yolop.
        if let Event::Key(key) = event
            && key.plain()
            && key.code == KeyCode::Char('c')
        {
            if let Some(provider) = state.selected().and_then(|index| rows.get(index)).cloned() {
                self.setup = Some(SetupStep::Credential {
                    rows,
                    provider,
                    state: SelectState::new(),
                    error: None,
                });
            } else {
                self.setup = Some(SetupStep::Provider { rows, state });
            }
            return;
        }
        match state.handle(event, rows.len()) {
            InputOutcome::Submitted => {
                let Some(provider) = state.selected().and_then(|index| rows.get(index)).cloned()
                else {
                    self.setup = Some(SetupStep::Provider { rows, state });
                    return;
                };
                if provider_connected(&provider) {
                    self.setup = Some(SetupStep::AwaitModels {
                        provider_id: provider.provider_id,
                    });
                    self.actions.push(Action::ListModels);
                } else {
                    self.setup = Some(SetupStep::Credential {
                        rows,
                        provider,
                        state: SelectState::new(),
                        error: None,
                    });
                }
            }
            InputOutcome::Cancelled => {
                self.setup = None;
                self.pending_selection = None;
            }
            _ => self.setup = Some(SetupStep::Provider { rows, state }),
        }
    }

    fn handle_credential_step(
        &mut self,
        event: &Event,
        rows: Vec<ProviderRow>,
        provider: ProviderRow,
        mut state: SelectState,
        error: Option<String>,
    ) {
        let options = credential_options(&provider);
        match state.handle(event, options.len()) {
            InputOutcome::Submitted => {
                let action = state
                    .selected()
                    .and_then(|index| options.get(index))
                    .map(|option| option.action)
                    .unwrap_or(CredentialAction::Skip);
                match action {
                    CredentialAction::BrowserLogin => {
                        self.actions.push(Action::StartLogin {
                            provider_id: provider.provider_id.clone(),
                            method: LoginMethod::Browser,
                        });
                        self.setup = Some(SetupStep::LoginWait {
                            rows,
                            provider,
                            method: LoginMethod::Browser,
                            device_code: None,
                        });
                    }
                    CredentialAction::DeviceLogin => {
                        self.actions.push(Action::StartLogin {
                            provider_id: provider.provider_id.clone(),
                            method: LoginMethod::Device,
                        });
                        self.setup = Some(SetupStep::LoginWait {
                            rows,
                            provider,
                            method: LoginMethod::Device,
                            device_code: None,
                        });
                    }
                    CredentialAction::PasteKey => {
                        self.setup = Some(SetupStep::KeyInput {
                            rows,
                            provider,
                            value: String::new(),
                            busy: false,
                            error: None,
                        });
                    }
                    CredentialAction::ClearSaved => {
                        self.actions.push(Action::ClearCredential {
                            provider_id: provider.provider_id.clone(),
                        });
                        self.setup = Some(SetupStep::Credential {
                            rows,
                            provider,
                            state,
                            error,
                        });
                    }
                    CredentialAction::Skip => {
                        self.setup = None;
                        self.pending_selection = None;
                        self.notice(Tone::Info, "setup skipped".to_string(), Vec::new());
                    }
                }
            }
            InputOutcome::Cancelled => {
                let mut provider_state = SelectState::new();
                provider_state.select(
                    rows.iter()
                        .position(|row| row.provider_id == provider.provider_id)
                        .or(Some(0)),
                );
                self.setup = Some(SetupStep::Provider {
                    rows,
                    state: provider_state,
                });
            }
            _ => {
                self.setup = Some(SetupStep::Credential {
                    rows,
                    provider,
                    state,
                    error,
                });
            }
        }
    }

    fn handle_key_input(
        &mut self,
        event: &Event,
        rows: Vec<ProviderRow>,
        provider: ProviderRow,
        mut value: String,
        busy: bool,
        error: Option<String>,
    ) {
        // While the key is being saved only Esc (back to credentials) works;
        // typing into a submitted form would race the host's answer.
        if busy {
            if matches!(event, Event::Key(key) if key.plain() && key.code == KeyCode::Esc) {
                self.pending_selection = None;
                self.setup = Some(SetupStep::Credential {
                    rows,
                    provider,
                    state: SelectState::new(),
                    error: None,
                });
            } else {
                self.setup = Some(SetupStep::KeyInput {
                    rows,
                    provider,
                    value,
                    busy,
                    error,
                });
            }
            return;
        }
        if let Event::Paste(pasted) = event {
            value.push_str(pasted.trim());
            self.setup = Some(SetupStep::KeyInput {
                rows,
                provider,
                value,
                busy: false,
                error: None,
            });
            return;
        }
        let Event::Key(key) = event else {
            self.setup = Some(SetupStep::KeyInput {
                rows,
                provider,
                value,
                busy,
                error,
            });
            return;
        };
        match key.code {
            KeyCode::Esc => {
                self.setup = Some(SetupStep::Credential {
                    rows,
                    provider,
                    state: SelectState::new(),
                    error: None,
                });
            }
            KeyCode::Enter => {
                let trimmed = value.trim().to_string();
                if trimmed.is_empty() {
                    self.setup = Some(SetupStep::KeyInput {
                        rows,
                        provider,
                        value,
                        busy: false,
                        error: Some("API key is empty — paste a key, or press Esc".to_string()),
                    });
                    return;
                }
                self.actions.push(Action::SetProviderKey {
                    provider_id: provider.provider_id.clone(),
                    api_key: trimmed.clone(),
                });
                self.pending_key_redactions
                    .push((provider.provider_id.clone(), trimmed));
                // Bound the belt-and-braces list if answers never arrive.
                if self.pending_key_redactions.len() > 8 {
                    self.pending_key_redactions.remove(0);
                }
                self.setup = Some(SetupStep::KeyInput {
                    rows,
                    provider,
                    value,
                    busy: true,
                    error: None,
                });
            }
            KeyCode::Backspace => {
                value.pop();
                self.setup = Some(SetupStep::KeyInput {
                    rows,
                    provider,
                    value,
                    busy: false,
                    error: None,
                });
            }
            KeyCode::Char(ch) if !key.ctrl => {
                value.push(ch);
                self.setup = Some(SetupStep::KeyInput {
                    rows,
                    provider,
                    value,
                    busy: false,
                    error: None,
                });
            }
            _ => {
                self.setup = Some(SetupStep::KeyInput {
                    rows,
                    provider,
                    value,
                    busy,
                    error,
                });
            }
        }
    }

    fn handle_login_wait(
        &mut self,
        event: &Event,
        rows: Vec<ProviderRow>,
        provider: ProviderRow,
        method: LoginMethod,
        device_code: Option<(String, String)>,
    ) {
        if matches!(event, Event::Key(key) if key.plain() && key.code == KeyCode::Esc) {
            self.actions.push(Action::CancelLogin);
            self.pending_selection = None;
            self.setup = Some(SetupStep::Credential {
                rows,
                provider,
                state: SelectState::new(),
                error: Some("sign-in canceled".to_string()),
            });
        } else {
            self.setup = Some(SetupStep::LoginWait {
                rows,
                provider,
                method,
                device_code,
            });
        }
    }

    /// Fold a host credential answer into the wizard.
    pub(crate) fn apply_credential_result(&mut self, provider_id: String, error: Option<String>) {
        let answered_provider = provider_id.clone();
        match self.setup.take() {
            Some(SetupStep::KeyInput {
                rows,
                provider,
                busy: true,
                ..
            }) if provider.provider_id == provider_id => {
                if let Some(message) = error {
                    let message = self.redact_secrets(&message);
                    self.setup = Some(SetupStep::KeyInput {
                        rows,
                        provider,
                        value: String::new(),
                        busy: false,
                        error: Some(message),
                    });
                } else {
                    self.finish_credential(provider_id);
                }
            }
            Some(SetupStep::LoginWait { rows, provider, .. })
                if provider.provider_id == provider_id =>
            {
                if let Some(message) = error {
                    let message = self.redact_secrets(&message);
                    self.setup = Some(SetupStep::Credential {
                        rows,
                        provider,
                        state: SelectState::new(),
                        error: Some(message),
                    });
                } else {
                    self.finish_credential(provider_id);
                }
            }
            step => {
                // The wizard moved on (or was dismissed): report out of band.
                self.setup = step;
                if self
                    .pending_selection
                    .as_ref()
                    .is_some_and(|(pending_provider, _)| pending_provider == &provider_id)
                {
                    self.pending_selection = None;
                }
                match error {
                    Some(_) => self.notice(
                        Tone::Error,
                        format!("{provider_id}: credential failed"),
                        Vec::new(),
                    ),
                    None => self.notice(
                        Tone::Info,
                        format!("{provider_id}: credential saved"),
                        Vec::new(),
                    ),
                }
            }
        }
        // The submission is answered; its redaction entry has done its job.
        self.pending_key_redactions
            .retain(|(pending_provider, _)| pending_provider != &answered_provider);
    }

    /// A credential landed: re-issue the selection that routed us here, or
    /// continue into the provider's model list.
    fn finish_credential(&mut self, provider_id: String) {
        self.notice(
            Tone::Info,
            format!("{provider_id}: credential saved"),
            Vec::new(),
        );
        if let Some((pending_provider, model)) = self.pending_selection.take() {
            if pending_provider == provider_id {
                self.setup = None;
                self.actions.push(Action::SelectModel {
                    provider_id: pending_provider,
                    model,
                });
                return;
            }
            self.pending_selection = Some((pending_provider, model));
        }
        self.setup = Some(SetupStep::AwaitModels { provider_id });
        self.actions.push(Action::ListModels);
    }

    pub(crate) fn apply_device_code(&mut self, verification_uri: String, user_code: String) {
        if let Some(SetupStep::LoginWait {
            method: LoginMethod::Device,
            device_code,
            ..
        }) = self.setup.as_mut()
        {
            *device_code = Some((verification_uri, user_code));
        }
    }

    pub(crate) fn apply_credential_cleared(&mut self, provider_id: String) {
        self.notice(
            Tone::Info,
            format!("{provider_id}: credential cleared"),
            Vec::new(),
        );
        match self.setup.as_ref() {
            Some(SetupStep::Credential { provider, .. }) if provider.provider_id == provider_id => {
                self.setup = Some(SetupStep::AwaitProviders {
                    provider_id: provider_id.clone(),
                });
                self.actions.push(Action::ListProviders);
            }
            Some(SetupStep::Provider { rows, .. })
                if rows.iter().any(|row| row.provider_id == provider_id) =>
            {
                self.actions.push(Action::ListProviders);
            }
            _ => {}
        }
    }

    /// Replace any submitted key material occurring in `text`. Entries are
    /// dropped on their matching [`crate::ChatEvent::CredentialResult`] and
    /// capped at push time, so unmatched submissions keep redacting for the
    /// rest of the session rather than expiring on unrelated errors.
    pub(crate) fn redact_secrets(&self, text: &str) -> String {
        let mut text = text.to_string();
        for (_, secret) in &self.pending_key_redactions {
            text = text.replace(secret, "[redacted]");
        }
        text
    }

    pub(crate) fn apply_error(&mut self, mut title: String, mut body: Vec<String>) {
        title = self.redact_secrets(&title);
        for line in &mut body {
            *line = self.redact_secrets(line);
        }
        match self.setup.take() {
            Some(SetupStep::KeyInput {
                rows,
                provider,
                value,
                busy,
                error,
            }) => {
                let secret = value.trim();
                if !secret.is_empty() {
                    title = title.replace(secret, "[redacted]");
                    for line in &mut body {
                        *line = line.replace(secret, "[redacted]");
                    }
                }
                if busy {
                    self.setup = Some(SetupStep::KeyInput {
                        rows,
                        provider,
                        value: String::new(),
                        busy: false,
                        error: Some(error_summary(&title, &body)),
                    });
                } else {
                    self.setup = Some(SetupStep::KeyInput {
                        rows,
                        provider,
                        value,
                        busy,
                        error,
                    });
                }
            }
            Some(SetupStep::LoginWait { rows, provider, .. }) => {
                self.setup = Some(SetupStep::Credential {
                    rows,
                    provider,
                    state: SelectState::new(),
                    error: Some(error_summary(&title, &body)),
                });
            }
            Some(SetupStep::AwaitModels { .. } | SetupStep::AwaitProviders { .. }) => {
                self.setup = None;
                self.pending_selection = None;
            }
            step => self.setup = step,
        }
        self.notice(Tone::Error, title, body);
    }
}

fn error_summary(title: &str, body: &[String]) -> String {
    body.first()
        .map(|line| format!("{title}: {line}"))
        .unwrap_or_else(|| title.to_string())
}
