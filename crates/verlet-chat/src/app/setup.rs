//! Setup window state machine.
//!
//! The `/setup` experience is a modal window (rendered as a tuika dialog in
//! `ui.rs`) whose home screen is an overview of configured providers, with a
//! searchable catalog picker and a custom-provider form behind it. The pi
//! coding agent's `/login` dialog is the UX reference. Presentation-only
//! discipline holds: `SetupStep` is the window's state, these `impl crate::app::App`
//! methods are its transitions, and every side effect is an [`crate::Action`] for
//! the host to execute. The window owns the whole input surface while it has
//! a visible step; Esc backs out one level at a time and closes from home.

/// What a catalog fetch was for, so [`crate::ChatEvent::ProviderCatalog`]
/// knows which screen to open when the rows arrive.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CatalogIntent {
    /// `/setup`: open the provider overview.
    Home,
    /// First-run gate: open the catalog picker directly.
    Catalog,
    /// A "needs login" model row routed here: land on the provider's
    /// credential entry, then re-issue the model selection.
    ForModel { provider_id: String },
}

/// Which screen a credential step returns to on Esc.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CredentialOrigin {
    Catalog,
    Menu,
}

/// Where the custom form's submission currently is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CustomBusy {
    Idle,
    /// Waiting for [`crate::ChatEvent::CustomProviderResult`].
    Upserting,
    /// The record exists; waiting for the key's
    /// [`crate::ChatEvent::CredentialResult`].
    SavingKey,
}

/// The custom-provider form's fields, in focus order.
pub(crate) const CUSTOM_FIELDS: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CustomField {
    Name,
    Id,
    Api,
    BaseUrl,
    ApiKey,
    HeaderName,
    HeaderValue,
    Models,
}

pub(crate) const CUSTOM_FIELD_ORDER: [CustomField; CUSTOM_FIELDS] = [
    CustomField::Name,
    CustomField::Id,
    CustomField::Api,
    CustomField::BaseUrl,
    CustomField::ApiKey,
    CustomField::HeaderName,
    CustomField::HeaderValue,
    CustomField::Models,
];

/// The API families a custom provider can speak, as `(wire value, label)`.
pub(crate) const API_FAMILIES: [(&str, &str); 3] = [
    ("openai_chat_completions", "OpenAI Chat Completions"),
    ("openai_responses", "OpenAI Responses"),
    ("anthropic_messages", "Anthropic Messages"),
];

/// The custom-provider form. `id` mirrors a slug of `name` until the user
/// edits it directly (`id_touched`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CustomForm {
    pub name: String,
    pub id: String,
    pub id_touched: bool,
    /// Index into [`API_FAMILIES`].
    pub api_index: usize,
    pub base_url: String,
    pub api_key: String,
    pub header_name: String,
    pub header_value: String,
    /// Raw model-id text, split on commas/whitespace at submit.
    pub models: String,
    /// Index into [`CUSTOM_FIELD_ORDER`].
    pub focus: usize,
}

impl CustomForm {
    pub(crate) fn new() -> Self {
        Self {
            name: String::new(),
            id: String::new(),
            id_touched: false,
            api_index: 0,
            base_url: String::new(),
            api_key: String::new(),
            header_name: String::new(),
            header_value: String::new(),
            models: String::new(),
            focus: 0,
        }
    }

    /// Prefill from an existing custom provider for editing. The key field
    /// starts empty: an empty key on submit means "leave the credential
    /// alone".
    pub(crate) fn from_row(row: &crate::CatalogProviderRow) -> Self {
        let api_index = API_FAMILIES
            .iter()
            .position(|(value, _)| *value == row.api)
            .unwrap_or(0);
        Self {
            name: row.display_name.clone(),
            id: row.provider_id.clone(),
            id_touched: true,
            api_index,
            base_url: row.base_url.clone(),
            api_key: String::new(),
            header_name: String::new(),
            header_value: String::new(),
            models: row.default_model.clone().unwrap_or_default(),
            focus: 0,
        }
    }

    pub(crate) fn focused(&self) -> CustomField {
        CUSTOM_FIELD_ORDER[self.focus.min(CUSTOM_FIELDS - 1)]
    }

    fn field_mut(&mut self, field: CustomField) -> Option<&mut String> {
        match field {
            CustomField::Name => Some(&mut self.name),
            CustomField::Id => Some(&mut self.id),
            CustomField::Api => None,
            CustomField::BaseUrl => Some(&mut self.base_url),
            CustomField::ApiKey => Some(&mut self.api_key),
            CustomField::HeaderName => Some(&mut self.header_name),
            CustomField::HeaderValue => Some(&mut self.header_value),
            CustomField::Models => Some(&mut self.models),
        }
    }

    /// Validate and build the submission. Ok also carries the API key text
    /// (possibly empty, meaning "no key follow-up").
    pub(crate) fn build_spec(&self) -> Result<crate::CustomProviderSpec, String> {
        let display_name = self.name.trim();
        if display_name.is_empty() {
            return Err("name is required".to_string());
        }
        let provider_id = self.id.trim();
        if provider_id.is_empty() {
            return Err("provider id is required".to_string());
        }
        if !provider_id
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-' || ch == '_')
        {
            return Err("provider id may only use a-z, 0-9, - and _".to_string());
        }
        validate_base_url(self.base_url.trim())?;
        let models = split_model_ids(&self.models);
        if models.is_empty() {
            return Err("at least one model id is required".to_string());
        }
        let header_name = self.header_name.trim();
        let header_value = self.header_value.trim();
        let header = match (header_name.is_empty(), header_value.is_empty()) {
            (true, true) => None,
            (false, false) => Some((header_name.to_string(), header_value.to_string())),
            _ => return Err("header name and value must both be set (or both empty)".to_string()),
        };
        Ok(crate::CustomProviderSpec {
            provider_id: provider_id.to_string(),
            display_name: display_name.to_string(),
            api: API_FAMILIES[self.api_index.min(API_FAMILIES.len() - 1)]
                .0
                .to_string(),
            base_url: self.base_url.trim().to_string(),
            header,
            models,
            keyless: self.api_key.trim().is_empty(),
        })
    }
}

/// Where the setup window is. Steps carry the catalog rows they need to
/// render and to return to the previous screen, so transitions never
/// re-fetch.
pub(crate) enum SetupStep {
    /// A catalog fetch is in flight; invisible (no input capture).
    AwaitCatalog { intent: CatalogIntent },
    /// The provider overview plus the `Connect` / `Add custom` actions.
    Home {
        rows: Vec<crate::CatalogProviderRow>,
        state: tuika::components::SelectState,
    },
    /// Actions for one configured provider.
    ProviderMenu {
        rows: Vec<crate::CatalogProviderRow>,
        provider: crate::CatalogProviderRow,
        state: tuika::components::SelectState,
        /// A delete is in flight; only Esc works.
        busy: bool,
        error: Option<String>,
    },
    /// The searchable catalog picker ("Connect a provider").
    Catalog {
        rows: Vec<crate::CatalogProviderRow>,
        filter: String,
        state: tuika::components::SelectState,
    },
    /// OAuth sign-in method choice (browser / device code).
    Credential {
        rows: Vec<crate::CatalogProviderRow>,
        provider: crate::CatalogProviderRow,
        origin: CredentialOrigin,
        state: tuika::components::SelectState,
        error: Option<String>,
    },
    /// Masked API-key entry. `busy` is set between submitting the key and
    /// the host's [`crate::ChatEvent::CredentialResult`].
    KeyInput {
        rows: Vec<crate::CatalogProviderRow>,
        provider: crate::CatalogProviderRow,
        origin: CredentialOrigin,
        value: String,
        busy: bool,
        error: Option<String>,
    },
    /// An OAuth login is running in the host. Device logins fill
    /// `device_code` with `(verification_uri, user_code)` when it arrives.
    LoginWait {
        rows: Vec<crate::CatalogProviderRow>,
        provider: crate::CatalogProviderRow,
        origin: CredentialOrigin,
        method: crate::LoginMethod,
        device_code: Option<(String, String)>,
    },
    /// The custom-provider form. `editing` carries the original provider id
    /// when this is an edit rather than a create.
    CustomForm {
        rows: Vec<crate::CatalogProviderRow>,
        form: Box<CustomForm>,
        editing: Option<String>,
        busy: CustomBusy,
        error: Option<String>,
    },
    /// The tool-kit step: installed kits plus the host's recommendations
    /// (EMO-611). Opened from the Home "Install tools" row or by the
    /// first-run offer after the first model lands.
    Kits {
        /// Catalog rows for the Esc return to Home; empty when the step was
        /// opened by the first-run offer (Esc then closes the window).
        rows: Vec<crate::CatalogProviderRow>,
        installed: Vec<crate::InstalledKitRow>,
        recommended: Vec<crate::RecommendedKitRow>,
        state: tuika::components::SelectState,
        /// An install is in flight; only Esc works (the install keeps
        /// running and reports to the transcript).
        busy: bool,
        error: Option<String>,
        /// The post-install status line ("installed pi; ...").
        notice: Option<String>,
    },
    /// A model list was requested for the chosen provider; the window closes
    /// into the (scoped) model picker when [`crate::ChatEvent::Models`]
    /// arrives.
    AwaitModels { provider_id: String },
}

/// One selectable row of the credential / provider-menu steps.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MenuOption {
    pub action: MenuAction,
    pub label: &'static str,
    pub hint: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MenuAction {
    PickModel,
    ReplaceKey,
    BrowserLogin,
    DeviceLogin,
    ClearSaved,
    EditCustom,
    DeleteCustom,
    Back,
}

/// The actions for one configured provider on the overview.
pub(crate) fn provider_menu_options(provider: &crate::CatalogProviderRow) -> Vec<MenuOption> {
    let mut options = vec![MenuOption {
        action: MenuAction::PickModel,
        label: "Pick a model",
        hint: "from this provider",
    }];
    if provider.auth_kind == "oauth" {
        options.push(MenuOption {
            action: MenuAction::BrowserLogin,
            label: "Sign in with browser",
            hint: "opens on this machine",
        });
        options.push(MenuOption {
            action: MenuAction::DeviceLogin,
            label: "Sign in with device code",
            hint: "works on headless terminals",
        });
        options.push(MenuOption {
            action: MenuAction::ClearSaved,
            label: "Clear saved login",
            hint: "remove the stored tokens",
        });
    } else {
        options.push(MenuOption {
            action: MenuAction::ReplaceKey,
            label: "Replace API key",
            hint: "stored by the server, never shown",
        });
        options.push(MenuOption {
            action: MenuAction::ClearSaved,
            label: "Clear saved key",
            hint: "remove this provider's key",
        });
    }
    if provider.custom {
        options.push(MenuOption {
            action: MenuAction::EditCustom,
            label: "Edit provider",
            hint: "change URL, api, models",
        });
        options.push(MenuOption {
            action: MenuAction::DeleteCustom,
            label: "Delete provider",
            hint: "remove the record and its key",
        });
    }
    options.push(MenuOption {
        action: MenuAction::Back,
        label: "Back",
        hint: "",
    });
    options
}

/// The credential choices for an OAuth provider.
pub(crate) fn oauth_options() -> Vec<MenuOption> {
    vec![
        MenuOption {
            action: MenuAction::BrowserLogin,
            label: "Sign in with browser",
            hint: "opens on this machine",
        },
        MenuOption {
            action: MenuAction::DeviceLogin,
            label: "Sign in with device code",
            hint: "works on headless terminals",
        },
        MenuOption {
            action: MenuAction::Back,
            label: "Back",
            hint: "",
        },
    ]
}

/// The rows the home overview lists: configured or custom providers, in
/// catalog order (the server sorts configured first).
pub(crate) fn overview_rows(rows: &[crate::CatalogProviderRow]) -> Vec<&crate::CatalogProviderRow> {
    rows.iter()
        .filter(|row| row.configured || row.custom)
        .collect()
}

/// Fixed actions appended under the overview rows.
pub(crate) const HOME_ACTIONS: [(&str, &str); 3] = [
    ("Connect a provider", "browse the catalog"),
    ("Add custom provider", "OpenAI- or Anthropic-compatible URL"),
    ("Install tools", "kits of agent tools for this instance"),
];

/// One selectable row of the kit step, ahead of the fixed Back row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum KitOption {
    /// Install `recommended[index]` (only rows with a found source and no
    /// installed record become options).
    Install { index: usize },
}

/// The selectable rows of the kit step. The Back row is appended by the
/// renderer and handler as index `options.len()`.
pub(crate) fn kit_step_options(
    installed: &[crate::InstalledKitRow],
    recommended: &[crate::RecommendedKitRow],
) -> Vec<KitOption> {
    recommended
        .iter()
        .enumerate()
        .filter(|(_, kit)| {
            kit.source.is_some() && !installed.iter().any(|record| record.name == kit.name)
        })
        .map(|(index, _)| KitOption::Install { index })
        .collect()
}

/// The status suffix for a catalog row, pi-style.
pub(crate) fn catalog_status(row: &crate::CatalogProviderRow) -> String {
    if row.configured {
        if row.auth_label.is_empty() {
            "✓ configured".to_string()
        } else {
            format!("✓ {}", row.auth_label)
        }
    } else if row.auth_kind == "oauth" {
        "sign in".to_string()
    } else {
        "API key".to_string()
    }
}

/// The one-line summary for an overview row: auth source, model count, and
/// base URL for custom entries.
pub(crate) fn overview_status(row: &crate::CatalogProviderRow) -> String {
    let mut status = catalog_status(row);
    if row.custom {
        status.push_str(&format!(" · {}", row.base_url));
    }
    if row.model_count > 0 {
        let plural = if row.model_count == 1 { "" } else { "s" };
        status.push_str(&format!(" · {} model{plural}", row.model_count));
    }
    status
}

/// Case-insensitive subsequence match, pi's fuzzy-filter behavior: every
/// query char must appear in order, not necessarily adjacent.
pub(crate) fn fuzzy_matches(haystack: &str, query: &str) -> bool {
    let mut chars = haystack.chars().flat_map(char::to_lowercase);
    query
        .chars()
        .flat_map(char::to_lowercase)
        .all(|needle| chars.any(|ch| ch == needle))
}

/// Catalog rows matching `filter`, substring matches (on name or id) first.
pub(crate) fn filtered_catalog<'a>(
    rows: &'a [crate::CatalogProviderRow],
    filter: &str,
) -> Vec<&'a crate::CatalogProviderRow> {
    let query = filter.trim().to_lowercase();
    if query.is_empty() {
        return rows.iter().collect();
    }
    let mut substring = Vec::new();
    let mut fuzzy = Vec::new();
    for row in rows {
        let name = row.display_name.to_lowercase();
        let id = row.provider_id.to_lowercase();
        if name.contains(&query) || id.contains(&query) {
            substring.push(row);
        } else if fuzzy_matches(&row.display_name, &query)
            || fuzzy_matches(&row.provider_id, &query)
        {
            fuzzy.push(row);
        }
    }
    substring.extend(fuzzy);
    substring
}

/// A kebab-case slug of a display name, for the custom form's derived id.
pub(crate) fn slugify(name: &str) -> String {
    let mut slug = String::new();
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
        } else if (ch == ' ' || ch == '-' || ch == '_' || ch == '.')
            && !slug.is_empty()
            && !slug.ends_with('-')
        {
            slug.push('-');
        }
    }
    slug.trim_end_matches('-').to_string()
}

/// Split raw model text on commas and whitespace, deduplicated in order.
pub(crate) fn split_model_ids(text: &str) -> Vec<String> {
    let mut models = Vec::new();
    for id in text.split([',', ' ', '\t', '\n']) {
        let id = id.trim();
        if !id.is_empty() && !models.iter().any(|existing| existing == id) {
            models.push(id.to_string());
        }
    }
    models
}

/// HTTP(S) base-URL validation: https for remote hosts, plain http only for
/// loopback/local hosts. Mirrors the app-server catalog's own sanitization.
pub(crate) fn validate_base_url(url: &str) -> Result<(), String> {
    if url.is_empty() {
        return Err("base URL is required".to_string());
    }
    let rest = if let Some(rest) = url.strip_prefix("https://") {
        rest
    } else if let Some(rest) = url.strip_prefix("http://") {
        let host = rest
            .split(['/', '?', '#'])
            .next()
            .unwrap_or("")
            .rsplit_once(':')
            .map_or_else(
                || rest.split(['/', '?', '#']).next().unwrap_or(""),
                |(host, port)| {
                    if port.chars().all(|ch| ch.is_ascii_digit()) {
                        host
                    } else {
                        rest.split(['/', '?', '#']).next().unwrap_or("")
                    }
                },
            )
            .trim_matches(['[', ']']);
        if !(host == "localhost" || host == "::1" || host == "0.0.0.0" || host.starts_with("127."))
        {
            return Err("plain http is only allowed for localhost".to_string());
        }
        rest
    } else {
        return Err("base URL must start with https:// (or http:// for localhost)".to_string());
    };
    if rest.split(['/', '?', '#']).next().unwrap_or("").is_empty() {
        return Err("base URL is missing a host".to_string());
    }
    Ok(())
}

impl crate::app::App {
    /// `/setup` and `/providers`: fetch the catalog and open the overview.
    pub(crate) fn open_setup_home(&mut self) {
        self.pending_selection = None;
        self.setup = Some(SetupStep::AwaitCatalog {
            intent: CatalogIntent::Home,
        });
        self.actions.push(crate::Action::FetchProviderCatalog);
    }

    /// First-run gate: no configured providers, open the catalog picker.
    pub(crate) fn apply_no_configured_providers(&mut self) {
        self.needs_provider = true;
        self.kit_offer_pending = true;
        if self.setup.is_none() && self.picker.is_none() {
            self.setup = Some(SetupStep::AwaitCatalog {
                intent: CatalogIntent::Catalog,
            });
            self.actions.push(crate::Action::FetchProviderCatalog);
        }
    }

    /// Route a chosen "needs login" model row into the window: remember the
    /// selection, fetch the catalog, and land on the provider's credential
    /// entry when it arrives.
    pub(crate) fn setup_for_model(&mut self, provider_id: String, model: String) {
        self.pending_selection = Some((provider_id.clone(), model));
        self.setup = Some(SetupStep::AwaitCatalog {
            intent: CatalogIntent::ForModel { provider_id },
        });
        self.actions.push(crate::Action::FetchProviderCatalog);
    }

    /// Fold an arrived catalog into the window.
    pub(crate) fn apply_provider_catalog(&mut self, rows: Vec<crate::CatalogProviderRow>) {
        if rows.iter().any(|row| row.configured) {
            self.needs_provider = false;
        }
        match self.setup.take() {
            Some(SetupStep::AwaitCatalog { intent }) => match intent {
                CatalogIntent::Home => self.open_home(rows),
                CatalogIntent::Catalog => self.open_catalog(rows),
                CatalogIntent::ForModel { provider_id } => {
                    let Some(provider) = rows
                        .iter()
                        .find(|row| row.provider_id == provider_id)
                        .cloned()
                    else {
                        self.pending_selection = None;
                        self.notice(
                            crate::cells::Tone::Error,
                            format!("provider {provider_id} is no longer available"),
                            Vec::new(),
                        );
                        return;
                    };
                    self.open_credential_entry(rows, provider, CredentialOrigin::Catalog);
                }
            },
            // A refresh with the window open: swap the rows in place. The
            // selection index is clamped by the next render.
            Some(SetupStep::Home { state, .. }) => {
                self.setup = Some(SetupStep::Home { rows, state })
            }
            Some(SetupStep::Catalog { filter, state, .. }) => {
                self.setup = Some(SetupStep::Catalog {
                    rows,
                    filter,
                    state,
                })
            }
            Some(SetupStep::ProviderMenu {
                provider,
                state,
                busy,
                error,
                ..
            }) => {
                // The provider may have changed (credential cleared) or
                // vanished (deleted); fall back to home in those cases.
                match rows
                    .iter()
                    .find(|row| row.provider_id == provider.provider_id)
                    .cloned()
                {
                    Some(refreshed) => {
                        self.setup = Some(SetupStep::ProviderMenu {
                            rows,
                            provider: refreshed,
                            state,
                            busy,
                            error,
                        })
                    }
                    None => self.open_home(rows),
                }
            }
            // Mid-flow steps keep their own provider snapshot; stash the
            // fresher rows for when the user backs out.
            Some(SetupStep::Credential {
                provider,
                origin,
                state,
                error,
                ..
            }) => {
                self.setup = Some(SetupStep::Credential {
                    rows,
                    provider,
                    origin,
                    state,
                    error,
                })
            }
            Some(SetupStep::KeyInput {
                provider,
                origin,
                value,
                busy,
                error,
                ..
            }) => {
                self.setup = Some(SetupStep::KeyInput {
                    rows,
                    provider,
                    origin,
                    value,
                    busy,
                    error,
                })
            }
            Some(SetupStep::LoginWait {
                provider,
                origin,
                method,
                device_code,
                ..
            }) => {
                self.setup = Some(SetupStep::LoginWait {
                    rows,
                    provider,
                    origin,
                    method,
                    device_code,
                })
            }
            Some(SetupStep::CustomForm {
                form,
                editing,
                busy,
                error,
                ..
            }) => {
                self.setup = Some(SetupStep::CustomForm {
                    rows,
                    form,
                    editing,
                    busy,
                    error,
                })
            }
            Some(SetupStep::Kits {
                installed,
                recommended,
                state,
                busy,
                error,
                notice,
                ..
            }) => {
                self.setup = Some(SetupStep::Kits {
                    rows,
                    installed,
                    recommended,
                    state,
                    busy,
                    error,
                    notice,
                })
            }
            // Stale: the window was dismissed while the fetch was in flight.
            step @ (None | Some(SetupStep::AwaitModels { .. })) => self.setup = step,
        }
    }

    fn open_home(&mut self, rows: Vec<crate::CatalogProviderRow>) {
        self.popup = None;
        let mut state = tuika::components::SelectState::new();
        let selected = overview_rows(&rows)
            .iter()
            .position(|row| row.active)
            .unwrap_or(0);
        state.select(Some(selected));
        self.setup = Some(SetupStep::Home { rows, state });
    }

    fn open_catalog(&mut self, rows: Vec<crate::CatalogProviderRow>) {
        self.popup = None;
        if rows.is_empty() {
            self.setup = None;
            self.pending_selection = None;
            self.notice(
                crate::cells::Tone::Error,
                "no providers available".to_string(),
                Vec::new(),
            );
            return;
        }
        let mut state = tuika::components::SelectState::new();
        state.select(Some(0));
        self.setup = Some(SetupStep::Catalog {
            rows,
            filter: String::new(),
            state,
        });
    }

    /// The credential entry point for a provider: key input for API-key
    /// providers, the sign-in method choice for OAuth ones.
    fn open_credential_entry(
        &mut self,
        rows: Vec<crate::CatalogProviderRow>,
        provider: crate::CatalogProviderRow,
        origin: CredentialOrigin,
    ) {
        self.popup = None;
        if provider.auth_kind == "oauth" {
            let mut state = tuika::components::SelectState::new();
            state.select(Some(0));
            self.setup = Some(SetupStep::Credential {
                rows,
                provider,
                origin,
                state,
                error: None,
            });
        } else {
            self.setup = Some(SetupStep::KeyInput {
                rows,
                provider,
                origin,
                value: String::new(),
                busy: false,
                error: None,
            });
        }
    }

    /// The window is modal while it has a visible step; every event lands
    /// here. Await states are invisible (a fetch is in flight) and do not
    /// capture input.
    pub(crate) fn setup_visible(&self) -> bool {
        !matches!(
            self.setup,
            None | Some(SetupStep::AwaitModels { .. }) | Some(SetupStep::AwaitCatalog { .. })
        )
    }

    pub(crate) fn handle_setup(&mut self, event: &tuika::event::Event) {
        let Some(step) = self.setup.take() else {
            return;
        };
        match step {
            SetupStep::Home { rows, state } => self.handle_home(event, rows, state),
            SetupStep::ProviderMenu {
                rows,
                provider,
                state,
                busy,
                error,
            } => self.handle_provider_menu(event, rows, provider, state, busy, error),
            SetupStep::Catalog {
                rows,
                filter,
                state,
            } => self.handle_catalog(event, rows, filter, state),
            SetupStep::Credential {
                rows,
                provider,
                origin,
                state,
                error,
            } => self.handle_credential(event, rows, provider, origin, state, error),
            SetupStep::KeyInput {
                rows,
                provider,
                origin,
                value,
                busy,
                error,
            } => self.handle_key_input(event, rows, provider, origin, value, busy, error),
            SetupStep::LoginWait {
                rows,
                provider,
                origin,
                method,
                device_code,
            } => self.handle_login_wait(event, rows, provider, origin, method, device_code),
            SetupStep::CustomForm {
                rows,
                form,
                editing,
                busy,
                error,
            } => self.handle_custom_form(event, rows, form, editing, busy, error),
            SetupStep::Kits {
                rows,
                installed,
                recommended,
                state,
                busy,
                error,
                notice,
            } => self.handle_kits(
                event,
                rows,
                installed,
                recommended,
                state,
                busy,
                error,
                notice,
            ),
            step @ (SetupStep::AwaitModels { .. } | SetupStep::AwaitCatalog { .. }) => {
                self.setup = Some(step);
            }
        }
    }

    fn handle_home(
        &mut self,
        event: &tuika::event::Event,
        rows: Vec<crate::CatalogProviderRow>,
        mut state: tuika::components::SelectState,
    ) {
        let overview_len = overview_rows(&rows).len();
        let total = overview_len + HOME_ACTIONS.len();
        match state.handle(event, total) {
            tuika::event::InputOutcome::Submitted => {
                let Some(index) = state.selected() else {
                    self.setup = Some(SetupStep::Home { rows, state });
                    return;
                };
                if index < overview_len {
                    let provider = overview_rows(&rows)[index].clone();
                    let mut menu_state = tuika::components::SelectState::new();
                    menu_state.select(Some(0));
                    self.setup = Some(SetupStep::ProviderMenu {
                        rows,
                        provider,
                        state: menu_state,
                        busy: false,
                        error: None,
                    });
                } else if index == overview_len {
                    self.open_catalog(rows);
                } else if index == overview_len + 1 {
                    self.setup = Some(SetupStep::CustomForm {
                        rows,
                        form: Box::new(CustomForm::new()),
                        editing: None,
                        busy: CustomBusy::Idle,
                        error: None,
                    });
                } else {
                    // "Install tools": stay on Home until the status
                    // arrives; ChatEvent::KitStatus opens the kit step.
                    self.actions.push(crate::Action::FetchKitStatus {
                        intent: crate::KitStatusIntent::Open,
                    });
                    self.setup = Some(SetupStep::Home { rows, state });
                }
            }
            tuika::event::InputOutcome::Cancelled => {
                self.setup = None;
                self.pending_selection = None;
            }
            _ => self.setup = Some(SetupStep::Home { rows, state }),
        }
    }

    fn handle_provider_menu(
        &mut self,
        event: &tuika::event::Event,
        rows: Vec<crate::CatalogProviderRow>,
        provider: crate::CatalogProviderRow,
        mut state: tuika::components::SelectState,
        busy: bool,
        error: Option<String>,
    ) {
        // While the delete is in flight only Esc (back to home) works.
        if busy {
            if matches!(event, tuika::event::Event::Key(key) if key.plain() && key.code == tuika::event::KeyCode::Esc)
            {
                self.open_home(rows);
            } else {
                self.setup = Some(SetupStep::ProviderMenu {
                    rows,
                    provider,
                    state,
                    busy,
                    error,
                });
            }
            return;
        }
        let options = provider_menu_options(&provider);
        match state.handle(event, options.len()) {
            tuika::event::InputOutcome::Submitted => {
                let action = state
                    .selected()
                    .and_then(|index| options.get(index))
                    .map(|option| option.action)
                    .unwrap_or(MenuAction::Back);
                match action {
                    MenuAction::PickModel => {
                        self.setup = Some(SetupStep::AwaitModels {
                            provider_id: provider.provider_id,
                        });
                        self.actions.push(crate::Action::ListModels);
                    }
                    MenuAction::ReplaceKey => {
                        self.setup = Some(SetupStep::KeyInput {
                            rows,
                            provider,
                            origin: CredentialOrigin::Menu,
                            value: String::new(),
                            busy: false,
                            error: None,
                        });
                    }
                    MenuAction::BrowserLogin | MenuAction::DeviceLogin => {
                        let method = if action == MenuAction::BrowserLogin {
                            crate::LoginMethod::Browser
                        } else {
                            crate::LoginMethod::Device
                        };
                        self.actions.push(crate::Action::StartLogin {
                            provider_id: provider.provider_id.clone(),
                            method,
                        });
                        self.setup = Some(SetupStep::LoginWait {
                            rows,
                            provider,
                            origin: CredentialOrigin::Menu,
                            method,
                            device_code: None,
                        });
                    }
                    MenuAction::ClearSaved => {
                        self.actions.push(crate::Action::ClearCredential {
                            provider_id: provider.provider_id.clone(),
                        });
                        self.setup = Some(SetupStep::ProviderMenu {
                            rows,
                            provider,
                            state,
                            busy: false,
                            error,
                        });
                    }
                    MenuAction::EditCustom => {
                        let form = Box::new(CustomForm::from_row(&provider));
                        self.setup = Some(SetupStep::CustomForm {
                            rows,
                            form,
                            editing: Some(provider.provider_id),
                            busy: CustomBusy::Idle,
                            error: None,
                        });
                    }
                    MenuAction::DeleteCustom => {
                        self.actions.push(crate::Action::DeleteCustomProvider {
                            provider_id: provider.provider_id.clone(),
                        });
                        self.setup = Some(SetupStep::ProviderMenu {
                            rows,
                            provider,
                            state,
                            busy: true,
                            error: None,
                        });
                    }
                    MenuAction::Back => self.open_home(rows),
                }
            }
            tuika::event::InputOutcome::Cancelled => self.open_home(rows),
            _ => {
                self.setup = Some(SetupStep::ProviderMenu {
                    rows,
                    provider,
                    state,
                    busy,
                    error,
                })
            }
        }
    }

    fn handle_catalog(
        &mut self,
        event: &tuika::event::Event,
        rows: Vec<crate::CatalogProviderRow>,
        mut filter: String,
        mut state: tuika::components::SelectState,
    ) {
        // Typing edits the filter; the select list only sees navigation keys.
        if let tuika::event::Event::Key(key) = event {
            match key.code {
                tuika::event::KeyCode::Char(ch) if !key.ctrl && !key.alt => {
                    filter.push(ch);
                    state.select(Some(0));
                    self.setup = Some(SetupStep::Catalog {
                        rows,
                        filter,
                        state,
                    });
                    return;
                }
                tuika::event::KeyCode::Backspace => {
                    filter.pop();
                    state.select(Some(0));
                    self.setup = Some(SetupStep::Catalog {
                        rows,
                        filter,
                        state,
                    });
                    return;
                }
                _ => {}
            }
        }
        let filtered_len = filtered_catalog(&rows, &filter).len();
        match state.handle(event, filtered_len) {
            tuika::event::InputOutcome::Submitted => {
                let provider = state
                    .selected()
                    .and_then(|index| filtered_catalog(&rows, &filter).get(index).copied())
                    .cloned();
                let Some(provider) = provider else {
                    self.setup = Some(SetupStep::Catalog {
                        rows,
                        filter,
                        state,
                    });
                    return;
                };
                self.open_credential_entry(rows, provider, CredentialOrigin::Catalog);
            }
            tuika::event::InputOutcome::Cancelled => self.open_home(rows),
            _ => {
                self.setup = Some(SetupStep::Catalog {
                    rows,
                    filter,
                    state,
                })
            }
        }
    }

    fn handle_credential(
        &mut self,
        event: &tuika::event::Event,
        rows: Vec<crate::CatalogProviderRow>,
        provider: crate::CatalogProviderRow,
        origin: CredentialOrigin,
        mut state: tuika::components::SelectState,
        error: Option<String>,
    ) {
        let options = oauth_options();
        match state.handle(event, options.len()) {
            tuika::event::InputOutcome::Submitted => {
                let action = state
                    .selected()
                    .and_then(|index| options.get(index))
                    .map(|option| option.action)
                    .unwrap_or(MenuAction::Back);
                match action {
                    MenuAction::BrowserLogin | MenuAction::DeviceLogin => {
                        let method = if action == MenuAction::BrowserLogin {
                            crate::LoginMethod::Browser
                        } else {
                            crate::LoginMethod::Device
                        };
                        self.actions.push(crate::Action::StartLogin {
                            provider_id: provider.provider_id.clone(),
                            method,
                        });
                        self.setup = Some(SetupStep::LoginWait {
                            rows,
                            provider,
                            origin,
                            method,
                            device_code: None,
                        });
                    }
                    _ => self.credential_back(rows, provider, origin),
                }
            }
            tuika::event::InputOutcome::Cancelled => self.credential_back(rows, provider, origin),
            _ => {
                self.setup = Some(SetupStep::Credential {
                    rows,
                    provider,
                    origin,
                    state,
                    error,
                })
            }
        }
    }

    /// Esc from a credential-flow step: back to where the flow started.
    fn credential_back(
        &mut self,
        rows: Vec<crate::CatalogProviderRow>,
        provider: crate::CatalogProviderRow,
        origin: CredentialOrigin,
    ) {
        match origin {
            CredentialOrigin::Catalog => {
                let mut state = tuika::components::SelectState::new();
                state.select(
                    rows.iter()
                        .position(|row| row.provider_id == provider.provider_id)
                        .or(Some(0)),
                );
                self.setup = Some(SetupStep::Catalog {
                    rows,
                    filter: String::new(),
                    state,
                });
            }
            CredentialOrigin::Menu => {
                let mut state = tuika::components::SelectState::new();
                state.select(Some(0));
                self.setup = Some(SetupStep::ProviderMenu {
                    rows,
                    provider,
                    state,
                    busy: false,
                    error: None,
                });
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_key_input(
        &mut self,
        event: &tuika::event::Event,
        rows: Vec<crate::CatalogProviderRow>,
        provider: crate::CatalogProviderRow,
        origin: CredentialOrigin,
        mut value: String,
        busy: bool,
        error: Option<String>,
    ) {
        // While the key is being saved only Esc (back out) works; typing
        // into a submitted form would race the host's answer.
        if busy {
            if matches!(event, tuika::event::Event::Key(key) if key.plain() && key.code == tuika::event::KeyCode::Esc)
            {
                self.pending_selection = None;
                self.credential_back(rows, provider, origin);
            } else {
                self.setup = Some(SetupStep::KeyInput {
                    rows,
                    provider,
                    origin,
                    value,
                    busy,
                    error,
                });
            }
            return;
        }
        if let tuika::event::Event::Paste(pasted) = event {
            value.push_str(pasted.trim());
            self.setup = Some(SetupStep::KeyInput {
                rows,
                provider,
                origin,
                value,
                busy: false,
                error: None,
            });
            return;
        }
        let tuika::event::Event::Key(key) = event else {
            self.setup = Some(SetupStep::KeyInput {
                rows,
                provider,
                origin,
                value,
                busy,
                error,
            });
            return;
        };
        match key.code {
            tuika::event::KeyCode::Esc => self.credential_back(rows, provider, origin),
            tuika::event::KeyCode::Enter => {
                let trimmed = value.trim().to_string();
                if trimmed.is_empty() {
                    self.setup = Some(SetupStep::KeyInput {
                        rows,
                        provider,
                        origin,
                        value,
                        busy: false,
                        error: Some("API key is empty — paste a key, or press Esc".to_string()),
                    });
                    return;
                }
                self.actions.push(crate::Action::SetProviderKey {
                    provider_id: provider.provider_id.clone(),
                    api_key: trimmed.clone(),
                });
                self.push_key_redaction(provider.provider_id.clone(), trimmed);
                self.setup = Some(SetupStep::KeyInput {
                    rows,
                    provider,
                    origin,
                    value,
                    busy: true,
                    error: None,
                });
            }
            tuika::event::KeyCode::Backspace => {
                value.pop();
                self.setup = Some(SetupStep::KeyInput {
                    rows,
                    provider,
                    origin,
                    value,
                    busy: false,
                    error: None,
                });
            }
            tuika::event::KeyCode::Char(ch) if !key.ctrl => {
                value.push(ch);
                self.setup = Some(SetupStep::KeyInput {
                    rows,
                    provider,
                    origin,
                    value,
                    busy: false,
                    error: None,
                });
            }
            _ => {
                self.setup = Some(SetupStep::KeyInput {
                    rows,
                    provider,
                    origin,
                    value,
                    busy,
                    error,
                });
            }
        }
    }

    fn handle_login_wait(
        &mut self,
        event: &tuika::event::Event,
        rows: Vec<crate::CatalogProviderRow>,
        provider: crate::CatalogProviderRow,
        origin: CredentialOrigin,
        method: crate::LoginMethod,
        device_code: Option<(String, String)>,
    ) {
        if matches!(event, tuika::event::Event::Key(key) if key.plain() && key.code == tuika::event::KeyCode::Esc)
        {
            self.actions.push(crate::Action::CancelLogin);
            self.pending_selection = None;
            let mut state = tuika::components::SelectState::new();
            state.select(Some(0));
            self.setup = Some(SetupStep::Credential {
                rows,
                provider,
                origin,
                state,
                error: Some("sign-in canceled".to_string()),
            });
        } else {
            self.setup = Some(SetupStep::LoginWait {
                rows,
                provider,
                origin,
                method,
                device_code,
            });
        }
    }

    fn handle_custom_form(
        &mut self,
        event: &tuika::event::Event,
        rows: Vec<crate::CatalogProviderRow>,
        mut form: Box<CustomForm>,
        editing: Option<String>,
        busy: CustomBusy,
        error: Option<String>,
    ) {
        // A submitted form only honors Esc until the host answers.
        if busy != CustomBusy::Idle {
            if matches!(event, tuika::event::Event::Key(key) if key.plain() && key.code == tuika::event::KeyCode::Esc)
            {
                self.open_home(rows);
            } else {
                self.setup = Some(SetupStep::CustomForm {
                    rows,
                    form,
                    editing,
                    busy,
                    error,
                });
            }
            return;
        }
        if let tuika::event::Event::Paste(pasted) = event {
            let field = form.focused();
            if let Some(value) = form.field_mut(field) {
                value.push_str(pasted.trim());
                if field == CustomField::Name && !form.id_touched {
                    form.id = slugify(&form.name);
                }
            }
            self.setup = Some(SetupStep::CustomForm {
                rows,
                form,
                editing,
                busy,
                error: None,
            });
            return;
        }
        let tuika::event::Event::Key(key) = event else {
            self.setup = Some(SetupStep::CustomForm {
                rows,
                form,
                editing,
                busy,
                error,
            });
            return;
        };
        match key.code {
            tuika::event::KeyCode::Esc => self.open_home(rows),
            tuika::event::KeyCode::Up | tuika::event::KeyCode::BackTab => {
                form.focus = form.focus.checked_sub(1).unwrap_or(CUSTOM_FIELDS - 1);
                self.setup = Some(SetupStep::CustomForm {
                    rows,
                    form,
                    editing,
                    busy,
                    error,
                });
            }
            tuika::event::KeyCode::Down | tuika::event::KeyCode::Tab => {
                form.focus = (form.focus + 1) % CUSTOM_FIELDS;
                self.setup = Some(SetupStep::CustomForm {
                    rows,
                    form,
                    editing,
                    busy,
                    error,
                });
            }
            tuika::event::KeyCode::Left | tuika::event::KeyCode::Right
                if form.focused() == CustomField::Api =>
            {
                let len = API_FAMILIES.len();
                form.api_index = if key.code == tuika::event::KeyCode::Right {
                    (form.api_index + 1) % len
                } else {
                    form.api_index.checked_sub(1).unwrap_or(len - 1)
                };
                self.setup = Some(SetupStep::CustomForm {
                    rows,
                    form,
                    editing,
                    busy,
                    error,
                });
            }
            tuika::event::KeyCode::Enter => match form.build_spec() {
                Ok(spec) => {
                    if !form.api_key.trim().is_empty() {
                        self.push_key_redaction(
                            spec.provider_id.clone(),
                            form.api_key.trim().to_string(),
                        );
                    }
                    self.actions
                        .push(crate::Action::UpsertCustomProvider { spec });
                    self.setup = Some(SetupStep::CustomForm {
                        rows,
                        form,
                        editing,
                        busy: CustomBusy::Upserting,
                        error: None,
                    });
                }
                Err(message) => {
                    self.setup = Some(SetupStep::CustomForm {
                        rows,
                        form,
                        editing,
                        busy,
                        error: Some(message),
                    });
                }
            },
            tuika::event::KeyCode::Backspace => {
                let field = form.focused();
                if let Some(value) = form.field_mut(field) {
                    value.pop();
                    if field == CustomField::Name && !form.id_touched {
                        form.id = slugify(&form.name);
                    } else if field == CustomField::Id {
                        form.id_touched = true;
                    }
                }
                self.setup = Some(SetupStep::CustomForm {
                    rows,
                    form,
                    editing,
                    busy,
                    error: None,
                });
            }
            tuika::event::KeyCode::Char(ch) if !key.ctrl => {
                let field = form.focused();
                if let Some(value) = form.field_mut(field) {
                    value.push(ch);
                    if field == CustomField::Name && !form.id_touched {
                        form.id = slugify(&form.name);
                    } else if field == CustomField::Id {
                        form.id_touched = true;
                    }
                }
                self.setup = Some(SetupStep::CustomForm {
                    rows,
                    form,
                    editing,
                    busy,
                    error: None,
                });
            }
            _ => {
                self.setup = Some(SetupStep::CustomForm {
                    rows,
                    form,
                    editing,
                    busy,
                    error,
                });
            }
        }
    }

    fn push_key_redaction(&mut self, provider_id: String, secret: String) {
        self.pending_key_redactions.push((provider_id, secret));
        // Bound the belt-and-braces list if answers never arrive.
        if self.pending_key_redactions.len() > 8 {
            self.pending_key_redactions.remove(0);
        }
    }

    /// Fold a host credential answer into the window.
    pub(crate) fn apply_credential_result(&mut self, provider_id: String, error: Option<String>) {
        let answered_provider = provider_id.clone();
        match self.setup.take() {
            Some(SetupStep::KeyInput {
                rows,
                provider,
                origin,
                busy: true,
                ..
            }) if provider.provider_id == provider_id => {
                if let Some(message) = error {
                    let message = self.redact_secrets(&message);
                    self.setup = Some(SetupStep::KeyInput {
                        rows,
                        provider,
                        origin,
                        value: String::new(),
                        busy: false,
                        error: Some(message),
                    });
                } else {
                    self.finish_credential(rows, provider);
                }
            }
            Some(SetupStep::LoginWait {
                rows,
                provider,
                origin,
                ..
            }) if provider.provider_id == provider_id => {
                if let Some(message) = error {
                    let message = self.redact_secrets(&message);
                    let mut state = tuika::components::SelectState::new();
                    state.select(Some(0));
                    self.setup = Some(SetupStep::Credential {
                        rows,
                        provider,
                        origin,
                        state,
                        error: Some(message),
                    });
                } else {
                    self.finish_credential(rows, provider);
                }
            }
            Some(SetupStep::CustomForm {
                rows,
                form,
                editing,
                busy: CustomBusy::SavingKey,
                ..
            }) if form.id.trim() == provider_id => {
                if let Some(message) = error {
                    let message = self.redact_secrets(&message);
                    self.setup = Some(SetupStep::CustomForm {
                        rows,
                        form,
                        editing,
                        busy: CustomBusy::Idle,
                        error: Some(message),
                    });
                } else {
                    let provider = self.custom_form_row(&rows, &form);
                    self.finish_credential(rows, provider);
                }
            }
            step => {
                // The window moved on (or was dismissed): report out of band.
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
                        crate::cells::Tone::Error,
                        format!("{provider_id}: credential failed"),
                        Vec::new(),
                    ),
                    None => {
                        self.needs_provider = false;
                        self.notice(
                            crate::cells::Tone::Info,
                            format!("{provider_id}: credential saved"),
                            Vec::new(),
                        )
                    }
                }
            }
        }
        // The submission is answered; its redaction entry has done its job.
        self.pending_key_redactions
            .retain(|(pending_provider, _)| pending_provider != &answered_provider);
    }

    /// The catalog row for a just-submitted custom form, synthesized when
    /// the refreshed catalog has not arrived yet.
    fn custom_form_row(
        &self,
        rows: &[crate::CatalogProviderRow],
        form: &CustomForm,
    ) -> crate::CatalogProviderRow {
        let provider_id = form.id.trim();
        rows.iter()
            .find(|row| row.provider_id == provider_id)
            .cloned()
            .unwrap_or_else(|| crate::CatalogProviderRow {
                provider_id: provider_id.to_string(),
                display_name: form.name.trim().to_string(),
                base_url: form.base_url.trim().to_string(),
                api: API_FAMILIES[form.api_index.min(API_FAMILIES.len() - 1)]
                    .0
                    .to_string(),
                auth_kind: "api_key".to_string(),
                env_vars: Vec::new(),
                configured: true,
                auth_label: "stored key".to_string(),
                custom: true,
                active: false,
                model_count: split_model_ids(&form.models).len(),
                default_model: split_model_ids(&form.models).into_iter().next(),
            })
    }

    /// A credential landed for `provider`: report it, then continue.
    fn finish_credential(
        &mut self,
        rows: Vec<crate::CatalogProviderRow>,
        provider: crate::CatalogProviderRow,
    ) {
        self.notice(
            crate::cells::Tone::Info,
            format!("{}: credential saved", provider.provider_id),
            Vec::new(),
        );
        self.continue_after_configure(rows, provider);
    }

    /// The provider became usable: re-issue the selection that routed us
    /// here, auto-select the provider's default model when the current model
    /// is unusable, or open the picker scoped to the provider.
    fn continue_after_configure(
        &mut self,
        rows: Vec<crate::CatalogProviderRow>,
        provider: crate::CatalogProviderRow,
    ) {
        let model_was_unusable = self.model_unusable();
        self.needs_provider = false;
        if let Some((pending_provider, model)) = self.pending_selection.take() {
            if pending_provider == provider.provider_id {
                self.setup = None;
                self.actions.push(crate::Action::SelectModel {
                    provider_id: pending_provider,
                    model,
                });
                return;
            }
            self.pending_selection = Some((pending_provider, model));
        }
        if model_was_unusable && let Some(model) = provider.default_model.clone() {
            self.setup = None;
            self.actions.push(crate::Action::SelectModel {
                provider_id: provider.provider_id,
                model,
            });
            return;
        }
        let _ = rows;
        self.setup = Some(SetupStep::AwaitModels {
            provider_id: provider.provider_id,
        });
        self.actions.push(crate::Action::ListModels);
    }

    /// Whether the active model cannot serve real turns: the first-run gate
    /// is up, or the launch echo pair is still selected.
    pub(crate) fn model_unusable(&self) -> bool {
        self.needs_provider || self.meta.model_label.starts_with("local/")
    }

    /// Fold a custom-provider upsert/delete answer into the window.
    pub(crate) fn apply_custom_provider_result(
        &mut self,
        provider_id: String,
        error: Option<String>,
    ) {
        match self.setup.take() {
            Some(SetupStep::CustomForm {
                rows,
                form,
                editing,
                busy: CustomBusy::Upserting,
                ..
            }) if form.id.trim() == provider_id => match error {
                Some(message) => {
                    let message = self.redact_secrets(&message);
                    self.setup = Some(SetupStep::CustomForm {
                        rows,
                        form,
                        editing,
                        busy: CustomBusy::Idle,
                        error: Some(message),
                    });
                }
                None => {
                    let api_key = form.api_key.trim().to_string();
                    if api_key.is_empty() {
                        self.notice(
                            crate::cells::Tone::Info,
                            format!("{provider_id}: provider saved"),
                            Vec::new(),
                        );
                        let provider = self.custom_form_row(&rows, &form);
                        // No key to save: continue as a configured provider.
                        self.continue_after_configure(rows, provider);
                    } else {
                        self.actions.push(crate::Action::SetProviderKey {
                            provider_id: provider_id.clone(),
                            api_key,
                        });
                        self.setup = Some(SetupStep::CustomForm {
                            rows,
                            form,
                            editing,
                            busy: CustomBusy::SavingKey,
                            error: None,
                        });
                    }
                }
            },
            Some(SetupStep::ProviderMenu {
                rows,
                provider,
                state,
                busy: true,
                ..
            }) if provider.provider_id == provider_id => match error {
                Some(message) => {
                    let message = self.redact_secrets(&message);
                    self.setup = Some(SetupStep::ProviderMenu {
                        rows,
                        provider,
                        state,
                        busy: false,
                        error: Some(message),
                    });
                }
                None => {
                    self.notice(
                        crate::cells::Tone::Info,
                        format!("{provider_id}: provider deleted"),
                        Vec::new(),
                    );
                    // Refresh the overview so the deleted row disappears.
                    self.setup = Some(SetupStep::AwaitCatalog {
                        intent: CatalogIntent::Home,
                    });
                    self.actions.push(crate::Action::FetchProviderCatalog);
                }
            },
            step => {
                self.setup = step;
                match error {
                    Some(_) => self.notice(
                        crate::cells::Tone::Error,
                        format!("{provider_id}: provider change failed"),
                        Vec::new(),
                    ),
                    None => self.notice(
                        crate::cells::Tone::Info,
                        format!("{provider_id}: provider saved"),
                        Vec::new(),
                    ),
                }
            }
        }
    }

    pub(crate) fn apply_device_code(&mut self, verification_uri: String, user_code: String) {
        if let Some(SetupStep::LoginWait {
            method: crate::LoginMethod::Device,
            device_code,
            ..
        }) = self.setup.as_mut()
        {
            *device_code = Some((verification_uri, user_code));
        }
    }

    pub(crate) fn apply_credential_cleared(&mut self, provider_id: String) {
        self.notice(
            crate::cells::Tone::Info,
            format!("{provider_id}: credential cleared"),
            Vec::new(),
        );
        // Refresh whatever screen shows auth state.
        match self.setup.as_ref() {
            Some(SetupStep::ProviderMenu { provider, .. })
                if provider.provider_id == provider_id =>
            {
                self.actions.push(crate::Action::FetchProviderCatalog);
            }
            Some(SetupStep::Home { .. } | SetupStep::Catalog { .. }) => {
                self.actions.push(crate::Action::FetchProviderCatalog);
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
                origin,
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
                        origin,
                        value: String::new(),
                        busy: false,
                        error: Some(error_summary(&title, &body)),
                    });
                } else {
                    self.setup = Some(SetupStep::KeyInput {
                        rows,
                        provider,
                        origin,
                        value,
                        busy,
                        error,
                    });
                }
            }
            Some(SetupStep::LoginWait {
                rows,
                provider,
                origin,
                ..
            }) => {
                let mut state = tuika::components::SelectState::new();
                state.select(Some(0));
                self.setup = Some(SetupStep::Credential {
                    rows,
                    provider,
                    origin,
                    state,
                    error: Some(error_summary(&title, &body)),
                });
            }
            Some(SetupStep::CustomForm {
                rows,
                mut form,
                editing,
                busy,
                error,
            }) => {
                let secret = form.api_key.trim().to_string();
                if !secret.is_empty() {
                    title = title.replace(&secret, "[redacted]");
                    for line in &mut body {
                        *line = line.replace(&secret, "[redacted]");
                    }
                }
                if busy != CustomBusy::Idle {
                    form.api_key.clear();
                    self.setup = Some(SetupStep::CustomForm {
                        rows,
                        form,
                        editing,
                        busy: CustomBusy::Idle,
                        error: Some(error_summary(&title, &body)),
                    });
                } else {
                    self.setup = Some(SetupStep::CustomForm {
                        rows,
                        form,
                        editing,
                        busy,
                        error,
                    });
                }
            }
            Some(SetupStep::AwaitModels { .. } | SetupStep::AwaitCatalog { .. }) => {
                self.setup = None;
                self.pending_selection = None;
            }
            step => self.setup = step,
        }
        self.notice(crate::cells::Tone::Error, title, body);
    }

    fn open_kits(
        &mut self,
        rows: Vec<crate::CatalogProviderRow>,
        installed: Vec<crate::InstalledKitRow>,
        recommended: Vec<crate::RecommendedKitRow>,
    ) {
        self.popup = None;
        let mut state = tuika::components::SelectState::new();
        state.select(Some(0));
        self.setup = Some(SetupStep::Kits {
            rows,
            installed,
            recommended,
            state,
            busy: false,
            error: None,
            notice: None,
        });
    }

    /// Fold an arrived kit status into the window per the fetch intent.
    pub(crate) fn apply_kit_status(
        &mut self,
        intent: crate::KitStatusIntent,
        installed: Vec<crate::InstalledKitRow>,
        recommended: Vec<crate::RecommendedKitRow>,
    ) {
        match self.setup.take() {
            // The step is open (install-result refresh): swap the lists in
            // place, keeping the busy flag and status lines.
            Some(SetupStep::Kits {
                rows,
                state,
                busy,
                error,
                notice,
                ..
            }) => {
                self.setup = Some(SetupStep::Kits {
                    rows,
                    installed,
                    recommended,
                    state,
                    busy,
                    error,
                    notice,
                })
            }
            // The Home "Install tools" row, or the first-run offer landing
            // while the window shows Home: open over the Home rows. The
            // offer only opens when a recommended kit is actually missing.
            Some(SetupStep::Home { rows, state }) => {
                if intent == crate::KitStatusIntent::Open
                    || any_recommended_missing(&installed, &recommended)
                {
                    self.open_kits(rows, installed, recommended);
                } else {
                    self.setup = Some(SetupStep::Home { rows, state });
                }
            }
            None => {
                let open = match intent {
                    crate::KitStatusIntent::Open => true,
                    crate::KitStatusIntent::OfferIfMissing => {
                        any_recommended_missing(&installed, &recommended) && self.picker.is_none()
                    }
                };
                if open {
                    self.open_kits(Vec::new(), installed, recommended);
                }
            }
            // Stale: the user moved into another step while the fetch was
            // in flight; never clobber a mid-flow screen.
            step => self.setup = step,
        }
    }

    /// An install finished: always post the receipt (or failure) to the
    /// transcript; update the kit step if it is still open.
    pub(crate) fn apply_kit_install_result(
        &mut self,
        name: String,
        error: Option<String>,
        receipt: Vec<String>,
    ) {
        match &error {
            Some(message) => self.notice(
                crate::cells::Tone::Error,
                format!("kit install failed: {name}"),
                vec![message.clone()],
            ),
            None => self.notice(
                crate::cells::Tone::Info,
                format!("installed kit {name}"),
                receipt,
            ),
        }
        if let Some(SetupStep::Kits {
            rows,
            installed,
            recommended,
            state,
            notice,
            ..
        }) = self.setup.take()
        {
            let succeeded = error.is_none();
            self.setup = Some(SetupStep::Kits {
                rows,
                installed,
                recommended,
                state,
                busy: false,
                error,
                notice: if succeeded {
                    Some(format!(
                        "installed {name}; tools load at the next daemon startup"
                    ))
                } else {
                    notice
                },
            });
            if succeeded {
                // Refresh so the installed list reflects the new record.
                self.actions.push(crate::Action::FetchKitStatus {
                    intent: crate::KitStatusIntent::Open,
                });
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_kits(
        &mut self,
        event: &tuika::event::Event,
        rows: Vec<crate::CatalogProviderRow>,
        installed: Vec<crate::InstalledKitRow>,
        recommended: Vec<crate::RecommendedKitRow>,
        mut state: tuika::components::SelectState,
        busy: bool,
        error: Option<String>,
        notice: Option<String>,
    ) {
        // While the install runs only Esc works; leaving is safe (the
        // install keeps running and reports to the transcript).
        if busy {
            if matches!(event, tuika::event::Event::Key(key) if key.plain() && key.code == tuika::event::KeyCode::Esc)
            {
                self.leave_kits(rows);
            } else {
                self.setup = Some(SetupStep::Kits {
                    rows,
                    installed,
                    recommended,
                    state,
                    busy,
                    error,
                    notice,
                });
            }
            return;
        }
        let options = kit_step_options(&installed, &recommended);
        let total = options.len() + 1;
        match state.handle(event, total) {
            tuika::event::InputOutcome::Submitted => {
                match state.selected().and_then(|index| options.get(index)) {
                    Some(KitOption::Install { index }) => {
                        let kit = &recommended[*index];
                        let Some(source) = kit.source.clone() else {
                            // Unreachable by construction (options require a
                            // source); keep the step unchanged if it happens.
                            self.setup = Some(SetupStep::Kits {
                                rows,
                                installed,
                                recommended,
                                state,
                                busy,
                                error,
                                notice,
                            });
                            return;
                        };
                        self.actions.push(crate::Action::InstallKit {
                            name: kit.name.clone(),
                            source,
                        });
                        self.setup = Some(SetupStep::Kits {
                            rows,
                            installed,
                            recommended,
                            state,
                            busy: true,
                            error: None,
                            notice: None,
                        });
                    }
                    // The trailing Back row (or no selection).
                    _ => self.leave_kits(rows),
                }
            }
            tuika::event::InputOutcome::Cancelled => self.leave_kits(rows),
            _ => {
                self.setup = Some(SetupStep::Kits {
                    rows,
                    installed,
                    recommended,
                    state,
                    busy,
                    error,
                    notice,
                });
            }
        }
    }

    /// Esc/Back from the kit step: Home when it was entered from Home,
    /// closed when the first-run offer opened it directly.
    fn leave_kits(&mut self, rows: Vec<crate::CatalogProviderRow>) {
        if rows.is_empty() {
            self.setup = None;
        } else {
            self.open_home(rows);
        }
    }
}

fn any_recommended_missing(
    installed: &[crate::InstalledKitRow],
    recommended: &[crate::RecommendedKitRow],
) -> bool {
    recommended
        .iter()
        .any(|kit| !installed.iter().any(|record| record.name == kit.name))
}

fn error_summary(title: &str, body: &[String]) -> String {
    body.first()
        .map(|line| format!("{title}: {line}"))
        .unwrap_or_else(|| title.to_string())
}
