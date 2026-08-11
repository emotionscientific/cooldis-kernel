//! The view tree: transcript, working indicator, popup, composer, footer,
//! and the modal setup / model-picker window.
//!
//! Ported from tuika's `codex` example (`examples/codex/ui.rs`). The whole
//! screen is one column. The transcript grows; everything below it is
//! measured first and pinned — the composer never moves, and new output
//! pushes the history up instead of the input down. The setup window and the
//! model picker render as a centered [`Dialog`] overlay that dims the base
//! tree underneath.

use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};

use tuika::components::Dialog;
use tuika::prelude::*;
use tuika::probe::RectProbe;

use crate::app::App;
use crate::app::setup::{
    API_FAMILIES, CUSTOM_FIELD_ORDER, CustomBusy, CustomField, CustomForm, HOME_ACTIONS, SetupStep,
    catalog_status, filtered_catalog, oauth_options, overview_rows, overview_status,
    provider_menu_options,
};
use crate::cells::short_id;
use crate::{CatalogProviderRow, LoginMethod};

/// Columns of blank kept down each side of the UI.
pub const GUTTER: u16 = 1;
/// Visual rows the composer grows to before it scrolls internally.
const MAX_COMPOSER_ROWS: u16 = 6;
/// Rows the completion popup shows before it windows around the selection.
const MAX_POPUP_ROWS: usize = 8;
/// Rows a modal list shows before it windows around the selection.
const MAX_MODAL_ROWS: usize = 12;
/// Blank rows between two transcript items.
const TRANSCRIPT_GAP: u16 = 1;
/// The modal window's outer width, clamped to the terminal.
const MODAL_WIDTH: u16 = 72;
/// Columns a select list spends on its `› ` selection marker.
const SELECT_MARKER_W: u16 = 2;

/// Build the frame. Takes `&mut App` because the transcript's streaming
/// markdown re-renders through its own cache, and because the scroll offset
/// is reconciled against this frame's geometry.
pub fn build(
    app: &mut App,
    area: Rect,
    theme: &Theme,
    sheet: &StyleSheet,
    probe: &RectProbe,
) -> Scene {
    let width = area.width.saturating_sub(GUTTER * 2);

    // Measure the pinned bottom stack first; the transcript takes what is left.
    let popup_items = app.popup_items();
    let popup_h = if popup_items.is_empty() {
        0
    } else {
        popup_items.len().min(MAX_POPUP_ROWS) as u16 + 1
    };
    let working_h = if app.turn_active() { 2 } else { 0 };
    let composer_rows = app
        .composer
        .visual_height(width.saturating_sub(4))
        .clamp(1, MAX_COMPOSER_ROWS);
    let body_h = composer_rows + 2;
    let bottom_h = working_h + popup_h + body_h + 1;
    let transcript_h = area.height.saturating_sub(bottom_h).max(1);

    // Reconcile the scroll offset with this frame's dimensions, then stash
    // them so `PageUp`/`PageDown` have geometry to work against before the
    // next one. Item heights are measured the way the viewport will measure
    // them — same width, same scrollbar setting — so the offset can't drift
    // from the paint.
    let items = app.transcript(width, theme, sheet);
    let ctx = RenderCtx::new(theme).with_sheet(*sheet);
    app.content_h = ItemScroll::measure_height(&items, width, TRANSCRIPT_GAP, true, &ctx);
    app.viewport_h = transcript_h as usize;
    app.scroll.clamp(app.content_h, app.viewport_h);

    let mut root = Flex::column()
        .padding(Padding::symmetric(GUTTER, 0))
        .background(Style::default().bg(theme.background))
        .grow(
            1,
            element(ItemScroll::new(items, &app.scroll).gap(TRANSCRIPT_GAP)),
        );
    if working_h > 0 {
        root = root.fixed(working_h, working(app, theme));
    }
    if popup_h > 0 {
        root = root.fixed(popup_h, popup(app, &popup_items, theme));
    }
    root = root.fixed(body_h, composer(app, theme, probe));
    root = root.fixed(1, footer(app, theme));

    let mut scene = Scene::new(element(root));
    if let Some(dialog) = modal(app, area, theme) {
        scene = scene.dialog(dialog);
    }
    scene
}

/// `⠹ Working (12s • Esc to interrupt)` — the row shown under the transcript
/// while a turn is in flight.
fn working(app: &App, theme: &Theme) -> Element {
    let label = Line::from(vec![
        Span::styled(
            "Working",
            Style::default().fg(theme.text).add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            // While the picker or the setup window is open it owns Esc
            // (dismiss, not interrupt), so the interrupt hint would lie.
            if app.picker.is_some() || app.setup_visible() {
                format!(" ({}s)", app.elapsed_secs())
            } else {
                format!(" ({}s • Esc to interrupt)", app.elapsed_secs())
            },
            theme.muted_style(),
        ),
    ]);
    let row = view! {
        row(gap = 1) {
            fixed(1) { node(Spinner::new(app.frame).color(theme.accent)) }
            grow(1) { node(Text::new(vec![label])) }
        }
    };
    element(Flex::column().fixed(1, element(Spacer)).fixed(1, row))
}

/// The slash-command completion picker.
fn popup(app: &App, items: &[(String, String)], theme: &Theme) -> Element {
    let pad = items
        .iter()
        .map(|(label, _)| label.chars().count())
        .max()
        .unwrap_or(0);
    let rows: Vec<Line<'static>> = items
        .iter()
        .map(|(label, blurb)| {
            Line::from(vec![
                Span::styled(
                    format!(
                        "{label}{:width$}  ",
                        "",
                        width = pad - label.chars().count()
                    ),
                    Style::default().fg(theme.accent_alt),
                ),
                Span::styled(blurb.clone(), theme.muted_style()),
            ])
        })
        .collect();
    let Some(state) = app.popup.as_ref().map(|popup| popup.state) else {
        return element(Spacer);
    };
    let list = SelectList::new(rows, &state).viewport(MAX_POPUP_ROWS as u16);
    element(
        Flex::column()
            .fixed(1, element(Spacer))
            .grow(1, element(list)),
    )
}

/// The modal window, when one is open: the setup screens or the model
/// picker, composed as a centered dialog over the dimmed base tree.
fn modal(app: &App, area: Rect, theme: &Theme) -> Option<Dialog> {
    let width = MODAL_WIDTH.min(area.width.saturating_sub(4)).max(24);
    let content_w = width.saturating_sub(4);
    let (title, content, content_h, hints): (String, Element, u16, Vec<(&str, &str)>) =
        if let Some(step) = app.setup.as_ref() {
            match step {
                SetupStep::AwaitCatalog { .. } | SetupStep::AwaitModels { .. } => return None,
                SetupStep::Home { rows, state } => {
                    let (content, rows_h) = setup_home(rows, state, theme, content_w);
                    (
                        "Providers".to_string(),
                        content,
                        rows_h,
                        vec![("↑↓", "move"), ("⏎", "select"), ("esc", "close")],
                    )
                }
                SetupStep::ProviderMenu {
                    provider,
                    state,
                    busy,
                    error,
                    ..
                } => {
                    let (content, rows_h) =
                        setup_provider_menu(provider, state, *busy, error.as_deref(), theme);
                    (
                        provider.display_name.clone(),
                        content,
                        rows_h,
                        vec![("↑↓", "move"), ("⏎", "select"), ("esc", "back")],
                    )
                }
                SetupStep::Catalog {
                    rows,
                    filter,
                    state,
                } => {
                    let (content, rows_h) = setup_catalog(rows, filter, state, theme, content_w);
                    (
                        "Connect a provider".to_string(),
                        content,
                        rows_h,
                        vec![("type", "search"), ("⏎", "select"), ("esc", "back")],
                    )
                }
                SetupStep::Credential {
                    provider,
                    state,
                    error,
                    ..
                } => {
                    let (content, rows_h) = setup_oauth_options(state, error.as_deref(), theme);
                    (
                        format!("Sign in to {}", provider.display_name),
                        content,
                        rows_h,
                        vec![("↑↓", "move"), ("⏎", "select"), ("esc", "back")],
                    )
                }
                SetupStep::KeyInput {
                    provider,
                    value,
                    busy,
                    error,
                    ..
                } => {
                    let (content, rows_h) =
                        setup_key_input(provider, value, *busy, error.as_deref(), theme, content_w);
                    (
                        format!("Connect {}", provider.display_name),
                        content,
                        rows_h,
                        vec![("⏎", "save"), ("esc", "back")],
                    )
                }
                SetupStep::LoginWait {
                    provider,
                    method,
                    device_code,
                    ..
                } => {
                    let (content, rows_h) =
                        setup_login_wait(*method, device_code.as_ref(), app, theme);
                    (
                        format!("Sign in to {}", provider.display_name),
                        content,
                        rows_h,
                        vec![("esc", "cancel")],
                    )
                }
                SetupStep::CustomForm {
                    form, busy, error, ..
                } => {
                    let (content, rows_h) =
                        setup_custom_form(form, *busy, error.as_deref(), theme, content_w);
                    (
                        "Custom provider".to_string(),
                        content,
                        rows_h,
                        vec![
                            ("↑↓/⇥", "field"),
                            ("←→", "api"),
                            ("⏎", "save"),
                            ("esc", "back"),
                        ],
                    )
                }
            }
        } else if let Some(picker) = app.picker.as_ref() {
            let (content, rows_h) = model_picker(picker, theme, content_w);
            (
                "Select a model".to_string(),
                content,
                rows_h,
                vec![("↑↓", "move"), ("⏎", "select"), ("esc", "close")],
            )
        } else {
            return None;
        };
    // Dialog chrome: border rows (2) + actions row.
    let height = (content_h + 3).min(area.height.saturating_sub(2));
    Some(
        Dialog::new(title, content)
            .size(width, height)
            .key_hints(hints)
            .dim_backdrop(true),
    )
}

/// The provider overview: configured providers plus the two entry actions.
fn setup_home(
    rows: &[CatalogProviderRow],
    state: &SelectState,
    theme: &Theme,
    width: u16,
) -> (Element, u16) {
    let overview = overview_rows(rows);
    let total_rows = overview.len() + HOME_ACTIONS.len();
    let scrollbar_w = u16::from(total_rows > MAX_MODAL_ROWS);
    let usable = width
        .saturating_sub(SELECT_MARKER_W)
        .saturating_sub(scrollbar_w);
    let max_suffix_w = u16::from(overview.iter().any(|row| row.active)) * 8;
    let max_name_w = overview
        .iter()
        .map(|row| tuika::width::str_cols(&row.display_name))
        .chain(
            HOME_ACTIONS
                .iter()
                .map(|(label, _)| tuika::width::str_cols(label)),
        )
        .max()
        .unwrap_or(0);
    let name_w = max_name_w.min(usable.saturating_sub(max_suffix_w.saturating_add(2)) / 2);
    let mut lines: Vec<Line<'static>> = overview
        .iter()
        .map(|row| {
            let name = fit_columns(&row.display_name, name_w);
            let name_pad = name_w.saturating_sub(tuika::width::str_cols(&name));
            let suffix_w = if row.active { 8 } else { 0 };
            let status = fit_columns(
                &overview_status(row),
                usable
                    .saturating_sub(name_w)
                    .saturating_sub(2)
                    .saturating_sub(suffix_w),
            );
            let mut spans = vec![
                Span::styled(
                    format!("{name}{:width$}  ", "", width = usize::from(name_pad)),
                    Style::default().fg(theme.text),
                ),
                Span::styled(status, Style::default().fg(theme.accent)),
            ];
            if row.active {
                spans.push(Span::styled(
                    "  active",
                    Style::default()
                        .fg(theme.accent)
                        .add_modifier(Modifier::BOLD),
                ));
            }
            Line::from(spans)
        })
        .collect();
    for (label, hint) in HOME_ACTIONS {
        let name = fit_columns(label, name_w);
        let name_pad = name_w.saturating_sub(tuika::width::str_cols(&name));
        lines.push(Line::from(vec![
            Span::styled(
                format!("{name}{:width$}  ", "", width = usize::from(name_pad)),
                Style::default().fg(theme.accent_alt),
            ),
            Span::styled(hint.to_string(), theme.muted_style()),
        ]));
    }
    let total = lines.len();
    let mut column = Flex::column();
    if overview.is_empty() {
        column = column.fixed(
            1,
            element(Text::new(vec![Line::from(Span::styled(
                "No providers configured yet.",
                theme.muted_style(),
            ))])),
        );
    }
    let list = SelectList::new(lines, state).viewport(MAX_MODAL_ROWS as u16);
    let empty_h = u16::from(overview.is_empty());
    column = column.grow(1, element(list));
    (element(column), total.min(MAX_MODAL_ROWS) as u16 + empty_h)
}

/// The action list for one configured provider.
fn setup_provider_menu(
    provider: &CatalogProviderRow,
    state: &SelectState,
    busy: bool,
    error: Option<&str>,
    theme: &Theme,
) -> (Element, u16) {
    let options = provider_menu_options(provider);
    let lines: Vec<Line<'static>> = options
        .iter()
        .map(|option| {
            Line::from(vec![
                Span::styled(
                    format!("{}  ", option.label),
                    Style::default().fg(theme.text),
                ),
                Span::styled(option.hint.to_string(), theme.muted_style()),
            ])
        })
        .collect();
    let count = lines.len() as u16;
    let list = SelectList::new(lines, state).viewport(MAX_MODAL_ROWS as u16);
    let mut column = Flex::column()
        .fixed(
            1,
            element(Text::new(vec![Line::from(Span::styled(
                overview_status(provider),
                theme.muted_style(),
            ))])),
        )
        .grow(1, element(list));
    let mut height = count + 1;
    if busy {
        column = column.fixed(
            1,
            element(Text::new(vec![Line::from(Span::styled(
                "deleting…",
                theme.muted_style(),
            ))])),
        );
        height += 1;
    } else if let Some(message) = error {
        column = column.fixed(1, modal_error(message, theme));
        height += 1;
    }
    (element(column), height)
}

/// The searchable catalog list.
fn setup_catalog(
    rows: &[CatalogProviderRow],
    filter: &str,
    state: &SelectState,
    theme: &Theme,
    width: u16,
) -> (Element, u16) {
    let filtered = filtered_catalog(rows, filter);
    let scrollbar_w = u16::from(filtered.len() > MAX_MODAL_ROWS);
    let usable = width
        .saturating_sub(SELECT_MARKER_W)
        .saturating_sub(scrollbar_w);
    let max_name_w = filtered
        .iter()
        .map(|row| tuika::width::str_cols(&row.display_name))
        .max()
        .unwrap_or(0);
    let name_w = max_name_w.min(usable.saturating_sub(2) / 2);
    let lines: Vec<Line<'static>> = filtered
        .iter()
        .map(|row| {
            let name = fit_columns(&row.display_name, name_w);
            let name_pad = name_w.saturating_sub(tuika::width::str_cols(&name));
            let status = fit_columns(
                &catalog_status(row),
                usable.saturating_sub(name_w).saturating_sub(2),
            );
            let configured = row.configured;
            Line::from(vec![
                Span::styled(
                    format!("{name}{:width$}  ", "", width = usize::from(name_pad)),
                    Style::default().fg(theme.text),
                ),
                Span::styled(
                    status,
                    if configured {
                        Style::default().fg(theme.accent)
                    } else {
                        theme.muted_style()
                    },
                ),
            ])
        })
        .collect();
    let count = lines.len();
    let search = Line::from(vec![
        Span::styled("Search: ", theme.muted_style()),
        Span::styled(filter.to_string(), Style::default().fg(theme.text)),
        Span::styled("▏", Style::default().fg(theme.accent_alt)),
    ]);
    let mut column = Flex::column().fixed(1, element(Text::new(vec![search])));
    let mut height = 1;
    if count == 0 {
        column = column.fixed(
            1,
            element(Text::new(vec![Line::from(Span::styled(
                "no providers match",
                theme.muted_style(),
            ))])),
        );
        height += 1;
    } else {
        let list = SelectList::new(lines, state).viewport(MAX_MODAL_ROWS as u16);
        column = column.grow(1, element(list));
        height += count.min(MAX_MODAL_ROWS) as u16;
    }
    (element(column), height)
}

/// OAuth sign-in method options.
fn setup_oauth_options(state: &SelectState, error: Option<&str>, theme: &Theme) -> (Element, u16) {
    let options = oauth_options();
    let lines: Vec<Line<'static>> = options
        .iter()
        .map(|option| {
            Line::from(vec![
                Span::styled(
                    format!("{}  ", option.label),
                    Style::default().fg(theme.text),
                ),
                Span::styled(option.hint.to_string(), theme.muted_style()),
            ])
        })
        .collect();
    let count = lines.len() as u16;
    let list = SelectList::new(lines, state).viewport(MAX_MODAL_ROWS as u16);
    let mut column = Flex::column().grow(1, element(list));
    let mut height = count;
    if let Some(message) = error {
        column = column.fixed(1, modal_error(message, theme));
        height += 1;
    }
    (element(column), height)
}

fn modal_error(message: &str, theme: &Theme) -> Element {
    // Matches the notice-cell error glyph and color in `cells.rs`.
    element(Text::new(vec![Line::from(vec![
        Span::styled("✗ ", Style::default().fg(theme.accent)),
        Span::styled(message.to_string(), Style::default().fg(theme.text)),
    ])]))
}

/// Masked API-key entry.
fn setup_key_input(
    provider: &CatalogProviderRow,
    value: &str,
    busy: bool,
    error: Option<&str>,
    theme: &Theme,
    width: u16,
) -> (Element, u16) {
    // The key never renders: a masked run of bullets stands in for it, and
    // overlong keys clip from the left so the caret edge stays visible.
    let masked = "•".repeat(
        value
            .chars()
            .count()
            .min(usize::from(width.saturating_sub(4))),
    );
    let mut lines = vec![Line::from(vec![
        Span::styled("Paste the API key", theme.muted_style()),
        Span::styled(
            provider
                .env_vars
                .first()
                .map(|env| format!("  (or set {env})"))
                .unwrap_or_default(),
            theme.muted_style(),
        ),
    ])];
    lines.push(Line::from(vec![
        Span::styled("› ", Style::default().fg(theme.accent_alt)),
        Span::styled(masked, Style::default().fg(theme.text)),
    ]));
    let mut height = 2;
    if let Some(message) = error {
        lines.push(Line::from(vec![
            Span::styled("✗ ", Style::default().fg(theme.accent)),
            Span::styled(message.to_string(), Style::default().fg(theme.text)),
        ]));
        height += 1;
    } else if busy {
        lines.push(Line::from(Span::styled("saving…", theme.muted_style())));
        height += 1;
    }
    (element(Text::new(lines)), height)
}

/// The login-wait body: device code once it arrives, spinner otherwise.
fn setup_login_wait(
    method: LoginMethod,
    device_code: Option<&(String, String)>,
    app: &App,
    theme: &Theme,
) -> (Element, u16) {
    match (method, device_code) {
        (LoginMethod::Device, Some((uri, code))) => {
            let lines = vec![
                Line::from(vec![
                    Span::styled("Open ".to_string(), theme.muted_style()),
                    Span::styled(uri.clone(), Style::default().fg(theme.text)),
                ]),
                Line::from(vec![
                    Span::styled("Enter code ".to_string(), theme.muted_style()),
                    Span::styled(
                        code.clone(),
                        Style::default()
                            .fg(theme.accent)
                            .add_modifier(Modifier::BOLD),
                    ),
                ]),
            ];
            (element(Text::new(lines)), 2)
        }
        _ => {
            let waiting = match method {
                LoginMethod::Browser => "waiting for the browser sign-in to finish",
                LoginMethod::Device => "requesting a device code",
            };
            let row = view! {
                row(gap = 1) {
                    fixed(1) { node(Spinner::new(app.frame).color(theme.accent)) }
                    grow(1) { node(Text::new(vec![Line::from(Span::styled(
                        waiting.to_string(),
                        theme.muted_style(),
                    ))])) }
                }
            };
            (element(Flex::column().fixed(1, row)), 1)
        }
    }
}

/// The custom-provider form: one row per field, focus marked with `›`.
fn setup_custom_form(
    form: &CustomForm,
    busy: CustomBusy,
    error: Option<&str>,
    theme: &Theme,
    width: u16,
) -> (Element, u16) {
    let label_w = 10u16;
    let value_w = usize::from(width.saturating_sub(label_w).saturating_sub(4));
    let focused = form.focused();
    let mut lines: Vec<Line<'static>> = Vec::new();
    for field in CUSTOM_FIELD_ORDER {
        let (label, value, muted_hint): (&str, String, &str) = match field {
            CustomField::Name => ("name", form.name.clone(), "display name"),
            CustomField::Id => ("id", form.id.clone(), "slug, a-z 0-9 - _"),
            CustomField::Api => (
                "api",
                format!(
                    "‹ {} ›",
                    API_FAMILIES[form.api_index.min(API_FAMILIES.len() - 1)].1
                ),
                "",
            ),
            CustomField::BaseUrl => ("base URL", form.base_url.clone(), "https://…"),
            CustomField::ApiKey => (
                "API key",
                "•".repeat(form.api_key.chars().count().min(value_w)),
                "optional for local servers",
            ),
            CustomField::HeaderName => ("header", form.header_name.clone(), "optional"),
            CustomField::HeaderValue => ("value", form.header_value.clone(), "optional"),
            CustomField::Models => ("models", form.models.clone(), "ids, first is default"),
        };
        let is_focused = field == focused;
        let marker = if is_focused { "› " } else { "  " };
        let shown = if value.is_empty() && !is_focused {
            Span::styled(muted_hint.to_string(), Style::default().fg(theme.dim))
        } else {
            Span::styled(
                fit_columns_left(&value, value_w as u16),
                if is_focused {
                    Style::default().fg(theme.text).add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(theme.text)
                },
            )
        };
        lines.push(Line::from(vec![
            Span::styled(marker.to_string(), Style::default().fg(theme.accent_alt)),
            Span::styled(
                format!("{label:<width$}", width = usize::from(label_w)),
                theme.muted_style(),
            ),
            shown,
        ]));
    }
    let mut height = lines.len() as u16;
    match busy {
        CustomBusy::Upserting => {
            lines.push(Line::from(Span::styled(
                "saving provider…",
                theme.muted_style(),
            )));
            height += 1;
        }
        CustomBusy::SavingKey => {
            lines.push(Line::from(Span::styled("saving key…", theme.muted_style())));
            height += 1;
        }
        CustomBusy::Idle => {
            if let Some(message) = error {
                lines.push(Line::from(vec![
                    Span::styled("✗ ", Style::default().fg(theme.accent)),
                    Span::styled(message.to_string(), Style::default().fg(theme.text)),
                ]));
                height += 1;
            }
        }
    }
    (element(Text::new(lines)), height)
}

/// The `/models` picker inside the modal frame. Auth problems and the active
/// selection are annotated per row; the width columns align on the longest
/// display name.
fn model_picker(picker: &crate::app::ModelPicker, theme: &Theme, width: u16) -> (Element, u16) {
    let scrollbar_w = u16::from(picker.rows.len() > MAX_MODAL_ROWS);
    let content_w = width
        .saturating_sub(SELECT_MARKER_W)
        .saturating_sub(scrollbar_w);
    let max_suffix_w = picker
        .rows
        .iter()
        .map(|row| tuika::width::str_cols(&model_row_suffix(row)))
        .max()
        .unwrap_or(0);
    let max_name_w = picker
        .rows
        .iter()
        .map(|row| tuika::width::str_cols(&row.display_name))
        .max()
        .unwrap_or(0);
    // Keep both existing columns visible on a narrow terminal. Each remains a
    // single SelectList row; overlong fields are clipped with an ellipsis.
    let name_w = max_name_w.min(content_w.saturating_sub(max_suffix_w.saturating_add(2)) / 2);
    let rows: Vec<Line<'static>> = picker
        .rows
        .iter()
        .map(|row| {
            let suffix = model_row_suffix(row);
            let suffix_w = tuika::width::str_cols(&suffix);
            let coordinate_w = content_w
                .saturating_sub(name_w)
                .saturating_sub(2)
                .saturating_sub(suffix_w);
            let name = fit_columns(&row.display_name, name_w);
            let name_pad = name_w.saturating_sub(tuika::width::str_cols(&name));
            let coordinate =
                fit_columns(&format!("{}/{}", row.provider_id, row.model), coordinate_w);
            let mut spans = vec![
                Span::styled(
                    format!("{name}{:width$}  ", "", width = usize::from(name_pad)),
                    Style::default().fg(theme.text),
                ),
                Span::styled(coordinate, theme.muted_style()),
            ];
            if row.active {
                spans.push(Span::styled(
                    "  active",
                    Style::default()
                        .fg(theme.accent)
                        .add_modifier(Modifier::BOLD),
                ));
            }
            if row.auth_status == "missing" {
                spans.push(Span::styled("  needs login", theme.muted_style()));
            }
            Line::from(spans)
        })
        .collect();
    let count = rows.len();
    let list = SelectList::new(rows, &picker.state).viewport(MAX_MODAL_ROWS as u16);
    (element(list), count.min(MAX_MODAL_ROWS) as u16)
}

fn model_row_suffix(row: &crate::ModelRow) -> String {
    let mut suffix = String::new();
    if row.active {
        suffix.push_str("  active");
    }
    if row.auth_status == "missing" {
        suffix.push_str("  needs login");
    }
    suffix
}

/// Fit text to terminal columns without slicing UTF-8 or splitting a wide
/// character across the right edge.
fn fit_columns(text: &str, max_cols: u16) -> String {
    if tuika::width::str_cols(text) <= max_cols {
        return text.to_string();
    }
    if max_cols == 0 {
        return String::new();
    }

    let body_cols = max_cols - 1;
    let mut fitted = String::new();
    for ch in text.chars() {
        fitted.push(ch);
        if tuika::width::str_cols(&fitted) > body_cols {
            fitted.pop();
            break;
        }
    }
    fitted.push('…');
    fitted
}

/// Fit text keeping the trailing edge visible (for input fields, so the
/// caret end of an overlong value stays on screen).
fn fit_columns_left(text: &str, max_cols: u16) -> String {
    if tuika::width::str_cols(text) <= max_cols {
        return text.to_string();
    }
    if max_cols == 0 {
        return String::new();
    }
    let body_cols = max_cols - 1;
    let mut fitted = String::new();
    for ch in text.chars().rev() {
        fitted.insert(0, ch);
        if tuika::width::str_cols(&fitted) > body_cols {
            fitted.remove(0);
            break;
        }
    }
    fitted.insert(0, '…');
    fitted
}

/// The rounded input box, with the caret placed by the host through `probe`.
fn composer(app: &App, theme: &Theme, probe: &RectProbe) -> Element {
    let input = element(
        TextInput::new(&app.composer)
            .style(Style::default().fg(theme.text))
            .placeholder(
                "Describe a task, or / for commands",
                Style::default().fg(theme.dim),
            )
            .highlights(app.composer_highlights(theme)),
    );
    let prompt = Text::new(vec![Line::from(Span::styled(
        "› ",
        Style::default().fg(theme.accent_alt),
    ))]);
    view! {
        boxed(border = BorderStyle::Rounded, border_color = theme.border,
              padding = Padding::symmetric(1, 0)) {
            row {
                fixed(2) { node(prompt) }
                grow(1) { node(probe.wrap(input)) }
            }
        }
    }
}

/// Key hints on the left; connection, thread, and turn state on the right.
fn footer(app: &App, theme: &Theme) -> Element {
    let hints = if app.setup_visible() || app.picker.is_some() {
        // The modal window shows its own key hints.
        "".to_string()
    } else if app.popup.is_some() {
        "  ↑↓ move   ⇥ complete   ⏎ run   esc dismiss".to_string()
    } else if app.needs_provider {
        "  no provider configured — /setup to connect one".to_string()
    } else {
        // `PgUp` spelled out rather than `⇞⇟`: those glyphs are missing from
        // most terminal fonts and land as replacement boxes.
        "  ⏎ send   ⇧⏎ newline   PgUp scroll   ⌃C quit".to_string()
    };
    let hint_style = if app.needs_provider && !app.setup_visible() && app.popup.is_none() {
        Style::default().fg(theme.accent)
    } else {
        theme.muted_style()
    };
    // Kept short enough to fit beside the hints on an 80-column terminal: a
    // StatusBar drops an overflowing right group outright. Connection, model,
    // and thread name live in the banner and `/status`.
    let status = format!("{} · {}  ", short_id(&app.meta.thread_id), app.turn_state);
    let bar = StatusBar::new()
        .left(vec![Span::styled(hints, hint_style)])
        .right(vec![Span::styled(status, Style::default().fg(theme.dim))]);
    element(bar)
}
