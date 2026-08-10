//! The view tree: transcript, working indicator, popup, composer, footer.
//!
//! Ported from tuika's `codex` example (`examples/codex/ui.rs`). The whole
//! screen is one column. The transcript grows; everything below it is
//! measured first and pinned — the composer never moves, and new output
//! pushes the history up instead of the input down.

use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};

use tuika::prelude::*;
use tuika::probe::RectProbe;

use crate::app::App;
use crate::cells::short_id;

/// Columns of blank kept down each side of the UI.
pub const GUTTER: u16 = 1;
/// Visual rows the composer grows to before it scrolls internally.
const MAX_COMPOSER_ROWS: u16 = 6;
/// Rows the completion popup shows before it windows around the selection.
const MAX_POPUP_ROWS: usize = 8;
/// Rows the model picker shows before it windows around the selection.
const MAX_PICKER_ROWS: usize = 10;
/// Blank rows between two transcript items.
const TRANSCRIPT_GAP: u16 = 1;

/// Build the frame. Takes `&mut App` because the transcript's streaming
/// markdown re-renders through its own cache, and because the scroll offset
/// is reconciled against this frame's geometry.
pub fn build(
    app: &mut App,
    area: Rect,
    theme: &Theme,
    sheet: &StyleSheet,
    probe: &RectProbe,
) -> Element {
    let width = area.width.saturating_sub(GUTTER * 2);

    // Measure the pinned bottom stack first; the transcript takes what is left.
    let popup_items = app.popup_items();
    let popup_h = if popup_items.is_empty() {
        0
    } else {
        popup_items.len().min(MAX_POPUP_ROWS) as u16 + 1
    };
    let picker_h = app
        .picker
        .as_ref()
        .map(|picker| picker.rows.len().min(MAX_PICKER_ROWS) as u16 + 2)
        .unwrap_or(0);
    let working_h = if app.turn_active() { 2 } else { 0 };
    let composer_rows = app
        .composer
        .visual_height(width.saturating_sub(4))
        .clamp(1, MAX_COMPOSER_ROWS);
    let body_h = composer_rows + 2;
    let bottom_h = working_h + popup_h + picker_h + body_h + 1;
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
    if picker_h > 0 {
        root = root.fixed(picker_h, picker(app, theme, width));
    }
    root = root.fixed(body_h, composer(app, theme, probe));
    root = root.fixed(1, footer(app, theme));
    element(root)
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
            format!(" ({}s • Esc to interrupt)", app.elapsed_secs()),
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

/// The `/models` picker: a titled list of selectable models. Auth problems
/// and the active selection are annotated per row; the width columns align
/// on the longest display name.
fn picker(app: &App, theme: &Theme, width: u16) -> Element {
    let Some(picker) = app.picker.as_ref() else {
        return element(Spacer);
    };
    let scrollbar_w = u16::from(picker.rows.len() > MAX_PICKER_ROWS);
    let content_w = width.saturating_sub(2).saturating_sub(scrollbar_w);
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
    let title = Text::new(vec![Line::from(Span::styled(
        "Select a model",
        Style::default().fg(theme.text).add_modifier(Modifier::BOLD),
    ))]);
    let list = SelectList::new(rows, &picker.state).viewport(MAX_PICKER_ROWS as u16);
    element(
        Flex::column()
            .fixed(1, element(Spacer))
            .fixed(1, element(title))
            .grow(1, element(list)),
    )
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
    let hints = if app.picker.is_some() {
        "  ↑↓ move   ⏎ select   esc dismiss"
    } else if app.popup.is_some() {
        "  ↑↓ move   ⇥ complete   ⏎ run   esc dismiss"
    } else {
        // `PgUp` spelled out rather than `⇞⇟`: those glyphs are missing from
        // most terminal fonts and land as replacement boxes.
        "  ⏎ send   ⇧⏎ newline   PgUp scroll   ⌃C quit"
    };
    // Kept short enough to fit beside the hints on an 80-column terminal: a
    // StatusBar drops an overflowing right group outright. Connection, model,
    // and thread name live in the banner and `/status`.
    let status = format!("{} · {}  ", short_id(&app.meta.thread_id), app.turn_state);
    let bar = StatusBar::new()
        .left(vec![Span::styled(hints, theme.muted_style())])
        .right(vec![Span::styled(status, Style::default().fg(theme.dim))]);
    element(bar)
}
