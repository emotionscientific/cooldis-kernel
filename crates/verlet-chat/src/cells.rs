//! Transcript cells — everything the chat prints above its composer.
//!
//! Ported from tuika's `codex` example (`examples/codex/history.rs`); the cell
//! shapes are kept close to upstream so the two stay diffable. Most cells are
//! rows of text — one `•` bullet per event, details indented under a `└` — but
//! the session banner and `/status` are bordered panels, laid out rather than
//! drawn as glyphs in strings. So a cell renders to an `Element`, not to
//! lines, and the transcript is an `ItemScroll` over those elements. A
//! streamed answer keeps a [`MarkdownState`] so only its in-flight tail
//! re-parses.

use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};

use tuika::components::MarkdownState;
use tuika::components::text::wrap_lines;
use tuika::prelude::*;

/// Rows of tool output kept inline before the middle is elided.
const MAX_OUTPUT_ROWS: usize = 6;

/// How a tool call ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecStatus {
    Running,
    Ok,
    Failed,
}

/// Severity of a local notice (slash-command output, interruptions, errors).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tone {
    Info,
    Warn,
    Error,
}

/// One item in the transcript.
pub enum Cell {
    /// The session header printed on startup and `/new`.
    Banner {
        version: String,
        rows: Vec<(String, String)>,
        tips: Vec<(String, String)>,
    },
    /// A labeled key/value block — `/status` output.
    Config {
        title: String,
        rows: Vec<(String, String)>,
    },
    /// The user's own message, echoed into the transcript.
    User(String),
    /// A reasoning summary, streamed in while the agent thinks.
    Reasoning { body: String },
    /// A tool call and a window of its output.
    Exec {
        /// Tool-call id, so streamed output and completion find their cell.
        id: String,
        title: String,
        output: Vec<String>,
        status: ExecStatus,
    },
    /// The assistant's answer, streamed as markdown.
    ///
    /// Boxed because `MarkdownState` carries the parse/flatten caches and
    /// would otherwise dominate the size of every other variant.
    Answer(Box<MarkdownState>),
    /// A local notice: interruptions, errors, slash-command acknowledgements.
    Notice {
        tone: Tone,
        title: String,
        body: Vec<String>,
    },
    /// `/sessions` output: one row per thread.
    Sessions(Vec<crate::SessionRow>),
}

impl Cell {
    /// Render this cell as a transcript item, laid out to `width`.
    pub fn view(&mut self, width: u16, theme: &Theme, sheet: &StyleSheet) -> Element {
        match self {
            Cell::Banner {
                version,
                rows,
                tips,
            } => banner(version, rows, tips, theme),
            Cell::Config { title, rows } => config(title, rows, theme),
            other => element(Text::new(other.lines(width, theme, sheet))),
        }
    }

    /// Render a line-shaped cell to transcript rows, wrapped to `width`.
    fn lines(&mut self, width: u16, theme: &Theme, sheet: &StyleSheet) -> Vec<Line<'static>> {
        match self {
            Cell::User(text) => user(text, width, theme),
            Cell::Reasoning { body } => reasoning(body, width, theme),
            Cell::Exec {
                title,
                output,
                status,
                ..
            } => exec(title, output, *status, theme),
            Cell::Answer(state) => answer(state, width, theme, sheet),
            Cell::Notice { tone, title, body } => notice(*tone, title, body, width, theme),
            Cell::Sessions(rows) => sessions(rows, theme),
            // Panel-shaped cells are handled by `view` and never reach here.
            Cell::Banner { .. } | Cell::Config { .. } => Vec::new(),
        }
    }
}

/// Wrap `child` so it takes only the width it measures, instead of stretching
/// to the transcript's.
fn shrink_to_fit(child: Element) -> Element {
    element(Flex::column().align(Align::Start).auto(child))
}

/// `• ` — the marker in front of every reported event.
fn bullet(theme: &Theme) -> Span<'static> {
    Span::styled("• ", Style::default().fg(theme.accent))
}

/// A cell header: the bullet plus a bold title and optional trailing detail.
fn header(title: &str, detail: Option<Span<'static>>, theme: &Theme) -> Line<'static> {
    let mut spans = vec![
        bullet(theme),
        Span::styled(
            title.to_string(),
            Style::default().fg(theme.text).add_modifier(Modifier::BOLD),
        ),
    ];
    spans.extend(detail);
    Line::from(spans)
}

/// Indent detail rows under a header: `  └ ` on the first, `    ` on the rest.
fn detail(rows: Vec<Line<'static>>, theme: &Theme) -> Vec<Line<'static>> {
    rows.into_iter()
        .enumerate()
        .map(|(i, line)| {
            let lead = if i == 0 { "  └ " } else { "    " };
            let mut spans = vec![Span::styled(lead, Style::default().fg(theme.dim))];
            spans.extend(line.spans);
            Line::from(spans)
        })
        .collect()
}

/// Prefix every row with `pad` columns of blank.
fn indent(rows: Vec<Line<'static>>, pad: usize) -> Vec<Line<'static>> {
    rows.into_iter()
        .map(|line| {
            let mut spans = vec![Span::raw(" ".repeat(pad))];
            spans.extend(line.spans);
            Line::from(spans)
        })
        .collect()
}

/// `label:` / `value` rows, aligned on the widest label.
fn kv_rows(rows: &[(String, String)], theme: &Theme) -> Vec<Line<'static>> {
    let pad = rows
        .iter()
        .map(|(k, _)| k.chars().count())
        .max()
        .unwrap_or(0);
    rows.iter()
        .map(|(k, v)| {
            Line::from(vec![
                Span::styled(
                    format!("{k}:{:width$} ", "", width = pad - k.chars().count()),
                    theme.muted_style(),
                ),
                Span::styled(v.clone(), Style::default().fg(theme.text)),
            ])
        })
        .collect()
}

fn banner(
    version: &str,
    rows: &[(String, String)],
    tips: &[(String, String)],
    theme: &Theme,
) -> Element {
    let mut panel = vec![Line::from(vec![
        Span::styled(
            ">_ ",
            Style::default()
                .fg(theme.accent)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("Verlet chat (v{version})"),
            Style::default().fg(theme.text).add_modifier(Modifier::BOLD),
        ),
    ])];
    panel.push(Line::default());
    panel.extend(kv_rows(rows, theme));

    let pad = tips
        .iter()
        .map(|(k, _)| k.chars().count())
        .max()
        .unwrap_or(0);
    let mut tip_rows = vec![
        Line::default(),
        Line::from(Span::styled(
            "Describe a task, or try one of these commands:",
            theme.muted_style(),
        )),
        Line::default(),
    ];
    tip_rows.extend(tips.iter().map(|(cmd, blurb)| {
        Line::from(vec![
            Span::styled(
                format!("{cmd}{:width$}  ", "", width = pad - cmd.chars().count()),
                Style::default().fg(theme.accent_alt),
            ),
            Span::styled(blurb.clone(), theme.muted_style()),
        ])
    }));

    element(
        Flex::column()
            .auto(shrink_to_fit(element(
                Boxed::new(element(Text::new(panel)))
                    .border(BorderStyle::Rounded)
                    .border_color(theme.border)
                    .padding(Padding::symmetric(1, 0)),
            )))
            .auto(element(Text::new(tip_rows))),
    )
}

fn config(title: &str, rows: &[(String, String)], theme: &Theme) -> Element {
    let panel = Boxed::new(element(Text::new(kv_rows(rows, theme))))
        .title(Line::from(Span::styled(
            format!(" {title} "),
            Style::default()
                .fg(theme.accent)
                .add_modifier(Modifier::BOLD),
        )))
        .border(BorderStyle::Rounded)
        .border_color(theme.border)
        .padding(Padding::symmetric(1, 0));
    shrink_to_fit(element(panel))
}

fn user(text: &str, width: u16, theme: &Theme) -> Vec<Line<'static>> {
    let body: Vec<Line<'static>> = text
        .lines()
        .map(|l| Line::from(Span::styled(l.to_string(), Style::default().fg(theme.text))))
        .collect();
    wrap_lines(&body, width.saturating_sub(2))
        .into_iter()
        .enumerate()
        .map(|(i, line)| {
            let lead = if i == 0 { "› " } else { "  " };
            let mut spans = vec![Span::styled(lead, Style::default().fg(theme.accent_alt))];
            spans.extend(line.spans);
            Line::from(spans)
        })
        .collect()
}

fn reasoning(body: &str, width: u16, theme: &Theme) -> Vec<Line<'static>> {
    let style = theme.muted_style().add_modifier(Modifier::ITALIC);
    let rows: Vec<Line<'static>> = body
        .split('\n')
        .map(|l| Line::from(Span::styled(l.to_string(), style)))
        .collect();
    let mut out = vec![header("Thinking", None, theme)];
    out.extend(indent(wrap_lines(&rows, width.saturating_sub(2)), 2));
    out
}

fn exec(title: &str, output: &[String], status: ExecStatus, theme: &Theme) -> Vec<Line<'static>> {
    let trailing = match status {
        ExecStatus::Running => None,
        ExecStatus::Ok => None,
        ExecStatus::Failed => Some(Span::styled(
            "  (failed)".to_string(),
            Style::default().fg(theme.accent),
        )),
    };
    let mut out = vec![Line::from({
        let mut spans = vec![
            bullet(theme),
            Span::styled(
                title.to_string(),
                Style::default().fg(theme.text).add_modifier(Modifier::BOLD),
            ),
        ];
        spans.extend(trailing);
        spans
    })];

    // Long output is elided in the middle, keeping a head and tail window.
    let mut rows: Vec<Line<'static>> = Vec::new();
    if output.len() > MAX_OUTPUT_ROWS {
        let head = MAX_OUTPUT_ROWS / 2;
        let tail = output.len() - (MAX_OUTPUT_ROWS - head);
        for line in &output[..head] {
            rows.push(Line::from(Span::styled(line.clone(), theme.muted_style())));
        }
        rows.push(Line::from(Span::styled(
            format!("… +{} lines", tail - head),
            Style::default().fg(theme.dim),
        )));
        for line in &output[tail..] {
            rows.push(Line::from(Span::styled(line.clone(), theme.muted_style())));
        }
    } else {
        rows.extend(
            output
                .iter()
                .map(|line| Line::from(Span::styled(line.clone(), theme.muted_style()))),
        );
    }
    out.extend(detail(rows, theme));
    out
}

fn answer(
    state: &mut MarkdownState,
    width: u16,
    theme: &Theme,
    sheet: &StyleSheet,
) -> Vec<Line<'static>> {
    // The markdown is wrapped two columns narrow so the bullet gutter fits.
    let body = state
        .lines(
            width.saturating_sub(2),
            theme,
            sheet,
            CodeHighlighter::Plain,
        )
        .to_vec();
    indent(body, 2)
        .into_iter()
        .enumerate()
        .map(|(i, line)| {
            if i > 0 {
                return line;
            }
            // Replace the first row's indent with the bullet.
            let mut spans = vec![bullet(theme)];
            spans.extend(line.spans.into_iter().skip(1));
            Line::from(spans)
        })
        .collect()
}

fn notice(
    tone: Tone,
    title: &str,
    body: &[String],
    width: u16,
    theme: &Theme,
) -> Vec<Line<'static>> {
    let (glyph, color) = match tone {
        Tone::Info => ("• ", theme.accent),
        Tone::Warn => ("⚠ ", theme.accent_alt),
        Tone::Error => ("✗ ", theme.accent),
    };
    let mut out = vec![Line::from(vec![
        Span::styled(glyph, Style::default().fg(color)),
        Span::styled(
            title.to_string(),
            Style::default().fg(theme.text).add_modifier(Modifier::BOLD),
        ),
    ])];
    let rows: Vec<Line<'static>> = body
        .iter()
        .map(|l| Line::from(Span::styled(l.clone(), theme.muted_style())))
        .collect();
    out.extend(indent(wrap_lines(&rows, width.saturating_sub(2)), 2));
    out
}

fn sessions(rows: &[crate::SessionRow], theme: &Theme) -> Vec<Line<'static>> {
    let mut out = vec![header("Sessions", None, theme)];
    if rows.is_empty() {
        out.extend(detail(
            vec![Line::from(Span::styled(
                "no sessions".to_string(),
                theme.muted_style(),
            ))],
            theme,
        ));
        return out;
    }
    let body = rows
        .iter()
        .map(|row| {
            let marker = if row.current { "* " } else { "  " };
            let mut spans = vec![
                Span::styled(marker.to_string(), Style::default().fg(theme.accent)),
                Span::styled(
                    format!("{} ", short_id(&row.id)),
                    Style::default().fg(theme.accent_alt),
                ),
                Span::styled(format!("{} ", row.name), Style::default().fg(theme.text)),
                Span::styled(format!("[{}]", row.status), theme.muted_style()),
            ];
            if !row.preview.is_empty() {
                spans.push(Span::styled(
                    format!(" - {}", row.preview),
                    Style::default().fg(theme.dim),
                ));
            }
            Line::from(spans)
        })
        .collect();
    out.extend(detail(body, theme));
    out
}

/// The first eight characters of a thread id, as shown everywhere in the UI.
pub(crate) fn short_id(id: &str) -> String {
    id.chars().take(8).collect()
}
