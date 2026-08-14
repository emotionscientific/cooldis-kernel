//! Transcript cells — everything the chat prints above its composer.
//!
//! Ported from tuika's `codex` example (`examples/codex/history.rs`); the cell
//! shapes are kept close to upstream so the two stay diffable. Most cells are
//! rows of text — one `•` bullet per event, details indented under a `└` — but
//! the session banner and `/status` are bordered panels, laid out rather than
//! drawn as glyphs in strings. So a cell renders to an `Element`, not to
//! lines, and the transcript is an `ItemScroll` over those elements. A
//! streamed answer keeps a [`tuika::components::MarkdownState`] so only its
//! in-flight tail re-parses.

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
    /// The session header printed on startup.
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
    Answer(Box<tuika::components::MarkdownState>),
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
    pub fn view(
        &mut self,
        width: u16,
        theme: &tuika::style::Theme,
        sheet: &tuika::style::StyleSheet,
    ) -> tuika::view::Element {
        match self {
            Cell::Banner {
                version,
                rows,
                tips,
            } => banner(version, rows, tips, theme),
            Cell::Config { title, rows } => config(title, rows, theme),
            other => tuika::view::element(tuika::components::Text::new(
                other.lines(width, theme, sheet),
            )),
        }
    }

    /// Render a line-shaped cell to transcript rows, wrapped to `width`.
    fn lines(
        &mut self,
        width: u16,
        theme: &tuika::style::Theme,
        sheet: &tuika::style::StyleSheet,
    ) -> Vec<ratatui::text::Line<'static>> {
        match self {
            Cell::User(text) => user(text, width, theme),
            Cell::Reasoning { body } => reasoning(body, width, theme),
            Cell::Exec {
                title,
                output,
                status,
                ..
            } => exec(title, output, *status, width, theme),
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
fn shrink_to_fit(child: tuika::view::Element) -> tuika::view::Element {
    tuika::view::element(
        tuika::components::Flex::column()
            .align(tuika::layout::Align::Start)
            .auto(child),
    )
}

/// `• ` — the marker in front of every reported event.
fn bullet(theme: &tuika::style::Theme) -> ratatui::text::Span<'static> {
    ratatui::text::Span::styled("• ", ratatui::style::Style::default().fg(theme.accent))
}

/// A cell header: the bullet plus a bold title and optional trailing detail.
fn header(
    title: &str,
    detail: Option<ratatui::text::Span<'static>>,
    theme: &tuika::style::Theme,
) -> ratatui::text::Line<'static> {
    let mut spans = vec![
        bullet(theme),
        ratatui::text::Span::styled(
            title.to_string(),
            ratatui::style::Style::default()
                .fg(theme.text)
                .add_modifier(ratatui::style::Modifier::BOLD),
        ),
    ];
    spans.extend(detail);
    ratatui::text::Line::from(spans)
}

/// Indent detail rows under a header: `  └ ` on the first, `    ` on the rest.
fn detail(
    rows: Vec<ratatui::text::Line<'static>>,
    theme: &tuika::style::Theme,
) -> Vec<ratatui::text::Line<'static>> {
    rows.into_iter()
        .enumerate()
        .map(|(i, line)| {
            let lead = if i == 0 { "  └ " } else { "    " };
            let mut spans = vec![ratatui::text::Span::styled(
                lead,
                ratatui::style::Style::default().fg(theme.dim),
            )];
            spans.extend(line.spans);
            ratatui::text::Line::from(spans)
        })
        .collect()
}

/// Prefix every row with `pad` columns of blank.
fn indent(
    rows: Vec<ratatui::text::Line<'static>>,
    pad: usize,
) -> Vec<ratatui::text::Line<'static>> {
    rows.into_iter()
        .map(|line| {
            let mut spans = vec![ratatui::text::Span::raw(" ".repeat(pad))];
            spans.extend(line.spans);
            ratatui::text::Line::from(spans)
        })
        .collect()
}

/// `label:` / `value` rows, aligned on the widest label.
fn kv_rows(
    rows: &[(String, String)],
    theme: &tuika::style::Theme,
) -> Vec<ratatui::text::Line<'static>> {
    let pad = rows
        .iter()
        .map(|(k, _)| k.chars().count())
        .max()
        .unwrap_or(0);
    rows.iter()
        .map(|(k, v)| {
            ratatui::text::Line::from(vec![
                ratatui::text::Span::styled(
                    format!("{k}:{:width$} ", "", width = pad - k.chars().count()),
                    theme.muted_style(),
                ),
                ratatui::text::Span::styled(
                    v.clone(),
                    ratatui::style::Style::default().fg(theme.text),
                ),
            ])
        })
        .collect()
}

fn banner(
    version: &str,
    rows: &[(String, String)],
    tips: &[(String, String)],
    theme: &tuika::style::Theme,
) -> tuika::view::Element {
    let mut panel = vec![ratatui::text::Line::from(vec![
        ratatui::text::Span::styled(
            ">_ ",
            ratatui::style::Style::default()
                .fg(theme.accent)
                .add_modifier(ratatui::style::Modifier::BOLD),
        ),
        ratatui::text::Span::styled(
            format!("Verlet chat (v{version})"),
            ratatui::style::Style::default()
                .fg(theme.text)
                .add_modifier(ratatui::style::Modifier::BOLD),
        ),
    ])];
    panel.push(ratatui::text::Line::default());
    panel.extend(kv_rows(rows, theme));

    let pad = tips
        .iter()
        .map(|(k, _)| k.chars().count())
        .max()
        .unwrap_or(0);
    let mut tip_rows = vec![
        ratatui::text::Line::default(),
        ratatui::text::Line::from(ratatui::text::Span::styled(
            "Describe a task, or try one of these commands:",
            theme.muted_style(),
        )),
        ratatui::text::Line::default(),
    ];
    tip_rows.extend(tips.iter().map(|(cmd, blurb)| {
        ratatui::text::Line::from(vec![
            ratatui::text::Span::styled(
                format!("{cmd}{:width$}  ", "", width = pad - cmd.chars().count()),
                ratatui::style::Style::default().fg(theme.accent_alt),
            ),
            ratatui::text::Span::styled(blurb.clone(), theme.muted_style()),
        ])
    }));

    tuika::view::element(
        tuika::components::Flex::column()
            .auto(shrink_to_fit(tuika::view::element(
                tuika::components::Boxed::new(tuika::view::element(tuika::components::Text::new(
                    panel,
                )))
                .border(tuika::style::BorderStyle::Rounded)
                .border_color(theme.border)
                .padding(tuika::geometry::Padding::symmetric(1, 0)),
            )))
            .auto(tuika::view::element(tuika::components::Text::new(tip_rows))),
    )
}

fn config(
    title: &str,
    rows: &[(String, String)],
    theme: &tuika::style::Theme,
) -> tuika::view::Element {
    let panel = tuika::components::Boxed::new(tuika::view::element(tuika::components::Text::new(
        kv_rows(rows, theme),
    )))
    .title(ratatui::text::Line::from(ratatui::text::Span::styled(
        format!(" {title} "),
        ratatui::style::Style::default()
            .fg(theme.accent)
            .add_modifier(ratatui::style::Modifier::BOLD),
    )))
    .border(tuika::style::BorderStyle::Rounded)
    .border_color(theme.border)
    .padding(tuika::geometry::Padding::symmetric(1, 0));
    shrink_to_fit(tuika::view::element(panel))
}

fn user(text: &str, width: u16, theme: &tuika::style::Theme) -> Vec<ratatui::text::Line<'static>> {
    let body: Vec<ratatui::text::Line<'static>> = text
        .lines()
        .map(|l| {
            ratatui::text::Line::from(ratatui::text::Span::styled(
                l.to_string(),
                ratatui::style::Style::default().fg(theme.text),
            ))
        })
        .collect();
    tuika::components::text::wrap_lines(&body, width.saturating_sub(2))
        .into_iter()
        .enumerate()
        .map(|(i, line)| {
            let lead = if i == 0 { "› " } else { "  " };
            let mut spans = vec![ratatui::text::Span::styled(
                lead,
                ratatui::style::Style::default().fg(theme.accent_alt),
            )];
            spans.extend(line.spans);
            ratatui::text::Line::from(spans)
        })
        .collect()
}

fn reasoning(
    body: &str,
    width: u16,
    theme: &tuika::style::Theme,
) -> Vec<ratatui::text::Line<'static>> {
    let style = theme
        .muted_style()
        .add_modifier(ratatui::style::Modifier::ITALIC);
    let rows: Vec<ratatui::text::Line<'static>> = body
        .split('\n')
        .map(|l| ratatui::text::Line::from(ratatui::text::Span::styled(l.to_string(), style)))
        .collect();
    let mut out = vec![header("Thinking", None, theme)];
    out.extend(indent(
        tuika::components::text::wrap_lines(&rows, width.saturating_sub(2)),
        2,
    ));
    out
}

fn exec(
    title: &str,
    output: &[String],
    status: ExecStatus,
    width: u16,
    theme: &tuika::style::Theme,
) -> Vec<ratatui::text::Line<'static>> {
    let trailing = match status {
        ExecStatus::Running => None,
        ExecStatus::Ok => None,
        ExecStatus::Failed => Some(ratatui::text::Span::styled(
            "  (failed)".to_string(),
            ratatui::style::Style::default().fg(theme.accent),
        )),
    };
    let mut out = vec![ratatui::text::Line::from({
        let mut spans = vec![
            bullet(theme),
            ratatui::text::Span::styled(
                title.to_string(),
                ratatui::style::Style::default()
                    .fg(theme.text)
                    .add_modifier(ratatui::style::Modifier::BOLD),
            ),
        ];
        spans.extend(trailing);
        spans
    })];

    // Wrap before eliding so a single long line remains inspectable instead of
    // being clipped at the viewport edge.
    let source = output
        .iter()
        .map(|line| {
            ratatui::text::Line::from(ratatui::text::Span::styled(
                line.clone(),
                theme.muted_style(),
            ))
        })
        .collect::<Vec<_>>();
    let wrapped = tuika::components::text::wrap_lines(&source, width.saturating_sub(4));
    let mut rows: Vec<ratatui::text::Line<'static>> = Vec::new();
    if wrapped.len() > MAX_OUTPUT_ROWS {
        let head = MAX_OUTPUT_ROWS / 2;
        let tail = wrapped.len() - (MAX_OUTPUT_ROWS - head);
        rows.extend_from_slice(&wrapped[..head]);
        rows.push(ratatui::text::Line::from(ratatui::text::Span::styled(
            format!("… +{} lines", tail - head),
            ratatui::style::Style::default().fg(theme.dim),
        )));
        rows.extend_from_slice(&wrapped[tail..]);
    } else {
        rows = wrapped;
    }
    out.extend(detail(rows, theme));
    out
}

fn answer(
    state: &mut tuika::components::MarkdownState,
    width: u16,
    theme: &tuika::style::Theme,
    sheet: &tuika::style::StyleSheet,
) -> Vec<ratatui::text::Line<'static>> {
    // The markdown is wrapped two columns narrow so the bullet gutter fits.
    let body = state
        .lines(
            width.saturating_sub(2),
            theme,
            sheet,
            tuika::highlight::CodeHighlighter::Plain,
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
            ratatui::text::Line::from(spans)
        })
        .collect()
}

fn notice(
    tone: Tone,
    title: &str,
    body: &[String],
    width: u16,
    theme: &tuika::style::Theme,
) -> Vec<ratatui::text::Line<'static>> {
    let (glyph, color) = match tone {
        Tone::Info => ("• ", theme.accent),
        Tone::Warn => ("⚠ ", theme.accent_alt),
        Tone::Error => ("✗ ", theme.accent),
    };
    let mut out = vec![ratatui::text::Line::from(vec![
        ratatui::text::Span::styled(glyph, ratatui::style::Style::default().fg(color)),
        ratatui::text::Span::styled(
            title.to_string(),
            ratatui::style::Style::default()
                .fg(theme.text)
                .add_modifier(ratatui::style::Modifier::BOLD),
        ),
    ])];
    let rows: Vec<ratatui::text::Line<'static>> = body
        .iter()
        .map(|l| {
            ratatui::text::Line::from(ratatui::text::Span::styled(l.clone(), theme.muted_style()))
        })
        .collect();
    out.extend(indent(
        tuika::components::text::wrap_lines(&rows, width.saturating_sub(2)),
        2,
    ));
    out
}

fn sessions(
    rows: &[crate::SessionRow],
    theme: &tuika::style::Theme,
) -> Vec<ratatui::text::Line<'static>> {
    let mut out = vec![header("Sessions", None, theme)];
    if rows.is_empty() {
        out.extend(detail(
            vec![ratatui::text::Line::from(ratatui::text::Span::styled(
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
                ratatui::text::Span::styled(
                    marker.to_string(),
                    ratatui::style::Style::default().fg(theme.accent),
                ),
                ratatui::text::Span::styled(
                    format!("{} ", short_id(&row.id)),
                    ratatui::style::Style::default().fg(theme.accent_alt),
                ),
                ratatui::text::Span::styled(
                    format!("{} ", row.name),
                    ratatui::style::Style::default().fg(theme.text),
                ),
                ratatui::text::Span::styled(format!("[{}]", row.status), theme.muted_style()),
            ];
            if !row.preview.is_empty() {
                spans.push(ratatui::text::Span::styled(
                    format!(" - {}", row.preview),
                    ratatui::style::Style::default().fg(theme.dim),
                ));
            }
            ratatui::text::Line::from(spans)
        })
        .collect();
    out.extend(detail(body, theme));
    out
}

/// The first eight characters of a thread id, as shown everywhere in the UI.
pub(crate) fn short_id(id: &str) -> String {
    id.chars().take(8).collect()
}
