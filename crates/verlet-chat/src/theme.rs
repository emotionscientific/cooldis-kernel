//! The chat palette.

/// The Verlet chat theme. With `no_color` (the `NO_COLOR` convention) every
/// slot collapses to the terminal's default foreground/background so the UI
/// degrades to structure-only styling.
pub fn chat_theme(no_color: bool) -> tuika::style::Theme {
    if no_color {
        return tuika::style::Theme {
            background: ratatui::style::Color::Reset,
            surface: ratatui::style::Color::Reset,
            text: ratatui::style::Color::Reset,
            muted: ratatui::style::Color::Reset,
            dim: ratatui::style::Color::Reset,
            accent: ratatui::style::Color::Reset,
            accent_alt: ratatui::style::Color::Reset,
            border: ratatui::style::Color::Reset,
            border_focused: ratatui::style::Color::Reset,
            selection_bg: ratatui::style::Color::Reset,
            selection_fg: ratatui::style::Color::Reset,
            code: tuika::style::CodeTheme::default(),
        };
    }
    // A dark, low-chroma palette: near-black ground, teal for the UI's own
    // marks, violet for user-entered tokens, muted gray for machine output.
    tuika::style::Theme {
        background: ratatui::style::Color::Rgb(13, 14, 16),
        surface: ratatui::style::Color::Rgb(23, 25, 28),
        text: ratatui::style::Color::Rgb(223, 226, 230),
        muted: ratatui::style::Color::Rgb(142, 148, 158),
        dim: ratatui::style::Color::Rgb(88, 94, 104),
        accent: ratatui::style::Color::Rgb(94, 187, 209),
        accent_alt: ratatui::style::Color::Rgb(197, 154, 231),
        border: ratatui::style::Color::Rgb(60, 66, 74),
        border_focused: ratatui::style::Color::Rgb(94, 187, 209),
        selection_bg: ratatui::style::Color::Rgb(35, 44, 52),
        selection_fg: ratatui::style::Color::Rgb(235, 238, 242),
        code: tuika::style::CodeTheme {
            link: ratatui::style::Color::Rgb(122, 172, 240),
            string: ratatui::style::Color::Rgb(140, 200, 140),
            heading: ratatui::style::Color::Rgb(223, 226, 230),
            ..tuika::style::CodeTheme::default()
        },
    }
}
