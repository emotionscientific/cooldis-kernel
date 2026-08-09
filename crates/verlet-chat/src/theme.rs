//! The chat palette.

use ratatui::style::Color;

use tuika::prelude::*;

/// The Verlet chat theme. With `no_color` (the `NO_COLOR` convention) every
/// slot collapses to the terminal's default foreground/background so the UI
/// degrades to structure-only styling.
pub fn chat_theme(no_color: bool) -> Theme {
    if no_color {
        return Theme {
            background: Color::Reset,
            surface: Color::Reset,
            text: Color::Reset,
            muted: Color::Reset,
            dim: Color::Reset,
            accent: Color::Reset,
            accent_alt: Color::Reset,
            border: Color::Reset,
            border_focused: Color::Reset,
            selection_bg: Color::Reset,
            selection_fg: Color::Reset,
            code: CodeTheme::default(),
        };
    }
    // A dark, low-chroma palette: near-black ground, teal for the UI's own
    // marks, violet for user-entered tokens, muted gray for machine output.
    Theme {
        background: Color::Rgb(13, 14, 16),
        surface: Color::Rgb(23, 25, 28),
        text: Color::Rgb(223, 226, 230),
        muted: Color::Rgb(142, 148, 158),
        dim: Color::Rgb(88, 94, 104),
        accent: Color::Rgb(94, 187, 209),
        accent_alt: Color::Rgb(197, 154, 231),
        border: Color::Rgb(60, 66, 74),
        border_focused: Color::Rgb(94, 187, 209),
        selection_bg: Color::Rgb(35, 44, 52),
        selection_fg: Color::Rgb(235, 238, 242),
        code: CodeTheme {
            link: Color::Rgb(122, 172, 240),
            string: Color::Rgb(140, 200, 140),
            heading: Color::Rgb(223, 226, 230),
            ..CodeTheme::default()
        },
    }
}
