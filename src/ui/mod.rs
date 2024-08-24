pub mod convert;
pub mod tabs;

use ratatui::{prelude::*, style::palette::tailwind, widgets::*};
use strum::{Display, EnumIter, FromRepr};

use crate::app::App;

use self::tabs::{explore, logs};

#[derive(Clone, Copy, Debug, Display, FromRepr, EnumIter)]
pub enum SelectedTab {
    #[strum(to_string = "Queries")]
    Queries,
    #[strum(to_string = "Logs")]
    Logs,
}

impl SelectedTab {
    pub fn title(self) -> Line<'static> {
        let padding = Span::from("  ");
        match self {
            SelectedTab::Queries => {
                let bold_char = Span::from("E").bold();
                let remaining = Span::from("xplore");
                Line::from_iter(vec![padding.clone(), bold_char, remaining, padding.clone()])
                    .fg(tailwind::SLATE.c200)
                    .bg(self.bg())
            }
            Self::Logs => {
                let bold_char = Span::from("L").bold();
                let remaining = Span::from("ogs");
                Line::from_iter(vec![padding.clone(), bold_char, remaining, padding.clone()])
                    .fg(tailwind::SLATE.c200)
                    .bg(self.bg())
            }
        }
    }

    const fn bg(self) -> Color {
        match self {
            Self::Queries => tailwind::EMERALD.c700,
            Self::Logs => tailwind::EMERALD.c700,
        }
    }

    const fn palette(self) -> tailwind::Palette {
        match self {
            Self::Queries => tailwind::STONE,
            Self::Logs => tailwind::STONE,
        }
    }

    /// Get the previous tab, if there is no previous tab return the current tab.
    pub fn previous(self) -> Self {
        let current_index: usize = self as usize;
        let previous_index = current_index.saturating_sub(1);
        Self::from_repr(previous_index).unwrap_or(self)
    }

    /// Get the next tab, if there is no next tab return the current tab.
    pub fn next(self) -> Self {
        let current_index = self as usize;
        let next_index = current_index.saturating_add(1);
        Self::from_repr(next_index).unwrap_or(self)
    }

    /// A block surrounding the tab's content
    fn block(self) -> Block<'static> {
        Block::default()
            .borders(Borders::ALL)
            .border_set(symbols::border::PROPORTIONAL_TALL)
            .padding(Padding::horizontal(1))
            .border_style(self.palette().c700)
    }

    fn render_explore(self, area: Rect, buf: &mut Buffer, app: &App) {
        explore::render_explore(area, buf, app)
    }

    fn render_logs(self, area: Rect, buf: &mut Buffer, app: &App) {
        logs::render_logs(area, buf, app)
    }

    /// Render the tab with the provided state.
    ///
    /// This used to be an impl of `Widget` but we weren't able to pass state
    /// as a paramter to the render method so moved to impl on the SelectedTab.
    /// It's not clear if this will have future impact.
    pub fn render(self, area: Rect, buf: &mut Buffer, app: &App) {
        match self {
            Self::Queries => self.render_explore(area, buf, app),
            Self::Logs => self.render_logs(area, buf, app),
        }
    }
}
