// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

pub mod convert;
pub mod tabs;

use ratatui::{prelude::*, style::palette::tailwind};
use strum::{Display, EnumIter, FromRepr};

use crate::tui::App;

use self::tabs::{context, history, logs, sql};

#[derive(Clone, Copy, Debug, Display, FromRepr, EnumIter)]
pub enum SelectedTab {
    #[allow(clippy::upper_case_acronyms)]
    #[strum(to_string = "SQL")]
    SQL,
    #[cfg(feature = "flightsql")]
    #[strum(to_string = "FlightSQL")]
    FlightSQL,
    #[strum(to_string = "History")]
    History,
    #[strum(to_string = "Logs")]
    Logs,
    #[strum(to_string = "Context")]
    Context,
}

impl SelectedTab {
    pub fn title(self, _app: &App) -> Line<'static> {
        let padding = Span::from("  ");
        match self {
            SelectedTab::SQL => {
                let title = Span::from("SQL (1)").bold();
                Line::from_iter(vec![padding.clone(), title, padding.clone()])
                    .fg(tailwind::SLATE.c200)
                    .bg(self.bg())
            }
            #[cfg(feature = "flightsql")]
            Self::FlightSQL => {
                let status = _app.state.flightsql_tab.connection_status().tab_display();
                let title_text = format!("FlightSQL{status} (2)");
                let title = Span::from(title_text).bold();
                Line::from_iter(vec![padding.clone(), title, padding.clone()])
                    .fg(tailwind::SLATE.c200)
                    .bg(self.bg())
            }
            Self::Logs => {
                #[cfg(feature = "flightsql")]
                let title = Span::from("LOGS (4)").bold();

                #[cfg(not(feature = "flightsql"))]
                let title = Span::from("LOGS (3)").bold();

                Line::from_iter(vec![padding.clone(), title, padding.clone()])
                    .fg(tailwind::SLATE.c200)
                    .bg(self.bg())
            }
            Self::Context => {
                #[cfg(feature = "flightsql")]
                let title = Span::from("CONTEXT (5)").bold();

                #[cfg(not(feature = "flightsql"))]
                let title = Span::from("CONTEXT (4)").bold();

                Line::from_iter(vec![padding.clone(), title, padding.clone()])
                    .fg(tailwind::SLATE.c200)
                    .bg(self.bg())
            }
            Self::History => {
                #[cfg(feature = "flightsql")]
                let title = Span::from("HISTORY (3)").bold();
                #[cfg(not(feature = "flightsql"))]
                let title = Span::from("HISTORY (2)").bold();
                Line::from_iter(vec![padding.clone(), title, padding.clone()])
                    .fg(tailwind::SLATE.c200)
                    .bg(self.bg())
            }
        }
    }

    const fn bg(self) -> Color {
        match self {
            Self::SQL => tailwind::ORANGE.c700,
            Self::Logs => tailwind::ORANGE.c700,
            Self::Context => tailwind::ORANGE.c700,
            Self::History => tailwind::ORANGE.c700,
            #[cfg(feature = "flightsql")]
            Self::FlightSQL => tailwind::ORANGE.c700,
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

    fn render_sql(self, area: Rect, buf: &mut Buffer, app: &App) {
        sql::render_sql(area, buf, app)
    }

    fn render_logs(self, area: Rect, buf: &mut Buffer, app: &App) {
        logs::render_logs(area, buf, app)
    }

    fn render_context(self, area: Rect, buf: &mut Buffer, app: &App) {
        context::render_context(area, buf, app)
    }

    fn render_history(self, area: Rect, buf: &mut Buffer, app: &App) {
        history::render_history(area, buf, app)
    }

    #[cfg(feature = "flightsql")]
    fn render_flightsql(self, area: Rect, buf: &mut Buffer, app: &App) {
        use self::tabs::flightsql;

        flightsql::render_sql(area, buf, app)
    }

    /// Render the tab with the provided state.
    ///
    /// This used to be an impl of `Widget` but we weren't able to pass state
    /// as a paramter to the render method so moved to impl on the SelectedTab.
    /// It's not clear if this will have future impact.
    pub fn render(self, area: Rect, buf: &mut Buffer, app: &App) {
        match self {
            Self::SQL => self.render_sql(area, buf, app),
            Self::Logs => self.render_logs(area, buf, app),
            Self::Context => self.render_context(area, buf, app),
            Self::History => self.render_history(area, buf, app),
            #[cfg(feature = "flightsql")]
            Self::FlightSQL => self.render_flightsql(area, buf, app),
        }
    }
}
