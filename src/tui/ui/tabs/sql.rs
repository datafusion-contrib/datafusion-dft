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

use ratatui::{
    buffer::Buffer,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{palette::tailwind, Style, Stylize},
    text::Span,
    widgets::{block::Title, Block, Borders, Paragraph, Row, StatefulWidget, Table, Widget, Wrap},
};

use crate::tui::App;
use crate::tui::{state::tabs::sql::SQLTabMode, ui::convert::record_batches_to_table};

pub fn render_sql_editor(area: Rect, buf: &mut Buffer, app: &App) {
    let sql_tab = &app.state.sql_tab;
    let border_color = if app.state.sql_tab.editor_editable() {
        tailwind::ORANGE.c500
    } else {
        tailwind::WHITE
    };
    let title = Span::from(" Editor ").fg(tailwind::WHITE);
    let mode_text = format!(" {:?} ", sql_tab.mode());
    let mode = Title::from(mode_text.as_str()).alignment(Alignment::Right);
    let block = Block::default()
        .title(title)
        .title(mode)
        .borders(Borders::ALL)
        .fg(border_color);
    let mut editor = app.state.sql_tab.active_editor_cloned();
    editor.set_style(Style::default().fg(tailwind::WHITE));
    editor.set_block(block);
    editor.render(area, buf)
}

pub fn render_sql_results(area: Rect, buf: &mut Buffer, app: &App) {
    // TODO: Change this to a match on state and batch
    let sql_tab = &app.state.sql_tab;
    match (
        sql_tab.current_batch(),
        sql_tab.current_page(),
        sql_tab.query_results_state(),
        sql_tab.execution_error(),
    ) {
        (Some(batch), Some(p), Some(s), None) => {
            let block = Block::default()
                .title(" Results ")
                .borders(Borders::ALL)
                .title(Title::from(format!(" Page {p} ")).alignment(Alignment::Right));
            let batches = vec![batch];
            let maybe_table = record_batches_to_table(&batches);

            let block = block.title_bottom("Stats").fg(tailwind::ORANGE.c500);
            match maybe_table {
                Ok(table) => {
                    let table = table
                        .highlight_style(Style::default().bg(tailwind::WHITE).fg(tailwind::BLACK))
                        .block(block);

                    let mut s = s.borrow_mut();
                    StatefulWidget::render(table, area, buf, &mut s);
                }
                Err(e) => {
                    let row = Row::new(vec![e.to_string()]);
                    let widths = vec![Constraint::Percentage(100)];
                    let table = Table::new(vec![row], widths).block(block);
                    Widget::render(table, area, buf);
                }
            }
        }
        (_, _, _, Some(e)) => {
            let dur = e.duration().as_millis();
            let block = Block::default()
                .title(" Results ")
                .borders(Borders::ALL)
                .title(Title::from(" Page ").alignment(Alignment::Right))
                .title_bottom(format!(" {}ms ", dur));
            let p = Paragraph::new(e.error().to_string())
                .block(block)
                .wrap(Wrap { trim: true });
            Widget::render(p, area, buf);
        }
        _ => {
            let block = Block::default()
                .title(" Results ")
                .borders(Borders::ALL)
                .title(Title::from(" Page ").alignment(Alignment::Right));
            let row = Row::new(vec!["Run a query to generate results"]);
            let widths = vec![Constraint::Percentage(100)];
            let table = Table::new(vec![row], widths).block(block);
            Widget::render(table, area, buf);
        }
    }
}

pub fn render_sql_help(area: Rect, buf: &mut Buffer, app: &App) {
    let block = Block::default();

    let help = match app.state.sql_tab.mode() {
        SQLTabMode::Normal => {
            if app.state.sql_tab.editor_editable() {
                vec!["'Esc' to exit edit mode"]
            } else {
                vec![
                    "'e' to edit",
                    "'c' to clear editor",
                    "'d' for DDL mode",
                    "'q' to exit app",
                    "'Enter' to run query",
                ]
            }
        }
        SQLTabMode::DDL => {
            if app.state.sql_tab.editor_editable() {
                vec!["'Esc' to exit edit mode"]
            } else {
                vec![
                    "'e' to edit",
                    "'c' to clear editor",
                    "'n' for Normal mode",
                    "'s' to save DDL",
                    "'Enter' to run DDL",
                ]
            }
        }
    };

    let help_text = help.join(" | ");
    let p = Paragraph::new(help_text)
        .block(block)
        .alignment(Alignment::Center);
    p.render(area, buf);
}

pub fn render_sql(area: Rect, buf: &mut Buffer, app: &App) {
    let mode = app.state.sql_tab.mode();

    match mode {
        SQLTabMode::Normal => {
            let constraints = vec![
                Constraint::Fill(1),
                Constraint::Fill(1),
                Constraint::Length(1),
            ];
            let [editor_area, results_area, help_area] =
                Layout::new(Direction::Vertical, constraints).areas(area);

            render_sql_editor(editor_area, buf, app);
            render_sql_results(results_area, buf, app);
            render_sql_help(help_area, buf, app);
        }
        SQLTabMode::DDL => {
            let constraints = vec![Constraint::Fill(1), Constraint::Length(1)];
            let [editor_area, help_area] =
                Layout::new(Direction::Vertical, constraints).areas(area);

            render_sql_editor(editor_area, buf, app);
            render_sql_help(help_area, buf, app);
        }
    };
}
