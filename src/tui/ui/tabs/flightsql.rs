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

use crate::tui::ui::convert::record_batch_to_table;
use crate::tui::App;

pub fn render_sql_editor(area: Rect, buf: &mut Buffer, app: &App) {
    let border_color = if app.state.flightsql_tab.editor_editable() {
        tailwind::ORANGE.c500
    } else {
        tailwind::WHITE
    };
    let title = Span::from(" Editor ").fg(tailwind::WHITE);
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .fg(border_color);
    let mut editor = app.state.flightsql_tab.editor();
    editor.set_block(block);
    editor.render(area, buf)
}

pub fn render_sql_results(area: Rect, buf: &mut Buffer, app: &App) {
    let flightsql_tab = &app.state.flightsql_tab;
    match (
        flightsql_tab.current_page(),
        flightsql_tab.current_page_results(),
        flightsql_tab.query_results_state(),
        flightsql_tab.execution_error(),
        flightsql_tab.in_progress(),
    ) {
        (_, _, _, _, true) => {
            let block = Block::default()
                .title(" Results ")
                .borders(Borders::ALL)
                .title(Title::from(" Page ").alignment(Alignment::Right));
            let row = Row::new(vec!["Executing query..."]);
            let widths = vec![Constraint::Percentage(100)];
            let table = Table::new(vec![row], widths).block(block);
            Widget::render(table, area, buf);
        }

        (Some(page), Some(batch), Some(s), None, false) => {
            let block = Block::default()
                .title(" Results ")
                .borders(Borders::ALL)
                .title(Title::from(format!(" Page {page} ")).alignment(Alignment::Right));
            let maybe_table = record_batch_to_table(&batch);

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
        (_, _, _, Some(e), _) => {
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
    let help = if app.state.flightsql_tab.editor_editable() {
        vec!["'Esc' to exit edit mode"]
    } else {
        vec![
            "'e' to edit",
            "'c' to clear editor",
            "'q' to exit app",
            "'Enter' to run query",
        ]
    };

    let help_text = help.join(" | ");
    let p = Paragraph::new(help_text)
        .block(block)
        .alignment(Alignment::Center);
    p.render(area, buf);
}

pub fn render_sql(area: Rect, buf: &mut Buffer, app: &App) {
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
