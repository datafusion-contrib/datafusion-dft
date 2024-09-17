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
    widgets::{block::Title, Block, Borders, Paragraph, Row, StatefulWidget, Table, Widget},
};

use crate::app::ui::convert::record_batches_to_table;
use crate::app::App;

pub fn render_sql_editor(area: Rect, buf: &mut Buffer, app: &App) {
    let border_color = if app.state.sql_tab.editor_editable() {
        tailwind::ORANGE.c500
    } else {
        tailwind::WHITE
    };
    let title = Span::from(" Editor ").fg(tailwind::WHITE);
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .fg(border_color);
    let mut editor = app.state.sql_tab.editor();
    editor.set_style(Style::default().fg(tailwind::WHITE));
    editor.set_block(block);
    editor.render(area, buf)
}

pub fn render_sql_results(area: Rect, buf: &mut Buffer, app: &App) {
    // TODO: Change this to a match on state and batch
    if let Some(batch) = app.state.sql_tab.current_batch() {
        if let Some(s) = app.state.sql_tab.query_results_state() {
            let block = Block::default()
                .title(" Results ")
                .borders(Borders::ALL)
                .title(Title::from(" Page ").alignment(Alignment::Right));
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
    } else if let Some(e) = app.state.sql_tab.execution_error() {
        let dur = e.duration().as_millis();
        let block = Block::default()
            .title(" Results ")
            .borders(Borders::ALL)
            .title(Title::from(" Page ").alignment(Alignment::Right))
            .title_bottom(format!(" {}ms ", dur));
        let row = Row::new(vec![e.error().to_string()]);
        let widths = vec![Constraint::Percentage(100)];
        let table = Table::new(vec![row], widths).block(block);
        Widget::render(table, area, buf);
    } else {
        let block = Block::default()
            .title(" Results ")
            .borders(Borders::ALL)
            .title(Title::from(" Page ").alignment(Alignment::Right));
        let row = Row::new(vec!["Run a query to generate results"]);
        let widths = vec![Constraint::Percentage(100)];
        let table = Table::new(vec![row], widths).block(block);
        Widget::render(table, area, buf);
    }

    if let Some(q) = app.state.sql_tab.query() {
        if let Some(r) = q.results() {
            if let Some(s) = app.state.sql_tab.query_results_state() {
                // let stats = Span::from(format!(
                //     " {} rows in {}ms ",
                //     q.num_rows().unwrap_or(0),
                //     q.execution_time().as_millis()
                // ))
                // .fg(tailwind::WHITE);
                // let block = block.title_bottom(stats).fg(tailwind::ORANGE.c500);
                // let maybe_table = record_batches_to_table(r);
                // match maybe_table {
                //     Ok(table) => {
                //         let table = table
                //             .highlight_style(
                //                 Style::default().bg(tailwind::WHITE).fg(tailwind::BLACK),
                //             )
                //             .block(block);
                //
                //         let mut s = s.borrow_mut();
                //         StatefulWidget::render(table, area, buf, &mut s);
                //     }
                //     Err(e) => {
                //         let row = Row::new(vec![e.to_string()]);
                //         let widths = vec![Constraint::Percentage(100)];
                //         let table = Table::new(vec![row], widths).block(block);
                //         Widget::render(table, area, buf);
                //     }
                // }
            }
        } else if let Some(e) = q.error() {
        }
    } else {
    }
}

pub fn render_sql_help(area: Rect, buf: &mut Buffer, app: &App) {
    let block = Block::default();
    let help = if app.state.sql_tab.editor_editable() {
        vec!["'Esc' to exit edit mode"]
    } else {
        vec!["'e' to edit", "'c' to clear editor", "'Enter' to run query"]
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
