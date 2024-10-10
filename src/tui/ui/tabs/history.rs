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

use log::info;
use ratatui::{
    buffer::Buffer,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{palette::tailwind, Style, Stylize},
    widgets::{Block, Borders, Cell, Paragraph, Row, StatefulWidget, Table, Widget},
};

use crate::tui::App;

fn render_query_placeholder(area: Rect, buf: &mut Buffer) {
    let block = Block::default()
        .title(" Selected Query ")
        .borders(Borders::ALL);
    let paragraph = Paragraph::new("Your selected query will show here").block(block);
    paragraph.render(area, buf);
}

pub fn render_query(area: Rect, buf: &mut Buffer, app: &App) {
    let block = Block::default()
        .title(" Selected Query ")
        .borders(Borders::ALL);
    // TODO: This is horrible, simplify later
    if let Some(history_table_state) = app.state.history_tab.history_table_state() {
        if let Some(selected) = history_table_state.borrow().selected() {
            info!("Selected: {}", selected);
            if let Some(selected_query) = app.state.history_tab.history().get(selected) {
                info!("Selected Query: {:?}", selected_query);
                let query = Paragraph::new(selected_query.sql().as_str()).block(block);
                query.render(area, buf);
            } else {
                info!("Rendering placeholder because no selected_query");
                render_query_placeholder(area, buf);
            }
        } else {
            render_query_placeholder(area, buf);
        }
    } else {
        render_query_placeholder(area, buf);
    }
}

pub fn render_query_history(area: Rect, buf: &mut Buffer, app: &App) {
    let block = Block::default()
        .title(" Query History ")
        .borders(Borders::ALL);
    let history = app.state.history_tab.history();
    let history_table_state = app.state.history_tab.history_table_state();
    match (history.is_empty(), history_table_state) {
        (true, _) | (_, None) => {
            let row = Row::new(vec!["Your query history will show here"]);
            let widths = vec![Constraint::Percentage(100)];
            let table = Table::new(vec![row], widths).block(block);
            Widget::render(table, area, buf);
        }
        (_, Some(table_state)) => {
            let widths = vec![
                Constraint::Percentage(25),
                Constraint::Percentage(25),
                Constraint::Percentage(25),
                Constraint::Percentage(25),
            ];
            let history = app.state.history_tab.history();
            let rows: Vec<Row> = history
                .iter()
                .map(|q| {
                    Row::new(vec![
                        Cell::from(q.context().as_str()),
                        Cell::from(q.sql().as_str()),
                        Cell::from(q.execution_time().as_millis().to_string()),
                        // Not sure showing scanned_bytes is useful anymore in the context of
                        // paginated queries.  Hard coding to zero for now but this will need to be
                        // revisted.  One option I have is removing these type of stats from the
                        // query history table (so we only show execution time) and then
                        // _anything_ ExecutionPlan statistics related is shown in the lower pane
                        // and their is a `analyze` mode that runs the query to completion and
                        // collects all stats to show in a table next to the query.
                        Cell::from(0.to_string()),
                    ])
                })
                .collect();

            let header = Row::new(vec![
                Cell::from("Context"),
                Cell::from("Query"),
                Cell::from("Execution Time(ms)"),
                Cell::from("Scanned Bytes"),
            ])
            .bg(tailwind::ORANGE.c300)
            .fg(tailwind::BLACK)
            .bold();
            let table = Table::new(rows, widths).header(header).block(block.clone());

            let table = table
                .highlight_style(Style::default().bg(tailwind::WHITE).fg(tailwind::BLACK))
                .block(block);

            let mut table_state = table_state.borrow_mut();
            StatefulWidget::render(table, area, buf, &mut table_state);
        }
    }
}

pub fn render_history_help(area: Rect, buf: &mut Buffer, app: &App) {
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

pub fn render_history(area: Rect, buf: &mut Buffer, app: &App) {
    let constraints = vec![
        Constraint::Fill(1),
        Constraint::Fill(1),
        Constraint::Length(1),
    ];
    let [history_area, query_area, help_area] =
        Layout::new(Direction::Vertical, constraints).areas(area);
    render_query_history(history_area, buf, app);
    render_query(query_area, buf, app);
    render_history_help(help_area, buf, app);
}
