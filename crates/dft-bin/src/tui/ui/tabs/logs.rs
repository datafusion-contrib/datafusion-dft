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
    style::{palette::tailwind, Style},
    widgets::{Block, Paragraph, Widget},
};
use tui_logger::TuiLoggerSmartWidget;

use crate::tui::App;

fn render_smart_widget(area: Rect, buf: &mut Buffer, app: &App) {
    let logs = TuiLoggerSmartWidget::default()
        .style_error(Style::default().fg(tailwind::RED.c700))
        .style_debug(Style::default().fg(tailwind::ORANGE.c500))
        .style_warn(Style::default().fg(tailwind::YELLOW.c700))
        .style_trace(Style::default().fg(tailwind::GRAY.c700))
        .style_info(Style::default().fg(tailwind::WHITE))
        .state(app.state.logs_tab.state());
    logs.render(area, buf);
}

fn render_logs_help(area: Rect, buf: &mut Buffer) {
    let help_text = ["f - Focus logs", "h - Hide logs", "⇧ ⇩ - Select target"].join(" | ");
    let block = Block::default();
    let help = Paragraph::new(help_text)
        .block(block)
        .alignment(Alignment::Center);
    help.render(area, buf);
}

pub fn render_logs(area: Rect, buf: &mut Buffer, app: &App) {
    let block = Block::default();
    block.render(area, buf);
    let constraints = vec![Constraint::Min(0), Constraint::Length(1)];
    let [logs_area, footer_area] = Layout::new(Direction::Vertical, constraints).areas(area);
    render_smart_widget(logs_area, buf, app);
    render_logs_help(footer_area, buf);
}
