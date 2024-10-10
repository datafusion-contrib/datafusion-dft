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
    layout::{Constraint, Direction, Layout, Rect},
    widgets::{Block, Borders, List, Widget},
};

use crate::tui::App;

pub fn render_physical_optimizers(area: Rect, buf: &mut Buffer, app: &App) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Physical Optimizers ");
    let context = app.execution.session_ctx();
    let state_lock = context.state_ref();
    let state = state_lock.read();
    let physical_optimizer_names = state.physical_optimizers().iter().map(|opt| opt.name());

    let list = List::new(physical_optimizer_names).block(block);
    list.render(area, buf)
}

pub fn render_config(area: Rect, buf: &mut Buffer, app: &App) {
    let block = Block::default().borders(Borders::ALL).title(" Config ");
    let context = app.execution.session_ctx();
    let state_lock = context.state_ref();
    let state = state_lock.read();
    let config = state.config();
    let bloom_filter_pruning = config.parquet_bloom_filter_pruning();
    let parquet_row_group_pruning = config.parquet_pruning();

    let config_options = vec![
        format!("Bloom filter pruning: {}", bloom_filter_pruning),
        format!("Parquet row group pruning: {}", parquet_row_group_pruning),
    ];

    let list = List::new(config_options).block(block);
    list.render(area, buf)
}

pub fn render_context(area: Rect, buf: &mut Buffer, app: &App) {
    let constraints = vec![Constraint::Percentage(50), Constraint::Percentage(50)];
    let [physical_optimizers_area, config_area] =
        Layout::new(Direction::Vertical, constraints).areas(area);
    render_physical_optimizers(physical_optimizers_area, buf, app);
    render_config(config_area, buf, app);
}
