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

use core::cell::RefCell;
use std::time::Duration;

use ratatui::widgets::TableState;

#[derive(Debug)]
pub struct HistoryQuery {
    sql: String,
    execution_time: Duration,
}

impl HistoryQuery {
    pub fn new(sql: String, execution_time: Duration) -> Self {
        Self {
            sql,
            execution_time,
        }
    }
    pub fn sql(&self) -> &String {
        &self.sql
    }

    pub fn execution_time(&self) -> &Duration {
        &self.execution_time
    }
}

#[derive(Debug, Default)]
pub struct HistoryTabState {
    history: Vec<HistoryQuery>,
    history_table_state: Option<RefCell<TableState>>,
}

impl<'app> HistoryTabState {
    pub fn new() -> Self {
        Self {
            history: Vec::new(),
            history_table_state: None,
        }
    }

    pub fn history(&self) -> &Vec<HistoryQuery> {
        &self.history
    }

    pub fn add_to_history(&mut self, query: HistoryQuery) {
        self.history.push(query)
    }

    pub fn history_table_state(&self) -> &Option<RefCell<TableState>> {
        &self.history_table_state
    }

    pub fn refresh_history_table_state(&mut self) {
        self.history_table_state = Some(RefCell::new(TableState::default()));
    }
}
