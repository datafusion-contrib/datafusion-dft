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

use crate::execution::ExecutionStats;

#[derive(Debug)]
pub enum Context {
    Local,
    FlightSQL,
}

impl Context {
    pub fn as_str(&self) -> &str {
        match self {
            Context::Local => "Local",
            Context::FlightSQL => "FlightSQL",
        }
    }
}

#[derive(Debug)]
pub struct HistoryQuery {
    context: Context,
    sql: String,
    execution_time: Duration,
    execution_stats: Option<ExecutionStats>,
    _error: Option<String>,
}

impl HistoryQuery {
    pub fn new(
        context: Context,
        sql: String,
        execution_time: Duration,
        execution_stats: Option<ExecutionStats>,
        _error: Option<String>,
    ) -> Self {
        Self {
            context,
            sql,
            execution_time,
            execution_stats,
            _error,
        }
    }
    pub fn sql(&self) -> &String {
        &self.sql
    }

    pub fn execution_time(&self) -> &Duration {
        &self.execution_time
    }

    pub fn execution_stats(&self) -> &Option<ExecutionStats> {
        &self.execution_stats
    }

    // pub fn scanned_bytes(&self) -> usize {
    //     if let Some(stats) = &self.execution_stats {
    //         stats.bytes_scanned()
    //     } else {
    //         0
    //     }
    // }

    pub fn context(&self) -> &Context {
        &self.context
    }
}

#[derive(Debug, Default)]
pub struct HistoryTabState {
    history: Vec<HistoryQuery>,
    history_table_state: Option<RefCell<TableState>>,
}

impl HistoryTabState {
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
