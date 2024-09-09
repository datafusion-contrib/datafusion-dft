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

pub mod tabs;

use crate::app::cli;
use crate::app::config::get_data_dir;
use crate::app::state::tabs::sql::SQLTabState;
use crate::app::ui::SelectedTab;
use log::{debug, error, info};
use std::path::PathBuf;

use self::tabs::{history::HistoryTabState, logs::LogsTabState};

use super::config::AppConfig;
#[cfg(feature = "flightsql")]
use crate::app::state::tabs::flightsql::FlightSQLTabState;

#[derive(Debug)]
pub struct Tabs {
    pub selected: SelectedTab,
}

impl Default for Tabs {
    fn default() -> Self {
        Self {
            selected: SelectedTab::SQL,
        }
    }
}

#[derive(Debug)]
pub struct AppState<'app> {
    pub config: AppConfig,
    pub should_quit: bool,
    pub data_dir: PathBuf,
    pub sql_tab: SQLTabState<'app>,
    #[cfg(feature = "flightsql")]
    pub flightsql_tab: FlightSQLTabState<'app>,
    pub logs_tab: LogsTabState,
    pub history_tab: HistoryTabState,
    pub tabs: Tabs,
}

pub fn initialize<'app>(args: cli::DftCli) -> AppState<'app> {
    debug!("Initializing state");
    let data_dir = get_data_dir();
    let config_path = args.get_config();
    debug!("Config path: {:?}", config_path);
    let config = if config_path.exists() {
        debug!("Config exists");
        let maybe_config_contents = std::fs::read_to_string(config_path);
        if let Ok(config_contents) = maybe_config_contents {
            let maybe_parsed_config: std::result::Result<AppConfig, toml::de::Error> =
                toml::from_str(&config_contents);
            match maybe_parsed_config {
                Ok(parsed_config) => {
                    info!("Parsed config: {:?}", parsed_config);
                    parsed_config
                }
                Err(err) => {
                    error!("Error parsing config: {:?}", err);
                    AppConfig::default()
                }
            }
        } else {
            AppConfig::default()
        }
    } else {
        debug!("No config, using default");
        AppConfig::default()
    };

    let tabs = Tabs::default();

    let sql_tab_state = SQLTabState::new();
    #[cfg(feature = "flightsql")]
    let flightsql_tab_state = FlightSQLTabState::new();
    let logs_tab_state = LogsTabState::default();
    let history_tab_state = HistoryTabState::default();

    AppState {
        config,
        data_dir,
        tabs,
        sql_tab: sql_tab_state,
        #[cfg(feature = "flightsql")]
        flightsql_tab: flightsql_tab_state,
        logs_tab: logs_tab_state,
        history_tab: history_tab_state,
        should_quit: false,
    }
}
