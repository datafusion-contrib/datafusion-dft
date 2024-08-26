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

use crate::app::config::get_data_dir;
use crate::app::state::tabs::explore::ExploreTabState;
use crate::cli;
use crate::ui::SelectedTab;
use log::{debug, error, info};
use std::path::PathBuf;

use self::tabs::logs::LogsTabState;

use super::config::AppConfig;

#[derive(Debug)]
pub struct Tabs {
    pub selected: SelectedTab,
}

impl Default for Tabs {
    fn default() -> Self {
        Self {
            selected: SelectedTab::Queries,
        }
    }
}

#[derive(Debug)]
pub struct AppState<'app> {
    pub config: AppConfig,
    pub should_quit: bool,
    pub data_dir: PathBuf,
    pub explore_tab: ExploreTabState<'app>,
    pub logs_tab: LogsTabState,
    pub tabs: Tabs,
}

pub fn initialize(args: &cli::DftCli) -> AppState {
    debug!("Initializing state");
    let data_dir = get_data_dir();
    let config_path = args.get_config();
    let config = if config_path.clone().is_some_and(|p| p.exists()) {
        debug!("Config exists");
        let maybe_config_contents = std::fs::read_to_string(config_path.unwrap());
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

    let explore_tab_state = ExploreTabState::new();
    let logs_tab_state = LogsTabState::default();

    AppState {
        config,
        data_dir,
        tabs,
        explore_tab: explore_tab_state,
        logs_tab: logs_tab_state,
        should_quit: false,
    }
}