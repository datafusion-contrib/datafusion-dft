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
