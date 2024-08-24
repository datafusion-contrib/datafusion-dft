use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};

use crate::app::config::get_data_dir;

const LONG_ABOUT: &str = "
Dft

Environment Variables
RUST_LOG { trace | debug | info | error }: Standard across rust ecosystem for determining log level of application.  Default is info.
LOG_SINK { stdout | file }: Write logs to file or stdout.  Default is file.
";

#[derive(Clone, Debug, Parser)]
#[command(author, version, about, long_about = LONG_ABOUT)]
pub struct DftCli {
    #[command(subcommand)]
    pub command: Option<Command>,
}

fn get_config_path(cli_config_arg: &Option<String>) -> PathBuf {
    if let Some(config) = cli_config_arg {
        Path::new(config).to_path_buf()
    } else {
        let mut config = get_data_dir();
        config.push("config.toml");
        config
    }
}

impl DftCli {
    pub fn get_config(&self) -> Option<PathBuf> {
        match &self.command {
            Some(Command::App(args)) => Some(get_config_path(&args.config)),
            _ => None,
        }
    }
}
// TODO: Add a command to get schema / market information
#[derive(Clone, Debug, Subcommand)]
pub enum Command {
    App(AppArgs),
}

#[derive(Clone, Debug, Default, clap::Args)]
#[command(version, about, long_about = None, hide = true)]
pub struct AppArgs {
    #[arg(short, long)]
    pub config: Option<String>,
}
