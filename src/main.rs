mod app;
mod cli;
mod telemetry;
mod ui;

use crate::app::state;
use app::run_app;
use clap::Parser;
use color_eyre::Result;

#[tokio::main]
async fn main() -> Result<()> {
    telemetry::initialize_logs()?;
    let cli = cli::DftCli::parse();
    let state = state::initialize(&cli);
    run_app(cli.clone(), state).await?;
    Ok(())
}
