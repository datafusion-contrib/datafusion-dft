use crate::app::config::{get_data_dir, LOG_ENV, LOG_FILE};
use color_eyre::Result;
use std::collections::HashMap;
use std::fs::OpenOptions;
use tracing::debug;
use tracing_error::ErrorLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, Layer};

pub fn initialize_tracing() -> Result<()> {
    let directory = get_data_dir();
    debug!("Fig data directory: {:?}", directory);
    std::fs::create_dir_all(directory.clone())?;
    let log_path = directory.join(LOG_FILE.clone());
    debug!("Fig log path: {:?}", log_path);
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;
    std::env::set_var(
        "RUST_LOG",
        std::env::var("RUST_LOG")
            .or_else(|_| std::env::var(LOG_ENV.clone()))
            .unwrap_or_else(|_| format!("{}=info", env!("CARGO_CRATE_NAME"))),
    );

    let log_sink = std::env::var("LOG_SINK").unwrap_or("file".to_string());
    if log_sink == "stdout" {
        fmt::init();
    } else {
        let file_subscriber = fmt::layer()
            .with_file(true)
            .with_line_number(true)
            .with_writer(log_file)
            .with_target(false)
            .with_ansi(false)
            .with_filter(tracing_subscriber::filter::EnvFilter::from_default_env());
        tracing_subscriber::registry()
            .with(file_subscriber)
            .with(ErrorLayer::default())
            .init();
    }

    Ok(())
}
