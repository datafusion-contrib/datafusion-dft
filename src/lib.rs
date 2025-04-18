pub mod args;
pub mod cli;
pub mod config;
pub mod execution;
#[cfg(any(feature = "flightsql", feature = "http"))]
pub mod server;
pub mod telemetry;
pub mod test_utils;
pub mod tpch;
pub mod tui;

pub const APP_NAME: &str = "dft";
