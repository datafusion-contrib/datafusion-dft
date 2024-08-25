use color_eyre::Result;
use log::LevelFilter;

pub fn initialize_logs() -> Result<()> {
    tui_logger::init_logger(LevelFilter::Debug).unwrap();
    tui_logger::set_default_level(LevelFilter::Debug);

    Ok(())
}
