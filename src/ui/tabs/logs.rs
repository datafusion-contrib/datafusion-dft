use ratatui::{
    buffer::Buffer,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{palette::tailwind, Style},
    widgets::{Block, Paragraph, Widget},
};
use tui_logger::TuiLoggerSmartWidget;

use crate::app::App;

fn render_smart_widget(area: Rect, buf: &mut Buffer, app: &App) {
    let logs = TuiLoggerSmartWidget::default()
        .style_error(Style::default().fg(tailwind::RED.c700))
        .style_debug(Style::default().fg(tailwind::ORANGE.c700))
        .style_warn(Style::default().fg(tailwind::YELLOW.c700))
        .style_trace(Style::default().fg(tailwind::GRAY.c700))
        .style_info(Style::default().fg(tailwind::WHITE))
        .state(app.state.logs_tab.state());
    logs.render(area, buf);
}

fn render_logs_help(area: Rect, buf: &mut Buffer) {
    let help_text = vec!["f - Focus logs", "h - Hide logs", "⇧ ⇩ - Select target"].join(" | ");

    let block = Block::default();
    let help = Paragraph::new(help_text)
        .block(block)
        .alignment(Alignment::Center);
    help.render(area, buf);
}

pub fn render_logs(area: Rect, buf: &mut Buffer, app: &App) {
    let block = Block::default();
    block.render(area, buf);
    let constraints = vec![Constraint::Min(0), Constraint::Length(1)];
    let [logs_area, footer_area] = Layout::new(Direction::Vertical, constraints).areas(area);
    render_smart_widget(logs_area, buf, app);
    render_logs_help(footer_area, buf);
}
