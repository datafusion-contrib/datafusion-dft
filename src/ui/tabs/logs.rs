use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::{palette::tailwind, Style, Stylize},
    widgets::{Block, Borders, Row, Table, Widget},
};
use tui_logger::TuiLoggerSmartWidget;

use crate::{app::App, ui::convert::record_batches_to_table};

// pub fn render_sql_editor(area: Rect, buf: &mut Buffer, app: &App) {
//     let border_color = if app.state.explore_tab.is_editable() {
//         tailwind::GREEN.c300
//     } else {
//         tailwind::WHITE
//     };
//     let block = Block::default()
//         .title(" Editor ")
//         .borders(Borders::ALL)
//         .fg(border_color)
//         .title_bottom(" Cmd+Enter to run query ");
//     let mut editor = app.state.explore_tab.editor();
//     editor.set_block(block);
//     editor.render(area, buf)
// }

// pub fn render_sql_results(area: Rect, buf: &mut Buffer, app: &App) {
//     let block = Block::default().title(" Results ").borders(Borders::ALL);
//     if let Some(r) = app.state.explore_tab.query_results() {
//         record_batches_to_table(r).render(area, buf);
//     } else {
//         let row = Row::new(vec!["Run a query to generate results"]);
//         let widths = vec![Constraint::Percentage(100)];
//         let table = Table::new(vec![row], widths).block(block);
//         table.render(area, buf);
//     }
// }

pub fn render_logs(area: Rect, buf: &mut Buffer, app: &App) {
    // let constraints = vec![Constraint::Percentage(50), Constraint::Percentage(50)];
    // let layout = Layout::new(Direction::Vertical, constraints).split(area);
    // let editor = layout[0];
    // let results = layout[1];
    // render_sql_editor(editor, buf, app);
    // render_sql_results(results, buf, app);
    let logs = TuiLoggerSmartWidget::default()
        .style_error(Style::default().fg(tailwind::RED.c700))
        .style_debug(Style::default().fg(tailwind::GREEN.c700))
        .style_warn(Style::default().fg(tailwind::YELLOW.c700))
        .style_trace(Style::default().fg(tailwind::GRAY.c700))
        .style_info(Style::default().fg(tailwind::BLUE.c700));
    logs.render(area, buf);
}
