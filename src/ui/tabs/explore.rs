use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::{palette::tailwind, Stylize},
    widgets::{Block, Borders, Row, Table, Widget},
};

use crate::{app::App, ui::convert::record_batches_to_table};

pub fn render_sql_editor(area: Rect, buf: &mut Buffer, app: &App) {
    let border_color = if app.state.explore_tab.is_editable() {
        tailwind::GREEN.c300
    } else {
        tailwind::WHITE
    };
    let block = Block::default()
        .title(" Editor ")
        .borders(Borders::ALL)
        .fg(border_color)
        .title_bottom(" Cmd+Enter to run query ");
    let mut editor = app.state.explore_tab.editor();
    editor.set_block(block);
    editor.render(area, buf)
}

pub fn render_sql_results(area: Rect, buf: &mut Buffer, app: &App) {
    let block = Block::default().title(" Results ").borders(Borders::ALL);
    if let Some(r) = app.state.explore_tab.query_results() {
        record_batches_to_table(r).render(area, buf);
    } else {
        let row = Row::new(vec!["Run a query to generate results"]);
        let widths = vec![Constraint::Percentage(100)];
        let table = Table::new(vec![row], widths).block(block);
        table.render(area, buf);
    }
}

pub fn render_explore(area: Rect, buf: &mut Buffer, app: &App) {
    let constraints = vec![Constraint::Percentage(50), Constraint::Percentage(50)];
    let layout = Layout::new(Direction::Vertical, constraints).split(area);
    let editor = layout[0];
    let results = layout[1];
    render_sql_editor(editor, buf, app);
    render_sql_results(results, buf, app);
}
