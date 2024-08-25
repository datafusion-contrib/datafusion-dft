use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::{palette::tailwind, Style, Stylize},
    widgets::{Block, Borders, Row, StatefulWidget, Table, Widget},
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
        if let Some(mut s) = app.state.explore_tab.query_results_state_clone() {
            let table = record_batches_to_table(r)
                .highlight_style(Style::default().bg(tailwind::WHITE).fg(tailwind::BLACK));
            StatefulWidget::render(table, area, buf, &mut s);
        }
    } else if let Some(e) = app.state.explore_tab.query_error() {
        let row = Row::new(vec![e.to_string()]);
        let widths = vec![Constraint::Percentage(100)];
        let table = Table::new(vec![row], widths).block(block);
        Widget::render(table, area, buf);
    } else {
        let row = Row::new(vec!["Run a query to generate results"]);
        let widths = vec![Constraint::Percentage(100)];
        let table = Table::new(vec![row], widths).block(block);
        Widget::render(table, area, buf);
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
