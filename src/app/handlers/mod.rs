use color_eyre::{eyre::eyre, Result};
use crossterm::event::{self, KeyCode, KeyEvent, KeyModifiers};
use datafusion::physical_plan::execute_stream;
use tracing::{debug, error, info};

use crate::{app::AppEvent, ui::SelectedTab};

use super::App;

pub fn crossterm_event_handler(event: event::Event) -> Option<AppEvent> {
    debug!("crossterm::event: {:?}", event);
    match event {
        event::Event::Key(key) => {
            if key.kind == event::KeyEventKind::Press {
                Some(AppEvent::Key(key))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn tab_navigation_handler(app: &mut App, key: KeyCode) {
    match key {
        KeyCode::Char('e') => app.state.tabs.selected = SelectedTab::Explore,
        _ => {}
    };
}

fn explore_tab_normal_mode_handler(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Char('q') => app.state.should_quit = true,
        KeyCode::Char('i') => app.state.explore_tab.edit(),
        tab @ (KeyCode::Char('p') | KeyCode::Char('t') | KeyCode::Char('e')) => {
            tab_navigation_handler(app, tab)
        }
        KeyCode::Enter => {
            info!("Run query");
            let query = app.state.explore_tab.editor().lines().join("");
            info!("Query: {}", query);
            let ctx = app.execution.session_ctx.clone();
            let _event_tx = app.app_event_tx.clone();
            // TODO: Maybe this should be on a separate runtime to prevent blocking main thread /
            // runtime
            tokio::spawn(async move {
                if let Ok(df) = ctx.sql(&query).await {
                    if let Ok(res) = df.collect().await.map_err(|e| eyre!(e)) {
                        info!("Results: {:?}", res);
                        let _ = _event_tx.send(AppEvent::ExploreQueryResult(res));
                    }
                } else {
                    error!("Error creating dataframe")
                }
            });
        }
        _ => {}
    }
}

fn explore_tab_editable_handler(app: &mut App, key: KeyEvent) {
    info!("KeyEvent: {:?}", key);
    match (key.code, key.modifiers) {
        (KeyCode::Esc, _) => app.state.explore_tab.exit_edit(),
        (KeyCode::Enter, KeyModifiers::CONTROL) => {
            let query = app.state.explore_tab.editor().lines().join("");
            let ctx = app.execution.session_ctx.clone();
            let _event_tx = app.app_event_tx.clone();
            // TODO: Maybe this should be on a separate runtime to prevent blocking main thread /
            // runtime
            tokio::spawn(async move {
                // TODO: Turn this into a match and return the error somehow
                if let Ok(df) = ctx.sql(&query).await {
                    if let Ok(res) = df.collect().await.map_err(|e| eyre!(e)) {
                        info!("Results: {:?}", res);
                        let _ = _event_tx.send(AppEvent::ExploreQueryResult(res));
                    }
                } else {
                    error!("Error creating dataframe")
                }
            });
        }
        _ => app.state.explore_tab.update_editor_content(key),
    }
}

fn explore_tab_app_event_handler(app: &mut App, event: AppEvent) {
    match event {
        AppEvent::Key(key) => match app.state.explore_tab.is_editable() {
            true => explore_tab_editable_handler(app, key),
            false => explore_tab_normal_mode_handler(app, key),
        },
        AppEvent::ExploreQueryResult(r) => app.state.explore_tab.set_query_results(r),
        AppEvent::Tick => {}
        AppEvent::Error => {}
        _ => {}
    };
}

pub fn app_event_handler(app: &mut App, event: AppEvent) -> Result<()> {
    let now = std::time::Instant::now();
    //TODO: Create event to action
    debug!("Tui::Event: {:?}", event);
    match app.state.tabs.selected {
        SelectedTab::Explore => explore_tab_app_event_handler(app, event),
    };
    debug!("Event handling took: {:?}", now.elapsed());
    Ok(())
}
