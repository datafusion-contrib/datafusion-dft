use color_eyre::{eyre::eyre, Result};
use log::{debug, error, info, trace};
use ratatui::crossterm::event::{self, KeyCode, KeyEvent, KeyModifiers};
use tui_logger::TuiWidgetEvent;

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
        KeyCode::Char('e') => app.state.tabs.selected = SelectedTab::Queries,
        KeyCode::Char('l') => app.state.tabs.selected = SelectedTab::Logs,
        _ => {}
    };
}

fn explore_tab_normal_mode_handler(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Char('q') => app.state.should_quit = true,
        KeyCode::Char('i') => {
            let editor = app.state.explore_tab.editor();
            let lines = editor.lines();
            let content = lines.join("");
            info!("Conent: {}", content);
            let default = "Enter a query here.";
            if content == default {
                info!("Clearing default content");
                app.state.explore_tab.clear_placeholder();
            }
            app.state.explore_tab.edit();
        }
        tab @ (KeyCode::Char('e') | KeyCode::Char('l')) => tab_navigation_handler(app, tab),
        KeyCode::Enter => {
            info!("Run query");
            let query = app.state.explore_tab.editor().lines().join("");
            info!("Query: {}", query);
            let ctx = app.execution.session_ctx.clone();
            let _event_tx = app.app_event_tx.clone();
            // TODO: Maybe this should be on a separate runtime to prevent blocking main thread /
            // runtime
            tokio::spawn(async move {
                match ctx.sql(&query).await {
                    Ok(df) => match df.collect().await {
                        Ok(res) => {
                            info!("Results: {:?}", res);
                            let _ = _event_tx.send(AppEvent::ExploreQueryResult(res));
                        }
                        Err(e) => {
                            error!("Error collecting results: {:?}", e);
                            let _ = _event_tx.send(AppEvent::ExploreQueryError(e.to_string()));
                        }
                    },
                    Err(e) => {
                        error!("Error creating dataframe: {:?}", e);
                        let _ = _event_tx.send(AppEvent::ExploreQueryError(e.to_string()));
                    }
                }
                // if let Ok(df) = ctx.sql(&query).await {
                //     if let Ok(res) = df.collect().await.map_err(|e| eyre!(e)) {
                //         info!("Results: {:?}", res);
                //         let _ = _event_tx.send(AppEvent::ExploreQueryResult(res));
                //     }
                // } else {
                //     error!("Error creating dataframe")
                // }
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

fn logs_tab_key_event_handler(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Char('h') => {
            app.state.logs_tab.transition(TuiWidgetEvent::HideKey);
        }
        KeyCode::Char('f') => {
            app.state.logs_tab.transition(TuiWidgetEvent::FocusKey);
        }
        KeyCode::Char('+') => {
            app.state.logs_tab.transition(TuiWidgetEvent::PlusKey);
        }
        KeyCode::Char('-') => {
            app.state.logs_tab.transition(TuiWidgetEvent::MinusKey);
        }
        KeyCode::Char('q') => app.state.should_quit = true,
        KeyCode::Char(' ') => {
            app.state.logs_tab.transition(TuiWidgetEvent::SpaceKey);
        }
        KeyCode::Esc => {
            app.state.logs_tab.transition(TuiWidgetEvent::EscapeKey);
        }
        KeyCode::Down => {
            app.state.logs_tab.transition(TuiWidgetEvent::DownKey);
        }
        KeyCode::Up => {
            app.state.logs_tab.transition(TuiWidgetEvent::UpKey);
        }
        KeyCode::Right => {
            app.state.logs_tab.transition(TuiWidgetEvent::RightKey);
        }
        KeyCode::Left => {
            app.state.logs_tab.transition(TuiWidgetEvent::LeftKey);
        }
        KeyCode::PageDown => {
            app.state.logs_tab.transition(TuiWidgetEvent::NextPageKey);
        }

        KeyCode::PageUp => {
            app.state.logs_tab.transition(TuiWidgetEvent::PrevPageKey);
        }
        KeyCode::Char('e') => {
            app.state.tabs.selected = SelectedTab::Queries;
        }
        _ => {}
    }
}

fn logs_tab_app_event_handler(app: &mut App, event: AppEvent) {
    match event {
        AppEvent::Key(key) => logs_tab_key_event_handler(app, key),
        AppEvent::ExploreQueryResult(r) => app.state.explore_tab.set_query_results(r),
        AppEvent::Tick => {}
        AppEvent::Error => {}
        _ => {}
    };
}

pub fn app_event_handler(app: &mut App, event: AppEvent) -> Result<()> {
    let now = std::time::Instant::now();
    //TODO: Create event to action
    trace!("Tui::Event: {:?}", event);
    if let AppEvent::ExecuteDDL(ddl) = event {
        let queries: Vec<String> = ddl.split(";").map(|s| s.to_string()).collect();
        queries.into_iter().for_each(|q| {
            let ctx = app.execution.session_ctx.clone();
            tokio::spawn(async move {
                if let Ok(df) = ctx.sql(&q).await {
                    if let Ok(_) = df.collect().await {
                        info!("Successful DDL");
                    }
                }
            });
        })
    } else {
        match app.state.tabs.selected {
            SelectedTab::Queries => explore_tab_app_event_handler(app, event),
            SelectedTab::Logs => logs_tab_app_event_handler(app, event),
        };
    }
    trace!("Event handling took: {:?}", now.elapsed());
    Ok(())
}
