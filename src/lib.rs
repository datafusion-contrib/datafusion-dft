// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

pub mod app;
pub mod cli;
pub mod events;
pub mod utils;

use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{borrow::Borrow, io};

use app::editor::Editor;
use app::{
    core::{App, AppReturn},
    error::DftError,
};
use cli::args::Args;
use ratatui::crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{backend::CrosstermBackend, Terminal};
// use tokio::sync::Mutex;

use crate::app::error::Result;
use crate::app::ui;

use crate::events::{Event, Events};

fn draw_app(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    app: &App<'_>,
) -> Result<()> {
    terminal.draw(|f| {
        f.render_widget(app, f.area());
    })?;
    Ok(())
}

pub async fn run_app(args: Args, ed: Editor<'_>) -> Result<()> {
    // let ed = Editor::default();
    let mut app = App::new(args, ed);
    enable_raw_mode().unwrap();
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture).unwrap();
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).unwrap();

    let tick_rate = Duration::from_millis(200);
    let events = Events::new(tick_rate);

    loop {
        let event = events.next().unwrap();

        terminal.draw(|f| {
            let area = f.area();
            f.render_widget(&app, area);
        })?;

        // Ensure the immutable borrow is dropped before we proceed.
        // Then handle the event (mutable borrow)
        let result = { app.event_handler(event).await };

        if result? == AppReturn::Exit {
            break;
        }
    }

    // loop {
    //     let event = events.next().unwrap();
    //     if let Event::Tick = event {
    //         terminal.draw(|f| f.render_widget(&app, f.area()))?;
    //     }
    //     // draw_app(&mut terminal, &app)?;
    //     // terminal.draw(|f| ui::draw_ui(f, &app))?;
    //     // terminal.draw(|f| ui::draw_ui(f, app.borrow()))?;
    //
    //     // Handle events
    //     let result = app.event_handler(event).await?;
    //     if result == AppReturn::Exit {
    //         break;
    //     }
    // }

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;
    Ok(())
}
