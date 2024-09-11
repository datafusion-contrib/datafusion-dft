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

pub mod config;
pub mod execution;
pub mod handlers;
pub mod state;

use crate::cli::DftCli;
use crate::{cli, ui};
use color_eyre::eyre::eyre;
use color_eyre::Result;
use crossterm::event as ct;
use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use futures::FutureExt;
use log::{debug, error, info, trace};
use ratatui::backend::CrosstermBackend;
use ratatui::crossterm::{
    self, cursor, event,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{prelude::*, style::palette::tailwind, widgets::*};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use strum::IntoEnumIterator;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use self::execution::ExecutionContext;
use self::handlers::{app_event_handler, crossterm_event_handler};
use self::state::tabs::sql::Query;

#[cfg(feature = "flightsql")]
use self::state::tabs::flightsql::FlightSQLQuery;

#[derive(Clone, Debug)]
pub enum AppEvent {
    Key(event::KeyEvent),
    Error,
    Tick,
    Quit,
    FocusLost,
    FocusGained,
    Render,
    Closed,
    Init,
    Paste(String),
    Mouse(event::MouseEvent),
    Resize(u16, u16),
    ExecuteDDL(String),
    QueryResult(Query),
    #[cfg(feature = "flightsql")]
    EstablishFlightSQLConnection,
    #[cfg(feature = "flightsql")]
    FlightSQLQueryResult(FlightSQLQuery),
}

pub struct App<'app> {
    pub cli: DftCli,
    pub state: state::AppState<'app>,
    pub execution: Arc<ExecutionContext>,
    pub app_event_tx: UnboundedSender<AppEvent>,
    pub app_event_rx: UnboundedReceiver<AppEvent>,
    pub app_cancellation_token: CancellationToken,
    pub task: JoinHandle<()>,
    pub streams_task: JoinHandle<()>,
}

impl<'app> App<'app> {
    pub fn new(state: state::AppState<'app>, cli: DftCli) -> Self {
        let (app_event_tx, app_event_rx) = mpsc::unbounded_channel();
        let app_cancellation_token = CancellationToken::new();
        let task = tokio::spawn(async {});
        let streams_task = tokio::spawn(async {});
        let execution = Arc::new(ExecutionContext::new(state.config.execution.clone()));

        Self {
            cli,
            state,
            task,
            streams_task,
            app_event_rx,
            app_event_tx,
            app_cancellation_token,
            execution,
        }
    }

    /// Enter app, optionally setup `crossterm` with UI settings such as alternative screen and
    /// mouse capture, then start event loop.
    pub fn enter(&mut self, ui: bool) -> Result<()> {
        if ui {
            ratatui::crossterm::terminal::enable_raw_mode()?;
            ratatui::crossterm::execute!(std::io::stdout(), EnterAlternateScreen, cursor::Hide)?;
            if self.state.config.interaction.mouse {
                ratatui::crossterm::execute!(std::io::stdout(), event::EnableMouseCapture)?;
            }
            if self.state.config.interaction.paste {
                ratatui::crossterm::execute!(std::io::stdout(), event::EnableBracketedPaste)?;
            }
        }
        self.start_event_loop();
        Ok(())
    }

    /// Stop event loop. Waits for task to finish for up to 100ms.
    pub fn stop(&self) -> Result<()> {
        self.cancel();
        let mut counter = 0;
        while !self.task.is_finished() {
            std::thread::sleep(std::time::Duration::from_millis(1));
            counter += 1;
            if counter > 50 {
                self.task.abort();
            }
            if counter > 100 {
                error!("Failed to abort task in 100 milliseconds for unknown reason");
                break;
            }
        }
        Ok(())
    }

    /// Exit app, disabling UI settings such as alternative screen and mouse capture.
    pub fn exit(&mut self) -> Result<()> {
        self.stop()?;
        if crossterm::terminal::is_raw_mode_enabled()? {
            if self.state.config.interaction.paste {
                crossterm::execute!(std::io::stdout(), event::DisableBracketedPaste)?;
            }
            if self.state.config.interaction.mouse {
                crossterm::execute!(std::io::stdout(), event::DisableMouseCapture)?;
            }
            crossterm::execute!(std::io::stdout(), LeaveAlternateScreen, cursor::Show)?;
            crossterm::terminal::disable_raw_mode()?;
        }
        Ok(())
    }

    pub fn cancel(&self) {
        self.app_cancellation_token.cancel();
    }

    /// Convert `crossterm::Event` into an application Event. If `None` is returned then the
    /// crossterm event is not yet supported by application
    fn handle_crossterm_event(event: event::Event) -> Option<AppEvent> {
        crossterm_event_handler(event)
    }

    pub fn send_app_event(app_event: AppEvent, tx: &UnboundedSender<AppEvent>) {
        // TODO: Can maybe make tx optional, add a self param, and get tx from self
        let res = tx.send(app_event);
        match res {
            Ok(_) => trace!("App event sent"),
            Err(err) => error!("Error sending app event: {}", err),
        };
    }

    /// Start tokio task which runs an event loop responsible for capturing
    /// terminal events and triggering render / tick events based
    /// on user configured rates.
    fn start_app_event_loop(&mut self) {
        let render_delay =
            std::time::Duration::from_secs_f64(1.0 / self.state.config.display.frame_rate);
        debug!("Render delay: {:?}", render_delay);
        let tick_delay =
            std::time::Duration::from_secs_f64(1.0 / self.state.config.display.tick_rate);
        // TODO-V1: Add this to config
        debug!("Tick delay: {:?}", tick_delay);
        self.cancel();
        self.app_cancellation_token = CancellationToken::new();
        let _cancellation_token = self.app_cancellation_token.clone();
        let _event_tx = self.app_event_tx.clone();

        self.task = tokio::spawn(async move {
            let mut reader = ct::EventStream::new();
            let mut tick_interval = tokio::time::interval(tick_delay);
            debug!("Tick interval: {:?}", tick_interval);
            let mut render_interval = tokio::time::interval(render_delay);
            debug!("Render interval: {:?}", render_interval);
            _event_tx.send(AppEvent::Init).unwrap();
            loop {
                let tick_delay = tick_interval.tick();
                let render_delay = render_interval.tick();
                let crossterm_event = reader.next().fuse();
                tokio::select! {
                  _ = _cancellation_token.cancelled() => {
                      break;
                  }
                  maybe_event = crossterm_event => {
                      let maybe_app_event = match maybe_event {
                            Some(Ok(event)) => {
                                Self::handle_crossterm_event(event)
                            }
                            Some(Err(_)) => Some(AppEvent::Error),
                            None => unimplemented!()
                      };
                      if let Some(app_event) = maybe_app_event {
                          Self::send_app_event(app_event, &_event_tx);
                      };
                  },
                  _ = tick_delay => Self::send_app_event(AppEvent::Tick, &_event_tx),
                  _ = render_delay => Self::send_app_event(AppEvent::Render, &_event_tx),
                }
            }
        });
    }

    pub fn execute_ddl(&mut self) {
        if let Some(user_dirs) = directories::UserDirs::new() {
            let datafusion_rc_path = user_dirs
                .home_dir()
                .join(".datafusion")
                .join(".datafusionrc");
            let maybe_ddl = std::fs::read_to_string(datafusion_rc_path);
            let ddl = match maybe_ddl {
                Ok(ddl) => {
                    info!("DDL: {:?}", ddl);
                    ddl
                }
                Err(err) => {
                    error!("Error reading DDL: {:?}", err);
                    return;
                }
            };
            let _ = self.app_event_tx.send(AppEvent::ExecuteDDL(ddl));
        } else {
            error!("No user directories found");
        }
    }

    #[cfg(feature = "flightsql")]
    pub fn establish_flightsql_connection(&self) {
        let _ = self
            .app_event_tx
            .send(AppEvent::EstablishFlightSQLConnection);
    }

    /// Dispatch to the appropriate event loop based on the command
    pub fn start_event_loop(&mut self) {
        self.start_app_event_loop()
    }

    /// Get the next event from event loop
    pub async fn next(&mut self) -> Result<AppEvent> {
        self.app_event_rx
            .recv()
            .await
            .ok_or(eyre!("Unable to get event"))
    }

    fn handle_app_event(&mut self, event: AppEvent) -> Result<()> {
        app_event_handler(self, event)
    }

    fn render_tabs(&self, area: Rect, buf: &mut Buffer) {
        let titles = ui::SelectedTab::iter().map(ui::SelectedTab::title);
        let highlight_style = (Color::default(), tailwind::ORANGE.c500);
        let selected_tab_index = self.state.tabs.selected as usize;
        Tabs::new(titles)
            .highlight_style(highlight_style)
            .select(selected_tab_index)
            .padding("", "")
            .divider(" ")
            .render(area, buf);
    }
}

impl Widget for &App<'_> {
    /// Note: Ratatui uses Immediate Mode rendering (i.e. the entire UI is redrawn)
    /// on every frame based on application state. There is no permanent widget object
    /// in memory.
    fn render(self, area: Rect, buf: &mut Buffer) {
        let vertical = Layout::vertical([Constraint::Length(1), Constraint::Min(0)]);
        let [header_area, inner_area] = vertical.areas(area);

        let horizontal = Layout::horizontal([Constraint::Min(0)]);
        let [tabs_area] = horizontal.areas(header_area);
        self.render_tabs(tabs_area, buf);
        self.state.tabs.selected.render(inner_area, buf, self);
    }
}

pub async fn run_app(cli: cli::DftCli, state: state::AppState<'_>) -> Result<()> {
    info!("Running app with state: {:?}", state);
    let mut app = App::new(state, cli.clone());

    app.execute_ddl();

    #[cfg(feature = "flightsql")]
    app.establish_flightsql_connection();

    let mut terminal = ratatui::Terminal::new(CrosstermBackend::new(std::io::stdout())).unwrap();
    app.enter(true)?;
    // Main loop for handling events
    loop {
        let event = app.next().await?;

        if let AppEvent::Render = event.clone() {
            terminal.draw(|f| f.render_widget(&app, f.area()))?;
        };

        app.handle_app_event(event)?;

        if app.state.should_quit {
            break;
        }
    }
    app.exit()
}

pub async fn execute_files_or_commands(
    files: Vec<PathBuf>,
    commands: Vec<String>,
    state: &state::AppState<'_>,
) -> Result<()> {
    match (files.is_empty(), commands.is_empty()) {
        (true, true) => Err(eyre!("No files or commands provided to execute")),
        (false, true) => execute_files(files, state).await,
        (true, false) => execute_commands(commands, state).await,
        (false, false) => Err(eyre!(
            "Cannot execute both files and commands at the same time"
        )),
    }
}
async fn execute_files(files: Vec<PathBuf>, state: &state::AppState<'_>) -> Result<()> {
    info!("Executing files: {:?}", files);
    let execution = ExecutionContext::new(state.config.execution.clone());

    for file in files {
        exec_from_file(&execution, &file).await?
    }

    Ok(())
}
async fn execute_commands(commands: Vec<String>, state: &state::AppState<'_>) -> Result<()> {
    info!("Executing commands: {:?}", commands);
    for command in commands {
        exec_from_string(&command, state).await?
    }

    Ok(())
}

async fn exec_from_string(sql: &str, state: &state::AppState<'_>) -> Result<()> {
    let dialect = GenericDialect {};
    let execution = ExecutionContext::new(state.config.execution.clone());
    let statements = DFParser::parse_sql_with_dialect(sql, &dialect)?;
    for statement in statements {
        execution.execute_and_print_statement(statement).await?;
    }
    Ok(())
}

/// run and execute SQL statements and commands from a file, against a context
/// with the given print options
pub async fn exec_from_file(ctx: &ExecutionContext, file: &Path) -> Result<()> {
    let file = File::open(file)?;
    let reader = BufReader::new(file);

    let mut query = String::new();

    for line in reader.lines() {
        let line = line?;
        if line.starts_with("#!") {
            continue;
        }
        if line.starts_with("--") {
            continue;
        }

        let line = line.trim_end();
        query.push_str(line);
        // if we found the end of a query, run it
        if line.ends_with(';') {
            // TODO: if the query errors, should we keep trying to execute
            // the other queries in the file? That is what datafusion-cli does...
            ctx.execute_and_print_stream_sql(&query).await?;
            query.clear();
        } else {
            query.push('\n');
        }
    }

    // run the last line(s) in file if the last statement doesn't contain ‘;’
    // ignore if it only consists of '\n'
    if query.contains(|c| c != '\n') {
        ctx.execute_and_print_stream_sql(&query).await?;
    }

    Ok(())
}
