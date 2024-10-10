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

pub mod app_execution;
pub mod handlers;
pub mod state;
pub mod ui;

use color_eyre::eyre::eyre;
use color_eyre::Result;
use crossterm::event as ct;
use datafusion::arrow::array::RecordBatch;
use futures::FutureExt;
use log::{debug, error, info, trace};
use ratatui::backend::CrosstermBackend;
use ratatui::crossterm::{
    self, cursor, event,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{prelude::*, style::palette::tailwind, widgets::*};
use std::sync::Arc;
use std::time::Duration;
use strum::IntoEnumIterator;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use self::app_execution::AppExecution;
use self::handlers::{app_event_handler, crossterm_event_handler};
use crate::execution::ExecutionContext;

#[derive(Clone, Debug)]
pub struct ExecutionError {
    query: String,
    error: String,
    duration: Duration,
}

#[derive(Clone, Debug)]
pub struct ExecutionResultsBatch {
    query: String,
    batch: RecordBatch,
    duration: Duration,
}

impl ExecutionResultsBatch {
    pub fn new(query: String, batch: RecordBatch, duration: Duration) -> Self {
        Self {
            query,
            batch,
            duration,
        }
    }

    pub fn query(&self) -> &str {
        &self.query
    }

    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    pub fn duration(&self) -> &Duration {
        &self.duration
    }
}

impl ExecutionError {
    pub fn new(query: String, error: String, duration: Duration) -> Self {
        Self {
            query,
            error,
            duration,
        }
    }

    pub fn query(&self) -> &str {
        &self.query
    }

    pub fn error(&self) -> &str {
        &self.error
    }

    pub fn duration(&self) -> &Duration {
        &self.duration
    }
}

#[derive(Debug)]
pub enum AppEvent {
    Key(event::KeyEvent),
    Error,
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
    // Query Execution
    NewExecution,
    ExecutionResultsNextBatch(ExecutionResultsBatch),
    ExecutionResultsPreviousPage,
    ExecutionResultsError(ExecutionError),
    // FlightSQL
    #[cfg(feature = "flightsql")]
    FlightSQLEstablishConnection,
    #[cfg(feature = "flightsql")]
    FlightSQLNewExecution,
    #[cfg(feature = "flightsql")]
    FlightSQLExecutionResultsNextBatch(ExecutionResultsBatch),
    #[cfg(feature = "flightsql")]
    FlightSQLExecutionResultsNextPage,
    #[cfg(feature = "flightsql")]
    FlightSQLExecutionResultsPreviousPage,
    #[cfg(feature = "flightsql")]
    FlightSQLExecutionResultsError(ExecutionError),
    #[cfg(feature = "flightsql")]
    FlightSQLFailedToConnect,
    #[cfg(feature = "flightsql")]
    FlightSQLConnected,
}

pub struct App<'app> {
    state: state::AppState<'app>,
    execution: Arc<AppExecution>,
    event_tx: UnboundedSender<AppEvent>,
    event_rx: UnboundedReceiver<AppEvent>,
    cancellation_token: CancellationToken,
    task: JoinHandle<()>,
    ddl_task: Option<JoinHandle<()>>,
}

impl<'app> App<'app> {
    pub fn new(state: state::AppState<'app>, execution: ExecutionContext) -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let cancellation_token = CancellationToken::new();
        let task = tokio::spawn(async {});
        // let ddl_task = tokio::spawn(async {});
        let app_execution = Arc::new(AppExecution::new(Arc::new(execution)));

        Self {
            state,
            task,
            event_rx,
            event_tx,
            cancellation_token,
            execution: app_execution,
            ddl_task: None,
        }
    }

    pub fn event_tx(&self) -> UnboundedSender<AppEvent> {
        self.event_tx.clone()
    }

    pub fn ddl_task(&mut self) -> &mut Option<JoinHandle<()>> {
        &mut self.ddl_task
    }

    pub fn event_rx(&mut self) -> &mut UnboundedReceiver<AppEvent> {
        &mut self.event_rx
    }

    pub fn execution(&self) -> Arc<AppExecution> {
        Arc::clone(&self.execution)
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    pub fn set_cancellation_token(&mut self, cancellation_token: CancellationToken) {
        self.cancellation_token = cancellation_token;
    }

    pub fn state(&self) -> &state::AppState<'app> {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut state::AppState<'app> {
        &mut self.state
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
        self.start_app_event_loop();
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
        self.cancellation_token.cancel();
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
    /// terminal events and triggering render events based on user configured rates.
    fn start_app_event_loop(&mut self) {
        let render_delay =
            std::time::Duration::from_secs_f64(1.0 / self.state.config.display.frame_rate);
        debug!("Render delay: {:?}", render_delay);
        // TODO-V1: Add this to config
        self.cancel();
        self.set_cancellation_token(CancellationToken::new());
        let _cancellation_token = self.cancellation_token();
        let _event_tx = self.event_tx();

        self.task = tokio::spawn(async move {
            let mut reader = ct::EventStream::new();
            let mut render_interval = tokio::time::interval(render_delay);
            debug!("Render interval: {:?}", render_interval);
            _event_tx.send(AppEvent::Init).unwrap();
            loop {
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
                  _ = render_delay => Self::send_app_event(AppEvent::Render, &_event_tx),
                }
            }
        });
    }

    /// Execute DDL from users DDL file
    pub fn execute_ddl(&mut self) {
        let ddl = self.execution.load_ddl().unwrap_or_default();
        info!("DDL: {:?}", ddl);
        if !ddl.is_empty() {
            self.state.sql_tab.add_ddl_to_editor(ddl.clone());
        }
        let _ = self.event_tx().send(AppEvent::ExecuteDDL(ddl));
    }

    #[cfg(feature = "flightsql")]
    pub fn establish_flightsql_connection(&self) {
        let _ = self.event_tx().send(AppEvent::FlightSQLEstablishConnection);
    }

    /// Get the next event from event loop
    pub async fn next(&mut self) -> Result<AppEvent> {
        self.event_rx()
            .recv()
            .await
            .ok_or(eyre!("Unable to get event"))
    }

    pub fn handle_app_event(&mut self, event: AppEvent) -> Result<()> {
        app_event_handler(self, event)
    }

    fn render_tabs(&self, area: Rect, buf: &mut Buffer) {
        let titles = ui::SelectedTab::iter().map(|t| ui::SelectedTab::title(t, self));
        let highlight_style = (Color::default(), tailwind::ORANGE.c500);
        let selected_tab_index = self.state.tabs.selected as usize;
        Tabs::new(titles)
            .highlight_style(highlight_style)
            .select(selected_tab_index)
            .padding("", "")
            .divider(" ")
            .render(area, buf);
    }

    pub async fn loop_without_render(&mut self) -> Result<()> {
        self.enter(false)?;
        // Main loop for handling events
        loop {
            let event = self.next().await?;
            self.handle_app_event(event)?;
            if self.state.should_quit {
                break Ok(());
            }
        }
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

impl App<'_> {
    /// Run the main event loop for the application
    pub async fn run_app(self) -> Result<()> {
        info!("Running app with state: {:?}", self.state);
        let mut app = self;

        app.execute_ddl();

        #[cfg(feature = "flightsql")]
        app.establish_flightsql_connection();

        let mut terminal =
            ratatui::Terminal::new(CrosstermBackend::new(std::io::stdout())).unwrap();
        app.enter(true)?;
        // Main loop for handling events
        loop {
            let event = app.next().await?;

            if let AppEvent::Render = &event {
                terminal.draw(|f| f.render_widget(&app, f.area()))?;
            };

            app.handle_app_event(event)?;

            if app.state.should_quit {
                break;
            }
        }
        app.exit()
    }
}
