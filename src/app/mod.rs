pub mod config;
pub mod execution;
pub mod handlers;
pub mod state;

use crate::cli::DftCli;
use crate::{cli, ui};
use color_eyre::eyre::eyre;
use color_eyre::Result;
use crossterm::event as ct;
use datafusion::arrow::array::RecordBatch;
use futures::FutureExt;
use futures_util::StreamExt;
use ratatui::backend::CrosstermBackend;
use ratatui::crossterm::{
    self, cursor, event,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{prelude::*, style::palette::tailwind, widgets::*};
use strum::IntoEnumIterator;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use self::execution::ExecutionContext;
use self::handlers::{app_event_handler, crossterm_event_handler};

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
    ExploreQueryResult(Vec<RecordBatch>),
}

pub struct App<'app> {
    pub cli: DftCli,
    pub state: state::AppState<'app>,
    pub execution: ExecutionContext,
    /// Client for making API calls with.  Holds a connection pool
    /// internally so it should be reused.
    pub app_event_tx: UnboundedSender<AppEvent>,
    pub app_event_rx: UnboundedReceiver<AppEvent>,
    pub app_cancellation_token: CancellationToken,
    pub task: JoinHandle<()>,
    pub streams_task: JoinHandle<()>,
}

impl<'app> App<'app> {
    fn new(state: state::AppState<'app>, cli: DftCli) -> Self {
        let (app_event_tx, app_event_rx) = mpsc::unbounded_channel();
        let app_cancellation_token = CancellationToken::new();
        let task = tokio::spawn(async {});
        let streams_task = tokio::spawn(async {});
        let execution = ExecutionContext::new(state.config.datafusion.clone());

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
                tracing::error!("Failed to abort task in 100 milliseconds for unknown reason");
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
            Ok(_) => debug!("App event sent"),
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
        let ddl = std::fs::read_to_string("~/.datafusionrc").unwrap();
        let _ = self.app_event_tx.send(AppEvent::ExecuteDDL(ddl));
    }

    /// Dispatch to the appropriate event loop based on the command
    pub fn start_event_loop(&mut self) {
        match self.cli.command {
            Some(cli::Command::App(_)) | None => self.start_app_event_loop(),
        }
    }

    /// Get the next event from event loop
    pub async fn next(&mut self) -> Result<AppEvent> {
        self.app_event_rx
            .recv()
            .await
            .ok_or(eyre!("Unable to get event"))
    }

    /// Load portfolio positions from disk, store them in application state with snapshot
    /// information, and connect to the relevant websockets to stream data for the positions.

    fn handle_app_event(&mut self, event: AppEvent) -> Result<()> {
        app_event_handler(self, event)
    }

    fn render_footer(&self, area: Rect, buf: &mut Buffer) {
        Line::raw(" Press q to quit ").centered().render(area, buf);
    }

    fn render_tabs(&self, area: Rect, buf: &mut Buffer) {
        let titles = ui::SelectedTab::iter().map(ui::SelectedTab::title);
        let highlight_style = (Color::default(), tailwind::GRAY.c700);
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
        let vertical = Layout::vertical([
            Constraint::Length(1),
            Constraint::Min(0),
            Constraint::Length(1),
        ]);
        let [header_area, inner_area, footer_area] = vertical.areas(area);

        let horizontal = Layout::horizontal([Constraint::Min(0)]);
        let [tabs_area] = horizontal.areas(header_area);
        self.render_tabs(tabs_area, buf);
        self.state.tabs.selected.render(inner_area, buf, self);
        self.render_footer(footer_area, buf);
    }
}

pub async fn run_app(cli: cli::DftCli, state: state::AppState<'_>) -> Result<()> {
    info!("Running app with state: {:?}", state);
    let mut app = App::new(state, cli.clone());

    match &cli.command {
        Some(cli::Command::App(_)) | None => {
            // app.load_data(false).await?;
            let mut terminal =
                ratatui::Terminal::new(CrosstermBackend::new(std::io::stdout())).unwrap();
            // Start event loop
            app.enter(true)?;
            // Main loop for handling events
            loop {
                let event = app.next().await?;

                if let AppEvent::Render = event.clone() {
                    terminal.draw(|f| f.render_widget(&app, f.size()))?;
                };

                app.handle_app_event(event)?;

                if app.state.should_quit {
                    break;
                }
            }
            app.exit()?;
        }
    }

    Ok(())
}
