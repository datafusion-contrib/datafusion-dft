use tui_logger::TuiWidgetState;

#[derive(Default)]
pub struct LogsTabState {
    state: TuiWidgetState,
}

impl LogsTabState {
    pub fn state(&self) -> &TuiWidgetState {
        &self.state
    }

    pub fn transition(&mut self, event: tui_logger::TuiWidgetEvent) {
        self.state.transition(event)
    }
}

impl std::fmt::Debug for LogsTabState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogsTabState").finish()
    }
}
