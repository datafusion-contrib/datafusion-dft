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

use std::io::{stdout, Stdout, Write};

use color_eyre::Result;
use crossterm::{
    cursor,
    event::{DisableBracketedPaste, EnableBracketedPaste},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Layout},
    style::Style,
    text::Span,
    widgets::{Paragraph, Widget},
    Terminal, TerminalOptions, Viewport,
};
use ratatui_textarea::TextArea;

pub(super) struct InlineTerminal {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl InlineTerminal {
    pub fn new() -> Result<Self> {
        // Construct the terminal before enabling raw mode. Drop restores the mode
        // on all subsequent error paths. The alternate screen is never entered.
        let terminal = Terminal::with_options(
            CrosstermBackend::new(stdout()),
            TerminalOptions {
                viewport: Viewport::Inline(5),
            },
        )?;
        let mut inline = Self { terminal };
        enable_raw_mode()?;
        execute!(inline.terminal.backend_mut(), EnableBracketedPaste)?;
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let _ = disable_raw_mode();
            let _ = execute!(stdout(), DisableBracketedPaste, cursor::Show);
            previous(info);
        }));
        Ok(inline)
    }

    pub fn draw(&mut self, editor: &TextArea<'_>, status: &str) -> Result<()> {
        self.terminal.draw(|frame| {
            let [status_area, input_area] =
                Layout::vertical([Constraint::Length(1), Constraint::Min(1)]).areas(frame.area());
            frame.render_widget(Paragraph::new(status), status_area);
            frame.render_widget(editor, input_area);
        })?;
        Ok(())
    }

    pub fn write(&mut self, text: &str) -> Result<()> {
        self.terminal.autoresize()?;
        // Insert one line at a time so output larger than the screen reaches
        // scrollback without allocating a terminal-sized result buffer.
        let width = self.terminal.size()?.width.max(1) as usize;
        for line in text.lines() {
            for part in wrap_line(line, width) {
                self.terminal.insert_before(1, |buffer| {
                    Paragraph::new(part).render(buffer.area, buffer);
                })?;
            }
        }
        Ok(())
    }
}

impl Drop for InlineTerminal {
    fn drop(&mut self) {
        let _ = self.terminal.clear();
        let row = self.terminal.get_frame().area().y;
        let _ = self.terminal.set_cursor_position((0, row));
        let _ = execute!(
            self.terminal.backend_mut(),
            DisableBracketedPaste,
            cursor::Show
        );
        let _ = disable_raw_mode();
        let _ = writeln!(self.terminal.backend_mut());
    }
}

// Wrap at grapheme boundaries so wide results remain present in scrollback.
fn wrap_line(line: &str, width: usize) -> Vec<String> {
    let span = Span::raw(line);
    let mut lines = Vec::new();
    let mut part = String::new();
    let mut used = 0;
    for grapheme in span.styled_graphemes(Style::default()) {
        let columns = Span::raw(grapheme.symbol).width();
        if used + columns > width && !part.is_empty() {
            lines.push(std::mem::take(&mut part));
            used = 0;
        }
        part.push_str(grapheme.symbol);
        used += columns;
    }
    lines.push(part);
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wraps_wide_results_without_losing_unicode() {
        let text = "ab界e\u{301}xy";
        let lines = wrap_line(text, 4);
        assert_eq!(lines.concat(), text);
        assert_eq!(lines, ["ab界", "e\u{301}xy"]);
        assert_eq!(wrap_line("", 80), [""]);
    }
}
