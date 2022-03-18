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

use log::debug;
use std::cmp;
use std::io;

use unicode_width::UnicodeWidthStr;

use crate::app::datafusion::context::QueryResultsMeta;

/// Single line of text in SQL Editor and cursor over it
pub struct Line {
    // text: String,
    text: io::Cursor<String>,
}

impl Default for Line {
    fn default() -> Line {
        Line {
            text: io::Cursor::new(String::new()),
        }
    }
}

/// All lines in SQL Editor
pub struct Input {
    pub lines: Vec<Line>,
    /// Current line in editor
    pub cursor_row: u16,
    /// Current column in editor
    pub cursor_column: u16,
}

impl Default for Input {
    fn default() -> Input {
        Input {
            lines: Vec::<Line>::new(),
            cursor_row: 0,
            cursor_column: 0,
        }
    }
}

impl Input {
    pub fn combine_lines(&self) -> String {
        let text: Vec<&str> = self
            .lines
            .iter()
            // Replace tabs with spaces
            .map(|line| line.text.get_ref().as_str())
            .collect();
        text.join("")
    }

    pub fn append_char(&mut self, c: char) {
        if self.lines.is_empty() {
            let line = Line::default();
            self.lines.push(line)
        }
        // self.lines[self.cursor_row as usize].text.get_mut().push(c);
        match c {
            '\n' => {
                self.lines[self.cursor_row as usize].text.get_mut().push(c);
                debug!(
                    "Line after appending char {:?} : {:?}",
                    c,
                    self.lines[self.cursor_row as usize].text.get_ref()
                );
                let line = Line::default();
                self.lines.push(line);
                self.cursor_row += 1;
                self.cursor_column = 0;
            }
            '\t' => {
                self.lines[self.cursor_row as usize]
                    .text
                    .get_mut()
                    .push_str("    ");
                self.cursor_column += 4
            }
            _ => {
                self.lines[self.cursor_row as usize].text.get_mut().push(c);
                self.cursor_column += 1;
            }
        }
        debug!(
            "Line after appending char '{}': {}",
            c,
            self.lines[self.cursor_row as usize].text.get_ref()
        );
    }

    pub fn pop(&mut self) -> Option<char> {
        self.lines[self.cursor_row as usize].text.get_mut().pop()
    }

    pub fn up_row(&mut self) {
        if self.cursor_row > 0 {
            let previous_col = self.cursor_column;
            self.cursor_row -= 1;
            let new_row_width = self.lines[self.cursor_row as usize].text.get_ref().width() as u16;
            let new_col = cmp::min(previous_col, new_row_width);
            self.cursor_column = new_col;
        }
    }

    pub fn down_row(&mut self) {
        if self.lines.is_empty() {
            return;
        } else if self.cursor_row + 1 < self.lines.len() as u16 {
            let previous_col = self.cursor_column;
            let new_row_width = self.lines[self.cursor_row as usize].text.get_ref().width() as u16;
            self.cursor_row += 1;
            let new_col = cmp::min(previous_col, new_row_width);
            self.cursor_column = new_col;
        }
    }

    pub fn next_char(&mut self) {
        if self.lines.is_empty()
            || self.cursor_column
                == self.lines[self.cursor_row as usize].text.get_ref().width() as u16
        {
            return;
        } else {
            self.cursor_column += 1
        }
    }

    pub fn previous_char(&mut self) {
        if self.cursor_column > 0 {
            self.cursor_column -= 1
        }
    }

    pub fn backspace(&mut self) {
        match self.lines[self.cursor_row as usize]
            .text
            .get_ref()
            .is_empty()
        {
            true => {
                self.up_row();
                // Pop newline character
                self.pop();
            }
            false => {
                self.lines[self.cursor_row as usize]
                    .text
                    .get_mut()
                    .remove((self.cursor_column - 1) as usize);
                self.cursor_column -= 1
            }
        }
    }

    pub fn clear(&mut self) {
        let lines = Vec::<Line>::new();
        self.lines = lines;
        self.cursor_row = 0;
        self.cursor_column = 0;
    }

    pub fn tab(&mut self) {
        self.append_char('\t')
    }
}

/// The entire editor and it's state
pub struct Editor {
    /// Current value of the input box
    pub input: Input,
    /// Flag if SQL statement was terminated with ';'
    pub sql_terminated: bool,
    /// History of QueryResultMeta
    pub history: Vec<QueryResultsMeta>,
}
impl Default for Editor {
    fn default() -> Editor {
        let mut line_lengths = Vec::new();
        line_lengths.push(0);
        let input = Input::default();
        Editor {
            input,
            history: Vec::new(),
            sql_terminated: false,
        }
    }
}

impl Editor {
    pub fn get_cursor_row(&self) -> u16 {
        self.input.cursor_row
    }

    pub fn get_cursor_column(&self) -> u16 {
        self.input.cursor_column
    }
}
