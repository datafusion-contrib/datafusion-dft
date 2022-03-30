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

use crate::app::datafusion::context::QueryResultsMeta;
use crate::app::error::Result;
use crate::app::AppReturn;

/// Single line of text in SQL Editor and cursor over it
#[derive(Debug, Clone)]
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
#[derive(Debug)]
pub struct Input {
    pub lines: Vec<Line>,
    /// Current line in editor
    pub cursor_line_number: u16,
    /// Current column in editor
    pub cursor_position_inside_line: u16,
}

impl Default for Input {
    fn default() -> Input {
        Input {
            lines: vec![Line::default()],
            cursor_line_number: 0,
            cursor_position_inside_line: 0,
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

    fn remove_line(&mut self, line_number: usize) {
        if line_number >= self.lines.len() - 1 {
            return;
        }

        for curr_line_number in line_number..self.lines.len()-1 {
            self.lines[curr_line_number as usize] = self.lines[(curr_line_number + 1) as usize].clone();
        }

        self.lines.pop();
    }

    pub fn append_char(&mut self, c: char) -> Result<AppReturn> {
        if self.lines.is_empty() {
            let line = Line::default();
            self.lines.push(line)
        }
        match c {
            '\n' => {
                self.lines[self.cursor_line_number as usize].text.get_mut().push(c);
                debug!(
                    "Line after appending char {:?} : {:?}",
                    c,
                    self.lines[self.cursor_line_number as usize].text.get_ref()
                );
                let line = Line::default();
                self.lines.push(line);
                self.cursor_line_number += 1;
                self.cursor_position_inside_line = 0;
            }
            '\t' => {
                let tab_replacement = "    ";
                self.lines[self.cursor_line_number as usize]
                    .text
                    .get_mut()
                    .push_str(tab_replacement);
                self.cursor_position_inside_line += tab_replacement.chars().count() as u16;
            }
            _ => {
                self.lines[self.cursor_line_number as usize].text.get_mut().push(c);
                self.cursor_position_inside_line += 1;
            }
        }
        debug!(
            "Line after appending char '{}': {}",
            c,
            self.lines[self.cursor_line_number as usize].text.get_ref()
        );
        Ok(AppReturn::Continue)
    }

    /// Remove last character from current line.
    pub fn pop(&mut self) -> Option<char> {
        self.lines[self.cursor_line_number as usize].text.get_mut().pop()
    }

    pub fn up_row(&mut self) -> Result<AppReturn> {
        if self.cursor_line_number == 0 {
            return Ok(AppReturn::Continue);
        }

        match self.lines[self.cursor_line_number as usize]
            .text
            .get_ref()
            .is_empty()
        {
            true => {
                self.cursor_line_number -= 1;
                let new_line_length = self.lines[self.cursor_line_number as usize]
                    .text
                    .get_ref()
                    .chars()
                    .count() as u16 - 1;
                self.cursor_position_inside_line = new_line_length;
            }
            false => {
                let previous_position_inside_line = self.cursor_position_inside_line;
                self.cursor_line_number -= 1;
                let new_line_length = (self.number_chars_in_current_line() - 1) as u16;
                let new_position_inside_line = cmp::min(previous_position_inside_line, new_line_length);
                self.cursor_position_inside_line = new_position_inside_line;
            }
        }

        Ok(AppReturn::Continue)
    }

    pub fn down_row(&mut self) -> Result<AppReturn> {
        if self.lines.is_empty() || self.cursor_line_number as usize == self.lines.len() - 1 {
            return Ok(AppReturn::Continue);
        } else if self.cursor_line_number + 1 < self.lines.len() as u16 {
            let previous_col = self.cursor_position_inside_line;
            let new_row_width = self.lines[self.cursor_line_number as usize]
                .text
                .get_ref()
                .chars()
                .count() as u16;
            self.cursor_line_number += 1;
            let new_col = cmp::min(previous_col, new_row_width);
            self.cursor_position_inside_line = new_col;
        }
        Ok(AppReturn::Continue)
    }

    fn number_chars_in_current_line(&self) -> usize {
        self.lines[self.cursor_line_number as usize]
            .text
            .get_ref()
            .chars()
            .count()
    }

    fn current_line_is_empty(&self) -> bool {
        self.lines[self.cursor_line_number as usize]
            .text
            .get_ref()
            .is_empty()
    }

    pub fn next_char(&mut self) -> Result<AppReturn> {
        let next_char = self.lines[self.cursor_line_number as usize]
            .text
            .get_ref()
            .chars()
            .skip(self.cursor_position_inside_line as usize)
            .take(1)
            .collect::<String>();

        // We should not jump over a new line. If we see a new line, we COULD jump to the next
        // line
        if next_char == "\n" {
            if (self.cursor_line_number as usize) < self.lines.len() {
                self.cursor_line_number += 1;
            }
            self.cursor_position_inside_line = 0;
            return Ok(AppReturn::Continue);
        }

        if self.lines.is_empty() ||
            self.cursor_position_inside_line == self.number_chars_in_current_line() as u16
            {
            return Ok(AppReturn::Continue);
        } else {
            self.cursor_position_inside_line += 1
        }
        Ok(AppReturn::Continue)
    }

    pub fn previous_char(&mut self) -> Result<AppReturn> {
        if self.cursor_position_inside_line > 0 {
            self.cursor_position_inside_line -= 1
        } else {
            if self.cursor_line_number > 0 {
                let line_of_prev_line = self.lines[(self.cursor_line_number - 1) as usize].text.get_ref().chars().count();
                if line_of_prev_line > 0 {
                    self.cursor_position_inside_line = line_of_prev_line as u16 - 1;
                }
                self.cursor_line_number -= 1;
            }
        }
        Ok(AppReturn::Continue)
    }

    pub fn backspace(&mut self) -> Result<AppReturn> {
        debug!("Backspace entered. Input Before: {:?}", self);
        match self.current_line_is_empty()
        {
            true => {
                self.up_row()?;
                // Pop newline character
                self.pop();
                // if self.cursor_position_inside_line > 0 {
                //     self.cursor_position_inside_line -= 1;
                // }
                // if !self.lines.is_empty() {
                //     self.lines.remove(self.cursor_row as usize);
                // }
            }
            false => {
                if self.cursor_position_inside_line >= 1 {
                    let num_chars_in_current_line = self.number_chars_in_current_line();
                    let curr_line = &self.lines[self.cursor_line_number as usize];
                    // let line_with_delete_character = self.remove_character_from_line()

                    let part_behind_deleted_character = curr_line
                        .text
                        .get_ref()
                        .chars()
                        .skip(self.cursor_position_inside_line as usize)
                        .take(num_chars_in_current_line);
                    let part_before_deleted_character = curr_line
                        .text
                        .get_ref()
                        .chars()
                        .take((self.cursor_position_inside_line - 1) as usize);
                    let line_without_deleted_character = part_before_deleted_character
                        .chain(part_behind_deleted_character)
                        .collect::<String>();
                    self.lines[self.cursor_line_number as usize] = Line {
                        text: io::Cursor::new(line_without_deleted_character),
                    };

                    self.cursor_position_inside_line -= 1;
                } else {
                    // the cur
                }
            }
        };
        debug!("Input After: {:?}", self);
        Ok(AppReturn::Continue)
    }

    pub fn clear(&mut self) -> Result<AppReturn> {
        let lines = Vec::<Line>::new();
        self.lines = lines;
        self.cursor_line_number = 0;
        self.cursor_position_inside_line = 0;
        Ok(AppReturn::Continue)
    }

    pub fn tab(&mut self) -> Result<AppReturn> {
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
        self.input.cursor_line_number
    }

    pub fn get_cursor_column(&self) -> u16 {
        self.input.cursor_position_inside_line
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;
    use crate::app::editor::editor::{Input, Line};

    #[test]
    fn non_ascii_delete() {
        let mut input: Input = Input {
            lines: vec![
                Line {
                    text: Cursor::new(String::from("ä")),
                }
            ],
            cursor_line_number: 0,
            cursor_position_inside_line: 1
        };

        input.backspace();
    }
}