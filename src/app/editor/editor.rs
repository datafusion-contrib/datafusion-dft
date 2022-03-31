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
            // Add new line characters
            .map(|line| line.text.get_ref().as_str())
            .collect();
        text.join("\n")
    }

    fn insert_character_into_current_line(&self, character: char, pos: usize) -> String {
        let mut line_as_char_vec = self.lines[self.cursor_line_number as usize]
            .text
            .get_ref()
            .chars()
            .collect::<Vec<char>>();
        line_as_char_vec.insert(pos, character);
        line_as_char_vec.iter().collect::<String>()

        // self.lines[self.cursor_line_number as usize]
        //     .text
        //     .get_ref()
        //     .chars()
        //     .take(pos)
        //     .chain(String::from(character).chars() )
        //     .chain(self.lines[self.cursor_line_number as usize].text.get_ref().chars().skip(pos).take(self.number_chars_in_current_line()))
        //     .collect::<String>()
    }

    pub fn insert_char(&mut self, c: char) -> Result<AppReturn> {
        if self.lines.is_empty() {
            let line = Line::default();
            self.lines.push(line)
        }
        match c {
            '\n' => {
                // self.lines[self.cursor_line_number as usize].text.get_mut().push(c);
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
            ch => {
                // if cursor is in the end of the line, then just append the character
                if self.cursor_position_inside_line as usize == self.number_chars_in_current_line() {
                    self.lines[self.cursor_line_number as usize].text.get_mut().push(c);
                } else {
                    // we have to construct a new string
                    let new_line = self.insert_character_into_current_line(ch, self.cursor_position_inside_line as usize);
                    self.lines[self.cursor_line_number as usize] = Line {
                        text: io::Cursor::new(new_line)
                    }
                }
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
                self.cursor_position_inside_line = 0;
            }
            false => {
                let position_inside_current_line = self.cursor_position_inside_line;
                self.cursor_line_number -= 1;
                let length_of_line_above = self.number_chars_in_current_line() as u16;
                let new_position_inside_line = cmp::min(position_inside_current_line, length_of_line_above);
                self.cursor_position_inside_line = new_position_inside_line;
            }
        }

        Ok(AppReturn::Continue)
    }

    pub fn end_row(&mut self) -> Result<AppReturn> {
        self.cursor_position_inside_line = self.number_chars_in_current_line() as u16;
        Ok(AppReturn::Continue)
    }

    pub fn home_row(&mut self) -> Result<AppReturn> {
        self.cursor_position_inside_line = 0;
        Ok(AppReturn::Continue)
    }

    pub fn down_row(&mut self) -> Result<AppReturn> {
        if self.lines.is_empty() || (self.cursor_line_number as usize) == self.lines.len() - 1 {
            return Ok(AppReturn::Continue);
        } else if self.cursor_line_number + 1 < self.lines.len() as u16 {
            let cursor_position_in_current_line = self.cursor_position_inside_line;
            let length_of_next_line = self.number_chars_in_next_line() as u16;
            self.cursor_line_number += 1;
            let cursor_position_inside_next_line = cmp::min(cursor_position_in_current_line, length_of_next_line);
            self.cursor_position_inside_line = cursor_position_inside_next_line;
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

    fn number_chars_in_next_line(&self) -> usize {
        if ((self.cursor_line_number + 1) as usize) < self.lines.len() {
            self.lines[(self.cursor_line_number + 1) as usize]
                .text
                .get_ref()
                .chars()
                .count()
        } else {
            0
        }
    }

    fn number_chars_in_previous_line(&self) -> usize {
        if self.cursor_line_number > 0 && self.lines.len() > 0 {
            self.lines[(self.cursor_line_number - 1) as usize]
                .text
                .get_ref()
                .chars()
                .count()
        } else {
            0
        }
    }

    fn current_line_is_empty(&self) -> bool {
        self.lines[self.cursor_line_number as usize]
            .text
            .get_ref()
            .is_empty()
    }

    pub fn next_char(&mut self) -> Result<AppReturn> {
        let chars_in_current_line = self.number_chars_in_current_line() as u16;
        if self.lines.is_empty() ||
            self.cursor_position_inside_line == chars_in_current_line{
            if (self.cursor_line_number as usize + 1) < self.lines.len() {
                self.cursor_line_number += 1;
                self.cursor_position_inside_line = 0;
            }
            return Ok(AppReturn::Continue);
        } else {
            if self.cursor_position_inside_line < chars_in_current_line {
                self.cursor_position_inside_line += 1;
            }
        }
        Ok(AppReturn::Continue)
    }

    pub fn previous_char(&mut self) -> Result<AppReturn> {
        if self.cursor_position_inside_line > 0 {
            // just go one position to the left
            self.cursor_position_inside_line -= 1
        } else {
            // we are in the beginning of a line -> jump to the end of the previous line
            if self.cursor_line_number > 0 {
                // we are not in the first line, so we can jump one line up
                let length_of_previous_line = self.number_chars_in_previous_line();
                if length_of_previous_line > 0 {
                    self.cursor_position_inside_line = length_of_previous_line as u16 - 1;
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
        self.insert_char('\t')
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
mod tests {
    use std::io::Cursor;
    use crate::app::editor::editor::{Input, Line};

    #[test]
    fn can_delete_non_ascii_characters() {
        let mut input: Input = Input {
            lines: vec![
                Line {
                    text: Cursor::new(String::from("äää")),
                }
            ],
            cursor_line_number: 0,
            cursor_position_inside_line: 3
        };

        input.backspace().expect("Expect that can delete character");
        assert_eq!(input.cursor_line_number, 0);
        assert_eq!(input.cursor_position_inside_line, 2);

        input.backspace().expect("Expect that can delete character");
        assert_eq!(input.cursor_line_number, 0);
        assert_eq!(input.cursor_position_inside_line, 1);
    }

    #[test]
    fn next_character_in_one_line() {
        let mut input: Input = Input {
            lines: vec![
                Line {
                    text: Cursor::new(String::from("aaa")),
                },
            ],
            cursor_line_number: 0,
            cursor_position_inside_line: 0
        };

        input.next_char().expect("Could move to next character");
        assert_eq!(input.cursor_position_inside_line, 1, "When moving once, cursor should be after first character");

        input.next_char().expect("Could move to next character");
        assert_eq!(input.cursor_position_inside_line, 2);

        input.next_char().expect("Could move to next character");
        assert_eq!(input.cursor_position_inside_line, 3);

        input.next_char().expect("Could move to next character");
        assert_eq!(input.cursor_position_inside_line, 3, "When line is over and no next line exists, cursor should stop");
        assert_eq!(input.cursor_line_number, 0, "When line is over and no next line exists, cursor should stop");
    }

    #[test]
    fn previous_character_in_one_line() {
        let mut input: Input = Input {
            lines: vec![
                Line {
                    text: Cursor::new(String::from("aaa")),
                },
            ],
            cursor_line_number: 0,
            cursor_position_inside_line: 3
        };

        input.previous_char().expect("Could move to previous character");
        assert_eq!(input.cursor_position_inside_line, 2);

        input.previous_char().expect("Could move to previous character");
        assert_eq!(input.cursor_position_inside_line, 1);

        input.previous_char().expect("Could move to previous character");
        assert_eq!(input.cursor_position_inside_line, 0);

        input.previous_char().expect("Could move to previous character");
        assert_eq!(input.cursor_position_inside_line, 0);
    }

    #[test]
    fn jump_to_next_line_on_next_character_at_the_end_of_line() {
        let mut input: Input = Input {
            lines: vec![
                Line {
                    text: Cursor::new(String::from("aa")),
                },

                Line {
                    text: Cursor::new(String::from("bb")),
                },
            ],
            cursor_line_number: 0,
            cursor_position_inside_line: 0
        };

        input.next_char().expect("Could move to next character");
        input.next_char().expect("Could move to next character");

        // we expect to jump to the next line here
        input.next_char().expect("Could move to next character");
        assert_eq!(input.cursor_line_number, 1, "Cursor should have jumped to next line");
        assert_eq!(input.cursor_position_inside_line, 0, "Cursor should be at beginning of the line");

        input.next_char().expect("Could move to next character");
        assert_eq!(input.cursor_line_number, 1);
        assert_eq!(input.cursor_position_inside_line, 1, "Cursor should be at the end of second line");

        input.next_char().expect("Could move to next character");
        assert_eq!(input.cursor_line_number, 1);
        assert_eq!(input.cursor_position_inside_line, 2);

        input.next_char().expect("Could move to next character");
        assert_eq!(input.cursor_line_number, 1);
        assert_eq!(input.cursor_position_inside_line, 2, "When there is no next line, cursor should stay unchanged");
    }

    #[test]
    fn jump_to_previous_line_on_previous_character_at_the_beginning_of_line() {
        let mut input: Input = Input {
            lines: vec![
                Line {
                    text: Cursor::new(String::from("aa")),
                },

                Line {
                    text: Cursor::new(String::from("bb")),
                },
            ],
            cursor_line_number: 1,
            cursor_position_inside_line: 0
        };

        input.previous_char().expect("Could move to next character");
        assert_eq!(input.cursor_line_number, 0, "Cursor should have jumped to previous line");
        assert_eq!(input.cursor_position_inside_line, 1, "Cursor should be at end of the previous line");
    }

    #[test]
    fn non_ascii_character_count() {
        let input: Input = Input {
            lines: vec![
                Line {
                    text: Cursor::new(String::from("äää")),
                }
            ],
            cursor_line_number: 0,
            cursor_position_inside_line: 0
        };

        assert_eq!(input.number_chars_in_current_line(), 3);

        let input2: Input = Input {
            lines: vec![
                Line {
                    text: Cursor::new(String::from("äääb")),
                }
            ],
            cursor_line_number: 0,
            cursor_position_inside_line: 0
        };
        assert_eq!(input2.number_chars_in_current_line(), 4);
    }

    #[test]
    fn test_append_char() {
        let mut input: Input = Input::default();

        // Input: ""
        input.insert_char('ä').expect("Could append a character");
        assert_eq!(input.cursor_line_number, 0);
        assert_eq!(input.cursor_position_inside_line, 1);
        assert_eq!(input.number_chars_in_current_line(), 1);

        // Input: "ä"
        input.insert_char('b').expect("Could append a character");
        assert_eq!(input.cursor_line_number, 0);
        assert_eq!(input.cursor_position_inside_line, 2);
        assert_eq!(input.number_chars_in_current_line(), 2);

        // Input: "äb"
        input.insert_char('\t').expect("Could append a character");
        assert_eq!(input.cursor_line_number, 0);
        assert_eq!(input.cursor_position_inside_line, 6);
        assert_eq!(input.number_chars_in_current_line(), 6);

        // Input: "äb    "
        input.insert_char('\n').expect("Could append a character");
        assert_eq!(input.cursor_line_number, 1);
        assert_eq!(input.cursor_position_inside_line, 0);
        assert_eq!(input.number_chars_in_current_line(), 0);

        // Input: "äb    "
        //        ""
        input.insert_char('a').expect("Could append a character");
        assert_eq!(input.cursor_line_number, 1);
        assert_eq!(input.cursor_position_inside_line, 1);
        assert_eq!(input.number_chars_in_current_line(), 1);

        // Input: "ä|b    " <- cursor |
        //        "a"
        input.up_row().expect("Can go up");
        input.insert_char('a').expect("Could append a character");
        assert_eq!(input.cursor_line_number, 0);
        assert_eq!(input.cursor_position_inside_line, 2);
        assert_eq!(input.number_chars_in_current_line(), 7, "{}", format!("Line is: {}", input.lines[input.cursor_line_number as usize].text.get_ref()));

        // Input: "äab    "
        //        "a|"       <- cursor |
        input.down_row().expect("Can go down");
        input.previous_char().expect("Can go left");
        input.insert_char('b');
        // Input: "äab    "
        //        "b|a"  <- cursor |
        assert_eq!(input.cursor_line_number, 1);
        assert_eq!(input.cursor_position_inside_line, 1);
        assert_eq!(input.lines[1].text.get_ref(), "ba");
    }

    #[test]
    fn test_up_row_and_down_row() {
        let mut input: Input = Input {
            lines: vec![
                Line {
                    text: Cursor::new(String::from("aaaa")),
                },
                Line {
                    text: Cursor::new(String::from("bbbb")),
                },
                Line {
                    text: Cursor::new(String::from("cccc")),
                },
                Line {
                    text: Cursor::new(String::from("")),
                },
                Line {
                    text: Cursor::new(String::from("dddd")),
                },
            ],
            cursor_line_number: 0,
            cursor_position_inside_line: 2
        };

        input.up_row().expect("No exception should be thrown.");
        assert_eq!(input.cursor_line_number, 0, "At 0th line, up_row has no effect");
        assert_eq!(input.cursor_position_inside_line, 2, "When up_row has no effect, the location inside the line should stay unchanged");

        input.down_row().expect("No exception should be thrown.");
        assert_eq!(input.cursor_line_number, 1);
        assert_eq!(input.cursor_position_inside_line, 2);

        input.down_row().expect("No exception should be thrown.");
        assert_eq!(input.cursor_line_number, 2);
        assert_eq!(input.cursor_position_inside_line, 2);

        input.down_row().expect("No exception should be thrown.");
        assert_eq!(input.cursor_line_number, 3);
        assert_eq!(input.cursor_position_inside_line, 0);

        input.down_row().expect("No exception should be thrown.");
        assert_eq!(input.cursor_line_number, 4);
        assert_eq!(input.cursor_position_inside_line, 0);

        input.down_row().expect("No exception should be thrown.");
        assert_eq!(input.cursor_line_number, 4, "At last line, down_row has no effect");
        assert_eq!(input.cursor_position_inside_line, 0);

        input.up_row().expect("No exception should be thrown.");
        assert_eq!(input.cursor_line_number, 3);
        assert_eq!(input.cursor_position_inside_line, 0, "When coming from an empty line, the cursor should be at 0th position.");


        let mut input2: Input = Input::default();
        // this use case caused a bug
        input2.insert_char('a').expect("Can append char");
        input2.insert_char('\n').expect("Can append new line");
        input2.up_row().expect("Can go up");
        input2.down_row().expect("Can go down");
        assert_eq!(input2.cursor_line_number, 1);
        assert_eq!(input2.cursor_position_inside_line, 0);
        input2.insert_char('b').expect("Can append char");
        assert_eq!(input2.cursor_line_number, 1);
        assert_eq!(input2.cursor_position_inside_line, 1);
        assert_eq!(input2.lines[0].text.get_ref(), "a");
        assert_eq!(input2.lines[1].text.get_ref(), "b");
    }

    #[test]
    fn test_insert_character_into_current_line() {
        let input: Input = Input {
            lines: vec![
                Line {
                    text: Cursor::new(String::from("aa")),
                }
            ],
            cursor_line_number: 0,
            cursor_position_inside_line: 1
        };

        let new_string = input.insert_character_into_current_line('b', 1);
        assert_eq!(new_string, "aba".to_string());

        let new_string2 = input.insert_character_into_current_line('c', 0);
        assert_eq!(new_string2, "caa".to_string());

        let new_string3 = input.insert_character_into_current_line('d', 2);
        assert_eq!(new_string3, "aad".to_string());

        let input2: Input = Input {
            lines: vec![
                Line {
                    text: Cursor::new(String::from("a")),
                }
            ],
            cursor_line_number: 0,
            cursor_position_inside_line: 1
        };

        let new_string4 = input2.insert_character_into_current_line('b', 1);
        assert_eq!(new_string4, "ab".to_string());
    }
}