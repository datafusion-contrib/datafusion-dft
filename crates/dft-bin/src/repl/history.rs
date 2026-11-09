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

use std::path::Path;

use color_eyre::Result;
use serde::{Deserialize, Serialize};

const MAX_STATEMENTS: usize = 1000;

#[derive(Default, Deserialize, Serialize)]
pub(super) struct History {
    #[serde(default)]
    statements: Vec<String>,
    #[serde(skip)]
    position: Option<usize>,
    #[serde(skip)]
    draft: String,
}

impl History {
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let mut history: Self = toml::from_str(&std::fs::read_to_string(path)?)?;
        history.trim();
        Ok(history)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Write a temporary file before replacement to preserve the previous history
        // if serialization or writing fails. Each process uses a separate temporary file.
        let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        use std::io::Write;
        let mut file = options.open(&temporary)?;
        file.write_all(toml::to_string(self)?.as_bytes())?;
        std::fs::rename(temporary, path)?;
        Ok(())
    }

    pub fn push(&mut self, sql: String) {
        self.position = None;
        self.draft.clear();
        if self.statements.last() != Some(&sql) {
            self.statements.push(sql);
            self.trim();
        }
    }

    fn trim(&mut self) {
        let excess = self.statements.len().saturating_sub(MAX_STATEMENTS);
        self.statements.drain(..excess);
    }

    pub fn previous(&mut self, draft: String) -> Option<String> {
        let position = match self.position {
            Some(position) => position.saturating_sub(1),
            None => {
                self.draft = draft;
                self.statements.len().checked_sub(1)?
            }
        };
        self.position = Some(position);
        Some(self.statements[position].clone())
    }

    pub fn next(&mut self) -> Option<String> {
        let position = self.position? + 1;
        if position < self.statements.len() {
            self.position = Some(position);
            Some(self.statements[position].clone())
        } else {
            self.position = None;
            Some(self.draft.clone())
        }
    }

    pub fn search(&self, query: &str, skip: usize) -> Option<String> {
        self.statements
            .iter()
            .rev()
            .filter(|sql| sql.contains(query))
            .nth(skip)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persists_multiline_sql_and_recovers_draft() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("repl-history.toml");
        let mut history = History::default();
        history.push("SELECT\n'quotes \" and ;';".into());
        history.push("SELECT 2;".into());
        history.save(&path).unwrap();
        let mut history = History::load(&path).unwrap();
        assert_eq!(history.previous("draft".into()).unwrap(), "SELECT 2;");
        assert!(history.previous(String::new()).unwrap().contains("quotes"));
        assert_eq!(history.next().unwrap(), "SELECT 2;");
        assert_eq!(history.next().unwrap(), "draft");
        assert!(history.search("quotes", 0).is_some());
        assert!(history.search("absent", 0).is_none());
    }
}
