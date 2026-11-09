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

//! SQL framing without parsing or rewriting server SQL.
use datafusion::sql::sqlparser::{
    dialect::GenericDialect,
    tokenizer::{Token, Tokenizer},
};

/// Return complete statements and preserve the unfinished suffix verbatim.
/// Tokenization protects semicolons inside strings, identifiers and comments.
/// Retain successful tokens when the final string or comment is unfinished.
pub(super) fn split_statements(sql: &str) -> (Vec<String>, String) {
    let mut tokens = Vec::new();
    let _ = Tokenizer::new(&GenericDialect, sql).tokenize_with_location_into_buf(&mut tokens);
    // Scan the original text once to translate token locations to byte offsets.
    let mut source = sql.char_indices();
    let mut location = (1u64, 1u64);
    let mut statements = Vec::new();
    let mut start = 0;
    let mut has_sql = false;
    for token in tokens {
        match token.token {
            Token::SemiColon => {
                let target = (token.span.start.line, token.span.start.column);
                let end = source
                    .find_map(|(offset, c)| {
                        let matched = location == target;
                        if c == '\n' {
                            location = (location.0 + 1, 1);
                        } else {
                            location.1 += 1;
                        }
                        matched.then_some(offset + 1)
                    })
                    .expect("Tokenizer locations refer to the original SQL");
                if has_sql {
                    statements.push(sql[start..end].trim().to_owned());
                }
                start = end;
                has_sql = false;
            }
            Token::Whitespace(_) => {}
            _ => has_sql = true,
        }
    }
    (statements, sql[start..].to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoted_comments_and_unicode() {
        let sql = "SELECT 'é;''x', \"a;b\"; -- ;\n/* ; /* nested ; */ */ SELECT $$x;y$$;";
        let (statements, tail) = split_statements(sql);
        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0], r#"SELECT 'é;''x', "a;b";"#);
        assert!(statements[1].ends_with("SELECT $$x;y$$;"));
        assert!(tail.is_empty());
    }

    #[test]
    fn preserves_incomplete_suffix() {
        for suffix in [" SELECT 'unfinished;", " /* unfinished;", " SELECT 2"] {
            let (statements, tail) = split_statements(&format!("SELECT 1;{suffix}"));
            assert_eq!(statements, ["SELECT 1;"]);
            assert_eq!(tail, suffix);
        }
    }

    #[test]
    fn ignores_empty_statements() {
        let (statements, tail) = split_statements("; -- comment;\n; SELECT 1;\n");
        assert_eq!(statements, ["SELECT 1;"]);
        assert_eq!(tail, "\n");
    }

    #[test]
    fn remote_sql_is_not_parsed_or_rewritten() {
        let (statements, _) = split_statements("SERVER SPECIFIC SYNTAX 'x;y'; SELECT 2;");
        assert_eq!(statements, ["SERVER SPECIFIC SYNTAX 'x;y';", "SELECT 2;"]);
    }
}
