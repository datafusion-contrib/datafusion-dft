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

//! Persistent SQL prompt in the normal terminal screen.
mod history;
mod input;
#[cfg(all(test, feature = "flightsql"))]
mod tests_flightsql;
mod ui;

use std::io::{stdin, stdout, IsTerminal};
use std::time::Instant;

use color_eyre::{eyre::eyre, Result};
use crossterm::event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers};
use datafusion::arrow::{array::RecordBatch, util::pretty::pretty_format_batches};
use datafusion_app::{ExecOptions, ExecResult};
use futures::StreamExt;
use ratatui_textarea::{CursorMove, TextArea};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    args::DftArgs,
    cli::create_execution,
    config::{get_data_dir, AppConfig},
    execution::AppExecution,
};
use history::History;
use input::split_statements;
use ui::InlineTerminal;

const HELP: &str = "End SQL with ;. Enter continues incomplete SQL. Alt+Enter inserts a newline.\nUp/Down: history at editor boundaries. Ctrl+R: search history.\nCtrl+C: clear input or cancel execution. Ctrl+D: exit with empty input.\n\\q: quit. \\?: help. \\update SQL;: FlightSQL statement-update request.\nRemote cancellation stops result consumption; server work can continue.";

/// A bounded channel keeps query execution from buffering unlimited results
/// while the terminal renders. A task owns all statement futures and streams.
enum Output {
    Batch(RecordBatch),
    Text(String),
    Finished(Result<()>),
    #[cfg(feature = "flightsql")]
    FlightQuery(
        datafusion_app::flightsql::FlightSQLContext,
        arrow_flight::FlightInfo,
    ),
}

struct Running {
    task: JoinHandle<()>,
    output: mpsc::Receiver<Output>,
    #[cfg(feature = "flightsql")]
    cancellation: Option<(
        datafusion_app::flightsql::FlightSQLContext,
        arrow_flight::FlightInfo,
    )>,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Running {
    fn start(
        execution: AppExecution,
        args: DftArgs,
        config: AppConfig,
        statements: Vec<String>,
        update: bool,
    ) -> Self {
        let (sender, output) = mpsc::channel(2);
        let task = tokio::spawn(async move {
            let result =
                run_statements(execution, &args, &config, statements, update, &sender).await;
            let _ = sender.send(Output::Finished(result)).await;
        });
        Self {
            task,
            output,
            #[cfg(feature = "flightsql")]
            cancellation: None,
        }
    }
}

async fn run_statements(
    #[allow(unused_mut)] mut execution: AppExecution,
    args: &DftArgs,
    config: &AppConfig,
    statements: Vec<String>,
    update: bool,
    sender: &mpsc::Sender<Output>,
) -> Result<()> {
    #[cfg(not(feature = "flightsql"))]
    let _ = config;
    for sql in statements {
        let start = Instant::now();
        #[cfg(feature = "flightsql")]
        if args.flightsql {
            use crate::config::ConnectionMode;
            if config.flightsql_client.connection_mode == ConnectionMode::PerStatement {
                execution.with_flightsql_ctx(execution.flightsql_ctx().disconnected());
            }
            let ctx = execution.flightsql_ctx();
            if ctx.client().lock().await.is_none() {
                ctx.create_client(None, None).await?;
            }
        }
        if update {
            #[cfg(feature = "flightsql")]
            if args.flightsql {
                let mut guard = execution.flightsql_client().lock().await;
                let client = guard
                    .as_mut()
                    .ok_or_else(|| eyre!("Missing FlightSQL client"))?;
                let count = client.execute_update(sql, None).await?;
                sender
                    .send(Output::Text(format!(
                        "{count} row(s) affected. Elapsed: {:.3}s",
                        start.elapsed().as_secs_f64()
                    )))
                    .await?;
                continue;
            }
            return Err(eyre!("\\update requires FlightSQL mode (-q)"));
        }
        #[cfg(feature = "flightsql")]
        let mut stream = if args.flightsql {
            let ctx = execution.flightsql_ctx();
            let (info, stream) = ctx.execute_query(&sql).await?;
            sender.send(Output::FlightQuery(ctx.clone(), info)).await?;
            stream
        } else {
            local_stream(&execution, &sql).await?
        };
        #[cfg(not(feature = "flightsql"))]
        let mut stream = local_stream(&execution, &sql).await?;
        let mut rows = 0usize;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            rows += batch.num_rows();
            // Render small slices rather than building a string for an entire result.
            if !args.time {
                for offset in (0..batch.num_rows()).step_by(128) {
                    sender
                        .send(Output::Batch(
                            batch.slice(offset, (batch.num_rows() - offset).min(128)),
                        ))
                        .await?;
                }
            }
        }
        sender
            .send(Output::Text(format!(
                "{rows} row(s). Elapsed: {:.3}s",
                start.elapsed().as_secs_f64()
            )))
            .await?;
    }
    Ok(())
}

async fn local_stream(
    execution: &AppExecution,
    sql: &str,
) -> Result<datafusion::execution::SendableRecordBatchStream> {
    match execution
        .execute_sql_with_opts(sql, ExecOptions::new(None, false))
        .await?
    {
        ExecResult::RecordBatchStream(stream) => Ok(stream),
        _ => Err(eyre!("Unsupported execution result")),
    }
}

/// Validate both explicit REPL mode and the no-TUI default before initializing state.
fn validate_args(args: &DftArgs) -> Result<()> {
    if args.command.is_some()
        || !args.commands.is_empty()
        || !args.files.is_empty()
        || args.bench
        || args.analyze
        || args.concat
        || args.output.is_some()
        || args.save.is_some()
        || args.run_before.is_some()
        || args.benchmark_iterations.is_some()
        || args.concurrent
        || args.append
        || args.print_headers
        || args.headers_only
        || args.print_format_options.is_some()
    {
        return Err(eyre!(
            "These batch options cannot be used with the REPL; use -c or -f"
        ));
    }
    #[cfg(not(feature = "flightsql"))]
    if args.flightsql {
        return Err(eyre!("FlightSQL is not enabled in this build"));
    }
    if !stdin().is_terminal() || !stdout().is_terminal() {
        return Err(eyre!(
            "The REPL requires terminal input and output; use -c or -f for batch execution"
        ));
    }
    Ok(())
}

fn editor(text: &str) -> TextArea<'static> {
    let mut editor = TextArea::new(text.split('\n').map(str::to_owned).collect());
    editor.move_cursor(CursorMove::Bottom);
    editor.move_cursor(CursorMove::End);
    editor
}

#[derive(Default)]
struct Search {
    query: String,
    skip: usize,
}

pub async fn try_run(args: DftArgs, config: AppConfig) -> Result<()> {
    validate_args(&args)?;
    let execution = create_execution(&args, &config, false).await?;
    if args.run_ddl {
        execution.execution_ctx().execute_ddl().await;
    }
    let history_path = get_data_dir().join("repl-history.toml");
    let (mut history, history_warning) = match History::load(&history_path) {
        Ok(history) => (history, None),
        Err(error) => (
            History::default(),
            Some(format!("Cannot load history: {error}")),
        ),
    };
    let mut terminal = InlineTerminal::new()?;
    terminal.write("dft SQL prompt. Use \\? for help. End statements with ;.")?;
    if let Some(warning) = history_warning {
        terminal.write(&warning)?;
    }
    let mut editor = editor("");
    let mut search: Option<Search> = None;
    let mut running: Option<Running> = None;
    let mut events = EventStream::new();
    loop {
        let status = if let Some(search) = &search {
            format!(
                "History search: {} | {}",
                search.query,
                history
                    .search(&search.query, search.skip)
                    .unwrap_or_else(|| "No match".into())
                    .replace('\n', " ")
            )
        } else if running.is_some() {
            "Executing. Ctrl+C: cancel".into()
        } else if args.flightsql {
            "flightsql>".into()
        } else {
            "dft>".into()
        };
        terminal.draw(&editor, &status)?;
        tokio::select! {
            output = async { running.as_mut().unwrap().output.recv().await }, if running.is_some() => {
                match output {
                    Some(Output::Batch(batch)) => {
                        if args.json {
                            let mut writer = datafusion::arrow::json::LineDelimitedWriter::new(Vec::new());
                            writer.write(&batch)?;
                            writer.finish()?;
                            terminal.write(&String::from_utf8(writer.into_inner())?)?;
                        } else {
                            terminal.write(&pretty_format_batches(&[batch])?.to_string())?;
                        }
                    }
                    Some(Output::Text(text)) => {
                        #[cfg(feature = "flightsql")]
                        { running.as_mut().unwrap().cancellation = None; }
                        terminal.write(&text)?;
                    }
                    #[cfg(feature = "flightsql")]
                    Some(Output::FlightQuery(ctx, info)) => {
                        running.as_mut().unwrap().cancellation = Some((ctx, info));
                    }
                    Some(Output::Finished(result)) => {
                        running = None;
                        if let Err(error) = result {
                            terminal.write(&format!("Error: {error}"))?;
                        }
                    }
                    None => {
                        running = None;
                        terminal.write("Error: execution task stopped unexpectedly")?;
                    }
                }
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                cancel(&mut running, &mut editor, &mut search, &mut terminal, args.flightsql).await?;
            }
            event = events.next() => {
                let Some(event) = event else { break };
                match event? {
                    Event::Resize(_, _) => {}
                    Event::Paste(text) if running.is_none() && search.is_none() => {
                        editor.insert_str(text.replace("\r\n", "\n").replace('\r', "\n"));
                    }
                    Event::Key(key) if key.kind != KeyEventKind::Release => {
                        let control = key.modifiers.contains(KeyModifiers::CONTROL);
                        if control && key.code == KeyCode::Char('c') {
                            cancel(&mut running, &mut editor, &mut search, &mut terminal, args.flightsql).await?;
                            continue;
                        }
                        if running.is_some() { continue; }
                        if let Some(active) = &mut search {
                            match key.code {
                                KeyCode::Esc => search = None,
                                KeyCode::Enter => {
                                    if let Some(sql) = history.search(&active.query, active.skip) {
                                        editor = self::editor(&sql);
                                    }
                                    search = None;
                                }
                                KeyCode::Char('r') if control => active.skip += 1,
                                KeyCode::Backspace => { active.query.pop(); active.skip = 0; }
                                KeyCode::Char(c) if !control => { active.query.push(c); active.skip = 0; }
                                _ => {}
                            }
                            continue;
                        }
                        if control && key.code == KeyCode::Char('d') && editor.lines().join("\n").is_empty() {
                            break;
                        }
                        if control && key.code == KeyCode::Char('r') {
                            search = Some(Search::default());
                            continue;
                        }
                        match key.code {
                            KeyCode::Up if editor.cursor().0 == 0 => {
                                if let Some(sql) = history.previous(editor.lines().join("\n")) {
                                    editor = self::editor(&sql);
                                }
                            }
                            KeyCode::Down if editor.cursor().0 + 1 == editor.lines().len() => {
                                if let Some(sql) = history.next() { editor = self::editor(&sql); }
                            }
                            KeyCode::Enter if key.modifiers.is_empty() => {
                                let text = editor.lines().join("\n");
                                let trimmed = text.trim();
                                if trimmed.is_empty() { continue; }
                                if trimmed == "\\q" { break; }
                                if trimmed == "\\?" {
                                    terminal.write(HELP)?;
                                    editor = self::editor("");
                                    continue;
                                }
                                let (sql, update) = match trimmed.strip_prefix("\\update ") {
                                    Some(sql) => (sql, true),
                                    None => (text.as_str(), false),
                                };
                                if trimmed.starts_with('\\') && !update {
                                    terminal.write("Unknown command. Use \\? for help.")?;
                                    editor = self::editor("");
                                    continue;
                                }
                                let (statements, tail) = split_statements(sql);
                                if statements.is_empty() {
                                    editor.insert_newline();
                                    continue;
                                }
                                terminal.write(&format!("{} {text}", if args.flightsql { "flightsql>" } else { "dft>" }))?;
                                history.push(text.clone());
                                if let Err(error) = history.save(&history_path) {
                                    terminal.write(&format!("Cannot save history: {error}"))?;
                                }
                                let tail = tail.trim_start_matches('\n');
                                editor = self::editor(&if update && !tail.trim().is_empty() {
                                    format!("\\update {tail}")
                                } else {
                                    tail.to_owned()
                                });
                                running = Some(Running::start(execution.clone(), args.clone(), config.clone(), statements, update));
                            }
                            KeyCode::Enter => { editor.insert_newline(); }
                            _ => { editor.input(key); }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

async fn cancel(
    running: &mut Option<Running>,
    editor: &mut TextArea<'static>,
    search: &mut Option<Search>,
    terminal: &mut InlineTerminal,
    remote: bool,
) -> Result<()> {
    #[allow(unused_mut)]
    let mut active = running.take();
    let executing = active.is_some();
    #[cfg(feature = "flightsql")]
    let cancellation = active.as_mut().and_then(|query| query.cancellation.take());
    // Abort the task before acquiring its client lock for cancellation.
    drop(active);
    #[cfg(feature = "flightsql")]
    if let Some((ctx, info)) = cancellation {
        let _ =
            tokio::time::timeout(std::time::Duration::from_secs(1), ctx.cancel_query(info)).await;
    }
    *editor = self::editor("");
    *search = None;
    terminal.write(if executing && remote {
        "Cancelled. Remote server work can continue."
    } else if executing {
        "Cancelled."
    } else {
        "^C"
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn rejects_batch_options_with_explicit_repl() {
        for options in [
            vec!["-c", "SELECT 1"],
            vec!["--bench"],
            vec!["--output", "x.csv"],
            vec!["--concat"],
        ] {
            let mut args = vec!["dft", "--repl"];
            args.extend(options);
            assert!(DftArgs::try_parse_from(args).is_err());
        }
    }

    #[tokio::test]
    async fn retains_local_session_and_stops_after_error() {
        let execution = create_execution(&DftArgs::default(), &AppConfig::default(), false)
            .await
            .unwrap();
        let (sender, mut receiver) = mpsc::channel(32);
        run_statements(
            execution.clone(),
            &DftArgs::default(),
            &AppConfig::default(),
            vec![
                "CREATE VIEW repl_test AS SELECT 42 AS answer".into(),
                "SELECT * FROM repl_test".into(),
            ],
            false,
            &sender,
        )
        .await
        .unwrap();
        let mut rows = 0;
        while let Ok(output) = receiver.try_recv() {
            if let Output::Batch(batch) = output {
                rows += batch.num_rows();
            }
        }
        assert_eq!(rows, 1);
        assert!(run_statements(
            execution.clone(),
            &DftArgs::default(),
            &AppConfig::default(),
            vec![
                "SELECT * FROM missing_table".into(),
                "DROP VIEW repl_test".into()
            ],
            false,
            &sender
        )
        .await
        .is_err());
        assert!(execution.session_ctx().table_exist("repl_test").unwrap());
    }
}
