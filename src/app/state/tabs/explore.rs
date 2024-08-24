use datafusion::arrow::array::RecordBatch;
use ratatui::crossterm::event::KeyEvent;
use ratatui::style::{palette::tailwind, Style};
use tui_textarea::TextArea;

#[derive(Debug)]
pub struct ExploreTabState<'app> {
    editor: TextArea<'app>,
    editor_editable: bool,
    query_results: Option<Vec<RecordBatch>>,
}

impl<'app> ExploreTabState<'app> {
    pub fn new() -> Self {
        let empty_text = vec!["Enter a query here.".to_string()];
        // TODO: Enable vim mode from config?
        let mut textarea = TextArea::new(empty_text);
        textarea.set_line_number_style(Style::default().bg(tailwind::GRAY.c400));
        Self {
            editor: textarea,
            editor_editable: false,
            query_results: None,
            // query_handle: None,
        }
    }

    pub fn editor(&self) -> TextArea {
        // TODO: Figure out how to do this without clone. Probably need logic in handler to make
        // updates to the Widget and then pass a ref
        self.editor.clone()
    }

    pub fn clear_placeholder(&mut self) {
        let default = "Enter a query here.";
        let lines = self.editor.lines();
        let content = lines.join("");
        if content == default {
            self.editor
                .move_cursor(tui_textarea::CursorMove::Jump(0, 0));
            self.editor.delete_str(default.len());
        }
    }

    pub fn update_editor_content(&mut self, key: KeyEvent) {
        self.editor.input(key);
    }

    pub fn edit(&mut self) {
        self.editor_editable = true;
    }

    pub fn exit_edit(&mut self) {
        self.editor_editable = false;
    }

    pub fn is_editable(&self) -> bool {
        self.editor_editable
    }

    pub fn set_query_results(&mut self, query_results: Vec<RecordBatch>) {
        self.query_results = Some(query_results);
    }

    pub fn query_results(&self) -> &Option<Vec<RecordBatch>> {
        &self.query_results
    }
}
