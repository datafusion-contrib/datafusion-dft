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

use datafusion::arrow::{
    array::{
        Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray, UInt16Array,
        UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::DataType,
};
use ratatui::{
    layout::Constraint,
    style::{palette::tailwind, Stylize},
    widgets::{Block, Borders, Cell, Row, Table},
};

macro_rules! convert_array_values_to_cells {
    ($rows:expr, $arr:expr, $typ:ty) => {
        if let Some(a) = $arr.as_any().downcast_ref::<$typ>() {
            for i in 0..$rows.len() {
                $rows[i].push(a.value(i).to_string().into());
            }
        }
    };
}

pub fn record_batch_to_table_header_cells(record_batch: &RecordBatch) -> Vec<Cell> {
    let mut cells = vec![Cell::new("#").bg(tailwind::LIME.c300).fg(tailwind::BLACK)];
    record_batch.schema_ref().fields().iter().for_each(|f| {
        let cell = Cell::new(f.name().as_str())
            .bg(tailwind::LIME.c300)
            .fg(tailwind::BLACK);
        cells.push(cell);
    });
    cells
}

pub fn create_row_number_cells(record_batch: &RecordBatch) -> Vec<Cell> {
    let cells: Vec<Cell> = (0..record_batch.num_rows())
        .map(|i| Cell::new(i.to_string()))
        .collect();
    cells
}

pub fn record_batch_to_table_row_cells(record_batch: &RecordBatch) -> Vec<Vec<Cell>> {
    let row_count = record_batch.num_rows();
    let column_count = record_batch.num_columns();

    let mut rows: Vec<Vec<Cell>> = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        rows.push(Vec::with_capacity(column_count))
    }

    let row_number_cells = create_row_number_cells(record_batch);

    for (i, cell) in row_number_cells.into_iter().enumerate() {
        rows[i].push(cell);
    }

    for arr in record_batch.columns() {
        match arr.data_type() {
            DataType::Utf8 => convert_array_values_to_cells!(rows, arr, StringArray),
            DataType::Int8 => convert_array_values_to_cells!(rows, arr, Int8Array),
            DataType::Int16 => convert_array_values_to_cells!(rows, arr, Int16Array),
            DataType::Int32 => convert_array_values_to_cells!(rows, arr, Int32Array),
            DataType::Int64 => convert_array_values_to_cells!(rows, arr, Int64Array),
            DataType::UInt8 => convert_array_values_to_cells!(rows, arr, UInt8Array),
            DataType::UInt16 => convert_array_values_to_cells!(rows, arr, UInt16Array),
            DataType::UInt32 => convert_array_values_to_cells!(rows, arr, UInt32Array),
            DataType::UInt64 => convert_array_values_to_cells!(rows, arr, UInt64Array),
            _ => {}
        }
    }
    rows
}

pub fn empty_results_table<'frame>() -> Table<'frame> {
    let header_row = Row::new(vec!["Result"]);
    let value_row = [Row::new(vec!["No results"])];
    let width = vec![Constraint::Percentage(100)];
    Table::new(value_row, width).header(header_row)
}

pub fn record_batches_to_table<'frame, 'results>(
    record_batches: &'results [RecordBatch],
) -> Table<'frame>
where
    // The results come from explore_tab state which persists until the next query is run which is
    // longer than a frame lifetime.
    'results: 'frame,
{
    if record_batches.is_empty() {
        empty_results_table()
    } else {
        let first_batch = &record_batches[0];
        let header_cells = record_batch_to_table_header_cells(first_batch);
        let header_row = Row::from_iter(header_cells).bold();
        let rows: Vec<Row> = record_batches
            .iter()
            .flat_map(|b| {
                let batch_row_cells = record_batch_to_table_row_cells(b);
                let rows: Vec<Row> = batch_row_cells.into_iter().map(Row::from_iter).collect();
                rows
            })
            .collect();
        let column_count = first_batch.num_columns() + 1;
        let widths = (0..column_count).map(|_| Constraint::Fill(1));
        let block = Block::default().borders(Borders::all());
        Table::new(rows, widths).header(header_row).block(block)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        ArrayRef, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray,
        UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };
    use ratatui::{
        style::{palette::tailwind, Stylize},
        widgets::Cell,
    };

    use super::{record_batch_to_table_header_cells, record_batch_to_table_row_cells};

    #[test]
    fn record_batch_to_header_test() {
        let a: ArrayRef = Arc::new(Int8Array::from(vec![1, 2, 3]));
        let b: ArrayRef = Arc::new(Int8Array::from(vec![1, 2, 3]));

        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();
        let header_cells = record_batch_to_table_header_cells(&batch);
        assert_eq!(
            header_cells,
            vec![
                Cell::new("#").bg(tailwind::LIME.c300).fg(tailwind::BLACK),
                Cell::new("a").bg(tailwind::LIME.c300).fg(tailwind::BLACK),
                Cell::new("b").bg(tailwind::LIME.c300).fg(tailwind::BLACK)
            ]
        );
    }

    #[test]
    fn single_column_record_batch_to_rows_test() {
        let a: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));

        let batch = RecordBatch::try_from_iter(vec![("a", a)]).unwrap();
        let table_cells = record_batch_to_table_row_cells(&batch);
        let expected = vec![
            vec![Cell::new("0"), Cell::new("a")],
            vec![Cell::new("1"), Cell::new("b")],
            vec![Cell::new("2"), Cell::new("c")],
        ];
        assert_eq!(table_cells, expected);

        let a: ArrayRef = Arc::new(Int8Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_from_iter(vec![("a", a)]).unwrap();
        let a_table_cells = record_batch_to_table_row_cells(&batch);
        let expected = vec![
            vec![Cell::new("0"), Cell::new("1")],
            vec![Cell::new("1"), Cell::new("2")],
            vec![Cell::new("2"), Cell::new("3")],
        ];
        assert_eq!(a_table_cells, expected);

        let a: ArrayRef = Arc::new(Int16Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_from_iter(vec![("a", a)]).unwrap();
        let a_table_cells = record_batch_to_table_row_cells(&batch);
        let expected = vec![
            vec![Cell::new("0"), Cell::new("1")],
            vec![Cell::new("1"), Cell::new("2")],
            vec![Cell::new("2"), Cell::new("3")],
        ];
        assert_eq!(a_table_cells, expected);

        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_from_iter(vec![("a", a)]).unwrap();
        let a_table_cells = record_batch_to_table_row_cells(&batch);
        let expected = vec![
            vec![Cell::new("0"), Cell::new("1")],
            vec![Cell::new("1"), Cell::new("2")],
            vec![Cell::new("2"), Cell::new("3")],
        ];
        assert_eq!(a_table_cells, expected);

        let a: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_from_iter(vec![("a", a)]).unwrap();
        let a_table_cells = record_batch_to_table_row_cells(&batch);
        let expected = vec![
            vec![Cell::new("0"), Cell::new("1")],
            vec![Cell::new("1"), Cell::new("2")],
            vec![Cell::new("2"), Cell::new("3")],
        ];
        assert_eq!(a_table_cells, expected);

        let a: ArrayRef = Arc::new(UInt8Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_from_iter(vec![("a", a)]).unwrap();
        let a_table_cells = record_batch_to_table_row_cells(&batch);
        let expected = vec![
            vec![Cell::new("0"), Cell::new("1")],
            vec![Cell::new("1"), Cell::new("2")],
            vec![Cell::new("2"), Cell::new("3")],
        ];
        assert_eq!(a_table_cells, expected);

        let a: ArrayRef = Arc::new(UInt16Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_from_iter(vec![("a", a)]).unwrap();
        let a_table_cells = record_batch_to_table_row_cells(&batch);
        let expected = vec![
            vec![Cell::new("0"), Cell::new("1")],
            vec![Cell::new("1"), Cell::new("2")],
            vec![Cell::new("2"), Cell::new("3")],
        ];
        assert_eq!(a_table_cells, expected);

        let a: ArrayRef = Arc::new(UInt32Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_from_iter(vec![("a", a)]).unwrap();
        let a_table_cells = record_batch_to_table_row_cells(&batch);
        let expected = vec![
            vec![Cell::new("0"), Cell::new("1")],
            vec![Cell::new("1"), Cell::new("2")],
            vec![Cell::new("2"), Cell::new("3")],
        ];
        assert_eq!(a_table_cells, expected);

        let a: ArrayRef = Arc::new(UInt64Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_from_iter(vec![("a", a)]).unwrap();
        let a_table_cells = record_batch_to_table_row_cells(&batch);
        let expected = vec![
            vec![Cell::new("0"), Cell::new("1")],
            vec![Cell::new("1"), Cell::new("2")],
            vec![Cell::new("2"), Cell::new("3")],
        ];
        assert_eq!(a_table_cells, expected);
    }

    #[test]
    fn multi_column_record_batch_to_rows_test() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();
        let a_table_cells = record_batch_to_table_row_cells(&batch);
        let expected = vec![
            vec![Cell::new("0"), Cell::new("1"), Cell::new("a")],
            vec![Cell::new("1"), Cell::new("2"), Cell::new("b")],
            vec![Cell::new("2"), Cell::new("3"), Cell::new("c")],
        ];
        assert_eq!(a_table_cells, expected);
    }
}