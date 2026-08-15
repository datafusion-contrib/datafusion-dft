use datafusion::arrow::array::{Array, RecordBatch, UInt32Array};
use datafusion::arrow::compute::{concat_batches, take_record_batch};
use datafusion::arrow::error::ArrowError;
use std::sync::Arc;

pub const PAGE_SIZE: usize = 100;

/// Calculate the row range needed for a given page
pub fn page_row_range(page: usize, page_size: usize) -> (usize, usize) {
    let start = page * page_size;
    let end = start + page_size;
    (start, end)
}

/// Check if we have enough rows loaded to display the requested page
pub fn has_sufficient_rows(loaded_rows: usize, page: usize, page_size: usize) -> bool {
    let (_start, end) = page_row_range(page, page_size);
    loaded_rows >= end
}

/// Extract a page of rows from loaded batches
/// This handles pagination across batch boundaries by concatenating only what's needed
pub fn extract_page(
    batches: &[RecordBatch],
    page: usize,
    page_size: usize,
) -> Result<RecordBatch, ArrowError> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(
            datafusion::arrow::datatypes::Schema::empty(),
        )));
    }

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let (start, end) = page_row_range(page, page_size);

    // Clamp end to available rows
    let end = end.min(total_rows);

    if start >= total_rows {
        // Page is beyond available data
        return Ok(RecordBatch::new_empty(batches[0].schema()));
    }

    // Create indices for the rows we want
    let indices = UInt32Array::from_iter_values((start as u32)..(end as u32));

    // Extract rows from batches
    extract_rows_from_batches(batches, &indices)
}

/// Extract specific rows (by global indices) from batches
/// Handles batch boundaries by concatenating only necessary batches
fn extract_rows_from_batches(
    batches: &[RecordBatch],
    indices: &dyn Array,
) -> Result<RecordBatch, ArrowError> {
    match batches.len() {
        0 => Ok(RecordBatch::new_empty(Arc::new(
            datafusion::arrow::datatypes::Schema::empty(),
        ))),
        1 => take_record_batch(&batches[0], indices),
        _ => {
            // Multiple batches: concat then extract rows
            // Only concat the batches we've loaded (lazy loading ensures minimal concat)
            let schema = batches[0].schema();
            let concatenated = concat_batches(&schema, batches)?;
            take_record_batch(&concatenated, indices)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_row_range() {
        assert_eq!(page_row_range(0, 100), (0, 100));
        assert_eq!(page_row_range(1, 100), (100, 200));
        assert_eq!(page_row_range(2, 50), (100, 150));
    }

    #[test]
    fn test_has_sufficient_rows() {
        assert!(has_sufficient_rows(100, 0, 100)); // Exactly enough
        assert!(has_sufficient_rows(150, 0, 100)); // More than enough
        assert!(!has_sufficient_rows(50, 0, 100)); // Not enough
        assert!(!has_sufficient_rows(150, 1, 100)); // Need 200, only have 150
    }
}
