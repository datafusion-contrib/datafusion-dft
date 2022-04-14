use crate::app::datafusion::context::{QueryResults, QueryResultsMeta};
use crate::app::ui::Scroll;

/// Compares the `QueryResults` of an executed query with
/// the expected `QueryResults`.  Given that `query_duration`
/// is a field in `QueryResultsMeta` that can vary between
/// executions this field is excluded when testing equality.  
/// This is a macro so errors appear on the correct line.
#[macro_export]
macro_rules! assert_results_eq {
    ($ACTUAL: expr, $EXPECTED: expr) => {
        let actual_static = StaticQueryFields::from($ACTUAL);
        let expected_static = StaticQueryFields::from($EXPECTED);

        assert_eq!(actual_static, expected_static);
    };
}

#[derive(PartialEq)]
struct StaticQueryFields {
    query: String,
    succeeded: bool,
    error: Option<String>,
    rows: usize,
}

impl From<QueryResults> for StaticQueryFields {
    fn from(query_results: QueryResults) -> Self {
        StaticQueryFields {
            query: query_results.meta.query,
            succeeded: query_results.meta.succeeded,
            error: query_results.meta.error,
            rows: query_results.meta.rows,
        }
    }
}

fn extract_static_results(query_results: QueryResults) -> StaticQueryFields {
    StaticQueryFields {
        query: query_results.meta.query,
        succeeded: query_results.meta.succeeded,
        error: query_results.meta.error,
        rows: query_results.meta.rows,
    }
}
