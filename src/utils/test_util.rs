use crate::app::datafusion::context::QueryResults;

/// Compares the `QueryResults` of an executed query with
/// the expected `QueryResults`.  Given that `query_duration`
/// is a field in `QueryResultsMeta` that can vary between
/// executions this field is excluded when testing equality.  
/// This is a macro so errors appear on the correct line.
pub fn assert_results_eq(actual: Option<QueryResults>, expected: Option<QueryResults>) {
    if actual.is_some() && expected.is_some() {
        let a = StaticQueryFields::from(actual.unwrap());
        let e = StaticQueryFields::from(expected.unwrap());
        assert_eq!(a, e)
    } else if actual.is_none() && expected.is_none() {
        assert!(true)
    } else {
        assert!(false)
    }
}

#[derive(Debug, PartialEq)]
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
