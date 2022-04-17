use crate::app::datafusion::context::QueryResults;

/// Compares the `QueryResults` of an executed query with
/// the expected `QueryResults`.  Given that `query_duration`
/// is a field in `QueryResultsMeta` that can vary between
/// executions this field is excluded when testing equality.  
/// This is a macro so errors appear on the correct line.
pub fn assert_results_eq(actual: Option<QueryResults>, expected: Option<QueryResults>) {
    if let (Some(act), Some(exp)) = (&actual, &expected) {
        let a = StaticQueryFields::from(act);
        let e = StaticQueryFields::from(exp);
        assert_eq!(a, e)
    } else if actual.is_none() && expected.is_none() {
        // Nones match means test passed
    } else {
        panic!("QueryResults do not match")
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

impl From<&QueryResults> for StaticQueryFields {
    fn from(query_results: &QueryResults) -> Self {
        StaticQueryFields {
            query: query_results.meta.query.clone(),
            succeeded: query_results.meta.succeeded,
            error: query_results.meta.error.clone(),
            rows: query_results.meta.rows,
        }
    }
}
