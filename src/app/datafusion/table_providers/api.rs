use futures::{future, stream, Future, Stream};
use http::Uri;
use hyper::client::Request;
use hyper::{Client, Method};
use serde_json;

pub enum ApiFormat {
    Json,
}

pub struct ApiConfig {
    uri: Uri,
    format: ApiFormat,
}

pub struct ApiTable {
    config: ApiConfig,
}

impl ApiTable {
    fn get() {
        let client = Client::new();
    }
}

#[cfg(test)]
mod test {
    #[tokio::test]
    fn test_get_api_results() {}
}
