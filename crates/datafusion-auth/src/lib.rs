use http_body::Body;
use tower_http::{
    auth::require_authorization::{Basic, Bearer},
    validate_request::ValidateRequestHeaderLayer,
};

pub fn basic_auth<ResBody>(
    username: &str,
    password: &str,
) -> ValidateRequestHeaderLayer<Basic<ResBody>>
where
    ResBody: Body + Default,
{
    ValidateRequestHeaderLayer::basic(username, password)
}

pub fn bearer_auth<ResBody>(token: &str) -> ValidateRequestHeaderLayer<Bearer<ResBody>>
where
    ResBody: Body + Default,
{
    ValidateRequestHeaderLayer::bearer(token)
}

#[cfg(test)]
mod tests {}
