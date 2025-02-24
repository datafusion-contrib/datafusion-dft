use datafusion::common::Result;
use http_body::Body;
pub use tower_http::{
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

pub struct BasicAuth {
    pub username: String,
    pub password: String,
}

pub struct AuthLayer<ResBody> {
    basic_auth: Option<ValidateRequestHeaderLayer<Basic<ResBody>>>,
    bearer_token: Option<ValidateRequestHeaderLayer<Bearer<ResBody>>>,
}

impl<ResBody: Body + Default> AuthLayer<ResBody> {
    pub fn try_new(
        basic_auth_credentials: Option<BasicAuth>,
        bearer_token: Option<String>,
    ) -> Result<Self> {
        if basic_auth_credentials.is_none() && bearer_token.is_none() {}
        let basic_auth = if let Some(basic_auth_credentials) = basic_auth_credentials {
            Some(basic_auth(
                &basic_auth_credentials.username,
                &basic_auth_credentials.password,
            ))
        } else {
            None
        };
        let bearer_token = if let Some(token) = bearer_token {
            Some(bearer_auth(&token))
        } else {
            None
        };

        let layer = Self {
            basic_auth,
            bearer_token,
        };
        Ok(layer)
    }
}

#[cfg(test)]
mod tests {}
