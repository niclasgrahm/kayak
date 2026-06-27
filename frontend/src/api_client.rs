use gloo_net::http::Request;
use streamer_core::StreamerDto;

pub struct ApiClient {
    pub base: String,
}

impl ApiClient {
    pub async fn list_streams(&self) -> Result<Vec<StreamerDto>, ApiError> {
        let resp = Request::get(&format!("{}/api/streams", self.base))
            .send()
            .await?;
        let dtos = resp.json::<Vec<StreamerDto>>().await?;
        Ok(dtos)
    }
}

#[derive(Clone, Debug)]
pub enum ApiError {
    Network(String),
}

impl From<gloo_net::Error> for ApiError {
    fn from(e: gloo_net::Error) -> Self {
        ApiError::Network(e.to_string())
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiError::Network(msg) => write!(f, "Network error: {msg}"),
        }
    }
}
