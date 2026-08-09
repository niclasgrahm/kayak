use gloo_net::http::Request;
use kayak_core::state::{BucketContents, BucketSummary};
use kayak_core::{
    ConfigFormat, Connections, LayoutFile, PipelineDto, SaveConfigRequest, SaveConfigResponse,
    SettingsDto,
};
use serde_json::Value;

pub struct ApiClient {
    pub base: String,
}

impl ApiClient {
    pub async fn list_pipelines(&self) -> Result<Vec<PipelineDto>, ApiError> {
        let resp = Request::get(&format!("{}/api/pipelines", self.base))
            .send()
            .await?;
        let dtos = resp.json::<Vec<PipelineDto>>().await?;
        Ok(dtos)
    }

    pub async fn settings(&self) -> Result<SettingsDto, ApiError> {
        let resp = Request::get(&format!("{}/api/settings", self.base))
            .send()
            .await?;
        Ok(resp.json::<SettingsDto>().await?)
    }

    /// Where the cards have been arranged. An empty answer is the normal one:
    /// nothing has been dragged, so everything is laid out automatically.
    pub async fn layout(&self) -> Result<LayoutFile, ApiError> {
        let resp = Request::get(&format!("{}/api/layout", self.base))
            .send()
            .await?;
        Ok(resp.json::<LayoutFile>().await?)
    }

    /// Replace the arrangement. The whole map rather than one pipeline, because
    /// that is what makes "put it back to automatic" an ordinary save of a
    /// smaller map instead of its own endpoint.
    pub async fn save_layout(&self, layout: &LayoutFile) -> Result<(), ApiError> {
        let resp = Request::put(&format!("{}/api/layout", self.base))
            .json(layout)?
            .send()
            .await?;
        if resp.ok() {
            Ok(())
        } else {
            Err(rejection(resp).await)
        }
    }

    /// Create a pipeline. `config` is the body the form built — a `Value`
    /// rather than a `Config` because the form assembles JSON field by field,
    /// and round-tripping it through the typed struct here would only move the
    /// same deserialization error to a place with less to say about it.
    pub async fn create_pipeline(&self, config: &Value) -> Result<PipelineDto, ApiError> {
        let resp = Request::post(&format!("{}/api/pipelines", self.base))
            .json(config)?
            .send()
            .await?;
        if !resp.ok() {
            return Err(rejection(resp).await);
        }
        Ok(resp.json::<PipelineDto>().await?)
    }

    pub async fn delete_pipeline(&self, id: &str) -> Result<(), ApiError> {
        let resp = Request::delete(&format!("{}/api/pipelines/{id}", self.base))
            .send()
            .await?;
        if resp.ok() {
            Ok(())
        } else {
            Err(rejection(resp).await)
        }
    }

    /// The systems pipelines can connect to, by name — the same map the
    /// connections file holds.
    pub async fn list_connections(&self) -> Result<Connections, ApiError> {
        let resp = Request::get(&format!("{}/api/connections", self.base))
            .send()
            .await?;
        Ok(resp.json::<Connections>().await?)
    }

    /// Add a connection. `connection` is `{"id": ..., "type": ..., ...}` as the
    /// form built it — a `Value` for the same reason `create_pipeline` takes
    /// one.
    /// The state buckets and how full they are, in name order.
    pub async fn list_state_buckets(&self) -> Result<Vec<BucketSummary>, ApiError> {
        let resp = Request::get(&format!("{}/api/state", self.base))
            .send()
            .await?;
        Ok(resp.json::<Vec<BucketSummary>>().await?)
    }

    /// One bucket's contents, newest key first and capped by the server.
    pub async fn state_bucket(&self, name: &str) -> Result<BucketContents, ApiError> {
        let resp = Request::get(&format!("{}/api/state/{name}", self.base))
            .send()
            .await?;
        Ok(resp.json::<BucketContents>().await?)
    }

    pub async fn create_connection(&self, connection: &Value) -> Result<(), ApiError> {
        let resp = Request::post(&format!("{}/api/connections", self.base))
            .json(connection)?
            .send()
            .await?;
        if resp.ok() {
            Ok(())
        } else {
            Err(rejection(resp).await)
        }
    }

    /// Remove a connection. The server refuses — with a 409 naming them — while
    /// a running pipeline still uses it.
    pub async fn delete_connection(&self, id: &str) -> Result<(), ApiError> {
        let resp = Request::delete(&format!("{}/api/connections/{id}", self.base))
            .send()
            .await?;
        if resp.ok() {
            Ok(())
        } else {
            Err(rejection(resp).await)
        }
    }

    /// Write the running graph to `name`, in `format`, beside the config the
    /// server was started from. Returns the path it landed at, which is the
    /// server's to report — the UI knows the file name but not the directory.
    pub async fn save_config(&self, name: &str, format: ConfigFormat) -> Result<String, ApiError> {
        let resp = Request::post(&format!("{}/api/config/save", self.base))
            .json(&SaveConfigRequest {
                name: name.to_string(),
                format: Some(format),
            })?
            .send()
            .await?;
        if !resp.ok() {
            return Err(rejection(resp).await);
        }
        Ok(resp.json::<SaveConfigResponse>().await?.path)
    }

    /// Throw away the running graph and reload the config file.
    pub async fn revert_config(&self) -> Result<(), ApiError> {
        let resp = Request::post(&format!("{}/api/config/revert", self.base))
            .send()
            .await?;
        if resp.ok() {
            Ok(())
        } else {
            Err(rejection(resp).await)
        }
    }
}

/// Turn a non-2xx response into the message the user sees.
///
/// The server puts the whole `anyhow` context chain in `{"error": "..."}`, and
/// it's the most useful thing there is — "unknown upstream 'x'" beats "422".
/// A body that isn't that shape falls back to the status, which is all a
/// rejection from axum's own extractors carries.
async fn rejection(resp: gloo_net::http::Response) -> ApiError {
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    let message = serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|v| v["error"].as_str().map(ToString::to_string))
        .or_else(|| (!body.trim().is_empty()).then(|| body.clone()))
        .unwrap_or_else(|| format!("request failed with status {status}"));
    ApiError::Rejected(message)
}

#[derive(Clone, Debug)]
pub enum ApiError {
    Network(String),
    /// The server understood the request and said no. The message is its own.
    Rejected(String),
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
            ApiError::Rejected(msg) => write!(f, "{msg}"),
        }
    }
}
