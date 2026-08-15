use gloo_net::http::Request;
use kayak_core::history::{PipelineHistory, Resolution};
use kayak_core::script::{DryRunRequest, DryRunResponse};
use kayak_core::state::{BucketContents, BucketSummary};
use kayak_core::{
    AuthDto, ConfigFormat, Connections, LayoutFile, LoginRequest, PipelineDto, SaveConfigRequest,
    SaveConfigResponse, SettingsDto,
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

    /// Run a script over some messages without creating a pipeline.
    ///
    /// Note the two-level result. The outer `ApiError` is the request going
    /// wrong; the inner [`DryRunResponse`] carries the script's own outcome,
    /// because **a script with a bug in it is a 200** — the request succeeded
    /// and where the bug is is the answer. Collapsing the two would throw away
    /// the line number, which is the whole point of asking.
    pub async fn dry_run_script(
        &self,
        request: &DryRunRequest,
    ) -> Result<DryRunResponse, ApiError> {
        let resp = Request::post(&format!("{}/api/scripts/dry-run", self.base))
            .json(request)?
            .send()
            .await?;
        if !resp.ok() {
            return Err(rejection(resp).await);
        }
        Ok(resp.json::<DryRunResponse>().await?)
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

    /// What a pipeline has been doing, from the server's in-memory record.
    ///
    /// The counterpart to the `/events` stream, not a replay of it: this is
    /// counts and aggregated failures, kept whether or not a browser was
    /// attached, and it carries no message payloads at all. `Fine` is the
    /// half-hour of five-second buckets a card backfills its live chart from;
    /// `Coarse` is the overnight record.
    pub async fn pipeline_history(
        &self,
        id: &str,
        resolution: Resolution,
    ) -> Result<PipelineHistory, ApiError> {
        let resolution = match resolution {
            Resolution::Fine => "fine",
            Resolution::Coarse => "coarse",
        };
        let resp = Request::get(&format!(
            "{}/api/pipelines/{id}/history?resolution={resolution}",
            self.base
        ))
        .send()
        .await?;
        Ok(resp.json::<PipelineHistory>().await?)
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
    /// Who the caller is, and whether this server asks at all.
    ///
    /// Public on the server, so this is the one call that works before signing
    /// in — which is what makes it the call that decides whether to show a
    /// login page.
    pub async fn whoami(&self) -> Result<AuthDto, ApiError> {
        let resp = Request::get(&format!("{}/api/auth/me", self.base))
            .send()
            .await?;
        if resp.ok() {
            Ok(resp.json::<AuthDto>().await?)
        } else {
            Err(rejection(resp).await)
        }
    }

    /// Exchange credentials for a session cookie.
    ///
    /// The cookie is `HttpOnly`, so nothing here ever sees it — the browser
    /// stores it and attaches it to everything afterwards, including the
    /// `EventSource` connection that no header could have authenticated.
    pub async fn login(&self, username: &str, password: &str) -> Result<AuthDto, ApiError> {
        let body = LoginRequest {
            username: username.to_string(),
            password: password.to_string(),
        };
        let resp = Request::post(&format!("{}/api/auth/login", self.base))
            .json(&body)?
            .send()
            .await?;
        if resp.ok() {
            Ok(resp.json::<AuthDto>().await?)
        } else {
            Err(rejection(resp).await)
        }
    }

    pub async fn logout(&self) -> Result<(), ApiError> {
        let resp = Request::post(&format!("{}/api/auth/logout", self.base))
            .send()
            .await?;
        if resp.ok() {
            Ok(())
        } else {
            Err(rejection(resp).await)
        }
    }

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
    // 401 is the one status the caller acts on rather than displays: a session
    // that has expired, or a server that was restarted out from under this tab,
    // means the page has to go back to the login form. Every other failure is
    // something to show and carry on from.
    if status == 401 {
        ApiError::Unauthorized(message)
    } else {
        ApiError::Rejected(message)
    }
}

#[derive(Clone, Debug)]
pub enum ApiError {
    Network(String),
    /// The server understood the request and said no. The message is its own.
    Rejected(String),
    /// Nobody is signed in — the session expired, or the server restarted and
    /// forgot it. Distinct from [`ApiError::Rejected`] because the UI *reacts*
    /// to it rather than printing it: the page drops back to the login form.
    Unauthorized(String),
}

impl From<gloo_net::Error> for ApiError {
    fn from(e: gloo_net::Error) -> Self {
        ApiError::Network(e.to_string())
    }
}

impl ApiError {
    /// What a failed sign-in should say.
    ///
    /// Its own wording rather than the server's, for the one case that matters:
    /// a 401 from `POST /api/auth/login` is **always** "wrong username or
    /// password", whatever the body said. The server takes care not to
    /// distinguish an unknown user from a wrong password — in its wording and
    /// in its timing — and it would be a poor joke to undo that here by
    /// printing whatever came back.
    ///
    /// The other two arms are worth telling apart because the fix differs: a
    /// network failure means the tab could not reach the server at all, which
    /// no amount of retyping a password will help.
    #[must_use]
    pub fn login_message(&self) -> String {
        match self {
            Self::Unauthorized(_) => "wrong username or password".to_string(),
            Self::Network(_) => "could not reach the server — is it still running?".to_string(),
            // a 500, or a body the server could not parse: rare, and the
            // server's own message is the useful thing to show
            Self::Rejected(message) => message.clone(),
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiError::Network(msg) => write!(f, "Network error: {msg}"),
            ApiError::Rejected(msg) | ApiError::Unauthorized(msg) => write!(f, "{msg}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ApiError;

    /// The server is careful not to say whether the username exists — not in
    /// the body and not in the timing. Echoing its message back would be
    /// harmless today and a leak the day someone makes that body more helpful,
    /// so the wording is fixed here instead.
    #[test]
    fn a_rejected_sign_in_never_says_which_half_was_wrong() {
        let message = ApiError::Unauthorized("no such user 'sam'".to_string()).login_message();
        assert_eq!(message, "wrong username or password");
        assert!(!message.contains("sam"));
    }

    /// A different problem with a different fix: retyping the password will
    /// not help if the server is not there.
    #[test]
    fn an_unreachable_server_says_so_rather_than_blaming_the_password() {
        let message = ApiError::Network("connection refused".to_string()).login_message();
        assert!(message.contains("could not reach the server"), "{message}");
        assert!(!message.contains("password"), "{message}");
    }

    /// Anything else is the server explaining itself, and that explanation is
    /// the useful thing to show.
    #[test]
    fn any_other_failure_shows_what_the_server_said() {
        assert_eq!(
            ApiError::Rejected("the session store is poisoned".to_string()).login_message(),
            "the session store is poisoned"
        );
    }
}
