use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};

use crate::state::PipelineError;

pub struct AppError {
    status: StatusCode,
    err: anyhow::Error,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        // the full cause chain is useful in the log but not in the response body
        if self.status.is_server_error() {
            tracing::error!("request failed: {:?}", self.err);
        } else {
            tracing::debug!("request rejected ({}): {}", self.status, self.err);
        }
        (
            self.status,
            // `{:#}` keeps the context chain, so the client learns the actual cause
            Json(serde_json::json!({"error": format!("{:#}", self.err)})),
        )
            .into_response()
    }
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        let err = err.into();
        // downcast_ref walks the whole cause chain, so added context doesn't
        // hide the classification
        let status = match err.downcast_ref::<PipelineError>() {
            // both are "there is nothing here to post to": one has no
            // pipeline, one has no http input on the pipeline it does have
            Some(PipelineError::NotFound(_) | PipelineError::NotAccepting(_)) => {
                StatusCode::NOT_FOUND
            }
            // the pipeline is there and behind, which is a "come back" rather
            // than a "you asked for the wrong thing"
            Some(PipelineError::Backpressure(_)) => StatusCode::SERVICE_UNAVAILABLE,
            // an `http` input's own credential, not the server's sign-in. No
            // `WWW-Authenticate` here for the reason `auth::refuse` gives: the
            // header is indistinguishable to a browser from a demand for the
            // login it already has, and nothing that posts data needs the
            // challenge in order to know to send credentials.
            Some(PipelineError::Unauthorized(_)) => StatusCode::UNAUTHORIZED,
            // both are "the server's state disagrees with what you asked for",
            // and both are fixed by doing something else first
            Some(PipelineError::DuplicateId(_) | PipelineError::ConnectionInUse(..)) => {
                StatusCode::CONFLICT
            }
            Some(PipelineError::InvalidConfig(_)) => StatusCode::UNPROCESSABLE_ENTITY,
            Some(PipelineError::Internal(_)) | None => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self { status, err }
    }
}
