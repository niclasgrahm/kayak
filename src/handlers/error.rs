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
            Some(PipelineError::NotFound(_)) => StatusCode::NOT_FOUND,
            Some(PipelineError::DuplicateId(_)) => StatusCode::CONFLICT,
            Some(PipelineError::InvalidConfig(_)) => StatusCode::UNPROCESSABLE_ENTITY,
            Some(PipelineError::Internal(_)) | None => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self { status, err }
    }
}
