use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
    response::IntoResponse,
};

use crate::{handlers::error::AppError, state::AppState, state::PipelineError};

/// The state buckets and how full they are — see `Operation::ListStateBuckets`
/// in `kayak_core::api_docs`.
// axum handlers have to be async even when they do no awaiting
#[allow(clippy::unused_async)]
pub async fn get_state_buckets(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.buckets().summaries())
}

/// What one bucket is holding — see `Operation::GetStateBucket`.
#[allow(clippy::unused_async)]
pub async fn get_state_bucket(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    // a bucket that isn't declared is a 404 the same way a pipeline that isn't
    // running is: the name is wrong, or the config that declares it hasn't been
    // loaded
    let contents = state
        .buckets()
        .contents(&bucket)
        .ok_or_else(|| PipelineError::NotFound(bucket.clone()))?;
    Ok(Json(contents))
}
