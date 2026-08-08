use std::sync::Arc;

use crate::{handlers::error::AppError, state::AppState};
use axum::{
    Json,
    extract::{Path, State},
    response::IntoResponse,
};
use kayak_core::config::Config;
use kayak_core::{IngestRequest, IngestResponse};
use reqwest::StatusCode;

pub async fn create_pipeline(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<Config>,
) -> Result<impl IntoResponse, AppError> {
    let pipeline = state.create_pipeline(payload)?;
    let body = serde_json::to_value(pipeline.view())?;
    Ok((StatusCode::CREATED, Json(body)))
}

pub async fn delete_pipeline(
    State(state): State<Arc<AppState>>,
    Path(pipeline_id): Path<String>,
) -> Result<StatusCode, AppError> {
    // AppError maps NotFound to 404; anything else is a genuine 500 rather
    // than a misleading "not found"
    state.delete_pipeline(&pipeline_id)?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn get_pipelines(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, AppError> {
    let pipelines = state.get_pipelines()?;
    Ok((StatusCode::OK, Json(pipelines)))
}

/// Post messages into a pipeline's `http` input — see `Operation::IngestMessages`
/// in `kayak_core::api_docs`.
pub async fn ingest_messages(
    State(state): State<Arc<AppState>>,
    Path(pipeline_id): Path<String>,
    Json(payload): Json<IngestRequest>,
) -> Result<impl IntoResponse, AppError> {
    // AppError maps the two failures apart: nothing to post to is a 404, a
    // pipeline that is behind is a 503
    let accepted = state.ingest(&pipeline_id, payload.into_messages())?;
    Ok((StatusCode::ACCEPTED, Json(IngestResponse { accepted })))
}
