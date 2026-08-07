use std::sync::Arc;

use crate::{handlers::error::AppError, state::AppState};
use axum::{
    Json,
    extract::{Path, State},
    response::IntoResponse,
};
use kayak_core::config::Config;
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
