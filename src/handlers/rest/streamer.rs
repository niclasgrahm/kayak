use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
    response::IntoResponse,
};
use reqwest::StatusCode;
use streamer_core::config::Config;
use crate::{handlers::error::AppError, state::AppState};

pub async fn create_stream(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<Config>,
) -> Result<impl IntoResponse, AppError> {
    let streamer = state.create_streamer(payload)?;
    let body = serde_json::to_value(streamer.view())?;
    Ok((StatusCode::CREATED, Json(body)))
}

pub async fn delete_stream(
    State(state): State<Arc<AppState>>,
    Path(stream_id): Path<String>,
) -> Result<StatusCode, AppError> {
    // AppError maps NotFound to 404; anything else is a genuine 500 rather
    // than a misleading "not found"
    state.delete_streamer(&stream_id)?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn get_streams(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, AppError> {
    let streamers = state.get_streamers()?;
    Ok((StatusCode::OK, Json(streamers)))
}
