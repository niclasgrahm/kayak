use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
};
use reqwest::StatusCode;

use crate::{config::Config, state::AppState};

pub async fn create_stream(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<Config>,
) -> (StatusCode, Json<serde_json::Value>) {
    match state.create_streamer(payload) {
        Ok(streamer) => {
            let body = serde_json::to_value(streamer.view()).unwrap();
            (StatusCode::CREATED, Json(body))
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": err.to_string()})),
        ),
    }
}

pub async fn delete_stream(
    State(state): State<Arc<AppState>>,
    Path(stream_id): Path<String>,
) -> StatusCode {
    match state.delete_streamer(stream_id) {
        Ok(()) => StatusCode::NO_CONTENT,
        // i guess we need to match on error; not found or something messed up
        Err(_err) => StatusCode::NOT_FOUND,
    }
}

pub async fn get_streams(
    State(state): State<Arc<AppState>>,
) -> (StatusCode, Json<serde_json::Value>) {
    match state.get_streamers() {
        Ok(streamers) => (StatusCode::OK, Json(streamers)),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": err.to_string()})),
        ),
    }
}
