use std::sync::Arc;

use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use streamer_core::LayoutFile;

use crate::{handlers::error::AppError, state::AppState};

/// Where the cards sit on the canvas.
///
/// Served separately from `/api/streams` because it is a different kind of
/// thing: the stream list is what the server is running, this is how someone
/// chose to look at it. A client that ignores this endpoint gets an
/// automatically laid out graph, which is the point.
// axum handlers have to be async even when they do no awaiting
#[allow(clippy::unused_async)]
pub async fn get_layout(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.layout())
}

/// Replace the arrangement and write it to disk.
///
/// The whole map, not a patch: the canvas already holds the complete
/// arrangement, and a full replacement is what makes "reset to automatic" a
/// send of `{}` rather than its own endpoint. It writes immediately — this is
/// the one edit that doesn't go through save, because it changes nothing the
/// server runs.
#[allow(clippy::unused_async)]
pub async fn put_layout(
    State(state): State<Arc<AppState>>,
    Json(layout): Json<LayoutFile>,
) -> Result<impl IntoResponse, AppError> {
    state.set_layout(layout)?;
    Ok(StatusCode::NO_CONTENT)
}
