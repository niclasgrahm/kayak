use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use kayak_core::connections::CreateConnectionRequest;

use crate::{handlers::error::AppError, state::AppState};

/// The connections pipelines can name, by name.
///
/// The same shape as the connections file itself, which is deliberate: what the
/// UI lists and what gets committed are one thing, so there is no second format
/// to keep in step.
// axum handlers have to be async even when they do no awaiting
#[allow(clippy::unused_async)]
pub async fn get_connections(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.connections())
}

/// Add a connection.
///
/// It changes what the *next* pipeline build can name and nothing else — no
/// running pipeline is touched, because a component reads its connection once,
/// when it is built. Like creating a pipeline, this writes nothing to disk; the
/// save does.
#[allow(clippy::unused_async)]
pub async fn create_connection(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<CreateConnectionRequest>,
) -> Result<impl IntoResponse, AppError> {
    state.create_connection(payload.id.clone(), payload.connection.clone())?;
    Ok((StatusCode::CREATED, Json(payload)))
}

/// Remove a connection, unless a running pipeline still names it — that comes
/// back as a 409 listing the pipelines, so the answer says what to do about it.
#[allow(clippy::unused_async)]
pub async fn delete_connection(
    State(state): State<Arc<AppState>>,
    Path(connection_id): Path<String>,
) -> Result<StatusCode, AppError> {
    state.delete_connection(&connection_id)?;
    Ok(StatusCode::NO_CONTENT)
}
