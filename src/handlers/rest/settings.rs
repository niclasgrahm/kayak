use std::sync::Arc;

use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use streamer_core::{SaveConfigRequest, SaveConfigResponse, SettingsDto};

use crate::{handlers::error::AppError, state::AppState};

/// How the server was started, and whether it has drifted from the file.
///
/// The canvas can't work either out on its own, and both change what it should
/// offer: there is no point showing a save button with nowhere to save to, and
/// "unsaved changes" is the only warning that edits — which are live in the
/// runtime — are not yet on disk.
// axum handlers have to be async even when they do no awaiting
#[allow(clippy::unused_async)]
pub async fn get_settings(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(SettingsDto {
        config_file: state.config_file_name(),
        unsaved_changes: state.has_unsaved_changes(),
    })
}

/// Write the running graph to a config file beside the one the server started
/// from.
///
/// `name` is a bare file name and is validated as one — this is a write to the
/// server's disk driven by a request, so the directory is not negotiable. Using
/// the loaded file's own name is how you overwrite it.
#[allow(clippy::unused_async)]
pub async fn save_config(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<SaveConfigRequest>,
) -> Result<impl IntoResponse, AppError> {
    let path = state.save_config_as(&payload.name)?;
    Ok((
        StatusCode::OK,
        Json(SaveConfigResponse {
            path: path.display().to_string(),
        }),
    ))
}

/// Throw away the running graph and start again from the config file.
///
/// The undo for a session of editing. It stops every running pipeline, so it is
/// as destructive as it sounds — the UI asks first.
///
/// It waits for the old pipelines to actually stop before rebuilding, so the
/// response landing means the new graph is the only one running.
pub async fn revert_config(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, AppError> {
    state.revert().await?;
    Ok(StatusCode::NO_CONTENT)
}
