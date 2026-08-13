use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
    response::IntoResponse,
};
use serde::Deserialize;

use crate::state::AppState;
use kayak_core::history::Resolution;

/// The `?resolution=` on the history endpoint.
///
/// Deliberately lenient: an unreadable or absent value is the default rather
/// than a 400. The parameter picks between two views of the same record, so
/// there is nothing a caller can lose by getting the other one, and a chart
/// that fails to draw because a query string was misspelled is a worse
/// outcome than a chart drawn at the wrong zoom.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct HistoryQuery {
    /// Taken as text and matched here rather than deserialized straight into
    /// [`Resolution`], which is what makes the leniency above real: a
    /// `#[derive]`d enum field rejects an unknown value and turns the whole
    /// request into a 400.
    resolution: Option<String>,
}

impl HistoryQuery {
    fn resolution(&self) -> Resolution {
        match self.resolution.as_deref() {
            Some("fine") => Resolution::Fine,
            _ => Resolution::Coarse,
        }
    }
}

/// What a pipeline has been doing — see `Operation::GetPipelineHistory` in
/// `kayak_core::api_docs`.
// axum handlers have to be async even when they do no awaiting
#[allow(clippy::unused_async)]
pub async fn get_pipeline_history(
    State(state): State<Arc<AppState>>,
    Path(pipeline_id): Path<String>,
    Query(query): Query<HistoryQuery>,
) -> impl IntoResponse {
    let resolution = query.resolution();
    // No 404 for an unknown id, unlike every other pipeline endpoint: history
    // deliberately outlives its pipeline, so "no such pipeline" and "that
    // pipeline has no history" are different questions and only the second one
    // is this endpoint's. An empty history is the honest answer to both.
    Json(state.history().get(&pipeline_id, resolution))
}
