use std::sync::Arc;

use crate::{handlers::error::AppError, inputs::http::PostMeta, state::AppState};
use axum::{
    Json,
    extract::{ConnectInfo, Path, State},
    http::{Extensions, HeaderMap, Method},
    response::IntoResponse,
};
use std::net::SocketAddr;
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
    method: Method,
    // read out of the extensions rather than taken as a `ConnectInfo`
    // extractor, because there may not be one: the address is only there when
    // the server was started with `into_make_service_with_connect_info`, and a
    // test driving the router through `tower::oneshot` has no peer at all. A
    // missing address is a `null` in the metadata, not a failed request.
    extensions: Extensions,
    headers: HeaderMap,
    // the body extractor has to come last — it consumes the request
    Json(payload): Json<IngestRequest>,
) -> Result<impl IntoResponse, AppError> {
    let meta = PostMeta::new(
        method.as_str(),
        extensions
            .get::<ConnectInfo<SocketAddr>>()
            .map(|ConnectInfo(addr)| addr.to_string()),
        headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.to_str().unwrap_or_default())),
    );
    // AppError maps the two failures apart: nothing to post to is a 404, a
    // pipeline that is behind is a 503
    let accepted = state.ingest(&pipeline_id, payload.into_messages(), meta)?;
    Ok((StatusCode::ACCEPTED, Json(IngestResponse { accepted })))
}
