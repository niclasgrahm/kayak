//! The kayak stream-processing runtime.
//!
//! Everything except the process entry point lives here rather than in
//! `main.rs`, so that integration tests in `tests/` (which are separate crates)
//! can build pipelines, drive run loops and call the HTTP handlers directly.

use axum::{
    Router,
    routing::{delete, get, post},
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;

pub mod config;
pub mod handlers;
pub mod inputs;
pub mod outputs;
pub mod state;
pub mod streamer;
pub mod testing;
pub mod transforms;

use crate::handlers::{
    rest::streamer::{create_stream, delete_stream, get_streams},
    ui::{docs::get_docs, ui::events_handler},
};
use crate::state::{AppState, StreamerHandle, StreamerId, UiEvent};

/// Threaded through every `build()` call. It carries the streamer map — needed
/// so a `streamer` input can look up its upstream and register an mpsc sender
/// on it — and the UI event channel.
pub struct BuildCtx<'a> {
    pub streamers: &'a mut HashMap<StreamerId, StreamerHandle>,
    pub events: broadcast::Sender<UiEvent>,
}

impl<'a> BuildCtx<'a> {
    pub fn new(
        streamers: &'a mut HashMap<StreamerId, StreamerHandle>,
        events: broadcast::Sender<UiEvent>,
    ) -> Self {
        Self { streamers, events }
    }
}

/// The JSON/SSE surface, without the Leptos routes. `main` merges this with the
/// frontend router; tests call it directly through `tower::ServiceExt::oneshot`,
/// which keeps them off real sockets.
pub fn api_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/docs", get(get_docs))
        .route("/events", get(events_handler))
        .route("/api/streams", post(create_stream))
        .route("/api/streams", get(get_streams))
        .route("/api/streams/{stream_id}", delete(delete_stream))
        .with_state(state)
}
