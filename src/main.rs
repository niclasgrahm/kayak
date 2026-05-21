use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post},
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

mod config;
mod inputs;
mod outputs;
mod state;
mod streamer;
mod transforms;
use crate::config::Config;
use crate::state::{AppState, StreamerHandle, StreamerId};
use crate::streamer::Streamer;

struct BuildCtx<'a> {
    streamers: &'a mut HashMap<StreamerId, StreamerHandle>,
}

impl<'a> BuildCtx<'a> {
    fn new(streamers: &'a mut HashMap<StreamerId, StreamerHandle>) -> Self {
        Self { streamers }
    }
}

#[tokio::main]
async fn main() {
    let state = AppState {
        streamers: Mutex::new(HashMap::new()),
    };
    let app = Router::new()
        .route("/", post(create_stream))
        .route("/", get(get_streams))
        .route("/{stream_id}", delete(delete_stream))
        .with_state(Arc::new(state));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:6767").await.unwrap();
    let _ = axum::serve(listener, app).await;
}

async fn get_streams(State(state): State<Arc<AppState>>) -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::OK,
        Json(serde_json::to_value(&*state.streamers.lock().unwrap()).unwrap()),
    )
}
async fn create_stream(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<Config>,
) -> (StatusCode, Json<serde_json::Value>) {
    let streamer = Arc::new(Streamer::new(payload.clone()));
    let mut app = state.streamers.lock().unwrap();
    let ctx = BuildCtx::new(&mut app);
    let join_handle = streamer.start(ctx);

    let streamer_handle = StreamerHandle {
        join_handle,
        shared: Arc::clone(&streamer),
    };
    let id = streamer.id.clone();
    app.insert(id, streamer_handle);
    let body = serde_json::to_value(streamer.view()).unwrap();
    (StatusCode::CREATED, Json(body))
}

async fn delete_stream(
    State(state): State<Arc<AppState>>,
    Path(stream_id): Path<String>,
) -> StatusCode {
    let mut app = state.streamers.lock().unwrap();
    if let Some(streamer) = app.get(stream_id.as_str()) {
        // signal cancellation here
        streamer.shared.cancellation_token.cancel();
        app.remove(stream_id.as_str());
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}
