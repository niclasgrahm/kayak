use anyhow::Result;
use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time;
use tokio;

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

#[derive(Debug)]
struct NatsConnection {}

#[derive(Debug, Deserialize, Serialize)]
enum Transform {}

#[tokio::main]
async fn main() {
    let state = AppState {
        streamers: Mutex::new(HashMap::new()),
    };
    let app = Router::new()
        .route("/", post(create_stream))
        .with_state(Arc::new(state));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:6767").await.unwrap();
    let _ = axum::serve(listener, app).await;
}

async fn create_stream(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<Config>,
) -> (StatusCode, Json<serde_json::Value>) {
    let streamer = Arc::new(Streamer::new(payload.clone()));
    let mut app = state.streamers.lock().unwrap();
    let mut ctx = BuildCtx::new(&mut app);
    let join_handle = streamer.start(ctx);

    let streamer_handle = StreamerHandle {
        join_handle,
        shared: Arc::clone(&streamer),
    };
    let id = streamer.id.clone();
    app.insert(id, streamer_handle);
    println!("streamer inserted into app state");
    let body = serde_json::to_value(streamer.view()).unwrap();
    (StatusCode::CREATED, Json(body))
}
