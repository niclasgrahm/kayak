use askama::Template;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::{delete, get, post},
};
use std::sync::{Arc, Mutex};
use std::{collections::HashMap, path::PathBuf};
use tracing::Level;

mod config;
mod inputs;
mod outputs;
mod state;
mod streamer;
mod transforms;
use crate::config::Config;
use crate::state::{AppState, StreamerHandle, StreamerId};
use crate::streamer::Streamer;
use clap::Parser;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    debug: bool,
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long, default_value_t = 6767)]
    port: u16,
}

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
    let args = Args::parse();
    let level = if args.debug {
        Level::DEBUG
    } else {
        Level::INFO
    };

    tracing_subscriber::fmt()
        .with_env_filter(match level {
            Level::DEBUG => "info,streamer=debug",
            _ => "info",
        })
        .init();
    let addr = format!("0.0.0.0:{}", args.port);
    tracing::info!("Starting server on {}", addr);

    let state = match &args.config {
        Some(path) => AppState::from_config(path).unwrap(),
        None => AppState::new(),
    };

    let app = Router::new()
        .route("/ui", get(index_handler))
        .route("/", post(create_stream))
        .route("/", get(get_streams))
        .route("/{stream_id}", delete(delete_stream))
        .with_state(Arc::new(state));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind to address");
    let _ = axum::serve(listener, app).await;
}

async fn get_streams(State(state): State<Arc<AppState>>) -> (StatusCode, Json<serde_json::Value>) {
    match state.get_streamers() {
        Ok(streamers) => (StatusCode::OK, Json(streamers)),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": err.to_string()})),
        ),
    }
}
async fn create_stream(
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

async fn delete_stream(
    State(state): State<Arc<AppState>>,
    Path(stream_id): Path<String>,
) -> StatusCode {
    match state.delete_streamer(stream_id) {
        Ok(_) => StatusCode::NO_CONTENT,
        // i guess we need to match on error; not found or something messed up
        Err(err) => StatusCode::NOT_FOUND,
    }
}

async fn index_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    #[derive(Template)]
    #[template(path = "index.html")]
    struct Tmpl {
        streamers: Vec<String>,
    }
    let streamers = state.get_streamer_ids().unwrap_or_default();
    let template = Tmpl { streamers };
    Html(template.render().unwrap())
}
