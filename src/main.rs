use askama::Template;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::{delete, get, post},
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
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
    tracing::info!("Starting server...");

    let state = AppState {
        streamers: Mutex::new(HashMap::new()),
    };
    let app = Router::new()
        .route("/ui", get(index_handler))
        .route("/", post(create_stream))
        .route("/", get(get_streams))
        .route("/{stream_id}", delete(delete_stream))
        .with_state(Arc::new(state));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:6767")
        .await
        .expect("failed to bind to address");
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
    match state.streamers.lock() {
        Ok(mut app) => {
            let ctx = BuildCtx::new(&mut app);
            let join_handle = streamer.start(ctx);

            let streamer_handle = StreamerHandle {
                join_handle,
                shared: Arc::clone(&streamer),
            };
            let id = streamer.id.clone();
            app.insert(id, streamer_handle);
            let body = serde_json::to_value(streamer.view()).unwrap();
            tracing::debug!("streamer created, config: {}", &body);
            (StatusCode::CREATED, Json(body))
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "foo"})),
        ),
    }
}

async fn delete_stream(
    State(state): State<Arc<AppState>>,
    Path(stream_id): Path<String>,
) -> StatusCode {
    let mut app = state.streamers.lock().unwrap();
    if let Some(streamer) = app.get(stream_id.as_str()) {
        // signal cancellation here
        streamer.shared.cancellation_token.cancel();
        app.remove(stream_id.clone().as_str());
        tracing::debug!("streamer deleted: {}", stream_id);

        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn index_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    #[derive(Template)]
    #[template(path = "index.html")]
    struct Tmpl {
        streamers: Vec<String>,
    }
    let streamers = state.streamers.lock().unwrap().keys().cloned().collect();
    let template = Tmpl { streamers };
    Html(template.render().unwrap())
}
