use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Deserialize, Serialize)]
struct NatsInputConfig {
    brokers: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Input {
    Nats(NatsInputConfig),
}

#[derive(Debug, Deserialize, Serialize)]
enum Transform {}

#[derive(Debug, Deserialize, Serialize)]
enum Output {
    Stdout,
}

#[derive(Debug, Deserialize, Serialize)]
struct Config {
    input: Input,
    transforms: Vec<Transform>,
    output: Output,
}

impl Config {
    fn new() -> Self {
        Self {
            input: Input::Nats(NatsInputConfig {
                brokers: "http://yoyo:4222".to_string(),
            }),
            transforms: Vec::new(),
            output: Output::Stdout,
        }
    }
}

#[derive(Serialize)]
struct Streamer {
    id: StreamerId,
    config: Config,
}

impl Streamer {
    fn new(config: Config) -> Self {
        Self {
            id: "foo".to_string(),
            config,
        }
    }
    fn start(&self) -> tokio::task::JoinHandle<()> {
        tokio::task::spawn(async move {
            println!("streamer started");
        })
    }
}
type StreamerId = String;
struct AppState {
    streamers: Mutex<HashMap<StreamerId, tokio::task::JoinHandle<()>>>,
}

#[tokio::main]
async fn main() {
    let state = AppState {
        streamers: Mutex::new(HashMap::new()),
    };
    let app = Router::new()
        .route("/", post(create_stream))
        .with_state(Arc::new(state));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:6767").await.unwrap();
    axum::serve(listener, app).await;
}

async fn create_stream(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<Config>,
) -> (StatusCode, Json<Streamer>) {
    let streamer = Streamer::new(payload);
    let streamer_handle = streamer.start();
    let mut app = state.streamers.lock().unwrap();
    app.insert(streamer.id.clone(), streamer_handle);
    println!("streamer inserted into app state");
    (StatusCode::CREATED, Json(streamer))
}
