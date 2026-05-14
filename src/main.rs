use anyhow::Result;
use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct NatsInputConfig {
    brokers: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum InputConfig {
    Nats(NatsInputConfig),
    Streamer { upstream: StreamerId },
}

struct BuildCtx<'a> {
    streamers: &'a mut HashMap<StreamerId, StreamerHandle>,
}

impl InputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Input> {
        match self {
            InputConfig::Nats(nats_cfg) => {
                todo!()
                // Input::Nats(NatsConnection {}),
            }
            InputConfig::Streamer { upstream } => {
                // let (_, rx) = mpsc::channel(100);
                // Input::Streamer(rx)
                todo!()
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
enum TransformConfig {}

#[derive(Clone, Debug, Deserialize, Serialize)]
enum OutputConfig {
    Stdout,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Config {
    input: InputConfig,
    transforms: Vec<TransformConfig>,
    output: OutputConfig,
}

impl Config {
    fn new() -> Self {
        Self {
            input: InputConfig::Nats(NatsInputConfig {
                brokers: "http://yoyo:4222".to_string(),
            }),
            transforms: Vec::new(),
            output: OutputConfig::Stdout,
        }
    }
}

#[derive(Debug)]
struct NatsConnection {}

#[derive(Debug)]
enum Input {
    Nats(NatsConnection),
    Streamer(tokio::sync::mpsc::Receiver<Arc<serde_json::Value>>),
}

#[derive(Debug, Deserialize, Serialize)]
enum Transform {}

#[derive(Debug, Deserialize, Serialize)]
enum Output {
    Stdout,
}

#[derive(Serialize)]
struct Streamer {
    id: StreamerId,
    config: Config,
    #[serde(skip)]
    input: Input,
    #[serde(skip)]
    transforms: Vec<Transform>,
    #[serde(skip)]
    output: Output,
    downstream_senders: Mutex<Vec<mpsc::Sender<Arc<serde_json::Value>>>>,
}

impl Streamer {
    fn new(config: Config) -> Self {
        let id = petname::petname(3, "-").unwrap();
        Self {
            id,
            config,
            input: Input::Nats(NatsConnection {}),
            transforms: Vec::new(),
            output: Output::Stdout,
            downstream_senders: Mutex::new(Vec::new()),
        }
    }
    fn start(&self) -> tokio::task::JoinHandle<()> {
        tokio::task::spawn(async move {
            println!("streamer started");
        })
    }
    fn subscribe(&mut self, tx: mpsc::Sender<Arc<serde_json::Value>>) -> Result<()> {
        let mut senders = self.downstream_senders.get_mut()?;
        senders.push(tx);
        Ok(())
    }
}
type StreamerId = String;
type StreamerHandle = tokio::task::JoinHandle<()>;
struct AppState {
    streamers: Mutex<HashMap<StreamerId, StreamerHandle>>,
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
    let _ = axum::serve(listener, app).await;
}

async fn create_stream(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<Config>,
) -> (StatusCode, Json<Streamer>) {
    let streamer = Streamer::new(payload.clone());
    let streamer_handle = streamer.start();
    let mut app = state.streamers.lock().unwrap();
    app.insert(streamer.id.clone(), streamer_handle);
    println!("streamer inserted into app state");
    (StatusCode::CREATED, Json(streamer))
}
