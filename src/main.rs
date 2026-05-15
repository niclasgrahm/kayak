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
    Dummy,
    Nats(NatsInputConfig),
    Streamer { upstream: StreamerId },
}

struct BuildCtx<'a> {
    streamers: &'a mut HashMap<StreamerId, StreamerHandle>,
}

impl InputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Input> {
        match self {
            InputConfig::Dummy => {
                todo!()
            }
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
    Dummy,
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
    downstream_senders: Mutex<Vec<mpsc::Sender<Arc<serde_json::Value>>>>,
}

#[derive(Serialize)]
struct StreamerView<'a> {
    id: &'a StreamerId,
    config: &'a Config,
}

struct StreamerRuntime {
    input: Input,
    transforms: Vec<Transform>,
    output: Output,
    shared: Arc<Streamer>,
}

impl Streamer {
    fn new(config: Config) -> Self {
        let id = petname::petname(3, "-").unwrap();
        Self {
            id,
            config,
            // input: Input::Nats(NatsConnection {}),
            // transforms: Vec::new(),
            // output: Output::Stdout,
            downstream_senders: Mutex::new(Vec::new()),
        }
    }

    fn create_runtime(self: &Arc<Self>) -> Result<StreamerRuntime> {
        Ok(StreamerRuntime {
            input: Input::Nats(NatsConnection {}),
            transforms: Vec::new(),
            output: Output::Stdout,
            shared: Arc::clone(self),
        })
    }
    fn start(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let runtime = self.create_runtime();
        tokio::task::spawn(async move {
            println!("streamer started");
        })
    }
    fn subscribe(&self, tx: mpsc::Sender<Arc<serde_json::Value>>) -> Result<()> {
        let mut senders = self.downstream_senders.lock().unwrap();
        senders.push(tx);
        Ok(())
    }
    fn view(&self) -> StreamerView<'_> {
        StreamerView {
            id: &self.id,
            config: &self.config,
        }
    }
}
type StreamerId = String;
struct StreamerHandle {
    join_handle: tokio::task::JoinHandle<()>,
    shared: Arc<Streamer>,
}

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
) -> (StatusCode, Json<serde_json::Value>) {
    let streamer = Arc::new(Streamer::new(payload.clone()));
    let join_handle = streamer.start();

    let streamer_handle = StreamerHandle {
        join_handle,
        shared: Arc::clone(&streamer),
    };
    let id = streamer.id.clone();
    let mut app = state.streamers.lock().unwrap();
    app.insert(id, streamer_handle);
    println!("streamer inserted into app state");
    let body = serde_json::to_value(streamer.view()).unwrap();
    (StatusCode::CREATED, Json(body))
}
