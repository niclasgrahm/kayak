use axum::{Json, Router, http::StatusCode, routing::post};
use serde::{Deserialize, Serialize};

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
    config: Config,
}

impl Streamer {
    fn new(config: Config) -> Self {
        Self { config }
    }
    fn start(self) {
        println!("hello from streamer")
    }
}

#[tokio::main]
async fn main() {
    let app = Router::new().route("/", post(create_stream));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:6767").await.unwrap();
    axum::serve(listener, app).await;
}

async fn create_stream(Json(_payload): Json<Config>) -> (StatusCode, Json<Streamer>) {
    let config = Config::new();
    (StatusCode::CREATED, Json(Streamer::new(config)))
}
