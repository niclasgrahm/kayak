use std::sync::Arc;
#[derive(Debug)]
struct NatsConnection {}

#[derive(Debug)]
pub enum Input {
    Dummy,
    Nats(NatsConnection),
    Streamer(tokio::sync::mpsc::Receiver<Arc<serde_json::Value>>),
}
