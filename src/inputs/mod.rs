use crate::config::NatsInputConfig;
use std::sync::Arc;

#[derive(Debug)]
pub enum Input {
    Dummy,
    Nats {
        cfg: NatsInputConfig,
        sub: Option<async_nats::Subscriber>,
    },
    Streamer(tokio::sync::mpsc::Receiver<Arc<serde_json::Value>>),
}
