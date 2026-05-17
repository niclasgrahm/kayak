use crate::config::NatsInputConfig;
use serde_json::Value;
use std::sync::Arc;

pub mod dummy;

pub enum Input {
    Dyn(Box<dyn InputSource>),
    Nats {
        cfg: NatsInputConfig,
        sub: Option<async_nats::Subscriber>,
    },
    Streamer(tokio::sync::mpsc::Receiver<Arc<serde_json::Value>>),
}

#[async_trait::async_trait]
pub trait InputSource: Send + 'static {
    async fn next(&mut self) -> anyhow::Result<Arc<Value>>;
}
