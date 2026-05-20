use serde_json::Value;
use std::sync::Arc;

pub mod dummy;
pub mod nats;
pub mod streamer;

#[async_trait::async_trait]
pub trait InputSource: Send + 'static {
    async fn next(&mut self) -> anyhow::Result<Arc<Value>>;
}
