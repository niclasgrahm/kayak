use anyhow::Result;
use std::sync::Arc;

pub mod dummy;
pub mod nats;
pub mod streamer;

pub type MessageBatch = Vec<Arc<serde_json::Value>>;

#[async_trait::async_trait]
pub trait InputSource: Send + 'static {
    async fn next(&mut self) -> anyhow::Result<Arc<MessageBatch>>;
}

pub struct Buffered {
    pub inner: Box<dyn InputSource>,
    pub buffer: usize,
}

#[async_trait::async_trait]
impl InputSource for Buffered {
    async fn next(&mut self) -> Result<Arc<MessageBatch>> {
        let mut batch = Vec::with_capacity(self.buffer);
        for _ in 0..self.buffer {
            let inner_batch = self.inner.next().await?;
            batch.extend(inner_batch.iter().cloned());
        }
        Ok(Arc::new(batch))
    }
}
