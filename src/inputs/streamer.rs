use std::sync::Arc;

use crate::inputs::{InputSource, MessageBatch};
use anyhow::Result;
use serde_json::Value;
pub struct StreamerInput {
    pub upstream: String,
    pub rx: tokio::sync::mpsc::Receiver<MessageBatch>,
}

#[async_trait::async_trait]
impl InputSource for StreamerInput {
    async fn next(&mut self) -> Result<MessageBatch> {
        Ok(self.rx.recv().await.expect("upstream channel closed"))
    }
}
