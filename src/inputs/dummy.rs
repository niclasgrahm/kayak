use crate::inputs::InputSource;
use crate::inputs::MessageBatch;
use anyhow::Result;
use chrono::Utc;
use serde_json::Value;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

pub struct DummyInput {
    pub interval: Duration,
}

#[async_trait::async_trait]
impl InputSource for DummyInput {
    async fn next(&mut self) -> Result<MessageBatch> {
        tokio::time::sleep(self.interval).await;
        Ok(vec![Arc::new(json!({
            "hello": "streamer",
            "current_time": Utc::now().to_string(),
        }))])
    }
}
