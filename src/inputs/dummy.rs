use crate::BuildCtx;
use crate::inputs::BuildInput;
use crate::inputs::InputSource;
use crate::inputs::MessageBatch;
use anyhow::Result;
use chrono::Utc;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

use streamer_core::config::DummyConfig;

impl BuildInput for DummyConfig {
    fn build(self, _ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        Ok(Box::new(DummyInput {
            interval: Duration::from_secs(self.duration),
        }))
    }
}

pub struct DummyInput {
    pub interval: Duration,
}

#[async_trait::async_trait]
impl InputSource for DummyInput {
    async fn next(&mut self) -> Result<Arc<MessageBatch>> {
        tokio::time::sleep(self.interval).await;
        tracing::debug!("Emitting dummy message inside dummy input");
        Ok(Arc::new(vec![Arc::new(json!({
            "hello": "streamer",
            "current_time": Utc::now().to_string(),
        }))]))
    }
}
