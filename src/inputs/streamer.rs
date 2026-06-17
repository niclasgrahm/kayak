use std::sync::Arc;

use crate::{
    BuildCtx,
    inputs::{BuildInput, InputSource, MessageBatch},
    state::StreamerId,
};
use anyhow::Result;
use anyhow::anyhow;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "streamer")]
pub struct StreamerConfig {
    pub upstream: StreamerId,
}

impl BuildInput for StreamerConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        let upstream_handle = ctx
            .streamers
            .get(&self.upstream)
            .ok_or_else(|| anyhow!("upstream streamer '{}' not found", self.upstream))?;
        let (tx, rx) = mpsc::channel(100);
        upstream_handle.shared.subscribe(tx)?;
        Ok(Box::new(StreamerInput {
            upstream: self.upstream,
            rx,
        }))
    }
}

pub struct StreamerInput {
    pub upstream: String,
    pub rx: tokio::sync::mpsc::Receiver<Arc<MessageBatch>>,
}

#[async_trait::async_trait]
impl InputSource for StreamerInput {
    async fn next(&mut self) -> Result<Arc<MessageBatch>> {
        Ok(self.rx.recv().await.expect("upstream channel closed"))
    }
}
