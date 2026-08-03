use std::sync::Arc;

use crate::{
    BuildCtx,
    inputs::{BuildInput, InputSource, MessageBatch},
};
use anyhow::Result;
use anyhow::anyhow;
use tokio::sync::mpsc;
use streamer_core::config::StreamerConfig;

impl BuildInput for StreamerConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        let upstream_handle = ctx
            .streamers
            .get(&self.upstream)
            .ok_or_else(|| anyhow!("upstream streamer '{}' not found", self.upstream))?;
        let (tx, rx) = mpsc::channel(100);
        upstream_handle.shared.subscribe(tx);
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
        // recv() only returns None once every sender is gone, i.e. the upstream
        // streamer was deleted or died. Nothing more will ever arrive.
        self.rx
            .recv()
            .await
            .ok_or_else(|| anyhow!("upstream streamer '{}' is gone", self.upstream))
    }
}
