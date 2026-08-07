use std::sync::Arc;

use crate::{
    BuildCtx,
    inputs::{BuildInput, InputSource, MessageBatch},
};
use anyhow::Result;
use anyhow::anyhow;
use kayak_core::config::PipelineConfig;
use tokio::sync::mpsc;

impl BuildInput for PipelineConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        let upstream_handle = ctx
            .pipelines
            .get(&self.upstream)
            .ok_or_else(|| anyhow!("upstream pipeline '{}' not found", self.upstream))?;
        let (tx, rx) = mpsc::channel(100);
        upstream_handle.shared.subscribe(tx);
        Ok(Box::new(PipelineInput {
            upstream: self.upstream,
            rx,
        }))
    }
}

pub struct PipelineInput {
    pub upstream: String,
    pub rx: tokio::sync::mpsc::Receiver<Arc<MessageBatch>>,
}

#[async_trait::async_trait]
impl InputSource for PipelineInput {
    async fn next(&mut self) -> Result<Arc<MessageBatch>> {
        // recv() only returns None once every sender is gone, i.e. the upstream
        // pipeline was deleted or died. Nothing more will ever arrive.
        self.rx
            .recv()
            .await
            .ok_or_else(|| anyhow!("upstream pipeline '{}' is gone", self.upstream))
    }
}
