use std::sync::Arc;

use crate::{
    BuildCtx,
    inputs::{BuildInput, InputSource, MessageBatch, envelope::Envelope},
};
use anyhow::Result;
use anyhow::anyhow;
use kayak_core::config::PipelineConfig;
use serde_json::Value;
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
            envelope: ctx.envelope("pipeline", None),
            upstream: self.upstream,
            rx,
        }))
    }
}

pub struct PipelineInput {
    pub upstream: String,
    /// What this input attaches to each message, if the config asked for any.
    ///
    /// Usually nothing: metadata is in band, so whatever the upstream attached
    /// is already on the message and arrives with it. Setting an `envelope`
    /// here says something about *this* hop rather than replacing that, and a
    /// `wrap` would nest the upstream's message inside a new one.
    pub envelope: Envelope,
    pub rx: tokio::sync::mpsc::Receiver<Arc<MessageBatch>>,
}

#[async_trait::async_trait]
impl InputSource for PipelineInput {
    async fn next(&mut self) -> Result<Arc<MessageBatch>> {
        // recv() only returns None once every sender is gone, i.e. the upstream
        // pipeline was deleted or died. Nothing more will ever arrive.
        let batch = self
            .rx
            .recv()
            .await
            .ok_or_else(|| anyhow!("upstream pipeline '{}' is gone", self.upstream))?;

        if !self.envelope.is_enabled() {
            return Ok(batch);
        }
        let upstream = Value::String(self.upstream.clone());
        let enveloped = batch
            .iter()
            .filter_map(|message| {
                let out = self
                    .envelope
                    .apply((**message).clone(), vec![("upstream", upstream.clone())]);
                if out.is_none() {
                    tracing::warn!(
                        "skipping a message from upstream pipeline '{}': it is not a json \
                         object, so a `merge` envelope has nowhere to attach metadata",
                        self.upstream
                    );
                }
                out.map(Arc::new)
            })
            .collect();
        Ok(Arc::new(enveloped))
    }
}
