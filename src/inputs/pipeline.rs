use std::sync::Arc;

use crate::{
    BuildCtx,
    inputs::{
        BuildInput, InputSource, MessageBatch,
        ack::{self, Delivery},
        envelope::Envelope,
    },
};
use anyhow::Result;
use anyhow::anyhow;
use kayak_core::config::PipelineConfig;
use serde_json::Value;
use tokio::sync::mpsc;

impl BuildInput for PipelineConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        // the upstream pipeline already decided this batch was "delivered" the
        // moment its `tx.send` into this input's channel returned `Ok` — see
        // the `ack` module docs — so there is nothing further for *this* hop
        // to acknowledge
        ack::require_receipt_only(ctx.ack_mode(), "pipeline")?;
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
    async fn next(&mut self) -> Result<Delivery> {
        // recv() only returns None once every sender is gone, i.e. the upstream
        // pipeline was deleted or died. Nothing more will ever arrive.
        let batch = self
            .rx
            .recv()
            .await
            .ok_or_else(|| anyhow!("upstream pipeline '{}' is gone", self.upstream))?;

        if !self.envelope.is_enabled() {
            return Ok(Delivery::new(batch));
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
        Ok(Delivery::new(Arc::new(enveloped)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::Pipeline;
    use crate::state::PipelineHandle;
    use kayak_core::config::AckMode;
    use std::collections::HashMap;

    fn build(ack_mode: Option<AckMode>) -> Result<Box<dyn InputSource>> {
        let mut pipelines = HashMap::new();
        let (events, _rx) = tokio::sync::broadcast::channel(4);
        let upstream = match Pipeline::new(crate::testing::stub_config("upstream")) {
            Ok(p) => Arc::new(p),
            Err(e) => panic!("building the stub upstream pipeline: {e:#}"),
        };
        pipelines.insert(
            "upstream".to_string(),
            PipelineHandle {
                join_handle: tokio::spawn(async {}),
                shared: upstream,
            },
        );
        let mut ctx = BuildCtx::new(&mut pipelines, "p".to_string(), events);
        ctx.ack_mode = ack_mode;
        PipelineConfig {
            upstream: "upstream".to_string(),
        }
        .build(&mut ctx)
    }

    /// The upstream already decided this batch was delivered the moment its
    /// own `tx.send` succeeded, so this hop has nothing further to
    /// acknowledge — but the default mode must still build.
    #[tokio::test]
    async fn absent_and_on_receipt_both_build() {
        assert!(build(None).is_ok());
        assert!(build(Some(AckMode::OnReceipt)).is_ok());
    }

    #[tokio::test]
    async fn on_delivery_is_refused() {
        let Err(err) = build(Some(AckMode::OnDelivery)) else {
            panic!("a pipeline input built with `ack: on_delivery`, which it cannot honour");
        };
        assert!(format!("{err:#}").contains("pipeline"), "{err:#}");
    }
}
