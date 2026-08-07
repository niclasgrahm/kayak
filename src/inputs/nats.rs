use crate::{
    BuildCtx,
    inputs::{BuildInput, InputSource, MessageBatch},
    secrets::Resolved,
};
use anyhow::{Context, Result};
use tokio_stream::StreamExt;

use std::sync::Arc;

use kayak_core::config::NatsConfig;

impl BuildInput for NatsConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        Ok(Box::new(NatsInput {
            urls: ctx
                .resolve(&self.urls)
                .context("failed to resolve secrets in the nats input url")?,
            subject: self.subject,
            sub: None,
        }))
    }
}

pub struct NatsInput {
    pub urls: Resolved,
    pub subject: String,
    pub sub: Option<async_nats::Subscriber>,
}

#[async_trait::async_trait]
impl InputSource for NatsInput {
    async fn next(&mut self) -> Result<Arc<MessageBatch>> {
        if self.sub.is_none() {
            let client = async_nats::connect(self.urls.expose())
                .await
                .with_context(|| format!("failed to connect to nats at {}", self.urls))?;
            let subscriber = client
                .subscribe(self.subject.clone())
                .await
                .with_context(|| {
                    format!("failed to subscribe to nats subject '{}'", self.subject)
                })?;
            self.sub = Some(subscriber);
        }
        let subscriber = self
            .sub
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("nats subscriber not initialized"))?;

        loop {
            let msg = subscriber
                .next()
                .await
                .ok_or_else(|| anyhow::anyhow!("nats subscription on '{}' ended", self.subject))?;
            // a single malformed payload shouldn't kill the pipeline; skip it
            // and wait for the next message
            match serde_json::from_slice(&msg.payload) {
                Ok(value) => return Ok(Arc::new(vec![Arc::new(value)])),
                Err(e) => tracing::warn!(
                    "skipping non-json message on nats subject '{}': {}",
                    self.subject,
                    e
                ),
            }
        }
    }
}
