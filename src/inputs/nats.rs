use crate::{
    BuildCtx,
    inputs::{BuildInput, InputSource, MessageBatch},
    secrets::Resolved,
};
use anyhow::{Context, Result};
use futures_util::FutureExt;
use tokio_stream::StreamExt;

use std::sync::Arc;

use kayak_core::config::NatsConfig;

impl BuildInput for NatsConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        let server = ctx
            .nats_connection(&self.connection)
            .context("the nats input cannot be built")?;
        Ok(Box::new(NatsInput {
            urls: ctx.resolve(&server.urls).with_context(|| {
                format!(
                    "failed to resolve secrets in the url of connection '{}'",
                    self.connection
                )
            })?,
            subject: self.subject,
            // one message per batch unless the config asks for more — see
            // `NatsInput::next`
            max_batch: crate::inputs::batch_cap(self.max_batch),
            sub: None,
        }))
    }
}

pub struct NatsInput {
    pub urls: Resolved,
    pub subject: String,
    /// Most messages in one batch. One unless the config says otherwise.
    pub max_batch: usize,
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

        // a single malformed payload shouldn't kill the pipeline; skip it and
        // wait for the next message
        let decode = |payload: &[u8]| match serde_json::from_slice::<serde_json::Value>(payload) {
            Ok(value) => Some(value),
            Err(e) => {
                tracing::warn!(
                    "skipping non-json message on nats subject '{}': {}",
                    self.subject,
                    e
                );
                None
            }
        };

        let mut batch: MessageBatch = Vec::new();
        while batch.is_empty() {
            let msg = subscriber
                .next()
                .await
                .ok_or_else(|| anyhow::anyhow!("nats subscription on '{}' ended", self.subject))?;
            if let Some(value) = decode(&msg.payload) {
                batch.push(Arc::new(value));
            }
        }

        // Whatever else has *already* arrived, up to the cap — never a wait for
        // one to fill, so a quiet subject still yields batches of one however
        // high `max_batch` is. With the default of 1 this loop never runs.
        while batch.len() < self.max_batch {
            let Some(ready) = subscriber.next().now_or_never() else {
                break;
            };
            let msg = ready
                .ok_or_else(|| anyhow::anyhow!("nats subscription on '{}' ended", self.subject))?;
            if let Some(value) = decode(&msg.payload) {
                batch.push(Arc::new(value));
            }
        }

        Ok(Arc::new(batch))
    }
}
