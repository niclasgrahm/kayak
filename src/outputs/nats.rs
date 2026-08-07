use anyhow::Context;
use bytes::Bytes;
use kayak_core::config::NatsOutputConfig;
use std::sync::Arc;

use crate::{
    BuildCtx,
    inputs::MessageBatch,
    outputs::{BuildOutput, OutputDestination},
    secrets::Resolved,
};

impl BuildOutput for NatsOutputConfig {
    fn build(self, ctx: &mut BuildCtx) -> anyhow::Result<Box<dyn OutputDestination>> {
        let server = ctx
            .nats_connection(&self.connection)
            .context("the nats output cannot be built")?;
        Ok(Box::new(NatsOutput {
            urls: ctx.resolve(&server.urls).with_context(|| {
                format!(
                    "failed to resolve secrets in the url of connection '{}'",
                    self.connection
                )
            })?,
            subject: self.subject,
            client: None,
        }))
    }
}

pub struct NatsOutput {
    urls: Resolved,
    subject: String,
    client: Option<async_nats::Client>,
}

#[async_trait::async_trait]
impl OutputDestination for NatsOutput {
    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> anyhow::Result<()> {
        // silently doing nothing when init() never ran would look like the
        // messages were published, so make it an error instead
        let client = self.client.as_ref().ok_or_else(|| {
            anyhow::anyhow!("nats output is not connected; init() was not called")
        })?;

        for msg in message_batch.iter() {
            let payload =
                serde_json::to_vec(msg).context("failed to serialize message for nats")?;
            client
                .publish(self.subject.clone(), Bytes::from(payload))
                .await
                .with_context(|| format!("failed to publish to nats subject '{}'", self.subject))?;
        }
        Ok(())
    }

    async fn init(&mut self) -> anyhow::Result<()> {
        self.client = Some(
            async_nats::connect(self.urls.expose())
                .await
                .with_context(|| format!("failed to connect to nats at {}", self.urls))?,
        );
        Ok(())
    }
}
