use anyhow::Context;
use bytes::Bytes;
use kayak_core::config::NatsOutputConfig;
use std::sync::Arc;
use std::time::Instant;

use crate::{
    BuildCtx,
    backoff::Gate,
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
            gate: Gate::new(),
        }))
    }
}

pub struct NatsOutput {
    urls: Resolved,
    subject: String,
    client: Option<async_nats::Client>,
    /// Paces reconnect attempts after the connection is lost — see `emit`.
    /// `init` never consults it, and does not need to: an
    /// output that can't reach its output at startup is retried by the run
    /// loop instead, on this same schedule — see
    /// `PipelineRuntime::init_outputs`.
    gate: Gate,
}

impl NatsOutput {
    async fn connect(&self) -> anyhow::Result<async_nats::Client> {
        async_nats::connect(self.urls.expose())
            .await
            .with_context(|| format!("failed to connect to nats at {}", self.urls))
    }
}

#[async_trait::async_trait]
impl OutputDestination for NatsOutput {
    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> anyhow::Result<()> {
        if self.client.is_none() {
            // Once a connection has failed, a reconnect is only attempted
            // once the backoff window has passed — otherwise a downed nats
            // server gets a fresh dial on every single batch, at whatever
            // rate the pipeline produces them, which is the "reconnect
            // storm" this exists to prevent.
            let now = Instant::now();
            if !self.gate.ready(now) {
                anyhow::bail!(
                    "nats output at {} is still unreachable; not retrying yet",
                    self.urls
                );
            }
            match self.connect().await {
                Ok(client) => {
                    self.gate.record_success();
                    self.client = Some(client);
                }
                Err(e) => {
                    self.gate.record_failure(now);
                    return Err(e);
                }
            }
        }
        let client = self.client.as_ref().ok_or_else(|| {
            anyhow::anyhow!("nats output is not connected; init() was not called")
        })?;

        for msg in message_batch.iter() {
            let payload =
                serde_json::to_vec(msg).context("failed to serialize message for nats")?;
            if let Err(e) = client
                .publish(self.subject.clone(), Bytes::from(payload))
                .await
                .with_context(|| format!("failed to publish to nats subject '{}'", self.subject))
            {
                // async-nats has already been trying to reconnect internally;
                // dropping the client here just means the next `emit` re-dials
                // through the same gated path rather than trusting a client
                // that has been failing to publish.
                self.client = None;
                self.gate.record_failure(Instant::now());
                return Err(e);
            }
        }
        self.gate.record_success();
        Ok(())
    }

    async fn init(&mut self) -> anyhow::Result<()> {
        self.client = Some(self.connect().await?);
        Ok(())
    }
}
