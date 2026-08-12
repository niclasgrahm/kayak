use anyhow::Context;
use kayak_core::config::RedisOutputConfig;
use std::sync::Arc;
use std::time::Instant;

use crate::{
    BuildCtx,
    backoff::Gate,
    inputs::MessageBatch,
    outputs::{BuildOutput, OutputDestination},
    secrets::Resolved,
};

impl BuildOutput for RedisOutputConfig {
    fn build(self, ctx: &mut BuildCtx) -> anyhow::Result<Box<dyn OutputDestination>> {
        let server = ctx
            .redis_connection(&self.connection)
            .context("the redis output cannot be built")?;
        Ok(Box::new(RedisOutput {
            url: ctx.resolve(&server.url).with_context(|| {
                format!(
                    "failed to resolve secrets in the url of connection '{}'",
                    self.connection
                )
            })?,
            channel: self.channel,
            connection: None,
            gate: Gate::new(),
        }))
    }
}

pub struct RedisOutput {
    url: Resolved,
    channel: String,
    connection: Option<::redis::aio::MultiplexedConnection>,
    /// Paces reconnect attempts after the connection is lost — see `emit`.
    /// `init` never consults it: a pipeline that can't reach its output at
    /// startup still fails to build, same as always.
    gate: Gate,
}

impl RedisOutput {
    async fn connect(&self) -> anyhow::Result<::redis::aio::MultiplexedConnection> {
        let client = ::redis::Client::open(self.url.expose())
            .with_context(|| format!("failed to build a redis client for {}", self.url))?;
        client
            .get_multiplexed_async_connection()
            .await
            .with_context(|| format!("failed to connect to redis at {}", self.url))
    }
}

#[async_trait::async_trait]
impl OutputDestination for RedisOutput {
    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> anyhow::Result<()> {
        if self.connection.is_none() {
            // Once a connection has failed, a reconnect is only attempted
            // once the backoff window has passed — otherwise a downed redis
            // server gets a fresh dial on every single batch, at whatever
            // rate the pipeline produces them, which is the "reconnect
            // storm" this exists to prevent.
            let now = Instant::now();
            if !self.gate.ready(now) {
                anyhow::bail!(
                    "redis output at {} is still unreachable; not retrying yet",
                    self.url
                );
            }
            match self.connect().await {
                Ok(connection) => {
                    self.gate.record_success();
                    self.connection = Some(connection);
                }
                Err(e) => {
                    self.gate.record_failure(now);
                    return Err(e);
                }
            }
        }
        let connection = self.connection.as_mut().ok_or_else(|| {
            anyhow::anyhow!("redis output is not connected; init() was not called")
        })?;

        for msg in message_batch.iter() {
            let payload =
                serde_json::to_vec(msg).context("failed to serialize message for redis")?;
            if let Err(e) = ::redis::AsyncCommands::publish::<_, _, ()>(
                connection,
                self.channel.clone(),
                payload,
            )
            .await
            .with_context(|| format!("failed to publish to redis channel '{}'", self.channel))
            {
                // dropping the connection here means the next `emit` re-dials
                // through the same gated path rather than trusting a
                // connection that has been failing to publish.
                self.connection = None;
                self.gate.record_failure(Instant::now());
                return Err(e);
            }
        }
        self.gate.record_success();
        Ok(())
    }

    async fn init(&mut self) -> anyhow::Result<()> {
        self.connection = Some(self.connect().await?);
        Ok(())
    }
}
