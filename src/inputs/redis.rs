use crate::{
    BuildCtx,
    backoff::Backoff,
    events::publish,
    inputs::{
        BuildInput, InputSource, MessageBatch,
        ack::{self, Delivery},
        envelope::{Envelope, Meta},
    },
    secrets::Resolved,
    state::{PipelineId, UiEvent},
};
use anyhow::{Context, Result};
use futures_util::FutureExt;
use kayak_core::{Stage, config::RedisConfig};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::StreamExt;

impl BuildInput for RedisConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        // redis pub/sub has no broker-side notion of "received" vs
        // "delivered" — a client that isn't subscribed simply misses whatever
        // was published — so there is nothing `on_delivery` could mean here,
        // the same rule the nats input follows.
        ack::require_receipt_only(ctx.ack_mode(), "redis")?;
        let server = ctx
            .redis_connection(&self.connection)
            .context("the redis input cannot be built")?;
        let url = ctx.resolve(&server.url).with_context(|| {
            format!(
                "failed to resolve secrets in the url of connection '{}'",
                self.connection
            )
        })?;
        let envelope = ctx.envelope("redis", Some(&self.connection));
        Ok(Box::new(RedisInput {
            url,
            channel: self.channel,
            connection_name: self.connection,
            // one message per batch unless the config asks for more — see
            // `RedisInput::next`
            max_batch: crate::inputs::batch_cap(self.max_batch),
            envelope,
            pubsub: None,
            pipeline_id: ctx.pipeline_id.clone(),
            events: ctx.events.clone(),
            backoff: Backoff::new(),
        }))
    }
}

pub struct RedisInput {
    pub url: Resolved,
    pub channel: String,
    pub connection_name: String,
    /// Most messages in one batch. One unless the config says otherwise.
    pub max_batch: usize,
    /// What this input attaches to each message, if the config asked for any.
    pub envelope: Envelope,
    pub pubsub: Option<::redis::aio::PubSub>,
    pub pipeline_id: PipelineId,
    pub events: broadcast::Sender<UiEvent>,
    /// How long to wait before the next reconnect attempt once the broker
    /// drops — see [`RedisInput::next`] for where it's consulted. Reset on
    /// every successful connect, so a second outage starts the schedule over
    /// rather than picking up where a much earlier one left off.
    backoff: Backoff,
}

/// What this input knows about one message: the channel it arrived on, plus
/// which connection it came through.
fn meta_of(connection_name: &str, channel: &str) -> Meta {
    vec![
        ("connection", Value::String(connection_name.to_string())),
        ("channel", Value::String(channel.to_string())),
    ]
}

impl RedisInput {
    /// Connects and subscribes, or reports why it couldn't and waits before
    /// trying again — never gives up, since a broker coming back after
    /// `docker compose down` is exactly the case this exists for. Reconnect
    /// attempts are paced by [`Backoff`] rather than firing at whatever rate
    /// `next` is being called, which is what keeps a downed redis server from
    /// being hammered on every pass of a fast pipeline.
    async fn reconnect(&mut self) -> ::redis::aio::PubSub {
        loop {
            match self.try_connect().await {
                Ok(pubsub) => {
                    if self.backoff.is_failing() {
                        tracing::info!(
                            "redis input reconnected to channel '{}' at {}",
                            self.channel,
                            self.url
                        );
                    }
                    self.backoff.succeeded();
                    return pubsub;
                }
                Err(e) => {
                    if !self.backoff.is_failing() {
                        tracing::error!(
                            "redis input on channel '{}' lost its connection, retrying: {e:?}",
                            self.channel
                        );
                        publish(&self.events, || {
                            UiEvent::error(self.pipeline_id.clone(), Stage::Input, &e)
                        });
                    }
                    tokio::time::sleep(self.backoff.failed()).await;
                }
            }
        }
    }

    async fn try_connect(&self) -> Result<::redis::aio::PubSub> {
        let client = ::redis::Client::open(self.url.expose())
            .with_context(|| format!("failed to build a redis client for {}", self.url))?;
        let mut pubsub = client
            .get_async_pubsub()
            .await
            .with_context(|| format!("failed to connect to redis at {}", self.url))?;
        pubsub
            .subscribe(&self.channel)
            .await
            .with_context(|| format!("failed to subscribe to redis channel '{}'", self.channel))?;
        Ok(pubsub)
    }
}

#[async_trait::async_trait]
impl InputSource for RedisInput {
    async fn next(&mut self) -> Result<Delivery> {
        // The outer loop is the reconnect path: a subscription ending mid-read
        // clears `self.pubsub` and goes round again rather than returning an
        // error, so a broker outage costs this input a wait, not its life.
        loop {
            if self.pubsub.is_none() {
                self.pubsub = Some(self.reconnect().await);
            }
            let pubsub = self
                .pubsub
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("redis pubsub not initialized"))?;

            // a single malformed payload shouldn't kill the pipeline; skip it
            // and wait for the next message. The envelope skips for the same
            // reason and is reported the same way — see the nats input's
            // `decode` for the fuller version of this comment.
            let channel = &self.channel;
            let connection_name = &self.connection_name;
            let envelope = &self.envelope;
            let decode = move |msg: &::redis::Msg| -> Option<Value> {
                let value = match serde_json::from_slice::<Value>(msg.get_payload_bytes()) {
                    Ok(value) => value,
                    Err(e) => {
                        tracing::warn!(
                            "skipping non-json message on redis channel '{channel}': {e}"
                        );
                        return None;
                    }
                };
                let own = if envelope.is_enabled() {
                    meta_of(connection_name, channel)
                } else {
                    Vec::new()
                };
                let enveloped = envelope.apply(value, own);
                if enveloped.is_none() {
                    tracing::warn!(
                        "skipping a message on redis channel '{channel}': its payload is not a \
                         json object, so a `merge` envelope has nowhere to attach metadata — \
                         use a `wrap` envelope for a channel carrying bare values"
                    );
                }
                enveloped
            };

            let mut batch: MessageBatch = Vec::new();
            let mut subscription_ended = false;
            {
                let mut stream = pubsub.on_message();
                while batch.is_empty() {
                    if let Some(msg) = stream.next().await {
                        if let Some(value) = decode(&msg) {
                            batch.push(Arc::new(value));
                        }
                    } else {
                        subscription_ended = true;
                        break;
                    }
                }
            }
            if subscription_ended {
                self.pubsub = None;
                continue;
            }

            // Whatever else has *already* arrived, up to the cap — never a
            // wait for one to fill, so a quiet channel still yields batches
            // of one however high `max_batch` is. With the default of 1 this
            // loop never runs. A subscription ending here doesn't cost the
            // batch already collected — it's returned now, and the reconnect
            // happens on the next call.
            let mut lost_after_batch = false;
            {
                let mut stream = pubsub.on_message();
                while batch.len() < self.max_batch {
                    let Some(ready) = stream.next().now_or_never() else {
                        break;
                    };
                    if let Some(msg) = ready {
                        if let Some(value) = decode(&msg) {
                            batch.push(Arc::new(value));
                        }
                    } else {
                        lost_after_batch = true;
                        break;
                    }
                }
            }
            if lost_after_batch {
                self.pubsub = None;
            }

            return Ok(Delivery::new(Arc::new(batch)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kayak_core::config::AckMode;
    use kayak_core::connections::{ConnectionKind, Connections};
    use std::collections::HashMap;

    fn build(ack_mode: Option<AckMode>) -> Result<Box<dyn InputSource>> {
        let mut pipelines = HashMap::new();
        let (events, _rx) = tokio::sync::broadcast::channel(4);
        let connections: Connections = [(
            "cache".to_string(),
            ConnectionKind::Redis(kayak_core::connections::RedisConnection {
                url: "redis://localhost:6379".into(),
            }),
        )]
        .into_iter()
        .collect();
        let mut ctx = BuildCtx::new(&mut pipelines, "p".to_string(), events)
            .with_connections(Arc::new(connections));
        ctx.ack_mode = ack_mode;
        RedisConfig {
            connection: "cache".to_string(),
            channel: "test.channel".to_string(),
            max_batch: None,
        }
        .build(&mut ctx)
    }

    /// The default, and the only mode redis pub/sub has ever supported:
    /// nothing to refuse.
    #[test]
    fn absent_and_on_receipt_both_build() {
        assert!(build(None).is_ok());
        assert!(build(Some(AckMode::OnReceipt)).is_ok());
    }

    /// Redis pub/sub has no broker-side "received" vs "delivered" — a message
    /// not read is simply gone — so `on_delivery` is refused rather than
    /// silently behaving like `on_receipt`.
    #[test]
    fn on_delivery_is_refused() {
        let Err(err) = build(Some(AckMode::OnDelivery)) else {
            panic!("a redis input built with `ack: on_delivery`, which it cannot honour");
        };
        assert!(format!("{err:#}").contains("redis"), "{err:#}");
    }
}
