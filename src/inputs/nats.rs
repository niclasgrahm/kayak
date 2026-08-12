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
use serde_json::Value;
use anyhow::{Context, Result};
use futures_util::FutureExt;
use tokio_stream::StreamExt;

use std::sync::Arc;
use tokio::sync::broadcast;

use kayak_core::{Stage, config::NatsConfig};

impl BuildInput for NatsConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        // core nats has no ack of any kind — a message not read is simply
        // gone, with no redelivery possible — so there is nothing `on_delivery`
        // could mean here. JetStream would be the thing to build against
        // instead, and is a different connection kind, not a mode of this one.
        ack::require_receipt_only(ctx.ack_mode(), "nats")?;
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
            envelope: ctx.envelope("nats", Some(&self.connection)),
            sub: None,
            pipeline_id: ctx.pipeline_id.clone(),
            events: ctx.events.clone(),
            backoff: Backoff::new(),
        }))
    }
}

pub struct NatsInput {
    pub urls: Resolved,
    pub subject: String,
    /// Most messages in one batch. One unless the config says otherwise.
    pub max_batch: usize,
    /// What this input attaches to each message, if the config asked for any.
    pub envelope: Envelope,
    pub sub: Option<async_nats::Subscriber>,
    pub pipeline_id: PipelineId,
    pub events: broadcast::Sender<UiEvent>,
    /// How long to wait before the next reconnect attempt once the broker
    /// drops — see [`NatsInput::next`] for where it's consulted. Reset on
    /// every successful connect, so a second outage starts the schedule
    /// over rather than picking up where a much earlier one left off.
    backoff: Backoff,
}

/// A nats message's headers as JSON: an object of arrays, because a header may
/// legitimately appear more than once and collapsing that would lose it.
fn headers_of(message: &async_nats::Message) -> Value {
    let Some(headers) = &message.headers else {
        return Value::Object(serde_json::Map::new());
    };
    let mut out = serde_json::Map::new();
    for (name, values) in headers.iter() {
        out.insert(
            name.to_string(),
            Value::Array(
                values
                    .iter()
                    .map(|v| Value::String(v.to_string()))
                    .collect(),
            ),
        );
    }
    Value::Object(out)
}

/// What this input knows about one message: the concrete subject it arrived on
/// — which is the whole reason a wildcard subscription is worth anything — plus
/// the reply subject and headers.
fn meta_of(message: &async_nats::Message) -> Meta {
    vec![
        ("subject", Value::String(message.subject.to_string())),
        (
            "reply",
            message
                .reply
                .as_ref()
                .map_or(Value::Null, |r| Value::String(r.to_string())),
        ),
        ("headers", headers_of(message)),
    ]
}

impl NatsInput {
    /// Connects and subscribes, or reports why it couldn't and waits before
    /// trying again — never gives up, since a broker coming back after
    /// `docker compose down` is exactly the case this exists for. Reconnect
    /// attempts are paced by [`Backoff`] rather than firing at whatever rate
    /// `next` is being called, which is what keeps a downed nats server from
    /// being hammered on every pass of a fast pipeline.
    ///
    /// One [`UiEvent::error`] per outage, on the attempt that starts it —
    /// not one per retry, the same "warn once, not per message" rule the
    /// state buckets follow — and a log line on the attempt that ends it.
    async fn reconnect(&mut self) -> async_nats::Subscriber {
        loop {
            match self.try_connect().await {
                Ok(sub) => {
                    if self.backoff.is_failing() {
                        tracing::info!(
                            "nats input reconnected to subject '{}' at {}",
                            self.subject,
                            self.urls
                        );
                    }
                    self.backoff.succeeded();
                    return sub;
                }
                Err(e) => {
                    if !self.backoff.is_failing() {
                        tracing::error!(
                            "nats input on subject '{}' lost its connection, retrying: {e:?}",
                            self.subject
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

    async fn try_connect(&self) -> Result<async_nats::Subscriber> {
        let client = async_nats::connect(self.urls.expose())
            .await
            .with_context(|| format!("failed to connect to nats at {}", self.urls))?;
        client
            .subscribe(self.subject.clone())
            .await
            .with_context(|| format!("failed to subscribe to nats subject '{}'", self.subject))
    }
}

#[async_trait::async_trait]
impl InputSource for NatsInput {
    async fn next(&mut self) -> Result<Delivery> {
        // The outer loop is the reconnect path: a subscription ending mid-read
        // clears `self.sub` and goes round again rather than returning an
        // error, so a broker outage costs this input a wait, not its life.
        loop {
            if self.sub.is_none() {
                self.sub = Some(self.reconnect().await);
            }
            let subscriber = self
                .sub
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("nats subscriber not initialized"))?;

            // a single malformed payload shouldn't kill the pipeline; skip it and
            // wait for the next message. The envelope skips for the same reason and
            // is reported the same way — a `merge` envelope over a bare number has
            // nowhere to put its field, which is a message this pipeline cannot
            // read rather than a pipeline that is misconfigured.
            let subject = &self.subject;
            let envelope = &self.envelope;
            let decode = move |message: &async_nats::Message| {
                let value = match serde_json::from_slice::<Value>(&message.payload) {
                    Ok(value) => value,
                    Err(e) => {
                        tracing::warn!(
                            "skipping non-json message on nats subject '{subject}': {e}"
                        );
                        return None;
                    }
                };
                let own = if envelope.is_enabled() {
                    meta_of(message)
                } else {
                    Vec::new()
                };
                let enveloped = envelope.apply(value, own);
                if enveloped.is_none() {
                    tracing::warn!(
                        "skipping a message on nats subject '{subject}': its payload is not a \
                         json object, so a `merge` envelope has nowhere to attach metadata — \
                         use a `wrap` envelope for a subject carrying bare values"
                    );
                }
                enveloped
            };

            let mut batch: MessageBatch = Vec::new();
            let mut subscription_ended = false;
            while batch.is_empty() {
                if let Some(msg) = subscriber.next().await {
                    if let Some(value) = decode(&msg) {
                        batch.push(Arc::new(value));
                    }
                } else {
                    subscription_ended = true;
                    break;
                }
            }
            if subscription_ended {
                self.sub = None;
                continue;
            }

            // Whatever else has *already* arrived, up to the cap — never a wait
            // for one to fill, so a quiet subject still yields batches of one
            // however high `max_batch` is. With the default of 1 this loop
            // never runs. A subscription ending here doesn't cost the batch
            // already collected — it's returned now, and the reconnect happens
            // on the next call.
            while batch.len() < self.max_batch {
                let Some(ready) = subscriber.next().now_or_never() else {
                    break;
                };
                if let Some(msg) = ready {
                    if let Some(value) = decode(&msg) {
                        batch.push(Arc::new(value));
                    }
                } else {
                    self.sub = None;
                    break;
                }
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
            "bus".to_string(),
            ConnectionKind::Nats(kayak_core::connections::NatsConnection {
                urls: "nats://localhost:4222".into(),
            }),
        )]
        .into_iter()
        .collect();
        let mut ctx = BuildCtx::new(&mut pipelines, "p".to_string(), events)
            .with_connections(Arc::new(connections));
        ctx.ack_mode = ack_mode;
        NatsConfig {
            connection: "bus".to_string(),
            subject: "test.subject".to_string(),
            max_batch: None,
        }
        .build(&mut ctx)
    }

    /// The default, and the only mode core nats has ever supported: nothing
    /// to refuse.
    #[test]
    fn absent_and_on_receipt_both_build() {
        assert!(build(None).is_ok());
        assert!(build(Some(AckMode::OnReceipt)).is_ok());
    }

    /// Core nats has no broker-side "received" vs "delivered" — a message not
    /// read is simply gone — so `on_delivery` is refused rather than silently
    /// behaving like `on_receipt`.
    #[test]
    fn on_delivery_is_refused() {
        let Err(err) = build(Some(AckMode::OnDelivery)) else {
            panic!("a nats input built with `ack: on_delivery`, which it cannot honour");
        };
        assert!(format!("{err:#}").contains("nats"), "{err:#}");
    }
}
