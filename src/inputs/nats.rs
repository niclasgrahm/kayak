use crate::{
    BuildCtx,
    inputs::{
        BuildInput, InputSource, MessageBatch,
        envelope::{Envelope, Meta},
    },
    secrets::Resolved,
};
use serde_json::Value;
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
            envelope: ctx.envelope("nats", Some(&self.connection)),
            sub: None,
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
                    tracing::warn!("skipping non-json message on nats subject '{subject}': {e}");
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
        while batch.is_empty() {
            let msg = subscriber
                .next()
                .await
                .ok_or_else(|| anyhow::anyhow!("nats subscription on '{}' ended", self.subject))?;
            if let Some(value) = decode(&msg) {
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
            if let Some(value) = decode(&msg) {
                batch.push(Arc::new(value));
            }
        }

        Ok(Arc::new(batch))
    }
}
