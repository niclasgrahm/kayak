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
use kayak_core::config::{KafkaConfig, KafkaStartAt};
use rdkafka::{
    ClientConfig, Message,
    consumer::{Consumer, StreamConsumer},
};
use std::sync::Arc;

impl BuildInput for KafkaConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        let cluster = ctx
            .kafka_connection(&self.connection)
            .context("the kafka input cannot be built")?;
        Ok(Box::new(KafkaInput {
            brokers: ctx.resolve(&cluster.brokers).with_context(|| {
                format!(
                    "failed to resolve secrets in the brokers of connection '{}'",
                    self.connection
                )
            })?,
            topic: self.topic,
            group: self.group,
            start_at: self.start_at.unwrap_or(KafkaStartAt::Latest),
            // one message per batch unless the config asks for more: batching
            // is an optimisation, never something imposed on a pipeline that
            // wants its messages one at a time
            max_batch: crate::inputs::batch_cap(self.max_batch),
            envelope: ctx.envelope("kafka", Some(&self.connection)),
            consumer: None,
        }))
    }
}

pub struct KafkaInput {
    brokers: Resolved,
    topic: String,
    group: String,
    start_at: KafkaStartAt,
    /// Most messages in one batch. One unless the config says otherwise — see
    /// [`KafkaInput::next`] for what raising it does and, just as importantly,
    /// what it doesn't.
    max_batch: usize,
    /// What this input attaches to each message, if the config asked for any.
    envelope: Envelope,
    /// Built on the first read, like the nats input: `build()` must not block
    /// on a broker that isn't up yet.
    consumer: Option<StreamConsumer>,
}

impl KafkaInput {
    fn connect(&self) -> Result<StreamConsumer> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", self.brokers.expose())
            .set("group.id", &self.group)
            .set(
                "auto.offset.reset",
                match self.start_at {
                    KafkaStartAt::Earliest => "earliest",
                    KafkaStartAt::Latest => "latest",
                },
            )
            // offsets are committed on a timer by the client; at-least-once
            // delivery, which is all the rest of the runtime promises anyway
            .set("enable.auto.commit", "true")
            .create()
            .with_context(|| format!("failed to create a kafka consumer for {}", self.brokers))?;
        consumer
            .subscribe(&[self.topic.as_str()])
            .with_context(|| format!("failed to subscribe to kafka topic '{}'", self.topic))?;
        Ok(consumer)
    }
}

impl KafkaInput {
    /// The next record's JSON, waiting for one. `None` means the record was
    /// skipped — no payload, or not JSON — and the caller should ask again.
    fn decode(&self, msg: &rdkafka::message::BorrowedMessage<'_>) -> Option<serde_json::Value> {
        let Some(payload) = msg.payload() else {
            tracing::warn!(
                "skipping kafka record with no payload on topic '{}'",
                self.topic
            );
            return None;
        };
        // a single malformed payload shouldn't kill the pipeline; skip it and
        // wait for the next record
        let value = match serde_json::from_slice(payload) {
            Ok(value) => value,
            Err(e) => {
                tracing::warn!(
                    "skipping non-json record on kafka topic '{}': {}",
                    self.topic,
                    e
                );
                return None;
            }
        };

        let own = if self.envelope.is_enabled() {
            Self::meta_of(msg)
        } else {
            Vec::new()
        };
        let enveloped = self.envelope.apply(value, own);
        if enveloped.is_none() {
            tracing::warn!(
                "skipping a record on kafka topic '{}': its payload is not a json object, \
                 so a `merge` envelope has nowhere to attach metadata — use a `wrap` \
                 envelope for a topic carrying bare values",
                self.topic
            );
        }
        enveloped
    }

    /// What this input knows about one record. `topic`, `partition` and
    /// `offset` together identify it exactly, which is what makes them worth
    /// carrying: a message that came out wrong can be found again.
    fn meta_of(msg: &rdkafka::message::BorrowedMessage<'_>) -> Meta {
        vec![
            ("topic", Value::String(msg.topic().to_string())),
            ("partition", Value::from(msg.partition())),
            ("offset", Value::from(msg.offset())),
            (
                "key",
                msg.key().map_or(Value::Null, |key| {
                    // a key is bytes, and one that isn't text is better
                    // reported as absent than as replacement characters
                    std::str::from_utf8(key).map_or(Value::Null, |k| Value::String(k.to_string()))
                }),
            ),
            (
                "timestamp",
                msg.timestamp()
                    .to_millis()
                    .and_then(chrono::DateTime::from_timestamp_millis)
                    .map_or(Value::Null, |t| Value::String(t.to_rfc3339())),
            ),
        ]
    }
}

#[async_trait::async_trait]
impl InputSource for KafkaInput {
    /// Wait for one record, then take whatever else is *already* waiting, up to
    /// `max_batch`.
    ///
    /// The second half is `now_or_never` rather than another `await` on purpose,
    /// and it is what makes batching safe to leave on: this never waits for a
    /// batch to fill. A topic producing one message a second yields batches of
    /// one whatever `max_batch` says, so raising it costs an idle pipeline
    /// nothing in latency. It only bites during a catch-up, which is the case it
    /// exists for — and with the default of 1 the drain loop doesn't run at all.
    async fn next(&mut self) -> Result<Arc<MessageBatch>> {
        if self.consumer.is_none() {
            self.consumer = Some(self.connect()?);
        }
        let consumer = self
            .consumer
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("kafka consumer not initialized"))?;

        let mut batch: MessageBatch = Vec::new();
        while batch.is_empty() {
            // recv() is cancel-safe, which matters: the run loop drops this
            // future every time it checks for cancellation
            let msg = consumer
                .recv()
                .await
                .with_context(|| format!("kafka consumer on '{}' failed", self.topic))?;
            if let Some(value) = self.decode(&msg) {
                batch.push(Arc::new(value));
            }
        }

        while batch.len() < self.max_batch {
            // already-buffered records only; the moment the broker has nothing
            // more for us this resolves to None and the batch goes as it is
            let Some(ready) = consumer.recv().now_or_never() else {
                break;
            };
            let msg = ready.with_context(|| format!("kafka consumer on '{}' failed", self.topic))?;
            if let Some(value) = self.decode(&msg) {
                batch.push(Arc::new(value));
            }
        }

        Ok(Arc::new(batch))
    }
}
