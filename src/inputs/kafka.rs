use crate::{
    BuildCtx,
    inputs::{
        BuildInput, InputSource, MessageBatch,
        ack::{Ack, Delivery},
        envelope::{Envelope, Meta},
    },
    secrets::Resolved,
};
use serde_json::Value;
use anyhow::{Context, Result};
use futures_util::FutureExt;
use kayak_core::config::{AckMode, KafkaConfig, KafkaStartAt};
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
            ack_mode: ctx.ack_mode(),
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
    /// `on_receipt` (the default) or `on_delivery` — decides both the client
    /// config `connect` builds and what `next` hands back as an [`Ack`]. See
    /// the `ack` module docs for what the two modes mean and their scope.
    ack_mode: AckMode,
    /// Built on the first read, like the nats input: `build()` must not block
    /// on a broker that isn't up yet. Shared with any [`KafkaAck`] this input
    /// hands out, since acknowledging one has to reach back into the same
    /// consumer to store an offset.
    consumer: Option<Arc<StreamConsumer>>,
}

impl KafkaInput {
    /// The client config `connect` builds against, pulled apart from the
    /// actual connection so the `ack_mode` → settings mapping is testable
    /// without a broker to create a consumer against.
    fn client_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", self.brokers.expose())
            .set("group.id", &self.group)
            .set(
                "auto.offset.reset",
                match self.start_at {
                    KafkaStartAt::Earliest => "earliest",
                    KafkaStartAt::Latest => "latest",
                },
            )
            // offsets are always committed on a timer by the client; what
            // `ack_mode` decides is *which* offset that timer sees. On
            // receipt the client stores (and so commits) whatever it just
            // handed us, before this pipeline has done anything with it. On
            // delivery, storing is turned off here and done explicitly by
            // `KafkaAck::ack` once the batch has cleared this pipeline — see
            // the `ack` module docs.
            .set("enable.auto.commit", "true");
        if self.ack_mode == AckMode::OnDelivery {
            config.set("enable.auto.offset.store", "false");
        }
        config
    }

    fn connect(&self) -> Result<StreamConsumer> {
        let consumer: StreamConsumer = self
            .client_config()
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

/// [`Ack`] for `AckMode::OnDelivery`: stores every record's offset once the
/// run loop says the batch has cleared this pipeline, so the client's
/// background commit picks it up on its next tick.
///
/// Every record this delivery carried gets an entry — including ones
/// `decode` skipped as unparseable, since those are handled too, just not
/// forwarded — otherwise a malformed record would be re-read and re-skipped
/// forever rather than being passed over exactly once. Storing an offset
/// lower than one already stored is harmless (librdkafka keeps the highest),
/// so there's no need to collapse this to one entry per partition.
struct KafkaAck {
    consumer: Arc<StreamConsumer>,
    topic: String,
    /// (partition, next offset to read) pairs — one past the record actually
    /// read, which is the convention `store_offset`/`commit` both expect.
    offsets: Vec<(i32, i64)>,
}

impl Ack for KafkaAck {
    fn ack(&self) {
        for &(partition, offset) in &self.offsets {
            if let Err(e) = self.consumer.store_offset(&self.topic, partition, offset) {
                tracing::warn!(
                    "failed to store kafka offset for '{}'/{}@{}: {}",
                    self.topic,
                    partition,
                    offset,
                    e
                );
            }
        }
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
    async fn next(&mut self) -> Result<Delivery> {
        if self.consumer.is_none() {
            self.consumer = Some(Arc::new(self.connect()?));
        }
        let consumer = self
            .consumer
            .clone()
            .ok_or_else(|| anyhow::anyhow!("kafka consumer not initialized"))?;

        let mut batch: MessageBatch = Vec::new();
        let mut offsets: Vec<(i32, i64)> = Vec::new();
        while batch.is_empty() {
            // recv() is cancel-safe, which matters: the run loop drops this
            // future every time it checks for cancellation
            let msg = consumer
                .recv()
                .await
                .with_context(|| format!("kafka consumer on '{}' failed", self.topic))?;
            offsets.push((msg.partition(), msg.offset() + 1));
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
            offsets.push((msg.partition(), msg.offset() + 1));
            if let Some(value) = self.decode(&msg) {
                batch.push(Arc::new(value));
            }
        }

        let batch = Arc::new(batch);
        Ok(match self.ack_mode {
            AckMode::OnReceipt => Delivery::new(batch),
            AckMode::OnDelivery => Delivery::with_ack(
                batch,
                Box::new(KafkaAck {
                    consumer,
                    topic: self.topic.clone(),
                    offsets,
                }) as Box<dyn Ack>,
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inputs::envelope::Envelope;
    use crate::testing::MapSecretStore;

    fn input(ack_mode: AckMode) -> KafkaInput {
        let brokers = match crate::secrets::resolve(&"localhost:9092".into(), &MapSecretStore::empty())
        {
            Ok(brokers) => brokers,
            Err(e) => panic!("resolving a secret-free broker string should not fail: {e:#}"),
        };
        KafkaInput {
            brokers,
            topic: "test.events".to_string(),
            group: "kayak".to_string(),
            start_at: KafkaStartAt::Latest,
            max_batch: 1,
            envelope: Envelope::none(),
            ack_mode,
            consumer: None,
        }
    }

    /// `on_receipt` is what this input has always done: the client stores and
    /// commits an offset the moment it hands the record over, with no help
    /// from `KafkaAck` needed.
    #[test]
    fn on_receipt_leaves_offset_storing_to_the_client() {
        let config = input(AckMode::OnReceipt).client_config();
        assert_eq!(config.get("enable.auto.commit"), Some("true"));
        assert_eq!(config.get("enable.auto.offset.store"), None);
    }

    /// `on_delivery` turns automatic offset storing off, which is what makes
    /// `KafkaAck::ack`'s explicit `store_offset` the only thing that advances
    /// the committed offset.
    #[test]
    fn on_delivery_turns_off_automatic_offset_storing() {
        let config = input(AckMode::OnDelivery).client_config();
        assert_eq!(config.get("enable.auto.commit"), Some("true"));
        assert_eq!(config.get("enable.auto.offset.store"), Some("false"));
    }
}
