use crate::{
    BuildCtx,
    backoff::Gate,
    inputs::MessageBatch,
    outputs::{BuildOutput, OutputDestination},
    secrets::Resolved,
};
use anyhow::{Context, Result};
use kayak_core::config::KafkaOutputConfig;
use rdkafka::{
    ClientConfig,
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How long a single record may take to reach the broker. Long enough to ride
/// out a leader election, short enough that a broker that is simply gone fails
/// the batch rather than parking the pipeline forever.
const SEND_TIMEOUT: Duration = Duration::from_secs(30);

impl BuildOutput for KafkaOutputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn OutputDestination>> {
        let cluster = ctx
            .kafka_connection(&self.connection)
            .context("the kafka output cannot be built")?;
        Ok(Box::new(KafkaOutput {
            brokers: ctx.resolve(&cluster.brokers).with_context(|| {
                format!(
                    "failed to resolve secrets in the brokers of connection '{}'",
                    self.connection
                )
            })?,
            topic: self.topic,
            producer: None,
            gate: Gate::new(),
        }))
    }
}

pub struct KafkaOutput {
    brokers: Resolved,
    topic: String,
    producer: Option<FutureProducer>,
    /// Paces retries after a send fails.
    ///
    /// Unlike the other outputs there is no client to drop and rebuild here:
    /// `FutureProducer` connects lazily and reconnects to brokers on its own,
    /// so the producer itself is never the thing that's stale. What the gate
    /// guards is the *send* — `producer.send` still does real work against a
    /// broker it believes is down, up to `SEND_TIMEOUT` each time, so without
    /// this a downed cluster gets hammered with one slow, doomed send per
    /// batch instead of the same backoff every other output gets.
    gate: Gate,
}

#[async_trait::async_trait]
impl OutputDestination for KafkaOutput {
    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> Result<()> {
        // as in the nats and postgres outputs: doing nothing here would look
        // like the records were published
        let producer = self.producer.as_ref().ok_or_else(|| {
            anyhow::anyhow!("kafka output is not connected; init() was not called")
        })?;

        let now = Instant::now();
        if !self.gate.ready(now) {
            anyhow::bail!(
                "kafka output for topic '{}' is still unreachable; not retrying yet",
                self.topic
            );
        }

        for msg in message_batch.iter() {
            let payload =
                serde_json::to_vec(msg).context("failed to serialize message for kafka")?;
            // no key: without one the records round-robin over the partitions,
            // which is the right default when nothing here knows what a
            // partition key would mean for an untyped message
            let record: FutureRecord<'_, (), Vec<u8>> =
                FutureRecord::to(self.topic.as_str()).payload(&payload);
            if let Err((e, _)) = producer.send(record, Timeout::After(SEND_TIMEOUT)).await {
                self.gate.record_failure(now);
                return Err(e)
                    .with_context(|| format!("failed to publish to kafka topic '{}'", self.topic));
            }
        }
        self.gate.record_success();
        Ok(())
    }

    async fn init(&mut self) -> Result<()> {
        self.producer = Some(
            ClientConfig::new()
                .set("bootstrap.servers", self.brokers.expose())
                .create()
                .with_context(|| {
                    format!("failed to create a kafka producer for {}", self.brokers)
                })?,
        );
        Ok(())
    }
}
