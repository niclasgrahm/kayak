use crate::{
    BuildCtx,
    inputs::MessageBatch,
    outputs::{BuildOutput, OutputDestination},
    secrets::Resolved,
};
use anyhow::{Context, Result};
use rdkafka::{
    ClientConfig,
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};
use std::sync::Arc;
use std::time::Duration;
use streamer_core::config::KafkaOutputConfig;

/// How long a single record may take to reach the broker. Long enough to ride
/// out a leader election, short enough that a broker that is simply gone fails
/// the batch rather than parking the pipeline forever.
const SEND_TIMEOUT: Duration = Duration::from_secs(30);

impl BuildOutput for KafkaOutputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn OutputDestination>> {
        Ok(Box::new(KafkaOutput {
            brokers: ctx
                .resolve(&self.brokers)
                .context("failed to resolve secrets in the kafka output brokers")?,
            topic: self.topic,
            producer: None,
        }))
    }
}

pub struct KafkaOutput {
    brokers: Resolved,
    topic: String,
    producer: Option<FutureProducer>,
}

#[async_trait::async_trait]
impl OutputDestination for KafkaOutput {
    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> Result<()> {
        // as in the nats and postgres outputs: doing nothing here would look
        // like the records were published
        let producer = self
            .producer
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("kafka output is not connected; init() was not called"))?;

        for msg in message_batch.iter() {
            let payload =
                serde_json::to_vec(msg).context("failed to serialize message for kafka")?;
            // no key: without one the records round-robin over the partitions,
            // which is the right default when nothing here knows what a
            // partition key would mean for an untyped message
            let record: FutureRecord<'_, (), Vec<u8>> =
                FutureRecord::to(self.topic.as_str()).payload(&payload);
            producer
                .send(record, Timeout::After(SEND_TIMEOUT))
                .await
                .map_err(|(e, _)| e)
                .with_context(|| format!("failed to publish to kafka topic '{}'", self.topic))?;
        }
        Ok(())
    }

    async fn init(&mut self) -> Result<()> {
        self.producer = Some(
            ClientConfig::new()
                .set("bootstrap.servers", self.brokers.expose())
                .create()
                .with_context(|| format!("failed to create a kafka producer for {}", self.brokers))?,
        );
        Ok(())
    }
}
