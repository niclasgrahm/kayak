use crate::{
    BuildCtx,
    inputs::{BuildInput, InputSource, MessageBatch},
    secrets::Resolved,
};
use anyhow::{Context, Result};
use rdkafka::{
    ClientConfig, Message,
    consumer::{Consumer, StreamConsumer},
};
use std::sync::Arc;
use streamer_core::config::{KafkaConfig, KafkaStartAt};

impl BuildInput for KafkaConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        Ok(Box::new(KafkaInput {
            brokers: ctx
                .resolve(&self.brokers)
                .context("failed to resolve secrets in the kafka input brokers")?,
            topic: self.topic,
            group: self.group,
            start_at: self.start_at.unwrap_or(KafkaStartAt::Latest),
            consumer: None,
        }))
    }
}

pub struct KafkaInput {
    brokers: Resolved,
    topic: String,
    group: String,
    start_at: KafkaStartAt,
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

#[async_trait::async_trait]
impl InputSource for KafkaInput {
    async fn next(&mut self) -> Result<Arc<MessageBatch>> {
        if self.consumer.is_none() {
            self.consumer = Some(self.connect()?);
        }
        let consumer = self
            .consumer
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("kafka consumer not initialized"))?;

        loop {
            // recv() is cancel-safe, which matters: the run loop drops this
            // future every time it checks for cancellation
            let msg = consumer
                .recv()
                .await
                .with_context(|| format!("kafka consumer on '{}' failed", self.topic))?;
            let Some(payload) = msg.payload() else {
                tracing::warn!(
                    "skipping kafka record with no payload on topic '{}'",
                    self.topic
                );
                continue;
            };
            // a single malformed payload shouldn't kill the pipeline; skip it
            // and wait for the next record
            match serde_json::from_slice(payload) {
                Ok(value) => return Ok(Arc::new(vec![Arc::new(value)])),
                Err(e) => tracing::warn!(
                    "skipping non-json record on kafka topic '{}': {}",
                    self.topic,
                    e
                ),
            }
        }
    }
}
