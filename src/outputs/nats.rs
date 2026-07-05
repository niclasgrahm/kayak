use bytes::Bytes;
use std::sync::Arc;
use streamer_core::config::NatsOutputConfig;


use crate::{
    BuildCtx,
    inputs::MessageBatch,
    outputs::{BuildOutput, OutputDestination},
};


impl BuildOutput for NatsOutputConfig {
    fn build(self, _ctx: &mut BuildCtx) -> anyhow::Result<Box<dyn OutputDestination>> {
        Ok(Box::new(NatsOutput {
            urls: self.urls,
            subject: self.subject,
            client: None,
        }))
    }
}

pub struct NatsOutput {
    urls: String,
    subject: String,
    client: Option<async_nats::Client>,
}

#[async_trait::async_trait]
impl OutputDestination for NatsOutput {
    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> anyhow::Result<()> {
        if let Some(c) = &self.client {
            for msg in message_batch.iter() {
                let msg2 = serde_json::to_vec(msg)?;
                c.publish(self.subject.clone(), Bytes::from(msg2)).await?;
            }
        }
        Ok(())
    }

    async fn init(&mut self) -> anyhow::Result<()> {
        self.client = Some(async_nats::connect(&self.urls).await?);
        Ok(())
    }
}
