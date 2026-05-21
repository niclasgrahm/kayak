use crate::config::NatsInputConfig;
use crate::inputs::{InputSource, MessageBatch};
use anyhow::Result;
use tokio_stream::StreamExt;

use serde_json::Value;
use std::sync::Arc;

pub struct NatsInput {
    pub cfg: NatsInputConfig,
    pub sub: Option<async_nats::Subscriber>,
}

#[async_trait::async_trait]
impl InputSource for NatsInput {
    async fn next(&mut self) -> Result<MessageBatch> {
        if self.sub.is_none() {
            let client = async_nats::connect(&self.cfg.urls)
                .await
                .expect("failed to connect to nats");
            let subscriber = client
                .subscribe(self.cfg.subject.clone())
                .await
                .expect("failed to subscribe to nats subject");
            self.sub = Some(subscriber);
        }
        let subscriber = self.sub.as_mut().unwrap();
        let msg = subscriber.next().await.expect("sub ended");
        let value = serde_json::from_slice(&msg.payload).expect("we assume json");
        Ok(vec![Arc::new(value)])
    }
}
