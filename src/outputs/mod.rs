use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::inputs::MessageBatch;
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutputKind {
    Stdout,
}

#[async_trait::async_trait]
pub trait OutputDestination: Send + 'static {
    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> anyhow::Result<()>;
}

pub struct StdoutOutput {}

#[async_trait::async_trait]
impl OutputDestination for StdoutOutput {
    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> anyhow::Result<()> {
        println!("{}", serde_json::to_string_pretty(&message_batch).unwrap());
        Ok(())
    }
}
