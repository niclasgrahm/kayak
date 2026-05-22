use std::sync::Arc;

use crate::{inputs::MessageBatch, outputs::OutputDestination};
use anyhow::Result;

pub struct StdoutOutput {}

#[async_trait::async_trait]
impl OutputDestination for StdoutOutput {
    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> anyhow::Result<()> {
        println!("{}", serde_json::to_string_pretty(&message_batch).unwrap());
        Ok(())
    }
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }
}
