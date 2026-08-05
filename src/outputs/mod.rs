use anyhow::Result;
use std::sync::Arc;

use crate::{BuildCtx, inputs::MessageBatch};

pub mod file;
pub mod kafka;
pub mod nats;
pub mod postgres;
pub mod stdout;

pub trait BuildOutput {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn OutputDestination>>;
}

#[async_trait::async_trait]
pub trait OutputDestination: Send + 'static {
    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> anyhow::Result<()>;
    async fn init(&mut self) -> Result<()>;
}
