use std::sync::Arc;

use crate::{BuildCtx, inputs::MessageBatch};

pub mod buffer;
pub mod http;
pub mod reduce;
pub mod splitter;

pub trait BuildTransform {
    fn build(self, ctx: &mut BuildCtx) -> anyhow::Result<Box<dyn Transform>>;
}

#[async_trait::async_trait]
pub trait Transform: Send + 'static {
    async fn apply(
        &mut self,
        message_batch: Arc<MessageBatch>,
    ) -> anyhow::Result<Vec<Arc<MessageBatch>>>;
}
