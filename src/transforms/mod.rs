use std::sync::Arc;

use crate::{BuildCtx, inputs::MessageBatch};

pub mod buffer;
pub mod filter;
pub mod http;
pub mod map;
pub mod reduce;
pub mod script;
pub mod splitter;
pub mod state;

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
