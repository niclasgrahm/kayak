use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    BuildCtx,
    inputs::MessageBatch,
    transforms::{BuildTransform, Transform},
};

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "buffer")]
pub struct BufferTransformConfig {
    pub size: usize,
}

impl BuildTransform for BufferTransformConfig {
    fn build(self, _ctx: &mut BuildCtx) -> anyhow::Result<Box<dyn Transform>> {
        Ok(Box::new(BufferTransform {
            size: self.size,
            current_batch: MessageBatch::new(),
            buffer: Vec::new(),
        }))
    }
}

pub struct BufferTransform {
    pub size: usize,
    current_batch: MessageBatch,
    buffer: Vec<Arc<MessageBatch>>,
}

#[async_trait::async_trait]
impl Transform for BufferTransform {
    async fn apply(
        &mut self,
        message_batch: Arc<MessageBatch>,
    ) -> anyhow::Result<Vec<Arc<MessageBatch>>> {
        for msg in message_batch.iter() {
            self.current_batch.push(msg.clone());
            if self.current_batch.len() >= self.size {
                let finished = std::mem::take(&mut self.current_batch);
                self.buffer.push(Arc::new(finished));
                self.current_batch = MessageBatch::new();
            }
        }

        Ok(std::mem::take(&mut self.buffer))
    }
}
