use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    BuildCtx,
    inputs::MessageBatch,
    transforms::{BuildTransform, Transform},
};

// takes a batch, emits each message as a separate batch
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "splitter")]
pub struct SplitterTransformConfig {
    out_size: usize,
}
impl BuildTransform for SplitterTransformConfig {
    fn build(self, _ctx: &mut BuildCtx) -> anyhow::Result<Box<dyn Transform>> {
        Ok(Box::new(SplitterTransform {
            out_size: self.out_size,
        }))
    }
}

pub struct SplitterTransform {
    out_size: usize,
}

#[async_trait::async_trait]
impl Transform for SplitterTransform {
    async fn apply(
        &mut self,
        message_batch: Arc<MessageBatch>,
    ) -> anyhow::Result<Vec<Arc<MessageBatch>>> {
        let mut outer = vec![];
        let mut inner = vec![];
        for msg in message_batch.iter() {
            inner.push(msg.clone());
            if inner.len() >= self.out_size {
                outer.push(Arc::new(inner));
                inner = vec![];
            }
        }
        // TODO: theres stuff left here
        Ok(outer)
    }
}
