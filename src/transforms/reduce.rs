use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{
    BuildCtx,
    inputs::MessageBatch,
    transforms::{BuildTransform, Transform},
};

#[derive(Clone, Debug, Deserialize, Serialize)]
enum ReduceFnKind {
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReduceTransformConfig {
    function: ReduceFnKind,
    field: String,
}
impl BuildTransform for ReduceTransformConfig {
    fn build(self, ctx: &mut BuildCtx) -> anyhow::Result<Box<dyn Transform>> {
        Ok(Box::new(ReduceTransform {
            function: self.function,
            field: self.field,
        }))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReduceTransform {
    function: ReduceFnKind,
    field: String,
}
#[async_trait::async_trait]
impl Transform for ReduceTransform {
    async fn apply(
        &mut self,
        message_batch: Arc<MessageBatch>,
    ) -> anyhow::Result<Vec<Arc<MessageBatch>>> {
        todo!()
    }
}
