use std::sync::Arc;
use anyhow::Context;
use streamer_core::config::{ReduceFnKind, ReduceTransformConfig};

use serde::{Deserialize, Serialize};

use crate::{
    BuildCtx,
    inputs::MessageBatch,
    transforms::{BuildTransform, Transform},
};

impl BuildTransform for ReduceTransformConfig {
    fn build(self, _ctx: &mut BuildCtx) -> anyhow::Result<Box<dyn Transform>> {
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
        // an empty batch is normal — a tumbling window can close without ever
        // receiving a message. There is nothing to reduce, so emit nothing
        // rather than inventing a 0 / NaN.
        if message_batch.is_empty() {
            return Ok(vec![]);
        }

        let values: Vec<f64> = message_batch
            .iter()
            .map(|msg| {
                msg.get(&self.field)
                    .and_then(|v| v.as_f64())
                    .with_context(|| format!("field '{}' missing or not numeric", self.field))
            })
            .collect::<anyhow::Result<_>>()?;

        // every branch below relies on values being non-empty, which the
        // guard above guarantees
        let reduced = match self.function {
            ReduceFnKind::Sum => values.iter().sum::<f64>(),
            ReduceFnKind::Avg => values.iter().sum::<f64>() / values.len() as f64,
            ReduceFnKind::Min => values.iter().copied().fold(f64::INFINITY, f64::min),
            ReduceFnKind::Max => values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        };

        let msg = serde_json::to_value(&ReduceOutputFormat {
            original_field: self.field.clone(),
            reduced_value: reduced,
        })?;

        let batch: MessageBatch = vec![Arc::new(msg)];
        Ok(vec![Arc::new(batch)])
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ReduceOutputFormat {
    original_field: String,
    reduced_value: f64,
}
