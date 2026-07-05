use std::sync::Arc;
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
        let values: Vec<f64> = message_batch
            .iter()
            .map(|msg| msg.get(&self.field).unwrap().as_f64().unwrap())
            .collect();

        let reduced = match self.function {
            ReduceFnKind::Sum => values.iter().sum::<f64>(),
            ReduceFnKind::Avg => {
                let top = values.iter().sum::<f64>();
                let bottom = values.len() as f64;
                // handle division by zero
                top / bottom
            }
            ReduceFnKind::Min => values.iter().copied().reduce(f64::min).unwrap(),
            ReduceFnKind::Max => values.iter().copied().reduce(f64::max).unwrap(),
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
