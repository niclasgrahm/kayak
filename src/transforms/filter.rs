use std::sync::Arc;

use streamer_core::config::FilterTransformConfig;
use streamer_core::config::FilterKind;
use streamer_core::config::NumericFilterOperatorKind;
use streamer_core::config::StringFilterOperatorKind;

use crate::{
    BuildCtx,
    inputs::MessageBatch,
    transforms::{BuildTransform, Transform},
};

impl BuildTransform for FilterTransformConfig {
    fn build(self, _ctx: &mut BuildCtx) -> anyhow::Result<Box<dyn Transform>> {
        Ok(Box::new(FilterTransform {
            filter: self.filter,
        }))
    }
}

pub struct FilterTransform {
    filter: FilterKind,
}

impl FilterTransform {
    /// A message that doesn't carry the field, or carries it with the wrong
    /// type, simply doesn't match — it can't satisfy the predicate. We warn
    /// rather than error out so one odd message doesn't stop the pipeline.
    fn matches(&self, message: &serde_json::Value) -> bool {
        match &self.filter {
            FilterKind::Numeric {
                field,
                operator,
                value,
            } => {
                let Some(field_value) = message.get(field).and_then(serde_json::Value::as_f64)
                else {
                    tracing::warn!(
                        "filter: field '{field}' missing or not a number; dropping message"
                    );
                    return false;
                };
                match operator {
                    NumericFilterOperatorKind::GreaterThan => field_value > *value,
                    NumericFilterOperatorKind::LessThan => field_value < *value,
                    NumericFilterOperatorKind::EqualTo => {
                        (field_value - *value).abs() < f64::EPSILON
                    }
                }
            }
            FilterKind::String {
                field,
                operator,
                value,
            } => {
                let Some(field_value) = message.get(field).and_then(serde_json::Value::as_str)
                else {
                    tracing::warn!(
                        "filter: field '{field}' missing or not a string; dropping message"
                    );
                    return false;
                };
                match operator {
                    StringFilterOperatorKind::EqualTo => field_value == value,
                    StringFilterOperatorKind::Contains => field_value.contains(value),
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Transform for FilterTransform {
    async fn apply(
        &mut self,
        message_batch: Arc<MessageBatch>,
    ) -> anyhow::Result<Vec<Arc<MessageBatch>>> {
        let out: MessageBatch = message_batch
            .iter()
            .filter(|message| self.matches(message))
            .cloned()
            .collect();

        // an empty batch carries no information downstream, and would make
        // e.g. a reducer produce a meaningless result
        if out.is_empty() {
            return Ok(vec![]);
        }
        Ok(vec![Arc::new(out)])
    }
}
