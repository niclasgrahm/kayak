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

#[async_trait::async_trait]
impl Transform for FilterTransform {
    async fn apply(
        &mut self,
        message_batch: Arc<MessageBatch>,
    ) -> anyhow::Result<Vec<Arc<MessageBatch>>> {
        let out = message_batch
            .iter()
            .filter(|message| match &self.filter {
                FilterKind::Numeric {
                    field,
                    operator,
                    value,
                } => {
                    let field_value = message.get(field).unwrap().as_f64().unwrap();
                    match operator {
                        NumericFilterOperatorKind::GreaterThan => field_value > *value,
                        NumericFilterOperatorKind::LessThan => field_value < *value,
                        NumericFilterOperatorKind::EqualTo => field_value == *value,
                    }
                }
                FilterKind::String {
                    field,
                    operator,
                    value,
                } => {
                    let field_value = message.get(field).unwrap().as_str().unwrap();
                    match operator {
                        StringFilterOperatorKind::EqualTo => field_value == value,
                        StringFilterOperatorKind::Contains => field_value.contains(value),
                    }
                }
            })
            .cloned()
            .collect();
        Ok(vec![Arc::new(out)])

    }
}
