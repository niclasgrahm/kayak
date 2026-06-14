use std::sync::Arc;

use crate::{
    BuildCtx,
    inputs::MessageBatch,
    transforms::{BuildTransform, Transform},
};

enum NumericFilterOperatorKind {
    GreaterThan,
    LessThan,
    EqualTo,
}

enum StringFilterOperatorKind {
    EqualTo,
    Contains,
}

enum FilterKind {
    Numeric {
        field: String,
        Operator: NumericFilterOperatorKind,
        value: f64,
    },
    String {
        field: String,
        Operator: StringFilterOperatorKind,
        value: String,
    },
}
pub struct FilterTransformConfig {
    pub filter: FilterKind,
}

impl BuildTransform for FilterTransformConfig {
    fn build(self, ctx: &mut BuildCtx) -> anyhow::Result<Box<dyn Transform>> {
        Ok(Box::new(FilterTransform {
            filter: self.filter,
        }))
    }
}

pub struct FilterTransform {
    pub filter: FilterKind,
}

#[async_trait::async_trait]
impl Transform for FilterTransform {
    async fn apply(
        &mut self,
        message_batch: Arc<MessageBatch>,
    ) -> anyhow::Result<Vec<Arc<MessageBatch>>> {
        todo!()
    }
}
