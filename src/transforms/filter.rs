use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    BuildCtx,
    inputs::MessageBatch,
    transforms::{BuildTransform, Transform},
};

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
enum NumericFilterOperatorKind {
    GreaterThan,
    LessThan,
    EqualTo,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
enum StringFilterOperatorKind {
    EqualTo,
    Contains,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
enum FilterKind {
    Numeric {
        /// the field to filter on
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
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "filter")]
pub struct FilterTransformConfig {
    #[serde(flatten)]
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
        let out = message_batch
            .iter()
            .filter(|message| match &self.filter {
                FilterKind::Numeric {
                    field,
                    Operator,
                    value,
                } => {
                    let field_value = message.get(field).unwrap().as_f64().unwrap();
                    match Operator {
                        NumericFilterOperatorKind::GreaterThan => field_value > *value,
                        NumericFilterOperatorKind::LessThan => field_value < *value,
                        NumericFilterOperatorKind::EqualTo => field_value == *value,
                    }
                }
                FilterKind::String {
                    field,
                    Operator,
                    value,
                } => {
                    let field_value = message.get(field).unwrap().as_str().unwrap();
                    match Operator {
                        StringFilterOperatorKind::EqualTo => field_value == value,
                        StringFilterOperatorKind::Contains => field_value.contains(value),
                    }
                }
            })
            .cloned()
            .collect();
        Ok(vec![Arc::new(out)])

        // let out = message_batch
        //     .iter()
        //     .filter(|message| match &self.filter {
        //         FilterKind::Numeric { field, Operator, value } => {
        //                     let field_value = message.get(field)?.as_f64()?;
        //                     match Operator {
        //                         NumericFilterOperatorKind::GreaterThan => field_value > *value,
        //                         NumericFilterOperatorKind::LessThan => field_value < *value,
        //                         NumericFilterOperatorKind::EqualTo => field_value == *value,
        //                     }
        //             FilterKind::String { field, Operator, value } => {
        //                 let field_value = message.get(field)?.as_str()?;
        //                 match Operator {
        //                     StringFilterOperatorKind::EqualTo => field_value == value,
        //                     StringFilterOperatorKind::Contains => field_value.contains(value),
        //             }
        //         }
        //     }}).cloned();
    }
}
