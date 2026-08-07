use std::sync::Arc;

use kayak_core::config::FilterKind;
use kayak_core::config::FilterTransformConfig;
use kayak_core::config::NumericFilterOperatorKind;
use kayak_core::config::StringFilterOperatorKind;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::batch;
    use serde_json::{Value, json};

    fn transform(filter: FilterKind) -> FilterTransform {
        FilterTransform { filter }
    }

    fn numeric(operator: NumericFilterOperatorKind, value: f64) -> FilterKind {
        FilterKind::Numeric {
            field: "value".to_string(),
            operator,
            value,
        }
    }

    /// Flatten `apply`'s nested output to the plain JSON that survived.
    async fn kept(t: &mut FilterTransform, values: Vec<serde_json::Value>) -> Vec<Vec<Value>> {
        let out = t.apply(batch(values)).await.unwrap_or_default();
        out.iter()
            .map(|b| b.iter().map(|m| (**m).clone()).collect())
            .collect()
    }

    #[tokio::test]
    async fn numeric_greater_than_keeps_only_larger_values() {
        let mut t = transform(numeric(NumericFilterOperatorKind::GreaterThan, 10.0));
        let out = kept(&mut t, vec![json!({"value": 5}), json!({"value": 20})]).await;
        assert_eq!(out, vec![vec![json!({"value": 20})]]);
    }

    #[tokio::test]
    async fn numeric_less_than_keeps_only_smaller_values() {
        let mut t = transform(numeric(NumericFilterOperatorKind::LessThan, 10.0));
        let out = kept(&mut t, vec![json!({"value": 5}), json!({"value": 20})]).await;
        assert_eq!(out, vec![vec![json!({"value": 5})]]);
    }

    #[tokio::test]
    async fn numeric_equal_to_matches_across_int_and_float_encodings() {
        let mut t = transform(numeric(NumericFilterOperatorKind::EqualTo, 10.0));
        let out = kept(&mut t, vec![json!({"value": 10}), json!({"value": 10.0})]).await;
        assert_eq!(
            out,
            vec![vec![json!({"value": 10}), json!({"value": 10.0})]]
        );
    }

    #[tokio::test]
    async fn string_operators_match_equality_and_substrings() {
        let mut equals = transform(FilterKind::String {
            field: "name".to_string(),
            operator: StringFilterOperatorKind::EqualTo,
            value: "kayak".to_string(),
        });
        let out = kept(
            &mut equals,
            vec![json!({"name": "kayak"}), json!({"name": "kayaking"})],
        )
        .await;
        assert_eq!(out, vec![vec![json!({"name": "kayak"})]]);

        let mut contains = transform(FilterKind::String {
            field: "name".to_string(),
            operator: StringFilterOperatorKind::Contains,
            value: "kayak".to_string(),
        });
        let out = kept(
            &mut contains,
            vec![json!({"name": "kayak"}), json!({"name": "kayaking"})],
        )
        .await;
        assert_eq!(
            out,
            vec![vec![json!({"name": "kayak"}), json!({"name": "kayaking"})]]
        );
    }

    /// A message that can't satisfy the predicate is dropped, not an error —
    /// one odd message must not stop the pipeline.
    #[tokio::test]
    async fn missing_or_mistyped_fields_are_dropped_without_erroring() {
        let mut t = transform(numeric(NumericFilterOperatorKind::GreaterThan, 0.0));
        let out = kept(
            &mut t,
            vec![
                json!({"other": 1}),
                json!({"value": "not a number"}),
                json!({"value": 1}),
            ],
        )
        .await;
        assert_eq!(out, vec![vec![json!({"value": 1})]]);
    }

    /// An all-dropped batch emits nothing at all, rather than an empty batch
    /// that a downstream reducer would turn into a meaningless result.
    #[tokio::test]
    async fn a_batch_with_no_matches_emits_nothing() {
        let mut t = transform(numeric(NumericFilterOperatorKind::GreaterThan, 100.0));
        let out = kept(&mut t, vec![json!({"value": 1})]).await;
        assert!(out.is_empty(), "expected no batches, got {out:?}");
    }
}
