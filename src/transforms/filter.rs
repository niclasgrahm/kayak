use std::sync::Arc;

use anyhow::bail;
use kayak_core::config::Condition;
use kayak_core::config::FilterTransformConfig;

use crate::{
    BuildCtx,
    inputs::MessageBatch,
    transforms::{BuildTransform, Transform, state},
};

impl BuildTransform for FilterTransformConfig {
    fn build(self, _ctx: &mut BuildCtx) -> anyhow::Result<Box<dyn Transform>> {
        // all of no conditions is everything, so an empty list would be a
        // filter that keeps every message — or, inverted, none of them
        if self.conditions.is_empty() {
            bail!("a filter needs at least one condition");
        }
        Ok(Box::new(FilterTransform {
            conditions: self.conditions,
            invert: self.invert,
        }))
    }
}

pub struct FilterTransform {
    conditions: Vec<Condition>,
    invert: bool,
}

impl FilterTransform {
    /// Whether a message is kept. A message that doesn't carry a tested field,
    /// or carries it with the wrong type, doesn't pass that condition — the
    /// same rule `remember`'s `when` follows, since it is the same check.
    fn keeps(&self, message: &serde_json::Value) -> bool {
        state::matches(&self.conditions, message) != self.invert
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
            .filter(|message| self.keeps(message))
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
    use kayak_core::config::{NumericFilterOperatorKind, StringFilterOperatorKind};
    use serde_json::{Value, json};

    fn transform(condition: Condition) -> FilterTransform {
        FilterTransform {
            conditions: vec![condition],
            invert: false,
        }
    }

    fn numeric(operator: NumericFilterOperatorKind, value: f64) -> Condition {
        Condition::Numeric {
            field: "value".to_string(),
            operator,
            value,
        }
    }

    fn string(field: &str, operator: StringFilterOperatorKind, value: &str) -> Condition {
        Condition::String {
            field: field.to_string(),
            operator,
            value: value.to_string(),
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
        let mut equals = transform(Condition::String {
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

        let mut contains = transform(Condition::String {
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

    /// The same field addressing every transform uses: a dotted path reaches
    /// into a nested object, which is what lets a filter select on metadata
    /// without knowing that metadata is a thing.
    #[tokio::test]
    async fn a_nested_field_can_be_filtered_on() {
        let mut t = transform(Condition::String {
            field: "_meta.subject".to_string(),
            operator: StringFilterOperatorKind::Contains,
            value: "temperature".to_string(),
        });

        let kept = kept(
            &mut t,
            vec![
                json!({ "n": 1, "_meta": { "subject": "m1.temperature" } }),
                json!({ "n": 2, "_meta": { "subject": "m1.pressure" } }),
            ],
        )
        .await;

        assert_eq!(kept, vec![vec![json!({ "n": 1, "_meta": { "subject": "m1.temperature" } })]]);
    }

    /// A source whose field names really do contain dots keeps working: the
    /// literal key is tried first, so nothing had to learn an escaping rule.
    #[tokio::test]
    async fn a_field_name_containing_a_dot_still_matches_itself() {
        let mut t = transform(Condition::Numeric {
            field: "a.b".to_string(),
            operator: NumericFilterOperatorKind::GreaterThan,
            value: 1.0,
        });

        let kept = kept(&mut t, vec![json!({ "a.b": 5 })]).await;
        assert_eq!(kept, vec![vec![json!({ "a.b": 5 })]]);
    }

    #[tokio::test]
    async fn not_equal_to_keeps_everything_but_the_value() {
        let mut t = transform(numeric(NumericFilterOperatorKind::NotEqualTo, 10.0));
        let out = kept(&mut t, vec![json!({"value": 10.0}), json!({"value": 11})]).await;
        assert_eq!(out, vec![vec![json!({"value": 11})]]);

        let mut t = transform(string("state", StringFilterOperatorKind::NotEqualTo, "OFF"));
        let out = kept(&mut t, vec![json!({"state": "OFF"}), json!({"state": "RUNNING"})]).await;
        assert_eq!(out, vec![vec![json!({"state": "RUNNING"})]]);
    }

    /// `one_of` and `none_of` are the list spellings of equal and not equal,
    /// and a missing field is in neither — absence is not a value.
    #[tokio::test]
    async fn one_of_and_none_of_test_a_set_and_neither_matches_a_missing_field() {
        let states = vec!["OFF".to_string(), "UNKNOWN".to_string()];
        let messages = || {
            vec![
                json!({"state": "OFF"}),
                json!({"state": "RUNNING"}),
                json!({"state": "UNKNOWN"}),
                json!({"other": 1}),
            ]
        };

        let mut one_of = transform(Condition::OneOf {
            field: "state".to_string(),
            values: states.clone(),
        });
        let out = kept(&mut one_of, messages()).await;
        assert_eq!(out, vec![vec![json!({"state": "OFF"}), json!({"state": "UNKNOWN"})]]);

        let mut none_of = transform(Condition::NoneOf {
            field: "state".to_string(),
            values: states,
        });
        let out = kept(&mut none_of, messages()).await;
        assert_eq!(out, vec![vec![json!({"state": "RUNNING"})]]);
    }

    /// Several conditions are "all of these": the melt-temperature case, a
    /// reading of one signal below a floor.
    #[tokio::test]
    async fn every_condition_has_to_hold() {
        let mut t = FilterTransform {
            conditions: vec![
                string("sensor", StringFilterOperatorKind::EqualTo, "melt"),
                numeric(NumericFilterOperatorKind::LessThan, 590.0),
            ],
            invert: false,
        };
        let out = kept(
            &mut t,
            vec![
                json!({"sensor": "melt", "value": 500}),
                json!({"sensor": "melt", "value": 600}),
                json!({"sensor": "oil", "value": 40}),
            ],
        )
        .await;
        assert_eq!(out, vec![vec![json!({"sensor": "melt", "value": 500})]]);
    }

    /// `invert` drops what the conditions match and keeps the rest — including
    /// a message that lacks the field, since that one matched nothing.
    #[tokio::test]
    async fn invert_drops_the_matches_and_keeps_what_matched_nothing() {
        let mut t = FilterTransform {
            conditions: vec![
                string("sensor", StringFilterOperatorKind::EqualTo, "melt"),
                numeric(NumericFilterOperatorKind::LessThan, 590.0),
            ],
            invert: true,
        };
        let out = kept(
            &mut t,
            vec![
                json!({"sensor": "melt", "value": 500}),
                json!({"sensor": "melt", "value": 600}),
                json!({"sensor": "oil", "value": 40}),
                json!({"note": "no fields at all"}),
            ],
        )
        .await;
        assert_eq!(
            out,
            vec![vec![
                json!({"sensor": "melt", "value": 600}),
                json!({"sensor": "oil", "value": 40}),
                json!({"note": "no fields at all"}),
            ]]
        );
    }

    #[test]
    fn a_filter_with_no_conditions_is_refused() {
        let config = FilterTransformConfig {
            conditions: vec![],
            invert: false,
        };
        let mut pipelines = std::collections::HashMap::new();
        let (events, _) = tokio::sync::broadcast::channel(1);
        let mut ctx = BuildCtx::new(&mut pipelines, "filter-test".into(), events);
        assert!(config.build(&mut ctx).is_err());
    }
}
