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
                    .and_then(serde_json::Value::as_f64)
                    .with_context(|| format!("field '{}' missing or not numeric", self.field))
            })
            .collect::<anyhow::Result<_>>()?;

        // every branch below relies on values being non-empty, which the
        // guard above guarantees
        let reduced = match self.function {
            ReduceFnKind::Sum => values.iter().sum::<f64>(),
            // a batch would have to hold 2^52 messages for this cast to lose
            // anything; it can't, they're all in memory at once
            #[allow(clippy::cast_precision_loss)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::batch;
    use serde_json::{Value, json};

    fn transform(function: ReduceFnKind) -> ReduceTransform {
        ReduceTransform {
            function,
            field: "value".to_string(),
        }
    }

    async fn reduce(function: ReduceFnKind, values: &[f64]) -> anyhow::Result<Vec<Value>> {
        let msgs = values.iter().map(|v| json!({ "value": v })).collect();
        let out = transform(function).apply(batch(msgs)).await?;
        Ok(out
            .iter()
            .flat_map(|b| b.iter().map(|m| (**m).clone()).collect::<Vec<_>>())
            .collect())
    }

    #[tokio::test]
    async fn each_reduce_function_produces_the_expected_value() -> anyhow::Result<()> {
        let cases = [
            (ReduceFnKind::Sum, 10.0),
            (ReduceFnKind::Avg, 2.5),
            (ReduceFnKind::Min, 1.0),
            (ReduceFnKind::Max, 4.0),
        ];
        for (function, expected) in cases {
            let out = reduce(function.clone(), &[1.0, 2.0, 3.0, 4.0]).await?;
            assert_eq!(
                out,
                vec![json!({"original_field": "value", "reduced_value": expected})],
                "unexpected result for {function:?}"
            );
        }
        Ok(())
    }

    /// A tumbling window can close without ever receiving a message. Emitting
    /// nothing is correct; a 0 or a NaN would be invented data.
    #[tokio::test]
    async fn an_empty_batch_emits_nothing() -> anyhow::Result<()> {
        let out = transform(ReduceFnKind::Sum).apply(batch(vec![])).await?;
        assert!(out.is_empty(), "expected no batches, got {out:?}");
        Ok(())
    }

    /// Unlike the filter, the reducer can't sensibly skip a message — a sum
    /// over "some of the messages" is silently wrong — so it errors instead.
    #[tokio::test]
    async fn a_missing_or_non_numeric_field_is_an_error() {
        let mut t = transform(ReduceFnKind::Sum);
        let res = t
            .apply(batch(vec![json!({"value": 1}), json!({"other": 2})]))
            .await;
        let Err(err) = res else {
            panic!("expected an error for a message missing the reduced field");
        };
        assert!(
            format!("{err:#}").contains("value"),
            "error should name the offending field: {err:#}"
        );
    }
}
