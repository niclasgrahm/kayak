use kayak_core::config::SplitterTransformConfig;
use std::sync::Arc;

use crate::{
    BuildCtx,
    inputs::MessageBatch,
    transforms::{BuildTransform, Transform},
};

impl BuildTransform for SplitterTransformConfig {
    fn build(self, _ctx: &mut BuildCtx) -> anyhow::Result<Box<dyn Transform>> {
        if self.out_size == 0 {
            anyhow::bail!("splitter: out_size must be at least 1");
        }
        Ok(Box::new(SplitterTransform {
            out_size: self.out_size,
        }))
    }
}

pub struct SplitterTransform {
    out_size: usize,
}

/// A leftover is emitted as a short final batch rather than held for the next
/// `apply()`. Holding it would make the splitter stateful and — like the idle
/// file part and the lazy bucket eviction — a transform gets no tick, so a
/// remainder on a stream that then goes quiet would be held for as long as the
/// pipeline runs. A short batch is visible; a message that arrives whenever the
/// next one happens to is not.

#[async_trait::async_trait]
impl Transform for SplitterTransform {
    async fn apply(
        &mut self,
        message_batch: Arc<MessageBatch>,
    ) -> anyhow::Result<Vec<Arc<MessageBatch>>> {
        let mut outer = vec![];
        let mut inner = vec![];
        for msg in message_batch.iter() {
            inner.push(msg.clone());
            if inner.len() >= self.out_size {
                outer.push(Arc::new(inner));
                inner = vec![];
            }
        }
        if !inner.is_empty() {
            outer.push(Arc::new(inner));
        }
        Ok(outer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::batch;
    use serde_json::json;

    async fn split(out_size: usize, count: usize) -> Vec<Vec<serde_json::Value>> {
        let msgs = (0..count).map(|i| json!({ "i": i })).collect();
        let out = SplitterTransform { out_size }
            .apply(batch(msgs))
            .await
            .unwrap_or_default();
        out.iter()
            .map(|b| b.iter().map(|m| (**m).clone()).collect())
            .collect()
    }

    #[tokio::test]
    async fn an_evenly_divisible_batch_is_split_into_equal_chunks() {
        let out = split(2, 4).await;
        assert_eq!(
            out,
            vec![
                vec![json!({"i": 0}), json!({"i": 1})],
                vec![json!({"i": 2}), json!({"i": 3})],
            ]
        );
    }

    #[tokio::test]
    async fn out_size_of_one_emits_a_batch_per_message() {
        assert_eq!(split(1, 3).await.len(), 3);
    }

    #[tokio::test]
    async fn the_remainder_is_emitted_as_a_short_final_batch() {
        let out = split(3, 4).await;
        assert_eq!(
            out,
            vec![
                vec![json!({"i": 0}), json!({"i": 1}), json!({"i": 2})],
                vec![json!({"i": 3})],
            ]
        );
    }

    #[tokio::test]
    async fn a_batch_smaller_than_out_size_comes_out_whole() {
        assert_eq!(
            split(10, 2).await,
            vec![vec![json!({"i": 0}), json!({"i": 1})]]
        );
    }

    #[tokio::test]
    async fn an_empty_batch_emits_nothing() {
        assert!(split(3, 0).await.is_empty());
    }

    #[test]
    fn an_out_size_of_zero_is_refused_at_build_time() {
        let (events, _) = tokio::sync::broadcast::channel(16);
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = BuildCtx::new(&mut pipelines, "splitter-test".into(), events);
        let err = SplitterTransformConfig { out_size: 0 }
            .build(&mut ctx)
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(err.contains("out_size"), "unexpected error: {err}");
    }
}
