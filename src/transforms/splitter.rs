use kayak_core::config::SplitterTransformConfig;
use std::sync::Arc;

use crate::{
    BuildCtx,
    inputs::MessageBatch,
    transforms::{BuildTransform, Transform},
};

impl BuildTransform for SplitterTransformConfig {
    fn build(self, _ctx: &mut BuildCtx) -> anyhow::Result<Box<dyn Transform>> {
        Ok(Box::new(SplitterTransform {
            out_size: self.out_size,
        }))
    }
}

pub struct SplitterTransform {
    out_size: usize,
}

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
        // TODO: theres stuff left here
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

    /// Documents a known bug rather than the desired behaviour: with
    /// `out_size: 3` and 4 messages, the 4th is silently dropped. When the
    /// leftover question in readme.md is decided, this test should flip to
    /// asserting the remainder is emitted.
    #[tokio::test]
    async fn known_bug_the_remainder_is_currently_discarded() {
        let out = split(3, 4).await;
        assert_eq!(
            out,
            vec![vec![json!({"i": 0}), json!({"i": 1}), json!({"i": 2})]]
        );
    }
}
