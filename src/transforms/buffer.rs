use std::sync::Arc;

use kayak_core::config::BufferTransformConfig;

use crate::{
    BuildCtx,
    inputs::MessageBatch,
    transforms::{BuildTransform, Transform},
};

impl BuildTransform for BufferTransformConfig {
    fn build(self, _ctx: &mut BuildCtx) -> anyhow::Result<Box<dyn Transform>> {
        Ok(Box::new(BufferTransform {
            size: self.size,
            current_batch: MessageBatch::new(),
            buffer: Vec::new(),
        }))
    }
}

pub struct BufferTransform {
    pub size: usize,
    current_batch: MessageBatch,
    buffer: Vec<Arc<MessageBatch>>,
}

#[async_trait::async_trait]
impl Transform for BufferTransform {
    async fn apply(
        &mut self,
        message_batch: Arc<MessageBatch>,
    ) -> anyhow::Result<Vec<Arc<MessageBatch>>> {
        for msg in message_batch.iter() {
            self.current_batch.push(msg.clone());
            if self.current_batch.len() >= self.size {
                let finished = std::mem::take(&mut self.current_batch);
                self.buffer.push(Arc::new(finished));
                self.current_batch = MessageBatch::new();
            }
        }

        Ok(std::mem::take(&mut self.buffer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::batch;
    use serde_json::json;

    fn transform(size: usize) -> BufferTransform {
        BufferTransform {
            size,
            current_batch: MessageBatch::new(),
            buffer: Vec::new(),
        }
    }

    async fn feed(t: &mut BufferTransform, count: usize) -> Vec<Vec<serde_json::Value>> {
        let msgs = (0..count).map(|i| json!({ "i": i })).collect();
        let out = t.apply(batch(msgs)).await.unwrap_or_default();
        out.iter()
            .map(|b| b.iter().map(|m| (**m).clone()).collect())
            .collect()
    }

    /// The buffer is stateful across calls: it holds messages back until it has
    /// `size` of them, then releases exactly one full batch.
    #[tokio::test]
    async fn messages_are_held_until_the_buffer_is_full() {
        let mut t = transform(3);
        assert!(feed(&mut t, 2).await.is_empty(), "2 of 3 should be held");
        let out = feed(&mut t, 1).await;
        assert_eq!(
            out,
            vec![vec![json!({"i": 0}), json!({"i": 1}), json!({"i": 0})]]
        );
    }

    /// One oversized input batch releases every full batch it contains in a
    /// single call — that's the "one batch in, N batches out" contract.
    #[tokio::test]
    async fn one_large_batch_releases_several_full_batches_at_once() {
        let mut t = transform(2);
        let out = feed(&mut t, 5).await;
        assert_eq!(out.len(), 2, "4 of 5 messages form 2 full batches: {out:?}");

        // the 5th is still buffered, and comes out once the next one arrives
        let out = feed(&mut t, 1).await;
        assert_eq!(out.len(), 1);
    }
}
