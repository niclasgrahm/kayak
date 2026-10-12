use anyhow::Result;
use std::sync::Arc;

use crate::{BuildCtx, inputs::MessageBatch};

pub mod clickhouse;
pub mod columns;
pub mod file;
pub mod http;
pub mod indu;
pub mod kafka;
pub mod mqtt;
pub mod nats;
pub mod postgres;
pub mod redis;
pub mod rotate;
pub mod s3;
pub mod stdout;
pub mod tidepool;

pub trait BuildOutput {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn OutputDestination>>;
}

#[async_trait::async_trait]
pub trait OutputDestination: Send + 'static {
    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> anyhow::Result<()>;
    async fn init(&mut self) -> Result<()>;

    /// Called once when the pipeline's run loop ends, however it ended.
    ///
    /// Most outputs have nothing to do here — a nats publish or a postgres
    /// insert is complete the moment `emit` returns — which is why this defaults
    /// to doing nothing. It exists for the two that hold a *part*: the file
    /// output has a JSON array to close, and the s3 output has a buffered object
    /// that has not been uploaded at all. Without this, cancelling a pipeline
    /// silently truncated the first and lost the second.
    ///
    /// It is called after cancellation, so anything awaited here is awaited by a
    /// server that is already shutting the pipeline down — do the one write, not
    /// a retry loop.
    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }

    /// Make everything emitted so far durable at the destination — called at
    /// every checkpoint barrier, before the checkpoint's acks may fire. See
    /// `docs/durability.md`.
    ///
    /// The default does nothing, which is right for every output whose `emit`
    /// only returns once the destination has the batch (a postgres insert, an
    /// http request). An output that buffers — a file in the page cache, a
    /// producer's queue — flushes here. An error fails the checkpoint for the
    /// pipeline's region; the run loop carries on and the next one covers it.
    async fn checkpoint(&mut self) -> Result<()> {
        Ok(())
    }

    /// What this output is holding that it has accepted but not yet written —
    /// the s3 output's unuploaded part. `None` (the default) for every output
    /// that holds nothing once [`OutputDestination::checkpoint`] has returned.
    fn snapshot(&self) -> Option<serde_json::Value> {
        None
    }

    /// Take back what [`OutputDestination::snapshot`] returned, before `init`.
    fn restore(&mut self, _state: serde_json::Value) -> Result<()> {
        Ok(())
    }
}
