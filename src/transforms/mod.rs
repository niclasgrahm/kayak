use std::sync::Arc;

use crate::{BuildCtx, inputs::MessageBatch};

pub mod buffer;
pub mod filter;
pub mod http;
pub mod map;
pub mod reduce;
pub mod script;
pub mod splitter;
pub mod state;

pub trait BuildTransform {
    fn build(self, ctx: &mut BuildCtx) -> anyhow::Result<Box<dyn Transform>>;
}

#[async_trait::async_trait]
pub trait Transform: Send + 'static {
    async fn apply(
        &mut self,
        message_batch: Arc<MessageBatch>,
    ) -> anyhow::Result<Vec<Arc<MessageBatch>>>;

    /// Resolves when this transform has something to hand on that no arriving
    /// batch will prompt — a window that closed, a gate that opened.
    ///
    /// This is the run loop's only tick. Transforms are otherwise driven
    /// entirely by arriving batches, which is fine for everything that
    /// transforms a message and wrong for anything that *holds* one: a
    /// `buffer` whose five seconds are up on a stream that has gone quiet
    /// would wait for a message that isn't coming, and its whole point is that
    /// it doesn't. The same missing tick is why bucket eviction is lazy and
    /// why an idle `file` output holds its part open; this is the first thing
    /// to answer it, and those are the next two candidates.
    ///
    /// **Must be cancel-safe.** The run loop builds these futures fresh on
    /// every pass and drops the losers, so anything a `wakeup` consumes before
    /// it resolves is lost. Keep the state — a deadline, a `watch` receiver —
    /// on the transform and build the future from it.
    ///
    /// The default never resolves, which is what every transform that only
    /// ever answers a batch wants.
    async fn wakeup(&mut self) {
        std::future::pending::<()>().await;
    }

    /// What that transform then wants to hand on. Called only after this
    /// transform's own [`Transform::wakeup`] resolved, and allowed to produce
    /// nothing — a wakeup is "look at me", not a promise.
    async fn flush(&mut self) -> anyhow::Result<Vec<Arc<MessageBatch>>> {
        Ok(vec![])
    }
}
