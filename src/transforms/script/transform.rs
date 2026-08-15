//! The `script` transform: a compiled script, run over every batch.
//!
//! Everything interesting is one level down — [`super::runner`] owns the engine
//! and the sandbox, [`super::source`] owns where the text comes from. What is
//! left here is the two decisions that belong to the transform rather than to
//! the script:
//!
//! - **Compiling happens at build time.** A script that does not parse is a
//!   pipeline that refuses to start, which is the same rule the reducer's
//!   duplicate-`as` check and the column mapping's contradictions follow. The
//!   alternative — discovering it on the first batch — turns a typo into a
//!   pipeline that exists, shows green until data arrives, and then fails
//!   forever.
//! - **A failing run fails the batch**, and nothing is emitted from it. Not
//!   "emit what the script managed before it failed": a script that threw
//!   half way through a batch has produced a partial answer, and passing that
//!   on would be a silent data loss dressed as a success.

use std::sync::Arc;

use anyhow::{Context, Result};
use kayak_core::script::ScriptTransformConfig;

use super::runner::{Bindings, ScriptRunner, StateBinding};
use super::source;
use crate::{
    BuildCtx,
    inputs::MessageBatch,
    transforms::{BuildTransform, Transform},
};

pub struct ScriptTransform {
    runner: ScriptRunner,
}

impl BuildTransform for ScriptTransformConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn Transform>> {
        let code = source::read(&self.source, ctx.script_dir.as_deref().map(std::path::PathBuf::as_path))
            .context("the 'script' transform could not read its script")?;

        // The bucket the pipeline declared, if it declared one. A pipeline
        // without a `state` block still builds — a script that never calls
        // `remember` is the common case, and whether one does cannot be known
        // from the text. See `runner::register_state`.
        let state = ctx.state.as_ref().and_then(|state| {
            ctx.buckets
                .contains(&state.bucket)
                .then(|| StateBinding {
                    buckets: Arc::clone(&ctx.buckets),
                    bucket: state.bucket.clone(),
                })
        });

        let runner = ScriptRunner::compile(&code, self.scope, self.max_operations, Bindings { state })
            .map_err(|err| anyhow::anyhow!("the 'script' transform did not compile: {err}"))?;

        Ok(Box::new(ScriptTransform { runner }))
    }
}

#[async_trait::async_trait]
impl Transform for ScriptTransform {
    async fn apply(&mut self, batch: Arc<MessageBatch>) -> Result<Vec<Arc<MessageBatch>>> {
        // Synchronous, and deliberately not moved to `spawn_blocking`: the
        // budget is what bounds this, and hopping threads per batch would cost
        // more than most scripts do. See `runner`'s module docs.
        self.runner
            .run(&batch)
            .map_err(|err| anyhow::anyhow!("{}", err.located()))
    }
}
