//! Putting some messages through a draft's transforms, without creating a
//! pipeline.
//!
//! The third and last piece of "see what you are building while you build it":
//! [`crate::sample`] fetches real messages from the input, [`crate::schema`]
//! reads the fields back off them, and this runs them down the chain so the
//! same is true of every transform after the first. What a `map` writes, what
//! a `filter` drops, what a `reduce` collapses a batch to — all of it is a
//! question the config cannot answer and one message can.
//!
//! It is the [`crate::script`] dry run generalised from one transform to the
//! chain, and it keeps that module's two rules because they are what make a
//! dry run worth believing:
//!
//! - **The transforms are the production ones**, built through the same
//!   `build()` a running pipeline uses, so a config that cannot build here
//!   cannot build there either — and says so in the same words.
//! - **State is never live.** The buckets are private to the request, seeded
//!   from `buckets` in the body and thrown away with the response. Reading
//!   production state would make the answer depend on what the server happened
//!   to be doing; writing it would give a dry run side effects.
//!
//! **Outputs are not part of it and cannot be.** A dry run that emitted would
//! be a pipeline. That is the line: everything up to the outputs is a question
//! about the data, and the outputs are the part that changes somebody else's
//! system.
//!
//! # Cardinality is the whole point of reporting per stage
//!
//! A transform takes one batch and produces *any number* of them — that is how
//! `splitter` works, and how a `filter` that drops everything produces none. So
//! a stage's result is a list of batches rather than a batch, and the shape of
//! that list is usually the answer someone is looking for: a `reduce` that
//! comes back with one message per group, a `buffer` that comes back with
//! nothing because it is still holding what it was given.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::TransformConfig;
use crate::state::PipelineState;

/// The most messages a dry run will accept.
///
/// It runs inside the request, so the bound is the server's rather than the
/// caller's — the same argument [`crate::sample::MAX_MESSAGES`] makes, and a
/// wider one here because a chain someone is designing is worth feeding a
/// realistic batch.
pub const MAX_MESSAGES: usize = 200;

/// Run a draft's transforms over some messages.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "pipeline dry run request")]
pub struct PipelineDryRunRequest {
    /// the messages to put in, as one batch — which is what a `buffer` or a
    /// `reduce` will treat them as. A sample from `POST /api/inputs/sample` is
    /// what the UI puts here.
    pub messages: Vec<Value>,
    /// the transforms, in order, exactly as they would be written in the
    /// pipeline. An empty list is allowed and answers the trivial question.
    #[serde(default)]
    pub transforms: Vec<TransformConfig>,
    /// the pipeline's `state` block, if its transforms need one. The bucket it
    /// names is created for this request alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<PipelineState>,
    /// what that bucket should already hold, keyed as the pipeline keys it.
    /// The warm-up a stateful chain would otherwise have to be talked through.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub buckets: BTreeMap<String, BTreeMap<String, Value>>,
}

impl PipelineDryRunRequest {
    /// The messages it will actually run, within the bound.
    #[must_use]
    pub fn messages_used(&self) -> &[Value] {
        let end = self.messages.len().min(MAX_MESSAGES);
        &self.messages[..end]
    }
}

/// What one transform in the chain did.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct StageResult {
    /// its position in `transforms`.
    pub index: usize,
    /// which transform it is (`filter`, `reduce`, ...), so the answer can be
    /// read without counting down the list.
    pub kind: String,
    /// what it handed on, one entry per batch. Several batches is a
    /// `splitter`; none is a `filter` that dropped everything, or a `buffer`
    /// still holding what it was given.
    pub batches: Vec<Vec<Value>>,
    /// what it handed on when the chain was drained at the end, if anything.
    ///
    /// Kept apart from `batches` because the difference is the interesting
    /// part: a running pipeline releases these on a timer or a gate rather
    /// than when the messages arrive, so a dry run that folded the two
    /// together would make a `buffer` look like it passes everything straight
    /// through.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on_flush: Vec<Vec<Value>>,
}

impl StageResult {
    /// Every message this stage handed on, in order — what the next stage saw,
    /// and what a schema is inferred from.
    #[must_use]
    pub fn messages(&self) -> Vec<Value> {
        self.batches
            .iter()
            .chain(self.on_flush.iter())
            .flat_map(|batch| batch.iter().cloned())
            .collect()
    }
}

/// Where a chain went wrong, when it did.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FailurePhase {
    /// The transform could not be built — the same failure creating the
    /// pipeline would have given, which is most of the value of building the
    /// real thing.
    Build,
    /// It was built and failed on a message. A running pipeline would drop
    /// that batch and carry on; a dry run stops, because what happened at the
    /// point of failure is the thing being asked about.
    Apply,
}

/// What the chain did.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "outcome", rename_all = "snake_case")]
#[schemars(title = "pipeline dry run response")]
pub enum PipelineDryRunResponse {
    Ran {
        /// one entry per transform, in order.
        stages: Vec<StageResult>,
        /// what the private buckets ended up holding, so a `remember` is
        /// visible rather than being a side effect nobody can see.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        buckets: BTreeMap<String, BTreeMap<String, Value>>,
    },
    Failed {
        /// the stages that completed before it. Kept, rather than thrown away
        /// with the failure: how far the messages got is half of what says
        /// *why* the failing transform failed.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        stages: Vec<StageResult>,
        /// which transform failed, by position.
        at: usize,
        kind: String,
        phase: FailurePhase,
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn request(messages: Vec<Value>) -> PipelineDryRunRequest {
        PipelineDryRunRequest {
            messages,
            transforms: Vec::new(),
            state: None,
            buckets: BTreeMap::new(),
        }
    }

    #[test]
    fn the_messages_are_bounded_by_the_server_rather_than_the_caller() {
        let request = request(vec![json!({"n": 1}); MAX_MESSAGES + 10]);
        assert_eq!(request.messages_used().len(), MAX_MESSAGES);
    }

    #[test]
    fn a_short_batch_is_used_whole() {
        let request = request(vec![json!({"n": 1}), json!({"n": 2})]);
        assert_eq!(request.messages_used().len(), 2);
    }

    /// What the next stage saw is both lists, in order — the flushed messages
    /// are held-back ones, not different ones.
    #[test]
    fn a_stages_messages_are_what_it_handed_on_at_either_moment() {
        let stage = StageResult {
            index: 0,
            kind: "buffer".to_string(),
            batches: vec![vec![json!({"n": 1})]],
            on_flush: vec![vec![json!({"n": 2}), json!({"n": 3})]],
        };
        assert_eq!(
            stage.messages(),
            vec![json!({"n": 1}), json!({"n": 2}), json!({"n": 3})]
        );
    }
}
