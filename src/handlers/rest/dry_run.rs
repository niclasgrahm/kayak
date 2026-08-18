//! Running a draft's transforms over some messages, without creating a
//! pipeline.
//!
//! See `Operation::DryRunPipeline` in `kayak_core::api_docs` for what the
//! endpoint is and [`kayak_core::dry_run`] for why it reports per stage. Four
//! things are worth knowing here:
//!
//! - **The transforms are built through the production `build()`**, with the
//!   same `BuildCtx` a pipeline gets except for its buckets — so a config that
//!   cannot build here cannot build there, in the same words.
//! - **Its buckets are private and thrown away**, exactly as
//!   `super::script`'s are, and for the same two reasons: live state would
//!   make the answer depend on what the server was doing, and writing it would
//!   give a dry run side effects.
//! - **A failure stops the chain**, unlike the run loop, which drops the batch
//!   and carries on. The two differ because the questions differ: a running
//!   pipeline has to survive a bad message, and someone asking about one wants
//!   to know exactly where it stopped being handled.
//! - **The chain is drained at the end** — every transform is asked once
//!   whether it has anything to hand on. That is the `flush` half of the run
//!   loop's tick, called directly rather than waited for: a `buffer` is
//!   allowed to answer "nothing", which is the ordinary case and is reported
//!   as such.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::{Json, extract::State, response::IntoResponse};
use kayak_core::config::TransformConfig;
use kayak_core::dry_run::{
    FailurePhase, PipelineDryRunRequest, PipelineDryRunResponse, StageResult,
};
use kayak_core::state::{StateBucketConfig, StateBuckets};
use serde_json::Value;

use crate::buckets::Buckets;
use crate::handlers::error::AppError;
use crate::inputs::MessageBatch;
use crate::state::AppState;
use crate::transforms::Transform;

/// The pipeline id a dry run builds under. Nothing is registered under it —
/// it exists because a `BuildCtx` has one and a component may put it in a log
/// line.
const DRY_RUN_ID: &str = "dry-run";

/// Run some messages through a chain of transforms — see
/// `Operation::DryRunPipeline`.
pub async fn dry_run_pipeline(
    State(state): State<Arc<AppState>>,
    Json(request): Json<PipelineDryRunRequest>,
) -> Result<impl IntoResponse, AppError> {
    let buckets = scratch(request.state.as_ref(), &request.buckets);
    let kinds: Vec<String> = request.transforms.iter().map(kind_of).collect();
    let mut stages: Vec<StageResult> = kinds
        .iter()
        .enumerate()
        .map(|(index, kind)| StageResult {
            index,
            kind: kind.clone(),
            batches: Vec::new(),
            on_flush: Vec::new(),
        })
        .collect();

    let mut transforms = Vec::with_capacity(request.transforms.len());
    for (at, config) in request.transforms.iter().cloned().enumerate() {
        match state.build_dry_run_transform(
            DRY_RUN_ID,
            config,
            Arc::clone(&buckets),
            request.state.clone(),
        ) {
            Ok(transform) => transforms.push(transform),
            // nothing ran, so there is nothing to show before it
            Err(err) => {
                let failure = Failure::build(at, &err);
                return Ok(Json(failure.response(&kinds, Vec::new())));
            }
        }
    }

    let batch: MessageBatch = request.messages_used().iter().cloned().map(Arc::new).collect();
    if let Err(failure) = feed(&mut transforms, &mut stages, vec![Arc::new(batch)], 0, false).await
    {
        return Ok(Json(failure.response(&kinds, stages)));
    }
    if let Err(failure) = drain(&mut transforms, &mut stages).await {
        return Ok(Json(failure.response(&kinds, stages)));
    }

    Ok(Json(PipelineDryRunResponse::Ran {
        buckets: contents(&buckets),
        stages,
    }))
}

/// Puts batches through the chain from `from` onwards, recording what each
/// stage handed on.
///
/// One batch in and any number out, each stage's output the next one's input —
/// the same shape `PipelineRuntime::through_transforms` has, without the
/// reporting. `on_flush` says which of the two lists a stage's output is
/// recorded in; see [`StageResult::on_flush`] for why they are kept apart.
async fn feed(
    transforms: &mut [Box<dyn Transform>],
    stages: &mut [StageResult],
    first: Vec<Arc<MessageBatch>>,
    from: usize,
    on_flush: bool,
) -> Result<(), Failure> {
    let mut carried = first;
    for (offset, transform) in transforms[from..].iter_mut().enumerate() {
        let index = from + offset;
        let mut produced = Vec::new();
        for batch in carried {
            match transform.apply(batch).await {
                Ok(out) => produced.extend(out),
                Err(err) => return Err(Failure::apply(index, &err)),
            }
        }
        let recorded: Vec<Vec<Value>> = produced.iter().map(values).collect();
        if on_flush {
            stages[index].on_flush.extend(recorded);
        } else {
            stages[index].batches = recorded;
        }
        carried = produced;
    }
    Ok(())
}

/// Asks every transform whether it will hand on what it is holding *now*.
///
/// The `flush` half of the run loop's tick, called directly rather than waited
/// for — a dry run has no tick to give. So a transform that is still waiting
/// answers "nothing", which is what the pipeline would do at that instant too,
/// and is reported as the empty stage it is. What one does release goes
/// through the rest of the chain from `index + 1`, since what it held has
/// already been through everything in front of it.
async fn drain(
    transforms: &mut [Box<dyn Transform>],
    stages: &mut [StageResult],
) -> Result<(), Failure> {
    for index in 0..transforms.len() {
        let released = transforms[index]
            .flush()
            .await
            .map_err(|err| Failure::apply(index, &err))?;
        if released.is_empty() {
            continue;
        }
        stages[index].on_flush.extend(released.iter().map(values));
        feed(transforms, stages, released, index + 1, true).await?;
    }
    Ok(())
}

/// Where a chain stopped, before it is paired with the stages that ran.
struct Failure {
    at: usize,
    phase: FailurePhase,
    message: String,
}

impl Failure {
    fn build(at: usize, err: &anyhow::Error) -> Self {
        Self {
            at,
            phase: FailurePhase::Build,
            message: format!("{err:#}"),
        }
    }

    fn apply(at: usize, err: &anyhow::Error) -> Self {
        Self {
            at,
            phase: FailurePhase::Apply,
            message: format!("{err:#}"),
        }
    }

    fn response(self, kinds: &[String], stages: Vec<StageResult>) -> PipelineDryRunResponse {
        PipelineDryRunResponse::Failed {
            stages,
            at: self.at,
            kind: kinds.get(self.at).cloned().unwrap_or_default(),
            phase: self.phase,
            message: self.message,
        }
    }
}

/// The transform's wire name, off its serialized tag — the same trick
/// `super::sample::kind_of` uses, and for the same reason: a match here would
/// be a second list of the transforms that could fall behind the first.
fn kind_of(config: &TransformConfig) -> String {
    serde_json::to_value(config)
        .ok()
        .and_then(|value| value["type"].as_str().map(ToString::to_string))
        .unwrap_or_default()
}

fn values(batch: &Arc<MessageBatch>) -> Vec<Value> {
    batch.iter().map(|message| (**message).clone()).collect()
}

/// Buckets holding only what this request asked them to.
///
/// The bucket the pipeline's `state` block names is declared with the default
/// bounds: a dry run is a handful of messages, so the limits a real deployment
/// sets are not what is being asked about, and refusing to run because a
/// bucket wasn't declared would be answering a different question.
fn scratch(
    state: Option<&kayak_core::state::PipelineState>,
    seed: &BTreeMap<String, BTreeMap<String, Value>>,
) -> Arc<Buckets> {
    let mut declared = StateBuckets::new();
    if let Some(state) = state {
        declared.insert(&state.bucket, StateBucketConfig::default());
    }
    let buckets = Buckets::from_config(&declared);
    if let Some(state) = state {
        for (key, values) in seed {
            buckets.remember(
                &state.bucket,
                key,
                values
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect(),
            );
        }
    }
    Arc::new(buckets)
}

/// What the private buckets ended up holding.
fn contents(buckets: &Buckets) -> BTreeMap<String, BTreeMap<String, Value>> {
    let mut out = BTreeMap::new();
    for summary in buckets.summaries() {
        let Some(contents) = buckets.contents(&summary.name) else {
            continue;
        };
        for entry in contents.entries {
            out.insert(entry.key, entry.values.into_iter().collect());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transform(json: serde_json::Value) -> TransformConfig {
        match serde_json::from_value(json) {
            Ok(config) => config,
            Err(err) => panic!("not a valid transform config: {err}"),
        }
    }

    #[test]
    fn the_kind_is_read_off_the_wire_tag() {
        assert_eq!(
            kind_of(&transform(serde_json::json!({"type": "buffer", "size": 2}))),
            "buffer"
        );
    }
}
