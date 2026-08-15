//! Running a script without creating a pipeline.
//!
//! See `Operation::DryRunScript` in `kayak_core::api_docs` for what this
//! endpoint is and why it exists. The two things worth knowing here:
//!
//! - **It runs the production runner**, configured the same way, because a dry
//!   run that can disagree with production is worse than none.
//! - **Its bucket is private and thrown away.** A dry run that read live state
//!   would answer differently depending on what the server was doing, and one
//!   that wrote live state would not be dry. Seeded from the request, returned
//!   in the response, dropped when the response is sent.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::{Json, extract::State, response::IntoResponse};
use kayak_core::script::{DryRunRequest, DryRunResponse, DryRunStage};
use kayak_core::state::{StateBucketConfig, StateBuckets};
use serde_json::Value;

use crate::buckets::Buckets;
use crate::handlers::error::AppError;
use crate::state::AppState;
use crate::transforms::script::error::ScriptErrorKind;
use crate::transforms::script::runner::{Bindings, ScriptRunner, StateBinding};
use crate::transforms::script::source;

/// The bucket a dry run remembers into. A name, not a configured thing: the
/// bucket exists for the length of one request and nothing else can name it.
const SCRATCH_BUCKET: &str = "dry-run";

/// Run a script over some messages — see `Operation::DryRunScript`.
#[allow(clippy::unused_async)]
pub async fn dry_run_script(
    State(state): State<Arc<AppState>>,
    Json(request): Json<DryRunRequest>,
) -> Result<impl IntoResponse, AppError> {
    // Reading the script is the one part that *is* a request error: a `file`
    // source naming something unreadable is not a script with a bug in it, it
    // is a request that could not be carried out.
    let script_dir = state.script_directory();
    let code =
        source::read(&request.source, script_dir.as_deref()).map_err(AppError::bad_request)?;

    let buckets = scratch(&request.state);
    let bindings = Bindings {
        state: Some(StateBinding {
            buckets: Arc::clone(&buckets),
            bucket: SCRATCH_BUCKET.to_string(),
        }),
    };

    let mut runner = match ScriptRunner::compile(
        &code,
        request.scope,
        request.max_operations,
        bindings,
        script_dir.as_deref(),
    ) {
            Ok(runner) => runner,
            Err(err) => return Ok(Json(failed(&err))),
        };

    let batch: Vec<Arc<Value>> = request.messages.into_iter().map(Arc::new).collect();
    let batches = match runner.run(&batch) {
        Ok(batches) => batches,
        Err(err) => return Ok(Json(failed(&err))),
    };

    Ok(Json(DryRunResponse::Emitted {
        batches: batches
            .iter()
            .map(|batch| batch.iter().map(|m| (**m).clone()).collect())
            .collect(),
        warnings: runner.warnings(),
        state: contents(&buckets),
    }))
}

/// A bucket holding only what the request asked it to.
fn scratch(seed: &BTreeMap<String, BTreeMap<String, Value>>) -> Arc<Buckets> {
    let mut declared = StateBuckets::new();
    declared.insert(SCRATCH_BUCKET, StateBucketConfig::default());
    let buckets = Buckets::from_config(&declared);
    for (key, values) in seed {
        buckets.remember(
            SCRATCH_BUCKET,
            key,
            values
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
        );
    }
    Arc::new(buckets)
}

/// What the scratch bucket ended up holding, so a script's `remember` is
/// visible rather than being a side effect nobody can see.
fn contents(buckets: &Buckets) -> BTreeMap<String, BTreeMap<String, Value>> {
    buckets
        .contents(SCRATCH_BUCKET)
        .map(|contents| {
            contents
                .entries
                .into_iter()
                .map(|entry| (entry.key, entry.values))
                .collect()
        })
        .unwrap_or_default()
}

fn failed(err: &crate::transforms::script::error::ScriptError) -> DryRunResponse {
    DryRunResponse::Failed {
        stage: match err.kind {
            ScriptErrorKind::Compile => DryRunStage::Compile,
            ScriptErrorKind::Runtime => DryRunStage::Runtime,
        },
        message: err.message.clone(),
        line: err.position.map(|p| p.line),
        column: err.position.map(|p| p.column),
    }
}
