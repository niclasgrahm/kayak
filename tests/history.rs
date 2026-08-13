//! Tests for what the server remembers after the fact — the store fed by the
//! run loop's counters, and the endpoint that serves it.
//!
//! The store's own rules (ring eviction, the signature cap, what survives a
//! deletion) are unit-tested in `src/history.rs`. These are about the two
//! seams that module can't reach on its own: that a real run loop actually
//! feeds it, and that a real request actually gets it back.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use kayak::history::History;
use kayak::pipeline::{Pipeline, PipelineRuntime};
use kayak::state::AppState;
use kayak::testing::{
    CollectingOutput, FailOnNth, ScriptedInput, WhenExhausted, batch, stub_config,
};
use kayak_core::history::{PipelineHistory, Resolution};
use kayak_core::server_config::HistoryConfig;
use serde_json::json;
use tokio::sync::broadcast;
use tower::ServiceExt;

fn pipeline(id: &str) -> Arc<Pipeline> {
    match Pipeline::new(stub_config(id)) {
        Ok(p) => Arc::new(p),
        Err(e) => panic!("building pipeline '{id}': {e:#}"),
    }
}

fn history(retention_secs: u64) -> Arc<History> {
    Arc::new(History::new(HistoryConfig { retention_secs }))
}

/// A run loop over a finite script, with an optional history store. The script
/// fails once exhausted, so `run()` returns on its own — an exhausted input is
/// a clean stop here, not a failure.
async fn run_once(
    shared: &Arc<Pipeline>,
    batches: Vec<Arc<kayak::inputs::MessageBatch>>,
    failing: bool,
    store: Option<&Arc<History>>,
) -> anyhow::Result<()> {
    let (events, _) = broadcast::channel(16);
    assert_eq!(
        events.receiver_count(),
        0,
        "nobody is subscribed to the feed in any of these tests, which is the point"
    );
    let transforms: Vec<Box<dyn kayak::transforms::Transform>> = if failing {
        vec![Box::new(FailOnNth::new(0))]
    } else {
        vec![]
    };
    let mut runtime = PipelineRuntime::from_parts(
        vec![Box::new(ScriptedInput::new(batches, WhenExhausted::Fail))],
        transforms,
        vec![Box::new(CollectingOutput::new())],
        Arc::clone(shared),
        events,
    )?;
    if let Some(store) = store {
        runtime = runtime.with_history(Arc::clone(store));
    }
    let _ = runtime.run().await;
    Ok(())
}

/// A run loop counts what passes through it **without anyone watching** — no
/// browser is attached to any of these tests, and that is the whole point:
/// `/events` is gated on a subscriber and history is not.
#[tokio::test]
async fn a_run_loop_counts_what_passes_through_it_with_nobody_watching() -> anyhow::Result<()> {
    let store = history(3_600);
    let shared = pipeline("counted");
    run_once(
        &shared,
        vec![
            batch(vec![json!({"n": 1}), json!({"n": 2})]),
            batch(vec![json!({"n": 3})]),
        ],
        false,
        Some(&store),
    )
    .await?;

    let id = "counted".to_string();
    store.sample([(&id, &shared.counters)], 0);

    let out = store.get(&id, Resolution::Coarse);
    assert_eq!(out.buckets.len(), 1);
    assert_eq!(out.buckets[0].inbound, 3, "three messages arrived");
    assert_eq!(out.buckets[0].outbound, 3, "and three left the transforms");
    Ok(())
}

/// The morning question. A component that fails leaves one readable entry
/// saying what broke, when it started and how often — which is what the UI
/// feed, being a live sample nobody was subscribed to, cannot say at all.
#[tokio::test]
async fn a_failing_component_leaves_a_readable_signature() -> anyhow::Result<()> {
    let store = history(3_600);
    let shared = pipeline("broken");
    run_once(
        &shared,
        vec![batch(vec![json!({"n": 1})]), batch(vec![json!({"n": 2})])],
        true,
        Some(&store),
    )
    .await?;

    let id = "broken".to_string();
    let out = store.get(&id, Resolution::Coarse);
    let Some(signature) = out.errors.first() else {
        panic!("the failure should be remembered even though nothing was watching");
    };
    assert!(
        signature.first_seen > 0,
        "it is stamped with when it happened"
    );
    assert!(signature.last_seen >= signature.first_seen);
    assert!(signature.count >= 1);

    // and the counter saw every one of them, throttled or not
    store.sample([(&id, &shared.counters)], 0);
    let counted = store.get(&id, Resolution::Coarse);
    let Some(bucket) = counted.buckets.first() else {
        panic!("sampling should have produced a bucket");
    };
    assert!(
        bucket.errors >= 1,
        "failures are counted in the bucket as well as named in the signature"
    );
    Ok(())
}

/// A runtime built without `with_history` keeps nothing. That is the default
/// every test and every `retention_secs: 0` deployment gets, and it must not
/// depend on remembering to turn something off.
#[tokio::test]
async fn a_runtime_without_a_history_records_nothing() -> anyhow::Result<()> {
    let store = history(3_600);
    let shared = pipeline("untracked");
    run_once(&shared, vec![batch(vec![json!({"n": 1})])], true, None).await?;

    let id = "untracked".to_string();
    assert!(
        store.get(&id, Resolution::Coarse).errors.is_empty(),
        "a runtime that was never given the store should not have reached it"
    );
    Ok(())
}

async fn get(state: Arc<AppState>, uri: &str) -> anyhow::Result<(StatusCode, PipelineHistory)> {
    let response = kayak::api_router(state)
        .oneshot(Request::builder().uri(uri).body(Body::empty())?)
        .await?;
    let status = response.status();
    let bytes = response.into_body().collect().await?.to_bytes();
    Ok((status, serde_json::from_slice(&bytes)?))
}

/// The endpoint serves what the store holds, and the query parameter picks the
/// ring.
#[tokio::test]
async fn the_endpoint_serves_both_resolutions() -> anyhow::Result<()> {
    let store = history(3_600);
    let id = "served".to_string();
    let counters = kayak::history::Counters::default();
    counters.add_inbound(12);
    store.sample([(&id, &counters)], 0);

    let state = Arc::new(AppState::new().with_history(Arc::clone(&store)));

    let (status, coarse) = get(Arc::clone(&state), "/api/pipelines/served/history").await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(coarse.resolution, Resolution::Coarse, "coarse is the default");
    assert_eq!(coarse.buckets.first().map(|b| b.inbound), Some(12));

    let (_, fine) = get(
        Arc::clone(&state),
        "/api/pipelines/served/history?resolution=fine",
    )
    .await?;
    assert_eq!(fine.resolution, Resolution::Fine);
    assert_eq!(fine.bucket_secs, kayak_core::history::FINE_BUCKET_SECS);
    assert_eq!(fine.buckets.first().map(|b| b.inbound), Some(12));
    Ok(())
}

/// An id nobody has heard of is an empty history and a 200, not a 404 —
/// history deliberately outlives its pipeline, so "no such pipeline" is not
/// this endpoint's question to answer.
#[tokio::test]
async fn an_unknown_pipeline_is_an_empty_history_rather_than_a_404() -> anyhow::Result<()> {
    let state = Arc::new(AppState::new().with_history(history(3_600)));
    let (status, out) = get(state, "/api/pipelines/never-existed/history").await?;
    assert_eq!(status, StatusCode::OK);
    assert!(out.buckets.is_empty());
    assert!(out.errors.is_empty());
    Ok(())
}

/// A misspelled resolution is the default rather than a 400: the parameter
/// picks between two views of one record, and a chart that refuses to draw
/// because of a typo in a query string is the worse outcome.
#[tokio::test]
async fn an_unreadable_resolution_falls_back_to_the_default() -> anyhow::Result<()> {
    let state = Arc::new(AppState::new().with_history(history(3_600)));
    let (status, out) = get(state, "/api/pipelines/x/history?resolution=nonsense").await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(out.resolution, Resolution::Coarse);
    Ok(())
}
