//! Tests for `AppState` and the pipeline *graph*: registering pipelines,
//! wiring a `pipeline` input onto its upstream, and the lifecycle rules the
//! HTTP layer maps onto status codes.

use std::time::Duration;

use kayak::state::{AppState, PipelineError};
use kayak::testing::MapSecretStore;
use kayak_core::config::{Config, InputKind};
use serde_json::json;

/// A pipeline that sits idle — the dummy input only ticks once an hour, so
/// nothing is emitted while the test runs.
fn idle(id: &str) -> anyhow::Result<Config> {
    Ok(serde_json::from_value(json!({
        "id": id,
        "inputs": [{ "type": "dummy", "duration": 3600 }],
        "transforms": [],
        "outputs": [{ "type": "stdout" }]
    }))?)
}

/// A pipeline whose dummy input ticks constantly, so downstreams see traffic.
fn chatty(id: &str) -> anyhow::Result<Config> {
    Ok(serde_json::from_value(json!({
        "id": id,
        "inputs": [{ "type": "dummy", "duration": 0 }],
        "transforms": [],
        "outputs": [{ "type": "stdout" }]
    }))?)
}

fn downstream_of(id: &str, upstream: &str) -> anyhow::Result<Config> {
    Ok(serde_json::from_value(json!({
        "id": id,
        "inputs": [{ "type": "pipeline", "upstream": upstream }],
        "transforms": [],
        "outputs": [{ "type": "stdout" }]
    }))?)
}

#[tokio::test]
async fn creating_a_pipeline_registers_it_under_its_configured_id() -> anyhow::Result<()> {
    let state = AppState::new();
    let created = state.create_pipeline(idle("p1")?)?;

    assert_eq!(created.id, "p1");
    assert_eq!(state.get_pipeline_ids(), vec!["p1".to_string()]);
    Ok(())
}

/// An omitted id becomes a random petname — three dash-separated words.
#[tokio::test]
async fn an_omitted_id_is_generated() -> anyhow::Result<()> {
    let mut config = idle("p1")?;
    config.id = None;

    let created = AppState::new().create_pipeline(config)?;
    assert_eq!(
        created.id.split('-').count(),
        3,
        "expected a 3-word petname, got '{}'",
        created.id
    );
    Ok(())
}

#[tokio::test]
async fn a_duplicate_id_is_refused() -> anyhow::Result<()> {
    let state = AppState::new();
    state.create_pipeline(idle("p1")?)?;

    let err = state.create_pipeline(idle("p1")?);
    assert!(
        matches!(err, Err(PipelineError::DuplicateId(ref id)) if id == "p1"),
        "expected DuplicateId, got {:?}",
        err.map(|s| s.id.clone())
    );
    // the original is untouched
    assert_eq!(state.get_pipeline_ids(), vec!["p1".to_string()]);
    Ok(())
}

/// A `pipeline` input names its upstream by id; naming one that isn't running
/// is a config error, not a server error.
#[tokio::test]
async fn a_pipeline_input_with_an_unknown_upstream_is_an_invalid_config() -> anyhow::Result<()> {
    let state = AppState::new();
    let err = state.create_pipeline(downstream_of("child", "missing")?);

    assert!(
        matches!(err, Err(PipelineError::InvalidConfig(_))),
        "expected InvalidConfig for an unknown upstream"
    );
    // the half-built pipeline must not be left in the map
    assert!(state.get_pipeline_ids().is_empty());
    Ok(())
}

/// The fan-out case from `config.json`: one source feeding several downstream
/// pipelines. All of them must actually receive the source's batches.
#[tokio::test]
async fn one_upstream_can_feed_several_downstream_pipelines() -> anyhow::Result<()> {
    let state = AppState::new();
    let upstream = state.create_pipeline(chatty("source")?)?;
    state.create_pipeline(downstream_of("a", "source")?)?;
    state.create_pipeline(downstream_of("b", "source")?)?;

    let mut ids = state.get_pipeline_ids();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec!["a".to_string(), "b".to_string(), "source".to_string()]
    );

    // subscribing after the fact is the same mechanism the downstreams used, so
    // receiving here proves the source is really fanning out
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    upstream.subscribe(tx);
    let got = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await?;
    assert!(got.is_some(), "upstream never fanned a batch out");
    Ok(())
}

#[tokio::test]
async fn deleting_an_unknown_pipeline_reports_not_found() {
    let state = AppState::new();
    let err = state.delete_pipeline("nope");
    assert!(
        matches!(err, Err(PipelineError::NotFound(ref id)) if id == "nope"),
        "expected NotFound"
    );
}

/// Deleting cancels the run loop rather than just dropping the handle.
#[tokio::test]
async fn deleting_a_pipeline_cancels_its_run_loop() -> anyhow::Result<()> {
    let state = AppState::new();
    let created = state.create_pipeline(idle("p1")?)?;

    state.delete_pipeline("p1")?;

    assert!(state.get_pipeline_ids().is_empty());
    assert!(
        created.cancellation_token.is_cancelled(),
        "the run loop was never signalled to stop"
    );
    Ok(())
}

/// Loading the whole of `config.json` exercises the multi-pipeline graph in one
/// go: every upstream must be registered before the pipelines that name it, or
/// building them fails.
///
/// The expectation is derived from the file rather than hardcoded — the file is
/// the UI's example config and is meant to grow — so what's actually pinned is
/// that *everything declared gets built*, and that it stays a graph rather than
/// a flat list of roots.
#[tokio::test]
async fn the_repository_config_file_starts_a_working_graph() -> anyhow::Result<()> {
    let declared: Vec<Config> =
        serde_json::from_str(&std::fs::read_to_string("example_config/config.json")?)?;
    // config.json references secrets, so it needs a store; the environment is
    // not something a test should depend on or write to
    //
    // ...and it has a file output, so it needs a `--data-dir` too, or that one
    // pipeline refuses to build and takes the load down with it. `dev_data` is
    // the same directory `just dev` passes and the connection's root sits
    // inside it, so the sample behaves here exactly as it does when run by
    // hand. It is gitignored; building the output creates it.
    let state = AppState::from_config_with(
        std::path::Path::new("example_config/config.json"),
        std::sync::Arc::new(MapSecretStore::new(
            "the config.json test store",
            &[("POSTGRES_PASSWORD", "hunter2")],
        )),
        None,
        Some(std::path::PathBuf::from("dev_data")),
    )?;

    let mut expected: Vec<String> = declared
        .iter()
        .map(|c| {
            c.id.clone()
                .ok_or_else(|| anyhow::anyhow!("config.json entries should all name themselves"))
        })
        .collect::<anyhow::Result<_>>()?;
    expected.sort_unstable();

    let mut ids = state.get_pipeline_ids();
    ids.sort_unstable();
    assert_eq!(ids, expected);

    assert!(
        declared.iter().any(|c| c
            .inputs
            .iter()
            .any(|i| matches!(i.kind, InputKind::Pipeline(_)))),
        "config.json has no `pipeline` input left, so it no longer exercises upstream wiring"
    );
    Ok(())
}

/// Ordering matters: a downstream declared before its upstream can't be built.
/// This is a real constraint on config files, so pin it rather than discover it
/// in production.
#[tokio::test]
async fn a_config_file_that_declares_a_downstream_first_is_rejected() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join(format!("kayak-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("out-of-order.json");
    std::fs::write(
        &path,
        serde_json::to_vec(&json!([downstream_of("child", "parent")?, idle("parent")?]))?,
    )?;

    let res = AppState::from_config(&path);
    std::fs::remove_file(&path)?;
    assert!(
        res.is_err(),
        "a downstream declared before its upstream should fail to load"
    );
    Ok(())
}

/// The http input end to end: what is posted to a running pipeline comes out of
/// its run loop. The subscription is the same fan-out mechanism a downstream
/// pipeline uses, so receiving here means a real pass through the pipeline
/// rather than a channel echoing back.
#[tokio::test]
async fn posted_messages_flow_through_the_pipeline() -> anyhow::Result<()> {
    let state = AppState::new();
    let pipeline = state.create_pipeline(serde_json::from_value(json!({
        "id": "ingest",
        "inputs": [{ "type": "http" }],
        "transforms": [],
        "outputs": [{ "type": "stdout" }]
    }))?)?;

    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    pipeline.subscribe(tx);

    let accepted = state.ingest("ingest", vec![json!({"n": 1}), json!({"n": 2})])?;
    assert_eq!(accepted, 2);

    let got = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("the pipeline emitted nothing"))?;
    // one post of two messages is one batch, not two passes
    assert_eq!(got.len(), 2);
    assert_eq!(got[0]["n"], json!(1));
    assert_eq!(got[1]["n"], json!(2));
    Ok(())
}

/// A post to a pipeline that exists but has no endpoint is told apart from one
/// to a pipeline that doesn't exist — both 404 at the HTTP layer, but only one
/// of them is fixed by creating the pipeline.
#[tokio::test]
async fn posting_is_refused_differently_by_a_missing_pipeline_and_a_missing_input()
-> anyhow::Result<()> {
    let state = AppState::new();
    state.create_pipeline(idle("p1")?)?;

    assert!(matches!(
        state.ingest("p1", vec![json!({"n": 1})]),
        Err(PipelineError::NotAccepting(ref id)) if id == "p1"
    ));
    assert!(matches!(
        state.ingest("nobody", vec![json!({"n": 1})]),
        Err(PipelineError::NotFound(ref id)) if id == "nobody"
    ));
    Ok(())
}

/// An empty post delivers nothing, but still has to answer the question the
/// endpoint is asked — a pipeline that isn't listening is a 404 whether the
/// body had messages in it or not.
#[tokio::test]
async fn an_empty_post_is_a_no_op_that_still_checks_the_endpoint() -> anyhow::Result<()> {
    let state = AppState::new();
    state.create_pipeline(serde_json::from_value(json!({
        "id": "ingest",
        "inputs": [{ "type": "http" }],
        "transforms": [],
        "outputs": [{ "type": "stdout" }]
    }))?)?;

    assert_eq!(state.ingest("ingest", vec![])?, 0);
    assert!(matches!(
        state.ingest("nobody", vec![]),
        Err(PipelineError::NotFound(_))
    ));
    Ok(())
}
