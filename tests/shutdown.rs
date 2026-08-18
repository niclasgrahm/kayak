//! What happens when the process is asked to stop.
//!
//! The signal handling itself (`kayak::shutdown::requested`) is not tested
//! here and deliberately is not: the only honest test raises a real signal at
//! the test binary, which every other test in this file shares. What is tested
//! is everything the signal *reaches* — that the graph is stopped, that the
//! outputs get their `finish`, and that the `/events` streams end, which is the
//! one thing without which axum's drain would never return.
//!
//! Offline like the rest of the suite: a tempdir and a `dummy` input.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::Request;
use kayak::api_router;
use kayak::state::AppState;
use kayak_core::connections::{ConnectionKind, FileConnection};
use serde_json::json;
use tower::ServiceExt;

/// A pipeline that sits idle — the dummy input only ticks once an hour.
fn idle(id: &str) -> anyhow::Result<kayak_core::config::Config> {
    Ok(serde_json::from_value(json!({
        "id": id,
        "inputs": [{ "type": "dummy", "duration": 3600 }],
        "transforms": [],
        "outputs": [{ "type": "stdout" }]
    }))?)
}

#[tokio::test]
async fn shutting_down_cancels_every_run_loop_and_empties_the_graph() -> anyhow::Result<()> {
    let state = AppState::new();
    let first = state.create_pipeline(idle("p1")?)?;
    let second = state.create_pipeline(idle("p2")?)?;

    state.shutdown().await;

    assert!(
        state.get_pipeline_ids().is_empty(),
        "the graph should be empty after a shutdown"
    );
    assert!(first.cancellation_token.is_cancelled(), "p1 was not stopped");
    assert!(
        second.cancellation_token.is_cancelled(),
        "p2 was not stopped"
    );
    Ok(())
}

/// The whole reason the signal handling exists. A `json_array` file has no
/// closing bracket until `finish` runs, so a file that parses as JSON is proof
/// the run loop was not merely dropped on the floor.
///
/// The `s3` output is the case that actually loses data — its part is only ever
/// in memory — but it can't be tested without a bucket, and it lands on the
/// same `finish`.
#[tokio::test]
async fn an_output_gets_its_finish_when_the_process_stops() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let data_dir = std::fs::canonicalize(directory.path())?;
    let root = data_dir.join("events");

    let state = AppState::new().with_data_dir(Some(data_dir.clone()))?;
    state.create_connection(
        "local-files".to_string(),
        ConnectionKind::File(FileConnection {
            root: root.to_string_lossy().into_owned(),
        }),
    )?;
    // One tick a second, so exactly one message is written before the shutdown
    // — enough for the array to be non-empty and few enough to read back.
    state.create_pipeline(serde_json::from_value(json!({
        "id": "to-disk",
        "inputs": [{ "type": "dummy", "duration": 1 }],
        "transforms": [],
        "outputs": [{
            "type": "file",
            "connection": "local-files",
            "path": "out",
            "format": "json_array"
        }]
    }))?)?;

    tokio::time::sleep(Duration::from_millis(1_500)).await;
    state.shutdown().await;

    let written = std::fs::read_dir(root.join("out"))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(written.len(), 1, "expected one part, got {written:?}");
    let text = std::fs::read_to_string(&written[0])?;
    let parsed: serde_json::Value = serde_json::from_str(&text)
        .unwrap_or_else(|e| panic!("the part was left unclosed ({e}): {text}"));
    assert!(
        parsed.as_array().is_some_and(|messages| !messages.is_empty()),
        "expected a non-empty json array, got {parsed}"
    );
    Ok(())
}

/// Without this the drain never finishes: an SSE response completes only when
/// the client goes away, and axum's graceful shutdown waits for every
/// connection that is still open.
#[tokio::test]
async fn the_event_stream_ends_when_the_process_stops() -> anyhow::Result<()> {
    let state = Arc::new(AppState::new());
    let app = api_router(Arc::clone(&state));

    let response = app
        .oneshot(Request::builder().uri("/events").body(Body::empty())?)
        .await?;
    let mut body = response.into_body().into_data_stream();

    state.begin_shutdown();

    // `collect` on a stream that never ends is a hang rather than a failure,
    // so the assertion is the timeout.
    let drained = tokio::time::timeout(Duration::from_secs(5), async move {
        while let Some(chunk) = futures_util::StreamExt::next(&mut body).await {
            chunk?;
        }
        Ok::<(), anyhow::Error>(())
    })
    .await;
    assert!(
        drained.is_ok(),
        "the event stream was still open after the shutdown began"
    );
    Ok(())
}

/// A revert stops every run loop through the same `stop_pipelines`, and must
/// not be mistaken for the process going away: cancelling the shutdown token
/// there would close the `/events` stream of every browser watching, once per
/// revert, and it would never reopen.
#[tokio::test]
async fn reverting_does_not_begin_a_shutdown() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("config.json");
    std::fs::write(&path, serde_json::to_string(&[idle("p1")?])?)?;

    let state = AppState::from_config(&path)?;
    let token = state.shutdown_token();
    state.revert().await?;

    assert!(
        !token.is_cancelled(),
        "a revert must not look like the process shutting down"
    );
    assert_eq!(state.get_pipeline_ids(), vec!["p1".to_string()]);
    Ok(())
}
